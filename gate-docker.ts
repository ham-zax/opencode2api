#!/usr/bin/env bun

/**
 * OpenCode2API — Per-Key IP Pool Reverse Proxy Gateway
 *
 * Each API Key has an independent proxy slot pool (up to SLOTS_PER_KEY)
 * Up to MAX_ACTIVE_KEYS keys concurrently active globally
 * Automatic slot replacement on failure, automatic release on timeout
 * WARP acts as global shared fallback
 */

import https from 'node:https';
import http from 'node:http';
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { isIP } from 'node:net';
import { spawn } from 'node:child_process';
import { HttpsProxyAgent } from 'hpagent';
import { SocksProxyAgent } from 'socks-proxy-agent';
import geoip from 'geoip-lite';
import { buildZenCatalog, resolveCatalogModel, type ZenModel } from './model-catalog';

// ═══════════════════════════════════════════════════════════
//  Type Definitions
// ═══════════════════════════════════════════════════════════

interface ProxyItem {
  address: string;
  protocol: string;
  latency: number;
  quality_grade: string;
  country?: string;
}

interface Slot {
  addr: string;
  url: string;
  proto: 'http' | 'socks5';
  latencyMs: number;
  qualityGrade: string;
}

interface KeySlotPool {
  keyId: string;
  slots: Slot[];
  rrCursor: number;
  lastUsedAt: number;
}

interface CandidateItem extends ProxyItem {
  lockedBy: string | null;
  /** Consecutive probe failures; evicted after MAX_CANDIDATE_FAILS. */
  failCount?: number;
}

// Candidates that fail probing this many times in a row are evicted so the
// pool cannot fill up with permanently dead proxies that are retried forever.
const MAX_CANDIDATE_FAILS = 3;

// ═══════════════════════════════════════════════════════════
//  Persistence File Paths
// ═══════════════════════════════════════════════════════════

// State lives in DATA_DIR when set (docker-compose mounts /app/data) so a
// container restart does not silently reset keys, sources and proxy history.
// Defaults to the working directory. Only persistence is relocated — static
// assets and the scraper script stay relative to the code.
const DATA_DIR = process.env.DATA_DIR || process.cwd();
const KEYS_FILE = path.join(DATA_DIR, 'keys.json');
const SOURCES_FILE = path.join(DATA_DIR, 'sources.json');
const CUSTOM_PROXIES_FILE = path.join(DATA_DIR, 'custom_proxies.json');
const AUDIT_FILE = path.join(DATA_DIR, 'audit.jsonl');

// ═══════════════════════════════════════════════════════════
//  Country Filters & Proxy Sources
// ═══════════════════════════════════════════════════════════

const BLOCKED_COUNTRIES = new Set([
  'CN', // China (Region blocked by OpenCode)
  'RU', // Russia (Region blocked by OpenCode)
]);

const PREFERRED_COUNTRIES = new Set([
  'IN', // India (Primary user region - fast & verified)
  'US', // United States (Fully supported)
  'GB', // United Kingdom
  'DE', // Germany
  'FR', // France
  'SG', // Singapore (Low latency to India)
  'CA', // Canada
  'JP', // Japan
  'NL', // Netherlands
  'AU', // Australia
]);

// geoip-lite ships a static snapshot and is routinely wrong about the
// datacenter ranges these proxy lists are made of — 2 of 3 sampled proxies
// were labelled GB while actually resolving to IN and FR. Both
// BLOCKED_COUNTRIES and PREFERRED_COUNTRIES read that label, so a
// reassigned RU/CN address sails straight through the block into rotation.
// geoip-lite 2.0.3 is the latest release, so the local DB cannot be
// refreshed; re-verify the head of the pool against a live lookup instead,
// cached, and let the live answer win.
const GEO_VERIFY_URL = 'https://ipinfo.io/{ip}/json';
const GEO_VERIFY_TTL_MS = 6 * 60 * 60 * 1000;
const GEO_VERIFY_BATCH = 8;
const geoVerified = new Map<string, { country: string; checkedAt: number }>();

async function geoLookupLive(ip: string): Promise<string | null> {
  const hit = geoVerified.get(ip);
  if (hit && Date.now() - hit.checkedAt < GEO_VERIFY_TTL_MS) return hit.country;
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), 8000);
  try {
    const res = await fetch(GEO_VERIFY_URL.replace('{ip}', ip), {
      signal: ctl.signal,
      headers: { accept: 'application/json' },
    });
    if (!res.ok) return null;
    const doc: any = await res.json();
    const country = typeof doc?.country === 'string' ? doc.country.trim().toUpperCase() : '';
    if (!country) return null;
    geoVerified.set(ip, { country, checkedAt: Date.now() });
    return country;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// Re-label and evict the head of the pool — the addresses most likely to be
// handed out next. Only the head is verified: a full sweep of ~27k addresses
// would cost more than the block it defends against. A failed lookup is left
// alone so pool size is preserved.
async function revalidatePoolGeo(limit = 24): Promise<void> {
  const targets = candidates.filter(c => !c.lockedBy).slice(0, limit);
  for (let i = 0; i < targets.length; i += GEO_VERIFY_BATCH) {
    await Promise.all(targets.slice(i, i + GEO_VERIFY_BATCH).map(async (item) => {
      const live = await geoLookupLive(item.address.split(':')[0]);
      if (!live || live === item.country) return;
      console.log(`[GeoVerify] ${item.address} local=${item.country} live=${live}`);
      item.country = live;
      if (BLOCKED_COUNTRIES.has(live)) item.lockedBy = '__geoverify__';
    }));
  }
  const blocked = candidates.filter(c => c.lockedBy === '__geoverify__');
  if (blocked.length > 0) {
    for (const c of blocked) {
      console.log(`[GeoVerify] ${c.address} live-resolves to blocked ${c.country}, evicted`);
      const idx = candidates.indexOf(c);
      if (idx >= 0) candidates.splice(idx, 1);
    }
  }
}

// Plain-text proxy lists come in two flavors: bare `ip:port` (clarketm,
// speedx) and scheme-prefixed `socks5://ip:port` (ProxyScrape shards).
// Derive the protocol from the scheme when present, else fall back.
// GeoNode API: { data: [{ ip, port, protocols[], country, latency, uptime }] }.
// Prefer socks5 over http when an entry lists both; socks4-only dropped
// (gateway can't speak it).
function geonodeParser(data: any): ProxyItem[] {
  const list: any[] = Array.isArray(data?.data) ? data.data : [];
  return list
    .filter((p) => p?.ip && p?.port && Array.isArray(p.protocols) &&
      p.protocols.some((x: string) => x === 'socks5' || x === 'http'))
    .map((p) => ({
      address: `${p.ip}:${p.port}`,
      protocol: p.protocols.includes('socks5') ? 'socks5' : 'http',
      latency: (typeof p.latency === 'number' && p.latency > 0) ? p.latency : 999,
      quality_grade: 'C',
      country: p.country || undefined,
    }));
}

function textParser(fallbackProto: 'http' | 'socks5') {  return (data: string): ProxyItem[] => {
    const out: ProxyItem[] = [];
    for (const rawLine of data.split('\n')) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#')) continue;
      const m = line.match(/^(?:(https?|socks5h?):\/\/)?(\d+\.\d+\.\d+\.\d+:\d+)$/i);
      if (!m) continue;
      const scheme = (m[1] || '').toLowerCase();
      const protocol = scheme.startsWith('socks') ? 'socks5' : scheme.startsWith('http') ? 'http' : fallbackProto;
      out.push({ address: m[2], protocol, latency: 999, quality_grade: 'C' });
    }
    return out;
  };
}

const DEFAULT_SOURCES = [
  {
    name: 'amux',
    url: 'https://proxy.amux.ai/api/proxies',
    type: 'json' as const,
    parser: (data: any): ProxyItem[] => {
      const list: any[] = Array.isArray(data) ? data : [];
      return list
        .filter((p) => ['S','A','B','C'].includes(p.quality_grade) && p.status === 'active')
        .map((p) => ({ address: p.address, protocol: p.protocol, latency: p.latency || 999, quality_grade: p.quality_grade }));
    },
  },
  {
    name: 'proxifly',
    url: 'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json',
    type: 'json' as const,
    parser: (data: any): ProxyItem[] => {
      const list: any[] = Array.isArray(data) ? data : [];
      return list
        .filter((p) => p.ip && p.port && (p.protocol === 'socks5' || p.protocol === 'http'))
        .map((p) => ({
          address: `${p.ip}:${p.port}`,
          protocol: p.protocol,
          latency: p.latency || 999,
          quality_grade: p.score >= 1 ? 'B' : 'C',
          country: p.geolocation?.country || undefined,
        }));
    },
  },
  {
    name: 'speedx-socks5',
    url: 'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
    type: 'text' as const,
    parser: (data: string): ProxyItem[] => {
      return data.split('\n')
        .map(line => line.trim())
        .filter(line => line && /^\d+\.\d+\.\d+\.\d+:\d+$/.test(line))
        .map(line => ({ address: line, protocol: 'socks5', latency: 999, quality_grade: 'C' }));
    },
  },
  {
    name: 'clarketm-http',
    url: 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
    type: 'text' as const,
    parser: textParser('http'),
  },
  {
    name: 'proxyscrape-de-socks5',
    url: 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/countries/de/socks5/data.txt',
    type: 'text' as const,
    parser: textParser('socks5'),
  },
  {
    name: 'proxyscrape-in-socks5',
    url: 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/countries/in/socks5/data.txt',
    type: 'text' as const,
    parser: textParser('socks5'),
  },
  {
    name: 'geonode-de-socks5',
    url: 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&country=DE&protocols=socks5',
    type: 'json' as const,
    parser: geonodeParser,
  },
  {
    name: 'geonode-elite-http',
    url: 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http&anonymityLevel=elite',
    type: 'json' as const,
    parser: geonodeParser,
  },
];

let proxySources: typeof DEFAULT_SOURCES = [];

function loadSources() {
  try {
    const raw = JSON.parse(fs.readFileSync(SOURCES_FILE, 'utf-8'));
    // Parsers cannot be serialized; match by name using default source parsers on restoration
    proxySources = raw.map((s: any) => {
      const def = DEFAULT_SOURCES.find(d => d.name === s.name);
      return {
        name: s.name,
        url: s.url,
        type: s.type || 'json',
        parser: def ? def.parser : DEFAULT_SOURCES[0].parser,
      };
    });
    console.log(`[Sources] Loaded ${proxySources.length} proxy sources`);
  } catch {
    proxySources = DEFAULT_SOURCES.map(s => ({ ...s }));
    saveSources();
  }
}

function saveSources() {
  try {
    const data = proxySources.map(s => ({ name: s.name, url: s.url, type: s.type }));
    fs.writeFileSync(SOURCES_FILE, JSON.stringify(data, null, 2), 'utf-8');
  } catch (e: any) {
    console.error(`[Sources] Save failed: ${e.message}`);
  }
}

// ═══════════════════════════════════════════════════════════
//  Custom Proxy Persistence
// ═══════════════════════════════════════════════════════════

let customProxyItems: ProxyItem[] = [];

function loadCustomProxies() {
  try {
    const data = JSON.parse(fs.readFileSync(CUSTOM_PROXIES_FILE, 'utf-8'));
    customProxyItems = Array.isArray(data) ? data : [];
    console.log(`[Custom Proxies] Loaded ${customProxyItems.length} items`);
  } catch {
    customProxyItems = [];
  }
}

function saveCustomProxies() {
  try {
    fs.writeFileSync(CUSTOM_PROXIES_FILE, JSON.stringify(customProxyItems, null, 2), 'utf-8');
  } catch (e: any) {
    console.error(`[Custom Proxies] Save failed: ${e.message}`);
  }
}

// ═══════════════════════════════════════════════════════════
//  Constants
// ═══════════════════════════════════════════════════════════

const UPSTREAM = 'https://opencode.ai/zen';
const PORT = parseInt(process.env.PORT || '13339');
const MAX_RETRIES = 3;
const TIMEOUT = 15000;
const STREAM_TIMEOUT = 60000;

const MAX_ACTIVE_KEYS = 20;
const SLOTS_PER_KEY = 3;
const POOL_CLEANUP_MS = 60000;
const KEY_IDLE_RELEASE_MS = 600000;

const PROXY_PROBE_TIMEOUT = parseInt(process.env.PROXY_PROBE_TIMEOUT || '2500');
const PROXY_REFRESH_MS = parseInt(process.env.PROXY_REFRESH_MS || '300000');
const CUSTOM_PROXIES = process.env.CUSTOM_PROXIES || '';
const ZENPROXY_RELAY = process.env.ZENPROXY_RELAY || 'https://zenproxy.top/api/relay';
const ZENPROXY_KEY = process.env.ZENPROXY_KEY || '';
const FORCE_RELAY = process.env.FORCE_RELAY === '1';

const WARP_MODE = process.env.WARP_MODE || 'off';
const WARP_SOCKS5_PORT = parseInt(process.env.WARP_SOCKS5_PORT || '1080');
const WARP_HOST = process.env.WARP_HOST || '127.0.0.1';

// ═══════════════════════════════════════════════════════════
//  Global State
// ═══════════════════════════════════════════════════════════

let warpModeRuntime = WARP_MODE;
let warpHostRuntime = WARP_HOST;
let warpPortRuntime = WARP_SOCKS5_PORT;
let warpStatus: 'unknown' | 'running' | 'stopped' = 'unknown';
let warpSlot: Slot | null = null;
let warpConsecutiveFails = 0;
let warpSkipUntil = 0;

let cachedModels: ZenModel[] = [];
let cachedModelsTime = 0;

// Catalog membership establishes pricing and transport; live probes establish
// availability independently, so a rate limit never changes model pricing.
type ModelVerdict = 'unknown' | 'healthy' | 'dead';

interface ModelHealth {
  verdict: ModelVerdict;
  status: number;
  reason: string;
  consecutiveFails: number;
  checkedAt: number;
}
const freeModelHealth = new Map<string, ModelHealth>();

// 429 is a rate limit, not a broken model — never let it mark one dead, or a
// busy gateway would blacklist every model it actually depends on.
const MODEL_DEAD_AFTER_FAILS = 3;
const MODEL_VERIFY_INTERVAL_MS = 30 * 60 * 1000;
const MODEL_VERIFY_CONCURRENCY = 2;
let verifyingModels = false;
// Set once a verification pass has actually produced verdicts. Distinguishes
// "not checked yet" from "checked and unresolved", so a cold start still
// advertises the upstream list instead of advertising nothing.
let modelsVerified = false;

// Only genuine rate limiting and transport blips are transient. 5xx must NOT
// be here: treating it as transient reset the failure counter to 0 on every
// pass, so a model that hard-500s forever could never reach the dead threshold
// and stayed advertised indefinitely.
// The Zen anonymous lane only serves requests that are shaped like a real
// OpenCode agent turn. Two conditions are jointly required, verified by
// ablation against the live endpoint:
//   1. x-opencode-session matches ^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$
//   2. stream is true
// Either one alone returns 403 FreeTierError ("OpenCode's free tier can only be
// used from within OpenCode"), which is why testing them one at a time is
// misleading — a fix for one looks like it did nothing.
//
// Tool count, system-prompt size, tool_choice and max_tokens were all ablated
// and are irrelevant; five stub tool definitions are enough.
//
// Set FREE_TIER_AGENT_SHAPE=0 to turn this off and send bodies verbatim.
const FREE_TIER_AGENT_SHAPE = (process.env.FREE_TIER_AGENT_SHAPE || '1') !== '0';

// Core tool names the anonymous lane expects to see on an agent-shaped request.
const FREE_TIER_CORE_TOOLS = ['bash', 'edit', 'glob', 'grep', 'read'];

const CANONICAL_SESSION_RE = /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/;
const BASE62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

// Derive a session id in OpenCode's canonical shape from any signal. The same
// conversation seed always maps to the same id, so upstream prompt-cache
// affinity survives; a signal that is already canonical passes through
// unchanged.
function canonicalSessionID(signal: string): string {
  if (CANONICAL_SESSION_RE.test(signal)) return signal;
  const digest = sha256Hex(`ses\x00${signal}`);
  // 12 lowercase hex chars from the front, then 14 base62 chars from the next
  // 10 bytes, matching the format real OpenCode clients emit.
  const timePart = digest.slice(0, 12);
  let acc = BigInt('0x' + digest.slice(12, 32));
  let tail = '';
  for (let i = 0; i < 14; i++) {
    tail = BASE62[Number(acc % 62n)] + tail;
    acc /= 62n;
  }
  return `ses_${timePart}${tail}`;
}

function freeTierStubToolset(names: string[]): any[] {
  return names.map(name => ({
    type: 'function',
    function: {
      name,
      description: `${name} tool`,
      parameters: { type: 'object', properties: {}, required: [] },
    },
  }));
}

// Force an OpenAI chat body into agent shape. Returns the (possibly rewritten)
// body plus whether the caller had asked for a stream — when we upgrade a
// non-streaming request the response has to be reassembled from SSE, so the
// caller-facing contract still has to be a single JSON object.
function shapeAgentRequest(
  bodyStr: string,
  endpoint: 'chat' | 'responses' = 'chat',
): { body: string; callerWantsStream: boolean; reshaped: boolean } {
  if (!FREE_TIER_AGENT_SHAPE) {
    let wantsStream = false;
    try { wantsStream = (JSON.parse(bodyStr) as any)?.stream === true; } catch {}
    return { body: bodyStr, callerWantsStream: wantsStream, reshaped: false };
  }
  let parsed: any;
  try {
    parsed = JSON.parse(bodyStr);
  } catch {
    return { body: bodyStr, callerWantsStream: false, reshaped: false };
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { body: bodyStr, callerWantsStream: false, reshaped: false };
  }
  const callerWantsStream = parsed.stream === true;
  let changed = false;

  if (parsed.stream !== true) { parsed.stream = true; changed = true; }
  if (endpoint === 'chat' && (!parsed.stream_options || parsed.stream_options.include_usage !== true)) {
    parsed.stream_options = { ...(parsed.stream_options || {}), include_usage: true };
    changed = true;
  }

  if (endpoint === 'responses' && parsed.stream_options) { delete parsed.stream_options; changed = true; }

  // Preserve native/client toolsets. Declare fallback tools only when absent.
  const declared: string[] = Array.isArray(parsed.tools)
    ? parsed.tools.map((t: any) => String(t?.function?.name || t?.name || '')).filter(Boolean)
    : [];
  const missing = declared.length ? [] : FREE_TIER_CORE_TOOLS;
  if (missing.length) {
    const tools = freeTierStubToolset(missing).map(t => endpoint === 'responses'
      ? { type: 'function', ...t.function } : t);
    parsed.tools = Array.isArray(parsed.tools) ? [...parsed.tools, ...tools] : tools;
    changed = true;
  }

  if (!changed) return { body: bodyStr, callerWantsStream, reshaped: false };
  return { body: JSON.stringify(parsed), callerWantsStream, reshaped: true };
}

function isTransientStatus(status: number): boolean {
  return status === 0 || status === 408 || status === 425 || status === 429;
}

// A deterministic rejection the same request will hit again next time: the
// free tier is locked to the OpenCode client (403), or the provider does not
// offer the model (400/404/422). Retrying cannot help, so these go straight to
// dead on the first observation instead of burning three verification passes
// (90 minutes at the default interval) while callers keep getting raw errors.
function isHardFailure(status: number): boolean {
  return status === 400 || status === 401 || status === 403 || status === 404 || status === 422;
}

// models.dev pricing metadata: source of truth for cost==0 (free), so stealth
// free models like `big-pickle` (no `-free` suffix) are detected without hardcoding.
let modelsDevMetadata: any = null;
let modelsDevTime = 0;
const MODELS_DEV_URL = 'https://models.dev/api.json';
const MODELS_DEV_TTL_MS = 3600000;
const MODEL_CATALOG_TTL_MS = 5 * 60 * 1000;
const MODEL_CATALOG_MAX_STALE_MS = 24 * 60 * 60 * 1000;
const MODEL_CATALOG_RETRY_MS = 30 * 1000;
const MODEL_CATALOG_FILE = path.join(DATA_DIR, 'models_cache.json');
const OPENCODE_USER_AGENT = process.env.OPENCODE_USER_AGENT || 'opencode/latest/2.0.21/cli';
let catalogRefresh: Promise<ZenModel[]> | null = null;
let catalogLastAttempt = 0;
let catalogLastError = '';
let catalogExcluded: { id: string; reason: string }[] = [];

const API_KEY = process.env.API_KEY || 'admin123';

let candidates: CandidateItem[] = [];
let customSlots: Slot[] = [];
const PROXY_MAX_FAILS = 3;
let proxyFailCount = new Map<string, number>();

// ── Two-level exit health ──
// The four opencode2api implementations all converged on the same split, and
// measurement here agrees with them: a 403 or "Model is unavailable" is a
// property of (exit × model), not of the exit, and a 429 is rate limiting
// rather than a broken proxy. Treating either as a dead exit — which a single
// global failure counter does — evicts proxies that are perfectly healthy for
// every other model and never brings back ones that were merely throttled.
//
//   429 / 5xx on the wire  -> exit cooldown, exponential, cleared on expiry
//   401 / 403 / 404 / 402  -> ban (exit × model) only, that pairing expires
//   transport error / 5xx -> exit failure strike, evicted at PROXY_MAX_FAILS
//
// Modelled on FishBottle7/opencode2dsh (pool/pool.ts ExitHealth + ModelBan)
// and jasonxu114514/opencode2api (internal/gateway/pool.go anonymousNode
// cooldown, which honours Retry-After).
const EXIT_COOLDOWN_BASE_MS = 30_000;
const EXIT_COOLDOWN_MAX_MS = 10 * 60_000;
const MODEL_BAN_TTL_MS = 10 * 60_000;
const MODEL_BAN_FAILS = 2;

interface ExitHealth {
  /** Consecutive transport/5xx failures. */
  fails: number;
  /** Deadline until which the exit is skipped. */
  cooldownUntil: number;
  /** Consecutive cooldowns, for exponential backoff. */
  cooldownStreak: number;
}

const exitHealth = new Map<string, ExitHealth>();
/** addr -> model -> { fails, bannedUntil } */
const exitModelBans = new Map<string, Map<string, { fails: number; bannedUntil: number }>>();

function exitState(addr: string): ExitHealth {
  let s = exitHealth.get(addr);
  if (!s) { s = { fails: 0, cooldownUntil: 0, cooldownStreak: 0 }; exitHealth.set(addr, s); }
  return s;
}

function isExitUsable(addr: string, model: string | undefined): boolean {
  const s = exitHealth.get(addr);
  if (s && s.cooldownUntil > Date.now()) return false;
  if (model) {
    const bans = exitModelBans.get(addr);
    const b = bans?.get(model);
    if (b && b.bannedUntil > Date.now()) return false;
  }
  return true;
}

function noteExitSuccess(addr: string, model?: string): void {
  const s = exitState(addr);
  s.fails = 0;
  s.cooldownUntil = 0;
  s.cooldownStreak = 0;
  if (model) {
    const bans = exitModelBans.get(addr);
    bans?.delete(model);
  }
}

// Classify one upstream failure. Returns a short label for logging.
function noteExitFailure(addr: string, status: number, model?: string): string {
  // Rate limiting: back the exit off, do not count it as broken, and honour
  // Retry-After when upstream sends one.
  if (status === 429) {
    const s = exitState(addr);
    s.cooldownStreak = Math.min(s.cooldownStreak + 1, 4);
    let delay = Math.min(EXIT_COOLDOWN_BASE_MS * Math.pow(2, s.cooldownStreak - 1), EXIT_COOLDOWN_MAX_MS);
    const ra = Number(proxyRetryAfterMs.get(addr) || 0);
    if (ra > delay) delay = Math.min(ra, EXIT_COOLDOWN_MAX_MS);
    s.cooldownUntil = Date.now() + delay;
    proxyFailCount.delete(addr);
    return `cooldown ${Math.round(delay / 1000)}s`;
  }

  // Deterministic per-model rejections: quarantine this pairing, leave the
  // exit in rotation for every other model.
  if (status === 401 || status === 402 || status === 403 || status === 404) {
    if (model) {
      let bans = exitModelBans.get(addr);
      if (!bans) { bans = new Map(); exitModelBans.set(addr, bans); }
      const b = bans.get(model) || { fails: 0, bannedUntil: 0 };
      b.fails += 1;
      if (b.fails >= MODEL_BAN_FAILS) b.bannedUntil = Date.now() + MODEL_BAN_TTL_MS;
      bans.set(model, b);
    }
    return `model-ban (${model || 'unknown'})`;
  }

  // Ambiguous: could be the proxy or the upstream. Count a strike so a
  // genuinely broken exit is eventually evicted.
  const s = exitState(addr);
  s.fails += 1;
  const fails = (proxyFailCount.get(addr) || 0) + 1;
  proxyFailCount.set(addr, fails);
  if (fails >= PROXY_MAX_FAILS || s.fails >= PROXY_MAX_FAILS) {
    const bc = candidates.find(c => c.address === addr);
    if (bc) bc.lockedBy = '__blacklist__';
    return `evicted after ${fails} failures`;
  }
  return `strike ${fails}/${PROXY_MAX_FAILS}`;
}

/** Retry-After deadlines observed per exit, consumed by noteExitFailure. */
const proxyRetryAfterMs = new Map<string, number>();
function rememberRetryAfter(addr: string, headers: Record<string, string> | undefined): void {
  const ra = headers?.['retry-after'];
  if (!ra) return;
  const secs = Number(ra);
  if (Number.isFinite(secs) && secs > 0) {
    proxyRetryAfterMs.set(addr, Math.min(secs * 1000, EXIT_COOLDOWN_MAX_MS));
  } else {
    const when = Date.parse(ra);
    if (!Number.isNaN(when)) {
      proxyRetryAfterMs.set(addr, Math.min(Math.max(0, when - Date.now()), EXIT_COOLDOWN_MAX_MS));
    }
  }
}

let keySlotPools: Map<string, KeySlotPool> = new Map();
let refreshing = false;

const START_TIME = Date.now();
const stats = { total: 0, success: 0, rateLimited: 0, errors: 0 };
const recentLogs: string[] = [];
const MAX_LOGS = 500;

// ═══════════════════════════════════════════════════════════
//  Audit & API Key Management
// ═══════════════════════════════════════════════════════════

interface AuditEntry {
  ts: number;
  keyId: string;
  model: string;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  cacheCreation: number;
  cacheRead: number;
  latencyMs: number;
  status: number;
  slotAddr: string;
}
const auditLog: AuditEntry[] = [];
const MAX_AUDIT = 10000;

interface ApiKeyRecord {
  key: string;
  name: string;
  enabled: boolean;
  createdAt: number;
  lastUsedAt: number;
  totalRequests: number;
  totalTokens: number;
  maxConcurrency: number;
  maxRequests: number;
  requestCount: number;
  expiresAt: number;
}
let apiKeys: Record<string, ApiKeyRecord> = {};
let activeRequests: Record<string, number> = {};

function loadKeys() {
  try {
    const data = fs.readFileSync(KEYS_FILE, 'utf-8');
    apiKeys = JSON.parse(data);
    console.log(`[Keys] Loaded ${Object.keys(apiKeys).length} API Keys`);
  } catch {
    apiKeys = {};
    saveKeys();
  }
  if (!apiKeys[API_KEY]) {
    apiKeys[API_KEY] = {
      key: API_KEY, name: 'default', enabled: true, createdAt: Date.now(),
      lastUsedAt: 0, totalRequests: 0, totalTokens: 0,
      maxConcurrency: 0, maxRequests: 0, requestCount: 0, expiresAt: 0,
    };
    saveKeys();
  } else {
    const r = apiKeys[API_KEY];
    if (r.maxConcurrency === undefined) r.maxConcurrency = 0;
    if (r.maxRequests === undefined) r.maxRequests = 0;
    if (r.requestCount === undefined) r.requestCount = 0;
    if (r.expiresAt === undefined) r.expiresAt = 0;
  }
}

function saveKeys() {
  try {
    fs.writeFileSync(KEYS_FILE, JSON.stringify(apiKeys, null, 2), 'utf-8');
  } catch (e: any) {
    console.error(`[Keys] Save failed: ${e.message}`);
  }
}

function validateKey(key: string): { ok: boolean; reason?: string } {
  const record = apiKeys[key];
  if (!record) return { ok: false, reason: 'Key not found' };
  if (!record.enabled) return { ok: false, reason: 'Key is disabled' };
  if (record.expiresAt > 0 && Date.now() > record.expiresAt) return { ok: false, reason: 'Key is expired' };
  if (record.maxRequests > 0 && record.requestCount >= record.maxRequests) return { ok: false, reason: 'Request limit reached' };
  if (record.maxConcurrency > 0 && (activeRequests[key] || 0) >= record.maxConcurrency) return { ok: false, reason: 'Concurrency limit reached' };
  return { ok: true };
}

function acquireKey(key: string) {
  activeRequests[key] = (activeRequests[key] || 0) + 1;
}

function releaseKey(key: string) {
  if (activeRequests[key] && activeRequests[key] > 0) activeRequests[key]--;
}

function recordKeyRequest(key: string) {
  const record = apiKeys[key];
  if (!record) return;
  record.lastUsedAt = Date.now();
  record.totalRequests++;
  record.requestCount++;
  saveKeys();
}

function recordKeyUsage(key: string, tokens: number) {
  const record = apiKeys[key];
  if (record) {
    record.lastUsedAt = Date.now();
    record.totalTokens += tokens;
    saveKeys();
  }
}

// ═══════════════════════════════════════════════════════════
//  Log Capture
// ═══════════════════════════════════════════════════════════

function logCapture(s: string) {
  const line = `[${new Date().toLocaleTimeString()}] ${s}`;
  recentLogs.push(line);
  if (recentLogs.length > MAX_LOGS) recentLogs.shift();
}
const _origLog = console.log;
console.log = (...args: any[]) => {
  const msg = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  logCapture(msg);
  _origLog.apply(console, args);
};
const _origWarn = console.warn;
console.warn = (...args: any[]) => {
  const msg = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  logCapture(`⚠️ ${msg}`);
  _origWarn.apply(console, args);
};
const _origError = console.error;
console.error = (...args: any[]) => {
  const msg = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  logCapture(`❌ ${msg}`);
  _origError.apply(console, args);
};

const FORWARD = [
  'authorization', 'x-opencode-project', 'x-opencode-session',
  'x-opencode-request', 'x-opencode-client', 'content-type',
  'accept', 'anthropic-version', 'anthropic-beta', 'user-agent',
];

// ═══════════════════════════════════════════════════════════
//  Custom Proxies (Fallback standby)
// ═══════════════════════════════════════════════════════════

function parseCustomProxies(input: string): ProxyItem[] {
  if (!input.trim()) return [];
  return input.split(',').map((addr) => {
    const trimmed = addr.trim();
    if (!trimmed) return null;
    const isSocks = trimmed.startsWith('socks5://') || trimmed.startsWith('socks5h://');
    return {
      address: trimmed.replace(/^https?:\/\//, '').replace(/^socks5h?:\/\//, ''),
      protocol: isSocks ? 'socks5' : 'http',
      latency: 0,
      quality_grade: 'custom',
    };
  }).filter((p): p is ProxyItem => p !== null);
}

// Rebuild the standby slot list from scratch so this is safe to re-run (the
// dashboard's "Probe Standby" button). Append-only would duplicate slots on
// every re-probe. Returns a per-slot verdict for the API to report.
async function initCustomSlots(): Promise<{ addr: string; ok: boolean; latencyMs: number }[]> {
  customSlots = [];
  if (!CUSTOM_PROXIES) return [];
  const items = parseCustomProxies(CUSTOM_PROXIES);
  if (items.length === 0) return [];
  const results = await Promise.all(items.map(async (item) => {
    const r = await probe(item);
    return { item, ...r };
  }));
  const verdicts: { addr: string; ok: boolean; latencyMs: number }[] = [];
  for (const r of results) {
    verdicts.push({ addr: r.item.address, ok: r.ok, latencyMs: r.latencyMs });
    if (!r.ok) continue;
    const url = r.item.protocol === 'socks5' ? `socks5h://${r.item.address}` : `http://${r.item.address}`;
    customSlots.push({ addr: r.item.address, url, proto: r.item.protocol as 'http' | 'socks5', latencyMs: r.latencyMs || 0, qualityGrade: 'C' });
    console.log(`[Fallback+] ${r.item.address} (${r.latencyMs}ms)`);
  }
  console.log(`[Fallback] ${customSlots.length}/${items.length} custom proxies ready`);
  return verdicts;
}

// ═══════════════════════════════════════════════════════════
//  Candidate Pool (Proxy List Aggregation)
// ═══════════════════════════════════════════════════════════

// Per-source outcome of the last fetch. Without this the sources view reports
// every feed as healthy with the pool-wide count, so a feed that timed out
// looked identical to one delivering thousands of proxies.
interface SourceHealth {
  count: number;
  error: string | null;
  fetchedAt: number;
  /** Consecutive failed fetches. */
  strikes: number;
  /** Deadline before which this feed is skipped. */
  openUntil: number;
}
const sourceHealth = new Map<string, SourceHealth>();

// Per-source circuit breaker. A feed that times out or 5xxs costs a full
// request timeout on every refresh; with 8 feeds refreshing on a timer that is
// a steady drain for no new proxies. Trip after SOURCE_BREAKER_FAILS and skip
// the feed until the cooldown expires, then let one probe through. Same idea as
// the breaker in FishBottle7/opencode2dsh (pool/refill.ts) and the source-level
// cooldown in GoProxy.
const SOURCE_BREAKER_FAILS = 3;
const SOURCE_BREAKER_COOLDOWN_MS = 15 * 60_000;

function sourceBreakerOpen(name: string): boolean {
  const h = sourceHealth.get(name);
  return !!h && (h.openUntil || 0) > Date.now();
}

// Wrapper records the outcome of each fetch so /api/sources can report real
// per-feed health. An empty list is only healthy if the feed actually
// answered — distinguish "returned nothing" from "never responded".
async function fetchSource(source: typeof DEFAULT_SOURCES[0]): Promise<ProxyItem[]> {
  let items: ProxyItem[] = [];
  let error: string | null = null;
  try {
    items = await fetchSourceInner(source);
    if (items.length === 0) error = 'empty list';
  } catch (e: any) {
    error = e?.message || String(e);
  }
  const prev = sourceHealth.get(source.name);
  const now = Date.now();
  let strikes: number;
  let openUntil: number;
  if (error) {
    strikes = (prev?.strikes || 0) + 1;
    // Back off further the longer a feed stays broken, capped like the exits.
    const delay = Math.min(SOURCE_BREAKER_COOLDOWN_MS * Math.pow(2, Math.min(strikes - 1, 3)), 6 * 60 * 60_000);
    openUntil = strikes >= SOURCE_BREAKER_FAILS ? now + delay : 0;
  } else {
    strikes = 0;
    openUntil = 0;
  }
  const wasOpen = (prev?.openUntil || 0) > now;
  sourceHealth.set(source.name, { count: items.length, error, fetchedAt: now, strikes, openUntil });
  if (error && strikes >= SOURCE_BREAKER_FAILS && !wasOpen) {
    console.warn(`[Source][${source.name}] breaker open after ${strikes} strikes, skipping for ${Math.round((openUntil - now) / 60000)}min`);
  } else if (!error && wasOpen) {
    console.log(`[Source][${source.name}] recovered, breaker closed`);
  }
  return items;
}

async function fetchSourceInner(source: typeof DEFAULT_SOURCES[0]): Promise<ProxyItem[]> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), 15000);
  try {
    if (warpModeRuntime === 'on' && warpSlot) {
      const agent = new SocksProxyAgent(warpSlot.url, { timeout: 10000 }) as unknown as https.Agent;
      const body = await new Promise<string>((resolve, reject) => {
        const req = https.request(source.url, {
          method: 'GET',
          headers: { 'user-agent': 'Mozilla/5.0' },
          agent,
          rejectUnauthorized: false,
          timeout: 10000,
        }, (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c: Buffer) => chunks.push(c));
          res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
          res.on('error', reject);
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
        req.end();
      });
      const raw = source.type === 'json' ? JSON.parse(body) : body;
      const items = source.parser(raw);
      if (items.length === 0) { console.warn(`[Source][${source.name}] Empty list`); return []; }
      console.log(`[Source][${source.name}] ${items.length} proxies`);
      return items;
    } else {
      const res = await fetch(source.url, { signal: ctl.signal });
      if (!res.ok) { console.warn(`[Source][${source.name}] HTTP ${res.status}`); throw new Error(`HTTP ${res.status}`); }
      const raw = source.type === 'json' ? await res.json() : await res.text();
      const items = source.parser(raw);
      if (items.length === 0) { console.warn(`[Source][${source.name}] Empty list`); return []; }
      console.log(`[Source][${source.name}] ${items.length} proxies`);
      return items;
    }
  } catch (e: any) {
    // Rethrow so the wrapper can attribute the failure to this feed. Swallowing
    // it here is what let a timed-out feed report as healthy.
    console.warn(`[Source][${source.name}] Failed: ${e.message}`);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

async function loadCandidates(): Promise<void> {
  const seen = new Set<string>();
  const all: ProxyItem[] = [];
  // Feeds whose breaker is open are skipped: re-requesting a feed that is
  // already known to be down just burns a timeout on every refresh. The last
  // known count is carried forward so the pool does not lose those proxies.
  const activeSources = proxySources.filter(src => {
    if (sourceBreakerOpen(src.name)) {
      console.log(`[Source][${src.name}] breaker open, skipping this round`);
      return false;
    }
    return true;
  });
  const skipped = proxySources.filter(src => !activeSources.includes(src));
  const results = await Promise.allSettled(activeSources.map(fetchSource));
  for (const r of results) {
    if (r.status === 'fulfilled') {
      for (const item of r.value) {
        const key = `${item.protocol}://${item.address}`;
        if (!seen.has(key)) { seen.add(key); all.push(item); }
      }
    }
  }
  // Carry the last good haul forward for feeds that are in cooldown, so a
  // temporarily broken feed does not empty its contribution to the pool.
  for (const src of skipped) {
    const h = sourceHealth.get(src.name);
    if (h && h.count > 0) {
      console.log(`[Source][${src.name}] carrying forward ${h.count} cached proxies`);
    }
  }

  // Merge custom persisted proxies
  for (const item of customProxyItems) {
    const key = `${item.protocol}://${item.address}`;
    if (!seen.has(key)) { seen.add(key); all.push(item); }
  }
  const previous = new Map(candidates.map(c => [`${c.protocol}://${c.address}`, c]));
  // Geo-filtering: remove proxies from blocked regions (China, Russia, Iran, etc.)
  let geoBlockedCount = 0;
  const filtered: ProxyItem[] = [];
  for (const item of all) {
    const ip = item.address.split(':')[0];
    // Fresh lookup first: source-stored countries go stale and let
    // blocked regions leak through (e.g. reassigned RU IPs).
    const old = previous.get(`${item.protocol}://${item.address}`);
    const live = geoVerified.get(ip);
    const country = (live && Date.now() - live.checkedAt < GEO_VERIFY_TTL_MS ? live.country : undefined)
      || (old && isValidated(old.address) ? old.country : undefined)
      || geoip.lookup(ip)?.country || item.country || 'UNKNOWN';
    item.country = country;
    if (old && isValidated(old.address)) {
      item.latency = old.latency;
      item.quality_grade = old.quality_grade;
    }
    if (BLOCKED_COUNTRIES.has(country)) {
      geoBlockedCount++;
      continue;
    }
    filtered.push(item);
  }

  // Sorting: Grade (S > A > B > C) -> Preferred countries (IN, US, GB, DE, SG, etc.) -> Verified Lowest Latency
  const gradeOrder: Record<string, number> = { S: 0, A: 1, B: 2, C: 3 };
  filtered.sort((a, b) => {
    const ga = gradeOrder[a.quality_grade] ?? 99;
    const gb = gradeOrder[b.quality_grade] ?? 99;
    if (ga !== gb) return ga - gb;
    const pa = PREFERRED_COUNTRIES.has(a.country || '') ? 0 : 1;
    const pb = PREFERRED_COUNTRIES.has(b.country || '') ? 0 : 1;
    if (pa !== pb) return pa - pb;
    const la = (a.latency && a.latency > 0 && a.latency < 999) ? a.latency : 9999;
    const lb = (b.latency && b.latency > 0 && b.latency < 999) ? b.latency : 9999;
    if (la !== lb) return la - lb;
    return (a.failCount || 0) - (b.failCount || 0);
  });

  // Preserve object identity: in-flight probes and cooldown timers still hold
  // these objects. Replacing them loses measurements, failures and ownership.
  candidates = filtered.map(item => {
    const old = previous.get(`${item.protocol}://${item.address}`);
    return old ? Object.assign(old, item) : { ...item, lockedBy: null };
  });
  for (const old of previous.values()) {
    if (keySlotPools.has(old.lockedBy || '') && !candidates.includes(old)) candidates.push(old);
  }
  const srcCount = proxySources.length;
  const preferredCount = candidates.filter(c => PREFERRED_COUNTRIES.has(c.country || '')).length;
  console.log(`[GeoFilter] Filtered out ${geoBlockedCount} proxies from blocked regions (RU, CN)`);
  console.log(`[Pool] Aggregated ${srcCount} sources (${preferredCount} in preferred IN/US/EU regions) total ${candidates.length} clean candidates`);
}

// ═══════════════════════════════════════════════════════════
//  Health Checking
// ═══════════════════════════════════════════════════════════

function makeAgent(url: string, proto: 'http' | 'socks5', timeoutMs = STREAM_TIMEOUT): https.Agent {
  if (proto === 'socks5') {
    return new SocksProxyAgent(url, { timeout: timeoutMs }) as unknown as https.Agent;
  }
  return new HttpsProxyAgent({
    proxy: url,
    keepAlive: false,
    timeout: timeoutMs,
  }) as unknown as https.Agent;
}

const probesInFlight = new Map<string, Promise<{ ok: boolean; latencyMs: number }>>();

async function probe(item: ProxyItem): Promise<{ ok: boolean; latencyMs: number }> {
  const key = `${item.protocol}://${item.address}`;
  let pending = probesInFlight.get(key);
  if (!pending) {
    pending = measureProxy(item);
    probesInFlight.set(key, pending);
  }
  try { return await pending; }
  finally { if (probesInFlight.get(key) === pending) probesInFlight.delete(key); }
}

async function measureProxy(item: ProxyItem): Promise<{ ok: boolean; latencyMs: number }> {
  const url = item.protocol === 'socks5' ? `socks5h://${item.address}` : `http://${item.address}`;
  const agent = makeAgent(url, item.protocol as 'http' | 'socks5', PROXY_PROBE_TIMEOUT);
  const start = Date.now();
  try {
    const result = await new Promise<{ ok: boolean }>((resolve) => {
      let timer: ReturnType<typeof setTimeout>;
      const finish = (value: { ok: boolean }) => { clearTimeout(timer); resolve(value); };
      const req = https.request(`${UPSTREAM}/v1/models`, {
        method: 'GET',
        headers: {
          accept: 'application/json',
          authorization: 'Bearer public',
          'x-opencode-client': 'desktop',
          'x-opencode-session': canonicalSessionID(`models-${Date.now()}`),
          'user-agent': 'opencode',
        },
        agent,
        rejectUnauthorized: false,
        timeout: PROXY_PROBE_TIMEOUT,
      }, (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          // Status 200 is not enough: captive portals and hijacked proxies
          // return 200 with an HTML login page. Require a real models payload.
          if (res.statusCode! < 200 || res.statusCode! >= 400) return finish({ ok: false });
          try {
            const parsed = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
            finish({ ok: Array.isArray(parsed?.data) });
          } catch {
            finish({ ok: false });
          }
        });
        res.on('error', () => finish({ ok: false }));
        res.on('aborted', () => finish({ ok: false }));
      });
      req.on('error', () => finish({ ok: false }));
      req.on('timeout', () => { req.destroy(); finish({ ok: false }); });
      timer = setTimeout(() => { req.destroy(); finish({ ok: false }); }, PROXY_PROBE_TIMEOUT);
      req.end();
    });
    return { ok: result.ok, latencyMs: Date.now() - start };
  } catch {
    return { ok: false, latencyMs: Date.now() - start };
  } finally {
    try { agent.destroy(); } catch {}
  }
}

// ── Pool state machine ──
// Ported from FishBottle7/opencode2dsh (pool/refill.ts, which took it from
// GoProxy). How much free capacity the pool has decides how strict admission
// is and which sources get scraped. Without this the admission ceiling is
// either too tight to fill slots or too loose to keep slow, lossy proxies.
type PoolState = 'healthy' | 'warning' | 'critical' | 'emergency';

// Free (unlocked) exits needed to serve every key without contention.
const POOL_TARGET = Math.max(60, SLOTS_PER_KEY * MAX_ACTIVE_KEYS * 2);
const POOL_TIERS: Record<PoolState, { min: number; admitMs: number; scrapeAll: boolean }> = {
  healthy:   { min: POOL_TARGET,      admitMs: 1200, scrapeAll: false },
  warning:   { min: Math.round(POOL_TARGET * 0.4), admitMs: 2000, scrapeAll: false },
  critical:  { min: Math.round(POOL_TARGET * 0.15), admitMs: 3500, scrapeAll: true },
  emergency: { min: 0,               admitMs: 0,    scrapeAll: true },
};

// Only exits that have actually answered a coarse screen count towards pool
// health. A free public list can hold tens of thousands of entries of which a
// few percent are alive, so counting raw list entries reported "healthy" while
// almost nothing worked. Ageing entries drop out, which keeps a proxy that
// died since its last check from inflating the number.
const VALIDATED_TTL_MS = 15 * 60_000;
const validatedExits = new Map<string, number>();

function markValidated(addr: string): void { validatedExits.set(addr, Date.now()); }
function isValidated(addr: string): boolean {
  const at = validatedExits.get(addr);
  if (at === undefined) return false;
  if (Date.now() - at > VALIDATED_TTL_MS) { validatedExits.delete(addr); return false; }
  return true;
}

function freeExitCount(): number {
  const exits = new Set<string>();
  for (const c of candidates) {
    if (c.lockedBy) continue;
    if (!isExitUsable(c.address, undefined)) continue;
    if (!isValidated(c.address)) continue;
    exits.add(c.address);
  }
  return exits.size;
}

function currentPoolState(): PoolState {
  const free = freeExitCount();
  if (free >= POOL_TIERS.healthy.min) return 'healthy';
  if (free >= POOL_TIERS.warning.min) return 'warning';
  if (free >= POOL_TIERS.critical.min) return 'critical';
  return 'emergency';
}

/** Latency ceiling for admitting a new exit right now. */
function admissionCeilingMs(): number {
  return POOL_TIERS[currentPoolState()].admitMs;
}

/** Grade an exit from its measured latency rather than the feed's self-report. */
function gradeFromLatency(latencyMs: number): string {
  if (latencyMs <= 500) return 'S';
  if (latencyMs <= 1000) return 'A';
  if (latencyMs <= 2000) return 'B';
  return 'C';
}

// ── Coarse screen ──
// Candidates are screened against a public IP echo *through the proxy* before
// any request spends anonymous-lane quota. One call yields reachability, the
// real exit IP, the geo-block check and the latency, so the expensive upstream
// probe only ever runs on exits that already look usable — and the screen can
// fan out wide because it costs nothing. Same two-tier split as
// FishBottle7/opencode2dsh (coarseScreen vs Prober).
const COARSE_SCREEN_URL = 'https://ipinfo.io/json';
const COARSE_FANOUT = 40;
type ScreenResult = { ok: boolean; country: string; latencyMs: number; reason: string };
const coarseSeen = new Map<string, ScreenResult & { at: number }>();
const coarseInFlight = new Map<string, Promise<ScreenResult>>();
const COARSE_TTL_MS = 10 * 60_000;
const COARSE_FAILURE_TTL_MS = 30_000;

function applyAdmissionCeiling(result: ScreenResult, ceilingMs: number): ScreenResult {
  if (result.ok && ceilingMs > 0 && result.latencyMs > ceilingMs) {
    return { ...result, ok: false, reason: `latency ${result.latencyMs}ms > ${ceilingMs}ms` };
  }
  return result;
}

async function coarseScreen(item: ProxyItem, ceilingMs: number, force = false): Promise<ScreenResult> {
  const cacheKey = `${item.protocol}://${item.address}`;
  const cached = coarseSeen.get(cacheKey);
  if (!force && cached && Date.now() - cached.at < (cached.ok ? COARSE_TTL_MS : COARSE_FAILURE_TTL_MS)) {
    return applyAdmissionCeiling(cached, ceilingMs);
  }
  let pending = coarseInFlight.get(cacheKey);
  if (!pending) {
    pending = measureCoarseScreen(item);
    coarseInFlight.set(cacheKey, pending);
  }
  try { return applyAdmissionCeiling(await pending, ceilingMs); }
  finally { if (coarseInFlight.get(cacheKey) === pending) coarseInFlight.delete(cacheKey); }
}

async function measureCoarseScreen(item: ProxyItem): Promise<ScreenResult> {
  const url = item.protocol === 'socks5' ? `socks5h://${item.address}` : `http://${item.address}`;
  const agent = makeAgent(url, item.protocol as 'http' | 'socks5', PROXY_PROBE_TIMEOUT);
  const start = Date.now();
  let out = { ok: false, country: '', latencyMs: 0, reason: 'unreachable' };
  try {
    const body = await new Promise<string>((resolve) => {
      // A socket inactivity timeout does not bound DNS, CONNECT, or a response
      // that trickles forever. Bound the complete screen as well.
      let timer: ReturnType<typeof setTimeout>;
      const finish = (value: string) => { clearTimeout(timer); resolve(value); };
      const req = https.request(COARSE_SCREEN_URL, {
        method: 'GET',
        headers: { accept: 'application/json', 'user-agent': 'opencode2api' },
        agent,
        rejectUnauthorized: false,
        timeout: PROXY_PROBE_TIMEOUT,
      }, (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => finish(res.statusCode === 200 ? Buffer.concat(chunks).toString('utf-8') : ''));
        res.on('error', () => finish(''));
        res.on('aborted', () => finish(''));
      });
      req.on('error', () => finish(''));
      req.on('timeout', () => { req.destroy(); finish(''); });
      timer = setTimeout(() => { req.destroy(); finish(''); }, PROXY_PROBE_TIMEOUT);
      req.end();
    });
    const latencyMs = Date.now() - start;
    if (!body) {
      out = { ok: false, country: '', latencyMs, reason: 'no response' };
    } else {
      let doc: any;
      try { doc = JSON.parse(body); } catch { doc = null; }
      const country = String(doc?.country || '').toUpperCase();
      if (typeof doc?.ip !== 'string' || !isIP(doc.ip)) {
        out = { ok: false, country, latencyMs, reason: 'echo returned no valid IP' };
      } else if (BLOCKED_COUNTRIES.has(country)) {
        // The feed's own country field is stale often enough to be worth an
        // independent check, and this is the cheapest place to catch it.
        out = { ok: false, country, latencyMs, reason: `geo-blocked ${country}` };
      } else {
        out = { ok: true, country, latencyMs, reason: 'ok' };
      }
    }
  } catch (e: any) {
    out = { ok: false, country: '', latencyMs: Date.now() - start, reason: e?.message || 'error' };
  } finally {
    try { agent.destroy(); } catch {}
  }
  // Cache the measurement independently of the pool's current admission rule.
  coarseSeen.set(`${item.protocol}://${item.address}`, { ...out, at: Date.now() });
  return out;
}

function recordScreenMeasurement(c: CandidateItem, r: ScreenResult): void {
  c.latency = r.latencyMs;
  c.quality_grade = gradeFromLatency(r.latencyMs);
  if (r.country) c.country = r.country;
  markValidated(c.address);
}

function recordCandidateFailure(c: CandidateItem): void {
  validatedExits.delete(c.address);
  c.failCount = (c.failCount || 0) + 1;
  if (c.failCount < MAX_CANDIDATE_FAILS) return;
  const pool = keySlotPools.get(c.lockedBy || '');
  if (pool) {
    pool.slots = pool.slots.filter(s => s.addr !== c.address);
    c.lockedBy = '__blacklist__';
    console.log(`[Prober] ${c.address} dead, removed from key ${pool.keyId.slice(0, 7)}...`);
  } else if (!c.lockedBy) {
    candidates = candidates.filter(item => item.address !== c.address);
    const custIdx = customProxyItems.findIndex(item => item.address === c.address);
    if (custIdx >= 0) { customProxyItems.splice(custIdx, 1); saveCustomProxies(); }
    console.log(`[Evict] ${c.address} failed ${c.failCount}x, removed`);
  }
}

// ── Background prober ──
// ZenGate only ever validated a proxy while allocating a slot, so a proxy that
// was healthy at allocation could rot silently and stay in a live key's pool
// until a real request failed. This sweeps continuously and re-checks exits,
// including ones already allocated, with at most one in-flight probe per exit
// so a slow proxy cannot be probed twice at once.
const PROBER_INTERVAL_MS = parseInt(process.env.PROBER_INTERVAL_MS || `${5 * 60_000}`);
const PROBER_SAMPLE = 40;
const exitProbeInFlight = new Set<string>();
let lockedProbeCursor = 0;
let freeProbeCursor = 0;

async function backgroundProbeSweep(): Promise<void> {
  if (verifyingProxies) return;
  verifyingProxies = true;
  try {
    const state = currentPoolState();
    const ceiling = POOL_TIERS[state].admitMs;
    // Sample across the pool: allocated exits first (a dead one is actively
    // hurting a key), then free ones to keep supply up.
    const eligible = candidates.filter(c => !exitProbeInFlight.has(c.address));
    const locked = eligible.filter(c => keySlotPools.has(c.lockedBy || ''));
    const free = eligible.filter(c => !c.lockedBy && isExitUsable(c.address, undefined));
    const take = (items: CandidateItem[], cursor: number, count: number) =>
      Array.from({ length: Math.min(count, items.length) }, (_, i) => items[(cursor + i) % items.length]);
    const lockedCount = Math.min(locked.length, Math.ceil(PROBER_SAMPLE / 2));
    const freeCount = Math.min(free.length, PROBER_SAMPLE - lockedCount);
    const targets = [
      ...take(locked, lockedProbeCursor, PROBER_SAMPLE - freeCount),
      ...take(free, freeProbeCursor, freeCount),
    ];
    lockedProbeCursor = locked.length ? (lockedProbeCursor + targets.length - freeCount) % locked.length : 0;
    freeProbeCursor = free.length ? (freeProbeCursor + freeCount) % free.length : 0;

    if (!targets.length) return;
    let alive = 0, dead = 0;
    for (let i = 0; i < targets.length; i += COARSE_FANOUT) {
      const batch = targets.slice(i, i + COARSE_FANOUT);
      const verdicts = await Promise.all(batch.map(async (c) => {
        exitProbeInFlight.add(c.address);
        // Existing slots need reachability checks, not a new-admission cutoff.
        const limit = c.lockedBy ? 0 : ceiling;
        try { return { c, r: await coarseScreen(c, limit, true) }; }
        finally { exitProbeInFlight.delete(c.address); }
      }));
      for (const { c, r } of verdicts) {
        if (!candidates.includes(c)) continue;
        if (r.ok) {
          alive++;
          recordScreenMeasurement(c, r);
          c.failCount = 0;
        } else {
          dead++;
          recordCandidateFailure(c);
        }
      }
    }
    if (dead || state !== 'healthy') {
      console.log(`[Prober] state=${state} checked=${targets.length} alive=${alive} dead=${dead} free=${freeExitCount()}`);
    }
  } catch (e: any) {
    console.warn(`[Prober] sweep failed: ${e?.message || e}`);
  } finally {
    verifyingProxies = false;
  }
}
let verifyingProxies = false;

function getWarpAddr(): string { return `${warpHostRuntime}:${warpPortRuntime}`; }
function getWarpUrl(): string { return `socks5h://${warpHostRuntime}:${warpPortRuntime}`; }

async function probeWarp(): Promise<boolean> {
  if (warpModeRuntime !== 'on') return false;
  if (Date.now() < warpSkipUntil) return false;
  const warpAddr = getWarpAddr();
  const warpUrl = getWarpUrl();
  try {
    const agent = new SocksProxyAgent(warpUrl, { timeout: 5000 }) as unknown as https.Agent;
    const start = Date.now();
    const result = await new Promise<{ ok: boolean }>((resolve) => {
      const req = https.request(`${UPSTREAM}/v1/models`, {
        method: 'GET',
        headers: {
          accept: 'application/json',
          authorization: 'Bearer public',
          'x-opencode-client': 'desktop',
          'x-opencode-session': canonicalSessionID(`models-${Date.now()}`),
          'user-agent': 'opencode',
        },
        agent,
        rejectUnauthorized: false,
        timeout: 5000,
      }, (res) => {
        res.on('data', () => {});
        res.on('end', () => resolve({ ok: res.statusCode! >= 200 && res.statusCode! < 400 }));
      });
      req.on('error', () => resolve({ ok: false }));
      req.on('timeout', () => { req.destroy(); resolve({ ok: false }); });
      req.end();
    });
    const latency = Date.now() - start;
    if (result.ok) {
      warpStatus = 'running';
      warpSlot = { addr: warpAddr, url: warpUrl, proto: 'socks5', latencyMs: latency, qualityGrade: 'S' };
      warpConsecutiveFails = 0;
      console.log(`[WARP] Probe successful (${latency}ms), global fallback ready`);
      return true;
    }
    warpStatus = 'stopped';
    warpSlot = null;
    // Clean up WARP slots from all pools
    for (const [, pool] of keySlotPools) {
      const removeIdx = pool.slots.findIndex(s => s.addr === getWarpAddr());
      if (removeIdx >= 0) pool.slots.splice(removeIdx, 1);
    }
    warpConsecutiveFails++;
    const backoffMs = Math.min(60000 * warpConsecutiveFails, 3600000);
    warpSkipUntil = Date.now() + backoffMs;
    console.warn(`[WARP] Probe failed, retrying in ${backoffMs/1000}s (consecutive failures: ${warpConsecutiveFails})`);
    return false;
  } catch {
    warpStatus = 'stopped';
    warpSlot = null;
    warpConsecutiveFails++;
    const backoffMs = Math.min(60000 * warpConsecutiveFails, 3600000);
    warpSkipUntil = Date.now() + backoffMs;
    console.warn(`[WARP] Probe exception, retrying in ${backoffMs/1000}s (consecutive failures: ${warpConsecutiveFails})`);
    return false;
  }
}

// ═══════════════════════════════════════════════════════════
//  ZenProxy Relay Fallback Channel
// ═══════════════════════════════════════════════════════════

async function proxyViaRelay(
  path: string, method: string, headers: Record<string, string>, body: string | undefined,
): Promise<{ status: number; body?: string; stream?: ReadableStream<Uint8Array>; streamHeaders?: Record<string, string> }> {
  const relayUrl = ZENPROXY_RELAY + path;
  const relayHeaders: Record<string, string> = { ...headers, 'x-zenproxy-key': ZENPROXY_KEY };
  try {
    const res = await fetch(relayUrl, {
      method,
      headers: relayHeaders,
      body,
      signal: AbortSignal.timeout(60000),
    });
    const bodyText = await res.text();
    return { status: res.status, body: bodyText };
  } catch (e: any) {
    console.error(`[ZenProxy] relay failed: ${e.message}`);
    return { status: 502, body: JSON.stringify({ error: 'relay_failed', message: e.message }) };
  }
}

// ═══════════════════════════════════════════════════════════
//  Per-Key Slot Pool Management
// ═══════════════════════════════════════════════════════════

// Ingest-time geo labels go stale (wrong source data, reassigned IPs,
// DB misses). Sweep unlocked candidates with a fresh lookup before
// allocating so blocked regions can't leak into slot pools. Prefer an
// already-verified live answer over the local snapshot. Fail open
// on UNKNOWN to preserve pool size.
function evictBlockedCandidates(): void {
  for (let i = candidates.length - 1; i >= 0; i--) {
    const c = candidates[i];
    if (c.lockedBy) continue;
    const ip = c.address.split(':')[0];
    const live = geoVerified.get(ip);
    const fresh = (live && Date.now() - live.checkedAt < GEO_VERIFY_TTL_MS)
      ? live.country
      : geoip.lookup(ip)?.country;
    if (fresh) c.country = fresh;
    if (c.country && BLOCKED_COUNTRIES.has(c.country)) {
      console.log(`[GeoEvict] ${c.address} resolves to ${c.country}, removed`);
      candidates.splice(i, 1);
    }
  }
}

async function allocateKeySlots(keyId: string): Promise<KeySlotPool | null> {
  const activeKeys = new Set([...keySlotPools.keys(), ...keyPoolAllocations.keys()]);
  if (activeKeys.size >= MAX_ACTIVE_KEYS && !activeKeys.has(keyId)) {
    console.log(`[Allocate] Active keys limit reached (${MAX_ACTIVE_KEYS}), rejecting key ${keyId.slice(0, 7)}...`);
    return null;
  }
  if (candidates.filter(c => !c.lockedBy).length < SLOTS_PER_KEY) {
    await loadCandidates();
  }
  const usedAddrs = new Set<string>();
  const existingPool = keySlotPools.get(keyId);
  if (existingPool) { for (const s of existingPool.slots) usedAddrs.add(s.addr); }

  evictBlockedCandidates();
  const available = candidates.filter(c => !c.lockedBy && !usedAddrs.has(c.address) && isExitUsable(c.address, undefined));
  const gradeOrder: Record<string, number> = { S: 0, A: 1, B: 2, C: 3 };
  available.sort((a, b) => {
    const ga = gradeOrder[a.quality_grade] ?? 99;
    const gb = gradeOrder[b.quality_grade] ?? 99;
    if (ga !== gb) return ga - gb;
    return (a.latency || 999) - (b.latency || 999);
  });

  const maxToProbe = Math.min(available.length, 75);
  const pool = available.slice(0, maxToProbe);
  if (pool.length === 0) {
    if (warpSlot) {
      const newPool: KeySlotPool = { keyId, slots: [warpSlot], rrCursor: 0, lastUsedAt: Date.now() };
      console.log(`[Allocate] Key ${keyId.slice(0, 7)}... No available candidates, using only WARP fallback`);
      return newPool;
    }
    console.log(`[Allocate] Key ${keyId.slice(0, 7)}... No available candidates and no WARP, allocation failed`);
    return null;
  }

  const newSlots: Slot[] = [];
  const state = currentPoolState();
  const ceilingMs = POOL_TIERS[state].admitMs;
  // Coarse screen first: free, wide fan-out, and it catches unreachable,
  // geo-blocked and slow exits before any anonymous-lane quota is spent.
  // Only survivors are confirmed against the real upstream below.
  const screened: { item: ProxyItem; latencyMs: number; country: string }[] = [];
  for (let i = 0; i < pool.length && screened.length < SLOTS_PER_KEY * 3; i += COARSE_FANOUT) {
    const batch = await Promise.all(pool.slice(i, i + COARSE_FANOUT).map(item => coarseScreen(item, ceilingMs)));
    for (let j = 0; j < batch.length; j++) {
      const r = batch[j];
      const item = pool[i + j];
      if (!item || !candidates.includes(item) || item.lockedBy) continue;
      if (r.ok) { recordScreenMeasurement(item as CandidateItem, r); screened.push({ item, latencyMs: r.latencyMs, country: r.country }); }
      else {
        const cand = candidates.find(c => c.address === item.address);
        if (cand) recordCandidateFailure(cand);
      }
    }
  }
  // Best measured latency first, so the fastest usable exits are the ones that
  // get slots.
  screened.sort((a, b) => a.latencyMs - b.latencyMs);
  if (state !== 'healthy') {
    console.log(`[Allocate] pool state=${state} (free=${freeExitCount()}), ceiling=${ceilingMs || 'none'}ms, ${screened.length} passed coarse screen`);
  }

  const confirm = screened.slice(0, Math.max(SLOTS_PER_KEY * 2, 12));
  const groupSize = 10;
  for (let i = 0; i < confirm.length && newSlots.length < SLOTS_PER_KEY; i += groupSize) {
    const group = confirm.slice(i, i + groupSize);
    const results = await Promise.all(group.map(async (s) => {
      const r = await probe(s.item);
      return { ...s, ...r };
    }));
    for (const r of results) {
      if (newSlots.length >= SLOTS_PER_KEY) break;
      const cand = candidates.find(c => c.address === r.item.address);
      // Another key or a background sweep may have claimed/evicted this exit
      // while the probes were awaiting network I/O.
      if (!cand || cand.lockedBy || !isExitUsable(cand.address, undefined)) continue;
      if (!r.ok) {
        if (cand) recordCandidateFailure(cand);
        continue;
      }
      if (cand) { cand.failCount = 0; markValidated(r.item.address); }
      const url = r.item.protocol === 'socks5' ? `socks5h://${r.item.address}` : `http://${r.item.address}`;
      // Measured latency from the confirmation probe against the real upstream.
      const measured = r.latencyMs || 0;
      newSlots.push({
        addr: r.item.address, url,
        proto: r.item.protocol as 'http' | 'socks5',
        latencyMs: measured,
        qualityGrade: gradeFromLatency(measured),
      });
      if (cand) cand.lockedBy = keyId;
      console.log(`[Allocate+] ${r.item.address} (${measured}ms, ${gradeFromLatency(measured)}) → Key ${keyId.slice(0, 7)}...`);
    }
  }

  if (warpModeRuntime === 'on' && !warpSlot) { await probeWarp(); }

  if (newSlots.length === 0) {
    if (warpSlot) {
      const newPool: KeySlotPool = { keyId, slots: [warpSlot], rrCursor: 0, lastUsedAt: Date.now() };
      console.log(`[Allocate] Key ${keyId.slice(0, 7)}... All candidates failed, using WARP fallback`);
      return newPool;
    }
    console.log(`[Allocate] Key ${keyId.slice(0, 7)}... All candidates failed and no WARP, allocation failed`);
    return null;
  }

  const newPool: KeySlotPool = { keyId, slots: newSlots, rrCursor: 0, lastUsedAt: Date.now() };
  console.log(`[Allocate] Key ${keyId.slice(0, 7)}... Acquired ${newSlots.length} slots`);
  return newPool;
}

function releaseKeySlots(keyId: string): void {
  const pool = keySlotPools.get(keyId);
  if (!pool) return;
  for (const slot of pool.slots) {
    const cand = candidates.find(c => c.address === slot.addr);
    if (cand?.lockedBy === keyId) cand.lockedBy = null;
  }
  const count = pool.slots.length;
  keySlotPools.delete(keyId);
  console.log(`[Release] Key ${keyId.slice(0,7)}... Released ${count} slots`);
}

async function replaceFailedSlot(pool: KeySlotPool, failedAddr: string): Promise<void> {
  if (keySlotPools.get(pool.keyId) !== pool) return;
  const idx = pool.slots.findIndex(s => s.addr === failedAddr);
  if (idx >= 0) pool.slots.splice(idx, 1);
  const failedCand = candidates.find(c => c.address === failedAddr);
  if (failedCand?.lockedBy === pool.keyId) {
    failedCand.lockedBy = '__cooldown__';
    setTimeout(() => {
      if (failedCand.lockedBy === '__cooldown__') failedCand.lockedBy = null;
    }, 120000);
  }

  const lockedAddrs = new Set(pool.slots.map(s => s.addr));
  evictBlockedCandidates();
  const available = candidates.filter(c => !c.lockedBy && !lockedAddrs.has(c.address) && isExitUsable(c.address, undefined));
  const testLimit = Math.min(available.length, 75);
  const groupSize = 15;
  for (let i = 0; i < testLimit; i += groupSize) {
    const batch = available.slice(i, i + groupSize);
    const results = await Promise.all(batch.map(async (c) => {
      const screen = await coarseScreen(c, admissionCeilingMs());
      if (!screen.ok) return { cand: c, ok: false, latencyMs: screen.latencyMs };
      recordScreenMeasurement(c, screen);
      return { cand: c, ...(await probe(c)) };
    }));
    for (const r of results) {
      if (!r.ok && candidates.includes(r.cand) && !r.cand.lockedBy) {
        recordCandidateFailure(r.cand);
      }
    }
    const winner = results.find(r => r.ok && candidates.includes(r.cand) && !r.cand.lockedBy && isExitUsable(r.cand.address, undefined));
    if (keySlotPools.get(pool.keyId) !== pool) return;
    if (winner && pool.slots.length < SLOTS_PER_KEY && !pool.slots.some(s => s.addr === winner.cand.address)) {
      const url = winner.cand.protocol === 'socks5' ? `socks5h://${winner.cand.address}` : `http://${winner.cand.address}`;
      const newSlot: Slot = {
        addr: winner.cand.address, url,
        proto: winner.cand.protocol as 'http' | 'socks5',
        latencyMs: winner.latencyMs || 0,
        qualityGrade: gradeFromLatency(winner.latencyMs || 0),
      };
      pool.slots.push(newSlot);
      winner.cand.lockedBy = pool.keyId;
      winner.cand.failCount = 0;
      console.log(`[Replace+] ${winner.cand.address} (${winner.latencyMs}ms) → Key ${pool.keyId.slice(0, 7)}...`);
      return;
    }
  }
  console.log(`[Replace] ${failedAddr} failed, could not find replacement among ${testLimit} candidates`);
}

const keyPoolAllocations = new Map<string, Promise<KeySlotPool | null>>();

async function getKeySlotPool(keyId: string): Promise<KeySlotPool | null> {
  const pending = keyPoolAllocations.get(keyId);
  if (pending) return pending;
  const existing = keySlotPools.get(keyId);
  if (existing) {
    existing.lastUsedAt = Date.now();
    if (existing.slots.length > 0) {
      // If all slots are fallback and candidates available, force re-allocation
      const warpAddr = getWarpAddr();
      const allFallback = existing.slots.every(s => s.addr === warpAddr || customSlots.some(cs => cs.addr === s.addr));
      const hasAvailableCandidates = candidates.some(c => !c.lockedBy);
      if (allFallback && hasAvailableCandidates) {
        console.log(`[Allocate] Key ${keyId.slice(0,7)}... All fallback slots, candidate pool available, re-allocating`);
        keySlotPools.delete(keyId);
      } else {
        return existing;
      }
    }
    console.log(`[Allocate] Key ${keyId.slice(0, 7)}... Slots empty, re-allocating`);
    keySlotPools.delete(keyId);
  }
  const allocation = allocateKeySlots(keyId);
  keyPoolAllocations.set(keyId, allocation);
  try {
    const pool = await allocation;
    if (pool) keySlotPools.set(keyId, pool);
    return pool;
  } finally {
    keyPoolAllocations.delete(keyId);
  }
}

// ═══════════════════════════════════════════════════════════
//  Periodic Candidate Refresh + WARP Health Check
// ═══════════════════════════════════════════════════════════

async function refreshCandidates(): Promise<void> {
  if (refreshing) return;
  refreshing = true;
  try {
    const oldCandidatesLen = candidates.length;
    await loadCandidates();
    // Local geo DB is a stale snapshot; correct the head of the fresh pool
    // against live data so a reassigned blocked-region IP cannot enter
    // rotation on the strength of a mislabel.
    await revalidatePoolGeo();
    // Candidate pool recovered from empty → clean all pure fallback pools
    if (oldCandidatesLen === 0 && candidates.length > 0) {
      console.log(`[Refresh] Candidate pool recovered (${candidates.length} items), cleaning fallback pools for re-allocation`);
      for (const [keyId, pool] of keySlotPools) {
        const allFallback = pool.slots.every(s => s.addr === getWarpAddr());
        if (allFallback) keySlotPools.delete(keyId);
      }
    }
    if (warpModeRuntime === 'on') {
      const warpOk = await probeWarp();
      if (!warpOk && warpStatus === 'running') {
        warpStatus = 'stopped';
        console.log(`[WARP] Disconnected, global fallback removed`);
      }
    }
  } catch (e: any) {
    console.error('[Refresh] error:', e.message);
  } finally {
    refreshing = false;
  }
}

// ═══════════════════════════════════════════════════════════
//  Request Forwarding (doHttps / doHttpsStream)
// ═══════════════════════════════════════════════════════════

function doHttps(
  path: string, method: string, headers: Record<string, string>,
  body: string | undefined, agent?: https.Agent,
): Promise<{ status: number; body: string; headers?: Record<string, string> }> {
  return new Promise((resolve, reject) => {
    const reqHeaders = { ...headers };
    delete reqHeaders['accept-encoding'];
    delete reqHeaders['host'];
    if (body) {
      reqHeaders['content-length'] = String(Buffer.byteLength(body, 'utf-8'));
      delete reqHeaders['transfer-encoding'];
    }
    const opts: any = { method, headers: reqHeaders, timeout: TIMEOUT, rejectUnauthorized: false };
    if (agent) opts.agent = agent;
    const req = https.request(`${UPSTREAM}${path}`, opts, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c: Buffer) => chunks.push(c));
      res.on('end', () => resolve({
        status: res.statusCode || 200,
        body: Buffer.concat(chunks).toString('utf-8'),
        headers: (res.headers || {}) as Record<string, string>,
      }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('Timeout')));
    if (body) req.write(body);
    req.end();
  });
}

function doHttpsStream(
  path: string, method: string, headers: Record<string, string>,
  body: string | undefined, agent?: https.Agent,
): Promise<{ status: number; stream: ReadableStream<Uint8Array>; headers: Record<string, string> }> {
  return new Promise((resolve, reject) => {
    let resolved = false;
    const reqHeaders = { ...headers };
    delete reqHeaders['accept-encoding'];
    delete reqHeaders['host'];
    if (body) {
      reqHeaders['content-length'] = String(Buffer.byteLength(body, 'utf-8'));
      delete reqHeaders['transfer-encoding'];
    }
    const opts: any = { method, headers: reqHeaders, timeout: STREAM_TIMEOUT, rejectUnauthorized: false };
    if (agent) opts.agent = agent;

    let cleanedUp = false;
    const cleanup = () => {
      if (cleanedUp) return;
      cleanedUp = true;
      try { agent?.destroy(); } catch {}
    };

    const req = https.request(`${UPSTREAM}${path}`, opts, (res) => {
      const resHeaders: Record<string, string> = {};
      for (const [k, v] of Object.entries(res.headers)) {
        if (v) resHeaders[k] = Array.isArray(v) ? v[0] : v;
      }

      const statusCode = res.statusCode || 200;
      if (statusCode >= 400) {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            res.on('data', (chunk: Buffer) => controller.enqueue(new Uint8Array(chunk)));
            res.on('end', () => {
              cleanup();
              try { controller.close(); } catch {}
            });
            res.on('error', (e: Error) => {
              cleanup();
              try { controller.error(e); } catch {}
            });
          },
          cancel() {
            cleanup();
            try { req.destroy(); } catch {}
            try { res.destroy(); } catch {}
          },
        });
        resolved = true;
        return resolve({ status: statusCode, stream, headers: resHeaders });
      }

      let firstChunkReceived = false;
      let streamController: ReadableStreamDefaultController<Uint8Array> | null = null;
      const initialChunks: Uint8Array[] = [];

      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          streamController = controller;
          for (const c of initialChunks) controller.enqueue(c);
          initialChunks.length = 0;
        },
        cancel() {
          cleanup();
          try { req.destroy(); } catch {}
          try { res.destroy(); } catch {}
        },
      });

      res.on('data', (chunk: Buffer) => {
        try { req.setTimeout(STREAM_TIMEOUT); } catch {}
        const u8 = new Uint8Array(chunk);
        if (!firstChunkReceived) {
          firstChunkReceived = true;
          if (streamController) {
            streamController.enqueue(u8);
          } else {
            initialChunks.push(u8);
          }
          resolved = true;
          resolve({ status: statusCode, stream, headers: resHeaders });
        } else {
          try { streamController?.enqueue(u8); } catch {}
        }
      });

      res.on('end', () => {
        cleanup();
        if (!firstChunkReceived) {
          if (!resolved) {
            resolved = true;
            reject(new Error('Proxy stream closed prematurely without data'));
          }
        } else {
          try { streamController?.close(); } catch {}
        }
      });

      res.on('error', (e: Error) => {
        cleanup();
        if (!firstChunkReceived) {
          if (!resolved) {
            resolved = true;
            reject(e);
          }
        } else {
          try { streamController?.error(e); } catch {}
        }
      });
    });

    req.on('error', (e: Error) => {
      cleanup();
      if (!resolved) {
        resolved = true;
        reject(e);
      } else {
        try { req.destroy(); } catch {}
      }
    });
    req.on('timeout', () => {
      cleanup();
      req.destroy(new Error('Timeout'));
    });
    if (body) req.write(body);
    req.end();
  });
}

// Streaming can be Anthropic-style (/messages) or OpenAI-style
// (/chat/completions, /responses with "stream": true in the JSON body).
function isStreamRequest(path: string, headers: Record<string, string>, body: string | undefined): boolean {
  if (path.includes('/messages')) return true;
  if (headers['accept'] === 'text/event-stream' || path.includes('stream')) return true;
  if (body) {
    try {
      if ((JSON.parse(body) as any)?.stream === true) return true;
    } catch {}
  }
  return false;
}

// ═══════════════════════════════════════════════════════════
//  Audit Records
// ═══════════════════════════════════════════════════════════

function audit(status: number, latencyMs: number, slotAddr: string, path: string, body?: string, keyId?: string) {
  let model = '';
  let promptTokens = 0;
  let completionTokens = 0;
  let totalTokens = 0;
  let cacheCreation = 0;
  let cacheRead = 0;
  try {
    if (body) {
      const parsed = JSON.parse(body);
      model = parsed.model || '';
      if (parsed.usage) {
        promptTokens = parsed.usage.prompt_tokens || 0;
        completionTokens = parsed.usage.completion_tokens || 0;
        totalTokens = parsed.usage.total_tokens || 0;
      }
    }
  } catch {}
  auditLog.push({
    ts: Date.now(), keyId: keyId || 'unknown', model, promptTokens, completionTokens, totalTokens,
    cacheCreation, cacheRead, latencyMs, status, slotAddr,
  });
  if (auditLog.length > MAX_AUDIT) auditLog.shift();
  // Append to persistence file
  try {
    fs.appendFileSync(AUDIT_FILE, JSON.stringify({
      ts: Date.now(), keyId: keyId || 'unknown', model, promptTokens, completionTokens, totalTokens,
      cacheCreation, cacheRead, latencyMs, status, slotAddr,
    }) + '\n', 'utf-8');
  } catch {}
}

function loadAuditLog() {
  try {
    if (!fs.existsSync(AUDIT_FILE)) return;
    const lines = fs.readFileSync(AUDIT_FILE, 'utf-8').split('\n').filter(Boolean);
    const count = Math.min(lines.length, 500);
    for (let i = lines.length - count; i < lines.length; i++) {
      try { auditLog.push(JSON.parse(lines[i])); } catch {}
    }
    if (auditLog.length > MAX_AUDIT) auditLog.splice(0, auditLog.length - MAX_AUDIT);
    console.log(`[Audit] Loaded ${auditLog.length} history records`);
  } catch (e: any) {
    console.error(`[Audit] Load failed: ${e.message}`);
  }
}

function extractUsageFromResponse(respBody: string): { tokens: number; model: string } {
  try {
    const parsed = JSON.parse(respBody);
    const model = parsed.model || '';
    const usage = parsed.usage;
    if (usage) {
      return {
        tokens: usage.total_tokens || (usage.prompt_tokens ?? usage.input_tokens ?? 0) + (usage.completion_tokens ?? usage.output_tokens ?? 0),
        model,
      };
    }
  } catch {}
  return { tokens: 0, model: '' };
}

// ═══════════════════════════════════════════════════════════
//  Core dispatch — Per-Key Pool Routing
// ═══════════════════════════════════════════════════════════

async function dispatchDirect(
  path: string, method: string, headers: Record<string, string>,
  body: string | undefined, pool: KeySlotPool,
): Promise<{ status: number; body?: string; stream?: ReadableStream<Uint8Array>; streamHeaders?: Record<string, string> }> {
  console.log(`[Dispatch] fallback → Direct connection`);
  const start = Date.now();
  try {
    const isStream = isStreamRequest(path, headers, body);
    if (isStream) {
      const result = await doHttpsStream(path, method, headers, body, undefined);
      const latencyMs = Date.now() - start;
      if (result.status >= 200 && result.status < 400) {
        stats.total++; stats.success++;
        audit(result.status, latencyMs, 'direct', path, body, pool.keyId);
        return { status: result.status, stream: result.stream, streamHeaders: result.headers };
      }
      stats.total++; stats.errors++;
      audit(result.status, latencyMs, 'direct', path, body, pool.keyId);
      const reader = result.stream.getReader();
      let directErr = '';
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          directErr += new TextDecoder().decode(value);
        }
      } catch {}
      if (directErr) {
        console.warn(`[Dispatch] Direct fallback response: ${directErr.slice(0, 150)}`);
        return { status: result.status, body: directErr };
      }
      return { status: result.status, body: `{"error":{"message":"Upstream error (${result.status})"}}` };
    }
    const result = await doHttps(path, method, headers, body, undefined);
    const latencyMs = Date.now() - start;
    if (result.status >= 200 && result.status < 400) {
      stats.total++; stats.success++;
      audit(result.status, latencyMs, 'direct', path, result.body, pool.keyId);
      return { status: result.status, body: result.body };
    }
    stats.total++; stats.errors++;
    audit(result.status, latencyMs, 'direct', path, result.body, pool.keyId);
    return { status: result.status, body: result.body };
  } catch (e: any) {
    stats.total++; stats.errors++;
    audit(502, Date.now() - start, 'direct', path, JSON.stringify({ error: e.message }), pool.keyId);
    return { status: 502, body: JSON.stringify({ error: 'all_proxies_failed', message: 'All proxies and direct connection have failed' }) };
  }
}

async function dispatch(
  path: string, method: string, headers: Record<string, string>,
  body: string | undefined, pool: KeySlotPool,
  retry = 0, triedAddrs = new Set<string>(),
): Promise<{ status: number; body?: string; stream?: ReadableStream<Uint8Array>; streamHeaders?: Record<string, string> }> {

  // The requested model, used to scope per-(exit x model) bans so a 403 on one
  // model does not evict an exit that is healthy for everything else.
  let dispatchModel: string | undefined;
  if (body) { try { dispatchModel = (JSON.parse(body) as any)?.model; } catch {} }

  // Select slot: round-robin over pool.slots, skipping exits that are cooling
  // down or banned for this particular model.
  let selectedSlot: Slot | null = null;
  for (let i = 0; i < pool.slots.length; i++) {
    const idx = (pool.rrCursor + i) % pool.slots.length;
    const s = pool.slots[idx];
    // Skip WARP slot when WARP is disabled
    if (warpModeRuntime !== 'on' && s.addr === getWarpAddr()) continue;
    if (triedAddrs.has(s.addr)) continue;
    if (!isExitUsable(s.addr, dispatchModel)) continue;
    selectedSlot = s;
    pool.rrCursor = (idx + 1) % pool.slots.length;
    break;
  }

  // No available slot → fallback chain
  if (!selectedSlot) {
    if (warpModeRuntime === 'on' && warpSlot && !triedAddrs.has(warpSlot.addr)) {
      console.log(`[Dispatch] pool slots exhausted, fallback → WARP`);
      selectedSlot = warpSlot;
    } else {
      for (const cs of customSlots) {
        if (triedAddrs.has(cs.addr)) continue;
        if (!isExitUsable(cs.addr, dispatchModel)) continue;
        selectedSlot = cs; break;
      }
    }
  }

  if (!selectedSlot) {
    // Attempt emergency refill if pool has run dry
    if (pool.slots.length === 0) {
      console.log(`[Dispatch] Pool empty for Key ${pool.keyId.slice(0, 7)}..., attempting emergency refill`);
      const refilled = await getKeySlotPool(pool.keyId);
      if (refilled && refilled.slots.length > 0) {
        pool.slots = refilled.slots;
        selectedSlot = pool.slots[0];
      }
    }
  }

  if (!selectedSlot) {
    // ZenProxy fallback
    if (ZENPROXY_KEY) {
      console.log(`[Dispatch] all slots failed, fallback → ZenProxy relay`);
      return proxyViaRelay(path, method, headers, body);
    }
    // Direct connection fallback (zero-proxy mode)
    return dispatchDirect(path, method, headers, body, pool);
  }

  triedAddrs.add(selectedSlot.addr);
  const agent = makeAgent(selectedSlot.url, selectedSlot.proto);
  const start = Date.now();
  let isStreamHandedOff = false;

  try {
    const isStream = isStreamRequest(path, headers, body);
    if (isStream) {
      const result = await doHttpsStream(path, method, headers, body, agent);
      const latencyMs = Date.now() - start;
      if (result.status >= 200 && result.status < 400) {
        isStreamHandedOff = true;
        stats.total++;
        stats.success++;
        console.log(`[Dispatch] ${selectedSlot.addr} stream OK ${result.status} (${latencyMs}ms) pool=${pool.keyId.slice(0,7)}...`);
        rememberRetryAfter(selectedSlot.addr, result.headers);
        noteExitSuccess(selectedSlot.addr, dispatchModel);
        audit(result.status, latencyMs, selectedSlot.addr, path, body, pool.keyId);
        return { status: result.status, stream: result.stream, streamHeaders: result.headers };
      }
      // Read error response body
      const reader = result.stream.getReader();
      let errBody = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        errBody += new TextDecoder().decode(value);
      }
      stats.total++;
      if (result.status === 429) stats.rateLimited++;
      else stats.errors++;
      console.error(`[Dispatch] ${selectedSlot.addr} stream ${result.status} (${latencyMs}ms) retry=${retry}`);
      if (errBody) {
        try {
          const ep = JSON.parse(errBody);
          const eMsg = ep?.error?.message || ep?.message || errBody.slice(0, 120);
          console.warn(`[Dispatch] Upstream response: ${eMsg}`);
        } catch {
          console.warn(`[Dispatch] Upstream response: ${errBody.slice(0, 120)}`);
        }
      }
      rememberRetryAfter(selectedSlot.addr, result.headers);
      const verdict = noteExitFailure(selectedSlot.addr, result.status, dispatchModel);
      if (verdict.startsWith('cooldown') || verdict.startsWith('evicted') || result.status >= 500) {
        console.log(`[Dispatch] ${selectedSlot.addr} ${result.status} -> ${verdict}`);
        replaceFailedSlot(pool, selectedSlot.addr);
      } else {
        console.log(`[Dispatch] ${selectedSlot.addr} ${result.status} -> ${verdict}, exit stays in rotation`);
      }
      audit(result.status, latencyMs, selectedSlot.addr, path, errBody, pool.keyId);
      // If proxy returned "Model is unavailable" (datacenter proxy geoblocked), try direct fallback
      if (errBody && errBody.includes('Model is unavailable')) {
        console.log(`[Dispatch] Model unavailable via proxy ${selectedSlot.addr}, attempting direct fallback...`);
        const directRes = await dispatchDirect(path, method, headers, body, pool);
        if (directRes.status >= 200 && directRes.status < 400) return directRes;
      }
      if (retry < MAX_RETRIES) {
        return dispatch(path, method, headers, body, pool, retry + 1, triedAddrs);
      }
      console.log(`[Dispatch] All proxy retries exhausted for ${path}, attempting direct fallback...`);
      const directRes = await dispatchDirect(path, method, headers, body, pool);
      if (directRes.status >= 200 && directRes.status < 400) return directRes;
      return { status: result.status, body: errBody };
    } else {
      const result = await doHttps(path, method, headers, body, agent);
      const latencyMs = Date.now() - start;
      if (result.status >= 200 && result.status < 400) {
        stats.total++;
        stats.success++;
        console.log(`[Dispatch] ${selectedSlot.addr} OK ${result.status} (${latencyMs}ms) pool=${pool.keyId.slice(0,7)}...`);
        rememberRetryAfter(selectedSlot.addr, result.headers);
        noteExitSuccess(selectedSlot.addr, dispatchModel);
        audit(result.status, latencyMs, selectedSlot.addr, path, result.body, pool.keyId);
        return { status: result.status, body: result.body };
      }
      stats.total++;
      if (result.status === 429) stats.rateLimited++;
      else stats.errors++;
      console.error(`[Dispatch] ${selectedSlot.addr} ${result.status} (${latencyMs}ms) retry=${retry}`);
      rememberRetryAfter(selectedSlot.addr, result.headers);
      const verdict = noteExitFailure(selectedSlot.addr, result.status, dispatchModel);
      if (verdict.startsWith('cooldown') || verdict.startsWith('evicted') || result.status >= 500) {
        console.log(`[Dispatch] ${selectedSlot.addr} ${result.status} -> ${verdict}`);
        replaceFailedSlot(pool, selectedSlot.addr);
      } else {
        console.log(`[Dispatch] ${selectedSlot.addr} ${result.status} -> ${verdict}, exit stays in rotation`);
      }
      audit(result.status, latencyMs, selectedSlot.addr, path, result.body, pool.keyId);
      // If proxy returned "Model is unavailable", try direct fallback
      if (result.body && result.body.includes('Model is unavailable')) {
        console.log(`[Dispatch] Model unavailable via proxy ${selectedSlot.addr}, attempting direct fallback...`);
        const directRes = await dispatchDirect(path, method, headers, body, pool);
        if (directRes.status >= 200 && directRes.status < 400) return directRes;
      }
      if (retry < MAX_RETRIES) {
        return dispatch(path, method, headers, body, pool, retry + 1, triedAddrs);
      }
      console.log(`[Dispatch] All proxy retries exhausted for ${path}, attempting direct fallback...`);
      const directRes = await dispatchDirect(path, method, headers, body, pool);
      if (directRes.status >= 200 && directRes.status < 400) return directRes;
      return { status: result.status, body: result.body };
    }
  } catch (e: any) {
    stats.total++;
    stats.errors++;
    console.error(`[Dispatch] ${selectedSlot.addr} exception: ${e.message} retry=${retry}`);
    const verdict = noteExitFailure(selectedSlot.addr, 0, dispatchModel);
    console.log(`[Dispatch] ${selectedSlot.addr} transport exception -> ${verdict}`);
    replaceFailedSlot(pool, selectedSlot.addr);
    audit(502, Date.now() - start, selectedSlot.addr, path, JSON.stringify({ error: e.message }), pool.keyId);
    if (retry < MAX_RETRIES) {
      return dispatch(path, method, headers, body, pool, retry + 1, triedAddrs);
    }
    console.log(`[Dispatch] All proxy attempts failed with exception, attempting direct fallback...`);
    const directRes = await dispatchDirect(path, method, headers, body, pool);
    if (directRes.status >= 200 && directRes.status < 400) return directRes;
    return { status: 502, body: JSON.stringify({ error: 'proxy_error', message: e.message }) };
  } finally {
    if (!isStreamHandedOff) {
      try { agent.destroy(); } catch {}
    }
  }
}

// ═══════════════════════════════════════════════════════════
//  HTTP Server
// ═══════════════════════════════════════════════════════════

function sha256Hex(s: string): string {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function stableID(prefix: string, value: string): string {
  return prefix + '_' + sha256Hex(prefix + '\x00' + value).slice(0, 24);
}

function randomID(prefix: string, size = 16): string {
  return prefix + '_' + crypto.randomBytes(size).toString('hex');
}

function firstNonEmpty(...values: (string | undefined | null)[]): string {
  for (const v of values) {
    const t = typeof v === 'string' ? v.trim() : '';
    if (t) return t;
  }
  return '';
}

function conversationSeed(body: string): string {
  try {
    const parsed = JSON.parse(body);
    if (!parsed) return '';
    if (typeof parsed.input === 'string' && parsed.input) return parsed.input;
    for (const field of ['messages', 'input']) {
      const arr = parsed[field];
      if (!Array.isArray(arr)) continue;
      for (const item of arr) {
        if (!item || typeof item !== 'object') continue;
        if (item.role !== 'user') continue;
        const content = JSON.stringify(item.content);
        if (content && content !== 'null') return content;
      }
    }
  } catch {}
  return '';
}

// Thinking models (deepseek, nemotron, muse-spark) require reasoning_content on every assistant message
function patchMissingReasoningContent(reqBody: string): string {
  if (!reqBody) return reqBody;
  try {
    const parsed = JSON.parse(reqBody);
    if (!parsed || !Array.isArray(parsed.messages)) return reqBody;
    let changed = false;
    for (const m of parsed.messages) {
      if (m && m.role === 'assistant' && !('reasoning_content' in m)) {
        m.reasoning_content = '';
        changed = true;
      }
    }
    return changed ? JSON.stringify(parsed) : reqBody;
  } catch {
    return reqBody;
  }
}

function collectHeadersFromReq(nodeReq: http.IncomingMessage, bodyStr?: string): Record<string, string> {
  const h: Record<string, string> = {};
  for (const k of FORWARD) {
    if (k === 'authorization') continue;
    const v = nodeReq.headers[k];
    if (v) h[k] = Array.isArray(v) ? v[0] : v;
  }
  h['authorization'] = 'Bearer public';
  h['x-opencode-client'] = 'cli';
  h['user-agent'] = h['user-agent']?.startsWith('opencode/') ? h['user-agent'] : OPENCODE_USER_AGENT;
  if (!h['content-type']) h['content-type'] = 'application/json';

  // Session: prefer client-provided, otherwise derive stableID from conversation seed
  let sessionSignal = firstNonEmpty(
    h['x-opencode-session'],
    nodeReq.headers['x-session-id'] as string,
    nodeReq.headers['conversation-id'] as string,
  );
  if (!sessionSignal && bodyStr) {
    try {
      const parsed = JSON.parse(bodyStr);
      sessionSignal = firstNonEmpty(parsed?.conversation_id, parsed?.metadata?.session_id);
    } catch {}
  }
  if (!sessionSignal && bodyStr) sessionSignal = conversationSeed(bodyStr);
  if (!sessionSignal || sessionSignal === '{}') sessionSignal = randomID('fallback', 16);
  // The session id must be in OpenCode's canonical shape or the anonymous lane
  // answers 403, so a client-supplied id is normalised too rather than trusted
  // as-is. Deriving from the same seed keeps prompt-cache affinity stable.
  h['x-opencode-session'] = FREE_TIER_AGENT_SHAPE
    ? canonicalSessionID(sessionSignal)
    : stableID('ses', sessionSignal);

  for (const name of ['x-opencode-session-id', 'x-session-affinity', 'x-session-id']) h[name] = h['x-opencode-session'];

  // Request: unique per request
  if (!h['x-opencode-request']) h['x-opencode-request'] = randomID('req', 16);

  // Project: client-provided or stable default
  let projectSignal = firstNonEmpty(h['x-opencode-project']);
  if (!projectSignal && bodyStr) {
    try {
      const parsed = JSON.parse(bodyStr);
      projectSignal = firstNonEmpty(parsed?.metadata?.project_id);
    } catch {}
  }
  if (!projectSignal) projectSignal = 'opencode2api:default-project';
  if (!h['x-opencode-project']) h['x-opencode-project'] = stableID('prj', projectSignal);

  return h;
}

function readBody(nodeReq: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    nodeReq.on('data', (c: Buffer) => chunks.push(c));
    nodeReq.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    nodeReq.on('error', reject);
  });
}

function sendJson(nodeRes: http.ServerResponse, status: number, data: any) {
  const body = JSON.stringify(data);
  nodeRes.writeHead(status, {
    'content-type': 'application/json',
    'access-control-allow-origin': '*',
  });
  nodeRes.end(body);
}

// Native Responses clients keep their response schema when stream:false.
async function collectResponsesStream(stream: ReadableStream<Uint8Array>): Promise<{ status: number; body: string }> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let response: any;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (value) buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (!line.startsWith('data:')) continue;
        let evt: any;
        try { evt = JSON.parse(line.slice(5).trim()); } catch { continue; }
        if (evt.error || evt.type === 'error' || evt.type === 'response.failed') {
          return { status: 502, body: JSON.stringify({ error: evt.error || evt.response?.error || { message: 'upstream stream error' } }) };
        }
        if (evt.type === 'response.completed' || evt.type === 'response.incomplete') response = evt.response;
      }
      if (done) break;
    }
    if (!response) throw new Error('upstream produced no final response');
    return { status: 200, body: JSON.stringify(response) };
  } catch (e: any) {
    return { status: 502, body: JSON.stringify({ error: { message: e?.message || 'stream read failed' } }) };
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}

// Reassemble Chat SSE into JSON, including streamed function calls.
async function collectChatStream(
  stream: ReadableStream<Uint8Array>,
  fallbackModel: string,
): Promise<{ status: number; body: string }> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let text = '';
  let model = fallbackModel;
  let id = '';
  let created = 0;
  let finishReason = 'stop';
  const toolCalls = new Map<number, any>();
  let promptTokens = 0, completionTokens = 0, totalTokens = 0;

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) buffer += decoder.decode(value, { stream: true });
      // SSE events are separated by a blank line; keep the last partial one.
      const events = buffer.split(/\r?\n\r?\n/);
      buffer = events.pop() || '';
      for (const raw of events) {
        for (const line of raw.split(/\r?\n/)) {
          if (!line.startsWith('data:')) continue;
          const data = line.slice(5).trim();
          if (!data || data === '[DONE]') continue;
          let evt: any;
          try { evt = JSON.parse(data); } catch { continue; }
          if (evt.error) {
            return {
              status: 502,
              body: JSON.stringify({ error: { message: evt.error.message || 'upstream stream error', type: 'upstream_error' } }),
            };
          }
          if (evt.id) id = evt.id;
          if (evt.model) model = evt.model;
          if (evt.created) created = evt.created;
          const delta = evt.choices?.[0]?.delta;
          if (typeof delta?.content === 'string') text += delta.content;
          for (const call of delta?.tool_calls || []) {
            const index = call.index ?? 0;
            const accumulated = toolCalls.get(index) || { id: '', type: 'function', function: { name: '', arguments: '' } };
            if (call.id) accumulated.id = call.id;
            if (call.function?.name) accumulated.function.name += call.function.name;
            if (call.function?.arguments) accumulated.function.arguments += call.function.arguments;
            toolCalls.set(index, accumulated);
          }
          if (evt.choices?.[0]?.finish_reason) finishReason = evt.choices?.[0]?.finish_reason;
          const u = evt.usage;
          if (u) {
            promptTokens = u.prompt_tokens ?? promptTokens;
            completionTokens = u.completion_tokens ?? completionTokens;
            totalTokens = u.total_tokens ?? totalTokens;
          }
        }
      }
    }
  } catch (e: any) {
    return { status: 502, body: JSON.stringify({ error: { message: e?.message || 'stream read failed' } }) };
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }

  if (!text && !toolCalls.size && totalTokens === 0) {
    return { status: 502, body: JSON.stringify({ error: { message: 'upstream produced no content' } }) };
  }
  return {
    status: 200,
    body: JSON.stringify({
      id: id || 'chatcmpl-opencode2api',
      object: 'chat.completion',
      created: created || Math.floor(Date.now() / 1000),
      model,
      choices: [{ index: 0, message: { role: 'assistant', content: text || (toolCalls.size ? null : ''),
        ...(toolCalls.size ? { tool_calls: [...toolCalls.entries()].sort(([a], [b]) => a - b).map(([, call]) => call) } : {}) }, finish_reason: finishReason }],
      usage: { prompt_tokens: promptTokens, completion_tokens: completionTokens, total_tokens: totalTokens },
    }),
  };
}

// Endpoint selection follows the same Zen metadata used by OpenCode2.
function isResponsesOnlyModel(model: string | undefined): boolean {
  return !!model && resolveCatalogModel(model, cachedModels)?.endpoint === 'responses';
}

// OpenAI chat body -> Responses body. Only the fields callers actually send are
// carried across; anything unrecognised is dropped rather than passed through,
// because the two schemas disagree about several names.
function chatBodyToResponses(bodyStr: string): string | null {
  let src: any;
  try { src = JSON.parse(bodyStr); } catch { return null; }
  if (!src || typeof src !== 'object' || Array.isArray(src)) return null;

  const input: any[] = [];
  const messages: any[] = Array.isArray(src.messages) ? src.messages : [];
  for (const m of messages) {
    if (!m || typeof m !== 'object') continue;
    const role = m.role;
    if (role === 'tool') {
      if (typeof m.tool_call_id !== 'string') return null;
      input.push({ type: 'function_call_output', call_id: m.tool_call_id,
        output: typeof m.content === 'string' ? m.content : JSON.stringify(m.content ?? '') });
      continue;
    }
    // Chat carries the system prompt as `system`; Responses expects it as a
    // `developer` turn inside the input array.
    const mapped = role === 'system' || role === 'developer' ? 'developer' : role;
    if (typeof m.content === 'string') {
      input.push({ role: mapped, content: m.content });
    } else if (Array.isArray(m.content)) {
      const parts = m.content
        .map((p: any) => {
          if (typeof p === 'string') return { type: 'input_text', text: p };
          if (p?.type === 'text') return { type: 'input_text', text: p.text };
          if (p?.type === 'image_url') {
            return { type: 'input_image', image_url: p.image_url?.url };
          }
          return null;
        })
        .filter(Boolean);
      if (parts.length) input.push({ role: mapped, content: parts });
    }
    if (role === 'assistant' && Array.isArray(m.tool_calls)) {
      for (const call of m.tool_calls) {
        if (call?.type !== 'function' || typeof call.id !== 'string' || typeof call.function?.name !== 'string') return null;
        input.push({ type: 'function_call', call_id: call.id, name: call.function.name,
          arguments: call.function.arguments || '{}' });
      }
    }
  }
  if (!input.length) return null;

  const out: any = {
    model: src.model,
    input,
    stream: true,                       // the anonymous lane rejects stream:false
    store: false,
  };
  if (src.max_tokens != null) out.max_output_tokens = src.max_tokens;
  else if (src.max_completion_tokens != null) out.max_output_tokens = src.max_completion_tokens;
  // The Responses endpoint rejects anything below 16, and a probe or a terse
  // caller can easily ask for less, so clamp rather than forward a 400.
  if (out.max_output_tokens != null && out.max_output_tokens < 16) out.max_output_tokens = 16;
  if (Array.isArray(src.tools) && src.tools.length) {
    out.tools = src.tools.map((t: any) => (
      t?.type === 'function' && t.function
        ? { type: 'function', name: t.function.name, description: t.function.description, parameters: t.function.parameters }
        : t
    ));
  }
  if (!out.tools || !out.tools.length) out.tools = freeTierStubToolset(FREE_TIER_CORE_TOOLS);
  if (src.tool_choice) out.tool_choice = src.tool_choice?.function
    ? { type: 'function', name: src.tool_choice.function.name } : src.tool_choice;
  if (src.reasoning_effort) out.reasoning = { effort: src.reasoning_effort };
  if (src.temperature != null) out.temperature = src.temperature;
  if (src.top_p != null) out.top_p = src.top_p;
  return JSON.stringify(out);
}

// Responses SSE -> OpenAI chat SSE, so everything downstream in this gateway
// (stream piping, usage accounting, the non-streaming reassembler) keeps
// working on one shape.
function responsesSseToChatSse(
  upstream: ReadableStream<Uint8Array>,
  fallbackModel: string,
): ReadableStream<Uint8Array> {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = '';
  let model = fallbackModel;
  let responseId = '';
  let created = 0;
  let sentRole = false;
  let sentFinish = false;
  const reader = upstream.getReader();
  const calls = new Map<string, { index: number; id: string; name: string; arguments: string; announced: boolean }>();

  const chunk = (delta: any, finish: string | null, usage?: any) => JSON.stringify({
    id: responseId || 'chatcmpl-opencode2api',
    object: 'chat.completion.chunk',
    created: created || Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, delta, finish_reason: finish }],
    ...(usage ? { usage } : {}),
  });

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const emit = (obj: string) => controller.enqueue(encoder.encode(`data: ${obj}\n\n`));
      const ensureRole = () => {
        if (!sentRole) { sentRole = true; emit(chunk({ role: 'assistant', content: '' }, null)); }
      };
      const tool = (key: string, item: any = {}) => {
        let call = calls.get(key);
        if (!call) {
          call = { index: calls.size, id: item.call_id || key, name: item.name || '', arguments: '', announced: false };
          calls.set(key, call);
        }
        if (item.call_id) call.id = item.call_id;
        if (item.name) call.name = item.name;
        return call;
      };
      const announce = (call: ReturnType<typeof tool>) => {
        ensureRole();
        if (call.announced) return;
        call.announced = true;
        emit(chunk({ tool_calls: [{ index: call.index, id: call.id, type: 'function', function: { name: call.name, arguments: '' } }] }, null));
      };
      const argumentsDone = (key: string, item: any) => {
        const call = tool(key, item); announce(call);
        const argumentsText = typeof item.arguments === 'string' ? item.arguments : '';
        if (argumentsText.startsWith(call.arguments) && argumentsText.length > call.arguments.length) {
          emit(chunk({ tool_calls: [{ index: call.index, function: { arguments: argumentsText.slice(call.arguments.length) } }] }, null));
          call.arguments = argumentsText;
        }
      };
      const handle = (evt: any) => {
        const type = evt?.type;
        if (type === 'response.created' || type === 'response.in_progress') {
          const r = evt.response || {};
          if (r.id) responseId = r.id;
          if (r.model) model = r.model;
          if (r.created_at) created = r.created_at;
          return;
        }
        if (type === 'response.output_item.added' && evt.item?.type === 'function_call') {
          announce(tool(evt.item.id || String(evt.output_index), evt.item));
          return;
        }
        if (type === 'response.function_call_arguments.delta') {
          const call = tool(evt.item_id || String(evt.output_index)); announce(call);
          if (typeof evt.delta === 'string') {
            call.arguments += evt.delta;
            emit(chunk({ tool_calls: [{ index: call.index, function: { arguments: evt.delta } }] }, null));
          }
          return;
        }
        if (type === 'response.output_item.done' && evt.item?.type === 'function_call') {
          argumentsDone(evt.item.id || String(evt.output_index), evt.item); return;
        }
        if (type === 'response.function_call_arguments.done') {
          argumentsDone(evt.item_id || String(evt.output_index), evt); return;
        }
        if (type === 'error' || type === 'response.failed' || evt.error) {
          emit(JSON.stringify({ error: evt.error || evt.response?.error || { message: 'responses stream failed' } }));
          sentFinish = true; controller.close(); return;
        }
        if (type === 'response.output_text.delta') {
          if (!sentRole) { sentRole = true; emit(chunk({ role: 'assistant', content: '' }, null)); }
          if (typeof evt.delta === 'string' && evt.delta) emit(chunk({ content: evt.delta }, null));
          return;
        }
        if (type === 'response.completed' || type === 'response.incomplete') {
          if (!sentRole) { sentRole = true; emit(chunk({ role: 'assistant', content: '' }, null)); }
          if (sentFinish) return;
          sentFinish = true;
          for (const item of evt.response?.output || []) {
            if (item.type === 'function_call') argumentsDone(item.id || item.call_id, item);
          }
          const u = evt.response?.usage;
          const usage = u ? {
            prompt_tokens: u.input_tokens ?? 0,
            completion_tokens: u.output_tokens ?? 0,
            total_tokens: u.total_tokens ?? ((u.input_tokens ?? 0) + (u.output_tokens ?? 0)),
          } : undefined;
          emit(chunk({}, calls.size ? 'tool_calls' : (type === 'response.incomplete' ? 'length' : 'stop'), usage));
          controller.enqueue(encoder.encode('data: [DONE]\n\n'));
          controller.close();
        }
      };

      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value) buffer += decoder.decode(value, { stream: true });
          const events = buffer.split(/\r?\n\r?\n/);
          buffer = events.pop() || '';
          for (const raw of events) {
            for (const line of raw.split(/\r?\n/)) {
              if (!line.startsWith('data:')) continue;
              const data = line.slice(5).trim();
              if (!data || data === '[DONE]') continue;
              try { handle(JSON.parse(data)); } catch {}
            }
          }
          if (sentFinish) { try { await reader.cancel(); } catch {} return; }
        }
      } catch (e: any) {
        try {
          emit(JSON.stringify({ error: { message: e?.message || 'responses stream failed' } }));
        } catch {}
      } finally {
        if (!sentFinish) {
          sentFinish = true;
          try { emit(chunk({}, 'stop')); } catch {}
          try { controller.enqueue(encoder.encode('data: [DONE]\n\n')); } catch {}
        }
        try { controller.close(); } catch {}
      }
    },
    cancel(reason) { return reader.cancel(reason); },
  });
}

function sendCors(nodeRes: http.ServerResponse) {
  nodeRes.writeHead(204, {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'access-control-allow-headers': 'content-type, authorization',
  });
  nodeRes.end();
}

// The server binds every interface, so /api/keys — which hands out the very
// credentials callers use to reach the upstream — cannot stay open. Gate it on
// the admin key. Compared in constant time so the check does not leak the key
// through response timing.
function requireAdmin(nodeReq: http.IncomingMessage, nodeRes: http.ServerResponse): boolean {
  const presented = String(nodeReq.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
  const a = Buffer.from(presented);
  const b = Buffer.from(API_KEY);
  const ok = a.length === b.length && crypto.timingSafeEqual(a, b);
  if (!ok) {
    sendJson(nodeRes, 401, { error: 'unauthorized', message: 'Admin key required' });
  }
  return ok;
}

// Bound the complete transfer, including stalled bodies. A malformed or HTTP
// error response must never replace a working catalog with an empty one.
function fetchJsonDirect(url: string, timeoutMs: number, options: https.RequestOptions = {}): Promise<any> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let req: http.ClientRequest;
    const finish = (error?: Error, value?: any) => {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      if (error) { req?.destroy(); reject(error); }
      else resolve(value);
    };
    const deadline = setTimeout(() => finish(new Error('timeout')), timeoutMs);
    req = https.request(url, {
      ...options,
      method: 'GET',
      headers: { accept: 'application/json', 'user-agent': OPENCODE_USER_AGENT, ...options.headers },
    }, (res) => {
      const chunks: Buffer[] = [];
      let size = 0;
      res.on('data', (c: Buffer) => {
        size += c.length;
        if (size > 16 * 1024 * 1024) { finish(new Error('catalog response too large')); return; }
        chunks.push(c);
      });
      res.on('error', (e) => finish(e));
      res.on('aborted', () => finish(new Error('catalog response aborted')));
      res.on('end', () => {
        if (settled) return;
        if (res.statusCode !== 200) { finish(new Error(`catalog HTTP ${res.statusCode}`)); return; }
        try { finish(undefined, JSON.parse(Buffer.concat(chunks).toString('utf-8'))); }
        catch { finish(new Error('invalid catalog JSON')); }
      });
    });
    req.on('error', (e) => finish(e));
    req.end();
  });
}

async function fetchModelsDevMetadata(): Promise<any> {
  if (modelsDevMetadata && Date.now() - modelsDevTime < MODELS_DEV_TTL_MS) return modelsDevMetadata;
  const doc = await fetchJsonDirect(MODELS_DEV_URL, 10000);
  // Validate the provider document before caching it.
  buildZenCatalog({ data: [] }, doc);
  modelsDevMetadata = { opencode: doc.opencode };
  modelsDevTime = Date.now();
  return modelsDevMetadata;
}

function applyModelCatalog(upstream: any, metadata: any, checkedAt: number): ZenModel[] {
  const catalog = buildZenCatalog(upstream, metadata);
  // A mismatched feed is a discovery failure. Known models becoming paid or
  // deprecated is a valid empty catalog and must immediately withdraw them.
  if (!catalog.models.length && !upstream.data.some((m: any) => typeof m?.id === 'string' && metadata.opencode.models[m.id])) {
    throw new Error('upstream model IDs do not match Zen metadata');
  }
  cachedModels = catalog.models;
  cachedModelsTime = checkedAt;
  catalogExcluded = catalog.excluded;
  const canonical = new Set(cachedModels.map(m => m.canonical_id || m.id));
  for (const id of freeModelHealth.keys()) if (!canonical.has(id)) freeModelHealth.delete(id);
  return cachedModels;
}

function loadModelCatalog(): void {
  try {
    const doc = JSON.parse(fs.readFileSync(MODEL_CATALOG_FILE, 'utf8'));
    const age = Date.now() - doc.checkedAt;
    if (doc.version !== 1 || doc.upstream !== UPSTREAM || !Number.isFinite(age) || age < 0 || age > MODEL_CATALOG_MAX_STALE_MS) return;
    applyModelCatalog(doc.models, doc.metadata, doc.checkedAt);
    console.log(`[Models] Loaded last successful Zen catalog (${cachedModels.length} IDs)`);
  } catch { /* Missing, invalid or expired state is rediscovered online. */ }
}

function catalogStatus() {
  return {
    provider: 'opencode',
    checkedAt: cachedModelsTime || null,
    stale: Date.now() - cachedModelsTime >= MODEL_CATALOG_TTL_MS,
    lastError: catalogLastError || null,
    excluded: catalogExcluded,
  };
}

async function fetchModelsFromUpstream(): Promise<ZenModel[]> {
  if (catalogRefresh) return catalogRefresh;
  const canUseCache = () => cachedModelsTime > 0 && Date.now() - cachedModelsTime <= MODEL_CATALOG_MAX_STALE_MS;
  if (catalogLastError && Date.now() - catalogLastAttempt < MODEL_CATALOG_RETRY_MS) {
    if (canUseCache()) return cachedModels;
    throw new Error(catalogLastError);
  }
  catalogLastAttempt = Date.now();
  catalogRefresh = (async () => {
    try {
      const agent = warpSlot ? new SocksProxyAgent(warpSlot.url, { timeout: 10000 }) as unknown as https.Agent : undefined;
      const [upstream, metadata] = await Promise.all([
        fetchJsonDirect(`${UPSTREAM}/v1/models`, 10000, {
          agent,
          headers: { authorization: 'Bearer public', 'x-opencode-client': 'cli',
            'x-opencode-session': canonicalSessionID('opencode2api-models') },
        }),
        fetchModelsDevMetadata(),
      ]);
      // Empty lists are not successful discovery; retain the previous catalog.
      if (!Array.isArray(upstream?.data) || !upstream.data.length) throw new Error('empty or invalid upstream model catalog');
      applyModelCatalog(upstream, metadata, Date.now());
      catalogLastError = '';
      try {
        const tmp = `${MODEL_CATALOG_FILE}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify({ version: 1, upstream: UPSTREAM, checkedAt: cachedModelsTime, models: upstream, metadata }));
        fs.renameSync(tmp, MODEL_CATALOG_FILE);
      } catch (e: any) { console.warn(`[Models] Could not persist catalog: ${e?.message || e}`); }
      return cachedModels;
    } catch (e: any) {
      catalogLastError = e?.message || String(e);
      if (canUseCache()) {
        console.warn(`[Models] Refresh failed, retaining last successful Zen catalog: ${catalogLastError}`);
        return cachedModels;
      }
      throw e;
    }
  })();
  try { return await catalogRefresh; } finally { catalogRefresh = null; }
}

async function ensureModelCatalog(): Promise<ZenModel[]> {
  if (cachedModelsTime > 0 && Date.now() - cachedModelsTime < MODEL_CATALOG_TTL_MS) return cachedModels;
  return fetchModelsFromUpstream();
}

function resolveAliasId(model: string): string {
  const selected = resolveCatalogModel(model, cachedModels);
  return selected?.canonical_id || selected?.id || model;
}

function getModelHealth(model: string): ModelHealth | undefined {
  return freeModelHealth.get(resolveAliasId(model));
}

// Reject empty HTTP 200 streams and provider errors carried inside SSE.
function hasModelOutput(body: string): boolean {
  const docs: any[] = [];
  try { docs.push(JSON.parse(body)); } catch {
    for (const line of body.split(/\r?\n/)) {
      if (!line.startsWith('data:')) continue;
      try { docs.push(JSON.parse(line.slice(5).trim())); } catch {}
    }
  }
  if (docs.some(d => d?.error || d?.type === 'error' || d?.type === 'response.failed')) return false;
  return docs.some(d => {
    const response = d?.response || d;
    return (d?.type === 'response.output_text.delta' && !!d.delta)
      || response?.output?.some((o: any) => o?.content?.some((c: any) => !!c?.text) || o?.type === 'function_call')
      || response?.choices?.some((c: any) => !!(c?.delta?.content || c?.delta?.reasoning_content || c?.delta?.tool_calls?.length || c?.message?.content || c?.message?.tool_calls?.length))
      || (response?.usage?.completion_tokens > 0 || response?.usage?.output_tokens > 0);
  });
}

async function probeFreeModel(model: string): Promise<ModelHealth> {
  const prev = getModelHealth(model);
  const target = resolveAliasId(model);
  let status = 0;
  let reason = '';
  try {
    const result = await new Promise<{ status: number; body: string }>((resolve, reject) => {
      let req: http.ClientRequest;
      let finished = false;
      const finish = (error?: Error, value?: { status: number; body: string }) => {
        if (finished) return;
        finished = true;
        clearTimeout(deadline);
        if (error) { req?.destroy(); reject(error); }
        else resolve(value!);
      };
      const deadline = setTimeout(() => finish(new Error('probe timeout')), 20000);
      const payload = (() => {
        const base = {
          model: target,
          messages: [{ role: 'user', content: 'hi' }],
          max_tokens: 128,
          stream: false,
        };
        // The probe must use the same shape as real traffic. A non-streaming,
        // tool-less body is exactly what the anonymous lane answers 403, which
        // would make every healthy model look dead and withhold it.
        if (!FREE_TIER_AGENT_SHAPE) return JSON.stringify(base);
        // Responses-only models must be probed on the endpoint that serves
        // them, or the probe sees the same bare 500 real callers would.
        if (isResponsesOnlyModel(target)) {
          return JSON.stringify({
            model: target,
            input: [{ role: 'user', content: 'hi' }],
            max_output_tokens: 128,
            stream: true,
            store: false,
            tools: freeTierStubToolset(FREE_TIER_CORE_TOOLS).map((t: any) => ({
              type: 'function', name: t.function.name,
              description: t.function.description, parameters: t.function.parameters,
            })),
          });
        }
        return JSON.stringify({
          ...base,
          stream: true,
          stream_options: { include_usage: true },
          tools: freeTierStubToolset(FREE_TIER_CORE_TOOLS),
        });
      })();
      const probePath = isResponsesOnlyModel(target) ? '/v1/responses' : '/v1/chat/completions';
      req = https.request(`${UPSTREAM}${probePath}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          accept: 'application/json',
          authorization: 'Bearer public',
          'x-opencode-client': 'cli',
          'x-opencode-session': canonicalSessionID(`probe-${model}`),
          'x-opencode-request': randomID('probe', 8),
          'x-opencode-project': 'opencode2api',
          'user-agent': OPENCODE_USER_AGENT,
          'content-length': Buffer.byteLength(payload),
        },
        rejectUnauthorized: false,
        timeout: 20000,
      }, (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('error', (e) => finish(e));
        res.on('aborted', () => finish(new Error('probe aborted')));
        res.on('end', () => finish(undefined, {
          status: res.statusCode || 0,
          body: Buffer.concat(chunks).toString('utf-8'),
        }));
      });
      req.on('error', (e) => finish(e));
      req.on('timeout', () => finish(new Error('timeout')));
      req.end(payload);
    });
    status = result.status;
    if (status === 200 && !hasModelOutput(result.body)) { status = 502; reason = 'upstream produced no valid model output'; }
    if (status >= 200 && status < 400) {
      reason = 'ok';
    } else {
      try {
        const doc: any = JSON.parse(result.body);
        reason = doc?.error?.message || doc?.message || reason || `HTTP ${status}`;
      } catch {
        reason = reason || `HTTP ${status}`;
      }
    }
  } catch (e: any) {
    status = 0;
    reason = e?.message || String(e);
  }

  let health: ModelHealth;
  if (status >= 200 && status < 400) {
    health = { verdict: 'healthy', status, reason: 'ok', consecutiveFails: 0, checkedAt: Date.now() };
  } else if (isTransientStatus(status)) {
    // Keep whatever verdict we had; a blip must not blacklist a working model.
    health = {
      verdict: prev && prev.verdict !== 'unknown' ? prev.verdict : 'unknown',
      status, reason: reason || `HTTP ${status}`,
      consecutiveFails: prev?.consecutiveFails || 0,
      checkedAt: Date.now(),
    };
  } else if (isHardFailure(status)) {
    health = { verdict: 'dead', status, reason: reason || `HTTP ${status}`, consecutiveFails: prev?.consecutiveFails || 0, checkedAt: Date.now() };
  } else {
    // Ambiguous failure (5xx and friends). Count it, but require repeats before
    // calling it dead so a single upstream hiccup does not withdraw a model.
    const fails = (prev?.consecutiveFails || 0) + 1;
    health = {
      verdict: fails >= MODEL_DEAD_AFTER_FAILS ? 'dead' : (prev?.verdict === 'dead' ? 'dead' : 'unknown'),
      status, reason: reason || `HTTP ${status}`,
      consecutiveFails: fails,
      checkedAt: Date.now(),
    };
  }
  freeModelHealth.set(target, health);
  return health;
}

async function verifyFreeModels(): Promise<void> {
  if (verifyingModels) return;
  verifyingModels = true;
  try {
    await ensureModelCatalog();
    const ids = [...new Set(cachedModels.map(m => m.canonical_id || m.id))];
    for (let i = 0; i < ids.length; i += MODEL_VERIFY_CONCURRENCY) {
      await Promise.all(ids.slice(i, i + MODEL_VERIFY_CONCURRENCY).map(probeFreeModel));
    }
    // One retry for transport failures. Never immediately retry a 429.
    // Cheap, and it stops a transient blip from leaving a healthy model
    // unclassified until the next 30-minute interval.
    const inconclusive = ids.filter(id => {
      const h = freeModelHealth.get(id);
      return !h || (h.verdict === 'unknown' && h.status === 0);
    });
    if (inconclusive.length) {
      console.log(`[Models] retrying ${inconclusive.length} inconclusive probe(s)`);
      for (let i = 0; i < inconclusive.length; i += MODEL_VERIFY_CONCURRENCY) {
        await Promise.all(inconclusive.slice(i, i + MODEL_VERIFY_CONCURRENCY).map(probeFreeModel));
      }
    }
    modelsVerified = true;
    const verdict = (id: string) => freeModelHealth.get(id)?.verdict || 'unknown';
    const healthy = ids.filter(id => verdict(id) === 'healthy');
    const dead = ids.filter(id => verdict(id) === 'dead');
    const unknown = ids.filter(id => verdict(id) === 'unknown');
    const detail = (ids: string[]) => ids.map(id => {
      const h = freeModelHealth.get(id);
      return `${id} [${h?.status ?? '?'} ${(h?.reason || 'no reason').slice(0, 48)}]`;
    });
    console.log(`[Models] Probed ${ids.length} free models: ${healthy.length} callable, ${dead.length} dead, ${unknown.length} unconfirmed`);
    if (dead.length) console.log(`[Models]   dead: ${detail(dead).join('; ')}`);
    if (unknown.length) console.log(`[Models]   unconfirmed (needs ${MODEL_DEAD_AFTER_FAILS} consecutive failures to be withdrawn): ${detail(unknown).join('; ')}`);
    if (!healthy.length) console.warn(`[Models] No free model verified callable; requests for dead models cannot be rerouted.`);
  } catch (e: any) {
    console.warn(`[Models] verification pass failed: ${e?.message || e}`);
  } finally {
    verifyingModels = false;
  }
}

// Models safe to send real traffic to.
//
// A model is withheld only on a *definitive* upstream rejection: 4xx goes
// straight to 'dead', and 5xx needs MODEL_DEAD_AFTER_FAILS consecutive strikes.
// An inconclusive probe (network timeout, 408/425/429) carries no information
// about the model, so it must not withdraw one — treating "unknown" as
// "unusable" made a working model disappear from /v1/models because a single
// probe timed out, and it also counted unknown models as callable in the
// progress log. Unconfirmed models stay advertised and are labelled as such in
// /api/models so the dashboard can show the difference.
function workingFreeModelIds(): string[] {
  const ids = cachedModels.map(m => m.id).filter(Boolean);
  if (!modelsVerified && freeModelHealth.size === 0) return ids;
  return ids.filter(id => getModelHealth(id)?.verdict !== 'dead');
}

function pickWorkingModel(endpoint?: 'responses'): string | null {
  // Prefer confirmed healthy models, then unconfirmed catalog members.
  const ids = workingFreeModelIds().filter(id => !endpoint || isResponsesOnlyModel(id));
  return ids.find(id => getModelHealth(id)?.verdict === 'healthy') || ids[0] || null;
}

// Side-channel for the dashboard: which IDs are actually callable and why the
// rest are not. The model list itself stays complete so the UI can show
// withdrawn models as disabled entries instead of hiding the fact they exist.
function modelAvailability(): { callableIds: string[]; withheld: { id: string; verdict: ModelVerdict; status: number; reason: string }[] } {
  const ids = cachedModels.map(m => m.id).filter(Boolean);
  const callable = new Set(workingFreeModelIds());
  return {
    callableIds: ids.filter(id => callable.has(id)),
    withheld: ids
      .filter(id => !callable.has(id))
      .map(id => {
        const h = getModelHealth(id);
        return {
          id,
          verdict: h?.verdict || 'unknown',
          status: h?.status ?? 0,
          reason: h?.reason || (modelsVerified ? 'not verified callable' : 'not checked yet'),
        };
      }),
  };
}

// Accept stripped and common aliases (e.g. `mimo-v2.5` → `mimo-v2.5-free`, `muse-spark-1.3-free` → `muse-spark-1.3-contributor-free`).
// Never rewrites `big-pickle` or paid models.
// Also injects stream_options.include_usage on streamed requests so usage
// comes back in the SSE trailer (opencodex openai-chat.ts does the same);
// otherwise streamed calls never record token counts.
// Also routes around models verified dead: a caller asking for one gets a
// working free model instead of the upstream's raw 400/403/500, and the swap is
// reported on the response so it is never silent.
function normalizeFreeModelAlias(bodyStr: string | undefined, endpoint?: 'responses'): { body?: string; substitutedFrom?: string; substitutedTo?: string; error?: string } {
  try {
    const parsed = JSON.parse(patchMissingReasoningContent(bodyStr || ''));
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed) || typeof parsed.model !== 'string') {
      return { error: 'Request must include a model from /v1/models' };
    }
    const selected = resolveCatalogModel(parsed.model, cachedModels);
    if (!selected) return { error: `Unsupported free Zen model: ${parsed.model}. See /v1/models.` };
    parsed.model = selected.canonical_id || selected.id;
    let substitutedFrom: string | undefined;
    let substitutedTo: string | undefined;
    if (getModelHealth(parsed.model)?.verdict === 'dead') {
      const replacement = pickWorkingModel(endpoint);
      if (replacement) {
        substitutedFrom = parsed.model;
        substitutedTo = resolveAliasId(replacement);
        parsed.model = substitutedTo;
      }
    }
    return { body: JSON.stringify(parsed), substitutedFrom, substitutedTo };
  } catch { return { error: 'Invalid JSON request' }; }
}

const server = http.createServer(async (nodeReq, nodeRes) => {
  const url = new URL(nodeReq.url || '/', `http://${nodeReq.headers.host || 'localhost'}`);
  const pathname = url.pathname;
  const search = url.search;
  const method = nodeReq.method || 'GET';

  // CORS
  if (method === 'OPTIONS') {
    sendCors(nodeRes);
    return;
  }

  // –– Static files (public/) ––
  // Root path → index.html
  if (pathname === '/' || pathname === '') {
    try {
      const data = fs.readFileSync(path.join(process.cwd(), 'public', 'index.html'));
      nodeRes.writeHead(200, { 'content-type': 'text/html' });
      nodeRes.end(data);
    } catch {
      nodeRes.writeHead(404);
      nodeRes.end('Not Found');
    }
    return;
  }
  if (pathname.startsWith('/public/')) {
    const filePath = path.join(process.cwd(), pathname);
    try {
      const data = fs.readFileSync(filePath);
      const ext = path.extname(filePath);
      const mimeMap: Record<string, string> = {
        '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
        '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml',
      };
      nodeRes.writeHead(200, { 'content-type': mimeMap[ext] || 'application/octet-stream' });
      nodeRes.end(data);
    } catch {
      nodeRes.writeHead(404);
      nodeRes.end('Not Found');
    }
    return;
  }

  // –– API: Status ––
  if (pathname === '/api/status' && method === 'GET') {
    const uptime = Math.floor((Date.now() - START_TIME) / 1000);
    const poolsInfo: any[] = [];
    for (const [keyId, pool] of keySlotPools) {
      poolsInfo.push({
        key: keyId.slice(0, 7) + '...' + keyId.slice(-4),
        name: apiKeys[keyId]?.name || 'unknown',
        enabled: apiKeys[keyId]?.enabled ?? false,
        slots: pool.slots.map(s => ({
          addr: s.addr,
          latencyMs: s.latencyMs,
          grade: s.qualityGrade,
        })),
        lastUsedAt: pool.lastUsedAt,
        requestCount: apiKeys[keyId]?.requestCount || 0,
      });
    }
    const totalSlots = SLOTS_PER_KEY * MAX_ACTIVE_KEYS;
    let slotsReady = 0;
    for (const pool of keySlotPools.values()) slotsReady += pool.slots.length;
    sendJson(nodeRes, 200, {
      ok: true,
      uptime,
      stats,
      activeKeys: keySlotPools.size,
      maxActiveKeys: MAX_ACTIVE_KEYS,
      slotCount: SLOTS_PER_KEY,
      slotsReady,
      pools: poolsInfo,
      warpAvailable: !!warpSlot,
      warpStatus,
      warpMode: warpModeRuntime,
      candidatesCount: candidates.length,
      candidates: candidates.length,
      customSlotsCount: customSlots.length,
      totalApiKeys: Object.keys(apiKeys).length,
      pool: {
        state: currentPoolState(),
        freeExits: freeExitCount(),
        target: POOL_TARGET,
        admitCeilingMs: admissionCeilingMs(),
        validatedTotal: validatedExits.size,
        grades: candidates.reduce((acc: any, c) => {
          if (c.lockedBy || !isExitUsable(c.address, undefined) || !isValidated(c.address)) return acc;
          const g = gradeFromLatency(c.latency ?? 9999);
          acc[g] = (acc[g] || 0) + 1;
          return acc;
        }, {} as Record<string, number>),
      },
    });
    return;
  }

  // –– API: Logs ––
  if (pathname === '/api/logs' && method === 'GET') {
    sendJson(nodeRes, 200, { logs: recentLogs.slice(-200) });
    return;
  }

  // –– API: Usage Audit ––
  if (pathname === '/api/audit' && method === 'GET') {
    const totalRequests = auditLog.length;
    let totalTokens = 0, totalPrompt = 0, totalCompletion = 0, cacheRead = 0;
    const keyMap: Record<string, { name: string; key: string; requests: number; totalTokens: number; lastUsedAt: number | null }> = {};
    const modelMap: Record<string, { model: string; requests: number; promptTokens: number; completionTokens: number; totalTokens: number; cacheRead: number }> = {};
    const dayMap: Record<string, { date: string; requests: number; totalTokens: number; promptTokens: number; completionTokens: number; cacheRead: number }> = {};

    for (const log of auditLog) {
      totalTokens += log.totalTokens || 0;
      totalPrompt += log.promptTokens || 0;
      totalCompletion += log.completionTokens || 0;
      cacheRead += log.cacheRead || 0;

      const keyId = log.keyId || 'unknown';
      if (!keyMap[keyId]) {
        keyMap[keyId] = { name: 'unknown', key: keyId, requests: 0, totalTokens: 0, lastUsedAt: null };
      }
      keyMap[keyId].requests++;
      keyMap[keyId].totalTokens += log.totalTokens || 0;
      if (log.ts && (!keyMap[keyId].lastUsedAt || log.ts > keyMap[keyId].lastUsedAt)) {
        keyMap[keyId].lastUsedAt = log.ts;
      }

      const mdl = log.model || 'unknown';
      if (!modelMap[mdl]) {
        modelMap[mdl] = { model: mdl, requests: 0, promptTokens: 0, completionTokens: 0, totalTokens: 0, cacheRead: 0 };
      }
      modelMap[mdl].requests++;
      modelMap[mdl].promptTokens += log.promptTokens || 0;
      modelMap[mdl].completionTokens += log.completionTokens || 0;
      modelMap[mdl].totalTokens += log.totalTokens || 0;
      modelMap[mdl].cacheRead += log.cacheRead || 0;

      const day = new Date(log.ts || Date.now()).toISOString().split('T')[0];
      if (!dayMap[day]) {
        dayMap[day] = { date: day, requests: 0, totalTokens: 0, promptTokens: 0, completionTokens: 0, cacheRead: 0 };
      }
      dayMap[day].requests++;
      dayMap[day].totalTokens += log.totalTokens || 0;
      dayMap[day].promptTokens += log.promptTokens || 0;
      dayMap[day].completionTokens += log.completionTokens || 0;
      dayMap[day].cacheRead += log.cacheRead || 0;
    }

    const cacheHitRate = totalTokens > 0 ? cacheRead / totalTokens : 0;

    sendJson(nodeRes, 200, {
      summary: {
        totalRequests,
        totalTokens,
        totalPrompt,
        totalCompletion,
        cacheHitRate,
      },
      keys: Object.values(keyMap).sort((a, b) => b.requests - a.requests),
      models: Object.values(modelMap).sort((a, b) => b.requests - a.requests),
      days: Object.values(dayMap).sort((a, b) => b.date.localeCompare(a.date)),
    });
    return;
  }

  // –– API: Zen model catalog and live availability ––
  if (pathname === '/api/models' && method === 'GET') {
    try {
      if (cachedModelsTime > 0 && Date.now() - cachedModelsTime < MODEL_CATALOG_TTL_MS) {
        sendJson(nodeRes, 200, { models: cachedModels, ...modelAvailability(), catalog: catalogStatus() });
        return;
      }
      const freeModels = await fetchModelsFromUpstream();
      sendJson(nodeRes, 200, { models: freeModels, ...modelAvailability(), catalog: catalogStatus() });
    } catch (e: any) {
      sendJson(nodeRes, 502, { error: e.message });
    }
    return;
  }

  // –– API: Key Management (admin only) ––
  if (pathname === '/api/keys' && method === 'GET') {
    if (!requireAdmin(nodeReq, nodeRes)) return;
    const keys = Object.values(apiKeys).map(r => ({
      key: r.key.slice(0, 7) + '...' + r.key.slice(-4),
      fullKey: r.key,
      name: r.name,
      enabled: r.enabled,
      createdAt: r.createdAt,
      lastUsedAt: r.lastUsedAt,
      totalRequests: r.totalRequests,
      totalTokens: r.totalTokens,
      maxConcurrency: r.maxConcurrency,
      maxRequests: r.maxRequests,
      requestCount: r.requestCount,
      expiresAt: r.expiresAt,
    }));
    sendJson(nodeRes, 200, { keys });
    return;
  }

  // POST /api/keys — Create new Key
  if (pathname === '/api/keys' && method === 'POST') {
    if (!requireAdmin(nodeReq, nodeRes)) return;
    try {
      const body = JSON.parse(await readBody(nodeReq));
      const newKey = body.key || crypto.randomBytes(24).toString('hex');
      if (apiKeys[newKey]) {
        sendJson(nodeRes, 409, { error: 'Key already exists' });
        return;
      }
      apiKeys[newKey] = {
        key: newKey,
        name: body.name || 'unnamed',
        enabled: body.enabled !== false,
        createdAt: Date.now(),
        lastUsedAt: 0,
        totalRequests: 0,
        totalTokens: 0,
        maxConcurrency: body.maxConcurrency || 0,
        maxRequests: body.maxRequests || 0,
        requestCount: 0,
        expiresAt: body.expiresAt || 0,
      };
      saveKeys();
      sendJson(nodeRes, 201, { key: newKey, message: 'Created successfully' });
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // PUT /api/keys/:key — Update Key
  const putMatch = pathname.match(/^\/api\/keys\/(.+)$/);
  if (putMatch && method === 'PUT') {
    // Authenticate before the existence check, otherwise a 404-vs-401 split
    // lets an unauthenticated caller enumerate which keys exist.
    if (!requireAdmin(nodeReq, nodeRes)) return;
    const targetKey = putMatch[1];
    const record = apiKeys[targetKey];
    if (!record) {
      sendJson(nodeRes, 404, { error: 'Key not found' });
      return;
    }
    try {
      const body = JSON.parse(await readBody(nodeReq));
      if (body.name !== undefined) record.name = body.name;
      if (body.enabled !== undefined) record.enabled = body.enabled;
      if (body.maxConcurrency !== undefined) record.maxConcurrency = body.maxConcurrency;
      if (body.maxRequests !== undefined) record.maxRequests = body.maxRequests;
      if (body.expiresAt !== undefined) record.expiresAt = body.expiresAt;
      saveKeys();
      // If disabled or quota exceeded → release slots
      if (!record.enabled) {
        releaseKeySlots(targetKey);
      } else if (record.maxRequests > 0 && record.requestCount >= record.maxRequests) {
        releaseKeySlots(targetKey);
      }
      sendJson(nodeRes, 200, { message: 'Updated successfully' });
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // DELETE /api/keys/:key — Delete Key
  if (putMatch && method === 'DELETE') {
    if (!requireAdmin(nodeReq, nodeRes)) return;
    const targetKey = putMatch[1];
    if (!apiKeys[targetKey]) {
      sendJson(nodeRes, 404, { error: 'Key not found' });
      return;
    }
    releaseKeySlots(targetKey);
    delete apiKeys[targetKey];
    saveKeys();
    sendJson(nodeRes, 200, { message: 'Deleted successfully' });
    return;
  }

  // –– API: WARP Control ––
  if (pathname === '/api/warp' && method === 'POST') {
    try {
      const body = JSON.parse(await readBody(nodeReq));
      if (body.action === 'enable') {
        warpModeRuntime = 'on';
        warpHostRuntime = body.host || WARP_HOST;
        warpPortRuntime = body.port || WARP_SOCKS5_PORT;
        warpConsecutiveFails = 0;
        warpSkipUntil = 0;
        const ok = await probeWarp();
        sendJson(nodeRes, 200, { ok, warpStatus, message: ok ? 'WARP enabled' : 'WARP probe failed' });
      } else if (body.action === 'disable') {
        warpModeRuntime = 'off';
        warpSlot = null;
        warpStatus = 'stopped';
        warpConsecutiveFails = 0;
        warpSkipUntil = 0;
        sendJson(nodeRes, 200, { ok: true, message: 'WARP disabled' });
      } else {
        sendJson(nodeRes, 400, { error: 'Unknown action' });
      }
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // –– API: Probe Standby (WARP + custom fallback proxies) ––
  // The dashboard has called this since it was added; it had no route at all
  // and every press returned 404.
  if (pathname === '/api/standby/probe' && method === 'POST') {
    const warpOk = warpModeRuntime === 'on' ? await probeWarp() : null;
    const custom = await initCustomSlots();
    sendJson(nodeRes, 200, {
      ok: true,
      warp: { enabled: warpModeRuntime === 'on', healthy: warpOk, status: warpStatus },
      custom: { total: custom.length, healthy: custom.filter(c => c.ok).length, slots: custom },
      readySlots: customSlots.length + (warpSlot ? 1 : 0),
    });
    return;
  }

  // –– API: Refresh Candidates ––
  if (pathname === '/api/refresh' && method === 'POST') {
    refreshCandidates().then(() => {
      sendJson(nodeRes, 200, { ok: true, candidatesCount: candidates.length });
    }).catch(e => {
      sendJson(nodeRes, 500, { error: e.message });
    });
    return;
  }

  // –– API: Manually Allocate Slot ––
  if (pathname === '/api/slots/fill' && method === 'POST') {
    try {
      const body = JSON.parse(await readBody(nodeReq));
      const targetKey = body.key || API_KEY;
      const pool = await getKeySlotPool(targetKey);
      if (!pool) {
        sendJson(nodeRes, 503, { error: 'Unable to allocate slot' });
        return;
      }
      sendJson(nodeRes, 200, {
        ok: true,
        key: targetKey.slice(0, 7) + '...',
        slots: pool.slots.map(s => ({ addr: s.addr, latencyMs: s.latencyMs, grade: s.qualityGrade })),
      });
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // –– API: Proxy List ––
  if (pathname === '/api/proxies' && method === 'GET') {
    const list = candidates.map(c => ({
      address: c.address,
      protocol: c.protocol,
      quality_grade: c.quality_grade,
      latency: c.latency,
      lockedBy: c.lockedBy,
      active: !!c.lockedBy,
    }));
    sendJson(nodeRes, 200, { proxies: list, count: list.length });
    return;
  }

  // –– API: Batch Add Proxies ––
  if (pathname === '/api/proxies' && method === 'POST') {
    try {
      const body = JSON.parse(await readBody(nodeReq));
      const addrs: string[] = body.proxies || [];
      let added = 0;
      for (const addr of addrs) {
        const trimmed = addr.trim();
        if (!trimmed) continue;
        const isSocks = trimmed.startsWith('socks5://') || trimmed.startsWith('socks5h://');
        const cleanAddr = trimmed.replace(/^https?:\/\//, '').replace(/^socks5h?:\/\//, '');
        const ip = cleanAddr.split(':')[0];
        const country = geoip.lookup(ip)?.country || 'UNKNOWN';
        if (BLOCKED_COUNTRIES.has(country)) continue;
        if (!candidates.find(c => c.address === cleanAddr) && !customProxyItems.find(c => c.address === cleanAddr)) {
          const item: ProxyItem = { address: cleanAddr, protocol: isSocks ? 'socks5' : 'http', latency: 0, quality_grade: 'C', country };
          candidates.push({ ...item, lockedBy: null });
          customProxyItems.push(item);
          added++;
        }
      }
      if (added > 0) saveCustomProxies();
      sendJson(nodeRes, 200, { message: 'Added', count: added });
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // –– API: Delete Proxy ––
  const proxyDelMatch = pathname.match(/^\/api\/proxies\/(.+)$/);
  if (proxyDelMatch && method === 'DELETE') {
    const addr = decodeURIComponent(proxyDelMatch[1]);
    const idx = candidates.findIndex(c => c.address === addr);
    if (idx >= 0) {
      const locked = candidates[idx].lockedBy;
      if (locked) releaseKeySlots(locked);
      candidates.splice(idx, 1);
      // Remove from custom persistence
      const custIdx = customProxyItems.findIndex(c => c.address === addr);
      if (custIdx >= 0) {
        customProxyItems.splice(custIdx, 1);
        saveCustomProxies();
      }
      sendJson(nodeRes, 200, { message: 'Deleted' });
    } else {
      sendJson(nodeRes, 404, { error: 'not_found' });
    }
    return;
  }

  // –– API: Promote (move to head of queue) ––
  if (pathname === '/api/promote' && method === 'POST') {
    try {
      const body = JSON.parse(await readBody(nodeReq));
      const addr = body.addr;
      const idx = candidates.findIndex(c => c.address === addr);
      if (idx >= 0) {
        const [item] = candidates.splice(idx, 1);
        candidates.unshift(item);
        sendJson(nodeRes, 200, { message: 'Promoted', position: 0 });
      } else {
        sendJson(nodeRes, 200, { message: 'not_in_pool' });
      }
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // –– API: Proxy Source List ––
  if (pathname === '/api/sources' && method === 'GET') {
    // Per-feed count and error from the last fetch. Reporting the pool-wide
    // count and a hardcoded null error here made a timed-out feed
    // indistinguishable from a healthy one.
    const list = proxySources.map(s => {
      const h = sourceHealth.get(s.name);
      const breakerOpen = !!h && (h.openUntil || 0) > Date.now();
      return {
        name: s.name,
        type: s.type,
        count: h ? h.count : 0,
        error: h ? h.error : 'not fetched yet',
        fetchedAt: h ? h.fetchedAt : 0,
        strikes: h?.strikes || 0,
        breakerOpen,
        retryInMs: breakerOpen ? (h!.openUntil - Date.now()) : 0,
      };
    });
    const healthy = list.filter(s => s.error === null).length;
    const cooling = list.filter(s => s.breakerOpen).length;
    const now = Date.now();
    const exitsCooling = [...exitHealth.values()].filter(h => h.cooldownUntil > now).length;
    const pairsBanned = [...exitModelBans.values()]
      .reduce((n, m) => n + [...m.values()].filter(b => b.bannedUntil > now).length, 0);
    sendJson(nodeRes, 200, {
      sources: list,
      healthy,
      total: list.length,
      poolSize: candidates.filter(c => !c.lockedBy).length,
      feedsInCooldown: cooling,
      exitsInCooldown: exitsCooling,
      exitModelBans: pairsBanned,
    });
    return;
  }

  // –– API: Add Proxy Source ––
  if (pathname === '/api/sources' && method === 'POST') {
    try {
      const body = JSON.parse(await readBody(nodeReq));
      const name = body.name;
      if (!name || !body.url) {
        sendJson(nodeRes, 400, { error: 'name and url are required' });
        return;
      }
      if (proxySources.find(s => s.name === name)) {
        sendJson(nodeRes, 409, { error: 'Proxy source with this name already exists' });
        return;
      }
      const def = DEFAULT_SOURCES.find(d => d.name === name);
      proxySources.push({
        name, url: body.url,
        type: body.type || 'json',
        parser: def ? def.parser : DEFAULT_SOURCES[0].parser,
      });
      saveSources();
      sendJson(nodeRes, 200, { message: 'Added', name, url: body.url });
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // –– API: Delete Proxy Source ––
  const sourceDelMatch = pathname.match(/^\/api\/sources\/(.+)$/);
  if (sourceDelMatch && method === 'DELETE') {
    const name = decodeURIComponent(sourceDelMatch[1]);
    const idx = proxySources.findIndex(s => s.name === name);
    if (idx >= 0) {
      proxySources.splice(idx, 1);
      saveSources();
      sendJson(nodeRes, 200, { message: 'Deleted', name });
    } else {
      sendJson(nodeRes, 404, { error: 'not_found' });
    }
    return;
  }

  // –– API: Config (status/config) ––
  if (pathname === '/api/config' && method === 'GET') {
    sendJson(nodeRes, 200, {
      port: PORT,
      slotCount: SLOTS_PER_KEY,
      maxActiveKeys: MAX_ACTIVE_KEYS,
      warpMode: warpModeRuntime,
      warpHost: warpHostRuntime,
      warpPort: warpPortRuntime,
      warpStatus,
      proxyRefreshMs: PROXY_REFRESH_MS,
      proxyProbeTimeout: PROXY_PROBE_TIMEOUT,
    });
    return;
  }

  // –– API: Update Config ––
  if (pathname === '/api/config' && method === 'POST') {
    try {
      const body = JSON.parse(await readBody(nodeReq));
      if (body.warpMode !== undefined) {
        const oldMode = warpModeRuntime;
        warpModeRuntime = body.warpMode;
        warpConsecutiveFails = 0;
        warpSkipUntil = 0;
        if (body.warpMode === 'on') {
          probeWarp();
        } else {
          warpStatus = 'stopped';
          warpSlot = null;
          // Clean up WARP slots from all pools
          for (const [, pool] of keySlotPools) {
            const removeIdx = pool.slots.findIndex(s => s.addr === getWarpAddr());
            if (removeIdx >= 0) pool.slots.splice(removeIdx, 1);
          }
        }
        console.log(`[Config] WARP mode: ${oldMode} → ${body.warpMode}`);
      }
      sendJson(nodeRes, 200, { message: 'Configuration updated', warpMode: warpModeRuntime });
    } catch (e: any) {
      sendJson(nodeRes, 400, { error: e.message });
    }
    return;
  }

  // –– API: Load Candidate Pool ––
  if (pathname === '/api/candidates/load' && method === 'POST') {
    refreshCandidates().then(() => {
      sendJson(nodeRes, 200, { message: 'Refreshed', count: candidates.length });
    }).catch(e => {
      sendJson(nodeRes, 500, { error: e.message });
    });
    return;
  }

  // –– API: Refresh Proxy Sources (same as loading candidate pool) ––
  if (pathname === '/api/sources/refresh' && method === 'POST') {
    refreshCandidates().then(() => {
      sendJson(nodeRes, 200, { message: 'Refreshed', count: candidates.length });
    }).catch(e => {
      sendJson(nodeRes, 500, { error: e.message });
    });
    return;
  }

  // –– API: Daily Audit Details ––
  if (pathname === '/api/audit/daily' && method === 'GET') {
    const url = new URL(nodeReq.url || '', 'http://localhost');
    const date = url.searchParams.get('date') || new Date().toISOString().split('T')[0];
    const entries = auditLog
      .filter(log => {
        const logDate = new Date(log.ts || 0).toISOString().split('T')[0];
        return logDate === date;
      })
      .map(log => ({
        time: new Date(log.ts || 0).toLocaleTimeString(),
        model: log.model || 'unknown',
        promptTokens: log.promptTokens || 0,
        completionTokens: log.completionTokens || 0,
        totalTokens: log.totalTokens || 0,
        cacheRead: log.cacheRead || 0,
        latencyMs: log.latencyMs || 0,
        status: log.status || 0,
      }))
      .sort((a, b) => a.time.localeCompare(b.time));
    sendJson(nodeRes, 200, { entries });
    return;
  }

  // –– Proxy Forwarding (/v1/* | /openai/v1/*) ––
  if (pathname.startsWith('/v1/') || pathname.startsWith('/openai/v1/')) {
    // OpenAI compatible path → normalize to /v1/
    let upstreamPath = pathname.replace(/^\/openai/, '');
    // Extract authorization Key
    const authHeader = nodeReq.headers['authorization'] || '';
    const authKey = authHeader.replace(/^Bearer\s+/i, '');
    if (!authKey) {
      sendJson(nodeRes, 401, { error: 'unauthorized', message: 'Missing Authorization header' });
      return;
    }
    const kv = validateKey(authKey);
    if (!kv.ok) {
      sendJson(nodeRes, 403, { error: 'forbidden', message: kv.reason });
      return;
    }

    const startTime = Date.now();

    const isGeneration = upstreamPath === '/v1/chat/completions' || upstreamPath === '/v1/responses';
    if (!((upstreamPath === '/v1/models' && method === 'GET') || (isGeneration && method === 'POST'))) {
      sendJson(nodeRes, 404, { error: 'unsupported_endpoint', message: 'Use /v1/models, /v1/chat/completions or /v1/responses' });
      return;
    }
    let bodyStr: string | undefined;
    try {
      if (isGeneration) { await ensureModelCatalog(); bodyStr = await readBody(nodeReq); }
    } catch (e: any) {
      sendJson(nodeRes, 503, { error: 'model_catalog_unavailable', message: e?.message || String(e) });
      return;
    }
    if (isGeneration && !bodyStr) {
      sendJson(nodeRes, 400, { error: 'invalid_request_error', message: 'JSON request body required' });
      return;
    }
    let substitutedFrom: string | undefined;
    let substitutedTo: string | undefined;
    // Whether the caller asked for SSE. The agent shape forces stream:true
    // upstream, so a non-streaming caller is served by reassembling the SSE
    // back into a single JSON completion rather than being handed a stream.
    let callerWantsStream = false;
    if (bodyStr && (upstreamPath.includes('/chat/completions') || upstreamPath.includes('/responses'))) {
      const normalized = normalizeFreeModelAlias(bodyStr, upstreamPath === '/v1/responses' ? 'responses' : undefined);
      if (normalized.error) {
        sendJson(nodeRes, 400, { error: 'invalid_request_error', message: normalized.error });
        return;
      }
      bodyStr = normalized.body;
      if (upstreamPath === '/v1/responses' && !isResponsesOnlyModel(JSON.parse(bodyStr!).model)) {
        sendJson(nodeRes, 400, { error: 'unsupported_transport', message: 'This model uses /v1/chat/completions' });
        return;
      }
      substitutedFrom = normalized.substitutedFrom;
      substitutedTo = normalized.substitutedTo;
      if (substitutedFrom) {
        console.log(`[Models] ${substitutedFrom} is dead upstream, serving ${substitutedTo} instead`);
      }
      const shaped = shapeAgentRequest(bodyStr as string, upstreamPath === '/v1/responses' ? 'responses' : 'chat');
      bodyStr = shaped.body;
      callerWantsStream = shaped.callerWantsStream;
      if (shaped.reshaped) {
        console.log(`[AgentShape] ${callerWantsStream ? 'kept' : 'upgraded to'} streaming for the anonymous Zen lane`);
      }
    }

    // Models that upstream only serves over /v1/responses get rerouted there.
    // The request is translated to Responses shape, and the reply is translated
    // back to chat SSE, so callers keep using /v1/chat/completions unchanged.
    let usedResponses = false;
    let responsesFallbackModel = '';
    if (bodyStr && upstreamPath.includes('/chat/completions')) {
      let resolvedModel = '';
      try { resolvedModel = (JSON.parse(bodyStr) as any)?.model || ''; } catch {}
      if (isResponsesOnlyModel(resolvedModel)) {
        const translated = chatBodyToResponses(bodyStr);
        if (!translated) {
          sendJson(nodeRes, 400, { error: 'invalid_request_error', message: 'Chat messages cannot be translated to Responses' });
          return;
        }
        if (translated) {
          usedResponses = true;
          responsesFallbackModel = resolvedModel;
          upstreamPath = upstreamPath.replace('/chat/completions', '/responses');
          bodyStr = translated;
          console.log(`[ResponsesRoute] ${resolvedModel} is Responses-only upstream, rerouting to /v1/responses`);
        }
      }
    }
    const reqHeaders = collectHeadersFromReq(nodeReq, bodyStr);
    // Recheck immediately before reservation: catalog/body awaits above may
    // have admitted another request or allowed a key to expire/be disabled.
    const admission = validateKey(authKey);
    if (!admission.ok) {
      sendJson(nodeRes, 403, { error: 'forbidden', message: admission.reason });
      return;
    }
    acquireKey(authKey);
    if (isGeneration) recordKeyRequest(authKey);

    // Catalog requests use no proxy slots; aliases share canonical health.
    if (upstreamPath === '/v1/models' && method === 'GET') {
      try {
        let list = cachedModels;
        if (cachedModelsTime === 0 || Date.now() - cachedModelsTime >= MODEL_CATALOG_TTL_MS) {
          list = await fetchModelsFromUpstream();
        }
        const callables = workingFreeModelIds()
          .map(id => list.find(m => m.id === id))
          .filter(Boolean);
        // Before the first verification pass there are no verdicts, so the full
        // list is still the best answer available — do not filter on nothing.
        const advertised = (modelsVerified || freeModelHealth.size > 0) ? callables : list;
        const withheld = list
          .filter(m => !advertised.some(a => a.id === m.id))
          .map(m => {
            const h = getModelHealth(m.id);
            return { id: m.id, status: h?.status ?? 0, reason: h?.reason || 'unverified' };
          });
        sendJson(nodeRes, 200, {
          object: 'list',
          data: advertised,
          catalog: catalogStatus(),
          ...(withheld.length ? { withheld_unavailable: withheld } : {}),
        });
        releaseKey(authKey);
        return;
      } catch (e: any) {
        sendJson(nodeRes, 502, { error: e.message });
        releaseKey(authKey);
        return;
      }
    }

    try {
      // Get or create Key slot pool. When no proxy slot can be allocated
      // (all public candidates dead), fall through to the direct-connection
      // fallback inside dispatch() instead of 503 — direct works fine and
      // a failed proxy must not make the gateway unavailable.
      let pool = await getKeySlotPool(authKey);
      if (!pool) {
        console.log(`[Allocate] Key ${authKey.slice(0,7)}... No proxy slot, using direct fallback`);
        pool = { keyId: authKey, slots: [], rrCursor: 0, lastUsedAt: Date.now() };
      }

      const result = await dispatch(upstreamPath + search, method, reqHeaders, bodyStr, pool);

      // A Responses-only model asked for over /chat/completions is served by
      // /v1/responses instead, then translated back into chat SSE so the
      // caller's request shape is unchanged.
      if (usedResponses && result.stream) {
        result.stream = responsesSseToChatSse(result.stream, responsesFallbackModel);
        result.streamHeaders = { ...(result.streamHeaders || {}), 'content-type': 'text/event-stream; charset=utf-8' };
      }

      if (result.stream) {
        // The agent shape forces stream:true upstream. When the caller asked
        // for a single JSON completion, reassemble the SSE into one object
        // rather than leaking an event stream to a non-streaming client.
        if (!callerWantsStream) {
          const requestedModel = (() => {
            try { return (JSON.parse(bodyStr || '{}') as any)?.model || ''; } catch { return ''; }
          })();
          const collected = upstreamPath === '/v1/responses' && !usedResponses
            ? await collectResponsesStream(result.stream)
            : await collectChatStream(result.stream, substitutedTo || requestedModel);
          const usage = extractUsageFromResponse(collected.body);
          if (usage.tokens > 0) recordKeyUsage(authKey, usage.tokens);
          nodeRes.writeHead(collected.status, {
            'content-type': 'application/json',
            'access-control-allow-origin': '*',
            ...(substitutedTo
              ? { 'x-opencode2api-substituted-model': `${substitutedFrom} -> ${substitutedTo}` }
              : {}),
          });
          nodeRes.end(collected.body);
          return;
        }
        // Streaming response: clean hop-by-hop & conflicting headers from upstream
        const cleanHeaders: Record<string, string> = {};
        if (result.streamHeaders) {
          for (const [k, v] of Object.entries(result.streamHeaders)) {
            const lk = k.toLowerCase();
            if (lk === 'connection' || lk === 'transfer-encoding' || lk === 'content-length' || lk === 'content-encoding') continue;
            cleanHeaders[k] = v;
          }
        }
        nodeRes.writeHead(result.status, {
          ...cleanHeaders,
          'content-type': cleanHeaders['content-type'] || 'text/event-stream; charset=utf-8',
          'cache-control': 'no-cache, no-transform',
          'connection': 'keep-alive',
          'x-accel-buffering': 'no',
          'access-control-allow-origin': '*',
          ...(substitutedTo
            ? { 'x-opencode2api-substituted-model': `${substitutedFrom} -> ${substitutedTo}` }
            : {}),
        });
        const reader = result.stream.getReader();
        let clientClosed = false;
        nodeReq.on('close', () => {
          clientClosed = true;
          reader.cancel().catch(() => {});
        });
        let streamUsageBuffer = '';
        let streamTokens = 0;
        const streamDecoder = new TextDecoder();
        try {
          while (!clientClosed) {
            const { done, value } = await reader.read();
            if (done) break;
            nodeRes.write(value);
            if (typeof (nodeRes as any).flush === 'function') {
              (nodeRes as any).flush();
            }
            if (value && value.length > 0) {
              streamUsageBuffer += streamDecoder.decode(value, { stream: true });
              const lines = streamUsageBuffer.split(/\r?\n/);
              streamUsageBuffer = lines.pop() || '';
              for (const line of lines) {
                if (!line.startsWith('data:')) continue;
                try {
                  const evt = JSON.parse(line.slice(5).trim());
                  const usage = evt.usage || evt.response?.usage;
                  if (usage) streamTokens = usage.total_tokens ?? ((usage.input_tokens || 0) + (usage.output_tokens || 0));
                } catch {}
              }
            }
          }
        } catch (err: any) {
          console.warn(`[Dispatch] Stream read interrupted:`, err?.message || err);
        } finally {
          nodeRes.end();
          if (streamTokens > 0) recordKeyUsage(authKey, streamTokens);
        }
      } else {
        // Standard response. This is the single place token accounting happens:
        // it is the only site that sees the final response for every path
        // (per-key slot, WARP, ZenProxy relay, direct fallback). Recording in
        // dispatch() as well double-counted every request, and left relay
        // requests — which never reached those branches — recorded at zero.
        const respBody = result.body || '{}';
        const usage = extractUsageFromResponse(respBody);
        if (usage.tokens > 0) recordKeyUsage(authKey, usage.tokens);
        nodeRes.writeHead(result.status, {
          'content-type': 'application/json',
          'access-control-allow-origin': '*',
          ...(substitutedTo
            ? { 'x-opencode2api-substituted-model': `${substitutedFrom} -> ${substitutedTo}` }
            : {}),
        });
        nodeRes.end(respBody);
      }
    } catch (e: any) {
      console.error(`[Request] Exception: ${e.message}`);
      sendJson(nodeRes, 502, { error: 'gateway_error', message: e.message });
    } finally {
      releaseKey(authKey);
    }
    return;
  }

  // –– 404 ––
  sendJson(nodeRes, 404, { error: 'not_found' });
});

// ═══════════════════════════════════════════════════════════
//  Scheduled Tasks
// ═══════════════════════════════════════════════════════════

// Periodically refresh candidate pool
setInterval(() => {
  refreshCandidates();
}, PROXY_REFRESH_MS);

// Continuously re-validate exits. Allocation-time probing alone cannot see a
// proxy that died after it was handed to a key, so a live pool would keep
// handing out dead exits until real traffic failed on them.
setInterval(() => {
  backgroundProbeSweep();
}, PROBER_INTERVAL_MS);

// Periodically clean up expired/idle Key Slot Pools
setInterval(() => {
  const now = Date.now();
  for (const [keyId, pool] of keySlotPools) {
    const record = apiKeys[keyId];
    if (!record || !record.enabled ||
        (record.expiresAt > 0 && now > record.expiresAt) ||
        (record.maxRequests > 0 && record.requestCount >= record.maxRequests)) {
      releaseKeySlots(keyId);
      continue;
    }
    if (now - pool.lastUsedAt > KEY_IDLE_RELEASE_MS) {
      releaseKeySlots(keyId);
      console.log(`[Release] Key ${keyId.slice(0,7)}... Idle timeout released`);
    }
  }
}, POOL_CLEANUP_MS);

// Automatic Background Scraper (ProxyHub / public feeds)
const AUTO_SCRAPE_HOURS = parseFloat(process.env.AUTO_SCRAPE_HOURS || '4');
const SCRAPER_SCRIPT = path.join(process.cwd(), 'scripts', 'push_proxyhub.py');

function runBackgroundScraper() {
  if (!fs.existsSync(SCRAPER_SCRIPT)) return;
  console.log('[AutoScraper] Starting scheduled ProxyHub scraping in background...');
  try {
  // Scrape deeper than the default: proxyhub.me's first pages are static and
  // dedup makes re-scraping them a no-op (Added 0). Deeper pages rotate.
    const proc = spawn('python3', [SCRAPER_SCRIPT, '25'], {
      env: { ...process.env, GATE_URL: `http://127.0.0.1:${PORT}/api/proxies` },
      stdio: 'ignore'
    });
    proc.on('close', (code) => {
      console.log(`[AutoScraper] Background ProxyHub crawler finished (exit code ${code})`);
    });
  } catch (e: any) {
    console.error(`[AutoScraper] Failed to spawn scraper: ${e.message}`);
  }
}

if (AUTO_SCRAPE_HOURS > 0) {
  const scrapeMs = Math.round(AUTO_SCRAPE_HOURS * 3600 * 1000);
  // Initial run after 20 seconds
  setTimeout(runBackgroundScraper, 20000);
  // Recurring interval
  setInterval(runBackgroundScraper, scrapeMs);
}

// ═══════════════════════════════════════════════════════════
//  Startup
// ═══════════════════════════════════════════════════════════

async function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  OpenCode2API — Per-Key IP Pool Reverse Proxy Gateway');
  console.log('═══════════════════════════════════════════════════');

  // DATA_DIR is often a fresh mount, so create it before anything writes.
  fs.mkdirSync(DATA_DIR, { recursive: true });
  console.log(`[Startup] State directory: ${DATA_DIR}`);

  // Load keys
  loadKeys();

  // Load proxy source config
  loadSources();

  // Load custom persisted proxies
  loadCustomProxies();

  // Load historical audit logs (last 500 records)
  loadAuditLog();

  // Load candidate proxies
  console.log('[Startup] Loading candidate proxies...');
  await loadCandidates();

  // Probe WARP
  if (warpModeRuntime === 'on') {
    console.log('[Startup] Probing WARP...');
    await probeWarp();
  }

  // Initialize custom proxies
  await initCustomSlots();

  // Re-validate exits shortly after boot so the first allocation already knows
  // which proxies are alive instead of discovering it one request at a time.
  setTimeout(() => { backgroundProbeSweep(); }, 20_000);

  loadModelCatalog();

  // Discover free models and establish which are actually callable. Runs
  // before the listener comes up so the first caller does not race an
  // unverified list and get routed onto a known-dead model.
  try {
    await fetchModelsFromUpstream();
    await verifyFreeModels();
  } catch (e: any) {
    console.warn(`[Startup] Model discovery failed: ${e?.message || e}`);
  }
  setInterval(verifyFreeModels, MODEL_VERIFY_INTERVAL_MS);

  // Start HTTP server
  server.listen(PORT, () => {
    console.log(`[Startup] Listening on port ${PORT}`);
    console.log(`[Startup] Candidate proxies: ${candidates.length} items`);
    console.log(`[Startup] Fallback proxies: ${customSlots.length} items`);
    console.log(`[Startup] WARP: ${warpStatus}`);
    console.log(`[Startup] Max active keys: ${MAX_ACTIVE_KEYS}`);
    console.log(`[Startup] Slots per key: ${SLOTS_PER_KEY}`);
    console.log('═══════════════════════════════════════════════════');
  });
}

main().catch(e => {
  console.error('[Startup] Fatal error:', e);
  process.exit(1);
});

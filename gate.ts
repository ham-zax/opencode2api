#!/usr/bin/env bun 

/**
 * OpenCode2API — SingBox Reverse Proxy Gateway
 * Uses sing-box subscription nodes + automatic 429 rotation + direct connection fallback
 */

import https from 'node:https';
import http from 'node:http';
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import net from 'node:net';

// ═══════════════════════════════════════════════════════════
//  Type Definitions
// ═══════════════════════════════════════════════════════════

interface ApiKeyRecord {
  key: string; name: string; enabled: boolean;
  createdAt: number; lastUsedAt: number;
  totalRequests: number; totalTokens: number;
  maxConcurrency: number; maxRequests: number;
  requestCount: number; expiresAt: number;
}

interface AuditEntry {
  ts: number; keyId: string; model: string;
  promptTokens: number; completionTokens: number; totalTokens: number;
  cacheCreation: number; cacheRead: number;
  latencyMs: number; status: number;
}

// ═══════════════════════════════════════════════════════════
//  Persistence File Paths
// ═══════════════════════════════════════════════════════════

const DATA_DIR = process.env.DATA_DIR || process.cwd();
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}

const KEYS_FILE = path.join(DATA_DIR, 'keys.json');
const AUDIT_FILE = path.join(DATA_DIR, 'audit.jsonl');
const MODELS_CACHE_FILE = path.join(DATA_DIR, 'models_cache.json');
const SINGBOX_CONFIG_DIR = path.join(process.cwd(), 'singbox');
const SUBSCRIPTION_FILE = path.join(DATA_DIR, 'subscription.json');

// ═══════════════════════════════════════════════════════════
//  Constants
// ═══════════════════════════════════════════════════════════

const UPSTREAM = 'https://opencode.ai/zen';
const PORT = parseInt(process.env.PORT || '13339');
const MAX_RETRIES = 3;
const TIMEOUT = 15000;
const STREAM_TIMEOUT = 300000;

// SingBox Configuration
const SINGBOX_HOST = process.env.SINGBOX_HOST || '127.0.0.1';
const SINGBOX_HTTP_PORT = parseInt(process.env.SINGBOX_HTTP_PORT || '10800');
const SINGBOX_SOCKS_PORT = parseInt(process.env.SINGBOX_SOCKS_PORT || '10801');
const SINGBOX_API_PORT = parseInt(process.env.SINGBOX_API_PORT || '9090');
const SINGBOX_MODE = process.env.SINGBOX_MODE || 'on';

const SINGBOX_SOCKS_URL = `socks5h://${SINGBOX_HOST}:${SINGBOX_SOCKS_PORT}`;
const SINGBOX_API_URL = `http://${SINGBOX_HOST}:${SINGBOX_API_PORT}`;

const API_KEY = process.env.API_KEY || 'admin123';
const START_TIME = Date.now();

// ═══════════════════════════════════════════════════════════
//  Global State
// ═══════════════════════════════════════════════════════════

let apiKeys: Record<string, ApiKeyRecord> = {};
let activeRequests: Record<string, number> = {};
let cachedModels: any[] = [];
let cachedModelsTime = 0;
let stats = { total: 0, success: 0, rateLimited: 0, errors: 0 };
let singboxNodeIndex = 0;
let singboxNodes: string[] = [];
let singboxOk = false;

const recentLogs: string[] = [];
const MAX_LOGS = 500;
const auditLog: AuditEntry[] = [];
const MAX_AUDIT = 10000;

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
  logCapture(msg); _origLog.apply(console, args);
};
const _origError = console.error;
console.error = (...args: any[]) => {
  const msg = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  logCapture(`❌ ${msg}`); _origError.apply(console, args);
};

// ═══════════════════════════════════════════════════════════
//  Upstream Header Whitelist (compatible with legacy opencode-gate behavior)
//  Only forward these headers to prevent sending dirty client headers (UA, accept-encoding, etc.) upstream
// ═══════════════════════════════════════════════════════════

const FORWARD = [
  'authorization', 'x-opencode-project', 'x-opencode-session',
  'x-opencode-request', 'x-opencode-client', 'content-type',
  'accept', 'anthropic-version', 'anthropic-beta',
];

function collectHeadersFromReq(nodeReq: http.IncomingMessage): Record<string, string> {
  const h: Record<string, string> = {};
  for (const k of FORWARD) {
    if (k === 'authorization') continue;
    const v = nodeReq.headers[k];
    if (v) h[k] = Array.isArray(v) ? v[0] : v;
  }
  h['authorization'] = 'Bearer public';
  if (!h['x-opencode-client']) h['x-opencode-client'] = 'desktop';
  if (!h['x-opencode-session']) h['x-opencode-session'] = crypto.randomUUID();
  if (!h['x-opencode-project']) h['x-opencode-project'] = crypto.randomUUID();
  if (!h['content-type']) h['content-type'] = 'application/json';
  h['user-agent'] = 'opencode';
  return h;
}

// ═══════════════════════════════════════════════════════════
//  SingBox Management
// ═══════════════════════════════════════════════════════════

function loadSingboxNodes() {
  try {
    const nodesFile = path.join(SINGBOX_CONFIG_DIR, 'nodes.json');
    if (!fs.existsSync(nodesFile)) {
      singboxNodes = [];
      singboxNodeIndex = 0;
      return;
    }
    const data = JSON.parse(fs.readFileSync(nodesFile, 'utf-8'));
    singboxNodes = data.nodes || [];
    singboxNodeIndex = 0;
    console.log(`[SingBox] Loaded ${singboxNodes.length} nodes`);
  } catch (e: any) {
    console.error(`[SingBox] Failed to load nodes: ${e.message}`);
    singboxNodes = [];
  }
}

async function initSingboxNode(): Promise<void> {
  if (singboxNodes.length === 0) return;
  const {SocksProxyAgent} = await import('socks-proxy-agent');
  const httpsMod = await import('node:https');
  // Test nodes one by one to find the first one that connects to opencode.ai
  const getRes = await fetch(`${SINGBOX_API_URL}/proxies/manual`, { signal: AbortSignal.timeout(3000) }).catch(() => null);
  const all = getRes && getRes.ok ? (await getRes.json() as any).all || [] : singboxNodes;
  const maxTest = Math.min(all.length, 60);
  for (let i = 0; i < maxTest; i++) {
    const node = all[i];
    try {
      await fetch(`${SINGBOX_API_URL}/proxies/manual`, {
        method: 'PUT', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: node }),
        signal: AbortSignal.timeout(3000),
      });
      await new Promise(r => setTimeout(r, 150));
    } catch {}
    const agent = new SocksProxyAgent(`socks5h://${SINGBOX_HOST}:${SINGBOX_SOCKS_PORT}`, { timeout: 8000 }) as unknown as https.Agent;
    try {
      const ok = await new Promise<boolean>((resolve) => {
        const req = httpsMod.request('https://opencode.ai/zen/v1/models', {
          headers: { 'authorization': 'Bearer public', 'x-opencode-client': 'desktop' },
          agent, rejectUnauthorized: false, signal: AbortSignal.timeout(6000),
        }, (r) => { resolve(r.statusCode === 200); });
        req.on('error', () => resolve(false));
        req.end();
      });
      if (ok) {
        singboxOk = true;
        console.log(`[SingBox] Initialized to working node: ${node} (index ${i}/${all.length})`);
        return;
      }
    } catch {}
  }
  singboxOk = false;
  console.warn('[SingBox] First 60 nodes are unavailable, falling back to direct connection');
}

async function checkSingboxHealth(): Promise<boolean> {
  if (SINGBOX_MODE !== 'on') { singboxOk = false; return false; }
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    const res = await fetch(`${SINGBOX_API_URL}/proxies`, { signal: controller.signal });
    clearTimeout(timer);
    singboxOk = res.ok;
    return res.ok;
  } catch {
    singboxOk = false;
    return false;
  }
}

async function switchSingboxNode(tried: Set<string> = new Set()): Promise<string | null> {
  if (SINGBOX_MODE !== 'on') return null;
  try {
    // Get all nodes and currently selected node from manual selector
    const getRes = await fetch(`${SINGBOX_API_URL}/proxies/manual`, { signal: AbortSignal.timeout(3000) });
    if (!getRes.ok) return null;
    const data = await getRes.json() as any;
    const all = data.all || [];
    const now = data.now || '';
    if (all.length === 0) return null;
    // Sequentially search for next untried node
    const startIdx = all.indexOf(now);
    for (let i = 1; i <= all.length; i++) {
      const idx = (startIdx + i) % all.length;
      const node = all[idx];
      if (tried.has(node)) continue;
      const putRes = await fetch(`${SINGBOX_API_URL}/proxies/manual`, {
        method: 'PUT', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: node }),
        signal: AbortSignal.timeout(3000),
      });
      if (putRes.ok) {
        console.log(`[SingBox] Switched node → ${node} (${idx}/${all.length})`);
        return node;
      }
    }
    return null;
  } catch (e: any) {
    console.warn(`[SingBox] Node switch error: ${e.message}`);
    return null;
  }
}

async function reloadSingboxConfig(): Promise<boolean> {
  if (SINGBOX_MODE !== 'on') return false;
  try {
    // Restart opengate-singbox container via Docker socket
    const sockPath = '/var/run/docker.sock';
    if (!fs.existsSync(sockPath)) {
      console.warn('[SingBox] Docker socket unavailable, skipping reload');
      return false;
    }
    await new Promise<void>((resolve, reject) => {
      const client = net.createConnection(sockPath, () => {
        client.write(
          'POST /containers/opengate-singbox/restart HTTP/1.1\r\n' +
          'Host: localhost\r\n' +
          'Content-Length: 0\r\n' +
          '\r\n'
        );
      });
      let resp = '';
      client.on('data', (chunk) => { resp += chunk.toString(); });
      client.on('end', () => {
        if (resp.includes('204') || resp.includes('200')) resolve();
        else reject(new Error(resp.split('\r\n')[0]));
      });
      client.on('error', reject);
      client.setTimeout(10000, () => { client.destroy(); reject(new Error('timeout')); });
    });
    console.log('[SingBox] Configuration reloaded, container restarted');
    // Wait for sing-box to start
    await new Promise(resolve => setTimeout(resolve, 3000));
    await checkSingboxHealth();
    loadSingboxNodes();
    return true;
  } catch (e: any) {
    console.error(`[SingBox] Reload failed: ${e.message}`);
    return false;
  }
}

// ═══════════════════════════════════════════════════════════
//  Subscription Management
// ═══════════════════════════════════════════════════════════

interface SubscriptionConfig {
  url: string;
  token: string;
  updatedAt: number;
}

function loadSubscription(): SubscriptionConfig | null {
  try {
    if (!fs.existsSync(SUBSCRIPTION_FILE)) return null;
    return JSON.parse(fs.readFileSync(SUBSCRIPTION_FILE, 'utf-8'));
  } catch { return null; }
}

function saveSubscription(sub: SubscriptionConfig) {
  fs.writeFileSync(SUBSCRIPTION_FILE, JSON.stringify(sub, null, 2), 'utf-8');
}

// Generate sing-box configuration (reuses vless parsing logic from glm-proxy)
async function generateSingboxConfig(sub: SubscriptionConfig): Promise<number> {
  // Fetch subscription
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);
  let raw: string;
  try {
    const res = await fetch(sub.url, {
      headers: { 'user-agent': 'curl/8.0' },
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`Subscription fetch failed HTTP ${res.status}`);
    raw = await res.text();
  } catch (e: any) {
    clearTimeout(timer);
    throw new Error(`Subscription fetch error: ${e.message}`);
  }
  clearTimeout(timer);

  // base64 decode
  let decoded = '';
  try {
    const normalized = raw.replace(/\s+/g, '');
    decoded = Buffer.from(normalized, 'base64').toString('utf-8');
    if (!decoded.trim().startsWith('vless://')) throw new Error('not vless');
  } catch {
    decoded = raw;
  }

  // Parse vless:// lines
  const lines = decoded.split('\n').map(l => l.trim()).filter(Boolean);
  let outbounds: any[] = [];
  const seen = new Set<string>();
  for (const line of lines) {
    if (!line.startsWith('vless://')) continue;
    const ob = parseVless(line);
    if (ob && !seen.has(ob['tag'])) {
      seen.add(ob['tag']);
      outbounds.push(ob);
    }
  }
  if (outbounds.length === 0) throw new Error('No vless nodes found in subscription');

  // Trim node count to reduce urltest probe overhead on upstream/CF quota
  const MAX_SINGBOX_NODES = 50;
  if (outbounds.length > MAX_SINGBOX_NODES) {
    console.log(`[SingBox] Node count ${outbounds.length} exceeds limit ${MAX_SINGBOX_NODES}, trimming...`);
    outbounds = outbounds.slice(0, MAX_SINGBOX_NODES);
  }

  const nodeTags = outbounds.map(o => o['tag']);
  const config = {
    log: { level: 'warn' as const, timestamp: true },
    inbounds: [
      { type: 'http' as const, tag: 'http-in', listen: '0.0.0.0', listen_port: SINGBOX_HTTP_PORT },
      { type: 'socks' as const, tag: 'socks-in', listen: '0.0.0.0', listen_port: SINGBOX_SOCKS_PORT },
    ],
    outbounds: [
      { type: 'selector' as const, tag: 'manual', outbounds: nodeTags, default: nodeTags[0] },
      { type: 'urltest' as const, tag: 'auto', outbounds: nodeTags,
        url: 'https://opencode.ai/zen/v1/models', interval: '40m', tolerance: 100, idle_timeout: '60m' },
      ...outbounds,
      { type: 'direct' as const, tag: 'direct' },
      { type: 'block' as const, tag: 'block' },
    ],
    route: {
      rules: [{ inbound: ['http-in', 'socks-in'], outbound: 'auto' }],
      final: 'auto' as const,
    },
    experimental: {
      clash_api: {
        external_controller: `0.0.0.0:${SINGBOX_API_PORT}`,
        external_ui: '',
        secret: '',
        default_mode: 'rule' as const,
      },
    },
  };

  fs.mkdirSync(SINGBOX_CONFIG_DIR, { recursive: true });
  fs.writeFileSync(path.join(SINGBOX_CONFIG_DIR, 'singbox_config.json'), JSON.stringify(config, null, 2), 'utf-8');
  fs.writeFileSync(path.join(SINGBOX_CONFIG_DIR, 'nodes.json'), JSON.stringify({ nodes: nodeTags, count: nodeTags.length }), 'utf-8');
  return nodeTags.length;
}

function parseVless(uri: string): any {
  const body = uri.slice('vless://'.length);
  const withHash = body.split('#', 1)[0] || body;
  const at = withHash.lastIndexOf('@');
  if (at === -1) return null;
  const uuid = withHash.slice(0, at);
  let rest = withHash.slice(at + 1);
  let query = '';
  if (rest.includes('?')) { const i = rest.indexOf('?'); query = rest.slice(i + 1); rest = rest.slice(0, i); }
  const params = Object.fromEntries(new URLSearchParams(query));
  const hostPort = rest.split('?')[0];
  const lastColon = hostPort.lastIndexOf(':');
  const host = hostPort.slice(0, lastColon);
  const port = parseInt(hostPort.slice(lastColon + 1), 10);
  if (!host || isNaN(port)) return null;
  return {
    type: 'vless', tag: `n-${host}-${port}`,
    server: host, server_port: port, uuid,
    tls: {
      enabled: params['security'] === 'tls',
      server_name: params['sni'] || params['host'] || host,
      utls: { enabled: true, fingerprint: params['fp'] || 'chrome' },
    },
    transport: { type: 'ws', path: params['path'] || '/', headers: { Host: params['host'] || host } },
  };
}

// ═══════════════════════════════════════════════════════════
//  Key Management
// ═══════════════════════════════════════════════════════════

function loadKeys() {
  try {
    if (!fs.existsSync(KEYS_FILE)) {
      apiKeys = {};
      // Default key
      const defaultKey = 'sk-default';
      apiKeys[defaultKey] = {
        key: defaultKey, name: 'default', enabled: true,
        createdAt: Date.now(), lastUsedAt: 0,
        totalRequests: 0, totalTokens: 0,
        maxConcurrency: 5, maxRequests: 1000000,
        requestCount: 0, expiresAt: Date.now() + 365 * 86400000,
      };
      saveKeys();
      console.log('[Key] Default key created');
      return;
    }
    apiKeys = JSON.parse(fs.readFileSync(KEYS_FILE, 'utf-8'));
    console.log(`[Key] Loaded ${Object.keys(apiKeys).length} keys`);
  } catch (e: any) {
    console.error(`[Key] Load failed: ${e.message}`);
    apiKeys = {};
  }
}

function saveKeys() {
  try {
    fs.writeFileSync(KEYS_FILE, JSON.stringify(apiKeys, null, 2), 'utf-8');
  } catch (e: any) {
    console.error(`[Key] Save failed: ${e.message}`);
  }
}

function validateKey(key: string): { valid: boolean; record?: ApiKeyRecord; reason?: string } {
  const record = apiKeys[key];
  if (!record) return { valid: false, reason: 'Key not found' };
  if (!record.enabled) return { valid: false, reason: 'Key is disabled' };
  if (record.expiresAt !== 0 && Date.now() > record.expiresAt) return { valid: false, reason: 'Key is expired' };
  if (record.maxRequests !== 0 && record.requestCount >= record.maxRequests) return { valid: false, reason: 'Request limit reached' };
  const current = activeRequests[key] || 0;
  if (record.maxConcurrency !== 0 && current >= record.maxConcurrency) return { valid: false, reason: 'Concurrency limit reached' };
  return { valid: true, record };
}

function acquireKey(key: string) {
  activeRequests[key] = (activeRequests[key] || 0) + 1;
}

function releaseKey(key: string) {
  if (activeRequests[key] > 0) activeRequests[key]--;
}

function recordKeyUsage(key: string, tokens: number) {
  const record = apiKeys[key];
  if (record) {
    record.totalRequests++;
    record.totalTokens += tokens;
    record.requestCount++;
    record.lastUsedAt = Date.now();
    saveKeys();
  }
}

// ═══════════════════════════════════════════════════════════
//  Audit Logging
// ═══════════════════════════════════════════════════════════

function audit(status: number, latencyMs: number, keyId: string, path: string, body?: string) {
  let model = '';
  let promptTokens = 0, completionTokens = 0, totalTokens = 0;
  let cacheCreation = 0, cacheRead = 0;
  try {
    if (body) {
      const parsed = JSON.parse(body);
      model = parsed.model || '';
      if (parsed.usage) {
        promptTokens = parsed.usage.prompt_tokens || 0;
        completionTokens = parsed.usage.completion_tokens || 0;
        totalTokens = parsed.usage.total_tokens || 0;
        cacheRead = parsed.usage.prompt_cache_hit_tokens || 0;
      }
    }
  } catch {}
  const entry: AuditEntry = {
    ts: Date.now(), keyId, model, promptTokens, completionTokens, totalTokens,
    cacheCreation, cacheRead, latencyMs, status,
  };
  auditLog.push(entry);
  if (auditLog.length > MAX_AUDIT) auditLog.shift();
  fs.appendFileSync(AUDIT_FILE, JSON.stringify(entry) + '\n');
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
    console.log(`[Audit] Loaded ${auditLog.length} historical records`);
  } catch (e: any) {
    console.error(`[Audit] Load failed: ${e.message}`);
  }
}

// ═══════════════════════════════════════════════════════════
//  Model Cache
// ═══════════════════════════════════════════════════════════

async function fetchModelsFromUpstream(): Promise<any[]> {
  try {
    const res = await fetch(`${UPSTREAM}/v1/models`, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) return [];
    const data = await res.json() as any;
    const models = data.data || data.models || [];
    cachedModels = models;
    cachedModelsTime = Date.now();
    saveModelsCache();
    return models;
  } catch {
    return cachedModels;
  }
}

function saveModelsCache() {
  try {
    fs.writeFileSync(MODELS_CACHE_FILE, JSON.stringify({ models: cachedModels, time: cachedModelsTime }, null, 2), 'utf-8');
  } catch {}
}

function loadModelsCache() {
  try {
    if (!fs.existsSync(MODELS_CACHE_FILE)) return false;
    const data = JSON.parse(fs.readFileSync(MODELS_CACHE_FILE, 'utf-8'));
    if (data.models) {
      cachedModels = data.models;
      cachedModelsTime = data.time || 0;
      return true;
    }
    return false;
  } catch { return false; }
}

// ═══════════════════════════════════════════════════════════
//  Forwarding (doHttps / doHttpsStream)
// ═══════════════════════════════════════════════════════════

function doHttps(
  path: string, method: string, headers: Record<string, string>,
  body: string | undefined, agent?: https.Agent,
): Promise<{ status: number; body: string }> {
  const { authorization, Authorization, host, Host, ...cleanHeaders } = headers;
  cleanHeaders['authorization'] = 'Bearer public';
  cleanHeaders['x-opencode-client'] = 'desktop';
  delete cleanHeaders['content-length'];
  delete cleanHeaders['transfer-encoding'];
  delete cleanHeaders['connection'];
  delete cleanHeaders['user-agent'];
  delete cleanHeaders['accept-encoding'];
  delete cleanHeaders['host'];
  return new Promise((resolve, reject) => {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), TIMEOUT);
    const opts: any = { method, headers: cleanHeaders, signal: ac.signal, rejectUnauthorized: false };
    if (agent) opts.agent = agent;
    const req = https.request(`${UPSTREAM}${path}`, opts, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c: Buffer) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode || 200, body: Buffer.concat(chunks).toString('utf-8') }));
      res.on('error', reject);
    });
    req.on('error', (e) => { clearTimeout(timer); reject(e); });
    if (body) req.write(body);
    req.end();
  });
}

function doHttpsStream(
  path: string, method: string, headers: Record<string, string>,
  body: string | undefined, agent?: https.Agent,
): Promise<{ status: number; stream: ReadableStream<Uint8Array>; headers: Record<string, string> }> {
  const { authorization, Authorization, host, Host, ...cleanHeaders } = headers;
  cleanHeaders['authorization'] = 'Bearer public';
  cleanHeaders['x-opencode-client'] = 'desktop';
  delete cleanHeaders['content-length'];
  delete cleanHeaders['transfer-encoding'];
  delete cleanHeaders['connection'];
  delete cleanHeaders['user-agent'];
  delete cleanHeaders['accept-encoding'];
  delete cleanHeaders['host'];
  return new Promise((resolve, reject) => {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), STREAM_TIMEOUT);
    const opts: any = { method, headers: cleanHeaders, signal: ac.signal, rejectUnauthorized: false };
    if (agent) opts.agent = agent;
    const req = https.request(`${UPSTREAM}${path}`, opts, (res) => {
      clearTimeout(timer);
      const resHeaders: Record<string, string> = {};
      for (const [k, v] of Object.entries(res.headers)) {
        if (v) resHeaders[k] = Array.isArray(v) ? v[0] : v;
      }
      res.on('end', () => {});
      res.on('error', () => {});
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          res.on('data', (chunk: Buffer) => controller.enqueue(new Uint8Array(chunk)));
          res.on('end', () => { try { controller.close(); } catch {} });
          res.on('error', (e: Error) => { try { controller.error(e); } catch {} });
        },
      });
      resolve({ status: res.statusCode || 200, stream, headers: resHeaders });
    });
    req.on('error', (e) => { clearTimeout(timer); reject(e); });
    if (body) req.write(body);
    req.end();
  });
}

// ═══════════════════════════════════════════════════════════
//  SingBox Outbound Scheduling
// ═══════════════════════════════════════════════════════════

async function getSingboxAgent(): Promise<https.Agent | undefined> {
  if (SINGBOX_MODE !== 'on' || !singboxOk) return undefined;
  try {
    const { SocksProxyAgent } = await import('socks-proxy-agent');
    return new SocksProxyAgent(SINGBOX_SOCKS_URL, { timeout: TIMEOUT }) as unknown as https.Agent;
  } catch {
    return undefined;
  }
}

// ═══════════════════════════════════════════════════════════
//  Main Request Dispatch
// ═══════════════════════════════════════════════════════════

// Determine if traffic should route through sing-box proxy (excluding direct paths)
function shouldUseProxy(url: string | undefined): boolean {
  if (SINGBOX_MODE !== 'on') return false;
  if (!url) return true;
  const directHosts = ['127.0.0.1', 'localhost', '192.168.', '10.', '172.16.', '172.17.', '172.18.', '172.19.'];
  const host = (() => { try { return new URL(url).hostname; } catch { return url; } })();
  return !directHosts.some(h => host.startsWith(h));
}

function extractUsageFromResponse(respBody: string): { tokens: number; model: string } {
  try {
    const parsed = JSON.parse(respBody);
    const model = parsed.model || '';
    const usage = parsed.usage;
    if (usage) {
      return {
        tokens: usage.total_tokens || (usage.prompt_tokens || 0) + (usage.completion_tokens || 0),
        model,
      };
    }
  } catch {}
  return { tokens: 0, model: '' };
}

async function dispatchNonStream(
  reqPath: string, reqMethod: string, reqHeaders: Record<string, string>,
  reqBody: string, keyId: string,
): Promise<{ status: number; body: string }> {
  // Append -free suffix (compatible with legacy opencode-gate behavior)
  if (reqBody && reqPath.includes('/chat/completions')) {
    try {
      const parsed = JSON.parse(reqBody);
      if (parsed.model && !parsed.model.endsWith('-free')) {
        parsed.model = parsed.model + '-free';
      }
      reqBody = JSON.stringify(parsed);
    } catch {}
  }
  let lastErr: any = null;
  let directFallback = false;
  const triedNodes = new Set<string>();
  console.log(`[Non-stream] Starting request ${reqPath} singboxOk=${singboxOk} key=${keyId.slice(0,7)}`);
  for (let attempt = 0; attempt < 20; attempt++) {
    const start = Date.now();
    let res: { status: number; body: string };
    try {
      if (directFallback || !singboxOk) {
        res = await doHttps(reqPath, reqMethod, reqHeaders, reqBody);
      } else {
        const agent = await getSingboxAgent();
        if (agent) {
          res = await doHttps(reqPath, reqMethod, reqHeaders, reqBody, agent);
        } else {
          res = await doHttps(reqPath, reqMethod, reqHeaders, reqBody);
        }
      }
    } catch (e: any) {
      lastErr = e;
      if (!directFallback && singboxOk) {
        console.log(`[Non-stream] Proxy connection error, switching node: ${e.message?.slice(0, 60) || e}`);
        await switchSingboxNode(triedNodes);
        continue;
      }
      stats.errors++;
      const fb = JSON.stringify({ error: { message: `Request failed: ${e.message || 'Unknown error'}` } });
      audit(502, 0, keyId, reqPath);
      return { status: 502, body: fb };
    }
    const latency = Date.now() - start;
    console.log(`[Non-stream] attempt ${attempt} upstream response ${res.status} (${latency}ms): ${res.body.slice(0,120)}`);
    if (res.status === 429) {
      stats.rateLimited++;
      console.log(`[429] Upstream rate limit, switching node (attempt ${attempt + 1})`);
      if (singboxOk) {
        try {
          const r = await fetch(`${SINGBOX_API_URL}/proxies/manual`, { signal: AbortSignal.timeout(2000) });
          if (r.ok) { const d = await r.json() as any; if (d.now) triedNodes.add(d.now); }
        } catch {}
        await switchSingboxNode(triedNodes);
      }
      if (attempt >= 5) directFallback = true;
      continue;
    }
    if (res.status >= 200 && res.status < 300) stats.success++;
    else if (res.status >= 500) {
      stats.errors++;
      // 500 is upstream internal error; switching nodes does not help, fall back directly to direct connection
      directFallback = true;
      continue;
    }
    stats.total++;
    audit(res.status, latency, keyId, reqPath, res.body);
    return res;
  }
  // Direct connection fallback
  try {
    const finalRes = await doHttps(reqPath, reqMethod, reqHeaders, reqBody);
    stats.total++;
    audit(finalRes.status, 0, keyId, reqPath, finalRes.body);
    return finalRes;
  } catch (e: any) {
    lastErr = e;
  }
  stats.errors++;
  const fb = JSON.stringify({ error: { message: `Upstream request failed: ${lastErr?.message || 'Unknown error'}` } });
  audit(502, 0, keyId, reqPath);
  return { status: 502, body: fb };
}async function dispatchStream(
  reqPath: string, reqMethod: string, reqHeaders: Record<string, string>,
  reqBody: string, keyId: string,
): Promise<{ status: number; stream: ReadableStream<Uint8Array>; headers: Record<string, string> }> {
  // Append -free suffix (compatible with legacy opencode-gate behavior, consistent with non-stream)
  if (reqBody && reqPath.includes('/chat/completions')) {
    try {
      const parsed = JSON.parse(reqBody);
      if (parsed.model && !parsed.model.endsWith('-free')) {
        parsed.model = parsed.model + '-free';
      }
      reqBody = JSON.stringify(parsed);
    } catch {}
  }
  let lastErr: any = null;
  let directFallback = false;
  const triedNodes = new Set<string>();
  for (let attempt = 0; attempt < 20; attempt++) {
    const start = Date.now();
    let res: { status: number; stream: ReadableStream<Uint8Array>; headers: Record<string, string> };
    try {
      if (directFallback || !singboxOk) {
        res = await doHttpsStream(reqPath, reqMethod, reqHeaders, reqBody);
      } else {
        const agent = await getSingboxAgent();
        if (agent) {
          res = await doHttpsStream(reqPath, reqMethod, reqHeaders, reqBody, agent);
        } else {
          res = await doHttpsStream(reqPath, reqMethod, reqHeaders, reqBody);
        }
      }
    } catch (e: any) {
      lastErr = e;
      if (!directFallback && singboxOk) {
        console.log(`[Stream] Proxy connection error, switching node: ${e.message?.slice(0, 60) || e}`);
        await switchSingboxNode(triedNodes);
        continue;
      }
      stats.errors++;
      const errBody = JSON.stringify({ error: { message: `Stream request failed: ${e.message || 'Unknown error'}` } });
      const stream = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new TextEncoder().encode(errBody)); controller.close(); } });
      audit(502, 0, keyId, reqPath);
      return { status: 502, stream, headers: {} };
    }
    if (res.status === 429) {
      stats.rateLimited++;
      console.log(`[429] Stream upstream rate limit, switching node (attempt ${attempt + 1})`);
      if (singboxOk) {
        try {
          const r = await fetch(`${SINGBOX_API_URL}/proxies/manual`, { signal: AbortSignal.timeout(2000) });
          if (r.ok) { const d = await r.json() as any; if (d.now) triedNodes.add(d.now); }
        } catch {}
        await switchSingboxNode(triedNodes);
      }
      if (attempt >= 5) directFallback = true;
      continue;
    }
    stats.total++;
    if (res.status >= 200 && res.status < 300) stats.success++;
    else if (res.status >= 500) {
      stats.errors++;
      // 500 is upstream internal error; switching nodes does not help, fall back directly to direct connection
      directFallback = true;
      continue;
    }
    audit(res.status, Date.now() - start, keyId, reqPath);
    return res;
  }
  // Direct connection fallback
  try {
    const finalRes = await doHttpsStream(reqPath, reqMethod, reqHeaders, reqBody);
    stats.total++;
    audit(finalRes.status, 0, keyId, reqPath);
    return finalRes;
  } catch (e: any) {
    lastErr = e;
  }
  stats.errors++;
  const errBody = JSON.stringify({ error: { message: `Stream request failed: ${lastErr?.message || 'Unknown error'}` } });
  const stream = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new TextEncoder().encode(errBody)); controller.close(); } });
  audit(502, 0, keyId, reqPath);
  return { status: 502, stream, headers: {} };
}function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    req.on('error', () => resolve(Buffer.concat(chunks).toString('utf-8')));
  });
}

function json(res: http.ServerResponse, status: number, obj: any) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}

const PUBLIC_DIR = path.join(process.cwd(), 'public');

const MIME_TYPES: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.webp': 'image/webp',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.eot': 'application/vnd.ms-fontobject',
  '.txt': 'text/plain; charset=utf-8',
  '.map': 'application/json',
};

/** Safely serve static files from public/ directory; returns true if response handled */
function serveStatic(res: http.ServerResponse, urlPath: string): boolean {
  try {
    // Resolve real path within public directory to prevent directory traversal
    const safePath = path.normalize(urlPath).replace(/^(\/\/)+/, '/');
    const filePath = path.join(PUBLIC_DIR, safePath);
    if (!filePath.startsWith(PUBLIC_DIR + path.sep) && filePath !== PUBLIC_DIR) return false;
    if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) return false;
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';
    const data = fs.readFileSync(filePath);
    res.writeHead(200, { 'content-type': contentType, 'cache-control': 'no-cache' });
    res.end(data);
    return true;
  } catch {
    return false;
  }
}

// ═══════════════════════════════════════════════════════════
//  HTTP Server
// ═══════════════════════════════════════════════════════════

async function handler(req: http.IncomingMessage, res: http.ServerResponse) {
  const url = req.url || '/';
  const method = req.method || 'GET';
  const parsed = new URL(url, `http://${req.headers.host || 'localhost'}`);
  const path = parsed.pathname;

  try {
    // ───────────────────────────────────────────────
    //  GET /  — Status page
    // ───────────────────────────────────────────────
    if (path === '/' && method === 'GET') {
      // Admin Panel: return public/index.html if present, else fallback to embedded status page
      const idxPath = PUBLIC_DIR + '/index.html';
      if (fs.existsSync(idxPath)) {
        const data = fs.readFileSync(idxPath);
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
        res.end(data);
        return;
      }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      res.end(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>OpenCode2API</title></head>
<body style="font-family:monospace;margin:2em">
<h2>🚀 OpenCode2API (SingBox Edition)</h2>
<p>Uptime: ${Math.floor((Date.now() - START_TIME) / 1000)}s</p>
<p>SingBox: ${SINGBOX_MODE === 'on' ? (singboxOk ? '✅ Healthy' : '❌ Offline') : '⏹️ Off'}</p>
<p>Nodes: ${singboxNodes.length}</p>
<p>Keys: ${Object.keys(apiKeys).length}</p>
<p>Requests: ${stats.total} (Success: ${stats.success} / Rate Limited: ${stats.rateLimited} / Errors: ${stats.errors})</p>
<p><a href="/status">/status</a> — <a href="/api/keys">/api/keys</a> — <a href="/api/audit">/api/audit</a> — <a href="/api/logs">/api/logs</a> — <a href="/api/models">/api/models</a></p>
</body></html>`);
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /status  — Brief status
    // ───────────────────────────────────────────────
    if (path === '/status' && method === 'GET') {
      json(res, 200, {
        uptime: Date.now() - START_TIME,
        singbox: { mode: SINGBOX_MODE, ok: singboxOk, nodes: singboxNodes.length, currentNode: singboxNodeIndex },
        keys: Object.keys(apiKeys).length,
        stats, activeRequests: Object.values(activeRequests).reduce((a, b) => a + b, 0),
        cachedModels: cachedModels.length,
      });
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /api/logs
    // ───────────────────────────────────────────────
    if (path === '/api/logs' && method === 'GET') {
      json(res, 200, { logs: recentLogs.slice(-200) });
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /api/audit
    // ───────────────────────────────────────────────
    if (path === '/api/audit' && method === 'GET') {
      json(res, 200, { audit: auditLog.slice(-500) });
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /api/keys
    // ───────────────────────────────────────────────
    if (path === '/api/keys' && method === 'GET') {
      json(res, 200, { keys: apiKeys });
      return;
    }

    // ───────────────────────────────────────────────
    //  POST /api/keys  — Create key
    // ───────────────────────────────────────────────
    if (path === '/api/keys' && method === 'POST') {
      const body = JSON.parse(await readBody(req));
      const key = body.key || 'sk-' + crypto.randomBytes(16).toString('hex');
      apiKeys[key] = {
        key, name: body.name || 'unnamed', enabled: true,
        createdAt: Date.now(), lastUsedAt: 0,
        totalRequests: 0, totalTokens: 0,
        maxConcurrency: body.maxConcurrency || 5,
        maxRequests: body.maxRequests || 1000000,
        requestCount: 0, expiresAt: body.expiresAt || Date.now() + 365 * 86400000,
      };
      saveKeys();
      json(res, 200, { success: true, key });
      return;
    }

    // ───────────────────────────────────────────────
    //  DELETE /api/keys/:key
    // ───────────────────────────────────────────────
    if (path.startsWith('/api/keys/') && method === 'DELETE') {
      const key = path.slice('/api/keys/'.length);
      if (apiKeys[key]) { delete apiKeys[key]; saveKeys(); json(res, 200, { success: true }); }
      else json(res, 404, { error: 'Key not found' });
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /api/models
    // ───────────────────────────────────────────────
    if (path === '/api/models' && method === 'GET') {
      json(res, 200, { data: cachedModels, cachedAt: cachedModelsTime });
      return;
    }

    // ───────────────────────────────────────────────
    //  POST /api/models/refresh  — Refresh model list
    // ───────────────────────────────────────────────
    if (path === '/api/models/refresh' && method === 'POST') {
      const models = await fetchModelsFromUpstream();
      json(res, 200, { success: true, count: models.length });
      return;
    }

    // ───────────────────────────────────────────────
    //  POST /api/subscription  — Add subscription
    // ───────────────────────────────────────────────
    if (path === '/api/subscription' && method === 'POST') {
      const body = JSON.parse(await readBody(req));
      if (!body.url) { json(res, 400, { error: 'url is required' }); return; }
      const sub: SubscriptionConfig = { url: body.url, token: body.token || '', updatedAt: Date.now() };
      try {
        const count = await generateSingboxConfig(sub);
        saveSubscription(sub);
        await reloadSingboxConfig();
        json(res, 200, { success: true, nodes: count, message: `Parsed ${count} nodes, sing-box reloaded` });
      } catch (e: any) {
        json(res, 500, { error: `Failed to generate config: ${e.message}` });
      }
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /api/subscription  — View subscription status
    // ───────────────────────────────────────────────
    if (path === '/api/subscription' && method === 'GET') {
      const sub = loadSubscription();
      json(res, 200, {
        subscription: sub,
        nodes: singboxNodes,
        currentNode: singboxNodes[singboxNodeIndex] || '',
        nodeIndex: singboxNodeIndex,
        singboxOk,
        configFile: fs.existsSync(path.join(SINGBOX_CONFIG_DIR, 'singbox_config.json')),
      });
      return;
    }

    // ───────────────────────────────────────────────
    //  POST /api/singbox/switch  — Manually switch node
    // ───────────────────────────────────────────────
    if (path === '/api/singbox/switch' && method === 'POST') {
      const node = await switchSingboxNode();
      if (node) json(res, 200, { success: true, node });
      else json(res, 500, { error: 'Switch failed' });
      return;
    }

    // ───────────────────────────────────────────────
    //  POST /api/singbox/check  — Check sing-box health
    // ───────────────────────────────────────────────
    if (path === '/api/singbox/check' && method === 'POST') {
      const ok = await checkSingboxHealth();
      json(res, 200, { ok, nodes: singboxNodes.length, currentNode: singboxNodes[singboxNodeIndex] || '' });
      return;
    }

    // ───────────────────────────────────────────────
    //  POST /api/singbox/reload  — Reload sing-box config
    // ───────────────────────────────────────────────
    if (path === '/api/singbox/reload' && method === 'POST') {
      const ok = await reloadSingboxConfig();
      json(res, 200, { success: ok });
      return;
    }

    // ───────────────────────────────────────────────
    //  GET /api/stats  — Detailed statistics
    // ───────────────────────────────────────────────
    if (path === '/api/stats' && method === 'GET') {
      json(res, 200, {
        stats,
        uptime: Date.now() - START_TIME,
        activeRequests: Object.entries(activeRequests).map(([k, v]) => ({ key: k, count: v })),
        singbox: { ok: singboxOk, nodes: singboxNodes.length },
      });
      return;
    }

function normalizeFreeModelAlias(bodyStr: string | undefined): string | undefined {
  if (!bodyStr) return bodyStr;
  try {
    const parsed = JSON.parse(bodyStr);
    const model = parsed?.model;
    if (typeof model === 'string' && model) {
      const KNOWN_EXPLICIT_ALIASES: Record<string, string> = {
        'muse-spark-1.3': 'muse-spark-1.3-contributor-free',
        'muse-spark-1.3-free': 'muse-spark-1.3-contributor-free',
        'muse-spark-1.2': 'muse-spark-1.2-contributor-free',
        'muse-spark-1.2-free': 'muse-spark-1.2-contributor-free',
        'ling-3.0-flash-fin': 'ling-3.0-flash-fin-free',
        'ling-3.0-flash': 'ling-3.0-flash-fin-free',
        'nemotron-3.5-lightning': 'nemotron-3.5-lightning-free',
        'nemotron-3-ultra': 'nemotron-3-ultra-free',
        'deepseek-v4-flash': 'deepseek-v4-flash-free',
        'mimo-v2.5': 'mimo-v2.5-free',
      };
      if (KNOWN_EXPLICIT_ALIASES[model]) {
        parsed.model = KNOWN_EXPLICIT_ALIASES[model];
      } else if (model !== 'big-pickle' && !model.endsWith('-free')) {
        parsed.model = `${model}-free`;
      }
    }
    if (parsed?.stream === true && parsed.stream_options === undefined) {
      parsed.stream_options = { include_usage: true };
    }
    return JSON.stringify(parsed);
  } catch {
    return bodyStr;
  }
}

    // ───────────────────────────────────────────────
    //  v1/chat/completions  — Non-streaming
    // ───────────────────────────────────────────────
    if (path === '/v1/chat/completions' && (method === 'POST' || method === 'OPTIONS')) {
      if (method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'POST,OPTIONS', 'access-control-allow-headers': '*' }); res.end(); return; }
      let body = await readBody(req);
      body = normalizeFreeModelAlias(body) || body;
      const auth = req.headers['authorization'] || '';
      const key = auth.replace(/^Bearer\s+/i, '').trim();
      const v = validateKey(key);
      if (!v.valid) { json(res, 401, { error: { message: v.reason } }); return; }
      acquireKey(key);
      try {
        const parsed = JSON.parse(body);
        const isStream = !!parsed.stream;
        recordKeyUsage(key, 0);
        if (isStream) {
          const result = await dispatchStream(path, method, collectHeadersFromReq(req), body, key);
          res.writeHead(result.status, {
            'content-type': 'text/event-stream',
            'cache-control': 'no-cache',
            'connection': 'keep-alive',
            ...result.headers,
          });
          const reader = result.stream.getReader();
          const pump = async () => {
            try { while (true) { const { done, value } = await reader.read(); if (done) { res.end(); return; } res.write(value); } }
            catch { res.end(); }
          };
          pump();
        } else {
          const result = await dispatchNonStream(path, method, collectHeadersFromReq(req), body, key);
          const usage = extractUsageFromResponse(result.body);
          if (usage.tokens > 0) recordKeyUsage(key, usage.tokens);
          res.writeHead(result.status, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
          res.end(result.body);
        }
      } catch (e: any) {
        json(res, 400, { error: { message: `Request parsing failed: ${e.message}` } });
      } finally {
        releaseKey(key);
      }
      return;
    }

    // ───────────────────────────────────────────────
    //  v1/* other endpoints — Proxy to upstream
    // ───────────────────────────────────────────────
    if (path.startsWith('/v1/')) {
      const auth = req.headers['authorization'] || '';
      const key = auth.replace(/^Bearer\s+/i, '').trim();
      const v = validateKey(key);
      if (!v.valid) { json(res, 401, { error: { message: v.reason } }); return; }
      acquireKey(key);
      try {
        let body = method === 'GET' || method === 'DELETE' ? undefined : await readBody(req);
        if (body && (path.includes('/chat/completions') || path.includes('/responses'))) {
          body = normalizeFreeModelAlias(body);
        }
        const result = await dispatchNonStream(path, method, collectHeadersFromReq(req), body || '', key);
        // /v1/models only preserves free models (compatible with legacy behavior; big-pickle is stealth free model)
        if (path === '/v1/models' && result.status === 200 && result.body) {
          try {
            const parsed = JSON.parse(result.body);
            const all = parsed.data || parsed.models || [];
            const freeModels = all.filter((m: any) => {
              const id = String(m.id || '');
              return id.endsWith('-free') || id === 'big-pickle';
            });
            const aliasMap: Record<string, string> = {
              'muse-spark-1.3-free': 'muse-spark-1.3-contributor-free',
              'muse-spark-1.2-free': 'muse-spark-1.2-contributor-free',
            };
            for (const [aliasId, canonicalId] of Object.entries(aliasMap)) {
              const target = freeModels.find((m: any) => m.id === canonicalId);
              if (target && !freeModels.some((m: any) => m.id === aliasId)) {
                freeModels.push({
                  id: aliasId,
                  object: 'model',
                  created: target.created,
                  owned_by: target.owned_by || 'opencode',
                });
              }
            }
            parsed.data = freeModels;
            parsed.models = freeModels;
            res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
            res.end(JSON.stringify(parsed));
            return;
          } catch {}
        }
        res.writeHead(result.status, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
        res.end(result.body);
      } catch (e: any) {
        json(res, 500, { error: { message: e.message } });
      } finally {
        releaseKey(key);
      }
      return;
    }

    // ───────────────────────────────────────────────
    //  Static file server — public/ directory (admin panel assets)
    // ───────────────────────────────────────────────
    if (method === 'GET' && !path.startsWith('/api/') && !path.startsWith('/v1/') && path !== '/status' && path !== '/ping') {
      if (serveStatic(res, path)) return;
      // SPA fallback: return index.html when non-API path file not found
      const idxPath = PUBLIC_DIR + '/index.html';
      if (fs.existsSync(idxPath)) {
        const data = fs.readFileSync(idxPath);
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
        res.end(data);
        return;
      }
    }

    // ───────────────────────────────────────────────
    //  404
    // ───────────────────────────────────────────────
    json(res, 404, { error: { message: 'not found' } });

  } catch (e: any) {
    console.error(`[handler] ${e.message}`);
    json(res, 500, { error: { message: e.message } });
  }
}

// ═══════════════════════════════════════════════════════════
//  Startup
// ═══════════════════════════════════════════════════════════

const server = http.createServer(handler);

server.on('request', (req, res) => {
  // ping health check
  if (req.url === '/ping') { res.writeHead(200); res.end('pong'); return; }
});

server.listen(PORT, '0.0.0.0', async () => {
  console.log(`\n[OpenCode2API] SingBox Edition started`);
  console.log(`[OpenCode2API] Port: ${PORT}`);
  console.log(`[OpenCode2API] Upstream: ${UPSTREAM}`);
  console.log(`[OpenCode2API] SingBox: ${SINGBOX_MODE === 'on' ? `Socks5 ${SINGBOX_SOCKS_URL} / API ${SINGBOX_API_URL}` : 'Disabled'}`);
  console.log(`[OpenCode2API] Data dir: ${DATA_DIR}`);
  console.log(`[OpenCode2API] API Key: ${API_KEY}\n`);

  // Load persisted data
  loadKeys();
  loadAuditLog();
  if (!loadModelsCache()) await fetchModelsFromUpstream();

  // Initialize sing-box
  if (SINGBOX_MODE === 'on') {
    loadSingboxNodes();
    const ok = await checkSingboxHealth();
    console.log(`[SingBox] Health check: ${ok ? '✅ Healthy' : '❌ Offline'}`);
    if (ok) {
      loadSingboxNodes();
      await initSingboxNode();
      if (singboxOk) {
        console.log(`[SingBox] Current node: ${singboxNodes[singboxNodeIndex]}`);
      }
    }
  }

  // Periodically refresh models
  setInterval(() => fetchModelsFromUpstream(), 60000);
  // Periodically check sing-box health
  if (SINGBOX_MODE === 'on') {
    setInterval(() => checkSingboxHealth(), 30000);
  }
});

// Graceful shutdown
process.on('SIGTERM', () => { console.log('Shutting down...'); server.close(); setTimeout(() => process.exit(0), 1000); });
process.on('SIGINT', () => { console.log('Shutting down...'); server.close(); setTimeout(() => process.exit(0), 1000); });

import { afterEach, describe, expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import http from 'node:http';
import { isIP } from 'node:net';
import vm from 'node:vm';
import { buildZenCatalog, resolveCatalogModel } from '../model-catalog';

// Run the real gateway definitions in isolation. Stop before scheduled tasks
// and startup so tests never scrape public proxies or call the live upstream.
const source = fs.readFileSync(new URL('../gate-docker.ts', import.meta.url), 'utf8');
const scheduledTasks = source.indexOf('//  Scheduled Tasks');
if (scheduledTasks < 0) throw new Error('Gateway scheduled-task boundary missing');
const definitions = new Bun.Transpiler({ loader: 'ts' }).transformSync(
  source.slice(0, scheduledTasks).replace(/^import .+;\r?\n/gm, ''),
);
const pendingTimers = new Set<ReturnType<typeof setTimeout>>();
afterEach(() => { for (const timer of pendingTimers) clearTimeout(timer); pendingTimers.clear(); });

function fixture(probeTimeout = 2500, env: Record<string, string> = {}) {
  let now = 1_000_000;
  let requests = 0;
  let replyStatus = 200;
  const stateFiles = new Map<string, string>();
  let reply: any = { ip: '203.0.113.1', country: 'US' };
  let latency = 1700;
  let hang = false;
  let streamChunks: string[] | null = null;
  let streamOpen = false;
  let destroyedRequests = 0;
  const queuedStreams: string[][] = [];
  let handler: any;
  const timers: { callback: () => void; delay: number; timer: ReturnType<typeof setTimeout> }[] = [];
  class Clock extends Date { static now() { return now; } }
  class Agent { destroy() {} }
  const context = vm.createContext({
    Buffer, URL, TextDecoder, TextEncoder, ReadableStream, AbortController, AbortSignal,
    setTimeout(callback: () => void, delay: number) {
      const timer = setTimeout(callback, delay);
      pendingTimers.add(timer);
      timers.push({ callback, delay, timer });
      return timer;
    }, clearTimeout, Date: Clock,
    console: { log() {}, warn() {}, error() {} },
    process: { cwd: () => path.resolve(import.meta.dir, '..'), env: { PROXY_PROBE_TIMEOUT: String(probeTimeout), ...env } },
    path, crypto, isIP, buildZenCatalog, resolveCatalogModel, fs: { ...fs,
      writeFileSync(file: string, value: string) { stateFiles.set(file, value); },
      readFileSync(file: string, ...args: any[]) { return stateFiles.has(file) ? stateFiles.get(file) : (fs.readFileSync as any)(file, ...args); },
      renameSync(from: string, to: string) { if (stateFiles.has(from)) { stateFiles.set(to, stateFiles.get(from)!); stateFiles.delete(from); } },
    }, spawn() {},
    HttpsProxyAgent: Agent, SocksProxyAgent: Agent,
    geoip: { lookup: () => ({ country: 'US' }) },
    http: { createServer: (fn: any) => { handler = fn; return {}; } },
    https: {
      request(_url: any, _options: any, callback: any) {
        requests++;
        const req = new EventEmitter() as any;
        req.destroy = () => { destroyedRequests++; };
        req.setTimeout = () => {};
        req.write = () => true;
        req.end = () => queueMicrotask(() => {
          if (hang) return;
          now += latency;
          const res = new EventEmitter() as any;
          res.statusCode = replyStatus;
          res.headers = {};
          res.destroy = () => {};
          callback(res);
          const chunks = queuedStreams.shift() || streamChunks;
          if (chunks) for (const chunk of chunks) res.emit('data', Buffer.from(chunk));
          else res.emit('data', Buffer.from(JSON.stringify(reply)));
          if (!streamOpen) res.emit('end');
        });
        return req;
      },
    },
  });
  const gateway: any = vm.runInContext(definitions + `
    ({ coarseScreen, probe, backgroundProbeSweep, allocateKeySlots, loadCandidates,
       replaceFailedSlot, getKeySlotPool, releaseKeySlots, pruneGloballyUnusableSlots, topUpKeySlotPool, desiredProxySlots,
       choosePoolSlot, exitActiveRequests, sessionExits, dispatch, doHttpsStream, initialSseEvent, readBody,
       freeExitCount, currentDemandKeyCount, operationalPoolTarget, currentPoolState, markValidated, noteExitFailure, noteExitSuccess,
       isExitUsable, validatedExits, exitHealth, exitModelBans, keySlotPools, coarseSeen,
       saveProxyHealthState, loadProxyHealthState, recordKeyRequest, recordKeyUsage, saveKeys,
       getKeysFile() { return KEYS_FILE; },
       setDraining(value) { runtimeDraining = value; },
       setCandidates(value) { candidates = value; },
       getCandidates() { return candidates; },
       setSources(value) { proxySources = value; },
       setCustom(value) { customProxyItems = value; },
       setScreen(value) { coarseScreen = value; },
       setProbe(value) { probe = value; },
       setAllocator(value) { allocateKeySlots = value; },
       setHttpsRequest(value) { https.request = value; },
       setDispatch(value) { dispatch = value; },
       setKeys(value) { apiKeys = value; },
       getKeys() { return apiKeys; },
       setModels(value) { cachedModels = value; cachedModelsTime = Date.now(); },
       activeRequests, fetchModelsFromUpstream, ensureModelCatalog, loadModelCatalog,
       normalizeFreeModelAlias, isResponsesOnlyModel, shapeAgentRequest, chatBodyToResponses, collectHeadersFromReq,
       declaredToolNames, authorizedToolNames, requestDeclaresTools, guardToolPolicyStream, monitorUpstreamSse, collectChatStream, responsesSseToChatSse, dispatchDirect, sendJson, sendJsonWithHeaders,
       probeFreeModel, verifyFreeModels, freeModelHealth, workingFreeModelIds, hasModelOutput,
       catalogStatus,
       setFetcher(value) { fetchJsonDirect = value; },
       setProbeModel(value) { probeFreeModel = value; },
       setMetadata(value) { modelsDevMetadata = value; modelsDevTime = Date.now(); },
       getModels() { return cachedModels; },
    })`, context);
  return {
    ...gateway,
    get requests() { return requests; },
    get destroyedRequests() { return destroyedRequests; },
    get now() { return now; },
    advance(ms: number) { now += ms; },
    echo(value: any, ms = 1700, status = 200) { reply = value; latency = ms; replyStatus = status; streamChunks = null; streamOpen = false; },
    echoStream(chunks: string[], open = false) { streamChunks = chunks; streamOpen = open; latency = 0; replyStatus = 200; },
    queueStreams(chunks: string[][]) { queuedStreams.push(...chunks); latency = 0; replyStatus = 200; },
    cache(doc: any) { stateFiles.set(path.resolve(import.meta.dir, '../models_cache.json'), JSON.stringify(doc)); },
    get cachedDoc() { const value = stateFiles.get(path.resolve(import.meta.dir, '../models_cache.json')); return value ? JSON.parse(value) : undefined; },
    get healthDoc() { const value = stateFiles.get(path.resolve(import.meta.dir, '../proxy_health_cache.json')); return value ? JSON.parse(value) : undefined; },
    seedState(file: string, value: any) { stateFiles.set(file, JSON.stringify(value)); },
    readState(file: string) { const value = stateFiles.get(file); return value ? JSON.parse(value) : undefined; },
    hang() { hang = true; },
    fireTimers(delay: number) {
      for (const entry of timers.filter(t => t.delay === delay)) {
        clearTimeout(entry.timer);
        entry.callback();
      }
    },
    handler: (...args: any[]) => handler(...args),
  };
}

function candidate(i: number, lockedBy: string | null = null): any {
  return { address: `203.0.113.${i}:8080`, protocol: 'http', latency: 999, quality_grade: 'C', country: 'US', lockedBy };
}
const healthy = async () => ({ ok: true, latencyMs: 100, country: 'US', reason: 'ok' });
const dead = async () => ({ ok: false, latencyMs: 100, country: '', reason: 'unreachable' });

describe('proxy pool regression checks', () => {
  test('health counts only fresh, unlocked, usable exits', () => {
    const g = fixture();
    const items = [candidate(1), candidate(2, 'key'), candidate(3), candidate(4)];
    g.setCandidates(items);
    for (const c of items.slice(0, 3)) g.markValidated(c.address);
    g.noteExitFailure(items[2].address, 429);
    expect(g.freeExitCount()).toBe(1);
    expect(g.currentPoolState()).toBe('degraded');
    g.advance(15 * 60_000 + 1);
    expect(g.freeExitCount()).toBe(0);
  });

  test('pool state is based on current workload, not 20-key reserve capacity', () => {
    const g = fixture();
    const items = Array.from({ length: 12 }, (_, i) => candidate(i + 1));
    g.setCandidates(items);
    for (const c of items) g.markValidated(c.address);
    expect(g.currentDemandKeyCount()).toBe(1);
    expect(g.operationalPoolTarget()).toBe(12);
    expect(g.currentPoolState()).toBe('healthy');
  });

  test('proxy health cache restores fresh validation and active cooldowns', () => {
    const g = fixture();
    const item = candidate(1);
    g.setCandidates([item]);
    g.markValidated(item.address);
    g.noteExitFailure(item.address, 429, 'big-pickle');
    g.saveProxyHealthState();
    expect(g.healthDoc.validated).toHaveLength(1);
    expect(g.healthDoc.cooldowns).toHaveLength(1);

    g.validatedExits.clear();
    g.exitHealth.clear();
    g.exitModelBans.clear();
    g.loadProxyHealthState();
    expect(g.validatedExits.has(item.address)).toBe(true);
    expect(g.exitHealth.get(item.address)?.cooldownUntil).toBeGreaterThan(g.now);
  });

  test('a concurrent successful stream cannot erase a newer cooldown or model ban', () => {
    const g = fixture();
    const addr = candidate(1).address;
    g.noteExitFailure(addr, 429, 'big-pickle');
    const deadline = g.exitHealth.get(addr).cooldownUntil;
    g.exitModelBans.set(addr, new Map([['big-pickle', { fails: 3, bannedUntil: g.now + 1000 }]]));
    g.noteExitSuccess(addr, 'big-pickle');
    expect(g.exitHealth.get(addr).cooldownUntil).toBe(deadline);
    expect(g.exitModelBans.get(addr).has('big-pickle')).toBe(true);
    g.advance(deadline - g.now + 1);
    g.noteExitSuccess(addr, 'big-pickle');
    expect(g.exitHealth.get(addr).cooldownUntil).toBe(0);
    expect(g.exitModelBans.get(addr).has('big-pickle')).toBe(false);
  });

  test('cached screen measurements obey the current admission ceiling', async () => {
    const g = fixture();
    expect((await g.coarseScreen(candidate(1), 0)).ok).toBe(true);
    expect((await g.coarseScreen(candidate(1), 1200)).ok).toBe(false);
    expect(g.requests).toBe(1);
  });

  test('a stricter cached latency rejection can be admitted when capacity is low', async () => {
    const g = fixture();
    expect((await g.coarseScreen(candidate(1), 1200)).ok).toBe(false);
    expect((await g.coarseScreen(candidate(1), 0)).ok).toBe(true);
    expect(g.requests).toBe(1);
  });

  test('screen rejects invalid IP echoes and blocked countries', async () => {
    const g = fixture();
    g.echo({ ip: 'not-an-ip', country: 'US' }, 100);
    expect((await g.coarseScreen(candidate(1), 0)).ok).toBe(false);
    g.echo({ ip: '203.0.113.2', country: 'CN' }, 100);
    expect((await g.coarseScreen(candidate(2), 0)).ok).toBe(false);
  });

  test('concurrent screens share one request and apply separate ceilings', async () => {
    const g = fixture();
    const results = await Promise.all([g.coarseScreen(candidate(1), 0), g.coarseScreen(candidate(1), 1200)]);
    expect(results.map(r => r.ok)).toEqual([true, false]);
    expect(g.requests).toBe(1);
  });

  test('screens have a complete-request deadline even before a socket connects', async () => {
    const g = fixture(10);
    g.hang();
    expect((await g.coarseScreen(candidate(1), 0)).ok).toBe(false);
  });

  test('upstream confirmation also has a complete-request deadline', async () => {
    const g = fixture(10);
    g.hang();
    expect((await g.probe(candidate(1))).ok).toBe(false);
  });

  test('concurrent upstream confirmations share one request', async () => {
    const g = fixture();
    g.echo({ data: [{ id: 'big-pickle' }] }, 100);
    const results = await Promise.all([g.probe(candidate(1)), g.probe(candidate(1))]);
    expect(results.every(r => r.ok)).toBe(true);
    expect(g.requests).toBe(1);
  });

  test('background sweeps rotate through all candidates', async () => {
    const g = fixture();
    const items = Array.from({ length: 65 }, (_, i) => candidate(i + 1));
    const checked = new Set<string>();
    g.setCandidates(items);
    g.setScreen(async (c: any) => { checked.add(c.address); return healthy(); });
    await g.backgroundProbeSweep();
    await g.backgroundProbeSweep();
    await g.backgroundProbeSweep();
    await g.backgroundProbeSweep();
    expect(checked.size).toBe(items.length);
  });

  test('direct stream health is decided at terminal completion, not first chunk', async () => {
    const g = fixture();
    const pool = { keyId: 'key', slots: [], rrCursor: 0, lastUsedAt: g.now };
    const body = JSON.stringify({ model: 'muse-spark-1.3-contributor-free', input: 'Hi', stream: true });
    g.echoStream(['data: {"type":"response.output_text.delta","delta":"partial"}\n\n']);
    const result = await g.dispatchDirect('/v1/responses', 'POST', { 'content-type': 'application/json' }, body, pool);
    expect(result.status).toBe(200);
    await expect(new Response(result.stream).text()).rejects.toThrow('terminal event');
    expect(g.exitHealth.get('__direct__')?.fails).toBe(1);
  });

  test('direct fallback respects its own 429 cooldown instead of hammering the same egress', async () => {
    const g = fixture();
    const pool = { keyId: 'key', slots: [], rrCursor: 0, lastUsedAt: g.now };
    const body = JSON.stringify({ model: 'muse-spark-1.3-contributor-free', input: 'Hi', stream: false });
    g.echo({ error: { type: 'FreeUsageLimitError', message: 'rate limited' } }, 100, 429);
    const first = await g.dispatchDirect('/v1/responses', 'POST', { 'content-type': 'application/json' }, body, pool);
    expect(first.status).toBe(429);
    expect(g.requests).toBe(1);

    const second = await g.dispatchDirect('/v1/responses', 'POST', { 'content-type': 'application/json' }, body, pool);
    expect(second.status).toBe(503);
    expect(Number(second.responseHeaders['retry-after'])).toBeGreaterThan(0);
    expect(g.requests).toBe(1);

    g.advance(30_001);
    g.echo({ id: 'ok' }, 100, 200);
    const recovered = await g.dispatchDirect('/v1/responses', 'POST', { 'content-type': 'application/json' }, body, pool);
    expect(recovered.status).toBe(200);
    expect(g.requests).toBe(2);
  });

  test('background success does not erase an upstream rate-limit cooldown', async () => {
    const g = fixture();
    const c = candidate(1);
    g.setCandidates([c]);
    g.noteExitFailure(c.address, 429);
    g.setScreen(healthy);
    await g.backgroundProbeSweep();
    expect(g.isExitUsable(c.address)).toBe(false);
  });

  test('a dead allocated exit is removed while healthy slots stay allocated', async () => {
    const g = fixture();
    const bad = candidate(1, 'key');
    bad.failCount = 2;
    const good = candidate(2, 'key');
    const pool = { keyId: 'key', slots: [bad, good].map(c => ({ addr: c.address })), rrCursor: 0, lastUsedAt: g.now };
    g.setCandidates([bad, good]);
    g.keySlotPools.set('key', pool);
    g.noteExitFailure(bad.address, 0);
    g.noteExitFailure(bad.address, 0);
    g.setScreen(async (c: any) => c === bad ? dead() : healthy());
    await g.backgroundProbeSweep();
    expect(pool.slots.map((s: any) => s.addr)).toEqual([good.address]);
    expect(g.keySlotPools.get('key')).toBe(pool);
    expect(bad.lockedBy).toBe('__blacklist__');
  });

  test('allocation evicts repeated coarse failures', async () => {
    const g = fixture();
    const items = [candidate(1), candidate(2), candidate(3)];
    items[0].failCount = 2;
    g.setCandidates(items);
    g.setScreen(async (c: any) => c === items[0] ? dead() : healthy());
    g.setProbe(async () => ({ ok: true, latencyMs: 150 }));
    await g.allocateKeySlots('key');
    expect(g.getCandidates().some((c: any) => c.address === items[0].address)).toBe(false);
  });

  test('allocation stores measured country and latency for free survivors', async () => {
    const g = fixture();
    const items = Array.from({ length: 6 }, (_, i) => candidate(i + 1));
    g.setCandidates(items);
    g.setScreen(async () => ({ ok: true, latencyMs: 100, country: 'DE', reason: 'ok' }));
    g.setProbe(async () => ({ ok: true, latencyMs: 150 }));
    await g.allocateKeySlots('key');
    expect(items[5].latency).toBe(100);
    expect(items[5].country).toBe('DE');
    expect(items[5].quality_grade).toBe('S');
  });

  test('a feed refresh retains measured health and failure counts', async () => {
    const g = fixture();
    const c = candidate(1);
    c.failCount = 2;
    c.latency = 100;
    c.quality_grade = 'S';
    c.country = 'DE';
    g.setCandidates([c]);
    g.markValidated(c.address);
    g.setSources([]);
    // Exercise the persisted-custom merge without making external requests.
    g.setCustom([candidate(1)]);
    await g.loadCandidates();
    const refreshed = g.getCandidates()[0];
    expect(refreshed.failCount).toBe(2);
    expect(refreshed.latency).toBe(100);
    expect(refreshed.quality_grade).toBe('S');
    expect(refreshed.country).toBe('DE');
  });

  test('same-key concurrent requests reuse a single allocation', async () => {
    const g = fixture();
    g.setCandidates(Array.from({ length: 6 }, (_, i) => candidate(i + 1)));
    g.setScreen(healthy);
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));
    const [first, second] = await Promise.all([g.getKeySlotPool('key'), g.getKeySlotPool('key')]);
    expect(first).toBe(second);
    expect(first.slots).toHaveLength(3);
    expect(g.getCandidates().filter((c: any) => c.lockedBy === 'key')).toHaveLength(3);
  });

  test('different keys cannot claim the same exit after concurrent probes', async () => {
    const g = fixture();
    g.setCandidates(Array.from({ length: 6 }, (_, i) => candidate(i + 1)));
    g.setScreen(healthy);
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));
    const pools = await Promise.all([g.getKeySlotPool('first'), g.getKeySlotPool('second')]);
    expect(pools.every(p => p?.slots.length === 3)).toBe(true);
    const addresses = pools.flatMap(p => p.slots.map((s: any) => s.addr));
    expect(new Set(addresses).size).toBe(addresses.length);
  });

  test('pending allocations count toward the maximum active keys', async () => {
    const g = fixture();
    g.setCandidates(Array.from({ length: 75 }, (_, i) => candidate(i + 1)));
    g.setScreen(healthy);
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));
    const pools = await Promise.all(Array.from({ length: 21 }, (_, i) => g.getKeySlotPool(`key-${i}`)));
    expect(pools[20]).toBeNull();
    expect(g.keySlotPools.size).toBeLessThanOrEqual(20);
  });

  test('release preserves an exit now owned by a different key', () => {
    const g = fixture();
    const c = candidate(1, 'second');
    g.setCandidates([c]);
    g.keySlotPools.set('first', { slots: [{ addr: c.address }] });
    g.releaseKeySlots('first');
    expect(c.lockedBy).toBe('second');
  });

  test('replacement screens geo-blocked exits and skips rate-limited exits', async () => {
    const g = fixture();
    const items = [candidate(1, 'key'), candidate(2), candidate(3), candidate(4)];
    const pool = { keyId: 'key', slots: [{ addr: items[0].address }], rrCursor: 0, lastUsedAt: g.now };
    g.setCandidates(items);
    g.keySlotPools.set('key', pool);
    g.noteExitFailure(items[1].address, 429);
    const probed: string[] = [];
    g.setScreen(async (c: any) => c === items[2] ? dead() : healthy());
    g.setProbe(async (c: any) => { probed.push(c.address); return { ok: true, latencyMs: 100 }; });
    await g.replaceFailedSlot(pool, items[0].address);
    expect(probed).toEqual([items[3].address]);
    expect(pool.slots.map(s => s.addr)).toEqual([items[3].address]);
  });

  test('partial pools top up from validated free exits before direct fallback', async () => {
    const g = fixture();
    const attached = candidate(41, 'key-a');
    const spare1 = candidate(42, null);
    const spare2 = candidate(43, null);
    g.setCandidates([attached, spare1, spare2]);
    const pool = {
      keyId: 'key-a', rrCursor: 0, lastUsedAt: g.now,
      slots: [{ addr: attached.address, url: `http://${attached.address}`, proto: 'http', latencyMs: 100, qualityGrade: 'B' }],
    };
    g.keySlotPools.set('key-a', pool);
    g.markValidated(spare1.address);
    g.markValidated(spare2.address);
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));

    const added = await g.topUpKeySlotPool(pool, 'muse-spark-1.3-contributor-free');
    expect(added).toBe(2);
    expect(pool.slots).toHaveLength(3);
    expect(new Set(pool.slots.map((slot: any) => slot.addr)).size).toBe(3);
    expect(spare1.lockedBy).toBe('key-a');
    expect(spare2.lockedBy).toBe('key-a');
  });

  test('cooled attached slots are pruned and replaced before direct fallback', async () => {
    const g = fixture();
    const cooled = candidate(51, 'key-a');
    const spare = candidate(52, null);
    const spare2 = candidate(53, null);
    const spare3 = candidate(54, null);
    g.setCandidates([cooled, spare, spare2, spare3]);
    g.keySlotPools.set('key-a', {
      keyId: 'key-a', rrCursor: 0, lastUsedAt: g.now,
      slots: [{ addr: cooled.address, url: `http://${cooled.address}`, proto: 'http', latencyMs: 100, qualityGrade: 'B' }],
    });
    g.noteExitFailure(cooled.address, 429);
    g.setScreen(healthy);
    g.setProbe(async (item: any) => ({ ok: item.address === spare.address, latencyMs: 100, country: 'US', reason: 'ok' }));

    const pool = await g.getKeySlotPool('key-a');
    expect(pool).not.toBeNull();
    expect(pool.slots.some((slot: any) => slot.addr === cooled.address)).toBe(false);
    expect(pool.slots.some((slot: any) => slot.addr === spare.address)).toBe(true);
    expect(cooled.lockedBy).toBeNull();
    expect(spare.lockedBy).toBe('key-a');
  });

  test('replacement cooldown expires even after a feed refresh', async () => {
    const g = fixture();
    const items = [candidate(1, 'key'), candidate(2), candidate(3)];
    const pool = { keyId: 'key', slots: [{ addr: items[0].address }], rrCursor: 0, lastUsedAt: g.now };
    g.setCandidates(items);
    g.keySlotPools.set('key', pool);
    g.setScreen(healthy);
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));
    await g.replaceFailedSlot(pool, items[0].address);
    expect(items[0].lockedBy).toBe('__cooldown__');
    g.setSources([]);
    g.setCustom(items.map((_: any, i: number) => candidate(i + 1)));
    await g.loadCandidates();
    g.fireTimers(120000);
    expect(g.getCandidates()[0].lockedBy).toBeNull();
  });

  test('stale replacement cannot change a different key\'s ownership', async () => {
    const g = fixture();
    const c = candidate(1, 'second');
    g.setCandidates([c]);
    const stale = { keyId: 'first', slots: [{ addr: c.address }] };
    await g.replaceFailedSlot(stale, c.address);
    expect(c.lockedBy).toBe('second');
    expect(stale.slots).toHaveLength(1);
  });
});

describe('shared key usage persistence', () => {
  const record = (name: string, n: number) => ({
    key: 'k', name, enabled: true, createdAt: 1, lastUsedAt: 0,
    totalRequests: n, requestCount: n, totalTokens: n * 10, maxConcurrency: 0, maxRequests: 0, expiresAt: 0,
  });

  test('a generation adds its usage to counters written by another generation', () => {
    const g = fixture();
    const file = g.getKeysFile();
    g.seedState(file, { k: record('disk', 10) });
    g.setKeys({ k: record('mem', 10) });
    g.seedState(file, { k: record('disk', 15) });
    g.recordKeyRequest('k');
    g.recordKeyUsage('k', 7);
    const saved = g.readState(file).k;
    expect(saved.totalRequests).toBe(16);
    expect(saved.requestCount).toBe(16);
    expect(saved.totalTokens).toBe(157);
    expect(g.getKeys().k.totalRequests).toBe(16);
  });

  test('a draining generation persists only usage and never edits or resurrects keys', () => {
    const g = fixture();
    const file = g.getKeysFile();
    g.setKeys({ k: record('mem', 10), gone: { ...record('gone', 1), key: 'gone' } });
    g.seedState(file, { k: record('disk', 20) });
    g.setDraining(true);
    g.recordKeyUsage('k', 50);
    g.recordKeyUsage('gone', 5);
    const saved = g.readState(file);
    expect(saved.k.name).toBe('disk');
    expect(saved.k.totalRequests).toBe(20);
    expect(saved.k.totalTokens).toBe(250);
    expect(saved.gone).toBeUndefined();
  });
});

describe('HTTP compatibility', () => {
  async function withServer(run: (base: string, g: any) => Promise<void>) {
    const g = fixture();
    g.setKeys({ 'test-key': { enabled: true, expiresAt: 0, maxRequests: 0, maxConcurrency: 0,
      requestCount: 0, totalRequests: 0, totalTokens: 0 } });
    g.setModels([{ id: 'big-pickle', object: 'model', endpoint: 'chat' }]);
    g.setCandidates([]);
    g.setSources([]);
    const server = http.createServer(g.handler);
    server.listen(0, '127.0.0.1');
    await new Promise<void>(resolve => server.once('listening', resolve));
    const address = server.address() as any;
    try { await run(`http://127.0.0.1:${address.port}`, g); }
    finally { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
  }
  const headers = { authorization: 'Bearer test-key', 'content-type': 'application/json' };
  const completion = (stream: boolean) => JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'Hi' }], stream });
  function replyStream() {
    const chunks = [
      { id: 'chatcmpl-test', model: 'big-pickle', choices: [{ index: 0, delta: { role: 'assistant', content: 'OK' }, finish_reason: null }] },
      { id: 'chatcmpl-test', model: 'big-pickle', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 } },
    ];
    const body = chunks.map(c => `data: ${JSON.stringify(c)}\n\n`).join('') + 'data: [DONE]\n\n';
    return new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(body)); controller.close(); } });
  }

  test('status exposes stream and direct-egress health controls', async () => {
    await withServer(async base => {
      const response = await fetch(base + '/api/status');
      expect(response.status).toBe(200);
      const doc = await response.json();
      expect(doc.timeouts.streamFirstByteMs).toBe(30000);
      expect(doc.timeouts.streamIdleMs).toBe(600000);
      expect(doc.timeouts.proxyConnectMs).toBe(15000);
      expect(doc.directEgress.usable).toBe(true);
      expect(doc.directEgress.retryAfterSeconds).toBe(0);
      expect(doc.pool.backpressureWaitMs).toBe(0);
      expect(doc.pool.generationConcurrencyLimit).toBeNull();
    });
  });

  test('gateway-generated 503 responses also include Retry-After', () => {
    const g = fixture();
    let status = 0;
    let responseHeaders: Record<string, string> = {};
    let body = '';
    const res = {
      writeHead(nextStatus: number, nextHeaders: Record<string, string>) { status = nextStatus; responseHeaders = nextHeaders; },
      end(value: string) { body = value; },
    } as any;
    g.sendJson(res, 503, { error: 'temporarily_unavailable' });
    expect(status).toBe(503);
    expect(responseHeaders['retry-after']).toBe('30');
    expect(JSON.parse(body).error).toBe('temporarily_unavailable');
  });

  test('models require a valid key and support the /openai prefix', async () => {
    await withServer(async base => {
      expect((await fetch(base + '/v1/models')).status).toBe(401);
      expect((await fetch(base + '/v1/models', { headers: { authorization: 'Bearer invalid' } })).status).toBe(403);
      const response = await fetch(base + '/openai/v1/models', { headers });
      expect(response.status).toBe(200);
      expect((await response.json()).data.map((m: any) => m.id)).toEqual(['big-pickle']);
    });
  });

  test('final 429 and 503 responses carry retry guidance', async () => {
    await withServer(async (base, g) => {
      g.setDispatch(async () => ({ status: 429, body: '{"error":"rate_limited"}', responseHeaders: { 'retry-after': '17' } }));
      let response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) });
      expect(response.status).toBe(429);
      expect(response.headers.get('retry-after')).toBe('17');
      await response.text();

      g.setDispatch(async () => ({ status: 503, body: '{"error":"unavailable"}' }));
      response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) });
      expect(response.status).toBe(503);
      expect(response.headers.get('retry-after')).toBe('30');
      await response.text();
    });
  });

  test.each([[15, false], [15, true], [25, false], [25, true]])('degraded pool admits %s simultaneous generations (stream=%s) without waiting for earlier ones', async (count, stream) => {
    await withServer(async (base, g) => {
      let calls = 0;
      let allStarted!: () => void;
      const started = new Promise<void>(resolve => { allStarted = resolve; });
      let release!: () => void;
      const gate = new Promise<void>(resolve => { release = resolve; });
      g.setDispatch(async () => {
        calls++;
        if (calls === count) allStarted();
        await gate;
        return { status: 200, stream: replyStream() };
      });
      const requests = Array.from({ length: count }, () => fetch(base + '/v1/chat/completions', {
        method: 'POST', headers, body: completion(stream),
      }));
      const deadline = setTimeout(release, 1000);
      try {
        await Promise.race([started, gate]);
        expect(calls).toBe(count);
        expect(g.activeRequests['test-key']).toBe(count);
      } finally {
        clearTimeout(deadline);
        release();
        const replies = await Promise.all(requests);
        for (const response of replies) {
          expect(response.status).toBe(200);
          if (stream) expect(await response.text()).toContain('data: [DONE]');
          else expect((await response.json()).choices[0].message.content).toBe('OK');
        }
      }
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  test('non-stream clients receive assembled JSON and usage is counted once', async () => {
    await withServer(async (base, g) => {
      g.setDispatch(async () => ({ status: 200, stream: replyStream() }));
      const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) });
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('application/json');
      const doc = await response.json();
      expect(doc.choices[0].message.content).toBe('OK');
      expect(doc.usage.total_tokens).toBe(3);
      expect(g.getKeys()['test-key'].totalRequests).toBe(1);
      expect(g.getKeys()['test-key'].totalTokens).toBe(3);
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  test.each([false, true])('client disconnect cancels an open generation (stream=%s) and releases the key', async (stream) => {
    await withServer(async (base, g) => {
      let cancelled = false;
      let markStarted!: () => void;
      const started = new Promise<void>(resolve => { markStarted = resolve; });
      g.setDispatch(async () => {
        markStarted();
        return { status: 200, stream: new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}\n\n'));
          },
          cancel() { cancelled = true; },
        }) };
      });
      const control = new AbortController();
      const pending = fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(stream), signal: control.signal });
      await started;
      if (stream) {
        const response = await pending;
        await response.body!.cancel();
      } else {
        control.abort();
        await expect(pending).rejects.toThrow();
      }
      for (let i = 0; i < 100 && (!cancelled || g.activeRequests['test-key']); i++) {
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      expect(cancelled).toBe(true);
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  test('chat collector rejects a truncated stream with no finish_reason', async () => {
    const g = fixture();
    const broken = new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}\n\n'));
      controller.close();
    } });
    const collected = await g.collectChatStream(broken, 'big-pickle');
    expect(collected.status).toBe(502);
    expect(JSON.parse(collected.body).error.message).toContain('finish_reason');
  });

  test('stream monitor rejects premature chat EOF and only marks terminal streams complete', async () => {
    const g = fixture();
    let completed = 0, failed = 0;
    const broken = new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}\n\n'));
      controller.close();
    } });
    const monitoredBroken = g.monitorUpstreamSse(broken, 'chat', { onComplete: () => completed++, onFailure: () => failed++ });
    await expect(new Response(monitoredBroken).text()).rejects.toThrow('terminal event');
    expect(completed).toBe(0); expect(failed).toBe(1);

    const monitoredGood = g.monitorUpstreamSse(replyStream(), 'chat', { onComplete: () => completed++, onFailure: () => failed++ });
    expect(await new Response(monitoredGood).text()).toContain('data: [DONE]');
    expect(completed).toBe(1); expect(failed).toBe(1);
  });

  test('Responses translation rejects premature EOF instead of synthesizing stop', async () => {
    const g = fixture();
    const broken = new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"type":"response.output_text.delta","delta":"partial"}\n\n'));
      controller.close();
    } });
    const translated = g.responsesSseToChatSse(g.monitorUpstreamSse(broken, 'responses'), 'muse-spark-1.3-contributor-free');
    await expect(new Response(translated).text()).rejects.toThrow('terminal event');
  });

  test('mid-stream transport failures become explicit SSE error events', async () => {
    await withServer(async (base, g) => {
      const broken = new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('data: {"id":"chatcmpl-test","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}\n\n'));
          controller.error(new Error('Upstream response aborted before completion'));
        },
      });
      g.setDispatch(async () => ({ status: 200, stream: broken, streamHeaders: { 'content-type': 'text/event-stream' } }));
      const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(true) });
      expect(response.status).toBe(200);
      const body = await response.text();
      expect(body).toContain('event: error');
      expect(body).toContain('"type":"upstream_stream_error"');
      expect(body).toContain('"code":"upstream_stream_interrupted"');
      expect(body).toContain('Upstream response aborted before completion');
      expect(body).not.toContain('data: [DONE]');
    });
  });

  test('stream clients receive SSE through the terminal DONE frame', async () => {
    await withServer(async (base, g) => {
      g.setDispatch(async () => ({ status: 200, stream: replyStream(), streamHeaders: { 'content-type': 'text/event-stream', 'content-length': '99999' } }));
      const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(true) });
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/event-stream');
      expect(response.headers.get('content-length')).toBeNull();
      const body = await response.text();
      expect(body).toContain('"content":"OK"');
      expect(body).toContain('data: [DONE]');
      expect(g.getKeys()['test-key'].totalRequests).toBe(1);
      expect(g.getKeys()['test-key'].totalTokens).toBe(3);
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  function responsesStream() {
    const response = { id: 'resp-test', object: 'response', model: 'muse-spark-9-contributor-free', status: 'completed',
      output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'OK' }] }],
      usage: { input_tokens: 2, input_tokens_details: { cached_tokens: 1 }, output_tokens: 1, total_tokens: 3 } };
    const events = [{ type: 'response.created', response: { id: response.id, model: response.model } },
      { type: 'response.output_text.delta', delta: 'OK' }, { type: 'response.completed', response }];
    const bytes = new TextEncoder().encode(events.map(evt => `data: ${JSON.stringify(evt)}\n\n`).join(''));
    return new ReadableStream({ start(controller) {
      controller.enqueue(bytes.slice(0, 37)); controller.enqueue(bytes.slice(37)); controller.close();
    } });
  }
  function responsesModels(g: any) {
    g.setModels([{ id: 'big-pickle', object: 'model', endpoint: 'chat' },
      { id: 'muse-spark-9-contributor-free', object: 'model', endpoint: 'responses' },
      { id: 'muse-spark-9-free', canonical_id: 'muse-spark-9-contributor-free', object: 'model', endpoint: 'responses' }]);
  }

  test('unsupported models, invalid JSON and other endpoints are rejected without forwarding', async () => {
    await withServer(async (base, g) => {
      let calls = 0; g.setDispatch(async () => { calls++; return { status: 200, body: '{}' }; });
      for (const model of ['deepseek-v4-flash-free', 'jev-1.13-free', 'go-only-free', 'paid-model']) {
        const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: JSON.stringify({ model, messages: [] }) });
        expect(response.status).toBe(400);
      }
      expect((await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: '{' })).status).toBe(400);
      expect((await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: '' })).status).toBe(400);
      expect((await fetch(base + '/v1/messages', { method: 'POST', headers, body: '{}' })).status).toBe(404);
      expect(calls).toBe(0);
      expect(g.activeRequests['test-key'] || 0).toBe(0);
    });
  });

  test('future Muse aliases route Chat to Responses and assemble a Chat reply', async () => {
    await withServer(async (base, g) => {
      responsesModels(g);
      g.setDispatch(async (path: string, _method: string, _headers: any, body: string) => {
        expect(path).toBe('/v1/responses');
        const doc = JSON.parse(body);
        expect(doc.model).toBe('muse-spark-9-contributor-free');
        expect(doc.input[0].role).toBe('developer');
        expect(doc.input[0].content).toContain('Do not call tools');
        expect(doc.input[1].content).toBe('Hi');
        expect(doc.tools).toHaveLength(5);
        expect(doc.tool_choice).toBeUndefined();
        return { status: 200, stream: responsesStream() };
      });
      const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers,
        body: JSON.stringify({ model: 'muse-spark-9-free', messages: [{ role: 'user', content: 'Hi' }], stream: false }) });
      expect(response.status).toBe(200);
      expect((await response.json()).choices[0].message.content).toBe('OK');
      expect(g.getKeys()['test-key'].totalTokens).toBe(3);
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  test('native Responses streams keep flat tools and count nested usage once', async () => {
    await withServer(async (base, g) => {
      responsesModels(g);
      g.setDispatch(async (path: string, _method: string, _headers: any, body: string) => {
        expect(path).toBe('/v1/responses');
        const doc = JSON.parse(body);
        expect(doc.input).toBe('Hi');
        expect(doc.stream_options).toBeUndefined();
        expect(doc.tools.every((t: any) => !t.function && t.name)).toBe(true);
        expect(doc.tools.find((t: any) => t.name === 'shell').parameters.required).toEqual(['command']);
        return { status: 200, stream: responsesStream() };
      });
      const response = await fetch(base + '/v1/responses', { method: 'POST', headers,
        body: JSON.stringify({ model: 'muse-spark-9-contributor-free', input: 'Hi', stream: true,
          tools: [{ type: 'function', name: 'shell', parameters: { type: 'object', required: ['command'] } }] }) });
      expect(response.status).toBe(200);
      expect(await response.text()).toContain('response.completed');
      expect(g.getKeys()['test-key'].totalRequests).toBe(1);
      expect(g.getKeys()['test-key'].totalTokens).toBe(3);
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  test('non-stream Responses clients receive a Response object', async () => {
    await withServer(async (base, g) => {
      responsesModels(g); g.setDispatch(async () => ({ status: 200, stream: responsesStream() }));
      const response = await fetch(base + '/v1/responses', { method: 'POST', headers,
        body: JSON.stringify({ model: 'muse-spark-9-free', input: 'Hi', stream: false }) });
      const doc = await response.json();
      expect(response.status).toBe(200);
      expect(doc.object).toBe('response');
      expect(doc.output[0].content[0].text).toBe('OK');
      expect(doc.choices).toBeUndefined();
      expect(g.getKeys()['test-key'].totalTokens).toBe(3);
    });
  });


  test('usage-less completions still consume the request quota', async () => {
    await withServer(async (base, g) => {
      g.getKeys()['test-key'].maxRequests = 1;
      g.setDispatch(async () => ({ status: 200, stream: new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"OK"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'));
        controller.close();
      } }) }));
      const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) });
      expect(response.status).toBe(200);
      expect((await response.json()).choices[0].message.content).toBe('OK');
      expect(g.getKeys()['test-key'].requestCount).toBe(1);
      expect(g.getKeys()['test-key'].totalTokens).toBe(0);
      expect((await fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) })).status).toBe(403);
    });
  });

  test('requests whose bodies arrive together cannot exceed key concurrency', async () => {
    await withServer(async (base, g) => {
      g.getKeys()['test-key'].maxConcurrency = 1;
      let dispatches = 0;
      g.setDispatch(async () => {
        dispatches++;
        await new Promise(resolve => setTimeout(resolve, 25));
        return { status: 200, stream: replyStream() };
      });
      const body = completion(false);
      const pending: any[] = [];
      const statuses = [0, 1].map(() => new Promise<number>((resolve, reject) => {
        const req = http.request(base + '/v1/chat/completions', { method: 'POST', headers: { ...headers, 'content-length': Buffer.byteLength(body) } }, res => {
          res.resume(); res.once('end', () => resolve(res.statusCode!));
        });
        req.once('error', reject); req.flushHeaders(); pending.push(req);
      }));
      await new Promise(resolve => setTimeout(resolve, 15));
      for (const req of pending) req.end(body);
      expect((await Promise.all(statuses)).sort()).toEqual([200, 403]);
      expect(dispatches).toBe(1);
      expect(g.activeRequests['test-key']).toBe(0);
    });
  });

  test('Muse tool history and returned tool calls survive Chat translation', async () => {
    await withServer(async (base, g) => {
      responsesModels(g);
      g.setDispatch(async (_path: string, _method: string, _headers: any, body: string) => {
        const doc = JSON.parse(body);
        expect(doc.input[0]).toEqual({ type: 'function_call', call_id: 'call-old', name: 'read', arguments: '{"path":"x"}' });
        expect(doc.input[1]).toEqual({ type: 'function_call_output', call_id: 'call-old', output: 'result' });
        expect(doc.tool_choice).toEqual({ type: 'function', name: 'read' });
        const item = { id: 'fc-new', type: 'function_call', call_id: 'call-new', name: 'read', arguments: '{"path":"y"}' };
        const events = [{ type: 'response.output_item.added', output_index: 0, item: { ...item, arguments: '' } },
          { type: 'response.function_call_arguments.delta', item_id: item.id, delta: '{"path":' },
          { type: 'response.function_call_arguments.delta', item_id: item.id, delta: '"y"}' },
          { type: 'response.output_item.done', output_index: 0, item },
          { type: 'response.completed', response: { output: [item] } }];
        return { status: 200, stream: new ReadableStream({ start(controller) {
          controller.enqueue(new TextEncoder().encode(events.map(e => `data: ${JSON.stringify(e)}\n\n`).join(''))); controller.close();
        } }) };
      });
      const response = await fetch(base + '/v1/chat/completions', { method: 'POST', headers,
        body: JSON.stringify({ model: 'muse-spark-9-free', stream: false,
          tool_choice: { type: 'function', function: { name: 'read' } },
          messages: [{ role: 'assistant', content: null, tool_calls: [{ id: 'call-old', type: 'function', function: { name: 'read', arguments: '{"path":"x"}' } }] },
            { role: 'tool', tool_call_id: 'call-old', content: 'result' }] }) });
      expect(response.status).toBe(200);
      const choice = (await response.json()).choices[0];
      expect(choice.finish_reason).toBe('tool_calls');
      expect(choice.message.content).toBeNull();
      expect(choice.message.tool_calls).toEqual([{ id: 'call-new', type: 'function', function: { name: 'read', arguments: '{"path":"y"}' } }]);
      expect(g.getKeys()['test-key'].requestCount).toBe(1);
    });
  });

});

describe('Zen discovery and health', () => {
  const upstream = { data: [{ id: 'big-pickle' }, { id: 'muse-spark-9-contributor-free' }] };
  const metadata = { opencode: { npm: '@ai-sdk/openai-compatible', models: {
    'big-pickle': { cost: { input: 0, output: 0 }, modalities: { output: ['text'] } },
    'muse-spark-9-contributor-free': { cost: { input: 0, output: 0 }, modalities: { output: ['text'] }, provider: { npm: '@ai-sdk/openai' } },
  } } };
  const discovery = (g: any) => g.setFetcher(async (url: string) => url.includes('models.dev') ? metadata : upstream);

  test('concurrent discovery shares fetches and saves a validated catalog', async () => {
    const g = fixture();
    let calls = 0;
    g.setFetcher(async (url: string) => { calls++; return url.includes('models.dev') ? metadata : upstream; });
    const [a, b] = await Promise.all([g.fetchModelsFromUpstream(), g.fetchModelsFromUpstream()]);
    expect(calls).toBe(2);
    expect(a).toBe(b);
    expect(a.map((m: any) => m.id)).toEqual(['big-pickle', 'muse-spark-9-contributor-free', 'muse-spark-9-free']);
    expect(g.cachedDoc.metadata.opencode.models['big-pickle'].cost.input).toBe(0);
    expect(g.isResponsesOnlyModel('muse-spark-9-free')).toBe(true);
  });

  test('failed refresh keeps its original timestamp and recovers after retry delay', async () => {
    const g = fixture(); discovery(g);
    await g.fetchModelsFromUpstream();
    const checkedAt = g.catalogStatus().checkedAt;
    g.advance(301000);
    let calls = 0;
    g.setFetcher(async () => { calls++; throw new Error('offline'); });
    expect((await g.ensureModelCatalog()).length).toBe(3);
    expect(g.catalogStatus().checkedAt).toBe(checkedAt);
    expect(g.catalogStatus().stale).toBe(true);
    expect(g.catalogStatus().lastError).toBe('offline');
    await g.ensureModelCatalog();
    expect(calls).toBe(1); // metadata is cached; upstream retry is throttled.
    g.advance(31000); discovery(g);
    await g.ensureModelCatalog();
    expect(g.catalogStatus().lastError).toBeNull();
    expect(g.catalogStatus().checkedAt).toBe(g.now);
  });

  test('a valid pricing change withdraws all models without retaining stale free entries', async () => {
    const g = fixture(); discovery(g); await g.fetchModelsFromUpstream();
    g.advance(3600001);
    const paid = JSON.parse(JSON.stringify(metadata));
    for (const entry of Object.values<any>(paid.opencode.models)) entry.cost.input = 1;
    let calls = 0;
    g.setFetcher(async (url: string) => { calls++; return url.includes('models.dev') ? paid : upstream; });
    expect(await g.ensureModelCatalog()).toHaveLength(0);
    expect(g.catalogStatus().lastError).toBeNull();
    await g.ensureModelCatalog();
    expect(calls).toBe(2);
  });

  test('expired cache fails closed rather than serving indefinitely', async () => {
    const g = fixture(); discovery(g); await g.fetchModelsFromUpstream();
    g.advance(24 * 3600000 + 1);
    g.setFetcher(async () => { throw new Error('offline'); });
    await expect(g.ensureModelCatalog()).rejects.toThrow('offline');
  });

  test('cold discovery does not guess free IDs when Zen metadata is missing', async () => {
    const g = fixture();
    g.setFetcher(async (url: string) => url.includes('models.dev') ? { 'opencode-go': metadata.opencode } : upstream);
    await expect(g.ensureModelCatalog()).rejects.toThrow('opencode.models');
    expect(g.getModels()).toHaveLength(0);
  });

  test('empty or malformed refresh cannot erase a good catalog', async () => {
    const g = fixture(); discovery(g); await g.fetchModelsFromUpstream();
    g.advance(301000); g.setFetcher(async () => ({ data: [] }));
    expect((await g.ensureModelCatalog()).length).toBe(3);
    expect(g.catalogStatus().lastError).toContain('empty or invalid');
  });

  test('persisted catalog reload validates namespace, pricing and expiry', () => {
    const doc = { version: 1, upstream: 'https://opencode.ai/zen', checkedAt: 1_000_000, models: upstream, metadata };
    const g = fixture(); g.cache(doc); g.loadModelCatalog();
    expect(g.getModels()).toHaveLength(3);
    for (const invalid of [
      { ...doc, upstream: 'https://opencode.ai/zen/go' },
      { ...doc, checkedAt: 1_000_001 },
      { ...doc, checkedAt: 1_000_000 - 25 * 3600000 },
      { ...doc, metadata: { 'opencode-go': metadata.opencode } },
    ]) {
      const other = fixture(); other.cache(invalid); other.loadModelCatalog();
      expect(other.getModels()).toHaveLength(0);
    }
  });

  test('aliases share health and become available again after a successful probe', async () => {
    const g = fixture(); discovery(g); await g.fetchModelsFromUpstream();
    g.echo({ error: { message: 'unavailable' } }, 0, 400);
    await g.probeFreeModel('muse-spark-9-free');
    expect(g.workingFreeModelIds()).toEqual(['big-pickle']);
    expect(g.freeModelHealth.size).toBe(1);
    g.echo({ choices: [{ message: { content: 'OK' } }] }, 0);
    await g.probeFreeModel('muse-spark-9-contributor-free');
    expect(g.workingFreeModelIds()).toHaveLength(3);
    expect(g.freeModelHealth.size).toBe(1);
  });

  test('verification probes each canonical model once and does not immediately retry 429', async () => {
    const g = fixture(); discovery(g); await g.fetchModelsFromUpstream();
    let calls = 0;
    g.setProbeModel(async (id: string) => {
      calls++;
      const h = { verdict: 'unknown', status: 429, reason: 'rate limit', consecutiveFails: 0, checkedAt: g.now };
      g.freeModelHealth.set(id, h); return h;
    });
    await g.verifyFreeModels();
    expect(calls).toBe(2);
    expect(g.workingFreeModelIds()).toHaveLength(3);
  });

  test('empty HTTP 200 is a failure; 429 preserves a confirmed healthy model', async () => {
    const g = fixture();
    g.echo({}, 0); const empty = await g.probeFreeModel('big-pickle');
    expect(empty.status).toBe(502); expect(empty.verdict).toBe('unknown');
    g.echo({ choices: [{ message: { content: 'OK' } }] }, 0);
    expect((await g.probeFreeModel('big-pickle')).verdict).toBe('healthy');
    g.echo({ error: { message: 'rate limit' } }, 0, 429);
    expect((await g.probeFreeModel('big-pickle')).verdict).toBe('healthy');
  });

  test('model probes have a full request deadline', async () => {
    const g = fixture(); g.hang();
    const pending = g.probeFreeModel('big-pickle'); g.fireTimers(20000);
    expect((await pending).status).toBe(0);
  });

  test('tool-free callers keep inert upstream stubs and explicit no-tool instructions', () => {
    const g = fixture();
    for (const endpoint of ['chat', 'responses'] as const) {
      const shaped = g.shapeAgentRequest(JSON.stringify({
        model: endpoint === 'chat' ? 'space-bunny-free' : 'muse-spark-1.3-contributor-free',
        ...(endpoint === 'chat' ? { messages: [{ role: 'user', content: 'write a long text' }] } : { input: 'write a long text' }),
        stream: false,
      }), endpoint);
      const body = JSON.parse(shaped.body);
      expect(body.stream).toBe(true);
      expect(body.tools.length).toBe(5);
      expect(body.tool_choice).toBeUndefined();
      expect(body.tools.every((tool: any) => String(tool.description || tool.function?.description || '').includes('do not call'))).toBe(true);
      if (endpoint === 'chat') expect(body.messages[0].content).toContain('Do not call tools');
      else expect(body.instructions).toContain('Do not call tools');
    }
  });

  test('tool policy guard blocks compatibility stubs that the caller did not declare', async () => {
    const g = fixture();
    const toolEvent = (name: string) => new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"x","type":"function","function":{"name":"${name}","arguments":"{}"}}]},"finish_reason":null}]}\n\n`));
      controller.close();
    }});

    await expect(new Response(g.guardToolPolicyStream(toolEvent('bash'), 'chat', new Set())).text())
      .rejects.toThrow('Blocked undeclared upstream tool call: bash');

    const allowed = new Set(['read']);
    expect(await new Response(g.guardToolPolicyStream(toolEvent('read'), 'chat', allowed)).text()).toContain('"name":"read"');
    await expect(new Response(g.guardToolPolicyStream(toolEvent('bash'), 'chat', allowed)).text())
      .rejects.toThrow('Blocked undeclared upstream tool call: bash');

    expect([...g.declaredToolNames(JSON.stringify({ tools: [{ type: 'function', function: { name: 'read' } }] }))]).toEqual(['read']);
    expect([...g.authorizedToolNames(JSON.stringify({ tool_choice: { type: 'function', function: { name: 'read' } } }))]).toEqual(['read']);
    expect(g.requestDeclaresTools(JSON.stringify({ tools: [] }))).toBe(false);
    expect(g.requestDeclaresTools(JSON.stringify({ tools: [{ type: 'function', function: { name: 'read' } }] }))).toBe(true);
  });

  test('agent shaping preserves client tools and appends missing core tools', () => {
    const g = fixture();
    const shaped = g.shapeAgentRequest(JSON.stringify({
      model: 'big-pickle',
      messages: [{ role: 'user', content: 'hello' }],
      stream: true,
      stream_options: { include_usage: true },
      tools: [{
        type: 'function',
        function: {
          name: 'read',
          description: 'Pi read tool',
          parameters: { type: 'object', properties: { path: { type: 'string' } } },
        },
      }],
    }), 'chat');
    const body = JSON.parse(shaped.body);
    const names = body.tools.map((tool: any) => tool.function?.name || tool.name);
    expect(shaped.reshaped).toBe(true);
    expect(names.filter((name: string) => name === 'read')).toHaveLength(1);
    for (const name of ['bash', 'edit', 'glob', 'grep', 'read']) expect(names).toContain(name);
    expect(body.tools.find((tool: any) => tool.function?.name === 'read').function.description).toBe('Pi read tool');
  });

  test('native headers retain session affinity and current client version', () => {
    const g = fixture();
    const session = 'ses_f02a200f4fferWXQQFP3o9w8x9';
    const headers = g.collectHeadersFromReq({ headers: { 'x-opencode-session': session, 'user-agent': 'opencode/latest/3.0.0/cli' } });
    expect(headers['user-agent']).toBe('opencode/latest/3.0.0/cli');
    for (const name of ['x-opencode-session', 'x-opencode-session-id', 'x-session-affinity', 'x-session-id']) expect(headers[name]).toBe(session);
    expect(headers.authorization).toBe('Bearer public');
  });
});

describe('stream admission and exit routing', () => {
  const body = JSON.stringify({ model: 'big-pickle', messages: [{ role: 'user', content: 'Hi' }], stream: true });
  const first = 'data: {"choices":[{"delta":{"content":"OK"},"finish_reason":null}]}\n\n';
  const final = 'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n';
  function pool(...addresses: string[]) {
    return { keyId: 'key', slots: addresses.map(addr => ({ addr, url: `http://${addr}`, proto: 'http' })), rrCursor: 0, lastUsedAt: 0 };
  }

  test('disconnect during an unfinished body settles the read and releases its listeners', async () => {
    const g = fixture();
    for (const alreadyAborted of [false, true]) {
      const req = new EventEmitter();
      const control = new AbortController();
      if (alreadyAborted) control.abort();
      const reading = g.readBody(req, control.signal);
      if (!alreadyAborted) { req.emit('data', Buffer.from('{')); control.abort(); }
      await expect(reading).rejects.toThrow('Client disconnected during request body');
      expect(req.eventNames()).toHaveLength(0);
    }
  });

  test('split SSE frames and heartbeats preserve bytes until productive output', async () => {
    const g = fixture();
    const chunks = [': keepalive\n\n', 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n', first.slice(0, 20), first.slice(20), final];
    g.echoStream(chunks);
    const result = await g.doHttpsStream('/v1/chat/completions', 'POST', {}, body);
    expect(await new Response(result.stream).text()).toBe(chunks.join(''));
    expect(g.destroyedRequests).toBe(0);
  });

  test('heartbeat-only EOF is retryable before stream handoff', async () => {
    const g = fixture();
    g.echoStream([': keepalive\n\n']);
    await expect(g.doHttpsStream('/v1/chat/completions', 'POST', {}, body)).rejects.toThrow('before a productive event');
    expect(g.destroyedRequests).toBe(1);
  });

  test('a real socket can wait beyond its first-byte timeout for productive output', async () => {
    const chunks = ': heartbeat\n\n' + first + final;
    const worker = http.createServer((req, res) => {
      req.resume();
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(': heartbeat\n\n');
      const timer = setTimeout(() => res.end(first + final), 160);
      res.once('close', () => clearTimeout(timer));
    });
    await new Promise<void>(resolve => worker.listen(0, '127.0.0.1', resolve));
    const port = (worker.address() as any).port;
    const g = fixture(2500, { STREAM_FIRST_BYTE_TIMEOUT_MS: '50', STREAM_FIRST_EVENT_TIMEOUT_MS: '1000' });
    g.setHttpsRequest((_url: any, options: any, callback: any) => http.request(`http://127.0.0.1:${port}/`, options, callback));
    try {
      const result = await g.doHttpsStream('/v1/chat/completions', 'POST', {}, body);
      expect(result.status).toBe(200);
      expect(await new Response(result.stream).text()).toBe(chunks);
    } finally {
      worker.closeAllConnections();
      await new Promise<void>(resolve => worker.close(() => resolve()));
    }
  });

  test('a real socket still enforces stream inactivity after productive output', async () => {
    const worker = http.createServer((req, res) => {
      req.resume();
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(first);
    });
    await new Promise<void>(resolve => worker.listen(0, '127.0.0.1', resolve));
    const port = (worker.address() as any).port;
    const g = fixture(2500, { STREAM_FIRST_BYTE_TIMEOUT_MS: '50', STREAM_FIRST_EVENT_TIMEOUT_MS: '1000', STREAM_IDLE_TIMEOUT_MS: '80' });
    g.setHttpsRequest((_url: any, options: any, callback: any) => http.request(`http://127.0.0.1:${port}/`, options, callback));
    try {
      const result = await g.doHttpsStream('/v1/chat/completions', 'POST', {}, body);
      await expect(new Response(result.stream).text()).rejects.toThrow('inactivity timeout');
    } finally {
      worker.closeAllConnections();
      await new Promise<void>(resolve => worker.close(() => resolve()));
    }
  });

  test('heartbeats cannot extend the initial productive-event deadline', async () => {
    const g = fixture();
    g.echoStream([': keepalive\n\n'], true);
    const pending = g.doHttpsStream('/v1/chat/completions', 'POST', {}, body);
    await new Promise(resolve => queueMicrotask(resolve));
    g.fireTimers(30000);
    await new Promise(resolve => setTimeout(resolve, 5));
    expect(g.destroyedRequests).toBe(0);
    g.fireTimers(120000);
    await expect(pending).rejects.toThrow('first SSE event timeout');
    expect(g.destroyedRequests).toBe(1);
  });

  test('initial prelude size is bounded before handoff', async () => {
    const g = fixture();
    g.echoStream([':' + 'x'.repeat(1024 * 1024)]);
    await expect(g.doHttpsStream('/v1/chat/completions', 'POST', {}, body)).rejects.toThrow('prelude exceeds');
  });

  test('early 200 error events rotate exits and apply a rate-limit cooldown', async () => {
    const g = fixture();
    g.queueStreams([['data: {"error":{"message":"Rate limit exceeded","type":"rate_limit"}}\n\n'], [first, final]]);
    const p = pool('203.0.113.1:8080', '203.0.113.2:8080');
    const result = await g.dispatch('/v1/chat/completions', 'POST', {}, body, p);
    expect(await new Response(result.stream).text()).toBe(first + final);
    expect(g.requests).toBe(2);
    expect(g.isExitUsable(p.slots[0].addr, 'big-pickle')).toBe(false);
    expect(g.exitActiveRequests.size).toBe(0);
  });

  test('Responses metadata alone does not commit an empty stream', async () => {
    const g = fixture();
    g.echoStream(['data: {"type":"response.created","response":{"id":"resp-test"}}\n\n']);
    await expect(g.doHttpsStream('/v1/responses', 'POST', {}, body)).rejects.toThrow('before a productive event');
    expect(g.initialSseEvent('data: {"type":"response.function_call_arguments.delta","delta":"{"}', true)).toBe(true);
  });

  test('client abort before first output cancels work without penalizing the exit', async () => {
    const g = fixture();
    g.hang();
    const control = new AbortController();
    const p = pool('203.0.113.1:8080');
    const pending = g.dispatch('/v1/chat/completions', 'POST', {}, body, p, 0, new Set(), control.signal);
    control.abort(new Error('cancelled by caller'));
    await expect(pending).rejects.toThrow('cancelled by caller');
    expect(g.exitHealth.size).toBe(0);
    expect(g.exitActiveRequests.size).toBe(0);
    expect(g.requests).toBe(1);
    expect(g.destroyedRequests).toBe(1);
  });

  test('abort after first output releases the exit and does not replay the request', async () => {
    const g = fixture();
    g.echoStream([first], true);
    const control = new AbortController();
    const p = pool('203.0.113.1:8080');
    const result = await g.dispatch('/v1/chat/completions', 'POST', {}, body, p, 0, new Set(), control.signal);
    expect(g.exitActiveRequests.get(p.slots[0].addr)).toBe(1);
    control.abort(new Error('cancelled by caller'));
    await expect(new Response(result.stream).text()).rejects.toThrow('cancelled by caller');
    expect(g.exitHealth.size).toBe(0);
    expect(g.exitActiveRequests.size).toBe(0);
    expect(g.requests).toBe(1);
  });

  test('session affinity survives sequential turns but gives parallel traffic a free exit', () => {
    const g = fixture();
    const p = pool('203.0.113.1:8080', '203.0.113.2:8080', '203.0.113.3:8080');
    const chosen = g.choosePoolSlot(p, 'big-pickle', 'session-a', new Set());
    expect(g.choosePoolSlot(p, 'big-pickle', 'session-a', new Set()).addr).toBe(chosen.addr);
    g.exitActiveRequests.set(chosen.addr, 1);
    const next = g.choosePoolSlot(p, 'big-pickle', 'session-a', new Set());
    expect(next.addr).not.toBe(chosen.addr);
    g.noteExitFailure(next.addr, 429);
    expect(g.choosePoolSlot(p, 'big-pickle', 'session-a', new Set()).addr).not.toBe(next.addr);
  });

  test('exit occupancy remains reserved until stream cancellation', async () => {
    const g = fixture();
    g.echoStream([first], true);
    const p = pool('203.0.113.1:8080');
    const result = await g.dispatch('/v1/chat/completions', 'POST', {}, body, p);
    expect(g.exitActiveRequests.get(p.slots[0].addr)).toBe(1);
    await result.stream.cancel();
    expect(g.exitActiveRequests.size).toBe(0);
    expect(g.exitHealth.size).toBe(0);
  });

  test('a truncated handed-off stream records a strike, releases occupancy, and never replays', async () => {
    const g = fixture();
    g.echoStream([first]);
    const p = pool('203.0.113.1:8080');
    const result = await g.dispatch('/v1/chat/completions', 'POST', {}, body, p);
    await expect(new Response(result.stream).text()).rejects.toThrow();
    expect(g.exitHealth.get(p.slots[0].addr)?.fails).toBe(1);
    expect(g.exitActiveRequests.size).toBe(0);
    expect(g.requests).toBe(1);
  });

  test('validated spare exits are probed once per top-up rather than once per winner', async () => {
    const g = fixture();
    const items = Array.from({ length: 6 }, (_, i) => candidate(i + 1, i === 0 ? 'key' : null));
    const p = pool(items[0].address);
    g.setCandidates(items);
    g.keySlotPools.set('key', p);
    for (const c of items) g.markValidated(c.address);
    const calls = new Map<string, number>();
    g.setProbe(async (c: any) => { calls.set(c.address, (calls.get(c.address) || 0) + 1); return { ok: true, latencyMs: 100 }; });
    expect(await g.topUpKeySlotPool(p)).toBe(2);
    expect(Math.max(...calls.values())).toBe(1);
    expect(p.slots).toHaveLength(3);
  });

  test('a replaced pool starts its own top-up while stale work cannot clear its mutex', async () => {
    const g = fixture();
    const items = Array.from({ length: 6 }, (_, i) => candidate(i + 1, i === 0 ? 'key' : null));
    g.setCandidates(items);
    for (const c of items) g.markValidated(c.address);
    let releaseOld!: () => void;
    let releaseNew!: () => void;
    const oldGate = new Promise<void>(resolve => { releaseOld = resolve; });
    const newGate = new Promise<void>(resolve => { releaseNew = resolve; });
    let phase = 0;
    let probes = 0;
    g.setProbe(async () => { probes++; await (phase === 0 ? oldGate : newGate); return { ok: true, latencyMs: 100 }; });
    const oldPool = pool(items[0].address);
    g.keySlotPools.set('key', oldPool);
    const staleWork = g.topUpKeySlotPool(oldPool);
    phase = 1;
    const livePool = pool(items[0].address);
    g.keySlotPools.set('key', livePool);
    const liveWork = g.topUpKeySlotPool(livePool);
    releaseOld();
    expect(await staleWork).toBe(0);
    const sharedWork = g.topUpKeySlotPool(livePool);
    expect(probes).toBe(10);
    releaseNew();
    expect(await liveWork).toBe(2);
    expect(await sharedWork).toBe(2);
    expect(oldPool.slots).toHaveLength(1);
    expect(livePool.slots).toHaveLength(3);
  });

  test('parallel demand grows validated routing capacity without capping requests', async () => {
    const g = fixture();
    const items = Array.from({ length: 18 }, (_, i) => candidate(i + 1, i < 3 ? 'key' : null));
    const p = pool(...items.slice(0, 3).map(c => c.address));
    g.setCandidates(items);
    g.keySlotPools.set('key', p);
    for (const c of items) g.markValidated(c.address);
    g.activeRequests.key = 25;
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));
    const [a, b] = await Promise.all([g.getKeySlotPool('key'), g.getKeySlotPool('key')]);
    expect(a).toBe(b);
    await g.topUpKeySlotPool(p);
    expect(p.slots).toHaveLength(16);
    expect(g.activeRequests.key).toBe(25);
    expect(new Set(p.slots.map((s: any) => s.addr)).size).toBe(16);
  });

  test('request path never waits for pool replenishment', async () => {
    const g = fixture();
    const items = Array.from({ length: 6 }, (_, i) => candidate(i + 1, i < 3 ? 'key' : null));
    const p = pool(...items.slice(0, 3).map(c => c.address));
    g.setCandidates(items);
    g.keySlotPools.set('key', p);
    for (const c of items) g.markValidated(c.address);
    g.activeRequests.key = 6;
    g.setProbe(() => new Promise(() => {}));
    const served = await Promise.race([
      g.getKeySlotPool('key'),
      new Promise(resolve => setTimeout(() => resolve('blocked'), 50)),
    ]);
    expect(served).toBe(p);
  });

  test('an empty top-up is not retried by every following request', async () => {
    const g = fixture();
    const items = Array.from({ length: 6 }, (_, i) => candidate(i + 1, i < 3 ? 'key' : null));
    const p = pool(...items.slice(0, 3).map(c => c.address));
    g.setCandidates(items);
    g.keySlotPools.set('key', p);
    for (const c of items) g.markValidated(c.address);
    g.activeRequests.key = 6;
    let probes = 0;
    g.setProbe(async () => { probes++; return { ok: false, latencyMs: 0 }; });
    await g.getKeySlotPool('key');
    await g.topUpKeySlotPool(p);
    const afterFirst = probes;
    expect(afterFirst).toBeGreaterThan(0);
    for (let i = 0; i < 5; i++) await g.getKeySlotPool('key');
    await new Promise(resolve => setTimeout(resolve, 5));
    expect(probes).toBe(afterFirst);
  });

  test('a model-specific top-up does not settle for a concurrent generic one', async () => {
    const g = fixture();
    const items = Array.from({ length: 12 }, (_, i) => candidate(i + 1, i < 2 ? 'key' : null));
    items[2].latency = 1;
    const p = pool(items[0].address, items[1].address);
    g.setCandidates(items);
    g.keySlotPools.set('key', p);
    for (const c of items) g.markValidated(c.address);
    for (const c of items.slice(0, 3)) g.exitModelBans.set(c.address, new Map([['m', { fails: 3, bannedUntil: g.now + 600_000 }]]));
    g.setProbe(healthy);
    const generic = g.topUpKeySlotPool(p);
    const forModel = g.topUpKeySlotPool(p, 'm');
    await Promise.all([generic, forModel]);
    expect(p.slots.some((slot: any) => g.isExitUsable(slot.addr, 'm'))).toBe(true);
  });

  test('a top-up with no deficit does not arm the empty backoff', async () => {
    const g = fixture();
    const items = Array.from({ length: 6 }, (_, i) => candidate(i + 1, i < 3 ? 'key' : null));
    const p = pool(...items.slice(0, 3).map(c => c.address));
    g.setCandidates(items);
    g.keySlotPools.set('key', p);
    for (const c of items) g.markValidated(c.address);
    g.setProbe(healthy);
    expect(await g.topUpKeySlotPool(p)).toBe(0);
    g.activeRequests.key = 5;
    await g.getKeySlotPool('key');
    for (let i = 0; i < 50 && p.slots.length < 5; i++) await new Promise(resolve => setTimeout(resolve, 2));
    expect(p.slots).toHaveLength(5);
  });

  test('a fresh pool grows when parallel demand arrives during its initial allocation', async () => {
    const g = fixture();
    const items = Array.from({ length: 18 }, (_, i) => candidate(i + 1));
    g.setCandidates(items);
    for (const c of items) g.markValidated(c.address);
    g.setScreen(healthy);
    g.setProbe(async () => ({ ok: true, latencyMs: 100 }));
    g.activeRequests.key = 1;
    const firstAllocation = g.getKeySlotPool('key');
    g.activeRequests.key = 25;
    const burst = Array.from({ length: 24 }, () => g.getKeySlotPool('key'));
    const pools = await Promise.all([firstAllocation, ...burst]);
    expect(pools.every(p => p === pools[0])).toBe(true);
    await g.topUpKeySlotPool(pools[0]);
    expect(pools[0].slots).toHaveLength(16);
    expect(new Set(pools[0].slots.map((s: any) => s.addr)).size).toBe(16);
    expect(g.activeRequests.key).toBe(25);
  });

  test('fresh allocation serves every waiting request before slow extra probes finish', async () => {
    const g = fixture();
    const items = Array.from({ length: 8 }, (_, i) => candidate(i + 1, i < 3 ? 'key' : null));
    const p = pool(...items.slice(0, 3).map(c => c.address));
    g.setCandidates(items);
    for (const c of items) g.markValidated(c.address);
    let ready!: () => void;
    let finishProbes!: () => void;
    const allocationGate = new Promise<void>(resolve => { ready = resolve; });
    const probeGate = new Promise<void>(resolve => { finishProbes = resolve; });
    g.setAllocator(async () => { await allocationGate; return p; });
    g.setProbe(async () => { await probeGate; return { ok: true, latencyMs: 100 }; });
    g.activeRequests.key = 1;
    const firstAllocation = g.getKeySlotPool('key');
    g.activeRequests.key = 6;
    const waiting = [firstAllocation, ...Array.from({ length: 5 }, () => g.getKeySlotPool('key'))];
    ready();
    try {
      const result = await Promise.race([
        Promise.all(waiting), new Promise(resolve => setTimeout(() => resolve('blocked'), 50)),
      ]);
      expect(Array.isArray(result)).toBe(true);
      expect((result as any[]).every(served => served === p)).toBe(true);
      expect(p.slots).toHaveLength(3);
    } finally { finishProbes(); }
    await g.topUpKeySlotPool(p);
    expect(p.slots).toHaveLength(6);
  });
});

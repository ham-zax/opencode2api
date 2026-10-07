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

function fixture(probeTimeout = 2500) {
  let now = 1_000_000;
  let requests = 0;
  let replyStatus = 200;
  const stateFiles = new Map<string, string>();
  let reply: any = { ip: '203.0.113.1', country: 'US' };
  let latency = 1700;
  let hang = false;
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
    process: { cwd: () => path.resolve(import.meta.dir, '..'), env: { PROXY_PROBE_TIMEOUT: String(probeTimeout) } },
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
        req.destroy = () => {};
        req.write = () => true;
        req.end = () => queueMicrotask(() => {
          if (hang) return;
          now += latency;
          const res = new EventEmitter() as any;
          res.statusCode = replyStatus;
          res.headers = {};
          callback(res);
          res.emit('data', Buffer.from(JSON.stringify(reply)));
          res.emit('end');
        });
        return req;
      },
    },
  });
  const gateway: any = vm.runInContext(definitions + `
    ({ coarseScreen, probe, backgroundProbeSweep, allocateKeySlots, loadCandidates,
       replaceFailedSlot, getKeySlotPool, releaseKeySlots,
       freeExitCount, currentDemandKeyCount, operationalPoolTarget, currentPoolState, poolGenerationConcurrencyCap, waitForPoolGenerationCapacity, markValidated, noteExitFailure,
       isExitUsable, validatedExits, exitHealth, exitModelBans, keySlotPools, coarseSeen,
       saveProxyHealthState, loadProxyHealthState,
       setCandidates(value) { candidates = value; },
       getCandidates() { return candidates; },
       setSources(value) { proxySources = value; },
       setCustom(value) { customProxyItems = value; },
       setScreen(value) { coarseScreen = value; },
       setProbe(value) { probe = value; },
       setDispatch(value) { dispatch = value; },
       setKeys(value) { apiKeys = value; },
       getKeys() { return apiKeys; },
       setModels(value) { cachedModels = value; cachedModelsTime = Date.now(); },
       activeRequests, fetchModelsFromUpstream, ensureModelCatalog, loadModelCatalog,
       normalizeFreeModelAlias, isResponsesOnlyModel, shapeAgentRequest, chatBodyToResponses, collectHeadersFromReq,
       requestDeclaresTools, guardToolFreeStream, monitorUpstreamSse, collectChatStream, responsesSseToChatSse, dispatchDirect, sendJson, sendJsonWithHeaders,
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
    get now() { return now; },
    advance(ms: number) { now += ms; },
    echo(value: any, ms = 1700, status = 200) { reply = value; latency = ms; replyStatus = status; },
    cache(doc: any) { stateFiles.set(path.resolve(import.meta.dir, '../models_cache.json'), JSON.stringify(doc)); },
    get cachedDoc() { const value = stateFiles.get(path.resolve(import.meta.dir, '../models_cache.json')); return value ? JSON.parse(value) : undefined; },
    get healthDoc() { const value = stateFiles.get(path.resolve(import.meta.dir, '../proxy_health_cache.json')); return value ? JSON.parse(value) : undefined; },
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

  test('generation concurrency cap tightens as pool capacity falls', () => {
    const g = fixture();
    expect(g.poolGenerationConcurrencyCap('degraded')).toBe(1);
    expect(g.poolGenerationConcurrencyCap('constrained')).toBe(2);
    expect(g.poolGenerationConcurrencyCap('watch')).toBeGreaterThanOrEqual(2);
    expect(Number.isFinite(g.poolGenerationConcurrencyCap('healthy'))).toBe(false);
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
    g.echo({ type: 'response.output_text.delta', delta: 'partial' }, 100, 200);
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
      expect(doc.timeouts.streamIdleMs).toBe(600000);
      expect(doc.timeouts.proxyConnectMs).toBe(15000);
      expect(doc.directEgress.usable).toBe(true);
      expect(doc.directEgress.retryAfterSeconds).toBe(0);
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

  test('degraded pool queues a brief concurrent burst instead of rejecting it', async () => {
    await withServer(async (base, g) => {
      let calls = 0;
      g.setDispatch(async () => {
        calls++;
        if (calls === 1) await new Promise(resolve => setTimeout(resolve, 25));
        return { status: 200, stream: replyStream() };
      });
      const first = fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) });
      await new Promise(resolve => setTimeout(resolve, 5));
      const second = fetch(base + '/v1/chat/completions', { method: 'POST', headers, body: completion(false) });
      const [a, b] = await Promise.all([first, second]);
      expect(a.status).toBe(200);
      expect(b.status).toBe(200);
      expect(calls).toBe(2);
    });
  });

  test('degraded pool eventually returns retry guidance when saturation persists', async () => {
    const g = fixture();
    g.activeRequests['test-key'] = 1;
    const pending = g.waitForPoolGenerationCapacity('test-key', 5);
    g.advance(5);
    g.fireTimers(5);
    const result = await pending;
    expect(result.ok).toBe(false);
    expect(result.state).toBe('degraded');
    expect(result.cap).toBe(1);
    expect(result.waitedMs).toBeGreaterThanOrEqual(5);
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

  test('tool-free response guard rejects synthetic tool calls', async () => {
    const g = fixture();
    const chat = new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"x","type":"function","function":{"name":"bash","arguments":"{}"}}]},"finish_reason":null}]}\n\n`));
      controller.close();
    }});
    await expect(new Response(g.guardToolFreeStream(chat, 'chat')).text()).rejects.toThrow('synthetic tool call');
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

#!/usr/bin/env bun

import http from 'node:http';
import { spawn, type ChildProcess } from 'node:child_process';
import path from 'node:path';

export interface ManagedRuntime {
  generation: string;
  port: number;
  inFlight: number;
  draining: boolean;
  agent: http.Agent;
  markDraining(): void;
  stop(force?: boolean): Promise<void>;
}

export type RuntimeFactory = (port: number, generation: string) => Promise<ManagedRuntime>;

export interface RuntimeSupervisorOptions {
  workerPorts: number[];
  factory: RuntimeFactory;
  drainTimeoutMs?: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function chooseStandbyPort(activePort: number | null, ports: number[], unavailable: Iterable<number> = []): number {
  if (ports.length < 2) throw new Error('At least two worker ports are required for zero-downtime reloads');
  const blocked = new Set(unavailable);
  if (activePort != null) blocked.add(activePort);
  const free = ports.find(port => !blocked.has(port));
  if (free == null) throw new Error('No standby worker port is available while an older generation is still draining');
  return free;
}

export class RuntimeSupervisor {
  private readonly workerPorts: number[];
  private readonly factory: RuntimeFactory;
  private readonly drainTimeoutMs: number;
  private generationCounter = 0;
  private reloadPromise: Promise<ManagedRuntime> | null = null;
  active: ManagedRuntime | null = null;
  draining = new Set<ManagedRuntime>();

  constructor(options: RuntimeSupervisorOptions) {
    this.workerPorts = options.workerPorts;
    this.factory = options.factory;
    this.drainTimeoutMs = options.drainTimeoutMs ?? 15 * 60_000;
  }

  private nextGeneration(): string {
    this.generationCounter++;
    return `${Date.now()}-${process.pid}-${this.generationCounter}`;
  }

  async start(): Promise<ManagedRuntime> {
    if (this.active) return this.active;
    const runtime = await this.factory(chooseStandbyPort(null, this.workerPorts), this.nextGeneration());
    this.active = runtime;
    return runtime;
  }

  async reload(): Promise<ManagedRuntime> {
    if (this.reloadPromise) return this.reloadPromise;
    this.reloadPromise = (async () => {
      const old = this.active;
      const port = chooseStandbyPort(
        old?.port ?? null,
        this.workerPorts,
        [...this.draining].map(runtime => runtime.port),
      );
      const next = await this.factory(port, this.nextGeneration());

      // Atomic routing switch: every request accepted after this assignment
      // goes to the validated replacement. Existing proxy requests keep their
      // already-open connection to the old runtime.
      this.active = next;

      if (old) {
        old.draining = true;
        this.draining.add(old);
        try { old.markDraining(); } catch {}
        void this.drainAndStop(old);
      }
      return next;
    })();

    try {
      return await this.reloadPromise;
    } finally {
      this.reloadPromise = null;
    }
  }

  private async drainAndStop(runtime: ManagedRuntime): Promise<void> {
    const deadline = Date.now() + this.drainTimeoutMs;
    while (runtime.inFlight > 0 && Date.now() < deadline) await sleep(100);
    const force = runtime.inFlight > 0;
    await runtime.stop(force);
    runtime.agent.destroy();
    this.draining.delete(runtime);
  }

  status() {
    return {
      active: this.active ? {
        generation: this.active.generation,
        port: this.active.port,
        inFlight: this.active.inFlight,
      } : null,
      draining: [...this.draining].map(runtime => ({
        generation: runtime.generation,
        port: runtime.port,
        inFlight: runtime.inFlight,
      })),
      reloading: !!this.reloadPromise,
    };
  }

  handle = (req: http.IncomingMessage, res: http.ServerResponse) => {
    if (req.url === '/__supervisor/status' && req.method === 'GET') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(this.status()));
      return;
    }

    const runtime = this.active;
    if (!runtime) {
      res.writeHead(503, { 'content-type': 'application/json', 'retry-after': '1' });
      res.end(JSON.stringify({ type: 'runtime_starting', message: 'Gateway runtime is starting.' }));
      return;
    }

    runtime.inFlight++;
    let finished = false;
    let cancelled = false;
    let upstreamRes: http.IncomingMessage | null = null;
    const finish = () => {
      if (finished) return;
      finished = true;
      runtime.inFlight = Math.max(0, runtime.inFlight - 1);
    };
    // Per-request cancellation only: destroys this proxy's upstream
    // request/response pair. Never touches runtime.agent, so unrelated
    // keep-alive sockets stay usable.
    let upstream!: http.ClientRequest;
    const cancelUpstream = () => {
      if (cancelled) return;
      cancelled = true;
      try { upstreamRes?.destroy(); } catch {}
      try { upstream.destroy(); } catch {}
    };

    upstream = http.request({
      host: '127.0.0.1',
      port: runtime.port,
      method: req.method,
      path: req.url,
      headers: { ...req.headers, host: `127.0.0.1:${runtime.port}` },
      agent: runtime.agent,
    }, incoming => {
      upstreamRes = incoming;
      if (cancelled || res.destroyed) {
        // Client went away before headers completed (or during the race
        // to pipe). Release the worker connection; finish fires on the
        // resulting 'close' below so drain occupancy is held until the
        // worker connection has actually been cancelled.
        try { incoming.destroy(); } catch {}
      } else {
        const headers = { ...incoming.headers };
        res.writeHead(incoming.statusCode || 502, headers);
        incoming.pipe(res);
      }
      incoming.once('end', finish);
      incoming.once('close', finish);
      incoming.once('error', incomingError => {
        finish();
        if (!cancelled && !res.destroyed && res.headersSent) {
          try { res.destroy(incomingError as Error); } catch {}
        }
      });
    });

    upstream.once('error', error => {
      finish();
      if (cancelled || res.destroyed) return;
      if (!res.headersSent) {
        res.writeHead(502, { 'content-type': 'application/json', 'retry-after': '1' });
        res.end(JSON.stringify({ type: 'runtime_proxy_error', message: error.message }));
      } else {
        res.destroy(error);
      }
    });
    // No response received yet: 'close' means the worker connection for
    // this request was released (including via cancelUpstream). When a
    // response exists, its own 'close' drives finish so occupancy is not
    // dropped while the body is still streaming.
    upstream.once('close', () => {
      if (!upstreamRes) finish();
    });

    req.once('aborted', () => {
      if (finished) return;
      cancelUpstream();
    });
    // Authoritative client-disconnect signal for both pre-header aborts
    // and mid-SSE disconnects. Cancels the worker connection but does not
    // drop occupancy here: finish waits for the upstream 'close' so drain
    // accounting cannot reach zero before cancellation propagates. Guarded
    // by finished/writableEnded so normally completed responses are left
    // alone and their keep-alive socket stays reusable.
    res.once('close', () => {
      if (finished) return;
      if (!res.writableEnded) cancelUpstream();
    });
    req.pipe(upstream);
  };
}

const FRONT_PORT = parseInt(process.env.PORT || '13339');
const FRONT_HOST = process.env.HOST || '0.0.0.0';
const WORKER_PORTS = (process.env.WORKER_PORTS || '13439,13440')
  .split(',')
  .map(value => Number(value.trim()))
  .filter(value => Number.isInteger(value) && value > 0 && value <= 65535);
const WORKER_READY_TIMEOUT_MS = parseInt(process.env.WORKER_READY_TIMEOUT_MS || '120000');
const DRAIN_TIMEOUT_MS = parseInt(process.env.DRAIN_TIMEOUT_MS || '900000');
const BUN_BIN = process.env.BUN_BIN || process.execPath;
const ROOT = process.cwd();

async function waitForWorker(port: number, generation: string, child: ChildProcess): Promise<void> {
  const deadline = Date.now() + WORKER_READY_TIMEOUT_MS;
  let lastError = 'worker not ready';
  while (Date.now() < deadline) {
    if (child.exitCode != null) throw new Error(`worker exited before readiness (code=${child.exitCode})`);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/status`, { signal: AbortSignal.timeout(2000) });
      if (response.ok) {
        const doc: any = await response.json();
        if (doc?.ok === true && doc?.runtime?.generation === generation) return;
        lastError = `unexpected generation ${doc?.runtime?.generation || 'missing'}`;
      } else {
        lastError = `HTTP ${response.status}`;
      }
    } catch (error: any) {
      lastError = error?.message || String(error);
    }
    await sleep(500);
  }
  throw new Error(`replacement worker failed readiness: ${lastError}`);
}

function productionFactory(): RuntimeFactory {
  return async (port, generation) => {
    const child = spawn(BUN_BIN, ['run', 'gate-docker.ts'], {
      cwd: ROOT,
      env: {
        ...process.env,
        PORT: String(port),
        HOST: '127.0.0.1',
        RUNTIME_GENERATION: generation,
        SUPERVISED_RUNTIME: '1',
      },
      stdio: 'inherit',
    });

    try {
      await waitForWorker(port, generation, child);
    } catch (error) {
      try { child.kill('SIGTERM'); } catch {}
      throw error;
    }

    const runtime: ManagedRuntime = {
      generation,
      port,
      inFlight: 0,
      draining: false,
      agent: new http.Agent({ keepAlive: true, maxSockets: 256 }),
      markDraining() {
        if (child.exitCode == null) child.kill('SIGUSR1');
      },
      async stop(force = false) {
        if (child.exitCode != null) return;
        child.kill(force ? 'SIGKILL' : 'SIGTERM');
        const deadline = Date.now() + 5000;
        while (child.exitCode == null && Date.now() < deadline) await sleep(50);
        if (child.exitCode == null) child.kill('SIGKILL');
      },
    };
    return runtime;
  };
}

export async function main() {
  const supervisor = new RuntimeSupervisor({
    workerPorts: WORKER_PORTS,
    factory: productionFactory(),
    drainTimeoutMs: DRAIN_TIMEOUT_MS,
  });
  const initial = await supervisor.start();
  const server = http.createServer(supervisor.handle);
  server.listen(FRONT_PORT, FRONT_HOST, () => {
    console.log(`[Supervisor] Listening on ${FRONT_HOST}:${FRONT_PORT}; active worker=${initial.port} generation=${initial.generation}`);
  });

  process.on('SIGHUP', () => {
    console.log('[Supervisor] Reload requested');
    void supervisor.reload()
      .then(runtime => console.log(`[Supervisor] Reload complete; active worker=${runtime.port} generation=${runtime.generation}`))
      .catch(error => console.error('[Supervisor] Reload failed; keeping current runtime:', error?.message || error));
  });

  const shutdown = async () => {
    server.close();
    const runtimes = [supervisor.active, ...supervisor.draining].filter(Boolean) as ManagedRuntime[];
    await Promise.all(runtimes.map(runtime => runtime.stop(true).catch(() => {})));
    process.exit(0);
  };
  process.on('SIGTERM', () => { void shutdown(); });
  process.on('SIGINT', () => { void shutdown(); });
}

if (import.meta.main) {
  main().catch(error => {
    console.error('[Supervisor] Fatal:', error?.stack || error);
    process.exit(1);
  });
}

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
  /** Calls the listener once if the runtime's process ends on its own (or already has). */
  onExit?(listener: () => void): void;
}

/** Factories must stop their starting worker before rejecting on cancellation. */
export type RuntimeFactory = (port: number, generation: string, signal?: AbortSignal) => Promise<ManagedRuntime>;

export interface RuntimeSupervisorOptions {
  workerPorts: number[];
  factory: RuntimeFactory;
  drainTimeoutMs?: number;
  /** First delay before retrying a replacement after the active runtime died. */
  recoverDelayMs?: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// The status document describes deploy state, so only answer local callers
// (reload script, operators); anything forwarded or remote is proxied as usual.
function isLoopbackRequest(req: http.IncomingMessage): boolean {
  const address = req.socket.remoteAddress;
  const local = address === '127.0.0.1' || address === '::1' || address === '::ffff:127.0.0.1';
  const forwarded = ['x-forwarded-for', 'x-real-ip', 'forwarded', 'cf-connecting-ip'].some(name => req.headers[name]);
  return local && !forwarded;
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
  private readonly recoverDelayMs: number;
  private generationCounter = 0;
  private recovering = false;
  private stopping = false;
  private readonly startupAbort = new AbortController();
  private startPromise: Promise<ManagedRuntime> | null = null;
  private reloadPromise: Promise<ManagedRuntime> | null = null;
  active: ManagedRuntime | null = null;
  draining = new Set<ManagedRuntime>();

  constructor(options: RuntimeSupervisorOptions) {
    this.workerPorts = options.workerPorts;
    this.factory = options.factory;
    this.drainTimeoutMs = options.drainTimeoutMs ?? 15 * 60_000;
    this.recoverDelayMs = options.recoverDelayMs ?? 1000;
  }

  private nextGeneration(): string {
    this.generationCounter++;
    return `${Date.now()}-${process.pid}-${this.generationCounter}`;
  }

  async start(): Promise<ManagedRuntime> {
    if (this.stopping) throw new Error('Supervisor is shutting down');
    if (this.active) return this.active;
    if (this.startPromise) return this.startPromise;
    const work = (async () => {
      const runtime = await this.factory(
        chooseStandbyPort(null, this.workerPorts, [...this.draining].map(draining => draining.port)),
        this.nextGeneration(), this.startupAbort.signal,
      );
      if (this.stopping) {
        try { await runtime.stop(true); } finally { runtime.agent.destroy(); }
        throw new Error('Supervisor is shutting down');
      }
      this.activate(runtime);
      return runtime;
    })();
    this.startPromise = work;
    try { return await work; }
    finally { if (this.startPromise === work) this.startPromise = null; }
  }

  private activate(runtime: ManagedRuntime): void {
    this.active = runtime;
    runtime.onExit?.(() => this.handleExit(runtime));
  }

  // A worker that dies on its own would otherwise leave the supervisor routing
  // to a dead port forever (systemd only sees the supervisor, which stays up).
  private handleExit(runtime: ManagedRuntime): void {
    if (this.stopping || this.active !== runtime) return;
    console.error(`[Supervisor] Active worker generation=${runtime.generation} port=${runtime.port} exited unexpectedly`);
    this.active = null;
    try { runtime.agent.destroy(); } catch {}
    void this.recover();
  }

  private async recover(): Promise<void> {
    if (this.recovering) return;
    this.recovering = true;
    let delay = this.recoverDelayMs;
    try {
      while (!this.stopping && !this.active) {
        try {
          const runtime = await this.reload();
          console.log(`[Supervisor] Recovered; active worker=${runtime.port} generation=${runtime.generation}`);
        } catch (error: any) {
          console.error('[Supervisor] Recovery failed, retrying:', error?.message || error);
          await sleep(delay);
          delay = Math.min(delay * 2, 30_000);
        }
      }
    } finally {
      this.recovering = false;
    }
  }

  /** Stops supervising: no recovery attempts, and all workers are stopped. */
  async shutdown(): Promise<void> {
    this.stopping = true;
    this.startupAbort.abort(new Error('Supervisor is shutting down'));
    const pending = [this.startPromise, this.reloadPromise].filter(Boolean) as Promise<ManagedRuntime>[];
    const runtimes = [this.active, ...this.draining].filter(Boolean) as ManagedRuntime[];
    await Promise.all([
      ...runtimes.map(async runtime => {
        try { await runtime.stop(true); } catch {} finally { runtime.agent.destroy(); }
      }),
      ...pending.map(work => work.catch(() => {})),
    ]);
  }

  async reload(): Promise<ManagedRuntime> {
    if (this.stopping) throw new Error('Supervisor is shutting down');
    if (this.reloadPromise) return this.reloadPromise;
    this.reloadPromise = (async () => {
      const old = this.active;
      const port = chooseStandbyPort(
        old?.port ?? null,
        this.workerPorts,
        [...this.draining].map(runtime => runtime.port),
      );
      const next = await this.factory(port, this.nextGeneration(), this.startupAbort.signal);
      if (this.stopping) {
        try { await next.stop(true); } finally { next.agent.destroy(); }
        throw new Error('Supervisor is shutting down');
      }

      // Atomic routing switch: every request accepted after this assignment
      // goes to the validated replacement. Existing proxy requests keep their
      // already-open connection to the old runtime.
      this.activate(next);

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
    try {
      const deadline = Date.now() + this.drainTimeoutMs;
      while (runtime.inFlight > 0 && Date.now() < deadline) await sleep(100);
      const force = runtime.inFlight > 0;
      await runtime.stop(force);
    } catch (error: any) {
      console.error(`[Supervisor] Failed to stop drained generation=${runtime.generation}:`, error?.message || error);
    } finally {
      try { runtime.agent.destroy(); } catch {}
      // Always release the worker port, otherwise two-port setups could never reload again.
      this.draining.delete(runtime);
    }
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
    if (req.url === '/__supervisor/status' && req.method === 'GET' && isLoopbackRequest(req)) {
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

function hasExited(child: ChildProcess): boolean {
  // A signal-terminated child has exitCode === null but signalCode set.
  return child.exitCode != null || child.signalCode != null;
}

async function waitForWorker(port: number, generation: string, child: ChildProcess, signal?: AbortSignal): Promise<void> {
  const deadline = Date.now() + WORKER_READY_TIMEOUT_MS;
  let lastError = 'worker not ready';
  while (Date.now() < deadline) {
    signal?.throwIfAborted();
    if (hasExited(child)) throw new Error(`worker exited before readiness (code=${child.exitCode ?? child.signalCode})`);
    try {
      const timeout = AbortSignal.timeout(2000);
      const response = await fetch(`http://127.0.0.1:${port}/api/status`, {
        signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
      });
      if (response.ok) {
        const doc: any = await response.json();
        if (doc?.ok === true && doc?.runtime?.generation === generation) return;
        lastError = `unexpected generation ${doc?.runtime?.generation || 'missing'}`;
      } else {
        lastError = `HTTP ${response.status}`;
      }
    } catch (error: any) {
      signal?.throwIfAborted();
      lastError = error?.message || String(error);
    }
    await sleep(500);
  }
  throw new Error(`replacement worker failed readiness: ${lastError}`);
}

async function stopWorker(child: ChildProcess, force = false): Promise<void> {
  if (hasExited(child)) return;
  child.kill(force ? 'SIGKILL' : 'SIGTERM');
  let deadline = Date.now() + 5000;
  while (!hasExited(child) && Date.now() < deadline) await sleep(50);
  if (!hasExited(child)) {
    child.kill('SIGKILL');
    deadline = Date.now() + 5000;
    while (!hasExited(child) && Date.now() < deadline) await sleep(50);
  }
  if (!hasExited(child)) throw new Error(`Worker pid=${child.pid} did not exit after SIGKILL`);
}

function productionFactory(): RuntimeFactory {
  return async (port, generation, signal) => {
    signal?.throwIfAborted();
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
      await waitForWorker(port, generation, child, signal);
    } catch (error) {
      await stopWorker(child);
      throw error;
    }

    const runtime: ManagedRuntime = {
      generation,
      port,
      inFlight: 0,
      draining: false,
      agent: new http.Agent({ keepAlive: true }),
      markDraining() {
        if (!hasExited(child)) child.kill('SIGUSR1');
      },
      async stop(force = false) {
        await stopWorker(child, force);
      },
      onExit(listener) {
        if (hasExited(child)) queueMicrotask(listener);
        else child.once('exit', listener);
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
    await supervisor.shutdown();
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

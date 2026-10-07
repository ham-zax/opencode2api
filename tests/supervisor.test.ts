import { describe, expect, test } from 'bun:test';
import http from 'node:http';
import { RuntimeSupervisor, chooseStandbyPort, type ManagedRuntime } from '../supervisor';

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  const port = typeof address === 'object' && address ? address.port : 0;
  await new Promise<void>(resolve => server.close(() => resolve()));
  return port;
}

async function listen(server: http.Server, port = 0): Promise<number> {
  await new Promise<void>(resolve => server.listen(port, '127.0.0.1', resolve));
  const address = server.address();
  return typeof address === 'object' && address ? address.port : 0;
}

async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('condition not reached');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

describe('runtime supervisor', () => {
  test('standby selection alternates worker ports and never reuses a draining port', () => {
    expect(chooseStandbyPort(null, [13439, 13440])).toBe(13439);
    expect(chooseStandbyPort(13439, [13439, 13440])).toBe(13440);
    expect(chooseStandbyPort(13440, [13439, 13440])).toBe(13439);
    expect(() => chooseStandbyPort(13440, [13439, 13440], [13439])).toThrow('No standby worker port');
  });

  test('validated replacement receives new traffic while the old runtime drains', async () => {
    const ports = [await freePort(), await freePort()];
    let releaseSlow!: () => void;
    const slowGate = new Promise<void>(resolve => { releaseSlow = resolve; });
    const stopped: string[] = [];
    const drainingSignals: string[] = [];

    const factory = async (port: number, generation: string): Promise<ManagedRuntime> => {
      const worker = http.createServer(async (req, res) => {
        if (req.url === '/slow') await slowGate;
        res.writeHead(200, { 'content-type': 'text/plain' });
        res.end(generation);
      });
      await listen(worker, port);
      return {
        generation,
        port,
        inFlight: 0,
        draining: false,
        agent: new http.Agent({ keepAlive: true }),
        markDraining() { drainingSignals.push(generation); },
        async stop() {
          await new Promise<void>(resolve => worker.close(() => resolve()));
          stopped.push(generation);
        },
      };
    };

    const supervisor = new RuntimeSupervisor({ workerPorts: ports, factory, drainTimeoutMs: 2000 });
    const first = await supervisor.start();
    const front = http.createServer(supervisor.handle);
    const frontPort = await listen(front);

    try {
      const slow = fetch(`http://127.0.0.1:${frontPort}/slow`).then(response => response.text());
      await waitFor(() => first.inFlight === 1);

      const second = await supervisor.reload();
      expect(second.generation).not.toBe(first.generation);
      expect(drainingSignals).toEqual([first.generation]);
      expect(supervisor.status().draining[0]?.inFlight).toBe(1);

      const fresh = await fetch(`http://127.0.0.1:${frontPort}/fresh`).then(response => response.text());
      expect(fresh).toBe(second.generation);

      releaseSlow();
      expect(await slow).toBe(first.generation);
      await waitFor(() => stopped.includes(first.generation));
      expect(supervisor.status().draining).toEqual([]);
      expect(supervisor.active?.generation).toBe(second.generation);
    } finally {
      releaseSlow();
      await new Promise<void>(resolve => front.close(() => resolve()));
      const runtimes = [supervisor.active, ...supervisor.draining].filter(Boolean) as ManagedRuntime[];
      await Promise.all(runtimes.map(runtime => runtime.stop(true).catch(() => {})));
    }
  });

  test('failed replacement leaves the current runtime active', async () => {
    const ports = [await freePort(), await freePort()];
    let calls = 0;
    const factory = async (port: number, generation: string): Promise<ManagedRuntime> => {
      calls++;
      if (calls === 2) throw new Error('replacement failed validation');
      const worker = http.createServer((_req, res) => res.end(generation));
      await listen(worker, port);
      return {
        generation,
        port,
        inFlight: 0,
        draining: false,
        agent: new http.Agent({ keepAlive: true }),
        markDraining() {},
        async stop() { await new Promise<void>(resolve => worker.close(() => resolve())); },
      };
    };

    const supervisor = new RuntimeSupervisor({ workerPorts: ports, factory });
    const first = await supervisor.start();
    await expect(supervisor.reload()).rejects.toThrow('replacement failed validation');
    expect(supervisor.active).toBe(first);
    await first.stop();
  });

  test('client disconnect before response headers cancels the worker request', async () => {
    const workerPort = await freePort();
    const ports = [workerPort, await freePort()];
    let workerHits = 0;
    let workerSawClose = false;
    const worker = http.createServer((_req, res) => {
      workerHits++;
      res.once('close', () => { workerSawClose = true; });
      // Never respond: the client goes away while waiting for headers.
    });
    await listen(worker, workerPort);
    const factory = async (port: number, generation: string): Promise<ManagedRuntime> => ({
      generation,
      port,
      inFlight: 0,
      draining: false,
      agent: new http.Agent({ keepAlive: true }),
      markDraining() {},
      async stop() {},
    });
    const supervisor = new RuntimeSupervisor({ workerPorts: ports, factory });
    const runtime = await supervisor.start();
    const front = http.createServer(supervisor.handle);
    const frontPort = await listen(front);

    try {
      await new Promise<void>(resolve => {
        const client = http.get(`http://127.0.0.1:${frontPort}/hang`, () => {});
        client.once('error', () => {});
        setTimeout(() => { client.destroy(); resolve(); }, 50);
      });
      // Cancellation must reach the worker; occupancy must settle only
      // after the worker connection is released (not on res 'close').
      await waitFor(() => workerSawClose, 2000);
      await waitFor(() => runtime.inFlight === 0, 2000);
      expect(workerHits).toBe(1);
      expect(workerSawClose).toBe(true);
      expect(runtime.inFlight).toBe(0);
    } finally {
      runtime.agent.destroy();
      await new Promise<void>(resolve => front.close(() => resolve()));
      await new Promise<void>(resolve => worker.close(() => resolve()));
    }
  });

  test('client disconnect during SSE cancels the worker stream and leaves the runtime reusable', async () => {
    const workerPort = await freePort();
    const ports = [workerPort, await freePort()];
    let sseCloses = 0;
    const worker = http.createServer((req, res) => {
      if (req.url === '/sse') {
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
        res.write('data: {"content":"first"}\n\n');
        res.once('close', () => { sseCloses++; });
        return;
      }
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
    await listen(worker, workerPort);
    const factory = async (port: number, generation: string): Promise<ManagedRuntime> => ({
      generation,
      port,
      inFlight: 0,
      draining: false,
      agent: new http.Agent({ keepAlive: true }),
      markDraining() {},
      async stop() {},
    });
    const supervisor = new RuntimeSupervisor({ workerPorts: ports, factory });
    const runtime = await supervisor.start();
    const front = http.createServer(supervisor.handle);
    const frontPort = await listen(front);

    try {
      await new Promise<void>(resolve => {
        const client = http.get(`http://127.0.0.1:${frontPort}/sse`, res => {
          res.once('data', () => { client.destroy(); resolve(); });
          res.once('error', () => {});
        });
        client.once('error', () => {});
      });
      await waitFor(() => sseCloses === 1, 2000);
      await waitFor(() => runtime.inFlight === 0, 2000);
      expect(sseCloses).toBe(1);
      expect(runtime.inFlight).toBe(0);

      // A normally completed response after a cancellation must still work
      // over the same keep-alive agent.
      const body = await fetch(`http://127.0.0.1:${frontPort}/plain`).then(response => response.text());
      expect(body).toBe('ok');
      expect(runtime.inFlight).toBe(0);
    } finally {
      runtime.agent.destroy();
      await new Promise<void>(resolve => front.close(() => resolve()));
      await new Promise<void>(resolve => worker.close(() => resolve()));
    }
  });

  test('cancelling one stream does not damage an unrelated concurrent request', async () => {
    const workerPort = await freePort();
    const ports = [workerPort, await freePort()];
    let releaseSlow!: () => void;
    const slowGate = new Promise<void>(resolve => { releaseSlow = resolve; });
    let sseCloses = 0;
    const worker = http.createServer(async (req, res) => {
      if (req.url === '/sse') {
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.write('data: first\n\n');
        res.once('close', () => { sseCloses++; });
        return;
      }
      if (req.url === '/slow') await slowGate;
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('slow-done');
    });
    await listen(worker, workerPort);
    const factory = async (port: number, generation: string): Promise<ManagedRuntime> => ({
      generation,
      port,
      inFlight: 0,
      draining: false,
      agent: new http.Agent({ keepAlive: true }),
      markDraining() {},
      async stop() {},
    });
    const supervisor = new RuntimeSupervisor({ workerPorts: ports, factory });
    const runtime = await supervisor.start();
    const front = http.createServer(supervisor.handle);
    const frontPort = await listen(front);

    try {
      const slow = fetch(`http://127.0.0.1:${frontPort}/slow`).then(response => response.text());
      await waitFor(() => runtime.inFlight === 1, 2000);

      await new Promise<void>(resolve => {
        const client = http.get(`http://127.0.0.1:${frontPort}/sse`, res => {
          res.once('data', () => { client.destroy(); resolve(); });
          res.once('error', () => {});
        });
        client.once('error', () => {});
      });
      await waitFor(() => sseCloses === 1, 2000);

      releaseSlow();
      await waitFor(() => runtime.inFlight === 0, 2000);
      expect(await slow).toBe('slow-done');
      expect(runtime.inFlight).toBe(0);
    } finally {
      try { releaseSlow(); } catch {}
      runtime.agent.destroy();
      await new Promise<void>(resolve => front.close(() => resolve()));
      await new Promise<void>(resolve => worker.close(() => resolve()));
    }
  });

  function crashableFactory() {
    const created: { generation: string; port: number; crash(): void; stopped: boolean }[] = [];
    const factory = async (port: number, generation: string): Promise<ManagedRuntime> => {
      let exitListener: (() => void) | undefined;
      const entry = {
        generation, port, stopped: false,
        crash() { exitListener?.(); },
      };
      created.push(entry);
      return {
        generation,
        port,
        inFlight: 0,
        draining: false,
        agent: new http.Agent({ keepAlive: true }),
        markDraining() {},
        onExit(listener) { exitListener = listener; },
        async stop() { entry.stopped = true; },
      };
    };
    return { created, factory };
  }

  test('an active worker that exits unexpectedly is replaced automatically', async () => {
    const { created, factory } = crashableFactory();
    const supervisor = new RuntimeSupervisor({ workerPorts: [13439, 13440], factory, recoverDelayMs: 5 });
    const first = await supervisor.start();
    created[0]!.crash();
    expect(supervisor.active).toBeNull();
    await waitFor(() => supervisor.active !== null);
    expect(supervisor.active).not.toBe(first);
    expect(created).toHaveLength(2);
    await supervisor.shutdown();
  });

  test('recovery retries until a replacement worker starts', async () => {
    const { created, factory: inner } = crashableFactory();
    let failures = 2;
    const factory = async (port: number, generation: string) => {
      if (created.length >= 1 && failures-- > 0) throw new Error('not ready');
      return inner(port, generation);
    };
    const supervisor = new RuntimeSupervisor({ workerPorts: [13439, 13440], factory, recoverDelayMs: 5 });
    await supervisor.start();
    created[0]!.crash();
    await waitFor(() => supervisor.active !== null, 2000);
    expect(failures).toBeLessThan(0);
    await supervisor.shutdown();
  });

  test('the exit of a draining worker does not trigger recovery', async () => {
    const { created, factory } = crashableFactory();
    const supervisor = new RuntimeSupervisor({ workerPorts: [13439, 13440], factory, recoverDelayMs: 5 });
    await supervisor.start();
    const second = await supervisor.reload();
    created[0]!.crash();
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(supervisor.active).toBe(second);
    expect(created).toHaveLength(2);
    await supervisor.shutdown();
  });

  test('shutdown stops every worker and suppresses recovery', async () => {
    const { created, factory } = crashableFactory();
    const supervisor = new RuntimeSupervisor({ workerPorts: [13439, 13440], factory, recoverDelayMs: 5 });
    const first = await supervisor.start();
    await supervisor.shutdown();
    expect(created[0]!.stopped).toBe(true);
    created[0]!.crash();
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(supervisor.active).toBe(first);
    expect(created).toHaveLength(1);
  });

  test('a failing stop does not leave the old runtime stuck in the draining set', async () => {
    const { created, factory } = crashableFactory();
    const supervisor = new RuntimeSupervisor({
      workerPorts: [13439, 13440],
      factory: async (port, generation) => {
        const runtime = await factory(port, generation);
        runtime.stop = async () => { throw new Error('stop failed'); };
        return runtime;
      },
      drainTimeoutMs: 50,
    });
    await supervisor.start();
    await supervisor.reload();
    await waitFor(() => supervisor.draining.size === 0, 2000);
    expect(created).toHaveLength(2);
    await supervisor.shutdown();
  });
});

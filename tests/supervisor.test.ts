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
});

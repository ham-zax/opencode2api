# API repair — 2026-10-16

Two gateway faults caused the incident:

- Synthetic probes received `403 FreeTierError`. The gateway incorrectly treated that client-context rejection as model unavailability.
- Cold requests waited for proxy allocation. Subsequent retries encountered seven upstream rate limits and two proxy authentication failures before the client timed out at 34 seconds.

The repair preserves model health when a probe lacks client context. Completed generation establishes healthy status. Cold requests use usable direct egress immediately. After a proxy failure, routing tries usable direct egress before further proxy retries. Direct cooldowns, model bans, caller tool permissions, and existing proxy fallback remain active.

The later routing update also gives direct egress priority over existing warm public and residential proxy pools. Rate limits trigger proxy fallback. Direct routing resumes after its cooldown. The gateway does not predict remaining upstream quota.

Routing update checks: 153 local tests and 147 ARM tests passed. Coverage includes warm pools, direct rate limiting, proxy fallback during cooldown, and return to direct afterward. The ARM supervisor loaded generation `1792116878207-934-4`. An OpenChamber Space Bunny request returned `DIRECT_ROUTE_OK`, and the gateway audit recorded HTTP 200 through `direct` with 1343 ms dispatch latency. This measures one request, not a speed guarantee.

Live checks used native OpenCode 2.0.22 request bodies through the authenticated ARM gateway. These results establish that request scope, not arbitrary third-party payload compatibility.

| Model | HTTP | Seconds | Output |
| --- | --- | --- | --- |
| big-pickle | 200 | 0.68 | OK |
| cat-jam-free | 200 | 12.09 | OK |
| mimo-v2.6-flash-free | 200 | 5.86 | OK |
| nemotron-3.5-super-vl-free | 200 | 6.17 | OK |
| nemotron-3.5-lightning-free | 200 | 1.37 | OK |
| longcat-2.5-preview-free | 200 | 3.31 | OK |
| deepseek-v4.1-flash-free | 200 | 2.49 | OK |
| space-bunny-free | 200 | 0.83 | OK |
| muse-spark-1.3-contributor-free | 200 | 1.15 | OK |

Chat, Responses, JSON replies, and the Muse alias passed. The catalog contains 10 IDs for nine distinct models. Local tests: 152 passed. ARM tests: 146 passed. ARM retains its pre-existing test differences.

Before modification, all seven existing changed files were copied to `/home/ubuntu/.local/state/opencode2api/repairs/20261015T235633Z`. The repair preserves API key values and configuration. Changes remain uncommitted.

## Repeat checks

A higher-concurrency pass encountered three client timeouts at 15 seconds. The gateway permits up to 120 seconds for the first productive event. Upstream Muse requests also returned `service_overloaded` during this period.

Subsequent checks on the final runtime completed successfully for all nine distinct models. Rechecks completed in 14.06 seconds for Nemotron Super, 1.88 seconds for Muse, and 4.05 seconds for Nemotron Lightning. These observations establish successful calls, not continuous provider availability.

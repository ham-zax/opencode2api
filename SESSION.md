# OpenCode2API handoff — 2026-10-02

## Stop state

Hamza paused implementation and requested a handoff in the repository. This document fulfills that request. Do not resume implementation, deployment, or global configuration changes without a new instruction.

The implementation push completed before the pause took effect. Published implementation commits:

- `b49e189` — fix: harden Zen discovery, native routing and proxy pools
- `7abdb11` — rebased existing local feature commit (previously `aff0a2c`)

Remote: `https://github.com/ham-zax/opencode2api.git`.

## Authorized task and outcome

Check the OpenCode API with all advertised free models, restrict the gateway to free Zen text models, improve future discovery/routing robustness, capture the installed `opencode2` protocol, and commit/push all changes. Implementation and publishing are complete. No deployment was requested or performed.

Initial user edits in `gate-docker.ts` added a proxy pool state machine and background screening. Those edits were preserved and repaired: measured health survives refreshes, screens have complete-request deadlines, failures remove only affected slots, sweeps rotate through candidates, upstream cooldowns survive reachability success, allocation is shared per key, pending allocations count toward limits, and ownership is rechecked after asynchronous probes.

Catalog/routing changes:

- `model-catalog.ts` joins live Zen model IDs with zero-cost `models.dev.opencode` entries, text modality and supported transports.
- Deprecated, paid, missing-metadata, unsupported transport, and Jev models are excluded. No suffix-only pricing fallback; `opencode-go` metadata is never used for admission.
- Responses selection derives from provider SDK metadata, with future Muse aliases generated automatically. Aliases share canonical health and are probed once.
- Discovery uses a single shared refresh, a five-minute demand cache, one-hour metadata cache, 30-second failure retry delay, and a persisted last successful catalog usable for at most 24 hours. Valid pricing changes can withdraw all models immediately.
- `/v1/models` and `/api/models` report catalog status, refresh errors and exclusions. Unknown/excluded models return 400. Unsupported forwarding endpoints return 404.
- Chat-to-Responses translation preserves tool history and streamed/JSON tool calls. Native Responses requests retain flat tools and return the appropriate Responses schema.
- Native tool definitions are preserved; fallback tools are added only if absent. Current native User-Agent values are retained and session-affinity headers match the canonical session.
- Concurrency is checked immediately before reservation, after catalog/body awaits. Admitted generations consume request quota even when upstream omits token usage; token totals are recorded separately.

Committed files: `gate-docker.ts`, `model-catalog.ts`, `package.json`, `README.md`, `docs/opencode2-api.md`, `tests/gateway.test.ts`, `tests/model-catalog.test.ts`, `tests/fixtures/opencode2-zen.json`.

## Native API evidence

Installed `/home/hamza/.opencode/bin/opencode2` wraps OpenCode 2.0.21. Isolated native runs through a loopback recorder returned HTTP 200 and `OK` for:

- `opencode/big-pickle` → `https://opencode.ai/zen/v1/chat/completions`
- `opencode/muse-spark-1.3-contributor-free` → `https://opencode.ai/zen/v1/responses`

Authorization is redacted and prompt content omitted from the committed capture fixture. See [the API capture](docs/opencode2-api.md) for observed headers, fields and provenance. Production uses current public metadata; the captured fixture is not a fixed production catalog.

The installed provider catalog had nine active free Zen text models at capture time. Final live discovery automatically found the newer `ling-3.1-flash-free`; ten canonical models were probed, nine healthy and Ling 3.0 Flash Fin unavailable (HTTP 400). Do not call Ling Go-only: it is present in Zen metadata. Jev is a real Zen structured-decision model at `/v1/systemone`, outside this chat gateway. DeepSeek free, Muse 1.2 free and MiMo 2.5 free are deprecated in the installed registry; paid variants can overlap Zen and Go.

The earlier full advertised-free test covered 13 IDs: ten returned valid output; Jev was initially tested on the wrong text endpoint, while DeepSeek free and Ling 3.0 failed. Final admission follows current metadata rather than that historical list.

## Verification

- `bun test`: **79 pass, 0 fail, 227 assertions**; isolated tests do not call public services.
- `bun build gate-docker.ts --target=bun --outfile=/tmp/opencode2api-verification/gateway.js`: passed.
- `tsc --noEmit --strict --target es2022 --module esnext --skipLibCheck model-catalog.ts`: passed.
- `git diff --check`, README JSON parsing, frontend JavaScript syntax and Python script syntax: passed.
- Live updated gateway: Big Pickle Chat JSON, Muse Chat-to-Responses JSON, and native Responses SSE all returned `OK` with HTTP 200.
- Installed OpenCode2 through the updated gateway: both `gateway-test/big-pickle` and `gateway-test/muse-spark-1.3-free` returned `OK`. The initial native-through-gateway attempt timed out; after preserving native tool definitions, the repeated check passed for both models.
- Full gateway standalone TypeScript checking remains unavailable due existing missing Node/geoip declarations and existing agent typings; no configured full type-check script exists. Docker is not installed, so Docker deployment was not tested.

## Remote integration decision

First push was rejected because remote daily data updates were ahead. Rebase onto `origin/main` completed cleanly; application source and tests were verified byte-for-byte unchanged against pre-rebase commit `6253c41`.

Hamza said to integrate only worthwhile changes. Incoming commits touched only `500ip.txt` and `cfip_opencode_formatted.txt`. Both current feeds have 500 unique global endpoints, valid ports/tags, matching endpoint order and sorted latency data, dated 2026-10-02. There are 114 new endpoints versus the prior local upstream baseline. Three of four sampled TLS-verified endpoints returned Zen `/v1/models` HTTP 200. One alternate-port sample returned 403 and was already in the old feed. The updates were retained as refreshed candidate data; this does not claim all 500 endpoints work with Zen.

## Cleanup and remaining work

All task Muse workers are closed. Temporary gateway and native capture servers are stopped; verification ports 13349 and 13451–13454 were confirmed free. No production service is intentionally left running. User's global OpenCode configuration was not edited.

No required implementation work remains. If Hamza resumes, first read the new instruction and current git state. Do not automatically deploy or change global provider configuration. Optional future deployment validation should use the actual requested environment.

This repository handoff replaces the outdated session status. Scratch evidence (not committed, may be temporary): `/tmp/opencode2api-verification`, including redacted native records in `native-capture/requests.json`, sanitized catalog in `opencode2-catalog.json`, test log `test-current.log`, and helper scripts. Do not share raw native settings, credential files, private server logs/passwords or runtime key state.

# OpenCode2API Technical Documentation

> Per-Key IP Pool Reverse Proxy Gateway — Multi-IP round-robin proxy acceleration for opencode.ai/zen free AI models

## Project Overview

OpenCode2API is a reverse proxy gateway written in TypeScript, specifically designed for free AI models (`-free` suffix) on [opencode.ai/zen](https://opencode.ai/zen). By aggregating multiple public proxy sources, subscription nodes, WARP tunnels, and custom proxies, it maintains an independent proxy Slot pool for each API Key. It achieves request-level round-robin scheduling and automatic failure failover/replacement to prevent single-IP rate limits (HTTP 429) or connection drops.

**Core Features:**
- Each API Key independently holds 3 proxy Slots (`SLOTS_PER_KEY`), with up to 20 keys active concurrently across the gateway (`MAX_ACTIVE_KEYS`)
- Request-level Round-Robin rotation: distributes normal traffic evenly across proxies in addition to handling failover
- Automated health-check probing and slot replacement on failure, with immediate resource recycling
- Four-tier Fallback chain: Pool Slots → WARP → Custom Proxies → Direct Connection
- Built-in subscription management, audit logging, and Key management REST APIs
- Supports both streaming (SSE) and non-streaming OpenAI-compatible request forwarding

## Architecture

```
                        ┌─────────────────────────────────┐
                        │        HTTP Server (:13339)      │
                        │   /v1/* → dispatch()             │
                        │   /api/* → management endpoints   │
                        └──────────┬──────────────────────┘
                                   │
                        ┌──────────▼──────────────────────┐
                        │       dispatch(authKey, pool)    │
                        │  1. RR select slot from pool     │
                        │  2. fallback → WARP              │
                        │  3. fallback → customSlots       │
                        │  4. fallback → direct connection │
                        └──────────┬──────────────────────┘
                                   │
               ┌────────────────────┼────────────────────┐
               ▼                    ▼                    ▼
       Per-Key Slot Pools     WARP (global)       Custom Proxies
       ┌──────┬──────┬─      ┌──────────┐       ┌──────────────┐
       │Key A │Key B │ ...   │ socks5   │       │ http/socks5  │
       │s1,s2,s3│s1,s2,s3│   │ 172.17.. │       │ (CUSTOM_PROXIES)
       └──────┴──────┴─      └──────────┘       └──────────────┘
                                   │
                        ┌──────────▼──────────┐
                        │    Candidate Pool     │
                        │ (candidates[] aggregated)│
                        └──────────┬──────────┘
                                   │
           ┌────────────────────────┼────────────────────────┐
           ▼                        ▼                        ▼
     Proxy Sources            Subscriptions           Proxy Pool
   (sources.json)        (CLASH_SUBSCRIBE_URLS)    (PROXY_POOL_URL)
  speedx-socks5           mihomo YAML parser         external HTTP
  speedx-http             SOCKS5/HTTP node parser   "proto://ip:port"
  amux (JSON API)
  hproxy
```

### Entry Point & Startup Sequence

Execution order in `main()` of `gate.ts`:

1. `loadKeys()` → Loads `keys.json`, ensuring the default admin key exists
2. `loadSources()` → Loads `sources.json`, saving default sources if the file does not exist
3. `loadCustomProxies()` → Loads `custom_proxies.json` (persisted proxies)
4. `loadAuditLog()` → Loads the 500 most recent audit log entries
5. `probeWarp()` → If `WARP_MODE=on`, probes WARP availability for global fallback
6. `loadCandidates()` → Fetches, deduplicates, and sorts candidate proxies from all sources by grade and latency
7. `fetchFromProxyPool()` → Fetches pre-validated proxies from external proxy-pool (used with priority)
8. `fetchAllSubscriptions()` → Downloads nodes from Clash subscriptions and merges them into the candidate pool
9. `startSubscriptionRefresh()` → Starts the 8-hour subscription refresh timer
10. `initCustomSlots()` → Probes custom proxies defined in `CUSTOM_PROXIES`
11. Starts the periodic candidate pool refresh timer (`PROXY_REFRESH_MS`, default 5 minutes)
12. `server.listen(PORT)` → Starts the HTTP server

## How It Works

### Key Allocation and Slot Pools

1. Client sends request with `Authorization: Bearer <key>` to `/v1/*`
2. `dispatch()` calls `getKeySlotPool(keyId)`:
   - If the Key already has a Pool → use it directly (if non-empty)
   - Otherwise, call `allocateKeySlots(keyId)`:
     - Check that global active Key count ≤ `MAX_ACTIVE_KEYS`
     - Sort unlocked candidate proxies by grade → latency, taking `SLOTS_PER_KEY * 15` proxies
     - Concurrently probe candidates in batches (5 at a time) until `SLOTS_PER_KEY` healthy Slots are acquired
     - Mark successfully probed Slots with `lockedBy = keyId`
     - If all probes fail and WARP is available, assign WARP as the single Slot for this Key
3. After the response finishes, `releaseKey(authKey)` decrements the concurrency counter (retaining assigned slots)

### Request Scheduling (dispatch)

Request dispatch flow in `gate.ts`:

1. **Sticky Preference + Cooldown-Aware Slot Selection**: Prefers reusing the ready (non-cooled-down) Slot pointed to by `rrCursor` to **preserve prompt cache warm state** on that proxy; only rotates when the Slot has already been tried or is cooling down.
2. **Pool Slots Exhausted** → Fallback to **WARP Slot** (globally shared)
3. **WARP Unavailable** → Fallback to **Custom Slots** (`customSlots[]`, from `CUSTOM_PROXIES` environment variable)
4. **All Above Fail** → Fallback to **Direct Connection**

Failure Handling (inspired by OCFreeRelay):
- **HTTP 429** → `markSlotCooldown()` applies **exponential backoff cooldown** (5s → 60s) to the Slot (retains Slot without deletion, reusing after cooldown)
- **HTTP 5xx** → `replaceFailedSlot()` removes the Slot and probes a new candidate from the pool to replace it
- Retries up to `MAX_RETRIES` (3) times, skipping already attempted and non-cooldown addresses
- Exceptions or timeouts similarly record cooldown and trigger replacement

### Slot Replacement Strategy

`replaceFailedSlot(pool, failedAddr)`:
1. Remove failed Slot from Pool, release candidate lock
2. Search `candidates` for an unlocked proxy
3. Asynchronously run `probe()`; on success, push to end of Pool and mark `lockedBy`

### Sticky + Cooldown Scheduling Policy

Each Key's Slot Pool maintains `rrCursor`. When `dispatch()` selects a Slot:

```typescript
const stickySlot = pool.slots[pool.rrCursor % pool.slots.length];
if (stickySlot && !tried && !(stickySlot.cooldownUntil > now)) {
  selectedSlot = stickySlot;   // Sticky: reuse to keep prompt cache warm
} else {
  // Rotate to next ready and non-cooling-down Slot
}
```

- **Sticky**: Reuses the current ready Slot continuously to maximize **prompt cache hit rate** and reduce 429 rate limits on OpenCode free models.
- **Cooldown**: Applies exponential backoff (5s → 60s + jitter) on 429/errors. The Slot is skipped during cooldown but **not deleted**, becoming available again once cooled down.
- If `selectedSlot = null`, triggers downstream fallback chain.

## Proxy Sources

### Source Types & Formats

Proxy sources are persisted and managed in `sources.json`, supporting three types:

| Type | Description | Parser |
|------|-------------|--------|
| `text` | Plain text list with `ip:port` per line | `genericTextParser` |
| `json` | JSON array containing `address`, `protocol`, `quality_grade`, `status`, `latency` | Filters `S/A/B/C` and `active` |
| `subscription` | Clash/mihomo YAML, parsing nodes in `proxies[]` with type `SOCKS5`/`HTTP` | `parseSubscriptionProxyNodes` |

### Default Sources

| Name | URL | Type | Description |
|------|-----|------|-------------|
| `speedx-socks5` | `https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt` | text | SOCKS5 proxy list |
| `speedx-http` | `https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt` | text | HTTP proxy list |
| `amux` | `https://proxy.amux.ai/api/proxies` | json | JSON API with quality grading |
| `hproxy` | `https://raw.githubusercontent.com/hproxy-com/free-proxy-list/refs/heads/main/all.txt` | text | HTTP proxy list |

### Management via API

```bash
# List all sources
curl http://localhost:13339/api/sources

# Add a new source (requires name + url)
curl -X POST http://localhost:13339/api/sources \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-source","url":"https://example.com/proxies.txt","type":"text"}'

# Delete a source
curl -X DELETE http://localhost:13339/api/sources/my-source

# Manually trigger candidate refresh
curl -X POST http://localhost:13339/api/refresh
```

`type` can be `text` or `json`. If the name matches a default source, its specific parser is used; otherwise, `json` uses the amux parser and `text` uses `genericTextParser`.

## Subscription System

### Environment Variables

`CLASH_SUBSCRIBE_URLS` (backward compatible with `CLASH_SUBSCRIBE_URL`) supports comma- or semicolon-separated subscription URLs:

```bash
CLASH_SUBSCRIBE_URLS="https://example1.com/sub.yaml,https://example2.com/sub.yaml"
```

Default value: `https://raw.githubusercontent.com/ovmvo/FreeSub/refs/heads/main/sub/permanent/mihomo.yaml`

### Parsing Mechanism

`parseSubscriptionProxyNodes(yamlText)`:
1. Loads YAML using `js-yaml`
2. Iterates over `doc.proxies[]`
3. Filters nodes where `type` is `socks5` or `http`
4. Extracts `server:port` as the address, setting quality grade to `A`
5. Deduplicates and merges nodes into `candidates[]`

### Automatic Refresh

- Fetches subscriptions immediately upon gateway startup
- Automatically refreshes every 8 hours (`SUBSCRIPTION_REFRESH_MS`)
- Merges only new addresses without disrupting existing candidates

### API Management

```bash
# View current subscription URLs
curl http://localhost:13339/api/subscriptions

# Replace subscription URLs at runtime
curl -X POST http://localhost:13339/api/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"action":"set","urls":["https://example.com/sub.yaml"]}'

# Revert to default URL
curl -X POST http://localhost:13339/api/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"action":"reset"}'

# Force immediate refresh
curl -X POST http://localhost:13339/api/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"action":"refresh"}'
```

## WARP Integration

### Configuration

```bash
WARP_MODE=off          # on/off, controls whether WARP is enabled
WARP_HOST=172.17.0.1   # WARP container SOCKS5 listening address
WARP_SOCKS5_PORT=1080  # WARP SOCKS5 port
```

### Working Mechanism

- WARP operates as a **globally shared fallback**, not bound exclusively to any single Key
- On startup, if `WARP_MODE=on`, runs `probeWarp()` to check connectivity
- `probeWarp(retries=3)`: Sends `GET /v1/models` through the SOCKS5 proxy with up to 3 retries at 1s intervals
- On success: sets `warpSlot`, `warpStatus='running'`, and resets consecutive failure count
- On failure: applies exponential backoff (`backoff = min(60s × failCount, 1h)`), setting `warpSkipUntil` to skip subsequent probes during the backoff window

### Known Limitations

- In environments where WARP is blocked by network filters (e.g. WireGuard UDP 2408 handshake drop, MASQUE/QUIC timeouts, H2/TCP 443 TLS handshake EOF), the `caomingjun/warp` container will remain unhealthy.

### API Control

```bash
# Enable WARP
curl -X POST http://localhost:13339/api/warp \
  -H 'Content-Type: application/json' \
  -d '{"action":"enable","host":"172.17.0.1","port":1080}'

# Disable WARP
curl -X POST http://localhost:13339/api/warp \
  -H 'Content-Type: application/json' \
  -d '{"action":"disable"}'
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Static admin panel (`public/index.html`) |
| `/v1/*`, `/openai/v1/*` | ALL | OpenAI-compatible proxy forwarding |
| `/api/status` | GET | Full status: stats, active Keys, Slots, WARP, candidate count |
| `/api/config` | GET | Runtime configuration values |
| `/api/config` | POST | Update WARP mode, port, limits, etc. |
| `/api/keys` | GET | List all API Key details |
| `/api/keys` | POST | Create a new API Key (auto-generated or custom key) |
| `/api/keys/:key` | PUT | Update Key properties (name, enabled, maxConcurrency, expiresAt) |
| `/api/keys/:key` | DELETE | Delete Key and release its assigned Slots |
| `/api/refresh` | POST | Trigger candidate pool refresh |
| `/api/candidates/load` | POST | Reload candidate pool (alias for refresh) |
| `/api/sources/refresh` | POST | Refresh proxy source candidates |
| `/api/sources` | GET | List proxy sources |
| `/api/sources` | POST | Add a proxy source |
| `/api/sources/:name` | DELETE | Delete a proxy source |
| `/api/slots/fill` | POST | Manually allocate Slots for a specified Key |
| `/api/proxies` | GET | List candidate proxies (includes lockedBy status) |
| `/api/proxies` | POST | Batch add proxies to candidate pool |
| `/api/proxies/:addr` | DELETE | Remove proxy from pool (releases associated Slots) |
| `/api/promote` | POST | Promote specified proxy to the head of the candidate pool (`{"addr":"ip:port"}`) |
| `/api/subscriptions` | GET | View subscription URLs |
| `/api/subscriptions` | POST | Manage subscriptions: `set`/`reset`/`refresh` |
| `/api/warp` | POST | Enable/disable WARP |
| `/api/models` | GET | Get cached free models list (5-minute cache) |
| `/api/audit` | GET | Aggregate usage audit (by Key, Model, Date) |
| `/api/audit/daily` | GET | Daily audit breakdown (`?date=2026-07-23`) |
| `/api/logs` | GET | Retrieve last 200 log entries |

## Configuration (Environment Variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `13339` | HTTP listening port |
| `API_KEY` | `admin123` | Default admin API Key |
| `DATA_DIR` | `cwd()` | Data persistence directory |
| `SLOTS_PER_KEY` | `3` | Number of Slots per Key |
| `MAX_ACTIVE_KEYS` | `20` | Max concurrent active Keys globally |
| `TIMEOUT` | `15000` | Standard request timeout (ms) |
| `STREAM_TIMEOUT` | `60000` | Streaming request timeout (ms) |
| `MAX_RETRIES` | `3` | Maximum retry attempts on failure |
| `PROXY_PROBE_TIMEOUT` | `15000` | Proxy health check probe timeout (ms) |
| `PROXY_REFRESH_MS` | `300000` | Candidate pool refresh interval (ms, default 5 min) |
| `WARP_MODE` | `off` | WARP enablement mode: `on`/`off` |
| `WARP_HOST` | `172.17.0.1` | WARP SOCKS5 host |
| `WARP_SOCKS5_PORT` | `1080` | WARP SOCKS5 port |
| `CUSTOM_PROXIES` | `""` | Comma-separated custom proxy addresses |
| `PROXY_POOL_URL` | `""` | External proxy-pool API URL |
| `CLASH_SUBSCRIBE_URLS` | FreeSub Default YAML | Comma/semicolon-separated subscription URLs |

## Docker Deployment

### docker-compose (Recommended)

```yaml
services:
  opencode2api:
    image: opencode2api:latest
    container_name: opencode2api
    restart: always
    ports:
      - "13339:13339"
    volumes:
      - ./data:/app/data
    environment:
      - PORT=13339
      - WARP_MODE=off
      - WARP_HOST=172.17.0.1
      - WARP_SOCKS5_PORT=1080
      - API_KEY=admin123
      - DATA_DIR=/app/data
      - PROXY_POOL_URL=http://host.docker.internal:13340/proxies?format=text
      - CLASH_SUBSCRIBE_URLS=https://raw.githubusercontent.com/ovmvo/FreeSub/refs/heads/main/sub/permanent/mihomo.yaml
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

### Build

```bash
docker build -t opencode2api .
docker compose up -d
```

### docker run

```bash
docker run -d --name opencode2api \
  -p 13339:13339 \
  -v ./data:/app/data \
  -e API_KEY=admin123 \
  -e WARP_MODE=off \
  opencode2api:latest
```

### Dockerfile

```dockerfile
FROM node:22-alpine
WORKDIR /app
RUN npm install -g tsx --registry=https://registry.npmmirror.com
COPY package.json package-lock.json ./
RUN npm install
COPY gate.ts .
COPY public/ public/
CMD ["npx", "tsx", "gate.ts"]
```

Runs TypeScript directly with `tsx` without a separate build step.

### Volumes

`./data:/app/data` persists the following files:

| File | Purpose |
|------|---------|
| `keys.json` | API Key records (usage, limits, expiration) |
| `sources.json` | Proxy source list (persisted on add/delete) |
| `custom_proxies.json` | Persisted custom proxies added via API |
| `audit.jsonl` | Append-only per-request audit logs |

## Slot Scheduling Details

### Allocation Strategy (allocateKeySlots)

```
1. Check that global active Key count < MAX_ACTIVE_KEYS
2. Sort unlocked candidates in ascending order by grade (S/A/B/C) → latency
3. Take the top SLOTS_PER_KEY * 15 candidates as the probe candidate pool
4. Partition probe candidates into batches: groupSize = max(SLOTS_PER_KEY, 5)
5. Concurrently probe candidates in each batch via probe(), processing batches sequentially
6. Add successfully probed proxies to the Key's Pool until SLOTS_PER_KEY slots are filled
7. If all candidates fail, fall back to WARP (if available)
```

### Dispatch Strategy (dispatch)

Request-level Round-Robin load balancing:

```
1. cursor = pool.rrCursor
2. Iterate pool.slots[(cursor + i) % n] to pick the first untried and non-cooling slot
3. Update rrCursor = (cursor + 1) % n
4. Cursor is preserved across completed requests
```

Successive requests sequentially cycle across different proxies, providing natural load distribution.

### Replacement Strategy (replaceFailedSlot)

```
1. Remove failed Slot address from Pool
2. Release lockedBy flag for that address
3. Find candidate that is unlocked and not currently used by any Slot
4. Asynchronously probe(); upon success, append to Pool
5. Mark new candidate address with lockedBy = keyId
```

### Cleanup Strategy

Executed every `POOL_CLEANUP_MS` (60s):
- Releases all Slots for disabled, expired, or quota-exceeded Keys
- Releases idle Pools exceeding `KEY_IDLE_RELEASE_MS` (600s, 10 minutes)

## Troubleshooting

### Check Operational Status

```bash
# Full status (Recommended)
curl http://localhost:13339/api/status | jq .

# Last 200 log entries
curl http://localhost:13339/api/logs | jq .logs

# Docker logs
docker logs --tail 50 opencode2api
```

### Subscriptions Not Taking Effect

```bash
# Check current subscription URLs
curl http://localhost:13339/api/subscriptions

# Force refresh
curl -X POST http://localhost:13339/api/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"action":"refresh"}'

# Check candidate pool size
curl http://localhost:13339/api/status | jq .candidatesCount
```

### WARP Not Working

```bash
# Check WARP status
curl http://localhost:13339/api/status | jq '{warpAvailable, warpStatus, warpMode}'

# Attempt manual re-enable (resets backoff counter)
curl -X POST http://localhost:13339/api/warp \
  -H 'Content-Type: application/json' \
  -d '{"action":"enable"}'

# Test WARP connectivity from inside Docker container
docker exec opencode2api wget -q -O- --timeout=5 http://172.17.0.1:1080
```

If `warpStatus === 'stopped'` and `warpSkipUntil` is active, probes are temporarily skipped. Manually triggering enable resets the counter.

### Rebuilding Docker Container

```bash
# Rebuild after modifying gate.ts
docker build -t opencode2api . && docker compose up -d

# Apply configuration changes in docker-compose.yml (no rebuild required)
docker compose up -d
```

### Performance Reference

- Processes ~15 requests successfully in 30s (subject to proxy quality)
- Candidate pool typically holds 20k–27k entries (4 sources + proxy-pool + subscriptions)
- Active subscription nodes: ~129 SOCKS5 nodes (FreeSub)
- 429 rate limit triggers automatic Slot replacement without failing downstream requests

---

*OpenCode2API v0.2.0 — See code comments in `gate.ts` for detailed module definitions.*

# OpenCode2API ⚡ (`opencode2api`)

> **High-Performance Multi-IP Reverse Proxy Gateway & Rate-Limit Bypass for OpenCode Zen Free AI Models (`opencode` & `opencode2`)**

[![Daily IP Update](https://github.com/ham-zax/opencode2api/actions/workflows/daily-ip-update.yml/badge.svg)](https://github.com/ham-zax/opencode2api/actions/workflows/daily-ip-update.yml)
[![OpenCode](https://img.shields.io/badge/OpenCode%20%2F%20OpenCode2-Compatible-orange?style=flat-square)](https://opencode.ai)
[![Runtime](https://img.shields.io/badge/runtime-Bun%20%7C%20Node.js-black?style=flat-square&logo=bun)](https://bun.sh)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue?style=flat-square&logo=typescript)](https://www.typescriptlang.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)
[![API](https://img.shields.io/badge/API-OpenAI%20Compatible-green?style=flat-square)](https://platform.openai.com/docs/api-reference)
[![Agent Ready](https://img.shields.io/badge/Agents-AGENT__INSTRUCTIONS.md-indigo?style=flat-square)](AGENT_INSTRUCTIONS.md)
[![License](https://img.shields.io/badge/license-MIT-purple?style=flat-square)](LICENSE)

---

## 🎯 Purpose & Overview

**OpenCode2API** (`opencode2api`) is an intelligent reverse proxy gateway built specifically to unlock, accelerate, and balance access to **free text-generation models** hosted on [OpenCode Zen](https://opencode.ai/zen), such as Big Pickle and Muse Spark. It targets the `opencode` provider; OpenCode Go subscriptions use a separate provider and endpoint.

Models are discovered from Zen's live `/v1/models` list and the `opencode` pricing and transport metadata in models.dev. Only zero-cost, active text models using Chat or Responses are admitted. Deprecated entries, Go-only entries, structured-decision models such as Jev, and unsupported transports are excluded. Muse compatibility aliases are generated for future versions automatically. Query `/v1/models` for current IDs instead of relying on a fixed list.

Discovery refreshes on demand after five minutes and before each scheduled health check. Refresh failures retain the last successful catalog for up to 24 hours, including across restarts via `DATA_DIR/models_cache.json`; the gateway fails closed when no valid catalog remains. `/api/models` and `/v1/models` report catalog age, refresh errors, and exclusion reasons. Probes distinguish unavailable models from rate limits and share health with aliases. Requests for a supported but unavailable model may be substituted, with `x-opencode2api-substituted-model` identifying the change. Unknown or excluded IDs return HTTP 400.

Chat clients can use `/v1/chat/completions` for all admitted models: the gateway translates Responses models automatically. Native Responses clients use `/v1/responses` for Responses models. Both endpoints support streaming and JSON replies. The [installed OpenCode2 API capture](docs/opencode2-api.md) records the verified protocol. Native client User-Agent values are preserved; `OPENCODE_USER_AGENT` overrides the gateway's default captured client version.

Direct access to free AI tiers on OpenCode Zen often results in strict single-IP rate limits (`HTTP 429: FreeUsageLimitError`), connection drops, and concurrency bottlenecks. **OpenCode2API** solves this by:
1. **Multi-IP Proxy Slot Pools**: Maintaining an isolated pool of healthy proxy slots per API key.
2. **Anycast Edge Routing**: Routing requests across hundreds of optimized Cloudflare Anycast edge IPs.
3. **Smart 429 Bypass & Failover**: Applying exponential backoff to rate-limited nodes while preserving prompt cache affinity on active nodes.
4. **Drop-in OpenAI & OpenCode2 Compatibility**: Exposing standard `/v1/chat/completions` and `/v1/models` alongside a built-in web management dashboard.

---

## ✨ Key Features

- 🔄 **Native OpenCode & OpenCode2 Support**: Direct provider integration via `@ai-sdk/openai-compatible` in `~/.config/opencode/opencode.json`.
- ⚡ **OpenAI-Compatible Endpoint**: Drop-in replacement for OpenAI SDK, Cursor, Claude Code, Cline, and Continue.
- 🛡️ **Per-Key Slot Allocation**: Each API key holds an isolated pool of proxy slots with independent health monitoring.
- 🚀 **Multi-Tier Fallback Chain**: `Proxy Pool Slots` ➔ `Cloudflare Anycast IPs` ➔ `WARP Tunnel` ➔ `Custom Proxies` ➔ `Direct Upstream`.
- 🤖 **Automated Daily Anycast IP Refresh**: Built-in GitHub Actions workflow and local benchmark script to rank top 500 low-latency edge IPs daily.
- 📊 **Built-In Web Dashboard**: Live traffic analytics, key management, candidate pool health tester, real-time logs, and chat playground.
- 📦 **Zero-Config Deployment**: Optimized Bun and Docker Compose setup.

---

## 🏗️ Architecture

```
                                 Client Request
                     (OpenCode2 CLI / OpenAI SDK / Web App)
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │      OpenCode2API (:13339)     │
                        │  • Auth & Concurrency     │
                        │  • Request Dispatcher     │
                        └─────────────┬─────────────┘
                                      │
                   ┌──────────────────┴──────────────────┐
                   ▼                                     ▼
        ┌─────────────────────┐               ┌─────────────────────┐
        │  Active Slot Pool   │               │   Admin Dashboard   │
        │ (Per-Key RoundRobin)│               │  (Web UI & Metrics) │
        └──────────┬──────────┘               └─────────────────────┘
                   │
         Failover / Fallback Chain
                   ├─────────────────────────┐
                   ▼                         ▼
         ┌───────────────────┐     ┌───────────────────┐
         │  Cloudflare Edge  │     │   WARP Fallback   │
         │  Anycast 500 IPs  │     │  (Global Standby) │
         └─────────┬─────────┘     └─────────┬─────────┘
                   │                         │
                   └────────────┬────────────┘
                                ▼
                   ┌─────────────────────────┐
                   │  OpenCode Zen Upstream  │
                   │    (opencode.ai/zen)    │
                   └─────────────────────────┘
```

---

## ⚡ Quick Start & How to Run

### Option 1: Direct Run in Terminal (Live Interactive Logs)

```bash
# Clone repository
git clone https://github.com/ham-zax/opencode2api.git
cd opencode2api

# Install dependencies
bun install

# Start gateway (Press Ctrl + C anytime to stop)
bun run gate-docker.ts
```

---

### Option 2: Run in Background (Silent Daemon)

```bash
# Start in background
nohup bun run gate-docker.ts > opencode2api.log 2>&1 &

# View live logs anytime
tail -f opencode2api.log

# Stop the background process
pkill -f "gate-docker.ts"
```

---

### Option 3: Run with Docker Compose (Production)

```bash
# Start OpenCode2API container
docker compose up -d

# View live logs
docker compose logs -f

# Stop container
docker compose down
```

Gateway will be live at `http://localhost:13339` with Web Dashboard accessible at `http://localhost:13339/`.

---

## 👤 Human Instructions & OpenCode / OpenCode2 Setup Guide

Follow these steps to configure `opencode` or `opencode2` CLI to use OpenCode2API for unlimited free AI completions.

### Step 1: Start OpenCode2API
Ensure OpenCode2API is running on `http://127.0.0.1:13339` (using Bun or Docker).

### Step 2: Configure `~/.config/opencode/opencode.json`
Add the `opencode2api` provider to your `~/.config/opencode/opencode.json` file under `"provider"`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "opencode2api": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "OpenCode2API",
      "options": {
        "baseURL": "http://127.0.0.1:13339/v1",
        "apiKey": "admin123"
      },
      "models": {
        "big-pickle": { "name": "Big Pickle" },
        "muse-spark-1.3-free": { "name": "Muse Spark 1.3 Free" }
      }
    }
  }
}
```

OpenCode2 v2 uses `providers`, `package`, and `settings`. The equivalent provider entry is:

```json
{
  "providers": {
    "opencode2api": {
      "name": "OpenCode2API",
      "package": "@opencode/ai/providers/openai-compatible",
      "settings": { "baseURL": "http://127.0.0.1:13339/v1", "apiKey": "admin123" },
      "models": {
        "big-pickle": { "name": "Big Pickle" },
        "muse-spark-1.3-free": { "name": "Muse Spark 1.3 Free" }
      }
    }
  }
}
```

Use IDs currently returned by the gateway when adding more models.

### Step 3: Run with OpenCode2 CLI
Verify your models and run queries directly:

```bash
# Verify models are recognized
opencode2 models | grep opencode2api

# Run prompts with Nemotron 3.5 Lightning
opencode2 run --model opencode2api/nemotron-3.5-lightning-free "Explain the difference between TCP and UDP"

# Run prompts with Hunyuan 3 (hy3-free)
opencode2 run --model opencode2api/hy3-free "What is 2+2?"
```

### Step 4: Web Admin Dashboard
Open [`http://localhost:13339/`](http://localhost:13339/) in your browser to inspect proxy slot health, view token usage graphs, create API keys, and test completions in the live chat playground.

### Step 5: Refresh Cloudflare Edge IPs
To benchmark and update the 500 lowest-latency Anycast IPs at any time:
```bash
python3 scripts/update_cf_ips.py
```

---

## 🤖 Agent Instructions

For AI coding assistants, autonomous agents, and pair programmers (OpenCode, Claude Code, Cursor, Agy, Antigravity, and Codex), comprehensive development and operational instructions are available in [`AGENT_INSTRUCTIONS.md`](AGENT_INSTRUCTIONS.md).

Agents working in this codebase should review `AGENT_INSTRUCTIONS.md` for header whitelisting rules, request dispatching policies, and test scripts.

---

## 💻 Standard OpenAI SDK Client Examples

OpenCode2API acts as a drop-in replacement for OpenAI endpoints (`baseURL: http://localhost:13339/v1`).

### Python (OpenAI SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:13339/v1",
    api_key="admin123"
)

response = client.chat.completions.create(
    model="claude-sonnet-4-6",
    messages=[
        {"role": "user", "content": "Explain quantum computing in one sentence."}
    ],
    stream=True
)

for chunk in response:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
print()
```

### JavaScript / TypeScript (OpenAI SDK)

```typescript
import OpenAI from "openai";

const openai = new OpenAI({
  baseURL: "http://localhost:13339/v1",
  apiKey: "admin123",
});

async function main() {
  const stream = await openai.chat.completions.create({
    model: "claude-sonnet-4-6",
    messages: [{ role: "user", content: "Hello from OpenCode2API!" }],
    stream: true,
  });

  for await (const chunk of stream) {
    process.stdout.write(chunk.choices[0]?.delta?.content || "");
  }
}

main();
```

### cURL

```bash
curl http://localhost:13339/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer admin123" \
  -d '{
    "model": "claude-sonnet-4-6",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": false
  }'
```

---

## ⚙️ Configuration Reference

OpenCode2API can be configured via environment variables in `docker-compose.yml` or your local `.env`:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `13339` | HTTP port for API and Dashboard. |
| `API_KEY` | `admin123` | Default master administrator API key. |
| `DATA_DIR` | `./data` | Directory for persistent storage (`keys.json`, `sources.json`, `audit.jsonl`). |
| `SLOTS_PER_KEY` | `3` | Number of concurrent proxy slots allocated per API key. |
| `MAX_ACTIVE_KEYS` | `20` | Maximum number of keys active simultaneously. |
| `WARP_MODE` | `off` | Enable Cloudflare WARP fallback (`on` / `off` / `fallback`). |
| `WARP_HOST` | `127.0.0.1` | Host address of local WARP SOCKS5 service. |
| `WARP_SOCKS5_PORT`| `1080` | Port of WARP SOCKS5 service. |
| `PROXY_REFRESH_MS`| `300000` | Interval (ms) for refreshing candidate proxy pools (5 min). |
| `PROXY_CONNECT_TIMEOUT_MS` | `15000` | Timeout (ms) for establishing a proxy connection; kept separate from generation lifetime. |
| `STREAM_IDLE_TIMEOUT_MS` | `600000` | Upstream SSE inactivity timeout (ms). Each received chunk refreshes the idle timer; this is not a total generation deadline. |
| `CLASH_SUBSCRIBE_URLS`| *(FreeSub YAML)* | Comma-separated Clash/Mihomo subscription URLs. |
| `PROXY_POOL_URL` | `""` | Optional external proxy pool API endpoint. |

---

## 📡 Management API Summary

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves Web Admin Dashboard |
| `POST` | `/v1/chat/completions` | OpenAI-compatible chat completions |
| `GET` | `/v1/models` | List current free Zen models and catalog status |
| `POST` | `/v1/responses` | Native Responses requests for Responses models |
| `GET` | `/api/status` | Complete gateway health, slot, and memory metrics |
| `GET` / `POST` | `/api/keys` | List or create API keys |
| `PUT` / `DELETE`| `/api/keys/:key` | Update key quotas or revoke key |
| `GET` / `POST` | `/api/sources` | Manage proxy sources (add/list/delete) |
| `POST` | `/api/refresh` | Force refresh candidate pool from all sources |
| `GET` | `/api/audit` | Usage metrics, token counts, and cache hit rates |
| `GET` | `/api/logs` | Fetch real-time rolling operational logs |

---

## 📁 Repository Structure

```
opencode2api/
├── AGENT_INSTRUCTIONS.md # Detailed developer and AI agent instructions
├── gate-docker.ts        # Standalone per-key multi-slot proxy pool gateway
├── gate.ts               # SingBox edition reverse proxy gateway
├── Dockerfile            # Container build recipe
├── docker-compose.yml    # Full stack deployment compose file
├── package.json          # Node / Bun package metadata
├── public/               # Web Admin Dashboard frontend assets
│   ├── index.html        # Modern responsive UI
│   ├── app.js            # Frontend state, API client & charts
│   └── style.css         # Styling system & dark/light theme
├── scripts/              # Automation and scraper scripts
│   ├── update_cf_ips.py  # Cloudflare Anycast latency tester & 500ip.txt generator
│   └── push_proxyhub.py  # Automated proxy pool scraper and pusher
├── .github/workflows/
│   └── daily-ip-update.yml # Automated daily GitHub Actions Anycast IP updater
├── WIKI.md               # In-depth technical specification
├── SESSION.md            # Operational session logs
└── 500ip.txt             # Pre-tested low-latency Cloudflare IP seed list
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

## Verification

Run `bun test` for isolated pool, catalog, request-routing, JSON, and SSE regression checks. Tests do not call external services. Check the production bundle with `bun build gate-docker.ts --target=bun --outfile=/tmp/opencode2api.js`.

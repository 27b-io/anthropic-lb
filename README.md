# anthropic-lb

<div align="center">

**Load-balancing reverse proxy for multiple Anthropic API accounts.**

[![Crates.io](https://img.shields.io/crates/v/anthropic-lb.svg)](https://crates.io/crates/anthropic-lb)
[![CI](https://github.com/27b-io/anthropic-lb/actions/workflows/ci.yml/badge.svg)](https://github.com/27b-io/anthropic-lb/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Quick Start](#quick-start) · [Configuration](#configuration) · [Client Setup](#client-setup) · [Security](#security) · [Upstreams](#openai-compatible-upstreams)

</div>

---

## Overview

Routes requests across multiple Anthropic accounts using dynamic capacity-based selection. Tracks utilization per account via Anthropic's rate-limit headers and prefers the one with the most headroom. When an account gets rate-limited (429), it cools down and traffic rotates to the next.

```
                        ┌──────────────────┐
                        │   anthropic-lb    │
  Client ──► auth ──────┤                  ├──► Account A (util: 0.12) ──► api.anthropic.com
             + budget   │  weighted-bucket │
             check      │  routing by      ├──► Account B (util: 0.67)
                        │  headroom        │
                        │                  ├──► Account C (429 — cooling)
                        └────────┬─────────┘
                                 │
                            shadow log
                          + token tracking
```

| Feature | Description |
|:--------|:------------|
| **Weighted routing** | Headroom-proportional bucket hashing with client affinity |
| **Soft utilization ceiling** | Accounts above `soft_limit` excluded from routing, breaking sticky affinity |
| **Time-adjusted utilization** | Discounts utilization near window reset; accounts about to reset get more traffic |
| **Status-based routing** | Parses API status headers — `warning`/`throttled`/`rejected` enforce utilization floors |
| **429 rotation** | Rate-limited accounts cool down, traffic shifts instantly |
| **5xx retry** | Automatic retry on 500/502/503/504/529 (picks different account) |
| **Token tracking** | Per-account and per-client input/output/cache token counters |
| **Client budgets** | Daily per-client token budgets with automatic reset |
| **Auto-cache** | Injects prompt caching beta header automatically |
| **Shadow logging** | Optional JSONL file with request metadata, tokens, latency |
| **Model routing** | Per-account model allowlists with wildcard prefix matching |
| **Client identification** | Via `X-Client-ID` header or IP-based mapping |
| **Streaming** | SSE/streaming responses flow through with usage extraction |
| **State persistence** | Utilization + reset times + status survive restarts |
| **Upstream routing** | Forward to OpenAI-compatible APIs via `/upstream/<name>/...` |
| **~6 MB binary** | Zero runtime dependencies |

---

## Quick Start

```bash
cargo install anthropic-lb

cp config.toml.example config.toml
# Edit config.toml with your account tokens

anthropic-lb config.toml
```

Or build from source:

```bash
cargo build --release
./target/release/anthropic-lb config.toml
```

---

## Configuration

```toml
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
rate_limit_cooldown_secs = 60
probe_interval_secs = 300

# Proxy authentication (optional — omit for local/Tailscale use)
# proxy_key = "your-secret-key-here"

# IP allowlist (optional — omit to allow all source IPs)
# allowed_ips = ["100.64.0.0/10", "10.0.0.0/8"]

# Auto-inject prompt caching beta header (default: true)
# auto_cache = true

# Shadow log — JSONL file with request metadata (optional)
# shadow_log = "shadow.jsonl"

# Utilization soft ceiling (0.0–1.0). Accounts above this are excluded
# from routing unless all accounts exceed it. Default: 0.90
# soft_limit = 0.90

# Per-client daily token budgets (optional)
# [client_budgets]
# "alice" = 5000000    # 5M tokens/day
# "bob" = 1000000      # 1M tokens/day

# IP-to-client-name mapping (optional, fallback when no X-Client-ID header)
# [client_names]
# "10.0.0.5" = "alice-desktop"
# "10.0.0.6" = "bob-laptop"

[[accounts]]
name = "primary"
token = "sk-ant-oat01-..."
# models = ["claude-sonnet-4-20250514", "claude-opus-*"]

[[accounts]]
name = "secondary"
token = "sk-ant-api03-..."
```

### Config Reference

| Field | Type | Default | Description |
|:------|:-----|:--------|:------------|
| `listen` | `String` | — | Bind address (e.g. `"127.0.0.1:8082"`) |
| `upstream` | `String` | — | Anthropic API base URL |
| `rate_limit_cooldown_secs` | `u64` | `60` | Seconds to cool down after 429 |
| `probe_interval_secs` | `u64` | `300` | Seconds between utilization probes (0 = disabled) |
| `proxy_key` | `String?` | `None` | Shared secret for proxy access |
| `allowed_ips` | `[String]?` | `None` | IP/CIDR allowlist |
| `auto_cache` | `bool` | `true` | Inject prompt caching beta header |
| `shadow_log` | `String?` | `None` | Path to JSONL shadow log file |
| `soft_limit` | `f64` | `0.90` | Utilization ceiling — accounts above are excluded from routing |
| `client_names` | `{IP: name}` | `{}` | IP → client ID mapping |
| `client_budgets` | `{name: tokens}` | `{}` | Daily token budget per client |
| `accounts[].name` | `String` | — | Display name for the account |
| `accounts[].token` | `String` | — | API key or `"passthrough"` |
| `accounts[].models` | `[String]` | `[]` | Model allowlist (empty = all) |

### Token Types

| Prefix | Auth method | Notes |
|:-------|:------------|:------|
| `sk-ant-oat*` | `Authorization: Bearer` | OAuth token; beta headers injected automatically |
| `sk-ant-api*` | `x-api-key` | Standard API key |
| `passthrough` | Caller's headers | Forwards client auth as-is |

> [!TIP]
> Use `passthrough` when clients have their own Anthropic credentials and you only want load-balancing without token injection.

### Model Routing

Restrict accounts to specific models with the `models` field:

```toml
[[accounts]]
name = "opus-only"
token = "sk-ant-oat01-..."
models = ["claude-opus-*"]  # Wildcard prefix match

[[accounts]]
name = "sonnet-only"
token = "sk-ant-api03-..."
models = ["claude-sonnet-4-20250514"]  # Exact match

[[accounts]]
name = "general"
token = "sk-ant-oat01-..."
# Empty models = serves all models
```

When a request specifies a model, only accounts whose `models` list matches (exact or prefix wildcard) are considered. Accounts with an empty `models` list serve all models.

---

## Client Setup

### Local / Tailscale (no proxy_key)

Omit `proxy_key` from config. Point Claude Code at the proxy:

```bash
export ANTHROPIC_BASE_URL=http://localhost:8082
```

Clients can use their own OAuth login (`claude login`) or set a dummy `ANTHROPIC_API_KEY` — either way the proxy strips client auth and injects the real account token.

### Exposed to the Internet (with proxy_key)

Set `proxy_key` in config. Clients send it as their API key:

```bash
export ANTHROPIC_BASE_URL=https://your-proxy.example.com
export ANTHROPIC_API_KEY=your-proxy-secret
```

> [!IMPORTANT]
> Claude Code sends the proxy key as `x-api-key`. The proxy validates it and swaps in the real account token. No Anthropic credentials or OAuth login needed on the client.

### Client Identification

Clients are identified for usage tracking and budget enforcement:

1. **`X-Client-ID` header** — explicit, takes priority
2. **`client_names` IP mapping** — fallback based on source IP
3. **`"-"`** — default when neither is set

Per-client token usage and budget status appear in `/_stats`.

---

## Security

Three layers, all optional — use what fits:

| Layer | Config | Effect |
|:------|:-------|:-------|
| **Listen binding** | `listen = "127.0.0.1:8082"` | Only accepts connections on that interface |
| **IP allowlist** | `allowed_ips = ["100.64.0.0/10"]` | Rejects unlisted source IPs (403) |
| **Proxy key** | `proxy_key = "secret"` | Requires `x-api-key` header match (401) |

IP check runs first, then proxy key. Both apply to all endpoints including `/_stats`.

> [!WARNING]
> With no `proxy_key` and no `allowed_ips`, the proxy is **open to all**. This is fine behind Tailscale or on localhost, but never expose an open proxy to the internet.

---

## Endpoints

| Route | Method | Description |
|:------|:-------|:------------|
| `/*` | Any | Proxied to upstream Anthropic API |
| `/v1/chat/completions` | POST | OpenAI-compatible → Anthropic translation |
| `/upstream/{name}/*` | Any | Forwarded to named OpenAI-compatible upstream |
| `/_stats` | GET | JSON stats (utilization, tokens, budgets) |

All endpoints are gated by `proxy_key` and `allowed_ips` when configured.

<details>
<summary><strong>Example <code>/_stats</code> response</strong></summary>

```json
{
  "accounts": [
    {
      "name": "primary",
      "passthrough": false,
      "requests_total": 1042,
      "utilization": 0.25,
      "representative_claim": "five_hour",
      "remaining_requests": 950,
      "remaining_tokens": 4800000,
      "hard_limited_remaining_secs": null,
      "token_usage": {
        "input_tokens": 2450000,
        "output_tokens": 180000,
        "cache_creation_input_tokens": 50000,
        "cache_read_input_tokens": 1200000
      }
    }
  ],
  "upstreams": [
    {
      "name": "portkey",
      "base_url": "https://portkey.example.com/v1",
      "requests_total": 87
    }
  ],
  "client_usage": {
    "alice": {
      "input_tokens": 1200000,
      "output_tokens": 90000,
      "cache_creation_input_tokens": 25000,
      "cache_read_input_tokens": 600000
    }
  },
  "client_budgets": {
    "alice": { "limit": 5000000, "used": 1915000, "remaining": 3085000 }
  },
  "strategy": "dynamic-capacity"
}
```

</details>

---

## OpenAI-Compatible Upstreams

Route requests to non-Anthropic APIs (OpenRouter, Portkey, local models) via named upstreams:

```toml
[[upstreams]]
name = "openrouter"
base_url = "https://openrouter.ai/api"
api_key = "sk-or-..."
```

Requests to `/upstream/openrouter/v1/chat/completions` are forwarded to `https://openrouter.ai/api/v1/chat/completions` with the API key injected as `Authorization: Bearer`.

---

## How It Works

```
1. Request arrives → validate proxy_key + IP allowlist
2. Identify client (X-Client-ID header → IP mapping → "-")
3. Check per-client daily token budget (429 if exceeded)
4. Extract model from request body
5. Filter accounts by model allowlist
6. Compute time-adjusted utilization (discount near window reset)
7. Apply status floors (warning ≥ 0.80, throttled ≥ 0.98, rejected = 1.0)
8. Exclude accounts above soft_limit utilization ceiling
9. Pick account via headroom-proportional weighted bucket hashing (client affinity)
10. Inject auth token + auto-cache header
11. Forward request to upstream Anthropic API
12. If 429 → mark rate-limited, retry with next account
13. If 5xx/529 → retry with different account
14. Parse rate-limit headers (utilization, reset times, status)
15. Extract token usage from response (streaming SSE or JSON body)
16. Record usage per-account + per-client, update budget
17. Write shadow log entry (async, non-blocking)
18. State persisted to disk, restored on restart
```

> [!TIP]
> The proxy reads Anthropic's `anthropic-ratelimit-unified-*` headers to track real utilization per rate-limit window (5h, 7d). Near window resets, utilization is time-discounted so accounts about to reset aren't unnecessarily avoided. API status signals (`allowed_warning`, `throttled`, `rejected`) enforce utilization floors regardless of the reported number.

---

## Shadow Logging

When `shadow_log` is set, every request writes a JSONL entry with:

```json
{
  "ts": "2026-02-13T20:15:00Z",
  "client": "alice",
  "account": "primary",
  "model": "claude-sonnet-4-20250514",
  "streaming": true,
  "latency_ms": 2340,
  "input_tokens": 1500,
  "output_tokens": 450,
  "cache_creation_input_tokens": 0,
  "cache_read_input_tokens": 800
}
```

Logging is fire-and-forget via an async channel — handlers never block on disk I/O.

---

## Deployment

```bash
# Build
cargo build --release

# Run directly
./target/release/anthropic-lb /path/to/config.toml

# Or install as a systemd service
sudo cp anthropic-lb.service /etc/systemd/system/
sudo systemctl enable --now anthropic-lb
```

<details>
<summary><strong>Example systemd unit</strong></summary>

```ini
[Unit]
Description=Anthropic LB - Load-balancing proxy for Anthropic API
After=network.target

[Service]
ExecStart=/usr/local/bin/anthropic-lb /opt/anthropic-lb/config.toml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

</details>

---

## Testing

```bash
# Run all tests (87 tests)
cargo test

# Lint gates (same as CI)
cargo fmt --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets

# Coverage report (requires cargo-llvm-cov)
cargo llvm-cov
```

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**[Crates.io](https://crates.io/crates/anthropic-lb)** · **[GitHub](https://github.com/27b-io/anthropic-lb)**

</div>

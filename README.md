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

Routes requests across multiple Anthropic accounts using dynamic capacity-based selection. Tracks utilization per account and prefers the one with the most headroom. When an account gets rate-limited (429), it cools down and traffic rotates to the next.

```
                        ┌──────────────┐
                        │  anthropic-lb │
  Client ──► proxy_key ─┤              ├─► Account A (util: 0.12) ──► api.anthropic.com
              check     │  pick lowest │
                        │  utilization ├─► Account B (util: 0.67)
                        │              │
                        │              ├─► Account C (429 — cooling)
                        └──────────────┘
```

| Feature | Description |
|:--------|:------------|
| **Dynamic routing** | Selects account with lowest utilization, not round-robin |
| **429 rotation** | Rate-limited accounts cool down, traffic shifts instantly |
| **Streaming** | SSE/streaming responses flow through without buffering |
| **State persistence** | Utilization data survives restarts |
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

[[accounts]]
name = "primary"
token = "sk-ant-oat01-..."

[[accounts]]
name = "secondary"
token = "sk-ant-api03-..."
```

### Token Types

| Prefix | Auth method | Notes |
|:-------|:------------|:------|
| `sk-ant-oat*` | `Authorization: Bearer` | OAuth token; beta headers injected automatically |
| `sk-ant-api*` | `x-api-key` | Standard API key |
| `passthrough` | Caller's headers | Forwards client auth as-is |

> [!TIP]
> Use `passthrough` when clients have their own Anthropic credentials and you only want load-balancing without token injection.

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
| `/upstream/{name}/*` | Any | Forwarded to named OpenAI-compatible upstream |
| `/_stats` | GET | JSON stats (utilization, rate limits, request counts) |

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
      "hard_limited_remaining_secs": null
    }
  ],
  "upstreams": [
    {
      "name": "openrouter",
      "base_url": "https://openrouter.ai/api",
      "requests_total": 87
    }
  ],
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
2. Pick account with lowest utilization (most headroom)
3. Forward request with that account's auth token
4. If upstream returns 429 → mark rate-limited, try next account
5. Cooled-down accounts re-enter rotation after cooldown period
6. Periodic probes refresh utilization data for each account
7. State persisted to disk, restored on restart
```

> [!TIP]
> The proxy reads Anthropic's `anthropic-ratelimit-unified-*` headers to track real utilization per rate-limit window (5h, 7d). This is more accurate than counting requests locally.

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
# Run all tests (14 tests, ~62% line coverage)
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

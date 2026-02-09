# anthropic-lb

Load-balancing reverse proxy for multiple Anthropic API accounts.

Routes requests across multiple accounts using dynamic capacity-based selection. Tracks utilization per account and prefers the one with the most headroom. When an account gets rate-limited (429), it's cooled down and traffic rotates to the next.

## Quick Start

```bash
cp config.toml.example config.toml
# Edit config.toml with your account tokens
cargo run --release
```

## Config

```toml
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
strategy = "dynamic-capacity"
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
token = "sk-ant-oat01-..."
```

### Token types

- `sk-ant-oat*` — OAuth token, injected as `Authorization: Bearer` (with required OAuth beta headers)
- `sk-ant-api*` — API key, injected as `x-api-key`
- `passthrough` — forwards caller's auth headers as-is

## Client Setup

### Local / Tailscale (no proxy_key)

When running behind Tailscale or on localhost, omit `proxy_key` from config. The proxy is open — no client auth needed. Just point Claude Code at it:

```bash
export ANTHROPIC_BASE_URL=http://localhost:8082
```

Clients can use their own OAuth login (`claude login`) or set a dummy `ANTHROPIC_API_KEY` — either way the proxy strips the client's auth and injects the real account token.

### Exposed to the internet (with proxy_key)

Set `proxy_key` in config. Clients must send it as their API key:

```bash
export ANTHROPIC_BASE_URL=https://your-proxy.example.com
export ANTHROPIC_API_KEY=your-proxy-secret
```

Claude Code sends the proxy key as `x-api-key`, the proxy validates it and swaps in the real account token. No Anthropic credentials or OAuth login needed on the client.

## Security

Three layers, all optional, use what fits:

| Layer | Config | Effect |
|---|---|---|
| **Listen binding** | `listen = "127.0.0.1:8082"` | Only accepts connections on that interface |
| **IP allowlist** | `allowed_ips = ["100.64.0.0/10"]` | Rejects connections from unlisted source IPs (403) |
| **Proxy key** | `proxy_key = "secret"` | Requires `x-api-key` header to match (401) |

IP check runs first, then proxy key. Both apply to all endpoints including `/_stats`.

## Endpoints

- `/*` — proxied to upstream Anthropic API
- `/_stats` — JSON stats (utilization, rate limits, request counts per account)

Both endpoints are gated by `proxy_key` when configured.

## How It Works

1. Request arrives → validate `proxy_key` if configured
2. Pick the account with the lowest utilization (most headroom)
3. Forward request with that account's auth token
4. If upstream returns 429 → mark account as rate-limited, try next
5. Cooled-down accounts re-enter rotation after `rate_limit_cooldown_secs`
6. Periodic probes refresh utilization data for each account
7. State is persisted to disk and restored on restart

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

~6MB static binary. Zero runtime dependencies.

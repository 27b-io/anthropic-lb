# anthropic-lb

Local load-balancing reverse proxy for multiple Anthropic Max OAuth accounts.

Sits on `localhost`, accepts Anthropic API requests, and round-robins them across multiple accounts. When an account gets rate-limited (429), it's cooled down and traffic rotates to the next.

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
strategy = "round-robin"
rate_limit_cooldown_secs = 60

[[accounts]]
name = "primary"
token = "sk-ant-..."

[[accounts]]
name = "secondary"
token = "sk-ant-..."
```

## Usage with OpenClaw

Point OpenClaw's Anthropic provider at the proxy:

```json5
{
  models: {
    providers: {
      anthropic: {
        baseUrl: "http://127.0.0.1:8082",
        // ... rest of config
      }
    }
  }
}
```

## Endpoints

- `/*` — proxied to upstream Anthropic API
- `/_stats` — JSON stats (requests per account, rate limit status)

## How It Works

1. Request arrives → pick next account (round-robin)
2. Forward request with that account's auth token
3. If upstream returns 429 → mark account as rate-limited, try next
4. Cooled-down accounts re-enter rotation after `rate_limit_cooldown_secs`
5. If ALL accounts are rate-limited → return 429 to caller

## Binary size

~6MB release build. Zero runtime dependencies.

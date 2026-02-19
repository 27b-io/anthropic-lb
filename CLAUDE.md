# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development

```bash
cargo build                          # Debug build
cargo build --release                # Release build (~6MB binary)
cargo test                           # Run all tests
cargo test <test_name>               # Run a single test (e.g. cargo test pick_account_filters_by_model)
cargo fmt --check                    # Format check (CI gate)
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets  # Lint (CI gate, warnings are errors)
cargo llvm-cov                       # Coverage report (requires cargo-llvm-cov)
```

Run the proxy: `./target/release/anthropic-lb config.toml`

## Architecture

Single-file Rust binary (`src/main.rs`, ~8500 lines) with inline tests. No library crate — everything lives in one file with section markers.

### Core Data Flow

```text
Request → IP allowlist check → proxy_key auth → pre_request_gate(operator bypass → budget → utilization limit → emergency brake) → pick_account(model) → forward to upstream → parse rate-limit headers → extract token usage → shadow log → persist state (+ Redis sync)
```

### Key Sections (in source order)

| Section | What it does |
|---------|-------------|
| **Config** (`Config`, `AccountConfig`, `UpstreamConfig`) | TOML deserialization structs |
| **Runtime state** (`AppState`, `Account`, `RateLimitInfo`) | Shared via `Arc<AppState>`, per-account `RwLock<RateLimitInfo>`, atomic counters, optional Redis `ConnectionManager` |
| **Persistence** (`PersistedState`) | JSON state file at `<config_path>.state.json`, saved after every request and on shutdown. Redis for cross-replica state when configured. |
| **Token usage** (`TokenUsage`, `record_usage`) | Extracts token counts from responses (streaming SSE + non-streaming JSON), tracks per-account and per-client |
| **Auto-cache** (`inject_cache_breakpoints`) | Injects up to 3 prompt cache breakpoints (last tool, system, last user message) unless cache_control already present |
| **Handlers** | Four axum handlers: `proxy_handler` (main Anthropic proxy), `upstream_handler` (OpenAI-compatible passthrough), `stats_handler` (`/_stats` JSON), `openai_chat_handler` (OpenAI→Anthropic format translation) |
| **OpenAI compatibility** (`translate_*`, `StreamContext`) | Translates `/v1/chat/completions` requests/responses between OpenAI and Anthropic formats, including streaming SSE |
| **Tests** (`mod tests`) | Inline at bottom — unit + integration tests using mock upstream servers |

### Account Selection (`pick_account`)

Headroom-proportional weighted bucket hashing:
1. Filter by model compatibility (if account has `models` allowlist)
2. Skip hard-limited (429) accounts
3. Each remaining account gets a bucket proportional to `(1.0 - utilization)`
4. Affinity key (client+session hash) provides sticky routing; no-affinity uses Fibonacci scatter
5. Retries on 429 (marks hard-limited) and 5xx/529 (rotates without marking)

### Token Type Detection (by prefix)

- `sk-ant-oat*` → `Authorization: Bearer` + injects `anthropic-beta: oauth-2025-04-20` and `anthropic-dangerous-direct-browser-access: true`. The OpenAI-compat handler additionally injects `claude-code-20250219` beta flag.
- `sk-ant-api*` → `x-api-key` header
- `passthrough` → forwards caller's auth headers untouched

### Upstream Routing

Named OpenAI-compatible upstreams configured in `[[upstreams]]` TOML sections. Requests to `/upstream/{name}/*` are forwarded with `Authorization: Bearer` API key injection.

### Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | required | Bind address (e.g. `"0.0.0.0:8080"`) |
| `upstream` | string | required | Anthropic API base URL |
| `proxy_key` | string? | none | Shared secret for `x-api-key` auth |
| `allowed_ips` | string[]? | none (allow all) | IP/CIDR allowlist |
| `auto_cache` | bool? | true | Auto-inject prompt cache breakpoints |
| `shadow_log` | string? | none | Path for JSONL audit trail |
| `soft_limit` | f64? | 0.90 | Utilization ceiling — accounts above excluded from routing |
| `client_names` | map? | {} | IP→client name mapping |
| `client_budgets` | map? | {} | client_id→daily token limit |
| `client_utilization_limits` | map? | {} | client_id→utilization ceiling (0.0–1.0) |
| `operators` | string[]? | [] | Client IDs that bypass all enforcement |
| `emergency_threshold` | f64? | 0.95 | All-accounts utilization threshold for emergency brake |
| `redis_url` | string? | none | Redis/Valkey URL for distributed state (`redis://` or `rediss://`) |
| `accounts[].name` | string | required | Account display name |
| `accounts[].token` | string | required | API key, OAuth token, or `"passthrough"` |
| `accounts[].models` | string[]? | [] (all) | Model allowlist (supports `*` suffix wildcards) |

## Testing Patterns

Tests use a `spawn_mock_upstream()` helper that starts a real TCP listener returning canned Anthropic-style responses with rate-limit headers. Integration tests bind to `127.0.0.1:0` (random port) and make real HTTP requests through the full axum router with `ConnectInfo<SocketAddr>`.

`test_state_with()` and `test_app()` are the two test fixture builders — the former for unit tests (no HTTP), the latter for integration tests (full router + mock upstream). `test_openai_app()` builds a minimal router for OpenAI-compat handler tests.

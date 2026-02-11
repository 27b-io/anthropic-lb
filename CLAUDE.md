# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development

```bash
cargo build                          # Debug build
cargo build --release                # Release build (~6MB binary)
cargo test                           # Run all 32 tests
cargo test <test_name>               # Run a single test (e.g. cargo test pick_prefers_lowest_utilization)
cargo fmt --check                    # Format check (CI gate)
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets  # Lint (CI gate, warnings are errors)
cargo llvm-cov                       # Coverage report (requires cargo-llvm-cov)
```

Run the proxy: `./target/release/anthropic-lb config.toml`

## Architecture

Single-file Rust binary (`src/main.rs`, ~2000 lines) with inline tests. No library crate — everything lives in one file with section markers.

### Core Data Flow

```
Request → IP allowlist check → proxy_key auth → pick_account() → forward to upstream → parse rate-limit headers → persist state
```

### Key Sections (in source order)

| Section | What it does |
|---------|-------------|
| **Config** (`Config`, `AccountConfig`, `UpstreamConfig`) | TOML deserialization structs |
| **Runtime state** (`AppState`, `Account`, `RateLimitInfo`) | Shared via `Arc<AppState>`, per-account `RwLock<RateLimitInfo>`, atomic counters |
| **Persistence** (`PersistedState`) | JSON state file at `<config_path>.state.json`, saved after every request and on shutdown |
| **Handlers** | Four axum handlers: `proxy_handler` (main Anthropic proxy), `upstream_handler` (OpenAI-compatible passthrough), `stats_handler` (`/_stats` JSON), `openai_chat_handler` (OpenAI→Anthropic format translation) |
| **OpenAI compatibility** (`translate_*`, `StreamContext`) | Translates `/v1/chat/completions` requests/responses between OpenAI and Anthropic formats, including streaming SSE |
| **Tests** (`mod tests`) | Inline at bottom — unit tests for IP/account selection + integration tests using a mock upstream server |

### Account Selection (`pick_account`)

Two-tier strategy:
1. **Dynamic capacity**: pick account with lowest `utilization` value (from Anthropic's `anthropic-ratelimit-unified-*` headers). Hard-limited (429) accounts are skipped.
2. **Round-robin fallback**: when no rate-limit data exists yet.

### Token Type Detection (by prefix)

- `sk-ant-oat*` → `Authorization: Bearer` + injects `anthropic-beta: oauth-2025-04-20` and `anthropic-dangerous-direct-browser-access: true`. The OpenAI-compat handler additionally injects `claude-code-20250219` beta flag.
- `sk-ant-api*` → `x-api-key` header
- `passthrough` → forwards caller's auth headers untouched

### Upstream Routing

Named OpenAI-compatible upstreams configured in `[[upstreams]]` TOML sections. Requests to `/upstream/{name}/*` are forwarded with `Authorization: Bearer` API key injection.

## Testing Patterns

Tests use a `spawn_mock_upstream()` helper that starts a real TCP listener returning canned Anthropic-style responses with rate-limit headers. Integration tests bind to `127.0.0.1:0` (random port) and make real HTTP requests through the full axum router with `ConnectInfo<SocketAddr>`.

`test_state_with()` and `test_app()` are the two test fixture builders — the former for unit tests (no HTTP), the latter for integration tests (full router + mock upstream). `test_openai_app()` builds a minimal router for OpenAI-compat handler tests.

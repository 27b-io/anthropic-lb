# AnthropicLB

Usage-aware load balancing proxy for Claude Code

## Anthropic Rate Limit Windows (Unified Headers)

The proxy reads utilization from `anthropic-ratelimit-unified-*` response headers. These govern subscription-based access (Claude Code, Pro, Max plans) and are separate from the per-minute token bucket limits documented at platform.claude.com.

**Two windows:**
- **5h window** — Fixed-duration window. Starts when usage begins, resets at a specific time (hard reset to zero). Dashboard shows "resets in X min." NOT a smooth sliding window — utilization does not gradually decay. It stays constant or increases within a window, then drops to zero at reset.
- **7d window** — Weekly ceiling. Per-model sub-budgets ("claims") tracked separately (e.g., `seven_day_sonnet`, `seven_day_opus`). The `representative-claim` header indicates which window currently constrains the account.

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

## Deployment

Production runs on the **mem** k8s cluster (`kubectl --context mem`), namespace `anthropic-lb`, managed by **Flux** from `27b-io/fleet-infra` repo.

| What | Where |
|------|-------|
| Flux manifests | `27b-io/fleet-infra` repo, `apps/mem/anthropic-lb/` |
| Config template | `externalsecret.yaml` (ExternalSecret → 1Password tokens + redis password) |
| Image policy | Flux `imagepolicy` auto-updates digest from `ghcr.io/27b-io/anthropic-lb:main` |
| Replicas | 2 (RollingUpdate, maxUnavailable=0) |
| Config delivery | init container copies secret → `/data/config.toml` at pod start |

**Config changes** require a pod restart after the ExternalSecret refreshes (secret is copied at init time, not watched):

```bash
kubectl --context mem -n anthropic-lb rollout restart deployment/anthropic-lb
kubectl --context lab -n mcp           rollout restart deployment/anthropic-lb
```

Also deployed on `lab` (namespace `mcp`) as a 2-pod Deployment serving real Claude Code traffic via Tailscale. There is no local systemd unit.

## Architecture

Single-file Rust binary (`src/main.rs`, ~12000 lines) with inline tests. No library crate — everything lives in one file with section markers.

### Core Data Flow

```text
Request → IP allowlist check → proxy_key auth → pre_request_gate(operator bypass → budget → utilization limit → emergency brake) → pick_endpoint(affinity, model, skip) → forward to endpoint → parse rate-limit headers → extract token usage → shadow log → persist state (+ Redis sync)
```

### Key Sections (in source order)

| Section | What it does |
|---------|-------------|
| **Config** (`Config`, `EndpointConfig`) | TOML deserialization structs |
| **Runtime state** (`AppState`, `Endpoint`, `RateLimitInfo`) | Shared via `Arc<AppState>`, per-endpoint `RwLock<RateLimitInfo>`, atomic counters, optional Redis `ConnectionManager` |
| **Persistence** (`PersistedState`) | JSON state file at `<config_path>.state.json`, saved after every request and on shutdown. Redis for cross-replica state when configured. |
| **Token usage** (`TokenUsage`, `record_usage`) | Extracts token counts from responses (streaming SSE + non-streaming JSON), tracks per-endpoint and per-client |
| **Auto-cache** (`inject_cache_breakpoints`) | Injects up to 3 prompt cache breakpoints (last tool, system, last user message) unless cache_control already present |
| **Handlers** | Four axum handlers: `proxy_handler` (main Anthropic proxy), `stats_handler` (`/_stats` JSON), `metrics_handler` (`/metrics` Prometheus), `openai_chat_handler` (OpenAI→Anthropic format translation) |
| **OpenAI compatibility** (`translate_*`, `StreamContext`) | Translates `/v1/chat/completions` requests/responses between OpenAI and Anthropic formats, including streaming SSE |
| **Tests** (`mod tests`) | Inline at bottom — unit + integration tests using mock upstream servers |

### Endpoint Selection (`pick_endpoint`)

Headroom-proportional weighted bucket hashing:
1. Filter by model compatibility (if endpoint has `models` allowlist)
2. Skip endpoints in the `skip` list (already tried in this retry loop)
3. Skip hard-limited (429) endpoints
4. Each remaining endpoint gets a bucket proportional to `(1.0 - utilization)`
5. Affinity key (client+session hash) provides sticky routing; no-affinity uses Fibonacci scatter
6. On 429 or 5xx/529, the failed endpoint index is added to `skip` and `pick_endpoint` is called again, guaranteeing a different endpoint on retry

### Token Type Detection

`protocol = "anthropic"` endpoints use prefix-based auth on the configured `token`:

- `sk-ant-oat*` → `Authorization: Bearer` + injects `anthropic-beta: oauth-2025-04-20` and `anthropic-dangerous-direct-browser-access: true`. The OpenAI-compat handler additionally injects `claude-code-20250219` beta flag.
- `sk-ant-api*` → `x-api-key` header
- `passthrough` → forwards caller's auth headers untouched

`protocol = "openai"` endpoints use `Authorization: Bearer` with the configured `token`.

### OAuth System Prompt Requirement

OAuth tokens (`sk-ant-oat*`) require the exact system prompt `"You are Claude Code, Anthropic's official CLI for Claude."` as the **first** system block to access sonnet/opus models. Without it, the API returns `400 invalid_request_error` with the unhelpful message `"Error"`. Haiku works without it.

`inject_oauth_system_prompt()` handles this automatically for both handlers when OAuth endpoints are configured. It prepends the prompt block, preserving any existing system content as subsequent blocks. Runs before auto-cache injection (which may add `cache_control` to the system block).

### Unified Endpoints

All routing targets are `[[endpoints]]` entries — there is one endpoint pool, no separate account/upstream concepts. Each endpoint has a `protocol`:

- `protocol = "anthropic"` (default) — an Anthropic-native endpoint. `base_url` defaults to `https://api.anthropic.com`.
- `protocol = "openai"` — an OpenAI-compatible endpoint. `base_url` is required and must be `https://`. When selected, the request is forwarded with automatic Anthropic↔OpenAI translation (`proxy_handler`) or direct passthrough (`openai_chat_handler`); streaming is supported on both paths.

### Endpoint Priority

All endpoints (anthropic and openai) share one priority space via the `priority` field (u32, default 0; lower = preferred). `pick_endpoint` partitions all candidates by priority and tries tiers in ascending order.

Within a tier: healthy candidates (`gate < soft_limit`) are preferred; if none are healthy, the tier degrades to its soft-limited candidates. Routing only advances to the next tier when the current tier has **zero total weight** (genuinely exhausted). So `soft_limit` is intra-tier load-shedding — it never causes a tier jump. Free capacity is fully drained before any paid (overage or OpenAI-endpoint) tier is touched.

An `openai`-protocol endpoint is a first-class routing candidate at its configured `priority` — it replaces the old `fallback_upstream`. Set it high (e.g. 100) so it is tried only after all Anthropic endpoint tiers. A startup `warn!` fires if an `openai` endpoint shares the lowest priority tier with an `anthropic` endpoint.

### Overage Awareness

When an endpoint serves via Anthropic **overage** (paid extra usage — `anthropic-ratelimit-unified-overage-in-use: true`), its exhausted 5h/7d subscription windows are superseded: the routing gate is computed from the overage window instead, so the endpoint stays routable. Its effective priority is demoted by `overage_penalty` (default 10) so free subscription capacity is always preferred. When the overage window itself fills (`overage-utilization` → 1.0) the endpoint's weight drops to 0 and routing moves on. The demotion auto-clears when the subscription window refills (`overage-in-use` goes absent → `false`).

### Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | required | Bind address (e.g. `"0.0.0.0:8080"`) |
| `proxy_key` | string? | none | Shared secret for `x-api-key` auth |
| `allowed_ips` | string[]? | none (allow all) | IP/CIDR allowlist |
| `auto_cache` | bool? | true | Auto-inject prompt cache breakpoints |
| `shadow_log` | string? | none | Path for JSONL audit trail |
| `soft_limit` | f64? | 0.90 | Utilization ceiling — endpoints above this are deprioritized within their tier; they are considered only when no healthy candidate is available and the tier degrades to its soft-limited members |
| `client_names` | map? | {} | IP→client name mapping |
| `client_budgets` | map? | {} | client_id→daily token limit |
| `client_utilization_limits` | map? | {} | client_id→utilization ceiling (0.0–1.0) |
| `operators` | string[]? | [] | Client IDs that bypass budget, utilization, and emergency brake enforcement (trust-based, not IP-verified; does not bypass IP allowlist) |
| `emergency_brake` | bool? | true | Enable/disable the emergency brake |
| `emergency_threshold` | f64? | 0.88 | Utilization threshold for the emergency brake — applied only to `Protocol::Anthropic` endpoints; OpenAI endpoints (stub `RateLimitInfo`) are excluded so they cannot prevent the brake from firing |
| `redis_url` | string? | none | Redis/Valkey URL for distributed state (`redis://` or `rediss://`) |
| `overage_penalty` | u32? | 10 | Priority penalty added to an endpoint while it serves via overage |
| `endpoints[].name` | string | required | Endpoint display name |
| `endpoints[].protocol` | string? | `"anthropic"` | `"anthropic"` (default) or `"openai"` |
| `endpoints[].base_url` | string? | `https://api.anthropic.com` | Base URL. Defaults to the Anthropic API for `anthropic`; required (and must be `https://`) for `openai` |
| `endpoints[].token` | string | required | API key, OAuth token, or `"passthrough"` |
| `endpoints[].models` | string[]? | [] (all) | Model allowlist (supports `*` suffix wildcards) |
| `endpoints[].priority` | u32? | 0 | Priority tier (0 = highest). Lower tiers tried first |


**Key headers parsed:**

| Header | Meaning |
|--------|---------|
| `anthropic-ratelimit-unified-representative-claim` | Which window is the binding constraint (e.g., `five_hour`, `seven_day_sonnet`) |
| `anthropic-ratelimit-unified-5h-utilization` | Raw 5h usage fraction (0.0–1.0) |
| `anthropic-ratelimit-unified-5h-reset` | Epoch timestamp when 5h window resets to zero |
| `anthropic-ratelimit-unified-7d-utilization` | Raw 7d usage fraction (0.0–1.0) |
| `anthropic-ratelimit-unified-7d-reset` | Epoch timestamp when 7d window resets |
| `anthropic-ratelimit-unified-5h-status` / `7d-status` | API pressure signal: `allowed`, `allowed_warning`, `throttled`, `rejected` |
| `anthropic-ratelimit-unified-overage-in-use` | Endpoint is currently serving via paid overage (always overwritten; absent → `false`) |
| `anthropic-ratelimit-unified-overage-status` | Overage window status — feeds the routing gate floor while overage is in use |
| `anthropic-ratelimit-unified-overage-utilization` | Overage budget consumed (0.0–1.0) |
| `anthropic-ratelimit-unified-overage-reset` | Epoch timestamp when the overage window resets |

**Logged values:** `util_5h` and `util_7d` in request/probe logs show **raw API values** (`info.utilization_5h`, `info.utilization_7d`). The `utilization` field shows the effective (time-adjusted) value used for routing decisions.

**Peak hour adjustments:** Anthropic dynamically reduces 5h token allowances during peak hours (05:00–11:00 PT weekdays). ~7% of users affected. Weekly caps unchanged.

## Testing Patterns

Tests use a `spawn_mock_upstream()` helper that starts a real TCP listener returning canned Anthropic-style responses with rate-limit headers. Integration tests bind to `127.0.0.1:0` (random port) and make real HTTP requests through the full axum router with `ConnectInfo<SocketAddr>`.

`test_state_with()` and `test_app()` are the two test fixture builders — the former for unit tests (no HTTP), the latter for integration tests (full router + mock upstream). `test_openai_app()` builds a minimal router for OpenAI-compat handler tests.

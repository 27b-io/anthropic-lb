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

## Releases (release-please)

Releases are automated by release-please (`release-please.yml` +
`release-please-config.json`); merging its rolling release PR tags `vX.Y.Z`,
which triggers `release.yml` (build + crates.io) and `docker.yml`.

- **Squash-merge titles must be conventional commits** — `feat: ...`,
  `fix: ...`, `feat!: ...`; ticket refs go in the scope (e.g.
  `fix(LAB-932): ...`). Non-conventional subjects are invisible to
  release-please and produce no release. The repo squash-merges with the
  PR title as the subject and a blank body, so the PR title is the whole
  commit message release-please sees unless the PR body carries an
  override block (below).
- **Scoped breaking changes use the spec form `type(scope)!: summary`**
  (e.g. `fix(LAB-3214)!: ...`). `type!(scope): ...` is NOT parsed —
  release-please silently drops the commit (no notes entry, no version
  bump); #172 was missed this way and its 0.2.5 entry restored by hand.
- **`BREAKING CHANGE:` footers go in the PR body** (the blank squash body
  discards branch-commit footers), inside an override block. The block
  replaces the whole commit message, so repeat the header:

  ```
  BEGIN_COMMIT_OVERRIDE
  fix(LAB-3214)!: summary

  BREAKING CHANGE: what breaks
  END_COMMIT_OVERRIDE
  ```
- **Pre-1.0 is pinned**: breaking changes bump the minor (never to 1.0.0),
  features bump the patch, and 0.x GitHub releases are flagged pre-release.
- **Releasing 1.0.0** is an explicit human act: land a commit with a
  `Release-As: 1.0.0` footer, then drop `bump-minor-pre-major` and
  `bump-patch-for-minor-pre-major` from `release-please-config.json`
  (`prerelease: true` is inert at major ≥ 1 and can stay).
- **Release PR bodies are machine-parsed** — release-please reads its own
  PR body on merge to create the tag; any bot that rewrites PR descriptions
  breaks the release (LAB-1675/#132: Kody did exactly this to #126). Kody
  must keep `chore(main): release` in its Ignored Title Keywords (Kodus
  console → Settings → Code Review → General); there is no in-repo config
  for this unless the console's config-file override is enabled.
- **crates.io requires trusted publishing** — a green `release.yml` `publish`
  job is the only publishing path; there is no manual token fallback. The
  `publish` job deliberately gates on `validate` only, not the binary matrix.

## Deployment

Deployed via GitOps to internal Kubernetes clusters as a multi-replica
`Deployment` (RollingUpdate, `maxUnavailable=0`). Cluster names, namespaces,
manifest paths, and the operational runbook live in a **private ops repo** —
they are intentionally not documented here.

Operational notes that affect the code:
- **Config delivery:** an init container copies the rendered config into the
  pod at startup; the running process does not watch it. **Config changes
  therefore require a pod restart** after the secret store refreshes.
- **Image updates:** the deployed image digest is updated automatically from
  the container registry; picking up a new image or config needs a pod restart.

## Architecture

Single Rust binary, no library crate. `src/main.rs` holds startup (`main`, config validation) and declares one module per subsystem; the table below says which file holds what. The crate keeps one flat namespace: every module opens with `use crate::*;` and `src/main.rs` glob-imports every module that exports names (`use state::*;` …), so an item used outside its own module is `pub(crate)` and nothing needs a module path. `routing` exports no names today (it is all `impl AppState`), so it has no glob: the first `pub(crate)` item another module uses from it needs its `use routing::*;` line. `persistence` exports only the test-only `FROZEN_NOW` clock override (its own types are private to it), so its glob is `#[cfg(test)] use persistence::*;`; its first non-test export drops the `#[cfg(test)]`. The external imports at the top of `src/main.rs` reach every module through that glob; an import only one module needs goes in that module, below its `use crate::*;`. The feature-gated `guard` module is used by path (`guard::…`). Tests sit next to the code they test, as `#[cfg(test)] mod tests` in `src/<module>/tests.rs`, split into `src/<module>/tests/<area>.rs` where a module has many; fixtures used by the tests of more than one module live in `src/test_support.rs`, and tests of `src/main.rs` itself in `src/tests.rs`. Put a new item in the module that owns its subsystem; do not grow `src/main.rs`.

### Core Data Flow

```text
Request → resolve_client_ip(peer, x-forwarded-for vs trusted_proxies) → IP allowlist check → authenticate([[clients]] key, else legacy proxy_key) → throttle failed credentials (valid principals always pass) → resolve client_id (authenticated principal, else x-client-id/IP map) → pre_request_gate(operator bypass → model allow-list → budget → utilization limit → emergency brake) → pick_endpoint(affinity, model, skip) → forward to endpoint → parse rate-limit headers → extract token usage → shadow log → persist state (+ Redis sync)
```

### Where things live

Some module names are older than their contents; this is the map.

| File | Holds |
|------|-------|
| `src/config.rs` | TOML config structs (`Config`, `ClientConfig`, `EndpointConfig`, `Protocol`, `RoutingStrategy`) |
| `src/state.rs` | `AppState`, `Endpoint`, `RateLimitInfo`: shared via `Arc<AppState>`, per-endpoint `RwLock<RateLimitInfo>`, atomic counters, optional fred `RedisClient` (auto-reconnecting). Also EWMA / burn rate, label bounding, request-body admission, affinity hashing and content fingerprints |
| `src/response_cache.rs` | The encrypted response cache (`ResponseCache`) **and** the admission surface: IP allow-list, failed-auth throttle, `authenticate`, `authorize_admin`, `resolve_client_ip` / `resolve_client_id`, client model allow-lists, the guard hook |
| `src/persistence.rs` | JSON state file at `<config_path>.state.json`, saved after every request and on shutdown; probes. Its tests hold the Redis coordination suites, including `src/persistence/tests/real_redis.rs` |
| `src/utilization.rs` | Time-adjusted utilization, 7d claims, waste risk, `compute_routing_weight`, `routing_candidates`. Also the retry / transport / Redis / body-limit constants, `start_coordination_redis`, credential fingerprinting, the client `anthropic-beta` allow-list (`DEFAULT_CLIENT_BETA_ALLOWLIST`) and beta body-field stripping |
| `src/routing.rs` | `pick_endpoint` and the weighted pickers, rate-limit header ingestion (`update_rate_info_for`), hard-limit marking, transport health, Redis sync / publish, `cluster_info` |
| `src/token_usage.rs` | `TokenUsage` and the streaming `SseUsageScanner`; upstream auth injection (`inject_account_auth`); `RequestContext` and the affinity key |
| `src/session_registry.rs` | Session registry (context-window visibility), usage / `proxied` logging, `record_usage`, budgets, the operator check, utilization limit, emergency brake and `pre_request_gate` |
| `src/oauth_prompt.rs`, `src/auto_cache.rs` | OAuth system-prompt injection; prompt-cache breakpoint injection (up to 3: last tool, system, last user message) |
| `src/handler.rs` | `proxy_handler` (main Anthropic proxy): retry / rotation outcomes, the upstream client, header reflection and stripping, guard responses |
| `src/fallback.rs` | Forwarding to `protocol = "openai"` endpoints (`try_fallback_upstream`, called by both handlers) |
| `src/stats.rs`, `src/metrics.rs` | `stats_handler` (`/_stats` JSON); `metrics_handler` (`/metrics` Prometheus) |
| `src/openai_compat.rs` | OpenAI→Anthropic translation for `/v1/chat/completions` (`translate_*`, `StreamContext`, streaming SSE) |
| `src/reverse_translation.rs` | Anthropic→OpenAI translation for `openai` endpoints, SSE error frames, and `openai_chat_handler` (the `/v1/chat/completions` handler) |
| `src/<module>/tests*`, `src/test_support.rs` | Unit + integration tests using mock upstream servers; shared fixtures |

### Endpoint Selection (`pick_endpoint`)

Headroom-proportional weighted bucket hashing:
1. Filter by model compatibility (if endpoint has `models` allowlist)
2. Skip endpoints in the `skip` list (already tried in this retry loop)
3. Skip hard-limited (429) endpoints
4. Each remaining endpoint gets a bucket proportional to `(1.0 - utilization)`
5. Affinity key (client+session hash) provides sticky routing; no-affinity uses Fibonacci scatter
6. On 429 or 5xx/529, the failed endpoint index is added to `skip` and `pick_endpoint` is called again, guaranteeing a different endpoint on retry

### Token Type Detection

`protocol = "anthropic"` endpoints: `passthrough` is matched exactly first; every other token dispatches on one prefix, `OAUTH_TOKEN_PREFIX`:

- `passthrough` (exact match, checked before prefix dispatch) → forwards caller's auth headers untouched
- `sk-ant-oat*` → `Authorization: Bearer` + injects `anthropic-beta: oauth-2025-04-20` and `anthropic-dangerous-direct-browser-access: true`. The OpenAI-compat handler additionally injects `claude-code-20250219` beta flag.
- any other token (API keys included) → `x-api-key` header

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

### Fable-Aware Routing (LAB-387)

Max plans include Fable only up to **50% of the weekly limit** (the "band"); past that, Fable bills as paid usage credits. Pro / standard Team plans include no Fable at all. Unlike other per-model claims the band is a **carve-out within the shared weekly pool**, not an independent sub-budget — usable Fable headroom is `min(band remaining, weekly pool remaining)`.

**Wire format (verified against a live `claude-fable-5` response, 2026-07-21):** the API does NOT emit a `seven_day_fable` representative claim. The band arrives as the `anthropic-ratelimit-unified-7d_oi-{utilization,reset,status}` triplet ("oi" = overage-included), present **only on Fable responses** — a sonnet response from the same account omits it. The parser normalises the triplet into an internal `seven_day_fable` claims_7d entry (`FABLE_BAND_CLAIM`) so the standard claims machinery applies; a one-shot `info!` fires per account on first sighting. Note: accounts with credits disabled report `overage-status: rejected` + `overage-disabled-reason: member_zero_credit_limit` — past-band Fable on such accounts **fails outright** rather than billing, so routing away before band exhaustion is availability, not just cost.

Routing consequences (`constraining_7d_claims`):
- A Fable request gates on **both** the band claim and the general `seven_day` claim: worse status wins, and waste-risk is capped by the pool's — a roomy band never masks a drained pool.
- The model-agnostic worst case (emergency brake, stats) reads claims through the `claim_gates_all_traffic` **allowlist** (`seven_day` + sonnet/opus/haiku sub-budgets): carve-outs like the band constrain only their own requests and must not brake all traffic.
- Fable is **never probed** (`PROBE_MODELS`): probes burn real quota at Fable's accelerated weekly-pool burn rate, and past the band would spend paid credits. Band claims refresh from organic Fable traffic; until one is seen, Fable routes on the general `seven_day` claim.
- `endpoints[].fable_included = false` (Pro accounts) demotes the endpoint by `overage_penalty` for Fable requests only — the tier system then drains included (Max) Fable capacity before touching always-paid accounts.
- Past-band Fable served via credits is signalled by the existing overage headers and handled by the overage machinery above (demoted tier, overage window governs). When every account is past its band, tier degradation routes Fable to whichever account has the most weekly headroom.
- **Observability:** `/_stats` exposes each endpoint's `claims_7d` map — a `seven_day_fable` key there means the account has served Fable and the band is being tracked; its absence after Fable traffic means the feature is inert (investigate).

### Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | required | Bind address (e.g. `"0.0.0.0:8080"`) |
| `clients[].name` | string | required | Identity the credential resolves to — becomes `client_id`, overriding `x-client-id` and `client_names` entirely |
| `clients[].key` | string | required | Per-client secret (`x-api-key`; also `Authorization: Bearer` on `/v1/chat/completions`). Constant-time compared |
| `clients[].models` | string[]? | [] (all) | Models this client may request; same exact + `*`-suffix matcher as `endpoints[].models`. Violation = 403 |
| `clients[].preferred_endpoints` | string[]? | [] (no pin) | Endpoint names this client is pinned to (LAB-2636/#151). While ≥1 is healthy (serves the model, not hard-limited/transport-unhealthy, `gate < soft_limit`, not overage/Fable-paid demoted), routing restricts to this set; otherwise the request spills to the full pool. Entries must name configured endpoints (startup-validated). Per-request serving logs (`proxied`, `proxied (openai-compat)`, `fallback: routing to unified OpenAI endpoint`) carry `pin=pinned\|spilled\|-` |
| `proxy_key` | string? | none | **Legacy** single shared secret for `x-api-key` auth. Mutually exclusive with `[[clients]]` — configuring both is rejected at startup |
| `allow_unauthenticated` | bool? | false | LAB-1192 default-deny escape hatch: startup FAILS with no credentials unless this is explicitly true. Incompatible with configured credentials. Trusted-network-only; warns at boot, and unauthenticated `/_stats`/`/metrics` access warns at most once per route per 5 min (`OPEN_ADMIN_WARN_INTERVAL`) |
| `allowed_ips` | string[]? | none (allow all) | IP/CIDR allowlist |
| `trusted_proxies` | string[]? | none | LBs whose `x-forwarded-for` is honoured (LAB-1192). Peer in list ⇒ client IP = rightmost XFF entry not in list; otherwise peer address, header ignored. One resolution function (`resolve_client_ip`), called once per handler |
| `auth_failure_limit` | u32? | 10 | Failed-auth attempts per client IP in the window before further invalid credentials get 429 + `retry-after`; valid credentials always pass. 0 disables. Counted in `anthropic_auth_failures_total{route,cred}` and logged with `peer` (the actual TCP socket address), `cred` (presented-credential header shape), a one-way `key_fp` (none for `auth-other`) and a clipped `ua` (LAB-4720); state bounded at 4096 IPs; eviction purges expired windows first, then the least-established live entry (lowest count, oldest window as tie-breaker) so fresh-failure floods can't flush an active lockout |
| `auth_failure_window_secs` | u64? | 300 | Failed-auth throttle window |
| `auto_cache` | bool? | true | Auto-inject prompt cache breakpoints |
| `shadow_log` | string? | none | Path for JSONL audit trail |
| `soft_limit` | f64? | 0.90 | Utilization ceiling — endpoints above this are deprioritized within their tier; they are considered only when no healthy candidate is available and the tier degrades to its soft-limited members |
| `client_names` | map? | {} | IP→client name mapping |
| `client_budgets` | map? | {} | client_id→daily token limit |
| `client_utilization_limits` | map? | {} | client_id→utilization ceiling (0.0–1.0) |
| `operators` | string[]? | [] | Client IDs that bypass the model allow-list, budget, utilization, and emergency brake enforcement (does not bypass IP allowlist). Under `[[clients]]` these name authenticated principals AND are the only principals allowed to read `/_stats` + `/metrics` (LAB-1192: 401 unauthenticated / 403 non-operator); without `[[clients]]`, operator status is trust-based and forgeable |
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
| `endpoints[].fable_included` | bool? | true | Plan includes Fable's 50%-of-weekly band. Set false for Pro / standard Team accounts: Fable requests demote the endpoint by `overage_penalty`; non-Fable routing unaffected |
| `endpoints[].allow_nonstandard_host` | bool? | false | Allow an `anthropic` endpoint whose `base_url` host isn't `api.anthropic.com` (otherwise startup fails — token exfil guard, LAB-1191) |
| `expose_upstream_ratelimit_headers` | bool? | false | Reflect upstream `anthropic-ratelimit-*` headers to callers (reveals pooled account capacity — trusted networks only, LAB-1191) |
| `allowed_client_betas` | string[]? | built-in list | Client `anthropic-beta` flags forwarded on OAuth endpoints (`*` suffix wildcard); a configured list REPLACES the default (copy defaults alongside additions); unlisted flags dropped + logged + counted in `anthropic_beta_flag_dropped_total` (LAB-1191). When anything is dropped, top-level body fields belonging to a dropped flag are stripped with it (only on `/v1/messages` + `/v1/messages/count_tokens`), so the feature turns off quietly instead of 400ing upstream — counted in `anthropic_beta_body_field_stripped_total` (LAB-1261). Fields owned by a flag that SURVIVED are protected, which requires `BETA_BODY_FIELDS` to stay total over this list (build-time test); a surviving flag with no row switches the strip off for that request rather than risk deleting its field |


**Key headers parsed:**

| Header | Meaning |
|--------|---------|
| `anthropic-ratelimit-unified-representative-claim` | Which window is the binding constraint (e.g., `five_hour`, `seven_day_sonnet`) |
| `anthropic-ratelimit-unified-5h-utilization` | Raw 5h usage fraction (0.0–1.0) |
| `anthropic-ratelimit-unified-5h-reset` | Epoch timestamp when 5h window resets to zero |
| `anthropic-ratelimit-unified-7d-utilization` | Raw 7d usage fraction (0.0–1.0) |
| `anthropic-ratelimit-unified-7d-reset` | Epoch timestamp when 7d window resets |
| `anthropic-ratelimit-unified-5h-status` / `7d-status` | API pressure signal: `allowed`, `allowed_warning`, `throttled`, `rejected` |
| `anthropic-ratelimit-unified-7d_oi-utilization` / `-reset` / `-status` | Fable included-band triplet ("oi" = overage-included) — fraction of the Max-plan Fable band (50% of weekly) consumed. Emitted only on Fable responses; normalised into the internal `seven_day_fable` claim |
| `anthropic-ratelimit-unified-overage-in-use` | Endpoint is currently serving via paid overage (always overwritten; absent → `false`) |
| `anthropic-ratelimit-unified-overage-status` | Overage window status — feeds the routing gate floor while overage is in use |
| `anthropic-ratelimit-unified-overage-utilization` | Overage budget consumed (0.0–1.0) |
| `anthropic-ratelimit-unified-overage-reset` | Epoch timestamp when the overage window resets |

**Logged values:** `util_5h` and `util_7d` in request/probe logs show **raw API values** (`info.utilization_5h`, `info.utilization_7d`). The `utilization` field shows the effective (time-adjusted) value used for routing decisions.

**Peak hour adjustments:** Anthropic dynamically reduces 5h token allowances during peak hours (05:00–11:00 PT weekdays). ~7% of users affected. Weekly caps unchanged.

## Testing Patterns

Tests use a `spawn_mock_upstream()` helper that starts a real TCP listener returning canned Anthropic-style responses with rate-limit headers. Integration tests bind to `127.0.0.1:0` (random port) and make real HTTP requests through the full axum router with `ConnectInfo<SocketAddr>`.

`test_state_with()` and `test_app()` are the two test fixture builders — the former for unit tests (no HTTP), the latter for integration tests (full router + mock upstream). `test_openai_app()` builds a minimal router for OpenAI-compat handler tests.

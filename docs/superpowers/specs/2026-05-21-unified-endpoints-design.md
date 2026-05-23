# Unified Endpoints — Design

**Date:** 2026-05-21
**Status:** Shipped — preserved as a historical artifact
**Author:** Ray Walker

> **Note:** This document records the design as it was *specified*, including
> mid-execution corrections (look for `REVISED` / `Correction` markers). The
> implementation diverged from this spec in places — for example, the runtime
> introduced `UsageTarget` and `ForwardOutcome` enums and the `classify_retry_status`
> helper that are not described here. The **source code is the authoritative
> reference**; this spec is kept in-tree to record the design process and the
> decisions made along the way, not as a live API contract.

## Problem

The proxy has two routing-candidate types — `[[accounts]]` (Anthropic-native) and
`[[upstreams]]` (OpenAI-compatible) — plus a `fallback_upstream` config key that
names one upstream as a special routing participant.

Three things are wrong with this:

1. **`fallback_upstream` is a misnomer.** It was added before priority tiers
   existed, as a pure last-resort escape hatch. Priority tiers landed afterward
   and now decide routing order entirely. "Fallback" semantics ("only when
   everything else fails") are just `priority = 100` today — the name lies about
   what the field does.
2. **`upstream` (the runtime/config concept) is a misnomer.** The whole binary
   is an upstream proxy. Calling one specific endpoint kind "upstream" overloads
   the word. What it actually denotes is "an endpoint that speaks a different
   wire format and needs a translation layer."
3. **Two config sections do essentially the same job.** Accounts and upstreams
   are both routing candidates partitioned into the same priority space. They
   differ only in wire format and auth injection. Two structs, two runtime
   types, and a special-cased `Option<usize>` index encode a distinction that
   should be a single field.

A concrete failure this caused: in the lab deployment, the `insight-gateway`
upstream had no `priority` set (default `0`), placing it in the same tier as the
main accounts. It won traffic immediately instead of acting as a last resort.
The fix was `priority = 100`, but the design invited the mistake.

## Goal

Collapse accounts and upstreams into a single `[[endpoints]]` concept. The wire
format becomes one field (`protocol`). Routing order is decided purely by
`priority`. The `fallback_upstream` and global `upstream` config keys are
deleted. Endpoint-type special-casing in the routing path is reduced to three
named, commented call sites (see Runtime model) instead of a separate struct,
runtime type, and `Option<usize>` index.

## Non-goals

- Supporting wire formats beyond Anthropic-native and OpenAI chat-completions.
  The design leaves room for more (`protocol` is an enum) but only two variants
  ship now.
- A config deprecation period. This is a hard break (see Migration).
- Keeping the `/upstream/{name}/*` passthrough route. It is unused and removed.

> **Correction (2026-05-22):** An earlier revision of this spec listed
> "routing OpenAI-format requests to Anthropic accounts" as a non-goal, on the
> belief that `openai_chat_handler` reached only upstreams. That belief was
> wrong — `openai_chat_handler` has always translated OpenAI→Anthropic, routed
> to Anthropic accounts, and translated the response back. That behavior is
> existing, tested, and **preserved**: in the unified model `openai_chat_handler`
> dispatches to both `Protocol::OpenAI` endpoints (direct) and
> `Protocol::Anthropic` endpoints (translated), symmetric with `proxy_handler`.

## Design

### Config schema

`[[accounts]]` and `[[upstreams]]` collapse into `[[endpoints]]`. The
`fallback_upstream` and `upstream` top-level keys are deleted.

**`base_url` safety.** For `protocol = "anthropic"`, the default base URL is
`https://api.anthropic.com`. An explicit override is allowed (staging /
self-hosted Anthropic-compatible gateways), but startup validation enforces:
(1) the URL must use the `https://` scheme; (2) if the host is not
`api.anthropic.com`, a `warn!` is logged at startup naming the endpoint and
host. A typo'd `base_url` would otherwise send real OAuth/API tokens to an
attacker-controlled host — `inject_account_auth` is host-agnostic and attaches
credentials based purely on token prefix.

**Priority validation.** At startup, if any `protocol = "openai"` endpoint
shares the lowest priority tier with any `protocol = "anthropic"` endpoint, a
`warn!` is logged naming both. This preserves the safety check the deleted
`fallback_upstream` priority comparison provided, and prevents the exact
lab `insight-gateway` incident in §Problem from recurring with the new schema.

```toml
listen = "0.0.0.0:8082"
soft_limit = 0.90
# no `upstream` key — base URL is encoded by each endpoint's protocol

[[endpoints]]
name = "primary"
token = "sk-ant-..."
# protocol = "anthropic" is the default

[[endpoints]]
name = "passbolt"
token = "sk-ant-oat01-..."
priority = 1

[[endpoints]]
name = "insight-gateway"
protocol = "openai"
base_url = "https://gateway.example.com"
token = "sk-..."
priority = 100
models = ["claude-opus-*"]
```

`EndpointConfig`:

| Field      | Type        | Default              | Notes |
|------------|-------------|----------------------|-------|
| `name`     | string      | required             | Display name; used for metrics labels and Redis keys |
| `protocol` | enum string | `"anthropic"`        | `"anthropic"` or `"openai"` |
| `base_url` | string?     | protocol-dependent   | Required for `openai`. For `anthropic`, defaults to `https://api.anthropic.com`; overridable for staging/self-hosted |
| `token`    | string      | required             | Auth credential. `"passthrough"` (anthropic only) forwards the caller's auth headers unchanged |
| `models`   | string[]    | `[]` (all)           | Model allowlist; supports `*` suffix wildcards. Works for both protocols |
| `priority` | u32         | `0`                  | Priority tier; lower tried first |

`protocol` encodes three things at once: wire format, auth injection method
(`x-api-key` for anthropic, `Bearer` for openai), and the default base URL.

### Runtime model

The `Account` and `Upstream` runtime structs merge into one `Endpoint`. The
routing enum `Endpoint { Account(usize), Upstream(usize) }` collapses to a plain
`usize` index into `state.endpoints`.

```rust
enum Protocol { Anthropic, OpenAI }

struct Endpoint {
    name: String,
    protocol: Protocol,
    base_url: String,            // resolved at startup
    token: String,
    passthrough: bool,           // only meaningful for Protocol::Anthropic
    models: Vec<String>,
    priority: u32,
    requests: AtomicU64,
    rate_info: RwLock<RateLimitInfo>,   // stub for OpenAI endpoints
    // ... token counters, burn rate, routing-weight gauges (unchanged)
}
```

OpenAI endpoints carry a stub `RateLimitInfo` (all fields `None`). This is
**not** indistinguishable from a healthy account: `effective_utilization()`
maps all-`None` fields to `(0.5, "unknown")`, which would break the emergency
brake and routing weight calculation. Three code paths therefore branch on
`endpoint.protocol`:

1. `routing_candidates()` — for `Protocol::OpenAI`, skip `compute_routing_weight`
   and push a fixed `RoutingCandidate { gate: 0.0, weight: 1.0, source: "openai" }`
   at the endpoint's configured priority. This is explicit, not implicit.
2. `is_emergency_brake_active()` — iterate only `Protocol::Anthropic` endpoints
   when checking the all-above-threshold condition. An OpenAI endpoint's
   `(0.5, "unknown")` must not vote against firing the brake.
3. Probe loop — skip `Protocol::OpenAI` endpoints. The existing
   `acct.passthrough` skip is a different predicate (OpenAI endpoints have
   `passthrough: false`); a separate explicit check is required.

The earlier framing of "zero special-casing" was wrong. The right framing:
special-casing is concentrated in three named call sites, each with a comment
explaining why and what it gates.

`AppState` changes:

- `accounts: Vec<Account>` → `endpoints: Vec<Endpoint>`
- `upstreams: Vec<Upstream>` → deleted
- `fallback_upstream: Option<usize>` → deleted
- `upstream: String` → deleted

The `skip` list threaded through the retry loop in both handlers becomes
`Vec<usize>` instead of `Vec<Endpoint>`.

### Routing & dispatch

`pick_endpoint` is structurally unchanged — same priority-tier partitioning,
same intra-tier soft-limit degradation. The only difference: candidates come
from a single `state.endpoints` pool, and `routing_candidates()` no longer has
the `if let Some(u_idx) = self.fallback_upstream` block.

**Both handlers can target any endpoint.** Each of the two inbound wire
formats can be served by each of the two endpoint protocols — four dispatch
cells, all of which exist in the code today. The refactor changes how the
endpoint is *selected* (one unified pool), not what happens after.

After an endpoint is picked, the handler dispatches on `endpoint.protocol`:

| Handler               | Endpoint protocol | Action |
|-----------------------|-------------------|--------|
| `proxy_handler`       | Anthropic         | forward to `base_url`, parse rate-limit headers, track utilization, `record_usage` |
| `proxy_handler`       | OpenAI            | translate Anthropic→OpenAI request, forward, translate response back |
| `openai_chat_handler` | Anthropic         | translate OpenAI→Anthropic request, forward, parse rate-limit headers, `record_usage`, translate response back |
| `openai_chat_handler` | OpenAI            | forward direct, no translation |

All four cells exist in the code today (`openai_chat_handler` already routes to
both Anthropic accounts and OpenAI upstreams). The refactor preserves every
cell; it only swaps the two legacy pools (`accounts`, `upstreams`) for the one
unified `endpoints` pool feeding `pick_endpoint`.

A new `try_fallback_upstream_unified()` serves OpenAI-protocol endpoints from
the unified pool, alongside the legacy `try_fallback_upstream()` (deleted in
Phase 4). **Critical contract difference:** `try_fallback_upstream()` returns
the upstream's error response directly (`Some(resp) => return resp`), so an
OpenAI endpoint's 429/5xx is sent straight to the caller instead of being
retried. `try_fallback_upstream_unified()` returns `None` on upstream 429/5xx
so the shared retry loop adds the endpoint to `skip` and tries the next one —
matching Anthropic-endpoint failure semantics. Non-429 4xx errors are still
returned to the caller (retry would not help). The `skip` push and retry are
owned by the shared loop, not the helper.

The `/upstream/{name}/*` route and `upstream_handler` are deleted.

### Persistence & state

`PersistedState` / `PersistedAccount` rename to `PersistedState` /
`PersistedEndpoint`. The top-level state-file key changes from `accounts` to
`endpoints`, so an old file does not deserialize into the new struct. This is a
**hard break**. On load failure or a missing `endpoints` key, the load path
logs a `warn!` ("state file format changed, starting clean") and starts with
empty runtime state — the failure must be visible in logs, not silent.

Routing is **not** instantly correct after a clean start: until the first probe
cycle completes, endpoints have no rate data and `effective_utilization()`
returns `(0.5, "unknown")`, so routing runs on uninformed default weights for
up to one `probe_interval_secs` (default 300s). This degraded-but-functional
window already exists today on any fresh start; the refactor does not change it.
It is acceptable — the proxy still serves traffic, just without utilization-
optimized routing for that window.

Redis keys (`alb:budget:*`, `alb:rate:*`, `alb:hard:*`, `alb:heartbeat:*`) are
keyed by `client_id` and endpoint `name`. Neither identifier changes, so no
Redis migration is needed.

### Metrics

Prometheus metric names are **left unchanged**: `anthropic_account_*` (request
count, routing weight, routing share, effective gate, utilization, passthrough
flag) keep their current names. The metric *values* are unaffected by this
refactor — only the internal config/runtime nouns change. Renaming the series
would create a Grafana-dashboard migration and a deprecation window for zero
functional gain, so it is explicitly out of scope. The `account` noun in the
metric name is a known, accepted minor staleness.

### Config migration

No backward-compat shim. A config using `[[accounts]]`, `[[upstreams]]`, or
`fallback_upstream` is rejected at startup with an explicit error naming the
removed keys and their replacements.

**Rejection mechanism.** `serde` silently ignores unknown TOML keys by default,
so a removed key would simply be dropped, not rejected. `#[serde(deny_unknown_fields)]`
is *not* used — it would break forward-compatibility of every other optional
config field. Instead, a targeted post-deserialize check runs the raw parsed
TOML `Value` for the three dead keys (`accounts`, `upstreams`, `fallback_upstream`)
and returns a hard error naming each one found and its replacement. This is new
code, ~15 lines, not a free side effect of the struct rename.

### Rollback

The roll-out is a hard break across three artifacts that change together: the
binary, the state-file format, and both cluster configs. A botched roll-out
(malformed `[[endpoints]]` config) causes a startup parse failure — the pod
crash-loops rather than serving wrong traffic, which is the safe failure mode.

Rollback procedure, prepared *before* cutover:

1. Record the current (pre-change) image digest from the running deployment.
2. Keep the pre-change config revision available in git (it already is — it is
   the prior commit of each `externalsecret.yaml`).
3. To roll back: pin the deployment image to the recorded prior digest and
   revert both `externalsecret.yaml` files to their prior revision. The old
   binary reads the old config; the state file is regenerated by probes.
4. The old and new binaries are **not** config-compatible in either direction —
   roll back binary and config together, never one without the other.

Both production deployments are updated in lockstep with the binary roll-out:

- **mem cluster** — `27b-io/fleet-infra`, `apps/mem/anthropic-lb/externalsecret.yaml`
- **lab cluster** — `27b-io/lab`, `k8s/mcp/anthropic-lb-externalsecret.yaml`

The repo's own `config.toml` and `CLAUDE.md` config-schema docs are updated in
the same change.

## Testing

**Blast radius.** `main.rs` is ~19500 lines with ~491 references to the
collapsed concepts (`accounts`, `fallback_upstream`, `Endpoint::`) and ~46
`fallback_upstream: None` test fixtures. This is not a typo-class find-replace:
the `Endpoint { Account(usize), Upstream(usize) }` enum is load-bearing — it
gives the compiler account-vs-upstream discrimination in the `skip` list and
`RoutingCandidate`. Collapsing it to a bare `usize` index into one unified
`Vec` removes that type-level safety; `pick_endpoint`, `routing_candidates`,
and both handler retry loops are a coupled set that must change in one commit.

Mechanical rewrites across existing fixtures:

- test state builders: `accounts` field → `endpoints` field
- `fallback_upstream: None` → removed
- `Endpoint::Account(i)` → bare `i`; `Endpoint::Upstream(u)` → bare `u`
- `test_state_with()` / `test_app()` gain a way to register `Protocol::OpenAI`
  endpoints alongside Anthropic ones

New tests:

- OpenAI-protocol endpoint participates in priority-tier routing
- Model allowlist on an OpenAI endpoint (e.g. opus-only) filters correctly
- `proxy_handler` → OpenAI endpoint performs Anthropic→OpenAI translation
- `openai_chat_handler` → unified `Protocol::Anthropic` endpoint performs the
  OpenAI→Anthropic request + response round-trip translation
- `try_fallback_upstream_unified()` returns `None` on an upstream 429/5xx so the
  retry loop advances to the next endpoint
- Emergency brake still fires when all Anthropic endpoints are above threshold
  even with an OpenAI endpoint present in the pool
- Config parse is rejected for `[[accounts]]`, `[[upstreams]]`, and
  `fallback_upstream`, each with a helpful error message
- Loading an old `accounts`-keyed state file starts clean and logs a `warn!`

Quality gates unchanged: `cargo test`, `cargo fmt --check`,
`RUSTFLAGS="-Dwarnings" cargo clippy --all-targets`.

## Risks

| Risk | Mitigation |
|------|------------|
| State file fails to load post-upgrade | Expected; load path logs `warn!` and starts clean. Routing runs on default weights until the first probe cycle (~300s) — degraded but functional, same as any fresh start |
| Botched roll-out: malformed config crash-loops the pod | Safe failure mode (no wrong traffic). Rollback procedure (see Rollback) prepared before cutover: pin prior image digest + revert both `externalsecret.yaml` revisions together |
| Both cluster configs must change in lockstep with the binary | Single coordinated change; configs and binary roll out together. Binary and config are not compatible across the break in either direction |
| Enum collapse (`Endpoint` → bare `usize`) loses compiler discrimination | The coupled set (`pick_endpoint`, `routing_candidates`, both handlers) changes in one commit; new tests cover endpoint-selection behavior, not just compilation |
| Large mechanical test refactor introduces typos | `cargo test` + clippy gate catches them |

## Open questions

None. All resolved during brainstorming.

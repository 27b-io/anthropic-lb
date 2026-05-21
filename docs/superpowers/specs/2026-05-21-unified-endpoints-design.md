# Unified Endpoints — Design

**Date:** 2026-05-21
**Status:** Approved for planning
**Author:** Ray Walker

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
deleted. No special-casing remains in the routing path.

## Non-goals

- Supporting wire formats beyond Anthropic-native and OpenAI chat-completions.
  The design leaves room for more (`protocol` is an enum) but only two variants
  ship now.
- A config deprecation period. This is a hard break (see Migration).
- Keeping the `/upstream/{name}/*` passthrough route. It is unused and removed.

## Design

### Config schema

`[[accounts]]` and `[[upstreams]]` collapse into `[[endpoints]]`. The
`fallback_upstream` and `upstream` top-level keys are deleted.

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

OpenAI endpoints carry a stub `RateLimitInfo`: all gates `0.0`, weight `1.0`,
no claims. This lets them flow through `routing_candidates()` and `pick_endpoint`
with zero special-casing — they look like a permanently healthy account. The
probe loop skips them, exactly as it already skips passthrough accounts.

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

After an endpoint is picked, the handler dispatches on `endpoint.protocol`:

| Handler input              | Endpoint protocol | Action |
|----------------------------|-------------------|--------|
| Anthropic (`proxy_handler`)| Anthropic         | forward to `base_url`, parse rate-limit headers, track utilization |
| Anthropic (`proxy_handler`)| OpenAI            | translate Anthropic→OpenAI, forward, translate response back |
| OpenAI (`openai_chat_handler`) | Anthropic     | translate OpenAI→Anthropic, forward, translate response back |
| OpenAI (`openai_chat_handler`) | OpenAI        | forward direct, no translation |

The `openai_chat_handler` → Anthropic-endpoint cell is **new behavior**. Today
an OpenAI-format request can only reach OpenAI upstreams. After this change it
can route to Anthropic accounts via reverse translation, widening the routing
surface. This is intentional and desired.

`try_fallback_upstream` is renamed `forward_translated()` (or inlined). The
`/upstream/{name}/*` route and `upstream_handler` are deleted.

### Persistence & state

`PersistedState` / `PersistedAccount` rename to use endpoint terminology. The
state-file format changes: an old file with an `accounts` key will not load.
This is a **hard break** — acceptable because the state file is fully
reconstructed from probe responses within one probe cycle (~5 min). On load
failure, start clean.

Redis keys (`alb:budget:*`, `alb:rate:*`, `alb:hard:*`, `alb:heartbeat:*`) are
keyed by `client_id` and endpoint `name`. Neither identifier changes, so no
Redis migration is needed.

### Metrics

Prometheus metrics named `anthropic_account_*` (request count, routing weight,
routing share, effective gate, utilization, passthrough flag) gain
`anthropic_endpoint_*` equivalents. Both series are emitted for one release:
the new `anthropic_endpoint_*` names carry the same values, and the old
`anthropic_account_*` names are kept as deprecated aliases so existing Grafana
dashboards and alert rules keep working.

The old aliases are marked deprecated in code comments and removed in a
follow-up change once dashboards have been migrated. This is the one place the
redesign does *not* take a hard break — metrics are an external contract with
dashboards outside this repo, and a dual-emit window costs little.

### Config migration

No backward-compat shim. A config using `[[accounts]]`, `[[upstreams]]`, or
`fallback_upstream` fails to parse at startup with an explicit error naming the
removed keys and their replacements.

Both production deployments are updated in lockstep with the binary roll-out:

- **mem cluster** — `27b-io/fleet-infra`, `apps/mem/anthropic-lb/externalsecret.yaml`
- **lab cluster** — `27b-io/lab`, `k8s/mcp/anthropic-lb-externalsecret.yaml`

The repo's own `config.toml` and `CLAUDE.md` config-schema docs are updated in
the same change.

## Testing

Mechanical rewrites across existing fixtures (~60 sites):

- test state builders: `accounts` field → `endpoints` field
- `fallback_upstream: None` → removed
- `Endpoint::Account(i)` → bare `i`; `Endpoint::Upstream(u)` → bare `u`
- `test_state_with()` / `test_app()` gain a way to register `Protocol::OpenAI`
  endpoints alongside Anthropic ones

New tests:

- OpenAI-protocol endpoint participates in priority-tier routing
- Model allowlist on an OpenAI endpoint (e.g. opus-only) filters correctly
- `proxy_handler` → OpenAI endpoint performs Anthropic→OpenAI translation
- `openai_chat_handler` → Anthropic endpoint performs OpenAI→Anthropic reverse
  translation (new capability — no current coverage)
- Config parse rejects `[[accounts]]`, `[[upstreams]]`, and `fallback_upstream`
  with helpful error messages

Quality gates unchanged: `cargo test`, `cargo fmt --check`,
`RUSTFLAGS="-Dwarnings" cargo clippy --all-targets`.

## Risks

| Risk | Mitigation |
|------|------------|
| External Grafana dashboards reference `anthropic_account_*` | Old metric names kept as deprecated aliases for one release; dashboards migrate before aliases are removed |
| State file fails to load post-upgrade | Expected; probes reconstruct within ~5 min. No action needed |
| Both cluster configs must change in lockstep with the binary | Single coordinated change; configs and binary roll out together |
| Large mechanical test refactor introduces typos | `cargo test` + clippy gate catches them |

## Open questions

None. All resolved during brainstorming.

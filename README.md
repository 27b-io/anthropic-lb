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
             + backpres │  routing by      ├──► Account B (util: 0.67)
             check      │  headroom        │
                        │                  ├──► Account C (429 — cooling)
                        └────────┬─────────┘
                                 │
                            shadow log
                          + token tracking
                          + Redis sync (opt)
```

| Feature | Description |
|:--------|:------------|
| **Weighted routing** | Headroom-proportional bucket hashing with client affinity |
| **Per-claim utilization** | Model-aware 7d claim windows (e.g. Sonnet vs Opus sub-budgets) |
| **Soft utilization ceiling** | Accounts above `soft_limit` excluded from routing, breaking sticky affinity |
| **Time-adjusted utilization** | Discounts utilization near window reset; accounts about to reset get more traffic |
| **Status-based routing** | Parses API status headers — `warning`/`throttled`/`rejected` enforce utilization floors |
| **429 rotation** | Rate-limited accounts cool down, traffic shifts instantly |
| **5xx retry** | Automatic retry on 500/502/503/504/529 (picks different account) |
| **Token tracking** | Per-account and per-client input/output/cache token counters, plus a per-(client, model) breakdown (`anthropic_client_model_token_usage_total{client,model,type}`) for model-mix and API-price attribution |
| **Client budgets** | Daily per-client token budgets with automatic reset |
| **Utilization limits** | Per-client utilization ceiling — 429 when all Anthropic endpoints exceed limit |
| **Operator bypass** | Designated client bypasses all budget, utilization, and emergency checks |
| **Emergency brake** | Auto-block all non-operator traffic when all Anthropic endpoints exceed threshold |
| **Distributed state** | Optional Redis/Valkey backend for cross-replica budget + hard-limit sync |
| **Auto-cache** | Injects prompt caching beta header automatically |
| **Shadow logging** | Optional JSONL file with request metadata, tokens, latency |
| **Model routing** | Per-account model allowlists with wildcard prefix matching |
| **Client identification** | Via `X-Client-ID` header or IP-based mapping |
| **Streaming** | SSE/streaming responses flow through with usage extraction (streams from `openai` endpoints are forwarded but not debited — see [What doesn't translate](#what-doesnt-translate)) |
| **State persistence** | Utilization + reset times + status survive restarts |
| **OpenAI-compatible endpoints** | Route to OpenAI-format APIs as first-class endpoints (`protocol = "openai"`) |
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
rate_limit_cooldown_secs = 60
probe_interval_secs = 300

# Authentication — per-client keys. The key IS the identity: the matched
# entry's name becomes client_id, and x-client-id is ignored.
# [[clients]]
# name = "alice"
# key = "<openssl rand -hex 32>"
# models = ["claude-sonnet-*", "claude-haiku-*"]  # optional; empty = all

# LEGACY single shared secret. Mutually exclusive with [[clients]] —
# configuring both fails startup.
# proxy_key = "<openssl rand -hex 32>"

# Startup FAILS unless [[clients]], proxy_key, or this flag is set.
# Trusted-network-only (NetworkPolicy / tailnet) — never on a public ingress.
# allow_unauthenticated = true

# IP allowlist (optional — omit to allow all source IPs)
# allowed_ips = ["100.64.0.0/10", "10.0.0.0/8"]

# Load balancers whose x-forwarded-for is trusted (optional). Client IP =
# rightmost XFF entry not in this list when the TCP peer is listed;
# otherwise the peer address, and the header is ignored.
# trusted_proxies = ["192.0.2.0/24"]

# Failed-auth throttle: after this many failures per client IP inside the
# window, further invalid credentials get 429 + retry-after. Valid credentials
# always pass so one caller cannot lock out authenticated NAT/LB neighbours.
# 0 disables.
# auth_failure_limit = 10
# auth_failure_window_secs = 300

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

# Per-client utilization limits (optional, 0.0–1.0)
# Client gets 429 when ALL model-compatible Anthropic endpoints exceed their limit.
# [client_utilization_limits]
# "alice" = 0.85
# "bob" = 0.95

# Operators — bypass all budget, utilization, and emergency checks
# operators = ["ops", "claude"]

# Emergency brake — auto-block non-operator traffic when all Anthropic
# endpoints exceed this threshold (OpenAI endpoints carry no rate-limit
# data and are excluded). Default: 0.88
# emergency_threshold = 0.88

# Redis/Valkey for distributed state across replicas (optional)
# Supports redis:// (plaintext) and rediss:// (TLS)
# redis_url = "redis://redis.example.com:6379"

# IP-to-client-name mapping (optional, fallback when no X-Client-ID header)
# [client_names]
# "192.0.2.10" = "alice-desktop"
# "192.0.2.11" = "bob-laptop"

[[endpoints]]
name = "primary"
token = "sk-ant-oat01-..."
# protocol = "anthropic"   # default; "openai" for OpenAI-compatible upstreams
# models = ["claude-sonnet-4-6", "claude-opus-*"]

[[endpoints]]
name = "secondary"
token = "sk-ant-api03-..."
```

### Config Reference

| Field | Type | Default | Description |
|:------|:-----|:--------|:------------|
| `listen` | `String` | — | Bind address (e.g. `"127.0.0.1:8082"`) |
| `rate_limit_cooldown_secs` | `u64` | `5` | Fallback cooldown after a capacity 429 when upstream sends no usable `retry-after` (burst 429s use their own 5→60s backoff) |
| `probe_interval_secs` | `u64` | `300` | Seconds between utilization probes (0 = disabled) |
| `clients[].name` | `String` | — | Identity this credential resolves to — becomes `client_id` |
| `clients[].key` | `String` | — | Per-client secret (`x-api-key`; also `Bearer` on `/v1/chat/completions`) |
| `clients[].models` | `[String]` | `[]` | Models this client may request (empty = all; `*` suffix wildcards) |
| `clients[].preferred_endpoints` | `[String]` | `[]` | Pin this client to named endpoint(s); spills to the full pool when none is healthy (incl. at paid overage). Names are startup-validated |
| `proxy_key` | `String?` | `None` | **Legacy** shared secret. Mutually exclusive with `[[clients]]` |
| `allow_unauthenticated` | `bool` | `false` | The one escape hatch from default-deny: boot with no credentials at all. Trusted-network-only; incompatible with configured credentials |
| `allowed_ips` | `[String]?` | `None` | IP/CIDR allowlist (unset = **allow all**) |
| `trusted_proxies` | `[String]?` | `None` | IPs/CIDRs of load balancers whose `x-forwarded-for` is honoured (unset = header ignored) |
| `forward_caller_identity` | `bool` | `false` | Relay caller-identity headers (`x-forwarded-for`, `x-real-ip`, `forwarded`, `true-client-ip`, `x-client-id`, `x-agent-id`, `x-session-id`) to the upstream; off = stripped once the proxy has used them (see *Real client IP behind a load balancer*) |
| `auth_failure_limit` | `u32` | `10` | Failed-auth attempts per client IP inside the window before further invalid credentials get 429; valid credentials always pass (0 = throttle off) |
| `auth_failure_window_secs` | `u64` | `300` | Failed-auth throttle window |
| `auto_cache` | `bool` | `true` | Inject prompt caching beta header |
| `shadow_log` | `String?` | `None` | Path to JSONL shadow log file |
| `debug_log` | `String?` | `None` | Path to an additional `debug`-level `tracing` log file (see [Logging](#logging)); the stderr stream stays at `info` regardless |
| `soft_limit` | `f64` | `0.90` | Utilization ceiling — accounts above are excluded from routing |
| `client_names` | `{IP: name}` | `{}` | IP → client ID mapping |
| `client_budgets` | `{name: tokens}` | `{}` | Daily token budget per client |
| `client_utilization_limits` | `{name: f64}` | `{}` | Per-client utilization ceiling (0.0–1.0) |
| `operators` | `Vec<String>` | `[]` | Client IDs that bypass all enforcement |
| `admin_readers` | `Vec<String>` | `[]` | Client IDs granted `/_stats` + `/metrics` and nothing else — every `/v1` surface answers 403. The role for a scrape or a dashboard. Requires `[[clients]]`; a name in both `admin_readers` and `operators` is a startup error |
| `strategy` | `String` | `dynamic-capacity-v1` | Routing strategy (see note below) |
| `emergency_threshold` | `f64` | `0.88` | Utilization threshold for emergency brake |
| `redis_url` | `String?` | `None` | Redis/Valkey URL for distributed state |
| `expose_upstream_ratelimit_headers` | `bool` | `false` | Reflect upstream `anthropic-ratelimit-*` headers to callers — they reveal the pooled capacity of every account, so enable on trusted networks only |
| `allowed_client_betas` | `[String]?` | built-in list | Client `anthropic-beta` flags forwarded upstream on OAuth endpoints (`*` suffix wildcard); a configured list **replaces** the built-in default — copy the defaults alongside additions. Unlisted flags are dropped, logged, and counted (`anthropic_beta_flag_dropped_total`) so a caller can't activate arbitrary beta features against the operator's accounts. A drop also strips any top-level body field belonging to a dropped flag (`anthropic_beta_body_field_stripped_total`), so a paired beta degrades off instead of failing the request |
| `endpoints[].name` | `String` | — | Display name for the endpoint |
| `endpoints[].protocol` | `String` | `"anthropic"` | `"anthropic"` (default) or `"openai"` |
| `endpoints[].base_url` | `String?` | `https://api.anthropic.com` | Base URL; required and must be `https://` for `openai` |
| `endpoints[].token` | `String` | — | API key, OAuth token, or `"passthrough"` |
| `endpoints[].models` | `[String]` | `[]` | Model allowlist (empty = all) |
| `endpoints[].priority` | `u32` | `0` | Priority tier (lower tried first) |
| `endpoints[].fable_included` | `bool` | `true` | Plan includes Fable band; set `false` for Pro / standard Team |
| `endpoints[].allow_nonstandard_host` | `bool` | `false` | Allow an `anthropic` endpoint whose `base_url` host isn't `api.anthropic.com` — without it startup fails, because the endpoint token would be sent to that host |
| `session_registry_max` | `usize` | `1000` | Max live-session entries tracked for `/_stats` `sessions` (0 = disabled) |
| `session_registry_ttl_secs` | `u64` | `1800` | Seconds after a session's last request before its entry is evicted |

> [!NOTE]
> **Strategy normalization**: Both `"dynamic-capacity"` and `"dynamic-capacity-v1"` are accepted in config, but the runtime normalizes to `"dynamic-capacity-v1"` in logs and `/_stats` output. Similarly, `"sticky-weighted"` normalizes to `"sticky-weighted-v2"`.

### Token Types

| Prefix | Auth method | Notes |
|:-------|:------------|:------|
| `passthrough` | Caller's headers | Exact match, checked before prefix dispatch; forwards client auth as-is |
| `sk-ant-oat*` | `Authorization: Bearer` | OAuth token; beta headers injected automatically |
| Any other token | `x-api-key` | Standard API keys, and anything not matching the OAuth prefix |

> [!TIP]
> Use `passthrough` when clients have their own Anthropic credentials and you only want load-balancing without token injection.

### Model Routing

Restrict endpoints to specific models with the `models` field:

```toml
[[endpoints]]
name = "opus-only"
token = "sk-ant-oat01-..."
models = ["claude-opus-*"]  # Wildcard prefix match

[[endpoints]]
name = "sonnet-only"
token = "sk-ant-api03-..."
models = ["claude-sonnet-4-6"]  # Exact match

[[endpoints]]
name = "general"
token = "sk-ant-oat01-..."
# Empty models = serves all models
```

When a request specifies a model, only accounts whose `models` list matches (exact or prefix wildcard) are considered. Accounts with an empty `models` list serve all models.

---

## Client Setup

### Local / trusted network (no credentials)

Set `allow_unauthenticated = true` — startup **fails** without a credential
otherwise, and the flag logs a warning at boot naming the risk. This mode is
for networks where access control already exists outside the proxy (a
Kubernetes NetworkPolicy, a tailnet). Point Claude Code at the proxy:

```bash
export ANTHROPIC_BASE_URL=http://localhost:8082
```

Clients can use their own OAuth login (`claude login`) or set a dummy `ANTHROPIC_API_KEY` — either way the proxy strips client auth and injects the real account token.

### Authenticated clients (`[[clients]]`)

Give each caller its own key. The key **is** the identity:

```toml
[[clients]]
name = "alice"
key = "<openssl rand -hex 32>"
models = ["claude-sonnet-*", "claude-haiku-*"]   # optional; empty = all models

[[clients]]
name = "bob"
key = "<openssl rand -hex 32>"
```

```bash
export ANTHROPIC_BASE_URL=https://your-proxy.example.com
export ANTHROPIC_API_KEY=<that client's key>
```

Claude Code sends it as `x-api-key`; the proxy validates it and swaps in the real account token, so no Anthropic credentials or OAuth login are needed on the client. `/v1/chat/completions` additionally accepts `Authorization: Bearer <key>`, since OpenAI SDKs send nothing else.

> [!IMPORTANT]
> With `[[clients]]` configured, **`x-client-id` and the `client_names` IP map are ignored entirely** — `client_id` comes from the verified credential. That is what makes per-client budgets, utilization ceilings, operator status, model allow-lists and response-cache tenancy enforceable rather than advisory: all five key on `client_id`.

Keys are compared in constant time against the whole table. Startup rejects anything that would otherwise fail silently at runtime: duplicate names, duplicate keys, a name with stray whitespace, and any `client_budgets` / `client_utilization_limits` / `operators` / `admin_readers` / `[response_cache].clients` entry naming no configured client. That last class matters — an unknown client passes the budget and utilization checks, so a one-character typo would mean *unlimited* spend with no log line and no metric.

`[[clients]]` is also incompatible with `token = "passthrough"` endpoints, and that combination is rejected at startup. Passthrough forwards the caller's auth headers upstream untouched, but under `[[clients]]` those headers carry the caller's *proxy* credential — forwarding them would hand every client key to the upstream.

### Legacy shared secret (`proxy_key`)

```toml
proxy_key = "your-secret-key-here"
```

One secret for everyone, sent as `x-api-key`. It authenticates but does **not** identify — every caller presents the same string, so `client_id` still comes from the client-asserted `X-Client-ID` header and a per-client model allow-list would be defeated by editing one header.

**Migration is a flag day, not a rolling cutover.** Setting both `proxy_key` and `[[clients]]` is rejected at startup with a named error rather than silently precedence-ordered — a half-applied migration cannot leave the weaker scheme quietly in force, but it also means there is no window where both credentials are accepted. Plan for it:

1. Mint one key per caller and stage it wherever each caller reads its credential from.
2. In a single config change, delete `proxy_key` and add the `[[clients]]` table; restart.
3. Flip every caller to its own key in the same maintenance window. Anything still sending the old shared secret gets a `401` from the moment the new config is live.

Keep the previous config to hand: rolling back is the same single-step swap in reverse.

> [!WARNING]
> Under `[[clients]]`, `/_stats` and `/metrics` require an **operator** credential (a client named in `operators`) — a plain client key gets `403`, no key gets `401`. If a Prometheus scrape, an uptime check, or a Kubernetes `httpGet` probe hits this proxy unauthenticated today, it will start failing — give those callers an operator key in the same change, or a failing probe becomes a restart loop. Under `allow_unauthenticated` both surfaces stay open (a `warn` fires at most once per route per 5 minutes so a scraper can't drown the log).

### Client Identification

With `[[clients]]`: the authenticated principal's `name`. Full stop.

Without it (legacy / open), identity is resolved from the request:

1. **`X-Client-ID` header** — explicit, takes priority
2. **`client_names` IP mapping** — fallback based on source IP
3. **`"-"`** — default when neither is set

Per-client token usage and budget status appear in `/_stats`.

### Per-client model allow-lists

`clients[].models` restricts which models a client may request, using the same exact-match + `*`-suffix wildcard semantics as `endpoints[].models`. Empty or absent = all models allowed.

A request for a model outside the list is rejected with **403** — a policy denial, distinct from the 429s that mean "capacity, try later" — and counted as `anthropic_client_model_denied_total{client,model}`. Operators bypass it, as they do every other gate check. The check sits in `pre_request_gate`, which both `/v1/messages` and `/v1/chat/completions` route through, so it covers both surfaces.

It **fails closed** on a model it cannot read. The proxy takes the model from the top-level `model` key of a JSON body; a body that does not parse, or a route that nests the model elsewhere (`/v1/messages/batches` puts it under `requests[].params.model`), yields no model — and a client that has an allow-list is then denied rather than waved through. Clients with no allow-list are unaffected.

---

## Security

> [!IMPORTANT]
> **Authentication is default-deny.** A config with neither `[[clients]]` nor `proxy_key` **fails startup** with a named error. The single escape hatch is `allow_unauthenticated = true`, which boots with a startup warning and is meant for networks where access control exists outside the proxy (NetworkPolicy, tailnet) — never a public ingress. A misconfigured deploy is a crash loop, not a silently open proxy.

| Layer | Config | Effect | Unset behaviour |
|:------|:-------|:-------|:----------------|
| **Listen binding** | `listen = "127.0.0.1:8082"` | Only accepts connections on that interface | — (required) |
| **IP allowlist** | `allowed_ips = ["100.64.0.0/10"]` | Rejects unlisted source IPs (403) | **allow all** |
| **Per-client keys** | `[[clients]]` | Requires a per-client credential; identity = the credential (401) | **startup error** (unless `allow_unauthenticated`) |
| **Proxy key** (legacy) | `proxy_key = "<64 hex>"` | Requires a single shared `x-api-key` (401) | **startup error** (unless `allow_unauthenticated`) |
| **Admin surfaces** | `operators = ["ops"]` | `/_stats` + `/metrics` need an operator credential (401/403) | no one can read them under `[[clients]]` |
| **Read-only principal** | `admin_readers = ["vmagent"]` | Same two surfaces, `/v1` answers 403 | scrapes need an operator key, which bypasses every limit |
| **Failed-auth throttle** | `auth_failure_limit` / `auth_failure_window_secs` | Further invalid credentials get 429 + `retry-after` per client IP after repeated failures; valid credentials always pass | on (10 / 300s) |
| **Trusted proxies** | `trusted_proxies = ["192.0.2.0/24"]` | Real client IP recovered from `x-forwarded-for` behind a listed LB | header ignored |
| **Caller-identity privacy** | `forward_caller_identity = false` | Caller IP and `x-client-id`/`x-agent-id`/`x-session-id` headers dropped before the upstream request | **stripped** |
| **Model allow-list** | `clients[].models` | Rejects models outside a client's list (403) | all models |

IP check runs first, then the credential check; failed credentials are subject to the throttle. All apply to every route including `/_stats` and `/metrics`. Credentials are compared in constant time, and startup rejects any configured credential shorter than 32 characters (generate with `openssl rand -hex 32`).

**TLS terminates at the ingress.** The proxy speaks plain HTTP and its container port must never be published directly to the internet — put it behind a TLS-terminating load balancer or ingress, list that LB in `trusted_proxies`, and let the ingress carry the certificate. Bearer credentials without TLS are credentials in cleartext.

### Admin surfaces are operator- and reader-only

`/_stats` disclosures are a reconnaissance report for anyone planning to spend the pool: raw `client_id`s, agent/session prefixes, models, **endpoint account names**, token counts, per-account utilisation and budgets. So under `[[clients]]` both `/_stats` and `/metrics` answer only to a client named in `operators` or `admin_readers` — unauthenticated gets `401`, a valid credential in neither list gets `403`. Under legacy `proxy_key` the (single) key holder is the operator by construction. Under `allow_unauthenticated` both surfaces serve, and unauthenticated access logs at `warn` — rate-limited to once per route per 5 minutes, so the open posture stays visible without a per-scrape firehose.

**Give a scrape `admin_readers`, not `operators`.** The two roles reach the same two surfaces, but `operators` also bypasses the model allow-list, the daily budget, the utilization ceiling and the emergency brake — so a monitoring credential handed out as an operator is unmetered authority to spend the pool, and any `client_budgets` entry written for that name is dead config the gate never reads. An `admin_readers` principal gets `403` on every `/v1` surface before endpoint selection, so it cannot reach an upstream at all. Use it for Prometheus/vmagent, uptime checks and Grafana datasources; keep `operators` for the human who genuinely needs the bypass. A name may appear in one list or the other, never both, and `admin_readers` requires `[[clients]]` — under legacy `proxy_key` there is no per-principal identity to scope, so configuring it is a startup error rather than a control that quietly does nothing.

### Real client IP behind a load balancer

Behind a GCLB/Cloudflare/ingress, the TCP peer is the LB — without XFF handling, IP allowlists degenerate to "allow the LB", per-IP throttles rate-limit the LB, and every log line records the LB. Configure `trusted_proxies` with the LB's address range; the client IP then becomes the **rightmost `x-forwarded-for` entry not itself in `trusted_proxies`** — the last hop an attacker cannot append to. From any peer *not* in the list the header is ignored entirely (never trusted, never logged as authoritative), and malformed entries fall back to the peer address.

Those headers stop here. Once the client IP is resolved, the proxy drops `x-forwarded-for`, `x-real-ip`, `forwarded`, `true-client-ip` and its own `x-client-id`, `x-agent-id`, `x-session-id` from the upstream request on both forward paths, so a pooled-account request upstream is not labelled with the caller behind the proxy. `forward_caller_identity = true` restores relaying. Two things this does not cover. Edge-added `cf-*` headers (`cf-connecting-ip` carries the same caller IP) are the ingress's to strip, not the proxy's ([#166](https://github.com/27b-io/anthropic-lb/issues/166)): the Cloudflare Worker does, a plain cloudflared tunnel does not, so on that path the caller IP still reaches the upstream. And Claude Code's native `x-claude-code-session-id` passes through untouched pending [#171](https://github.com/27b-io/anthropic-lb/issues/171).

> [!IMPORTANT]
> Behind a load balancer, **`[[clients]]` credentials are the identity**. The `client_names` IP map is a lab-only convenience: it maps *source addresses*, and once traffic arrives through an LB the recovered XFF address is only as trustworthy as the LB's own header hygiene. Do not hang budgets or operator status on `client_names` on a public ingress.

### Failed-auth throttling

This in-process throttle bounds the volume of detailed `401` responses; it does not reduce credential-comparison throughput. Credential-stuffing and request-rate controls belong at the public ingress. After `auth_failure_limit` failures from one client IP inside `auth_failure_window_secs`, further **invalid** credentials from that IP get `429` with `retry-after`. Credential comparison runs first, before the throttle check, so a known-good principal still passes when NATs and load balancers collapse unrelated callers onto one resolved address. Successful requests do not clear the shared IP's failure window, so invalid traffic remains throttled until expiry. This trade relies on the enforced `MIN_KEY_LEN = 32`; if the credential floor is lowered, the throttle check should run before credential comparison instead. Failures are counted in `anthropic_auth_failures_total{route,cred}` and logged with the resolved client IP plus, because every caller behind a NAT gateway or TCP-forwarding VIP resolves to one address, the attribution the IP cannot give: the actual TCP `peer` socket address (IP:port — distinct from the resolved client IP once `trusted_proxies` is in play), the header shape the credential arrived in (`cred`: `none` / `x-api-key` / `bearer` / `auth-other`), a one-way 12-hex `key_fp` of the presented value for `x-api-key`/`bearer` (never the value or any prefix of it — `auth-other` gets no fingerprint, since there is no bare credential to hash), and the `ua` clipped to 64 chars. The user-agent is deliberately not a metric label: its series would be claimed by whoever fails first on a public ingress. To check whether a suspect key is the one being rejected, fingerprint it the same way and compare: `python3 -c 'import hashlib,sys; print(hashlib.blake2s(b"anthropic-lb/auth-fp\x1f" + sys.argv[1].encode()).hexdigest()[:12])' "$KEY"`. The throttle table is bounded (4096 IPs), so the tracking structure itself cannot be flooded into an OOM — and eviction is threat-aware: expired windows are purged first, then the least-established live entry (lowest failure count, oldest window as tie-breaker) is evicted, so a flood of fresh failures cannot flush an active lockout to reset it.

### Credential-path hardening

The proxy forwards operator OAuth/API tokens upstream, so the paths a request
can steer are locked down by default:

- **Endpoint hosts are pinned.** An `anthropic`-protocol endpoint whose
  `base_url` host isn't `api.anthropic.com` fails startup validation —
  a typo'd or tampered URL would ship the account token to that host.
  Deliberate mirrors opt in per endpoint with `allow_nonstandard_host = true`.
- **Redirects are never followed.** The upstream HTTP client uses
  `redirect::Policy::none()`; a `3xx` from an upstream surfaces to the caller
  as a `502` with a distinct log line instead of re-sending credentials to
  the `Location` target.
- **Response headers are allow-listed.** Only `content-type`,
  `content-length`, `cache-control`, `request-id`, `retry-after`, and
  `x-should-retry` are reflected to callers (plus the proxy's own
  `x-budget-status`).
  `anthropic-ratelimit-*` (the pooled capacity of every account),
  `set-cookie`, and org-identifying headers are stripped;
  `expose_upstream_ratelimit_headers = true` restores the ratelimit
  passthrough for trusted networks.
- **Client beta flags are allow-listed.** On OAuth endpoints, client
  `anthropic-beta` values outside `allowed_client_betas` are dropped before
  forwarding, logged at `warn`, and counted in
  `anthropic_beta_flag_dropped_total{flag}`. The built-in default covers the
  flags the proxy itself needs, the flag families Claude Code sends, and
  `fast-mode-*`; the authoritative list is `DEFAULT_CLIENT_BETA_ALLOWLIST`
  in `src/utilization.rs`. Known body pairings — top-level ones degrade quietly
  when their flag is dropped (next bullet); nested ones still `400`, which is
  why their flags stay on the default list:
  - `context-management-*` ↔ top-level `context_management`
  - `fast-mode-*` ↔ top-level `speed: "fast"`
  - `dangerous-tool-use-*` + `auto-mode-classifier-*` ↔ top-level `safeguards`
  - *(nested)* `mid-conversation-tool-changes-*`, `per-turn-control-*`,
    `timing-*` ↔ `tool_addition`/`tool_removal` blocks and
    `output_config.effort`/`timing` on the `role: "system"` entry in `messages`
- **A dropped flag takes its body field with it.** Some beta families pair a
  header flag with a top-level request-body field that only exists when the
  flag is declared (the top-level pairings above). Forwarding
  that field without its flag is a hard upstream `400`, not a quiet downgrade,
  so when the filter drops anything the proxy also removes every top-level
  body field that is neither base `/v1/messages` schema nor owned by a flag
  that survived the SAME request. Scoped to `/v1/messages` and
  `/v1/messages/count_tokens` — every other route forwards its body
  byte-for-byte whatever the filter did to the header. Retained fields are
  spliced through as their original bytes, so nothing below the top level is
  reformatted.

  The keep-side depends on `BETA_BODY_FIELDS` being **total** over the
  allow-list: a surviving flag protects its body field only if a row claims
  it, and a family with no row would have its field deleted out from under a
  caller entitled to use it. A build-time test enforces that. If a flag
  survives that has no row at all — an operator's custom
  `allowed_client_betas` carrying a family this binary predates — the proxy
  cannot tell that family's fields from an orphan's, so it forwards the body
  untouched and forgoes the degrade for that request. Restoring a stripped
  feature therefore needs BOTH an allow-list entry and a `BETA_BODY_FIELDS`
  row: the allow-list stops the header being dropped, the row is what protects
  the body half. The feature turns off; the request still
  works — including for a beta family this proxy has never heard of, with no
  allow-list change. Strips are counted in
  `anthropic_beta_body_field_stripped_total{field}` and warned on first
  sighting; a non-zero rate means a paired family is in live traffic and
  needs both an allow-list entry and a row. Nested pairings
  (`extended-cache-ttl-*` → `cache_control.ttl`, and the per-turn family's
  fields inside `messages`) are out of scope — they are on the default
  allow-list and so are never dropped.
- **A fast-mode `429` is forwarded to the caller, not treated as account
  exhaustion.** Fast mode (`speed: "fast"`) bills against its own rate bucket,
  separate from the account's 5h/7d windows, so a `429` on a fast request does
  not cool the account or rotate to another one — the caller gets the `429`
  with upstream's `retry-after`, exactly as a direct Anthropic client would.
  Without this, one client looping fast requests would hard-limit every
  account in turn and deny standard-speed traffic to everyone else. Occurrences
  are counted as `anthropic_fast_mode_429_total{account}`. Transient *burst*
  `429`s (`x-should-retry` with no `retry-after` and no rate-limit headers) are
  excluded: those are per-minute limits on the account itself, so they keep
  their usual backoff and rotation whatever speed was requested.

### Known Limitations

- **Client ID spoofing (legacy configs only)**: without `[[clients]]`, the `x-client-id` header takes priority over the `client_names` IP mapping, so any authenticated client can claim any identity — including an operator name, another client's budget, or another client's response-cache tenant. Configure `[[clients]]` to close this; it is the reason that mode exists.
- **Emergency brake is model-blind**: The brake evaluates worst-case utilization across all model claims. If sonnet is exhausted but haiku has headroom, the brake blocks all traffic including haiku. This is intentional fail-safe behavior.

---

## Guardrails

An optional content-inspection layer scans each request for leaked secrets and
PII before it is forwarded upstream. It is **opt-in at build time** and **shadow
mode by default** — it counts and annotates, but blocks nothing, until you have
measured false-positive rates on your own traffic.

### Building with the guard

The layer lives behind the `guard` cargo feature and is **off by default**:

```bash
cargo build --release                    # no guard; unchanged behaviour, no extra deps
cargo build --release --features guard   # guard compiled in
```

With the feature off, the scanner crates are not compiled and request handling
is byte-for-byte unchanged. The two scanners are pure Rust (no C/C++ build
dependency): a secrets scanner over a bundled gitleaks/kingfisher ruleset, and a
PII scanner (emails, cards, IPs, JWTs, national ids, provider API-key shapes).

### What it scans

Only the **newest `user` text and `tool_result` blocks** of the request body —
the freshest untrusted content. The `system` prompt is never scanned (it is
operator-trusted and a known false-positive surface). Detection is strictly
read-only: the body forwarded upstream is byte-identical to what the client
sent, so prompt-cache prefixes and routing are never disturbed. To bound
hot-path latency, at most 32 KiB of that content is scanned per request; a
larger newest turn has its tail left unscanned (surfaced as `truncated` in the
guard log).

Scanning is content-driven: a proxied request is scanned when its JSON body
carries a Messages-shaped `messages` **array** — `/v1/messages`,
`/v1/messages/count_tokens`, and `/v1/chat/completions`. A request with no
body, such as `GET /v1/models`, has nothing to scan and passes through.

On `/v1/chat/completions` the body is scanned after translation to the Messages
shape, but judged **readable** on the body the client sent. The translator is
lossy in three places — a non-array `messages`, a `tool` message's non-string
`content`, and a malformed `image_url` part all collapse to empty before the
scanner sees them — so a document that is unreadable on the wire would
otherwise read as clean, which is worse than reading as unscanned.

Content the Messages shape does not carry is not scanned: fields translation
drops outright (`messages[].name`, the top-level `user`), content blocks of a
type the scanner does not read (`image`, `document`, `thinking`), older turns,
and a body with no `messages` field at all (`/v1/complete`'s `prompt`, batch
requests). Those are **coverage** limits — the guard read the document and there
was nothing in it that it reads.

A `messages` the scanner cannot **read** is a different thing, and is treated as
one: an element that is not an object, a `role` that is absent, not a string, or
not `user`/`assistant`, or a newest-turn `content` that is present in a shape the
scanner cannot walk (an object, a text block whose `text` is not a string, a
`tool_result` whose content is neither string nor block array). Those are not
"nothing to scan" — the guard could not tell what it was looking at, and under
`block` they fail closed (below). Note an absent field is not the same as a
present unreadable one: absent content cannot be hiding anything.

### Per-client policy

Each client sets its policy under `[[clients]]`:

```toml
[[clients]]
name = "alice"
key  = "<openssl rand -hex 32>"
guard = "annotate"   # "off" | "annotate" | "block"  (default: "annotate")
```

- `off` — skip scanning entirely.
- `annotate` (**default**) — scan, count, and log findings, but always forward
  the request. Shadow mode.
- `block` — reject a request carrying any finding.

Operator clients are always `off` regardless of configuration.

- **Annotate** adds an `X-Guard-Findings: <count>` response header and a
  structured `guard` log line keyed by request id (scanner, detection type,
  count — never the matched text).
- **Block** returns `HTTP 400` with:

  ```json
  {
    "type": "error",
    "error": {
      "type": "guard_blocked",
      "message": "...",
      "findings": [ { "scanner": "...", "detection_type": "...", "start": 0, "end": 0 } ]
    }
  }
  ```

  Findings carry **byte offsets only — never the matched secret** — in logs, the
  error body, and metrics alike. On `/v1/chat/completions` the same 400 uses the
  OpenAI error envelope (`error.code = "guard_blocked"`).

`block` **fails closed.** A request it cannot scan in full is rejected with the
same 400 rather than forwarded unscanned:

- the body is not JSON — a parse differential must not smuggle content past the
  scan; this includes multipart uploads such as `/v1/files`;
- `messages` is present but is not an array — a string, an object, a number,
  `null`. The scanner reads that field as an array, so none of it reaches the
  scan while all of it reaches the upstream. This applies on every path. A body
  carrying **no** `messages` key is not rejected on any path but
  `/v1/chat/completions` (below), `/v1/messages` included: the proxy serves
  every Anthropic endpoint through one handler, and most of them
  (`/v1/complete`, `/v1/models`) never send the field;
- `messages` is an array the scanner cannot read — an element that is not an
  object, a `role` absent / not a string / not `user` or `assistant` (`"User"`
  included: the compare is exact), or a newest-turn `content` present in a shape
  it cannot walk. Each of these is content the guard never saw and the upstream
  would have;
- the body parsed as JSON but is not an object — a bare string or array is not a
  request any endpoint here accepts, and reads as "no `messages` field" without
  this rule;
- on `/v1/chat/completions` only, `messages` is absent or not an array, or a
  `user`/`tool` message's content is in a shape translation would flatten (a
  non-string `tool` content, a content part with no string `type`, a `text` that
  is not a string, an `image_url` that is not an object with a string `url`).
  That endpoint is a single API which requires `messages`, and an
  `openai`-protocol endpoint forwards the client's original bytes, so what
  translation drops still ships;
- the newest-turn content exceeded the scan limit, so its tail was never
  inspected — otherwise padding past the limit would bypass enforcement.

What is **not** rejected: a readable document that simply carries no text the
scanner reads. An image-only turn, a conversation with no `user` turn yet, and a
non-Messages body on any path but `/v1/chat/completions` all forward untouched
under `block` — the guard read them and there was nothing to scan, which is not
a scan failure.

A JSON null cannot hide content, so the guard reads `null` as absent. Where an
absent field passes — a newest-turn `content`, a text block's `text`, a
`tool_result.content` — so does `null`, and a client whose serializer emits
`null` for an omitted optional is not rejected. Where absence is rejected — a
message's `role`, a content block's `type` — so is `null`. The one exception is
`messages` itself. On any path but `/v1/chat/completions` a body with no
`messages` key is not a Messages request and forwards, but `messages: null` names
the field and is rejected. `/v1/chat/completions` requires `messages` and rejects
both, as listed above.

A block-mode client must therefore send JSON Messages traffic in a shape the
scanner can parse, and keep scannable content within the limit. `annotate` (shadow mode) never rejects — it
scans best-effort and always forwards.

### Metrics

On `/metrics` (when built with the feature):

- `anthropic_guard_verdicts_total{client, scanner, verdict}` — counter.
- `anthropic_guard_scan_duration_seconds` — histogram of per-request scan time.

The `client` dimension of the verdicts counter is cardinality-bounded (overflow
folds into `_other`). Under a legacy shared-secret configuration the client id
is caller-asserted, so per-client verdict counts are only reliable when
per-client keys (`[[clients]]`) are configured — the same posture as the other
per-client counters.

---

## Endpoints

| Route | Method | Description |
|:------|:-------|:------------|
| `/*` | Any | Proxied to upstream Anthropic API |
| `/v1/chat/completions` | POST | OpenAI-compatible → Anthropic translation |
| `/_stats` | GET | JSON stats (utilization, tokens, budgets, live sessions) |
| `/metrics` | GET | Prometheus-format metrics |

All endpoints are gated by `[[clients]]` (or legacy `proxy_key`) and
`allowed_ips`. `/_stats` and `/metrics` are **operator- and reader-scoped**: under
`[[clients]]` they require a credential whose name is in `operators` or `admin_readers`
(401 unauthenticated / 403 for any other client — see §Security).

### Session context-window visibility

`/_stats` includes a `sessions` array — the live sessions seen by this
replica (Anthropic-protocol traffic), sorted by context-window occupancy
descending, capped to the top 50. A "session" is one affinity routing pin,
so fan-out subagents sharing a coarse session id appear as distinct entries.
Each entry:

| Field | Meaning |
|:------|:--------|
| `session` | Redacted session label — hash of the affinity key (also appears in `prompt too long` WARN logs for joining) |
| `client_id` | Resolved client id (operators masked as `_operator`) |
| `agent` / `session_prefix` | First 8 chars of the `x-agent-id` / session-id headers as sent |
| `model` / `endpoint` | Model and the endpoint the session is currently pinned to |
| `last_prompt_tokens` | `input + cache_read + cache_creation` from the last successful response — the prompt's window occupancy |
| `context_window` | Model context window: 200k, or 1M when the request carries the `context-1m` beta |
| `context_window_pct` | Occupancy percent — sessions near/over 100 are about to hit `prompt is too long` |
| `requests` / `last_seen` | Request count and epoch of last activity |

The registry is in-memory, per-replica, bounded (`session_registry_max`) and
TTL-evicted (`session_registry_ttl_secs`); raw IPs and session ids never
leave the process. Routing does not read it.

Upstream `400 invalid_request_error` responses matching *"prompt is too
long"* are additionally counted as `anthropic_prompt_too_long_total{model}`
on `/metrics` and logged as a structured WARN with the redacted session
label plus the observed-vs-max token counts parsed from the error message.
The 400 itself is forwarded to the client unchanged.

Requests rejected by a client's model allow-list are counted as
`anthropic_client_model_denied_total{client,model}` and logged at WARN. The
`model` label is caller-controlled, so the label set is bounded — overflow
past 64 distinct pairs lumps into a single global
`client="_other",model="_other"` bucket (hard bound: 64 + 1 series).
`_other` is a reserved client name: config validation rejects a
`[[clients]]` entry or `client_names` value named `_other`, and a legacy
`x-client-id: _other` header is ignored, so real traffic can never
pre-claim the overflow key. `anthropic_client_model_token_usage_total`
shares the same overflow scheme (bucket at 256 + 1 series).

### Per-claim rate-limit visibility

The API reports model-specific sub-budgets ("claims") alongside the general
5h/7d windows. Each claim's utilization and waste risk have always been on
`/metrics`; its **status** and **reset** are exported too:

| Series | Meaning |
|:-------|:--------|
| `anthropic_claim_rate_limit_status{account,claim}` | Status ordinal — `0=allowed, 1=warning, 2=throttled, 3=rejected`. Same encoding as `anthropic_account_rate_limit_status`. |
| `anthropic_claim_reset_seconds{account,claim}` | Seconds until that claim's window resets. Omitted (not zeroed) once the reset is in the past. |

Status is **not** derivable from utilization in either direction, so read it
rather than thresholding the percentage: an account can sit at 0.98 and still
be `allowed_warning` — the router keeps routing to it — while another reads
1.0 and is `rejected`, which makes the router hard-skip it for that claim.

Across replicas these are gauges mirroring the same upstream claim, so
aggregate with `max by (account, claim)`; `sum` multiplies the reading by the
replica count.

The `claim` label comes from the upstream `representative-claim` header,
never from client input, but it is still an unvalidated remote string. It is
truncated to 64 characters, and each account retains at most 32 distinct
**unreserved** claim keys. Reserved keys — the four that gate all traffic plus
the model-band carve-out — are always admitted, so the hard bound is 37 per
account. That bound holds on the live header path and on the two paths that
restore the map whole (the persisted state file and the cross-replica mirror),
so a map written by an older build cannot restore unbounded.

Keys already present keep updating past the cap; a genuinely new one past it is
dropped, and the refusal is logged once per account rather than once per
request. Unlike the caller-labelled counters above, overflow is *not* folded
into an `_other` bucket: both routing lookups match exact keys and the
emergency brake's input is allowlist-filtered, so an unknown key is already
inert to routing and a bucket would only add a fake claim to the metrics.
Reserving the keys that are *not* inert is what makes the cap safe — without
that, a flood of unknown keys could lock out `seven_day` itself, and an account
with no derivable weekly utilization routes as though it had no weekly limit.

### Pool exhaustion

`anthropic_pool_exhausted_total{kind}` counts the responses this proxy
generated itself because no endpoint could serve the request:

| `kind` | Response | Cause |
|:-------|:---------|:------|
| `rate_limited` | `429 exhausted all endpoints` (no `Retry-After`) | Every eligible endpoint was rate-limited or gated, or a retry round saw a 529. |
| `transient` | `503 + Retry-After: 1` | Every eligible endpoint failed in transport, with no 529. |

The per-account gauges describe the pool's *state*; this counter is the only
signal that a caller was actually turned away because of it. Both label values
are emitted from process start, so a flat zero is a measurement rather than an
absence of data. These are independent per-replica event counts — aggregate
with `sum by (kind)`, where `max` would undercount.

### Request latency, restarts and build identity

`anthropic_http_request_duration_seconds{route, status}` is a histogram of the
time from request receipt to the moment the proxy sends response headers. For a
streamed response none of the stream time is included: the proxy returns the
upstream headers and pumps the body outside the measured span. It is recorded
by router-wide middleware, so every response counts,
including the 401/403/429/503 the proxy generates itself before any upstream is
contacted. `route` is a closed vocabulary (`/v1/messages`,
`/v1/messages/count_tokens`, `/v1/chat/completions`, `/_stats`, `/metrics`,
`other`) and `status` is the HTTP code, so a caller cannot mint series by
varying the URL. A request the client abandons before response headers is not
recorded, because the server drops the in-flight request when the client
disconnects. Under an upstream stall the slowest requests can therefore be
missing from the tail. Per-replica: aggregate with `sum by (le, route, status)`
before `histogram_quantile`.

`process_start_time_seconds` is the standard start-time gauge:
`time() - process_start_time_seconds` is uptime and
`changes(process_start_time_seconds[1h])` counts restarts behind one scrape
target, so a recycle no longer has to be inferred from counter resets. A
replacement that comes up under a new `instance` label is a new series, which
`changes()` does not count.

`anthropic_lb_info{strategy, version, revision}` identifies the running build.
`version` is the crate version; `revision` is the 7-character git commit taken
from the `GIT_SHA` Docker build argument (`unknown` when built without it), so
it compares directly against a `sha-*` image tag.

`anthropic_upstream_transport_errors_total` has two scopes, and
`anthropic_cluster_redis_connected` tells them apart on each scrape. Where that
gauge is `1`, the counter is the fleet-wide total read back from Redis, and
every such replica reports the same value. Aggregate those with `max`, since
`sum` multiplies the count by the number of replicas. Everywhere else the
replica reports its own count: without Redis, before its first sync, or while
Redis is unreachable. Aggregate those with `sum`. Without Redis the count is
every failure the replica has seen. With Redis it is the failures still to be
flushed. A flush reads Redis's reply to each command, so a count Redis applied
is never sent again, and a count Redis rejected is kept for the next flush. Only
when no reply arrives at all (a dropped connection or a timeout) is the whole
batch kept and sent again, so after a lost reply some of those failures can
already be in the fleet total. It can also under-count: if Redis rejects writes
but still serves reads, the gauge stays `1` and that replica's kept failures
appear in neither term. During a partial outage, the `max` over connected
replicas plus the `sum` over the rest is therefore an estimate, not an exact
total.

A replica that changes scope also steps its series between the fleet total and
its local count, which `rate()` and `increase()` read as a counter reset or a
burst of new failures. Connected replicas also read the fleet total on their own
5-second ticks, so a `max` taken before `rate()` falls back to an older reading
when the freshest replica drops out, and `rate()` reads that as a reset too.
Keep only fleet-scope readings, take the rate per replica, then take the `max`:

```promql
max by (kind) (
  rate((anthropic_upstream_transport_errors_total
    and on(instance) anthropic_cluster_redis_connected == 1)[5m:])
)
```

The HELP text states the scopes on the scrape.

### OpenAI JSON-mode compatibility

> [!IMPORTANT]
> **Breaking change:** `/v1/chat/completions` now strips surrounding Markdown
> fences only when the request explicitly sets
> `response_format: {"type":"json_object"}`. Previously, non-streaming
> responses were stripped unconditionally, which could alter a legitimate
> fenced code-block response. To preserve the prior raw-JSON normalization,
> send `response_format: {"type":"json_object"}`; otherwise consume the
> model content verbatim. Streaming and non-streaming responses now follow the
> same rule.

<details>
<summary><strong>Example <code>/_stats</code> response</strong></summary>

```json
{
  "endpoints": [
    {
      "name": "primary",
      "protocol": "anthropic",
      "priority": 0,
      "passthrough": false,
      "requests_total": 1042,
      "utilization": 0.25,
      "representative_claim": "five_hour",
      "remaining_requests": 950,
      "remaining_tokens": 4800000,
      "hard_limited_remaining_secs": null,
      "burn_rate": { "last_5m": 12.5, "last_1h": 10.2, "last_6h": 8.7 },
      "headroom_requests": 42000,
      "token_usage": {
        "input_tokens": 2450000,
        "output_tokens": 180000,
        "cache_creation_input_tokens": 50000,
        "cache_read_input_tokens": 1200000
      }
    },
    {
      "name": "openrouter",
      "protocol": "openai",
      "priority": 100,
      "passthrough": false,
      "requests_total": 87,
      "utilization": null,
      "token_usage": {
        "input_tokens": 210000,
        "output_tokens": 15000,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0
      }
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
    "alice": { "daily_limit": 5000000, "used_today": 1915000, "remaining": 3085000 }
  },
  "aggregate": {
    "total_headroom_requests": null,
    "consumers": {
      "alice": { "share": 0.65, "requests_per_minute": 4.2 }
    }
  },
  "sessions": [
    {
      "session": "9f2c4a1b8e3d5f07",
      "client_id": "alice",
      "agent": "d3adbeef",
      "session_prefix": "a1b2c3d4",
      "model": "claude-sonnet-4-5",
      "endpoint": "primary",
      "last_prompt_tokens": 187000,
      "context_window": 200000,
      "context_window_pct": 93.5,
      "requests": 42,
      "last_seen": 1753600000
    }
  ],
  "cluster": {
    "redis_connected": true,
    "replicas_seen": 3,
    "budget_usage": {
      "alice": { "limit": 5000000, "used": 1915000 }
    }
  },
  "strategy": "dynamic-capacity-v1"
}
```

</details>

### OpenAI thinking effort

OpenAI clients can set thinking effort on `/v1/chat/completions`:
`reasoning_effort` is translated to Anthropic's `output_config.effort` when it
is one of `low`, `medium`, `high`, `xhigh` or `max`. `minimal`, `none` and any
other value are dropped with a warn log, and the request runs at the model's
default effort — which can cost more than the low effort the client asked for.
No `thinking` block is added.

Per-model support is not checked. A model without effort support, or without
the requested level, rejects the request with a non-retryable 400 (for example
`This model does not support effort level 'xhigh'. Supported levels: high, low,
max, medium.`), the same way OpenAI rejects `reasoning_effort` on a
non-reasoning model. Before this translation existed the field was silently
ignored, so a client that always sends it must now send it only to models that
support it.

---

## OpenAI-Compatible Upstreams

Route to non-Anthropic, OpenAI-compatible APIs (OpenRouter, local models) by adding an endpoint with `protocol = "openai"`. There is no separate upstream pool and no `/upstream/<name>/` route — an OpenAI endpoint is a first-class member of the unified `[[endpoints]]` pool. Because these endpoints carry no Anthropic rate-limit data, they are not selected by headroom: each enters routing as a fixed-weight candidate at its configured `priority` (the transport circuit breaker still applies).

```toml
[[endpoints]]
name = "openrouter"
protocol = "openai"
base_url = "https://openrouter.ai/api"   # required for openai; must be https://
token = "sk-or-..."
priority = 100   # tried only after Anthropic tiers are exhausted
```

The configured `token` is injected as `Authorization: Bearer`, and the request is forwarded to `base_url` with automatic Anthropic↔OpenAI translation (any proxied path) or direct passthrough (`POST /v1/chat/completions`). Routing by `priority` is how an OpenAI endpoint replaces the old `fallback_upstream`: give it a high `priority` so free Anthropic capacity drains first.

### What doesn't translate

The Anthropic↔OpenAI translation layer is not lossless. When an Anthropic-format request is routed to an `openai` endpoint:

- **Silently dropped** (no OpenAI equivalent, request proceeds without them): `thinking` (extended reasoning), `cache_control` / prompt caching, `top_k`, `metadata`. The passthrough set is only `temperature`, `top_p`, `stream`, `max_tokens`, `stop_sequences`.
- **Silently dropped**: `tool_choice: {"type": "none"}` — the only unhandled `tool_choice` variant.
- **Hard 400, no retry** (request itself is the problem, so rotating endpoints won't help): `document` blocks (PDFs) and image `source.type` values other than `base64`/`url`, when they sit directly in a user message's `content`. The same blocks inside `tool_result.content` or an assistant turn are silently dropped instead.
- **Streaming responses from `openai` endpoints record no token usage** — budget/utilization checks still run, but nothing is debited (non-streaming has been debited since [#107](https://github.com/27b-io/anthropic-lb/pull/107)).
- **In-band upstream SSE error events are dropped mid-stream on translated streams** (Anthropic-format requests streamed from an `openai` endpoint) rather than surfaced to the client ([#94](https://github.com/27b-io/anthropic-lb/issues/94), open); direct passthrough (`POST /v1/chat/completions`) forwards them unchanged.
- **The emergency brake only watches Anthropic endpoints** and fires pre-routing: once the Anthropic pool is saturated it 429s every authenticated non-operator request that clears the model allow-list, budget and utilization checks (operator clients bypass it), even ones whose model is served exclusively by an `openai` endpoint with capacity to spare.

---

## How It Works

```
1. Request arrives → validate proxy_key + IP allowlist
2. Identify client (X-Client-ID header → IP mapping → "-")
3. Pre-request gate:
   a. Operator? → bypass all checks
   b. Check per-client daily token budget (429 if exceeded)
   c. Check per-client utilization limit (429 if all Anthropic endpoints above limit)
   d. Emergency brake (429 if all Anthropic endpoints above emergency_threshold)
4. Extract model from request body
5. Filter accounts by model allowlist
6. Compute time-adjusted utilization per claim window (5h, 7d per model)
7. Apply status floors (warning ≥ 0.80, throttled ≥ 0.98, rejected = 1.0)
8. Exclude accounts above soft_limit utilization ceiling
9. Pick account via headroom-proportional weighted bucket hashing (client affinity)
10. Inject auth token + auto-cache header
11. Forward request to upstream Anthropic API
12. If 429 → mark rate-limited (propagate to Redis), add to skip list, retry with next account
13. If 5xx/529 → add to skip list, retry with different account
14. Parse rate-limit headers (utilization per claim, reset times, status)
15. Extract token usage from response (streaming SSE or JSON body; streams from `openai` endpoints have no usage extraction)
16. Record extracted usage per-account + per-client, update budget (local + Redis)
17. Write shadow log entry (async, non-blocking)
18. State persisted to disk (+ Redis if configured), restored on restart
```

> [!TIP]
> The proxy reads Anthropic's `anthropic-ratelimit-unified-*` headers to track real utilization per rate-limit window (5h, 7d) and per-model claim (e.g. Sonnet vs Opus sub-budgets). Near window resets, utilization is time-discounted so accounts about to reset aren't unnecessarily avoided. API status signals (`allowed_warning`, `throttled`, `rejected`) enforce utilization floors regardless of the reported number.

---

## Logging

Structured `tracing` logs go to stderr at `info` level by default (one line
per request that reaches upstream, carrying routing context — client, model,
account, status, utilization — and token usage together; on the native
`/v1/messages` path the line also carries `fp`, the content fingerprint, `-`
when the body was unparseable). Override the filter with `RUST_LOG` (e.g.
`RUST_LOG=anthropic_lb=debug`), or set `debug_log = "/path/to/file.log"` in
the config to additionally write a `debug`-level file log alongside the
`info`-level stderr stream. At `debug`, the full per-request content
fingerprinting detail (`fingerprint`: `fp`/`fps`/`bps`) becomes visible —
useful for diagnosing routing-affinity stickiness, but too high-volume for
the default filter.

---

## Shadow Logging

When `shadow_log` is set, every request writes a JSONL entry with:

```json
{
  "ts": "2026-02-13T20:15:00Z",
  "client": "alice",
  "account": "primary",
  "model": "claude-sonnet-4-6",
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

## Distributed State (Redis)

For multi-replica deployments, configure `redis_url` to share state across instances:

```toml
redis_url = "redis://redis.example.com:6379"
# or with TLS:
# redis_url = "rediss://redis.example.com:6380"
```

| What's shared | Mechanism | Propagation |
|:--------------|:----------|:------------|
| **Budget counters** | Atomic `INCRBY` per request | Immediate |
| **Hard limits (429)** | `SETEX` on mark + background sync | Immediate local, ~5s cross-replica |
| **Rate info** | JSON blob per account | ~5s (background sync) |
| **Replica heartbeats** | `SET EX 30` per instance | ~5s |

**Fail-open**: All Redis *operations* degrade gracefully. If a syntactically valid `redis_url` is unreachable, each replica falls back to local-only state and reconnects in the background. No request is ever blocked by a Redis error. A `redis_url` that fails to *parse* (e.g. a rotated password containing an unescaped `/`, `?`, or `#`) is a config error, not a connectivity one — it fails startup outright rather than silently running without the shared state an operator asked for.

**Key schema** (all keys auto-expire via TTL):

```text
alb:budget:{client_id}:{epoch_day}  →  u64    (48h TTL)
alb:hard:{account_name}             →  u64    (cooldown TTL)
alb:rate:{account_name}             →  JSON   (reset-based TTL)
alb:heartbeat:{instance_id}         →  u64    (30s TTL)
```

When Redis is connected, `/_stats` includes a `cluster` section with replica count and cross-replica budget usage. The per-client `client_budgets` block and the `anthropic_client_budget_*` gauges are also re-seeded from the shared counter every ~5s, so a freshly restarted replica reports the fleet's spend for the day rather than only what it has seen itself.

> [!NOTE]
> `redis_url` is entirely optional. Omit it for single-instance deployments — behavior is identical to running without Redis.

---

## Response Cache (opt-in, encrypted)

An **opt-in** response cache for non-streaming `POST /v1/messages` and
`POST /v1/messages/count_tokens`. When an allow-listed client replays a
byte-identical request, the previous response is served from cache — **the
upstream call is skipped entirely, so a hit burns zero 5h/7d rate-limit
headroom and does not count against the client's daily token budget**. That
is the point: eval reruns, pipeline replays, and retries after client-side
timeouts stop costing quota. The two endpoints share one allow-list and key
scheme but never cross-serve — the cache key folds in which endpoint a
request came in on, so a body sent to both never gets the wrong one back.

**Default-off.** Without a `[response_cache]` section — or with an empty
`clients` list — behavior is byte-identical to a build without the feature.
Clients not on the allow-list never touch the cache.

```toml
[response_cache]
# Only these client IDs (x-client-id header / client_names mapping) use the cache.
clients = ["alice"]

# Backend: "cachekitio" (cachekit.io SaaS) or "redis" (local Redis/Valkey).
backend = "redis"
redis_url = "redis://redis.example.com:6379/1"
# backend = "cachekitio"
# api_key = "ck_live_..."
# api_url = "https://cachekit.example.com"  # optional; default https://api.cachekit.io. HTTPS enforced, private/loopback IPs rejected at startup.

# Hex-encoded 32-byte master key for client-side encryption (MANDATORY —
# there is no plaintext mode). Generate: openssl rand -hex 32
master_key = "…64 hex chars…"

# Entry TTL. Default: 3600 (1 hour).
# ttl_secs = 3600

# Per-operation budget before the cache fails open. Default: 250.
# op_timeout_ms = 250
```

**What is cached:** the full response body (status + content-type + JSON body)
of **2xx, non-streaming** `/v1/messages` and `/v1/messages/count_tokens`
responses. Streaming (`"stream": true`) requests always bypass the cache, as
do non-JSON responses and bodies over 1 MiB. Error responses are never cached.

**Where it lands, and what the backend can read:** entries are encrypted
**client-side** (in the proxy process) with AES-256-GCM before touching any
storage layer — the in-process L1, local Redis, or cachekit.io only ever hold
**ciphertext**. Per-client keys are derived from `master_key` via HKDF with the
client ID as tenant, so two clients sending identical prompts get separate,
mutually-undecryptable entries. Cache keys are content digests (Blake2s-256 of
model + canonical body + beta headers + client ID + endpoint) — no prompt text
appears in keys, logs, or metrics.

**How a hit looks:** identical response body, plus an `x-alb-cache: hit`
header. Upstream per-request headers (request IDs, rate-limit snapshots) are
not replayed — they described the original exchange.

**Fail-open:** a slow, dead, or misconfigured cache backend never blocks a
request. Every cache operation is bounded by `op_timeout_ms`; on any error the
request proceeds upstream exactly as if the cache did not exist. Hit / miss /
store / error counters are exposed on `/metrics`, one series per endpoint
(`anthropic_response_cache_*_total{surface="messages"}` and
`{surface="count_tokens"}`).

> [!IMPORTANT]
> **Cache isolation is only as strong as the identity it is keyed on.**
> Per-client encryption keys are derived from `client_id`, so what that
> derivation buys depends on where `client_id` comes from:
>
> - **With `[[clients]]`** (recommended): `client_id` is a verified principal,
>   so the derivation is a real confidentiality boundary — N clients, N
>   boundaries. Every name in `[response_cache].clients` must match a
>   configured client, enforced at startup.
> - **Without it** (legacy `proxy_key` / open): `client_id` is the
>   client-asserted `x-client-id` header, so any authenticated caller who
>   *presents* an opted-in client's ID reads that client's cached responses,
>   prompt content included. In that mode the allow-list is **one** trust
>   domain, not N — only opt in clients that already trust each other.

> [!WARNING]
> **Non-determinism caveat — opting in is consent to replay.** Sampling
> parameters (`temperature`, `top_p`, `top_k`) are part of the cache key but do
> **not** disable caching: an opted-in client replaying an identical
> `temperature > 0` request within the TTL gets the **same** answer back, not a
> fresh sample. If your workload needs fresh samples per call, do not opt that
> client in (or vary the request, e.g. a nonce field in metadata).

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

### Docker

```bash
# GIT_SHA bakes the commit into anthropic_lb_info{revision} (omit it → "unknown")
docker build --build-arg GIT_SHA=$(git rev-parse HEAD) -t anthropic-lb .
docker run -v /path/to/config.toml:/etc/anthropic-lb/config.toml anthropic-lb
```

Pre-built images are published to `ghcr.io/27b-io/anthropic-lb` on every push to `main` and on version tags.

<details>
<summary><strong>Docker Compose with Redis</strong></summary>

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  anthropic-lb:
    image: ghcr.io/27b-io/anthropic-lb:main
    ports: ["8082:8082"]
    volumes:
      - ./config.toml:/etc/anthropic-lb/config.toml
    depends_on: [redis]
```

Add `redis_url = "redis://redis:6379"` to `config.toml`.

</details>

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

The Rust toolchain is pinned in `rust-toolchain.toml`, so local builds, CI,
and the Docker image all use the same compiler — rustup reads the file
automatically and installs that version on first `cargo` invocation. (CI's
toolchain action only bootstraps `stable`; every `cargo` command still
resolves through the pin via rustup's toolchain-file override, and the
Dockerfile copies the file into the builder stage.) Toolchain updates arrive as Renovate PRs
and are never automerged (CI infra has a wide blast radius). This matters
because CI runs with `-Dwarnings`: a new stable Rust ships new clippy lints that
can fail code nobody touched, and with the pin that failure lands as a red
*bump PR* — reviewable, with the lint fixes in the same branch — instead of a
red `main` that blocks every merge and the next release
([#142](https://github.com/27b-io/anthropic-lb/issues/142)). Fix new lints
inside the bump PR; don't weaken `-Dwarnings`. Reproduce a bump locally with
`rustup toolchain install <new-version> --profile minimal --component clippy && RUSTFLAGS="-Dwarnings" cargo +<new-version> clippy --all-targets`.
If the lints can't be fixed right now, close the bump PR — `main` stays on the
old pin and Renovate reopens it on the next run.

```bash
# Run all tests
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

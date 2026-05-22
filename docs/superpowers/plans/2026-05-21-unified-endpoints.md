# Unified Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse `[[accounts]]` and `[[upstreams]]` into a single `[[endpoints]]` concept with a `protocol` field, delete the `fallback_upstream` and global `upstream` config keys, and concentrate endpoint-type special-casing into three named, commented call sites.

**Architecture:** Single-file Rust binary (`src/main.rs`, ~19500 lines, axum + tokio). The refactor introduces a `Protocol` enum (`Anthropic` | `OpenAI`), a unified `EndpointConfig`/runtime `Endpoint` type, and a `Vec<usize>` skip list replacing the `Endpoint { Account(usize), Upstream(usize) }` routing enum. The `routing_candidates`/`is_emergency_brake_active`/probe loop are the three named sites that explicitly branch on `endpoint.protocol`. Hard break on config schema, state-file format, and the `/upstream/{name}/*` route.

**Tech Stack:** Rust 2021, tokio, axum 0.7+, reqwest, redis (`ConnectionManager`), serde+serde_json, toml. Quality gates: `cargo test`, `cargo fmt --check`, `RUSTFLAGS="-Dwarnings" cargo clippy --all-targets`.

**Spec:** [docs/superpowers/specs/2026-05-21-unified-endpoints-design.md](../specs/2026-05-21-unified-endpoints-design.md)

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/main.rs` | Entire binary (config, runtime, handlers, tests) | Modify — all changes land here |
| `Cargo.toml` | Dependencies | No change |
| `config.toml` | Local dev config example | Modify — rewrite to `[[endpoints]]` schema |
| `CLAUDE.md` | Repo-level config schema docs | Modify — update "Config Fields" table and routing sections |
| `27b-io/fleet-infra/apps/mem/anthropic-lb/externalsecret.yaml` | Mem cluster ExternalSecret | Modify — rewrite TOML template |
| `27b-io/lab/k8s/mcp/anthropic-lb-externalsecret.yaml` | Lab cluster ExternalSecret | Modify — rewrite TOML template |

The plan touches one source file and four config/doc files. All work happens in the existing branch `feat/overage-aware-routing` unless you create a fresh worktree.

## Execution Order Rationale

The refactor cannot be cleanly bite-sized into independent TDD increments because the runtime type collapse ripples across ~491 call sites simultaneously. The plan therefore uses a **strangler pattern** internally:

1. **Phase 1 (TDD):** Build the new types alongside the old. Each step is a normal red-green-commit cycle.
2. **Phase 2 (compiler-driven):** Migrate consumers one structural area at a time. The compiler enumerates every site that must change; the existing test suite is the safety net. Each task ends with a green `cargo test` + commit.
3. **Phase 3 (TDD):** Layer in the behavior corrections the spec mandates (Protocol::OpenAI short-circuits, retry on 429/5xx, candidate filtering). Each behavior change starts with a failing test that exercises the spec requirement.
4. **Phase 4:** Delete the old code that is now unreferenced.
5. **Phase 5:** Update external docs and both cluster configs.

Commit after each task. The plan never leaves the tree red across a task boundary.

---

## Phase 1 — Config foundation (TDD)

### Task 1: Add `Protocol` enum

**Files:**
- Modify: `src/main.rs` near the existing `RoutingStrategy` enum (~line 115)

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block (search for `mod tests {` near the end of `src/main.rs`):

```rust
#[test]
fn protocol_deserializes_lowercase_strings() {
    let p: Protocol = serde_json::from_str(r#""anthropic""#).unwrap();
    assert_eq!(p, Protocol::Anthropic);
    let p: Protocol = serde_json::from_str(r#""openai""#).unwrap();
    assert_eq!(p, Protocol::OpenAI);
}

#[test]
fn protocol_default_is_anthropic() {
    assert_eq!(Protocol::default(), Protocol::Anthropic);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test protocol_deserializes_lowercase_strings 2>&1 | tail -5
```

Expected: `error[E0412]: cannot find type 'Protocol' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Insert near the `RoutingStrategy` enum (~line 115):

```rust
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Protocol {
    #[default]
    Anthropic,
    OpenAI,
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test protocol_ 2>&1 | tail -10
```

Expected: `test result: ok. 2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: add Protocol enum (Anthropic, OpenAI)"
```

---

### Task 2: Add `EndpointConfig` and parse `[[endpoints]]` blocks

**Files:**
- Modify: `src/main.rs` (Config struct ~line 29, new EndpointConfig struct ~near AccountConfig at line 89)

- [ ] **Step 1: Write the failing test**

In `mod tests`:

```rust
#[test]
fn endpoint_config_parses_minimal_anthropic_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[endpoints]]
name = "primary"
token = "sk-ant-test"
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.endpoints.len(), 1);
    assert_eq!(cfg.endpoints[0].name, "primary");
    assert_eq!(cfg.endpoints[0].protocol, Protocol::Anthropic);
    assert_eq!(cfg.endpoints[0].base_url, None);
    assert_eq!(cfg.endpoints[0].token, "sk-ant-test");
    assert!(cfg.endpoints[0].models.is_empty());
    assert_eq!(cfg.endpoints[0].priority, 0);
}

#[test]
fn endpoint_config_parses_openai_with_base_url() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[endpoints]]
name = "gateway"
protocol = "openai"
base_url = "https://gateway.example.com"
token = "sk-test"
priority = 100
models = ["claude-opus-*"]
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    let ep = &cfg.endpoints[0];
    assert_eq!(ep.protocol, Protocol::OpenAI);
    assert_eq!(ep.base_url.as_deref(), Some("https://gateway.example.com"));
    assert_eq!(ep.priority, 100);
    assert_eq!(ep.models, vec!["claude-opus-*".to_string()]);
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test endpoint_config_ 2>&1 | tail -10
```

Expected: `error: no field 'endpoints' on type 'Config'`.

- [ ] **Step 3: Write minimal implementation**

Add the struct near `AccountConfig` (~line 89):

```rust
#[derive(Deserialize, Clone)]
struct EndpointConfig {
    name: String,
    /// Wire format. Default: anthropic (sends to api.anthropic.com via x-api-key).
    #[serde(default)]
    protocol: Protocol,
    /// Override base URL. For protocol = anthropic, defaults to https://api.anthropic.com.
    /// For protocol = openai, this field is required (validated at startup).
    base_url: Option<String>,
    /// Auth credential. "passthrough" (anthropic only) forwards caller's auth headers.
    token: String,
    /// Model allowlist (supports "*" suffix wildcards). Empty = all models.
    #[serde(default)]
    models: Vec<String>,
    /// Priority tier (0 = highest). Lower tiers tried first.
    #[serde(default)]
    priority: u32,
}
```

Add to `Config` (line 29 area; insert before `accounts`):

```rust
/// Unified routing endpoints. Each entry is either Anthropic-native or
/// OpenAI-compatible, distinguished by `protocol`. Replaces [[accounts]]
/// + [[upstreams]] + fallback_upstream.
#[serde(default)]
endpoints: Vec<EndpointConfig>,
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test endpoint_config_ 2>&1 | tail -10
```

Expected: `test result: ok. 2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: add EndpointConfig and [[endpoints]] config parsing"
```

---

### Task 3: Reject dead keys (`[[accounts]]`, `[[upstreams]]`, `fallback_upstream`)

**Files:**
- Modify: `src/main.rs` — add validation helper near config parsing in `main()` (~line 7560), and a `mod tests` test

The Config struct still has `accounts`/`upstreams`/`fallback_upstream` fields at this point — they'll be deleted in Phase 4. We add a post-parse check that scans the raw TOML for dead keys *and* — once those struct fields are gone in Phase 4 — the same check rejects them as unknown.

- [ ] **Step 1: Write the failing test**

In `mod tests`:

```rust
#[test]
fn config_rejects_legacy_accounts_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "primary"
token = "sk-ant-test"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(err.contains("accounts"), "error must name 'accounts': {err}");
    assert!(err.contains("endpoints"), "error must mention replacement: {err}");
}

#[test]
fn config_rejects_legacy_upstreams_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[upstreams]]
name = "fallback"
base_url = "https://example.com"
api_key = "key"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(err.contains("upstreams"));
    assert!(err.contains("endpoints"));
}

#[test]
fn config_rejects_fallback_upstream_key() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"
fallback_upstream = "anything"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(err.contains("fallback_upstream"));
    assert!(err.contains("priority"));
}

#[test]
fn config_accepts_endpoints_only_schema() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"

[[endpoints]]
name = "primary"
token = "sk-ant-test"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    assert!(reject_legacy_config_keys(&value).is_ok());
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test config_rejects_ config_accepts_endpoints 2>&1 | tail -10
```

Expected: `cannot find function 'reject_legacy_config_keys' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Add this free function above `async fn main()` (~line 7530):

```rust
/// Reject removed config keys with explicit errors. Run after the raw TOML
/// has been parsed to a `toml::Value`, before strongly-typed deserialization.
///
/// `serde` silently drops unknown keys by default; this gives the operator
/// a clear migration message instead of a silent misconfiguration.
fn reject_legacy_config_keys(value: &toml::Value) -> Result<(), String> {
    let table = match value.as_table() {
        Some(t) => t,
        None => return Ok(()),
    };
    if table.contains_key("accounts") {
        return Err(
            "config: [[accounts]] is no longer supported — use [[endpoints]] (see CLAUDE.md)"
                .to_string(),
        );
    }
    if table.contains_key("upstreams") {
        return Err(
            "config: [[upstreams]] is no longer supported — use [[endpoints]] with protocol = \"openai\" (see CLAUDE.md)"
                .to_string(),
        );
    }
    if table.contains_key("fallback_upstream") {
        return Err(
            "config: fallback_upstream is no longer supported — set a high priority on the OpenAI endpoint instead (see CLAUDE.md)"
                .to_string(),
        );
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test config_rejects_ config_accepts_endpoints 2>&1 | tail -10
```

Expected: `test result: ok. 4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: reject removed config keys with explicit migration errors"
```

---

### Task 4: Wire `reject_legacy_config_keys` into `main()`

**Files:**
- Modify: `src/main.rs` `async fn main()` (~line 7533)

Find the existing block in `main()` that reads the config file and parses it to `Config`. It currently looks something like `let config: Config = toml::from_str(&raw).unwrap_or_else(...)`. Insert the rejection check between raw parse and typed parse.

- [ ] **Step 1: Locate the current parse call**

```bash
grep -n "toml::from_str\|let config: Config" src/main.rs | head -10
```

Note the line number for the config-parsing call.

- [ ] **Step 2: Modify `main()` to call the rejector before typed deserialization**

Replace the existing typed parse with a two-step parse. The pattern is:

```rust
// Parse raw value first so we can reject dead keys with clear errors
let raw_value: toml::Value = toml::from_str(&config_text)
    .unwrap_or_else(|e| panic!("config parse error: {e}"));
if let Err(msg) = reject_legacy_config_keys(&raw_value) {
    panic!("{msg}");
}
let config: Config = raw_value
    .try_into()
    .unwrap_or_else(|e| panic!("config parse error: {e}"));
```

- [ ] **Step 3: Verify it compiles and tests still pass**

```bash
cargo build 2>&1 | tail -5
cargo test --lib 2>&1 | tail -5
```

Expected: clean build, no test regressions.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: wire dead-key rejection into startup config parse"
```

---

### Task 5: Validate `base_url` and emit priority-collision warning

**Files:**
- Modify: `src/main.rs` add a free `validate_endpoints()` function and call it from `main()` post-parse

- [ ] **Step 1: Write the failing test**

In `mod tests`:

```rust
#[test]
fn validate_endpoints_warns_on_non_anthropic_host() {
    // Logs side-effect — we only check the return value (Ok if validation passes)
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://staging.anthropic.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
    }];
    // Must succeed (warn-only, not reject).
    assert!(validate_endpoints(&endpoints).is_ok());
}

#[test]
fn validate_endpoints_rejects_http_base_url() {
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("http://insecure.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(err.contains("https"), "error must mention https: {err}");
    assert!(err.contains("primary"));
}

#[test]
fn validate_endpoints_requires_base_url_for_openai() {
    let endpoints = vec![EndpointConfig {
        name: "gateway".to_string(),
        protocol: Protocol::OpenAI,
        base_url: None,
        token: "sk-test".to_string(),
        models: vec![],
        priority: 100,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(err.contains("base_url"));
    assert!(err.contains("gateway"));
}

#[test]
fn validate_endpoints_accepts_well_formed_mix() {
    let endpoints = vec![
        EndpointConfig {
            name: "primary".to_string(),
            protocol: Protocol::Anthropic,
            base_url: None,
            token: "sk-ant".to_string(),
            models: vec![],
            priority: 0,
        },
        EndpointConfig {
            name: "gateway".to_string(),
            protocol: Protocol::OpenAI,
            base_url: Some("https://gateway.example".to_string()),
            token: "sk-test".to_string(),
            models: vec![],
            priority: 100,
        },
    ];
    assert!(validate_endpoints(&endpoints).is_ok());
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test validate_endpoints_ 2>&1 | tail -10
```

Expected: `cannot find function 'validate_endpoints'`.

- [ ] **Step 3: Write minimal implementation**

Add above `async fn main()` (~line 7530):

```rust
/// Validate endpoint configuration. Returns the first hard error encountered.
/// Soft warnings (non-canonical anthropic host, priority collision with an
/// openai endpoint) are emitted as `warn!` and do not return an error.
fn validate_endpoints(endpoints: &[EndpointConfig]) -> Result<(), String> {
    for ep in endpoints {
        if let Some(url) = ep.base_url.as_deref() {
            if !url.starts_with("https://") {
                return Err(format!(
                    "endpoint '{}': base_url must start with https:// (got '{}')",
                    ep.name, url
                ));
            }
        }
        match ep.protocol {
            Protocol::OpenAI => {
                if ep.base_url.is_none() {
                    return Err(format!(
                        "endpoint '{}': base_url is required for protocol = openai",
                        ep.name
                    ));
                }
            }
            Protocol::Anthropic => {
                if let Some(url) = ep.base_url.as_deref() {
                    if !url.starts_with("https://api.anthropic.com") {
                        warn!(
                            endpoint = ep.name,
                            base_url = url,
                            "anthropic endpoint base_url is non-canonical — verify this is intentional"
                        );
                    }
                }
            }
        }
    }

    // Priority-collision warning: an OpenAI endpoint sharing the lowest tier
    // with any Anthropic endpoint recreates the bug §Problem cites.
    let lowest = endpoints.iter().map(|e| e.priority).min().unwrap_or(0);
    let openai_at_lowest: Vec<&str> = endpoints
        .iter()
        .filter(|e| e.protocol == Protocol::OpenAI && e.priority == lowest)
        .map(|e| e.name.as_str())
        .collect();
    let anthropic_at_lowest: Vec<&str> = endpoints
        .iter()
        .filter(|e| e.protocol == Protocol::Anthropic && e.priority == lowest)
        .map(|e| e.name.as_str())
        .collect();
    if !openai_at_lowest.is_empty() && !anthropic_at_lowest.is_empty() {
        warn!(
            priority = lowest,
            openai = ?openai_at_lowest,
            anthropic = ?anthropic_at_lowest,
            "openai endpoint(s) share the lowest priority tier with anthropic endpoint(s) — paid OpenAI capacity will compete with free Anthropic capacity"
        );
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test validate_endpoints_ 2>&1 | tail -10
```

Expected: `test result: ok. 4 passed`.

- [ ] **Step 5: Wire into `main()`**

After the typed-parse call in `main()`, add:

```rust
if let Err(msg) = validate_endpoints(&config.endpoints) {
    panic!("{msg}");
}
```

Verify:

```bash
cargo build 2>&1 | tail -3
cargo test --lib 2>&1 | tail -3
```

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "feat: validate endpoint base_url and warn on priority collision"
```

---

## Phase 2 — Runtime type unification (compiler-driven strangler)

This phase introduces the unified runtime `Endpoint` type and migrates each consumer one structural area at a time. Each task ends with a green `cargo test` + commit. The old `Account`/`Upstream` types stay alive until Phase 4.

### Task 6: Add runtime `Endpoint` struct alongside `Account`/`Upstream`

**Files:**
- Modify: `src/main.rs` add the struct ~near line 451 (after `struct Upstream`), and an enum alias for the routing index

- [ ] **Step 1: Add the new struct**

Insert after `struct Upstream` (~line 459, before `struct AppState`):

```rust
/// Unified routing endpoint. Replaces the separate `Account` and `Upstream`
/// runtime structs (which remain temporarily during the migration). After
/// Phase 4 cleanup, only this struct survives.
///
/// Rate-limit/utilization fields are populated only for `Protocol::Anthropic`.
/// `Protocol::OpenAI` endpoints carry a stub `RateLimitInfo` (all fields None);
/// three code sites branch on `protocol` to handle this correctly:
///   1. `routing_candidates()` — short-circuits to a fixed RoutingCandidate.
///   2. `is_emergency_brake_active()` — iterates only Anthropic endpoints.
///   3. probe loop — skips OpenAI endpoints.
struct Endpoint {
    name: String,
    protocol: Protocol,
    /// Resolved at startup: api.anthropic.com for Anthropic default, else the
    /// explicit base_url. No trailing slash.
    base_url: String,
    token: String,
    /// True iff token == "passthrough". Only meaningful for Protocol::Anthropic.
    passthrough: bool,
    models: Vec<String>,
    priority: u32,
    requests: AtomicU64,
    rate_info: RwLock<RateLimitInfo>,
    burn_rate: Mutex<BurnRate>,
    input_tokens: AtomicU64,
    output_tokens: AtomicU64,
    cache_creation_tokens: AtomicU64,
    cache_read_tokens: AtomicU64,
    last_routing_weight: AtomicU64,
    last_routing_share: AtomicU64,
    last_effective_gate: AtomicU64,
}

impl Endpoint {
    /// Check if this endpoint can serve the given model. Empty allowlist = all.
    /// Identical to the historical `Account::serves_model` predicate.
    fn serves_model(&self, model: &str) -> bool {
        if self.models.is_empty() || model.is_empty() {
            return true;
        }
        self.models.iter().any(|pattern| {
            if let Some(prefix) = pattern.strip_suffix('*') {
                model.starts_with(prefix)
            } else {
                model == pattern
            }
        })
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo build 2>&1 | tail -3
```

Expected: clean build (no warnings since the struct is unreferenced; that's fine — the next task uses it). If a `dead_code` warning fires under `-Dwarnings`, prefix with `#[allow(dead_code)]` temporarily and remove the attribute at the end of Phase 2.

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add runtime Endpoint struct (parallel to Account/Upstream)"
```

---

### Task 7: Add `endpoints: Vec<Endpoint>` to `AppState` and populate it from config

**Files:**
- Modify: `src/main.rs` `AppState` struct (~line 460), `main()` config wiring (~line 7618)

- [ ] **Step 1: Add field to `AppState`**

Insert after the `accounts` field in `AppState` (~line 463):

```rust
/// Unified endpoints. After Phase 4, this replaces `accounts` and `upstreams`.
/// During migration, both sets are populated so consumers can be migrated
/// one at a time.
endpoints: Vec<Endpoint>,
```

- [ ] **Step 2: Populate it in `main()`**

In `main()`, find the existing `let accounts: Vec<Account> = config.accounts ...` block (~line 7618). Immediately after that block (and the `let upstreams: Vec<Upstream> = ...` block ~line 7648), add:

```rust
// Build the unified endpoint vector from the new [[endpoints]] config.
// During the migration, the old accounts/upstreams Vecs remain alongside.
let endpoints: Vec<Endpoint> = config
    .endpoints
    .iter()
    .map(|ec| {
        let passthrough = ec.token == "passthrough";
        let base_url = match ec.protocol {
            Protocol::Anthropic => ec
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.anthropic.com".to_string()),
            Protocol::OpenAI => ec.base_url.clone().expect(
                "validate_endpoints should have rejected an openai endpoint without base_url",
            ),
        };
        info!(
            name = ec.name,
            protocol = ?ec.protocol,
            base_url = base_url.as_str(),
            priority = ec.priority,
            passthrough,
            models = ?ec.models,
            "loaded endpoint"
        );
        Endpoint {
            name: ec.name.clone(),
            protocol: ec.protocol,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: ec.token.clone(),
            passthrough,
            models: ec.models.clone(),
            priority: ec.priority,
            requests: AtomicU64::new(0),
            rate_info: RwLock::new(RateLimitInfo::default()),
            burn_rate: Mutex::new(BurnRate::new()),
            input_tokens: AtomicU64::new(0),
            output_tokens: AtomicU64::new(0),
            cache_creation_tokens: AtomicU64::new(0),
            cache_read_tokens: AtomicU64::new(0),
            last_routing_weight: AtomicU64::new(0),
            last_routing_share: AtomicU64::new(0),
            last_effective_gate: AtomicU64::new(0),
        }
    })
    .collect();
```

- [ ] **Step 3: Pass `endpoints` to the `AppState` constructor**

In `main()`, find the `Arc::new(AppState { ... })` block (~line 7790). Add `endpoints,` to the struct literal (placement near `accounts,` keeps related fields adjacent).

- [ ] **Step 4: Update test fixture builders**

In `mod tests`, find `test_state_with_strategy` (~line 7979). Add `endpoints: vec![],` to the `AppState { ... }` literal.

- [ ] **Step 5: Verify it compiles and tests pass**

```bash
cargo build 2>&1 | tail -3
cargo test --lib 2>&1 | tail -3
```

Expected: clean build, test suite green.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "feat: populate AppState.endpoints from [[endpoints]] config"
```

---

### Task 8: Migrate `routing_candidates` to read from `state.endpoints`

**Files:**
- Modify: `src/main.rs` `routing_candidates` (~line 1591), and the `Endpoint` routing-index enum (~line 534)

The routing enum `Endpoint { Account(usize), Upstream(usize) }` conflicts in name with the new runtime struct. **Rename the enum to `EndpointIdx`** before any consumer migrates.

- [ ] **Step 1: Rename routing enum**

Find the routing index enum (~line 534) and rename:

```rust
/// Index into one of the runtime endpoint pools. After Phase 4 cleanup this
/// collapses to a bare `usize`. Kept as an enum during the migration so
/// `Account`/`Upstream` consumers and the new `Endpoint` consumer can coexist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EndpointIdx {
    Account(usize),
    Upstream(usize),
    /// Index into AppState.endpoints (new unified pool).
    Unified(usize),
}

impl EndpointIdx {
    #[cfg(test)]
    fn account(self) -> Option<usize> {
        match self {
            EndpointIdx::Account(i) => Some(i),
            _ => None,
        }
    }
}
```

Update every reference from `Endpoint::Account`/`Endpoint::Upstream` to `EndpointIdx::Account`/`EndpointIdx::Upstream`. Use rust-analyzer or:

```bash
grep -n "Endpoint::Account\|Endpoint::Upstream\|: Endpoint\b\|Vec<Endpoint>\|&\[Endpoint\]" src/main.rs
```

Replace each occurrence (use `sed -i` with care — review the diff):

```bash
sed -i 's/Endpoint::Account/EndpointIdx::Account/g; s/Endpoint::Upstream/EndpointIdx::Upstream/g' src/main.rs
# Targeted manual edits where `Endpoint` was used as a bare type name in signatures (skip: &[Endpoint], Vec<Endpoint>, etc).
```

- [ ] **Step 2: Update the routing-index type alias in signatures**

Search for any remaining bare `Endpoint` used as a routing-index type in signatures (not the runtime struct). Specifically `pick_endpoint`, `routing_candidates`, and the `skip` parameter:

```bash
grep -n "skip: &\[Endpoint\]\|skip: Vec<Endpoint>\|-> Option<Endpoint>\|: Endpoint," src/main.rs
```

Update those signatures: `Endpoint` → `EndpointIdx`.

- [ ] **Step 3: Verify compile**

```bash
cargo build 2>&1 | tail -10
```

Expected: clean build.

- [ ] **Step 4: Migrate `routing_candidates` to also enumerate `self.endpoints`**

In `routing_candidates` (~line 1591), after the existing loop over `self.accounts.iter().enumerate()`, add a second loop:

```rust
// Unified endpoints. During the migration, both the old `accounts`+`upstreams`
// pools and the new `endpoints` pool are enumerated. The deployed config will
// populate only one side at a time — both populated is a configuration error
// caught elsewhere.
for (i, ep) in self.endpoints.iter().enumerate() {
    if skip.contains(&EndpointIdx::Unified(i)) {
        continue;
    }
    if !ep.serves_model(model) {
        continue;
    }
    match ep.protocol {
        Protocol::OpenAI => {
            // OpenAI endpoints carry no rate-limit data — push a fixed candidate
            // at the configured priority. This is one of the three named
            // `match protocol` sites (see Endpoint struct docs).
            trace!(
                endpoint = ep.name,
                priority = ep.priority,
                "pick: candidate (openai, fixed)"
            );
            candidates.push(RoutingCandidate {
                endpoint: EndpointIdx::Unified(i),
                priority: ep.priority,
                gate_5h: 0.0,
                gate_7d: 0.0,
                gate: 0.0,
                wr: 0.0,
                weight: 1.0,
                source: "openai",
            });
        }
        Protocol::Anthropic => {
            let rw = match compute_routing_weight(
                &*ep.rate_info.read().await,
                Self::now_epoch(),
                model,
            ) {
                Some(rw) => rw,
                None => {
                    trace!(
                        endpoint = ep.name,
                        model = model,
                        "pick: skipping, 7d claim rejected"
                    );
                    continue;
                }
            };
            let effective_priority = if rw.overage_active {
                ep.priority.saturating_add(self.overage_penalty)
            } else {
                ep.priority
            };
            trace!(
                endpoint = ep.name,
                gate = format!("{:.4}", rw.gate),
                weight = format!("{:.4}", rw.weight),
                priority = effective_priority,
                "pick: candidate (anthropic)"
            );
            candidates.push(RoutingCandidate {
                endpoint: EndpointIdx::Unified(i),
                priority: effective_priority,
                gate_5h: rw.gate_5h,
                gate_7d: rw.gate_7d,
                gate: rw.gate,
                wr: rw.wr,
                weight: rw.weight,
                source: rw.source,
            });
        }
    }
}
```

- [ ] **Step 5: Verify compile and tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib 2>&1 | tail -5
```

Expected: clean build, all tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "refactor: routing_candidates enumerates AppState.endpoints with protocol branch"
```

---

### Task 9: Migrate proxy_handler dispatch on `EndpointIdx::Unified`

**Files:**
- Modify: `src/main.rs` `proxy_handler` (~line 3703–4000), `try_fallback_upstream` callers

- [ ] **Step 1: Find the dispatch site**

```bash
grep -n "match state.pick_endpoint\|match pick_endpoint\|Some(Endpoint::Account\|Some(EndpointIdx::Account\|Some(EndpointIdx::Upstream\|Some(EndpointIdx::Unified" src/main.rs
```

There are two dispatch sites — one in `proxy_handler` (~line 3848) and one in `openai_chat_handler` (~line 7112).

- [ ] **Step 2: Add a `Unified` arm to `proxy_handler`'s dispatch**

In `proxy_handler` (~line 3848), the current dispatch reads:

```rust
let idx = match state.pick_endpoint(affinity, &model, &skip).await {
    Some(EndpointIdx::Account(i)) => i,
    Some(EndpointIdx::Upstream(u)) => { /* try_fallback_upstream then continue */ }
    None => { /* 429 all exhausted */ }
};
```

Add a `Unified(i)` arm before the `None`:

```rust
Some(EndpointIdx::Unified(i)) => {
    let ep = &state.endpoints[i];
    match ep.protocol {
        Protocol::Anthropic => {
            // Take the same path as the existing Account branch using `ep`
            // instead of `acct`. Detailed forward logic is shared via the
            // post-pick code block immediately after this match.
            // (Use `i` as the unified index and a helper closure or branch
            // on `Protocol::Anthropic` further down where the per-account
            // headers/url/usage tracking happens.)
            // Set a `unified_idx = Some(i);` flag and fall through to the
            // shared Anthropic forward block, OR factor a `forward_anthropic`
            // helper accepting `&Endpoint`. The implementation choice is
            // left to the engineer; both compile and test cleanly.
            unimplemented!("see Task 9 step 4")
        }
        Protocol::OpenAI => {
            match try_fallback_upstream_unified(
                &state, &body_bytes, &req_id, &client_id, &model, i, true,
            )
            .await
            {
                Some(resp) => return resp,
                None => {
                    skip.push(EndpointIdx::Unified(i));
                    continue;
                }
            }
        }
    }
}
```

This step intentionally leaves the Anthropic arm with `unimplemented!()` — step 3 factors the shared forwarding code into a function that accepts either an `&Account` or an `&Endpoint`.

- [ ] **Step 3: Factor the shared Anthropic forward path**

The existing Anthropic forward path is ~lines 3878–3975 (everything from `let acct = &state.accounts[idx];` through the response handling and `record_usage` call). Factor it into a helper:

```rust
/// Forward a single Anthropic-protocol request. Returns:
///   - Some(Response) — final caller response (success or terminal error)
///   - None — retry-eligible failure; caller should add to skip and loop
async fn forward_anthropic(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &[u8],
    base_url: &str,
    token: &str,
    passthrough: bool,
    endpoint_name: &str,
    requests_counter: &AtomicU64,
    rate_info: &RwLock<RateLimitInfo>,
    burn_rate: &Mutex<BurnRate>,
    req_id: &str,
    client_id: &str,
    model: &str,
) -> Option<Response> {
    // SOURCE: copy the inline forward block from proxy_handler at
    //   src/main.rs lines ~3878–3975 (everything from `let acct = &state.accounts[idx];`
    //   through the response handling and `record_usage` call).
    // SUBSTITUTIONS to apply while pasting:
    //   `state.upstream`        -> `base_url` parameter
    //   `acct.token`            -> `token` parameter
    //   `acct.passthrough`      -> `passthrough` parameter
    //   `acct.name`             -> `endpoint_name` parameter
    //   `acct.requests`         -> `requests_counter` parameter
    //   `acct.rate_info`        -> `rate_info` parameter
    //   `acct.burn_rate`        -> `burn_rate` parameter
    //   `state.record_usage(idx, ..)` -> leave as `// TODO(unified)` marker;
    //     Task 14 introduces a `UsageTarget` enum and re-wires this call.
    // Return type: convert the existing inline `return resp` / `continue` flow
    //   into `Some(resp)` / `None` so the caller's loop owns retry-vs-respond.
    todo!("paste lines ~3878–3975 with the substitutions in the comment above")
}
```

Important — `record_usage` currently takes `account_idx: usize`. Both the old (`state.accounts`) and new (`state.endpoints`) consumers need to call it. For now, the Account branch keeps calling `record_usage(account_idx, ...)`; the Endpoint branch will call a new `record_usage_unified(endpoint_idx, ...)` introduced in Task 14. Until then, the unified Anthropic forward path can route through either by passing the account index when it's available, or by skipping the call when only `endpoint_idx` is meaningful. **Do not run with traffic in this transitional state — Phase 2 ends with usage tracking restored in Task 14.**

For this task: factor the function with `record_usage` left as a `// TODO(unified): wire record_usage_unified in Task 14` and proceed to step 4 below. The compiler is the test.

- [ ] **Step 4: Update both call sites to use the helper**

Replace the inline Anthropic forward block in the Account arm with:

```rust
let acct = &state.accounts[idx];
match forward_anthropic(
    &state, &parts, &body_bytes,
    &state.upstream, &acct.token, acct.passthrough,
    &acct.name, &acct.requests, &acct.rate_info, &acct.burn_rate,
    &req_id, &client_id, &model,
).await {
    Some(resp) => return resp,
    None => {
        skip.push(EndpointIdx::Account(idx));
        continue;
    }
}
```

And in the unified Anthropic arm:

```rust
let ep = &state.endpoints[i];
match forward_anthropic(
    &state, &parts, &body_bytes,
    &ep.base_url, &ep.token, ep.passthrough,
    &ep.name, &ep.requests, &ep.rate_info, &ep.burn_rate,
    &req_id, &client_id, &model,
).await {
    Some(resp) => return resp,
    None => {
        skip.push(EndpointIdx::Unified(i));
        continue;
    }
}
```

- [ ] **Step 5: Verify compile and tests**

```bash
cargo build 2>&1 | tail -5
cargo test --lib 2>&1 | tail -5
```

Expected: clean build; tests using the legacy `state.accounts` path still pass (no test currently routes through `state.endpoints`).

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "refactor: factor Anthropic forward path; add Unified arm in proxy_handler"
```

---

### Task 10: Add `try_fallback_upstream_unified` for `EndpointIdx::Unified` OpenAI endpoints

**Files:**
- Modify: `src/main.rs` near `try_fallback_upstream` (~line 4181)

- [ ] **Step 1: Add the helper**

Insert immediately after `try_fallback_upstream`:

```rust
/// Forward to a Protocol::OpenAI endpoint in the unified pool. Identical
/// behavior to `try_fallback_upstream`, but reads from `state.endpoints`
/// instead of `state.upstreams`.
///
/// IMPORTANT: Returns `None` on upstream 429/5xx so the retry loop can
/// add the endpoint to `skip` and try the next candidate. The original
/// `try_fallback_upstream` returns the error response directly; the
/// unified version corrects that to match Anthropic-endpoint failure
/// semantics (see spec §Routing & dispatch).
async fn try_fallback_upstream_unified(
    state: &AppState,
    body_bytes: &[u8],
    req_id: &str,
    client_id: &str,
    model: &str,
    endpoint_idx: usize,
    translate: bool,
) -> Option<Response> {
    let ep = &state.endpoints[endpoint_idx];
    // Copy the body of `try_fallback_upstream` here, substituting:
    //   `state.upstreams[upstream_idx]` -> `state.endpoints[endpoint_idx]`
    //   `upstream.base_url` -> `ep.base_url`
    //   `upstream.api_key` -> `ep.token`
    //   `upstream.name`    -> `ep.name`
    //   `upstream.requests` -> `ep.requests`
    // Then change the error-response branch (current: `Some(resp_anthropic_error)`)
    // to return None for status 429 and 5xx (>=500), so the caller can retry.
    // For 4xx other than 429, keep the existing "return error to caller"
    // behavior (those are client errors, not retry-eligible).
    //
    // The 429/5xx -> None branch is the spec's required behavior change.
    // SOURCE: copy the body of `try_fallback_upstream` at src/main.rs lines
    //   ~4181–4426 with the substitutions listed above and the 429/5xx -> None
    //   change applied at the !status.is_success() branch.
    todo!("paste lines ~4181–4426 with substitutions + 429/5xx -> None")
}
```

Concretely the retry decision at the end of the existing function reads:

```rust
let status = resp.status();
if !status.is_success() {
    // existing: returns Some(error_response)
    // unified change:
    if status.as_u16() == 429 || status.is_server_error() {
        warn!(
            req_id, endpoint = ep.name,
            status = status.as_u16(),
            "fallback: upstream returned retry-eligible error, advancing"
        );
        return None;
    }
    // 4xx client errors: still return to caller — retry would not help.
    // ... existing body-to-Anthropic-error-shape translation ...
}
```

- [ ] **Step 2: Verify compile**

```bash
cargo build 2>&1 | tail -3
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add try_fallback_upstream_unified with 429/5xx-as-retryable"
```

---

### Task 11: Add a unified-endpoint dispatch arm to `openai_chat_handler`

> **Revised 2026-05-22.** An earlier version of this task filtered
> `openai_chat_handler` to OpenAI endpoints only, on the false belief that the
> handler reached only upstreams today. It does not — `openai_chat_handler`
> already translates OpenAI→Anthropic and routes to Anthropic accounts. That
> behavior is preserved. This task ADDS a `Unified` dispatch arm; it does not
> filter anything out.

**Files:**
- Modify: `src/main.rs` `openai_chat_handler` (locate via `grep -n "async fn openai_chat_handler"`).

**Context.** `openai_chat_handler` has a retry loop with a `match state.pick_endpoint(...)` dispatch. Today it has three arms:
- `EndpointIdx::Account(i)` — translates the request to Anthropic, forwards to `state.upstream`, translates the response back to OpenAI. This is a large inline block.
- `EndpointIdx::Upstream(u)` — calls `try_fallback_upstream(.., translate=false)` (forwards the original OpenAI body directly).
- `EndpointIdx::Unified(_) => unreachable!(...)` — the placeholder added in Phase 2a.

You will replace the `unreachable!()` with a real arm that branches on `state.endpoints[i].protocol`:
- `Protocol::OpenAI` → forward direct, no translation (same as the `Upstream` arm).
- `Protocol::Anthropic` → translate + forward + translate-back (same as the `Account` arm).

- [ ] **Step 1: Factor the openai-compat Anthropic forward path**

The `EndpointIdx::Account(i)` arm contains a large inline block: build the Anthropic-translated request, forward to the Anthropic API, handle 429/529/5xx, parse rate-limit headers, `record_usage`, and translate the response (streaming + non-streaming) back to OpenAI.

Factor this block into a helper — call it `forward_openai_compat_anthropic` — parameterised the same way `forward_anthropic` was in Task 9: it takes `base_url`, `token`, `passthrough`, `endpoint_name`, `usage_target: UsageTarget`, `requests_counter: &AtomicU64`, `rate_info: &RwLock<RateLimitInfo>`, `burn_rate: &Mutex<BurnRate>`, plus the pre-translated `anthropic_body` / `oauth_anthropic_body`, the request context (`req_id`, `client_id`, etc.), and whatever else the inline block reads. Return a `ForwardOutcome` (the enum from Task 9) so the retry loop owns `skip`/`saw_529`/retry exactly as it does for `forward_anthropic`.

This mirrors Task 9 precisely. If `forward_anthropic` and `forward_openai_compat_anthropic` end up sharing significant structure, that is acceptable — do NOT try to merge them; Phase 4 collapses pool-specific code. Copy-and-adapt is fine here.

- [ ] **Step 2: Replace the dispatch match**

```rust
let picked = match state.pick_endpoint(affinity, &model, &skip).await {
    Some(e) => e,
    None => {
        warn!("all endpoints exhausted");
        return (StatusCode::TOO_MANY_REQUESTS, "all upstream endpoints exhausted")
            .into_response();
    }
};
match picked {
    EndpointIdx::Account(i) => {
        let acct = &state.accounts[i];
        match forward_openai_compat_anthropic(
            &state, &parts, &anthropic_body, &oauth_anthropic_body,
            &state.upstream, &acct.token, acct.passthrough, &acct.name,
            UsageTarget::Account(i), &acct.requests, &acct.rate_info, &acct.burn_rate,
            &req_id, &client_id, /* ...remaining ctx args... */ &model,
        ).await {
            ForwardOutcome::Done(resp) => return resp,
            ForwardOutcome::Retry { saw_529: s, push_skip } => {
                if s { saw_529 = true; }
                if push_skip { skip.push(EndpointIdx::Account(i)); }
                continue;
            }
        }
    }
    EndpointIdx::Upstream(u) => {
        match try_fallback_upstream(&state, &body_bytes, &req_id, &client_id, &model, u, false).await {
            Some(resp) => return resp,
            None => { skip.push(EndpointIdx::Upstream(u)); continue; }
        }
    }
    EndpointIdx::Unified(i) => {
        let ep = &state.endpoints[i];
        match ep.protocol {
            Protocol::Anthropic => {
                match forward_openai_compat_anthropic(
                    &state, &parts, &anthropic_body, &oauth_anthropic_body,
                    &ep.base_url, &ep.token, ep.passthrough, &ep.name,
                    UsageTarget::Unified(i), &ep.requests, &ep.rate_info, &ep.burn_rate,
                    &req_id, &client_id, /* ...remaining ctx args... */ &model,
                ).await {
                    ForwardOutcome::Done(resp) => return resp,
                    ForwardOutcome::Retry { saw_529: s, push_skip } => {
                        if s { saw_529 = true; }
                        if push_skip { skip.push(EndpointIdx::Unified(i)); }
                        continue;
                    }
                }
            }
            Protocol::OpenAI => {
                match try_fallback_upstream_unified(
                    &state, &body_bytes, &req_id, &client_id, &model, i, false,
                ).await {
                    Some(resp) => return resp,
                    None => { skip.push(EndpointIdx::Unified(i)); continue; }
                }
            }
        }
    }
}
```

The exact parameter lists must match whatever `forward_openai_compat_anthropic` ends up needing — adapt. The point: Account and Unified-Anthropic both call the helper; Upstream and Unified-OpenAI both forward direct.

- [ ] **Step 3: Keep the eager translation**

`openai_chat_handler` translates `openai_body` → `anthropic_body` (and the OAuth variant) before the retry loop. **Keep that** — both the Account arm and the new Unified-Anthropic arm consume `anthropic_body`. (The earlier plan said to delete it; that was tied to the now-reverted "OpenAI-only" filtering. The OpenAI-protocol arms simply ignore `anthropic_body` and forward `body_bytes` — a small unused-translation cost on the all-OpenAI-endpoints path, acceptable and unchanged from today's behavior where the translation also always runs.)

- [ ] **Step 4: Verify compile and tests**

```bash
cargo build 2>&1 | tail -5
cargo test --bin anthropic-lb 2>&1 | tail -8
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets 2>&1 | tail -3
```

Expected: clean build, all existing tests pass (the ~10 `test_openai_app` tests route through the `Account` arm, which still calls the same logic — now via `forward_openai_compat_anthropic`).

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "refactor: factor openai-compat Anthropic forward; openai_chat_handler dispatches unified endpoints"
```

---

### Task 12: Migrate emergency brake — skip `Protocol::OpenAI` endpoints

**Files:**
- Modify: `src/main.rs` `is_emergency_brake_active` (~line 3346)

- [ ] **Step 1: Write the failing test**

In `mod tests`:

```rust
#[tokio::test]
async fn emergency_brake_fires_when_only_anthropic_above_threshold_with_openai_present() {
    // Setup: 1 anthropic at utilization 0.95, 1 openai with stub rate info.
    // Pre-fix: brake never fires because openai stub yields (0.5, "unknown").
    // Post-fix: brake fires because openai is skipped from the iteration.
    let acct = make_account("anthropic", "sk-ant");
    {
        let mut info = acct.rate_info.write().await;
        info.utilization = Some(0.95);
        info.utilization_5h = Some(0.95);
    }
    let mut state = test_state_with(vec![acct]);
    let st = Arc::get_mut(&mut state).expect("uniquely owned");
    st.endpoints.push(Endpoint {
        name: "openai".to_string(),
        protocol: Protocol::OpenAI,
        base_url: "https://gateway.example".to_string(),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 100,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    st.emergency_threshold = 0.88;
    assert!(state.is_emergency_brake_active().await,
        "brake must fire: anthropic is above threshold; openai must not vote");
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test emergency_brake_fires_when_only_anthropic 2>&1 | tail -15
```

Expected: assertion fails (brake does not fire because OpenAI endpoint's stub voted against it).

- [ ] **Step 3: Modify `is_emergency_brake_active`**

Replace the body (~line 3346–3368) with:

```rust
async fn is_emergency_brake_active(&self) -> bool {
    if !self.emergency_brake {
        return false;
    }
    let now_epoch = Self::now_epoch();
    let mut all_above = true;
    let mut any_known = false;

    // Iterate legacy Account pool — Anthropic by definition.
    for acct in &self.accounts {
        let info = acct.rate_info.read().await;
        let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
        if source != "unknown" {
            any_known = true;
        }
        if util < self.emergency_threshold {
            all_above = false;
            break;
        }
    }

    // Iterate unified endpoints, but ONLY Protocol::Anthropic.
    // OpenAI endpoints have no rate-limit data and must not vote against firing
    // the brake — their stub RateLimitInfo would otherwise resolve to
    // (0.5, "unknown"), forcing all_above = false. This is one of the three
    // named `match protocol` sites (see Endpoint struct docs).
    if all_above {
        for ep in &self.endpoints {
            if ep.protocol != Protocol::Anthropic {
                continue;
            }
            let info = ep.rate_info.read().await;
            let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
            if source != "unknown" {
                any_known = true;
            }
            if util < self.emergency_threshold {
                all_above = false;
                break;
            }
        }
    }

    all_above && any_known
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test emergency_brake_ 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "fix: emergency brake skips Protocol::OpenAI endpoints"
```

---

### Task 13: Migrate probe loop — skip `Protocol::OpenAI` endpoints

**Files:**
- Modify: `src/main.rs` `probe_account` (~line 783) and `main()` probe-loop spawn (~line 7851)

- [ ] **Step 1: Find the probe loop**

```bash
grep -n "tokio::spawn.*async move\|probe_account\|PROBE_MODELS\|n_accounts = " src/main.rs
```

The probe loop is in `main()` (~line 7851) and iterates by account index calling `probe_account`. We need an analogous loop for unified endpoints.

- [ ] **Step 2: Write the failing test**

This is harder to unit-test cleanly because the probe loop spawns. Add a smaller test that verifies a probe-style helper skips OpenAI endpoints:

```rust
#[tokio::test]
async fn probe_endpoint_unified_skips_openai() {
    let ep = Endpoint {
        name: "openai".to_string(),
        protocol: Protocol::OpenAI,
        base_url: "http://127.0.0.1:1".to_string(),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 100,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    };
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);
    // probe_endpoint_unified must early-return without HTTP. We assert it
    // does not panic, does not increment requests, and completes in <100ms
    // (since base_url 127.0.0.1:1 would hang/connect-refuse otherwise).
    let start = std::time::Instant::now();
    state.probe_endpoint_unified(0, "claude-haiku-4-5").await;
    assert!(start.elapsed() < Duration::from_millis(100),
        "probe must short-circuit for OpenAI endpoints");
    assert_eq!(state.endpoints[0].requests.load(Ordering::Relaxed), 0);
}
```

- [ ] **Step 3: Add the unified probe method**

Near `probe_account` (~line 783), add:

```rust
/// Probe a unified-pool endpoint by index. Skips Protocol::OpenAI endpoints —
/// they have no rate-limit headers to refresh. This is one of the three
/// named `match protocol` sites (see Endpoint struct docs).
async fn probe_endpoint_unified(&self, idx: usize, model: &str) {
    let ep = &self.endpoints[idx];
    if ep.protocol == Protocol::OpenAI {
        debug!(endpoint = ep.name, "skipping probe for openai endpoint");
        return;
    }
    if ep.passthrough {
        debug!(endpoint = ep.name, "skipping probe for passthrough endpoint");
        return;
    }
    // SOURCE: copy the body of `probe_account` at src/main.rs lines ~783–~980
    //   with these substitutions:
    //     `&self.accounts[idx]`  -> `&self.endpoints[idx]`
    //     `acct.passthrough`     -> (already checked above; remove duplicate)
    //     `acct.token`           -> `ep.token`
    //     `acct.name`            -> `ep.name`
    //     `acct.rate_info`       -> `ep.rate_info`
    //     `state.upstream`       -> `ep.base_url`
    todo!("paste lines ~783–~980 with the substitutions in the comment above")
}
```

- [ ] **Step 4: Add a probe-loop branch for endpoints**

In `main()`, after the existing `let n_accounts = probe_state.accounts.len();` probe loop, add a parallel loop:

```rust
let n_endpoints = probe_state.endpoints.len();
if n_endpoints > 0 {
    let probe_state = state.clone();
    tokio::spawn(async move {
        const PROBE_MODELS: &[&str] =
            &["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-6"];
        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut model_idx = 0;
        loop {
            for i in 0..n_endpoints {
                let model = PROBE_MODELS[model_idx % PROBE_MODELS.len()];
                probe_state.probe_endpoint_unified(i, model).await;
            }
            model_idx += 1;
            tokio::time::sleep(Duration::from_secs(probe_interval)).await;
        }
    });
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cargo test probe_endpoint_unified_skips_openai 2>&1 | tail -10
```

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "fix: probe loop skips Protocol::OpenAI endpoints"
```

---

### Task 14: Add `record_usage_unified`, finalize Anthropic forward path

**Files:**
- Modify: `src/main.rs` `record_usage` (~line 3145), `forward_anthropic` from Task 9

- [ ] **Step 1: Add the unified record helper**

Above `record_usage` (~line 3145):

```rust
/// Per-endpoint usage tracking (unified pool). Mirrors `record_usage` exactly,
/// but indexes into `state.endpoints` instead of `state.accounts`.
///
/// After Phase 4 cleanup, `record_usage` is removed and only this version
/// survives.
async fn record_usage_unified(
    &self,
    endpoint_idx: usize,
    client_id: &str,
    usage: &TokenUsage,
) {
    let ep = &self.endpoints[endpoint_idx];
    // SOURCE: copy the body of `record_usage` at src/main.rs lines ~3145–~3340
    //   with these substitutions:
    //     `&self.accounts[account_idx]` -> `&self.endpoints[endpoint_idx]`
    //     `acct.input_tokens`           -> `ep.input_tokens`
    //     `acct.output_tokens`          -> `ep.output_tokens`
    //     `acct.cache_creation_tokens`  -> `ep.cache_creation_tokens`
    //     `acct.cache_read_tokens`      -> `ep.cache_read_tokens`
    //     `acct.name`                   -> `ep.name`
    //     (Redis key paths keyed by ep.name remain unchanged — Phase 2 Task 18.)
    todo!("paste lines ~3145–~3340 with the substitutions in the comment above")
}
```

- [ ] **Step 2: Wire it into `forward_anthropic`**

In `forward_anthropic`, the `record_usage` call (which had the `TODO(unified)` marker from Task 9) needs both branches. Accept a `usage_target: UsageTarget` enum:

```rust
enum UsageTarget {
    LegacyAccount(usize),
    Unified(usize),
}

// In forward_anthropic signature: add `usage_target: UsageTarget` and
// dispatch:
match usage_target {
    UsageTarget::LegacyAccount(i) => state.record_usage(i, client_id, &usage).await,
    UsageTarget::Unified(i) => state.record_usage_unified(i, client_id, &usage).await,
}
```

Update both call sites in `proxy_handler` to pass `UsageTarget::LegacyAccount(idx)` and `UsageTarget::Unified(i)` respectively.

- [ ] **Step 3: Verify compile and tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib 2>&1 | tail -5
```

Expected: clean build, all tests pass. The unified path is now fully functional including usage tracking.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: record_usage_unified + UsageTarget; finalize Anthropic forward path"
```

---

### Task 15: Migrate persistence — `PersistedEndpoint`, top-level `endpoints` key

**Files:**
- Modify: `src/main.rs` `PersistedState`/`PersistedAccount` (~line 615), `save_state`/`load_state` (~lines 724, 985)

- [ ] **Step 1: Write the failing test**

In `mod tests`:

```rust
#[tokio::test]
async fn load_state_warns_and_starts_clean_on_legacy_accounts_key() {
    // Pre: state file uses `accounts` (legacy) top-level key.
    // Expect: load_state returns without populating, no panic.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), r#"{"accounts":[{"name":"primary","requests_total":42}],"saved_at":0}"#).unwrap();
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().state_path = tmp.path().to_path_buf();
    Arc::get_mut(&mut state).unwrap().endpoints.push(Endpoint {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: "https://api.anthropic.com".to_string(),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    state.load_state().await;  // must not panic
    // Legacy `accounts` key must NOT have been used to populate endpoints
    assert_eq!(state.endpoints[0].requests.load(Ordering::Relaxed), 0,
        "legacy accounts-keyed state file must not deserialize into endpoints");
}
```

- [ ] **Step 2: Rename `PersistedAccount` → `PersistedEndpoint` and the top-level key**

Replace `struct PersistedState` and `struct PersistedAccount` (~lines 615–660):

```rust
#[derive(Serialize, Deserialize)]
struct PersistedState {
    /// Per-endpoint persisted runtime state. Replaces the legacy `accounts` key.
    endpoints: Vec<PersistedEndpoint>,
    #[serde(default)]
    saved_at: u64,
}

#[derive(Serialize, Deserialize)]
struct PersistedEndpoint {
    name: String,
    requests_total: u64,
    utilization: Option<f64>,
    #[serde(default)]
    utilization_7d: Option<f64>,
    #[serde(default)]
    utilization_5h: Option<f64>,
    representative_claim: Option<String>,
    #[serde(default)]
    reset_5h: Option<u64>,
    #[serde(default)]
    reset_7d: Option<u64>,
    #[serde(default)]
    status_5h: Option<String>,
    #[serde(default)]
    status_7d: Option<String>,
    #[serde(default)]
    claims_7d: HashMap<String, ClaimWindowData>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    #[serde(default)]
    overage_in_use: bool,
    #[serde(default)]
    overage_status: Option<String>,
    #[serde(default)]
    overage_utilization: Option<f64>,
    #[serde(default)]
    overage_reset: Option<u64>,
    hard_limited_until_epoch: Option<u64>,
    #[serde(default)]
    last_updated_epoch: Option<u64>,
}
```

- [ ] **Step 3: Update `save_state` to write `endpoints` from `self.endpoints`**

In `save_state` (~line 724), replace the loop over `self.accounts` with a loop over `self.endpoints`. During the migration both pools exist; **the unified pool is now the canonical state**. The legacy `self.accounts` loop is removed from `save_state` entirely (any production rollout will have already populated `self.endpoints` from `[[endpoints]]` config).

- [ ] **Step 4: Update `load_state` to read `endpoints`**

In `load_state` (~line 985), replace `for pa in &persisted.accounts` with `for pe in &persisted.endpoints` and `self.accounts.iter().find` with `self.endpoints.iter().find`.

Add an explicit warn for legacy-format state:

```rust
let persisted: PersistedState = match serde_json::from_str(&data) {
    Ok(s) => s,
    Err(e) => {
        warn!(error = %e,
            "failed to parse persisted state — possible legacy format ('accounts' top-level key was removed); starting fresh"
        );
        return;
    }
};
```

Because `PersistedState` no longer has an `accounts` field, the legacy file fails to deserialize (no `endpoints` key present), triggering the warn-and-return branch.

- [ ] **Step 5: Run test to verify it passes**

```bash
cargo test load_state_warns_and_starts_clean 2>&1 | tail -10
cargo test --lib 2>&1 | tail -5
```

Expected: new test passes; existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "refactor: persist as PersistedEndpoint under 'endpoints' top-level key"
```

---

### Task 16: Migrate metrics emit (`refresh_metrics_weights`, prometheus output)

**Files:**
- Modify: `src/main.rs` `refresh_metrics_weights` (~line 1727), `metrics_handler` Prometheus block (~lines 4943–5260)

- [ ] **Step 1: Migrate `refresh_metrics_weights`**

Currently iterates `self.accounts`. Add a second pass that iterates `self.endpoints` and updates the same gauges on the unified runtime fields (`last_routing_weight`, `last_routing_share`, `last_effective_gate`).

```bash
grep -n "fn refresh_metrics_weights" src/main.rs
```

Read the function body and add a parallel `for ep in &self.endpoints` block at the end that performs the same per-endpoint computation but on `Endpoint` fields.

- [ ] **Step 2: Migrate Prometheus emission**

Search for `anthropic_account_` references in `metrics_handler`:

```bash
grep -n "anthropic_account_" src/main.rs
```

Per spec, **metric names are left unchanged** (`anthropic_account_*`). The metrics emit loop already iterates `self.accounts`. Add a second iteration over `self.endpoints` that emits the same metrics with the endpoint's `name` as the `name` label. Both pools share the same metric names — the labels differ only by which name each entry has, and there are no collisions in production (one pool is populated at a time after Phase 5).

- [ ] **Step 3: Verify compile and tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib metrics 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "refactor: metrics_handler and refresh_metrics_weights enumerate AppState.endpoints"
```

---

### Task 17: Migrate `stats_handler` JSON output

**Files:**
- Modify: `src/main.rs` `stats_handler` (~look for `fn stats_handler` and the response builder block)

- [ ] **Step 1: Find stats_handler**

```bash
grep -n "fn stats_handler\|\"accounts\":" src/main.rs | head -10
```

- [ ] **Step 2: Add an `endpoints` array to the JSON response**

The JSON currently has an `accounts` array. Add a parallel `endpoints` array sourced from `self.endpoints` with the same field shape (`name`, `requests`, `passthrough`, utilization fields, priority, etc.). Add a `protocol` field to each entry.

- [ ] **Step 3: Verify compile and tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib stats 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: stats_handler exposes 'endpoints' array alongside 'accounts'"
```

---

### Task 18: Migrate Redis sync (rate-info, hard-limit, heartbeat)

**Files:**
- Modify: `src/main.rs` Redis sync functions (search for `sync_from_redis`, `redis_set_hard_limit`, `redis_heartbeat`)

- [ ] **Step 1: Find Redis call sites**

```bash
grep -n "alb:rate:\|alb:hard:\|alb:heartbeat:\|sync_from_redis" src/main.rs | head -20
```

- [ ] **Step 2: Mirror each sync function to enumerate `self.endpoints`**

For each function that currently iterates `self.accounts`, add a parallel iteration over `self.endpoints` using the endpoint's `name` as the Redis key (matches the existing key format `alb:rate:<name>`, etc.). Skip `Protocol::OpenAI` endpoints — they have no rate-limit data to sync.

- [ ] **Step 3: Verify compile and tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib redis 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "refactor: Redis sync paths enumerate AppState.endpoints (Anthropic only)"
```

---

## Phase 3 — Behavior corrections (TDD)

### Task 19: New test — OpenAI endpoint participates in priority routing

**Files:**
- Modify: `src/main.rs` `mod tests`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn openai_endpoint_participates_at_configured_priority() {
    let acct = make_account("anthropic", "sk-ant");
    {
        let mut info = acct.rate_info.write().await;
        info.utilization = Some(0.0); // healthy
    }
    let mut state = test_state_with(vec![acct]);
    let st = Arc::get_mut(&mut state).unwrap();
    st.endpoints.push(Endpoint {
        name: "openai".to_string(),
        protocol: Protocol::OpenAI,
        base_url: "https://gw.example".to_string(),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 100,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    let candidates = state.routing_candidates("claude-opus-4-7", &[]).await;
    let openai_candidate = candidates.iter().find(|c| matches!(c.endpoint, EndpointIdx::Unified(_)));
    assert!(openai_candidate.is_some(), "openai endpoint must be a candidate");
    let c = openai_candidate.unwrap();
    assert_eq!(c.priority, 100);
    assert_eq!(c.weight, 1.0);
    assert_eq!(c.gate, 0.0);
}
```

- [ ] **Step 2: Run — expect pass (the routing_candidates change in Task 8 already covers this)**

```bash
cargo test openai_endpoint_participates 2>&1 | tail -5
```

Expected: pass (this test is verification of Task 8's behavior).

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "test: openai endpoint participates in priority routing at configured tier"
```

---

### Task 20: New test — model allowlist on OpenAI endpoint filters correctly

**Files:**
- Modify: `src/main.rs` `mod tests`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn openai_endpoint_with_opus_only_allowlist_excludes_sonnet() {
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(Endpoint {
        name: "opus-gw".to_string(),
        protocol: Protocol::OpenAI,
        base_url: "https://gw.example".to_string(),
        token: "sk".to_string(),
        passthrough: false,
        models: vec!["claude-opus-*".to_string()],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    let cs_opus = state.routing_candidates("claude-opus-4-7", &[]).await;
    let cs_sonnet = state.routing_candidates("claude-sonnet-4-6", &[]).await;
    assert_eq!(cs_opus.len(), 1, "opus must hit the opus-only endpoint");
    assert_eq!(cs_sonnet.len(), 0, "sonnet must be filtered out");
}
```

- [ ] **Step 2: Run — expect pass**

```bash
cargo test openai_endpoint_with_opus_only 2>&1 | tail -5
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "test: model allowlist filters openai endpoints by model match"
```

---

### Task 21: New test — `forward_translated()` returns `None` on upstream 429/5xx

**Files:**
- Modify: `src/main.rs` `mod tests` (integration-style with a mock upstream)

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn try_fallback_upstream_unified_returns_none_on_429() {
    // Mock upstream returns 429
    let app = Router::new().fallback(any(|| async { (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response() }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(Endpoint {
        name: "rl-gw".to_string(),
        protocol: Protocol::OpenAI,
        base_url: format!("http://{}", addr),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    let body = br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#;
    let result = try_fallback_upstream_unified(
        &state, body, "req-1", "client-1", "claude-opus-4-7", 0, false,
    ).await;
    assert!(result.is_none(), "429 must return None for retry, not Response");
}

#[tokio::test]
async fn try_fallback_upstream_unified_returns_none_on_500() {
    let app = Router::new().fallback(any(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response() }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(Endpoint {
        name: "broken".to_string(),
        protocol: Protocol::OpenAI,
        base_url: format!("http://{}", addr),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    let body = br#"{"model":"claude-opus-4-7","messages":[],"max_tokens":1}"#;
    let result = try_fallback_upstream_unified(
        &state, body, "req-1", "client-1", "claude-opus-4-7", 0, false,
    ).await;
    assert!(result.is_none(), "500 must return None for retry");
}
```

- [ ] **Step 2: Run — expect pass (Task 10 already implements this behavior)**

```bash
cargo test try_fallback_upstream_unified 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "test: try_fallback_upstream_unified returns None on 429/5xx for retry"
```

---

### Task 22: New test — `openai_chat_handler` routes to a unified Anthropic endpoint

> **Revised 2026-05-22.** Was "openai_chat_handler routes only to OpenAI
> endpoints" — reversed along with Task 11. This test now proves the OPPOSITE:
> an OpenAI-format request is correctly served by a unified `Protocol::Anthropic`
> endpoint via the OpenAI→Anthropic→OpenAI round-trip translation.

**Files:**
- Modify: `src/main.rs` `mod tests`

- [ ] **Step 1: Write the test**

Use a mock upstream that returns a canned Anthropic `messages` response. Build an `AppState` whose `endpoints` pool contains one `Protocol::Anthropic` endpoint pointed at the mock (and empty `accounts`/`upstreams`). POST an OpenAI-format `/v1/chat/completions` request; assert a 200 whose body is OpenAI-shaped (`choices[0].message.content` present), proving the request was translated to Anthropic, forwarded to the unified endpoint, and the response translated back.

```rust
#[tokio::test]
async fn openai_chat_handler_routes_to_unified_anthropic_endpoint() {
    // Mock Anthropic upstream: returns a minimal messages response.
    let mock = Router::new().fallback(any(|| async {
        axum::Json(serde_json::json!({
            "id": "msg_1", "type": "message", "role": "assistant",
            "model": "claude-opus-4-7",
            "content": [{"type": "text", "text": "hi back"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 2}
        })).into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, mock).await.unwrap(); });

    let mut state = test_state_with(vec![]); // no legacy accounts
    Arc::get_mut(&mut state).unwrap().endpoints.push(Endpoint {
        name: "unified-anthropic".to_string(),
        protocol: Protocol::Anthropic,
        base_url: format!("http://{}", mock_addr),
        token: "sk-ant-api-test".to_string(),
        passthrough: false,
        models: vec![],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });
    let app = Router::new()
        .route("/v1/chat/completions", axum::routing::post(openai_chat_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

    let resp = reqwest::Client::new()
        .post(format!("http://{}/v1/chat/completions", addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["choices"][0]["message"]["content"].is_string(),
        "response must be OpenAI-shaped after round-trip translation");
}
```

Adapt the `Endpoint { .. }` field list and the mock response shape to whatever the actual structs require. If the codebase has a `make_endpoint` test helper by this point (added in Phase 4 Task 25 — may not exist yet), use it; otherwise hand-construct.

- [ ] **Step 2: Run — expect pass (Task 11 wired this path)**

```bash
cargo test --bin anthropic-lb openai_chat_handler_routes_to_unified_anthropic 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "test: openai_chat_handler routes to a unified Anthropic endpoint with translation"
```

---

### Task 23: New test — `proxy_handler` translates to OpenAI endpoint

**Files:**
- Modify: `src/main.rs` `mod tests`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn proxy_handler_translates_to_openai_endpoint() {
    // Mock OpenAI upstream: assert it received an OpenAI-format request
    // (contains `messages` array but no Anthropic-only fields like `system` as array).
    let (tx, mut rx) = tokio::sync::mpsc::channel::<serde_json::Value>(1);
    let app = Router::new().fallback(any(move |req: Request<Body>| {
        let tx = tx.clone();
        async move {
            let bytes = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let _ = tx.send(v).await;
            (StatusCode::OK, axum::Json(serde_json::json!({
                "id": "chatcmpl-x",
                "object": "chat.completion",
                "model": "claude-opus-4-7",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "hi back"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
            }))).into_response()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(Endpoint {
        name: "openai-gw".to_string(),
        protocol: Protocol::OpenAI,
        base_url: format!("http://{}", upstream_addr),
        token: "sk".to_string(),
        passthrough: false,
        models: vec![],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    });

    let proxy_app = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, proxy_app).await.unwrap(); });

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://{}/v1/messages", proxy_addr))
        .json(&serde_json::json!({
            "model": "claude-opus-4-7",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        }))
        .send()
        .await
        .unwrap();

    let received = rx.recv().await.expect("upstream must receive a request");
    assert!(received.get("messages").is_some(),
        "translated request must have OpenAI `messages` field");
    assert_eq!(received["model"], "claude-opus-4-7");
}
```

- [ ] **Step 2: Run — expect pass**

```bash
cargo test proxy_handler_translates_to_openai 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "test: proxy_handler translates Anthropic request to OpenAI endpoint"
```

---

## Phase 4 — Cleanup

### Task 24: Remove `upstream_handler` and `/upstream/{name}/*` route

**Files:**
- Modify: `src/main.rs` (`upstream_handler` ~line 4430, router definition ~line 7830)

- [ ] **Step 1: Delete the route registration**

In `main()` (~line 7830), remove the line:

```rust
.route("/upstream/{name}/{*rest}", any(upstream_handler))
```

- [ ] **Step 2: Delete the handler function**

Delete `async fn upstream_handler(...) { ... }` entirely (~line 4430 through end of function).

- [ ] **Step 3: Delete its tests**

```bash
grep -n "upstream_handler_forwards_to_named\|upstream_handler_rejects_unknown" src/main.rs
```

Delete the two test functions found.

- [ ] **Step 4: Verify compile and tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib 2>&1 | tail -5
```

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "refactor: remove /upstream/{name}/* passthrough route and handler"
```

---

### Task 25: Remove legacy `accounts`, `upstreams`, `fallback_upstream`, `upstream` from runtime

**Files:**
- Modify: `src/main.rs` `AppState`, `Config`, all remaining `state.accounts`/`state.upstreams`/`state.fallback_upstream`/`state.upstream` references

- [ ] **Step 1: Find every remaining reference**

```bash
grep -nE "state\.(accounts|upstreams|fallback_upstream|upstream)\b|self\.(accounts|upstreams|fallback_upstream|upstream)\b|config\.(accounts|upstreams|fallback_upstream|upstream)\b" src/main.rs
```

For each: replace with the equivalent endpoints-based access. Most are mechanical (`self.accounts.iter()` → no longer needed; was already migrated to `self.endpoints` in Phase 2).

- [ ] **Step 2: Delete fields from `AppState`**

In `struct AppState`:
- Delete `accounts: Vec<Account>,`
- Delete `upstreams: Vec<Upstream>,`
- Delete `fallback_upstream: Option<usize>,`
- Delete `upstream: String,`

- [ ] **Step 3: Delete fields from `Config`**

In `struct Config`:
- Delete `accounts: Vec<AccountConfig>,`
- Delete `#[serde(default)] upstreams: Vec<UpstreamConfig>,`
- Delete `fallback_upstream: Option<String>,`
- Delete `upstream: String,`

- [ ] **Step 4: Delete dead struct definitions**

- Delete `struct AccountConfig` (~line 89)
- Delete `struct UpstreamConfig` (~line 104)
- Delete `struct Account` (~line 421)
- Delete `struct Upstream` (~line 451)

- [ ] **Step 5: Collapse `EndpointIdx` to bare `usize`**

The enum now has only one variant in use (`Unified(usize)`). Replace `EndpointIdx` with a type alias or just `usize`:

- Delete `enum EndpointIdx { ... }` and `impl EndpointIdx { ... }`
- Replace `EndpointIdx::Unified(i)` with `i` everywhere it appears
- Replace `skip: &[EndpointIdx]` with `skip: &[usize]` in function signatures
- Replace `RoutingCandidate { endpoint: EndpointIdx, .. }` with `endpoint: usize`

```bash
grep -n "EndpointIdx" src/main.rs
```

Iterate until grep returns nothing.

- [ ] **Step 6: Delete `try_fallback_upstream`, `record_usage`, `probe_account` (legacy)**

These were the legacy-Account versions. The unified versions (`try_fallback_upstream_unified`, `record_usage_unified`, `probe_endpoint_unified`) replace them. Rename the unified versions to drop the `_unified` suffix:

```bash
sed -i 's/try_fallback_upstream_unified/try_fallback_upstream/g; s/record_usage_unified/record_usage/g; s/probe_endpoint_unified/probe_account/g' src/main.rs
# Then manually rename the function definitions.
```

Hmm — `probe_account` is a misnomer if it now operates on `Endpoint`. Rename to `probe_endpoint` instead.

- [ ] **Step 7: Delete `make_account` test helper, replace with `make_endpoint`**

In `mod tests`, replace `fn make_account` with:

```rust
fn make_endpoint(name: &str, token: &str) -> Endpoint {
    Endpoint {
        name: name.to_string(),
        protocol: Protocol::Anthropic,
        base_url: "https://api.anthropic.com".to_string(),
        token: token.to_string(),
        passthrough: token == "passthrough",
        models: vec![],
        priority: 0,
        requests: AtomicU64::new(0),
        rate_info: RwLock::new(RateLimitInfo::default()),
        burn_rate: Mutex::new(BurnRate::new()),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
        cache_creation_tokens: AtomicU64::new(0),
        cache_read_tokens: AtomicU64::new(0),
        last_routing_weight: AtomicU64::new(0),
        last_routing_share: AtomicU64::new(0),
        last_effective_gate: AtomicU64::new(0),
    }
}
```

Replace every `make_account(...)` call with `make_endpoint(...)` and update test_state_with* signatures from `Vec<Account>` to `Vec<Endpoint>`. The `accounts:` field in the `AppState { ... }` test literals becomes `endpoints:` (it already exists from Task 7) — just pass the vec to the right field.

`test_state_with_fallback` is no longer needed (the fallback concept is gone) — delete it. Any test using it must be rewritten to insert an `Endpoint { protocol: OpenAI, .. }` into `state.endpoints` directly.

- [ ] **Step 8: Verify compile and all tests pass**

```bash
cargo build 2>&1 | tail -10
cargo test --lib 2>&1 | tail -10
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets 2>&1 | tail -10
cargo fmt --check 2>&1 | tail -3
```

Expected: clean build, **all** tests green, clippy clean, fmt clean. This is the moment of truth — the refactor is done.

- [ ] **Step 9: Commit**

```bash
git add src/main.rs
git commit -m "refactor: delete legacy Account/Upstream/fallback_upstream; collapse EndpointIdx to usize"
```

---

## Phase 5 — Docs & deployment

### Task 26: Update local `config.toml` example

**Files:**
- Modify: `config.toml` (project root)

- [ ] **Step 1: Rewrite as `[[endpoints]]`**

Replace any `[[accounts]]` / `[[upstreams]]` / `fallback_upstream` lines with `[[endpoints]]` blocks per the new schema. Remove the global `upstream = "..."` key.

- [ ] **Step 2: Verify the binary parses it**

```bash
./target/release/anthropic-lb config.toml --check 2>&1 | tail -10
# (If --check doesn't exist, run normally and Ctrl-C after startup logs confirm "loaded endpoint ..." entries)
```

- [ ] **Step 3: Commit**

```bash
git add config.toml
git commit -m "docs: rewrite local config.toml example with [[endpoints]] schema"
```

---

### Task 27: Update `CLAUDE.md` config schema docs

**Files:**
- Modify: `CLAUDE.md` (project root)

- [ ] **Step 1: Update the "Config Fields" table**

Find the table section:

```bash
grep -n "Config Fields\|accounts\[\]\|upstreams\[\]\|fallback_upstream" CLAUDE.md
```

Rewrite to use `endpoints[]` rows with the new field set: `name`, `protocol`, `base_url`, `token`, `models`, `priority`. Remove rows for `accounts[]`, `upstreams[]`, `fallback_upstream`, and the global `upstream`.

- [ ] **Step 2: Update the "Architecture" / "Unified Endpoint Priority" sections**

The section describes how accounts and the fallback upstream share one priority space. Rewrite to describe a single `[[endpoints]]` pool with a `protocol` field.

- [ ] **Step 3: Update the "Token Type Detection (by prefix)" section**

Mention that `protocol = "openai"` endpoints are authed via `Bearer` (no prefix logic), while `protocol = "anthropic"` retains the prefix-based behavior (`sk-ant-oat*` → Bearer + OAuth beta headers, `sk-ant-api*` → `x-api-key`, `"passthrough"` → forward client auth).

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md for [[endpoints]] schema"
```

---

### Task 28: Update mem cluster ExternalSecret

**Files:**
- Modify: `/home/fish/code/27b.io/fleet-infra/apps/mem/anthropic-lb/externalsecret.yaml`

- [ ] **Step 1: Rewrite the inline TOML template**

Replace the three `[[accounts]]` blocks with `[[endpoints]]` blocks. There are no upstreams in the mem cluster — just the three Anthropic accounts.

```yaml
        config.toml: |
          listen = "0.0.0.0:8082"
          redis_url = "redis://:{{ .redis_password }}@redis.redis.svc.cluster.local:6379"
          strategy = "sticky-weighted-v2"
          auto_cache = true
          shadow_log = "/tmp/shadow.jsonl"
          soft_limit = 0.90
          probe_interval_secs = 300

          emergency_brake = false
          emergency_threshold = 0.95

          [client_names]

          [[endpoints]]
          name = "primary"
          token = "{{ .token_primary }}"

          [[endpoints]]
          name = "jeff"
          token = "{{ .token_jeff }}"

          [[endpoints]]
          name = "insight"
          token = "{{ .token_claude_code_insight }}"
```

Note: the global `upstream = "https://api.anthropic.com"` line is removed — endpoints derive their base URL from `protocol` (defaulting to Anthropic).

- [ ] **Step 2: Commit (in the fleet-infra repo)**

```bash
cd /home/fish/code/27b.io/fleet-infra
git add apps/mem/anthropic-lb/externalsecret.yaml
git commit -m "anthropic-lb: migrate config to [[endpoints]] schema"
```

Note: do not push yet — the binary must ship in lockstep.

---

### Task 29: Update lab cluster ExternalSecret

**Files:**
- Modify: `/home/fish/code/27b.io/lab/k8s/mcp/anthropic-lb-externalsecret.yaml`

- [ ] **Step 1: Rewrite the inline TOML template**

Convert all `[[accounts]]` blocks to `[[endpoints]]` and convert the `[[upstreams]]` block to `[[endpoints]]` with `protocol = "openai"`. Remove the `fallback_upstream = "..."` line and the global `upstream = "..."` line.

```yaml
        config.toml: |
          listen = "0.0.0.0:8082"
          strategy = "sticky-weighted-v2"
          rate_limit_cooldown_secs = 60
          probe_interval_secs = 300
          auto_cache = true
          soft_limit = 0.90
          emergency_brake = false
          emergency_threshold = 0.95

          [[endpoints]]
          name = "passbolt"
          token = "{{ .token_passbolt }}"

          [[endpoints]]
          name = "primary"
          token = "{{ .token_primary }}"

          [[endpoints]]
          name = "jeff"
          token = "{{ .token_jeff }}"

          [[endpoints]]
          name = "insight"
          token = "{{ .token_insight }}"

          [[endpoints]]
          name = "insight-gateway"
          protocol = "openai"
          base_url = "https://gateway.lobster-python.ts.net"
          token = "{{ .token_insight_gateway }}"
          priority = 100
```

Confirm with the operator (Ray) whether the current `priority = 1` settings on all four accounts (as seen in the file's existing state) should be preserved verbatim or normalized to defaults during this migration.

- [ ] **Step 2: Commit (in the lab repo)**

```bash
cd /home/fish/code/27b.io/lab
git add k8s/mcp/anthropic-lb-externalsecret.yaml
git commit -m "anthropic-lb: migrate config to [[endpoints]] schema"
```

Note: do not push yet — coordinate with the binary roll-out.

---

### Task 30: Final quality gates and integration sanity

**Files:**
- None (verification only)

- [ ] **Step 1: Run all quality gates**

```bash
cd /home/fish/code/anthropic-lb
cargo fmt --check 2>&1 | tail -3
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets 2>&1 | tail -3
cargo test 2>&1 | tail -10
```

Expected: all clean.

- [ ] **Step 2: Smoke test locally**

```bash
cargo build --release
./target/release/anthropic-lb config.toml &
sleep 2
curl -sv http://127.0.0.1:8080/_stats | jq '.endpoints | length'
kill %1
```

Expected: the smoke test returns the configured endpoint count.

- [ ] **Step 3: Push the binary, then the configs (in this order)**

```bash
cd /home/fish/code/anthropic-lb
git push origin feat/overage-aware-routing

# After CI passes and the binary image lands:
cd /home/fish/code/27b.io/fleet-infra && git push
cd /home/fish/code/27b.io/lab && git push
```

- [ ] **Step 4: Verify the deployments**

```bash
kubectl --context mem -n anthropic-lb logs -l app=anthropic-lb --since=2m | grep -E "loaded endpoint|protocol"
kubectl --context lab -n mcp logs -l app=anthropic-lb --since=2m | grep -E "loaded endpoint|protocol"
```

Expected: each pod logs `loaded endpoint ... protocol=Anthropic|OpenAI` lines for every configured endpoint.

- [ ] **Step 5: Final commit (if any docs slipped)**

```bash
cd /home/fish/code/anthropic-lb
git status
# If any tracked file was modified mid-task and not committed, commit now.
```

---

## Rollback procedure (if a roll-out fails)

If `cargo test` was green but a cluster pod fails to start or behaves incorrectly:

1. **Identify the prior good image digest:**

   ```bash
   kubectl --context <ctx> -n <ns> describe deployment anthropic-lb | grep "Image:"
   ```

   Find the prior digest in the deployment's revision history:

   ```bash
   kubectl --context <ctx> -n <ns> rollout history deployment/anthropic-lb
   ```

2. **Roll back the binary and the config together:**

   ```bash
   kubectl --context <ctx> -n <ns> rollout undo deployment/anthropic-lb
   cd /home/fish/code/27b.io/<repo>
   git revert HEAD       # reverts the externalsecret.yaml migration
   git push
   ```

3. **Verify recovery:**

   ```bash
   kubectl --context <ctx> -n <ns> logs -l app=anthropic-lb --since=1m | tail -20
   ```

The old binary and the old `[[accounts]]`-style config must travel together — neither side parses the other.

---

## Notes for the implementing engineer

- **The single-file refactor is structurally aggressive but mechanically simple.** The compiler enumerates every site that needs to change. Trust the type system; do not rush past clippy warnings.
- **Do not skip the warn on legacy state file load.** Silent state discard in production is the kind of thing that becomes a 3am surprise.
- **Phase 2 tasks have non-trivial coupling** between `forward_anthropic`, `record_usage_unified`, and the `UsageTarget` enum. Read all three of Tasks 9, 10, and 14 before starting any of them.
- **Phase 3 tests should already pass** after Phase 2's structural changes — they are spec-coverage tests, not red-green increments. If any fails, it indicates the structural change in Phase 2 missed a case; do not try to fix the test, fix the structural code.
- **Phase 5 deploys are last** because the binary must be deployed before the config switch in either cluster (any pod that picks up the new config before getting the new binary will crash-loop on parse).
- **Commits are atomic, but the refactor is not.** Plan ~4 hours of focused work; budget 6.

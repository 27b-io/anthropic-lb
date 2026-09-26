use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use fred::{
    clients::RedisClient,
    interfaces::{ClientLike, EventInterface, HashesInterface, KeysInterface, LuaInterface},
    types::{
        ConnectionConfig, Expiration, PerformanceConfig, ReconnectPolicy, RedisConfig, RedisValue,
        SetOptions,
    },
};
use ipnet::IpNet;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, trace, warn};

// LAB-3877: Tier 0 request-content guard. Compiled only behind the `guard`
// cargo feature; with it off, none of this module (or its scanner crates) is
// built and request handling is unchanged.
#[cfg(feature = "guard")]
mod guard;

mod auto_cache;
mod config;
mod fallback;
mod handler;
mod metrics;
mod oauth_prompt;
mod openai_compat;
mod persistence;
mod response_cache;
mod reverse_translation;
mod routing;
mod session_registry;
mod state;
mod stats;
#[cfg(test)]
mod test_support;
mod token_usage;
mod utilization;

use auto_cache::*;
use config::*;
use fallback::*;
use handler::*;
use metrics::*;
use oauth_prompt::*;
use openai_compat::*;
use response_cache::*;
use reverse_translation::*;
use session_registry::*;
use state::*;
use stats::*;
#[cfg(test)]
use test_support::*;
use token_usage::*;
use utilization::*;

// ── Main ────────────────────────────────────────────────────────────

/// Validate endpoint configuration. Returns the first hard error encountered.
/// A non-canonical host on an anthropic endpoint is a hard error unless the
/// endpoint opts in via `allow_nonstandard_host` (then it degrades to the
/// `warn!`). Priority collision with an openai endpoint stays a soft warning.
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
                    // Parse the URL and compare hosts exactly. A naive
                    // `starts_with("https://api.anthropic.com")` would accept
                    // `https://api.anthropic.com.evil.example` as canonical.
                    let host = reqwest::Url::parse(url)
                        .ok()
                        .and_then(|u| u.host_str().map(str::to_string));
                    if host.as_deref() != Some("api.anthropic.com") {
                        // This endpoint's token is forwarded to that host on
                        // every request — a typo here is credential exfil, so
                        // it must be an explicit opt-in, not a scrolled-past
                        // warning (LAB-1191 / 2026-06-02 audit finding 1).
                        if ep.allow_nonstandard_host != Some(true) {
                            return Err(format!(
                                "endpoint '{}': base_url host '{}' is not api.anthropic.com — \
                                 the endpoint token would be sent to a non-Anthropic host. \
                                 Set allow_nonstandard_host = true on this endpoint if intentional",
                                ep.name,
                                host.as_deref().unwrap_or("<unparseable>")
                            ));
                        }
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

/// Startup validation for the `[[clients]]` registry (LAB-1083).
///
/// Every rule here exists because its violation fails SILENTLY at runtime
/// rather than loudly: a duplicate key resolves to whichever entry the scan
/// saw last, a duplicate or empty name silently merges two callers' budgets and
/// cache tenancy, and a `[response_cache].clients` typo makes the cache inert
/// for that client with no signal at all. Cheap to catch at boot; expensive to
/// notice in production.
fn validate_clients(config: &Config) -> Result<(), String> {
    let clients = &config.clients;
    let mut seen_names: Vec<&str> = Vec::with_capacity(clients.len());
    let mut seen_keys: Vec<&str> = Vec::with_capacity(clients.len());
    for c in clients {
        if c.name.trim().is_empty() {
            return Err("client: name must not be empty".to_string());
        }
        // Stored untrimmed, so " geo" would become a client_id that matches no
        // `client_budgets` / `operators` / `[response_cache].clients` key —
        // silently unenforced budget and cache tenancy. `resolve_client_id`
        // trims its header input; this path has nothing to trim it later.
        if c.name != c.name.trim() {
            return Err(format!(
                "client '{}': name must not have leading or trailing whitespace",
                c.name
            ));
        }
        if c.name == "-" {
            return Err(
                "client: name must not be \"-\" (the unknown-client sentinel — budget enforcement skips it)"
                    .to_string(),
            );
        }
        if c.name == "_operator" {
            return Err(
                "client: name must not be \"_operator\" (the reserved operator-aggregation label on /_stats and /metrics)"
                    .to_string(),
            );
        }
        if c.name == "_other" {
            return Err(
                "client: name must not be \"_other\" (the reserved metrics overflow-bucket label — a real client with this name would merge into the (\"_other\", \"_other\") overflow key)"
                    .to_string(),
            );
        }
        if c.key.is_empty() {
            return Err(format!("client '{}': key must not be empty", c.name));
        }
        // NOTE: the MIN_KEY_LEN strength floor (AC-13) is enforced in
        // validate_exposure, not here — this function's structural rules
        // (empty / duplicate / whitespace) are exercised by tests with
        // deliberately short keys, and the deployment-posture floor belongs
        // with the other posture checks. Both reference the shared const.
        if seen_names.contains(&c.name.as_str()) {
            return Err(format!("client '{}': duplicate name", c.name));
        }
        // preferred_endpoints must name configured endpoints — same posture as
        // the registry cross-checks below: a typo would silently leave the
        // client un-pinned, defeating the whole point of a dedicated account.
        for pe in &c.preferred_endpoints {
            if !config.endpoints.iter().any(|ep| ep.name == *pe) {
                return Err(format!(
                    "client '{}': preferred_endpoints entry '{}' does not match any configured endpoint",
                    c.name, pe
                ));
            }
        }
        // Names, not keys, in the error — never log a credential.
        if seen_keys.contains(&c.key.as_str()) {
            return Err(format!(
                "client '{}': duplicate key (already used by another client)",
                c.name
            ));
        }
        seen_names.push(&c.name);
        seen_keys.push(&c.key);
    }

    // The legacy `client_names` IP map is the third identity entry point
    // (`resolve_client_id`'s fallback) — an IP mapped to a reserved sentinel
    // would resolve real traffic to it, bypassing the header filter above.
    for (ip, name) in &config.client_names {
        // Same failure as an untrimmed `[[clients]]` name above: resolved
        // verbatim by `resolve_client_id`, matches no budget key.
        if name.is_empty() || name != name.trim() {
            return Err(format!(
                "client_names: \"{ip}\" maps to \"{name}\" — value must be non-empty with no leading or trailing whitespace"
            ));
        }
        if name == "-" || name == "_operator" || name == "_other" {
            return Err(format!(
                "client_names: \"{ip}\" maps to reserved name \"{name}\" (\"-\" = unknown-client sentinel, \"_operator\" = operator-aggregation label, \"_other\" = metrics overflow bucket)"
            ));
        }
    }

    // One client registry, not five. Every one of these config surfaces keys on
    // a client name, and every one of them fails SILENTLY on a typo — in the
    // dangerous direction: `check_budget` and `check_utilization_limit` both
    // return Ok(()) for an unknown client, so a mistyped budget means UNLIMITED
    // spend against the operator's accounts with no log line and no metric; a
    // mistyped `operators` entry silently gates the caller it was meant to
    // exempt; a mistyped `response_cache.clients` entry silently makes the
    // cache inert. Only enforceable when [[clients]] is configured — on the
    // legacy path client ids are header-derived and there is no registry to
    // check against.

    // `admin_readers` (LAB-4395) is the one cross-check surface that must fire on
    // the legacy path too, because there it cannot work at all: with a single
    // shared `proxy_key` the key holder IS the operator (`authenticate`
    // returns no principal, so `authorize_admin` serves), and `client_id` on
    // the proxy path is caller-asserted via `x-client-id`. An `admin_readers` entry
    // would therefore restrict nobody and grant nobody — a control that reads
    // as scoping while scoping nothing. Reject rather than half-handle,
    // exactly as with `passthrough` below. Unlike `operators`, there are no
    // pre-existing configs carrying this key, so nothing regresses.
    if !config.admin_readers.is_empty() && clients.is_empty() {
        return Err(
            "admin_readers: requires [[clients]] — under legacy proxy_key the key holder is the operator by construction and client ids are caller-asserted, so a read-only role cannot be enforced"
                .to_string(),
        );
    }
    // One name, one role. `operators` bypasses every request policy and
    // `admin_readers` is refused every proxied request; a name in both is a config
    // whose author meant one of two opposite things. `pre_request_gate`
    // resolves the overlap to the denial, but silently resolving it is how a
    // typo becomes an outage or an unmetered key — so name it at boot.
    if let Some(dup) = config
        .admin_readers
        .iter()
        .find(|r| config.operators.contains(r))
    {
        return Err(format!(
            "admin_readers: \"{dup}\" is also in operators — a client is either a read-only principal or an operator, never both"
        ));
    }

    if clients.is_empty() {
        return Ok(());
    }

    // `token = "passthrough"` forwards the caller's auth headers to the
    // upstream UNTOUCHED (`inject_account_auth` returns before the
    // header-strip). Under [[clients]] the caller's `x-api-key` is its PROXY
    // credential, not an upstream one — so the two modes together would
    // transmit every client key verbatim to that endpoint's `base_url`. They
    // are contradictory by construction: passthrough means "the caller brings
    // its own upstream credential", [[clients]] means "that header is mine".
    // Reject rather than half-handle, exactly as with proxy_key above.
    if let Some(ep) = config.endpoints.iter().find(|e| e.token == "passthrough") {
        return Err(format!(
            "endpoint '{}': token = \"passthrough\" is incompatible with [[clients]] — passthrough forwards the caller's auth headers upstream, which would leak client keys to {}",
            ep.name,
            ep.base_url.as_deref().unwrap_or("https://api.anthropic.com")
        ));
    }

    let known = |name: &str| seen_names.contains(&name);
    for (surface, name) in std::iter::empty()
        .chain(config.client_budgets.keys().map(|k| ("client_budgets", k)))
        .chain(
            config
                .client_utilization_limits
                .keys()
                .map(|k| ("client_utilization_limits", k)),
        )
        .chain(config.operators.iter().map(|k| ("operators", k)))
        .chain(config.admin_readers.iter().map(|k| ("admin_readers", k)))
        .chain(
            config
                .response_cache
                .iter()
                .flat_map(|rc| rc.clients.iter().map(|k| ("response_cache.clients", k))),
        )
    {
        if !known(name) {
            return Err(format!(
                "{surface}: \"{name}\" names no configured [[clients]] entry"
            ));
        }
    }
    Ok(())
}

/// Startup exposure posture (LAB-1192): unauthenticated is a BOOT FAILURE,
/// not a default. Same shape as `reject_legacy_config_keys` — a named error
/// at startup instead of a silent misconfiguration in production, where an
/// open proxy is indistinguishable from a configured one until someone
/// finds it.
fn validate_exposure(config: &Config) -> Result<(), String> {
    let has_credentials = config.proxy_key.is_some() || !config.clients.is_empty();
    let allow_unauthenticated = config.allow_unauthenticated.unwrap_or(false);
    if !has_credentials && !allow_unauthenticated {
        return Err(
            "config: no credentials configured — add [[clients]] entries (or legacy proxy_key), \
             or explicitly set allow_unauthenticated = true for a trusted-network-only deployment \
             (see README §Authentication)"
                .to_string(),
        );
    }
    // One escape hatch, one meaning. Credentials + allow_unauthenticated
    // together would make the flag silently dead (or worse, ambiguous) —
    // exactly the half-applied-migration state the mutual-exclusion rules
    // exist to reject.
    if has_credentials && allow_unauthenticated {
        return Err(
            "config: allow_unauthenticated = true is incompatible with configured credentials — \
             remove it, or remove [[clients]]/proxy_key (see README §Authentication)"
                .to_string(),
        );
    }
    // AC-13: a static bearer credential on a public ingress gets scanned;
    // below 32 characters the keyspace is the vulnerability.
    if let Some(ref key) = config.proxy_key {
        if key.len() < MIN_KEY_LEN {
            return Err(format!(
                "config: proxy_key is shorter than {MIN_KEY_LEN} characters — generate one with `openssl rand -hex 32`"
            ));
        }
    }
    for c in &config.clients {
        if c.key.len() < MIN_KEY_LEN {
            return Err(format!(
                "client '{}': key is shorter than {MIN_KEY_LEN} characters — generate one with `openssl rand -hex 32`",
                c.name
            ));
        }
    }
    if config
        .auth_failure_limit
        .unwrap_or(DEFAULT_AUTH_FAILURE_LIMIT)
        > 0
        && config
            .auth_failure_window_secs
            .unwrap_or(DEFAULT_AUTH_FAILURE_WINDOW_SECS)
            == 0
    {
        return Err(
            "config: auth_failure_window_secs must be > 0 (set auth_failure_limit = 0 to disable the throttle)"
                .to_string(),
        );
    }
    Ok(())
}

/// Reject removed config keys with explicit errors. Run after the raw TOML
/// has been parsed to a `toml::Value`, before strongly-typed deserialization.
///
/// `serde` silently drops unknown keys by default; this gives the operator
/// a clear migration message instead of a silent misconfiguration.
///
/// The `admin_readers` arm also rejects a MISPLACED spelling, not just a
/// removed one, because that drop fails OPEN: an empty `admin_readers` does
/// not disable the read-only principal, it promotes it to an ordinary client
/// with full proxy authority, and `validate_clients`' overlap and membership
/// cross-checks never run for want of a name to check.
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
    // LAB-4395. Two ways to lose the read-only principal silently, both of
    // which promote it rather than disable it (see this function's doc):
    //   1. `readers` at the root — the pre-rename spelling, still the natural
    //      guess for anyone working from the original ticket.
    //   2. `admin_readers` (or `readers`) written UNDER a `[[clients]]` or
    //      `[[endpoints]]` header — TOML binds a bare key to the table above
    //      it, so appending the line to the end of a config nests it. This is
    //      the likelier mistake of the two: the spelling is right and the file
    //      looks correct. Neither struct has such a field, so no false hits.
    if table.contains_key("readers") {
        return Err(
            "config: `readers` is not a config key — the read-only principal list is `admin_readers` (see README §Config Reference)"
                .to_string(),
        );
    }
    for section in ["clients", "endpoints"] {
        let Some(entries) = table.get(section).and_then(|v| v.as_array()) else {
            continue;
        };
        for entry in entries {
            let Some(entry) = entry.as_table() else {
                continue;
            };
            for key in ["admin_readers", "readers"] {
                if entry.contains_key(key) {
                    return Err(format!(
                        "config: `{key}` found inside a [[{section}]] entry — it is a TOP-LEVEL key; a bare key after a [[{section}]] header binds to that entry and is silently dropped. Move `admin_readers` above the first [[{section}]] block (see README §Config Reference)"
                    ));
                }
            }
        }
    }
    // LAB-1083: `proxy_key` is the legacy single shared secret, `[[clients]]`
    // its per-client replacement. Rejecting the combination rather than
    // precedence-ordering it is deliberate — a silent winner between two
    // authentication schemes is exactly the ambiguity that gets an operator's
    // migration half-applied and the weaker one left in force.
    if table.contains_key("proxy_key") && table.contains_key("clients") {
        return Err(
            "config: proxy_key and [[clients]] are mutually exclusive — [[clients]] supersedes it; remove proxy_key (see README §Authentication)"
                .to_string(),
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    // Parse config first so debug_log path is available for tracing setup
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_str = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("failed to read {config_path}: {e}"));
    let raw_value: toml::Value =
        toml::from_str(&config_str).unwrap_or_else(|e| panic!("config parse error: {e}"));
    if let Err(msg) = reject_legacy_config_keys(&raw_value) {
        panic!("{msg}");
    }
    let config: Config = raw_value
        .try_into()
        .unwrap_or_else(|e| panic!("config parse error: {e}"));
    if let Err(msg) = validate_endpoints(&config.endpoints) {
        panic!("{msg}");
    }
    if let Err(msg) = validate_clients(&config) {
        panic!("config: {msg}");
    }
    if let Err(msg) = validate_exposure(&config) {
        panic!("{msg}");
    }

    // Set up tracing: stderr (info+) always, plus optional debug log file
    {
        use tracing_subscriber::prelude::*;
        let stderr_layer = tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "anthropic_lb=info".into()),
        );
        if let Some(ref debug_path) = config.debug_log {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(debug_path)
                .unwrap_or_else(|e| panic!("failed to open debug log {debug_path}: {e}"));
            let file_layer = tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(file))
                .with_filter(tracing_subscriber::EnvFilter::new("anthropic_lb=debug"));
            tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer)
                .init();
            eprintln!("debug logging to {debug_path}");
        } else {
            tracing_subscriber::registry().with(stderr_layer).init();
        }
    }

    let routing_strategy = RoutingStrategy::parse(config.strategy.as_deref())
        .unwrap_or_else(|e| panic!("invalid strategy: {e}"));

    assert!(
        !config.endpoints.is_empty(),
        "at least one [[endpoints]] entry required"
    );

    // Validate new config fields
    for (client, limit) in &config.client_utilization_limits {
        assert!(
            (0.0..=1.0).contains(limit),
            "client_utilization_limits.{client}: must be 0.0-1.0, got {limit}"
        );
    }
    for op in &config.operators {
        assert!(
            op != "-",
            "operators cannot contain '-' (the unknown-client sentinel)"
        );
    }
    if let Some(thresh) = config.emergency_threshold {
        assert!(
            (0.0..=1.0).contains(&thresh),
            "emergency_threshold must be 0.0-1.0, got {thresh}"
        );
    }

    let cooldown = Duration::from_secs(config.rate_limit_cooldown_secs.unwrap_or(5));

    // Parse IP allowlist + trusted proxy list (LAB-1192) — same IP/CIDR syntax.
    let allowed_ips = parse_ip_entries(config.allowed_ips.as_deref(), "allowed_ips");
    let trusted_proxies = parse_ip_entries(config.trusted_proxies.as_deref(), "trusted_proxies");
    if allowed_ips.is_empty() {
        warn!("IP allowlist DISABLED — all source IPs accepted");
    } else {
        info!(count = allowed_ips.len(), "IP allowlist enabled");
    }
    if !trusted_proxies.is_empty() {
        info!(
            count = trusted_proxies.len(),
            "trusted proxies configured — x-forwarded-for honoured from these peers"
        );
    } else if !config.clients.is_empty() || config.proxy_key.is_some() {
        // Credentials but no trusted_proxies: behind a load balancer every
        // client collapses to the LB's peer IP, leaving one shared allowlist
        // decision and one shared invalid-credential throttle bucket. Valid
        // credentials still pass, but source attribution and per-client
        // failure isolation are lost. Warn loudly (LAB-1192, amended by
        // LAB-1193).
        warn!(
            "no trusted_proxies configured — if this instance sits behind a load balancer, \
             all clients share the LB's peer IP for the allowlist decision and one \
             invalid-credential throttle bucket; valid credentials still pass, but \
             per-client source attribution and failure isolation are lost; set \
             trusted_proxies to the LB's address range"
        );
    }

    // Operators gate /_stats + /metrics under [[clients]] (LAB-1192 AC-4). An
    // empty operators list there means NO principal can read them — a silent
    // way to blind a monitoring scrape. Warn so the omission is visible.
    if !config.clients.is_empty() && config.operators.is_empty() && config.admin_readers.is_empty()
    {
        warn!(
            "[[clients]] configured with empty operators AND admin_readers lists — /_stats and \
             /metrics will reject EVERY caller (403); name at least one client in admin_readers \
             (read-only, the right role for a scrape) or operators, or your monitoring \
             goes blind"
        );
    }

    // Build the unified endpoint vector from the [[endpoints]] config.
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
                fable_included: ec.fable_included.unwrap_or(true),
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

    if !config.clients.is_empty() {
        let with_allowlist = config
            .clients
            .iter()
            .filter(|c| !c.models.is_empty())
            .count();
        info!(
            clients = config.clients.len(),
            with_model_allowlist = with_allowlist,
            "per-client authentication enabled — x-client-id is ignored, identity comes from the credential"
        );
    } else if config.proxy_key.is_some() {
        warn!("legacy shared proxy_key in use — every caller shares one identity; migrate to [[clients]] (see README §Authentication)");
    } else {
        // validate_exposure guarantees this state is only reachable with the
        // flag explicitly set (AC-2).
        warn!(
            "allow_unauthenticated = true — proxy, /_stats and /metrics accept unauthenticated \
             traffic; safe ONLY on a trusted network (NetworkPolicy/tailnet), never on a public ingress"
        );
    }

    info!(
        strategy = routing_strategy.as_str(),
        num_endpoints = endpoints.len(),
        "routing strategy selected"
    );

    let state_path = PathBuf::from(&config_path).with_extension("state.json");

    // Set up shadow log writer if configured
    let shadow_log_tx = if let Some(ref path) = config.shadow_log {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(10_000);
        let log_path = PathBuf::from(path);
        info!(path = %log_path.display(), "shadow log enabled");
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    error!(path = %log_path.display(), error = %e, "failed to open shadow log");
                    return;
                }
            };
            while let Some(line) = rx.recv().await {
                let data = format!("{}\n", line);
                if let Err(e) = file.write_all(data.as_bytes()).await {
                    error!(error = %e, "shadow log write failed");
                }
            }
        });
        Some(tx)
    } else {
        None
    };

    // Set up Redis connection for distributed state (if configured).
    // Timeout budgets carry over from the old ConnectionManager: 2s per
    // command, 5s to establish a connection. The startup contract (LAB-1639):
    // the connect runs in the BACKGROUND under the retry-forever reconnect
    // policy — startup never blocks on Redis and never aborts for an
    // unreachable backend. A process that boots during a backend outage
    // serves local-only (coordination ops gated off via
    // `coordination_redis`) and attaches automatically when the backend
    // becomes reachable. A config error (unparseable URL) is different —
    // LAB-3026: an operator who set `redis_url` has said shared state is
    // required, so a URL that fails to parse (e.g. a password with an
    // unescaped `@`/`/`/`?`/`#`/`:`) fails startup outright instead of
    // silently downgrading to local-only, matching the `response_cache`
    // config gate below. Mid-run outage behaviour is unchanged from LAB-932
    // AC5: once connected, drops reconnect with capped exponential backoff.
    let redis = if let Some(ref url) = config.redis_url {
        let perf = PerformanceConfig {
            default_command_timeout: REDIS_COMMAND_TIMEOUT,
            ..Default::default()
        };
        let conn_config = ConnectionConfig {
            // Keep connection_timeout in step with REDIS_STARTUP_GRACE: the
            // startup WARN is timed to fire after one full connect budget.
            connection_timeout: Duration::from_secs(5),
            internal_command_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        // 0 = retry forever; delays 100ms → 30s, doubling.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
        match start_coordination_redis(url.as_str(), perf, conn_config, policy) {
            Ok(client) => Some(client),
            // Log only `kind()` (a fixed enum, e.g. `Url`/`Config`) — never
            // the error's `Display`/`details()`, which for a malformed URL
            // can echo the offending fragment back, credential included.
            Err(e) => panic!("redis_url: failed to parse ({:?})", e.kind()),
        }
    } else {
        None
    };

    // LAB-933: opt-in encrypted response cache. Invalid CONFIG fails startup
    // — an operator who configured a cache must not silently run without
    // one. An unreachable BACKEND does not: operations fail open per-request.
    let response_cache = match config.response_cache {
        Some(ref cfg) => match ResponseCache::from_config(cfg).await {
            Ok(Some(rc)) => {
                info!(
                    clients = rc.clients.len(),
                    backend = %cfg.backend,
                    ttl_secs = cfg.ttl_secs.unwrap_or(ResponseCache::DEFAULT_TTL_SECS),
                    "response cache enabled"
                );
                Some(rc)
            }
            Ok(None) => {
                info!("response_cache configured with an empty client allow-list — inert");
                None
            }
            Err(msg) => panic!("response_cache config error: {msg}"),
        },
        None => None,
    };

    // LAB-3877: build the Tier 0 guard once (rule/regex compilation is not
    // cheap). A broken ruleset fails startup loudly rather than silently
    // scanning nothing — the same fail-loud posture as the config gates above.
    #[cfg(feature = "guard")]
    let guard = match guard::Guard::new() {
        Ok(g) => {
            info!("content guard enabled (Tier 0 rules scanners; shadow-mode default)");
            g
        }
        Err(msg) => panic!("guard init failed: {msg}"),
    };

    let state = Arc::new(AppState {
        // Liveness knobs are load-bearing against Anthropic's Cloudflare edge:
        // h2 PING (while_idle) evicts half-closed pooled streams before they're
        // reused; read_timeout catches mid-stream stalls without waiting out
        // the full request budget; pool_idle_timeout keeps connections warm
        // through Claude Code read/think pauses to avoid paying a fresh TLS
        // handshake on every burst. read_timeout is set at 180s so Opus's
        // extended-thinking pauses (which can exceed 90s of inter-chunk
        // silence on deep reasoning) don't trip a false-positive interruption.
        client: upstream_client_builder()
            .read_timeout(Duration::from_secs(180))
            .build()
            .expect("failed to build HTTP client"),
        // Same knobs MINUS read_timeout: a non-streaming response has no
        // inter-chunk cadence to police — the only bytes arrive when
        // generation finishes, so a read_timeout is a hard cap on generation
        // time (LAB-718). The 900s total budget still bounds the request.
        client_nonstreaming: upstream_client_builder()
            .build()
            .expect("failed to build non-streaming HTTP client"),
        endpoints,
        robin: AtomicUsize::new(0),
        routing_strategy,
        cooldown,
        transport_cooldown: TRANSPORT_UNHEALTHY_COOLDOWN,
        state_path,
        proxy_key: config.proxy_key.clone(),
        clients: config.clients.clone(),
        #[cfg(feature = "guard")]
        guard,
        allowed_ips,
        trusted_proxies,
        auth_throttle: AuthThrottle::new(
            config
                .auth_failure_limit
                .unwrap_or(DEFAULT_AUTH_FAILURE_LIMIT),
            Duration::from_secs(
                config
                    .auth_failure_window_secs
                    .unwrap_or(DEFAULT_AUTH_FAILURE_WINDOW_SECS),
            ),
        ),
        auth_failures: Mutex::new(HashMap::new()),
        open_admin_warn: Mutex::new(HashMap::new()),
        client_names: config.client_names.clone(),
        auto_cache: config.auto_cache.unwrap_or(true),
        client_usage: Mutex::new(HashMap::new()),
        client_model_usage: Mutex::new(HashMap::new()),
        shadow_log_tx,
        shadow_log_dropped: AtomicU64::new(0),
        client_budgets: config.client_budgets.clone(),
        budget_usage: Mutex::new(HashMap::new()),
        client_utilization_limits: config.client_utilization_limits.clone(),
        operators: config.operators.clone(),
        admin_readers: config.admin_readers.clone(),
        emergency_brake: config.emergency_brake.unwrap_or(true),
        emergency_threshold: config
            .emergency_threshold
            .unwrap_or(DEFAULT_EMERGENCY_THRESHOLD),
        client_request_rates: Mutex::new(HashMap::new()),
        soft_limit: config.soft_limit.unwrap_or(0.90),
        redis,
        redis_ever_connected: AtomicBool::new(false),
        cluster_info_cache: Mutex::new(None),
        next_req_id: AtomicU64::new(0),
        instance_id: {
            use std::collections::hash_map::RandomState;
            use std::hash::{BuildHasher, Hasher};
            RandomState::new().build_hasher().finish() as u16
        },
        probe_interval_secs: config.probe_interval_secs.unwrap_or(300),
        overage_penalty: config.overage_penalty.unwrap_or(10),
        upstream_transport_errors: Mutex::new(HashMap::new()),
        inflight_body_bytes: AtomicU64::new(0),
        max_inflight_body_bytes: config
            .max_inflight_body_mb
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(DEFAULT_MAX_INFLIGHT_BODY_BYTES),
        body_shed_total: AtomicU64::new(0),
        body_read_timeout: Duration::from_secs(
            config
                .body_read_timeout_secs
                .unwrap_or(DEFAULT_BODY_READ_TIMEOUT_SECS),
        ),
        body_read_timeout_total: AtomicU64::new(0),
        affinity_migrations: Default::default(),
        pool_exhausted: Default::default(),
        sessions: Mutex::new(HashMap::new()),
        session_registry_max: config
            .session_registry_max
            .unwrap_or(DEFAULT_SESSION_REGISTRY_MAX),
        session_registry_ttl_secs: config
            .session_registry_ttl_secs
            .unwrap_or(DEFAULT_SESSION_REGISTRY_TTL_SECS),
        expose_upstream_ratelimit_headers: config
            .expose_upstream_ratelimit_headers
            .unwrap_or(false),
        forward_caller_identity: config.forward_caller_identity.unwrap_or(false),
        allowed_client_betas: config.allowed_client_betas.clone().unwrap_or_else(|| {
            DEFAULT_CLIENT_BETA_ALLOWLIST
                .iter()
                .map(|s| s.to_string())
                .collect()
        }),
        beta_flags_dropped: Mutex::new(HashMap::new()),
        beta_body_fields_stripped: Mutex::new(HashMap::new()),
        prompt_too_long: Mutex::new(HashMap::new()),
        fast_mode_429: Mutex::new(HashMap::new()),
        entitlement_400: Mutex::new(HashMap::new()),
        model_denied: Mutex::new(HashMap::new()),
        client_rejections: Mutex::new(HashMap::new()),
        unsupported_models: Mutex::new(HashMap::new()),
        response_cache,
    });

    if state.auto_cache {
        info!("auto-cache enabled");
    }

    // Observe the coordination client's first connect: opens the
    // `coordination_redis` gate and owns the startup connected/unreachable
    // log lines (LAB-1639).
    state.spawn_redis_connect_watcher();

    // Restore persisted state (cooldowns, utilization, request counts)
    state.load_state().await;

    // Seed metric weights from restored state so gauges aren't zero on cold start.
    state.refresh_metrics_weights().await;

    let app = Router::new()
        .route("/_stats", axum::routing::get(stats_handler))
        .route("/metrics", axum::routing::get(metrics_handler))
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .fallback(any(proxy_handler))
        .with_state(state.clone());

    let addr: SocketAddr = config
        .listen
        .parse()
        .unwrap_or_else(|e| panic!("invalid listen address: {e}"));

    info!(
        %addr,
        rate_limit_cooldown_secs = cooldown.as_secs(),
        configured = ?config.rate_limit_cooldown_secs,
        "anthropic-lb starting"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // Spawn periodic probe task. `probe_endpoint` internally skips OpenAI
    // endpoints (they expose no Anthropic rate-limit headers).
    let probe_interval = config.probe_interval_secs.unwrap_or(300);
    if probe_interval > 0 {
        let probe_state = state.clone();
        let n_endpoints = probe_state.endpoints.len();
        tokio::spawn(async move {
            // Fable is deliberately NOT probed: probes burn real quota, Fable
            // burns the shared weekly pool faster than other families, and past
            // the included band a probe would spend paid credits. The Fable band
            // claim (seven_day_fable) refreshes from organic Fable traffic; until
            // one is seen, routing falls back to the general seven_day claim.
            const PROBE_MODELS: &[&str] =
                &["claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-6"];
            // Stagger initial probes: wait 10s then probe all endpoints
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!(
                interval_secs = probe_interval,
                "starting utilization probes"
            );
            loop {
                for i in 0..n_endpoints {
                    let ep = &probe_state.endpoints[i];
                    // Probe all model families per endpoint per cycle
                    for model in PROBE_MODELS {
                        if ep.serves_model(model) {
                            probe_state.probe_endpoint(i, model).await;
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
                // Recompute metric weights once per cycle, after all probes
                // have refreshed rate-limit data. Keeps the gauges aligned
                // with steady-state pool health, not per-request bias.
                probe_state.refresh_metrics_weights().await;
                probe_state.publish_routing_weights().await;
                tokio::time::sleep(Duration::from_secs(probe_interval)).await;
            }
        });
    } else {
        // Probes disabled — but `update_rate_info_for()` still refreshes data
        // on every inbound request. Without this fallback ticker the routing
        // weight gauges would freeze at startup values forever.
        let metrics_state = state.clone();
        tokio::spawn(async move {
            const FALLBACK_INTERVAL: Duration = Duration::from_secs(60);
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!("probes disabled — metrics weights refresh on a 60s timer");
            loop {
                metrics_state.refresh_metrics_weights().await;
                metrics_state.publish_routing_weights().await;
                tokio::time::sleep(FALLBACK_INTERVAL).await;
            }
        });
    }

    // Spawn Redis state sync + heartbeat task (if Redis configured)
    if state.redis.is_some() {
        let sync_state = state.clone();
        let instance_id: u64 = {
            use std::collections::hash_map::RandomState;
            use std::hash::{BuildHasher, Hasher};
            RandomState::new().build_hasher().finish()
        };
        tokio::spawn(async move {
            // Wait for initial startup to complete
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("starting redis state sync (5s interval)");
            loop {
                sync_state.sync_from_redis().await;
                // Heartbeat: register this instance
                if let Some(redis) = sync_state.coordination_redis() {
                    let key = format!("alb:heartbeat:{instance_id}");
                    let _ = redis_set_ex(redis, &key, AppState::now_epoch().to_string(), 30).await;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // Bounded drain so a wedged stream can't hold the process past the
    // orchestrator's kill deadline and earn a SIGKILL with unsaved state.
    // In k8s this is `terminationGracePeriodSeconds`; keep SHUTDOWN_DRAIN_SECS
    // strictly less so the proxy exits cleanly before SIGKILL. Long Claude
    // Code streams (Opus on long outputs) can run 60–120s, so the budget
    // must accommodate that or in-flight responses get cut.
    const SHUTDOWN_DRAIN_SECS: u64 = 160;
    let shutdown_state = state.clone();

    // The drain deadline must start ticking from signal-arrival, not from
    // process start — `tokio::time::timeout(d, server)` would arm `d` when
    // first polled and force-exit any uptime > d. The signal handler fires
    // notify_waiters(), and the deadline future awaits that before its sleep.
    //
    // `drain_triggered` closes a race in `Notify::notify_waiters()`: it only
    // wakes waiters registered at call time. Without the flag, a signal that
    // arrives before drain_deadline registers its waiter is lost and the
    // deadline never fires. With the flag, drain_deadline registers the
    // waiter eagerly via Notified::enable(), then checks the flag — so
    // either path (signal-before-poll, signal-after-poll) is observed.
    let drain_signal = Arc::new(tokio::sync::Notify::new());
    let drain_signal_in = drain_signal.clone();
    let drain_triggered = Arc::new(AtomicBool::new(false));
    let drain_triggered_in = drain_triggered.clone();

    let shutdown = async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM");
        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
        info!(
            drain_budget_secs = SHUTDOWN_DRAIN_SECS,
            "draining in-flight requests"
        );
        drain_triggered_in.store(true, Ordering::SeqCst);
        drain_signal_in.notify_waiters();
    };

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown);

    let drain_deadline = async {
        let notified = drain_signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !drain_triggered.load(Ordering::SeqCst) {
            notified.await;
        }
        tokio::time::sleep(Duration::from_secs(SHUTDOWN_DRAIN_SECS)).await;
    };

    tokio::select! {
        res = server => match res {
            Ok(()) => info!("drain complete"),
            Err(e) => error!(error = %e, "server error during drain"),
        },
        _ = drain_deadline => warn!(
            drain_budget_secs = SHUTDOWN_DRAIN_SECS,
            "drain timeout exceeded — forcing exit; in-flight streams will be cut"
        ),
    }
    info!("saving state");
    shutdown_state.save_state().await;
    info!("state saved, shutdown complete");
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

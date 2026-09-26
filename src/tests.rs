use super::*;

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
    assert!(
        err.contains("accounts"),
        "error must name 'accounts': {err}"
    );
    assert!(
        err.contains("endpoints"),
        "error must mention replacement: {err}"
    );
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

#[test]
fn validate_endpoints_rejects_non_anthropic_host() {
    // LAB-1191 AC-1: a non-canonical host on an anthropic endpoint would send
    // the account token to that host — hard startup error naming the endpoint.
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://staging.anthropic.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(
        err.contains("primary"),
        "error must name the endpoint: {err}"
    );
    assert!(
        err.contains("allow_nonstandard_host"),
        "error must name the opt-out: {err}"
    );
}

#[test]
fn validate_endpoints_allows_non_anthropic_host_with_opt_in() {
    // LAB-1191 AC-2: explicit opt-in keeps the old warn-only behaviour.
    let endpoints = vec![EndpointConfig {
        name: "staging".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://staging.anthropic.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: Some(true),
    }];
    assert!(validate_endpoints(&endpoints).is_ok());
}

#[test]
fn validate_endpoints_rejects_lookalike_anthropic_host() {
    // LAB-1191 AC-3: exact-host comparison — a canonical-prefix lookalike
    // domain must NOT pass as canonical.
    let endpoints = vec![EndpointConfig {
        name: "evil".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://api.anthropic.com.evil.example".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
    let err = validate_endpoints(&endpoints).unwrap_err();
    assert!(err.contains("evil"));
}

#[test]
fn validate_endpoints_accepts_canonical_anthropic_host() {
    // LAB-1191 AC-3: the canonical host boots without opt-in.
    let endpoints = vec![EndpointConfig {
        name: "primary".to_string(),
        protocol: Protocol::Anthropic,
        base_url: Some("https://api.anthropic.com".to_string()),
        token: "sk-ant".to_string(),
        models: vec![],
        priority: 0,
        fable_included: None,
        allow_nonstandard_host: None,
    }];
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
        fable_included: None,
        allow_nonstandard_host: None,
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
        fable_included: None,
        allow_nonstandard_host: None,
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
            fable_included: None,
            allow_nonstandard_host: None,
        },
        EndpointConfig {
            name: "gateway".to_string(),
            protocol: Protocol::OpenAI,
            base_url: Some("https://gateway.example".to_string()),
            token: "sk-test".to_string(),
            models: vec![],
            priority: 100,
            fable_included: None,
            allow_nonstandard_host: None,
        },
    ];
    assert!(validate_endpoints(&endpoints).is_ok());
}

// ── AC-5 / AC-6: startup validation ──

#[test]
fn config_rejects_proxy_key_and_clients_together() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
proxy_key = "legacy"

[[clients]]
name = "geo"
key = "key-geo"
"#;
    let value: toml::Value = toml::from_str(toml_str).unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    assert!(
        err.contains("proxy_key"),
        "error must name proxy_key: {err}"
    );
    assert!(err.contains("clients"), "error must name clients: {err}");
}

#[test]
fn config_accepts_proxy_key_alone_and_clients_alone() {
    for toml_str in [
        "listen = \"0.0.0.0:8080\"\nproxy_key = \"legacy\"\n",
        "listen = \"0.0.0.0:8080\"\n\n[[clients]]\nname = \"geo\"\nkey = \"key-geo\"\n",
    ] {
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        assert!(reject_legacy_config_keys(&value).is_ok());
    }
}

#[test]
fn config_parses_clients_table_with_model_allow_list() {
    let toml_str = r#"
listen = "0.0.0.0:8080"

[[clients]]
name = "geo"
key = "key-geo"
models = ["claude-haiku-*", "claude-sonnet-4-6"]

[[clients]]
name = "radar"
key = "key-radar"
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.clients.len(), 2);
    assert_eq!(cfg.clients[0].name, "geo");
    assert_eq!(cfg.clients[0].key, "key-geo");
    assert_eq!(cfg.clients[0].models.len(), 2);
    assert!(
        cfg.clients[1].models.is_empty(),
        "omitted models must default to empty (= all allowed)"
    );
}

const RC_BLOCK: &str = "\n[response_cache]\nbackend = \"redis\"\nredis_url = \"redis://localhost\"\nmaster_key = \"0000000000000000000000000000000000000000000000000000000000000000\"\n";

#[test]
fn validate_clients_rejects_duplicate_names_and_keys() {
    let err = validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[[clients]]\nname = \"geo\"\nkey = \"k2\"\n",
    ))
    .unwrap_err();
    assert!(err.contains("duplicate name"), "{err}");

    let err = validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[[clients]]\nname = \"radar\"\nkey = \"k1\"\n",
    ))
    .unwrap_err();
    assert!(err.contains("duplicate key"), "{err}");
    assert!(
        !err.contains("k1"),
        "the error must not echo the credential: {err}"
    );
}

#[test]
fn validate_clients_rejects_bad_names_and_empty_keys() {
    for (fragment, needle) in [
        ("[[clients]]\nname = \"\"\nkey = \"k1\"\n", "name"),
        ("[[clients]]\nname = \"   \"\nkey = \"k1\"\n", "name"),
        ("[[clients]]\nname = \"-\"\nkey = \"k1\"\n", "sentinel"),
        // Reserved: /_stats and /metrics rewrite operator identities to the
        // literal "_operator", so a real tenant with that name would silently
        // merge with the aggregated operator bucket.
        (
            "[[clients]]\nname = \"_operator\"\nkey = \"k1\"\n",
            "_operator",
        ),
        // Reserved: the metrics overflow bucket is keyed ("_other", "_other");
        // a real client with that name could pre-create the exact pair and
        // later overflow denials/usage would merge into it (CodeRabbit, #148).
        ("[[clients]]\nname = \"_other\"\nkey = \"k1\"\n", "_other"),
        // The legacy client_names IP map is the third identity entry point
        // (resolve_client_id's fallback) — its values must not claim a
        // reserved sentinel either (expert-panel finding, #148 follow-up).
        ("[client_names]\n\"10.0.0.5\" = \"-\"\n", "reserved"),
        ("[client_names]\n\"10.0.0.5\" = \"_operator\"\n", "reserved"),
        ("[client_names]\n\"10.0.0.5\" = \"_other\"\n", "reserved"),
        ("[[clients]]\nname = \"geo\"\nkey = \"\"\n", "key"),
        // Untrimmed: stored verbatim, so it would become a client_id matching
        // no client_budgets / operators / response_cache.clients key.
        ("[[clients]]\nname = \" geo\"\nkey = \"k1\"\n", "whitespace"),
        ("[[clients]]\nname = \"geo \"\nkey = \"k1\"\n", "whitespace"),
    ] {
        let err = validate_clients(&cfg(fragment)).unwrap_err();
        assert!(err.contains(needle), "expected '{needle}' in: {err}");
    }
}

#[test]
fn validate_clients_accepts_a_well_formed_table() {
    assert!(validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\nmodels = [\"claude-haiku-*\"]\n\n[[clients]]\nname = \"radar\"\nkey = \"k2\"\n",
    ))
    .is_ok());
}

/// One client registry, not five. Each of these config surfaces keys on a
/// client name and each fails SILENTLY on a typo — a mistyped budget means
/// UNLIMITED spend (`check_budget` returns Ok for an unknown client), a
/// mistyped operator silently gates the caller it meant to exempt, a mistyped
/// cache client silently disables the cache.
#[test]
fn validate_clients_rejects_any_surface_naming_no_configured_client() {
    // NOTE: top-level scalars/arrays must precede the first table header —
    // a bare `operators = [...]` written after `[[clients]]` binds to that
    // table instead and is silently dropped, making the test pass vacuously.
    let base = "\n[[clients]]\nname = \"geo\"\nkey = \"k1\"\n";
    for (fragment, surface) in [
        (
            format!("{base}\n[client_budgets]\ngeo-pipeline = 100\n"),
            "client_budgets",
        ),
        (
            format!("{base}\n[client_utilization_limits]\ngeo-pipeline = 0.5\n"),
            "client_utilization_limits",
        ),
        (
            format!("operators = [\"geo-pipeline\"]\n{base}"),
            "operators",
        ),
        (
            format!("{base}{RC_BLOCK}clients = [\"geo-pipeline\"]\n"),
            "response_cache.clients",
        ),
    ] {
        let err = validate_clients(&cfg(&fragment)).unwrap_err();
        assert!(err.contains(surface), "expected '{surface}' in: {err}");
        assert!(err.contains("geo-pipeline"), "must name the typo: {err}");
    }
}

#[test]
fn validate_clients_accepts_every_surface_naming_a_configured_client() {
    let fragment = format!(
        "operators = [\"geo\"]\n\n[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[client_budgets]\ngeo = 100\n\n[client_utilization_limits]\ngeo = 0.5\n{RC_BLOCK}clients = [\"geo\"]\n"
    );
    let parsed = cfg(&fragment);
    // Guard against the vacuous-pass trap above: assert the fragment really
    // populated all four surfaces before asserting validation accepts them.
    assert_eq!(parsed.operators, vec!["geo".to_string()]);
    assert!(parsed.client_budgets.contains_key("geo"));
    assert!(parsed.client_utilization_limits.contains_key("geo"));
    assert_eq!(
        parsed.response_cache.as_ref().unwrap().clients,
        vec!["geo".to_string()]
    );
    assert!(validate_clients(&parsed).is_ok());
}

/// On the legacy path there is no registry to check against, so none of the
/// cross-checks may fire — existing configs must keep booting.
#[test]
fn validate_clients_skips_all_crosschecks_without_a_client_table() {
    let fragment = format!(
        "operators = [\"anything\"]\n\n[client_budgets]\nanything = 100\n{RC_BLOCK}clients = [\"anything\"]\n"
    );
    assert!(validate_clients(&cfg(&fragment)).is_ok());
}

/// `passthrough` forwards the caller's auth headers upstream untouched. Under
/// `[[clients]]` those headers carry the client's PROXY key, so the two modes
/// together would transmit every client key to the upstream.
#[test]
fn validate_clients_rejects_passthrough_endpoint_alongside_clients() {
    let err = validate_clients(&cfg(
        "[[clients]]\nname = \"geo\"\nkey = \"k1\"\n\n[[endpoints]]\nname = \"managed\"\ntoken = \"passthrough\"\n",
    ))
    .unwrap_err();
    assert!(err.contains("passthrough"), "{err}");
    assert!(err.contains("managed"), "must name the endpoint: {err}");
}

/// …but passthrough on the legacy path is untouched.
#[test]
fn validate_clients_allows_passthrough_without_a_client_table() {
    assert!(validate_clients(&cfg(
        "[[endpoints]]\nname = \"managed\"\ntoken = \"passthrough\"\n",
    ))
    .is_ok());
}

// ── LAB-1192: exposure controls ─────────────────────────────────
//
// Four mechanisms, one posture: unauthenticated is a boot failure not a
// default (AC-1..3), the admin surfaces answer only to an operator
// principal (AC-4..6), behind a trusted load balancer the proxy resolves
// the real client IP or refuses to guess (AC-7..9), and credential
// guessing is throttled per client IP with bounded state (AC-11..12).

// ── AC-3: startup posture ──

/// 64-hex — what `openssl rand -hex 32` emits, and the documented floor.
const STRONG_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn exposure_rejects_a_credential_free_config_by_default() {
    let err = validate_exposure(&cfg("")).unwrap_err();
    assert!(
        err.contains("allow_unauthenticated"),
        "error must name the escape hatch: {err}"
    );
    assert!(
        err.contains("[[clients]]"),
        "error must name the credential fix: {err}"
    );
}

#[test]
fn exposure_accepts_the_explicit_escape_hatch() {
    assert!(validate_exposure(&cfg("allow_unauthenticated = true\n")).is_ok());
    // `false` is NOT an escape hatch — omission and explicit false are the
    // same default-deny.
    assert!(validate_exposure(&cfg("allow_unauthenticated = false\n")).is_err());
}

#[test]
fn exposure_accepts_either_credential_form() {
    assert!(validate_exposure(&cfg(&format!("proxy_key = \"{STRONG_KEY}\"\n"))).is_ok());
    assert!(validate_exposure(&cfg(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\"\n"
    )))
    .is_ok());
}

#[test]
fn exposure_rejects_the_flag_alongside_credentials() {
    let err = validate_exposure(&cfg(&format!(
        "allow_unauthenticated = true\nproxy_key = \"{STRONG_KEY}\"\n"
    )))
    .unwrap_err();
    assert!(err.contains("incompatible"), "{err}");
    let err = validate_exposure(&cfg(&format!(
        "allow_unauthenticated = true\n[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\"\n"
    )))
    .unwrap_err();
    assert!(err.contains("incompatible"), "{err}");
}

/// AC-13: every configured credential form is held to the 32-char floor, and
/// the error carries the generation command.
#[test]
fn exposure_rejects_short_credentials_with_the_generation_command() {
    for fragment in [
        "proxy_key = \"short\"\n".to_string(),
        "[[clients]]\nname = \"geo\"\nkey = \"short\"\n".to_string(),
        // 31 chars — one under the floor, so the boundary is pinned.
        format!("proxy_key = \"{}\"\n", "a".repeat(31)),
    ] {
        let err = validate_exposure(&cfg(&fragment)).unwrap_err();
        assert!(
            err.contains("openssl rand -hex 32"),
            "error must carry the generation command: {err}"
        );
    }
    // 32 exactly passes.
    assert!(validate_exposure(&cfg(&format!("proxy_key = \"{}\"\n", "a".repeat(32)))).is_ok());
}

#[test]
fn exposure_rejects_a_zero_window_with_the_throttle_enabled() {
    let err = validate_exposure(&cfg(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nauth_failure_window_secs = 0\n"
    )))
    .unwrap_err();
    assert!(err.contains("auth_failure_window_secs"), "{err}");
    // Explicitly disabled throttle: a zero window is fine because it is never read.
    assert!(validate_exposure(&cfg(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nauth_failure_limit = 0\nauth_failure_window_secs = 0\n"
    )))
    .is_ok());
}

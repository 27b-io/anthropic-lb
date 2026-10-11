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

/// LAB-4395: both ways to lose the read-only principal silently. Either drop
/// fails OPEN — an empty `admin_readers` promotes the principal to an ordinary
/// client with full proxy authority rather than disabling it.
#[test]
fn config_rejects_a_dropped_read_only_principal() {
    // 1. the pre-rename spelling at the root.
    let value: toml::Value = toml::from_str(
        r#"
listen = "0.0.0.0:8080"
readers = ["grafana"]
"#,
    )
    .unwrap();
    let err = reject_legacy_config_keys(&value).unwrap_err();
    // Backticked, so `admin_readers` in the same message cannot satisfy it.
    assert!(err.contains("`readers`"), "{err}");
    assert!(err.contains("`admin_readers`"), "{err}");

    // 2. the right spelling under the wrong header, at any depth. TOML binds
    //    a bare key to the table above it, so each of these nests the list
    //    into that table and serde drops it there. `[response_cache]` and
    //    `[clients.guard]` are the cases a hand-listed section scan missed.
    let entry = "name = \"grafana\"\nkey = \"dummy-key\"\n";
    for (header, body, path) in [
        ("[[clients]]", entry, "clients[0].admin_readers"),
        ("[[endpoints]]", entry, "endpoints[0].admin_readers"),
        ("[response_cache]", "", "response_cache.admin_readers"),
        (
            "[[clients]]\nname = \"grafana\"\nkey = \"dummy-key\"\n\n[clients.guard]",
            "",
            "clients[0].guard.admin_readers",
        ),
    ] {
        let toml_str =
            format!("listen = \"0.0.0.0:8080\"\n\n{header}\n{body}admin_readers = [\"grafana\"]\n");
        let value: toml::Value = toml::from_str(&toml_str).unwrap();
        // Guard against the test rotting into a tautology: assert the key
        // really did nest before asserting that we catch it nesting.
        assert!(
            value.as_table().unwrap().get("admin_readers").is_none(),
            "{header}: expected the key to bind to the table, not the root"
        );
        let err = reject_legacy_config_keys(&value).unwrap_err();
        assert!(err.contains(&format!("`{path}`")), "{header}: {err}");
        assert!(err.contains("TOP-LEVEL"), "{header}: {err}");
    }

    // A client that happens to be NAMED `readers` is a map key in the
    // name-keyed tables, not a misplaced role list.
    let named: toml::Value = toml::from_str(
        r#"
listen = "0.0.0.0:8080"

[client_budgets]
readers = 1000

[client_utilization_limits]
admin_readers = 0.5
"#,
    )
    .unwrap();
    assert!(reject_legacy_config_keys(&named).is_ok());

    // …and the correct placement still boots.
    let ok: toml::Value = toml::from_str(
        r#"
listen = "0.0.0.0:8080"
admin_readers = ["grafana"]

[[clients]]
name = "grafana"
key = "dummy-key"
"#,
    )
    .unwrap();
    assert!(reject_legacy_config_keys(&ok).is_ok());
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

// ── LAB-1192: exposure controls ─────────────────────────────────
//
// Four mechanisms, one posture: unauthenticated is a boot failure not a
// default (AC-1..3), the admin surfaces answer only to an operator
// principal (AC-4..6), behind a trusted load balancer the proxy resolves
// the real client IP or refuses to guess (AC-7..9), and credential
// guessing is throttled per client IP with bounded state (AC-11..12).

// ── AC-3: startup posture ──

#[test]
fn rate_limit_cooldown_rejects_values_outside_one_day() {
    // 0 makes the capacity-429 cooldown a no-op; i64::MAX is the largest value
    // TOML carries and overflows `Instant` at the first capacity 429.
    for secs in ["0", "86401", "9223372036854775807"] {
        let err =
            validate_rate_limit_cooldown(&cfg(&format!("rate_limit_cooldown_secs = {secs}\n")))
                .unwrap_err();
        assert!(err.starts_with("config:"), "{err}");
        assert!(err.contains("rate_limit_cooldown_secs"), "{err}");
        assert!(err.contains("1..=86400"), "{err}");
    }
    let mut config = cfg("");
    config.rate_limit_cooldown_secs = Some(u64::MAX);
    assert!(validate_rate_limit_cooldown(&config).is_err());
}

#[test]
fn rate_limit_cooldown_accepts_one_second_to_one_day_and_the_default() {
    for fragment in [
        "rate_limit_cooldown_secs = 1\n",
        "rate_limit_cooldown_secs = 86400\n",
        "",
    ] {
        assert!(
            validate_rate_limit_cooldown(&cfg(fragment)).is_ok(),
            "{fragment:?}"
        );
    }
}

#[test]
fn auth_throttle_rejects_a_zero_window_while_enabled() {
    let err = validate_auth_throttle(&cfg(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nauth_failure_window_secs = 0\n"
    )))
    .unwrap_err();
    assert!(err.contains("auth_failure_window_secs"), "{err}");
    // Explicitly disabled throttle: a zero window is fine because it is never read.
    assert!(validate_auth_throttle(&cfg(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nauth_failure_limit = 0\nauth_failure_window_secs = 0\n"
    )))
    .is_ok());
}

/// Every rule that used to be a `main()` panic is now reachable from
/// `load_config`, so a test sees what a real boot refuses.
#[test]
fn load_config_refuses_what_main_used_to_panic_on() {
    let base = "listen = \"0.0.0.0:8080\"\nallow_unauthenticated = true\n";
    for (fragment, needle) in [
        ("[client_utilization_limits]\ngeo = 2.0\n", "client_utilization_limits.geo"),
        ("operators = [\"-\"]\n", "operators cannot contain '-'"),
        (
            "[response_cache]\nbackend = \"redis\"\nredis_url = \"redis://localhost\"\nmaster_key = \"00\"\nclients = [\"-\"]\n",
            "response_cache.clients cannot contain",
        ),
    ] {
        let err = match load_config(&format!("{base}{fragment}")) {
            Ok(_) => panic!("must be refused:\n{fragment}"),
            Err(e) => e,
        };
        assert!(err.contains(needle), "expected '{needle}' in: {err}");
    }
}

/// LAB-6939: `models = [""]` is the supported deny-all form, and a production
/// config relies on it as the fail-closed backstop for a read-only principal.
/// Loaded through `load_config`, so a future boot validator that rejects an
/// empty pattern, or a normalise pass that drops it (collapsing `[""]` to `[]`,
/// which allows ALL models), fails here instead of at deploy.
#[test]
fn load_config_boots_and_keeps_an_empty_model_pattern() {
    let toml_str = format!(
        r#"
listen = "0.0.0.0:8080"
admin_readers = ["ops-console"]

[[endpoints]]
name = "primary"
token = "sk-ant-test"

[[clients]]
name = "ops-console"
key = "{STRONG_KEY}"
models = [""]

[[clients]]
name = "geo"
key = "{STRONG_KEY}-geo"
"#
    );
    let (_, _, policy) = load_config(&toml_str)
        .unwrap_or_else(|e| panic!("a models = [\"\"] client must boot: {e}"));
    let ops = policy
        .clients
        .iter()
        .find(|c| c.name == "ops-console")
        .unwrap();
    assert_eq!(ops.models, vec![String::new()]);
}

/// A planted secret shares no 8-character run with `err`. Version-neutral: it
/// asserts on what the message must not hold, never on toml's wording.
fn assert_no_secret_fragment(err: &str, secret: &str) {
    let chars: Vec<char> = secret.chars().collect();
    for w in chars.windows(8) {
        let frag: String = w.iter().collect();
        assert!(
            !err.contains(&frag),
            "error quotes '{frag}' of the secret: {err}"
        );
    }
}

/// A syntax error on a secret line names where it is, not what it holds.
#[test]
fn load_config_syntax_error_names_line_and_column_not_the_value() {
    let raw =
        format!("listen = \"0.0.0.0:8080\"\n\n[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\n");
    let err = match load_config(&raw) {
        Ok(_) => panic!("an unterminated string must be refused"),
        Err(e) => e,
    };
    assert_no_secret_fragment(&err, STRONG_KEY);
    assert!(
        err.contains("line 5, column "),
        "expected the line and column: {err}"
    );
}

/// A value of the wrong type is refused without echoing the value.
#[test]
fn load_config_type_error_does_not_echo_the_value() {
    let raw = format!("listen = \"0.0.0.0:8080\"\nallow_unauthenticated = \"{STRONG_KEY}\"\n");
    let err = match load_config(&raw) {
        Ok(_) => panic!("a string where a bool belongs must be refused"),
        Err(e) => e,
    };
    assert_no_secret_fragment(&err, STRONG_KEY);
}

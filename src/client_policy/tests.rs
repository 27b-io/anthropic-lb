use super::*;

/// `ClientPolicy::build`'s refusal of a TOML fragment: the boot error.
fn refused(fragment: &str) -> String {
    match ClientPolicy::build(&cfg(fragment)) {
        Ok(_) => panic!("the build must refuse:\n{fragment}"),
        Err(e) => e,
    }
}

/// The policy `build` makes of a fragment it must accept.
fn built(fragment: &str) -> (AuthMode, BuiltPolicy) {
    ClientPolicy::build(&cfg(fragment))
        .unwrap_or_else(|e| panic!("the build must accept: {e}\n{fragment}"))
}

/// Two distinct keys that clear `MIN_KEY_LEN`, so a fragment breaks only the
/// rule its test names.
const K1: &str = "k1-0123456789abcdef0123456789abcdef";
const K2: &str = "k2-0123456789abcdef0123456789abcdef";
const K3: &str = "k3-0123456789abcdef0123456789abcdef";

const RC_BLOCK: &str = "\n[response_cache]\nbackend = \"redis\"\nredis_url = \"redis://localhost\"\nmaster_key = \"0000000000000000000000000000000000000000000000000000000000000000\"\n";

// ── `[[clients]]` structure ──

#[test]
fn build_rejects_duplicate_names_and_keys() {
    let err = refused(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n\n[[clients]]\nname = \"geo\"\nkey = \"{K2}\"\n"
    ));
    assert!(err.contains("duplicate name"), "{err}");

    let err = refused(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n\n[[clients]]\nname = \"radar\"\nkey = \"{K1}\"\n"
    ));
    assert!(err.contains("duplicate key"), "{err}");
    assert!(
        !err.contains("k1-"),
        "the error must not echo the credential, or any of it: {err}"
    );
}

#[test]
fn build_rejects_bad_names_and_empty_keys() {
    let client =
        |name: &str, key: &str| format!("[[clients]]\nname = \"{name}\"\nkey = \"{key}\"\n");
    // `allow_unauthenticated` keeps the client_names rows boot-valid otherwise,
    // so each row is refused for its own rule.
    let names = |value: &str| {
        format!("allow_unauthenticated = true\n[client_names]\n\"10.0.0.5\" = \"{value}\"\n")
    };
    for (fragment, needle) in [
        (client("", K1), "name"),
        (client("   ", K1), "name"),
        (client("-", K1), "sentinel"),
        // Reserved: /_stats and /metrics rewrite operator identities to the
        // literal "_operator", so a real tenant with that name would silently
        // merge with the aggregated operator bucket.
        (client("_operator", K1), "operator-aggregation"),
        // Reserved: the metrics overflow bucket is keyed ("_other", "_other");
        // a real client with that name could pre-create the exact pair and
        // later overflow denials/usage would merge into it (CodeRabbit, #148).
        (client("_other", K1), "overflow-bucket"),
        // The legacy client_names IP map is the third identity entry point
        // (resolve_client_id's fallback) — its values must not claim a
        // reserved sentinel either (#148 follow-up).
        (names("-"), "reserved"),
        (names("_operator"), "reserved"),
        (names("_other"), "reserved"),
        (names(""), "non-empty"),
        (names("alice "), "whitespace"),
        (client("geo", ""), "key must not be empty"),
        // Untrimmed: stored verbatim, so it would become a client_id matching
        // no client_budgets / operators / response_cache.clients key.
        (client(" geo", K1), "whitespace"),
        (client("geo ", K1), "whitespace"),
    ] {
        let err = refused(&fragment);
        assert!(err.contains(needle), "expected '{needle}' in: {err}");
    }
}

#[test]
fn build_accepts_a_well_formed_table() {
    built(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\nmodels = [\"claude-haiku-*\"]\n\n[[clients]]\nname = \"radar\"\nkey = \"{K2}\"\n"
    ));
}

/// One client registry, not five. Each of these config surfaces keys on a
/// client name and each fails SILENTLY on a typo — a mistyped budget means
/// UNLIMITED spend (`check_budget` returns Ok for an unknown client), a
/// mistyped operator silently gates the caller it meant to exempt, a mistyped
/// cache client silently disables the cache.
#[test]
fn build_rejects_any_surface_naming_no_configured_client() {
    // NOTE: top-level scalars/arrays must precede the first table header —
    // a bare `operators = [...]` written after `[[clients]]` binds to that
    // table instead and is silently dropped, making the test pass vacuously.
    let base = format!("\n[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n");
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
        let err = refused(&fragment);
        assert!(err.contains(surface), "expected '{surface}' in: {err}");
        assert!(err.contains("geo-pipeline"), "must name the typo: {err}");
    }
}

#[test]
fn build_accepts_every_surface_naming_a_configured_client() {
    let fragment = format!(
        "operators = [\"geo\"]\n\n[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n\n[client_budgets]\ngeo = 100\n\n[client_utilization_limits]\ngeo = 0.5\n{RC_BLOCK}clients = [\"geo\"]\n"
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
    assert!(ClientPolicy::build(&parsed).is_ok());
}

/// On the legacy path there is no registry to check against, so none of the
/// cross-checks may fire — existing configs must keep booting.
#[test]
fn build_skips_all_crosschecks_without_a_client_table() {
    built(&format!(
        "proxy_key = \"{STRONG_KEY}\"\noperators = [\"anything\"]\n\n[client_budgets]\nanything = 100\n{RC_BLOCK}clients = [\"anything\"]\n"
    ));
}

/// AC-3 (LAB-4395): `admin_readers` is bound by the same registry rule as every
/// other name-keyed surface — a typo would silently create a role nobody
/// holds, leaving the credential it was meant to scope on its old one.
#[test]
fn build_rejects_a_reader_naming_no_configured_client() {
    let err = refused(&format!(
        "admin_readers = [\"grafanna\"]\n\n[[clients]]\nname = \"grafana\"\nkey = \"{K1}\"\n"
    ));
    assert!(
        err.contains("admin_readers"),
        "must name the surface: {err}"
    );
    assert!(err.contains("grafanna"), "must name the typo: {err}");
}

/// AC-3: one name, one role. `operators` bypasses every policy and `admin_readers`
/// is refused every request — a name in both is a config whose author meant
/// one of two opposite things, so it fails at boot instead of silently
/// resolving to either.
#[test]
fn build_rejects_a_name_in_both_operators_and_readers() {
    let err = refused(&format!(
        "operators = [\"ops\"]\nadmin_readers = [\"ops\"]\n\n[[clients]]\nname = \"ops\"\nkey = \"{K1}\"\n"
    ));
    assert!(err.contains("admin_readers"), "{err}");
    assert!(err.contains("operators"), "{err}");
    assert!(err.contains("ops"), "must name the offending name: {err}");
}

/// Unlike the other cross-checks, this one fires on the LEGACY path too:
/// with one shared `proxy_key` the key holder is the operator by construction
/// and proxy-path client ids are caller-asserted, so a `admin_readers` entry would
/// restrict nobody while reading as though it scoped something.
#[test]
fn build_rejects_readers_without_a_client_table() {
    let err = refused(&format!(
        "proxy_key = \"{STRONG_KEY}\"\nadmin_readers = [\"grafana\"]\n"
    ));
    assert!(err.contains("admin_readers"), "{err}");
    assert!(
        err.contains("[[clients]]"),
        "must say what it requires: {err}"
    );
}

/// The happy path: disjoint roles over configured clients boot fine.
#[test]
fn build_accepts_disjoint_operator_and_reader_roles() {
    let fragment = format!(
        "operators = [\"ops\"]\nadmin_readers = [\"grafana\", \"vmagent\"]\n\n[[clients]]\nname = \"ops\"\nkey = \"{K1}\"\n\n[[clients]]\nname = \"grafana\"\nkey = \"{K2}\"\n\n[[clients]]\nname = \"vmagent\"\nkey = \"{K3}\"\n"
    );
    let parsed = cfg(&fragment);
    // Guard against the vacuous-pass trap: top-level arrays written after a
    // table header bind to that table and vanish silently.
    assert_eq!(
        parsed.admin_readers,
        vec!["grafana".to_string(), "vmagent".to_string()]
    );
    assert!(ClientPolicy::build(&parsed).is_ok());
}

/// `passthrough` forwards the caller's auth headers upstream untouched. Under
/// `[[clients]]` those headers carry the client's PROXY key, so the two modes
/// together would transmit every client key to the upstream.
#[test]
fn build_rejects_passthrough_endpoint_alongside_clients() {
    let err = refused(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n\n[[endpoints]]\nname = \"managed\"\ntoken = \"passthrough\"\n"
    ));
    assert!(err.contains("passthrough"), "{err}");
    assert!(err.contains("managed"), "must name the endpoint: {err}");
}

/// …but passthrough on the legacy path is untouched.
#[test]
fn build_allows_passthrough_without_a_client_table() {
    built(&format!(
        "proxy_key = \"{STRONG_KEY}\"\n\n[[endpoints]]\nname = \"managed\"\ntoken = \"passthrough\"\n"
    ));
}

// ── The name rule: ^[a-z0-9][a-z0-9_-]{0,62}$ ──

/// `build` on one `[[clients]]` entry named `name`, built in Rust so no TOML
/// escaping stands between the test and the name.
fn build_one_named(name: &str) -> Result<(AuthMode, BuiltPolicy), String> {
    let mut config = cfg("");
    config.clients = vec![mk_client(name, K1, &[])];
    ClientPolicy::build(&config)
}

#[test]
fn build_accepts_names_inside_the_rule() {
    let longest = "a".repeat(63);
    for name in [
        "a",
        "0",
        "geo",
        "build-bot",
        "team_alpha",
        "9lives",
        "a-",
        "a_",
        longest.as_str(),
    ] {
        if let Err(e) = build_one_named(name) {
            panic!("{name:?} must pass the name rule: {e}");
        }
    }
}

#[test]
fn build_rejects_names_outside_the_rule() {
    let too_long = "a".repeat(64);
    for name in [
        "Geo",
        "GEO",
        "geo.pipeline",
        "geo pipeline",
        "geo/pipeline",
        "geo:pipeline",
        "-geo",
        "\u{e9}",
        "geo\u{0}",
        "geo\u{2028}x",
        too_long.as_str(),
    ] {
        let Err(err) = build_one_named(name) else {
            panic!("{name:?} must fail the name rule");
        };
        assert!(
            err.contains("^[a-z0-9][a-z0-9_-]{0,62}$"),
            "{name:?}: {err}"
        );
        assert!(
            err.starts_with(&format!("config: client {name:?}: ")),
            "{name:?}: {err}"
        );
    }
}

/// `_operator` and `_other` keep their own messages; every other leading `_`
/// is reserved too.
#[test]
fn build_reserves_every_name_starting_with_an_underscore() {
    for name in ["_dynamic", "_", "__other", "_geo"] {
        let Err(err) = build_one_named(name) else {
            panic!("{name:?} must be reserved");
        };
        assert!(err.contains("reserved"), "{name}: {err}");
    }
}

/// The rule is for `[[clients]]` names only. `client_names` values and
/// `x-client-id` are legacy identity paths with their own, older rules.
#[test]
fn name_rule_leaves_the_legacy_identity_paths_alone() {
    built("allow_unauthenticated = true\n[client_names]\n\"10.0.0.5\" = \"Build.Box\"\n");
    let state = test_state_with(vec![]);
    let headers = hdrs(&[("x-client-id", "Team.Alpha")]);
    assert_eq!(state.resolve_client_id(&test_ip(), &headers), "Team.Alpha");
}

// ── Credential posture ──

#[test]
fn build_rejects_a_credential_free_config_by_default() {
    let err = refused("");
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
fn build_accepts_the_explicit_escape_hatch() {
    built("allow_unauthenticated = true\n");
    // `false` is NOT an escape hatch — omission and explicit false are the
    // same default-deny.
    refused("allow_unauthenticated = false\n");
}

#[test]
fn build_accepts_either_credential_form() {
    built(&format!("proxy_key = \"{STRONG_KEY}\"\n"));
    built(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\"\n"
    ));
}

#[test]
fn build_rejects_the_flag_alongside_credentials() {
    let err = refused(&format!(
        "allow_unauthenticated = true\nproxy_key = \"{STRONG_KEY}\"\n"
    ));
    assert!(err.contains("incompatible"), "{err}");
    let err = refused(&format!(
        "allow_unauthenticated = true\n[[clients]]\nname = \"geo\"\nkey = \"{STRONG_KEY}\"\n"
    ));
    assert!(err.contains("incompatible"), "{err}");
}

/// AC-13: every configured credential form is held to the 32-char floor, and
/// the error carries the generation command.
#[test]
fn build_rejects_short_credentials_with_the_generation_command() {
    for fragment in [
        "proxy_key = \"short\"\n".to_string(),
        "[[clients]]\nname = \"geo\"\nkey = \"short\"\n".to_string(),
        // 31 chars — one under the floor, so the boundary is pinned.
        format!("proxy_key = \"{}\"\n", "a".repeat(31)),
        format!(
            "[[clients]]\nname = \"geo\"\nkey = \"{}\"\n",
            "a".repeat(31)
        ),
    ] {
        let err = refused(&fragment);
        assert!(
            err.contains("openssl rand -hex 32"),
            "error must carry the generation command: {err}"
        );
    }
    // 32 exactly passes.
    built(&format!("proxy_key = \"{}\"\n", "a".repeat(32)));
    built(&format!(
        "[[clients]]\nname = \"geo\"\nkey = \"{}\"\n",
        "a".repeat(32)
    ));
}

// ── Value rules that used to panic in main() ──

#[test]
fn build_rejects_a_utilization_limit_outside_zero_to_one() {
    for limit in ["1.5", "-0.1", "nan", "inf"] {
        let err = refused(&format!(
            "allow_unauthenticated = true\n[client_utilization_limits]\ngeo = {limit}\n"
        ));
        assert!(
            err.contains("client_utilization_limits.geo: must be 0.0-1.0"),
            "{limit}: {err}"
        );
    }
    for limit in ["0.0", "0.5", "1.0"] {
        built(&format!(
            "allow_unauthenticated = true\n[client_utilization_limits]\ngeo = {limit}\n"
        ));
    }
}

/// Without `[[clients]]` operator names are header-derived, so no registry
/// check stands between `-` and the unknown-client sentinel.
#[test]
fn build_rejects_the_sentinel_in_operators() {
    let err = refused("allow_unauthenticated = true\noperators = [\"-\"]\n");
    assert_eq!(
        err,
        "operators cannot contain '-' (the unknown-client sentinel)"
    );
}

/// As a cache tenant, `-` would opt in every unidentified caller.
#[test]
fn build_rejects_the_sentinel_in_response_cache_clients() {
    let err = refused(&format!(
        "allow_unauthenticated = true\n{RC_BLOCK}clients = [\"-\"]\n"
    ));
    assert!(err.starts_with("response_cache config error:"), "{err}");
    assert!(err.contains("sentinel"), "{err}");
}

// ── What the build produces ──

#[test]
fn build_decides_the_auth_mode_once_from_the_config() {
    let (mode, _) = built("allow_unauthenticated = true\n");
    assert!(matches!(mode, AuthMode::Open));

    let (mode, policy) = built(&format!("proxy_key = \"{STRONG_KEY}\"\n"));
    assert!(
        matches!(mode, AuthMode::SharedKey(d) if d == key_digest(STRONG_KEY)),
        "the shared key is held as its digest"
    );
    assert!(policy.principals.is_empty());

    let (mode, policy) = built(&format!("[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n"));
    assert!(matches!(mode, AuthMode::Clients));
    assert_eq!(policy.principals.len(), 1);
}

/// Each key is held as its digest, mapped to its principal with the role its
/// name holds.
#[test]
fn build_maps_each_key_digest_to_its_principal_and_role() {
    let (_, policy) = built(&format!(
        "operators = [\"ops\"]\nadmin_readers = [\"viewer\"]\n\n[[clients]]\nname = \"ops\"\nkey = \"{K1}\"\n\n[[clients]]\nname = \"viewer\"\nkey = \"{K2}\"\n\n[[clients]]\nname = \"geo\"\nkey = \"{K3}\"\nmodels = [\"claude-haiku-*\"]\n"
    ));
    let principal = |key: &str| policy.principals.get(&key_digest(key)).cloned();
    let expect = |name: &str, role| {
        Some(Principal {
            name: name.to_string(),
            role,
        })
    };
    assert_eq!(principal(K1), expect("ops", Role::Operator));
    assert_eq!(principal(K2), expect("viewer", Role::Reader));
    assert_eq!(principal(K3), expect("geo", Role::Client));
    assert_eq!(policy.principals.len(), 3);

    // The keyless entries keep everything else the table said.
    let geo = policy.clients.iter().find(|c| c.name == "geo").unwrap();
    assert_eq!(geo.models, vec!["claude-haiku-*".to_string()]);
    assert_eq!(policy.operators, vec!["ops".to_string()]);
    assert_eq!(policy.admin_readers, vec!["viewer".to_string()]);
}

/// The digest is the SHA-256 of the key's text, the digest a client store
/// keeps for the keys it mints (the corpus's `key_digests` cases check that
/// side), and it shares nothing with the fingerprint a rejected credential is
/// logged under.
#[test]
fn key_digest_is_the_sha256_of_the_key_and_unrelated_to_the_logged_fingerprint() {
    // FIPS 180-2, appendix B.1.
    assert_eq!(
        digest_hex(&key_digest("abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let key = test_key("geo");
    assert_ne!(
        credential_fingerprint(Some(key.as_bytes())),
        digest_hex(&key_digest(&key))[..12],
        "a logged fingerprint must not be a prefix of the stored digest"
    );
}

/// LAB-6939: `models = [""]` is the supported deny-all form, and a production
/// config relies on it as the fail-closed backstop for a read-only principal.
/// The build must keep it, not normalise it to `[]` (which allows ALL models).
#[test]
fn build_keeps_an_empty_model_pattern() {
    let (_, policy) = built(&format!(
        "admin_readers = [\"scraper\"]\n\n[[clients]]\nname = \"scraper\"\nkey = \"{K1}\"\nmodels = [\"\"]\n"
    ));
    let scraper = policy.clients.iter().find(|c| c.name == "scraper").unwrap();
    assert_eq!(scraper.models, vec![String::new()]);
}

// ── One snapshot per request ──

/// The stream every `spawn_held_upstream` response carries: 10 input and 5
/// output tokens.
const HELD_STREAM: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_held\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);
/// The tokens each `spawn_held_upstream` response reports.
const HELD_TOKENS: u64 = 15;
/// A streaming request body both proxied routes accept.
const STREAM_BODY: &str = r#"{"model":"claude-sonnet-4-6","max_tokens":50,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

/// An upstream whose first request notifies `started` and then waits for
/// `release` before the response begins; later requests are answered at
/// once. Every response reports 75% five-hour utilization, with the reset
/// hours away so no near-reset discount applies. A request that reaches it is
/// past authentication, the gate and routing, but its response headers, its
/// relay and its usage are all still to come.
async fn spawn_held_upstream(
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
) -> String {
    let first = Arc::new(AtomicBool::new(true));
    let app = Router::new().fallback(any(move || {
        let (started, release) = (started.clone(), release.clone());
        let hold = first.swap(false, Ordering::SeqCst);
        async move {
            if hold {
                started.notify_one();
                release.notified().await;
            }
            let reset = (AppState::now_epoch() + 4 * 3600).to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header(
                    "anthropic-ratelimit-unified-representative-claim",
                    "five_hour",
                )
                .header("anthropic-ratelimit-unified-5h-utilization", "0.75")
                .header("anthropic-ratelimit-unified-5h-reset", reset)
                .header("anthropic-ratelimit-unified-5h-status", "allowed")
                .body(Body::from(HELD_STREAM))
                .unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// POST `STREAM_BODY` to `route` with `key` and read the whole response.
/// Returns its status and its `x-budget-status`.
async fn post_stream(
    addr: SocketAddr,
    route: &'static str,
    key: String,
) -> (reqwest::StatusCode, Option<String>) {
    let resp = Client::new()
        .post(format!("http://{addr}{route}"))
        .header("x-api-key", key)
        .header("content-type", "application/json")
        .body(STREAM_BODY)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let pressure = resp
        .headers()
        .get("x-budget-status")
        .map(|v| v.to_str().unwrap().to_string());
    resp.bytes().await.unwrap();
    (status, pressure)
}

/// Wait until `name`'s recorded token total reaches `total`: a stream's usage
/// is recorded by its relay task as the stream ends.
async fn wait_for_usage(state: &AppState, name: &str, total: u64) {
    let recorded = || {
        state
            .client_usage
            .lock()
            .unwrap()
            .get(name)
            .map_or(0, |t| t.iter().sum::<u64>())
    };
    for _ in 0..200 {
        if recorded() >= total {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(recorded(), total, "{name}'s usage was not recorded");
}

/// A request keeps the policy snapshot it authenticated against, to its end,
/// across a swap that lands while it waits for the upstream; the requests
/// after the swap read the new policy. The swap gives the client a new key
/// and takes away its budget and its utilization ceiling. The held request
/// still shows the old snapshot in the two decisions it makes after the
/// swap: the pressure header (75% utilization is "elevated", raised to
/// "critical" by a 0.9 ceiling) and the budget its usage is charged to when
/// the stream ends. Both proxied routes, since each makes those decisions
/// in its own forward path and relay task.
#[tokio::test]
async fn a_request_keeps_its_snapshot_across_a_swap_mid_request() {
    for route in ["/v1/messages", "/v1/chat/completions"] {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mut ep = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
        ep.base_url = spawn_held_upstream(started.clone(), release.clone()).await;
        let (old_key, new_key) = (test_key("alpha-old"), test_key("alpha-new"));
        let state = Arc::new(
            TestPolicy::clients(vec![mk_client("team-alpha", &old_key, &[])])
                .budgets([("team-alpha".to_string(), 1_000_000)].into())
                .utilization_limits([("team-alpha".to_string(), 0.9)].into())
                .install(AppState {
                    endpoints: vec![ep],
                    ..test_state_base()
                }),
        );
        let addr = serve(build_router(state.clone())).await;
        let budget_used = || {
            state
                .budget_usage
                .lock()
                .unwrap()
                .get("team-alpha")
                .map(|(_, used)| *used)
        };

        let held = tokio::spawn(post_stream(addr, route, old_key.clone()));
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("the request never reached the upstream");
        TestPolicy::clients(vec![mk_client("team-alpha", &new_key, &[])]).swap_into(&state);
        release.notify_one();
        let (status, pressure) = held.await.unwrap();
        assert_eq!(status, reqwest::StatusCode::OK, "{route}");
        assert_eq!(
            pressure.as_deref(),
            Some("critical"),
            "{route}: the held request's pressure header applies the ceiling of the \
             snapshot it authenticated against"
        );
        wait_for_usage(&state, "team-alpha", HELD_TOKENS).await;
        assert_eq!(
            budget_used(),
            Some(HELD_TOKENS),
            "{route}: the held request is charged under the budget of the snapshot it \
             authenticated against, which the current policy no longer has"
        );

        assert_eq!(
            post_stream(addr, route, old_key).await.0,
            reqwest::StatusCode::UNAUTHORIZED,
            "{route}: a request after the swap is authenticated against the new policy"
        );
        let (status, pressure) = post_stream(addr, route, new_key).await;
        assert_eq!(status, reqwest::StatusCode::OK, "{route}");
        assert_eq!(
            pressure.as_deref(),
            Some("elevated"),
            "{route}: a request after the swap has no ceiling to raise its pressure"
        );
        wait_for_usage(&state, "team-alpha", 2 * HELD_TOKENS).await;
        assert_eq!(
            budget_used(),
            Some(HELD_TOKENS),
            "{route}: a request after the swap is charged under the new policy, with no budget"
        );
    }
}

/// Only a `[[clients]]` stack takes a new policy. Under a shared key or an
/// open proxy a caller's name is its own claim, so a client table swapped in
/// there would hand any header-chosen name that client's models, pins and
/// guard policy: the swap is refused and the policy left as it was.
#[test]
fn a_swap_is_refused_unless_callers_authenticate_with_client_keys() {
    let table = || {
        ClientPolicy::unchecked(&cfg(&format!(
            "[[clients]]\nname = \"team-alpha\"\nkey = \"{K1}\"\nmodels = [\"claude-haiku-*\"]\n"
        )))
        .1
    };
    for state in [
        TestPolicy::open().install(test_state_base()),
        TestPolicy::shared_key(&test_key("shared")).install(test_state_base()),
    ] {
        let before = state.current_client_policy();
        assert!(state.swap_client_policy(table()).is_err());
        assert!(
            Arc::ptr_eq(&before, &state.current_client_policy()),
            "the policy is left as it was"
        );
        assert!(
            state.caller("team-alpha").entry().is_none(),
            "a header-chosen name gains no client entry"
        );
    }
}

/// After a swap that removes, changes and adds budgets, every exported
/// budget figure for a client (the limit and remaining gauges on `/metrics`,
/// and the `/_stats` budget) follows the swap and agrees with the limit the
/// gate enforces. A per-client lookup that did not follow the swap, on
/// either side, shows here as a gauge the gate disagrees with, rather than
/// as a silently stale dashboard. The clock is frozen mid-day, so the usage
/// set here and the day the handlers read are one day.
#[tokio::test]
async fn every_budget_gauge_shows_the_limit_the_gate_enforces_after_a_swap() {
    let _clock = FrozenClock::at(1_704_110_400);
    let (before, after) = budget_swap_policies();
    let state = Arc::new(before.install(test_state_base()));
    let today = AppState::now_epoch() / 86400;
    let used_before: HashMap<&str, u64> = [
        ("team-alpha", 700),
        ("team-bravo", 1_200),
        ("team-charlie", 300),
    ]
    .into();
    for (name, used) in &used_before {
        state
            .budget_usage
            .lock()
            .unwrap()
            .insert(name.to_string(), (today, *used));
    }
    after.swap_into(&state);

    let addr = serve(build_router(state.clone())).await;
    let get = |path: &str| {
        Client::new()
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", test_key("ops"))
            .send()
    };
    let metrics = get("/metrics").await.unwrap().text().await.unwrap();
    let stats: serde_json::Value = get("/_stats").await.unwrap().json().await.unwrap();
    let limits = metric_by_client(&metrics, "anthropic_client_budget_limit");
    let remaining = metric_by_client(&metrics, "anthropic_client_budget_remaining");
    assert_eq!(
        limits,
        [
            ("team-bravo".to_string(), 5_000),
            ("team-charlie".to_string(), 300)
        ]
        .into(),
        "the gauges follow the swap"
    );

    for name in ["ops", "team-alpha", "team-bravo", "team-charlie"] {
        let caller = state.caller(name);
        let set_used = |used: u64| {
            state
                .budget_usage
                .lock()
                .unwrap()
                .insert(name.to_string(), (today, used));
        };
        match limits.get(name).copied() {
            Some(limit) => {
                assert_eq!(
                    stats["client_budgets"][name]["daily_limit"], limit,
                    "{name}"
                );
                assert_eq!(
                    remaining.get(name).copied(),
                    Some(limit.saturating_sub(used_before[name])),
                    "{name}"
                );
                set_used(limit - 1);
                assert!(
                    state.check_budget(&caller).await.is_ok(),
                    "{name}: the gate admits below the gauge's limit"
                );
                set_used(limit);
                assert!(
                    state.check_budget(&caller).await.is_err(),
                    "{name}: the gate refuses at the gauge's limit"
                );
            }
            None => {
                assert!(stats["client_budgets"].get(name).is_none(), "{name}");
                assert!(!remaining.contains_key(name), "{name}");
                set_used(u64::MAX - 1);
                assert!(
                    state.check_budget(&caller).await.is_ok(),
                    "{name}: no gauge, so the gate sets no limit"
                );
            }
        }
    }
}

// ── [client_store] ───────────────────────────────────────────────────

/// One `[[clients]]` entry, so a fragment breaks only the rule its test names.
fn one_client() -> String {
    format!("\n[[clients]]\nname = \"geo\"\nkey = \"{K1}\"\n")
}

/// A client store's clients are principals of a client key; under a shared
/// key or an open proxy a caller's name is its own claim.
#[test]
fn build_rejects_a_client_store_without_a_client_table() {
    let store = "\n[client_store]\npoll_url = \"https://store.example/_clients/poll\"\n";
    for credentials in [
        format!("proxy_key = \"{K1}\"\n"),
        "allow_unauthenticated = true\n".to_string(),
    ] {
        let err = refused(&format!("{credentials}{store}"));
        assert!(
            err.contains("client_store") && err.contains("[[clients]]"),
            "{err}"
        );
    }
}

#[test]
fn build_rejects_a_poll_url_or_token_it_cannot_use_safely() {
    for (store, expect) in [
        ("poll_url = \"store.example/_clients/poll\"", "not a URL"),
        (
            "poll_url = \"ftp://store.example/poll\"",
            "http:// or https://",
        ),
        (
            "poll_url = \"http://store.example/poll\"\ntoken = \"reader-token\"",
            "only over https://",
        ),
        (
            "poll_url = \"https://store.example/poll\"\ntoken = \"\"",
            "must not be empty",
        ),
        (
            "poll_url = \"http://reader:reader-token@store.example/poll\"",
            "must not carry credentials",
        ),
        (
            "poll_url = \"https://reader-token@store.example/poll\"",
            "must not carry credentials",
        ),
    ] {
        let err = refused(&format!("{}\n[client_store]\n{store}\n", one_client()));
        assert!(err.contains(expect), "expected '{expect}' in: {err}");
        assert!(!err.contains("reader-token"), "never echo a token: {err}");
    }
}

/// The policy a boot installs has the store's gate closed, so nothing waits
/// on the reader to start before ordinary clients are refused.
#[test]
fn build_closes_the_gate_of_a_client_store() {
    for store in [
        "poll_url = \"http://store.internal/_clients/poll\"",
        "poll_url = \"https://store.example/_clients/poll\"\ntoken = \"reader-token\"",
    ] {
        let (_, policy) = built(&format!("{}\n[client_store]\n{store}\n", one_client()));
        let gate = policy.store.as_ref().expect("a client store gate");
        assert!(!gate.complete && !gate.loaded);
        let principal = Principal {
            name: "geo".to_string(),
            role: Role::Client,
        };
        assert_eq!(
            policy.store_refusal(&principal),
            Some(StoreRefusal::Unloaded)
        );
    }
    let (_, policy) = built(&one_client());
    assert!(policy.store.is_none(), "no store, no gate");
}

/// Under a client store a response-cache tenant may name a client the store
/// will bring; it must still be a client name.
#[test]
fn a_response_cache_tenant_may_name_a_store_client_only_under_a_client_store() {
    let store = "\n[client_store]\npoll_url = \"https://store.example/_clients/poll\"\n";
    built(&format!(
        "{}{store}{RC_BLOCK}clients = [\"geo\", \"store-only\"]\n",
        one_client()
    ));
    let err = refused(&format!(
        "{}{store}{RC_BLOCK}clients = [\"Not-A-Name\"]\n",
        one_client()
    ));
    assert!(
        err.contains("response_cache.clients") && err.contains("Not-A-Name"),
        "{err}"
    );
    let err = refused(&format!(
        "{}{RC_BLOCK}clients = [\"store-only\"]\n",
        one_client()
    ));
    assert!(
        err.contains("names no configured [[clients]] entry"),
        "{err}"
    );
}

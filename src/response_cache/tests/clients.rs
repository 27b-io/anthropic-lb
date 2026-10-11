use super::*;

// ── AC-1: distinct key → distinct identity, with no x-client-id ──

#[test]
fn distinct_client_keys_resolve_to_distinct_identities() {
    let state = state_with_clients(vec![
        mk_client("geo", &test_key("key-geo"), &[]),
        mk_client("radar", &test_key("key-radar"), &[]),
    ]);
    let ip = test_ip();

    for (key, expected) in [("key-geo", "geo"), ("key-radar", "radar")] {
        let headers = hdrs(&[("x-api-key", &test_key(key))]);
        let auth = state
            .authenticate(&headers, false)
            .expect("configured key must authenticate");
        let principal = auth
            .principal
            .as_ref()
            .expect("a [[clients]] match must yield a principal");
        assert_eq!(principal.name, expected);
        // And the identity the rest of the proxy sees follows it — with NO
        // x-client-id header present on either request.
        let rctx = RequestContext::from_request(&state, &ip, &headers, auth);
        assert_eq!(rctx.caller.name(), expected);
    }
}

// ── AC-2: unknown / missing credential → 401 ──

#[test]
fn unknown_client_key_is_rejected() {
    let state = state_with_clients(vec![mk_client("geo", &test_key("key-geo"), &[])]);
    let resp = state
        .authenticate(&hdrs(&[("x-api-key", &test_key("key-wrong"))]), false)
        .expect_err("unknown key must not authenticate");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn missing_credential_is_rejected_when_clients_configured() {
    let state = state_with_clients(vec![mk_client("geo", &test_key("key-geo"), &[])]);
    let resp = state
        .authenticate(&hyper::HeaderMap::new(), false)
        .expect_err("absent credential must not authenticate");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// A near-miss must not authenticate. Guards the digest match against a
/// length- or prefix-tolerant rewrite. Every miss but the empty one is long
/// enough to reach the lookup.
#[test]
fn client_key_match_is_exact_not_prefix() {
    let key = test_key("key-geo");
    let state = state_with_clients(vec![mk_client("geo", &key, &[])]);
    for wrong in [
        key[..key.len() - 1].to_string(),
        format!("{key} "),
        format!("{key}x"),
        format!("x{key}"),
        String::new(),
        key.to_uppercase(),
    ] {
        assert!(
            state
                .authenticate(&hdrs(&[("x-api-key", &wrong)]), false)
                .is_err(),
            "'{wrong}' must not authenticate as '{key}'"
        );
    }
}

// ── AC-2: bearer accepted on the OpenAI-compat surface only ──

#[test]
fn bearer_credential_accepted_only_where_enabled() {
    let state = state_with_clients(vec![mk_client("geo", &test_key("key-geo"), &[])]);
    let headers = hdrs(&[("authorization", &format!("Bearer {}", test_key("key-geo")))]);

    let principal = state
        .authenticate(&headers, true)
        .expect("bearer must authenticate where enabled")
        .principal
        .expect("principal");
    assert_eq!(principal.name, "geo");

    // The native surface may carry the caller's OWN upstream token in
    // `Authorization` (passthrough endpoints), so bearer is not accepted there.
    assert!(state.authenticate(&headers, false).is_err());
}

#[test]
fn bearer_scheme_match_is_case_insensitive() {
    let state = state_with_clients(vec![mk_client("geo", &test_key("key-geo"), &[])]);
    let headers = hdrs(&[("authorization", &format!("bEaReR {}", test_key("key-geo")))]);
    assert_eq!(
        state
            .authenticate(&headers, true)
            .unwrap()
            .principal
            .unwrap()
            .name,
        "geo"
    );
}

// ── AC-4: the credential wins over a spoofed x-client-id ──

#[test]
fn spoofed_x_client_id_is_ignored_under_clients_table() {
    let state = state_with_clients(vec![
        mk_client("alpha", &test_key("key-alpha"), &[]),
        mk_client("bravo", &test_key("key-bravo"), &[]),
    ]);
    let ip = test_ip();
    let headers = hdrs(&[
        ("x-api-key", &test_key("key-alpha")),
        ("x-client-id", "bravo"),
    ]);

    let auth = state.authenticate(&headers, false).unwrap();
    assert_eq!(auth.principal.as_ref().unwrap().name, "alpha");

    let rctx = RequestContext::from_request(&state, &ip, &headers, auth);
    assert_eq!(
        rctx.caller.name(),
        "alpha",
        "authenticated principal must win over the x-client-id header"
    );
}

/// The `client_names` IP map is the other client-influenced identity source.
/// It must also lose to the credential.
#[test]
fn client_names_ip_map_is_ignored_under_clients_table() {
    let mut client_names = HashMap::new();
    client_names.insert(TEST_IP.to_string(), "from-ip-map".to_string());
    let state = Arc::new(
        TestPolicy::clients(vec![mk_client("alpha", &test_key("key-alpha"), &[])]).install(
            AppState {
                client_names,
                ..test_state_base()
            },
        ),
    );
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", &test_key("key-alpha"))]);
    let auth = state.authenticate(&headers, false).unwrap();
    assert!(auth.principal.is_some());
    let rctx = RequestContext::from_request(&state, &ip, &headers, auth);
    assert_eq!(rctx.caller.name(), "alpha");
}

// ── AC-5: legacy proxy_key path unchanged ──

#[test]
fn legacy_proxy_key_still_authenticates_and_yields_no_principal() {
    let state =
        Arc::new(TestPolicy::shared_key(&test_key("shared-secret")).install(test_state_base()));

    let principal = state
        .authenticate(&hdrs(&[("x-api-key", &test_key("shared-secret"))]), false)
        .expect("correct legacy key must authenticate")
        .principal;
    assert!(
        principal.is_none(),
        "legacy path has no principal — identity stays header/IP-derived"
    );

    assert!(state
        .authenticate(&hdrs(&[("x-api-key", &test_key("nope"))]), false)
        .is_err());
    assert!(state.authenticate(&hyper::HeaderMap::new(), false).is_err());
}

#[test]
fn legacy_proxy_key_leaves_x_client_id_resolution_intact() {
    let state =
        Arc::new(TestPolicy::shared_key(&test_key("shared-secret")).install(test_state_base()));
    let ip = test_ip();
    let headers = hdrs(&[
        ("x-api-key", &test_key("shared-secret")),
        ("x-client-id", "gastown"),
    ]);
    let auth = state.authenticate(&headers, false).unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, auth);
    assert_eq!(
        rctx.caller.name(),
        "gastown",
        "without [[clients]], x-client-id remains the identity source"
    );
}

#[test]
fn open_proxy_authenticates_every_request() {
    let state = test_state_with(vec![]);
    assert!(state
        .authenticate(&hyper::HeaderMap::new(), false)
        .unwrap()
        .principal
        .is_none());
}

// ── Keys held and matched as digests ──

/// Open mode checks nothing: no credential, a junk one or a long one all pass
/// with no principal, exactly as before keys became digests.
#[test]
fn open_proxy_ignores_whatever_credential_is_presented() {
    let state = test_state_with(vec![]);
    assert!(matches!(state.auth_mode, AuthMode::Open));
    for key in ["junk".to_string(), test_key("anything")] {
        for allow_bearer in [false, true] {
            let headers = hdrs(&[
                ("x-api-key", &key),
                ("authorization", &format!("Bearer {key}")),
            ]);
            assert!(state
                .authenticate(&headers, allow_bearer)
                .unwrap()
                .principal
                .is_none());
        }
    }
}

/// The shared key is held as its digest, and matched on either header where
/// Bearer is read, exactly as the plaintext compare did.
#[test]
fn legacy_shared_key_is_held_and_matched_as_a_digest() {
    let key = test_key("shared-secret");
    let state = Arc::new(TestPolicy::shared_key(&key).install(test_state_base()));
    assert!(
        matches!(state.auth_mode, AuthMode::SharedKey(d) if d == key_digest(&key)),
        "the state holds the digest, not the key"
    );
    let bearer = hdrs(&[("authorization", &format!("Bearer {key}"))]);
    assert!(state
        .authenticate(&bearer, true)
        .unwrap()
        .principal
        .is_none());
    assert!(
        state.authenticate(&bearer, false).is_err(),
        "the native surface still reads only x-api-key"
    );
}

/// A presented credential shorter than `MIN_KEY_LEN` never matches, whatever
/// the table holds. The build refuses a short key, so only an unchecked state
/// can hold one: it stands for a table filled some other way. The refusal is
/// the ordinary 401, counted and throttled like any other.
#[test]
fn a_short_presented_key_never_matches_and_is_counted_and_throttled() {
    let short = "a".repeat(MIN_KEY_LEN - 1);
    let state = Arc::new(
        TestPolicy::clients(vec![mk_client("weak", &short, &[])]).install_unchecked(AppState {
            auth_throttle: AuthThrottle::new(2, Duration::from_secs(60)),
            ..test_state_base()
        }),
    );
    // Guard against a vacuous pass: the table really holds the short key.
    assert!(state
        .current_client_policy()
        .principals
        .contains_key(&key_digest(&short)));

    let ip = test_ip();
    let peer: SocketAddr = "10.0.0.7:40000".parse().unwrap();
    let headers = hdrs(&[("x-api-key", &short)]);
    let refuse = || {
        state
            .authenticate_throttled(&ip, peer, &headers, false, "proxy")
            .expect_err("a short key must not authenticate")
            .status()
    };
    assert_eq!(refuse(), StatusCode::UNAUTHORIZED);
    assert_eq!(refuse(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        refuse(),
        StatusCode::TOO_MANY_REQUESTS,
        "the throttle counts a short-key refusal like any other failure"
    );
    assert_eq!(
        state
            .auth_failures
            .lock()
            .unwrap()
            .get(&("proxy", "x-api-key")),
        Some(&3)
    );
}

/// The legacy shared key gets the same floor: a short shared key, which only
/// an unchecked state can hold, never matches either.
#[test]
fn a_short_presented_shared_key_never_matches() {
    let short = "s".repeat(MIN_KEY_LEN - 1);
    let state = TestPolicy::shared_key(&short).install_unchecked(test_state_base());
    assert!(matches!(state.auth_mode, AuthMode::SharedKey(d) if d == key_digest(&short)));
    for allow_bearer in [false, true] {
        let err = state
            .authenticate(&hdrs(&[("x-api-key", &short)]), allow_bearer)
            .expect_err("a short shared key must not authenticate");
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }
}

/// The floor is `MIN_KEY_LEN` itself: a key of exactly that length still
/// matches, one character less never does.
#[test]
fn the_short_key_floor_is_min_key_len_exactly() {
    let at_floor = "b".repeat(MIN_KEY_LEN);
    let under = "c".repeat(MIN_KEY_LEN - 1);
    let state = Arc::new(
        TestPolicy::clients(vec![
            mk_client("floor", &at_floor, &[]),
            mk_client("under", &under, &[]),
        ])
        .install_unchecked(test_state_base()),
    );
    let auth = |key: &str| state.authenticate(&hdrs(&[("x-api-key", key)]), false);
    assert_eq!(auth(&at_floor).unwrap().principal.unwrap().name, "floor");
    assert!(auth(&under).is_err());
}

/// The short-key test is per credential, not per request: on the OpenAI-compat
/// surface a junk `x-api-key`, short or long, does not stop a valid Bearer
/// beside it from authenticating.
#[test]
fn a_junk_api_key_beside_a_valid_bearer_still_authenticates() {
    let state = state_with_clients(vec![mk_client("geo", &test_key("key-geo"), &[])]);
    for junk in ["junk".to_string(), test_key("junk")] {
        let headers = hdrs(&[
            ("x-api-key", &junk),
            ("authorization", &format!("Bearer {}", test_key("key-geo"))),
        ]);
        let principal = state
            .authenticate(&headers, true)
            .expect("the valid Bearer must authenticate")
            .principal
            .expect("principal");
        assert_eq!(principal.name, "geo", "{junk}");
        assert!(
            state.authenticate(&headers, false).is_err(),
            "{junk}: where Bearer is not read, the junk key alone decides"
        );
    }
}

/// Same, through the router on `/v1/chat/completions`.
#[tokio::test]
async fn chat_completions_serves_a_valid_bearer_beside_a_junk_api_key() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(&mock_url, vec![mk_client("geo", &test_key("key-geo"), &[])]);
    let addr = serve(app).await;
    for junk in ["junk".to_string(), test_key("junk")] {
        let resp = Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("x-api-key", &junk)
            .header("authorization", format!("Bearer {}", test_key("key-geo")))
            .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{junk}");
    }
}

/// The mode is decided at boot, never re-inferred from the table: a
/// `[[clients]]` state whose tables are empty refuses every credential rather
/// than falling open.
#[test]
fn an_empty_principal_table_refuses_rather_than_opens() {
    let state = TestPolicy::clients(vec![mk_client("geo", &test_key("key-geo"), &[])])
        .install(test_state_base());
    state
        .swap_client_policy(ClientPolicy::unchecked(&cfg("")).1)
        .unwrap();
    assert!(matches!(state.auth_mode, AuthMode::Clients));
    for headers in [hdrs(&[]), hdrs(&[("x-api-key", &test_key("key-geo"))])] {
        for allow_bearer in [false, true] {
            let err = state
                .authenticate(&headers, allow_bearer)
                .expect_err("an empty table must not open the proxy");
            assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
        }
    }
}

// ── AC-7 / AC-8: the allow-list and its shared matcher ──

/// `serves_model` delegates to `model_matches`, so asserting the two agree
/// would be `A == A`. Pin the concrete wildcard semantics instead — including
/// the empty-model ALLOW, which is correct for routing (don't narrow the pool
/// on an unknown model) and is deliberately NOT what the client allow-list
/// does.
#[test]
fn endpoint_model_matcher_semantics() {
    let mut ep = mk_endpoint("a", "sk-ant-api-x");
    ep.models = vec![
        "claude-haiku-*".to_string(),
        "claude-sonnet-4-6".to_string(),
    ];
    assert!(ep.serves_model("claude-haiku-4-5"), "wildcard hit");
    assert!(ep.serves_model("claude-sonnet-4-6"), "exact hit");
    assert!(!ep.serves_model("claude-opus-5"), "miss");
    assert!(
        !ep.serves_model("claude-sonnet-4-6-x"),
        "exact is not a prefix"
    );
    assert!(ep.serves_model(""), "unknown model must not narrow routing");

    ep.models.clear();
    assert!(ep.serves_model("claude-opus-5"), "empty list = all models");
}

/// LAB-6894: an OpenAI-compatible gateway may split a comma `model` and serve
/// every model it names, so a non-empty list matches none, even through a
/// `*` pattern that would match the whole string. An empty list still allows
/// everything, and both gates share the rule.
#[test]
fn model_matcher_refuses_a_comma_model_under_a_list() {
    let haiku = vec!["claude-haiku-*".to_string()];
    for model in [
        "claude-haiku-4-5,claude-opus-5",
        "claude-haiku-4-5,",
        ",claude-haiku-4-5",
        ",",
    ] {
        assert!(!model_matches(&haiku, model), "{model:?}");
        assert!(!model_matches(&["*".to_string()], model), "{model:?}");
        assert!(model_matches(&[], model), "empty list allows {model:?}");
    }
    assert!(model_matches(&haiku, "claude-haiku-4-5"));

    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    assert!(!state
        .caller("limited")
        .allows_model("claude-haiku-4-5,claude-opus-5"));
}

#[test]
fn client_allow_list_hit_miss_wildcard_and_empty() {
    let state = state_with_clients(vec![
        mk_client(
            "limited",
            &test_key("k1"),
            &["claude-haiku-*", "claude-sonnet-4-6"],
        ),
        mk_client("unlimited", &test_key("k2"), &[]),
    ]);

    // exact hit
    assert!(state.caller("limited").allows_model("claude-sonnet-4-6"));
    // wildcard hit
    assert!(state.caller("limited").allows_model("claude-haiku-4-5"));
    // miss
    assert!(!state.caller("limited").allows_model("claude-opus-5"));
    // a near-miss on the exact pattern is still a miss
    assert!(!state
        .caller("limited")
        .allows_model("claude-sonnet-4-6-extra"));
    // empty list = all models
    assert!(state.caller("unlimited").allows_model("claude-opus-5"));
    // unknown client (legacy path only) = all models
    assert!(state.caller("-").allows_model("claude-opus-5"));
}

/// The allow-list must FAIL CLOSED on an unreadable model.
///
/// `proxy_handler` sets `model = ""` whenever the body does not parse as JSON,
/// and only ever reads a TOP-LEVEL `model` key — so a request to a route that
/// nests it (`/v1/messages/batches` puts it under `requests[].params.model`),
/// or any body the parser rejects, arrives at the gate with no model. If that
/// allowed, a client restricted to haiku would reach opus by sending a body we
/// cannot read. The endpoint matcher's empty-allows-all is right for routing
/// and wrong here; this test is the line between them.
#[test]
fn client_allow_list_denies_an_unreadable_model() {
    let state = state_with_clients(vec![
        mk_client("limited", &test_key("k1"), &["claude-haiku-*"]),
        mk_client("unlimited", &test_key("k2"), &[]),
    ]);
    assert!(
        !state.caller("limited").allows_model(""),
        "a client WITH an allow-list must be denied when the model is unknown"
    );
    assert!(
        state.caller("unlimited").allows_model(""),
        "a client with no allow-list is unaffected — nothing to enforce"
    );
}

/// LAB-6939: `models = [""]` denies every model, including the wildcard and
/// empty strings. It is the documented deny-all form; `[]` means all models.
#[test]
fn client_allow_list_of_one_empty_pattern_denies_every_model() {
    let state = state_with_clients(vec![mk_client("deny-all", &test_key("k1"), &[""])]);
    for model in ["claude-sonnet-5", "claude-haiku-4-5", "", "*", "none"] {
        assert!(
            !state.caller("deny-all").allows_model(model),
            "models = [\"\"] must deny {model:?}"
        );
    }
}

#[tokio::test]
async fn gate_denies_unreadable_model_for_restricted_client() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    let err = state
        .pre_request_gate("-", &state.caller("limited"), "")
        .await
        .expect_err("unknown model must be denied for a restricted client");
    assert_eq!(err.status(), StatusCode::FORBIDDEN);
    let json = parse_error_envelope(err).await;
    assert_eq!(json["type"], "error");
    assert_eq!(json["error"]["type"], "permission_error");
    let text = json["error"]["message"].as_str().unwrap();
    assert!(
        text.contains("no model could be read"),
        "message should explain the empty-model denial, got: {text}"
    );
}

/// End-to-end proof of the same thing: an unparseable body must not smuggle a
/// restricted client past its allow-list. Since LAB-6781 the ingress rule
/// refuses it before the allow-list runs; the allow-list's own denial is
/// pinned by `native_surface_denies_restricted_client_sending_multipart_body`.
#[tokio::test]
async fn native_surface_denies_restricted_client_sending_unparseable_body() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client(
            "limited",
            &test_key("key-limited"),
            &["claude-haiku-*"],
        )],
    );
    let addr = serve(app).await;
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", test_key("key-limited"))
        .body("this is not json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let json = parse_wire_error_envelope(resp).await;
    assert_eq!(json["error"]["type"], "invalid_request_error");
}

/// The one unparsed body that still reaches the gate is a multipart upload.
/// It carries no model the proxy can read, so the allow-list denies it.
#[tokio::test]
async fn native_surface_denies_restricted_client_sending_multipart_body() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client(
            "limited",
            &test_key("key-limited"),
            &["claude-haiku-*"],
        )],
    );
    let addr = serve(app).await;
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "multipart/form-data; boundary=x")
        .header("x-api-key", test_key("key-limited"))
        .body("--x\r\n\r\nhi\r\n--x--\r\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
}

/// The model string is caller-controlled and bounded only by the body cap.
/// Untruncated it would be retained for the process lifetime and re-serialized
/// into `/metrics` on every scrape.
#[test]
fn denied_model_label_is_truncated() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    let huge = "z".repeat(100_000);
    state.note_model_denied(&state.caller("limited"), &huge);
    let counts = state.model_denied.lock().unwrap();
    let (_, label) = counts.keys().next().unwrap();
    assert!(
        label.chars().count() <= MAX_LABEL_CHARS + 1,
        "label not truncated: {} chars",
        label.chars().count()
    );
}

/// The other half of the same guarantee: the 403 body echoes the denied model
/// through `truncate_label`, so an oversized model field must not be reflected
/// back untruncated.
#[tokio::test]
async fn denied_model_response_body_is_truncated() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    let huge = "z".repeat(100_000);
    let err = state
        .pre_request_gate("-", &state.caller("limited"), &huge)
        .await
        .expect_err("oversized model must be denied");
    let json = parse_error_envelope(err).await;
    assert_eq!(json["error"]["type"], "permission_error");
    assert!(
        json["error"]["message"].as_str().unwrap().chars().count() < 1_000,
        "403 message must not echo the untruncated model"
    );
}

/// Truncation is char-based: slicing a multi-byte string on a byte boundary
/// would panic.
#[test]
fn truncate_label_handles_multibyte_without_panicking() {
    let s = "é".repeat(200);
    let out = truncate_label(&s);
    assert!(out.chars().count() <= MAX_LABEL_CHARS + 1);
    assert_eq!(truncate_label("short"), "short");
}

// ── AC-11: denial counter + bounded model cardinality ──

#[tokio::test]
async fn model_denial_increments_counter_per_client_and_model() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    for _ in 0..3 {
        assert!(state
            .pre_request_gate("-", &state.caller("limited"), "claude-opus-5")
            .await
            .is_err());
    }
    assert!(state
        .pre_request_gate("-", &state.caller("limited"), "claude-fable-5")
        .await
        .is_err());

    let counts = state.model_denied.lock().unwrap();
    assert_eq!(
        counts.get(&("limited".to_string(), "claude-opus-5".to_string())),
        Some(&3)
    );
    assert_eq!(
        counts.get(&("limited".to_string(), "claude-fable-5".to_string())),
        Some(&1)
    );
}

/// The model label is caller-controlled — unbounded growth here would be a
/// metrics-cardinality DoS. Bound is cap + N + 1; `note_model_denied`
/// documents the scheme (LAB-2332, LAB-4028).
#[test]
fn model_denial_labels_are_bounded_by_other_overflow() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    for i in 0..(MAX_MODEL_DENIED_LABELS + 25) {
        state.note_model_denied(&state.caller("limited"), &format!("junk-model-{i}"));
    }
    // Rotating an unconfigured client id past the cap must NOT mint keys.
    for i in 0..50 {
        state.note_model_denied(&state.caller(&format!("evil-{i}")), "claude-x");
    }
    // An already-tracked pair keeps its own key past the cap. Without the
    // `contains_key` clause it would drain into overflow with the bound still
    // satisfied — attribution lost, tests green.
    state.note_model_denied(&state.caller("limited"), "junk-model-0");
    let counts = state.model_denied.lock().unwrap();
    assert!(
        counts.len() <= MAX_MODEL_DENIED_LABELS + state.current_client_policy().clients.len() + 1,
        "label map grew unbounded: {} entries",
        counts.len()
    );
    assert_eq!(
        counts.get(&("limited".to_string(), "junk-model-0".to_string())),
        Some(&2),
        "an already-tracked pair must keep incrementing its own key past the cap"
    );
    assert_eq!(
        counts.get(&("limited".to_string(), "_other".to_string())),
        Some(&25),
        "a configured client's overflow must land in its own bucket"
    );
    assert_eq!(
        counts.get(&("_other".to_string(), "_other".to_string())),
        Some(&50),
        "unconfigured ids must share the global _other bucket, not be dropped"
    );
}

/// A configured client can ASK for a model literally named `_other`. That
/// denial must not alias the client's overflow bucket, or overflow counts are
/// inflated and the client pre-empts its own first-overflow warn (LAB-4028).
#[tokio::test]
async fn model_denial_literal_other_model_does_not_alias_overflow_bucket() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    assert!(state
        .pre_request_gate("-", &state.caller("limited"), "_other")
        .await
        .is_err());
    for i in 0..(MAX_MODEL_DENIED_LABELS + 25) {
        state.note_model_denied(&state.caller("limited"), &format!("junk-model-{i}"));
    }
    assert!(state
        .pre_request_gate("-", &state.caller("limited"), "_other")
        .await
        .is_err());

    let counts = state.model_denied.lock().unwrap();
    assert_eq!(
        counts.get(&("limited".to_string(), "_other".to_string())),
        Some(&26),
        "overflow bucket must count overflow only: 1 literal + 63 junk fill the cap, 26 junk spill"
    );
    assert_eq!(
        counts.get(&("limited".to_string(), "__other".to_string())),
        Some(&2),
        "a literal `_other` model must keep a label of its own"
    );
}

/// The `_other` escape must stay injective: a literal `__other` model must not
/// share the escaped `_other` label, or the two denials merge into one counter
/// and the second model never gets its first-denial warn.
#[tokio::test]
async fn model_denial_escape_keeps_other_and_dunder_other_distinct() {
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    // Bare `other` guards the leading-`_` requirement: without it, `other`
    // would be escaped onto `_other` and pre-claim the overflow bucket.
    for model in ["other", "_other", "__other"] {
        assert!(state
            .pre_request_gate("-", &state.caller("limited"), model)
            .await
            .is_err());
    }

    let counts = state.model_denied.lock().unwrap();
    assert_eq!(counts.len(), 3, "three models, three labels: {counts:?}");
    assert!(
        counts.values().all(|&n| n == 1),
        "each model must mint its own key: {counts:?}"
    );
    assert!(
        !counts.contains_key(&("limited".to_string(), "_other".to_string())),
        "neither model may claim the overflow bucket"
    );
}

/// The model is caller-controlled JSON, so it can carry a raw newline. The
/// denial warn must log it escaped, or a client can forge a log line
/// (CWE-117, LAB-5442).
#[test]
fn model_denial_log_escapes_newline_in_model() {
    let buf = log_capture_buf();
    let state = state_with_clients(vec![mk_client(
        "limited",
        &test_key("k1"),
        &["claude-haiku-*"],
    )]);
    // First denial for this pair, so it takes the warn arm the capture sees.
    state.note_model_denied(&state.caller("limited"), "x\nFORGED-LAB5442 WARN fake");

    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains(r"\nFORGED-LAB5442"),
        "denial warn must log the model escaped, got:\n{output}"
    );
    assert!(
        !output.contains("\nFORGED-LAB5442"),
        "a raw newline in the model must not start a new log line"
    );
}

// ── Integration: both surfaces, through the real router ──

fn authed_app(upstream_url: &str, clients: Vec<ClientConfig>) -> (Router, Arc<AppState>) {
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = upstream_url.to_string();
    let state = Arc::new(TestPolicy::clients(clients).install(AppState {
        endpoints: vec![acct],
        ..test_state_base()
    }));
    (build_router(state.clone()), state)
}

/// Native surface: the request authenticates as `limited` while claiming to be
/// `unlimited`. It must be gated as `limited` — 403 — not waved through.
#[tokio::test]
async fn native_surface_gates_spoofing_client_by_its_real_identity() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = authed_app(
        &mock_url,
        vec![
            mk_client("limited", &test_key("key-limited"), &["claude-haiku-*"]),
            mk_client("unlimited", &test_key("key-unlimited"), &[]),
        ],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", test_key("key-limited"))
        .header("x-client-id", "unlimited")
        .body(r#"{"model":"claude-opus-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(resp.text().await.unwrap().contains("limited"));

    // Same credential, a model it IS allowed: unaffected.
    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", test_key("key-limited"))
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert_eq!(
        state
            .model_denied
            .lock()
            .unwrap()
            .get(&("limited".to_string(), "claude-opus-5".to_string())),
        Some(&1)
    );
}

/// OpenAI-compat surface: same gate, reached through the other handler —
/// `pre_request_gate` is called from both, so one placement covers both.
#[tokio::test]
async fn openai_surface_enforces_the_same_allow_list() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client(
            "limited",
            &test_key("key-limited"),
            &["claude-haiku-*"],
        )],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", test_key("key-limited")))
        .header("x-client-id", "someone-else")
        .body(r#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", test_key("key-limited")))
        .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// AC-2: no site left on the old single-key path.
#[tokio::test]
async fn all_four_auth_sites_reject_an_unknown_key() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(&mock_url, vec![mk_client("geo", &test_key("key-geo"), &[])]);
    let addr = serve(app).await;
    let client = Client::new();

    for path in ["/_stats", "/metrics"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", test_key("key-wrong"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path} accepted an unknown key"
        );
        // …and the right one is recognized — but 403, not 200: the admin
        // surfaces are operator-only since LAB-1192 and `geo` is a plain
        // client. The operator 200 path is covered by the AC-6 matrix tests.
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", test_key("key-geo"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN, "{path}");
    }

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", test_key("key-wrong"))
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("x-api-key", test_key("key-wrong"))
        .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // The bearer branch is reachable only on this surface — cover its reject
    // path too: wrong key, bare scheme, and a valid key without the scheme.
    for auth in [
        format!("Bearer {}", test_key("key-wrong")),
        "Bearer".to_string(),
        test_key("key-geo"),
    ] {
        let resp = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", &auth)
            .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "'{auth}' must not authenticate"
        );
    }
}

/// AC-11: the denial counter reaches /metrics under its documented name.
/// The scrape presents an OPERATOR credential — /metrics is operator-only
/// since LAB-1192.
#[tokio::test]
async fn metrics_exposes_the_model_denial_counter() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = mock_url.to_string();
    let state = Arc::new(
        TestPolicy::clients(vec![
            mk_client("limited", &test_key("key-limited"), &["claude-haiku-*"]),
            mk_client("ops", &test_key("key-ops"), &[]),
        ])
        .operators(&["ops"])
        .install(AppState {
            endpoints: vec![acct],
            ..test_state_base()
        }),
    );
    let app = build_router(state.clone());
    assert!(state
        .pre_request_gate("-", &state.caller("limited"), "claude-opus-5")
        .await
        .is_err());

    let addr = serve(app).await;
    let body = Client::new()
        .get(format!("http://{addr}/metrics"))
        .header("x-api-key", test_key("key-ops"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains(
            "anthropic_client_model_denied_total{client=\"limited\",model=\"claude-opus-5\"} 1"
        ),
        "denial counter missing from /metrics:\n{body}"
    );
}

/// LAB-6939 rollback case: with no `admin_readers`, `models = [""]` is the only
/// thing between the client's key and the pool. Both surfaces must 403 a
/// well-formed request for a real model before anything reaches upstream.
#[tokio::test]
async fn empty_model_pattern_denies_both_surfaces_without_reaching_upstream() {
    let (mock_url, hits) = spawn_flaky_upstream(0, ANTHROPIC_OK_BODY).await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client("deny-all", &test_key("key-deny"), &[""])],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", test_key("key-deny"))
        .body(r#"{"model":"claude-sonnet-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", test_key("key-deny")))
        .body(r#"{"model":"claude-sonnet-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
}

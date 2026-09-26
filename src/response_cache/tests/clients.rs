use super::*;

// ── AC-1: distinct key → distinct identity, with no x-client-id ──

#[test]
fn distinct_client_keys_resolve_to_distinct_identities() {
    let state = state_with_clients(vec![
        mk_client("geo", "key-geo", &[]),
        mk_client("radar", "key-radar", &[]),
    ]);
    let ip = test_ip();

    for (key, expected) in [("key-geo", "geo"), ("key-radar", "radar")] {
        let headers = hdrs(&[("x-api-key", key)]);
        let principal = state
            .authenticate(&headers, false)
            .expect("configured key must authenticate")
            .expect("a [[clients]] match must yield a principal");
        assert_eq!(principal.name, expected);
        // And the identity the rest of the proxy sees follows it — with NO
        // x-client-id header present on either request.
        let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));
        assert_eq!(rctx.client_id, expected);
    }
}

// ── AC-2: unknown / missing credential → 401 ──

#[test]
fn unknown_client_key_is_rejected() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let resp = state
        .authenticate(&hdrs(&[("x-api-key", "key-wrong")]), false)
        .expect_err("unknown key must not authenticate");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn missing_credential_is_rejected_when_clients_configured() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let resp = state
        .authenticate(&hyper::HeaderMap::new(), false)
        .expect_err("absent credential must not authenticate");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// A near-miss must not authenticate. Guards the constant-time comparison
/// against a length- or prefix-tolerant rewrite.
#[test]
fn client_key_match_is_exact_not_prefix() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    for wrong in ["key-ge", "key-geo ", "key-geox", "", "KEY-GEO"] {
        assert!(
            state
                .authenticate(&hdrs(&[("x-api-key", wrong)]), false)
                .is_err(),
            "'{wrong}' must not authenticate as 'key-geo'"
        );
    }
}

// ── AC-2: bearer accepted on the OpenAI-compat surface only ──

#[test]
fn bearer_credential_accepted_only_where_enabled() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let headers = hdrs(&[("authorization", "Bearer key-geo")]);

    let principal = state
        .authenticate(&headers, true)
        .expect("bearer must authenticate where enabled")
        .expect("principal");
    assert_eq!(principal.name, "geo");

    // The native surface may carry the caller's OWN upstream token in
    // `Authorization` (passthrough endpoints), so bearer is not accepted there.
    assert!(state.authenticate(&headers, false).is_err());
}

#[test]
fn bearer_scheme_match_is_case_insensitive() {
    let state = state_with_clients(vec![mk_client("geo", "key-geo", &[])]);
    let headers = hdrs(&[("authorization", "bEaReR key-geo")]);
    assert_eq!(
        state.authenticate(&headers, true).unwrap().unwrap().name,
        "geo"
    );
}

// ── AC-4: the credential wins over a spoofed x-client-id ──

#[test]
fn spoofed_x_client_id_is_ignored_under_clients_table() {
    let state = state_with_clients(vec![
        mk_client("alpha", "key-alpha", &[]),
        mk_client("bravo", "key-bravo", &[]),
    ]);
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "key-alpha"), ("x-client-id", "bravo")]);

    let principal = state.authenticate(&headers, false).unwrap().unwrap();
    assert_eq!(principal.name, "alpha");

    let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));
    assert_eq!(
        rctx.client_id, "alpha",
        "authenticated principal must win over the x-client-id header"
    );
}

/// The `client_names` IP map is the other client-influenced identity source.
/// It must also lose to the credential.
#[test]
fn client_names_ip_map_is_ignored_under_clients_table() {
    let mut client_names = HashMap::new();
    client_names.insert(TEST_IP.to_string(), "from-ip-map".to_string());
    let state = Arc::new(AppState {
        clients: vec![mk_client("alpha", "key-alpha", &[])],
        client_names,
        ..test_state_base()
    });
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "key-alpha")]);
    let principal = state.authenticate(&headers, false).unwrap().unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, Some(principal));
    assert_eq!(rctx.client_id, "alpha");
}

// ── AC-5: legacy proxy_key path unchanged ──

#[test]
fn legacy_proxy_key_still_authenticates_and_yields_no_principal() {
    let state = Arc::new(AppState {
        proxy_key: Some("shared-secret".to_string()),
        ..test_state_base()
    });

    let principal = state
        .authenticate(&hdrs(&[("x-api-key", "shared-secret")]), false)
        .expect("correct legacy key must authenticate");
    assert!(
        principal.is_none(),
        "legacy path has no principal — identity stays header/IP-derived"
    );

    assert!(state
        .authenticate(&hdrs(&[("x-api-key", "nope")]), false)
        .is_err());
    assert!(state.authenticate(&hyper::HeaderMap::new(), false).is_err());
}

#[test]
fn legacy_proxy_key_leaves_x_client_id_resolution_intact() {
    let state = Arc::new(AppState {
        proxy_key: Some("shared-secret".to_string()),
        ..test_state_base()
    });
    let ip = test_ip();
    let headers = hdrs(&[("x-api-key", "shared-secret"), ("x-client-id", "gastown")]);
    let principal = state.authenticate(&headers, false).unwrap();
    let rctx = RequestContext::from_request(&state, &ip, &headers, principal);
    assert_eq!(
        rctx.client_id, "gastown",
        "without [[clients]], x-client-id remains the identity source"
    );
}

#[test]
fn open_proxy_authenticates_every_request() {
    let state = test_state_with(vec![]);
    assert!(state
        .authenticate(&hyper::HeaderMap::new(), false)
        .unwrap()
        .is_none());
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

#[test]
fn client_allow_list_hit_miss_wildcard_and_empty() {
    let state = state_with_clients(vec![
        mk_client("limited", "k1", &["claude-haiku-*", "claude-sonnet-4-6"]),
        mk_client("unlimited", "k2", &[]),
    ]);

    // exact hit
    assert!(state.client_allows_model("limited", "claude-sonnet-4-6"));
    // wildcard hit
    assert!(state.client_allows_model("limited", "claude-haiku-4-5"));
    // miss
    assert!(!state.client_allows_model("limited", "claude-opus-5"));
    // a near-miss on the exact pattern is still a miss
    assert!(!state.client_allows_model("limited", "claude-sonnet-4-6-extra"));
    // empty list = all models
    assert!(state.client_allows_model("unlimited", "claude-opus-5"));
    // unknown client (legacy path only) = all models
    assert!(state.client_allows_model("-", "claude-opus-5"));
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
        mk_client("limited", "k1", &["claude-haiku-*"]),
        mk_client("unlimited", "k2", &[]),
    ]);
    assert!(
        !state.client_allows_model("limited", ""),
        "a client WITH an allow-list must be denied when the model is unknown"
    );
    assert!(
        state.client_allows_model("unlimited", ""),
        "a client with no allow-list is unaffected — nothing to enforce"
    );
}

#[tokio::test]
async fn gate_denies_unreadable_model_for_restricted_client() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let err = state
        .pre_request_gate("limited", "")
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
/// restricted client past its allow-list.
#[tokio::test]
async fn native_surface_denies_restricted_client_sending_unparseable_body() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = authed_app(
        &mock_url,
        vec![mk_client("limited", "key-limited", &["claude-haiku-*"])],
    );
    let addr = serve(app).await;
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-limited")
        .body("this is not json")
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
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let huge = "z".repeat(100_000);
    state.note_model_denied("limited", &huge);
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
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let huge = "z".repeat(100_000);
    let err = state
        .pre_request_gate("limited", &huge)
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
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    for _ in 0..3 {
        assert!(state
            .pre_request_gate("limited", "claude-opus-5")
            .await
            .is_err());
    }
    assert!(state
        .pre_request_gate("limited", "claude-fable-5")
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
/// metrics-cardinality DoS.
#[test]
fn model_denial_labels_are_bounded_by_other_overflow() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    for i in 0..(MAX_MODEL_DENIED_LABELS + 25) {
        state.note_model_denied("limited", &format!("junk-model-{i}"));
    }
    // Expert-panel finding (LAB-2330, mirrored by LAB-2332): rotating the
    // caller-controlled client id past the cap must NOT mint per-client
    // overflow keys — the bound has to hold on the client axis too.
    for i in 0..50 {
        state.note_model_denied(&format!("evil-{i}"), "claude-x");
    }
    let counts = state.model_denied.lock().unwrap();
    assert!(
        counts.len() <= MAX_MODEL_DENIED_LABELS + 1,
        "label map grew unbounded: {} entries",
        counts.len()
    );
    // Overflow denials are not dropped — they land in the ONE global bucket:
    // 25 "limited" overflow models + 50 rotated clients.
    assert_eq!(
        counts.get(&("_other".to_string(), "_other".to_string())),
        Some(&75),
        "overflow must land in the global _other bucket, not be dropped"
    );
}

// ── Integration: both surfaces, through the real router ──

fn authed_app(upstream_url: &str, clients: Vec<ClientConfig>) -> (Router, Arc<AppState>) {
    let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    acct.base_url = upstream_url.to_string();
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients,
        ..test_state_base()
    });
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
            mk_client("limited", "key-limited", &["claude-haiku-*"]),
            mk_client("unlimited", "key-unlimited", &[]),
        ],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-limited")
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
        .header("x-api-key", "key-limited")
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
        vec![mk_client("limited", "key-limited", &["claude-haiku-*"])],
    );
    let addr = serve(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer key-limited")
        .header("x-client-id", "someone-else")
        .body(r#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer key-limited")
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
    let (app, _state) = authed_app(&mock_url, vec![mk_client("geo", "key-geo", &[])]);
    let addr = serve(app).await;
    let client = Client::new();

    for path in ["/_stats", "/metrics"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("x-api-key", "key-wrong")
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
            .header("x-api-key", "key-geo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN, "{path}");
    }

    let resp = client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-wrong")
        .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header("x-api-key", "key-wrong")
        .body(r#"{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // The bearer branch is reachable only on this surface — cover its reject
    // path too: wrong key, bare scheme, and a valid key without the scheme.
    for auth in ["Bearer key-wrong", "Bearer", "key-geo"] {
        let resp = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", auth)
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
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        clients: vec![
            mk_client("limited", "key-limited", &["claude-haiku-*"]),
            mk_client("ops", "key-ops", &[]),
        ],
        operators: vec!["ops".to_string()],
        ..test_state_base()
    });
    let app = build_router(state.clone());
    assert!(state
        .pre_request_gate("limited", "claude-opus-5")
        .await
        .is_err());

    let addr = serve(app).await;
    let body = Client::new()
        .get(format!("http://{addr}/metrics"))
        .header("x-api-key", "key-ops")
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

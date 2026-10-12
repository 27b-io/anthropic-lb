use super::*;

// ── Serving: what the gate lets through ─────────────────────────────

/// A record version the store wrote a moment ago.
fn just_now() -> u64 {
    now_us() - 1_000
}

/// A full restrictions part: `disabled` names disabled, `revoked` digests
/// revoked.
fn full(disabled: &[&str], revoked: &[[u8; 32]]) -> Value {
    let records: Vec<(&str, RecordState, u64)> = disabled
        .iter()
        .map(|n| (*n, Disabled, just_now()))
        .collect();
    restrictions_part(&body(true, &records, revoked))
}

/// The JSON error body of a rejection.
async fn error_message(rejection: AuthRejection) -> String {
    let json = parse_error_envelope(rejection.into_response()).await;
    json["error"]["message"].as_str().unwrap().to_string()
}

fn ok(name: &str) -> Result<String, u16> {
    Ok(name.to_string())
}

#[tokio::test]
async fn an_instance_holding_no_restrictions_serves_no_ordinary_client_file_keys_included() {
    let inst = Instance::offline(&["file-only"]);
    assert_eq!(inst.auth(&test_key("file-only")), Err(503));
    // Operators and admin readers work from the start.
    assert_eq!(inst.auth(&test_key("ops")), ok("ops"));
    assert_eq!(inst.auth(&test_key("viewer")), ok("viewer"));
    // A key that matches nothing may be a store key still to load.
    assert_eq!(inst.auth(&test_key("nobody")), Err(503));
    // No credential, or one too short to be any key, is a failed one as ever.
    assert_eq!(
        inst.state
            .authenticate(&hdrs(&[]), false)
            .map_err(|r| r.into_response().status().as_u16())
            .err(),
        Some(401)
    );
    assert_eq!(inst.auth("short"), Err(401));

    let rejection = inst
        .state
        .authenticate(&hdrs(&[("x-api-key", &test_key("file-only"))]), false)
        .unwrap_err();
    let resp = rejection.into_response();
    assert_eq!(resp.headers()["retry-after"], "1");
    let json = parse_error_envelope(resp).await;
    assert_eq!(json["error"]["type"], "overloaded_error");
}

#[tokio::test]
async fn an_instance_with_a_document_but_no_restrictions_stays_unloaded() {
    let inst = Instance::offline(&["file-only"]);
    let doc = document(wall_ms(), vec![client("acme", &test_key("acme"))], &[]);
    inst.answer(answer(doc_part(&doc), unavailable())).await;
    assert!(inst.metrics().document.is_some(), "the document is held");
    assert!(!inst.metrics().loaded);
    assert_eq!(inst.auth(&test_key("acme")), Err(503));
    assert_eq!(inst.auth(&test_key("file-only")), Err(503));
    assert_eq!(inst.auth(&test_key("ops")), ok("ops"));

    // The restrictions arrive: loaded.
    inst.answer(answer(doc_part_held(&doc), full(&[], &[])))
        .await;
    assert!(inst.metrics().loaded);
    assert_eq!(inst.auth(&test_key("acme")), ok("acme"));
    assert_eq!(inst.auth(&test_key("file-only")), ok("file-only"));
    // Loaded, a key that matches nothing is a failed credential again.
    assert_eq!(inst.auth(&test_key("nobody")), Err(401));
}

#[tokio::test]
async fn a_disabled_name_refuses_store_and_file_keys_on_a_loaded_and_an_unloaded_instance() {
    // Unloaded, holding a complete set and no document: the file's clients
    // are served under the restrictions.
    let inst = Instance::offline(&["file-only", "both", "other"]);
    inst.answer(answer(unavailable(), full(&["file-only", "both"], &[])))
        .await;
    assert!(!inst.metrics().loaded);
    assert_eq!(inst.auth(&test_key("file-only")), Err(403));
    assert_eq!(inst.auth(&test_key("both")), Err(403));
    assert_eq!(inst.auth(&test_key("other")), ok("other"));
    let rejection = inst
        .state
        .authenticate(&hdrs(&[("x-api-key", &test_key("both"))]), false)
        .unwrap_err();
    assert_eq!(error_message(rejection).await, "client_disabled");

    // Unloaded, holding only a partial part: its disable already applies.
    let inst = Instance::offline(&["file-only", "other"]);
    inst.answer(answer(
        unavailable(),
        restrictions_part(&body(false, &[("file-only", Disabled, just_now())], &[])),
    ))
    .await;
    assert_eq!(inst.auth(&test_key("file-only")), Err(403));
    assert_eq!(inst.auth(&test_key("other")), Err(503));

    // Loaded: the store's keys and the file's keys of the name alike.
    let inst = Instance::offline(&["both", "other"]);
    let doc = document(
        wall_ms(),
        vec![
            client("acme", &test_key("acme")),
            client("both", &test_key("both-store")),
        ],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&["acme", "both"], &[])))
        .await;
    assert!(inst.metrics().loaded);
    assert_eq!(inst.auth(&test_key("acme")), Err(403));
    assert_eq!(inst.auth(&test_key("both-store")), Err(403));
    assert_eq!(inst.auth(&test_key("both")), Err(403));
    assert_eq!(inst.auth(&test_key("other")), ok("other"));

    // A newer enable lifts it, for every key of the name.
    let enabled = body(
        true,
        &[("acme", Disabled, just_now()), ("both", Enabled, now_us())],
        &[],
    );
    inst.answer(answer(doc_part_held(&doc), restrictions_part(&enabled)))
        .await;
    assert_eq!(inst.auth(&test_key("both-store")), ok("both"));
    assert_eq!(inst.auth(&test_key("both")), ok("both"));
    assert_eq!(inst.auth(&test_key("acme")), Err(403));
}

#[tokio::test]
async fn a_disabled_record_naming_a_privileged_principal_refuses_nothing() {
    let inst = Instance::offline(&[]);
    inst.answer(answer(unavailable(), full(&["ops", "viewer"], &[])))
        .await;
    assert_eq!(inst.auth(&test_key("ops")), ok("ops"));
    assert_eq!(inst.auth(&test_key("viewer")), ok("viewer"));
    assert!(inst.alert("privileged_name"));
    let report = inst.store.lock_report().clone();
    assert_eq!(report.privileged_disabled, vec!["ops", "viewer"]);
    assert!(report.unmatched_disabled.is_empty());
}

#[tokio::test]
async fn a_quarantined_client_refuses_every_key_of_its_name() {
    let inst = Instance::offline(&["both", "other"]);
    let doc = document(
        wall_ms(),
        vec![
            client_with(
                "both",
                &[&test_key("both-store")],
                json!({"daily_budget": 0}),
            ),
            client("acme", &test_key("acme")),
        ],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&[], &[]))).await;
    assert_eq!(inst.auth(&test_key("both-store")), Err(403));
    assert_eq!(
        inst.auth(&test_key("both")),
        Err(403),
        "its file key falls with it"
    );
    assert_eq!(
        inst.auth(&test_key("acme")),
        ok("acme"),
        "the rest installs"
    );
    assert_eq!(inst.auth(&test_key("other")), ok("other"));
    let rejection = inst
        .state
        .authenticate(&hdrs(&[("x-api-key", &test_key("both"))]), false)
        .unwrap_err();
    assert_eq!(error_message(rejection).await, "client_quarantined");
    assert_eq!(inst.metrics().quarantined, 1);
    assert!(inst.alert("quarantined"));
    assert_eq!(
        inst.store.lock_report().quarantined,
        vec![("both".to_string(), "client.daily_budget")]
    );
}

#[tokio::test]
async fn a_name_in_the_file_and_the_store_is_one_client_under_the_store_policy() {
    let mut both = mk_client("both", &test_key("both"), &["claude-haiku-*"]);
    both.preferred_endpoints = vec!["acct-a".to_string()];
    let clients = vec![
        mk_client("ops", &test_key("ops"), &[]),
        mk_client("viewer", &test_key("viewer"), &[""]),
        both,
    ];
    let inst = Instance::new(
        "http://127.0.0.1:9",
        clients,
        "https://store.invalid/_clients/poll",
        None,
    );
    let doc = document(
        wall_ms(),
        vec![client_with(
            "both",
            &[&test_key("both-store")],
            json!({
                "daily_budget": 5000,
                "utilization_limit": 0.5,
                "models": ["claude-opus-*"],
                "preferred_endpoints": [],
            }),
        )],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&[], &[]))).await;
    // Both key sets authenticate as the one client.
    assert_eq!(inst.auth(&test_key("both")), ok("both"));
    assert_eq!(inst.auth(&test_key("both-store")), ok("both"));
    // The store's policy governs both.
    let caller = inst.state.caller("both");
    assert_eq!(caller.budget(), Some(5000));
    assert_eq!(caller.utilization_limit(), Some(0.5));
    assert!(caller.allows_model("claude-opus-5"));
    assert!(!caller.allows_model("claude-haiku-4-5"));
    assert_eq!(
        caller.preferred_endpoints(),
        None,
        "the store's empty pin list governs"
    );
}

/// Schema 1 has no guard policy, so a name in both keeps the file's.
#[cfg(feature = "guard")]
#[tokio::test]
async fn a_name_in_the_file_and_the_store_keeps_the_file_guard_policy() {
    let mut both = mk_client("both", &test_key("both"), &[]);
    both.guard = crate::guard::GuardPolicy::Block;
    let mut clients = file_clients(&[]);
    clients.push(both);
    let inst = Instance::new(
        "http://127.0.0.1:9",
        clients,
        "https://store.invalid/_clients/poll",
        None,
    );
    let doc = document(
        wall_ms(),
        vec![
            client("both", &test_key("both-store")),
            client("acme", &test_key("acme")),
        ],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&[], &[]))).await;
    assert_eq!(
        inst.state.caller("both").guard_policy(),
        crate::guard::GuardPolicy::Block
    );
    assert_eq!(
        inst.state.caller("acme").guard_policy(),
        crate::guard::GuardPolicy::default()
    );
}

#[tokio::test]
async fn a_revoked_digest_refuses_its_store_key_only() {
    let inst = Instance::offline(&[]);
    let doc = document(
        wall_ms(),
        vec![client_with(
            "acme",
            &[&test_key("acme-1"), &test_key("acme-2")],
            json!({}),
        )],
        &[],
    );
    inst.answer(answer(
        doc_part(&doc),
        full(&[], &[key_digest(&test_key("acme-1"))]),
    ))
    .await;
    assert_eq!(inst.auth(&test_key("acme-1")), Err(401));
    assert_eq!(inst.auth(&test_key("acme-2")), ok("acme"));
}

#[tokio::test]
async fn a_revoked_digest_outside_the_store_is_ignored_with_an_alert() {
    let inst = Instance::offline(&["file-only"]);
    inst.answer(answer(
        unavailable(),
        full(
            &[],
            &[
                key_digest(&test_key("file-only")),
                key_digest(&test_key("ops")),
            ],
        ),
    ))
    .await;
    assert_eq!(inst.auth(&test_key("file-only")), ok("file-only"));
    assert_eq!(inst.auth(&test_key("ops")), ok("ops"));
    assert_eq!(inst.metrics().revoked_outside_store, 2);
}

#[tokio::test]
async fn a_store_client_named_for_a_privileged_principal_is_ignored() {
    let inst = Instance::offline(&[]);
    let doc = document(
        wall_ms(),
        vec![
            client_with("ops", &[&test_key("store-ops")], json!({"daily_budget": 0})),
            client("acme", &test_key("acme")),
        ],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&[], &[]))).await;
    assert_eq!(
        inst.auth(&test_key("store-ops")),
        Err(401),
        "none of its keys is looked up"
    );
    let principal = inst
        .state
        .authenticate(&hdrs(&[("x-api-key", &test_key("ops"))]), false)
        .unwrap()
        .principal
        .unwrap();
    assert_eq!(
        principal.role,
        Role::Operator,
        "the file's operator keeps the name"
    );
    assert_eq!(inst.auth(&test_key("acme")), ok("acme"));
    assert!(inst.alert("privileged_name"));
    assert_eq!(inst.metrics().quarantined, 0, "dropped, never quarantined");
}

#[tokio::test]
async fn a_store_key_that_is_a_config_file_key_quarantines_the_store_client() {
    let inst = Instance::offline(&["file-only", "same"]);
    let doc = document(
        wall_ms(),
        vec![
            // Another name: the file key keeps working for its own client.
            client_with(
                "acme",
                &[&test_key("file-only"), &test_key("acme-2")],
                json!({}),
            ),
            // The same name: the file key falls with the quarantine.
            client("same", &test_key("same")),
        ],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&[], &[]))).await;
    assert_eq!(inst.auth(&test_key("file-only")), ok("file-only"));
    assert_eq!(inst.auth(&test_key("acme-2")), Err(403));
    assert_eq!(inst.auth(&test_key("same")), Err(403));
    let quarantined = inst.store.lock_report().quarantined.clone();
    assert_eq!(
        quarantined,
        vec![
            ("acme".to_string(), "key.digest_in_config"),
            ("same".to_string(), "key.digest_in_config"),
        ]
    );
}

#[tokio::test]
async fn a_pin_to_an_endpoint_this_instance_lacks_is_dropped() {
    let inst = Instance::offline(&[]);
    let doc = document(
        wall_ms(),
        vec![client_with(
            "acme",
            &[&test_key("acme")],
            json!({"preferred_endpoints": ["acct-a", "elsewhere"]}),
        )],
        &[],
    );
    inst.answer(answer(doc_part(&doc), full(&[], &[]))).await;
    assert_eq!(
        inst.state.caller("acme").preferred_endpoints(),
        Some(&["acct-a".to_string()][..])
    );
    assert_eq!(
        inst.store.lock_report().dropped_pins,
        vec![("acme".to_string(), "elsewhere".to_string())]
    );
}

/// The store's refusals are of credentials that may well be good: none is
/// counted against the client IP, so none can trip the failed-auth throttle.
#[tokio::test]
async fn store_refusals_never_count_as_failed_credentials() {
    let inst = Instance::offline(&["file-only"]);
    let peer: SocketAddr = "203.0.113.9:4000".parse().unwrap();
    let ip = peer.ip();
    for _ in 0..(DEFAULT_AUTH_FAILURE_LIMIT * 2) {
        let resp = inst
            .state
            .authenticate_throttled(
                &ip,
                peer,
                &hdrs(&[("x-api-key", &test_key("file-only"))]),
                false,
                "proxy",
            )
            .unwrap_err();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    inst.answer(answer(unavailable(), full(&["file-only"], &[])))
        .await;
    for _ in 0..(DEFAULT_AUTH_FAILURE_LIMIT * 2) {
        let resp = inst
            .state
            .authenticate_throttled(
                &ip,
                peer,
                &hdrs(&[("x-api-key", &test_key("file-only"))]),
                false,
                "proxy",
            )
            .unwrap_err();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
    assert!(lock_recovering(&inst.state.auth_failures, "test").is_empty());
    let refusals: HashMap<&str, u64> = inst.metrics().refusals.into_iter().collect();
    assert_eq!(
        refusals["unloaded"],
        u64::from(DEFAULT_AUTH_FAILURE_LIMIT * 2)
    );
    assert_eq!(
        refusals["disabled"],
        u64::from(DEFAULT_AUTH_FAILURE_LIMIT * 2)
    );
}

/// The gate through the production router, on the wire.
#[tokio::test]
async fn the_gate_and_ready_on_the_wire() {
    let (upstream, _handle) = spawn_mock_upstream().await;
    let inst = Instance::new(
        &upstream,
        file_clients(&["file-only", "other"]),
        "https://store.invalid/_clients/poll",
        None,
    );
    let addr = serve(build_router(inst.state.clone())).await;
    let http = Client::new();
    let send = |key: String| {
        http.post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", key)
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
    };
    let ready = || http.get(format!("http://{addr}/_ready")).send();

    let resp = send(test_key("file-only")).await.unwrap();
    assert_eq!(resp.status(), 503);
    assert_eq!(resp.headers()["retry-after"], "1");
    assert_eq!(ready().await.unwrap().status(), 503);

    let doc = document(wall_ms(), vec![client("acme", &test_key("acme"))], &[]);
    inst.answer(answer(doc_part(&doc), full(&["file-only"], &[])))
        .await;
    assert_eq!(ready().await.unwrap().status(), 200);
    let resp = send(test_key("file-only")).await.unwrap();
    assert_eq!(resp.status(), 403);
    let json: Value = resp.json().await.unwrap();
    assert_eq!(json["error"]["message"], "client_disabled");
    assert_eq!(send(test_key("acme")).await.unwrap().status(), 200);
    assert_eq!(send(test_key("other")).await.unwrap().status(), 200);
}

/// Without a client store, `/_ready` has nothing to wait for.
#[tokio::test]
async fn ready_answers_200_without_a_client_store() {
    let (upstream, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&upstream, None);
    let addr = serve(app).await;
    let resp = Client::new()
        .get(format!("http://{addr}/_ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

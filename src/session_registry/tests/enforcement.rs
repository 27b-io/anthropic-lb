use super::*;

// ── Unit: pre-request-gate rejection counter (LAB-2551) ────────

/// A budget-exhausted request must both 429 and increment the rejection
/// counter under its (client, reason) key — the counter is the only
/// machine-readable record of a gate rejection (the warn! log is not
/// chartable).
#[tokio::test]
async fn gate_rejection_counted_by_client_and_reason() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 100u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });
    state.record_budget_usage("client-a", 200).await;

    for expected in [1u64, 2] {
        let resp = state
            .pre_request_gate("client-a", "claude-sonnet-4-6")
            .await
            .expect_err("exhausted budget must reject");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let counts = state.client_rejections.lock().unwrap();
        assert_eq!(
            counts.get(&("client-a".to_string(), "budget")),
            Some(&expected),
            "each rejection must increment the (client, budget) key"
        );
    }
}

/// Past the cap, rejections for NEW clients must lump into the single global
/// `_other` key (keeping the reason label) — a per-client overflow key would
/// be unbounded on the client axis under legacy header auth (CWE-770, the
/// LAB-2332 lesson). Existing keys keep counting past the cap.
#[test]
fn rejection_counter_overflow_lumps_into_global_other() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    // The cap bounds distinct CLIENTS, not (client, reason) entries: 32
    // clients on 2 reasons each is 64 entries but only 32 slots — a new
    // client must still be admitted with its own key.
    for i in 0..32 {
        state.note_client_rejection(&format!("client-{i}"), "budget");
        state.note_client_rejection(&format!("client-{i}"), "utilization");
    }
    state.note_client_rejection("client-32", "budget");
    {
        let counts = state.client_rejections.lock().unwrap();
        assert_eq!(
            counts.get(&("client-32".to_string(), "budget")),
            Some(&1),
            "entry count must not gate admission — only distinct clients do"
        );
    }
    // Fill up to the distinct-client cap.
    for i in 33..MAX_CLIENT_REJECTION_LABELS {
        state.note_client_rejection(&format!("client-{i}"), "budget");
    }
    // Over the cap: new clients bucket into ("_other", reason)…
    state.note_client_rejection("fresh-1", "budget");
    state.note_client_rejection("fresh-2", "brake");
    // …while an existing key still counts…
    state.note_client_rejection("client-0", "budget");
    // …and a TRACKED client's first hit on a NEW reason keeps its own key —
    // a brake event stamps every active client at once, so crossing the cap
    // mid-incident must not split a tracked client's attribution.
    state.note_client_rejection("client-0", "brake");

    let counts = state.client_rejections.lock().unwrap();
    assert_eq!(counts.get(&("_other".to_string(), "budget")), Some(&1));
    assert_eq!(counts.get(&("_other".to_string(), "brake")), Some(&1));
    assert_eq!(counts.get(&("client-0".to_string(), "budget")), Some(&2));
    assert_eq!(counts.get(&("client-0".to_string(), "brake")), Some(&1));
    assert!(
        counts.len() <= 3 * (MAX_CLIENT_REJECTION_LABELS + 1),
        "map must stay hard-bounded at reasons × (tracked clients + _other)"
    );
}

/// The client id is caller-controlled under legacy header auth: an oversized
/// value must be truncated BEFORE becoming a map key, or it is retained for
/// the process lifetime and re-serialized on every /metrics scrape.
#[test]
fn rejection_counter_truncates_client_label() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let huge = "x".repeat(4096);
    state.note_client_rejection(&huge, "brake");
    let counts = state.client_rejections.lock().unwrap();
    let (client, _) = counts.keys().next().expect("one entry recorded");
    assert!(
        client.chars().count() <= MAX_LABEL_CHARS + 1,
        "client label must be truncated (got {} chars)",
        client.chars().count()
    );
}

/// End to end: a budget-429 through the router must surface as
/// `anthropic_client_rejections_total{client,reason}` on /metrics, and the
/// family header must be present even before that (discoverability at zero).
#[tokio::test]
async fn metrics_expose_client_rejections() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 100u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });
    state.record_budget_usage("client-a", 200).await;
    let addr = serve(build_router(state)).await;
    let c = reqwest::Client::new();

    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("# TYPE anthropic_client_rejections_total counter"),
        "family header must be exported before any rejection:\n{m}"
    );

    let resp = c
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .header("x-client-id", "client-a")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains(r#"anthropic_client_rejections_total{client="client-a",reason="budget"} 1"#),
        "budget 429 must increment the labelled counter:\n{m}"
    );
}

#[tokio::test]
async fn limit_all_below() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.80);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.50, 0.40, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.60, 0.50, now + 10000, now + 100000).await;
    assert!(state
        .check_utilization_limit("testclient", "")
        .await
        .is_ok());
}

#[tokio::test]
async fn limit_all_above() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.50);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.90, 0.80, now + 10000, now + 100000).await;
    let result = state.check_utilization_limit("testclient", "").await;
    assert!(result.is_err(), "all above limit should return Err");
    let retry = result.unwrap_err();
    assert!(retry >= 60, "retry-after should be >= 60");
    assert!(retry <= 3600, "retry-after should be <= 3600");
}

#[tokio::test]
async fn limit_one_below() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.70);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.90, 0.80, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.50, 0.40, now + 10000, now + 100000).await;
    assert!(state
        .check_utilization_limit("testclient", "")
        .await
        .is_ok());
}

#[tokio::test]
async fn limit_no_config() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.check_utilization_limit("anyone", "").await.is_ok());
}

#[tokio::test]
async fn limit_operator_bypass() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("ray".to_string(), 0.10); // very low limit
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.95, 0.90, now + 10000, now + 100000).await;
    // Operator bypasses everything
    assert!(state.is_operator("ray"));
    assert!(state.pre_request_gate("ray", "").await.is_ok());
    // Non-operator does not bypass
    assert!(!state.is_operator("gastown"));
}

#[tokio::test]
async fn limit_no_compatible_accounts_passes() {
    // When no account serves the requested model, check_utilization_limit
    // should return Ok (let pick_account handle the "no account" error later)
    let mut limits = HashMap::new();
    limits.insert("test-client".to_string(), 0.01); // very low limit
    let mut acct = mk_endpoint("a", "sk-ant-api-x");
    acct.models = vec!["claude-sonnet".to_string()]; // only serves sonnet
    let state = Arc::new(AppState {
        endpoints: vec![acct],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Request for "claude-opus" — no account serves it → should pass
    assert!(
        state
            .check_utilization_limit("test-client", "claude-opus")
            .await
            .is_ok(),
        "should not 429 when no account serves the requested model"
    );
}

#[tokio::test]
async fn limit_unknown_accounts_fail_open() {
    // Accounts with no rate data (source="unknown") should not trigger the limit gate
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.30); // below the 0.5 unknown default
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Don't set any utilization — accounts remain "unknown" (0.5)
    // With limit=0.30, unknown 0.5 would appear "above limit" without fail-open
    assert!(
        state
            .check_utilization_limit("testclient", "")
            .await
            .is_ok(),
        "should fail-open when all accounts have unknown utilization"
    );
}

#[tokio::test]
async fn limit_mixed_known_unknown_fails_open() {
    // Known accounts above limit + one unknown account → should NOT 429
    // The unknown account may have capacity; let pick_account route to it
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("testclient".to_string(), 0.50);
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("a", "sk-ant-api-x"),
            mk_endpoint("b", "sk-ant-api-y"),
            mk_endpoint("c", "sk-ant-api-z"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Two known accounts above limit, one unknown (no data set)
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.90, 0.80, now + 10000, now + 100000).await;
    // Account "c" has no rate data → unknown
    assert!(
        state
            .check_utilization_limit("testclient", "")
            .await
            .is_ok(),
        "should fail-open when unknown compatible account may have capacity"
    );
}

#[tokio::test]
async fn emergency_all_above_threshold() {
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-x"),
        mk_endpoint("b", "sk-ant-api-y"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.90, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.97, 0.95, now + 10000, now + 100000).await;
    assert!(state.is_emergency_brake_active().await);
}

#[tokio::test]
async fn emergency_one_below() {
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-x"),
        mk_endpoint("b", "sk-ant-api-y"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.90, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.80, 0.70, now + 10000, now + 100000).await;
    assert!(!state.is_emergency_brake_active().await);
}

#[tokio::test]
async fn emergency_operator_bypass() {
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("ray".to_string(), 0.10);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        client_utilization_limits: limits,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.98, 0.96, now + 10000, now + 100000).await;
    assert!(state.is_emergency_brake_active().await);
    // Operator bypasses pre_request_gate even during emergency
    assert!(state.pre_request_gate("ray", "").await.is_ok());
    // Non-operator gets blocked
    assert!(state.pre_request_gate("gastown", "").await.is_err());
}

#[tokio::test]
async fn emergency_no_data() {
    // All accounts have default (0.5, "unknown") — brake should NOT activate (fail-open)
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-x"),
        mk_endpoint("b", "sk-ant-api-y"),
    ]);
    assert!(
        !state.is_emergency_brake_active().await,
        "brake should fail-open with no data"
    );
}

#[tokio::test]
async fn emergency_stale_data_with_unified() {
    // Stale reset times but valid unified utilization at 0.97
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        // Resets in the past → stale per-window data
        info.utilization_5h = Some(0.97);
        info.reset_5h = Some(1);
        info.utilization_7d = Some(0.97);
        info.reset_7d = Some(1);
        // But unified utilization is valid
        info.utilization = Some(0.97);
    }
    assert!(
        state.is_emergency_brake_active().await,
        "unified fallback should count"
    );
}

#[tokio::test]
async fn emergency_configurable_threshold() {
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        emergency_threshold: 0.80, // custom low threshold
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.85, 0.70, now + 10000, now + 100000).await;
    assert!(
        state.is_emergency_brake_active().await,
        "0.85 should exceed custom 0.80 threshold"
    );
}

// ── Emergency brake: known/unknown interaction tests ──
// These tests document the quota-maximization design intent:
// Unknown accounts (no rate data) return (0.5, "unknown") from effective_utilization.
// The brake does NOT fire when unknown accounts exist because:
//   (a) 0.5 < 0.88 default threshold → all_above = false, OR
//   (b) even if threshold is low enough, any_known = false blocks activation.
// This is intentional: unknown accounts might have capacity, and activating
// the brake blocks ALL non-operator traffic — a blunt instrument that wastes quota.

#[tokio::test]
async fn emergency_mixed_known_above_plus_unknown_preserves_capacity() {
    // Two known accounts above threshold + one unknown (no data).
    // Unknown returns (0.5, "unknown") — its 0.5 < 0.88 breaks the all_above check.
    // Brake stays inactive: the unknown account might have available quota.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("known-a", "sk-ant-api-a"),
        mk_endpoint("known-b", "sk-ant-api-b"),
        mk_endpoint("unknown-c", "sk-ant-api-c"), // no rate data set
    ]);
    set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.95, 0.91, now + 10000, now + 100000).await;
    // Account 2: no data → effective_utilization returns (0.5, "unknown")
    assert!(
        !state.is_emergency_brake_active().await,
        "brake must not fire: unknown account at 0.5 < 0.88 threshold means potential capacity"
    );
}

#[tokio::test]
async fn emergency_mixed_known_below_plus_unknown_inactive() {
    // Some known above, some known below, plus an unknown. Brake inactive on multiple grounds.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("high", "sk-ant-api-a"),
        mk_endpoint("low", "sk-ant-api-b"),
        mk_endpoint("unknown", "sk-ant-api-c"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.50, 0.40, now + 10000, now + 100000).await;
    // Account 2: no data
    assert!(
        !state.is_emergency_brake_active().await,
        "brake must not fire: known account below threshold + unknown account"
    );
}

#[tokio::test]
async fn emergency_unknown_with_low_threshold_still_fails_open() {
    // Edge case: threshold set to 0.4, below the unknown default of 0.5.
    // Unknown's 0.5 >= 0.4 so all_above stays true, BUT any_known is false.
    // The any_known guard prevents firing — fail-open even with aggressive threshold.
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("mystery", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        emergency_threshold: 0.40, // below unknown's default 0.5
        ..test_state_base()
    });
    // No rate data set — account returns (0.5, "unknown")
    // 0.5 >= 0.4 threshold → all_above is true
    // But any_known is false → brake does NOT fire
    assert!(
        !state.is_emergency_brake_active().await,
        "brake must fail-open: no known accounts even though unknown's 0.5 exceeds 0.40 threshold"
    );
}

#[tokio::test]
async fn emergency_mixed_known_above_low_threshold_plus_unknown_fires() {
    // Converse of above: threshold 0.4, one KNOWN account at 0.6, one unknown at default 0.5.
    // Both 0.6 >= 0.4 and 0.5 >= 0.4 → all_above = true.
    // Known account exists → any_known = true.
    // Brake fires. This is correct because we have real data showing distress.
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("known", "sk-ant-api-a"),
            mk_endpoint("unknown", "sk-ant-api-b"),
        ],
        state_path: PathBuf::from("/tmp/test.state.json"),
        emergency_threshold: 0.40,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.60, 0.55, now + 10000, now + 100000).await;
    // Account 1: no data → (0.5, "unknown"), 0.5 >= 0.4 → all_above stays true
    assert!(
        state.is_emergency_brake_active().await,
        "brake should fire: known account above 0.40 threshold + unknown's 0.5 also above"
    );
}

#[tokio::test]
async fn emergency_all_known_at_exact_threshold_fires() {
    // Boundary: accounts at exactly the threshold. The check is `util < threshold`,
    // so util == threshold means NOT below → all_above stays true → brake fires.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    set_account_utilization(&state, 0, 0.88, 0.88, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.88, 0.88, now + 10000, now + 100000).await;
    assert!(
        state.is_emergency_brake_active().await,
        "at exactly threshold (0.88): util is NOT < threshold, so brake should fire"
    );
}

#[tokio::test]
async fn emergency_one_known_just_below_threshold_inactive() {
    // Boundary: one account at threshold - epsilon. Just below → all_above = false.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);
    set_account_utilization(&state, 0, 0.96, 0.92, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.879, 0.85, now + 10000, now + 100000).await;
    assert!(
        !state.is_emergency_brake_active().await,
        "0.879 < 0.88 threshold: brake should not fire"
    );
}

#[tokio::test]
async fn emergency_single_known_above_threshold_fires() {
    // Single account fleet, known and above threshold → brake fires.
    let now = AppState::now_epoch();
    let state = test_state_with(vec![mk_endpoint("solo", "sk-ant-api-x")]);
    set_account_utilization(&state, 0, 0.95, 0.92, now + 10000, now + 100000).await;
    assert!(
        state.is_emergency_brake_active().await,
        "single known account above threshold: brake should fire"
    );
}

#[tokio::test]
async fn emergency_single_unknown_account_fails_open() {
    // Single unknown account — both guards prevent activation:
    // 0.5 < 0.88 → all_above = false, AND any_known = false.
    let state = test_state_with(vec![mk_endpoint("solo", "sk-ant-api-x")]);
    assert!(
        !state.is_emergency_brake_active().await,
        "single unknown account: must fail-open"
    );
}

#[tokio::test]
async fn gate_unknown_client_not_operator() {
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // "-" is not the operator
    assert!(!state.is_operator("-"));
    assert!(!state.is_operator("gastown"));
    assert!(state.is_operator("ray"));
}

// ── Task 7: Full integration tests ──

#[tokio::test]
async fn request_rejected_by_utilization_limit() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("-".to_string(), 0.50); // default client gets 0.50 limit
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-limit-reject.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Set utilization above client's limit (0.80 > 0.50)
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert!(
        resp.headers().get("retry-after").is_some(),
        "429 from utilization limit should include Retry-After"
    );
}

#[tokio::test]
async fn request_passes_utilization_limit() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("-".to_string(), 0.90);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-limit-pass.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        client_utilization_limits: limits,
        ..test_state_base()
    });
    // Set utilization below client's limit (0.50 < 0.90)
    set_account_utilization(&state, 0, 0.50, 0.40, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn emergency_brake_blocks_non_operator() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url),
            mk_endpoint_at("b", "sk-ant-api-test-bbb", &mock_url),
        ],
        state_path: PathBuf::from("/tmp/test-emergency-block.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // All accounts above emergency threshold. 5h=0.96 > emergency threshold (0.88).
    set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.97, 0.0, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("emergency"),
        "emergency brake response should mention 'emergency': {body}"
    );
}

#[tokio::test]
async fn emergency_brake_allows_operator() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut client_names = HashMap::new();
    client_names.insert("127.0.0.1".to_string(), "ray".to_string());
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url),
            mk_endpoint_at("b", "sk-ant-api-test-bbb", &mock_url),
        ],
        state_path: PathBuf::from("/tmp/test-emergency-operator.state.json"),
        proxy_key: Some("key".to_string()),
        client_names,
        auto_cache: false,
        operators: vec!["ray".to_string()],
        ..test_state_base()
    });
    // All accounts above emergency threshold — 5h only (avoid claim penalty on 7d)
    set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.97, 0.0, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    // Request comes from 127.0.0.1 which maps to "ray" (the operator)
    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "operator should bypass emergency brake"
    );
}

#[tokio::test]
async fn openai_handler_enforces_utilization_limit() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let mut limits = HashMap::new();
    limits.insert("-".to_string(), 0.50);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-openai-limit.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        client_utilization_limits: limits,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.80, 0.70, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "OpenAI-compat handler should enforce utilization limits"
    );
}

#[tokio::test]
async fn openai_handler_enforces_emergency_brake() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let now = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("a", "sk-ant-api-test-aaa", &mock_url)],
        state_path: PathBuf::from("/tmp/test-openai-emergency.state.json"),
        proxy_key: Some("key".to_string()),
        auto_cache: false,
        ..test_state_base()
    });
    set_account_utilization(&state, 0, 0.96, 0.0, now + 10000, now + 100000).await;

    let app = build_router(state);
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/chat/completions", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "OpenAI-compat handler should enforce emergency brake"
    );
}

#[tokio::test]
async fn no_new_config_identical_behavior() {
    // Default config: no operator, no limits, no emergency override
    // Should behave exactly like before the feature was added
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&mock_url, Some("key".to_string()));
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("content-type", "application/json")
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    // Default config: no limits, no emergency → request should succeed
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Budget status header present even with no config (default healthy)
    assert!(resp.headers().get("x-budget-status").is_some());

    // Stats should still work with aggregate section
    let stats = client
        .get(format!("http://{}/_stats", addr))
        .header("x-api-key", "key")
        .send()
        .await
        .unwrap();
    assert_eq!(stats.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = stats.json().await.unwrap();
    assert!(body["endpoints"].is_array());
    assert!(body["aggregate"].is_object());
    assert_eq!(body["strategy"], "dynamic-capacity-v1");
}

// ── AC-9 / AC-10: enforcement in pre_request_gate ──

#[tokio::test]
async fn gate_denies_model_outside_client_allow_list_with_403_naming_both() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    let err = state
        .pre_request_gate("limited", "claude-opus-5")
        .await
        .expect_err("opus must be denied");
    assert_eq!(
        err.status(),
        StatusCode::FORBIDDEN,
        "policy denial is 403, not 429 — 429 means 'retry later', which this never becomes"
    );
    let body = axum::body::to_bytes(err.into_body(), 64 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("limited"),
        "body must name the client: {text}"
    );
    assert!(
        text.contains("claude-opus-5"),
        "body must name the model: {text}"
    );
}

#[tokio::test]
async fn gate_allows_model_inside_client_allow_list() {
    let state = state_with_clients(vec![mk_client("limited", "k1", &["claude-haiku-*"])]);
    assert!(state
        .pre_request_gate("limited", "claude-haiku-4-5")
        .await
        .is_ok());
}

#[tokio::test]
async fn gate_allow_list_bypassed_by_operators() {
    let state = Arc::new(AppState {
        clients: vec![mk_client("limited", "k1", &["claude-haiku-*"])],
        operators: vec!["limited".to_string()],
        ..test_state_base()
    });
    assert!(
        state
            .pre_request_gate("limited", "claude-opus-5")
            .await
            .is_ok(),
        "operators bypass the allow-list like every other gate check"
    );
}

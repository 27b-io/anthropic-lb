use super::*;

// ── Priority tier tests ──────────────────────────────────────────

#[tokio::test]
async fn pick_account_respects_priority_tiers() {
    // Tier 0 accounts with headroom should get ALL traffic — tier 1 gets nothing.
    let mut primary = mk_endpoint("primary", "sk-ant-api-a");
    primary.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-b");
    fallback.priority = 1;
    let state = test_state_with(vec![primary, fallback]);

    let now = AppState::now_epoch();
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 0, "all traffic should go to tier 0 when healthy");
    }
}

#[tokio::test]
async fn pick_account_falls_through_to_lower_priority() {
    // Tier 0 hard-limited → tier 1 should receive traffic.
    let mut primary = mk_endpoint("primary", "sk-ant-api-a");
    primary.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-b");
    fallback.priority = 1;
    let state = test_state_with(vec![primary, fallback]);

    // Hard-limit tier 0
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(
            idx, 1,
            "tier 1 should get traffic when tier 0 is hard-limited"
        );
    }
}

#[tokio::test]
async fn pick_account_priority_default_zero() {
    // Accounts without explicit priority should behave as tier 0.
    let a = mk_endpoint("a", "sk-ant-api-a");
    let b = mk_endpoint("b", "sk-ant-api-b");
    assert_eq!(a.priority, 0);
    assert_eq!(b.priority, 0);

    let state = test_state_with(vec![a, b]);
    let now = AppState::now_epoch();
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    // Both should receive traffic (same tier)
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }
    assert!(counts[0] > 0, "account a should get traffic");
    assert!(counts[1] > 0, "account b should get traffic");
}

#[tokio::test]
async fn pick_account_priority_soft_limit_stays_in_tier() {
    // Tier 0 accounts above soft_limit but still alive (weight > 0) → tier 0 is
    // degraded-used, NOT skipped. soft_limit is intra-tier load-shedding; it must
    // never cause a jump to a lower-priority (paid) tier while free capacity remains.
    let mut primary_a = mk_endpoint("primary_a", "sk-ant-api-a");
    primary_a.priority = 0;
    let mut primary_b = mk_endpoint("primary_b", "sk-ant-api-b");
    primary_b.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-c");
    fallback.priority = 1;
    let state = test_state_with_soft_limit(vec![primary_a, primary_b, fallback], 0.90);

    let now = AppState::now_epoch();
    // Tier 0: above soft limit (0.95) but still has headroom — weight > 0.
    for i in 0..2 {
        let mut info = state.endpoints[i].rate_info.write().await;
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now + 10000);
    }
    // Tier 1: healthy.
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert!(
            idx == 0 || idx == 1,
            "tier 0 must be drained (degraded) before tier 1 is touched"
        );
    }
}

#[tokio::test]
async fn pick_account_priority_zero_weight_tier_falls_through() {
    // Tier 0 accounts at zero weight (util 1.0 → gate 1.0) → genuinely exhausted →
    // routing falls through to tier 1.
    let mut primary_a = mk_endpoint("primary_a", "sk-ant-api-a");
    primary_a.priority = 0;
    let mut primary_b = mk_endpoint("primary_b", "sk-ant-api-b");
    primary_b.priority = 0;
    let mut fallback = mk_endpoint("fallback", "sk-ant-api-c");
    fallback.priority = 1;
    let state = test_state_with_soft_limit(vec![primary_a, primary_b, fallback], 0.90);

    let now = AppState::now_epoch();
    // Tier 0: fully exhausted — util 1.0 → gate 1.0 → weight 0.
    for i in 0..2 {
        let mut info = state.endpoints[i].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now + 10000);
    }
    // Tier 1: healthy.
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 2, "tier 1 used once tier 0 is genuinely zero-weight");
    }
}

#[tokio::test]
async fn pick_account_multiple_tiers_cascade() {
    // Three tiers: 0, 1, 2. Tier 0 and 1 exhausted → tier 2 gets traffic.
    let mut t0 = mk_endpoint("t0", "sk-ant-api-a");
    t0.priority = 0;
    let mut t1 = mk_endpoint("t1", "sk-ant-api-b");
    t1.priority = 1;
    let mut t2 = mk_endpoint("t2", "sk-ant-api-c");
    t2.priority = 2;
    let state = test_state_with(vec![t0, t1, t2]);

    // Hard-limit tiers 0 and 1
    for i in 0..2 {
        let mut info = state.endpoints[i].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(
            idx, 2,
            "tier 2 should get traffic when tiers 0 and 1 are exhausted"
        );
    }
}

#[tokio::test]
async fn pick_account_all_tiers_exhausted_returns_none() {
    // All tiers hard-limited → None.
    let mut t0 = mk_endpoint("t0", "sk-ant-api-a");
    t0.priority = 0;
    let mut t1 = mk_endpoint("t1", "sk-ant-api-b");
    t1.priority = 1;
    let state = test_state_with(vec![t0, t1]);

    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    assert!(state.pick_endpoint(None, "", &[]).await.is_none());
}

// ── Overage tests ────────────────────────────────────────────────

#[tokio::test]
async fn update_rate_info_parses_overage_headers() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-overage-in-use",
        "true".parse().unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-overage-status",
        "allowed".parse().unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-overage-utilization",
        "0.25".parse().unwrap(),
    );
    let reset = AppState::now_epoch() + 100000;
    headers.insert(
        "anthropic-ratelimit-unified-overage-reset",
        reset.to_string().parse().unwrap(),
    );
    state.update_rate_info(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(info.overage_in_use);
    assert_eq!(info.overage_status.as_deref(), Some("allowed"));
    assert_eq!(info.overage_utilization, Some(0.25));
    assert_eq!(info.overage_reset, Some(reset));
}

#[tokio::test]
async fn update_rate_info_overage_absent_resets_to_false() {
    // Corner 1: an account previously in overage whose next response omits the
    // overage-in-use header must drop back to overage_in_use=false.
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a")]);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.overage_in_use = true;
        info.overage_status = Some("allowed".to_string());
        info.overage_utilization = Some(0.5);
    }
    // Fresh response with no overage headers at all.
    let headers = reqwest::header::HeaderMap::new();
    state.update_rate_info(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(!info.overage_in_use, "overage_in_use must reset to false");
    assert_eq!(info.overage_status, None);
    assert_eq!(info.overage_utilization, None);
}

#[test]
fn routing_weight_overage_active_keeps_account_routable() {
    // 5h subscription window rejected/exhausted, but overage is covering it →
    // the account must have non-zero weight, not be dropped.
    let now = AppState::now_epoch();
    let mut info = RateLimitInfo {
        utilization_5h: Some(1.0),
        reset_5h: Some(now + 3000),
        status_5h: Some("rejected".to_string()),
        ..Default::default()
    };
    info.overage_in_use = true;
    info.overage_status = Some("allowed".to_string());
    info.overage_utilization = Some(0.0);
    info.overage_reset = Some(now + 100000);

    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("overage account must not be skipped");
    assert!(rw.overage_active, "overage_active flag must be set");
    assert!(
        rw.weight > 0.0,
        "overage account with fresh overage budget must have non-zero weight, got {}",
        rw.weight
    );
    assert_eq!(rw.source, "overage");
}

#[test]
fn routing_weight_overage_exhausted_zero_weight() {
    // Overage in use but overage budget itself exhausted (utilization 1.0) → weight 0.
    let now = AppState::now_epoch();
    let mut info = RateLimitInfo {
        utilization_5h: Some(1.0),
        reset_5h: Some(now + 3000),
        status_5h: Some("rejected".to_string()),
        ..Default::default()
    };
    info.overage_in_use = true;
    info.overage_status = Some("allowed".to_string());
    info.overage_utilization = Some(1.0);
    info.overage_reset = Some(now + 100000);

    let rw =
        compute_routing_weight(&info, "claude-sonnet-4-6", now, false).expect("still a candidate");
    assert_eq!(rw.weight, 0.0, "exhausted overage → zero weight");
}

#[tokio::test]
async fn pick_account_overage_demoted_below_free() {
    // Free account (eff. priority 0) must drain before an overage account
    // (eff. priority 0 + overage_penalty 10) receives any traffic.
    let free = mk_endpoint("free", "sk-ant-api-a");
    let overage = mk_endpoint("overage", "sk-ant-api-b");
    let state = test_state_with(vec![free, overage]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now + 3000);
        info.status_5h = Some("rejected".to_string());
        info.overage_in_use = true;
        info.overage_status = Some("allowed".to_string());
        info.overage_utilization = Some(0.0);
        info.overage_reset = Some(now + 100000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 0, "free account preferred over overage account");
    }
}

#[tokio::test]
async fn pick_account_overage_used_when_free_exhausted() {
    // Free account at zero weight → the overage account (demoted) is used.
    let free = mk_endpoint("free", "sk-ant-api-a");
    let overage = mk_endpoint("overage", "sk-ant-api-b");
    let state = test_state_with(vec![free, overage]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(1.0); // zero weight
        info.reset_5h = Some(now + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(1.0);
        info.reset_5h = Some(now + 3000);
        info.status_5h = Some("rejected".to_string());
        info.overage_in_use = true;
        info.overage_status = Some("allowed".to_string());
        info.overage_utilization = Some(0.0);
        info.overage_reset = Some(now + 100000);
    }

    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 1, "overage account used once free tier is exhausted");
    }
}

#[tokio::test]
async fn pick_endpoint_openai_is_last_resort() {
    // A priority-100 OpenAI endpoint is a routing candidate: a healthy
    // Anthropic endpoint beats it; it is selected only once the lower
    // tier is exhausted.
    let mut openai = make_endpoint("fallback", Protocol::OpenAI);
    openai.priority = 100;
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-a"), openai]);
    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }

    for _ in 0..50 {
        assert_eq!(
            state.pick_endpoint(None, "", &[]).await,
            Some(0),
            "healthy endpoint beats the priority-100 openai endpoint"
        );
    }

    // Hard-limit the Anthropic endpoint → the openai endpoint is the only
    // remaining candidate.
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }
    assert_eq!(
        state.pick_endpoint(None, "", &[]).await,
        Some(1),
        "openai endpoint selected once the lower tier is exhausted"
    );
}

#[test]
fn is_operator_checks_configured_operator() {
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec!["special-operator".to_string()],
        ..test_state_base()
    });

    assert!(state.is_operator("special-operator"));
    assert!(!state.is_operator("regular-client"));
}

#[test]
fn is_operator_returns_false_when_no_operator_configured() {
    let state = test_state_with(vec![]);
    assert!(!state.is_operator("any-client"));
}

#[test]
fn is_operator_supports_multiple_operators() {
    let state = Arc::new(AppState {
        client: Client::new(),
        client_nonstreaming: Client::new(),
        state_path: PathBuf::from("/tmp/test.state.json"),
        operators: vec![
            "ray".to_string(),
            "openclaw".to_string(),
            "claude".to_string(),
        ],
        ..test_state_base()
    });
    assert!(state.is_operator("ray"));
    assert!(state.is_operator("openclaw"));
    assert!(state.is_operator("claude"));
    assert!(!state.is_operator("gastown"));
    assert!(!state.is_operator("-"));
}

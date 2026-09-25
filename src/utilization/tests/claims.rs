use super::*;

// ── resolve_7d_claim tests ────────────────────────────────────

#[test]
fn resolve_7d_claim_model_specific() {
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.80),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    claims.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        claims_7d: claims,
        ..Default::default()
    };
    let claim = resolve_7d_claim(&info, "claude-sonnet-4-6").unwrap();
    assert!(
        (claim.utilization.unwrap() - 0.80).abs() < 0.001,
        "should pick model-specific claim"
    );
}

#[test]
fn resolve_7d_claim_fallback_general() {
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        claims_7d: claims,
        ..Default::default()
    };
    let claim = resolve_7d_claim(&info, "claude-sonnet-4-6").unwrap();
    assert!(
        (claim.utilization.unwrap() - 0.50).abs() < 0.001,
        "should fall back to general"
    );
}

#[test]
fn resolve_7d_claim_empty_claims() {
    let info = RateLimitInfo::default();
    assert!(resolve_7d_claim(&info, "claude-sonnet-4-6").is_none());
}

#[test]
fn resolve_7d_claim_empty_model() {
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(1000000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        resolve_7d_claim(&info, "").is_none(),
        "empty model should return None"
    );
}

// ── Per-claim 7d model-specific tests ──────────────────────────

#[tokio::test]
async fn model_specific_routing() {
    // Sonnet 7d at 0.85 (no penalty), Opus 7d at 0.10 → Opus sees low util
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.85),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    claims_7d.insert(
        "seven_day_opus".to_string(),
        ClaimWindowData {
            utilization: Some(0.10),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    let (util_opus, _, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    // Sonnet 7d at 0.85 (max with 5h 0.30) = 0.85, Opus 7d at 0.10 (max with 5h 0.30) = 0.30
    assert!(
        (util_sonnet - 0.85).abs() < 0.01,
        "sonnet should be 0.85: got {util_sonnet}"
    );
    // Opus at 0.10 → should stay at ~0.30 (max with 5h)
    assert!(util_opus < 0.35, "opus should be low: got {util_opus}");
    // The gap should be large — this is the whole point of per-claim routing
    assert!(
        util_sonnet - util_opus > 0.50,
        "sonnet-opus gap should be >0.50"
    );
}

#[tokio::test]
async fn claim_fallback_general() {
    // Only "seven_day" (general) claim → used for all models
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    let (util_opus, _, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    let (util_haiku, _, _, _) = effective_utilization(&info, now_epoch, "claude-haiku-4-5");
    // All should see the same general claim (0.50, no penalty)
    assert!(
        (util_sonnet - 0.50).abs() < 0.01,
        "sonnet: got {util_sonnet}"
    );
    assert!((util_opus - 0.50).abs() < 0.01, "opus: got {util_opus}");
    assert!((util_haiku - 0.50).abs() < 0.01, "haiku: got {util_haiku}");
}

#[tokio::test]
async fn cross_model_isolation() {
    // Sonnet claim at 0.95 should NOT affect Opus effective_utilization
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    // No opus claim and no general claim
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util_opus, source, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    // Opus has no specific claim and no general fallback → only 5h at 0.20
    assert_eq!(source, "5h");
    assert!(
        (util_opus - 0.20).abs() < 0.01,
        "opus should only see 5h: got {util_opus}"
    );

    let (util_sonnet, _, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    assert!(
        (util_sonnet - 0.95).abs() < 0.01,
        "sonnet should be 0.95: got {util_sonnet}"
    );
}

#[tokio::test]
async fn emergency_brake_worst_case() {
    // Emergency brake (model="") should use max across all claims
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    claims_7d.insert(
        "seven_day_opus".to_string(),
        ClaimWindowData {
            utilization: Some(0.10),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util, _, _, _) = effective_utilization(&info, now_epoch, "");
    // Should pick sonnet's 0.95 (no penalty now)
    assert!(
        (util - 0.95).abs() < 0.01,
        "emergency brake should use worst claim: got {util}"
    );
}

#[tokio::test]
async fn stale_claim_eviction() {
    // Claim with expired reset should be ignored
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch - 1), // stale
            status: None,
            ..Default::default()
        },
    );
    claims_7d.insert(
        "seven_day_opus".to_string(),
        ClaimWindowData {
            utilization: Some(0.30),
            reset: Some(now_epoch + 100000), // fresh
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    // For sonnet model: sonnet claim is stale, no general fallback → only 5h
    let (util_sonnet, source, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    assert_eq!(source, "5h");
    assert!(
        (util_sonnet - 0.20).abs() < 0.01,
        "stale sonnet should be ignored: got {util_sonnet}"
    );

    // For opus model: opus claim is fresh
    let (util_opus, source, _, _) = effective_utilization(&info, now_epoch, "claude-opus-4-6");
    assert_eq!(source, "7d");
    assert!(
        (util_opus - 0.30).abs() < 0.01,
        "fresh opus should be used: got {util_opus}"
    );
}

#[tokio::test]
async fn model_family_extraction() {
    assert_eq!(model_family("claude-sonnet-4-6"), "sonnet");
    assert_eq!(model_family("claude-opus-4-6"), "opus");
    assert_eq!(model_family("claude-haiku-4-5"), "haiku");
    assert_eq!(model_family("claude-3-5-sonnet"), "sonnet");
    assert_eq!(model_family("claude-fable-5"), "fable");
    assert_eq!(model_family("unknown-model"), "");
}

#[tokio::test]
async fn constraining_claims_fable_pairs_band_with_pool() {
    let mut info = RateLimitInfo::default();
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.30),
            ..Default::default()
        },
    );

    // No band claim yet → primary is the general claim, no pool cap.
    let (primary, cap) = constraining_7d_claims(&info, "claude-fable-5");
    assert_eq!(primary.unwrap().utilization, Some(0.30));
    assert!(cap.is_none(), "no band claim → nothing to pair");

    // Band claim present → (band, Some(pool)).
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(0.80),
            ..Default::default()
        },
    );
    let (primary, cap) = constraining_7d_claims(&info, "claude-fable-5");
    assert_eq!(primary.unwrap().utilization, Some(0.80));
    assert_eq!(cap.unwrap().utilization, Some(0.30));

    // Non-Fable families never get a pool cap, even with a band claim present.
    let (_, cap) = constraining_7d_claims(&info, "claude-sonnet-4-6");
    assert!(cap.is_none(), "sonnet must keep single-claim semantics");
}

#[tokio::test]
async fn fable_effective_utilization_binds_on_worse_of_band_and_pool() {
    let now_epoch = AppState::now_epoch();
    let mut info = RateLimitInfo::default();
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.90),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(0.20),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );

    // Fable is bound by the drained pool (0.90), not its roomy band (0.20).
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "claude-fable-5");
    assert_eq!(source, "7d");
    assert!((util - 0.90).abs() < 0.01, "pool should bind: got {util}");
}

#[tokio::test]
async fn worst_case_utilization_allowlists_claims() {
    // Model-agnostic worst case (emergency brake path) reads ONLY claims
    // that gate all traffic. Neither the exhausted Fable band nor an
    // unknown future carve-out may drive it — either would brake ALL
    // traffic while regular budgets are healthy.
    let now_epoch = AppState::now_epoch();
    let mut info = RateLimitInfo::default();
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.30),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now_epoch + 302400),
            status: Some("rejected".to_string()),
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        "seven_day_future_carveout_oi".to_string(),
        ClaimWindowData {
            utilization: Some(0.95),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );

    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "7d");
    assert!(
        (util - 0.30).abs() < 0.01,
        "only allowlisted claims may drive the worst case: got {util}"
    );

    // Sanity on the predicate itself.
    assert!(claim_gates_all_traffic("seven_day"));
    assert!(claim_gates_all_traffic("seven_day_sonnet"));
    assert!(claim_gates_all_traffic("seven_day_opus"));
    assert!(claim_gates_all_traffic("seven_day_haiku"));
    assert!(!claim_gates_all_traffic(FABLE_BAND_CLAIM));
    assert!(!claim_gates_all_traffic("seven_day_future_carveout_oi"));
}

#[tokio::test]
async fn parse_7d_oi_headers_populates_band_claim() {
    // Header shape captured from a live claude-fable-5 response through the
    // LB on 2026-07-21 (LAB-387 verification): the Fable band arrives as the
    // 7d_oi triplet, NOT as a seven_day_fable representative claim.
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);
    let now_epoch = AppState::now_epoch();

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-representative-claim",
        HeaderValue::from_static("five_hour"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.09"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-utilization",
        HeaderValue::from_static("0.15"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-reset",
        HeaderValue::from_str(&format!("{}", now_epoch + 302400)).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d-status",
        HeaderValue::from_static("allowed"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-utilization",
        HeaderValue::from_static("0.26"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-reset",
        HeaderValue::from_str(&format!("{}", now_epoch + 302400)).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-status",
        HeaderValue::from_static("allowed"),
    );
    state.update_rate_info(0, &headers).await;

    {
        let info = state.endpoints[0].rate_info.read().await;
        let band = info.claims_7d.get(FABLE_BAND_CLAIM).expect("band claim");
        assert_eq!(band.utilization, Some(0.26));
        assert_eq!(band.reset, Some(now_epoch + 302400));
        assert_eq!(band.status.as_deref(), Some("allowed"));
        let general = info.claims_7d.get("seven_day").expect("general claim");
        assert_eq!(general.utilization, Some(0.15));
    }

    // A later non-Fable response (no 7d_oi triplet — the sonnet shape
    // captured from the same account) must NOT clear the band claim.
    let mut sonnet_headers = reqwest::header::HeaderMap::new();
    sonnet_headers.insert(
        "anthropic-ratelimit-unified-representative-claim",
        HeaderValue::from_static("five_hour"),
    );
    sonnet_headers.insert(
        "anthropic-ratelimit-unified-7d-utilization",
        HeaderValue::from_static("0.16"),
    );
    state.update_rate_info(0, &sonnet_headers).await;

    {
        let info = state.endpoints[0].rate_info.read().await;
        let band = info.claims_7d.get(FABLE_BAND_CLAIM).expect("band persists");
        assert_eq!(band.utilization, Some(0.26), "absence must not clear band");
        assert_eq!(
            info.claims_7d.get("seven_day").unwrap().utilization,
            Some(0.16),
            "general claim refreshed by the non-fable response"
        );
    }
}

#[tokio::test]
async fn band_only_signal_never_drives_brake_fallback() {
    // CodeRabbit finding on PR #87: the flat `info.utilization` derivation
    // included the band, so with 5h absent/stale and no allowlisted 7d
    // claim, a drained band could reach the brake via the raw-unified
    // fallback. A band-only account must resolve to "unknown" (fail-open),
    // not to the band's utilization.
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);
    let now_epoch = AppState::now_epoch();

    // Fable response shape carrying ONLY the band triplet (no 5h/7d data).
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-utilization",
        HeaderValue::from_static("1.0"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-reset",
        HeaderValue::from_str(&format!("{}", now_epoch + 302400)).unwrap(),
    );
    headers.insert(
        "anthropic-ratelimit-unified-7d_oi-status",
        HeaderValue::from_static("rejected"),
    );
    state.update_rate_info(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.claims_7d.contains_key(FABLE_BAND_CLAIM),
        "band claim parsed"
    );
    assert_eq!(
        info.utilization, None,
        "flat unified utilization must not derive from the band"
    );
    assert_eq!(
        info.utilization_7d, None,
        "flat 7d utilization must not derive from the band"
    );
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(
        source, "unknown",
        "model-agnostic view of a band-only account is unknown, got {util} from {source}"
    );
}

#[tokio::test]
async fn fable_band_wr_survives_pool_with_missing_reset() {
    // A pool claim with utilization but NO reset yields waste_risk 0.0
    // (missing data, not a real zero) — it must not erase the band's
    // urgency signal via min().
    let now_epoch = AppState::now_epoch();
    let mut info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        ..Default::default()
    };
    info.claims_7d.insert(
        FABLE_BAND_CLAIM.to_string(),
        ClaimWindowData {
            utilization: Some(0.20),
            reset: Some(now_epoch + 302400),
            status: None,
            ..Default::default()
        },
    );
    info.claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.90),
            reset: None, // stale/missing — waste_risk yields 0.0
            status: None,
            last_seen: now_epoch,
        },
    );

    let rw = compute_routing_weight(&info, "claude-fable-5", now_epoch, false)
        .expect("account routable");
    assert!(
        rw.wr > 0.0,
        "band waste_risk must survive a data-less pool cap: got {}",
        rw.wr
    );
}

#[tokio::test]
async fn extract_client_version_parsing() {
    assert_eq!(
        extract_client_version("claude-cli/2.1.68 (external, cli)"),
        Some("2.1.68")
    );
    assert_eq!(extract_client_version("anthropic-sdk/1.0.0"), Some("1.0.0"));
    assert_eq!(extract_client_version("curl/8.5.0"), Some("8.5.0"));
    assert_eq!(extract_client_version("no-version"), None);
    assert_eq!(extract_client_version("foo/"), None);
}

#[tokio::test]
async fn flat_field_compat_fallback() {
    // When claims_7d is empty, should fall back to flat utilization_7d fields
    let now_epoch = AppState::now_epoch();
    let info = RateLimitInfo {
        utilization_5h: Some(0.30),
        reset_5h: Some(now_epoch + 10000),
        utilization_7d: Some(0.60),
        reset_7d: Some(now_epoch + 100000),
        // claims_7d empty — migration/compat path
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "claude-sonnet-4-6");
    assert_eq!(source, "7d");
    assert!((util - 0.60).abs() < 0.01, "should use flat 7d: got {util}");
}

#[tokio::test]
async fn claim_aware_routing_prefers_low_model_util() {
    // Account A: Sonnet 7d at 0.90 (high), Opus 7d at 0.10 (low)
    // Account B: Sonnet 7d at 0.10 (low), Opus 7d at 0.90 (high)
    // Routing for Sonnet should prefer B, routing for Opus should prefer A
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // Set 5h low for both
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, now + 100000).await;

    // Set model-specific 7d utilization
    set_model_utilization(&state, 0, "claude-sonnet-4-6", 0.90, now + 100000).await;
    set_model_utilization(&state, 0, "claude-opus-4-6", 0.10, now + 100000).await;
    set_model_utilization(&state, 1, "claude-sonnet-4-6", 0.10, now + 100000).await;
    set_model_utilization(&state, 1, "claude-opus-4-6", 0.90, now + 100000).await;

    // Run many picks — Sonnet routing should consistently prefer B (index 1)
    let mut sonnet_picks = [0u32; 2];
    let mut opus_picks = [0u32; 2];
    for i in 0..100 {
        let key = format!("client_{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            sonnet_picks[idx] += 1;
        }
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
        {
            opus_picks[idx] += 1;
        }
    }
    // B should get most Sonnet traffic (has lower Sonnet util)
    assert!(
        sonnet_picks[1] > sonnet_picks[0],
        "Sonnet should prefer B: A={}, B={}",
        sonnet_picks[0],
        sonnet_picks[1]
    );
    // A should get most Opus traffic (has lower Opus util)
    assert!(
        opus_picks[0] > opus_picks[1],
        "Opus should prefer A: A={}, B={}",
        opus_picks[0],
        opus_picks[1]
    );
}

use super::*;

// ── time_adjusted_utilization unit tests ────────────────────────

#[test]
fn time_adjust_no_reset() {
    // No reset timestamp → raw util returned unchanged
    let result = time_adjusted_utilization(Some(0.80), None, None, NEAR_RESET_5H_SECS, 1000000);
    assert_eq!(result, Some(0.80));
}

#[test]
fn time_adjust_outside_threshold() {
    // 5h reset in 2 hours = 7200s, threshold is 3600s → no discount
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    assert_eq!(result, Some(0.90));
}

#[test]
fn time_adjust_inside_threshold() {
    // 5h reset in 30 min = 1800s, threshold 3600s → discount = 1800/3600 = 0.50
    let now = 1000000u64;
    let reset = now + 1800;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    let expected = 0.90 * 0.50;
    assert!((result.unwrap() - expected).abs() < 1e-10);
}

#[test]
fn time_adjust_at_threshold_boundary() {
    // Reset exactly at threshold boundary (1h = 3600s) → discount = 3600/3600 = 1.0
    let now = 1000000u64;
    let reset = now + 3600;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    assert_eq!(result, Some(0.90));
}

#[test]
fn time_adjust_near_reset_floor() {
    // Reset in 1 minute = 60s → discount = max(60/3600, 0.05) = 0.05 (floor)
    let now = 1000000u64;
    let reset = now + 60;
    let raw = 60.0 / 3600.0; // 0.0167, below TIME_FRACTION_FLOOR
    assert!(raw < TIME_FRACTION_FLOOR);
    let result = time_adjusted_utilization(Some(0.95), Some(reset), None, NEAR_RESET_5H_SECS, now);
    let expected = 0.95 * TIME_FRACTION_FLOOR;
    assert!((result.unwrap() - expected).abs() < 1e-10);
}

#[test]
fn time_adjust_past_reset() {
    // Reset already happened → None (stale data)
    let now = 1000000u64;
    let reset = now - 100;
    let result = time_adjusted_utilization(Some(0.90), Some(reset), None, NEAR_RESET_5H_SECS, now);
    assert_eq!(result, None);
}

#[test]
fn time_adjust_throttled() {
    // Status=throttled overrides low util → floor at 0.98
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("throttled"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
}

#[test]
fn time_adjust_warning() {
    // Status=allowed_warning overrides low util → floor at 0.80
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.50),
        Some(reset),
        Some("allowed_warning"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(WARNING_UTIL_FLOOR));
}

#[test]
fn time_adjust_warning_already_higher() {
    // Util already above warning floor → util wins
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.90),
        Some(reset),
        Some("allowed_warning"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(0.90));
}

#[test]
fn time_adjust_none_util() {
    // No utilization data → None
    let result = time_adjusted_utilization(None, Some(1000000), None, NEAR_RESET_5H_SECS, 999000);
    assert_eq!(result, None);
}

#[test]
fn time_adjust_7d_window() {
    // 7d reset in 3 hours = 10800s, threshold 21600s → discount = 10800/21600 = 0.50
    let now = 1000000u64;
    let reset = now + 10800;
    let result = time_adjusted_utilization(Some(0.80), Some(reset), None, NEAR_RESET_7D_SECS, now);
    let expected = 0.80 * 0.50;
    assert!((result.unwrap() - expected).abs() < 1e-10);
}

#[test]
fn time_adjust_throttled_overrides_discount() {
    // Near reset AND throttled → discount applies but floor wins
    let now = 1000000u64;
    let reset = now + 600;
    let result = time_adjusted_utilization(
        Some(0.50),
        Some(reset),
        Some("throttled"),
        NEAR_RESET_5H_SECS,
        now,
    );
    // Discounted: 0.50 * (600/3600) = 0.083, but throttle floor = 0.98
    assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
}

#[test]
fn time_adjust_status_without_reset() {
    // Status present but no reset → floor applied to raw util
    let result = time_adjusted_utilization(
        Some(0.30),
        None,
        Some("throttled"),
        NEAR_RESET_5H_SECS,
        1000000,
    );
    assert_eq!(result, Some(THROTTLE_UTIL_FLOOR));
}

#[test]
fn time_adjust_rejected() {
    // Status=rejected → floor at 1.0 (fully exhausted, zero bucket share)
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("rejected"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(REJECTED_UTIL_FLOOR));
}

#[test]
fn time_adjust_rejected_near_reset() {
    // Even near reset, rejected still maps to 1.0 — API is actively refusing
    let now = 1000000u64;
    let reset = now + 60; // 1 minute from reset
    let result = time_adjusted_utilization(
        Some(0.95),
        Some(reset),
        Some("rejected"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(REJECTED_UTIL_FLOOR));
}

#[test]
fn time_adjust_unknown_status_gets_warning_floor() {
    // Unknown non-"allowed" status → defensive WARNING_UTIL_FLOOR (Bug #4)
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("some_future_status"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(WARNING_UTIL_FLOOR));
}

#[test]
fn time_adjust_allowed_status_no_floor() {
    // Explicit "allowed" status → no floor, just raw util
    let now = 1000000u64;
    let reset = now + 7200;
    let result = time_adjusted_utilization(
        Some(0.30),
        Some(reset),
        Some("allowed"),
        NEAR_RESET_5H_SECS,
        now,
    );
    assert_eq!(result, Some(0.30));
}

#[tokio::test]
async fn status_clears_when_header_absent() {
    // Bug #1: stale status should clear when API sends utilization but no status header
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);

    // First response: set throttled status
    let mut headers1 = reqwest::header::HeaderMap::new();
    headers1.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.30"),
    );
    headers1.insert(
        "anthropic-ratelimit-unified-5h-status",
        HeaderValue::from_static("throttled"),
    );
    headers1.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("9999999999"),
    );
    state.update_rate_info(0, &headers1).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(info.status_5h.as_deref(), Some("throttled"));
    }

    // Second response: utilization header present, NO status header → clears
    let mut headers2 = reqwest::header::HeaderMap::new();
    headers2.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.25"),
    );
    headers2.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("9999999999"),
    );
    state.update_rate_info(0, &headers2).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.status_5h, None,
            "status should clear when header absent"
        );
    }
}

#[tokio::test]
async fn status_persists_when_no_util_header() {
    // If neither util nor status header is present for a window, don't clear status
    // (the response might be for a different window entirely)
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);

    // Set throttled status
    let mut headers1 = reqwest::header::HeaderMap::new();
    headers1.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.30"),
    );
    headers1.insert(
        "anthropic-ratelimit-unified-5h-status",
        HeaderValue::from_static("throttled"),
    );
    state.update_rate_info(0, &headers1).await;

    // Response with no 5h headers at all (maybe only 7d headers)
    let headers2 = reqwest::header::HeaderMap::new();
    state.update_rate_info(0, &headers2).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.status_5h.as_deref(),
            Some("throttled"),
            "status should persist when no util header for that window"
        );
    }
}

#[tokio::test]
async fn pick_returns_none_when_all_rejected() {
    // Bug #5: all-rejected accounts should return None (zero total headroom)
    let now_epoch = AppState::now_epoch();
    let state = test_state_with(vec![
        mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
        mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
    ]);
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.50);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.50);
        info.status_5h = Some("rejected".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.reset_7d = Some(now_epoch + 86400);
    }
    let result = state.pick_endpoint(None, "", &[]).await;
    assert_eq!(result, None, "all-rejected should return None");
}

#[tokio::test]
async fn reset_sanity_rejects_far_future() {
    // Bug #6: reset timestamp > block duration from now should be rejected
    let accounts = vec![mk_endpoint("acct-a", "sk-ant-api-test-aaa")];
    let state = test_state_with(accounts);

    let mut headers = reqwest::header::HeaderMap::new();
    // 5h window: reset 10h from now (> 5h max) → should NOT be stored
    headers.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("9999999999"),
    );
    // 7d window: reset 30d from now (> 7d max) → should NOT be stored
    headers.insert(
        "anthropic-ratelimit-unified-7d-reset",
        HeaderValue::from_static("9999999999"),
    );
    state.update_rate_info(0, &headers).await;
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert_eq!(
            info.reset_5h, None,
            "far-future 5h reset should be rejected"
        );
        assert_eq!(
            info.reset_7d, None,
            "far-future 7d reset should be rejected"
        );
    }
}

// ── Effective utilization tests ────────────────────────────────

#[tokio::test]
async fn effective_util_both_windows() {
    let now_epoch = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.80),
            reset: Some(now_epoch + 100000),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.60),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    // 7d at 0.80 (no penalty). Max(0.60, 0.80) = 0.80
    assert_eq!(source, "7d");
    assert!((util - 0.80).abs() < 0.01, "expected ~0.80, got {util}");
}

#[tokio::test]
async fn effective_util_5h_only() {
    let now_epoch = AppState::now_epoch();
    // 7d claim with reset in the past → stale, evicted by time_adjusted_utilization
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.90),
            reset: Some(now_epoch - 1),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.60),
        reset_5h: Some(now_epoch + 10000),
        claims_7d,
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "5h");
    assert!(
        (util - 0.60).abs() < 0.01,
        "should use 5h value: got {util}"
    );
}

#[tokio::test]
async fn effective_util_7d_only() {
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
        // 5h stale
        utilization_5h: Some(0.40),
        reset_5h: Some(now_epoch - 1),
        claims_7d,
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "7d");
    // 0.50 is below CLAIM_PENALTY_THRESHOLD, so no penalty
    assert!(
        (util - 0.50).abs() < 0.01,
        "should use 7d value: got {util}"
    );
}

#[tokio::test]
async fn effective_util_fallback_unified() {
    let now_epoch = AppState::now_epoch();
    // Both 5h and all 7d claims stale → falls through to unified
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.50),
            reset: Some(now_epoch - 1),
            status: None,
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.40),
        reset_5h: Some(now_epoch - 1),
        claims_7d,
        utilization: Some(0.65),
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "unified");
    assert!(
        (util - 0.65).abs() < 0.001,
        "should use unified: got {util}"
    );
}

#[tokio::test]
async fn effective_util_fallback_legacy() {
    let now_epoch = AppState::now_epoch();
    let info = RateLimitInfo {
        remaining_tokens: Some(300_000),
        limit_tokens: Some(1_000_000),
        ..Default::default()
    };
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "legacy");
    assert!(
        (util - 0.70).abs() < 0.01,
        "should use legacy token ratio: got {util}"
    );
}

#[tokio::test]
async fn effective_util_fallback_unknown() {
    let now_epoch = AppState::now_epoch();
    let info = RateLimitInfo::default();
    let (util, source, _, _) = effective_utilization(&info, now_epoch, "");
    assert_eq!(source, "unknown");
    assert!(
        (util - 0.50).abs() < 0.001,
        "should default to 0.5: got {util}"
    );
}

#[test]
fn status_to_floor_mapping() {
    assert_eq!(status_to_floor(Some("rejected")), REJECTED_UTIL_FLOOR);
    assert_eq!(status_to_floor(Some("throttled")), THROTTLE_UTIL_FLOOR);
    assert_eq!(status_to_floor(Some("allowed_warning")), WARNING_UTIL_FLOOR);
    assert_eq!(status_to_floor(Some("allowed")), 0.0);
    assert_eq!(status_to_floor(None), 0.0);
}

#[test]
fn status_to_floor_unknown_defaults_to_warning() {
    let floor = status_to_floor(Some("unknown_status"));
    assert_eq!(
        floor, WARNING_UTIL_FLOOR,
        "unknown status should map to warning floor"
    );
}

#[test]
fn status_to_ordinal_mapping() {
    assert_eq!(status_to_ordinal(Some("rejected")), 3.0);
    assert_eq!(status_to_ordinal(Some("throttled")), 2.0);
    assert_eq!(status_to_ordinal(Some("allowed_warning")), 1.0);
    assert_eq!(status_to_ordinal(Some("allowed")), 0.0);
    assert_eq!(status_to_ordinal(None), 0.0);
    // Unknown statuses map to 1.0 (warning-level)
    assert_eq!(status_to_ordinal(Some("new_unknown_status")), 1.0);
}

#[test]
fn time_adjusted_utilization_stale_data() {
    let now = 1000000u64;
    let reset_past = 999000u64; // Reset already happened

    let result =
        time_adjusted_utilization(Some(0.50), Some(reset_past), Some("allowed"), 3600.0, now);

    assert_eq!(
        result, None,
        "stale data (reset in past) should return None"
    );
}

#[test]
fn time_adjusted_utilization_near_reset() {
    let now = 1000000u64;
    let reset = now + 1800; // 30 minutes until reset
    let near_reset_threshold = 3600.0; // 1 hour threshold

    let result = time_adjusted_utilization(
        Some(0.80),
        Some(reset),
        Some("allowed"),
        near_reset_threshold,
        now,
    );

    assert!(result.is_some());
    let adjusted = result.unwrap();
    assert!(
        adjusted < 0.80,
        "near-reset utilization should be discounted"
    );
    assert!(
        adjusted >= 0.04,
        "should apply minimum discount floor (0.05)"
    );
}

#[test]
fn time_adjusted_utilization_mid_block() {
    let now = 1000000u64;
    let reset = now + 10800; // 3 hours until reset
    let near_reset_threshold = 3600.0; // 1 hour threshold

    let result = time_adjusted_utilization(
        Some(0.80),
        Some(reset),
        Some("allowed"),
        near_reset_threshold,
        now,
    );

    assert!(result.is_some());
    let adjusted = result.unwrap();
    assert_eq!(adjusted, 0.80, "mid-block utilization should be unchanged");
}

#[test]
fn time_adjusted_utilization_status_floor_minimum() {
    let now = 1000000u64;
    let reset = now + 100; // Very close to reset

    let result = time_adjusted_utilization(
        Some(0.80),
        Some(reset),
        Some("throttled"), // Floor of 0.98
        3600.0,
        now,
    );

    assert!(result.is_some());
    let adjusted = result.unwrap();
    assert_eq!(
        adjusted, THROTTLE_UTIL_FLOOR,
        "status floor should override time discount"
    );
}

#[tokio::test]
async fn effective_utilization_prefers_most_constrained() {
    // When both windows have data, should return max (most constrained)
    let now = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.60),
            reset: Some(now + 100000),
            status: Some("allowed".to_string()),
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.80),
        reset_5h: Some(now + 10000),
        status_5h: Some("allowed".to_string()),
        claims_7d,
        ..Default::default()
    };

    let (util, source, _, _) = effective_utilization(&info, now, "");
    assert!(util > 0.60, "should use higher (5h) utilization");
    assert_eq!(source, "5h");
}

#[tokio::test]
async fn effective_utilization_7d_no_penalty() {
    // 7d window should pass through without penalty
    let now = AppState::now_epoch();
    let mut claims_7d = HashMap::new();
    claims_7d.insert(
        "seven_day".to_string(),
        ClaimWindowData {
            utilization: Some(0.85),
            reset: Some(now + 100000),
            status: Some("allowed".to_string()),
            ..Default::default()
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.50),
        reset_5h: Some(now + 10000),
        status_5h: Some("allowed".to_string()),
        claims_7d,
        ..Default::default()
    };

    let (util, _, _, _) = effective_utilization(&info, now, "");
    assert!(
        (util - 0.85).abs() < 0.01,
        "7d window should pass through at 0.85 without penalty: got {util}"
    );
}

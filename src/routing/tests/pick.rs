use super::*;

// ── Unit: pick_account ──────────────────────────────────────────

#[tokio::test]
async fn pick_prefers_lowest_utilization() {
    // With weighted buckets, the account with more headroom should get
    // a proportionally larger share of traffic
    let state = test_state_with(vec![
        mk_endpoint("high", "sk-ant-api-high"),
        mk_endpoint("low", "sk-ant-api-low"),
    ]);

    // high=0.8 (headroom 0.2), low=0.2 (headroom 0.8) → 80% should go to "low"
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.8);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.2);
    }

    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    // "low" (idx=1) should get ~80% of traffic (±5%)
    let low_pct = counts[1] as f64 / 1000.0;
    assert!(
        (0.75..=0.85).contains(&low_pct),
        "low-util account should get ~80% traffic, got {:.1}%",
        low_pct * 100.0
    );
}

#[tokio::test]
async fn pick_skips_hard_limited() {
    let state = test_state_with(vec![
        mk_endpoint("limited", "sk-ant-api-a"),
        mk_endpoint("available", "sk-ant-api-b"),
    ]);

    // Hard-limit the first account
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.1); // great utilization but hard-limited
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.9);
    }

    let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
    assert_eq!(
        idx, 1,
        "should skip hard-limited account despite lower utilization"
    );
}

#[tokio::test]
async fn pick_round_robin_when_no_info() {
    // With no utilization data, all accounts get headroom=0.5 (equal buckets)
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
        mk_endpoint("c", "sk-ant-api-c"),
    ]);

    // Call many times without affinity — Fibonacci scatter should distribute evenly
    let mut counts = [0u32; 3];
    for _ in 0..300 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    // Each should get ~33% (±10%)
    for (i, &count) in counts.iter().enumerate() {
        let pct = count as f64 / 300.0;
        assert!(
            (0.23..=0.43).contains(&pct),
            "account {} should get ~33% traffic, got {:.1}%",
            i,
            pct * 100.0
        );
    }
}

#[tokio::test]
async fn pick_returns_none_when_all_limited() {
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);

    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
    }

    assert!(state.pick_endpoint(None, "", &[]).await.is_none());
}

#[tokio::test]
async fn pick_recovers_after_hard_limit_expires() {
    // After a hard limit expires with stale data, the account should still be
    // selectable with 0.5 (unknown) utilization instead of being permanently stuck.
    let state = test_state_with(vec![mk_endpoint("recovering", "sk-ant-api-a")]);

    // Simulate mark_hard_limited: set hard_limited_until in the past (expired),
    // poison remaining_tokens to 0, set high utilization from the 429 response.
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let hard_limit_time = Instant::now() - Duration::from_secs(10);
        info.hard_limited_until = Some(hard_limit_time);
        info.remaining_tokens = Some(0);
        info.remaining_requests = Some(0);
        info.utilization = Some(1.0);
        info.utilization_5h = Some(1.0);
        // last_updated before the hard limit → stale_after_hard_limit = true
        info.last_updated = Some(hard_limit_time - Duration::from_secs(1));
    }

    let result = state.pick_endpoint(None, "", &[]).await;
    assert!(
        result.is_some(),
        "account with expired hard limit should be selectable despite stale high utilization"
    );
}

#[tokio::test]
async fn pick_ignores_stale_rejected_claim_after_hard_limit() {
    // A "rejected" 7d claim from a 429 response should not permanently block the
    // account once the hard limit has expired without fresh data.
    let state = test_state_with(vec![mk_endpoint("recovering", "sk-ant-api-a")]);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let hard_limit_time = Instant::now() - Duration::from_secs(10);
        info.hard_limited_until = Some(hard_limit_time);
        info.last_updated = Some(hard_limit_time - Duration::from_secs(1));
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch + 100000),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert!(
        result.is_some(),
        "stale rejected claim after expired hard limit should not block account"
    );
}

#[tokio::test]
async fn pick_still_skips_fresh_rejected_claim() {
    // If data was refreshed AFTER the hard limit (e.g., by a probe that got fresh
    // "rejected" status), the account should still be skipped.
    let state = test_state_with(vec![
        mk_endpoint("rejected", "sk-ant-api-a"),
        mk_endpoint("available", "sk-ant-api-b"),
    ]);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let hard_limit_time = Instant::now() - Duration::from_secs(300);
        info.hard_limited_until = Some(hard_limit_time);
        // last_updated AFTER the hard limit → data is fresh, not stale
        info.last_updated = Some(Instant::now() - Duration::from_secs(5));
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch + 100000),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.5);
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert_eq!(
        result,
        Some(1),
        "fresh rejected claim should still skip the account"
    );
}

#[tokio::test]
async fn pick_ignores_expired_rejected_claim_without_hard_limit() {
    let state = test_state_with(vec![
        mk_endpoint("recovered", "sk-ant-api-a"),
        mk_endpoint("available", "sk-ant-api-b"),
    ]);
    let now_epoch = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.30);
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now_epoch.saturating_sub(1)),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.80);
        info.utilization_5h = Some(0.80);
        info.reset_5h = Some(now_epoch + 10000);
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert_eq!(
        result,
        Some(0),
        "expired rejected claim should not block account selection"
    );
}

#[tokio::test]
async fn pick_uses_fresh_data_after_hard_limit_cleared() {
    // After a probe clears hard_limited_until and refreshes data, the normal
    // routing logic should apply (not the 0.5 fallback).
    let state = test_state_with(vec![
        mk_endpoint("low_util", "sk-ant-api-a"),
        mk_endpoint("high_util", "sk-ant-api-b"),
    ]);

    {
        // hard_limited_until is None (cleared by probe), fresh data available
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.2);
        info.last_updated = Some(Instant::now());
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.8);
        info.last_updated = Some(Instant::now());
    }

    // low_util should get ~80% of traffic (headroom=0.8 vs 0.2)
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }
    let low_pct = counts[0] as f64 / 1000.0;
    assert!(
        low_pct > 0.70,
        "low-util account should get majority of traffic, got {:.1}%",
        low_pct * 100.0
    );
}

#[tokio::test]
async fn mark_hard_limited_detects_burst_429() {
    // Burst 429: x-should-retry=true, no retry-after, no rate-limit headers.
    // Should use short cooldown, NOT poison remaining_tokens/requests.
    let state = test_state_with(vec![mk_endpoint("burst-test", "sk-ant-api-a")]);

    // Pre-set some remaining tokens to verify they aren't poisoned
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.remaining_tokens = Some(5000);
        info.remaining_requests = Some(10);
    }

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-should-retry", HeaderValue::from_static("true"));
    // No retry-after, no anthropic-ratelimit-*, no x-ratelimit-*

    state.mark_hard_limited(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.hard_limited_until.is_some(),
        "burst 429 should still set hard_limited_until"
    );
    assert_eq!(
        info.remaining_tokens,
        Some(5000),
        "burst 429 should NOT poison remaining_tokens"
    );
    assert_eq!(
        info.remaining_requests,
        Some(10),
        "burst 429 should NOT poison remaining_requests"
    );
    assert_eq!(info.consecutive_burst_429s, 1);
}

#[tokio::test]
async fn mark_hard_limited_capacity_429_poisons_state() {
    // Capacity 429: has rate-limit headers → should poison remaining_tokens/requests to 0.
    let state = test_state_with(vec![mk_endpoint("cap-test", "sk-ant-api-a")]);

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.remaining_tokens = Some(5000);
        info.remaining_requests = Some(10);
    }

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-should-retry", HeaderValue::from_static("true"));
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("0.99"),
    );
    // Has rate-limit headers → NOT a burst

    state.mark_hard_limited(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.remaining_tokens,
        Some(0),
        "capacity 429 should poison remaining_tokens to 0"
    );
    assert_eq!(
        info.remaining_requests,
        Some(0),
        "capacity 429 should poison remaining_requests to 0"
    );
    assert_eq!(
        info.consecutive_burst_429s, 0,
        "capacity 429 should reset burst counter"
    );
}

#[tokio::test]
async fn mark_hard_limited_burst_exponential_backoff() {
    // Consecutive burst 429s should produce increasing cooldowns.
    let state = test_state_with(vec![mk_endpoint("backoff-test", "sk-ant-api-a")]);

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-should-retry", HeaderValue::from_static("true"));

    // Fire 5 consecutive burst 429s
    let mut cooldowns = Vec::new();
    for _ in 0..5 {
        state.mark_hard_limited(0, &headers).await;
        let info = state.endpoints[0].rate_info.read().await;
        let until = info.hard_limited_until.unwrap();
        let remaining = until.duration_since(Instant::now());
        cooldowns.push(remaining.as_secs());
    }

    // Should be roughly 5, 10, 20, 40, 60 (with some timing slack)
    assert!(
        cooldowns[0] <= 6,
        "1st burst should be ~5s, got {}s",
        cooldowns[0]
    );
    assert!(
        (9..=11).contains(&cooldowns[1]),
        "2nd burst should be ~10s, got {}s",
        cooldowns[1]
    );
    assert!(
        (19..=21).contains(&cooldowns[2]),
        "3rd burst should be ~20s, got {}s",
        cooldowns[2]
    );
    assert!(
        (39..=41).contains(&cooldowns[3]),
        "4th burst should be ~40s, got {}s",
        cooldowns[3]
    );
    assert!(
        (59..=61).contains(&cooldowns[4]),
        "5th burst should be ~60s, got {}s",
        cooldowns[4]
    );

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.consecutive_burst_429s, 5);
}

#[tokio::test]
async fn mark_hard_limited_retry_after_overrides_default() {
    // When retry-after is present, it should be used regardless of x-should-retry.
    let state = test_state_with(vec![mk_endpoint("retry-test", "sk-ant-api-a")]);

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("30"));

    state.mark_hard_limited(0, &headers).await;

    let info = state.endpoints[0].rate_info.read().await;
    let until = info.hard_limited_until.unwrap();
    let remaining = until.duration_since(Instant::now()).as_secs();
    assert!(
        (29..=31).contains(&remaining),
        "retry-after=30 should set ~30s cooldown, got {}s",
        remaining
    );
    assert_eq!(
        info.consecutive_burst_429s, 0,
        "non-burst 429 should reset burst counter"
    );
}

#[tokio::test]
async fn pick_does_not_bias_unknown_accounts() {
    // Unknown accounts get headroom=0.5, known account with 0.1 util gets headroom=0.9
    // Traffic should favor the known account proportionally
    let state = test_state_with(vec![
        mk_endpoint("known", "sk-ant-api-known"),
        mk_endpoint("unknown", "sk-ant-api-unknown"),
    ]);

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.1); // headroom = 0.9
    }
    // accounts[1] has no rate info → headroom = 0.5

    // known should get ~64% (0.9 / 1.4), unknown ~36% (0.5 / 1.4)
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    let known_pct = counts[0] as f64 / 1000.0;
    assert!(
        (0.57..=0.71).contains(&known_pct),
        "known account should get ~64% traffic, got {:.1}%",
        known_pct * 100.0
    );
}

#[tokio::test]
async fn pick_sticky_same_affinity() {
    // Same affinity key should always return the same account when headroom
    // is close enough. LEGACY_AFFINITY_OVERRIDE_RATIO (0.5) compares the picked
    // account's affinity_headroom to the other's — affinity is preserved when
    // the ratio exceeds the threshold, i.e. no single account has 2x the room.
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
        mk_endpoint("c", "sk-ant-api-c"),
    ]);

    // Similar utilization → similar weights → ratio stays above 0.5
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.40);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.45);
    }
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization = Some(0.50);
    }

    let key = "192.168.1.1:client-42:agent-7:session-abc";
    let first = state.pick_endpoint(Some(key), "", &[]).await.unwrap();
    for _ in 0..100 {
        let idx = state.pick_endpoint(Some(key), "", &[]).await.unwrap();
        assert_eq!(
            idx, first,
            "same affinity key must always pick same account"
        );
    }
}

#[tokio::test]
async fn pick_affinity_overridden_by_weight_disparity() {
    // When the affinity-picked account's weight is less than 50% of the best
    // account's weight, affinity should be overridden. This tests the scenario
    // where 5h utilization is similar but 7d utilization is vastly different —
    // the weight formula captures the 7d disparity via waste_risk.
    let state = test_state_with(vec![
        mk_endpoint("low_7d", "sk-ant-api-a"),
        mk_endpoint("high_7d", "sk-ant-api-b"),
    ]);
    let now_epoch = AppState::now_epoch();

    // Both have similar 5h utilization (so gate_5h is similar)
    // but vastly different 7d utilization via claims_7d.
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.12);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.30),
                reset: Some(now_epoch + 300000),
                status: None,
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(now_epoch + 10000);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.85),
                reset: Some(now_epoch + 300000),
                status: None,
                ..Default::default()
            },
        );
    }

    // Every affinity key should pick the low-7d account because
    // its weight is much higher (more waste_risk × similar headroom).
    for i in 0..200 {
        let key = format!("sticky-client-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_eq!(
            idx, 0,
            "client {} picked account {} but should pick 'low_7d' (weight ratio < 0.5)",
            i, idx
        );
    }
}

#[tokio::test]
async fn affinity_override_balanced_no_7d_data() {
    // Scenario: balanced 5h, no 7d data → affinity preserved
    // Primary 5h=0.20, Jeff 5h=0.25, no claims_7d
    // Weights: headroom_only → 0.80 vs 0.75, ratio=0.94 > 0.5
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.25);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }

    assert_affinity_distribution(
        &state,
        "balanced-client",
        500,
        None,
        "balanced accounts should see traffic on both (affinity preserved)",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_moderate_7d_disparity() {
    // Scenario: similar 5h, moderate 7d difference → affinity preserved
    // Primary 5h=0.15, 7d=0.40 vs Jeff 5h=0.15, 7d=0.60
    // unused-7d headroom 0.60 vs 0.40 isn't extreme enough to trigger override
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.15, 0.40, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.15, 0.60, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "moderate-client",
        500,
        None,
        "moderate disparity should preserve affinity on both accounts",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_massive_7d_disparity() {
    // Scenario: egregious disparity — one account nearly spent, other fresh
    // Primary 5h=0.10, 7d=0.10 vs Jeff 5h=0.10, 7d=0.95
    // Jeff's unused-7d headroom (0.05) is far below 0.25× primary's (0.90)
    // → all traffic overridden to primary
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.95, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "production-client",
        200,
        Some(0),
        "routed to jeff despite massive 7d disparity",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_one_exhausted() {
    // Scenario: one account nearly spent on 7d budget
    // Primary 5h=0.10, 7d=0.10 vs Jeff 5h=0.10, 7d=0.90
    // Extreme headroom ratio → all traffic to primary
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "exhausted-client",
        200,
        Some(0),
        "routed to nearly-exhausted account",
    )
    .await;
}

// ── Unit: model-based routing ──────────────────────────────────

#[test]
fn account_serves_model_no_filter() {
    let acct = mk_endpoint("a", "sk-ant-api-x");
    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-haiku-4-5"));
    assert!(acct.serves_model(""));
}

#[test]
fn account_serves_model_exact_match() {
    let mut acct = mk_endpoint("a", "sk-ant-api-x");
    acct.models = vec!["claude-sonnet-4-6".to_string()];
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(!acct.serves_model("claude-opus-4-6"));
}

#[test]
fn account_serves_model_prefix_match() {
    let mut acct = mk_endpoint("a", "sk-ant-api-x");
    acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-*".to_string()];
    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(!acct.serves_model("claude-haiku-4-5"));
}

#[tokio::test]
async fn pick_account_filters_by_model() {
    let mut acct_a = mk_endpoint("opus-only", "sk-ant-api-a");
    acct_a.models = vec!["claude-opus-*".to_string()];

    let acct_b = mk_endpoint("any-model", "sk-ant-api-b");

    let state = test_state_with(vec![acct_a, acct_b]);

    // Requesting opus: both accounts eligible
    let idx = state
        .pick_endpoint(None, "claude-opus-4-6", &[])
        .await
        .unwrap();
    assert!(idx == 0 || idx == 1);

    // Requesting haiku: only acct_b eligible
    let idx = state
        .pick_endpoint(None, "claude-haiku-4-5", &[])
        .await
        .unwrap();
    assert_eq!(idx, 1);
}

#[tokio::test]
async fn soft_limit_excludes_overloaded_accounts() {
    let acct_a = mk_endpoint("healthy", "sk-ant-api-a");
    let acct_b = mk_endpoint("overloaded", "sk-ant-api-b");

    let accounts = vec![acct_a, acct_b];

    // Set utilizations before building state
    let now = AppState::now_epoch();
    {
        let mut info = accounts[0].rate_info.write().await;
        info.utilization = Some(0.30);
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
    }
    {
        let mut info = accounts[1].rate_info.write().await;
        info.utilization = Some(0.95);
        info.utilization_5h = Some(0.95);
        info.reset_5h = Some(now + 10000);
    }

    let state = Arc::new(AppState {
        endpoints: accounts,
        soft_limit: 0.90,
        ..test_state_base()
    });

    // Try many affinity keys — all should route to healthy (idx 0)
    for i in 0..20 {
        let key = format!("client-{}", i);
        let idx = state.pick_endpoint(Some(&key), "any", &[]).await.unwrap();
        assert_eq!(
            idx, 0,
            "client '{}' routed to overloaded account despite soft limit",
            key
        );
    }
}

#[tokio::test]
async fn routing_candidates_ignore_unmatched_7d_state() {
    let state = test_state_with(vec![mk_endpoint("acct-a", "sk-ant-api-a")]);
    let now = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.20);
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now + 10000);
        info.claims_7d.insert(
            "seven_day_haiku".to_string(),
            ClaimWindowData {
                utilization: Some(0.95),
                reset: Some(now + 100000),
                status: Some("throttled".to_string()),
                ..Default::default()
            },
        );
        // Derived aggregate 7d fields reflect the unrelated claim, but routing
        // for opus must ignore them when no applicable 7d claim exists.
        info.utilization_7d = Some(0.95);
        info.reset_7d = Some(now + 100000);
        info.status_7d = Some("throttled".to_string());
    }

    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    assert_eq!(candidates.len(), 1, "expected one routing candidate");

    let candidate = &candidates[0];
    assert_eq!(candidate.source, "headroom_only");
    assert!(
        (candidate.gate_7d - 0.0).abs() < f64::EPSILON,
        "unmatched 7d state should not leak into gate_7d: {:?}",
        candidate
    );
    assert!(
        (candidate.weight - 0.8).abs() < 0.0001,
        "weight should remain 5h headroom-only when no 7d claim applies: {:?}",
        candidate
    );
}

// ── Unit: unified endpoint participation in routing ────────────

#[tokio::test]
async fn openai_endpoint_participates_at_configured_priority() {
    let acct = mk_endpoint("anthropic", "sk-ant-api-a");
    {
        let mut info = acct.rate_info.write().await;
        info.utilization = Some(0.0); // healthy
    }
    let mut state = test_state_with(vec![acct]);
    let st = Arc::get_mut(&mut state).unwrap();
    let mut ep = make_endpoint("openai", Protocol::OpenAI);
    ep.priority = 100;
    st.endpoints.push(ep);

    let candidates = state.routing_candidates("claude-opus-4-7", &[]).await;
    let openai_candidate = candidates.iter().find(|c| c.source == "openai");
    assert!(
        openai_candidate.is_some(),
        "openai endpoint must be a candidate"
    );
    let c = openai_candidate.unwrap();
    assert_eq!(c.endpoint, 1, "openai endpoint is at index 1");
    assert_eq!(c.priority, 100);
    assert_eq!(c.weight, 1.0);
    assert_eq!(c.gate, 0.0);
}

#[tokio::test]
async fn openai_endpoint_with_opus_only_allowlist_excludes_sonnet() {
    let mut state = test_state_with(vec![]);
    let st = Arc::get_mut(&mut state).unwrap();
    let mut ep = make_endpoint("opus-gw", Protocol::OpenAI);
    ep.models = vec!["claude-opus-*".to_string()];
    st.endpoints.push(ep);

    let cs_opus = state.routing_candidates("claude-opus-4-7", &[]).await;
    let cs_sonnet = state.routing_candidates("claude-sonnet-4-6", &[]).await;
    assert_eq!(cs_opus.len(), 1, "opus must hit the opus-only endpoint");
    assert_eq!(cs_sonnet.len(), 0, "sonnet must be filtered out");
}

// ── pick_account integration tests for time-adjusted routing ────

#[tokio::test]
async fn pick_prefers_near_reset_account() {
    // Account A: 5h=0.95 reset in 10min, 7d=0.30 (7d binding after discount)
    // Account B: 5h=0.60 reset in 3h, 7d=0.50
    // A's 5h gets heavily discounted, 7d=0.30 becomes binding → A has more headroom than B
    let now_epoch = AppState::now_epoch();
    let accounts = vec![
        mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
        mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
    ];
    let state = test_state_with(accounts);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.95);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.95);
        info.reset_5h = Some(now_epoch + 600); // 10 min
        info.reset_7d = Some(now_epoch + 86400); // 1 day out
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.60);
        info.utilization_7d = Some(0.50);
        info.utilization = Some(0.60);
        info.reset_5h = Some(now_epoch + 10800); // 3 hours
        info.reset_7d = Some(now_epoch + 86400);
    }

    // Run 100 picks without affinity to see distribution
    let mut a_count = 0;
    for _ in 0..100 {
        if let Some(idx) = state.pick_endpoint(None, "", &[]).await {
            if idx == 0 {
                a_count += 1;
            }
        }
    }
    // A's effective = max(adj_5h, adj_7d) = max(0.95*0.167, 0.30) = 0.30
    // B's effective = max(0.60, 0.50) = 0.60 (5h outside discount zone)
    // A headroom=0.70, B headroom=0.40 → A gets ~64% of traffic
    assert!(
        a_count > 50,
        "Account A (near-reset 5h) should get majority: got {a_count}/100"
    );
}

#[tokio::test]
async fn pick_throttled_excludes() {
    // Account A: status=throttled (floor=0.98, above soft_limit=0.90)
    // Account B: healthy
    let now_epoch = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
            mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
        ],
        soft_limit: 0.90, // Key: not 1.0 — throttled (0.98) will be excluded
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.utilization_7d = Some(0.20);
        info.utilization = Some(0.30);
        info.status_5h = Some("throttled".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.reset_7d = Some(now_epoch + 86400);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.40);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.40);
        info.status_5h = Some("allowed".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.reset_7d = Some(now_epoch + 86400);
    }

    let mut b_count = 0;
    for _ in 0..100 {
        if let Some(idx) = state.pick_endpoint(None, "", &[]).await {
            if idx == 1 {
                b_count += 1;
            }
        }
    }
    // A is throttled → effective=0.98 → excluded by soft_limit=0.90
    // B gets all traffic
    assert_eq!(b_count, 100, "Throttled account A should be soft-excluded");
}

#[tokio::test]
async fn pick_model_specific_7d_throttled_claim_excludes() {
    let now_epoch = AppState::now_epoch();
    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
            mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
        ],
        soft_limit: 0.90,
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.utilization = Some(0.20);
        info.status_5h = Some("allowed".to_string());
        info.reset_5h = Some(now_epoch + 7200);
        info.claims_7d.insert(
            "seven_day_opus".to_string(),
            ClaimWindowData {
                utilization: Some(0.20),
                reset: Some(now_epoch + 86400),
                status: Some("throttled".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.40);
        info.utilization = Some(0.40);
        info.status_5h = Some("allowed".to_string());
        info.reset_5h = Some(now_epoch + 7200);
    }

    let mut b_count = 0;
    for i in 0..100 {
        let key = format!("sticky-opus-{i}");
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
        {
            if idx == 1 {
                b_count += 1;
            }
        }
    }

    assert_eq!(
        b_count, 100,
        "model-specific throttled 7d claim should soft-exclude account A"
    );
}

#[tokio::test]
async fn pick_mid_block_unchanged() {
    // Both accounts mid-block (3h remaining on 5h) — outside discount zone
    // Should behave identically to raw utilization
    let now_epoch = AppState::now_epoch();
    let accounts = vec![
        mk_endpoint("acct-a", "sk-ant-api-test-aaa"),
        mk_endpoint("acct-b", "sk-ant-api-test-bbb"),
    ];
    let state = test_state_with(accounts);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.80);
        info.utilization_7d = Some(0.40);
        info.utilization = Some(0.80);
        info.reset_5h = Some(now_epoch + 10800); // 3h out
        info.reset_7d = Some(now_epoch + 86400);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.40);
        info.utilization_7d = Some(0.30);
        info.utilization = Some(0.40);
        info.reset_5h = Some(now_epoch + 10800);
        info.reset_7d = Some(now_epoch + 86400);
    }

    let mut b_count = 0;
    for _ in 0..100 {
        if let Some(idx) = state.pick_endpoint(None, "", &[]).await {
            if idx == 1 {
                b_count += 1;
            }
        }
    }
    // A: effective=max(0.80, 0.40)=0.80, headroom=0.20
    // B: effective=max(0.40, 0.30)=0.40, headroom=0.60
    // B should get ~75% (0.60 / 0.80)
    assert!(
        b_count > 60,
        "Mid-block: B (lower util) should dominate: got {b_count}/100"
    );
    assert!(
        b_count < 90,
        "Mid-block: A should still get some traffic: B got {b_count}/100"
    );
}

// ── pick_account waste_risk routing tests ─────────────────────

#[tokio::test]
async fn pick_account_prefers_expiring_quota() {
    // Account A: 7d=0.40, reset in 1 day → high waste_risk
    // Account B: 7d=0.40, reset in 6 days → low waste_risk
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // Both 5h at 0.30
    set_account_utilization(&state, 0, 0.30, 0.40, now + 10000, now + 86400).await;
    set_account_utilization(&state, 1, 0.30, 0.40, now + 10000, now + 518400).await;

    // Override claims with different resets
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now + 86400),
                status: None,
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.40),
                reset: Some(now + 518400),
                status: None,
                ..Default::default()
            },
        );
    }

    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("client_{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            picks[idx] += 1;
        }
    }
    assert!(
        picks[0] > picks[1] * 2,
        "A (expiring) should get >2x traffic vs B: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn pick_fable_prefers_band_headroom() {
    // Two Max accounts, equal 5h and weekly pool; A's Fable band is nearly
    // spent (0.90), B's is roomy (0.20) → Fable traffic should prefer B.
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.30, now + 10000, now + 302400).await;
    set_account_utilization(&state, 1, 0.30, 0.30, now + 10000, now + 302400).await;
    set_model_utilization(&state, 0, "claude-fable-5", 0.90, now + 302400).await;
    set_model_utilization(&state, 1, "claude-fable-5", 0.20, now + 302400).await;

    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("client_{}", i);
        if let Some(idx) = state.pick_endpoint(Some(&key), "claude-fable-5", &[]).await {
            picks[idx] += 1;
        }
    }
    assert!(
        picks[1] > picks[0] * 2,
        "B (roomy fable band) should get >2x traffic vs A: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn pick_fable_capped_by_shared_pool() {
    // Fable shares the weekly pool: A's band is barely touched (0.10) but its
    // weekly pool is nearly drained (0.95). B is balanced (0.50/0.50).
    // Without the pool cap A's roomy band would win; with it, B must win.
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.95, now + 10000, now + 302400).await;
    set_account_utilization(&state, 1, 0.30, 0.50, now + 10000, now + 302400).await;
    set_model_utilization(&state, 0, "claude-fable-5", 0.10, now + 302400).await;
    set_model_utilization(&state, 1, "claude-fable-5", 0.50, now + 302400).await;

    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("client_{}", i);
        if let Some(idx) = state.pick_endpoint(Some(&key), "claude-fable-5", &[]).await {
            picks[idx] += 1;
        }
    }
    assert!(
        picks[1] > picks[0] * 2,
        "B should win — A's drained pool caps its roomy band: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn fable_band_rejected_skips_for_fable_only() {
    // Exhausted Fable band (rejected, no overage) → account skipped for Fable
    // requests but still fully routable for other families.
    let acct = mk_endpoint("a", "sk-ant-api-a");
    let state = test_state_with(vec![acct]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.30, now + 10000, now + 302400).await;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            FABLE_BAND_CLAIM.to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 302400),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }

    assert!(
        state
            .routing_candidates("claude-fable-5", &[])
            .await
            .is_empty(),
        "rejected band must skip the account for fable"
    );
    assert_eq!(
        state
            .routing_candidates("claude-sonnet-4-6", &[])
            .await
            .len(),
        1,
        "sonnet routing must be unaffected by the fable band"
    );
}

#[tokio::test]
async fn fable_pool_rejected_skips_fable_despite_roomy_band() {
    // The shared weekly pool is rejected — a roomy band claim must not keep
    // the account routable for Fable.
    let acct = mk_endpoint("a", "sk-ant-api-a");
    let state = test_state_with(vec![acct]);
    let now = AppState::now_epoch();

    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 302400),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
        info.claims_7d.insert(
            FABLE_BAND_CLAIM.to_string(),
            ClaimWindowData {
                utilization: Some(0.20),
                reset: Some(now + 302400),
                status: None,
                ..Default::default()
            },
        );
    }

    assert!(
        state
            .routing_candidates("claude-fable-5", &[])
            .await
            .is_empty(),
        "rejected weekly pool must skip the account for fable"
    );
}

#[tokio::test]
async fn fable_included_false_demotes_for_fable_only() {
    // A Pro-plan account (fable_included = false) is paid capacity for Fable
    // from the first token: demoted by overage_penalty for Fable requests,
    // untouched for everything else.
    let mut acct_a = mk_endpoint("a", "sk-ant-api-a");
    acct_a.fable_included = false;
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    set_account_utilization(&state, 0, 0.30, 0.30, now + 10000, now + 302400).await;
    set_account_utilization(&state, 1, 0.30, 0.30, now + 10000, now + 302400).await;

    let fable = state.routing_candidates("claude-fable-5", &[]).await;
    let a = fable.iter().find(|c| c.endpoint == 0).unwrap();
    let b = fable.iter().find(|c| c.endpoint == 1).unwrap();
    assert_eq!(
        a.priority, 10,
        "paid-fable account demoted by overage_penalty"
    );
    assert_eq!(b.priority, 0, "included account keeps its tier");

    let sonnet = state.routing_candidates("claude-sonnet-4-6", &[]).await;
    assert!(
        sonnet.iter().all(|c| c.priority == 0),
        "non-fable traffic must not see the demotion"
    );

    // Tier ordering: all Fable traffic lands on the included account.
    for i in 0..50 {
        let key = format!("client_{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-fable-5", &[])
            .await
            .unwrap();
        assert_eq!(idx, 1, "fable must drain included capacity first");
    }
}

#[tokio::test]
async fn pick_account_dampens_by_5h() {
    // Account A: high waste_risk but high 5h → dampened
    // Account B: lower waste_risk but low 5h → more traffic
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // A: 5h=0.85, 7d=0.20 (waste_risk ~5.0 with 1.5d remaining)
    set_account_utilization(&state, 0, 0.85, 0.20, now + 10000, now + 129600).await;
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.20),
                reset: Some(now + 129600),
                status: None,
                ..Default::default()
            },
        );
    }

    // B: 5h=0.30, 7d=0.50 (waste_risk ~1.2 with 3.5d remaining)
    set_account_utilization(&state, 1, 0.30, 0.50, now + 10000, now + 302400).await;
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(0.50),
                reset: Some(now + 302400),
                status: None,
                ..Default::default()
            },
        );
    }

    let mut picks = [0u32; 2];
    for i in 0..500 {
        let key = format!("client_{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            picks[idx] += 1;
        }
    }
    // A: wr=0.80/0.2143=3.73, weight=3.73*0.15=0.56
    // B: wr=0.50/0.50=1.0, weight=1.0*0.70=0.70
    // B should get more (~55.6% share)
    assert!(
        picks[1] > picks[0],
        "B (low 5h) should get more traffic: A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn pick_account_fallback_no_7d_data() {
    // No 7d claims → falls back to headroom-only weighting
    // Uses affinity (sticky) traffic to verify weight-proportional distribution
    // across distinct session keys, exercising the affinity path.
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    // Set 5h only, no claims_7d
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.30);
        info.claims_7d.clear();
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.70);
        info.reset_5h = Some(now + 10000);
        info.utilization = Some(0.70);
        info.claims_7d.clear();
    }

    // Each distinct session key deterministically hashes into the weight-space;
    // 200 different keys should distribute ~70/30 matching A's higher headroom.
    let mut picks = [0u32; 2];
    for i in 0..200 {
        let key = format!("session-{}", i);
        if let Some(idx) = state
            .pick_endpoint(Some(&key), "claude-sonnet-4-6", &[])
            .await
        {
            picks[idx] += 1;
        }
    }

    // Verify the affinity path returns Some for a well-formed session key
    let session = "10.42.0.1:client-test:agent-1:session-fallback";
    assert!(
        state
            .pick_endpoint(Some(session), "claude-sonnet-4-6", &[])
            .await
            .is_some(),
        "affinity pick should return Some when accounts are available"
    );

    // A headroom=0.70, B headroom=0.30. A should get ~70% of 200 = ~140 picks.
    // Require at least 65% (130) to catch regressions while allowing hash variance.
    assert!(
        picks[0] >= 130,
        "expected A to get ~70% but got A={}, B={}",
        picks[0],
        picks[1]
    );
}

#[tokio::test]
async fn emergency_brake_triggers_at_88() {
    // All accounts at 88% raw 7d → brake engages with new threshold
    let now = AppState::now_epoch();
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);

    // Verify DEFAULT_EMERGENCY_THRESHOLD is 0.88
    assert!(
        (DEFAULT_EMERGENCY_THRESHOLD - 0.88).abs() < 0.001,
        "default emergency threshold should be 0.88"
    );

    set_account_utilization(&state, 0, 0.89, 0.89, now + 10000, now + 100000).await;
    set_account_utilization(&state, 1, 0.89, 0.89, now + 10000, now + 100000).await;

    // effective_utilization for both should be >= 0.88
    let info0 = state.endpoints[0].rate_info.read().await;
    let (util0, _, _, _) = effective_utilization(&info0, now, "");
    drop(info0);
    assert!(
        util0 >= DEFAULT_EMERGENCY_THRESHOLD,
        "util should be >= threshold: {util0}"
    );
}

#[tokio::test]
async fn emergency_brake_fires_when_only_anthropic_above_threshold_with_openai_present() {
    // 1 anthropic account at utilization 0.95, 1 openai endpoint with stub rate info.
    // A naive "iterate all endpoints" version sees the openai stub at (0.5, "unknown")
    // and forces all_above = false → brake never fires. The correct "skip OpenAI"
    // version excludes it and the brake fires.
    let acct = mk_endpoint("anthropic", "sk-ant");
    {
        let mut info = acct.rate_info.write().await;
        info.utilization = Some(0.95);
        info.utilization_5h = Some(0.95);
    }
    let mut state = test_state_with(vec![acct]);
    let st = Arc::get_mut(&mut state).expect("uniquely owned");
    let mut openai_ep = make_endpoint("openai", Protocol::OpenAI);
    openai_ep.priority = 100;
    st.endpoints.push(openai_ep);
    st.emergency_threshold = 0.88;
    assert!(
        state.is_emergency_brake_active().await,
        "brake must fire: anthropic is above threshold; openai must not vote"
    );
}

#[tokio::test]
async fn probe_endpoint_skips_openai() {
    // The mock upstream injects `5h-utilization: 0.25` headers. If the probe
    // ran, `rate_info.utilization_5h` would become Some(0.25). The OpenAI
    // skip means the endpoint is never contacted and rate_info stays None.
    let (mock_url, _h) = spawn_mock_upstream().await;
    let mut ep = make_endpoint("openai", Protocol::OpenAI);
    ep.base_url = mock_url;
    ep.priority = 100;
    let mut state = test_state_with(vec![]);
    Arc::get_mut(&mut state).unwrap().endpoints.push(ep);

    state.probe_endpoint(0, "claude-haiku-4-5").await;

    // rate_info must be untouched: the OpenAI endpoint was skipped, no HTTP
    // call was made. A naive "probe all endpoints" version would have hit
    // the mock and set utilization_5h to Some(0.25).
    let info = state.endpoints[0].rate_info.read().await;
    assert!(
        info.utilization_5h.is_none(),
        "probe must short-circuit for OpenAI endpoints — rate_info must stay untouched"
    );
    assert_eq!(state.endpoints[0].requests.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn pick_account_all_7d_rejected_returns_none() {
    // If all accounts have rejected 7d claims for the model, pick_account returns None
    let acct_a = mk_endpoint("a", "sk-ant-api-a");
    let acct_b = mk_endpoint("b", "sk-ant-api-b");
    let state = test_state_with(vec![acct_a, acct_b]);
    let now = AppState::now_epoch();

    for idx in 0..2 {
        let mut info = state.endpoints[idx].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.reset_5h = Some(now + 10000);
        info.claims_7d.insert(
            "seven_day_sonnet".to_string(),
            ClaimWindowData {
                utilization: Some(1.0),
                reset: Some(now + 100000),
                status: Some("rejected".to_string()),
                ..Default::default()
            },
        );
    }

    let result = state
        .pick_endpoint(Some("test"), "claude-sonnet-4-6", &[])
        .await;
    assert!(
        result.is_none(),
        "all-rejected should return None, got {:?}",
        result
    );
}

#[test]
fn account_serves_model_empty_filter_allows_all() {
    let acct = mk_endpoint("test", "sk-ant-api-x");
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model(""));
}

#[test]
fn account_serves_model_prefix_wildcard() {
    let mut acct = mk_endpoint("test", "sk-ant-api-x");
    acct.models = vec!["claude-opus-*".to_string()];

    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-opus-future"));
    assert!(!acct.serves_model("claude-sonnet-4-6"));
}

#[test]
fn account_serves_model_multiple_patterns() {
    let mut acct = mk_endpoint("test", "sk-ant-api-x");
    acct.models = vec!["claude-opus-*".to_string(), "claude-sonnet-4-6".to_string()];

    assert!(acct.serves_model("claude-opus-4-6"));
    assert!(acct.serves_model("claude-sonnet-4-6"));
    assert!(!acct.serves_model("claude-sonnet-3-5"));
    assert!(!acct.serves_model("claude-haiku-3-5"));
}

#[tokio::test]
async fn pick_account_rejected_account_gets_no_traffic() {
    let state = test_state_with(vec![
        mk_endpoint("rejected", "sk-ant-api-a"),
        mk_endpoint("healthy", "sk-ant-api-b"),
    ]);

    let now = AppState::now_epoch();
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.90);
        info.reset_5h = Some(now + 10000);
        info.status_5h = Some("rejected".to_string()); // Rejected = util floor 1.0
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.50);
    }

    // All requests should go to healthy account
    for _ in 0..100 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        assert_eq!(idx, 1, "rejected account should receive no traffic");
    }
}

#[tokio::test]
async fn pick_account_all_throttled_uses_all() {
    // When all accounts are throttled (status=throttled, floor=0.98 > soft_limit=0.90),
    // should use all accounts (graceful degradation)
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
    ]);

    let now = AppState::now_epoch();
    for acct in &state.endpoints {
        let mut info = acct.rate_info.write().await;
        info.utilization_5h = Some(0.80);
        info.reset_5h = Some(now + 10000);
        info.status_5h = Some("throttled".to_string()); // Floor 0.98 > soft_limit 0.90
    }

    // Both accounts should receive traffic
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    assert!(
        counts[0] > 0,
        "first throttled account should get some traffic"
    );
    assert!(
        counts[1] > 0,
        "second throttled account should get some traffic"
    );
}

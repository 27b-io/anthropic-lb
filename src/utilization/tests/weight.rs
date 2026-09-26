use super::*;

// ── Waste risk tests ──────────────────────────────────────────

#[test]
fn waste_risk_normal() {
    let now = 1_000_000u64;
    let reset = now + 302400; // 3.5 days remaining (half of 7d)
    let wr = waste_risk(Some(0.40), Some(reset), now);
    // unused=0.60, remaining_fraction=302400/604800=0.5, wr=0.60/0.5=1.2
    assert!((wr - 1.2).abs() < 0.01, "expected ~1.2, got {wr}");
}

#[test]
fn waste_risk_stale_reset() {
    let now = 1_000_000u64;
    let wr = waste_risk(Some(0.40), Some(now - 100), now);
    assert_eq!(wr, 0.0, "stale reset should return 0");
}

#[test]
fn waste_risk_no_reset() {
    let wr = waste_risk(Some(0.40), None, 1_000_000);
    assert_eq!(wr, 0.0, "no reset should return 0");
}

#[test]
fn waste_risk_under_60s_remaining() {
    let now = 1_000_000u64;
    let wr = waste_risk(Some(0.40), Some(now + 30), now);
    assert_eq!(wr, 0.0, "<60s remaining should return 0");
}

#[test]
fn waste_risk_fully_utilized() {
    let now = 1_000_000u64;
    let wr = waste_risk(Some(1.0), Some(now + 302400), now);
    assert_eq!(wr, 0.0, "fully utilized should return 0");
}

#[test]
fn waste_risk_near_reset_urgency() {
    let now = 1_000_000u64;
    // 2 hours remaining, 50% unused — very urgent
    let wr = waste_risk(Some(0.50), Some(now + 7200), now);
    // unused=0.50, remaining_fraction=7200/604800=0.0119, wr=0.50/0.0119=42→clamped to 10
    assert_eq!(wr, 10.0, "near-reset high-unused should clamp to 10");
}

#[test]
fn waste_risk_clamped() {
    let now = 1_000_000u64;
    // 1 hour remaining, 90% unused
    let wr = waste_risk(Some(0.10), Some(now + 3600), now);
    // unused=0.90, remaining_fraction=3600/604800≈0.00595, wr≈151→clamped to 10
    assert_eq!(wr, 10.0, "should clamp to 10.0");
}

#[test]
fn waste_risk_boundary_at_60s() {
    let now = 1_000_000u64;
    // Exactly 60s remaining — guard is > 60, so should return 0
    assert_eq!(waste_risk(Some(0.40), Some(now + 60), now), 0.0);
    // 61s remaining — just above boundary, should return non-zero
    assert!(waste_risk(Some(0.40), Some(now + 61), now) > 0.0);
}

#[test]
fn waste_risk_util_above_one() {
    let now = 1_000_000u64;
    // API can return util > 1.0; unused should clamp to 0
    let wr = waste_risk(Some(1.05), Some(now + 302400), now);
    assert_eq!(wr, 0.0, "util > 1.0 should produce zero waste risk");
}

// ── compute_routing_weight tests ──────────────────────────────

#[test]
fn routing_weight_basic_5h_only() {
    // No 7d data → headroom_only, weight = headroom = 1 - gate_5h
    let now = 1_000_000u64;
    let info = RateLimitInfo {
        utilization_5h: Some(0.40),
        reset_5h: Some(now + 18000), // 5h window
        status_5h: Some("allowed".to_string()),
        ..Default::default()
    };
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("should produce weight");
    assert!(rw.gate_5h > 0.0 && rw.gate_5h < 1.0);
    assert_eq!(rw.source, "headroom_only");
    assert!(rw.wr == 0.0);
    assert!(rw.weight > 0.0);
}

#[test]
fn routing_weight_with_waste_risk() {
    // 7d claim present → waste_risk sourced weight
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(0.40),
            reset: Some(now + 302400), // 3.5 days
            status: Some("allowed".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("should produce weight");
    assert_eq!(rw.source, "waste_risk");
    assert!(rw.wr > 0.0, "waste_risk should be positive");
    // weight = wr * headroom (both positive)
    assert!(rw.weight > 0.0);
}

#[test]
fn routing_weight_rejected_returns_none() {
    // 7d claim rejected → None (account should be skipped)
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now + 302400),
            status: Some("rejected".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        compute_routing_weight(&info, "claude-sonnet-4-6", now, false).is_none(),
        "rejected claim should return None"
    );
}

#[test]
fn routing_weight_expired_rejected_claim_returns_some() {
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now.saturating_sub(1)),
            status: Some("rejected".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        compute_routing_weight(&info, "claude-sonnet-4-6", now, false).is_some(),
        "expired rejected claim should not return None"
    );
}

#[test]
fn routing_weight_stale_uses_fallback() {
    // stale_after_hard_limit = true → both gates fallback to 0.5
    let now = 1_000_000u64;
    let info = RateLimitInfo {
        utilization_5h: Some(0.95), // would be high, but stale
        reset_5h: Some(now + 18000),
        status_5h: Some("throttled".to_string()),
        ..Default::default()
    };
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, true)
        .expect("stale should still produce weight");
    assert_eq!(rw.gate_5h, 0.5);
    assert_eq!(rw.gate_7d, 0.5);
    assert_eq!(rw.gate, 0.5);
}

#[test]
fn routing_weight_rejected_but_stale_still_returns_some() {
    // Stale after hard limit + rejected claim → should NOT skip (give it a chance)
    let now = 1_000_000u64;
    let mut claims = HashMap::new();
    claims.insert(
        "seven_day_sonnet".to_string(),
        ClaimWindowData {
            utilization: Some(1.0),
            reset: Some(now + 302400),
            status: Some("rejected".to_string()),
            last_seen: now,
        },
    );
    let info = RateLimitInfo {
        utilization_5h: Some(0.20),
        reset_5h: Some(now + 18000),
        status_5h: Some("allowed".to_string()),
        claims_7d: claims,
        ..Default::default()
    };
    assert!(
        compute_routing_weight(&info, "claude-sonnet-4-6", now, true).is_some(),
        "stale rejected should still return Some (give account a chance)"
    );
}

#[test]
fn routing_weight_publish_ttl_uses_fallback_interval_when_probes_disabled() {
    assert_eq!(AppState::routing_weight_publish_ttl(0), 120);
}

#[test]
fn routing_weight_publish_ttl_doubles_probe_interval() {
    assert_eq!(AppState::routing_weight_publish_ttl(300), 600);
}

#[test]
fn routing_weight_no_data_uses_defaults() {
    // No utilization data at all → gate_5h=0.5 (unknown), headroom=0.5
    let now = 1_000_000u64;
    let info = RateLimitInfo::default();
    let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false)
        .expect("should produce weight with defaults");
    assert_eq!(rw.gate_5h, 0.5);
    assert_eq!(rw.source, "headroom_only");
    assert!(rw.weight > 0.0);
}

/// LAB-4441: the gate published on `/metrics` must equal the gate the router
/// uses for the same `RateLimitInfo`. The metrics path once lacked the overage
/// branch and published gate 1.0 / weight 0 for accounts the router was
/// actively serving through paid overage. (c) guards the non-overage path.
#[test]
fn metrics_gate_matches_routing_gate() {
    let now = 1_000_000u64;
    let seven_day = |status: &str| {
        HashMap::from([(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.60),
                reset: Some(now + 302400),
                status: Some(status.to_string()),
                last_seen: now,
            },
        )])
    };
    // Subscription windows exhausted (both rejected); overage carrying load.
    let overage = |util: f64, reset: u64| RateLimitInfo {
        utilization_5h: Some(1.0),
        reset_5h: Some(now + 7200),
        status_5h: Some("rejected".to_string()),
        claims_7d: seven_day("rejected"),
        overage_in_use: true,
        overage_status: Some("allowed".to_string()),
        overage_utilization: Some(util),
        overage_reset: Some(reset),
        ..Default::default()
    };
    let half_ramp = now + (NEAR_RESET_OVERAGE_SECS / 2.0) as u64;
    let cases = [
        (
            "(a) overage, rejected 7d",
            overage(0.30, now + 86_400),
            0.30,
        ),
        (
            "(b) overage, near-reset ramp",
            overage(0.80, half_ramp),
            0.40,
        ),
        (
            "(c) overage inactive",
            RateLimitInfo {
                utilization_5h: Some(0.40),
                reset_5h: Some(now + 7200),
                status_5h: Some("allowed".to_string()),
                claims_7d: seven_day("allowed_warning"),
                ..Default::default()
            },
            WARNING_UTIL_FLOOR,
        ),
    ];
    for (name, info, want) in cases {
        let rw = compute_routing_weight(&info, "claude-sonnet-4-6", now, false).expect(name);
        let (gate, weight) = metrics_gate_weight(&info, now, Instant::now()).expect(name);
        assert_eq!(gate, rw.gate, "{name}: metrics gate != routing gate");
        assert!(
            (gate - want).abs() < 1e-9,
            "{name}: gate {gate}, want {want}"
        );
        assert_eq!(
            weight, rw.weight,
            "{name}: metrics weight != routing weight"
        );
        assert!(weight > 0.0, "{name}: servable account published weight 0");
    }
}

// ── classify_hard_limit_sync tests ────────────────────────────

#[test]
fn classify_hard_limit_none_is_ignore() {
    let now_instant = Instant::now();
    assert_eq!(
        classify_hard_limit_sync(None, 1_000_000, now_instant),
        HardLimitSync::Ignore
    );
}

#[test]
fn classify_hard_limit_sentinel_is_clear() {
    let now_instant = Instant::now();
    assert_eq!(
        classify_hard_limit_sync(Some(HARD_LIMIT_CLEARED_SENTINEL), 1_000_000, now_instant),
        HardLimitSync::Clear
    );
}

#[test]
fn classify_hard_limit_future_epoch_is_update() {
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    let future = now_epoch + 300; // 5 min from now
    match classify_hard_limit_sync(Some(future), now_epoch, now_instant) {
        HardLimitSync::Update(until) => {
            let expected = now_instant + Duration::from_secs(300);
            let delta = if until > expected {
                until.duration_since(expected)
            } else {
                expected.duration_since(until)
            };
            assert!(delta < Duration::from_millis(1), "until Instant mismatch");
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn classify_hard_limit_past_epoch_is_ignore() {
    // Stale non-zero value — e.g. a hard limit that already expired via TTL
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    assert_eq!(
        classify_hard_limit_sync(Some(now_epoch - 100), now_epoch, now_instant),
        HardLimitSync::Ignore
    );
}

#[test]
fn classify_hard_limit_epoch_equal_now_is_ignore() {
    // Boundary: epoch == now means expired now, not future
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    assert_eq!(
        classify_hard_limit_sync(Some(now_epoch), now_epoch, now_instant),
        HardLimitSync::Ignore
    );
}

#[test]
fn classify_hard_limit_clamps_far_future() {
    // Corrupt or malicious Redis value must not create a panic-inducing Instant
    // nor a permanent undead hard limit. Clamp to 24h.
    let now_instant = Instant::now();
    let now_epoch = 1_000_000u64;
    match classify_hard_limit_sync(Some(u64::MAX), now_epoch, now_instant) {
        HardLimitSync::Update(until) => {
            let capped = now_instant + Duration::from_secs(86_400);
            let delta = if until > capped {
                until.duration_since(capped)
            } else {
                capped.duration_since(until)
            };
            assert!(delta < Duration::from_millis(1), "expected 24h clamp");
        }
        other => panic!("expected Update (clamped), got {other:?}"),
    }
}

#[tokio::test]
async fn signal_hard_limit_recovery_without_redis_is_noop() {
    // Without Redis, the helper must still refresh metrics + publish weights
    // locally. It must not panic and must not attempt Redis I/O.
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-test")]);
    // Pre-seed a non-zero weight so we can assert refresh_metrics_weights ran
    state.endpoints[0]
        .last_routing_weight
        .store(0u64, Ordering::Relaxed);
    state
        .signal_hard_limit_recovery(&state.endpoints[0].name)
        .await;
    // refresh_metrics_weights always writes a value (even zero) — the point
    // is that the method completed without Redis and without panicking.
    let w = f64::from_bits(
        state.endpoints[0]
            .last_routing_weight
            .load(Ordering::Relaxed),
    );
    assert!(w.is_finite(), "weight atomic must remain valid: {w}");
}

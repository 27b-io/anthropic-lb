use super::*;

mod real_redis;

#[test]
fn epoch_to_iso8601_known_values() {
    // 2024-01-01T00:00:00Z = 1704067200
    assert_eq!(
        AppState::epoch_to_iso8601(1704067200),
        "2024-01-01T00:00:00Z"
    );
    // Unix epoch
    assert_eq!(AppState::epoch_to_iso8601(0), "1970-01-01T00:00:00Z");
    // 2026-02-14T12:30:45Z = approximate check
    let result = AppState::epoch_to_iso8601(1771157445);
    assert!(
        result.starts_with("2026-02-"),
        "expected 2026-02, got {result}"
    );
    assert!(result.ends_with('Z'));
}

#[test]
fn epoch_to_iso8601_leap_year() {
    // 2024-02-29T00:00:00Z (leap day) = 1709164800
    let result = AppState::epoch_to_iso8601(1709164800);
    assert_eq!(
        result, "2024-02-29T00:00:00Z",
        "should handle leap year correctly"
    );
}

#[test]
fn epoch_to_iso8601_edge_of_year() {
    // 2023-12-31T23:59:59Z = 1704067199
    let result = AppState::epoch_to_iso8601(1704067199);
    assert_eq!(
        result, "2023-12-31T23:59:59Z",
        "should handle end of year correctly"
    );
}

// ── Redis distributed state tests ──────────────────────────────────

#[tokio::test]
async fn cluster_info_returns_none_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.cluster_info().await.is_none());
}

#[tokio::test]
async fn sync_from_redis_noop_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    // Should not panic or error when redis is None
    state.sync_from_redis().await;
    // Cluster cache should remain None
    assert!(state.cluster_info_cache.lock().unwrap().is_none());
}

/// sync_from_redis / publish_routing_weights build the SyncTarget list over
/// the endpoint pool. With an endpoints-only config (and a mix of
/// Anthropic + OpenAI protocols) neither path may panic.
#[tokio::test]
async fn sync_and_publish_handle_endpoint_pool_without_redis() {
    let mut state = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state).unwrap();
        st.endpoints
            .push(make_endpoint("ep-a", Protocol::Anthropic));
        st.endpoints.push(make_endpoint("ep-oai", Protocol::OpenAI));
    }
    // Both paths must be no-ops (redis is None) and must not panic.
    state.sync_from_redis().await;
    state.publish_routing_weights().await;
    assert!(state.cluster_info_cache.lock().unwrap().is_none());
}

#[tokio::test]
async fn budget_local_fallback_without_redis() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 100u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Budget check uses local path when redis is None
    assert!(state.check_budget("client-a").await.is_ok());
    state.record_budget_usage("client-a", 80).await;
    assert!(state.check_budget("client-a").await.is_ok());
    state.record_budget_usage("client-a", 30).await;
    assert!(state.check_budget("client-a").await.is_err());
}

#[test]
fn redis_rate_info_serialization_roundtrip() {
    let mut claims = HashMap::new();
    claims.insert(
        "claude-sonnet-4-6".to_string(),
        ClaimWindowData {
            utilization: Some(0.42),
            reset: Some(1700000000),
            status: Some("active".to_string()),
            ..Default::default()
        },
    );

    let info = RedisRateInfo {
        utilization: Some(0.5),
        utilization_5h: Some(0.3),
        utilization_7d: Some(0.6),
        reset_5h: Some(1700000000),
        reset_7d: Some(1700500000),
        status_5h: Some("active".to_string()),
        status_7d: Some("active".to_string()),
        claims_7d: claims,
        representative_claim: Some("five_hour".to_string()),
        remaining_requests: Some(100),
        remaining_tokens: Some(50000),
        limit_requests: Some(200),
        limit_tokens: Some(100000),
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: 1700000000,
    };

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: RedisRateInfo = serde_json::from_str(&json).unwrap();

    assert_eq!(info.utilization, deserialized.utilization);
    assert_eq!(info.utilization_5h, deserialized.utilization_5h);
    assert_eq!(info.utilization_7d, deserialized.utilization_7d);
    assert_eq!(info.reset_5h, deserialized.reset_5h);
    assert_eq!(info.reset_7d, deserialized.reset_7d);
    assert_eq!(info.updated_at, deserialized.updated_at);
    assert_eq!(info.claims_7d.len(), deserialized.claims_7d.len());
    let claim = deserialized.claims_7d.get("claude-sonnet-4-6").unwrap();
    assert_eq!(claim.utilization, Some(0.42));
    assert_eq!(claim.reset, Some(1700000000));
}

#[test]
fn redis_rate_info_empty_fields() {
    let info = RedisRateInfo {
        utilization: None,
        utilization_5h: None,
        utilization_7d: None,
        reset_5h: None,
        reset_7d: None,
        status_5h: None,
        status_7d: None,
        claims_7d: HashMap::new(),
        representative_claim: None,
        remaining_requests: None,
        remaining_tokens: None,
        limit_requests: None,
        limit_tokens: None,
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: 0,
    };

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: RedisRateInfo = serde_json::from_str(&json).unwrap();
    assert!(deserialized.utilization.is_none());
    assert!(deserialized.claims_7d.is_empty());
    assert_eq!(deserialized.updated_at, 0);
}

#[tokio::test]
async fn hard_limit_unchanged_by_sync_without_redis() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);

    // Set a local hard limit
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(30));
    }

    // sync_from_redis should not touch it when redis is None
    state.sync_from_redis().await;

    let info = state.endpoints[0].rate_info.read().await;
    assert!(info.hard_limited_until.is_some());
}

#[tokio::test]
async fn record_budget_usage_skips_zero_tokens() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 100u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Recording 0 tokens should be a no-op
    state.record_budget_usage("client-a", 0).await;
    let map = state.budget_usage.lock().unwrap();
    assert!(map.get("client-a").is_none());
}

#[tokio::test]
async fn record_budget_usage_skips_unknown_client() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    // No budgets configured — recording should be a no-op
    state.record_budget_usage("unknown-client", 500).await;
    let map = state.budget_usage.lock().unwrap();
    assert!(map.is_empty());
}

/// LAB-3217: the sync-tick fold seeds an empty mirror, never lowers a higher
/// one (the LAB-1962 floor), replaces a stale day instead of summing it,
/// leaves a mirror that already rolled past the fetched day alone, and skips
/// zero/absent counters. Pure; the Redis-backed half lives in
/// `redis_integration::sync_from_redis_seeds_budget_mirror_from_shared_counter`.
#[tokio::test]
async fn fold_budget_mirror_seeds_floors_and_replaces_stale_day() {
    let today = AppState::now_epoch() / 86400;
    let state = Arc::new(AppState {
        budget_usage: Mutex::new(
            [
                ("floor".to_string(), (today, 500u64)),
                ("stale".to_string(), (today - 1, 900)),
                ("ahead".to_string(), (today + 1, 70)),
            ]
            .into(),
        ),
        ..test_state_base()
    });

    state.fold_budget_mirror(
        today,
        [
            ("seed", 300u64),
            ("floor", 200),
            ("stale", 100),
            ("ahead", 999),
            ("zero", 0),
        ],
    );

    let map = state.budget_usage.lock().unwrap();
    assert_eq!(
        map["seed"],
        (today, 300),
        "empty mirror seeds from the shared counter"
    );
    assert_eq!(
        map["floor"],
        (today, 500),
        "a higher local floor is never lowered"
    );
    assert_eq!(
        map["stale"],
        (today, 100),
        "a stale day is replaced, not summed"
    );
    assert_eq!(
        map["ahead"],
        (today + 1, 70),
        "a mirror already past the fetched day is left alone"
    );
    assert!(
        !map.contains_key("zero"),
        "a zero/absent counter folds nothing"
    );
}

// ── Rate info merge: "most recent wins" ─────────────────────

#[tokio::test]
async fn rate_info_merge_remote_newer_wins() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let now_epoch = AppState::now_epoch();

    // Set local rate info with an older timestamp
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.last_updated_epoch = Some(now_epoch - 60); // 60s ago
    }

    // Simulate remote data that's newer (10s ago)
    let remote = RedisRateInfo {
        utilization: Some(0.80),
        utilization_5h: Some(0.75),
        utilization_7d: Some(0.60),
        reset_5h: Some(now_epoch + 3600),
        reset_7d: Some(now_epoch + 86400),
        status_5h: Some("allowed_warning".to_string()),
        status_7d: Some("allowed".to_string()),
        claims_7d: HashMap::new(),
        representative_claim: Some("five_hour".to_string()),
        remaining_requests: Some(50),
        remaining_tokens: Some(25000),
        limit_requests: Some(200),
        limit_tokens: Some(100000),
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: now_epoch - 10, // 10s ago — newer than local
    };

    // Apply same merge logic as sync_from_redis
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        let local_age = info
            .last_updated_epoch
            .map(|epoch| now_epoch.saturating_sub(epoch))
            .unwrap_or(u64::MAX);
        let remote_age = now_epoch.saturating_sub(remote.updated_at);
        assert!(
            remote_age < local_age,
            "remote ({}s) should be newer than local ({}s)",
            remote_age,
            local_age
        );
        // Remote wins — apply
        info.utilization_5h = remote.utilization_5h;
        info.last_updated_epoch = Some(remote.updated_at);
    }

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(info.utilization_5h, Some(0.75));
    assert_eq!(info.last_updated_epoch, Some(now_epoch - 10));
}

#[tokio::test]
async fn rate_info_merge_local_newer_preserved() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let now_epoch = AppState::now_epoch();

    // Set local rate info with a recent timestamp (5s ago)
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.30);
        info.last_updated_epoch = Some(now_epoch - 5);
    }

    // Remote data is older (120s ago)
    let remote = RedisRateInfo {
        utilization: Some(0.80),
        utilization_5h: Some(0.75),
        utilization_7d: None,
        reset_5h: None,
        reset_7d: None,
        status_5h: None,
        status_7d: None,
        claims_7d: HashMap::new(),
        representative_claim: None,
        remaining_requests: None,
        remaining_tokens: None,
        limit_requests: None,
        limit_tokens: None,
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: now_epoch - 120, // 120s ago — older than local
    };

    // Apply same merge logic as sync_from_redis
    {
        let info = state.endpoints[0].rate_info.read().await;
        let local_age = info
            .last_updated_epoch
            .map(|epoch| now_epoch.saturating_sub(epoch))
            .unwrap_or(u64::MAX);
        let remote_age = now_epoch.saturating_sub(remote.updated_at);
        assert!(
            remote_age >= local_age,
            "local ({}s) should be newer than remote ({}s)",
            local_age,
            remote_age
        );
        // Local wins — do NOT apply remote
    }

    let info = state.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.utilization_5h,
        Some(0.30),
        "local data should be preserved"
    );
}

#[tokio::test]
async fn rate_info_merge_no_local_epoch_remote_wins() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    let now_epoch = AppState::now_epoch();

    // Local has no last_updated_epoch (fresh state)
    {
        let info = state.endpoints[0].rate_info.read().await;
        assert!(info.last_updated_epoch.is_none());
    }

    let remote = RedisRateInfo {
        utilization: Some(0.50),
        utilization_5h: Some(0.40),
        utilization_7d: None,
        reset_5h: None,
        reset_7d: None,
        status_5h: None,
        status_7d: None,
        claims_7d: HashMap::new(),
        representative_claim: None,
        remaining_requests: None,
        remaining_tokens: None,
        limit_requests: None,
        limit_tokens: None,
        overage_in_use: false,
        overage_status: None,
        overage_utilization: None,
        overage_reset: None,
        updated_at: now_epoch - 30,
    };

    // When local has no epoch, local_age = u64::MAX, so remote always wins
    {
        let info = state.endpoints[0].rate_info.read().await;
        let local_age = info
            .last_updated_epoch
            .map(|epoch| now_epoch.saturating_sub(epoch))
            .unwrap_or(u64::MAX);
        let remote_age = now_epoch.saturating_sub(remote.updated_at);
        assert!(
            remote_age < local_age,
            "remote should win when local has no epoch"
        );
    }
}

// ── State persistence round-trip ─────────────────────────────

#[tokio::test]
async fn state_roundtrip_preserves_burst_backoff_and_leaves_no_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.json");
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("primary", "sk-ant-api-aaa")],
        state_path: state_path.clone(),
        ..test_state_base()
    });
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.consecutive_burst_429s = 3;
        info.utilization_5h = Some(0.4);
        info.reset_5h = Some(AppState::now_epoch() + 18000);
    }
    state.save_state().await;

    // Atomic write must leave no temp sibling behind in the directory.
    let leftover: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp"))
        .collect();
    assert!(
        leftover.is_empty(),
        "atomic save must clean up its temp file(s): {leftover:?}"
    );

    // Restart: a fresh state restores the burst-429 backoff stage (B3-07).
    let restored = Arc::new(AppState {
        endpoints: vec![mk_endpoint("primary", "sk-ant-api-aaa")],
        state_path: state_path.clone(),
        ..test_state_base()
    });
    restored.load_state().await;
    let info = restored.endpoints[0].rate_info.read().await;
    assert_eq!(
        info.consecutive_burst_429s, 3,
        "burst-429 backoff stage must survive a restart (B3-07)"
    );
}

/// Concurrent save_state calls must serialize cleanly: a valid, parseable
/// final file and no temp file left behind (no deadlock, no torn file). The
/// freshness-ordering guarantee itself is structural (the save lock); this
/// guards the concurrent path against corruption/deadlock.
#[tokio::test]
async fn concurrent_save_state_is_serialized_and_clean() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.json");
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("primary", "sk-ant-api-aaa")],
        state_path: state_path.clone(),
        ..test_state_base()
    });
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = state.clone();
        handles.push(tokio::spawn(async move { s.save_state().await }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let data = tokio::fs::read_to_string(&state_path).await.unwrap();
    serde_json::from_str::<PersistedState>(&data).expect("final state file must be valid JSON");
    let leftover: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp"))
        .collect();
    assert!(
        leftover.is_empty(),
        "no temp files may remain after concurrent saves: {leftover:?}"
    );
}

#[tokio::test]
async fn state_persistence_roundtrip() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let state_path = PathBuf::from(tmp.path());

    let state = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("primary", "sk-ant-api-aaa"),
            mk_endpoint("secondary", "sk-ant-api-bbb"),
        ],
        state_path: state_path.clone(),
        ..test_state_base()
    });

    // Set up some state to persist
    let now_epoch = AppState::now_epoch();
    state.endpoints[0].requests.store(42, Ordering::Relaxed);
    state.endpoints[1].requests.store(17, Ordering::Relaxed);
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.65);
        info.utilization_5h = Some(0.50);
        info.utilization_7d = Some(0.70);
        info.reset_5h = Some(now_epoch + 18000); // future
        info.reset_7d = Some(now_epoch + 604800);
        info.status_5h = Some("allowed_warning".to_string());
        info.remaining_requests = Some(100);
        info.remaining_tokens = Some(50000);
        info.limit_requests = Some(200);
        info.limit_tokens = Some(100000);
        info.representative_claim = Some("five_hour".to_string());
        // Set a known per-account epoch (older than now — simulates probe from 30s ago)
        info.last_updated_epoch = Some(now_epoch - 30);
        info.claims_7d.insert(
            "seven_day".to_string(),
            ClaimWindowData {
                utilization: Some(0.70),
                reset: Some(now_epoch + 604800),
                status: Some("allowed".to_string()),
                ..Default::default()
            },
        );
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.20);
        info.reset_5h = Some(now_epoch + 18000);
        // hard-limit 60s from now
        info.hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
    }

    // Save state
    state.save_state().await;

    // Verify file exists and is valid JSON
    let data = tokio::fs::read_to_string(&state_path).await.unwrap();
    let persisted: PersistedState = serde_json::from_str(&data).unwrap();
    assert_eq!(persisted.endpoints.len(), 2);
    assert_eq!(persisted.endpoints[0].requests_total, 42);
    assert_eq!(persisted.endpoints[1].requests_total, 17);
    assert!(persisted.saved_at > 0);

    // Create a fresh state and load into it
    let state2 = Arc::new(AppState {
        endpoints: vec![
            mk_endpoint("primary", "sk-ant-api-aaa"),
            mk_endpoint("secondary", "sk-ant-api-bbb"),
        ],
        state_path,
        ..test_state_base()
    });

    // Load state
    state2.load_state().await;

    // Verify fields survived the round-trip
    assert_eq!(
        state2.endpoints[0].requests.load(Ordering::Relaxed),
        42,
        "request count should persist"
    );
    assert_eq!(state2.endpoints[1].requests.load(Ordering::Relaxed), 17);

    {
        let info = state2.endpoints[0].rate_info.read().await;
        // Unified is recomputed as max(utilization_5h, utilization_7d)
        assert_eq!(info.utilization, Some(0.70));
        assert_eq!(info.utilization_5h, Some(0.50));
        assert_eq!(info.remaining_requests, Some(100));
        assert_eq!(info.remaining_tokens, Some(50000));
        assert_eq!(info.limit_requests, Some(200));
        assert_eq!(info.limit_tokens, Some(100000));
        assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
        assert!(
            !info.claims_7d.is_empty(),
            "claims_7d should survive round-trip"
        );
        let claim = info.claims_7d.get("seven_day").unwrap();
        assert_eq!(claim.utilization, Some(0.70));
        // last_updated_epoch should be the per-account value, not saved_at or now()
        assert_eq!(
            info.last_updated_epoch,
            Some(now_epoch - 30),
            "last_updated_epoch should be the per-account persisted value"
        );
    }

    {
        let info = state2.endpoints[1].rate_info.read().await;
        assert_eq!(info.utilization_5h, Some(0.20));
        // Hard limit should have been restored (future epoch)
        assert!(
            info.hard_limited_until.is_some(),
            "hard_limited_until should survive round-trip"
        );
    }
}

#[tokio::test]
async fn load_state_warns_and_starts_clean_on_legacy_accounts_key() {
    // A state file using the legacy `accounts` top-level key must NOT
    // deserialize into the new `endpoints`-keyed PersistedState. load_state
    // logs a warn and starts clean.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        tmp.path(),
        r#"{"accounts":[{"name":"primary","requests_total":42}],"saved_at":0}"#,
    )
    .unwrap();
    let mut state = test_state_with(vec![mk_endpoint("primary", "sk-ant")]);
    Arc::get_mut(&mut state).unwrap().state_path = tmp.path().to_path_buf();
    state.load_state().await; // must not panic
    assert_eq!(
        state.endpoints[0].requests.load(Ordering::Relaxed),
        0,
        "legacy accounts-keyed state file must not load into the new schema"
    );
}

#[tokio::test]
async fn save_load_roundtrip_unified_endpoints() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut state = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state).unwrap();
        st.state_path = tmp.path().to_path_buf();
        st.endpoints.push(make_endpoint("ep1", Protocol::Anthropic));
    }
    let now_epoch = AppState::now_epoch();
    state.endpoints[0].requests.store(7, Ordering::Relaxed);
    {
        // load_state recomputes `utilization` from the surviving 5h/7d
        // windows, so a bare flat `utilization` would not survive. Set a
        // 5h window with a future reset so it persists and drives util.
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.42);
        info.utilization_5h = Some(0.42);
        info.reset_5h = Some(now_epoch + 18000);
    }
    state.save_state().await;

    // Fresh state with the same endpoint name, load from the file.
    let mut state2 = test_state_with(vec![]);
    {
        let st = Arc::get_mut(&mut state2).unwrap();
        st.state_path = tmp.path().to_path_buf();
        st.endpoints.push(make_endpoint("ep1", Protocol::Anthropic));
    }
    state2.load_state().await;
    assert_eq!(state2.endpoints[0].requests.load(Ordering::Relaxed), 7);
    assert_eq!(
        state2.endpoints[0].rate_info.read().await.utilization,
        Some(0.42)
    );
}

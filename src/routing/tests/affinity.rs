use super::*;

// ── LAB-3295: status-floor-bound migrations log at INFO with reason="floored" ──

/// DynamicCapacityV1 only: StickyWeightedV2 counts moves, not overridden
/// requests. Pin ONE sticky key that hashes to `sticky_idx` (via
/// `keys_hashing_to`, the same idiom the GH#156 counter tests use), route it
/// once, and return the
/// per-reason affinity-migration counter (`[loaded, spent, floored]`) plus the
/// capture buffer's log lines mentioning `marker`. Deterministic: the key lands
/// on the sticky account, so the override fires exactly once. The counter is
/// per-`state`; the log buffer is process-global, so we filter by the unique
/// `marker` the key carries verbatim into the override line's `affinity=` field.
async fn migrate_one_sticky(
    state: &AppState,
    sticky_idx: usize,
    marker: &str,
) -> ([u64; 3], Vec<String>) {
    let buf = log_capture_buf();
    let key = keys_hashing_to(state, sticky_idx, 1, 50_000, marker)
        .await
        .pop()
        .expect("a key hashing to the sticky account");
    let picked = state
        .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
        .await
        .unwrap();
    assert_ne!(
        picked, sticky_idx,
        "the override must migrate the session off the sticky account"
    );
    let [loaded, spent, floored, ..] = migrations(state);
    let counts = [loaded, spent, floored];
    drop(buf);
    (counts, log_lines(marker))
}

/// `anthropic_affinity_migrations_total`, indexed by `MigrationReason`.
fn migrations(state: &AppState) -> [u64; 5] {
    std::array::from_fn(|i| state.affinity_migrations[i].load(Ordering::Relaxed))
}

/// The captured log lines carrying `marker`. The buffer is process-global, so
/// each test filters by a marker its own affinity keys carry verbatim.
fn log_lines(marker: &str) -> Vec<String> {
    let buf = log_capture_buf();
    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    output
        .lines()
        .filter(|l| l.contains(marker))
        .map(str::to_string)
        .collect()
}

/// Set a status floor on account `idx`'s general `seven_day` claim (the claim
/// that gates opus), so its gate is bound by an Anthropic status flag rather
/// than raw utilisation — the LAB-3295 floor-bound case.
async fn set_7d_status(state: &AppState, idx: usize, status: &str) {
    let mut info = state.endpoints[idx].rate_info.write().await;
    info.claims_7d
        .get_mut("seven_day")
        .expect("set_account_utilization populates seven_day")
        .status = Some(status.to_string());
}

/// AC-5(a), legacy DynamicCapacityV1: same floor-bound scenario at the other
/// override site — it too logs at INFO with `reason="floored"`.
#[tokio::test]
async fn affinity_floor_bound_migration_logs_info_legacy() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("sticky", "sk-ant-api-a"),
            mk_endpoint("fresh", "sk-ant-api-b"),
        ],
        RoutingStrategy::DynamicCapacityV1,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, now + 300000).await;
    set_7d_status(&state, 0, "allowed_warning").await;

    let marker = "lab3295-floored-legacy";
    let (counts, lines) = migrate_one_sticky(&state, 0, marker).await;

    assert_eq!(
        counts,
        [0, 0, 1],
        "legacy site: only floored migrations expected, got {counts:?}"
    );
    assert!(
        !lines.is_empty(),
        "expected override log lines for {marker}"
    );
    for l in &lines {
        assert!(
            l.contains(" INFO ")
                && l.contains("reason=\"floored\"")
                && !l.contains("affinity broken"),
            "legacy floor-bound migration must log INFO/floored, not the WARN: {l}"
        );
    }
}

#[tokio::test]
async fn affinity_override_both_rough() {
    // Scenario: both accounts in bad shape — affinity preserved
    // Primary 5h=0.85, 7d=0.20 vs Jeff 5h=0.10, 7d=0.90
    // Primary has 5h pressure but 7d budget; Jeff has 5h headroom but 7d exhausted
    // Weights should be close enough to preserve affinity
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.85, 0.20, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 300000).await;

    assert_affinity_distribution(
        &state,
        "both-rough",
        500,
        None,
        "both-rough scenario should preserve affinity on both accounts",
    )
    .await;
}

#[tokio::test]
async fn affinity_override_preserves_stickiness_with_moderate_disparity() {
    // With 3 candidates whose weights are moderately different (none below
    // 0.25 ratio to the best), the override does NOT fire — all three
    // accounts receive sticky traffic via proportional bucket hashing.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("steve", "sk-ant-api-b"),
            mk_endpoint("jeff", "sk-ant-api-c"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    // primary is clearly best, steve middling, jeff worst
    set_account_utilization(&state, 0, 0.13, 0.41, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.15, 0.55, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.12, 0.79, now + 10000, now + 300000).await;

    let mut saw = [false; 3];
    for i in 0..1000 {
        let key = format!("three-way-client-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        saw[idx] = true;
        if saw[0] && saw[1] && saw[2] {
            break;
        }
    }
    assert!(
        saw[0] && saw[1] && saw[2],
        "all 3 accounts should receive traffic with moderate disparity"
    );
}

/// Helper: find affinity keys whose *sticky* account is `target_idx` (i.e. the
/// sessions that move when that account is the loaded one): the account the
/// state's strategy hashes the key to over every candidate, before any
/// affinity rule. StickyWeightedV2 hashes by rendezvous score, DynamicCapacityV1
/// by walking the weighted buckets. Static given fixed utilizations.
async fn keys_hashing_to(
    state: &AppState,
    target_idx: usize,
    want: usize,
    scan: usize,
    prefix: &str,
) -> Vec<String> {
    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    if state.routing_strategy == RoutingStrategy::StickyWeightedV2 {
        let all: Vec<&RoutingCandidate> = candidates.iter().collect();
        return (0..scan)
            .map(|i| format!("{prefix}-{i}"))
            .filter(|key| state.pick_rendezvous(&all, key).unwrap().endpoint == target_idx)
            .take(want)
            .collect();
    }
    let total_weight: f64 = candidates.iter().map(|c| c.weight).sum();
    let mut boundaries: Vec<(usize, f64)> = Vec::new();
    let mut cumulative = 0.0;
    for c in &candidates {
        cumulative += c.weight;
        boundaries.push((c.endpoint, cumulative));
    }
    let mut out = Vec::new();
    for i in 0..scan {
        let key = format!("{}-{}", prefix, i);
        let target = (stable_affinity_hash(&key) as f64 / u64::MAX as f64) * total_weight;
        for &(idx, boundary) in &boundaries {
            if target < boundary {
                if idx == target_idx {
                    out.push(key);
                }
                break;
            }
        }
        if out.len() >= want {
            break;
        }
    }
    out
}

#[tokio::test]
async fn affinity_override_spreads_across_pool_not_single_best() {
    // Regression: when a session's sticky account is too loaded, the move
    // must NOT funnel every such session onto the single global argmax (`best`).
    // That target rotates as utilizations drift, sweeping sessions across the
    // pool and paying a cold-cache `cache_creation` charge on every hop
    // (measured: a swept client ran a 1.18 create:read ratio vs ~0.05 for sticky
    // clients). Overridden sessions must spread across the healthy remainder.
    let state = sticky_v2_state(vec![
        mk_endpoint("loaded", "sk-ant-api-a"),
        mk_endpoint("h1", "sk-ant-api-b"),
        mk_endpoint("h2", "sk-ant-api-c"),
        mk_endpoint("h3", "sk-ant-api-d"),
    ]);
    let now = AppState::now_epoch();
    // idx0's 5h gate is past soft_limit, so it fails the overage rule.
    set_account_utilization(&state, 0, 0.95, 0.30, now + 10000, now + 300000).await;
    // Three healthy accounts with a clear single best (h1): under the old
    // `picked = best` code every overridden session would herd onto h1.
    set_account_utilization(&state, 1, 0.08, 0.15, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.15, 0.30, now + 10000, now + 300000).await;
    set_account_utilization(&state, 3, 0.22, 0.45, now + 10000, now + 300000).await;

    let sessions = keys_hashing_to(&state, 0, 60, 30000, "spread-session").await;
    assert!(
        sessions.len() >= 30,
        "need enough overridden sessions to judge spread, got {}",
        sessions.len()
    );

    let mut destinations = std::collections::HashSet::new();
    for s in &sessions {
        let idx = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_ne!(idx, 0, "overridden session must leave the loaded account");
        destinations.insert(idx);
    }
    assert!(
            destinations.len() >= 2,
            "overridden sessions herded onto {} account(s) {:?}; expected spread across the healthy pool",
            destinations.len(),
            destinations,
        );
}

/// Regression (GH#156 / LAB-2684): two accounts under IDENTICAL load whose
/// only difference is time-to-weekly-reset. `weight` carries waste_risk =
/// unused / remaining_fraction_of_7d, so the account that reset yesterday
/// (wr ≈ 1) weighs ~8x less than one resetting in 20h (wr ≈ 7.5). The old
/// `picked.weight < best.weight * ratio` override in BOTH strategies read that
/// as "too loaded" and migrated every session off the FRESHEST accounts in the
/// pool (25% of prod requests WARNed on accounts at ≤17% utilisation).
/// Reset-time skew alone must never break affinity.
async fn assert_reset_time_skew_keeps_affinity(strategy: RoutingStrategy) {
    let mut state = test_state_with_strategy(
        vec![
            mk_endpoint("fresh", "sk-ant-api-a"),
            mk_endpoint("expiring", "sk-ant-api-b"),
        ],
        strategy,
    );
    Arc::get_mut(&mut state).unwrap().soft_limit = PROD_SOFT_LIMIT;
    let now = AppState::now_epoch();
    let in_6_5_days = now + 6 * 86400 + 43200;
    let in_20_hours = now + 20 * 3600;
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, in_6_5_days).await;
    set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, in_20_hours).await;

    // A session whose sticky bucket is the fresh (low-waste-risk) account.
    let key = keys_hashing_to(&state, 0, 1, 20000, "skew-session")
        .await
        .pop()
        .expect("a key hashing to the fresh account");
    for i in 0..500 {
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_eq!(
            idx, 0,
            "{strategy:?} request {i}: reset-time skew alone migrated the session off its hashed account"
        );
    }
}

#[tokio::test]
async fn affinity_override_ignores_reset_time_skew() {
    assert_reset_time_skew_keeps_affinity(RoutingStrategy::StickyWeightedV2).await;
}

#[tokio::test]
async fn affinity_override_ignores_reset_time_skew_legacy() {
    assert_reset_time_skew_keeps_affinity(RoutingStrategy::DynamicCapacityV1).await;
}

/// Regression (LAB-4719): a `reason="spent"` migration fled an account at
/// headroom 0.040 and landed on its equally spent twin (headroom 0.040,
/// util_7d 0.96), which answered with Anthropic's entitlement 400, while the
/// same log line named a healthy `best_account` (util_7d 0.21 → headroom 0.79;
/// 0.040 / 0.79 = the logged `ratio=0.051`). The twin got picked because a
/// 7d-spent account is still gate-healthy and its expiring quota earns it a
/// large waste-risk bucket. A replacement the override would itself flee must
/// never be chosen.
#[tokio::test]
async fn affinity_spent_migration_skips_equally_spent_replacement() {
    let state = sticky_v2_state(vec![
        mk_endpoint("spent", "sk-ant-api-a"),
        mk_endpoint("spent-twin", "sk-ant-api-b"),
        mk_endpoint("healthy", "sk-ant-api-c"),
    ]);
    let now = AppState::now_epoch();
    // Both spent accounts reset in 20h: outside the near-reset ramp (so the 4%
    // left reads as spent) yet close enough for a sizeable bucket.
    set_account_utilization(&state, 0, 0.00, 0.96, now + 10000, now + 20 * 3600).await;
    set_account_utilization(&state, 1, 0.00, 0.96, now + 10000, now + 20 * 3600).await;
    set_account_utilization(&state, 2, 0.12, 0.21, now + 10000, now + 5 * 86400).await;

    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    let headroom: Vec<(f64, &str)> = candidates
        .iter()
        .map(|c| {
            let (h, bind) = affinity_headroom(c);
            (h, bind.as_str())
        })
        .collect();
    for (i, want) in [0.04, 0.04, 0.79].into_iter().enumerate() {
        assert!(
            (headroom[i].0 - want).abs() < 1e-9,
            "fixture must reproduce the logged headroom triple: {headroom:?}"
        );
    }
    assert_eq!(
        headroom[0].1, "spent",
        "the sticky account must bind on its week"
    );

    let sessions = keys_hashing_to(&state, 0, 40, 20000, "lab4719-session").await;
    assert_eq!(
        sessions.len(),
        40,
        "need sessions sticky on the spent account"
    );
    for s in &sessions {
        let idx = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_eq!(
            idx, 2,
            "session {s} sticky on the spent account must migrate to the healthy one, not its equally spent twin"
        );
    }
}

#[tokio::test]
async fn dynamic_capacity_v1_ignores_replica_local_request_history() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
        ],
        RoutingStrategy::DynamicCapacityV1,
    );
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }

    state.endpoints[1].requests.store(900, Ordering::Relaxed);
    state.endpoints[0].requests.store(100, Ordering::Relaxed);

    let mut primary_count = 0u32;
    let total = 200u32;
    for i in 0..total {
        let key = format!("balance-test-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if idx == 0 {
            primary_count += 1;
        }
    }

    assert!(
        (60..=140).contains(&primary_count),
        "dynamic-capacity-v1 should ignore replica-local request skew, got {}/{} to primary",
        primary_count,
        total
    );
}

#[tokio::test]
async fn sticky_weighted_v2_preserves_hash_distribution_under_skewed_history() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization_5h = Some(0.10);
        info.reset_5h = Some(AppState::now_epoch() + 10000);
    }

    state.endpoints[1].requests.store(900, Ordering::Relaxed);
    state.endpoints[0].requests.store(100, Ordering::Relaxed);

    let mut primary_count = 0u32;
    let total = 200u32;
    for i in 0..total {
        let key = format!("balance-test-{}", i);
        let idx = state
            .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if idx == 0 {
            primary_count += 1;
        }
    }

    assert!(
        (60..=140).contains(&primary_count),
        "sticky-weighted-v2 should stay near hash distribution, got {}/{} to primary",
        primary_count,
        total
    );
}

#[tokio::test]
async fn pick_unsticky_on_overload() {
    // When a preferred account gets overloaded, sessions should migrate.
    // Hash-based pick assigns ~60% of sessions to primary initially. After
    // overload primary's gate is 0.99, past soft_limit, so it fails the
    // overage rule and every session that hashed to it moves.
    let state = sticky_v2_state(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("backup", "sk-ant-api-b"),
    ]);

    // Start with primary having lots of headroom
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.2); // headroom = 0.8
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.5); // headroom = 0.5
    }

    // Collect keys that initially pick primary
    let mut primary_keys: Vec<String> = Vec::new();
    for i in 0..500 {
        let key = format!("test-client-{}", i);
        if state.pick_endpoint(Some(&key), "", &[]).await.unwrap() == 0 {
            primary_keys.push(key);
        }
    }
    assert!(
        primary_keys.len() >= 50,
        "should find many keys that pick primary"
    );

    // Now overload primary: util=0.99 (headroom=0.01), backup stays at 0.5 (headroom=0.5)
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.99);
    }

    // All sessions that hashed to primary should migrate.
    let mut migrated = 0usize;
    for key in &primary_keys {
        if state.pick_endpoint(Some(key), "", &[]).await.unwrap() == 1 {
            migrated += 1;
        }
    }
    assert_eq!(
        migrated,
        primary_keys.len(),
        "every client on the overloaded account should migrate"
    );
}

#[tokio::test]
async fn affinity_sticky_near_equal_weights() {
    // Game theory: two accounts with similar utilization must produce
    // perfectly stable session routing. This is the prompt-cache scenario —
    // bouncing between accounts wastes cache-creation tokens.
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.07, 0.73, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.08, 0.71, now + 10000, now + 300000).await;

    let session = "10.42.0.1:claude:first-steps:-:9e8efc8c-2891-4206-ae10-8bcd5fa7e1f0";
    let first = state
        .pick_endpoint(Some(session), "claude-opus-4-6", &[])
        .await
        .unwrap();

    // Same session, 100 consecutive requests: must ALWAYS pick the same account
    for i in 0..100 {
        let pick = state
            .pick_endpoint(Some(session), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_eq!(
            pick, first,
            "request {} routed to account {} instead of {}, session is bouncing",
            i, pick, first
        );
    }
}

#[tokio::test]
async fn affinity_stable_despite_utilization_drift() {
    // Game theory: utilization changes slightly after each response,
    // but sessions must remain stable. Simulates the real scenario where
    // each request nudges the 5h utilization up by a tiny amount.
    // Uses multiple sessions and asserts low aggregate migration rate,
    // avoiding boundary-sensitivity from any single hard-coded key.
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.05, 0.70, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.05, 0.68, now + 10000, now + 300000).await;

    // Record initial picks for 100 distinct sessions
    let num_sessions = 100;
    let sessions: Vec<String> = (0..num_sessions)
        .map(|i| format!("drift-session-{}", i))
        .collect();
    let mut initial_picks = Vec::with_capacity(num_sessions);
    for s in &sessions {
        initial_picks.push(
            state
                .pick_endpoint(Some(s), "claude-opus-4-6", &[])
                .await
                .unwrap(),
        );
    }

    // Simulate 50 requests with drifting utilization
    for i in 0..50 {
        let drift = 0.002 * (i as f64);
        {
            let mut info = state.endpoints[0].rate_info.write().await;
            info.utilization_5h = Some(0.05 + drift);
        }
        {
            let mut info = state.endpoints[1].rate_info.write().await;
            info.utilization_5h = Some(0.05 + drift * 0.8);
        }
    }

    // After drift, check how many sessions migrated
    let mut migrated = 0usize;
    for (j, s) in sessions.iter().enumerate() {
        let pick = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        if pick != initial_picks[j] {
            migrated += 1;
        }
    }

    // Allow up to 10% migration from boundary effects — small drift shouldn't
    // cause wholesale session migration.
    assert!(
        migrated <= num_sessions / 10,
        "too many sessions migrated after util drift: {}/{} (max {})",
        migrated,
        num_sessions,
        num_sessions / 10,
    );
}

#[tokio::test]
async fn affinity_different_sessions_distribute() {
    // Game theory: different sessions should naturally distribute across
    // accounts via hash, providing cross-account balance without per-session
    // instability. This is the balancing mechanism.
    let state = test_state_with(vec![
        mk_endpoint("primary", "sk-ant-api-a"),
        mk_endpoint("jeff", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.50, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.50, now + 10000, now + 300000).await;

    let mut picks = [0u32; 2];
    for i in 0..500 {
        let session = format!("session-{}", i);
        let idx = state
            .pick_endpoint(Some(&session), "claude-opus-4-6", &[])
            .await
            .unwrap();
        picks[idx] += 1;
    }

    // With equal weights, hash should distribute roughly 50/50 (±15%)
    let primary_pct = picks[0] as f64 / 500.0;
    assert!(
        (0.35..=0.65).contains(&primary_pct),
        "expected ~50/50 distribution, got primary={} jeff={} ({:.0}%)",
        picks[0],
        picks[1],
        primary_pct * 100.0
    );
}

#[tokio::test]
async fn affinity_breaks_on_egregious_disparity() {
    // Game theory: when one account is under extreme pressure,
    // cache locality cost is worth paying to avoid quota exhaustion.
    let state = test_state_with(vec![
        mk_endpoint("healthy", "sk-ant-api-a"),
        mk_endpoint("dying", "sk-ant-api-b"),
    ]);
    let now = AppState::now_epoch();
    // healthy: 5h=0.10, 7d=0.20 (lots of capacity)
    // dying: 5h=0.10, 7d=0.98 (nearly exhausted)
    set_account_utilization(&state, 0, 0.10, 0.20, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.98, now + 10000, now + 300000).await;

    // ALL sessions should go to healthy (dying's weight is negligible)
    let mut healthy_count = 0u32;
    for i in 0..200 {
        let session = format!("session-{}", i);
        if state
            .pick_endpoint(Some(&session), "claude-opus-4-6", &[])
            .await
            .unwrap()
            == 0
        {
            healthy_count += 1;
        }
    }
    assert_eq!(
        healthy_count, 200,
        "all sessions should override to healthy account, got {}/200",
        healthy_count
    );
}

#[tokio::test]
async fn affinity_moderate_disparity_stays_sticky() {
    // Game theory: moderate 7d difference (0.73 vs 0.41) should NOT
    // break stickiness. The cache-creation cost outweighs the routing
    // benefit at this disparity level.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.13, 0.41, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.12, 0.79, now + 10000, now + 300000).await;

    // Both accounts should retain their hashed sessions
    let mut primary_picks = 0u32;
    let mut jeff_picks = 0u32;
    for i in 0..500 {
        let session = format!("moderate-session-{}", i);
        match state
            .pick_endpoint(Some(&session), "claude-opus-4-6", &[])
            .await
            .unwrap()
        {
            0 => primary_picks += 1,
            _ => jeff_picks += 1,
        }
    }
    // Both should get traffic (no one-sided override)
    assert!(
        primary_picks > 100 && jeff_picks > 100,
        "moderate disparity should preserve stickiness on both: primary={}, jeff={}",
        primary_picks,
        jeff_picks
    );
}

#[tokio::test]
async fn pick_proportional_distribution() {
    // Verify distribution matches headroom ratios over many calls
    let state = test_state_with(vec![
        mk_endpoint("a", "sk-ant-api-a"),
        mk_endpoint("b", "sk-ant-api-b"),
        mk_endpoint("c", "sk-ant-api-c"),
    ]);

    // a=0.2 util (headroom 0.8), b=0.5 util (headroom 0.5), c=0.8 util (headroom 0.2)
    // Total headroom = 1.5. Expected: a=53.3%, b=33.3%, c=13.3%
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.2);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.5);
    }
    {
        let mut info = state.endpoints[2].rate_info.write().await;
        info.utilization = Some(0.8);
    }

    let mut counts = [0u32; 3];
    let total = 10000u32;
    for _ in 0..total {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }

    let pcts: Vec<f64> = counts.iter().map(|&c| c as f64 / total as f64).collect();
    // Expected: ~53.3%, ~33.3%, ~13.3% (±3%)
    assert!(
        (0.50..=0.57).contains(&pcts[0]),
        "account a should get ~53% traffic, got {:.1}%",
        pcts[0] * 100.0
    );
    assert!(
        (0.30..=0.37).contains(&pcts[1]),
        "account b should get ~33% traffic, got {:.1}%",
        pcts[1] * 100.0
    );
    assert!(
        (0.10..=0.17).contains(&pcts[2]),
        "account c should get ~13% traffic, got {:.1}%",
        pcts[2] * 100.0
    );
}

// ── LAB-9360: a keyed session leaves its account only under the overage rule ──
//
// StickyWeightedV2 keeps a keyed session on the account that wins its
// rendezvous hash among the candidates passing the overage rule: affinity
// headroom of at least `1 - soft_limit`, and a candidate in the request's
// tier. The fixtures run at the production `soft_limit`, since the test base's
// open ceiling of 1.0 makes the rule pass everything.

const PROD_SOFT_LIMIT: f64 = 0.90;
const POOL: usize = 40;
const OPUS: &str = "claude-opus-4-6";
/// The account the AC tests' sessions start on.
const HOME: usize = 7;

/// A StickyWeightedV2 state at the production `soft_limit`.
fn sticky_v2_state(endpoints: Vec<Endpoint>) -> Arc<AppState> {
    let mut state = test_state_with_strategy(endpoints, RoutingStrategy::StickyWeightedV2);
    Arc::get_mut(&mut state).unwrap().soft_limit = PROD_SOFT_LIMIT;
    state
}

/// `n` neutral accounts, all priority 0.
fn pool_endpoints(n: usize) -> Vec<Endpoint> {
    (0..n)
        .map(|i| mk_endpoint(&format!("acct-{i:02}"), &format!("sk-ant-api-{i:02}")))
        .collect()
}

/// A production-shaped pool: StickyWeightedV2, `soft_limit` 0.90, every
/// Anthropic account healthy, with weekly use spread from 0.10 to 0.60 so the
/// weights differ.
async fn prod_pool_of(endpoints: Vec<Endpoint>) -> Arc<AppState> {
    let n = endpoints.len();
    let state = sticky_v2_state(endpoints);
    let now = AppState::now_epoch();
    for i in 0..n {
        let util_7d = 0.10 + 0.50 * i as f64 / n as f64;
        set_account_utilization(&state, i, 0.10, util_7d, now + 10_000, now + 4 * 86_400).await;
    }
    state
}

async fn prod_pool() -> Arc<AppState> {
    prod_pool_of(pool_endpoints(POOL)).await
}

/// A key whose session starts on `HOME`, routed there once as a request's
/// first pick, so this replica remembers it there. The key carries `marker`.
async fn seed_session(state: &AppState, marker: &str) -> String {
    // Install the capture subscriber before any line this test asserts on.
    log_capture_buf();
    let key = keys_hashing_to(state, HOME, 1, 50_000, marker)
        .await
        .pop()
        .expect("a key hashing to the home account");
    assert_eq!(state.pick_endpoint(Some(&key), OPUS, &[]).await, Some(HOME));
    key
}

/// The session keeps `HOME` for 100 consecutive picks, and nothing counts or
/// logs a move.
async fn assert_session_stays(state: &AppState, key: &str, marker: &str) {
    for i in 0..100 {
        assert_eq!(
            state.pick_endpoint(Some(key), OPUS, &[]).await,
            Some(HOME),
            "pick {i} left the home account"
        );
    }
    assert_eq!(migrations(state), [0; 5], "no move, no count");
    assert_eq!(log_lines(marker), Vec::<String>::new(), "no move, no line");
}

/// The session leaves `HOME` for one account that fails no part of the
/// overage rule, and stays there for 50 picks; the move counts once under
/// `reason` and logs one line naming the reason and both accounts.
async fn assert_session_leaves(state: &AppState, key: &str, marker: &str, reason: MigrationReason) {
    let mut served = std::collections::BTreeSet::new();
    for _ in 0..50 {
        served.insert(state.pick_endpoint(Some(key), OPUS, &[]).await.unwrap());
    }
    let served: Vec<usize> = served.into_iter().collect();
    let [new] = served[..] else {
        panic!("the session flapped across {served:?}");
    };
    assert_ne!(new, HOME, "the session must leave its account");

    let candidates = state.routing_candidates(OPUS, &[]).await;
    let c = candidates
        .iter()
        .find(|c| c.endpoint == new)
        .expect("the new account is a candidate: not hard-limited or cooling");
    assert_eq!(c.priority, 0, "the new account is not overage-demoted");
    assert!(
        c.gate < state.soft_limit,
        "the new account's gate is under soft_limit"
    );
    assert!(
        affinity_headroom(c).0 >= 1.0 - state.soft_limit,
        "the new account is not near its 5h or weekly limit"
    );

    let mut want = [0; 5];
    want[reason as usize] = 1;
    assert_eq!(
        migrations(state),
        want,
        "one move, counted once as {}",
        reason.as_str()
    );
    let lines = log_lines(marker);
    let [line] = &lines[..] else {
        panic!("one move must log exactly one line, got {lines:?}");
    };
    let from = format!("from_account=\"acct-{HOME:02}\"");
    let to = format!("to_account=\"acct-{new:02}\"");
    let why = format!("reason=\"{}\"", reason.as_str());
    assert!(
        line.contains(" INFO ")
            && line.contains(&from)
            && line.contains(&to)
            && line.contains(&why),
        "the line must name the reason and both accounts: {line}"
    );
}

/// Weekly status on `HOME`'s general claim, as Anthropic reports it.
async fn set_home_weekly(
    state: &AppState,
    util_5h: f64,
    util_7d: f64,
    reset_in: u64,
    status: &str,
) {
    let now = AppState::now_epoch();
    set_account_utilization(state, HOME, util_5h, util_7d, now + 10_000, now + reset_in).await;
    set_7d_status(state, HOME, status).await;
}

const HOUR: u64 = 3600;

/// AC1: the weekly `allowed_warning` floor (gate 0.80) leaves headroom 0.20,
/// which used to sit under 0.25 × a best account at 0.80 and moved the session
/// on every request. It now keeps the session. The floor is set before the
/// session is placed: it cuts the account's weight, and a stateless hash
/// re-homes part of an account's keys once when its weight falls.
#[tokio::test]
async fn affinity_warning_floor_keeps_the_session() {
    let state = prod_pool().await;
    let marker = "lab9360-ac1";
    set_home_weekly(&state, 0.10, 0.80, 4 * 24 * HOUR, "allowed_warning").await;
    let key = seed_session(&state, marker).await;

    let candidates = state.routing_candidates(OPUS, &[]).await;
    let headroom = |i: usize| affinity_headroom(&candidates[i]).0;
    assert!(
        (headroom(HOME) - 0.20).abs() < 1e-9,
        "home sits on the warning floor"
    );
    assert!(
        headroom(0) >= 0.80,
        "the best account has headroom of at least 0.80"
    );

    assert_session_stays(&state, &key, marker).await;
}

/// AC2: a partly spent week (0.88, reset days away) keeps the session.
#[tokio::test]
async fn affinity_partly_spent_week_keeps_the_session() {
    let state = prod_pool().await;
    let marker = "lab9360-ac2";
    set_home_weekly(&state, 0.20, 0.88, 4 * 24 * HOUR, "allowed_warning").await;
    let key = seed_session(&state, marker).await;
    assert_session_stays(&state, &key, marker).await;
}

/// AC3: a 5h gate past `soft_limit` moves the session.
#[tokio::test]
async fn affinity_session_leaves_a_loaded_5h_window() {
    let state = prod_pool().await;
    let marker = "lab9360-ac3-5h";
    let key = seed_session(&state, marker).await;
    let now = AppState::now_epoch();
    set_account_utilization(&state, HOME, 0.92, 0.30, now + 10_000, now + 4 * 86_400).await;
    assert_session_leaves(&state, &key, marker, MigrationReason::Loaded).await;
}

/// AC3: weekly use past `soft_limit`, outside the near-reset ramp, moves the
/// session, though the gate never carries weekly use.
#[tokio::test]
async fn affinity_session_leaves_a_spent_week() {
    let state = prod_pool().await;
    let marker = "lab9360-ac3-7d";
    let key = seed_session(&state, marker).await;
    let now = AppState::now_epoch();
    set_account_utilization(&state, HOME, 0.10, 0.92, now + 10_000, now + 4 * 86_400).await;
    assert_session_leaves(&state, &key, marker, MigrationReason::Spent).await;
}

/// AC3: the `throttled` status floor (0.98) moves the session.
#[tokio::test]
async fn affinity_session_leaves_a_throttled_account() {
    let state = prod_pool().await;
    let marker = "lab9360-ac3-throttled";
    let key = seed_session(&state, marker).await;
    set_7d_status(&state, HOME, "throttled").await;
    assert_session_leaves(&state, &key, marker, MigrationReason::Floored).await;
}

/// AC3: an account serving via overage is demoted out of the tier, which
/// moves the session.
#[tokio::test]
async fn affinity_session_leaves_an_overage_account() {
    let state = prod_pool().await;
    let marker = "lab9360-ac3-overage";
    let key = seed_session(&state, marker).await;
    {
        let mut info = state.endpoints[HOME].rate_info.write().await;
        info.overage_in_use = true;
        info.overage_utilization = Some(0.10);
        info.overage_reset = Some(AppState::now_epoch() + 86_400);
    }
    assert_session_leaves(&state, &key, marker, MigrationReason::Unavailable).await;
}

/// AC3: an account cooling after a 429 is not a candidate, which moves the
/// session.
#[tokio::test]
async fn affinity_session_leaves_a_hard_limited_account() {
    let state = prod_pool().await;
    let marker = "lab9360-ac3-429";
    let key = seed_session(&state, marker).await;
    state.endpoints[HOME]
        .rate_info
        .write()
        .await
        .hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
    assert_session_leaves(&state, &key, marker, MigrationReason::Unavailable).await;
}

/// AC4: weekly quota expiring within the near-reset ramp is headroom, so an
/// account at 0.95 with its week resetting in 3h keeps its sessions.
#[tokio::test]
async fn affinity_quota_near_reset_keeps_the_session() {
    let state = prod_pool().await;
    let marker = "lab9360-ac4-stays";
    set_home_weekly(&state, 0.10, 0.95, 3 * HOUR, "allowed_warning").await;
    let key = seed_session(&state, marker).await;
    assert_session_stays(&state, &key, marker).await;
}

/// AC4: the same account with its week resetting in 30h is spent, and its
/// session leaves.
#[tokio::test]
async fn affinity_quota_far_from_reset_moves_the_session() {
    let state = prod_pool().await;
    let marker = "lab9360-ac4-moves";
    set_home_weekly(&state, 0.10, 0.95, 3 * HOUR, "allowed_warning").await;
    let key = seed_session(&state, marker).await;
    set_home_weekly(&state, 0.10, 0.95, 30 * HOUR, "allowed_warning").await;
    assert_session_leaves(&state, &key, marker, MigrationReason::Spent).await;
}

/// A session that left an unavailable account goes back when it returns,
/// and the return counts as `rehomed`: its replacement still passes.
#[tokio::test]
async fn affinity_return_to_a_recovered_account_counts_as_rehomed() {
    let state = prod_pool().await;
    let marker = "lab9360-rehomed";
    let key = seed_session(&state, marker).await;
    state.endpoints[HOME]
        .rate_info
        .write()
        .await
        .hard_limited_until = Some(Instant::now() + Duration::from_secs(60));
    assert_ne!(state.pick_endpoint(Some(&key), OPUS, &[]).await, Some(HOME));
    state.endpoints[HOME]
        .rate_info
        .write()
        .await
        .hard_limited_until = None;
    assert_eq!(state.pick_endpoint(Some(&key), OPUS, &[]).await, Some(HOME));

    let mut want = [0; 5];
    want[MigrationReason::Unavailable as usize] = 1;
    want[MigrationReason::Rehomed as usize] = 1;
    assert_eq!(migrations(&state), want);
    assert_eq!(log_lines(marker).len(), 2, "one line per move");
}

/// A retry's re-pick never feeds the signal: the session's next first pick
/// is compared with its last first pick.
#[tokio::test]
async fn affinity_retry_repick_is_not_a_move() {
    let state = prod_pool().await;
    let marker = "lab9360-retry";
    let key = seed_session(&state, marker).await;
    let retry = state.pick_endpoint(Some(&key), OPUS, &[HOME]).await;
    assert!(retry.is_some_and(|i| i != HOME), "the retry goes elsewhere");
    assert_eq!(state.pick_endpoint(Some(&key), OPUS, &[]).await, Some(HOME));
    assert_eq!(migrations(&state), [0; 5]);
    assert_eq!(log_lines(marker), Vec::<String>::new());
}

/// AC8: when no account in the tier passes the overage rule, keyed requests
/// are still served by the hash over the whole tier: no `None`, no jump to
/// the next tier, and no counted move.
#[tokio::test]
async fn affinity_nothing_passes_hashes_over_the_tier() {
    let mut endpoints = pool_endpoints(POOL + 1);
    endpoints[POOL].priority = 1;
    let state = prod_pool_of(endpoints).await;
    let marker = "lab9360-ac8";
    let key = seed_session(&state, marker).await;

    let now = AppState::now_epoch();
    for i in 0..POOL {
        set_account_utilization(&state, i, 0.10, 0.95, now + 10_000, now + 4 * 86_400).await;
    }
    let candidates = state.routing_candidates(OPUS, &[]).await;
    let tier: Vec<&RoutingCandidate> = candidates.iter().filter(|c| c.priority == 0).collect();
    assert!(
        tier.iter()
            .all(|c| !state.keeps_affinity(c) && c.gate < state.soft_limit),
        "every tier-0 account is healthy by gate yet fails the overage rule"
    );
    let want = state.pick_rendezvous(&tier, &key).unwrap().endpoint;

    for i in 0..50 {
        assert_eq!(
            state.pick_endpoint(Some(&key), OPUS, &[]).await,
            Some(want),
            "pick {i} must hash over the whole tier"
        );
    }
    assert_eq!(migrations(&state), [0; 5], "a degraded pick is not a move");
    assert_eq!(log_lines(marker), Vec::<String>::new());
}

/// Synthetic candidates for the pure picker tests: every account passes the
/// overage rule, and weights spread 1:10 across the pool.
fn spread_candidates(n: usize) -> Vec<RoutingCandidate> {
    (0..n)
        .map(|i| RoutingCandidate {
            endpoint: i,
            priority: 0,
            gate_5h: 0.10,
            gate_7d: 0.0,
            gate: 0.10,
            wr: 1.0,
            unused_7d: 0.90,
            weight: 1.0 + 9.0 * i as f64 / (n - 1) as f64,
            gate_floor_bound: false,
            source: "waste_risk",
        })
        .collect()
}

/// The StickyWeightedV2 keyed pick for `key` over `pool`.
fn keyed_pick(state: &AppState, pool: &[RoutingCandidate], key: &str) -> usize {
    let refs: Vec<&RoutingCandidate> = pool.iter().collect();
    let total: f64 = pool.iter().map(|c| c.weight).sum();
    state
        .pick_sticky_weighted_v2(&refs, total, Some(key))
        .endpoint
}

/// AC5: a weight change moves keys only onto the account whose weight rose,
/// and at most twice its gain in share; removing an account moves only its
/// own keys. The range walk this replaces shifted every boundary after the
/// changed account, re-homing keys all over the pool.
#[tokio::test]
async fn affinity_weight_change_moves_keys_only_onto_or_off_that_account() {
    let state = sticky_v2_state(pool_endpoints(POOL));
    let pool = spread_candidates(POOL);
    let keys: Vec<String> = (0..10_000).map(|i| format!("lab9360-ac5-{i}")).collect();
    let before: Vec<usize> = keys.iter().map(|k| keyed_pick(&state, &pool, k)).collect();

    let changed = 13;
    let mut reset = pool.clone();
    reset[changed].weight *= 2.5;
    let share =
        |p: &[RoutingCandidate]| p[changed].weight / p.iter().map(|c| c.weight).sum::<f64>();
    let gain = share(&reset) - share(&pool);
    let mut moved = 0;
    for (k, &was) in keys.iter().zip(&before) {
        let now = keyed_pick(&state, &reset, k);
        if now != was {
            assert_eq!(
                now, changed,
                "key {k} moved to an account whose weight did not change"
            );
            moved += 1;
        }
    }
    let moved_share = moved as f64 / keys.len() as f64;
    assert!(moved > 0, "a 2.5x weight must win some keys");
    assert!(
        moved_share <= 2.0 * gain,
        "moved {moved_share:.4} of keys for a share gain of {gain:.4}"
    );

    let removed = 21;
    let without: Vec<RoutingCandidate> = pool
        .iter()
        .copied()
        .filter(|c| c.endpoint != removed)
        .collect();
    for (k, &was) in keys.iter().zip(&before) {
        let now = keyed_pick(&state, &without, k);
        if now != was {
            assert_eq!(was, removed, "key {k} moved though its account stayed");
        }
    }
}

/// AC6: over 100,000 keys, every account's key count is within 4 sigma of its
/// weight share.
#[tokio::test]
async fn affinity_placement_is_proportional_to_weight() {
    let state = sticky_v2_state(pool_endpoints(POOL));
    let pool = spread_candidates(POOL);
    let n = 100_000;
    let mut counts = [0usize; POOL];
    for i in 0..n {
        counts[keyed_pick(&state, &pool, &format!("lab9360-ac6-{i}"))] += 1;
    }
    let total: f64 = pool.iter().map(|c| c.weight).sum();
    for c in &pool {
        let s = c.weight / total;
        let expected = n as f64 * s;
        let sigma = (n as f64 * s * (1.0 - s)).sqrt();
        let got = counts[c.endpoint] as f64;
        assert!(
            (got - expected).abs() <= 4.0 * sigma,
            "account {} got {got} keys, expected {expected:.0} ± {:.0}",
            c.endpoint,
            4.0 * sigma
        );
    }
}

/// AC7: the move signal's memory stays bounded, and remembers a key across
/// a generation's turnover.
#[test]
fn affinity_last_pick_is_bounded() {
    let mut map = AffinityLastPick::default();
    let generation = AffinityLastPick::GENERATION as u64;
    assert_eq!(map.swap(0, 1), None, "a first sight has no last pick");
    for k in 1..generation - 1 {
        map.swap(k, 0);
    }
    assert_eq!(
        map.swap(0, 2),
        Some(1),
        "a key seen in this generation is remembered"
    );
    for k in generation..3 * generation {
        map.swap(k, 0);
        assert!(
            map.len() <= 2 * AffinityLastPick::GENERATION,
            "the map must stay bounded"
        );
    }
    assert_eq!(
        map.swap(0, 3),
        None,
        "a key two generations old is forgotten"
    );
}

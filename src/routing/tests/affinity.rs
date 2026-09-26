use super::*;

// ── LAB-3295: status-floor-bound migrations log at INFO with reason="floored" ──

/// Pin ONE sticky key that hashes to `sticky_idx` (via `keys_hashing_to`, the
/// same idiom the GH#156 counter tests use), route it once, and return the
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
    let counts = [
        state.affinity_migrations[AffinityBind::Loaded as usize].load(Ordering::Relaxed),
        state.affinity_migrations[AffinityBind::Spent as usize].load(Ordering::Relaxed),
        state.affinity_migrations[AffinityBind::Floored as usize].load(Ordering::Relaxed),
    ];
    let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let lines = output
        .lines()
        .filter(|l| l.contains(marker))
        .map(str::to_string)
        .collect();
    (counts, lines)
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

/// AC-5(a), StickyWeightedV2: the sticky account is fresh on raw utilisation
/// (5h/7d both 0.10) but Anthropic has flagged its weekly window
/// (`allowed_warning` → 0.80 gate floor). The override still fires — routing is
/// unchanged — but the migration is a routine status-floor move, so it logs at
/// INFO with `reason="floored"` and increments only the floored counter. No
/// `affinity broken` WARN is emitted.
#[tokio::test]
async fn affinity_floor_bound_migration_logs_info_v2() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("sticky", "sk-ant-api-a"),
            mk_endpoint("fresh", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, now + 300000).await;
    set_7d_status(&state, 0, "allowed_warning").await;

    let marker = "lab3295-floored-v2";
    let (counts, lines) = migrate_one_sticky(&state, 0, marker).await;

    assert_eq!(
        counts,
        [0, 0, 1],
        "a status-floor migration counts only as floored, got [loaded,spent,floored]={counts:?}"
    );
    assert!(
        !lines.is_empty(),
        "expected at least one override log line for marker {marker}"
    );
    for l in &lines {
        assert!(
            l.contains(" INFO ") && !l.contains(" WARN "),
            "floor-bound migration must log at INFO, not WARN: {l}"
        );
        assert!(
            l.contains("reason=\"floored\"") && l.contains("affinity migrated"),
            "line must carry reason=\"floored\" and the routine message: {l}"
        );
        assert!(
            !l.contains("affinity broken"),
            "the `affinity broken` WARN text must not appear for a floor-bound migration: {l}"
        );
    }
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

/// AC-5(b): the sticky account is genuinely load-bound — raw 5h utilisation
/// 0.80, status `allowed` (no floor). The migration stays a WARN with
/// `reason="loaded"` and the unchanged `affinity broken` message.
#[tokio::test]
async fn affinity_raw_load_migration_stays_warn() {
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("sticky", "sk-ant-api-a"),
            mk_endpoint("fresh", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    // Raw 5h load of 0.80 → gate 0.80 with no status floor involved.
    set_account_utilization(&state, 0, 0.80, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.10, now + 10000, now + 300000).await;

    let marker = "lab3295-loaded";
    let (counts, lines) = migrate_one_sticky(&state, 0, marker).await;

    assert_eq!(
        counts,
        [1, 0, 0],
        "raw-load migration counts only as loaded, never floored, got {counts:?}"
    );
    assert!(
        !lines.is_empty(),
        "expected override log lines for {marker}"
    );
    for l in &lines {
        assert!(
            l.contains(" WARN ")
                && l.contains("reason=\"loaded\"")
                && l.contains("affinity broken"),
            "raw-load migration must stay a WARN with the unchanged message: {l}"
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

#[tokio::test]
async fn affinity_override_fires_with_three_candidates_egregious_disparity() {
    // When one account is near-exhausted (85% util) and others have plenty
    // of headroom, sessions that hash into the exhausted account's tiny
    // bucket should be overridden to the best account.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("jeff", "sk-ant-api-b"),
            mk_endpoint("insight", "sk-ant-api-c"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.09, 0.09, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.20, 0.31, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.85, 0.85, now + 10000, now + 300000).await;

    // Find a key that would naturally hash into insight's bucket.
    // Candidates/weights are static, so compute boundaries once.
    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
    let total_weight: f64 = candidates.iter().map(|c| c.weight).sum();
    let mut boundaries: Vec<(usize, f64)> = Vec::new();
    let mut cumulative = 0.0;
    for c in &candidates {
        cumulative += c.weight;
        boundaries.push((c.endpoint, cumulative));
    }

    let mut insight_key = None;
    for i in 0..10000 {
        let key = format!("find-insight-{}", i);
        let target = (stable_affinity_hash(&key) as f64 / u64::MAX as f64) * total_weight;
        for &(idx, boundary) in &boundaries {
            if target < boundary {
                if idx == 2 {
                    insight_key = Some(key);
                }
                break;
            }
        }
        if insight_key.is_some() {
            break;
        }
    }
    let key = insight_key.expect("should find a key that hashes to insight's bucket");

    // With the override, pick_account should redirect away from insight
    let idx = state
        .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
        .await
        .unwrap();
    // New contract: the override migrates to a session-stable replacement
    // re-picked from the healthy remainder (deterministic per session, NOT the
    // global argmax). It must be one of the healthy accounts, never the loaded
    // one — but which healthy account is the session's stable hash, not "best".
    assert_ne!(
        idx, 2,
        "insight (85% util) should be overridden despite affinity hash landing there"
    );
    assert!(
        idx == 0 || idx == 1,
        "override must land on a healthy account (primary or jeff), got {idx}"
    );
}

/// Helper: under StickyWeightedV2, find affinity keys whose *sticky* bucket is
/// `target_idx` (i.e. the sessions that get overridden when that account is the
/// loaded one). Boundaries are static given fixed utilizations.
async fn keys_hashing_to(
    state: &AppState,
    target_idx: usize,
    want: usize,
    scan: usize,
    prefix: &str,
) -> Vec<String> {
    let candidates = state.routing_candidates("claude-opus-4-6", &[]).await;
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
    // Regression: when a session's sticky account is too loaded, the override
    // must NOT funnel every such session onto the single global argmax (`best`).
    // That target rotates as utilizations drift, sweeping sessions across the
    // pool and paying a cold-cache `cache_creation` charge on every hop
    // (measured: a swept client ran a 1.18 create:read ratio vs ~0.05 for sticky
    // clients). Overridden sessions must spread across the healthy remainder.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("loaded", "sk-ant-api-a"),
            mk_endpoint("h1", "sk-ant-api-b"),
            mk_endpoint("h2", "sk-ant-api-c"),
            mk_endpoint("h3", "sk-ant-api-d"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    // idx0 is loaded enough to trigger the override (weight << best * 0.25).
    set_account_utilization(&state, 0, 0.80, 0.80, now + 10000, now + 300000).await;
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

#[tokio::test]
async fn affinity_override_destination_independent_of_argmax() {
    // Regression for the cascade: the override must not chase the global argmax.
    // When the healthy argmax flips (utilizations drift), a session that was
    // already overridden must NOT migrate with it. The old behavior (picked =
    // best) moved EVERY overridden session to whichever account was momentarily
    // best, so a single argmax flip migrated ~100% of them — the cache-burning
    // sweep. A session-stable replacement keeps migration low.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("loaded", "sk-ant-api-a"),
            mk_endpoint("h1", "sk-ant-api-b"),
            mk_endpoint("h2", "sk-ant-api-c"),
            mk_endpoint("h3", "sk-ant-api-d"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.80, 0.80, now + 10000, now + 300000).await;
    set_account_utilization(&state, 3, 0.15, 0.30, now + 10000, now + 300000).await;
    // Config A: h1 is best.
    set_account_utilization(&state, 1, 0.08, 0.18, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.22, 0.42, now + 10000, now + 300000).await;

    // The override set (sessions whose sticky bucket is the loaded account) is
    // fixed by idx0's weight, which the later h1/h2 swap leaves untouched.
    let sessions = keys_hashing_to(&state, 0, 60, 40000, "flip-session").await;
    assert!(
        sessions.len() >= 30,
        "need override sessions, got {}",
        sessions.len()
    );
    let mut dest_a = Vec::with_capacity(sessions.len());
    for s in &sessions {
        let idx = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_ne!(
            idx, 0,
            "override must fire for a session hashed to the loaded account"
        );
        dest_a.push(idx);
    }

    // Config B: swap h1 and h2 utilizations so h2 becomes best. The healthy
    // weight *set* is unchanged (symmetric swap) — only the argmax label moves.
    set_account_utilization(&state, 1, 0.22, 0.42, now + 10000, now + 300000).await;
    set_account_utilization(&state, 2, 0.08, 0.18, now + 10000, now + 300000).await;

    let mut migrated = 0usize;
    for (k, s) in sessions.iter().enumerate() {
        let idx = state
            .pick_endpoint(Some(s), "claude-opus-4-6", &[])
            .await
            .unwrap();
        assert_ne!(idx, 0, "override must still fire after the argmax flip");
        if idx != dest_a[k] {
            migrated += 1;
        }
    }
    let rate = migrated as f64 / sessions.len() as f64;
    assert!(
            rate < 0.40,
            "argmax flip migrated {}/{} ({:.0}%) overridden sessions; destination must not chase the argmax",
            migrated,
            sessions.len(),
            rate * 100.0,
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
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("fresh", "sk-ant-api-a"),
            mk_endpoint("expiring", "sk-ant-api-b"),
        ],
        strategy,
    );
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

#[tokio::test]
async fn affinity_override_spent_discounts_near_weekly_reset() {
    // Panel finding on GH#156: an account at 90% weekly with 3h to reset gets
    // the LARGEST bucket share (waste_risk ≈ 5.6 — burn expiring quota first),
    // so a naive unused-7d comparison would migrate every session the buckets
    // just placed there, on every request. Near the weekly reset, remaining
    // quota is headroom, not "spent": the override must stay quiet.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("fresh", "sk-ant-api-a"),
            mk_endpoint("expiring", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    let now = AppState::now_epoch();
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.90, now + 10000, now + 3 * 3600).await;

    let key = keys_hashing_to(&state, 1, 1, 20000, "expiring-session")
        .await
        .pop()
        .expect("a key hashing to the expiring account");
    let idx = state
        .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
        .await
        .unwrap();
    assert_eq!(
        idx, 1,
        "session must stay on the expiring account it hashed to"
    );
    let [loaded, spent, floored] = &state.affinity_migrations;
    assert_eq!(spent.load(Ordering::Relaxed), 0, "spent counter");
    assert_eq!(loaded.load(Ordering::Relaxed), 0, "loaded counter");
    assert_eq!(floored.load(Ordering::Relaxed), 0, "floored counter");
}

#[tokio::test]
async fn affinity_migration_counter_names_the_binding_window() {
    // `anthropic_affinity_migrations_total{reason}` must attribute each override
    // to the window that actually bound the sticky account (GH#156 AC-5).
    let now = AppState::now_epoch();

    // reason="spent": identical 5h, the sticky account's WEEK is nearly gone.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("fresh", "sk-ant-api-a"),
            mk_endpoint("spent", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.10, 0.95, now + 10000, now + 300000).await;
    let key = keys_hashing_to(&state, 1, 1, 20000, "spent-session")
        .await
        .pop()
        .expect("a key hashing to the spent account");
    let idx = state
        .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
        .await
        .unwrap();
    assert_eq!(idx, 0, "session must leave the spent account");
    let [loaded, spent, floored] = &state.affinity_migrations;
    assert_eq!(spent.load(Ordering::Relaxed), 1, "spent counter");
    assert_eq!(loaded.load(Ordering::Relaxed), 0, "loaded counter");
    assert_eq!(floored.load(Ordering::Relaxed), 0, "floored counter");

    // reason="loaded": identical 7d, the sticky account's 5h gate is the limit.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("idle", "sk-ant-api-a"),
            mk_endpoint("busy", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
    set_account_utilization(&state, 0, 0.10, 0.10, now + 10000, now + 300000).await;
    set_account_utilization(&state, 1, 0.85, 0.10, now + 10000, now + 300000).await;
    let key = keys_hashing_to(&state, 1, 1, 20000, "loaded-session")
        .await
        .pop()
        .expect("a key hashing to the busy account");
    let idx = state
        .pick_endpoint(Some(&key), "claude-opus-4-6", &[])
        .await
        .unwrap();
    assert_eq!(idx, 0, "session must leave the busy account");
    let [loaded, spent, floored] = &state.affinity_migrations;
    assert_eq!(loaded.load(Ordering::Relaxed), 1, "loaded counter");
    assert_eq!(spent.load(Ordering::Relaxed), 0, "spent counter");
    assert_eq!(floored.load(Ordering::Relaxed), 0, "floored counter");
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
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("spent", "sk-ant-api-a"),
            mk_endpoint("spent-twin", "sk-ant-api-b"),
            mk_endpoint("healthy", "sk-ant-api-c"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );
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
    let [loaded, spent, floored] = &state.affinity_migrations;
    assert_eq!(spent.load(Ordering::Relaxed), 40, "spent counter");
    assert_eq!(loaded.load(Ordering::Relaxed), 0, "loaded counter");
    assert_eq!(floored.load(Ordering::Relaxed), 0, "floored counter");
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
    // Hash-based pick assigns ~50% of sessions to each account initially.
    // After overload: primary weight=0.01, backup weight=0.5.
    // Ratio 0.01/0.5 = 0.02 < 0.25 threshold → override fires for all
    // sessions that hashed to primary.
    let state = test_state_with_strategy(
        vec![
            mk_endpoint("primary", "sk-ant-api-a"),
            mk_endpoint("backup", "sk-ant-api-b"),
        ],
        RoutingStrategy::StickyWeightedV2,
    );

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

    // All sessions that hashed to primary should migrate (egregious disparity)
    let mut migrated = 0usize;
    for key in &primary_keys {
        if state.pick_endpoint(Some(key), "", &[]).await.unwrap() == 1 {
            migrated += 1;
        }
    }

    let migration_pct = migrated as f64 / primary_keys.len() as f64;
    assert!(
        migration_pct > 0.95,
        "at least 95% of clients should migrate, got {:.1}% ({}/{})",
        migration_pct * 100.0,
        migrated,
        primary_keys.len()
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

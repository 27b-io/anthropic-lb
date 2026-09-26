use super::*;

// ── Unit: per-client budget ────────────────────────────────────

#[tokio::test]
async fn budget_check_no_limit_configured() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    assert!(state.check_budget("any-client").await.is_ok());
}

#[tokio::test]
async fn budget_check_within_limit() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Within budget
    assert!(state.check_budget("client-a").await.is_ok());

    // Record some usage
    state.record_budget_usage("client-a", 500).await;
    assert!(state.check_budget("client-a").await.is_ok());

    // Exceed budget
    state.record_budget_usage("client-a", 600).await;
    assert!(state.check_budget("client-a").await.is_err());

    // Unknown client has no budget, always ok
    assert!(state.check_budget("unknown").await.is_ok());
}

/// A poisoned `budget_usage` lock must not disable enforcement: `check_budget`
/// still denies an exhausted client (an `if let Ok` skip returned `Ok(())`,
/// granting the budget), and the poison is cleared so `record_budget_usage`
/// keeps counting afterwards rather than skipping for the life of the process.
#[tokio::test]
async fn budget_check_enforces_through_poisoned_lock() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    budgets.insert("client-b".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });
    state.record_budget_usage("client-a", 1500).await;
    poison(&state.budget_usage);

    assert_eq!(
        state.check_budget("client-a").await,
        Err(0),
        "an exhausted client must stay denied through a poisoned lock"
    );
    assert!(
        !state.budget_usage.is_poisoned(),
        "recovery must clear the poison, not just bypass it"
    );

    // Accumulation survives too: new usage is counted and enforced.
    state.record_budget_usage("client-b", 1200).await;
    assert!(state.check_budget("client-b").await.is_err());
}

/// The accumulator itself going through a poisoned lock: usage recorded
/// while poisoned must be counted, not silently dropped.
#[tokio::test]
async fn budget_record_counts_through_poisoned_lock() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });
    poison(&state.budget_usage);
    state.record_budget_usage("client-a", 1500).await;
    assert!(state.check_budget("client-a").await.is_err());
}

/// `anthropic_auth_failures_total` is the brute-force signal: a poisoned
/// lock must not freeze it (writer skips) or zero it (reader skips).
#[tokio::test]
async fn auth_failure_counter_survives_poisoned_lock() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    state.count_auth_failure("proxy", "none");
    poison(&state.auth_failures);
    state.count_auth_failure("proxy", "none");
    assert!(!state.auth_failures.is_poisoned());
    assert_eq!(
        state.auth_failures.lock().unwrap().get(&("proxy", "none")),
        Some(&2)
    );
}

/// `note_model_denied` keeps counting through a poisoned lock.
#[tokio::test]
async fn model_denied_counter_survives_poisoned_lock() {
    let state = test_state_with(vec![mk_endpoint("a", "sk-ant-api-x")]);
    state.note_model_denied("client-a", "claude-opus-4-7");
    poison(&state.model_denied);
    state.note_model_denied("client-a", "claude-opus-4-7");
    assert!(!state.model_denied.is_poisoned());
    let key = ("client-a".to_string(), "claude-opus-4-7".to_string());
    assert_eq!(state.model_denied.lock().unwrap().get(&key), Some(&2));
}

/// The `/_stats` and `/metrics` budget readers report recovered usage through
/// a poisoned lock instead of zero.
#[tokio::test]
async fn budget_readers_report_usage_through_poisoned_lock() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });
    state.record_budget_usage("client-a", 400).await;
    let addr = serve(build_router(state.clone())).await;
    let c = reqwest::Client::new();

    poison(&state.budget_usage);
    let stats: serde_json::Value = c
        .get(format!("http://{addr}/_stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        stats["client_budgets"]["client-a"]["used_today"], 400,
        "{stats}"
    );

    poison(&state.budget_usage);
    let m = c
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        m.contains("anthropic_client_budget_used{client=\"client-a\"} 400"),
        "poisoned budget usage must not be reported as zero:\n{m}"
    );
}

// ── Budget day rollover ──────────────────────────────────────

#[tokio::test]
async fn budget_day_rollover_resets_counter() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 10000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Pre-populate with yesterday's usage (high usage that would exceed budget)
    let yesterday = AppState::now_epoch() / 86400 - 1;
    {
        let mut map = state.budget_usage.lock().unwrap();
        map.insert("client-a".to_string(), (yesterday, 9500));
    }

    // Recording usage today should reset the counter (day rollover)
    state.record_budget_usage("client-a", 50).await;

    let map = state.budget_usage.lock().unwrap();
    let (day, used) = map.get("client-a").unwrap();
    let today = AppState::now_epoch() / 86400;
    assert_eq!(*day, today, "day should be today after rollover");
    assert_eq!(*used, 50, "usage should be 50 (reset, not 9550)");
}

#[tokio::test]
async fn budget_check_respects_day_boundary() {
    let mut budgets = HashMap::new();
    budgets.insert("client-a".to_string(), 1000u64);
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint("a", "sk-ant-api-x")],
        client_budgets: budgets,
        ..test_state_base()
    });

    // Pre-populate with yesterday's exhausted budget
    let yesterday = AppState::now_epoch() / 86400 - 1;
    {
        let mut map = state.budget_usage.lock().unwrap();
        map.insert("client-a".to_string(), (yesterday, 5000));
    }

    // Budget check for today should pass — yesterday's usage doesn't count
    assert!(
        state.check_budget("client-a").await.is_ok(),
        "yesterday's exhausted budget should not block today"
    );
}

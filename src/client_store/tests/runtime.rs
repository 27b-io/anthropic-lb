use super::*;
use axum::extract::Query;

// ── The reader running: polls, the listener, resume, logs, metrics ───

/// One poll a mock store saw: its headers and its query.
type Poll = (hyper::HeaderMap, HashMap<String, String>);

/// What a mock store serves, and the polls it saw.
#[derive(Clone, Default)]
struct MockStore {
    /// The document text and the restrictions body it holds.
    content: Arc<Mutex<(Option<String>, Option<String>)>>,
    polls: Arc<Mutex<Vec<Poll>>>,
    /// How long it takes to answer.
    delay: Duration,
}

impl MockStore {
    /// Serve on a free port; returns the poll URL.
    async fn serve(&self) -> String {
        let app = Router::new()
            .route("/_clients/poll", axum::routing::get(mock_store_poll))
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/_clients/poll")
    }

    fn hold(&self, document: Option<String>, body: Option<String>) {
        *self.content.lock().unwrap() = (document, body);
    }

    fn polls(&self) -> Vec<Poll> {
        self.polls.lock().unwrap().clone()
    }
}

/// Answer a poll the way docs/client-store.md says a store answers: the text
/// when the versions differ, the body when the tags do.
async fn mock_store_poll(
    State(store): State<MockStore>,
    headers: hyper::HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    store.polls.lock().unwrap().push((headers, query.clone()));
    tokio::time::sleep(store.delay).await;
    let (text, body) = store.content.lock().unwrap().clone();
    let document = match text {
        None => json!({"status": "ok", "version": 0}),
        Some(text) => {
            let mut part = doc_part(&text);
            if query.get("version") == Some(&part["version"].to_string()) {
                part.as_object_mut().unwrap().remove("text");
            }
            part
        }
    };
    let restrictions = match body {
        None => unavailable(),
        Some(body) => {
            let mut part = restrictions_part(&body);
            if query.get("tag").map(String::as_str) == part["tag"].as_str() {
                part.as_object_mut().unwrap().remove("body");
            }
            part
        }
    };
    axum::Json(json!({"document": document, "restrictions": restrictions})).into_response()
}

/// A URL nothing listens on.
async fn closed_poll_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{addr}/_clients/poll")
}

// ── The poll ─────────────────────────────────────────────────────────

/// Polls carry a User-Agent (an edge that challenges requests without one
/// would otherwise refuse them), the reader token, and the reader's report.
#[tokio::test]
async fn the_poll_sends_a_user_agent_its_token_and_its_report() {
    let store = MockStore::default();
    let doc = document(wall_ms(), vec![client("acme", &test_key("acme"))], &[]);
    let restrictions = body(true, &[("gone", Disabled, now_us() - 1_000)], &[digest(4)]);
    store.hold(Some(doc.clone()), Some(restrictions.clone()));
    let url = store.serve().await;
    // The fixture's config has no token, so the boot check passes over plain
    // http; the reader is then built with one, to see it sent.
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&[]), &url, None);
    let config = ClientStoreConfig {
        poll_url: url.clone(),
        token: Some("reader-token".to_string()),
    };
    let reader = ClientStore::new(
        &config,
        &inst.state.current_client_policy(),
        vec!["acct-a".to_string()],
        inst._dir.as_ref().unwrap().path().join("other.json"),
    )
    .unwrap();

    assert!(reader.poll(&inst.state).await, "the store answered");
    assert!(reader.poll(&inst.state).await);
    let polls = store.polls();
    assert_eq!(polls.len(), 2);
    let (headers, first) = &polls[0];
    assert_eq!(
        headers["user-agent"],
        concat!("anthropic-lb/", env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(headers["authorization"], "Bearer reader-token");
    assert_eq!(first["version"], "0");
    assert_eq!(first.get("tag"), None);
    assert_eq!(first.get("digest"), None);
    assert_eq!(first["records_version"], "0");
    assert_eq!(
        first["set_hash"],
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(first["set_complete"], "false");

    // The second poll reports what the first installed.
    let (_, second) = &polls[1];
    let held = reader.lock_reader().set.clone();
    assert_eq!(
        second["version"],
        serde_json::from_str::<Value>(&doc).unwrap()["version"].to_string()
    );
    assert_eq!(second["digest"], digest_hex(&sha256(doc.as_bytes())));
    assert_eq!(second["tag"], digest_hex(&sha256(restrictions.as_bytes())));
    assert_eq!(second["set_hash"], digest_hex(&held.hash()));
    assert_eq!(second["set_complete"], "true");
    assert!(held.complete);
}

#[tokio::test]
async fn a_poll_answered_with_an_error_status_keeps_what_is_held() {
    let url = closed_poll_url().await;
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&[]), &url, None);
    assert!(!inst.store.poll(&inst.state).await);
    let app = Router::new().route(
        "/_clients/poll",
        axum::routing::get(|| async { StatusCode::UNAUTHORIZED }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let inst = Instance::new(
        "http://127.0.0.1:9",
        file_clients(&[]),
        &format!("http://{addr}/_clients/poll"),
        None,
    );
    assert!(!inst.store.poll(&inst.state).await);
    assert!(!inst.metrics().complete);
}

// ── The listener ─────────────────────────────────────────────────────

#[tokio::test]
async fn the_listener_waits_for_the_first_answered_poll() {
    let store = MockStore {
        delay: Duration::from_millis(400),
        ..Default::default()
    };
    store.hold(
        Some(document(wall_ms(), vec![], &[])),
        Some(body(true, &[], &[])),
    );
    let url = store.serve().await;
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&[]), &url, None);
    let started = Instant::now();
    await_first_poll(ClientStore::spawn(&inst.state)).await;
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(400), "waited {waited:?}");
    assert!(waited < FIRST_POLL_WAIT, "waited {waited:?}");
    assert!(
        inst.metrics().loaded,
        "the request that woke it meets a loaded instance"
    );
}

#[tokio::test]
async fn the_listener_opens_after_3_seconds_with_the_store_down() {
    let url = closed_poll_url().await;
    let inst = Instance::new(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        &url,
        None,
    );
    let started = Instant::now();
    await_first_poll(ClientStore::spawn(&inst.state)).await;
    let waited = started.elapsed();
    assert!(waited >= FIRST_POLL_WAIT, "waited {waited:?}");
    assert!(
        waited < FIRST_POLL_WAIT + Duration::from_secs(1),
        "waited {waited:?}"
    );
    // Open, and closed to every ordinary client.
    assert!(!inst.metrics().loaded);
    assert_eq!(inst.auth(&test_key("file-only")), Err(503));
    assert_eq!(inst.auth(&test_key("ops")), Ok("ops".to_string()));
}

#[tokio::test]
async fn without_a_client_store_the_listener_does_not_wait() {
    let started = Instant::now();
    await_first_poll(None).await;
    assert!(started.elapsed() < Duration::from_millis(100));
}

// ── Resume ───────────────────────────────────────────────────────────

/// What is held, saved beside the state file at every change, with the
/// set's completeness mark.
#[tokio::test]
async fn every_change_writes_the_resume_copy_with_its_completeness_mark() {
    let inst = Instance::offline(&[]);
    let path = inst
        ._dir
        .as_ref()
        .unwrap()
        .path()
        .join("config.client-store.json");
    let doc = document(
        wall_ms(),
        vec![client("acme", &test_key("acme"))],
        &[digest(2)],
    );
    inst.answer_and_save(answer(
        doc_part(&doc),
        restrictions_part(&body(false, &[("a", Disabled, now_us() - 1)], &[digest(1)])),
    ))
    .await;
    let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved["document"], doc);
    assert_eq!(saved["complete"], false);
    assert_eq!(saved["records"][0]["name"], "a");
    assert_eq!(saved["records"][0]["state"], "disabled");
    assert_eq!(
        saved["revoked_digests"],
        json!([digest_hex(&digest(1)), digest_hex(&digest(2))])
    );

    inst.answer_and_save(answer(
        doc_part_held(&doc),
        restrictions_part(&body(true, &[], &[])),
    ))
    .await;
    let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(saved["complete"], true);
}

/// A process restarted beside its resume copy resumes the document, the
/// revocation union and the restrictions, and counts as loaded only when the
/// saved set is complete.
#[tokio::test]
async fn a_resume_restores_what_was_held_and_is_loaded_only_with_a_complete_set() {
    let dir = tempfile::tempdir().unwrap();
    let clients = || file_clients(&["file-only", "other"]);
    let poll_url = "https://store.invalid/_clients/poll";
    let doc = document(
        wall_ms(),
        vec![
            client("acme", &test_key("acme")),
            client_with(
                "beta",
                &[&test_key("beta-1"), &test_key("beta-2")],
                json!({}),
            ),
        ],
        &[],
    );
    let partial = body(
        false,
        &[("file-only", Disabled, now_us() - 1)],
        &[key_digest(&test_key("beta-1"))],
    );

    // Before the restart: a document and a partial set.
    let before = Instance::in_dir("http://127.0.0.1:9", clients(), poll_url, None, dir.path());
    before
        .answer_and_save(answer(doc_part(&doc), restrictions_part(&partial)))
        .await;
    drop(before);

    let after = Instance::in_dir("http://127.0.0.1:9", clients(), poll_url, None, dir.path());
    after.store.resume(&after.state).await;
    assert!(
        after.metrics().document.is_some(),
        "the document is resumed"
    );
    assert!(!after.metrics().loaded, "a partial set never loads");
    assert_eq!(
        after.auth(&test_key("file-only")),
        Err(403),
        "its disable is resumed"
    );
    assert_eq!(after.auth(&test_key("other")), Err(503));
    assert_eq!(after.auth(&test_key("acme")), Err(503));

    // A complete set saved: the restart is loaded at once.
    after
        .answer_and_save(answer(
            doc_part_held(&doc),
            restrictions_part(&body(
                true,
                &[("file-only", Disabled, now_us() - 1)],
                &[key_digest(&test_key("beta-1"))],
            )),
        ))
        .await;
    drop(after);
    let again = Instance::in_dir("http://127.0.0.1:9", clients(), poll_url, None, dir.path());
    again.store.resume(&again.state).await;
    assert!(again.metrics().loaded);
    assert_eq!(again.auth(&test_key("acme")), Ok("acme".to_string()));
    assert_eq!(again.auth(&test_key("other")), Ok("other".to_string()));
    assert_eq!(again.auth(&test_key("file-only")), Err(403));
    assert_eq!(
        again.auth(&test_key("beta-1")),
        Err(401),
        "the revocation union is resumed"
    );
    assert_eq!(again.auth(&test_key("beta-2")), Ok("beta".to_string()));
}

#[tokio::test]
async fn a_resume_copy_that_fails_its_checks_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.client-store.json");
    let poll_url = "https://store.invalid/_clients/poll";
    for copy in [
        "not json".to_string(),
        json!({"document": null, "complete": true, "records": [{"name": "_x", "state": "disabled", "version": 1}], "revoked_digests": []}).to_string(),
        json!({"document": null, "complete": true, "records": [], "revoked_digests": ["nope"]}).to_string(),
    ] {
        std::fs::write(&path, &copy).unwrap();
        let inst = Instance::in_dir("http://127.0.0.1:9", file_clients(&["file-only"]), poll_url, None, dir.path());
        inst.store.resume(&inst.state).await;
        assert!(!inst.metrics().complete, "{copy}");
        assert_eq!(inst.auth(&test_key("file-only")), Err(503), "{copy}");
    }
    // A document that fails its checks is dropped; the set still resumes.
    let bad_doc = json!({"document": "{\"schema\":2}", "complete": true, "records": [], "revoked_digests": []});
    std::fs::write(&path, bad_doc.to_string()).unwrap();
    let inst = Instance::in_dir(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        poll_url,
        None,
        dir.path(),
    );
    inst.store.resume(&inst.state).await;
    assert!(inst.metrics().complete && inst.metrics().document.is_none());
    assert_eq!(
        inst.auth(&test_key("file-only")),
        Ok("file-only".to_string())
    );
}

// ── Logs ─────────────────────────────────────────────────────────────

/// The captured log lines that carry `marker`.
fn log_lines(marker: &str) -> Vec<String> {
    let buf = log_capture_buf();
    let text = String::from_utf8_lossy(&buf.lock().unwrap()).to_string();
    text.lines()
        .filter(|l| l.contains(marker))
        .map(str::to_string)
        .collect()
}

/// The value of `field=` on a log line.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    let start = line
        .find(&format!(" {name}="))
        .unwrap_or_else(|| panic!("no {name} in {line}"))
        + name.len()
        + 2;
    line[start..].split(' ').next().unwrap()
}

/// Why a line was logged: a string field, so it is quoted.
fn reason(line: &str) -> &str {
    field(line, "reason").trim_matches('"')
}

/// Each set goes out in lines of at most 16 KiB, the container runtime's
/// line limit, each carrying the hash of the whole set, and the chunks
/// rebuild the set text byte for byte.
#[test]
fn the_restrictions_are_logged_in_chunks_of_at_most_16_kib_carrying_the_set_hash() {
    use base64::Engine;
    log_capture_buf();
    let mut set = RestrictionSet {
        complete: true,
        ..Default::default()
    };
    for i in 0..1024u32 {
        set.records.insert(
            format!("chunk-test-{i:04}"),
            Record {
                state: Disabled,
                version: NOW_US + u64::from(i),
            },
        );
    }
    for i in 0..4096u32 {
        set.revoked_digests.insert(sha256(&i.to_be_bytes()));
    }
    let text = set.text();
    let hash = digest_hex(&sha256(text.as_bytes()));
    log_restrictions(&set, "change");

    let lines = log_lines(&hash);
    let chunks = text.len().div_ceil(LOG_CHUNK_BYTES);
    assert!(chunks > 30, "the test set spans many lines");
    assert_eq!(lines.len(), chunks);
    let mut rebuilt = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        assert!(
            line.len() <= 16 * 1024,
            "line {} is {} bytes",
            i + 1,
            line.len()
        );
        assert_eq!(field(line, "set_hash"), hash);
        assert_eq!(field(line, "complete"), "true");
        assert_eq!(field(line, "chunk"), (i + 1).to_string());
        assert_eq!(field(line, "chunks"), chunks.to_string());
        rebuilt.extend(
            base64::engine::general_purpose::STANDARD
                .decode(field(line, "data"))
                .unwrap(),
        );
    }
    assert_eq!(rebuilt, text.as_bytes());
    assert_eq!(digest_hex(&sha256(&rebuilt)), hash);
}

/// The restrictions are logged at every change, and once a day without one.
#[tokio::test]
async fn the_restrictions_are_logged_on_every_change_and_once_a_day() {
    log_capture_buf();
    let store = MockStore::default();
    let marker = "log-every-change";
    let first = body(true, &[(marker, Disabled, now_us() - 1)], &[]);
    store.hold(None, Some(first.clone()));
    let url = store.serve().await;
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&[]), &url, None);

    assert!(inst.store.poll(&inst.state).await);
    let set_one = digest_hex(&inst.store.lock_reader().set.hash());
    let changes = |hash: &str| {
        log_lines(hash)
            .iter()
            .filter(|l| reason(l) == "change")
            .count()
    };
    assert_eq!(changes(&set_one), 1);
    // No change, no line.
    assert!(inst.store.poll(&inst.state).await);
    assert_eq!(changes(&set_one), 1);

    // A change logs the new set.
    store.hold(None, Some(body(true, &[(marker, Enabled, now_us())], &[])));
    assert!(inst.store.poll(&inst.state).await);
    let set_two = digest_hex(&inst.store.lock_reader().set.hash());
    assert_ne!(set_one, set_two);
    assert_eq!(changes(&set_two), 1);

    // Once a day, unchanged.
    let daily = |hash: &str| {
        log_lines(hash)
            .iter()
            .filter(|l| reason(l) == "daily")
            .count()
    };
    let start = Instant::now();
    let mut last = start;
    inst.store
        .log_daily(&mut last, start + Duration::from_secs(60));
    assert_eq!(daily(&set_two), 0);
    inst.store.log_daily(&mut last, start + FULL_LOG_INTERVAL);
    assert_eq!(daily(&set_two), 1);
    inst.store.log_daily(
        &mut last,
        start + FULL_LOG_INTERVAL + Duration::from_secs(60),
    );
    assert_eq!(daily(&set_two), 1, "the next is a day after this one");
}

/// Every install logs the exact document bytes, with its version and digest.
#[tokio::test]
async fn an_install_logs_the_exact_document_bytes() {
    use base64::Engine;
    log_capture_buf();
    let doc = document(
        wall_ms(),
        vec![client("install-log-test", &test_key("install-log-test"))],
        &[],
    );
    let digest = digest_hex(&sha256(doc.as_bytes()));
    let store = MockStore::default();
    store.hold(Some(doc.clone()), None);
    let url = store.serve().await;
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&[]), &url, None);
    assert!(inst.store.poll(&inst.state).await);
    let lines = log_lines(&digest);
    assert_eq!(lines.len(), 1);
    assert_eq!(reason(&lines[0]), "install");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(field(&lines[0], "data"))
        .unwrap();
    assert_eq!(bytes, doc.as_bytes());
}

// ── Metrics ──────────────────────────────────────────────────────────

/// The value of the one series of `family` with `labels`.
fn series(body: &str, family: &str, labels: &str) -> Option<String> {
    let prefix = if labels.is_empty() {
        format!("{family} ")
    } else {
        format!("{family}{{{labels}}} ")
    };
    body.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .map(str::to_string)
}

fn count_series(body: &str, family: &str) -> usize {
    body.lines()
        .filter(|l| l.starts_with(&format!("{family}{{")) || l.starts_with(&format!("{family} ")))
        .count()
}

#[tokio::test]
async fn the_metrics_export_what_is_held_and_every_alert() {
    let (upstream, _handle) = spawn_mock_upstream().await;
    let inst = Instance::new(
        &upstream,
        file_clients(&["file-only", "both"]),
        "https://store.invalid/_clients/poll",
        None,
    );
    let doc = document(
        wall_ms(),
        vec![client("both", &test_key("both-store"))],
        &[],
    );
    let version = serde_json::from_str::<Value>(&doc).unwrap()["version"].to_string();
    // Seven disabled names that match no client, one that does.
    let mut records: Vec<(&str, RecordState, u64)> = ["u1", "u2", "u3", "u4", "u5", "u6", "u7"]
        .iter()
        .map(|n| (*n, Disabled, now_us() - 10))
        .collect();
    records.push(("file-only", Disabled, now_us() - 5));
    inst.answer(answer(
        doc_part(&doc),
        restrictions_part(&body_ahead(
            true,
            3,
            &records,
            &[key_digest(&test_key("ops"))],
        )),
    ));
    let addr = serve(build_router(inst.state.clone())).await;
    let metrics = Client::new()
        .get(format!("http://{addr}/metrics"))
        .header("x-api-key", test_key("ops"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let doc_labels = format!(
        "version=\"{version}\",digest=\"{}\"",
        digest_hex(&sha256(doc.as_bytes()))
    );
    assert_eq!(
        series(&metrics, "anthropic_client_store_document", &doc_labels).as_deref(),
        Some("1")
    );
    let records_version = inst.store.lock_reader().set.records_version(now_us());
    assert_eq!(
        series(
            &metrics,
            "anthropic_client_store_restrictions",
            &format!("records_version=\"{records_version}\"")
        )
        .as_deref(),
        Some("1")
    );
    assert_eq!(
        series(&metrics, "anthropic_client_store_loaded", "").as_deref(),
        Some("1")
    );
    assert_eq!(
        series(&metrics, "anthropic_client_store_restrictions_complete", "").as_deref(),
        Some("1")
    );
    assert_eq!(
        series(&metrics, "anthropic_client_store_part_full", "").as_deref(),
        Some("1")
    );
    // The store's count of enabled records left out as too far ahead.
    assert_eq!(
        series(&metrics, "anthropic_client_store_enabled_ahead", "").as_deref(),
        Some("3")
    );
    // Unmatched disabled names: a count, and at most five labels.
    assert_eq!(
        series(&metrics, "anthropic_client_store_unmatched_disabled", "").as_deref(),
        Some("7")
    );
    assert_eq!(
        count_series(&metrics, "anthropic_client_store_unmatched_disabled_name"),
        5
    );
    for name in ["u1", "u2", "u3", "u4", "u5"] {
        assert!(series(
            &metrics,
            "anthropic_client_store_unmatched_disabled_name",
            &format!("name=\"{name}\"")
        )
        .is_some());
    }
    assert_eq!(
        series(&metrics, "anthropic_client_store_revoked_outside_store", "").as_deref(),
        Some("1")
    );
    // A toml_client series for exactly each ordinary config-file client.
    assert_eq!(
        count_series(&metrics, "anthropic_client_store_toml_client"),
        2
    );
    for name in ["file-only", "both"] {
        assert_eq!(
            series(
                &metrics,
                "anthropic_client_store_toml_client",
                &format!("client=\"{name}\"")
            )
            .as_deref(),
            Some("1")
        );
    }
    // Every alert is exported, raised or not.
    assert_eq!(count_series(&metrics, "anthropic_client_store_alert"), 8);
    assert_eq!(
        series(
            &metrics,
            "anthropic_client_store_alert",
            "alert=\"restrictions_behind\""
        )
        .as_deref(),
        Some("0")
    );
    assert_eq!(
        series(
            &metrics,
            "anthropic_client_store_documents_installed_total",
            ""
        )
        .as_deref(),
        Some("1")
    );
    assert_eq!(
        series(
            &metrics,
            "anthropic_client_store_restrictions_changes_total",
            ""
        )
        .as_deref(),
        Some("1")
    );
    assert!(series(
        &metrics,
        "anthropic_client_store_last_ok_timestamp_seconds",
        "part=\"document\""
    )
    .is_some_and(|s| s != "0"));
    assert_eq!(
        count_series(&metrics, "anthropic_client_store_refusals_total"),
        3
    );
}

/// Without `[client_store]`, none of its series appear.
#[tokio::test]
async fn without_a_client_store_no_series_appear() {
    let (upstream, _handle) = spawn_mock_upstream().await;
    let (app, _state) = test_app(&upstream, None);
    let addr = serve(app).await;
    let metrics = Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!metrics.contains("anthropic_client_store_"));
}

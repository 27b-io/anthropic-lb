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

/// A local address nothing listens on.
async fn free_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// A URL nothing listens on.
async fn closed_poll_url() -> String {
    format!("http://{}/_clients/poll", free_addr().await)
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
        &inst.store.base,
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
        delay: Duration::from_secs(1),
        ..Default::default()
    };
    store.hold(
        Some(document(wall_ms(), vec![], &[])),
        Some(body(true, &[], &[])),
    );
    let url = store.serve().await;
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&[]), &url, None);
    let addr = free_addr().await;
    let started = Instant::now();
    let state = inst.state.clone();
    let booting = tokio::spawn(async move { bind_after_first_poll(&state, addr).await });

    // Nothing listens while the first poll is unanswered.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "the listener opened before the first answered poll"
    );
    let _listener = booting.await.unwrap().unwrap();
    let waited = started.elapsed();
    assert!(waited >= Duration::from_secs(1), "waited {waited:?}");
    assert!(waited < FIRST_POLL_WAIT, "waited {waited:?}");
    assert!(
        inst.metrics().loaded,
        "the request that woke it meets a loaded instance"
    );
    assert!(tokio::net::TcpStream::connect(addr).await.is_ok());
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
    let _listener = bind_after_first_poll(&inst.state, free_addr().await)
        .await
        .unwrap();
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
    let state = Arc::new(test_state_base());
    let started = Instant::now();
    let _listener = bind_after_first_poll(&state, free_addr().await)
        .await
        .unwrap();
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
    inst.answer(answer(
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

    inst.answer(answer(
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
        .answer(answer(doc_part(&doc), restrictions_part(&partial)))
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
        .answer(answer(
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

/// Holds up every save of the resume copy in a directory: the copy's sibling
/// becomes a FIFO, and opening a FIFO to write blocks until a reader opens
/// it. Linux only, where syncing a FIFO fails, so a save that `release` lets
/// through fails and removes the copy.
///
/// `release` blocks its thread until a save opens the FIFO, so a test that
/// uses it runs on a multi-thread runtime: a poll still fetching then goes
/// on, where one thread would wait forever. Dropping it releases any save
/// still waiting, so a failed assertion fails the test instead of hanging it
/// (the runtime waits for every blocking thread when it shuts down): opening
/// a FIFO to read and write never blocks on Linux. It then unlinks the FIFO,
/// so a later save writes a plain file.
#[cfg(target_os = "linux")]
struct HeldSaves(std::path::PathBuf);

#[cfg(target_os = "linux")]
impl HeldSaves {
    fn new(dir: &std::path::Path) -> Self {
        let fifo = dir.join("config.client-store.json.tmp");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success(), "mkfifo failed");
        Self(fifo)
    }

    /// Let the saves waiting now through, and return what they wrote.
    fn release(&self) -> Vec<u8> {
        use std::io::Read;
        let mut written = Vec::new();
        std::fs::File::open(&self.0)
            .unwrap()
            .read_to_end(&mut written)
            .unwrap();
        written
    }
}

#[cfg(target_os = "linux")]
impl Drop for HeldSaves {
    fn drop(&mut self) {
        let _open = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.0);
        let _ = std::fs::remove_file(&self.0);
    }
}

/// One poll of `inst`'s store, run as a task.
#[cfg(target_os = "linux")]
fn spawn_poll(inst: &Instance) -> tokio::task::JoinHandle<bool> {
    let (state, store) = (inst.state.clone(), inst.store.clone());
    tokio::spawn(async move { store.poll(&state).await })
}

/// A restriction is enforced only once it is saved. While a poll's save of a
/// new disable is held up, the instance keeps enforcing exactly what its copy
/// on disk holds, so a crash at that moment loses nothing it enforced.
/// Enforcing first would leave a window in which a restart resumes the older
/// copy and serves the disabled name again.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_restriction_is_enforced_only_once_it_is_saved() {
    let store = MockStore::default();
    store.hold(None, Some(body(true, &[], &[])));
    let url = store.serve().await;
    let inst = Instance::new(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        &url,
        None,
    );
    let path = inst
        ._dir
        .as_ref()
        .unwrap()
        .path()
        .join("config.client-store.json");
    assert!(inst.store.poll(&inst.state).await, "the store answered");
    assert_eq!(
        inst.auth(&test_key("file-only")),
        Ok("file-only".to_string())
    );
    let saved = std::fs::read(&path).unwrap();

    let held = HeldSaves::new(inst._dir.as_ref().unwrap().path());
    store.hold(
        None,
        Some(body(true, &[("file-only", Disabled, now_us() - 1)], &[])),
    );
    let polling = spawn_poll(&inst);

    // The save is held: the disable is neither enforced nor reported held,
    // and the copy on disk is exactly what is enforced.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!polling.is_finished(), "the save is held up");
    assert_eq!(
        inst.auth(&test_key("file-only")),
        Ok("file-only".to_string()),
        "enforced before it was saved"
    );
    assert!(
        inst.store.lock_reader().set.records.is_empty(),
        "reported held before it was saved"
    );
    assert_eq!(std::fs::read(&path).unwrap(), saved);

    // Released, the save writes a copy that holds the disable, then fails (a
    // FIFO cannot be synced) and removes the old copy. Only then is the
    // disable enforced, so a restart now waits for the store instead of
    // resuming a copy without it.
    let copy: Value = serde_json::from_slice(&held.release()).expect("the save wrote a copy");
    assert_eq!(copy["records"][0]["name"], "file-only");
    assert_eq!(copy["records"][0]["state"], "disabled");
    assert!(polling.await.unwrap(), "the store answered");
    assert_eq!(inst.auth(&test_key("file-only")), Err(403));
    assert!(!path.exists(), "the copy that lacks the disable is gone");
}

/// Polls run one at a time. A poll builds the next reader on a copy and
/// saves it before installing it, so a second poll that ran meanwhile would
/// build on what the first was about to replace, and whichever installed
/// last would drop the other's merge.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn polls_run_one_at_a_time_so_no_merge_is_lost() {
    let store = MockStore::default();
    store.hold(None, Some(body(true, &[], &[])));
    let url = store.serve().await;
    let inst = Instance::new("http://127.0.0.1:9", file_clients(&["a", "b"]), &url, None);
    let dir = inst._dir.as_ref().unwrap().path().to_path_buf();
    assert!(inst.store.poll(&inst.state).await, "the store answered");

    // The first poll merges a disable of `a`, and its save is held up while
    // a second poll starts on a part that disables `b` alone.
    let held = HeldSaves::new(&dir);
    store.hold(
        None,
        Some(body(true, &[("a", Disabled, now_us() - 1)], &[])),
    );
    let first = spawn_poll(&inst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    store.hold(
        None,
        Some(body(true, &[("b", Disabled, now_us() - 1)], &[])),
    );
    let second = spawn_poll(&inst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!first.is_finished() && !second.is_finished());

    held.release();
    drop(held);
    assert!(first.await.unwrap(), "the store answered");
    assert!(second.await.unwrap(), "the store answered");
    assert_eq!(
        inst.auth(&test_key("a")),
        Err(403),
        "the first merge was lost"
    );
    assert_eq!(
        inst.auth(&test_key("b")),
        Err(403),
        "the second merge was lost"
    );
}

/// A save that failed is owed until one lands, and is retried even while
/// the store is away, so the copy catches up with what is enforced as soon
/// as the disk allows.
#[tokio::test]
async fn a_failed_save_is_retried_while_the_store_is_away() {
    let url = closed_poll_url().await;
    let inst = Instance::new(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        &url,
        None,
    );
    let dir = inst._dir.as_ref().unwrap().path().to_path_buf();
    let path = dir.join("config.client-store.json");
    let blocker = dir.join("config.client-store.json.tmp");
    std::fs::create_dir(&blocker).unwrap();
    inst.answer(answer(
        unavailable(),
        restrictions_part(&body(true, &[("file-only", Disabled, now_us() - 1)], &[])),
    ))
    .await;
    assert!(!path.exists());
    assert!(inst.alert("resume_unsaved"));

    std::fs::remove_dir(&blocker).unwrap();
    assert!(!inst.store.poll(&inst.state).await, "the store is away");
    assert!(path.exists(), "the owed save landed");
    assert!(!inst.alert("resume_unsaved"));
    let restarted = Instance::in_dir(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        &url,
        None,
        &dir,
    );
    restarted.store.resume(&restarted.state).await;
    assert_eq!(restarted.auth(&test_key("file-only")), Err(403));
}

/// A save that fails syncs the directory after it removes the last copy, as
/// a save that lands does after its rename: a removal survives a power cut
/// only once its directory is synced. Here the directory cannot be opened to
/// sync, so the removal is reported failed.
#[cfg(unix)]
#[test]
fn a_failed_save_syncs_the_removal_of_the_last_copy() {
    use std::os::unix::fs::PermissionsExt;
    /// Gives the directory its permissions back, so it can be cleaned up.
    struct Restore<'a>(&'a std::path::Path);
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o700));
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.client-store.json");
    // Write and search but not read: entries can be made and removed, and
    // the directory cannot be opened to sync.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
    let _restore = Restore(dir.path());
    if std::fs::File::open(dir.path()).is_ok() {
        eprintln!("skipped: this user can open any directory");
        return;
    }
    let (_, removal) =
        save_copy(&path, Ok(b"{}".to_vec())).expect_err("the save cannot sync its directory");
    assert!(removal.is_some(), "the removal was not synced");
    assert!(!path.exists(), "the copy is removed");
}

/// A save that fails removes the last copy, so a restart in between waits
/// for the store instead of resuming a copy that lacks what was held. The
/// next poll saves again, change or not.
#[tokio::test]
async fn a_failed_save_removes_the_stale_copy_and_is_retried() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.client-store.json");
    let poll_url = "https://store.invalid/_clients/poll";
    let clients = || file_clients(&["file-only"]);
    let inst = Instance::in_dir("http://127.0.0.1:9", clients(), poll_url, None, dir.path());
    inst.answer(answer(
        unavailable(),
        restrictions_part(&body(true, &[], &[])),
    ))
    .await;
    assert!(path.exists());

    // The next save cannot land: its sibling path is taken.
    let blocker = dir.path().join("config.client-store.json.tmp");
    std::fs::create_dir(&blocker).unwrap();
    let disabled = restrictions_part(&body(true, &[("file-only", Disabled, now_us() - 1)], &[]));
    inst.answer(answer(unavailable(), disabled.clone())).await;
    assert_eq!(
        inst.auth(&test_key("file-only")),
        Err(403),
        "held in memory"
    );
    assert!(!path.exists(), "the copy that lacks the disable is gone");
    // A restart now resumes nothing, and waits for the store.
    let restarted = Instance::in_dir("http://127.0.0.1:9", clients(), poll_url, None, dir.path());
    restarted.store.resume(&restarted.state).await;
    assert_eq!(restarted.auth(&test_key("file-only")), Err(503));

    // The path frees up: the next poll saves, though nothing changed.
    std::fs::remove_dir(&blocker).unwrap();
    inst.answer(answer(unavailable(), disabled)).await;
    assert!(path.exists());
    let restarted = Instance::in_dir("http://127.0.0.1:9", clients(), poll_url, None, dir.path());
    restarted.store.resume(&restarted.state).await;
    assert_eq!(
        restarted.auth(&test_key("file-only")),
        Err(403),
        "the disable survives the restart"
    );
}

/// A save whose removal cannot be synced is logged as an error, even when
/// an earlier, benign failure means the warning for this run of failures is
/// already out: the log is rate-limited only for failures that removed the
/// last copy durably.
#[cfg(unix)]
#[tokio::test]
async fn an_unsynced_removal_is_logged_after_a_benign_failure() {
    use std::os::unix::fs::PermissionsExt;
    /// Gives the directory its permissions back, so it can be cleaned up.
    struct Restore<'a>(&'a std::path::Path);
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o700));
        }
    }

    log_capture_buf();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.client-store.json");
    let marker = path.display().to_string();
    let poll_url = "https://store.invalid/_clients/poll";
    let inst = Instance::in_dir(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        poll_url,
        None,
        dir.path(),
    );
    let disabled = restrictions_part(&body(true, &[("file-only", Disabled, now_us() - 1)], &[]));

    // Failure 1 is benign: the save cannot land, and the removal is synced.
    std::fs::create_dir(dir.path().join("config.client-store.json.tmp")).unwrap();
    inst.answer(answer(unavailable(), disabled.clone())).await;
    assert!(inst.store.resume_unsaved());
    let unsafe_lines = || {
        log_lines(&marker)
            .into_iter()
            .filter(|l| l.contains("ERROR") && l.contains("durably remove"))
            .count()
    };
    assert_eq!(unsafe_lines(), 0);

    // Failure 2 cannot sync its removal: the directory cannot be opened.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
    let _restore = Restore(dir.path());
    if std::fs::File::open(dir.path()).is_ok() {
        eprintln!("skipped: this user can open any directory");
        return;
    }
    inst.answer(answer(unavailable(), disabled)).await;
    assert_eq!(unsafe_lines(), 1, "the unsafe second failure is logged");
}

/// A save whose removal does not land is logged at the first of a run of
/// them, then once every `FAILURE_LOG_EVERY`, so a directory that stays
/// broken is not logged once a poll. A benign failure or a landed save ends
/// the run, and the next unsafe failure is logged at once. The copy's own
/// path is a directory here, so no user can remove it.
#[tokio::test]
async fn repeated_unsafe_failures_are_logged_once_every_failure_log_every() {
    log_capture_buf();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.client-store.json");
    let tmp = dir.path().join("config.client-store.json.tmp");
    let marker = path.display().to_string();
    let poll_url = "https://store.invalid/_clients/poll";
    let inst = Instance::in_dir(
        "http://127.0.0.1:9",
        file_clients(&["file-only"]),
        poll_url,
        None,
        dir.path(),
    );
    // A fresh version each call, so every answer changes the set.
    let disabled = || restrictions_part(&body(true, &[("file-only", Disabled, now_us() - 1)], &[]));
    let unsafe_lines = || {
        log_lines(&marker)
            .into_iter()
            .filter(|l| l.contains("ERROR") && l.contains("durably remove"))
            .count()
    };

    // The save cannot land, and the last copy cannot be removed.
    std::fs::create_dir(&tmp).unwrap();
    std::fs::create_dir(&path).unwrap();
    for _ in 0..2 * FAILURE_LOG_EVERY {
        inst.answer(answer(unavailable(), disabled())).await;
    }
    assert!(inst.store.resume_unsaved(), "resume_unsaved stays raised");
    assert_eq!(
        unsafe_lines(),
        3,
        "unsafe failures 1, 60 and 120 are logged"
    );

    // A benign failure ends the run: the next unsafe failure is logged.
    std::fs::remove_dir(&path).unwrap();
    inst.answer(answer(unavailable(), disabled())).await;
    assert_eq!(unsafe_lines(), 3, "the benign failure is no unsafe line");
    std::fs::create_dir(&path).unwrap();
    inst.answer(answer(unavailable(), disabled())).await;
    assert_eq!(unsafe_lines(), 4, "logged at once after a benign failure");
    inst.answer(answer(unavailable(), disabled())).await;
    assert_eq!(unsafe_lines(), 4, "the repeat is rate-limited");

    // A landed save ends the run too.
    std::fs::remove_dir(&path).unwrap();
    std::fs::remove_dir(&tmp).unwrap();
    inst.answer(answer(unavailable(), disabled())).await;
    assert!(!inst.store.resume_unsaved(), "the save landed");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    std::fs::create_dir(&tmp).unwrap();
    inst.answer(answer(unavailable(), disabled())).await;
    assert!(inst.store.resume_unsaved());
    assert_eq!(unsafe_lines(), 5, "logged at once after a landed save");
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
    ))
    .await;
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
    // The set hash beside it is the one a poll reports. Whoever reads both it and the set hash of
    // the part a store serves can tell whether a write has reached this instance, so it is checked
    // against the set text of what the part sent (docs/client-store.md, "The set hash"), built here
    // by hand rather than by the reader.
    let records_version = inst.store.lock_reader().set.records_version(now_us());
    let mut lines: Vec<String> = records
        .iter()
        .map(|(name, _, version)| format!("r {name} disabled {version}\n"))
        .collect();
    lines.sort();
    lines.push(format!("d {}\n", digest_hex(&key_digest(&test_key("ops")))));
    let set_hash = digest_hex(&sha256(lines.concat().as_bytes()));
    assert_eq!(
        series(
            &metrics,
            "anthropic_client_store_restrictions",
            &format!("records_version=\"{records_version}\",set_hash=\"{set_hash}\"")
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
    // Every alert is exported, raised or not: a store that left records out
    // as ahead, and a revoked digest it cannot revoke, each raise one.
    assert_eq!(count_series(&metrics, "anthropic_client_store_alert"), 12);
    let alert = |name: &str| {
        series(
            &metrics,
            "anthropic_client_store_alert",
            &format!("alert=\"{name}\""),
        )
    };
    assert_eq!(alert("enabled_ahead").as_deref(), Some("1"));
    assert_eq!(alert("revoked_outside_store").as_deref(), Some("1"));
    assert_eq!(alert("quarantined").as_deref(), Some("0"));
    assert_eq!(alert("resume_unsaved").as_deref(), Some("0"));
    assert_eq!(alert("restrictions_behind").as_deref(), Some("0"));
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

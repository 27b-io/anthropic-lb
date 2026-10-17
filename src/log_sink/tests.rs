use super::*;
use std::collections::VecDeque;
use tracing_subscriber::prelude::*;

// ── Collectors ──────────────────────────────────────────────────────

/// One POST a collector received.
struct Post {
    at: Instant,
    content_type: Option<String>,
    body: String,
}

impl Post {
    fn rows(&self) -> Vec<serde_json::Value> {
        self.body
            .lines()
            .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON: {l}: {e}")))
            .collect()
    }
}

type Posts = Arc<Mutex<Vec<Post>>>;

/// A collector on 127.0.0.1 that records every POST and answers each with the
/// next scripted status, then 200.
async fn spawn_collector(script: Vec<StatusCode>) -> (String, Posts) {
    let posts: Posts = Default::default();
    let script = Arc::new(Mutex::new(VecDeque::from(script)));
    let recorded = posts.clone();
    let app = Router::new().fallback(any(move |req: Request<Body>| {
        let recorded = recorded.clone();
        let script = script.clone();
        async move {
            let content_type = req
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            recorded.lock().unwrap().push(Post {
                at: Instant::now(),
                content_type,
                body: String::from_utf8(body.to_vec()).unwrap(),
            });
            let status = script.lock().unwrap().pop_front();
            status.unwrap_or(StatusCode::OK)
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/insert/jsonline"), posts)
}

/// A collector that accepts connections and never answers. Returns its URL
/// and how many connections it has accepted.
async fn spawn_hung_collector() -> (String, Arc<AtomicUsize>) {
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            count.fetch_add(1, Ordering::SeqCst);
            held.push(socket);
        }
    });
    (format!("http://{addr}/insert/jsonline"), accepted)
}

fn sink_at(url: &str, capacity: usize, interval: Duration) -> Arc<LogSink> {
    sink_with(url, capacity, QUEUE_BYTES, interval)
}

fn sink_with(
    url: &str,
    capacity: usize,
    max_queued_bytes: u64,
    interval: Duration,
) -> Arc<LogSink> {
    LogSink::start_with(
        &LogSinkConfig {
            url: url.to_string(),
        },
        capacity,
        max_queued_bytes,
        interval,
    )
    .unwrap()
}

fn pending(sink: &LogSink) -> u64 {
    sink.pending.load(Ordering::Relaxed)
}

/// Poll `done` every 10 ms until it holds; fail after `within`.
async fn eventually(what: &str, within: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ── Delivery ────────────────────────────────────────────────────────

/// A lone line waits for company no longer than the flush interval, then
/// leaves as one JSON row: `_msg` the line exactly, `_time` its capture time.
/// The content type is checked exactly because a collector may treat any
/// other as a form and ingest nothing.
#[tokio::test]
async fn a_lone_line_leaves_within_the_flush_interval() {
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = LogSink::start(&LogSinkConfig { url }).unwrap();
    let line = r#"2026-10-17T00:00:00.000001Z  INFO anthropic_lb: proxied msg="a \"quoted\" tab	and \\ backslash""#;

    let before = now_rfc3339();
    let pushed = Instant::now();
    sink.push(format!("{line}\n").as_bytes());
    eventually("the POST", Duration::from_secs(5), || {
        posts.lock().unwrap().len() == 1
    })
    .await;

    let posts = posts.lock().unwrap();
    let post = &posts[0];
    let waited = post.at.duration_since(pushed);
    assert!(
        waited < Duration::from_millis(1500),
        "a lone line took {waited:?}"
    );
    assert_eq!(post.content_type.as_deref(), Some("application/x-ndjson"));
    assert!(post.body.ends_with('\n'), "rows are newline-terminated");
    let rows = post.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_msg"], line);
    // now_rfc3339 is fixed-width UTC, so string order is time order.
    let time = rows[0]["_time"].as_str().unwrap();
    let after = now_rfc3339();
    assert_eq!(
        time.len(),
        before.len(),
        "{time} is not the fmt layer's shape"
    );
    assert!(
        before.as_str() <= time && time <= after.as_str(),
        "{time} is not between {before} and {after}"
    );
}

/// The layer `main` installs ships plain text: with colour on (no `NO_COLOR`
/// in the test environment), a sink's copy would otherwise carry the level and
/// field-name escapes into every `_msg`.
#[tokio::test]
async fn the_sink_gets_plain_text_from_the_stdout_layer() {
    // With NO_COLOR set the default layer is plain anyway, and this would pass
    // without the code it guards.
    assert!(
        std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()),
        "unset NO_COLOR to run this test: it proves nothing with colour already off"
    );
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = sink_at(&url, 16, Duration::from_millis(20));
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(stdout_layer(Some(sink.clone()))),
    );
    // See requests_are_unaffected_by_a_hung_sink: a callsite's cached interest
    // can predate this scoped subscriber.
    tracing::callsite::rebuild_interest_cache();
    tracing::info!(host = "collector:9428", "ansi probe");

    eventually("the POST", Duration::from_secs(5), || {
        !posts.lock().unwrap().is_empty()
    })
    .await;
    let posts = posts.lock().unwrap();
    let msg = posts[0].rows()[0]["_msg"].as_str().unwrap().to_string();
    assert!(msg.contains("ansi probe"), "{msg}");
    assert!(msg.contains(r#"host="collector:9428""#), "{msg}");
    assert!(
        !msg.contains('\x1b'),
        "escape codes in the sink's copy: {msg:?}"
    );
}

/// One write is one event: its lines become one row each, CRLF and empty lines
/// stripped, invalid UTF-8 replaced, and every row carries the same `_time`.
#[tokio::test]
async fn a_multi_line_write_becomes_rows_sharing_one_time() {
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = sink_at(&url, 100, Duration::from_millis(50));

    sink.push(b"first\r\nsecond\n\n\xffthird");
    eventually("the rows", Duration::from_secs(5), || sink.sent() == 3).await;

    let posts = posts.lock().unwrap();
    assert_eq!(posts.len(), 1);
    let rows = posts[0].rows();
    let msgs: Vec<&str> = rows.iter().map(|r| r["_msg"].as_str().unwrap()).collect();
    assert_eq!(msgs, ["first", "second", "\u{FFFD}third"]);
    assert!(rows.iter().all(|r| r["_time"] == rows[0]["_time"]));
    assert_eq!(pending(&sink), 0);
}

/// Writes that arrive inside one flush interval share one POST, in order.
#[tokio::test]
async fn quick_writes_share_one_post() {
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = sink_at(&url, 100, Duration::from_millis(300));

    for i in 0..5 {
        sink.push(format!("line {i}\n").as_bytes());
    }
    eventually("the batch", Duration::from_secs(5), || sink.sent() == 5).await;

    let posts = posts.lock().unwrap();
    assert_eq!(posts.len(), 1, "five quick writes, one POST");
    let msgs: Vec<String> = posts[0]
        .rows()
        .iter()
        .map(|r| r["_msg"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(msgs, ["line 0", "line 1", "line 2", "line 3", "line 4"]);
}

/// A batch stops at MAX_BATCH_LINES lines or once it passes MAX_BATCH_BYTES,
/// whatever the interval: a backlog leaves in bounded POSTs.
#[tokio::test]
async fn a_batch_is_bounded_by_lines_and_bytes() {
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = sink_at(&url, 5_000, Duration::from_millis(100));

    for i in 0..2_500 {
        sink.push(format!("line {i}\n").as_bytes());
    }
    eventually("the line-bounded batches", Duration::from_secs(5), || {
        sink.sent() == 2_500
    })
    .await;
    let sizes: Vec<usize> = posts
        .lock()
        .unwrap()
        .iter()
        .map(|p| p.rows().len())
        .collect();
    assert_eq!(sizes, [1_000, 1_000, 500]);

    posts.lock().unwrap().clear();
    // Lines at the line cap: a batch fills to MAX_BATCH_BYTES, the line that
    // crosses it riding along, and the rest leave in the next one.
    let big = "x".repeat(MAX_LINE_BYTES);
    let row = serde_json::to_vec(&Line {
        time: now_rfc3339(),
        msg: big.clone(),
    })
    .unwrap()
    .len()
        + 1;
    let first = MAX_BATCH_BYTES.div_ceil(row);
    for _ in 0..first + 3 {
        sink.push(big.as_bytes());
    }
    eventually("the byte-bounded batches", Duration::from_secs(5), || {
        sink.sent() == 2_500 + first as u64 + 3
    })
    .await;
    let sizes: Vec<usize> = posts
        .lock()
        .unwrap()
        .iter()
        .map(|p| p.rows().len())
        .collect();
    assert_eq!(
        sizes,
        [first, 3],
        "the line that crosses the cap still rides"
    );
}

// ── Failure ─────────────────────────────────────────────────────────

/// Nothing listening: every line is counted dropped, none sent, and pending
/// settles back to zero (shutdown's drain does not wait on a dead collector).
#[tokio::test]
async fn a_down_sink_drops_and_settles() {
    // Bound but never listening: connections are refused, yet the port stays
    // reserved. A port merely released could be handed to a parallel test's
    // listener before the POST, which would then land in that test.
    let reserved = tokio::net::TcpSocket::new_v4().unwrap();
    reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = reserved.local_addr().unwrap();
    let sink = sink_at(
        &format!("http://{addr}/insert/jsonline"),
        100,
        Duration::from_millis(20),
    );

    for i in 0..3 {
        sink.push(format!("line {i}\n").as_bytes());
    }
    eventually("the drops", Duration::from_secs(5), || pending(&sink) == 0).await;

    assert_eq!(sink.dropped(), 3);
    assert_eq!(sink.sent(), 0);
}

/// Shutdown's drain gives up at its budget and reports what is still
/// undelivered, rather than waiting on a POST that never answers.
#[tokio::test]
async fn drain_stops_at_its_budget() {
    let (url, accepted) = spawn_hung_collector().await;
    let sink = sink_at(&url, 8, Duration::from_millis(20));

    sink.push(b"stuck\n");
    eventually("the hung POST", Duration::from_secs(5), || {
        accepted.load(Ordering::SeqCst) == 1
    })
    .await;
    let started = Instant::now();
    assert_eq!(sink.drain(Duration::from_millis(100)).await, 1);
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(1),
        "drain overran its budget: {took:?}"
    );
}

/// A line past `MAX_LINE_BYTES` leaves cut, marked with its full length, and
/// under the line limit a collector such as VictoriaLogs enforces.
#[tokio::test]
async fn an_oversized_line_is_cut_and_marked() {
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = sink_at(&url, 100, Duration::from_millis(20));
    // Control characters, the worst case for JSON escaping (6 bytes each):
    // the cut must leave even these under the collector's line limit.
    let huge = "\u{1}".repeat(5 << 20);

    sink.push(format!("model={huge}\n").as_bytes());
    eventually("the POST", Duration::from_secs(5), || {
        !posts.lock().unwrap().is_empty()
    })
    .await;
    let (msg, body_len) = {
        let posts = posts.lock().unwrap();
        let msg = posts[0].rows()[0]["_msg"].as_str().unwrap().to_string();
        (msg, posts[0].body.len())
    };
    let marker = format!(
        " …[log sink: cut from {} bytes]",
        huge.len() + "model=".len()
    );
    assert!(
        msg.ends_with(&marker),
        "unmarked: …{}",
        &msg[msg.len() - 60..]
    );
    assert_eq!(msg.len(), MAX_LINE_BYTES + marker.len());
    assert!(
        body_len < 256 << 10,
        "over VictoriaLogs' default line limit"
    );
    // Counted once the 200 is read, which can trail the collector's record.
    eventually("the line counted sent", Duration::from_secs(5), || {
        sink.sent() == 1
    })
    .await;
}

/// A cut that would split a multi-byte character backs off to the character's
/// start, so a valid line ends in whole characters, never U+FFFD.
#[test]
fn a_cut_never_splits_a_character() {
    // 'x' then 2-byte 'é's: the even MAX_LINE_BYTES offset lands mid-character.
    let raw = format!("x{}", "é".repeat(MAX_LINE_BYTES));
    let msg = message(raw.as_bytes());
    let (kept, _marker) = msg.split_once(" …[log sink: cut").unwrap();
    assert!(
        !kept.contains('\u{FFFD}'),
        "a split character became U+FFFD"
    );
    assert_eq!(kept.len(), MAX_LINE_BYTES - 1);
    assert!(kept.ends_with('é'));
}

/// Lines queued before the last handle goes are still posted: the queue keeps
/// what was sent before it closed, and the flush task drains it.
#[tokio::test]
async fn lines_queued_before_the_sink_goes_are_still_posted() {
    let (url, posts) = spawn_collector(vec![]).await;
    let sink = sink_at(&url, 2 * MAX_BATCH_LINES, Duration::from_millis(20));
    // More than one batch, so the drain has to outlive the first POST.
    let total = MAX_BATCH_LINES + 10;

    for i in 0..total {
        sink.push(format!("last words {i}\n").as_bytes());
    }
    drop(sink);
    let rows =
        |posts: &Posts| -> usize { posts.lock().unwrap().iter().map(|p| p.rows().len()).sum() };
    eventually("every queued line", Duration::from_secs(5), || {
        rows(&posts) == total
    })
    .await;
    let posts = posts.lock().unwrap();
    assert_eq!(posts.len(), 2, "two batches");
    assert_eq!(
        posts[1].rows().last().unwrap()["_msg"],
        format!("last words {}", total - 1)
    );
}

/// A malformed line past the cap is cut and marked like any other, and its
/// buffer is sized to the converted text, not the raw bytes.
#[test]
fn a_malformed_cut_line_is_sized_to_its_text() {
    let raw = vec![0xFFu8; MAX_LINE_BYTES + 10];
    let msg = message(&raw);
    assert!(msg.ends_with(&format!(" …[log sink: cut from {} bytes]", raw.len())));
    assert_eq!(msg.capacity(), msg.len(), "the cut line was over-allocated");
}

/// The queue is bounded in bytes as well as lines: while a POST hangs, lines
/// that would take it past `max_queued_bytes` are dropped at once, and the bytes a
/// batch takes out are released.
#[tokio::test]
async fn the_queue_is_bounded_in_bytes() {
    let (url, accepted) = spawn_hung_collector().await;
    let line = "x".repeat(MAX_LINE_BYTES);
    // Room for three such lines and their timestamps, not four.
    let budget = 4 * MAX_LINE_BYTES as u64 - 1;
    let sink = sink_with(&url, 1_000, budget, Duration::from_millis(20));

    sink.push(b"in flight\n");
    eventually("the hung POST", Duration::from_secs(5), || {
        accepted.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        sink.queued_bytes.load(Ordering::Relaxed),
        0,
        "a batch releases its bytes"
    );

    for _ in 0..10 {
        sink.push(format!("{line}\n").as_bytes());
    }
    assert_eq!(pending(&sink), 1 + 3);
    assert_eq!(sink.dropped(), 7);
    assert!(sink.queued_bytes.load(Ordering::Relaxed) <= budget);
}

/// A collector that accepts and never answers: while its POST hangs, pushes
/// return at once, fill the small queue, and the overflow is counted dropped.
#[tokio::test]
async fn a_hung_sink_never_blocks_a_push() {
    let (url, accepted) = spawn_hung_collector().await;
    let sink = sink_at(&url, 8, Duration::from_millis(20));

    sink.push(b"in flight\n");
    eventually("the hung POST", Duration::from_secs(5), || {
        accepted.load(Ordering::SeqCst) == 1
    })
    .await;

    let started = Instant::now();
    for i in 0..500 {
        sink.push(format!("line {i}\n").as_bytes());
    }
    let took = started.elapsed();
    assert!(took < Duration::from_millis(50), "500 pushes took {took:?}");
    // The in-flight line plus a full queue are pending; the rest dropped.
    assert_eq!(pending(&sink), 1 + 8);
    assert_eq!(sink.dropped(), 500 - 8);
    assert_eq!(sink.sent(), 0);
    // Each line the full queue refused gave its bytes back: only the 8 queued
    // lines are counted, or a long outage would leak the budget away for good.
    let queued: u64 = (0..8)
        .map(|i| (now_rfc3339().len() + format!("line {i}").len()) as u64)
        .sum();
    assert_eq!(sink.queued_bytes.load(Ordering::Relaxed), queued);
}

/// A 500 drops its batch, unretried; the next batch to get a 200 is sent.
#[tokio::test]
async fn a_failing_sink_recovers() {
    let (url, posts) = spawn_collector(vec![StatusCode::INTERNAL_SERVER_ERROR]).await;
    let sink = sink_at(&url, 100, Duration::from_millis(20));

    sink.push(b"lost\n");
    eventually("the failed batch", Duration::from_secs(5), || {
        pending(&sink) == 0
    })
    .await;
    assert_eq!((sink.sent(), sink.dropped()), (0, 1));

    sink.push(b"kept\n");
    eventually("the recovered batch", Duration::from_secs(5), || {
        pending(&sink) == 0
    })
    .await;
    assert_eq!((sink.sent(), sink.dropped()), (1, 1));

    let posts = posts.lock().unwrap();
    let msgs: Vec<String> = posts
        .iter()
        .flat_map(|p| p.rows())
        .map(|r| r["_msg"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(msgs, ["lost", "kept"], "the failed batch is not resent");
}

/// The flush task logs a change of health, never each failed batch, and the
/// recovery line counts every line dropped since the failure began.
#[test]
fn health_reports_transitions_only() {
    let mut health = Health::default();
    assert_eq!(health.observe(true, 0), None);
    assert_eq!(health.observe(false, 4), Some(Transition::Failing));
    assert_eq!(health.observe(false, 9), None);
    assert_eq!(
        health.observe(true, 20),
        Some(Transition::Recovered { dropped: 16 })
    );
    assert_eq!(health.observe(true, 20), None);
    assert_eq!(health.observe(false, 20), Some(Transition::Failing));
}

// ── Requests ────────────────────────────────────────────────────────

async fn send_message(client: &Client, addr: SocketAddr) -> reqwest::StatusCode {
    client
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap()
        .status()
}

/// Lines from request handling reach the sink, and a hung collector costs a
/// request nothing: every request answers 200 well inside the POST timeout
/// that a request waiting on the sink would sit through, while the overflow
/// is counted dropped and shows on `/metrics`.
#[tokio::test]
async fn requests_are_unaffected_by_a_hung_sink() {
    let (sink_url, accepted) = spawn_hung_collector().await;
    let sink = sink_at(&sink_url, 2, Duration::from_millis(20));
    // Scoped to this test's thread, which also runs the server: the default
    // #[tokio::test] runtime is single-threaded.
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(
            stdout_layer(Some(sink.clone()))
                .with_filter(tracing_subscriber::EnvFilter::new("anthropic_lb=info")),
        ),
    );

    let (upstream_url, _upstream) = spawn_mock_upstream().await;
    let mut endpoint = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
    endpoint.base_url = upstream_url;
    let state = Arc::new(TestPolicy::open().install(AppState {
        endpoints: vec![endpoint],
        log_sink: Some(sink.clone()),
        ..test_state_base()
    }));
    let addr = serve(build_router(state)).await;
    let client = Client::new();

    // A callsite another test's thread registers first can cache "never" for
    // a scoped subscriber (tracing_core::callsite, "Rebuilding Cached
    // Interest"). One request registers every callsite on the path; the
    // rebuild then re-reads them with this subscriber in place.
    assert_eq!(send_message(&client, addr).await, reqwest::StatusCode::OK);
    tracing::callsite::rebuild_interest_cache();
    // Its lines are the first batch, so the POST that hangs is under way
    // before the requests measured below.
    eventually("the hung POST", Duration::from_secs(5), || {
        accepted.load(Ordering::SeqCst) == 1
    })
    .await;
    let seen = |s: &LogSink| s.sent() + s.dropped() + pending(s);
    let before = seen(&sink);

    let started = Instant::now();
    for _ in 0..10 {
        assert_eq!(send_message(&client, addr).await, reqwest::StatusCode::OK);
    }
    let took = started.elapsed();
    assert!(
        took < POST_TIMEOUT / 2,
        "ten requests took {took:?} behind a hung sink"
    );
    assert!(
        seen(&sink) >= before + 10,
        "request lines must reach the sink: {before} -> {}",
        seen(&sink)
    );
    assert!(
        sink.dropped() > 0,
        "a queue of 2 behind a hung POST overflows"
    );

    let body = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("# TYPE anthropic_log_sink_lines_sent_total counter"));
    assert!(body.contains("\nanthropic_log_sink_lines_sent_total 0\n"));
    assert!(body.contains("# TYPE anthropic_log_sink_lines_dropped_total counter"));
    let dropped: u64 = body
        .lines()
        .find_map(|l| l.strip_prefix("anthropic_log_sink_lines_dropped_total "))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no dropped counter:\n{body}"));
    assert!(dropped > 0);
}

// ── Config ──────────────────────────────────────────────────────────

const BASE: &str = "listen = \"0.0.0.0:8080\"\nallow_unauthenticated = true\n";

#[test]
fn config_without_log_sink_is_off() {
    let (config, _, _) = load_config(BASE).unwrap();
    assert!(config.log_sink.is_none());
}

#[test]
fn config_accepts_an_http_or_https_url() {
    for url in [
        "http://logs.example:9428/insert/jsonline",
        "https://logs.example/insert/jsonline?_stream_fields=host",
    ] {
        let (config, _, _) = load_config(&format!("{BASE}[log_sink]\nurl = \"{url}\"\n"))
            .unwrap_or_else(|e| panic!("{url} must boot: {e}"));
        assert_eq!(config.log_sink.unwrap().url, url);
    }
}

/// A URL with credentials or another scheme is refused, and no message quotes
/// it: a URL can hold a secret.
#[test]
fn config_refuses_credentials_and_other_schemes_without_echoing_the_url() {
    for (url, needle) in [
        (
            "https://ingest:s3cret-password@logs.example/insert/jsonline",
            "must not carry credentials",
        ),
        (
            "https://ingest-user@logs.example/insert/jsonline",
            "must not carry credentials",
        ),
        ("ftp://logs.example/insert/jsonline", "not an http(s) URL"),
        ("logs.example/insert/jsonline", "not an http(s) URL"),
    ] {
        let err = match load_config(&format!("{BASE}[log_sink]\nurl = \"{url}\"\n")) {
            Ok(_) => panic!("{url} must be refused"),
            Err(e) => e,
        };
        assert!(err.starts_with("log_sink.url: "), "{err}");
        assert!(err.contains(needle), "expected '{needle}' in: {err}");
        for part in ["s3cret", "ingest", "logs.example", "jsonline"] {
            assert!(!err.contains(part), "the error quotes '{part}': {err}");
        }
    }
}

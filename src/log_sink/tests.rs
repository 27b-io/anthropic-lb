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
    LogSink::start_with(
        &LogSinkConfig {
            url: url.to_string(),
        },
        capacity,
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

/// Seconds since the epoch of an RFC 3339 UTC timestamp with a fraction, the
/// one shape `now_rfc3339` writes (`2026-10-17T01:02:03.123456Z`).
fn rfc3339_epoch(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let shape = b.len() > 21
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'.'
        && s.ends_with('Z')
        && s[20..s.len() - 1].bytes().all(|c| c.is_ascii_digit());
    if !shape {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, m, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (hh, mm, ss) = (n(11..13)?, n(14..16)?, n(17..19)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hh * 3600 + mm * 60 + ss)
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
    let time = rows[0]["_time"].as_str().unwrap();
    let epoch = rfc3339_epoch(time).unwrap_or_else(|| panic!("not RFC 3339: {time}"));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!((now - epoch).abs() < 30, "{time} is not now");
}

/// The layer `main` installs ships plain text: with colour on (no `NO_COLOR`
/// in the test environment), a sink's copy would otherwise carry the level and
/// field-name escapes into every `_msg`.
#[tokio::test]
async fn the_sink_gets_plain_text_from_the_stdout_layer() {
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
    let big = "x".repeat(MAX_BATCH_BYTES / 2 + 1);
    for _ in 0..3 {
        sink.push(big.as_bytes());
    }
    eventually("the byte-bounded batches", Duration::from_secs(5), || {
        sink.sent() == 2_503
    })
    .await;
    let sizes: Vec<usize> = posts
        .lock()
        .unwrap()
        .iter()
        .map(|p| p.rows().len())
        .collect();
    assert_eq!(sizes, [2, 1], "the line that crosses the cap still rides");
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
    assert_eq!(sink.drain(Duration::from_secs(1)).await, 0);
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

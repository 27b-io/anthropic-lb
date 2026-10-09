//! Upstream client budgets: a streamed reply runs for as long as its chunks
//! keep coming, a silent stream is cut by the stall guard, and only
//! non-streaming requests keep the 900 s total cap. Budgets run 200x faster
//! under test (`upstream_budget`).

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MESSAGES_BODY: &str = r#"{"model":"claude-sonnet-4-6","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

fn delta(i: usize) -> String {
    format!(
        "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{i}\"}}}}\n\n"
    )
}

/// Raw-TCP upstream answering one request with a chunked SSE stream: `frames`
/// spaced `spacing` apart, then the chunked terminator. With no frames it
/// sends only the response head and then holds the connection silent.
async fn spawn_paced_upstream(frames: Vec<String>, spacing: Duration) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let _ = s.read(&mut buf).await;
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
        s.write_all(head.as_bytes()).await.unwrap();
        if frames.is_empty() {
            tokio::time::sleep(Duration::from_secs(60)).await;
            return;
        }
        for f in frames {
            if s.write_all(format!("{:x}\r\n{f}\r\n", f.len()).as_bytes())
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(spacing).await;
        }
        let _ = s.write_all(b"0\r\n\r\n").await;
    });
    format!("http://{addr}")
}

/// Frames at a third of the stall guard, for 1.3x the non-streaming cap: a
/// healthy stream that runs past the old shared 900 s budget.
fn steady_frames() -> (Vec<String>, Duration) {
    let spacing = upstream_budget(STREAMING_STALL_SECS) / 3;
    let n = (upstream_budget(NONSTREAMING_TOTAL_SECS).as_millis() * 13 / 10 / spacing.as_millis())
        as usize;
    ((0..n).map(delta).collect(), spacing)
}

/// Checked at compile time: a budget under the floor fails the test build.
#[test]
fn streaming_budget_outlasts_a_full_reply() {
    // A full 128K-token reply at about 110 tokens/s takes about 1,160 s.
    const {
        assert!(
            STREAMING_TOTAL_SECS >= 1800,
            "the streaming budget must not cut a legitimate generation"
        );
        assert!(STREAMING_TOTAL_SECS > NONSTREAMING_TOTAL_SECS);
    }
}

/// The defect: the streaming client inherited the 900 s total, so a stream
/// still delivering chunks died at 900 s. Through the real router, a steady
/// stream that runs past that cap now arrives whole.
#[tokio::test]
async fn steady_stream_outlives_the_nonstreaming_cap() {
    let (frames, spacing) = steady_frames();
    let expected: String = frames.concat();
    let url = spawn_paced_upstream(frames, spacing).await;
    let state = Arc::new(AppState {
        endpoints: vec![mk_endpoint_at("acct-a", "sk-ant-api-a", &url)],
        auto_cache: false,
        client: streaming_client(),
        client_nonstreaming: nonstreaming_client(),
        ..test_state_base()
    });
    let addr = serve(build_router(state)).await;

    let started = Instant::now();
    let resp = Client::new()
        .post(format!("http://{addr}/v1/messages"))
        .header("content-type", "application/json")
        .body(MESSAGES_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.expect("stream must complete");
    let elapsed = started.elapsed();
    assert!(
        elapsed > upstream_budget(NONSTREAMING_TOTAL_SECS),
        "stream must run past the non-streaming cap, took {elapsed:?}"
    );
    assert_eq!(body, expected, "the whole stream arrives verbatim");
}

/// The total budget no longer guards stalls; the stall guard does. A stream
/// that goes silent is cut once it has been silent for the stall guard, long
/// before any total budget.
#[tokio::test]
async fn silent_stream_is_cut_by_the_stall_guard() {
    let url = spawn_paced_upstream(vec![], Duration::ZERO).await;
    let started = Instant::now();
    let resp = streaming_client().post(&url).send().await.unwrap();
    let err = resp.bytes().await.expect_err("a silent stream must be cut");
    let elapsed = started.elapsed();
    assert!(err.is_timeout(), "got {err:?}");
    assert!(
        elapsed >= upstream_budget(STREAMING_STALL_SECS)
            && elapsed < upstream_budget(NONSTREAMING_TOTAL_SECS),
        "cut after {elapsed:?}; stall guard is {:?}",
        upstream_budget(STREAMING_STALL_SECS)
    );
}

/// The non-streaming client keeps its total cap, so the steady stream above
/// would have died under it: the router test discriminates.
#[tokio::test]
async fn nonstreaming_client_keeps_its_total_cap() {
    let (frames, spacing) = steady_frames();
    let url = spawn_paced_upstream(frames, spacing).await;
    let started = Instant::now();
    let resp = nonstreaming_client().post(&url).send().await.unwrap();
    let err = resp.bytes().await.expect_err("the total cap must cut it");
    let elapsed = started.elapsed();
    assert!(err.is_timeout(), "got {err:?}");
    assert!(
        elapsed >= upstream_budget(NONSTREAMING_TOTAL_SECS),
        "cut after {elapsed:?}, before the cap"
    );
}

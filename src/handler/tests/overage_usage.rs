//! Tokens served on paid extra usage land in `overage_usage` (LAB-8496),
//! keyed on the serving response's own `overage-in-use` header — over both
//! routes that relay an Anthropic upstream, streaming and not.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CLIENT: &str = "metered";
const MODEL: &str = "claude-sonnet-4-6";
const ROUTES: [&str; 2] = ["/v1/messages", "/v1/chat/completions"];

const JSON_BODY: &str = "{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"hi\"}],\"model\":\"claude-sonnet-4-6\",\"stop_reason\":\"end_turn\",\"usage\":{\"input_tokens\":7,\"output_tokens\":5,\"cache_creation_input_tokens\":2,\"cache_read_input_tokens\":3}}";
const SSE_BODY: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":7,\"output_tokens\":1,\"cache_creation_input_tokens\":2,\"cache_read_input_tokens\":3}}}\n\n\
event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n\
event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
/// `[input, output, cache_creation, cache_read]` both bodies report.
const TOKENS: [u64; 4] = [7, 5, 2, 3];

/// Raw-TCP upstream answering every request with a 200 carrying
/// `overage-in-use: <overage>`, its body delimited by connection close.
async fn spawn_upstream(stream: bool, overage: bool) -> String {
    let (content_type, body) = if stream {
        ("text/event-stream", SSE_BODY)
    } else {
        ("application/json", JSON_BODY)
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\n\
         anthropic-ratelimit-unified-status: allowed\r\n\
         anthropic-ratelimit-unified-overage-in-use: {overage}\r\n\
         connection: close\r\n\r\n{body}"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let _ = s.read(&mut buf).await; // drain the request before responding
            let _ = s.write_all(response.as_bytes()).await;
            let _ = s.flush().await;
        }
    });
    format!("http://{addr}")
}

async fn serve_one(url: &str, path: &str, body: String) -> Arc<AppState> {
    let mut acct = make_endpoint("acct", Protocol::Anthropic);
    acct.base_url = url.to_owned();
    let state = test_state_with(vec![acct]);
    let addr = serve(build_router(state.clone())).await;
    let resp = Client::new()
        .post(format!("http://{addr}{path}"))
        .header("content-type", "application/json")
        .header("x-client-id", CLIENT)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{path}");
    resp.text().await.unwrap();
    state
}

/// `(overage, all)` usage for `(CLIENT, MODEL)` once `all` is booked — a
/// stream finalizes in the relay's detached task — or after 5 s.
async fn settled(state: &AppState) -> (Option<[u64; 4]>, Option<[u64; 4]>) {
    let key = (CLIENT.to_string(), MODEL.to_string());
    for _ in 0..500 {
        if state.lock_client_model_usage().contains_key(&key) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let all = state.lock_client_model_usage().get(&key).copied();
    let overage = state.lock_overage_usage().get(&key).copied();
    (overage, all)
}

fn request_body(stream: bool, speed: Option<&str>) -> String {
    let speed = speed.map_or(String::new(), |s| format!(",\"speed\":\"{s}\""));
    format!(
        "{{\"model\":\"{MODEL}\",\"max_tokens\":8,\"stream\":{stream}{speed},\"messages\":[{{\"role\":\"user\",\"content\":\"hi\"}}]}}"
    )
}

#[tokio::test]
async fn overage_response_books_its_tokens() {
    for path in ROUTES {
        for stream in [false, true] {
            let url = spawn_upstream(stream, true).await;
            let state = serve_one(&url, path, request_body(stream, None)).await;
            let (overage, all) = settled(&state).await;
            assert_eq!(all, Some(TOKENS), "{path} stream={stream}: usage recorded");
            assert_eq!(
                overage,
                Some(TOKENS),
                "{path} stream={stream}: an overage response books its tokens"
            );
        }
    }
}

#[tokio::test]
async fn standard_response_books_nothing() {
    for path in ROUTES {
        for stream in [false, true] {
            let url = spawn_upstream(stream, false).await;
            let state = serve_one(&url, path, request_body(stream, None)).await;
            let (overage, all) = settled(&state).await;
            assert_eq!(all, Some(TOKENS), "{path} stream={stream}: usage recorded");
            assert_eq!(
                overage, None,
                "{path} stream={stream}: a non-overage response books nothing"
            );
        }
    }
}

/// A fast-mode 200 is kept out of the account's shared `RateLimitInfo`
/// (LAB-2693), so the shared flag stays `false` while this response says
/// `true`. The meter follows the response: it is what the request billed.
#[tokio::test]
async fn meter_reads_the_response_not_the_account() {
    let url = spawn_upstream(false, true).await;
    let state = serve_one(&url, "/v1/messages", request_body(false, Some("fast"))).await;
    let (overage, _) = settled(&state).await;
    assert!(
        !state.endpoints[0].rate_info.read().await.overage_in_use,
        "fast-mode headers must not reach the shared account state"
    );
    assert_eq!(overage, Some(TOKENS));
}

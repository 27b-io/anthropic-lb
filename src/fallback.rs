use crate::*;

// ── Fallback upstream handler ────────────────────────────────────────

/// Anthropic→OpenAI request body for `try_fallback_upstream`, built lazily by
/// `proxy_handler` on the first OpenAI-endpoint attempt of a request and
/// memoized across rotations/retries (LAB-716).
pub(crate) enum FallbackBody {
    /// Translated + serialized once; `is_streaming` read from the same parse.
    Ready {
        body: bytes::Bytes,
        is_streaming: bool,
    },
    /// Request JSON didn't parse (or couldn't re-serialize) — rotate past
    /// OpenAI endpoints; Anthropic endpoints still forward the raw bytes and
    /// let the upstream reject them.
    Unparseable,
    /// Parsed but not representable in OpenAI format — terminal for the
    /// request: rotating would fail identically on every endpoint.
    Untranslatable(String),
}

pub(crate) fn build_openai_fallback_body(body_bytes: &[u8]) -> FallbackBody {
    let parsed: serde_json::Value = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(_) => return FallbackBody::Unparseable,
    };
    let is_streaming = parsed
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);
    let openai_body = match translate_anthropic_request_to_openai(&parsed) {
        Ok(v) => v,
        Err(msg) => return FallbackBody::Untranslatable(msg),
    };
    match serde_json::to_vec(&openai_body) {
        Ok(b) => FallbackBody::Ready {
            body: b.into(),
            is_streaming,
        },
        Err(e) => {
            // Can't happen for a Value built from parsed JSON (string keys
            // only) — but don't rotate invisibly if it somehow does.
            warn!(error = %e, "fallback: translated body failed to serialize");
            FallbackBody::Unparseable
        }
    }
}

/// Forward a request to a `Protocol::OpenAI` endpoint. Callers hand over the
/// final wire body: `proxy_handler` pre-translates via
/// `build_openai_fallback_body` and sets `translate = true` so the *response*
/// is translated back to Anthropic format; `openai_chat_handler` passes the
/// client's OpenAI-format bytes (temperature-stripped per LAB-798 when the
/// model hard-rejects it, otherwise verbatim) with `translate = false`
/// (passthrough).
///
/// Returns `ForwardOutcome` so the caller's retry loop applies the SAME
/// round-gated policy as the Anthropic path (`apply_round_outcome`): a
/// transport send failure is `transient` (round 0 retries this endpoint in
/// place, later rounds rotate, exhaustion is a retryable 503) and feeds the
/// per-endpoint circuit breaker; upstream 429/5xx rotate immediately. Other
/// 4xx (e.g. 400, 401) are `Done` — retry won't help on client-side errors.
///
/// Non-streaming responses are accounted via `finalize_non_stream`: OpenAI
/// bodies carry `usage.prompt_tokens`/`completion_tokens`, mapped to
/// input/output tokens so per-client token + budget enforcement sees this
/// spend (LAB-712). Streaming responses still record nothing — the SSE
/// translator does not yet extract incremental usage (rides with LAB-717).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_fallback_upstream(
    state: &AppState,
    // Final wire body (already OpenAI-shaped), serialized once by the
    // caller; each attempt clones the refcounted `Bytes`.
    request_body: &bytes::Bytes,
    req_id: &str,
    client_id: &str,
    client_ip: &std::net::IpAddr,
    agent_id: &str,
    session_id: &str,
    model: &str,
    endpoint_idx: usize,
    request_start: std::time::Instant,
    translate: bool, // true = upstream response needs OpenAI→Anthropic translation
    is_streaming: bool,
) -> ForwardOutcome {
    let ep = &state.endpoints[endpoint_idx];

    info!(
        req_id,
        client_id,
        model,
        upstream = ep.name,
        translate,
        pin = state.pin_status(client_id, endpoint_idx),
        "fallback: routing to unified OpenAI endpoint"
    );

    ep.requests.fetch_add(1, Ordering::Relaxed);

    let url = format!("{}/v1/chat/completions", ep.base_url);

    // Same non-streaming read_timeout exemption as forward_anthropic (LAB-718).
    let http_client = if is_streaming {
        &state.client
    } else {
        &state.client_nonstreaming
    };
    let resp = match http_client
        .post(&url)
        .header("authorization", format!("Bearer {}", ep.token))
        .header("content-type", "application/json")
        .body(request_body.clone())
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!(
                req_id,
                upstream = ep.name,
                error = %e,
                detail = %describe_reqwest_error(&e),
                "fallback: unified OpenAI endpoint request failed"
            );
            // Same dashboard counter the Anthropic forward paths feed.
            let kind = if e.is_timeout() {
                "timeout"
            } else if e.is_connect() {
                "connect"
            } else {
                "other"
            };
            *state.lock_transport_errors().entry(kind).or_insert(0) += 1;
            // Health signal + transient classification — closes the #69 gap
            // where this branch swallowed transport errors to a bare `None`.
            state.record_transport_failure(endpoint_idx).await;
            return ForwardOutcome::Retry {
                saw_529: false,
                push_skip: false,
                transient: true,
            };
        }
    };

    // Any HTTP response (even 429/5xx) proves the transport path is alive —
    // clear the circuit-breaker counter.
    state.record_transport_success(endpoint_idx).await;

    let status = resp.status();

    // Classify 429 / 529 / other 5xx via the shared helper so OpenAI
    // endpoints get the same policy as the Anthropic paths: a 429 marks the
    // endpoint hard-limited (honouring retry-after) so `pick_endpoint` skips
    // it for the cooldown window, and a 529 flags the long-base BEBO backoff.
    // Previously this was a bare rotate — every subsequent request re-hammered
    // the still-rate-limited endpoint before rotating (GH #97).
    // Downstream parses OpenAI errors on the passthrough path
    // (translate = false); Anthropic errors when translating back.
    let mut resp = match classify_retry_status(
        state,
        status,
        &ep.rate_info,
        &ep.name,
        resp,
        /* openai_error_shape */ !translate,
        // The OpenAI request shape cannot express `speed` — never fast.
        None,
    )
    .await
    {
        Ok(resp) => resp,
        Err(outcome) => return outcome,
    };

    if !status.is_success() {
        let err_body = resp
            .text()
            .await
            .unwrap_or_else(|_| "upstream error".to_string());
        warn!(
            req_id,
            upstream = ep.name,
            status = status.as_u16(),
            body = %err_body,
            "fallback: unified endpoint returned error"
        );
        // Gateway rejected the MODEL (e.g. LiteLLM "Invalid model name"):
        // negative-cache the pair and rotate instead of handing the client a
        // misleading "your request is invalid" 400 — the model is fine, this
        // endpoint just doesn't serve it (LAB-941, observed 2026-07-27 when a
        // 529 storm drained the Anthropic pool into insight-gateway).
        let model_unsupported = serde_json::from_str::<serde_json::Value>(&err_body)
            .map(|v| is_model_unsupported_error(status, &v))
            .unwrap_or(false);
        let response = if translate {
            // Return error in Anthropic format
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "type": "error",
                        "error": {
                            "type": "api_error",
                            "message": err_body,
                        }
                    })
                    .to_string(),
                ))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                })
        } else {
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(err_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                })
        };
        if model_unsupported {
            state.note_model_unsupported(&ep.name, endpoint_idx, model);
            return ForwardOutcome::RetryModelUnsupported(Box::new(response));
        }
        return ForwardOutcome::Done(Box::new(response));
    }

    // Streaming response
    if is_streaming {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);
        let req_id = req_id.to_string();
        let upstream_name = ep.name.clone();
        let translate_response = translate;

        tokio::spawn(async move {
            let mut buffer: Vec<u8> = Vec::new();
            // `ctx.terminal` serves both branches. Translate: `completed` by
            // the translator, `errored` by the translator (in-band error) or
            // this loop (transport Err / end-of-stream guard). Passthrough:
            // `completed` is set below when upstream's `[DONE]` has been
            // forwarded verbatim, so an error frame on the next read doesn't
            // ship a second `[DONE]` and break strict OpenAI parsers.
            let mut ctx = ReverseStreamContext::default();
            let mut client_gone = false;
            // Carries any partial trailing SSE line between chunks so the
            // `[DONE]` terminator is detected across resp.chunk() boundaries.
            // A naive byte-window scan would false-positive on the literal
            // string "data: [DONE]" appearing inside a JSON content delta,
            // so we split on SSE newline boundaries and only treat a complete
            // `data: [DONE]` line as the terminator.
            let mut done_scan_tail: Vec<u8> = Vec::new();

            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        if translate_response {
                            buffer.extend_from_slice(&chunk);
                            while let Some(pos) = buffer.windows(2).position(|w| w == b"\n\n") {
                                let event = String::from_utf8_lossy(&buffer[..pos]).into_owned();
                                buffer.drain(..pos + 2);

                                for line in event.lines() {
                                    if let Some(data) = line.strip_prefix("data: ") {
                                        let events =
                                            translate_openai_sse_to_anthropic(data, &mut ctx);
                                        for ev in events {
                                            if tx.send(Ok(bytes::Bytes::from(ev))).await.is_err() {
                                                client_gone = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if client_gone || ctx.terminal.errored {
                                    // errored: the in-band error frame just
                                    // sent is the stream's final frame — stop
                                    // draining so nothing can follow it.
                                    break;
                                }
                            }
                        } else {
                            if !ctx.terminal.completed {
                                done_scan_tail.extend_from_slice(&chunk);
                                while let Some(nl) = done_scan_tail.iter().position(|&b| b == b'\n')
                                {
                                    let line_end = if nl > 0 && done_scan_tail[nl - 1] == b'\r' {
                                        nl - 1
                                    } else {
                                        nl
                                    };
                                    let is_done_marker = if let Some(payload) =
                                        done_scan_tail[..line_end].strip_prefix(b"data:")
                                    {
                                        payload.trim_ascii() == b"[DONE]"
                                    } else {
                                        false
                                    };
                                    done_scan_tail.drain(..=nl);
                                    if is_done_marker {
                                        ctx.terminal.completed = true;
                                        done_scan_tail.clear();
                                        break;
                                    }
                                }
                            }
                            if tx.send(Ok(chunk)).await.is_err() {
                                client_gone = true;
                            }
                        }
                        if client_gone || ctx.terminal.errored {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(req_id, error = %e, "fallback: unified endpoint SSE read failed");
                        // When a terminator already went out (`message_stop`
                        // translated, or `[DONE]` forwarded verbatim) the
                        // client saw a complete stream — no frame is sent and
                        // the stream stays a success.
                        if ctx.terminal.reached() {
                            debug!(
                                req_id,
                                "fallback: transport error after terminator — error frame suppressed"
                            );
                            break;
                        }
                        // Error frame is terminal: mark the ctx so the
                        // post-loop buffer flush translates to nothing, no
                        // frame follows the error, and finalization logs the
                        // stream as failed. Downstream protocol depends on
                        // whether we're translating: translate_response=true
                        // → /v1/messages client expects Anthropic SSE; false
                        // → /v1/chat/completions passthrough, OpenAI SSE.
                        ctx.terminal.errored = true;
                        let msg = format!("upstream stream interrupted: {e}");
                        let frame = if translate_response {
                            anthropic_error_frame(&msg)
                        } else {
                            openai_error_frame(&msg)
                        };
                        if tx.send(Ok(frame)).await.is_err() {
                            client_gone = true;
                        }
                        break;
                    }
                }
            }

            // Flush remaining buffer
            if translate_response && !buffer.is_empty() && !client_gone {
                let remaining = String::from_utf8_lossy(&buffer).into_owned();
                for line in remaining.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        let events = translate_openai_sse_to_anthropic(data, &mut ctx);
                        for ev in events {
                            if tx.send(Ok(bytes::Bytes::from(ev))).await.is_err() {
                                client_gone = true;
                                break;
                            }
                        }
                    }
                }
            }

            // End-of-stream reconciliation, translate branch: the upstream
            // ended (clean EOF, or a `[DONE]` with no message to stop) without
            // the Anthropic stream ever reaching a terminator. Left alone the
            // /v1/messages client would hold `message_start` + deltas and a
            // closed socket, or a 200 with an empty SSE body. Terminate
            // explicitly. (The passthrough branch forwards whatever the
            // upstream sent and does not synthesise terminators for it.)
            if translate_response && !client_gone && !ctx.terminal.reached() {
                warn!(
                    req_id,
                    upstream = upstream_name,
                    message_started = ctx.message_started,
                    "fallback: upstream stream ended without a terminator — error frame sent"
                );
                ctx.terminal.errored = true;
                if tx
                    .send(Ok(anthropic_error_frame(
                        "upstream closed stream before completion",
                    )))
                    .await
                    .is_err()
                {
                    client_gone = true;
                }
            }

            if client_gone {
                debug!(req_id, "fallback: client disconnected during stream");
            }
            if ctx.terminal.errored {
                warn!(
                    req_id,
                    upstream = upstream_name,
                    "fallback: unified endpoint stream ended with upstream error frame"
                );
            } else {
                info!(
                    req_id,
                    upstream = upstream_name,
                    "fallback: unified endpoint stream complete"
                );
            }
        });

        return ForwardOutcome::Done(Box::new(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .header("connection", "keep-alive")
                .body(Body::from_stream(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                ))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback stream error").into_response()
                }),
        ));
    }

    // Non-streaming response
    let resp_body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            // Mid-body failure after a received response: transport reached the
            // upstream, so this is NOT a breaker signal — plain rotation.
            error!(req_id, error = %e, "fallback: failed to read unified endpoint response body");
            return ROTATE;
        }
    };

    // OpenAI non-streaming bodies carry usage.prompt_tokens/completion_tokens —
    // record them like the Anthropic path so per-client token + budget
    // enforcement (`pre_request_gate`) sees OpenAI-endpoint spend (LAB-712).
    let openai_resp: serde_json::Value = match serde_json::from_slice(&resp_body) {
        Ok(v) => v,
        Err(e) => {
            // Still serve the response (passthrough forwards the raw bytes),
            // but the malformed body means usage records as zero — say so.
            warn!(
                req_id,
                client_id,
                model,
                upstream = ep.name,
                error = %e,
                "fallback: unified endpoint response body is not valid JSON; usage not recorded"
            );
            serde_json::json!({})
        }
    };
    let usage = TokenUsage::from_openai_response_body(&openai_resp);
    finalize_non_stream(
        state,
        ep,
        req_id,
        client_id,
        model,
        openai_resp.get("model").and_then(|v| v.as_str()),
        &ep.name,
        &client_ip.to_string(),
        agent_id,
        session_id,
        status.as_u16(),
        &usage,
        request_start.elapsed().as_millis() as u64,
        !translate, // openai_compat marks the client surface: translate=false ⇒ OpenAI-format caller
        // OpenAI-protocol endpoints don't consume Anthropic context windows —
        // the session registry (LAB-916) tracks Anthropic-bound traffic only.
        None,
        0,
        // OpenAI endpoints carry stub RateLimitInfo — no routing snapshot to
        // merge with; this path keeps its own usage-only log line (LAB-3214).
        None,
    )
    .await;

    if translate {
        let anthropic_resp = translate_openai_response_to_anthropic(&openai_resp);
        info!(
            req_id,
            upstream = ep.name,
            "fallback: unified endpoint translated response"
        );
        ForwardOutcome::Done(Box::new(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(anthropic_resp.to_string()))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        ))
    } else {
        info!(
            req_id,
            upstream = ep.name,
            "fallback: unified endpoint forwarded response"
        );
        ForwardOutcome::Done(Box::new(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(resp_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fallback error").into_response()
                }),
        ))
    }
}

#[cfg(test)]
mod tests;

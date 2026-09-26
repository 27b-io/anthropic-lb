use crate::*;

// ── Token usage extraction ──────────────────────────────────────────

#[derive(Default, Debug, Clone)]
pub(crate) struct TokenUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) cache_creation_input_tokens: u64,
    pub(crate) cache_read_input_tokens: u64,
}

impl TokenUsage {
    /// Parse usage from an Anthropic API response body (non-streaming JSON).
    pub(crate) fn from_response_body(body: &serde_json::Value) -> Self {
        let usage = match body.get("usage") {
            Some(u) => u,
            None => return Self::default(),
        };
        Self {
            input_tokens: usage
                .get("input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            output_tokens: usage
                .get("output_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_creation_input_tokens: usage
                .get("cache_creation_input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_read_input_tokens: usage
                .get("cache_read_input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        }
    }

    /// Parse usage from an OpenAI-format response body (non-streaming JSON).
    /// OpenAI reports `prompt_tokens`/`completion_tokens`; there is no
    /// Anthropic-style cache-token split, so those fields stay 0.
    pub(crate) fn from_openai_response_body(body: &serde_json::Value) -> Self {
        Self {
            input_tokens: body
                .pointer("/usage/prompt_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            output_tokens: body
                .pointer("/usage/completion_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            ..Self::default()
        }
    }

    /// Parse usage from a complete SSE transcript. Test convenience wrapper —
    /// production streaming paths feed chunks through `SseUsageScanner`
    /// incrementally instead of buffering the stream.
    #[cfg(test)]
    fn from_sse_text(text: &str) -> Self {
        let mut scanner = SseUsageScanner::default();
        scanner.push(text.as_bytes());
        scanner.finish();
        scanner.usage
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.input_tokens == 0
            && self.output_tokens == 0
            && self.cache_creation_input_tokens == 0
            && self.cache_read_input_tokens == 0
    }
}

/// Cap on any single SSE line `SseUsageScanner` will hold or scan — applied
/// uniformly whether the line completes within one chunk or is carried across
/// chunks. Usage-bearing lines (`message_start` / `message_delta`) are well
/// under 1 KiB; anything larger is content we don't need, and a malformed
/// upstream must not be able to grow scanner memory without bound (LAB-717).
const SSE_SCAN_MAX_LINE: usize = 64 * 1024;

/// Single-terminator bookkeeping for one downstream SSE stream (LAB-4031).
///
/// An SSE stream has exactly one terminator, and an error frame is one:
/// never an error frame after the success terminator, never a success
/// terminator after an error frame. Every stream loop and translator gates
/// terminator emission on this — one vocabulary, so a reviewer can grep
/// `terminal.` and find each site. The translating loops also synthesise a
/// terminator when the upstream ends without one; the byte-passthrough
/// loops forward the upstream's stream as-is and only suppress a second
/// terminator. It lives where the downstream truth is observable: on
/// `SseUsageScanner` for the byte-passthrough (downstream == upstream), on
/// `StreamContext` / `ReverseStreamContext` for the translating loops
/// (the proxy emits the terminator itself).
#[derive(Default)]
pub(crate) struct SseTerminal {
    /// Success terminator went downstream: Anthropic `message_stop` or
    /// OpenAI `data: [DONE]`.
    pub(crate) completed: bool,
    /// Error frame went downstream — an in-band upstream error translated,
    /// or one synthesised on a transport failure / premature end of stream.
    pub(crate) errored: bool,
}

impl SseTerminal {
    /// Either terminator has gone downstream — nothing may follow it.
    pub(crate) fn reached(&self) -> bool {
        self.completed || self.errored
    }
}

/// Incremental SSE token-usage extractor: O(1) memory per in-flight stream.
///
/// Replaces the old whole-stream `sse_buf` / `raw_sse` accumulation (LAB-717):
/// each upstream chunk is line-scanned as it passes through, keeping only a
/// capped partial-line carry, the running `TokenUsage` (last `message_start` /
/// `message_delta` wins, matching the old post-hoc parse), and bounded event
/// metadata for the `stream_end_no_usage` diagnostic.
#[derive(Default)]
pub(crate) struct SseUsageScanner {
    pub(crate) usage: TokenUsage,
    /// Model reported by the upstream `message_start` event (LAB-2330) —
    /// the response-derived model used for per-(client, model) accounting,
    /// preferred over the caller-supplied request model.
    pub(crate) model: Option<String>,
    /// Bytes of the current line seen so far, awaiting its `\n`.
    carry: Vec<u8>,
    /// Set when a line overflows `SSE_SCAN_MAX_LINE`; the rest of that line
    /// is discarded up to the next newline.
    skipping_oversized_line: bool,
    /// First five `event:` types, for the `stream_end_no_usage` preview.
    pub(crate) event_preview: Vec<String>,
    pub(crate) event_count: usize,
    pub(crate) bytes_seen: usize,
    /// Terminator state of the bytes forwarded so far.
    pub(crate) terminal: SseTerminal,
}

impl SseUsageScanner {
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.bytes_seen = self.bytes_seen.saturating_add(chunk.len());
        let mut rest = chunk;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            let (head, tail) = rest.split_at(nl);
            rest = &tail[1..];
            if self.skipping_oversized_line {
                // `head` is the tail of a discarded oversized line.
                self.skipping_oversized_line = false;
            } else if self.carry.len() + head.len() > SSE_SCAN_MAX_LINE {
                // Line exceeds the cap even though it terminated within this
                // chunk — discard it like the cross-chunk case, and release
                // the backing allocation rather than retaining its capacity.
                self.carry = Vec::new();
            } else if self.carry.is_empty() {
                self.scan_line(head);
            } else {
                self.carry.extend_from_slice(head);
                let line = std::mem::take(&mut self.carry);
                self.scan_line(&line);
                self.carry = line;
                self.carry.clear(); // reuse the allocation, drop the contents
            }
        }
        if rest.is_empty() || self.skipping_oversized_line {
            return;
        }
        self.carry.extend_from_slice(rest);
        if self.carry.len() > SSE_SCAN_MAX_LINE {
            self.carry = Vec::new();
            self.skipping_oversized_line = true;
        }
    }

    /// Flush the trailing unterminated line (a stream may not end with `\n`).
    pub(crate) fn finish(&mut self) {
        if !self.carry.is_empty() {
            let line = std::mem::take(&mut self.carry);
            self.scan_line(&line);
        }
    }

    fn scan_line(&mut self, raw: &[u8]) {
        let line = String::from_utf8_lossy(raw);
        let line = line.trim();
        if let Some(ev) = line.strip_prefix("event:") {
            self.event_count += 1;
            if self.event_preview.len() < 5 {
                self.event_preview.push(ev.trim_start().to_string());
            }
            match ev.trim() {
                "message_stop" => self.terminal.completed = true,
                "error" => self.terminal.errored = true,
                _ => {}
            }
            return;
        }
        let Some(data) = line.strip_prefix("data: ") else {
            return;
        };
        // Cheap pre-filter: only usage-bearing event types are worth a JSON
        // parse, and their type string must appear literally in the payload.
        // False positives (a content delta mentioning "message_start") fall
        // through to the type match below and are ignored, same as before.
        if !data.contains("message_start") && !data.contains("message_delta") {
            return;
        }
        let Ok(event) = serde_json::from_str::<serde_json::Value>(data) else {
            return;
        };
        match event.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "message_start" => {
                if let Some(m) = event
                    .pointer("/message/model")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    self.model = Some(m.to_owned());
                }
                if let Some(msg_usage) = event.get("message").and_then(|m| m.get("usage")) {
                    self.usage.input_tokens = msg_usage
                        .get("input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    self.usage.cache_creation_input_tokens = msg_usage
                        .get("cache_creation_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    self.usage.cache_read_input_tokens = msg_usage
                        .get("cache_read_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                }
            }
            "message_delta" => {
                if let Some(delta_usage) = event.get("usage") {
                    self.usage.output_tokens = delta_usage
                        .get("output_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                }
            }
            _ => {}
        }
    }
}

/// Inject account authentication headers. Handles API keys, OAuth tokens,
/// and passthrough mode. For OAuth, merges required beta flags with the
/// client flags that survive the `allowed_betas` allow-list; the flags it
/// dropped are returned so the caller can log and count them (LAB-1191 /
/// audit finding 5 — an unfiltered merge let any caller activate arbitrary
/// beta features against the operator's account).
pub(crate) fn inject_account_auth(
    headers: &mut axum::http::HeaderMap,
    token: &str,
    passthrough: bool,
    allowed_betas: &[String],
) -> Vec<String> {
    if passthrough {
        return Vec::new();
    }
    headers.remove("authorization");
    headers.remove("x-api-key");
    let mut dropped: Vec<String> = Vec::new();
    if token.starts_with(OAUTH_TOKEN_PREFIX) {
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        headers.insert(
            "anthropic-dangerous-direct-browser-access",
            HeaderValue::from_static("true"),
        );
        // Merge required OAuth beta flags with the allow-listed client flags.
        // Use get_all to handle multiple anthropic-beta headers
        let mut flags: Vec<String> = Vec::new();
        for flag in headers
            .get_all("anthropic-beta")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|s| s.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            // OAUTH_BETA_FLAGS are unconditionally (re-)added below, so a
            // client flag in that set is never actually dropped — reporting
            // it as such (e.g. under a custom allowlist omitting them) would
            // make the drop diagnostics lie.
            if beta_flag_allowed(allowed_betas, flag) || OAUTH_BETA_FLAGS.contains(&flag) {
                if !flags.iter().any(|f| f == flag) {
                    flags.push(flag.to_string());
                }
            } else if !dropped.iter().any(|f| f == flag) {
                dropped.push(flag.to_string());
            }
        }
        for flag in OAUTH_BETA_FLAGS {
            if !flags.iter().any(|f| f == flag) {
                flags.push(flag.to_string());
            }
        }
        headers.insert(
            "anthropic-beta",
            HeaderValue::from_str(&flags.join(",")).unwrap(),
        );
    } else {
        // Anything that is not OAuth (API keys included) is sent as x-api-key.
        headers.insert("x-api-key", HeaderValue::from_str(token).unwrap());
    }
    dropped
}

/// Client identity extracted from request headers.
pub(crate) struct RequestContext {
    pub(crate) client_id: String,
    pub(crate) client_ver: String,
    pub(crate) agent_id: String,
    pub(crate) session_id: String,
}

/// Build the routing affinity key. `fp` is the content fingerprint
/// (system+first-user digest); when present it is APPENDED as the finest
/// discriminator so fan-out agents that share one coarse session-id (e.g. an
/// 80-agent workflow all tagged with the parent session) get distinct keys and
/// distribute, while a stable-prefix conversation keeps a stable fp and stays
/// sticky. When `fp` is absent the key is byte-identical to the legacy
/// header-only form (no mass rehash of existing traffic).
pub(crate) fn affinity_routing_key(
    client_ip: &IpAddr,
    client_id: &str,
    agent_id: &str,
    session_id: &str,
    fp: Option<&str>,
) -> Option<String> {
    let has_identity = client_id != "-" || agent_id != "-" || session_id != "-" || fp.is_some();
    if !has_identity {
        return None;
    }
    let base = format!("{}:{}:{}:{}", client_ip, client_id, agent_id, session_id);
    Some(match fp {
        Some(f) => format!("{base}:{f}"),
        None => base,
    })
}

impl RequestContext {
    /// `principal` is the authenticated client from `AppState::authenticate`.
    /// When present it IS the identity — `x-client-id` and the `client_names`
    /// IP map are not consulted at all. This one substitution is what makes
    /// budgets, ceilings, operator bypass, the model allow-list and the
    /// response-cache tenant unspoofable: every one of them keys on
    /// `client_id`, and this is where `client_id` is born.
    pub(crate) fn from_request(
        state: &AppState,
        client_ip: &IpAddr,
        headers: &axum::http::HeaderMap,
        principal: Option<&ClientConfig>,
    ) -> Self {
        Self {
            client_id: match principal {
                Some(c) => c.name.clone(),
                None => state.resolve_client_id(client_ip, headers),
            },
            client_ver: headers
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .and_then(extract_client_version)
                .unwrap_or("-")
                .to_string(),
            agent_id: headers
                .get("x-agent-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("-")
                .to_string(),
            session_id: headers
                .get("x-claude-code-session-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    headers
                        .get("x-session-id")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                })
                .unwrap_or("-")
                .to_string(),
        }
    }

    /// Build the routing affinity key. `fp` is the content fingerprint
    /// (system+first-user digest); when present it is APPENDED as the finest
    /// discriminator so fan-out agents that share one coarse session-id (e.g. an
    /// 80-agent workflow all tagged with the parent session) get distinct keys
    /// and distribute, while a stable-prefix conversation keeps a stable fp and
    /// stays sticky. When `fp` is absent the key is unchanged from header-only
    /// behaviour (no mass rehash).
    pub(crate) fn affinity_key(&self, client_ip: &IpAddr, fp: Option<&str>) -> Option<String> {
        affinity_routing_key(
            client_ip,
            &self.client_id,
            &self.agent_id,
            &self.session_id,
            fp,
        )
    }
}

#[cfg(test)]
mod tests;

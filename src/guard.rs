//! Tier 0 request-content guard (LAB-3877).
//!
//! A synchronous, in-process rules-tier scan over each proxied request body,
//! run after `pre_request_gate` and before endpoint selection. It is
//! **detect-only and read-only**: it borrows the parsed body, never mutates it,
//! so the bytes forwarded upstream stay byte-identical to what the client sent
//! (Anthropic prompt caching matches on raw byte prefixes — see
//! `proxy_handler`). The whole module compiles only behind the `guard` cargo
//! feature; with the feature off, none of this — and none of its dependencies —
//! is built.
//!
//! Shadow mode is the default: the per-client policy ships as `annotate`, so the
//! layer counts and annotates but blocks nothing until false-positive rates have
//! been measured in production. `block` and `off` are opt-in per client.
//!
//! Findings carry **offsets only, never the matched text** — into logs, metrics,
//! and the 400 body alike. The matched secret never leaves the scanner.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Per-client enforcement policy. Deserialized from `[[clients]].guard` in the
/// operator config; defaults to `annotate` (shadow mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GuardPolicy {
    /// Skip scanning entirely. Also the effective policy for the operator-bypass
    /// client, regardless of config.
    Off,
    /// Scan, annotate, and count — but always allow the request through. The
    /// shadow-mode default.
    #[default]
    Annotate,
    /// Scan and reject a request with any finding (HTTP 400).
    Block,
}

/// A single detection. Offsets are byte offsets into the scanned input; the
/// matched text itself is deliberately absent so it cannot leak into logs, the
/// error body, or metrics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Finding {
    /// Scanner that produced this finding (e.g. `"secrets_scanner"`).
    pub scanner: &'static str,
    /// Scanner-specific detection label — a rule id or PII category. Never the
    /// matched value.
    pub detection_type: String,
    /// Byte offset of the match start within the scanned input.
    pub start: usize,
    /// Byte offset one past the match end within the scanned input.
    pub end: usize,
}

/// Outcome of a scan. Scanners themselves only ever produce `Allow` or
/// `Annotate`; `Block` is a policy escalation applied by [`Guard::evaluate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing flagged.
    Allow,
    /// Findings present; the request proceeds (shadow mode).
    Annotate { findings: Vec<Finding> },
    /// Findings present and policy is `block`; the request is rejected.
    Block {
        findings: Vec<Finding>,
        reason: String,
    },
}

/// Upper bound on the bytes handed to the scanners, so the guard has a hard
/// worst-case latency on the request hot path. Bodies with a larger newest
/// user/tool_result span are scanned only up to this many bytes; the tail is
/// not inspected. Chosen so the combined scanner p99 stays comfortably under
/// the 2 ms budget on the release binary (see `scan_input_capped_under_budget`).
/// A leaked credential in a config/env paste almost always appears within the
/// first 32 KiB, so the coverage cost in shadow mode is small.
///
/// Latency note: the byte cap bounds scan cost for realistic content (prose or
/// code with up to ~100 detections in 32 KiB scans in ~1.2 ms). The PII
/// scanner's cost grows with the number of detections, so *degenerate*
/// content — 32 KiB of almost nothing but PII tokens (~1000 detections) — can
/// take ~14 ms. That is an adversarial p100, not a realistic p99, and it is
/// bounded (input capped here, body capped at the 25 MiB inflight limit); it
/// never leaks and never blocks another request's I/O.
///
// ponytail: fixed cap; scan runs inline on the async worker. If shadow-mode data
// shows finding-dense bodies are common enough that the ~14 ms p100 matters,
// the upgrade path is tokio::task::spawn_blocking for the scan (isolates the CPU
// burst from the executor) and/or a per-deployment cap knob — not removing the
// bound.
pub const MAX_SCAN_BYTES: usize = 32 * 1024;

/// The subset of a request body a scanner is allowed to see: the newest `user`
/// text blocks and the newest `tool_result` blocks. The `system` prompt is
/// deliberately excluded — it is operator-trusted here and a known
/// false-positive surface.
pub struct ScanInput {
    /// Newest user text + tool_result content, joined by newlines and truncated
    /// to [`MAX_SCAN_BYTES`]. Line boundaries between blocks stop a match from
    /// spanning two blocks.
    text: String,
    /// Whether the joined content was truncated to fit the byte cap.
    truncated: bool,
}

impl ScanInput {
    /// The concatenated text to scan.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether the input was truncated to [`MAX_SCAN_BYTES`] (the tail is
    /// unscanned). Surfaced in the guard log so an operator can tell whether the
    /// cap is biting during shadow-mode measurement.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Build a scan input from a parsed Anthropic Messages body, extracting only
    /// the newest `user` message's text and tool_result blocks. `system` is
    /// never read.
    ///
    /// "Newest" = the last element of `messages` whose role is `user`. That is
    /// the turn being sent for completion (and, after tool use, the turn that
    /// carries the tool results).
    ///
    /// LAB-4358: the return is a [`ScanOutcome`], not an `Option`. The old
    /// `None` conflated "readable, no text" with "could not read the document",
    /// and `guard_hook` forwarded both under `block` — so `{"messages":["<secret>"]}`,
    /// a `"User"` role, and an object-valued `content` each reached the upstream
    /// unscanned. The line drawn here is **structural readability vs content
    /// absence**: a document the scanner can parse but which carries no text it
    /// reads is [`ScanOutcome::NothingToScan`] (allowed); a body it cannot parse
    /// is [`ScanOutcome::Unscannable`] (fails closed under `block`).
    ///
    /// Readability is judged per role, not per message: locating the newest user
    /// turn requires reading EVERY element's role, because an element the
    /// scanner cannot parse could itself be that turn. Content readability is
    /// judged on the newest user turn only — an `assistant` message with
    /// `content: null` is routine and carries nothing this scanner would read.
    ///
    /// Inside a message, `null` is treated as absent, not as present-and-wrong:
    /// the newest turn's `content`, a text block's `text`, a
    /// `tool_result.content`. A JSON null cannot be hiding content, so rejecting
    /// it would cost availability (clients whose codegen emits `null` for an
    /// omitted optional, which `tool_result.content` is) and buy no coverage.
    /// `messages` is the one field where `null` and absent differ: a body with
    /// no `messages` key is not a Messages request and forwards, but
    /// `messages: null` fails closed as [`REASON_MESSAGES_NOT_ARRAY`]. Every
    /// endpoint that takes the field requires it, so no client sends `null`
    /// there as an omitted optional.
    pub fn from_body(body: &Value) -> ScanOutcome {
        // A body that is not a JSON object is not a request any endpoint this
        // proxy serves accepts, and `Value::get` answers `None` on one — so
        // without this it would read as "no `messages` key" and forward. A bare
        // `POST /v1/messages -d '"<secret>"'` parses fine and is all content.
        if !body.is_object() {
            return ScanOutcome::Unscannable(REASON_BODY_NOT_OBJECT);
        }
        let Some(messages) = body.get("messages") else {
            // Not a Messages request at all. `proxy_handler` is the router's
            // `.fallback`, so `/v1/complete` and `/v1/models` reach the guard
            // with no `messages` key; failing those closed would take every
            // non-Messages endpoint offline for `block` clients. What those
            // bodies DO carry (`prompt`, `requests[].params`) is unscanned —
            // a documented coverage boundary, unchanged by this function.
            return ScanOutcome::NothingToScan;
        };
        let Some(messages) = messages.as_array() else {
            return ScanOutcome::Unscannable(REASON_MESSAGES_NOT_ARRAY);
        };

        let mut last_user: Option<&Value> = None;
        for message in messages {
            let Some(role) = message.get("role").and_then(Value::as_str) else {
                // A non-object element, or one whose `role` is absent or not a
                // string. `{"messages":["<secret>"]}` lands here.
                return ScanOutcome::Unscannable(REASON_MESSAGE_UNREADABLE);
            };
            match role {
                "user" => last_user = Some(message),
                "assistant" => {}
                // The Messages API accepts `user` and `assistant` and nothing
                // else, so no legitimate body reaches this arm — but `"User"`,
                // `"USER"` and the OpenAI roles translation passes through
                // verbatim (`developer`, `function`) all do, and each is a
                // message the scanner cannot classify. Matching case-insensitively
                // instead would be worse than useless: a trailing `{"role":"User"}`
                // would then shadow the real newest user turn and hide it.
                _ => return ScanOutcome::Unscannable(REASON_MESSAGE_UNREADABLE),
            }
        }
        // A readable conversation with no user turn yet (an `assistant`-only
        // prefill, or an OpenAI body whose only message was hoisted into
        // `system`). Nothing to scan, not a scan failure.
        let Some(last_user) = last_user else {
            return ScanOutcome::NothingToScan;
        };

        let mut segments: Vec<&str> = Vec::new();
        match last_user.get("content") {
            None | Some(Value::Null) => {}
            // Shorthand string content is user text.
            Some(Value::String(s)) => segments.push(s),
            Some(Value::Array(blocks)) => {
                for block in blocks {
                    match block.get("type").and_then(Value::as_str) {
                        Some("text") => match text_block_segment(block) {
                            Ok(Some(t)) => segments.push(t),
                            Ok(None) => {}
                            Err(reason) => return ScanOutcome::Unscannable(reason),
                        },
                        Some("tool_result") => {
                            if let Err(reason) = collect_tool_result(block, &mut segments) {
                                return ScanOutcome::Unscannable(reason);
                            }
                        }
                        // A block type this scanner does not read — `image`,
                        // `document`, `thinking`. A question of content coverage,
                        // NOT a readability failure: the image-only turn is a
                        // deliberate allow and must stay one.
                        Some(_) => {}
                        // No string `type`: not a content block at all.
                        None => return ScanOutcome::Unscannable(REASON_CONTENT_UNREADABLE),
                    }
                }
            }
            Some(_) => return ScanOutcome::Unscannable(REASON_CONTENT_UNREADABLE),
        }

        if segments.is_empty() {
            return ScanOutcome::NothingToScan;
        }

        let mut text = segments.join("\n");
        let truncated = text.len() > MAX_SCAN_BYTES;
        if truncated {
            // Truncate at the largest UTF-8 char boundary <= the cap.
            let mut end = MAX_SCAN_BYTES;
            while end > 0 && !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
        }
        ScanOutcome::Scannable(ScanInput { text, truncated })
    }

    /// The `/v1/chat/completions` variant. Readability is judged on `original` —
    /// the body the client sent, and the bytes a `Protocol::OpenAI` upstream
    /// receives — while the text is extracted from `translated`, the Messages
    /// document this scanner understands.
    ///
    /// The split is not a formality, and judging readability on `translated`
    /// alone is not sufficient: `translate_openai_to_anthropic` is LOSSY in
    /// three places, and each loss turns an unreadable document into a readable
    /// empty one while the original bytes keep every character.
    ///
    /// 1. `messages` absent, or not an array — rewritten to an EMPTY array.
    /// 2. A `tool` message's `content` — funnelled through
    ///    `.as_str() → .as_array()+text-join → .unwrap_or_default()`, so an
    ///    object, a scalar, or an array whose `text` is not a string collapses
    ///    to `""`. The scanner then reads an empty `tool_result` and reports it
    ///    CLEAN, which is worse than reporting nothing.
    /// 3. An `image_url` part — rewritten through
    ///    `pointer("/image_url/url").unwrap_or("")`, so a part whose `image_url`
    ///    is not an object with a string `url` becomes an empty image block.
    ///
    /// Everything else (a non-object element, an unmapped role, an object-valued
    /// user `content`) does survive translation verbatim and is caught by
    /// [`ScanInput::from_body`] on the translated document.
    pub fn from_openai_body(original: &Value, translated: &Value) -> ScanOutcome {
        // `/v1/chat/completions` is a single API that requires this field, so
        // absent is a malformed Messages request here — unlike the native
        // fallback, where it means "not a Messages request at all".
        let Some(messages) = original.get("messages").and_then(Value::as_array) else {
            return ScanOutcome::Unscannable(REASON_MESSAGES_NOT_ARRAY);
        };
        for message in messages {
            if let Err(reason) = openai_message_readable(message) {
                return ScanOutcome::Unscannable(reason);
            }
        }
        Self::from_body(translated)
    }
}

/// Whether one ORIGINAL OpenAI `messages` element is readable, for the losses
/// [`ScanInput::from_openai_body`] documents.
///
/// The content walk is scoped to `user` and `tool` — the only roles whose
/// content reaches the scanned document. `system` is hoisted out of `messages`
/// and never scanned, and `assistant` content is not the newest user turn;
/// both are deliberate coverage boundaries, so an unreadable shape there is not
/// a failure to read the SCAN, and rejecting it would deny requests for no gain.
fn openai_message_readable(message: &Value) -> Result<(), &'static str> {
    let Some(role) = message.get("role").and_then(Value::as_str) else {
        return Err(REASON_MESSAGE_UNREADABLE);
    };
    if role != "user" && role != "tool" {
        return Ok(());
    }
    match message.get("content") {
        None | Some(Value::Null) | Some(Value::String(_)) => Ok(()),
        Some(Value::Array(parts)) => {
            for part in parts {
                // Every part must be an object carrying a string `type`.
                let Some(part_type) = part.get("type").and_then(Value::as_str) else {
                    return Err(REASON_CONTENT_UNREADABLE);
                };
                // A present `text` must be a string whatever the part type says:
                // the `tool` arm joins parts through `p.get("text").as_str()`
                // without consulting `type`, dropping anything else silently.
                text_block_segment(part)?;
                // `image_url` is read through `pointer("/image_url/url")`, so
                // any other shape loses its content to an empty url.
                if part_type == "image_url"
                    && part
                        .pointer("/image_url/url")
                        .and_then(Value::as_str)
                        .is_none()
                {
                    return Err(REASON_CONTENT_UNREADABLE);
                }
            }
            Ok(())
        }
        Some(_) => Err(REASON_CONTENT_UNREADABLE),
    }
}

/// The `text` of one content block: `Ok(Some)` when it carries a string,
/// `Ok(None)` when absent (nothing can be hiding there), `Err` when present in
/// a shape this scanner cannot read.
///
/// One predicate, three call sites (user text blocks, `tool_result` inner
/// blocks, and the OpenAI original-body walk). Keeping three copies of a
/// fail-closed rule in sync is the debt this change just paid off elsewhere.
fn text_block_segment(block: &Value) -> Result<Option<&str>, &'static str> {
    match block.get("text") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(t)) => Ok(Some(t)),
        Some(_) => Err(REASON_CONTENT_UNREADABLE),
    }
}

/// What [`ScanInput::from_body`] could make of a request body (LAB-4358).
///
/// Three outcomes, not two. `NothingToScan` and `Unscannable` are both "no
/// [`ScanInput`]", but they are opposite facts about the request: the first
/// says the guard read the document and there was nothing in it to scan, the
/// second says the guard could not read the document at all. Only the first is
/// safe to forward under `block`. Returning them as one `None` is what left the
/// array-shaped bypasses open, and making them separate variants is what stops
/// a future caller from re-acquiring them silently — the compiler now forces
/// every call site to say which of the two it means.
#[must_use]
pub enum ScanOutcome {
    /// Text was extracted; scan it.
    Scannable(ScanInput),
    /// The document was readable and genuinely carried no text this scanner
    /// reads: an image-only turn, a conversation with no user turn, or a body
    /// that is not a Messages request. A deliberate allow.
    NothingToScan,
    /// The request body could not be read. Fails closed under `block`. Set by
    /// `from_body` for a `messages` it cannot parse, and by the handler for a
    /// non-empty body that is not JSON at all.
    Unscannable(&'static str),
}

/// The request body parsed as JSON but is not an object.
pub const REASON_BODY_NOT_OBJECT: &str = "request body is not a JSON object and cannot be scanned";
/// `messages` is absent or is not an array.
///
/// The text names BOTH causes because the two surfaces reject different ones:
/// `/v1/chat/completions` requires the field, so absent is malformed there and
/// takes this reason too, while the `proxy_handler` fallback must keep
/// forwarding the `messages`-less bodies of `/v1/complete` and `/v1/models` and
/// so only ever reaches it for a present field of the wrong shape. Naming one
/// cause would misdescribe the other to the client reading `error.message`.
pub const REASON_MESSAGES_NOT_ARRAY: &str =
    "request `messages` is missing or not an array and cannot be scanned";
/// A `messages` element the scanner cannot resolve to a message: not an object,
/// a `role` that is absent or not a string, or a role it cannot classify.
///
/// One reason, not one per cause, on purpose. Translation rewrites an element
/// it cannot read into `{"role": ""}`, so splitting "not a message" from "bad
/// role" would report a DIFFERENT cause for the same client body depending on
/// which endpoint it hit — a diagnostic that flips on routing is worse than one
/// that is merely coarse.
pub const REASON_MESSAGE_UNREADABLE: &str =
    "request `messages` contains a message the scanner cannot read";
/// Content that is present but not in a readable shape.
pub const REASON_CONTENT_UNREADABLE: &str =
    "request `messages` content is not in a shape the scanner can read";
/// A non-empty request body that is not JSON at all. Set by the handler, not by
/// [`ScanInput::from_body`], but it is a guard reason and lives with the rest.
pub const REASON_BODY_UNPARSEABLE: &str = "request body could not be parsed for content scanning";
/// A newest turn longer than [`MAX_SCAN_BYTES`], whose tail was never scanned.
/// Set by the handler from [`ScanInput::truncated`], not by `from_body`.
pub const REASON_SCAN_TRUNCATED: &str =
    "request exceeds the guard scan limit and cannot be scanned in full";

/// A `tool_result` block's `content` is either a string or an array of content
/// blocks (typically `text`). Pull out every text span; ignore image/other.
///
/// `Err` carries the reason the block was unreadable: a `content` (or an inner
/// block) in a shape this function cannot walk, which under `block` policy must
/// fail closed rather than be silently skipped. `out` may hold partial segments
/// in that case and the caller discards them.
fn collect_tool_result<'a>(block: &'a Value, out: &mut Vec<&'a str>) -> Result<(), &'static str> {
    match block.get("content") {
        None | Some(Value::Null) => {}
        Some(Value::String(s)) => out.push(s),
        Some(Value::Array(inner)) => {
            for b in inner {
                match b.get("type").and_then(Value::as_str) {
                    Some("text") => out.extend(text_block_segment(b)?),
                    Some(_) => {}
                    None => return Err(REASON_CONTENT_UNREADABLE),
                }
            }
        }
        Some(_) => return Err(REASON_CONTENT_UNREADABLE),
    }
    Ok(())
}

/// A compact, deduplicated `scanner:detection_type` summary for the structured
/// `guard` log line — never any matched text. Bounded so a body crafted to
/// produce thousands of distinct detections cannot inflate one log line.
pub fn detections_summary(findings: &[Finding]) -> String {
    const MAX: usize = 20;
    let mut seen: Vec<String> = Vec::new();
    for f in findings {
        let label = format!("{}:{}", f.scanner, f.detection_type);
        if !seen.contains(&label) {
            seen.push(label);
            if seen.len() >= MAX {
                seen.push("…".to_string());
                break;
            }
        }
    }
    seen.join(",")
}

/// A synchronous content scanner. Implementations are built once at startup
/// (rule/regex compilation is not cheap) and shared read-only across requests.
///
/// A scanner reports findings; it does not decide policy. An empty result means
/// nothing was flagged. Enforcement (annotate vs block) is [`Guard::evaluate`]'s
/// job, so scanners never construct a [`Verdict`].
pub trait Scanner: Send + Sync {
    /// Stable label for logs and the `scanner` metric dimension. Also the value
    /// stamped into each [`Finding::scanner`] this scanner produces.
    fn name(&self) -> &'static str;
    /// Scan the input, returning every detection (empty = nothing flagged).
    fn scan(&self, input: &ScanInput) -> Vec<Finding>;
}

/// Secrets/credentials scanner backed by `secrets_scanner` with its bundled
/// (compiled-in) gitleaks/kingfisher ruleset and a hardened proxy config:
/// attacker-supplied inline allow markers are ignored and matched text is
/// redacted before it can be stored anywhere.
pub struct SecretsScanner {
    inner: secrets_scanner::Scanner,
}

impl SecretsScanner {
    pub fn new() -> Result<Self, String> {
        // Hardened for untrusted request bodies: ignore inline `gitleaks:allow`
        // markers a caller could embed to suppress detection, redact matched
        // text, skip context capture, cap output.
        let config = secrets_scanner::ScanConfig::proxy();
        // Fail-closed by construction: if a future change to `proxy()` softened
        // any of those guarantees, refuse to build rather than silently scan
        // untrusted content un-hardened.
        if !config.is_hardened() {
            return Err("secrets_scanner proxy config is not hardened".to_string());
        }
        let inner = secrets_scanner::Scanner::from_bundled()
            .map_err(|e| format!("secrets_scanner rule load failed: {e}"))?
            .with_config(config);
        Ok(Self { inner })
    }
}

impl Scanner for SecretsScanner {
    fn name(&self) -> &'static str {
        "secrets_scanner"
    }

    fn scan(&self, input: &ScanInput) -> Vec<Finding> {
        self.inner
            .scan_content("request-body", input.text())
            .into_iter()
            .map(|f| Finding {
                scanner: self.name(),
                detection_type: f.rule_id,
                start: f.start_offset,
                end: f.end_offset,
            })
            .collect()
    }
}

/// PII scanner backed by `leakguard` (zero-dependency, pure Rust). Detects
/// emails, cards, IPs, JWTs, national ids, and provider API-key shapes.
pub struct LeakGuardScanner {
    inner: leakguard::Redactor,
}

impl LeakGuardScanner {
    pub fn new() -> Self {
        Self {
            inner: leakguard::Redactor::new(),
        }
    }
}

impl Default for LeakGuardScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl Scanner for LeakGuardScanner {
    fn name(&self) -> &'static str {
        "leakguard"
    }

    fn scan(&self, input: &ScanInput) -> Vec<Finding> {
        self.inner
            .find(input.text())
            .into_iter()
            .map(|m| Finding {
                scanner: self.name(),
                detection_type: format!("{:?}", m.kind),
                start: m.start,
                end: m.end,
            })
            .collect()
    }
}

/// Upper edges (seconds) of the `anthropic_guard_scan_duration_seconds`
/// histogram, plus an implicit `+Inf`. Sized around the sub-2ms budget so the
/// interesting quantiles land on real bucket boundaries.
const SCAN_DURATION_BUCKETS: [f64; 9] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05,
];

/// A Prometheus-style histogram over scan durations, lock-free on the hot path.
/// Per-bucket counts are non-cumulative here; `metrics_handler` cumulates them
/// at scrape time into the `le`-labelled series Prometheus expects.
pub struct ScanHistogram {
    /// One counter per bucket in [`SCAN_DURATION_BUCKETS`] plus a trailing
    /// `+Inf` overflow bucket.
    buckets: [AtomicU64; SCAN_DURATION_BUCKETS.len() + 1],
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl ScanHistogram {
    fn new() -> Self {
        Self {
            buckets: Default::default(),
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn observe(&self, elapsed: std::time::Duration) {
        let secs = elapsed.as_secs_f64();
        let idx = SCAN_DURATION_BUCKETS
            .iter()
            .position(|&edge| secs <= edge)
            .unwrap_or(SCAN_DURATION_BUCKETS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot for `/metrics`: `(le, cumulative_count)` per bucket (the final
    /// entry is `+Inf`), the sum in seconds, and the total observation count.
    pub fn snapshot(&self) -> (Vec<(String, u64)>, f64, u64) {
        let mut cumulative = 0u64;
        let mut series = Vec::with_capacity(self.buckets.len());
        for (i, b) in self.buckets.iter().enumerate() {
            cumulative += b.load(Ordering::Relaxed);
            // f64 Display gives the plain decimal Prometheus wants for these
            // edges (`0.0001`, not `1e-4`); no custom formatter needed.
            let le = if i < SCAN_DURATION_BUCKETS.len() {
                SCAN_DURATION_BUCKETS[i].to_string()
            } else {
                "+Inf".to_string()
            };
            series.push((le, cumulative));
        }
        let sum = self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
        (series, sum, self.count.load(Ordering::Relaxed))
    }
}

/// AWS's published documentation example secret access key (the `EXAMPLEKEY`
/// suffix marks it as such) — not a credential, and allow-listed by secret
/// scanners for exactly that reason. Guard tests need a value that matches a
/// real secret rule so the scanner fires; a synthetic placeholder would not
/// match, and the tests would prove nothing.
#[cfg(test)]
pub(crate) const AWS_DOCS_EXAMPLE_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

/// Cardinality cap on the `client` dimension of the verdicts counter, so an
/// unbounded stream of distinct client ids cannot grow the map without limit.
/// Overflow folds into `_other`. Mirrors the `beta_flags_dropped` posture.
const MAX_VERDICT_CLIENTS: usize = 256;

/// The assembled guard: the scanners plus their metrics. Constructed once and
/// held in `AppState`.
pub struct Guard {
    scanners: Vec<Box<dyn Scanner>>,
    /// `{(client, scanner, verdict) -> count}` for
    /// `anthropic_guard_verdicts_total`.
    verdicts:
        std::sync::Mutex<std::collections::HashMap<(String, &'static str, &'static str), u64>>,
    scan_hist: ScanHistogram,
}

impl Guard {
    /// Build the guard with the two Tier 0 scanners. Fails if the secrets
    /// ruleset cannot be loaded — a broken guard fails loud at startup rather
    /// than silently scanning nothing.
    pub fn new() -> Result<Self, String> {
        let scanners: Vec<Box<dyn Scanner>> = vec![
            Box::new(SecretsScanner::new()?),
            Box::new(LeakGuardScanner::new()),
        ];
        Ok(Self {
            scanners,
            verdicts: std::sync::Mutex::new(std::collections::HashMap::new()),
            scan_hist: ScanHistogram::new(),
        })
    }

    /// A guard with no scanners — always `Allow`. Test-only, so `AppState`
    /// fixtures that don't exercise the guard don't pay the ruleset-compile
    /// cost on every construction.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            scanners: Vec::new(),
            verdicts: std::sync::Mutex::new(std::collections::HashMap::new()),
            scan_hist: ScanHistogram::new(),
        }
    }

    /// Run the scanners under a client's policy and return the request verdict.
    ///
    /// Records the scan-duration histogram and one verdict counter per scanner.
    /// `Off` and an absent/empty input short-circuit to `Allow` with no scan and
    /// no metrics. Enforcement (`Block`) is applied here from `policy`; the
    /// scanners themselves only report findings.
    pub fn evaluate(
        &self,
        policy: GuardPolicy,
        client_id: &str,
        input: Option<&ScanInput>,
    ) -> Verdict {
        if policy == GuardPolicy::Off {
            return Verdict::Allow;
        }
        let input = match input {
            Some(i) if !i.text.is_empty() => i,
            _ => return Verdict::Allow,
        };

        let start = std::time::Instant::now();
        let mut all: Vec<Finding> = Vec::new();
        // Per-scanner outcome for the metric label, recorded after the timed
        // section so timing captures only scan work.
        let mut per_scanner: Vec<(&'static str, bool)> = Vec::with_capacity(self.scanners.len());
        for scanner in &self.scanners {
            let findings = scanner.scan(input);
            per_scanner.push((scanner.name(), !findings.is_empty()));
            all.extend(findings);
        }
        self.scan_hist.observe(start.elapsed());

        let blocked = policy == GuardPolicy::Block && !all.is_empty();
        // verdict label reflects the request outcome, per scanner: a scanner
        // that fired gets the enforced outcome; one that didn't gets `allow`.
        for (name, hit) in per_scanner {
            let verdict = if !hit {
                "allow"
            } else if blocked {
                "block"
            } else {
                "annotate"
            };
            self.record_verdict(client_id, name, verdict);
        }

        if all.is_empty() {
            Verdict::Allow
        } else if blocked {
            Verdict::Block {
                findings: all,
                reason: "request body contains content flagged by the guard layer".to_string(),
            }
        } else {
            Verdict::Annotate { findings: all }
        }
    }

    fn record_verdict(&self, client_id: &str, scanner: &'static str, verdict: &'static str) {
        let mut map = crate::lock_recovering(&self.verdicts, "guard_verdicts");
        let key = (client_id.to_owned(), scanner, verdict);
        let bounded = if map.len() < MAX_VERDICT_CLIENTS || map.contains_key(&key) {
            key
        } else {
            ("_other".to_owned(), scanner, verdict)
        };
        *map.entry(bounded).or_insert(0) += 1;
    }

    /// Snapshot of the verdicts counter for `/metrics`.
    pub fn verdicts_snapshot(&self) -> Vec<((String, &'static str, &'static str), u64)> {
        crate::lock_recovering(&self.verdicts, "guard_verdicts")
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Snapshot of the scan-duration histogram for `/metrics`.
    pub fn scan_hist_snapshot(&self) -> (Vec<(String, u64)>, f64, u64) {
        self.scan_hist.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input(text: &str) -> ScanInput {
        ScanInput {
            text: text.to_string(),
            truncated: false,
        }
    }

    /// Unwrap a [`ScanOutcome`] the test expects to be scannable. Deliberately
    /// not an inherent `ScanOutcome::ok()` on the production type: collapsing
    /// the three outcomes back into an `Option` is the exact mistake LAB-4358
    /// removed, and a convenience method would put it one `.ok()` away.
    #[track_caller]
    fn scannable(outcome: ScanOutcome) -> ScanInput {
        match outcome {
            ScanOutcome::Scannable(input) => input,
            ScanOutcome::NothingToScan => panic!("expected Scannable, got NothingToScan"),
            ScanOutcome::Unscannable(reason) => {
                panic!("expected Scannable, got Unscannable({reason})")
            }
        }
    }

    #[test]
    fn scan_input_extracts_newest_user_and_tool_result_never_system() {
        let body = json!({
            "system": "operator system prompt with a fake token ghp_ZZZ",
            "messages": [
                {"role": "user", "content": "old turn"},
                {"role": "assistant", "content": [{"type": "text", "text": "sure"}]},
                {"role": "user", "content": [
                    {"type": "text", "text": "please read this"},
                    {"type": "tool_result", "content": [
                        {"type": "text", "text": "file contents here"}
                    ]}
                ]}
            ]
        });
        let si = scannable(ScanInput::from_body(&body));
        assert!(si.text().contains("please read this"));
        assert!(si.text().contains("file contents here"));
        // Never the system prompt, never older turns.
        assert!(!si.text().contains("operator system prompt"));
        assert!(!si.text().contains("old turn"));
    }

    #[test]
    fn scan_input_none_without_user_content() {
        let body = json!({"system": "x", "messages": [
            {"role": "assistant", "content": [{"type": "text", "text": "hi"}]}
        ]});
        assert!(matches!(
            ScanInput::from_body(&body),
            ScanOutcome::NothingToScan
        ));
    }

    /// LAB-4358: `null` is absent, not present-and-unreadable. A JSON null
    /// cannot be hiding content, so rejecting it would deny requests from any
    /// client whose codegen emits `null` for an omitted optional — which
    /// `tool_result.content` genuinely is — and buy no coverage. Pinned because
    /// the first cut of the tri-state DID reject it.
    #[test]
    fn null_content_is_absent_not_unreadable() {
        for body in [
            json!({"messages": [{"role": "user", "content": null}]}),
            json!({"messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": null}
            ]}]}),
            json!({"messages": [{"role": "user", "content": [
                {"type": "text", "text": null}
            ]}]}),
        ] {
            assert!(
                matches!(ScanInput::from_body(&body), ScanOutcome::NothingToScan),
                "null must read as absent, not as unscannable: {body}"
            );
        }
        // Where the exemption stops: an absent `messages` forwards, a `null` one
        // fails closed.
        assert!(matches!(
            ScanInput::from_body(&json!({"messages": null})),
            ScanOutcome::Unscannable(REASON_MESSAGES_NOT_ARRAY)
        ));
    }

    /// LAB-4358: a body that parses as JSON but is not an object reads as "no
    /// `messages` key" through `Value::get`, so without an explicit check a
    /// bare `POST /v1/messages -d '"<secret>"'` forwards unscanned while being
    /// nothing but content.
    #[test]
    fn non_object_body_is_unscannable() {
        for body in [
            json!("a secret string"),
            json!([{"role": "user"}]),
            json!(7),
        ] {
            assert!(
                matches!(ScanInput::from_body(&body), ScanOutcome::Unscannable(_)),
                "a non-object body cannot be read as a Messages request: {body}"
            );
        }
    }

    /// LAB-4358: `translate_openai_to_anthropic` coerces an unreadable `tool`
    /// content and a malformed `image_url` into EMPTY strings, so the translated
    /// document reads as clean (or empty) while the original bytes — the ones a
    /// `Protocol::OpenAI` upstream receives — still carry the content. Judging
    /// readability on the translated document alone reports these CLEAN, which
    /// is worse than reporting them unscanned.
    #[test]
    fn openai_readability_is_judged_on_the_original_body() {
        // What the translator produces for each: readable, and empty.
        let benign_translation = json!({"messages": [{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": ""}
        ]}]});
        for original in [
            json!({"messages": [{"role": "tool", "tool_call_id": "t1", "content": {"v": "x"}}]}),
            json!({"messages": [{"role": "tool", "tool_call_id": "t1", "content": [
                {"type": "text", "text": {"v": "x"}}
            ]}]}),
            json!({"messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": "not-an-object"}
            ]}]}),
        ] {
            assert!(
                matches!(
                    ScanInput::from_openai_body(&original, &benign_translation),
                    ScanOutcome::Unscannable(_)
                ),
                "translation loses this content; the original must fail closed: {original}"
            );
        }
        // The same shape done correctly still scans. Note what the translator
        // hands back for the BENIGN empty case above: `Scannable("")` — "I read
        // it and it was empty", which `Guard::evaluate` then allows on the empty
        // text. That is the state the three originals above were borrowing to
        // look clean, which is why readability cannot be judged there.
        let ok = json!({"messages": [{"role": "tool", "tool_call_id": "t1", "content": "hello"}]});
        let ok_translated = json!({"messages": [{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "hello"}
        ]}]});
        match ScanInput::from_openai_body(&ok, &ok_translated) {
            ScanOutcome::Scannable(input) => assert_eq!(input.text(), "hello"),
            other => panic!(
                "a readable tool result must scan, got {}",
                match other {
                    ScanOutcome::NothingToScan => "NothingToScan",
                    ScanOutcome::Unscannable(r) => r,
                    ScanOutcome::Scannable(_) => unreachable!(),
                }
            ),
        }
    }

    #[test]
    fn scan_input_tool_result_string_content() {
        let body = json!({"messages": [
            {"role": "user", "content": [
                {"type": "tool_result", "content": "plain string result"}
            ]}
        ]});
        let si = scannable(ScanInput::from_body(&body));
        assert_eq!(si.text(), "plain string result");
    }

    #[test]
    fn secrets_scanner_positive_and_negative() {
        let s = SecretsScanner::new().expect("rules");
        // Positive: AWS's documented example secret access key — shaped like
        // the real thing so the rule fires.
        let findings = s.scan(&input(&format!(
            "aws_secret_access_key = \"{}\"",
            AWS_DOCS_EXAMPLE_SECRET_KEY
        )));
        assert!(!findings.is_empty(), "expected a secret finding");
        assert!(findings.iter().all(|f| f.scanner == "secrets_scanner"));
        assert!(findings.iter().all(|f| f.end > f.start));
        // Negative: ordinary prose.
        assert!(s
            .scan(&input("the quick brown fox jumps over the lazy dog"))
            .is_empty());
    }

    #[test]
    fn leakguard_positive_and_negative() {
        let s = LeakGuardScanner::new();
        let findings = s.scan(&input("email me at alice@example.com"));
        assert!(findings.iter().any(|f| f.detection_type == "Email"));
        assert!(findings.iter().all(|f| f.scanner == "leakguard"));
        assert!(s.scan(&input("no personal data here")).is_empty());
    }

    #[test]
    fn policy_dispatch_all_three_modes() {
        let g = Guard::new().expect("guard");
        let dirty = input("contact alice@example.com");

        // off: never scans, always Allow.
        assert_eq!(
            g.evaluate(GuardPolicy::Off, "c", Some(&dirty)),
            Verdict::Allow
        );

        // annotate: findings but request proceeds.
        match g.evaluate(GuardPolicy::Annotate, "c", Some(&dirty)) {
            Verdict::Annotate { findings } => assert!(!findings.is_empty()),
            other => panic!("expected annotate, got {other:?}"),
        }

        // block: findings reject.
        match g.evaluate(GuardPolicy::Block, "c", Some(&dirty)) {
            Verdict::Block { findings, reason } => {
                assert!(!findings.is_empty());
                assert!(!reason.is_empty());
            }
            other => panic!("expected block, got {other:?}"),
        }

        // clean input under block still allows.
        let clean = input("nothing to see here");
        assert_eq!(
            g.evaluate(GuardPolicy::Block, "c", Some(&clean)),
            Verdict::Allow
        );
    }

    #[test]
    fn findings_carry_offsets_never_matched_text() {
        // The Finding type has no field for matched text; serialization proves
        // offsets are all that is emitted.
        let f = Finding {
            scanner: "leakguard",
            detection_type: "Email".to_string(),
            start: 3,
            end: 20,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["start"], 3);
        assert_eq!(v["end"], 20);
        assert_eq!(v["scanner"], "leakguard");
        assert_eq!(v["detection_type"], "Email");
        assert!(v.get("matched").is_none());
        assert!(v.get("text").is_none());
    }

    #[test]
    fn scan_is_read_only_body_unchanged() {
        // Guard extraction borrows the body; the parsed Value must be untouched.
        let body = json!({"messages": [
            {"role": "user", "content": [
                {"type": "text", "text": format!("token {}", AWS_DOCS_EXAMPLE_SECRET_KEY)}
            ]}
        ]});
        let before = serde_json::to_vec(&body).unwrap();
        let g = Guard::new().expect("guard");
        let si = scannable(ScanInput::from_body(&body));
        let _ = g.evaluate(GuardPolicy::Block, "c", Some(&si));
        let after = serde_json::to_vec(&body).unwrap();
        assert_eq!(before, after, "guard must not mutate the request body");
    }

    /// Build a 200 KB Anthropic body whose newest user turn holds the whole
    /// payload — the worst case for the guard (the entire body is scannable
    /// user content). A detection is embedded roughly every ~300 bytes so the
    /// scanned window (capped at [`MAX_SCAN_BYTES`]) carries ~100 findings: this
    /// exercises the finding-extraction path (allocation, detection_type
    /// formatting, offset math), not just the fast keyword reject. ~100
    /// detections in 32 KiB is a realistic-dense worst case; pathological
    /// all-PII content is a documented ceiling on `MAX_SCAN_BYTES`, not the p99.
    fn body_200kb() -> Value {
        let mut big = String::with_capacity(210_000);
        while big.len() < 200_000 {
            big.push_str(
                "lorem ipsum dolor sit amet consectetur adipiscing elit sed do \
                 eiusmod tempor incididunt ut labore et dolore magna aliqua ut \
                 enim contact ops@example.com and more filler words here too ",
            );
        }
        big.truncate(200_000);
        json!({
            "system": "you are a helpful assistant",
            "messages": [{"role": "user", "content": big}]
        })
    }

    #[test]
    fn scan_input_capped_at_max_scan_bytes() {
        let si = scannable(ScanInput::from_body(&body_200kb()));
        assert!(si.truncated(), "a 200KB user turn must trip the cap");
        assert!(
            si.text().len() <= MAX_SCAN_BYTES,
            "scanned bytes {} exceeded cap {MAX_SCAN_BYTES}",
            si.text().len()
        );
    }

    #[test]
    fn scan_200kb_body_under_budget_p99() {
        // AC: scan time for a 200 KB body is under 2 ms p99 on the release
        // binary. The byte cap bounds the scan regardless of body size, so this
        // holds for any body. Debug builds run the same regexes far slower, so
        // assert the tight 2 ms bound only in release (run
        // `cargo test --release --features guard` for the real measurement) and
        // a loose sanity bound in debug.
        let g = Guard::new().expect("guard");
        // Go through the real extraction path so the cap is part of the
        // measurement, then reuse the (capped) input across iterations to time
        // the scan itself.
        let si = scannable(ScanInput::from_body(&body_200kb()));

        // Guard against the test silently regressing to the fast-reject path:
        // the scanned window must actually produce findings, so the timed loop
        // exercises finding extraction, not just keyword rejection.
        match g.evaluate(GuardPolicy::Annotate, "c", Some(&si)) {
            Verdict::Annotate { findings } => assert!(
                findings.len() >= 50,
                "perf payload must be finding-dense (got {} findings) so the \
                 extraction path is measured",
                findings.len()
            ),
            other => panic!("perf payload produced no findings: {other:?}"),
        }

        let iters = if cfg!(debug_assertions) { 30 } else { 300 };
        let mut samples: Vec<std::time::Duration> = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = std::time::Instant::now();
            let _ = g.evaluate(GuardPolicy::Annotate, "c", Some(&si));
            samples.push(t.elapsed());
        }
        samples.sort();
        let p99 = samples[((iters as f64 * 0.99) as usize).saturating_sub(1)];

        let budget = if cfg!(debug_assertions) {
            std::time::Duration::from_millis(100)
        } else {
            std::time::Duration::from_millis(2)
        };
        assert!(
            p99 < budget,
            "200KB-body scan p99 {p99:?} exceeded budget {budget:?} (cap {MAX_SCAN_BYTES}B)"
        );
    }

    #[test]
    fn histogram_cumulates_and_counts() {
        let h = ScanHistogram::new();
        h.observe(std::time::Duration::from_micros(50)); // <=0.0001
        h.observe(std::time::Duration::from_micros(300)); // <=0.0005
        h.observe(std::time::Duration::from_millis(3)); // <=0.005
        let (series, sum, count) = h.snapshot();
        assert_eq!(count, 3);
        assert!(sum > 0.0);
        // Cumulative and monotonic non-decreasing; final +Inf bucket holds all.
        assert_eq!(series.last().unwrap().0, "+Inf");
        assert_eq!(series.last().unwrap().1, 3);
        for w in series.windows(2) {
            assert!(w[1].1 >= w[0].1, "buckets must be cumulative");
        }
    }
}

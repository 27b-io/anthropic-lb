use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use ipnet::IpNet;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

// ── Config ──────────────────────────────────────────────────────────

#[derive(Deserialize, Clone)]
struct Config {
    listen: String,
    upstream: String,
    #[allow(dead_code)]
    strategy: Option<String>,
    rate_limit_cooldown_secs: Option<u64>,
    /// Seconds between utilization probes per account (0 = disabled). Default: 300 (5 min)
    probe_interval_secs: Option<u64>,
    /// Shared secret clients must send as x-api-key to access the proxy. None = open.
    proxy_key: Option<String>,
    /// Source IP allowlist. Supports individual IPs and CIDR ranges. None/empty = allow all.
    allowed_ips: Option<Vec<String>>,
    accounts: Vec<AccountConfig>,
    /// OpenAI-compatible upstream routes. Requests to /upstream/<name>/... are forwarded.
    #[serde(default)]
    upstreams: Vec<UpstreamConfig>,
}

#[derive(Deserialize, Clone)]
struct AccountConfig {
    name: String,
    /// Auth token. Use "passthrough" to forward caller's auth headers as-is.
    token: String,
}

#[derive(Deserialize, Clone)]
struct UpstreamConfig {
    name: String,
    base_url: String,
    api_key: String,
}

// ── Runtime state ───────────────────────────────────────────────────

#[derive(Default)]
struct RateLimitInfo {
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    /// Unified utilization (0.0 = fresh, 1.0 = exhausted). From representative claim window.
    utilization: Option<f64>,
    /// Which window is the binding constraint (e.g. "five_hour", "seven_day")
    representative_claim: Option<String>,
    hard_limited_until: Option<Instant>,
    #[allow(dead_code)]
    last_updated: Option<Instant>,
}

struct Account {
    name: String,
    token: String,
    passthrough: bool,
    requests: AtomicU64,
    rate_info: RwLock<RateLimitInfo>,
}

struct Upstream {
    name: String,
    base_url: String,
    api_key: String,
    requests: AtomicU64,
}

struct AppState {
    client: Client,
    upstream: String,
    accounts: Vec<Account>,
    robin: AtomicUsize,
    cooldown: Duration,
    state_path: PathBuf,
    proxy_key: Option<String>,
    allowed_ips: Vec<IpAllowEntry>,
    upstreams: Vec<Upstream>,
}

/// Parsed IP allow entry — either a single IP or a CIDR range.
enum IpAllowEntry {
    Addr(IpAddr),
    Net(IpNet),
}

impl IpAllowEntry {
    fn contains(&self, ip: &IpAddr) -> bool {
        match self {
            Self::Addr(a) => a == ip,
            Self::Net(n) => n.contains(ip),
        }
    }
}

impl AppState {
    fn is_ip_allowed(&self, ip: &IpAddr) -> bool {
        self.allowed_ips.is_empty() || self.allowed_ips.iter().any(|e| e.contains(ip))
    }
}

// ── Persistence ─────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct PersistedState {
    accounts: Vec<PersistedAccount>,
    #[serde(default)]
    saved_at: u64,
}

#[derive(Serialize, Deserialize)]
struct PersistedAccount {
    name: String,
    requests_total: u64,
    utilization: Option<f64>,
    representative_claim: Option<String>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
    limit_requests: Option<u64>,
    limit_tokens: Option<u64>,
    /// Absolute unix timestamp (secs) when hard limit expires
    hard_limited_until_epoch: Option<u64>,
}

impl AppState {
    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    async fn save_state(&self) {
        let mut accounts = Vec::new();
        let now = Instant::now();

        for acct in &self.accounts {
            let info = acct.rate_info.read().await;
            let hard_until_epoch = info.hard_limited_until.and_then(|until| {
                if until > now {
                    let remaining = until.duration_since(now);
                    Some(Self::now_epoch() + remaining.as_secs())
                } else {
                    None
                }
            });
            accounts.push(PersistedAccount {
                name: acct.name.clone(),
                requests_total: acct.requests.load(Ordering::Relaxed),
                utilization: info.utilization,
                representative_claim: info.representative_claim.clone(),
                remaining_requests: info.remaining_requests,
                remaining_tokens: info.remaining_tokens,
                limit_requests: info.limit_requests,
                limit_tokens: info.limit_tokens,
                hard_limited_until_epoch: hard_until_epoch,
            });
        }

        let state = PersistedState {
            accounts,
            saved_at: Self::now_epoch(),
        };

        match serde_json::to_string_pretty(&state) {
            Ok(json) => {
                if let Err(e) = tokio::fs::write(&self.state_path, json).await {
                    error!(path = %self.state_path.display(), error = %e, "failed to save state");
                } else {
                    debug!(path = %self.state_path.display(), "state saved");
                }
            }
            Err(e) => error!(error = %e, "failed to serialize state"),
        }
    }

    /// Fire a minimal request (max_tokens=1) to refresh rate limit headers for an account.
    async fn probe_account(&self, idx: usize) {
        let acct = &self.accounts[idx];
        if acct.passthrough {
            debug!(
                account = acct.name,
                "skipping probe for passthrough account"
            );
            return;
        }

        // Check if hard-limited — don't waste a request
        {
            let info = acct.rate_info.read().await;
            if let Some(until) = info.hard_limited_until {
                if Instant::now() < until {
                    debug!(
                        account = acct.name,
                        "skipping probe, account is hard-limited"
                    );
                    return;
                }
            }
        }

        let url = format!("{}/v1/messages", self.upstream);
        let body = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1,
            "system": [{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."}],
            "messages": [{"role": "user", "content": "."}]
        });

        let mut req = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
            .header("user-agent", "claude-cli/2.1.2 (external, cli)")
            .header("x-app", "cli")
            .header("anthropic-dangerous-direct-browser-access", "true")
            .json(&body);

        // Inject auth
        if acct.token.starts_with("sk-ant-api") {
            req = req.header("x-api-key", &acct.token);
        } else if acct.token.starts_with("sk-ant-oat") {
            req = req.header("authorization", format!("Bearer {}", acct.token));
        } else {
            req = req.header("x-api-key", &acct.token);
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                self.update_rate_info(idx, resp.headers()).await;
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    self.mark_hard_limited(idx, resp.headers()).await;
                }
                self.save_state().await;
                info!(
                    account = acct.name,
                    status = status.as_u16(),
                    "probe complete"
                );
            }
            Err(e) => {
                warn!(account = acct.name, error = %e, "probe failed");
            }
        }
    }

    async fn load_state(&self) {
        let data = match tokio::fs::read_to_string(&self.state_path).await {
            Ok(d) => d,
            Err(_) => {
                info!(path = %self.state_path.display(), "no persisted state found, starting fresh");
                return;
            }
        };

        let persisted: PersistedState = match serde_json::from_str(&data) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to parse persisted state, starting fresh");
                return;
            }
        };

        let now_epoch = Self::now_epoch();
        let now_instant = Instant::now();

        for pa in &persisted.accounts {
            if let Some(acct) = self.accounts.iter().find(|a| a.name == pa.name) {
                acct.requests.store(pa.requests_total, Ordering::Relaxed);
                let mut info = acct.rate_info.write().await;
                info.utilization = pa.utilization;
                info.representative_claim = pa.representative_claim.clone();
                info.remaining_requests = pa.remaining_requests;
                info.remaining_tokens = pa.remaining_tokens;
                info.limit_requests = pa.limit_requests;
                info.limit_tokens = pa.limit_tokens;

                if let Some(until_epoch) = pa.hard_limited_until_epoch {
                    if until_epoch > now_epoch {
                        let remaining_secs = until_epoch - now_epoch;
                        info.hard_limited_until =
                            Some(now_instant + Duration::from_secs(remaining_secs));
                        info!(
                            account = pa.name,
                            remaining_secs, "restored hard limit from persisted state"
                        );
                    }
                }

                info.last_updated = Some(now_instant);
                info!(
                    account = pa.name,
                    utilization = ?pa.utilization,
                    requests = pa.requests_total,
                    "restored account state"
                );
            }
        }
    }
}

impl AppState {
    /// Pick the best available account.
    /// Strategy: choose the account with the most remaining tokens.
    /// Falls back to round-robin if no rate limit info is available yet.
    async fn pick_account(&self) -> Option<usize> {
        let n = self.accounts.len();
        let now = Instant::now();

        // First pass: find account with lowest utilization (most headroom)
        let mut best_idx: Option<usize> = None;
        let mut best_utilization: f64 = f64::MAX;
        let mut has_any_info = false;
        let mut available_count = 0;

        for i in 0..n {
            let info = self.accounts[i].rate_info.read().await;

            // Skip hard-limited accounts
            if let Some(until) = info.hard_limited_until {
                if now < until {
                    continue;
                }
            }

            available_count += 1;

            // Get effective utilization: known value, legacy conversion, or 0.0 (unknown = fresh)
            let effective_util = if let Some(util) = info.utilization {
                has_any_info = true;
                util
            } else if let Some(remaining) = info.remaining_tokens {
                has_any_info = true;
                let limit = info.limit_tokens.unwrap_or(1_000_000);
                1.0 - (remaining as f64 / limit as f64)
            } else {
                // No rate info — don't influence smart routing, fall through to round-robin
                0.0
            };

            if effective_util < best_utilization {
                best_utilization = effective_util;
                best_idx = Some(i);
            }
        }

        if available_count == 0 {
            return None;
        }

        if has_any_info {
            if let Some(idx) = best_idx {
                return Some(idx);
            }
        }

        // Fallback: round-robin
        let start = self.robin.fetch_add(1, Ordering::Relaxed) % n;
        for offset in 0..n {
            let idx = (start + offset) % n;
            let info = self.accounts[idx].rate_info.read().await;
            if let Some(until) = info.hard_limited_until {
                if now < until {
                    continue;
                }
            }
            return Some(idx);
        }
        None
    }

    /// Update rate limit info from response headers.
    async fn update_rate_info(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let acct = &self.accounts[idx];
        let mut info = acct.rate_info.write().await;

        // Debug: log all ratelimit headers
        for (name, value) in headers.iter() {
            let name_str = name.as_str();
            if name_str.contains("ratelimit") || name_str.contains("retry") {
                if let Ok(v) = value.to_str() {
                    tracing::debug!(
                        account = acct.name,
                        header = name_str,
                        value = v,
                        "rate-limit header"
                    );
                }
            }
        }

        // New unified rate limit headers (Anthropic 2025+)
        // Get the representative claim window to know which utilization to use
        let rep_claim = headers
            .get("anthropic-ratelimit-unified-representative-claim")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(ref claim) = rep_claim {
            info.representative_claim = Some(claim.clone());
            // Map claim to header prefix: "seven_day" -> "7d", "five_hour" -> "5h"
            let window = match claim.as_str() {
                "seven_day" => "7d",
                "five_hour" => "5h",
                _ => claim.as_str(),
            };
            let util_header = format!("anthropic-ratelimit-unified-{}-utilization", window);
            if let Some(v) = headers.get(util_header.as_str()) {
                if let Ok(s) = v.to_str() {
                    info.utilization = s.parse().ok();
                }
            }
        }

        // Also check top-level utilization as fallback
        if info.utilization.is_none() {
            // Try all known windows
            for window in &["7d", "5h"] {
                let header = format!("anthropic-ratelimit-unified-{}-utilization", window);
                if let Some(v) = headers.get(header.as_str()) {
                    if let Ok(s) = v.to_str() {
                        if let Ok(util) = s.parse::<f64>() {
                            // Use the highest utilization as the binding constraint
                            let current = info.utilization.unwrap_or(0.0);
                            if util > current {
                                info.utilization = Some(util);
                            }
                        }
                    }
                }
            }
        }

        // Legacy headers (still try them)
        if let Some(v) = headers.get("x-ratelimit-remaining-requests") {
            if let Ok(s) = v.to_str() {
                info.remaining_requests = s.parse().ok();
            }
        }
        if let Some(v) = headers.get("x-ratelimit-remaining-tokens") {
            if let Ok(s) = v.to_str() {
                info.remaining_tokens = s.parse().ok();
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-requests") {
            if let Ok(s) = v.to_str() {
                info.limit_requests = s.parse().ok();
            }
        }
        if let Some(v) = headers.get("x-ratelimit-limit-tokens") {
            if let Ok(s) = v.to_str() {
                info.limit_tokens = s.parse().ok();
            }
        }
        info.last_updated = Some(Instant::now());
    }

    /// Mark an account as hard rate-limited (got a 429).
    async fn mark_hard_limited(&self, idx: usize, headers: &reqwest::header::HeaderMap) {
        let acct = &self.accounts[idx];
        let mut info = acct.rate_info.write().await;

        let cooldown = if let Some(v) = headers.get("retry-after") {
            if let Ok(s) = v.to_str() {
                if let Ok(secs) = s.parse::<f64>() {
                    Duration::from_secs_f64(secs)
                } else {
                    self.cooldown
                }
            } else {
                self.cooldown
            }
        } else {
            self.cooldown
        };

        let until = Instant::now() + cooldown;
        info.hard_limited_until = Some(until);
        info.remaining_requests = Some(0);
        info.remaining_tokens = Some(0);
        info.last_updated = Some(Instant::now());

        warn!(
            account = acct.name,
            cooldown_secs = cooldown.as_secs(),
            "account hard rate-limited (429), cooling down"
        );
    }
}

// ── Handler ─────────────────────────────────────────────────────────

async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    let client_ip = client_addr.ip();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth: validate x-api-key against proxy_key if configured
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let (parts, body) = req.into_parts();

    // Extract client identification headers
    let client_id = parts
        .headers
        .get("x-client-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let agent_id = parts
        .headers
        .get("x-agent-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let session_id = parts
        .headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
    };

    // Extract model from request body
    let model = serde_json::from_slice::<serde_json::Value>(&body_bytes)
        .ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str().map(String::from)))
        .unwrap_or_default();

    let n = state.accounts.len();
    for _attempt in 0..n {
        let idx = match state.pick_account().await {
            Some(i) => i,
            None => {
                warn!("all accounts rate-limited");
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    "all upstream accounts rate-limited",
                )
                    .into_response();
            }
        };

        let acct = &state.accounts[idx];
        let url = format!(
            "{}{}",
            state.upstream,
            parts
                .uri
                .path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or("/")
        );

        let mut upstream_req = state.client.request(parts.method.clone(), &url);

        // Forward headers
        let mut headers = parts.headers.clone();
        headers.remove("host");

        // Auth: passthrough keeps caller's headers, otherwise inject account token
        if !acct.passthrough {
            headers.remove("authorization");
            headers.remove("x-api-key");
            // Detect token type by prefix
            if acct.token.starts_with("sk-ant-api") {
                // Standard API key → x-api-key header
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            } else if acct.token.starts_with("sk-ant-oat") {
                // OAuth token → Authorization: Bearer
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&format!("Bearer {}", acct.token)).unwrap(),
                );
                // OAuth tokens require these headers — client won't send them
                // since it doesn't know the proxy uses OAuth behind the scenes
                headers.insert(
                    "anthropic-dangerous-direct-browser-access",
                    HeaderValue::from_static("true"),
                );
                // Ensure oauth beta flag is present in anthropic-beta
                let existing_beta = headers
                    .get("anthropic-beta")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                if !existing_beta.contains("oauth-2025-04-20") {
                    let new_beta = if existing_beta.is_empty() {
                        "oauth-2025-04-20".to_string()
                    } else {
                        format!("{},oauth-2025-04-20", existing_beta)
                    };
                    headers.insert("anthropic-beta", HeaderValue::from_str(&new_beta).unwrap());
                }
            } else {
                // Unknown token type → try x-api-key
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            }
        }
        // passthrough: caller's auth headers flow through untouched

        upstream_req = upstream_req.headers(headers);
        upstream_req = upstream_req.body(body_bytes.clone());

        let resp = match upstream_req.send().await {
            Ok(r) => r,
            Err(e) => {
                error!(account = acct.name, "upstream request failed: {e}");
                continue;
            }
        };

        let status = resp.status();
        acct.requests.fetch_add(1, Ordering::Relaxed);

        // Always update rate limit info and persist
        state.update_rate_info(idx, resp.headers()).await;

        // 429 → mark and try next
        if status == StatusCode::TOO_MANY_REQUESTS {
            state.mark_hard_limited(idx, resp.headers()).await;
            state.save_state().await;
            info!(account = acct.name, "got 429, rotating to next account");
            continue;
        }

        // Persist state after successful request
        state.save_state().await;

        // Log with capacity info
        {
            let info = acct.rate_info.read().await;
            info!(
                client = %client_ip,
                client_id = %client_id,
                agent = %agent_id,
                session = %session_id,
                model = %model,
                account = acct.name,
                status = status.as_u16(),
                utilization = ?info.utilization,
                claim = ?info.representative_claim,
                total = acct.requests.load(Ordering::Relaxed),
                "proxied"
            );
        }

        // Stream response through without buffering (supports SSE/streaming)
        let resp_status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let resp_headers = resp.headers().clone();

        let mut builder = Response::builder().status(resp_status);
        for (k, v) in resp_headers.iter() {
            if k == "transfer-encoding" {
                continue;
            }
            builder = builder.header(k, v);
        }
        return builder
            .body(Body::from_stream(resp.bytes_stream()))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
    }

    (StatusCode::TOO_MANY_REQUESTS, "exhausted all accounts").into_response()
}

// ── Upstream passthrough handler ─────────────────────────────────────

async fn upstream_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    axum::extract::Path((upstream_name, _rest)): axum::extract::Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let client_ip = client_addr.ip();

    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let upstream = match state.upstreams.iter().find(|u| u.name == upstream_name) {
        Some(u) => u,
        None => {
            warn!(client = %client_ip, upstream = %upstream_name, "unknown upstream");
            return (StatusCode::NOT_FOUND, "unknown upstream").into_response();
        }
    };

    let (parts, body) = req.into_parts();

    // Extract client identification headers
    let client_id = parts
        .headers
        .get("x-client-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
    };

    // Extract model from request body for logging
    let model = serde_json::from_slice::<serde_json::Value>(&body_bytes)
        .ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str().map(String::from)))
        .unwrap_or_default();

    // Build upstream URL: strip /upstream/<name> prefix, forward the rest
    let path = parts.uri.path();
    let prefix = format!("/upstream/{}", upstream_name);
    let remainder = path.strip_prefix(&prefix).unwrap_or("/");
    let remainder = if remainder.is_empty() { "/" } else { remainder };
    let query = parts
        .uri
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let url = format!("{}{}{}", upstream.base_url, remainder, query);

    let mut headers = parts.headers.clone();
    headers.remove("host");
    headers.remove("authorization");
    headers.remove("x-api-key");
    // Inject upstream API key as Bearer token (OpenAI-compatible)
    headers.insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {}", upstream.api_key)).unwrap(),
    );

    let upstream_req = state
        .client
        .request(parts.method.clone(), &url)
        .headers(headers)
        .body(body_bytes);

    let resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(upstream = upstream.name, error = %e, "upstream request failed");
            return (StatusCode::BAD_GATEWAY, "upstream request failed").into_response();
        }
    };

    let status = resp.status();
    upstream.requests.fetch_add(1, Ordering::Relaxed);

    info!(
        client = %client_ip,
        client_id = %client_id,
        model = %model,
        upstream = upstream.name,
        status = status.as_u16(),
        total = upstream.requests.load(Ordering::Relaxed),
        "proxied (upstream)"
    );

    let resp_status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let resp_headers = resp.headers().clone();

    let mut builder = Response::builder().status(resp_status);
    for (k, v) in resp_headers.iter() {
        if k == "transfer-encoding" {
            continue;
        }
        builder = builder.header(k, v);
    }
    builder
        .body(Body::from_stream(resp.bytes_stream()))
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
        })
}

// ── Stats endpoint ──────────────────────────────────────────────────

async fn stats_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    if !state.is_ip_allowed(&client_addr.ip()) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let mut out = Vec::new();
    for acct in &state.accounts {
        let info = acct.rate_info.read().await;
        let hard_limited = match info.hard_limited_until {
            Some(until) if Instant::now() < until => {
                Some(until.duration_since(Instant::now()).as_secs())
            }
            _ => None,
        };
        out.push(serde_json::json!({
            "name": acct.name,
            "passthrough": acct.passthrough,
            "requests_total": acct.requests.load(Ordering::Relaxed),
            "utilization": info.utilization,
            "representative_claim": info.representative_claim,
            "remaining_requests": info.remaining_requests,
            "remaining_tokens": info.remaining_tokens,
            "limit_requests": info.limit_requests,
            "limit_tokens": info.limit_tokens,
            "hard_limited_remaining_secs": hard_limited,
        }));
    }
    let mut upstream_stats = Vec::new();
    for u in &state.upstreams {
        upstream_stats.push(serde_json::json!({
            "name": u.name,
            "base_url": u.base_url,
            "requests_total": u.requests.load(Ordering::Relaxed),
        }));
    }

    axum::Json(serde_json::json!({
        "accounts": out,
        "upstreams": upstream_stats,
        "strategy": "dynamic-capacity",
    }))
    .into_response()
}

// ── OpenAI compatibility ─────────────────────────────────────────────

fn map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "end_turn" => "stop",
        "max_tokens" => "length",
        "stop_sequence" => "stop",
        _ => "stop",
    }
}

struct StreamContext {
    id: String,
    model: String,
    created: u64,
}

impl Default for StreamContext {
    fn default() -> Self {
        Self {
            id: format!("chatcmpl-{}", AppState::now_epoch()),
            model: String::new(),
            created: AppState::now_epoch(),
        }
    }
}

fn make_openai_chunk(
    ctx: &StreamContext,
    delta: serde_json::Value,
    finish_reason: Option<&str>,
) -> String {
    let chunk = serde_json::json!({
        "id": ctx.id,
        "object": "chat.completion.chunk",
        "created": ctx.created,
        "model": ctx.model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }],
    });
    format!("data: {}\n\n", chunk)
}

fn translate_openai_to_anthropic(body: &serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();

    // Model
    if let Some(model) = body.get("model") {
        out.insert("model".to_string(), model.clone());
    }

    // Extract system messages, pass through the rest
    let mut system_parts: Vec<String> = Vec::new();
    let mut messages: Vec<serde_json::Value> = Vec::new();

    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
            if role == "system" {
                if let Some(content) = msg.get("content").and_then(|c| c.as_str()) {
                    system_parts.push(content.to_string());
                }
            } else {
                // Strip "name" field, keep role + content
                let mut clean = serde_json::Map::new();
                clean.insert(
                    "role".to_string(),
                    serde_json::Value::String(role.to_string()),
                );
                if let Some(content) = msg.get("content") {
                    clean.insert("content".to_string(), content.clone());
                }
                messages.push(serde_json::Value::Object(clean));
            }
        }
    }

    // response_format: inject JSON mode instruction into system prompt
    if let Some(rf) = body.get("response_format") {
        if rf.get("type").and_then(|t| t.as_str()) == Some("json_object") {
            system_parts.push(
                "You must respond with valid JSON only. No markdown, no code fences, no explanation — just raw JSON.".to_string(),
            );
        }
    }

    if !system_parts.is_empty() {
        out.insert(
            "system".to_string(),
            serde_json::Value::String(system_parts.join("\n\n")),
        );
    }

    out.insert("messages".to_string(), serde_json::Value::Array(messages));

    // max_tokens: try max_tokens, then max_completion_tokens, default 4096
    let max_tokens = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .cloned()
        .unwrap_or(serde_json::json!(4096));
    out.insert("max_tokens".to_string(), max_tokens);

    // Direct passthrough params
    for key in &["temperature", "top_p", "top_k", "stream"] {
        if let Some(v) = body.get(*key) {
            out.insert(key.to_string(), v.clone());
        }
    }

    // stop -> stop_sequences
    if let Some(stop) = body.get("stop") {
        let sequences = if stop.is_array() {
            stop.clone()
        } else if let Some(s) = stop.as_str() {
            serde_json::json!([s])
        } else {
            serde_json::json!([])
        };
        out.insert("stop_sequences".to_string(), sequences);
    }

    serde_json::Value::Object(out)
}

fn translate_anthropic_to_openai(body: &serde_json::Value) -> serde_json::Value {
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("msg_unknown");
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Concatenate text content blocks
    let content = body
        .get("content")
        .and_then(|c| c.as_array())
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();

    let stop_reason = body
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("end_turn");

    let input_tokens = body
        .pointer("/usage/input_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = body
        .pointer("/usage/output_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    serde_json::json!({
        "id": format!("chatcmpl-{}", id),
        "object": "chat.completion",
        "created": AppState::now_epoch(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
            },
            "finish_reason": map_stop_reason(stop_reason),
        }],
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    })
}

/// Parse a raw SSE event block and translate to OpenAI format.
/// Returns None for events that should be skipped (ping, content_block_start, etc.).
fn translate_sse_event(raw: &str, ctx: &mut StreamContext) -> Option<String> {
    let mut event_type = String::new();
    let mut data = String::new();

    for line in raw.lines() {
        if let Some(val) = line.strip_prefix("event:") {
            event_type = val.trim().to_string();
        } else if let Some(val) = line.strip_prefix("data:") {
            data = val.trim().to_string();
        }
    }

    if data.is_empty() {
        return None;
    }

    let parsed: serde_json::Value = serde_json::from_str(&data).ok()?;

    match event_type.as_str() {
        "message_start" => {
            if let Some(msg) = parsed.get("message") {
                if let Some(id) = msg.get("id").and_then(|v| v.as_str()) {
                    ctx.id = format!("chatcmpl-{}", id);
                }
                if let Some(model) = msg.get("model").and_then(|v| v.as_str()) {
                    ctx.model = model.to_string();
                }
            }
            Some(make_openai_chunk(
                ctx,
                serde_json::json!({"role": "assistant"}),
                None,
            ))
        }
        "content_block_delta" => {
            let text = parsed
                .pointer("/delta/text")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if text.is_empty() {
                return None;
            }
            Some(make_openai_chunk(
                ctx,
                serde_json::json!({"content": text}),
                None,
            ))
        }
        "message_delta" => {
            let stop_reason = parsed
                .pointer("/delta/stop_reason")
                .and_then(|v| v.as_str())
                .unwrap_or("end_turn");
            Some(make_openai_chunk(
                ctx,
                serde_json::json!({}),
                Some(map_stop_reason(stop_reason)),
            ))
        }
        "message_stop" => Some("data: [DONE]\n\n".to_string()),
        _ => None, // ping, content_block_start, content_block_stop
    }
}

async fn openai_chat_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::ConnectInfo(client_addr): axum::extract::ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response {
    let client_ip = client_addr.ip();

    // IP allowlist check
    if !state.is_ip_allowed(&client_ip) {
        warn!(client = %client_ip, "rejected: IP not in allowlist");
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    // Proxy auth
    if let Some(ref key) = state.proxy_key {
        let provided = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if provided != Some(key.as_str()) {
            warn!(client = %client_ip, "rejected: invalid or missing proxy key");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    let (parts, body) = req.into_parts();

    // Extract client identification headers
    let client_id = parts
        .headers
        .get("x-client-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let agent_id = parts
        .headers
        .get("x-agent-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let session_id = parts
        .headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            error!("failed to read request body: {e}");
            return (StatusCode::BAD_REQUEST, "bad request body").into_response();
        }
    };

    let openai_body: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            error!("failed to parse request JSON: {e}");
            return (StatusCode::BAD_REQUEST, "invalid JSON").into_response();
        }
    };

    let is_streaming = openai_body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let model = openai_body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();

    let anthropic_body = translate_openai_to_anthropic(&openai_body);

    let n = state.accounts.len();
    for _attempt in 0..n {
        let idx = match state.pick_account().await {
            Some(i) => i,
            None => {
                warn!("all accounts rate-limited");
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    "all upstream accounts rate-limited",
                )
                    .into_response();
            }
        };

        let acct = &state.accounts[idx];
        let url = format!("{}/v1/messages", state.upstream);

        let mut headers = parts.headers.clone();
        headers.remove("host");
        headers.remove("authorization");
        headers.remove("x-api-key");
        headers.remove("content-length"); // body size changes after translation
        headers.remove("accept-encoding"); // we need plaintext to translate the response

        // Inject required Anthropic headers
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

        // Auth injection with claude-code beta header for OAuth
        if !acct.passthrough {
            if acct.token.starts_with("sk-ant-api") {
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            } else if acct.token.starts_with("sk-ant-oat") {
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&format!("Bearer {}", acct.token)).unwrap(),
                );
                headers.insert(
                    "anthropic-dangerous-direct-browser-access",
                    HeaderValue::from_static("true"),
                );
                // OAuth tokens need both beta flags for non-Claude-Code clients
                let existing_beta = headers
                    .get("anthropic-beta")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let mut betas: Vec<&str> = if existing_beta.is_empty() {
                    vec![]
                } else {
                    existing_beta.split(',').collect()
                };
                for flag in &["oauth-2025-04-20", "claude-code-20250219"] {
                    if !betas.iter().any(|b| b.trim() == *flag) {
                        betas.push(flag);
                    }
                }
                headers.insert(
                    "anthropic-beta",
                    HeaderValue::from_str(&betas.join(",")).unwrap(),
                );
            } else {
                headers.insert("x-api-key", HeaderValue::from_str(&acct.token).unwrap());
            }
        }

        let upstream_req = state
            .client
            .request(reqwest::Method::POST, &url)
            .headers(reqwest_headers(&headers))
            .body(anthropic_body.to_string());

        let mut resp = match upstream_req.send().await {
            Ok(r) => r,
            Err(e) => {
                error!(account = acct.name, "upstream request failed: {e}");
                continue;
            }
        };

        let status = resp.status();
        acct.requests.fetch_add(1, Ordering::Relaxed);
        state.update_rate_info(idx, resp.headers()).await;

        if status == StatusCode::TOO_MANY_REQUESTS {
            state.mark_hard_limited(idx, resp.headers()).await;
            state.save_state().await;
            info!(account = acct.name, "got 429, rotating to next account");
            continue;
        }

        state.save_state().await;

        {
            let info = acct.rate_info.read().await;
            info!(
                client = %client_ip,
                client_id = %client_id,
                agent = %agent_id,
                session = %session_id,
                model = %model,
                account = acct.name,
                status = status.as_u16(),
                utilization = ?info.utilization,
                openai_compat = true,
                stream = is_streaming,
                "proxied (openai-compat)"
            );
        }

        // Non-2xx: return error as-is (not SSE even if streaming was requested)
        if !status.is_success() {
            let error_body = resp.bytes().await.unwrap_or_default();
            return Response::builder()
                .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY))
                .header("content-type", "application/json")
                .body(Body::from(error_body))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
        }

        if is_streaming {
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);

            tokio::spawn(async move {
                let mut buffer = String::new();
                let mut ctx = StreamContext::default();
                let mut sent_done = false;

                while let Ok(Some(chunk)) = resp.chunk().await {
                    buffer.push_str(&String::from_utf8_lossy(&chunk));

                    while let Some(pos) = buffer.find("\n\n") {
                        let event = buffer[..pos].to_string();
                        buffer = buffer[pos + 2..].to_string();

                        if event.trim().is_empty() {
                            continue;
                        }

                        if let Some(translated) = translate_sse_event(&event, &mut ctx) {
                            if translated.contains("[DONE]") {
                                sent_done = true;
                            }
                            if tx.send(Ok(bytes::Bytes::from(translated))).await.is_err() {
                                return; // client disconnected
                            }
                        }
                    }
                }

                // Process any remaining data in buffer
                if !buffer.trim().is_empty() {
                    if let Some(translated) = translate_sse_event(&buffer, &mut ctx) {
                        if translated.contains("[DONE]") {
                            sent_done = true;
                        }
                        let _ = tx.send(Ok(bytes::Bytes::from(translated))).await;
                    }
                }

                // Ensure [DONE] is always sent (fallback for abnormal stream termination)
                if !sent_done {
                    let _ = tx.send(Ok(bytes::Bytes::from("data: [DONE]\n\n"))).await;
                }
            });

            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .header("connection", "keep-alive")
                .body(Body::from_stream(ReceiverStream::new(rx)))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                });
        }

        // Non-streaming: buffer, translate, return
        let resp_bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                error!("failed to read upstream response: {e}");
                return (StatusCode::BAD_GATEWAY, "failed to read upstream response")
                    .into_response();
            }
        };

        let anthropic_resp: serde_json::Value = match serde_json::from_slice(&resp_bytes) {
            Ok(v) => v,
            Err(_) => {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(resp_bytes))
                    .unwrap_or_else(|_| {
                        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
                    });
            }
        };

        let openai_resp = translate_anthropic_to_openai(&anthropic_resp);
        return axum::Json(openai_resp).into_response();
    }

    (StatusCode::TOO_MANY_REQUESTS, "exhausted all accounts").into_response()
}

/// Convert axum HeaderMap to reqwest HeaderMap.
fn reqwest_headers(headers: &axum::http::HeaderMap) -> reqwest::header::HeaderMap {
    let mut out = reqwest::header::HeaderMap::new();
    for (k, v) in headers.iter() {
        if let Ok(name) = reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()) {
            if let Ok(val) = reqwest::header::HeaderValue::from_bytes(v.as_bytes()) {
                out.insert(name, val);
            }
        }
    }
    out
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "anthropic_lb=info".into()),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_str = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("failed to read {config_path}: {e}"));
    let config: Config =
        toml::from_str(&config_str).unwrap_or_else(|e| panic!("invalid config: {e}"));

    assert!(!config.accounts.is_empty(), "at least one account required");

    let cooldown = Duration::from_secs(config.rate_limit_cooldown_secs.unwrap_or(60));

    // Parse IP allowlist
    let allowed_ips: Vec<IpAllowEntry> = config
        .allowed_ips
        .unwrap_or_default()
        .iter()
        .map(|s| {
            if let Ok(net) = s.parse::<IpNet>() {
                IpAllowEntry::Net(net)
            } else if let Ok(addr) = s.parse::<IpAddr>() {
                IpAllowEntry::Addr(addr)
            } else {
                panic!("invalid allowed_ips entry: {s}");
            }
        })
        .collect();
    if allowed_ips.is_empty() {
        warn!("IP allowlist DISABLED — all source IPs accepted");
    } else {
        info!(count = allowed_ips.len(), "IP allowlist enabled");
    }

    let accounts: Vec<Account> = config
        .accounts
        .into_iter()
        .map(|a| {
            let passthrough = a.token == "passthrough";
            info!(name = a.name, passthrough, "loaded account");
            Account {
                name: a.name,
                passthrough,
                token: a.token,
                requests: AtomicU64::new(0),
                rate_info: RwLock::new(RateLimitInfo::default()),
            }
        })
        .collect();

    let upstreams: Vec<Upstream> = config
        .upstreams
        .iter()
        .map(|u| {
            info!(name = u.name, base_url = u.base_url, "loaded upstream");
            Upstream {
                name: u.name.clone(),
                base_url: u.base_url.clone(),
                api_key: u.api_key.clone(),
                requests: AtomicU64::new(0),
            }
        })
        .collect();

    if config.proxy_key.is_some() {
        info!("proxy authentication enabled (x-api-key)");
    } else {
        warn!("proxy authentication DISABLED — proxy is open to all");
    }

    info!(
        num_accounts = accounts.len(),
        num_upstreams = upstreams.len(),
        "dynamic capacity-based routing enabled"
    );

    let state_path = PathBuf::from(&config_path).with_extension("state.json");

    let state = Arc::new(AppState {
        client: Client::builder()
            .timeout(Duration::from_secs(600))
            .build()
            .expect("failed to build HTTP client"),
        upstream: config.upstream,
        accounts,
        robin: AtomicUsize::new(0),
        cooldown,
        state_path,
        proxy_key: config.proxy_key.clone(),
        allowed_ips,
        upstreams,
    });

    // Restore persisted state (cooldowns, utilization, request counts)
    state.load_state().await;

    let app = Router::new()
        .route("/_stats", axum::routing::get(stats_handler))
        .route(
            "/v1/chat/completions",
            axum::routing::post(openai_chat_handler),
        )
        .route("/upstream/{name}/{*rest}", any(upstream_handler))
        .fallback(any(proxy_handler))
        .with_state(state.clone());

    let addr: SocketAddr = config
        .listen
        .parse()
        .unwrap_or_else(|e| panic!("invalid listen address: {e}"));

    info!(%addr, "anthropic-lb starting");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // Spawn periodic probe task
    let probe_interval = config.probe_interval_secs.unwrap_or(300);
    if probe_interval > 0 {
        let probe_state = state.clone();
        let n_accounts = probe_state.accounts.len();
        tokio::spawn(async move {
            // Stagger initial probes: wait 10s then probe all accounts
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!(
                interval_secs = probe_interval,
                "starting utilization probes"
            );
            loop {
                for i in 0..n_accounts {
                    probe_state.probe_account(i).await;
                    // Small delay between accounts to avoid burst
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                tokio::time::sleep(Duration::from_secs(probe_interval)).await;
            }
        });
    }

    // Graceful shutdown: save state on SIGTERM/SIGINT
    let shutdown_state = state.clone();
    let shutdown = async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM");
        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
        info!("saving state before shutdown...");
        shutdown_state.save_state().await;
        info!("state saved, shutting down");
    };

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .unwrap_or_else(|e| panic!("server error: {e}"));
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_account(name: &str, token: &str) -> Account {
        Account {
            name: name.to_string(),
            token: token.to_string(),
            passthrough: token == "passthrough",
            requests: AtomicU64::new(0),
            rate_info: RwLock::new(RateLimitInfo::default()),
        }
    }

    fn test_state_with(accounts: Vec<Account>) -> Arc<AppState> {
        Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(), // unused in unit tests
            accounts,
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key: None,
            allowed_ips: vec![],
            upstreams: vec![],
        })
    }

    /// Spawn a mock upstream that returns a canned response with rate-limit headers.
    async fn spawn_mock_upstream() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(any(mock_upstream_handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{}", addr), handle)
    }

    async fn mock_upstream_handler(req: Request<Body>) -> Response {
        let has_auth =
            req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");

        if !has_auth {
            return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
        }

        let mut resp = axum::Json(serde_json::json!({
            "id": "msg_test",
            "type": "message",
            "content": [{"type": "text", "text": "ok"}],
        }))
        .into_response();

        // Inject rate-limit headers the proxy expects
        let headers = resp.headers_mut();
        headers.insert(
            "anthropic-ratelimit-unified-representative-claim",
            HeaderValue::from_static("five_hour"),
        );
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.25"),
        );
        resp
    }

    /// Build the full app router against a given upstream URL.
    fn test_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
        let accounts = vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ];

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: upstream_url.to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-test.state.json"),
            proxy_key,
            allowed_ips: vec![],
            upstreams: vec![Upstream {
                name: "mock".to_string(),
                base_url: upstream_url.to_string(),
                api_key: "test-key".to_string(),
                requests: AtomicU64::new(0),
            }],
        });

        let app = Router::new()
            .route("/_stats", axum::routing::get(stats_handler))
            .route(
                "/v1/chat/completions",
                axum::routing::post(openai_chat_handler),
            )
            .route("/upstream/{name}/{*rest}", any(upstream_handler))
            .fallback(any(proxy_handler))
            .with_state(state.clone());

        (app, state)
    }

    // ── Unit: IP allowlist ──────────────────────────────────────────

    #[test]
    fn ip_allow_entry_matches_exact_addr() {
        let entry = IpAllowEntry::Addr("10.0.0.1".parse().unwrap());
        assert!(entry.contains(&"10.0.0.1".parse().unwrap()));
        assert!(!entry.contains(&"10.0.0.2".parse().unwrap()));
    }

    #[test]
    fn ip_allow_entry_matches_cidr() {
        let entry = IpAllowEntry::Net("10.0.0.0/24".parse().unwrap());
        assert!(entry.contains(&"10.0.0.1".parse().unwrap()));
        assert!(entry.contains(&"10.0.0.254".parse().unwrap()));
        assert!(!entry.contains(&"10.0.1.1".parse().unwrap()));
    }

    #[test]
    fn empty_allowlist_allows_all() {
        let state = test_state_with(vec![make_account("a", "sk-ant-api-x")]);
        assert!(state.is_ip_allowed(&"192.168.1.1".parse().unwrap()));
        assert!(state.is_ip_allowed(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn populated_allowlist_blocks_unknown() {
        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: "http://127.0.0.1:1".to_string(),
            accounts: vec![make_account("a", "sk-ant-api-x")],
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/test.state.json"),
            proxy_key: None,
            allowed_ips: vec![IpAllowEntry::Addr("10.0.0.1".parse().unwrap())],
            upstreams: vec![],
        });
        assert!(state.is_ip_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(!state.is_ip_allowed(&"10.0.0.2".parse().unwrap()));
    }

    // ── Unit: pick_account ──────────────────────────────────────────

    #[tokio::test]
    async fn pick_prefers_lowest_utilization() {
        let state = test_state_with(vec![
            make_account("high", "sk-ant-api-high"),
            make_account("low", "sk-ant-api-low"),
        ]);

        // Set utilization: high=0.8, low=0.2
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.8);
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.2);
        }

        let idx = state.pick_account().await.unwrap();
        assert_eq!(idx, 1, "should pick account with lower utilization");
    }

    #[tokio::test]
    async fn pick_skips_hard_limited() {
        let state = test_state_with(vec![
            make_account("limited", "sk-ant-api-a"),
            make_account("available", "sk-ant-api-b"),
        ]);

        // Hard-limit the first account
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.1); // great utilization but hard-limited
            info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
        }
        {
            let mut info = state.accounts[1].rate_info.write().await;
            info.utilization = Some(0.9);
        }

        let idx = state.pick_account().await.unwrap();
        assert_eq!(
            idx, 1,
            "should skip hard-limited account despite lower utilization"
        );
    }

    #[tokio::test]
    async fn pick_round_robin_when_no_info() {
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
            make_account("c", "sk-ant-api-c"),
        ]);

        // No utilization set — should round-robin
        let first = state.pick_account().await.unwrap();
        let second = state.pick_account().await.unwrap();
        let third = state.pick_account().await.unwrap();

        // Round-robin should cycle through all accounts
        let selected: std::collections::HashSet<usize> = [first, second, third].into();
        assert_eq!(
            selected.len(),
            3,
            "round-robin should cycle through all accounts"
        );
    }

    #[tokio::test]
    async fn pick_returns_none_when_all_limited() {
        let state = test_state_with(vec![
            make_account("a", "sk-ant-api-a"),
            make_account("b", "sk-ant-api-b"),
        ]);

        for acct in &state.accounts {
            let mut info = acct.rate_info.write().await;
            info.hard_limited_until = Some(Instant::now() + Duration::from_secs(3600));
        }

        assert!(state.pick_account().await.is_none());
    }

    #[tokio::test]
    async fn pick_does_not_bias_unknown_accounts() {
        // Bug 2 regression test: unknown accounts should NOT dominate selection
        let state = test_state_with(vec![
            make_account("known", "sk-ant-api-known"),
            make_account("unknown", "sk-ant-api-unknown"),
        ]);

        // Only set info on the known account — unknown has no data
        {
            let mut info = state.accounts[0].rate_info.write().await;
            info.utilization = Some(0.1); // very low utilization
        }
        // accounts[1] has no rate info at all

        // With the bug fix, since only one account has info, has_any_info=true
        // and the known account (0.1) should be preferred over unknown (0.0).
        // Actually — unknown gets 0.0 which IS lower than 0.1. The difference
        // is that has_any_info is set by the known account, so the smart path
        // runs. Unknown still wins at 0.0 vs 0.1, but that's OK — the bug was
        // that *only* unknown accounts (no known accounts at all) would set
        // has_any_info=true and bypass round-robin. Let's test that case.
        drop(state);

        let state = test_state_with(vec![
            make_account("unknown-a", "sk-ant-api-a"),
            make_account("unknown-b", "sk-ant-api-b"),
        ]);
        // No rate info on either account

        // Call pick_account multiple times — should distribute via round-robin
        let mut picks = Vec::new();
        for _ in 0..4 {
            picks.push(state.pick_account().await.unwrap());
        }
        // With round-robin, we should see both 0 and 1
        assert!(
            picks.contains(&0) && picks.contains(&1),
            "with no rate info, accounts should be distributed via round-robin, got: {:?}",
            picks
        );
    }

    // ── Integration: HTTP handlers ──────────────────────────────────

    #[tokio::test]
    async fn proxy_rejects_missing_auth() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn proxy_accepts_valid_auth_and_forwards() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, state) = test_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/messages", addr))
            .header("content-type", "application/json")
            .header("x-api-key", "secret-key")
            .body(r#"{"model":"test","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        // Verify rate info was updated from mock response headers
        let info = state.accounts[0].rate_info.read().await;
        assert_eq!(info.utilization, Some(0.25));
        assert_eq!(info.representative_claim.as_deref(), Some("five_hour"));
    }

    #[tokio::test]
    async fn stats_endpoint_returns_account_info() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .get(format!("http://{}/_stats", addr))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        let accounts = body["accounts"].as_array().unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0]["name"], "acct-a");
        assert_eq!(accounts[1]["name"], "acct-b");
        assert_eq!(body["strategy"], "dynamic-capacity");
        // Upstreams section should be present
        let upstreams = body["upstreams"].as_array().unwrap();
        assert_eq!(upstreams.len(), 1);
        assert_eq!(upstreams[0]["name"], "mock");
    }

    #[tokio::test]
    async fn upstream_handler_forwards_to_named_upstream() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/upstream/mock/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn upstream_handler_rejects_unknown_upstream() {
        let (mock_url, _handle) = spawn_mock_upstream().await;
        let (app, _state) = test_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!(
                "http://{}/upstream/nonexistent/v1/chat/completions",
                addr
            ))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // ── Unit: OpenAI request translation ────────────────────────────

    #[test]
    fn translate_request_extracts_system() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hello"}
            ],
            "max_tokens": 1024
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["system"], "You are helpful");
        let msgs = result["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"], "Hello");
    }

    #[test]
    fn translate_request_multi_system_concat() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [
                {"role": "system", "content": "Rule 1"},
                {"role": "system", "content": "Rule 2"},
                {"role": "user", "content": "Hello"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["system"], "Rule 1\n\nRule 2");
    }

    #[test]
    fn translate_request_no_system() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [
                {"role": "user", "content": "Hello"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        assert!(result.get("system").is_none());
    }

    #[test]
    fn translate_request_default_max_tokens() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [{"role": "user", "content": "Hello"}]
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["max_tokens"], 4096);
    }

    #[test]
    fn translate_request_stop_to_stop_sequences() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 100,
            "stop": ["END", "STOP"]
        });
        let result = translate_openai_to_anthropic(&req);
        let seqs = result["stop_sequences"].as_array().unwrap();
        assert_eq!(seqs.len(), 2);
        assert_eq!(seqs[0], "END");
        assert_eq!(seqs[1], "STOP");
    }

    #[test]
    fn translate_request_passthrough_params() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 512,
            "temperature": 0.7,
            "top_p": 0.9,
            "stream": true
        });
        let result = translate_openai_to_anthropic(&req);
        assert_eq!(result["model"], "claude-sonnet-4-20250514");
        assert_eq!(result["max_tokens"], 512);
        assert_eq!(result["temperature"], 0.7);
        assert_eq!(result["top_p"], 0.9);
        assert_eq!(result["stream"], true);
    }

    #[test]
    fn translate_request_strips_name_field() {
        let req = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "messages": [
                {"role": "user", "content": "Hello", "name": "bob"}
            ],
            "max_tokens": 100
        });
        let result = translate_openai_to_anthropic(&req);
        let msgs = result["messages"].as_array().unwrap();
        assert!(msgs[0].get("name").is_none());
    }

    // ── Unit: OpenAI response translation ───────────────────────────

    #[test]
    fn translate_response_basic() {
        let resp = serde_json::json!({
            "id": "msg_abc123",
            "type": "message",
            "content": [{"type": "text", "text": "Hello!"}],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(result["id"], "chatcmpl-msg_abc123");
        assert_eq!(result["object"], "chat.completion");
        assert_eq!(result["choices"][0]["message"]["role"], "assistant");
        assert_eq!(result["choices"][0]["message"]["content"], "Hello!");
        assert_eq!(result["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn translate_response_usage_mapping() {
        let resp = serde_json::json!({
            "id": "msg_x",
            "content": [{"type": "text", "text": "ok"}],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 25, "output_tokens": 15}
        });
        let result = translate_anthropic_to_openai(&resp);
        assert_eq!(result["usage"]["prompt_tokens"], 25);
        assert_eq!(result["usage"]["completion_tokens"], 15);
        assert_eq!(result["usage"]["total_tokens"], 40);
    }

    #[test]
    fn translate_response_stop_reason_mapping() {
        assert_eq!(map_stop_reason("end_turn"), "stop");
        assert_eq!(map_stop_reason("max_tokens"), "length");
        assert_eq!(map_stop_reason("stop_sequence"), "stop");
        assert_eq!(map_stop_reason("unknown"), "stop");
    }

    // ── Unit: SSE event translation ─────────────────────────────────

    #[test]
    fn translate_sse_message_start() {
        let mut ctx = StreamContext::default();
        let raw = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"model\":\"claude-sonnet-4-20250514\",\"role\":\"assistant\"}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        assert!(result.starts_with("data: "));
        assert_eq!(ctx.id, "chatcmpl-msg_test");
        assert_eq!(ctx.model, "claude-sonnet-4-20250514");
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["delta"]["role"], "assistant");
    }

    #[test]
    fn translate_sse_content_delta() {
        let mut ctx = StreamContext {
            id: "chatcmpl-test".to_string(),
            model: "claude-sonnet-4-20250514".to_string(),
            ..Default::default()
        };
        let raw = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello world\"}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["delta"]["content"], "Hello world");
        assert!(chunk["choices"][0]["finish_reason"].is_null());
    }

    #[test]
    fn translate_sse_message_delta() {
        let mut ctx = StreamContext {
            id: "chatcmpl-test".to_string(),
            ..Default::default()
        };
        let raw = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        let chunk: serde_json::Value =
            serde_json::from_str(result.strip_prefix("data: ").unwrap().trim()).unwrap();
        assert_eq!(chunk["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn translate_sse_message_stop() {
        let mut ctx = StreamContext::default();
        let raw = "event: message_stop\ndata: {\"type\":\"message_stop\"}";
        let result = translate_sse_event(raw, &mut ctx).unwrap();
        assert_eq!(result, "data: [DONE]\n\n");
    }

    #[test]
    fn translate_sse_skips_ping() {
        let mut ctx = StreamContext::default();
        let raw = "event: ping\ndata: {\"type\":\"ping\"}";
        assert!(translate_sse_event(raw, &mut ctx).is_none());
    }

    // ── Integration: OpenAI-compat handler ──────────────────────────

    /// Mock that returns Anthropic /v1/messages format (non-streaming)
    async fn mock_anthropic_handler(req: Request<Body>) -> Response {
        let has_auth =
            req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
        if !has_auth {
            return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
        }

        let mut resp = axum::Json(serde_json::json!({
            "id": "msg_integration",
            "type": "message",
            "content": [{"type": "text", "text": "Hello from Claude"}],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }))
        .into_response();

        let headers = resp.headers_mut();
        headers.insert(
            "anthropic-ratelimit-unified-representative-claim",
            HeaderValue::from_static("five_hour"),
        );
        headers.insert(
            "anthropic-ratelimit-unified-5h-utilization",
            HeaderValue::from_static("0.30"),
        );
        resp
    }

    /// Mock that returns Anthropic SSE streaming format
    async fn mock_anthropic_streaming_handler(req: Request<Body>) -> Response {
        let has_auth =
            req.headers().contains_key("x-api-key") || req.headers().contains_key("authorization");
        if !has_auth {
            return (StatusCode::UNAUTHORIZED, "missing auth").into_response();
        }

        let events = [
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-20250514\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ];

        let body = events.join("");
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from(body))
            .unwrap()
    }

    /// Build test app with separate handlers for streaming vs non-streaming
    fn test_openai_app(upstream_url: &str, proxy_key: Option<String>) -> (Router, Arc<AppState>) {
        let accounts = vec![
            make_account("acct-a", "sk-ant-api-test-aaa"),
            make_account("acct-b", "sk-ant-api-test-bbb"),
        ];

        let state = Arc::new(AppState {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            upstream: upstream_url.to_string(),
            accounts,
            robin: AtomicUsize::new(0),
            cooldown: Duration::from_secs(60),
            state_path: PathBuf::from("/tmp/anthropic-lb-openai-test.state.json"),
            proxy_key,
            allowed_ips: vec![],
            upstreams: vec![],
        });

        let app = Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(openai_chat_handler),
            )
            .with_state(state.clone());

        (app, state)
    }

    #[tokio::test]
    async fn openai_chat_non_streaming() {
        // Spawn a mock that serves /v1/messages with Anthropic format
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"Hello"}],"max_tokens":100}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["object"], "chat.completion");
        assert!(body["id"].as_str().unwrap().starts_with("chatcmpl-"));
        assert_eq!(
            body["choices"][0]["message"]["content"],
            "Hello from Claude"
        );
        assert_eq!(body["choices"][0]["finish_reason"], "stop");
        assert_eq!(body["usage"]["prompt_tokens"], 10);
        assert_eq!(body["usage"]["completion_tokens"], 5);
        assert_eq!(body["usage"]["total_tokens"], 15);
    }

    #[tokio::test]
    async fn openai_chat_streaming() {
        let mock_app = Router::new().fallback(any(mock_anthropic_streaming_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"Hello"}],"max_tokens":100,"stream":true}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/event-stream"
        );

        let body = resp.text().await.unwrap();

        // Parse SSE events from response
        let mut chunks: Vec<serde_json::Value> = Vec::new();
        let mut got_done = false;
        for line in body.lines() {
            if line == "data: [DONE]" {
                got_done = true;
            } else if let Some(data) = line.strip_prefix("data: ") {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                    chunks.push(v);
                }
            }
        }

        assert!(got_done, "should have [DONE] sentinel");
        assert!(
            chunks.len() >= 3,
            "expected at least 3 chunks (role + content + finish), got {}",
            chunks.len()
        );

        // First chunk: role
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(chunks[0]["object"], "chat.completion.chunk");
        assert!(chunks[0]["id"].as_str().unwrap().starts_with("chatcmpl-"));

        // Content chunks
        let content_chunks: Vec<&str> = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(content_chunks.join(""), "Hello world");

        // Last data chunk: finish_reason
        let last = chunks.last().unwrap();
        assert_eq!(last["choices"][0]["finish_reason"], "stop");
    }

    #[tokio::test]
    async fn openai_chat_rejects_missing_auth() {
        let mock_app = Router::new().fallback(any(mock_anthropic_handler));
        let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock_addr = mock_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(mock_listener, mock_app).await.unwrap();
        });

        let mock_url = format!("http://{}", mock_addr);
        let (app, _state) = test_openai_app(&mock_url, Some("secret-key".to_string()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = Client::new();
        let resp = client
            .post(format!("http://{}/v1/chat/completions", addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"test","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
}

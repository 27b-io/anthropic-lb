use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
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
    accounts: Vec<AccountConfig>,
}

#[derive(Deserialize, Clone)]
struct AccountConfig {
    name: String,
    /// Auth token. Use "passthrough" to forward caller's auth headers as-is.
    token: String,
}

// ── Runtime state ───────────────────────────────────────────────────

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

impl Default for RateLimitInfo {
    fn default() -> Self {
        Self {
            remaining_requests: None,
            remaining_tokens: None,
            limit_requests: None,
            limit_tokens: None,
            utilization: None,
            representative_claim: None,
            hard_limited_until: None,
            last_updated: None,
        }
    }
}

struct Account {
    name: String,
    token: String,
    passthrough: bool,
    requests: AtomicU64,
    rate_info: RwLock<RateLimitInfo>,
}

struct AppState {
    client: Client,
    upstream: String,
    accounts: Vec<Account>,
    robin: AtomicUsize,
    cooldown: Duration,
    state_path: PathBuf,
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
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
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
                if let Err(e) = std::fs::write(&self.state_path, json) {
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
            debug!(account = acct.name, "skipping probe for passthrough account");
            return;
        }

        // Check if hard-limited — don't waste a request
        {
            let info = acct.rate_info.read().await;
            if let Some(until) = info.hard_limited_until {
                if Instant::now() < until {
                    debug!(account = acct.name, "skipping probe, account is hard-limited");
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

        let mut req = self.client.post(&url)
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
                info!(account = acct.name, status = status.as_u16(), "probe complete");
            }
            Err(e) => {
                warn!(account = acct.name, error = %e, "probe failed");
            }
        }
    }

    async fn load_state(&self) {
        let data = match std::fs::read_to_string(&self.state_path) {
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
                        info.hard_limited_until = Some(now_instant + Duration::from_secs(remaining_secs));
                        info!(
                            account = pa.name,
                            remaining_secs,
                            "restored hard limit from persisted state"
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
                // Unknown utilization = assume fresh (0.0), prefer it
                has_any_info = true;
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
                    tracing::debug!(account = acct.name, header = name_str, value = v, "rate-limit header");
                }
            }
        }

        // New unified rate limit headers (Anthropic 2025+)
        // Get the representative claim window to know which utilization to use
        let rep_claim = headers.get("anthropic-ratelimit-unified-representative-claim")
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
    let client_ip = client_addr.ip().to_string();
    let (parts, body) = req.into_parts();
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
                headers.insert(
                    "x-api-key",
                    HeaderValue::from_str(&acct.token).unwrap(),
                );
            } else if acct.token.starts_with("sk-ant-oat") {
                // OAuth token → Authorization: Bearer (stealth headers from caller pass through)
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&format!("Bearer {}", acct.token)).unwrap(),
                );
            } else {
                // Unknown token type → try x-api-key
                headers.insert(
                    "x-api-key",
                    HeaderValue::from_str(&acct.token).unwrap(),
                );
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
                model = %model,
                account = acct.name,
                status = status.as_u16(),
                utilization = ?info.utilization,
                claim = ?info.representative_claim,
                total = acct.requests.load(Ordering::Relaxed),
                "proxied"
            );
        }

        // Return response
        let resp_status =
            StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let resp_headers = resp.headers().clone();
        let resp_bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                error!("failed to read upstream response: {e}");
                return (StatusCode::BAD_GATEWAY, "upstream read error").into_response();
            }
        };

        let mut builder = Response::builder().status(resp_status);
        for (k, v) in resp_headers.iter() {
            if k == "transfer-encoding" {
                continue;
            }
            builder = builder.header(k, v);
        }
        return builder
            .body(Body::from(resp_bytes))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
            });
    }

    (StatusCode::TOO_MANY_REQUESTS, "exhausted all accounts").into_response()
}

// ── Stats endpoint ──────────────────────────────────────────────────

async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
    axum::Json(serde_json::json!({
        "accounts": out,
        "strategy": "dynamic-capacity",
    }))
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

    assert!(
        !config.accounts.is_empty(),
        "at least one account required"
    );

    let cooldown = Duration::from_secs(config.rate_limit_cooldown_secs.unwrap_or(60));

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

    info!(
        num_accounts = accounts.len(),
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
    });

    // Restore persisted state (cooldowns, utilization, request counts)
    state.load_state().await;

    let app = Router::new()
        .route("/_stats", axum::routing::get(stats_handler))
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
            info!(interval_secs = probe_interval, "starting utilization probes");
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

    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(shutdown)
        .await
        .unwrap_or_else(|e| panic!("server error: {e}"));
}

use crate::*;

// ── Response cache (LAB-933) ────────────────────────────────────────
//
// Opt-in, client-side-encrypted response cache on non-streaming
// /v1/messages. The prize is headroom, not latency: a hit skips the
// upstream call entirely, so replayed traffic (eval reruns, pipeline
// replays, post-timeout retries) burns zero 5h/7d rate-limit budget.
// Everything below fails OPEN — a sick cache degrades to a miss/skipped
// write and the request proceeds upstream exactly as today.

/// A cached /v1/messages response: status + content-type + body, nothing
/// else. Upstream per-request headers (request ids, rate-limit snapshots,
/// x-budget-status) describe the ORIGINAL exchange and are deliberately not
/// replayed; a hit instead carries `x-alb-cache: hit`.
#[derive(Serialize, Deserialize)]
pub(crate) struct CachedResponse {
    pub(crate) status: u16,
    pub(crate) content_type: String,
    #[serde(with = "serde_bytes")]
    pub(crate) body: Vec<u8>,
}

/// Which endpoint a response-cache entry belongs to (LAB-929). Folded into
/// the cache key itself (not just the metric label) so a client that sends
/// byte-identical bodies to `/v1/messages` and `/v1/messages/count_tokens`
/// (a common pattern — count before you send) can never have one surface's
/// cached entry served back for the other.
#[derive(Clone, Copy)]
pub(crate) enum CacheSurface {
    Messages,
    CountTokens,
}

impl CacheSurface {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::CountTokens => "count_tokens",
        }
    }
}

/// Runtime handle for the response cache: one shared backend, one
/// `cachekit::CacheKit` per allow-listed client with `tenant_id =
/// client_id`. HKDF-SHA256 then derives a distinct AES-256-GCM key per
/// client, so cross-client isolation (AC7) is cryptographic — even a key
/// collision could not decrypt another client's entry. The map doubles as
/// the opt-in allow-list: no entry, no cache (AC2). All values are
/// encrypted before any storage layer sees them, in-process L1 included
/// (verified against cachekit-rs 0.5.0 `SecureCache::set_with_ttl`, which
/// writes ciphertext to both L1 and the backend).
pub(crate) struct ResponseCache {
    pub(crate) clients: HashMap<String, cachekit::CacheKit>,
    op_timeout: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    errors: AtomicU64,
    stores: AtomicU64,
    // LAB-929: count_tokens gets its own series (AC4) rather than sharing
    // the /v1/messages counters above — a HashMap<&str, _> would be more
    // machinery than two fixed, permanent surfaces warrant.
    count_tokens_hits: AtomicU64,
    count_tokens_misses: AtomicU64,
    count_tokens_errors: AtomicU64,
    count_tokens_stores: AtomicU64,
}

/// First 16 hex chars of a cache-key digest — the ONLY form a cache key may
/// take in any diagnostic output (AC5).
pub(crate) fn key_digest_prefix(key: &str) -> &str {
    &key[..key.len().min(16)]
}

/// Decode a hex master key, enforcing the 32-byte SecureCache minimum.
fn decode_hex_key(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return Err("response_cache.master_key must be an even-length hex string".into());
    }
    let bytes = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|_| "response_cache.master_key contains non-hex characters".to_string())?;
    if bytes.len() < 32 {
        return Err(format!(
            "response_cache.master_key must decode to at least 32 bytes, got {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

/// Derive the response-cache storage key (AC6): hex Blake2s-256 over
/// (model ␟ canonical body JSON ␟ sorted anthropic-beta values ␟
/// anthropic-version ␟ URI query ␟ client_id ␟ fp ␟ fps ␟ surface). The
/// full-body digest is what carries correctness — two requests differing
/// anywhere, including a nested structural position, serialize differently
/// (`preserve_order` keeps the client's key order) and get different keys.
/// anthropic-version and the query string are keyed because they change the
/// RESPONSE schema for an identical body (an SDK upgrade mid-TTL must miss,
/// not replay the old shape). The 48-bit SipHash content fingerprints are
/// folded in to reuse the existing canonical machinery (per AC6), but are
/// deliberately not trusted alone: 48 bits over prompt content is collision
/// territory, and a key collision here would serve someone the wrong
/// completion. client_id in the material separates keys per client on top
/// of the per-tenant encryption (AC7). `surface` (LAB-929) separates
/// `/v1/messages` from `/v1/messages/count_tokens` so an identical body
/// posted to both endpoints never cross-serves — a count vs. a completion
/// are not interchangeable, no matter how the key material lines up
/// otherwise. The emitted key is a digest only — no prompt content ever
/// appears in a storage key or diagnostic line (AC5).
#[allow(clippy::too_many_arguments)]
pub(crate) fn response_cache_key(
    model: &str,
    body: &serde_json::Value,
    headers: &hyper::HeaderMap,
    query: Option<&str>,
    client_id: &str,
    fp: &str,
    fps: &str,
    surface: &str,
) -> String {
    use blake2::{Blake2s256, Digest};
    let mut betas: Vec<&str> = headers
        .get_all("anthropic-beta")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    betas.sort_unstable();
    let version = headers
        .get("anthropic-version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let canonical = serde_json::to_string(body).unwrap_or_default();
    let mut h = Blake2s256::new();
    for part in [
        model,
        &canonical,
        &betas.join(","),
        version,
        query.unwrap_or(""),
        client_id,
        fp,
        fps,
        surface,
    ] {
        h.update(part.as_bytes());
        h.update([0x1f]);
    }
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

impl ResponseCache {
    /// Storage-key namespace: entries land as `alb-resp:<digest>`.
    const NAMESPACE: &'static str = "alb-resp";
    pub(crate) const DEFAULT_TTL_SECS: u64 = 3600;
    const DEFAULT_OP_TIMEOUT_MS: u64 = 250;
    /// Per-client in-process L1 entries (ciphertext at rest). Together with
    /// `MAX_BODY_BYTES` this bounds worst-case L1 memory per client.
    const L1_CAPACITY: usize = 64;
    /// Bodies larger than this are not cached (the entry would also bump
    /// into cachekit's 5 MiB payload ceiling once encrypted + encoded).
    /// Bounds both backend value growth and worst-case L1 memory.
    pub(crate) const MAX_BODY_BYTES: usize = 1024 * 1024;

    /// Build from config. `Ok(None)` when the allow-list is empty (inert,
    /// AC2). Config errors — bad key, missing backend params — are `Err`:
    /// startup fails loudly rather than running with a silently-disabled
    /// cache. Backend REACHABILITY is not required at startup: connection
    /// failures are logged and every later operation fails open (AC10).
    pub(crate) async fn from_config(cfg: &ResponseCacheConfig) -> Result<Option<Self>, String> {
        if cfg.clients.is_empty() {
            return Ok(None);
        }
        let master_key = decode_hex_key(&cfg.master_key)?;
        let backend: cachekit::SharedBackend = match cfg.backend.as_str() {
            "redis" => {
                let url = cfg
                    .redis_url
                    .as_deref()
                    .ok_or("response_cache.redis_url is required for backend = \"redis\"")?;
                let b = cachekit::backend::redis::RedisBackend::builder()
                    .url(url)
                    .build()
                    .map_err(|e| format!("response_cache redis backend: {e}"))?;
                // fred's connection task detaches when the handle drops.
                // KNOWN LIMITATION (cachekit-rs 0.5.0, flagged upstream):
                // auto-reconnect is pub(crate) and unreachable from this
                // builder, so after a failed initial connect OR any later
                // Redis outage the client does NOT recover — every cache op
                // errors (fail-open, bounded by op_timeout per op) until the
                // process restarts. Watch anthropic_response_cache_errors_total.
                match b.connect().await {
                    Ok(_handle) => info!("response cache redis backend connected"),
                    Err(e) => {
                        warn!(error = %e, "response cache redis connect failed — cache disabled until restart (fail-open)")
                    }
                }
                std::sync::Arc::new(b)
            }
            "cachekitio" => {
                let api_key = cfg
                    .api_key
                    .as_deref()
                    .ok_or("response_cache.api_key is required for backend = \"cachekitio\"")?;
                let mut builder =
                    cachekit::backend::cachekitio::CachekitIO::builder().api_key(api_key);
                if let Some(url) = &cfg.api_url {
                    // allow_custom_host is required for hosts outside
                    // cachekit's built-in allow-list (api.cachekit.io /
                    // api.staging.cachekit.io) — e.g. the dev environment.
                    // The validator still enforces HTTPS and blocks
                    // private/loopback IPs regardless.
                    builder = builder.api_url(url).allow_custom_host(true);
                }
                std::sync::Arc::new(
                    builder
                        .build()
                        .map_err(|e| format!("response_cache cachekitio backend: {e}"))?,
                )
            }
            other => {
                return Err(format!(
                    "response_cache.backend must be \"cachekitio\" or \"redis\", got \"{other}\""
                ))
            }
        };
        Self::from_parts(
            backend,
            &cfg.clients,
            &master_key,
            Duration::from_secs(cfg.ttl_secs.unwrap_or(Self::DEFAULT_TTL_SECS)),
            Duration::from_millis(cfg.op_timeout_ms.unwrap_or(Self::DEFAULT_OP_TIMEOUT_MS)),
        )
        .map(Some)
    }

    /// Assemble per-client caches over a shared backend. Split out from
    /// `from_config` so tests can inject mock backends through the same
    /// construction path production uses.
    fn from_parts(
        backend: cachekit::SharedBackend,
        clients: &[String],
        master_key: &[u8],
        ttl: Duration,
        op_timeout: Duration,
    ) -> Result<Self, String> {
        let mut map = HashMap::new();
        for client_id in clients {
            if client_id == "-" {
                return Err(
                    "response_cache.clients cannot contain \"-\" (the unknown-client sentinel — it would opt in every unidentified caller)"
                        .into(),
                );
            }
            // The builder's default_ttl is the SINGLE source of entry
            // lifetime — writes use plain `set()` so the two can't drift.
            let ck = cachekit::CacheKit::builder()
                .backend(backend.clone())
                .default_ttl(ttl)
                .namespace(Self::NAMESPACE)
                .l1_capacity(Self::L1_CAPACITY)
                .encryption_from_bytes(master_key, client_id)
                .map_err(|e| format!("response_cache encryption init for \"{client_id}\": {e}"))?
                .build()
                .map_err(|e| format!("response_cache init for \"{client_id}\": {e}"))?;
            map.insert(client_id.clone(), ck);
        }
        Ok(Self {
            clients: map,
            op_timeout,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            stores: AtomicU64::new(0),
            count_tokens_hits: AtomicU64::new(0),
            count_tokens_misses: AtomicU64::new(0),
            count_tokens_errors: AtomicU64::new(0),
            count_tokens_stores: AtomicU64::new(0),
        })
    }

    pub(crate) fn hits_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.hits,
            CacheSurface::CountTokens => &self.count_tokens_hits,
        }
    }

    pub(crate) fn misses_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.misses,
            CacheSurface::CountTokens => &self.count_tokens_misses,
        }
    }

    pub(crate) fn errors_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.errors,
            CacheSurface::CountTokens => &self.count_tokens_errors,
        }
    }

    pub(crate) fn stores_for(&self, surface: CacheSurface) -> &AtomicU64 {
        match surface {
            CacheSurface::Messages => &self.stores,
            CacheSurface::CountTokens => &self.count_tokens_stores,
        }
    }

    /// Read an entry. Every failure mode — no encryption handle, backend
    /// error, decrypt error, timeout — degrades to `None` and the request
    /// proceeds upstream (AC10). Diagnostics carry the key digest only,
    /// never content (AC5).
    pub(crate) async fn lookup(
        &self,
        client_id: &str,
        key: &str,
        surface: CacheSurface,
    ) -> Option<CachedResponse> {
        let cache = self.clients.get(client_id)?;
        let op = async { cache.secure()?.get::<CachedResponse>(key).await };
        match tokio::time::timeout(self.op_timeout, op).await {
            Ok(Ok(Some(entry))) => {
                self.hits_for(surface).fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            Ok(Ok(None)) => {
                self.misses_for(surface).fetch_add(1, Ordering::Relaxed);
                None
            }
            Ok(Err(e)) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(key_digest = key_digest_prefix(key), error = %e, "response cache read failed — proceeding upstream");
                None
            }
            Err(_) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(
                    key_digest = key_digest_prefix(key),
                    timeout_ms = self.op_timeout.as_millis() as u64,
                    "response cache read timed out — proceeding upstream"
                );
                None
            }
        }
    }

    /// Write an entry. Failures are counted and logged (digest only) but
    /// never affect the client response (AC10).
    pub(crate) async fn store(
        &self,
        client_id: &str,
        key: &str,
        entry: &CachedResponse,
        surface: CacheSurface,
    ) {
        let Some(cache) = self.clients.get(client_id) else {
            return;
        };
        let op = async { cache.secure()?.set(key, entry).await };
        match tokio::time::timeout(self.op_timeout, op).await {
            Ok(Ok(())) => {
                self.stores_for(surface).fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(key_digest = key_digest_prefix(key), error = %e, "response cache write failed — skipped");
            }
            Err(_) => {
                self.errors_for(surface).fetch_add(1, Ordering::Relaxed);
                warn!(
                    key_digest = key_digest_prefix(key),
                    timeout_ms = self.op_timeout.as_millis() as u64,
                    "response cache write timed out — skipped"
                );
            }
        }
    }
}

/// Build the client-facing response for a cache hit. Only ever called for
/// entries written from 2xx responses (AC3). The entry came through AEAD
/// decryption, so an invalid stored status means OUR bug — surface a loud
/// 500, never a fabricated 200.
pub(crate) fn cached_hit_response(entry: CachedResponse) -> Response {
    let Ok(status) = StatusCode::from_u16(entry.status) else {
        error!(
            status = entry.status,
            "cached entry carries an invalid status code — refusing to serve"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "cached response rebuild error",
        )
            .into_response();
    };
    let content_type = if entry.content_type.is_empty() {
        "application/json"
    } else {
        entry.content_type.as_str()
    };
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("x-alb-cache", "hit")
        .body(Body::from(entry.body))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "cached response rebuild error",
            )
                .into_response()
        })
}

/// Parsed IP allow entry — either a single IP or a CIDR range.
pub(crate) enum IpAllowEntry {
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

/// Parse a list of IP/CIDR strings into allow entries, panicking (startup) on
/// a malformed entry. Shared by `allowed_ips` and `trusted_proxies` — one
/// parse + panic semantics, named by `field` for the error.
pub(crate) fn parse_ip_entries(entries: Option<&[String]>, field: &str) -> Vec<IpAllowEntry> {
    entries
        .unwrap_or_default()
        .iter()
        .map(|s| {
            if let Ok(net) = s.parse::<IpNet>() {
                IpAllowEntry::Net(net)
            } else if let Ok(addr) = s.parse::<IpAddr>() {
                IpAllowEntry::Addr(addr)
            } else {
                panic!("invalid {field} entry: {s}");
            }
        })
        .collect()
}

/// Default failed-auth attempts per IP inside the window before 429.
pub(crate) const DEFAULT_AUTH_FAILURE_LIMIT: u32 = 10;
/// Default failed-auth throttle window.
pub(crate) const DEFAULT_AUTH_FAILURE_WINDOW_SECS: u64 = 300;
/// Hard cap on tracked IPs. The key is attacker-controlled, so an unbounded
/// map here would itself be the DoS the throttle exists to prevent (AC-12).
/// 4096 entries × ~64 bytes ≈ 256 KiB worst case — memory stays flat.
const AUTH_THROTTLE_CAPACITY: usize = 4096;
/// Minimum spacing between `allow_unauthenticated` admin-access warns per route.
const OPEN_ADMIN_WARN_INTERVAL: Duration = Duration::from_secs(300);
/// Minimum length of any configured credential (LAB-1192 AC-13). A static
/// bearer on a public ingress gets scanned; below this the keyspace is the
/// vulnerability. 32 chars = what `openssl rand -hex 32` (64 hex) exceeds.
pub(crate) const MIN_KEY_LEN: usize = 32;

/// Per-client-IP failed-authentication throttle (LAB-1192).
///
/// Fixed window per IP: the first failure starts the window, subsequent
/// failures increment the count, and once the count reaches `max_failures`,
/// further INVALID credentials from that IP get `429 + retry-after` until the
/// window expires. Valid credentials always pass: an IP may represent many
/// callers behind a NAT or load balancer, so one bad caller must not lock out
/// every authenticated neighbour sharing that address.
pub(crate) struct AuthThrottle {
    /// Failures per window before throttling. 0 = throttle disabled.
    max_failures: u32,
    window: Duration,
    capacity: usize,
    /// ip → (window_start, failures). Bounded by `capacity`: expired windows
    /// are purged first, then the least-established (lowest-count) live entry
    /// is evicted, so a flood of fresh failures cannot flush an active lockout.
    entries: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl AuthThrottle {
    pub(crate) fn new(max_failures: u32, window: Duration) -> Self {
        Self::with_capacity(max_failures, window, AUTH_THROTTLE_CAPACITY)
    }

    fn with_capacity(max_failures: u32, window: Duration, capacity: usize) -> Self {
        Self {
            max_failures,
            window,
            capacity,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Lock the failure table via `lock_recovering`. Every `entries` site
    /// MUST go through here: a skip-on-poison lock would make `check` report
    /// "not throttled" and `record_failure` stop counting (fail-open).
    fn lock_entries(&self) -> std::sync::MutexGuard<'_, HashMap<IpAddr, (Instant, u32)>> {
        lock_recovering(&self.entries, "auth_throttle")
    }

    /// Returns `Some(retry_after_secs)` while `ip` is throttled. Expired
    /// windows are removed on sight, so steady-state size tracks only IPs
    /// that failed recently.
    fn check(&self, ip: &IpAddr) -> Option<u64> {
        if self.max_failures == 0 {
            return None;
        }
        let mut entries = self.lock_entries();
        let (start, count) = *entries.get(ip)?;
        let elapsed = start.elapsed();
        if elapsed >= self.window {
            entries.remove(ip);
            return None;
        }
        if count < self.max_failures {
            return None;
        }
        // Round UP to whole seconds, never advertise 0: `as_secs()` truncates,
        // so a client honouring the header exactly would return a sub-second
        // early and eat a spurious extra 429.
        let remaining = self.window - elapsed;
        let secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
        Some(secs.max(1))
    }

    fn record_failure(&self, ip: IpAddr) {
        if self.max_failures == 0 {
            return;
        }
        let mut entries = self.lock_entries();
        match entries.get_mut(&ip) {
            Some((start, count)) => {
                if start.elapsed() >= self.window {
                    *start = Instant::now();
                    *count = 1;
                } else {
                    *count = count.saturating_add(1);
                }
            }
            None => {
                if entries.len() >= self.capacity {
                    // ponytail: O(n) purge + min-scan on insert at capacity —
                    // fires only with 4096 live entries under active attack; an
                    // LRU/heap if it ever shows up in a profile.
                    //
                    // Purge expired windows first — they are no active threat
                    // and the correct thing to drop. Only if the table is full
                    // of LIVE windows do we evict, and then the
                    // LEAST-established one (lowest failure count). An
                    // established lockout has `count >= max_failures`, strictly
                    // above an attacker's fresh `count = 1` floods, so a burst
                    // of new IPs cannot evict a real offender's lockout to reset
                    // it (AC-12: the eviction policy must not itself be an
                    // attacker's escape hatch).
                    let window = self.window;
                    entries.retain(|_, (start, _)| start.elapsed() < window);
                    if entries.len() >= self.capacity {
                        if let Some(victim) = entries
                            .iter()
                            .min_by_key(|(_, (start, count))| (*count, *start))
                            .map(|(k, _)| *k)
                        {
                            entries.remove(&victim);
                        }
                    }
                }
                entries.insert(ip, (Instant::now(), 1));
            }
        }
    }
}

impl AppState {
    pub(crate) fn is_ip_allowed(&self, ip: &IpAddr) -> bool {
        self.allowed_ips.is_empty() || self.allowed_ips.iter().any(|e| e.contains(ip))
    }

    /// Resolve the REAL client IP behind a trusted load balancer (LAB-1192).
    ///
    /// When the TCP peer is inside `trusted_proxies`, the client IP is the
    /// rightmost `x-forwarded-for` entry that is not itself a trusted proxy —
    /// the last hop an attacker cannot append to. In every other case the
    /// peer address wins and the header is ignored entirely: an XFF from an
    /// untrusted peer is attacker input, never trusted, never logged as
    /// authoritative. Malformed or empty entries abort the walk and fall back
    /// to the peer address — never guess.
    ///
    /// This is the ONE resolution function (AC-8): every handler calls it
    /// exactly once, immediately, and threads the result into the IP
    /// allowlist, authentication, `client_names`, budgets, the auth throttle
    /// and every log line. A second `client_addr.ip()` use inside a handler
    /// is a review defect.
    pub(crate) fn resolve_client_ip(&self, peer: IpAddr, headers: &hyper::HeaderMap) -> IpAddr {
        // Canonicalize an IPv4-mapped IPv6 peer (`::ffff:a.b.c.d`) to the bare
        // v4 address BEFORE the trust check. On a dual-stack listener (`::`)
        // v4 clients arrive mapped, and a v4 `trusted_proxies` CIDR would
        // otherwise never match — silently disabling XFF resolution and
        // letting the LB's address become the client IP for the allowlist,
        // throttle and budgets.
        let peer = peer.to_canonical();
        let is_trusted = |ip: &IpAddr| self.trusted_proxies.iter().any(|e| e.contains(ip));
        // `any()` over an empty list is already false, so an empty
        // `trusted_proxies` falls through here — direct-connection behaviour.
        if !is_trusted(&peer) {
            return peer;
        }
        // Multiple x-forwarded-for headers are one logical comma-joined list
        // (RFC 7230 §3.2.2); walk values last-to-first, entries right-to-left.
        for value in headers.get_all("x-forwarded-for").iter().rev() {
            let Ok(s) = value.to_str() else { return peer };
            for entry in s.rsplit(',') {
                let Ok(ip) = entry.trim().parse::<IpAddr>().map(|ip| ip.to_canonical()) else {
                    return peer;
                };
                if !is_trusted(&ip) {
                    return ip;
                }
            }
        }
        // Header absent, or every hop is a trusted proxy: the nearest
        // trusted peer is the most authoritative address we have.
        peer
    }

    /// Authenticate a request's credential (LAB-1083).
    ///
    /// Returns the authenticated principal when `[[clients]]` is configured,
    /// `None` when it is not (legacy `proxy_key`, or an open proxy), and a 401
    /// `Response` when a configured credential does not match.
    ///
    /// `allow_bearer` additionally accepts `Authorization: Bearer` — set only
    /// on the OpenAI-compat surface, whose SDKs send nothing else. It is NOT
    /// enabled on the native surface, where `Authorization` may legitimately
    /// carry the caller's own upstream token in `passthrough` mode; widening
    /// acceptance there would be a gratuitous auth surface on a ticket whose
    /// whole purpose is to narrow one.
    fn authenticate(
        &self,
        headers: &hyper::HeaderMap,
        allow_bearer: bool,
    ) -> Result<Option<&ClientConfig>, Box<Response>> {
        // Boxed Err, matching `reserve_request_body` — an inline `Response` is
        // 128+ bytes on the hot success path (clippy::result_large_err).
        let unauthorized = || -> Box<Response> {
            Box::new((StatusCode::UNAUTHORIZED, "unauthorized").into_response())
        };
        let from_header = headers.get("x-api-key").and_then(|v| v.to_str().ok());
        let from_bearer = if allow_bearer {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    // RFC 7235: auth scheme is case-insensitive
                    if v.len() >= 7 && v[..7].eq_ignore_ascii_case("bearer ") {
                        Some(&v[7..])
                    } else {
                        None
                    }
                })
        } else {
            None
        };

        if !self.clients.is_empty() {
            for presented in [from_header, from_bearer].into_iter().flatten() {
                if let Some(c) = self.match_client(presented) {
                    return Ok(Some(c));
                }
            }
            return Err(unauthorized());
        }

        // Legacy: one shared secret, no principal. Byte-for-byte the same
        // accept/reject decision as before LAB-1083 — only the comparison
        // primitive changed (audit finding 4).
        if let Some(ref key) = self.proxy_key {
            let ok = [from_header, from_bearer]
                .into_iter()
                .flatten()
                .any(|p| bool::from(key.as_bytes().ct_eq(p.as_bytes())));
            if !ok {
                return Err(unauthorized());
            }
        }
        Ok(None)
    }

    /// Authenticate, then throttle failed credentials (LAB-1193).
    ///
    /// This supersedes LAB-1192 AC-11's pre-comparison ordering after the
    /// 2026-08-24 shared-IP outage. The key comparison deliberately runs
    /// before the throttle decision.
    /// Valid credentials always pass, even when the resolved IP has an active
    /// failure window: behind NAT or an LB that IP may represent unrelated
    /// callers, and rejecting a known-good principal turns ten bad requests
    /// into a five-minute denial of service for every neighbour. Invalid
    /// credentials still get `429 + retry-after` once the IP reaches the
    /// limit, and successful traffic does NOT clear the shared failure state.
    /// This trade relies on `MIN_KEY_LEN` keeping credential guessing
    /// impractical; if the credential floor is lowered, restore AC-11's
    /// pre-comparison ordering.
    ///
    /// Every rejection is counted per route in
    /// `anthropic_auth_failures_total`; throttle 429s keep the metric rising
    /// through a sustained attack instead of plateauing at the limit.
    ///
    /// Rejections also log what was presented (LAB-4720), because behind a
    /// NAT gateway or TCP-forwarding VIP every caller resolves to one IP,
    /// and the fields that normally attribute a request (`client_id`, `ver`,
    /// `agent`) are derived from the credential that just failed. The
    /// user-agent is recorded with `?` (Debug) so it renders quoted and
    /// escaped: it is caller-controlled, and unquoted it could forge the
    /// other key=value fields on the same line.
    pub(crate) fn authenticate_throttled(
        &self,
        client_ip: &IpAddr,
        peer: SocketAddr,
        headers: &hyper::HeaderMap,
        allow_bearer: bool,
        route: &'static str,
    ) -> Result<Option<&ClientConfig>, Box<Response>> {
        match self.authenticate(headers, allow_bearer) {
            Ok(principal) => Ok(principal),
            Err(unauthorized) => {
                let (cred, presented) = presented_credential(headers);
                let key_fp = credential_fingerprint(presented);
                let ua = bounded_user_agent(headers);
                self.count_auth_failure(route, cred);
                if let Some(retry_after) = self.auth_throttle.check(client_ip) {
                    warn!(
                        client = %client_ip,
                        peer = %peer,
                        route,
                        cred,
                        key_fp = %key_fp,
                        ua = ?ua,
                        retry_after,
                        "rejected: failed-auth throttle active"
                    );
                    let mut resp = (
                        StatusCode::TOO_MANY_REQUESTS,
                        "too many failed authentication attempts",
                    )
                        .into_response();
                    resp.headers_mut()
                        .insert("retry-after", HeaderValue::from(retry_after));
                    return Err(Box::new(resp));
                }
                self.auth_throttle.record_failure(*client_ip);
                warn!(
                    client = %client_ip,
                    peer = %peer,
                    route,
                    cred,
                    key_fp = %key_fp,
                    ua = ?ua,
                    "rejected: invalid or missing credential"
                );
                Err(unauthorized)
            }
        }
    }

    /// Count one rejection under `(route, cred)`. The user-agent is
    /// deliberately NOT a label: its slots would be claimed first-come by
    /// unauthenticated callers on the public ingress, blinding the metric
    /// for every later legitimate caller. It lives on the log line instead.
    pub(crate) fn count_auth_failure(&self, route: &'static str, cred: &'static str) {
        *lock_recovering(&self.auth_failures, "auth_failures")
            .entry((route, cred))
            .or_insert(0) += 1;
    }

    /// Gate an admin surface (`/_stats`, `/metrics`) behind an OPERATOR
    /// principal (LAB-1192 AC-4). Returns the rejection response, or `None`
    /// when the caller may proceed.
    ///
    /// Under `[[clients]]`: unauthenticated → 401, an authenticated principal
    /// in neither `operators` nor `admin_readers` → 403 — `/_stats` discloses
    /// other clients' ids and the endpoint account names, which a per-client
    /// key holder has no business reading. An `admin_readers` principal (LAB-4395) is admitted here and ONLY
    /// here: it is exactly the credential to hand a scrape or a dashboard,
    /// because `pre_request_gate` refuses it on every proxied surface.
    /// Under legacy `proxy_key`, a valid key serves: one shared
    /// secret means the key holder IS the operator. Under
    /// `allow_unauthenticated` (the only way to boot with no credentials),
    /// both surfaces serve but each access logs at `warn` (AC-5) so the open
    /// posture stays visible even on a trusted network.
    pub(crate) fn authorize_admin(
        &self,
        client_ip: &IpAddr,
        peer: SocketAddr,
        headers: &hyper::HeaderMap,
        route: &'static str,
    ) -> Option<Box<Response>> {
        match self.authenticate_throttled(client_ip, peer, headers, false, route) {
            Err(resp) => Some(resp),
            Ok(Some(c)) if !self.is_operator(&c.name) && !self.is_admin_reader(&c.name) => {
                warn!(
                    client = %client_ip,
                    client_id = %c.name,
                    route,
                    "rejected: admin surface requires an operator or read-only principal"
                );
                Some(Box::new(
                    (
                        StatusCode::FORBIDDEN,
                        "forbidden: operator or read-only principal required",
                    )
                        .into_response(),
                ))
            }
            Ok(Some(_)) => None,
            Ok(None) => {
                // allow_unauthenticated: keep the open posture VISIBLE (AC-5)
                // but rate-limit the warn per route — in the lab a vmagent
                // scrapes /metrics every ~15s, and one warn per scrape is
                // ~11k lines/day/replica that drowns the signal it exists to
                // give. Once per route per OPEN_ADMIN_WARN_INTERVAL preserves
                // visibility without the firehose.
                if self.proxy_key.is_none() && self.should_warn_open_admin(route) {
                    warn!(
                        client = %client_ip,
                        route,
                        "unauthenticated admin access (allow_unauthenticated) — trusted-network-only; rate-limited log"
                    );
                }
                None
            }
        }
    }

    /// Rate-limiter for the `allow_unauthenticated` admin-access warn: true at
    /// most once per route per `OPEN_ADMIN_WARN_INTERVAL`.
    fn should_warn_open_admin(&self, route: &'static str) -> bool {
        let mut last = lock_recovering(&self.open_admin_warn, "open_admin_warn");
        let now = Instant::now();
        match last.get(route) {
            Some(t) if t.elapsed() < OPEN_ADMIN_WARN_INTERVAL => false,
            _ => {
                last.insert(route, now);
                true
            }
        }
    }

    /// Constant-time lookup of a presented credential in the client table.
    ///
    /// What this actually guarantees, precisely — do not read more into it:
    /// `ct_eq` removes the per-byte prefix oracle that `==` has, which is the
    /// leak that matters (it is what lets an attacker recover a key byte by
    /// byte). Audit finding 4, closed.
    ///
    /// The full-table scan removes the coarse "how far down the table did we
    /// get" signal. It does NOT make the whole function constant-time: the
    /// `hit = Some(c)` store is a data-dependent branch the optimizer may
    /// emit as one, and `ct_eq` short-circuits on unequal lengths. Both
    /// residuals are a handful of instructions against milliseconds of network
    /// and TLS jitter, and key length is not a secret — so neither is
    /// exploitable remotely. Want the stronger property? Accumulate with
    /// `subtle::Choice`; don't assume this already does.
    fn match_client(&self, presented: &str) -> Option<&ClientConfig> {
        let mut hit: Option<&ClientConfig> = None;
        for c in &self.clients {
            if bool::from(c.key.as_bytes().ct_eq(presented.as_bytes())) {
                hit = Some(c);
            }
        }
        hit
    }

    /// Whether `client_id` may request `model` (LAB-1083).
    ///
    /// Unknown client, or a client with an empty list, allows everything.
    /// "Unknown client" is only reachable on the legacy path: with
    /// `[[clients]]` configured, `client_id` is always a principal name.
    ///
    /// FAILS CLOSED on an empty `model`, and this is the one place where the
    /// shared matcher's semantics are deliberately NOT inherited. An empty
    /// model means the caller's model is UNKNOWN to us — the body did not
    /// parse as JSON, or it carries no top-level `model` (the batches API
    /// nests it under `requests[].params.model`, and `proxy_handler` is the
    /// catch-all route). For `Endpoint::serves_model` "unknown" rightly means
    /// "don't narrow the routing pool"; for a policy gate it must mean
    /// "deny", or a restricted client reaches any model by sending a body we
    /// cannot read.
    pub(crate) fn client_allows_model(&self, client_id: &str, model: &str) -> bool {
        match self.clients.iter().find(|c| c.name == client_id) {
            Some(c) if c.models.is_empty() => true,
            Some(c) => !model.is_empty() && model_matches(&c.models, model),
            None => true,
        }
    }

    /// LAB-3877: resolve the Tier 0 guard policy for a client. The operator
    /// bypass is always `Off` (it forwards operator-trusted content). Otherwise
    /// the client's configured policy, defaulting to `Annotate` (shadow mode)
    /// for any caller not in the table — including the legacy unknown-client
    /// "-" and open-`proxy_key` modes, so shadow mode observes everything.
    #[cfg(feature = "guard")]
    fn client_guard_policy(&self, client_id: &str) -> guard::GuardPolicy {
        if self.is_operator(client_id) {
            return guard::GuardPolicy::Off;
        }
        self.clients
            .iter()
            .find(|c| c.name == client_id)
            .map(|c| c.guard)
            .unwrap_or_default()
    }

    /// LAB-3877: run the Tier 0 content guard for one request — after
    /// `pre_request_gate`, before endpoint selection, on every surface that
    /// forwards client content (`proxy_handler` and `openai_chat_handler`).
    /// `Ok(Some(n))` is an `Annotate` count for `X-Guard-Findings`; `Err` is
    /// the 400 to return instead, in the OpenAI envelope when `openai_shape`.
    ///
    /// Fail closed under `block`: a body the guard could not scan in full
    /// cannot be certified clean, so it is rejected rather than forwarded
    /// unscanned (LAB-3877 review). Two cases, both bypasses of an enforcing
    /// policy otherwise: [`guard::ScanOutcome::Unscannable`] — the scanner could
    /// not READ the document, so a parse differential vs the upstream could
    /// smuggle content past the scan — and a newest turn longer than the scan
    /// cap, whose tail was never inspected (the "pad past the cap, then the
    /// secret" bypass). [`guard::ScanOutcome::NothingToScan`] — the document was
    /// readable and carried no text (e.g. an image-only turn) — is NOT a scan
    /// failure and is allowed. Annotate / shadow mode never rejects: it measures
    /// best-effort.
    ///
    /// LAB-4358: the discrimination is read straight off the outcome the scanner
    /// returned. It used to arrive as a separate `unscannable` argument each
    /// handler derived for itself from the body's `messages` shape, which is how
    /// the two surfaces drifted into disagreeing about which shapes were
    /// readable at all. The reason strings are `&'static str` by construction —
    /// no request content may enter a rejection body or a log line.
    ///
    /// LAB-3878: then the Tier 1 detector, if one is configured, on the same
    /// scan input. `annotate` hands it to a background task and returns at once
    /// (see `guard::detector` for why shadow mode is off the request path);
    /// `block` waits for it, at most the detector's `timeout_ms`. A Tier 0
    /// `block` verdict returns before the detector is asked.
    #[cfg(feature = "guard")]
    pub(crate) async fn guard_hook(
        &self,
        req_id: &str,
        client_id: &str,
        outcome: &guard::ScanOutcome,
        openai_shape: bool,
    ) -> Result<Option<usize>, Box<Response>> {
        let policy = self.client_guard_policy(client_id);
        let input = match outcome {
            guard::ScanOutcome::Scannable(input) => Some(input),
            guard::ScanOutcome::NothingToScan | guard::ScanOutcome::Unscannable(_) => None,
        };
        let truncated = input.is_some_and(guard::ScanInput::truncated);
        // Why this request cannot be certified clean, if it cannot. The two
        // causes are not alike and neither is `NothingToScan`: the scanner could
        // not READ the document, or it read a newest turn whose tail ran past
        // the scan cap.
        let unreadable = match outcome {
            guard::ScanOutcome::Unscannable(reason) => Some(*reason),
            _ if truncated => Some(guard::REASON_SCAN_TRUNCATED),
            _ => None,
        };
        if let Some(reason) = unreadable {
            // Logged under EVERY policy, not just `block`. Shadow mode exists so
            // an operator can size the blast radius before flipping a client to
            // `block`; a fail-closed cause that only logs once it is already
            // rejecting makes the one number they need unmeasurable until the
            // outage. `would-block` is the same event `block` would reject on.
            //
            // Only `block` is a WARN: it is rejecting traffic. `would-block` is
            // shadow telemetry, and unconfigured clients default to `annotate`,
            // so every oversized tool-result turn would otherwise raise a
            // warning nobody needs to act on. INFO is the production default
            // filter, so the rollout signal still ships.
            if policy == guard::GuardPolicy::Block {
                warn!(
                    req_id,
                    client_id = %client_id,
                    verdict = "block",
                    reason,
                    "guard: unscannable request"
                );
                return Err(Box::new(guard_blocked_response(&[], reason, openai_shape)));
            }
            info!(
                req_id,
                client_id = %client_id,
                verdict = "would-block",
                reason,
                "guard: unscannable request"
            );
        }
        let tier0 = match self.guard.evaluate(policy, client_id, input) {
            guard::Verdict::Allow => None,
            guard::Verdict::Annotate { findings } => Some(findings),
            guard::Verdict::Block { findings, reason } => {
                self.log_guard_verdict(req_id, client_id, "block", &findings, truncated);
                return Err(Box::new(guard_blocked_response(
                    &findings,
                    &reason,
                    openai_shape,
                )));
            }
        };
        if let Some(findings) = &tier0 {
            self.log_guard_verdict(req_id, client_id, "annotate", findings, truncated);
        }
        let tier0 = tier0.map(|f| f.len());

        let (Some(detector), Some(input)) = (self.guard.detector(), input) else {
            return Ok(tier0);
        };
        if input.text().is_empty() {
            return Ok(tier0);
        }
        match policy {
            guard::GuardPolicy::Off => Ok(tier0),
            guard::GuardPolicy::Annotate => {
                detector.shadow(
                    req_id.to_owned(),
                    client_id.to_owned(),
                    input.text().to_owned(),
                );
                Ok(tier0)
            }
            // Under `block`, Tier 0 either rejected above or found nothing, so
            // the detector's outcome is the whole answer.
            guard::GuardPolicy::Block => {
                debug_assert!(tier0.is_none(), "evaluate never annotates under block");
                match detector.enforce(req_id, client_id, input.text()).await {
                    guard::detector::Enforced::Allow => Ok(None),
                    guard::detector::Enforced::FailOpen => Ok(Some(1)),
                    guard::detector::Enforced::Block(findings) => Err(Box::new(
                        guard_blocked_response(&findings, guard::REASON_FLAGGED, openai_shape),
                    )),
                    guard::detector::Enforced::Unavailable => {
                        Err(Box::new(guard_unavailable_response(openai_shape)))
                    }
                }
            }
        }
    }

    #[cfg(feature = "guard")]
    fn log_guard_verdict(
        &self,
        req_id: &str,
        client_id: &str,
        verdict: &'static str,
        findings: &[guard::Finding],
        truncated: bool,
    ) {
        warn!(
            req_id,
            client_id = %client_id,
            verdict,
            findings = findings.len(),
            truncated,
            detections = %guard::detections_summary(findings),
            "guard"
        );
    }

    /// Count + log a model-allowlist denial.
    ///
    /// The model string is caller-controlled and bounded only by the request
    /// body cap, so it is truncated BEFORE becoming a map key: an untruncated
    /// label would be retained for the process lifetime and re-serialized into
    /// the `/metrics` body on every scrape. Label COUNT is separately
    /// hard-bounded at `MAX_MODEL_DENIED_LABELS` + 1: once the cap is
    /// reached, every new pair lumps into a single global
    /// `("_other", "_other")` bucket.
    ///
    /// Logs at `warn` the first time a (client, model) pair is denied and at
    /// `debug` thereafter — a client hammering a denied model must not be able
    /// to drive unbounded warn-level log volume. Pairs lumped into the
    /// overflow bucket share its first-seen flag (deliberate: client-id
    /// rotation must not mint warns). The counter still records every denial.
    /// Mirrors the once-per-model pattern used for unsupported-model
    /// warnings.
    pub(crate) fn note_model_denied(&self, client_id: &str, model: &str) {
        let model = truncate_label(model);
        let first_time = {
            let mut counts = lock_recovering(&self.model_denied, "model_denied");
            let key = (client_id.to_owned(), model.clone());
            let label = if counts.len() < MAX_MODEL_DENIED_LABELS || counts.contains_key(&key) {
                key
            } else {
                // Map full and this pair is new: lump into ONE global
                // overflow bucket — hard bound of MAX_MODEL_DENIED_LABELS
                // + 1 entries. A per-client ("<client>", "_other") key
                // would let x-client-id rotation (legacy auth modes) grow
                // the map without bound (expert-panel finding, LAB-2330;
                // mirrored here by LAB-2332).
                ("_other".to_owned(), "_other".to_owned())
            };
            let entry = counts.entry(label).or_insert(0);
            *entry += 1;
            *entry == 1
        };
        if first_time {
            warn!(
                client_id = %client_id,
                model = %model,
                "rejected: model not in client allow-list"
            );
        } else {
            debug!(
                client_id = %client_id,
                model = %model,
                "rejected: model not in client allow-list"
            );
        }
    }

    /// Count a pre-request-gate 429 rejection (LAB-2551).
    ///
    /// `client_id` is truncated before becoming a map key (caller-controlled
    /// under legacy header auth), and past `MAX_CLIENT_REJECTION_LABELS`
    /// distinct clients NEW clients lump into the single global `_other` key
    /// — keeping the reason label so overflow traffic still charts by cause.
    /// Callers already log the rejection; this only feeds `/metrics`.
    pub(crate) fn note_client_rejection(&self, client_id: &str, reason: &'static str) {
        let mut counts = self.lock_client_rejections();
        let key = (truncate_label(client_id), reason);
        // Tracked = the CLIENT has any entry, not this exact (client, reason)
        // pair: a tracked client's first rejection under a new reason must
        // not fall to `_other` just because the cap was crossed in between.
        // The cap likewise bounds distinct CLIENTS, not (client, reason)
        // entries — one client on all 3 reasons must burn one slot, not
        // three. O(cap) scans, only on the rejection path.
        let tracked = counts.keys().any(|(c, _)| *c == key.0);
        let distinct_clients = counts
            .keys()
            .map(|(c, _)| c.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len();
        let key = if tracked || distinct_clients < MAX_CLIENT_REJECTION_LABELS {
            key
        } else {
            ("_other".to_owned(), reason)
        };
        *counts.entry(key).or_insert(0) += 1;
    }

    /// Resolve client identity: x-client-id header → IP map fallback → "-"
    ///
    /// Header takes precedence to support multiple clients per IP.
    ///
    /// ONLY reached when no `[[clients]]` table is configured. Under
    /// `[[clients]]`, identity comes from the verified credential
    /// (`RequestContext::from_request`) and this client-asserted path is dead.
    ///
    /// The `debug_assert` is the choke point for that invariant. Nothing in the
    /// type system stops a future handler from calling
    /// `RequestContext::from_request(.., None)` and silently reinstating
    /// header-asserted identity; this makes that mistake fail loudly in tests
    /// and debug builds instead of quietly becoming an identity bypass.
    pub(crate) fn resolve_client_id(&self, ip: &IpAddr, headers: &hyper::HeaderMap) -> String {
        debug_assert!(
            self.clients.is_empty(),
            "resolve_client_id reached with [[clients]] configured — a caller \
             skipped the authenticated principal and identity is now spoofable"
        );
        if let Some(id) = headers.get("x-client-id").and_then(|v| v.to_str().ok()) {
            let id = id.trim();
            // "_operator" is the reserved operator-aggregation label on
            // /_stats and /metrics — a self-asserted claim to it would merge
            // this caller's usage into the hidden operator bucket. "_other"
            // is the reserved metrics overflow-bucket label (LAB-2330/2332) —
            // claiming it would merge this caller into the overflow key.
            if !id.is_empty() && id != "-" && id != "_operator" && id != "_other" {
                return id.to_string();
            }
        }
        // Fallback: IP mapping or unknown
        self.client_names
            .get(&ip.to_string())
            .cloned()
            .unwrap_or_else(|| "-".to_string())
    }
}

#[cfg(test)]
mod tests;

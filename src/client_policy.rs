use crate::*;
use std::collections::hash_map::Entry;
use std::sync::PoisonError;

// ── Client policy ───────────────────────────────────────────────────
//
// Every per-client rule is checked here, by one fallible builder, and the
// result is one value: how callers authenticate, which principal each client
// key resolves to, and what each name may do. `load_config` runs it on every
// boot, so a test that builds its per-client state through it holds none a
// boot would refuse.
//
// The policy is shared as one immutable snapshot. A request reads it once, at
// authentication, and carries that snapshot as its `Caller` through every
// per-client decision it makes, so no two of them can disagree about the
// policy. Replacing the snapshot reaches only the requests that authenticate
// afterwards.

/// How callers authenticate. Decided once per build, by `AuthMode::of`, and
/// never re-inferred at runtime. In particular nothing infers it from how many
/// clients a table holds: an empty principal table refuses every credential,
/// it does not open the proxy.
pub(crate) enum AuthMode {
    /// `allow_unauthenticated = true`: no credential is checked.
    Open,
    /// Legacy `proxy_key`: one shared secret, held as its `key_digest`. A
    /// match yields no principal, so identity stays header- or IP-derived.
    SharedKey([u8; 32]),
    /// `[[clients]]`: a presented key must be in the principal table.
    Clients,
}

/// What an authenticated `[[clients]]` principal may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// Named in `operators`: bypasses every request policy and may read
    /// `/_stats` and `/metrics`.
    Operator,
    /// Named in `admin_readers`: may read `/_stats` and `/metrics`, and is
    /// refused on every proxied route.
    Reader,
    /// Every other `[[clients]]` entry.
    Client,
}

/// The caller a client key authenticated as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Principal {
    /// The `[[clients]]` name. It becomes the request's `client_id`.
    pub(crate) name: String,
    pub(crate) role: Role,
}

/// One `[[clients]]` entry without its key: what the proxy enforces for that
/// name.
pub(crate) struct ClientEntry {
    pub(crate) name: String,
    pub(crate) models: Vec<String>,
    pub(crate) preferred_endpoints: Vec<String>,
    #[cfg(feature = "guard")]
    pub(crate) guard: guard::GuardPolicy,
}

/// Every per-client rule of a config, validated together by `build`. Client
/// keys are held only as digests: nothing here, or anywhere it is installed,
/// keeps a plaintext client key. Installed whole and never edited in place:
/// `AppState::swap_client_policy` replaces it.
pub(crate) struct ClientPolicy {
    /// `key_digest` of each `[[clients]]` key → the principal it resolves to.
    /// Empty unless the auth mode is `Clients`.
    pub(crate) principals: HashMap<[u8; 32], Principal>,
    pub(crate) clients: Vec<ClientEntry>,
    pub(crate) client_budgets: HashMap<String, u64>,
    pub(crate) client_utilization_limits: HashMap<String, f64>,
    pub(crate) operators: Vec<String>,
    pub(crate) admin_readers: Vec<String>,
}

/// What authenticating a request established: the policy snapshot it read,
/// and the principal the credential resolved to in that snapshot.
/// `RequestContext::from_request` turns the two into the request's `Caller`.
pub(crate) struct Authenticated {
    pub(crate) policy: Arc<ClientPolicy>,
    /// `None` under a shared key or an open proxy, where identity is header-
    /// or IP-derived instead.
    pub(crate) principal: Option<Principal>,
}

/// The principal only: the policy is the whole client table.
impl std::fmt::Debug for Authenticated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticated")
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

/// One request's caller: the principal it resolved to, and the policy
/// snapshot it was resolved against. Built once per request and handed to
/// every per-client decision the request makes (the gate, the guard, routing
/// pins, the pressure header, the budget its usage is charged to), so all of
/// them read one policy however long the request runs.
#[derive(Clone)]
pub(crate) struct Caller {
    pub(crate) policy: Arc<ClientPolicy>,
    /// Its `name` is the request's `client_id`.
    pub(crate) principal: Principal,
}

impl Caller {
    /// A caller known by name rather than by a client key: the header- or
    /// IP-derived identity of a shared-key or open proxy. Its role is looked
    /// up by name, so there an operator is whoever asserts an operator's
    /// name, as `operators` documents.
    pub(crate) fn named(policy: Arc<ClientPolicy>, name: String) -> Self {
        let role = role_in(&policy.operators, &policy.admin_readers, &name);
        Self {
            policy,
            principal: Principal { name, role },
        }
    }

    /// The request's `client_id`.
    pub(crate) fn name(&self) -> &str {
        &self.principal.name
    }

    /// The caller's `[[clients]]` entry. Always `None` on a shared-key or
    /// open proxy, whose policy has no client table, so a header-derived name
    /// can never take on a configured client's models, pins or guard policy.
    pub(crate) fn entry(&self) -> Option<&ClientEntry> {
        self.policy.clients.iter().find(|c| c.name == self.name())
    }

    /// The caller's daily token budget, if it has one.
    pub(crate) fn budget(&self) -> Option<u64> {
        self.policy.client_budgets.get(self.name()).copied()
    }

    /// The caller's utilization ceiling, if it has one.
    pub(crate) fn utilization_limit(&self) -> Option<f64> {
        self.policy
            .client_utilization_limits
            .get(self.name())
            .copied()
    }

    /// Whether the caller may request `model` (LAB-1083).
    ///
    /// A caller with no entry, or an entry with an empty list, may request
    /// anything. "No entry" is only reachable on the legacy path: with
    /// `[[clients]]` configured, every caller is a principal from the table.
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
    pub(crate) fn allows_model(&self, model: &str) -> bool {
        match self.entry() {
            Some(c) if c.models.is_empty() => true,
            Some(c) => !model.is_empty() && model_matches(&c.models, model),
            None => true,
        }
    }

    /// Whether an attempt on `ep` is bound by a `models` list, the endpoint's
    /// or the caller's — i.e. whether a body-level fallback could serve it a
    /// model that list does not name, so the attempt's body loses its
    /// fallback fields (`strip_top_level_fields`; LAB-5970, LAB-6794). Judged
    /// per attempt: the retry loop may land on a differently restricted
    /// endpoint. Same identity semantics as `allows_model`.
    pub(crate) fn attempt_restricts_models(&self, ep: &Endpoint) -> bool {
        !ep.models.is_empty() || self.entry().is_some_and(|c| !c.models.is_empty())
    }

    /// The caller's non-empty `preferred_endpoints` list, if it has one.
    pub(crate) fn preferred_endpoints(&self) -> Option<&[String]> {
        self.entry()
            .map(|c| c.preferred_endpoints.as_slice())
            .filter(|p| !p.is_empty())
    }

    /// LAB-3877: the caller's Tier 0 guard policy. An operator's is always
    /// `Off` (it forwards operator-trusted content). Otherwise the entry's
    /// configured policy, defaulting to `Annotate` (shadow mode) for a caller
    /// with no entry — including the legacy unknown-client "-" and open-
    /// `proxy_key` modes, so shadow mode observes everything.
    #[cfg(feature = "guard")]
    pub(crate) fn guard_policy(&self) -> guard::GuardPolicy {
        if self.principal.role == Role::Operator {
            return guard::GuardPolicy::Off;
        }
        self.entry().map(|c| c.guard).unwrap_or_default()
    }
}

impl AppState {
    /// The client policy in force. Read once per request, by authentication,
    /// and once per pass by a background reader; a request then reads only
    /// the snapshot its `Caller` holds.
    pub(crate) fn current_client_policy(&self) -> Arc<ClientPolicy> {
        // A poisoned lock still holds a whole snapshot: the one write swaps
        // the `Arc` in a single move that cannot panic.
        Arc::clone(
            &self
                .client_policy
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Replace the client policy. A request already authenticated keeps the
    /// snapshot it holds, to its end; every request after this reads `next`.
    ///
    /// Refused unless callers authenticate with `[[clients]]` keys. Under a
    /// shared key or an open proxy a caller's name is its own claim, so a
    /// client table swapped in there would hand any header-chosen name that
    /// client's models, pins and guard policy. Under `[[clients]]` every
    /// policy is safe to install: a key it does not hold is refused.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the swap point for a runtime policy source; only tests swap today"
        )
    )]
    pub(crate) fn swap_client_policy(&self, next: ClientPolicy) -> Result<(), &'static str> {
        if !matches!(self.auth_mode, AuthMode::Clients) {
            return Err("the client policy can be replaced only under [[clients]] authentication");
        }
        let next = Arc::new(next);
        let previous = std::mem::replace(
            &mut *self
                .client_policy
                .write()
                .unwrap_or_else(PoisonError::into_inner),
            next,
        );
        // Freed here, outside the lock, unless a request still holds it.
        drop(previous);
        Ok(())
    }
}

/// The digest a client key is held and matched as: BLAKE2s-256 over a purpose
/// tag and the key. Every key the proxy stores or compares goes through this
/// one function.
///
/// Matching a presented key is a hash-map lookup on its digest, which is not
/// constant-time and need not be. Timing can reveal at most something about a
/// stored digest, and a digest cannot be turned back into a key, which is
/// what the proxy accepts. That rests on keys being long and random. Boot
/// enforces only the length (`MIN_KEY_LEN`); the documented generator,
/// `openssl rand -hex 32`, supplies the randomness.
///
/// The tag differs from `credential_fingerprint`'s, so the fingerprint a
/// rejection logs has nothing in common with a stored digest.
pub(crate) fn key_digest(key: &str) -> [u8; 32] {
    use blake2::{Blake2s256, Digest};
    let mut h = Blake2s256::new();
    h.update(b"anthropic-lb/client-key\x1f");
    h.update(key.as_bytes());
    h.finalize().into()
}

impl AuthMode {
    /// The mode `config` describes, inferred exactly as it always was:
    /// `[[clients]]`, else `proxy_key`, else open (which `check_credentials`
    /// allows only with `allow_unauthenticated`). `reject_legacy_config_keys`
    /// refuses both credential forms together, so the order never picks a
    /// winner. Every check that depends on the mode takes this value rather
    /// than looking at the config again.
    fn of(config: &Config) -> Self {
        if !config.clients.is_empty() {
            Self::Clients
        } else if let Some(key) = &config.proxy_key {
            Self::SharedKey(key_digest(key))
        } else {
            Self::Open
        }
    }
}

impl ClientPolicy {
    /// Check every per-client rule in `config`, then build the policy and the
    /// auth mode it implies. Each error is the message a boot fails with. The
    /// structural `[[clients]]` rules run first, then the credential posture,
    /// then the value ranges, so a structural error still wins over a key
    /// that is too short, as it always has. The rest of `load_config` (legacy
    /// keys, endpoints, throttle, cooldown) is not repeated here.
    pub(crate) fn build(config: &Config) -> Result<(AuthMode, Self), String> {
        let auth_mode = AuthMode::of(config);
        let principals =
            check_clients(config, &auth_mode).map_err(|msg| format!("config: {msg}"))?;
        check_credentials(config, &auth_mode)?;
        check_policy_values(config)?;
        Ok((auth_mode, Self::assemble(config, principals)))
    }

    /// Build with no check at all, for the tests that pin how the runtime
    /// behaves in a state `build` refuses. A duplicate key keeps its last
    /// entry. Never reachable from a boot.
    #[cfg(test)]
    pub(crate) fn unchecked(config: &Config) -> (AuthMode, Self) {
        let principals = config
            .clients
            .iter()
            .map(|c| (key_digest(&c.key), principal(config, &c.name)))
            .collect();
        (AuthMode::of(config), Self::assemble(config, principals))
    }

    /// The policy `config` describes, around a principal table already built.
    fn assemble(config: &Config, principals: HashMap<[u8; 32], Principal>) -> Self {
        let clients = config
            .clients
            .iter()
            .map(|c| ClientEntry {
                name: c.name.clone(),
                models: c.models.clone(),
                preferred_endpoints: c.preferred_endpoints.clone(),
                #[cfg(feature = "guard")]
                guard: c.guard,
            })
            .collect();
        Self {
            principals,
            clients,
            client_budgets: config.client_budgets.clone(),
            client_utilization_limits: config.client_utilization_limits.clone(),
            operators: config.operators.clone(),
            admin_readers: config.admin_readers.clone(),
        }
    }

    /// Whether `name` is an operator's. Only for folding name-keyed records
    /// into the `_operator` label on `/_stats` and `/metrics`: a request's own
    /// role is its `Caller`'s principal.
    pub(crate) fn is_operator(&self, name: &str) -> bool {
        self.operators.iter().any(|op| op == name)
    }
}

/// The principal a `[[clients]]` name resolves to.
fn principal(config: &Config, name: &str) -> Principal {
    Principal {
        name: name.to_string(),
        role: role_in(&config.operators, &config.admin_readers, name),
    }
}

/// The role `name` holds under these lists. Reader before operator: a name in
/// both (which `check_clients` refuses) resolves to the denial, as
/// `pre_request_gate` does.
fn role_in(operators: &[String], admin_readers: &[String], name: &str) -> Role {
    if admin_readers.iter().any(|r| r == name) {
        Role::Reader
    } else if operators.iter().any(|op| op == name) {
        Role::Operator
    } else {
        Role::Client
    }
}

/// The `[[clients]]` name rule: `^[a-z0-9][a-z0-9_-]{0,62}$`. Names become
/// metric labels and log fields, so they get one narrow alphabet, and a
/// leading `_` stays free for internal labels such as `_operator` and
/// `_other`. Applied only to `[[clients]]` names; `x-client-id` and the
/// `client_names` map keep their own, older rules.
fn check_client_name(name: &str) -> Result<(), String> {
    if name.starts_with('_') {
        return Err(format!(
            "client {name:?}: names starting with '_' are reserved for internal labels"
        ));
    }
    let mut chars = name.chars();
    let first_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let rest_ok =
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if !first_ok || !rest_ok || name.len() > 63 {
        // Debug-quoted: an out-of-alphabet character may be invisible.
        return Err(format!(
            "client {name:?}: name must match ^[a-z0-9][a-z0-9_-]{{0,62}}$ \
             (lowercase letters, digits, '_' and '-', at most 63 characters)"
        ));
    }
    Ok(())
}

/// The structural `[[clients]]` rules (LAB-1083), and the principal table
/// they check: each key's digest → its principal.
///
/// Every rule here exists because its violation fails SILENTLY at runtime
/// rather than loudly: a duplicate key would resolve to whichever entry the
/// table took last, a duplicate or empty name silently merges two callers'
/// budgets and cache tenancy, and a `[response_cache].clients` typo makes the
/// cache inert for that client with no signal at all. Cheap to catch at boot;
/// expensive to notice in production.
fn check_clients(
    config: &Config,
    auth_mode: &AuthMode,
) -> Result<HashMap<[u8; 32], Principal>, String> {
    let clients = &config.clients;
    let mut seen_names: Vec<&str> = Vec::with_capacity(clients.len());
    let mut principals = HashMap::with_capacity(clients.len());
    for c in clients {
        if c.name.trim().is_empty() {
            return Err("client: name must not be empty".to_string());
        }
        // Stored untrimmed, so " geo" would become a client_id that matches no
        // `client_budgets` / `operators` / `[response_cache].clients` key —
        // silently unenforced budget and cache tenancy. `resolve_client_id`
        // trims its header input; this path has nothing to trim it later.
        if c.name != c.name.trim() {
            return Err(format!(
                "client '{}': name must not have leading or trailing whitespace",
                c.name
            ));
        }
        if c.name == "-" {
            return Err(
                "client: name must not be \"-\" (the unknown-client sentinel — budget enforcement skips it)"
                    .to_string(),
            );
        }
        if c.name == "_operator" {
            return Err(
                "client: name must not be \"_operator\" (the reserved operator-aggregation label on /_stats and /metrics)"
                    .to_string(),
            );
        }
        if c.name == "_other" {
            return Err(
                "client: name must not be \"_other\" (the reserved metrics overflow-bucket label — a real client with this name would merge into the (\"_other\", \"_other\") overflow key)"
                    .to_string(),
            );
        }
        // After the specific checks above, so their messages still name the
        // sentinel a config collided with.
        check_client_name(&c.name)?;
        if c.key.is_empty() {
            return Err(format!("client '{}': key must not be empty", c.name));
        }
        // NOTE: the MIN_KEY_LEN strength floor (AC-13) is enforced in
        // check_credentials, after this function, so a config breaking a
        // structural rule here reports that rule rather than its key length.
        if seen_names.contains(&c.name.as_str()) {
            return Err(format!("client '{}': duplicate name", c.name));
        }
        // preferred_endpoints must name configured endpoints — same posture as
        // the registry cross-checks below: a typo would silently leave the
        // client un-pinned, defeating the whole point of a dedicated account.
        for pe in &c.preferred_endpoints {
            if !config.endpoints.iter().any(|ep| ep.name == *pe) {
                return Err(format!(
                    "client '{}': preferred_endpoints entry '{}' does not match any configured endpoint",
                    c.name, pe
                ));
            }
        }
        // Keys are held and matched as digests, so a duplicate is found as
        // one, by the insert itself: no entry can ever replace another. Names,
        // not keys, in the error — never log a credential.
        match principals.entry(key_digest(&c.key)) {
            Entry::Occupied(_) => {
                return Err(format!(
                    "client '{}': duplicate key (already used by another client)",
                    c.name
                ))
            }
            Entry::Vacant(slot) => {
                slot.insert(principal(config, &c.name));
            }
        }
        seen_names.push(&c.name);
    }

    // The legacy `client_names` IP map is the third identity entry point
    // (`resolve_client_id`'s fallback) — an IP mapped to a reserved sentinel
    // would resolve real traffic to it, bypassing the header filter above.
    for (ip, name) in &config.client_names {
        // Same failure as an untrimmed `[[clients]]` name above: resolved
        // verbatim by `resolve_client_id`, matches no budget key.
        if name.is_empty() || name != name.trim() {
            return Err(format!(
                "client_names: \"{ip}\" maps to \"{name}\" — value must be non-empty with no leading or trailing whitespace"
            ));
        }
        if name == "-" || name == "_operator" || name == "_other" {
            return Err(format!(
                "client_names: \"{ip}\" maps to reserved name \"{name}\" (\"-\" = unknown-client sentinel, \"_operator\" = operator-aggregation label, \"_other\" = metrics overflow bucket)"
            ));
        }
    }

    // One client registry, not five. Every one of these config surfaces keys on
    // a client name, and every one of them fails SILENTLY on a typo — in the
    // dangerous direction: `check_budget` and `check_utilization_limit` both
    // return Ok(()) for an unknown client, so a mistyped budget means UNLIMITED
    // spend against the operator's accounts with no log line and no metric; a
    // mistyped `operators` entry silently gates the caller it was meant to
    // exempt; a mistyped `response_cache.clients` entry silently makes the
    // cache inert. Only enforceable when [[clients]] is configured — on the
    // legacy path client ids are header-derived and there is no registry to
    // check against.

    // `admin_readers` (LAB-4395) is the one cross-check surface that must fire on
    // the legacy path too, because there it cannot work at all: with a single
    // shared `proxy_key` the key holder IS the operator (`authenticate`
    // returns no principal, so `authorize_admin` serves), and `client_id` on
    // the proxy path is caller-asserted via `x-client-id`. An `admin_readers` entry
    // would therefore restrict nobody and grant nobody — a control that reads
    // as scoping while scoping nothing. Reject rather than half-handle,
    // exactly as with `passthrough` below. Unlike `operators`, there are no
    // pre-existing configs carrying this key, so nothing regresses.
    if !config.admin_readers.is_empty() && !matches!(auth_mode, AuthMode::Clients) {
        return Err(
            "admin_readers: requires [[clients]] — under legacy proxy_key the key holder is the operator by construction and client ids are caller-asserted, so a read-only role cannot be enforced"
                .to_string(),
        );
    }
    // One name, one role. `operators` bypasses every request policy and
    // `admin_readers` is refused every proxied request; a name in both is a config
    // whose author meant one of two opposite things. `pre_request_gate`
    // resolves the overlap to the denial, but silently resolving it is how a
    // typo becomes an outage or an unmetered key — so name it at boot.
    if let Some(dup) = config
        .admin_readers
        .iter()
        .find(|r| config.operators.contains(r))
    {
        return Err(format!(
            "admin_readers: \"{dup}\" is also in operators — a client is either a read-only principal or an operator, never both"
        ));
    }

    if !matches!(auth_mode, AuthMode::Clients) {
        return Ok(principals);
    }

    // `token = "passthrough"` forwards the caller's auth headers to the
    // upstream UNTOUCHED (`inject_account_auth` returns before the
    // header-strip). Under [[clients]] the caller's `x-api-key` is its PROXY
    // credential, not an upstream one — so the two modes together would
    // transmit every client key verbatim to that endpoint's `base_url`. They
    // are contradictory by construction: passthrough means "the caller brings
    // its own upstream credential", [[clients]] means "that header is mine".
    // Reject rather than half-handle.
    if let Some(ep) = config.endpoints.iter().find(|e| e.token == "passthrough") {
        return Err(format!(
            "endpoint '{}': token = \"passthrough\" is incompatible with [[clients]] — passthrough forwards the caller's auth headers upstream, which would leak client keys to {}",
            ep.name,
            ep.base_url.as_deref().unwrap_or("https://api.anthropic.com")
        ));
    }

    let known = |name: &str| seen_names.contains(&name);
    for (surface, name) in std::iter::empty()
        .chain(config.client_budgets.keys().map(|k| ("client_budgets", k)))
        .chain(
            config
                .client_utilization_limits
                .keys()
                .map(|k| ("client_utilization_limits", k)),
        )
        .chain(config.operators.iter().map(|k| ("operators", k)))
        .chain(config.admin_readers.iter().map(|k| ("admin_readers", k)))
        .chain(
            config
                .response_cache
                .iter()
                .flat_map(|rc| rc.clients.iter().map(|k| ("response_cache.clients", k))),
        )
    {
        if !known(name) {
            return Err(format!(
                "{surface}: \"{name}\" names no configured [[clients]] entry"
            ));
        }
    }
    Ok(principals)
}

/// Startup exposure posture (LAB-1192): unauthenticated is a BOOT FAILURE,
/// not a default. Same shape as `reject_legacy_config_keys` — a named error
/// at startup instead of a silent misconfiguration in production, where an
/// open proxy is indistinguishable from a configured one until someone
/// finds it.
fn check_credentials(config: &Config, auth_mode: &AuthMode) -> Result<(), String> {
    let has_credentials = !matches!(auth_mode, AuthMode::Open);
    let allow_unauthenticated = config.allow_unauthenticated.unwrap_or(false);
    if !has_credentials && !allow_unauthenticated {
        return Err(
            "config: no credentials configured — add [[clients]] entries (or legacy proxy_key), \
             or explicitly set allow_unauthenticated = true for a trusted-network-only deployment \
             (see README §Client Setup)"
                .to_string(),
        );
    }
    // One escape hatch, one meaning. Credentials + allow_unauthenticated
    // together would make the flag silently dead (or worse, ambiguous) —
    // exactly the half-applied-migration state the mutual-exclusion rules
    // exist to reject.
    if has_credentials && allow_unauthenticated {
        return Err(
            "config: allow_unauthenticated = true is incompatible with configured credentials — \
             remove it, or remove [[clients]]/proxy_key (see README §Client Setup)"
                .to_string(),
        );
    }
    // AC-13: a static bearer credential on a public ingress gets scanned;
    // below 32 characters the keyspace is the vulnerability.
    if let Some(ref key) = config.proxy_key {
        if key.len() < MIN_KEY_LEN {
            return Err(format!(
                "config: proxy_key is shorter than {MIN_KEY_LEN} characters — generate one with `openssl rand -hex 32`"
            ));
        }
    }
    for c in &config.clients {
        if c.key.len() < MIN_KEY_LEN {
            return Err(format!(
                "client '{}': key is shorter than {MIN_KEY_LEN} characters — generate one with `openssl rand -hex 32`",
                c.name
            ));
        }
    }
    Ok(())
}

/// Range and sentinel rules on the name-keyed policy values. Without
/// `[[clients]]` these names are header-derived, so the registry cross-checks
/// in `check_clients` cannot catch a `-` here.
fn check_policy_values(config: &Config) -> Result<(), String> {
    for (client, limit) in &config.client_utilization_limits {
        if !(0.0..=1.0).contains(limit) {
            return Err(format!(
                "client_utilization_limits.{client}: must be 0.0-1.0, got {limit}"
            ));
        }
    }
    if config.operators.iter().any(|op| op == "-") {
        return Err("operators cannot contain '-' (the unknown-client sentinel)".to_string());
    }
    if config
        .response_cache
        .as_ref()
        .is_some_and(|rc| rc.clients.iter().any(|c| c == "-"))
    {
        return Err(
            "response_cache config error: response_cache.clients cannot contain \"-\" (the unknown-client sentinel — it would opt in every unidentified caller)"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;

use crate::*;
use std::collections::hash_map::Entry;
use std::collections::HashSet;
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

/// One `[[clients]]` entry without its key, or one client of a client store
/// document: what the proxy enforces for that name.
#[derive(Clone)]
pub(crate) struct ClientEntry {
    pub(crate) name: String,
    pub(crate) models: Vec<String>,
    pub(crate) preferred_endpoints: Vec<String>,
    #[cfg(feature = "guard")]
    pub(crate) guard: guard::GuardPolicy,
}

/// Every per-client rule of a config, validated together by `build`, and,
/// on an instance that reads a client store, what its reader holds
/// (`with_store`). Client keys are held only as digests: nothing here, or
/// anywhere it is installed, keeps a plaintext client key. Installed whole
/// and never edited in place: `AppState::swap_client_policy` replaces it.
#[derive(Clone)]
pub(crate) struct ClientPolicy {
    /// `key_digest` of each `[[clients]]` key → the principal it resolves to.
    /// Empty unless the auth mode is `Clients`.
    pub(crate) principals: HashMap<[u8; 32], Principal>,
    pub(crate) clients: Vec<ClientEntry>,
    pub(crate) client_budgets: HashMap<String, u64>,
    pub(crate) client_utilization_limits: HashMap<String, f64>,
    pub(crate) operators: Vec<String>,
    pub(crate) admin_readers: Vec<String>,
    /// Present exactly when the instance reads a client store: what the store
    /// lets it serve. `build` leaves it closed, so an instance serves no
    /// ordinary client until its reader holds a complete set of restrictions.
    pub(crate) store: Option<StoreGate>,
}

/// What a client store lets an instance serve, from what its reader holds
/// (docs/client-store.md). It never touches an operator or an admin reader.
#[derive(Clone, Debug, Default)]
pub(crate) struct StoreGate {
    /// The reader holds a complete set of restrictions. Until it does, no
    /// ordinary client is served, config-file keys included.
    pub(crate) complete: bool,
    /// The reader holds a complete set and a document, so every client it
    /// will ever know can be served. Until then a key that matches nothing
    /// may be one the document will bring, and is answered 503.
    pub(crate) loaded: bool,
    /// Names with a disabled record. Every key of an ordinary client of that
    /// name is refused, from the store or the config file.
    pub(crate) disabled: HashSet<String>,
    /// Store clients whose policy failed this instance's checks. Every key of
    /// the name is refused, config-file keys included.
    pub(crate) quarantined: HashSet<String>,
}

/// Why the client store turns away a credential that may well be good. Never
/// counted as a failed credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreRefusal {
    /// The name has a disabled record: 403 `client_disabled`.
    Disabled,
    /// The store's policy for the name failed this instance's checks: 403
    /// `client_quarantined`.
    Quarantined,
    /// The instance cannot serve the credential yet: 503 with `retry-after`.
    Unloaded,
}

impl StoreRefusal {
    /// Every refusal, in the order the client store exports them. Its
    /// refusal counters are sized by this list.
    pub(crate) const ALL: [Self; 3] = [Self::Disabled, Self::Quarantined, Self::Unloaded];

    /// The `reason` label it is counted under.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Quarantined => "quarantined",
            Self::Unloaded => "unloaded",
        }
    }

    pub(crate) fn response(self) -> Response {
        match self {
            Self::Disabled => {
                proxy_error_response(StatusCode::FORBIDDEN, "permission_error", "client_disabled")
            }
            Self::Quarantined => proxy_error_response(
                StatusCode::FORBIDDEN,
                "permission_error",
                "client_quarantined",
            ),
            Self::Unloaded => {
                let mut resp = proxy_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "overloaded_error",
                    "client table not loaded yet",
                );
                resp.headers_mut()
                    .insert("retry-after", HeaderValue::from_static("1"));
                resp
            }
        }
    }
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

/// A `ClientPolicy` that passed `ClientPolicy::build`, or that `with_store`
/// derived from one: the only kind `AppState::swap_client_policy` installs.
/// The field is private to this module, so no other code can wrap a policy
/// it assembled by hand, whose budgets or pins could name no principal and
/// so never be enforced. Reads go through `Deref`; there is no way back in.
#[derive(Clone)]
pub(crate) struct BuiltPolicy(ClientPolicy);

impl std::ops::Deref for BuiltPolicy {
    type Target = ClientPolicy;

    fn deref(&self) -> &ClientPolicy {
        &self.0
    }
}

impl BuiltPolicy {
    /// The policy itself, for the boot's first install.
    pub(crate) fn into_inner(self) -> ClientPolicy {
        self.0
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
    pub(crate) fn swap_client_policy(&self, next: BuiltPolicy) -> Result<(), &'static str> {
        if !matches!(self.auth_mode, AuthMode::Clients) {
            return Err("the client policy can be replaced only under [[clients]] authentication");
        }
        let next = Arc::new(next.0);
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

/// The digest a client key is held and matched as: the SHA-256 of the key's
/// text. Every key the proxy stores or compares goes through this one
/// function. It is the digest a client store keeps for the keys it mints
/// (docs/client-store.md, "Key digests"), so a config-file key and a store key
/// are matched by the one lookup.
///
/// Matching a presented key is a hash-map lookup on its digest, which is not
/// constant-time and need not be. Timing can reveal at most something about a
/// stored digest, and a digest cannot be turned back into a key, which is
/// what the proxy accepts. That rests on keys being long and random. Boot
/// enforces only the length (`MIN_KEY_LEN`); the documented generator,
/// `openssl rand -hex 32`, supplies the randomness, and a store mints 32
/// random bytes.
///
/// `credential_fingerprint` hashes a rejected credential with another
/// function under its own tag, so the fingerprint a rejection logs has
/// nothing in common with a stored digest.
pub(crate) fn key_digest(key: &str) -> [u8; 32] {
    sha256(key.as_bytes())
}

/// SHA-256 of `bytes`: the one hash behind key digests, client store document
/// digests, restrictions tags and restriction set hashes.
pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
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
    pub(crate) fn build(config: &Config) -> Result<(AuthMode, BuiltPolicy), String> {
        let auth_mode = AuthMode::of(config);
        let principals =
            check_clients(config, &auth_mode).map_err(|msg| format!("config: {msg}"))?;
        check_credentials(config, &auth_mode)?;
        check_policy_values(config)?;
        check_client_store(config, &auth_mode).map_err(|msg| format!("config: {msg}"))?;
        Ok((auth_mode, BuiltPolicy(Self::assemble(config, principals))))
    }

    /// Build with no check at all, for the tests that pin how the runtime
    /// behaves in a state `build` refuses. A duplicate key keeps its last
    /// entry. Never reachable from a boot.
    #[cfg(test)]
    pub(crate) fn unchecked(config: &Config) -> (AuthMode, BuiltPolicy) {
        let principals = config
            .clients
            .iter()
            .map(|c| (key_digest(&c.key), principal(config, &c.name)))
            .collect();
        (
            AuthMode::of(config),
            BuiltPolicy(Self::assemble(config, principals)),
        )
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
            store: config.client_store.as_ref().map(|_| StoreGate::default()),
        }
    }

    /// Whether `name` is an operator's, by the rule a principal's role is
    /// given. Only for folding name-keyed records into the `_operator` label
    /// on `/_stats` and `/metrics`: a request's own role is its `Caller`'s
    /// principal.
    pub(crate) fn is_operator(&self, name: &str) -> bool {
        role_in(&self.operators, &self.admin_readers, name) == Role::Operator
    }

    /// Whether `name` is an operator's or an admin reader's: a name a client
    /// store can neither grant nor restrict.
    pub(crate) fn is_privileged(&self, name: &str) -> bool {
        self.is_operator(name) || self.admin_readers.iter().any(|r| r == name)
    }

    /// Why the client store turns `principal` away, if it does. Only an
    /// ordinary client is ever refused: a store never touches an operator or
    /// an admin reader.
    pub(crate) fn store_refusal(&self, principal: &Principal) -> Option<StoreRefusal> {
        let gate = self.store.as_ref()?;
        if principal.role != Role::Client {
            None
        } else if gate.disabled.contains(&principal.name) {
            Some(StoreRefusal::Disabled)
        } else if !gate.complete {
            Some(StoreRefusal::Unloaded)
        } else if gate.quarantined.contains(&principal.name) {
            Some(StoreRefusal::Quarantined)
        } else {
            None
        }
    }
}

impl BuiltPolicy {
    /// This policy, the config file's, with what a client store reader holds
    /// applied (docs/client-store.md): the document's clients join the
    /// principal table and govern their names, and the restrictions set the
    /// gate. `endpoints` are this instance's endpoint names, the only ones a
    /// store client may be pinned to here. Also returns what the operator
    /// should be told about the result.
    pub(crate) fn with_store(&self, reader: &Reader, endpoints: &[String]) -> (Self, StoreReport) {
        let mut next = self.0.clone();
        let mut report = StoreReport::default();
        let mut quarantined = HashSet::new();
        // Every config-file key, whatever its role.
        let file_digests: HashSet<[u8; 32]> = self.principals.keys().copied().collect();
        let set = &reader.set;
        for client in reader.document.iter().flat_map(|d| &d.clients) {
            let name = &client.name;
            // The config file's operator or admin reader keeps the name, its
            // keys and its policy; the store's client is dropped whole.
            if self.is_privileged(name) {
                report.privileged_clients.push(name.clone());
                continue;
            }
            // A store key that is a config-file key: only an edit or a
            // hand-written import puts one there. The key keeps working for
            // its own principal, and the store's client falls.
            let policy = match &client.policy {
                Ok(_) if client.digests.iter().any(|d| file_digests.contains(d)) => {
                    Err("key.digest_in_config")
                }
                policy => policy.as_ref().map_err(|rule| *rule),
            };
            for d in &client.digests {
                // A revoked digest refuses its store key for good.
                if !set.revoked_digests.contains(d) && !file_digests.contains(d) {
                    next.principals.insert(
                        *d,
                        Principal {
                            name: name.clone(),
                            role: Role::Client,
                        },
                    );
                }
            }
            // The store's policy governs the name, config-file keys included.
            next.client_budgets.remove(name);
            next.client_utilization_limits.remove(name);
            let file_entry = next.clients.iter().position(|c| c.name == *name);
            #[cfg(feature = "guard")]
            let guard = file_entry
                .map(|i| next.clients[i].guard)
                .unwrap_or_default();
            if let Some(i) = file_entry {
                next.clients.remove(i);
            }
            let policy = match policy {
                Ok(policy) => policy,
                Err(rule) => {
                    quarantined.insert(name.clone());
                    report.quarantined.push((name.clone(), rule));
                    continue;
                }
            };
            let (pins, dropped): (Vec<String>, Vec<String>) = policy
                .preferred_endpoints
                .iter()
                .cloned()
                .partition(|pin| endpoints.contains(pin));
            report
                .dropped_pins
                .extend(dropped.into_iter().map(|pin| (name.clone(), pin)));
            next.clients.push(ClientEntry {
                name: name.clone(),
                models: policy.models.clone(),
                preferred_endpoints: pins,
                // Schema 1 has no guard policy, so the config file's stays.
                #[cfg(feature = "guard")]
                guard,
            });
            if let Some(budget) = policy.daily_budget {
                next.client_budgets.insert(name.clone(), budget);
            }
            if let Some(limit) = policy.utilization_limit {
                next.client_utilization_limits.insert(name.clone(), limit);
            }
        }

        let disabled: HashSet<String> = set
            .records
            .iter()
            .filter(|(_, r)| r.state == RecordState::Disabled)
            .map(|(name, _)| name.clone())
            .collect();
        let known = |name: &str| {
            self.clients.iter().any(|c| c.name == name)
                || reader
                    .document
                    .iter()
                    .flat_map(|d| &d.clients)
                    .any(|c| c.name == name)
        };
        for name in &disabled {
            if self.is_privileged(name) {
                report.privileged_disabled.push(name.clone());
            } else if !known(name) {
                report.unmatched_disabled.push(name.clone());
            }
        }
        report.revoked_outside_store = set
            .revoked_digests
            .iter()
            .filter(|d| file_digests.contains(*d))
            .count();
        report.privileged_disabled.sort();
        report.unmatched_disabled.sort();

        next.store = Some(StoreGate {
            complete: set.complete,
            loaded: reader.loaded(),
            disabled,
            quarantined,
        });
        (Self(next), report)
    }
}

/// What an operator should hear about a policy `with_store` built: every
/// place the store and this instance's config file disagree.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StoreReport {
    /// Store clients quarantined here, with the rule each broke.
    pub(crate) quarantined: Vec<(String, &'static str)>,
    /// Store clients dropped because their name is an operator's or an admin
    /// reader's here.
    pub(crate) privileged_clients: Vec<String>,
    /// Disabled records naming an operator or an admin reader here, which
    /// refuse nothing.
    pub(crate) privileged_disabled: Vec<String>,
    /// Disabled names that match no client this instance knows: a mistyped
    /// `disable`, or a client not yet created.
    pub(crate) unmatched_disabled: Vec<String>,
    /// Revoked digests matching a config-file key. A store revokes only its
    /// own keys, so these are ignored here.
    pub(crate) revoked_outside_store: usize,
    /// Pins dropped for naming an endpoint this instance lacks:
    /// `(client, endpoint)`.
    pub(crate) dropped_pins: Vec<(String, String)>,
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
    if !is_name(name) {
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
    // Under a client store a response-cache tenant may name a client the
    // store will bring. It stays inert until that client is loaded, so only
    // the name rule applies to it.
    let tenants_from_file = config.client_store.is_none();
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
                .filter(|_| tenants_from_file)
                .flat_map(|rc| rc.clients.iter().map(|k| ("response_cache.clients", k))),
        )
    {
        if !known(name) {
            return Err(format!(
                "{surface}: \"{name}\" names no configured [[clients]] entry"
            ));
        }
    }
    if let Some(name) = config
        .response_cache
        .iter()
        .flat_map(|rc| &rc.clients)
        .find(|name| !is_name(name))
    {
        return Err(format!(
            "response_cache.clients: \"{name}\" is not a client name (^[a-z0-9][a-z0-9_-]{{0,62}}$)"
        ));
    }
    Ok(principals)
}

/// `[client_store]`: only under `[[clients]]`, where the operators and admin
/// readers stay in this file and every other caller is a principal from a
/// key. Under a shared key or an open proxy a caller's name is its own claim,
/// so a store's clients and restrictions could protect nothing. A token is
/// sent only over https.
fn check_client_store(config: &Config, auth_mode: &AuthMode) -> Result<(), String> {
    let Some(store) = &config.client_store else {
        return Ok(());
    };
    if !matches!(auth_mode, AuthMode::Clients) {
        return Err(
            "client_store: requires [[clients]] authentication — the store's clients are \
             principals of a client key, and under proxy_key or allow_unauthenticated a \
             caller's name is its own claim"
                .to_string(),
        );
    }
    let url = reqwest::Url::parse(&store.poll_url)
        .map_err(|_| "client_store.poll_url: not a URL".to_string())?;
    if !matches!(url.scheme(), "https" | "http") || !url.has_host() {
        return Err("client_store.poll_url: must be an http:// or https:// URL".to_string());
    }
    // A user or password in the URL would be sent as basic auth, over plain
    // http too, and the URL is logged at boot.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(
            "client_store.poll_url: must not carry credentials — use client_store.token"
                .to_string(),
        );
    }
    match &store.token {
        Some(token) if token.is_empty() => {
            Err("client_store.token: must not be empty (remove it to poll without one)".to_string())
        }
        Some(_) if url.scheme() != "https" => Err(
            "client_store.token: a token is sent only over https:// — the poll_url is plain http"
                .to_string(),
        ),
        _ => Ok(()),
    }
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

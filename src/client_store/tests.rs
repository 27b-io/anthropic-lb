use super::*;
use serde_json::{json, Map, Value};

mod corpus;
mod reader;
mod runtime;
mod serving;

// ── Fixtures ─────────────────────────────────────────────────────────

/// The reader's clock in the pure tests: 2026-10-11T06:26:40Z, in Unix
/// microseconds.
const NOW_US: u64 = 1_791_700_000_000_000;
const NOW_MS: u64 = NOW_US / 1000;
const MINUTE_US: u64 = 60_000_000;
const MINUTE_MS: u64 = 60_000;

use RecordState::{Disabled, Enabled};

/// A store client named `name` with one key per `keys` entry, the
/// document's defaults for every policy member, and the members of `policy`
/// over them.
fn client_with(name: &str, keys: &[&str], policy: Value) -> Value {
    let keys: Vec<Value> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            json!({
                "id": format!("k000000{i}"),
                "label": "",
                "digest": digest_hex(&key_digest(key)),
                "created_at": "2026-10-01T09:30:00.000Z",
            })
        })
        .collect();
    let mut client = json!({
        "name": name,
        "daily_budget": null,
        "utilization_limit": null,
        "models": [],
        "preferred_endpoints": [],
        "keys": keys,
    });
    for (member, value) in policy.as_object().expect("policy object") {
        client[member] = value.clone();
    }
    client
}

/// A store client with the one key `key` and no limits.
fn client(name: &str, key: &str) -> Value {
    client_with(name, &[key], json!({}))
}

/// A schema 1 document's text, as a store writes it.
fn document(version: u64, clients: Vec<Value>, revoked: &[[u8; 32]]) -> String {
    serde_json::to_string(&json!({
        "schema": 1,
        "version": version,
        "updated_at": "2026-10-11T06:26:40.123Z",
        "clients": clients,
        "revoked_digests": revoked.iter().map(digest_hex).collect::<Vec<_>>(),
    }))
    .unwrap()
}

/// A restrictions body, as a store writes it.
fn body(full: bool, records: &[(&str, RecordState, u64)], revoked: &[[u8; 32]]) -> String {
    body_ahead(full, 0, records, revoked)
}

/// A restrictions body whose store left out `enabled_ahead` enabled records.
fn body_ahead(
    full: bool,
    enabled_ahead: u64,
    records: &[(&str, RecordState, u64)],
    revoked: &[[u8; 32]],
) -> String {
    let mut records: Vec<(&str, RecordState, u64)> = records.to_vec();
    records.sort_by_key(|(name, ..)| *name);
    let mut revoked: Vec<String> = revoked.iter().map(digest_hex).collect();
    revoked.sort();
    serde_json::to_string(&json!({
        "full": full,
        "enabled_ahead": enabled_ahead,
        "records": records
            .iter()
            .map(|(name, state, version)| json!({"name": name, "state": state.as_str(), "version": version}))
            .collect::<Vec<_>>(),
        "revoked_digests": revoked,
    }))
    .unwrap()
}

/// The document part of an answer that carries `text`.
fn doc_part(text: &str) -> Value {
    let version = serde_json::from_str::<Value>(text).unwrap()["version"]
        .as_u64()
        .unwrap();
    json!({
        "status": "ok",
        "version": version,
        "digest": digest_hex(&sha256(text.as_bytes())),
        "text": text,
    })
}

/// The document part of an answer to a reader that already holds `text`.
fn doc_part_held(text: &str) -> Value {
    let mut part = doc_part(text);
    part.as_object_mut().unwrap().remove("text");
    part
}

/// The restrictions part of an answer that carries `body`.
fn restrictions_part(body: &str) -> Value {
    json!({
        "status": "ok",
        "tag": digest_hex(&sha256(body.as_bytes())),
        "body": body,
    })
}

fn unavailable() -> Value {
    json!({"status": "unavailable"})
}

/// A poll answer.
fn answer(document: Value, restrictions: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"document": document, "restrictions": restrictions})).unwrap()
}

/// A digest that is no key's.
fn digest(byte: u8) -> [u8; 32] {
    [byte; 32]
}

/// One query parameter of `query`.
fn param<'a>(query: &'a [(&'static str, String)], name: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, v)| v.as_str())
}

// ── An instance reading a client store ──────────────────────────────

/// The config file every instance below starts from: the operator `ops`, the
/// admin reader `viewer` (denied every model, as a scrape credential is), and
/// one ordinary client per name in `ordinary`. Each key is `test_key(name)`.
fn file_clients(ordinary: &[&str]) -> Vec<ClientConfig> {
    let mut clients = vec![
        mk_client("ops", &test_key("ops"), &[]),
        mk_client("viewer", &test_key("viewer"), &[""]),
    ];
    clients.extend(ordinary.iter().map(|n| mk_client(n, &test_key(n), &[])));
    clients
}

/// An instance with the endpoint `acct-a` at `upstream`, the config file of
/// `file_clients`, and a client store polled at `poll_url`. Its resume copy
/// lives in a directory removed with the fixture.
struct Instance {
    state: Arc<AppState>,
    store: Arc<ClientStore>,
    _dir: Option<tempfile::TempDir>,
}

impl Instance {
    fn new(
        upstream: &str,
        clients: Vec<ClientConfig>,
        poll_url: &str,
        token: Option<&str>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut inst = Self::in_dir(upstream, clients, poll_url, token, dir.path());
        inst._dir = Some(dir);
        inst
    }

    /// An instance whose resume copy is kept in `dir`, which outlives it.
    fn in_dir(
        upstream: &str,
        clients: Vec<ClientConfig>,
        poll_url: &str,
        token: Option<&str>,
        dir: &std::path::Path,
    ) -> Self {
        let mut acct = mk_endpoint("acct-a", "sk-ant-api-test-aaa");
        acct.base_url = upstream.to_string();
        let state = TestPolicy::clients(clients)
            .operators(&["ops"])
            .admin_readers(&["viewer"])
            .client_store(poll_url, token)
            .install(AppState {
                endpoints: vec![acct],
                ..test_state_base()
            });
        let config = ClientStoreConfig {
            poll_url: poll_url.to_string(),
            token: token.map(str::to_string),
        };
        let store = Arc::new(
            ClientStore::new(
                &config,
                &state.current_client_policy(),
                vec!["acct-a".to_string()],
                dir.join("config.client-store.json"),
            )
            .unwrap(),
        );
        let state = Arc::new(AppState {
            client_store: Some(store.clone()),
            ..state
        });
        Self {
            state,
            store,
            _dir: None,
        }
    }

    /// An instance whose store is never polled: tests hand it answers.
    fn offline(ordinary: &[&str]) -> Self {
        Self::new(
            "http://127.0.0.1:9",
            file_clients(ordinary),
            "https://store.invalid/_clients/poll",
            None,
        )
    }

    /// Apply `answer` at the system clock, as a poll does.
    fn answer(&self, answer: Vec<u8>) {
        self.store
            .apply(&self.state, &answer, now_us())
            .expect("readable answer");
    }

    /// Apply `answer` and save the resume copy, as a poll does.
    async fn answer_and_save(&self, answer: Vec<u8>) {
        let changed = self
            .store
            .apply(&self.state, &answer, now_us())
            .expect("readable answer");
        self.store.save(changed.resume).await;
    }

    /// How authenticating `key` turns out: the principal's name, or the
    /// status the request is answered with.
    fn auth(&self, key: &str) -> Result<String, u16> {
        match self.state.authenticate(&hdrs(&[("x-api-key", key)]), false) {
            Ok(auth) => Ok(auth.principal.expect("a principal").name),
            Err(rejection) => Err(rejection.into_response().status().as_u16()),
        }
    }

    fn metrics(&self) -> ClientStoreMetrics {
        self.store.metrics()
    }

    fn alert(&self, name: &str) -> bool {
        self.metrics()
            .alerts
            .into_iter()
            .find(|(alert, _)| *alert == name)
            .unwrap_or_else(|| panic!("no alert {name}"))
            .1
    }
}

/// The system clock in Unix milliseconds, for documents an instance must
/// install now.
fn wall_ms() -> u64 {
    now_us() / 1000
}

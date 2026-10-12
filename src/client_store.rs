use crate::*;
use serde_json::value::RawValue;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

// ── Client store reader ─────────────────────────────────────────────
//
// A client store keeps a fleet's ordinary clients in one place: the document,
// which grants (clients, keys, policies), and the restrictions, which deny
// (disabled names, revoked key digests). Every instance polls the store and
// enforces what it receives; it can never write. docs/client-store.md is the
// contract between a store and its readers, and tests/client-store/ the
// corpus both sides test against.
//
// The first half of this module is that contract on the reader's side, as
// pure functions of their input and the reader's clock: the document and part
// checks, the merge, the set hash, and `Reader`, which turns one poll answer
// into what the instance holds. The second half runs it: the poll loop, the
// resume file, the logs and `/_ready`. `BuiltPolicy::with_store` turns what a
// reader holds into the policy requests authenticate against.

/// The largest document text or restrictions body a reader accepts: 2 MiB.
pub(crate) const MAX_TEXT_BYTES: usize = 2 * 1024 * 1024;
/// The largest integer the format carries: 2^53−1, JavaScript's largest safe
/// integer.
const MAX_INT: u64 = (1 << 53) - 1;
/// How far ahead of the reader's clock a document version may be: 15 minutes,
/// in the Unix milliseconds versions count.
pub(crate) const AHEAD_LIMIT_MS: u64 = 15 * 60 * 1000;
/// The same limit for record versions, which count Unix microseconds.
pub(crate) const AHEAD_LIMIT_US: u64 = AHEAD_LIMIT_MS * 1000;

const MAX_CLIENTS: usize = 64;
const MAX_MODELS: usize = 64;
const MAX_PINS: usize = 16;
const MAX_KEYS: usize = 2;

const DOCUMENT_MEMBERS: [&str; 5] = [
    "schema",
    "version",
    "updated_at",
    "clients",
    "revoked_digests",
];
const CLIENT_MEMBERS: [&str; 6] = [
    "name",
    "daily_budget",
    "utilization_limit",
    "models",
    "preferred_endpoints",
    "keys",
];
const KEY_MEMBERS: [&str; 4] = ["id", "label", "digest", "created_at"];
const PART_MEMBERS: [&str; 4] = ["full", "enabled_ahead", "records", "revoked_digests"];
const RECORD_MEMBERS: [&str; 3] = ["name", "state", "version"];

// ── Values ──────────────────────────────────────────────────────────

/// The name rule client, pin and record names share, the one `[[clients]]`
/// names follow: `^[a-z0-9][a-z0-9_-]{0,62}$`. A leading `_` is reserved.
pub(crate) fn is_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..]
            .iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-')
}

/// A digest's text, 64 lowercase hex characters, as its 32 bytes.
pub(crate) fn parse_digest(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (byte, [hi, lo]) in out.iter_mut().zip(b.as_chunks::<2>().0) {
        *byte = (nibble(*hi)? << 4) | nibble(*lo)?;
    }
    Some(out)
}

/// A digest's text: 64 lowercase hex characters.
pub(crate) fn digest_hex(d: &[u8; 32]) -> String {
    use std::fmt::Write;
    d.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ`, as `Date.prototype.toISOString` writes it.
/// Only the pattern is checked.
fn is_timestamp(s: &str) -> bool {
    const PATTERN: &[u8; 24] = b"0000-00-00T00:00:00.000Z";
    s.len() == PATTERN.len()
        && s.bytes().zip(PATTERN).all(|(c, &p)| {
            if p == b'0' {
                c.is_ascii_digit()
            } else {
                c == p
            }
        })
}

/// A JSON object's members, each kept as its raw text until a rule reads it.
/// A member no rule reads is never built, however large, so checking a text
/// costs a small multiple of its size whatever it holds.
type Members<'a> = HashMap<String, &'a RawValue>;

/// `raw` as a `T`, or `None` when it is not one. A typed parse fails at the
/// first token of another kind, without building the value it meets.
fn parse<'a, T: Deserialize<'a>>(raw: &'a RawValue) -> Option<T> {
    serde_json::from_str(raw.get()).ok()
}

/// A JSON integer from 0 to 2^53−1. Only a number written as plain digits
/// parses to a `u64`, so a sign, a fraction or an exponent fails here.
fn int(raw: &RawValue) -> Option<u64> {
    parse::<u64>(raw).filter(|&n| n <= MAX_INT)
}

/// 1 to 128 printable ASCII characters other than `"`, `,` and `\`, with `*`
/// at most once and only last.
fn is_model_pattern(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=128).contains(&b.len())
        && b.iter()
            .all(|&c| (b'!'..=b'~').contains(&c) && !matches!(c, b'"' | b',' | b'\\'))
        && s.find('*').is_none_or(|i| i == b.len() - 1)
}

/// `k` and 7 lowercase hex digits.
fn is_key_id(s: &str) -> bool {
    s.len() == 8
        && s.starts_with('k')
        && s.bytes()
            .skip(1)
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// 0 to 64 printable ASCII characters, the space included.
fn is_label(s: &str) -> bool {
    s.len() <= 64 && s.bytes().all(|c| (b' '..=b'~').contains(&c))
}

/// Whether `obj` holds exactly the members `names`.
fn has_members(obj: &Members, names: &[&str]) -> bool {
    obj.len() == names.len() && names.iter().all(|n| obj.contains_key(*n))
}

/// The string member `name` of `obj`, if it is one.
fn str_member(obj: &Members, name: &str) -> Option<String> {
    obj.get(name).and_then(|raw| parse(raw))
}

/// `raw` as the elements of an array, each still raw.
fn elements<'a>(raw: Option<&&'a RawValue>) -> Option<Vec<&'a RawValue>> {
    parse(raw?)
}

/// `raw` as an array of at most `max` strings that each pass `valid`.
fn strings(raw: Option<&&RawValue>, max: usize, valid: fn(&str) -> bool) -> Option<Vec<String>> {
    let items = elements(raw)?;
    if items.len() > max {
        return None;
    }
    items
        .into_iter()
        .map(|s| parse::<String>(s).filter(|s| valid(s)))
        .collect()
}

/// `raw` as a digest's 32 bytes, if it is a digest's text.
fn digest_of(raw: Option<&&RawValue>) -> Option<[u8; 32]> {
    parse::<String>(raw?).as_deref().and_then(parse_digest)
}

// ── The document ────────────────────────────────────────────────────

/// A document that passed the envelope checks (docs/client-store.md,
/// "Checks"). Whether a reader installs it also depends on the version the
/// reader holds and on its clock (`Reader`).
pub(crate) struct Document {
    /// The exact text the store wrote.
    pub(crate) text: String,
    /// The SHA-256 of `text`.
    pub(crate) digest: [u8; 32],
    pub(crate) version: u64,
    pub(crate) clients: Vec<StoreClient>,
    pub(crate) revoked_digests: Vec<[u8; 32]>,
}

/// One client of a document. Its name and key digests passed the envelope
/// checks, so each digest maps to exactly this name. Its policy is the client
/// rule that quarantines it, when one does.
pub(crate) struct StoreClient {
    pub(crate) name: String,
    pub(crate) digests: Vec<[u8; 32]>,
    pub(crate) policy: Result<StorePolicy, &'static str>,
}

/// What a document lets one client do, with the meaning the same settings
/// have in a config file.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StorePolicy {
    pub(crate) daily_budget: Option<u64>,
    pub(crate) utilization_limit: Option<f64>,
    pub(crate) models: Vec<String>,
    pub(crate) preferred_endpoints: Vec<String>,
}

/// Check `text` as a document. `Err` is the envelope rule that refuses it,
/// the first in the table's order. A client rule quarantines only its client,
/// inside the `Ok`.
pub(crate) fn check_document(text: &str) -> Result<Document, &'static str> {
    if text.len() > MAX_TEXT_BYTES {
        return Err("document.size");
    }
    let doc: Members = serde_json::from_str(text).map_err(|_| "document.object")?;
    // Before any other member, so a newer schema is reported as one.
    if doc.get("schema").and_then(|raw| int(raw)) != Some(1) {
        return Err("document.schema");
    }
    if !has_members(&doc, &DOCUMENT_MEMBERS) {
        return Err("document.members");
    }
    let version = doc
        .get("version")
        .and_then(|raw| int(raw))
        .filter(|&v| v >= 1)
        .ok_or("document.version")?;
    if !str_member(&doc, "updated_at").is_some_and(|t| is_timestamp(&t)) {
        return Err("document.updated_at");
    }
    let clients: Vec<Members> = elements(doc.get("clients"))
        .filter(|a| a.len() <= MAX_CLIENTS)
        .and_then(|a| a.into_iter().map(parse).collect())
        .ok_or("document.clients")?;
    let names: Vec<String> = clients
        .iter()
        .map(|c| str_member(c, "name"))
        .collect::<Option<_>>()
        .ok_or("document.name")?;
    let mut seen_names = HashSet::with_capacity(names.len());
    if !names
        .iter()
        .all(|n| is_name(n) && seen_names.insert(n.as_str()))
    {
        return Err("document.name");
    }
    let keys: Vec<Vec<Members>> = clients
        .iter()
        .map(|c| {
            elements(c.get("keys"))?
                .into_iter()
                .map(|k| parse::<Members>(k).filter(|k| k.contains_key("digest")))
                .collect::<Option<Vec<_>>>()
        })
        .collect::<Option<_>>()
        .ok_or("document.keys")?;
    let revoked = elements(doc.get("revoked_digests")).ok_or("document.revoked_digests")?;
    let digests: Vec<Vec<[u8; 32]>> = keys
        .iter()
        .map(|ks| ks.iter().map(|k| digest_of(k.get("digest"))).collect())
        .collect::<Option<_>>()
        .ok_or("document.digest")?;
    let revoked_digests: Vec<[u8; 32]> = revoked
        .iter()
        .map(|d| digest_of(Some(d)))
        .collect::<Option<_>>()
        .ok_or("document.digest")?;
    let mut seen = HashSet::new();
    if !digests
        .iter()
        .flatten()
        .chain(&revoked_digests)
        .all(|d| seen.insert(*d))
    {
        return Err("document.digest_once");
    }
    let clients = clients
        .iter()
        .zip(names)
        .zip(keys)
        .zip(digests)
        .map(|(((c, name), keys), digests)| StoreClient {
            name,
            digests,
            policy: check_client(c, &keys),
        })
        .collect();
    Ok(Document {
        text: text.to_string(),
        digest: sha256(text.as_bytes()),
        version,
        clients,
        revoked_digests,
    })
}

/// The client rules, in the table's order, on one client whose envelope
/// already passed: `Err` is the first rule it breaks. Rules on keys are
/// checked across all of the client's keys before the next rule.
fn check_client(c: &Members, keys: &[Members]) -> Result<StorePolicy, &'static str> {
    if !has_members(c, &CLIENT_MEMBERS) {
        return Err("client.members");
    }
    // `Some(None)` is `null`: no budget, or no ceiling.
    let daily_budget = match c
        .get("daily_budget")
        .and_then(|raw| parse::<Option<u64>>(raw))
    {
        Some(None) => None,
        Some(Some(n)) if (1..=MAX_INT).contains(&n) => Some(n),
        _ => return Err("client.daily_budget"),
    };
    let utilization_limit = match c
        .get("utilization_limit")
        .and_then(|raw| parse::<Option<f64>>(raw))
    {
        Some(None) => None,
        Some(Some(l)) if l > 0.0 && l <= 1.0 => Some(l),
        _ => return Err("client.utilization_limit"),
    };
    let models = strings(c.get("models"), MAX_MODELS, is_model_pattern).ok_or("client.models")?;
    let preferred_endpoints = strings(c.get("preferred_endpoints"), MAX_PINS, is_name)
        .ok_or("client.preferred_endpoints")?;
    if keys.len() > MAX_KEYS {
        return Err("client.keys");
    }
    if !keys.iter().all(|k| has_members(k, &KEY_MEMBERS)) {
        return Err("key.members");
    }
    let ids: Vec<String> = keys
        .iter()
        .map(|k| str_member(k, "id").filter(|id| is_key_id(id)))
        .collect::<Option<_>>()
        .ok_or("key.id")?;
    if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
        return Err("key.id");
    }
    if !keys
        .iter()
        .all(|k| str_member(k, "label").is_some_and(|l| is_label(&l)))
    {
        return Err("key.label");
    }
    if !keys
        .iter()
        .all(|k| str_member(k, "created_at").is_some_and(|t| is_timestamp(&t)))
    {
        return Err("key.created_at");
    }
    Ok(StorePolicy {
        daily_budget,
        utilization_limit,
        models,
        preferred_endpoints,
    })
}

// ── The restrictions ────────────────────────────────────────────────

/// A restriction record's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordState {
    Disabled,
    Enabled,
}

impl RecordState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "disabled" => Some(Self::Disabled),
            "enabled" => Some(Self::Enabled),
            _ => None,
        }
    }
}

/// One name's restriction: its state, and the version (Unix microseconds of
/// the store's clock) at which it was set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) state: RecordState,
    pub(crate) version: u64,
}

/// A restrictions body that passed the part rules.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PartBody {
    /// The store vouches that the part carries all of its restrictions.
    pub(crate) full: bool,
    /// Enabled records the store left out for being ahead of its clock.
    pub(crate) enabled_ahead: u64,
    pub(crate) records: BTreeMap<String, Record>,
    pub(crate) revoked_digests: BTreeSet<[u8; 32]>,
}

/// Check a restrictions body against its tag. `Err` is the first part rule
/// it breaks, in the table's order: size and tag before the body is parsed.
pub(crate) fn check_part(tag: &str, body: &str) -> Result<PartBody, &'static str> {
    if body.len() > MAX_TEXT_BYTES {
        return Err("part.size");
    }
    if parse_digest(tag) != Some(sha256(body.as_bytes())) {
        return Err("part.tag");
    }
    let part: Members = serde_json::from_str(body).map_err(|_| "part.object")?;
    if !has_members(&part, &PART_MEMBERS) {
        return Err("part.members");
    }
    let full = part
        .get("full")
        .and_then(|raw| parse::<bool>(raw))
        .ok_or("part.full")?;
    let enabled_ahead = part
        .get("enabled_ahead")
        .and_then(|raw| int(raw))
        .ok_or("part.enabled_ahead")?;
    let records: Vec<Members> = elements(part.get("records"))
        .and_then(|a| {
            a.into_iter()
                .map(|r| parse::<Members>(r).filter(|r| has_members(r, &RECORD_MEMBERS)))
                .collect()
        })
        .ok_or("part.records")?;
    let mut seen = HashSet::with_capacity(records.len());
    let names: Vec<String> = records
        .iter()
        .map(|r| str_member(r, "name").filter(|n| is_name(n) && seen.insert(n.clone())))
        .collect::<Option<_>>()
        .ok_or("part.record_name")?;
    let states: Vec<RecordState> = records
        .iter()
        .map(|r| {
            str_member(r, "state")
                .as_deref()
                .and_then(RecordState::parse)
        })
        .collect::<Option<_>>()
        .ok_or("part.record_state")?;
    let versions: Vec<u64> = records
        .iter()
        .map(|r| {
            r.get("version")
                .and_then(|raw| int(raw))
                .filter(|&v| v >= 1)
        })
        .collect::<Option<_>>()
        .ok_or("part.record_version")?;
    let digests = elements(part.get("revoked_digests")).ok_or("part.revoked_digests")?;
    let mut revoked_digests = BTreeSet::new();
    for d in &digests {
        match digest_of(Some(d)) {
            Some(d) if revoked_digests.insert(d) => {}
            _ => return Err("part.revoked_digests"),
        }
    }
    let records = names
        .into_iter()
        .zip(states)
        .zip(versions)
        .map(|((name, state), version)| (name, Record { state, version }))
        .collect();
    Ok(PartBody {
        full,
        enabled_ahead,
        records,
        revoked_digests,
    })
}

/// The restrictions a reader holds. Every part it accepts is merged in, and
/// nothing is ever replaced whole, so a store that loses a restriction cannot
/// lift it here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RestrictionSet {
    /// Set once a full part has been merged, and never cleared.
    pub(crate) complete: bool,
    pub(crate) records: BTreeMap<String, Record>,
    /// Every revoked digest the reader has seen, from parts and documents.
    /// None ever leaves.
    pub(crate) revoked_digests: BTreeSet<[u8; 32]>,
}

/// What the last merged part lacks of the set a reader holds: the store has
/// lost these, or not yet written them.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Behind {
    pub(crate) disabled: Vec<String>,
    pub(crate) revoked_digests: Vec<[u8; 32]>,
}

impl Behind {
    pub(crate) fn is_empty(&self) -> bool {
        self.disabled.is_empty() && self.revoked_digests.is_empty()
    }
}

impl RestrictionSet {
    /// Merge one accepted part at the reader's clock `now_us`
    /// (docs/client-store.md, "Merging a part"). Returns the names of the
    /// enabled records skipped for being more than 15 minutes ahead.
    pub(crate) fn merge(&mut self, part: &PartBody, now_us: u64) -> Vec<String> {
        let ceiling = now_us.saturating_add(AHEAD_LIMIT_US);
        let mut skipped_ahead = Vec::new();
        for (name, record) in &part.records {
            if record.state == RecordState::Enabled {
                // A damaged table can add denials but never lift one.
                if !part.full {
                    continue;
                }
                // Skipping an enabled record can only keep a name disabled.
                if record.version > ceiling {
                    skipped_ahead.push(name.clone());
                    continue;
                }
            }
            let wins = match self.records.get(name) {
                None => true,
                Some(held) => {
                    record.version > held.version
                        || (record.version == held.version
                            && record.state == RecordState::Disabled
                            && held.state == RecordState::Enabled)
                }
            };
            if wins {
                self.records.insert(name.clone(), *record);
            }
        }
        if part.full {
            self.records.retain(|name, held| {
                held.state == RecordState::Disabled || part.records.contains_key(name)
            });
            self.complete = true;
        }
        self.revoked_digests.extend(&part.revoked_digests);
        skipped_ahead
    }

    /// The disables and digests this set holds that `part` lacks. Every record
    /// in the part counts, merged or skipped.
    pub(crate) fn behind(&self, part: &PartBody) -> Behind {
        let disabled = self
            .records
            .iter()
            .filter(|(_, held)| held.state == RecordState::Disabled)
            .filter(|(name, held)| {
                !part.records.get(*name).is_some_and(|p| match p.state {
                    RecordState::Disabled => p.version >= held.version,
                    RecordState::Enabled => p.version > held.version,
                })
            })
            .map(|(name, _)| name.clone())
            .collect();
        let revoked_digests = self
            .revoked_digests
            .difference(&part.revoked_digests)
            .copied()
            .collect();
        Behind {
            disabled,
            revoked_digests,
        }
    }

    /// The highest version among the records held, leaving out any more than
    /// 15 minutes ahead of `now_us`, or 0: what a poll reports.
    pub(crate) fn records_version(&self, now_us: u64) -> u64 {
        let ceiling = now_us.saturating_add(AHEAD_LIMIT_US);
        self.records
            .values()
            .map(|r| r.version)
            .filter(|&v| v <= ceiling)
            .max()
            .unwrap_or(0)
    }

    /// The set text (docs/client-store.md, "The set hash"): one line per
    /// record in byte order of name, then one per digest in order.
    pub(crate) fn text(&self) -> String {
        use std::fmt::Write;
        let mut text = String::new();
        for (name, r) in &self.records {
            let _ = writeln!(text, "r {name} {} {}", r.state.as_str(), r.version);
        }
        for d in &self.revoked_digests {
            let _ = writeln!(text, "d {}", digest_hex(d));
        }
        text
    }

    /// The set hash: the SHA-256 of the set text.
    pub(crate) fn hash(&self) -> [u8; 32] {
        sha256(self.text().as_bytes())
    }
}

// ── The reader ──────────────────────────────────────────────────────

/// What a reader holds, and what its polls found. Each poll is built by
/// `query` and its answer applied by `apply`, both at the reader's clock.
#[derive(Clone, Default)]
pub(crate) struct Reader {
    /// The installed document.
    pub(crate) document: Option<Arc<Document>>,
    pub(crate) set: RestrictionSet,
    /// The tag and body of the last part merged, kept to judge what the store
    /// lacks whenever the held set changes.
    last_part: Option<([u8; 32], PartBody)>,
    /// The enabled records the last merge skipped for being ahead. While any
    /// are, polls carry no tag, so every answer brings the body again.
    pub(crate) skipped_ahead: Vec<String>,
    pub(crate) status: PollStatus,
}

/// What the last polls found, for the alerts and the metrics.
#[derive(Clone, Default)]
pub(crate) struct PollStatus {
    /// The store's version is below the one held.
    pub(crate) store_behind: bool,
    /// The store's digest differs at the version held: it was changed outside
    /// its write path.
    pub(crate) store_edited: bool,
    /// The store's document was refused for being more than 15 minutes ahead.
    pub(crate) document_ahead: bool,
    /// The fault that refused the last document part, if it was refused.
    pub(crate) document_fault: Option<&'static str>,
    /// The fault that refused the last restrictions part, if it was refused.
    pub(crate) restrictions_fault: Option<&'static str>,
    /// When a document part last arrived and passed its checks (Unix ms).
    pub(crate) document_ok_ms: u64,
    /// When a restrictions part last arrived and passed its checks (Unix ms).
    pub(crate) restrictions_ok_ms: u64,
    pub(crate) documents_installed: u64,
    pub(crate) restrictions_changes: u64,
    /// Documents refused for being too far ahead, each counted once.
    pub(crate) documents_refused_ahead: u64,
    last_refused_ahead: Option<[u8; 32]>,
}

/// What one answer changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Applied {
    pub(crate) document_installed: bool,
    pub(crate) set_changed: bool,
}

/// The two parts of a poll answer, each decoded on its own so that a bad
/// document cannot spoil the restrictions. Unknown members are ignored.
#[derive(Deserialize)]
struct Answer<'a> {
    #[serde(borrow, default)]
    document: Option<&'a RawValue>,
    #[serde(borrow, default)]
    restrictions: Option<&'a RawValue>,
}

/// One answer part, its members kept raw. Members it does not name are
/// skipped without being built, so an answer costs what its known members do.
#[derive(Deserialize)]
struct RawPart<'a> {
    #[serde(borrow, default)]
    status: Option<&'a RawValue>,
    #[serde(borrow, default)]
    version: Option<&'a RawValue>,
    #[serde(borrow, default)]
    digest: Option<&'a RawValue>,
    #[serde(borrow, default)]
    text: Option<&'a RawValue>,
    #[serde(borrow, default)]
    tag: Option<&'a RawValue>,
    #[serde(borrow, default)]
    body: Option<&'a RawValue>,
}

/// One answer part with its `status` checked: `None` when the part is
/// unavailable.
fn answer_part<'a>(
    raw: Option<&'a RawValue>,
    fault: &'static str,
) -> Result<Option<RawPart<'a>>, &'static str> {
    let part: RawPart = raw.and_then(parse).ok_or(fault)?;
    match part.status.and_then(parse::<String>).as_deref() {
        Some("unavailable") => Ok(None),
        Some("ok") => Ok(Some(part)),
        _ => Err(fault),
    }
}

impl Reader {
    /// The tag a poll carries: the last merged part's, unless none has been
    /// merged or the last merge skipped a record for being ahead.
    fn tag(&self) -> Option<[u8; 32]> {
        self.last_part
            .as_ref()
            .filter(|_| self.skipped_ahead.is_empty())
            .map(|(tag, _)| *tag)
    }

    /// The query parameters of the next poll (docs/client-store.md, "The
    /// poll"): `version` and `tag` choose what the answer carries, the rest
    /// report what this reader holds.
    pub(crate) fn query(&self, now_us: u64) -> Vec<(&'static str, String)> {
        let mut q = vec![(
            "version",
            self.document.as_ref().map_or(0, |d| d.version).to_string(),
        )];
        if let Some(d) = &self.document {
            q.push(("digest", digest_hex(&d.digest)));
        }
        if let Some(tag) = self.tag() {
            q.push(("tag", digest_hex(&tag)));
        }
        q.push((
            "records_version",
            self.set.records_version(now_us).to_string(),
        ));
        q.push(("set_hash", digest_hex(&self.set.hash())));
        q.push(("set_complete", self.set.complete.to_string()));
        q
    }

    /// Apply one poll answer, the response to `query` at the same clock.
    /// `Err` means the answer as a whole could not be read: the reader keeps
    /// everything it holds. A part that fails its checks is refused alone,
    /// and recorded in `status`.
    pub(crate) fn apply(&mut self, answer: &[u8], now_us: u64) -> Result<Applied, &'static str> {
        let answer: Answer = serde_json::from_slice(answer).map_err(|_| "poll.answer")?;
        let now_ms = now_us / 1000;
        let mut applied = Applied::default();
        // The tag this poll carried: the part answers relative to it.
        let sent_tag = self.tag();
        let before = self.set.clone();

        match self.apply_document(answer.document, now_ms) {
            Ok(installed) => {
                self.status.document_fault = None;
                applied.document_installed = installed;
            }
            Err(fault) => self.status.document_fault = Some(fault),
        }
        match self.apply_restrictions(answer.restrictions, sent_tag, now_us) {
            Ok(()) => self.status.restrictions_fault = None,
            Err(fault) => self.status.restrictions_fault = Some(fault),
        }
        applied.set_changed = self.set != before;
        if applied.set_changed {
            self.status.restrictions_changes += 1;
        }
        Ok(applied)
    }

    /// The document part. `Ok(true)` when it installed a document.
    fn apply_document(
        &mut self,
        raw: Option<&RawValue>,
        now_ms: u64,
    ) -> Result<bool, &'static str> {
        let Some(part) = answer_part(raw, "poll.document")? else {
            return Ok(false);
        };
        let version = part.version.and_then(int).ok_or("poll.document.version")?;
        let digest = match version {
            0 => None,
            _ => Some(digest_of(part.digest.as_ref()).ok_or("poll.document.digest")?),
        };
        let held = self.document.as_ref().map_or(0, |d| d.version);
        if version <= held {
            self.status.document_ok_ms = now_ms;
            self.status.document_ahead = false;
            self.status.store_behind = version < held;
            self.status.store_edited =
                version == held && held > 0 && digest != self.document.as_ref().map(|d| d.digest);
            return Ok(false);
        }
        let text: String = part.text.and_then(parse).ok_or("poll.document.text")?;
        if digest != Some(sha256(text.as_bytes())) {
            return Err("poll.document.digest");
        }
        let doc = check_document(&text)?;
        if doc.version != version {
            return Err("poll.document.version");
        }
        self.status.document_ok_ms = now_ms;
        self.status.store_behind = false;
        self.status.store_edited = false;
        if version > now_ms.saturating_add(AHEAD_LIMIT_MS) {
            self.status.document_ahead = true;
            if self.status.last_refused_ahead != Some(doc.digest) {
                self.status.last_refused_ahead = Some(doc.digest);
                self.status.documents_refused_ahead += 1;
            }
            return Ok(false);
        }
        self.status.document_ahead = false;
        self.install(doc);
        self.status.documents_installed += 1;
        Ok(true)
    }

    /// Install a checked document: it replaces the one held, and its revoked
    /// digests join the set.
    pub(crate) fn install(&mut self, doc: Document) {
        self.set.revoked_digests.extend(&doc.revoked_digests);
        self.document = Some(Arc::new(doc));
    }

    /// The restrictions part.
    fn apply_restrictions(
        &mut self,
        raw: Option<&RawValue>,
        sent_tag: Option<[u8; 32]>,
        now_us: u64,
    ) -> Result<(), &'static str> {
        let Some(part) = answer_part(raw, "poll.restrictions")? else {
            return Ok(());
        };
        let tag_text: String = part.tag.and_then(parse).ok_or("poll.restrictions.tag")?;
        let tag = parse_digest(&tag_text).ok_or("poll.restrictions.tag")?;
        let body: String = match part.body {
            Some(raw) => parse(raw).ok_or("poll.restrictions.body")?,
            // No body: the part is the one this reader merged last.
            None if sent_tag == Some(tag) => {
                self.status.restrictions_ok_ms = now_us / 1000;
                return Ok(());
            }
            None => return Err("poll.restrictions.body"),
        };
        let body = check_part(&tag_text, &body)?;
        self.skipped_ahead = self.set.merge(&body, now_us);
        self.last_part = Some((tag, body));
        self.status.restrictions_ok_ms = now_us / 1000;
        Ok(())
    }

    /// What the last merged part lacks of the set held. Judged again whenever
    /// either changes, since installing a document grows the held digests.
    pub(crate) fn behind(&self) -> Behind {
        self.last_part
            .as_ref()
            .map(|(_, part)| self.set.behind(part))
            .unwrap_or_default()
    }

    /// Whether the instance is loaded: it holds a document and a complete set
    /// of restrictions.
    pub(crate) fn loaded(&self) -> bool {
        self.set.complete && self.document.is_some()
    }

    /// The last merged part, when one has been.
    pub(crate) fn last_part(&self) -> Option<&PartBody> {
        self.last_part.as_ref().map(|(_, part)| part)
    }

    /// Whether the clock-skew alert stands: a document or an enabled record
    /// refused for being too far ahead of this reader's clock.
    pub(crate) fn clock_skew(&self) -> bool {
        self.status.document_ahead || !self.skipped_ahead.is_empty()
    }
}

// ── Running the reader ──────────────────────────────────────────────

/// How often a loaded instance polls.
const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// How often an instance polls until it is loaded.
const UNLOADED_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How long boot holds the listener shut waiting for the first answered poll,
/// so that a store outage cannot hold the port shut.
pub(crate) const FIRST_POLL_WAIT: Duration = Duration::from_secs(3);
/// The largest poll answer read: two 2 MiB texts with room for their JSON
/// escaping, and for members a later store version may add.
const MAX_ANSWER_BYTES: usize = 16 * 1024 * 1024;
/// How often the document and restrictions held are logged when nothing
/// changes, so the newest copy is always in the log store's retention.
const FULL_LOG_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// A store outage logs its first failed poll, then every this many.
const FAILURE_LOG_EVERY: u64 = 60;
/// Bytes of a document or set text per log line, before base64. Its 12 KiB of
/// base64 and the line's other fields stay under the 16 KiB a container
/// runtime keeps on one line.
const LOG_CHUNK_BYTES: usize = 9 * 1024;

/// A client store reader running inside the proxy: it polls the store,
/// installs what it receives into the client policy, and keeps a copy beside
/// the state file for a restart that keeps it.
pub(crate) struct ClientStore {
    poll_url: reqwest::Url,
    /// `Bearer <token>`, marked sensitive.
    authorization: Option<HeaderValue>,
    http: reqwest::Client,
    resume_path: PathBuf,
    /// The config file's policy: every policy the store yields is built on it.
    base: BuiltPolicy,
    /// This instance's endpoint names, the only pins a store client keeps.
    endpoints: Vec<String>,
    /// The config file's ordinary clients, exported one series each so the
    /// operator can see which clients are still to move into the store.
    file_clients: Vec<String>,
    /// What is held. Only `resume` and `poll` write it, each under `writer`.
    reader: Mutex<Reader>,
    /// Held by `resume`, and by each poll from its query to its last save. A
    /// poll builds the next reader on a copy and saves it before installing
    /// it, so a second writer meanwhile would build on what was about to be
    /// replaced, and one of the two merges would be lost.
    writer: tokio::sync::Mutex<()>,
    /// What the last policy built had to say (`BuiltPolicy::with_store`).
    report: Mutex<StoreReport>,
    /// Credentials refused by the store's gate, one counter per
    /// `StoreRefusal` (`refusal_counter`).
    refusals: [AtomicU64; StoreRefusal::ALL.len()],
    /// Turns true at the first answered poll.
    answered: tokio::sync::watch::Sender<bool>,
    /// Polls failed in a row.
    failures: AtomicU64,
    /// Resume copy saves failed in a row. While above 0 a copy is owed, and
    /// every poll tries to save one.
    resume_failures: AtomicU64,
}

/// What `ClientStore::metrics` reports.
pub(crate) struct ClientStoreMetrics {
    /// The document held: its version and digest.
    pub(crate) document: Option<(u64, String)>,
    /// The highest record version held, as a poll reports it.
    pub(crate) records_version: u64,
    pub(crate) complete: bool,
    pub(crate) loaded: bool,
    /// Whether the last part merged was full; `None` before the first.
    pub(crate) part_full: Option<bool>,
    /// The store's count of enabled records it left out for being ahead.
    pub(crate) enabled_ahead: u64,
    /// Every alert, raised or not.
    pub(crate) alerts: Vec<(&'static str, bool)>,
    pub(crate) unmatched_disabled: Vec<String>,
    pub(crate) quarantined: usize,
    pub(crate) revoked_outside_store: usize,
    /// When each part last arrived and passed its checks, in Unix seconds.
    pub(crate) document_ok_secs: u64,
    pub(crate) restrictions_ok_secs: u64,
    pub(crate) documents_installed: u64,
    pub(crate) restrictions_changes: u64,
    pub(crate) documents_refused_ahead: u64,
    pub(crate) file_clients: Vec<String>,
    pub(crate) refusals: Vec<(&'static str, u64)>,
}

/// What one answer changed, to log once the reader is released.
struct Changed {
    /// The document it installed.
    document: Option<Arc<Document>>,
    /// The set, when it changed.
    set: Option<RestrictionSet>,
}

/// The copy a restart resumes from: the document text and the set held.
#[derive(Serialize, Deserialize)]
struct Resume {
    document: Option<String>,
    complete: bool,
    records: Vec<ResumeRecord>,
    revoked_digests: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct ResumeRecord {
    name: String,
    state: String,
    version: u64,
}

/// The reader's clock: Unix microseconds.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// The alerts a reader raises, by the `alert` label each is exported under.
/// `resume_unsaved` is whether the last save of the resume copy failed.
fn alerts(
    reader: &Reader,
    report: &StoreReport,
    resume_unsaved: bool,
) -> Vec<(&'static str, bool)> {
    let part = reader.last_part();
    vec![
        ("store_behind", reader.status.store_behind),
        ("store_edited", reader.status.store_edited),
        ("clock_skew", reader.clock_skew()),
        ("restrictions_behind", !reader.behind().is_empty()),
        ("restrictions_partial", part.is_some_and(|p| !p.full)),
        ("enabled_ahead", part.is_some_and(|p| p.enabled_ahead > 0)),
        ("document_refused", reader.status.document_fault.is_some()),
        (
            "restrictions_refused",
            reader.status.restrictions_fault.is_some(),
        ),
        (
            "privileged_name",
            !report.privileged_clients.is_empty() || !report.privileged_disabled.is_empty(),
        ),
        ("quarantined", !report.quarantined.is_empty()),
        ("revoked_outside_store", report.revoked_outside_store > 0),
        ("resume_unsaved", resume_unsaved),
    ]
}

impl ClientStore {
    /// Build the reader from `[client_store]`. `base` is the config file's
    /// policy, `endpoints` this instance's endpoint names, and `resume_path`
    /// where the copy a restart resumes from is kept.
    pub(crate) fn new(
        config: &ClientStoreConfig,
        base: &BuiltPolicy,
        endpoints: Vec<String>,
        resume_path: PathBuf,
    ) -> Result<Self, String> {
        let poll_url = reqwest::Url::parse(&config.poll_url)
            .map_err(|_| "client_store.poll_url: not a URL".to_string())?;
        if poll_url.scheme() == "http" {
            warn!(
                "client_store.poll_url is plain http: safe only over a channel that never leaves this \
                 platform, since anyone on the path could rewrite the clients and restrictions it carries"
            );
        }
        let authorization = match &config.token {
            Some(token) => {
                let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| "client_store.token: not a valid header value".to_string())?;
                value.set_sensitive(true);
                Some(value)
            }
            None => None,
        };
        let http = Client::builder()
            .user_agent(concat!("anthropic-lb/", env!("CARGO_PKG_VERSION")))
            // A redirect would carry the token to wherever it points.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("client_store: HTTP client: {e}"))?;
        let file_clients = base
            .clients
            .iter()
            .map(|c| c.name.clone())
            .filter(|name| !base.is_privileged(name))
            .collect();
        Ok(Self {
            poll_url,
            authorization,
            http,
            resume_path,
            base: base.clone(),
            endpoints,
            file_clients,
            reader: Mutex::new(Reader::default()),
            writer: tokio::sync::Mutex::new(()),
            report: Mutex::new(StoreReport::default()),
            refusals: Default::default(),
            answered: tokio::sync::watch::channel(false).0,
            failures: AtomicU64::new(0),
            resume_failures: AtomicU64::new(0),
        })
    }

    fn lock_reader(&self) -> std::sync::MutexGuard<'_, Reader> {
        lock_recovering(&self.reader, "client_store_reader")
    }

    fn lock_report(&self) -> std::sync::MutexGuard<'_, StoreReport> {
        lock_recovering(&self.report, "client_store_report")
    }

    /// Count one credential the store's gate refused.
    pub(crate) fn count_refusal(&self, refusal: StoreRefusal) {
        self.refusal_counter(refusal)
            .fetch_add(1, Ordering::Relaxed);
    }

    /// The counter of `refusal`. The match is exhaustive and its indices are
    /// constants, so a new refusal does not compile until it has a counter
    /// of its own, in the array `StoreRefusal::ALL` sizes.
    fn refusal_counter(&self, refusal: StoreRefusal) -> &AtomicU64 {
        match refusal {
            StoreRefusal::Disabled => &self.refusals[0],
            StoreRefusal::Quarantined => &self.refusals[1],
            StoreRefusal::Unloaded => &self.refusals[2],
        }
    }

    /// Resume from the copy beside the state file, if a restart left one. The
    /// document passes the same checks as one from the store, and the
    /// instance counts as loaded only when the saved set is complete. A copy
    /// that fails its checks is ignored, and the instance waits for the store.
    pub(crate) async fn resume(&self, state: &AppState) {
        let _writer = self.writer.lock().await;
        let bytes = match tokio::fs::read(&self.resume_path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(path = %self.resume_path.display(), error = %e, "client store: cannot read the resume copy; waiting for the store");
                return;
            }
        };
        let Some(set) = serde_json::from_slice::<Resume>(&bytes)
            .ok()
            .and_then(|r| resumed_set(&r).map(|set| (set, r.document)))
        else {
            warn!(path = %self.resume_path.display(), "client store: resume copy is malformed; ignored, waiting for the store");
            return;
        };
        let (set, text) = set;
        let document = text.and_then(|text| match check_document(&text) {
            Ok(doc) if doc.version <= (now_us() / 1000).saturating_add(AHEAD_LIMIT_MS) => Some(doc),
            Ok(doc) => {
                warn!(
                    version = doc.version,
                    "client store: resumed document is too far ahead of the clock; ignored"
                );
                None
            }
            Err(rule) => {
                warn!(
                    rule,
                    "client store: resumed document fails its checks; ignored"
                );
                None
            }
        });
        let (document, set) = {
            let mut reader = self.lock_reader();
            reader.set = set;
            if let Some(doc) = document {
                reader.install(doc);
            }
            self.publish(state, &reader);
            (reader.document.clone(), reader.set.clone())
        };
        info!(
            document_version = document.as_ref().map_or(0, |d| d.version),
            complete = set.complete,
            "client store: resumed from the copy beside the state file"
        );
        if let Some(doc) = &document {
            log_document(doc, "resume");
        }
        log_restrictions(&set, "resume");
    }

    /// Start polling, for the life of the process. The receiver turns true at
    /// the first answered poll.
    fn spawn(state: &Arc<AppState>) -> Option<tokio::sync::watch::Receiver<bool>> {
        let store = state.client_store.clone()?;
        let answered = store.answered.subscribe();
        let state = state.clone();
        tokio::spawn(async move { store.run(&state).await });
        Some(answered)
    }

    async fn run(&self, state: &AppState) {
        let mut last_full_log = Instant::now();
        loop {
            if self.poll(state).await {
                self.answered.send_replace(true);
            }
            self.log_daily(&mut last_full_log, Instant::now());
            let loaded = state
                .current_client_policy()
                .store
                .as_ref()
                .is_none_or(|g| g.loaded);
            tokio::time::sleep(if loaded {
                POLL_INTERVAL
            } else {
                UNLOADED_POLL_INTERVAL
            })
            .await;
        }
    }

    /// Log the document and the restrictions held once `FULL_LOG_INTERVAL`
    /// has passed between `last` and `now`, even when nothing changed, so the
    /// newest copy of each stays inside the log store's retention.
    fn log_daily(&self, last: &mut Instant, now: Instant) {
        if now.saturating_duration_since(*last) < FULL_LOG_INTERVAL {
            return;
        }
        *last = now;
        let (document, set) = {
            let reader = self.lock_reader();
            (reader.document.clone(), reader.set.clone())
        };
        if let Some(doc) = &document {
            log_document(doc, "daily");
        }
        log_restrictions(&set, "daily");
    }

    /// One poll: ask, apply the answer, and act on what changed. Returns
    /// whether the store answered. Polls run one at a time, under `writer`.
    pub(crate) async fn poll(&self, state: &AppState) -> bool {
        let _writer = self.writer.lock().await;
        let query = self.lock_reader().query(now_us());
        let changed = match self.fetch(&query).await {
            Ok(answer) => self.apply(state, &answer, now_us()).await,
            Err(e) => Err(e),
        };
        let changed = match changed {
            Ok(changed) => changed,
            Err(e) => {
                // Once at the start of an outage, then once every
                // FAILURE_LOG_EVERY polls, not once a poll.
                let failures = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
                if failures == 1 || failures.is_multiple_of(FAILURE_LOG_EVERY) {
                    warn!(error = %e, failures, "client store: poll failed; keeping what is held");
                }
                self.save_if_owed().await;
                return false;
            }
        };
        let failures = self.failures.swap(0, Ordering::Relaxed);
        if failures > 0 {
            info!(failures, "client store: polls answered again");
        }
        if let Some(doc) = &changed.document {
            log_document(doc, "install");
        }
        if let Some(set) = &changed.set {
            log_restrictions(set, "change");
        }
        true
    }

    /// Apply one answer at the reader's clock `now_us`. What it changes is
    /// built on a copy of the reader, saved to the resume copy, and only then
    /// installed. So no restriction is enforced, or reported held, before it
    /// is saved: a crash at any point leaves a resume copy that holds every
    /// restriction this instance enforced, or no copy at all. The one
    /// exception, a copy that can be neither written nor removed, is logged
    /// as an error. A save that
    /// hangs holds this answer back, and the polls after it, until the disk
    /// answers; what is held stays enforced meanwhile, and each part's last
    /// good arrival stops advancing. Called by `poll`, under `writer`. `Err`
    /// when the answer could not be read at all.
    async fn apply(&self, state: &AppState, answer: &[u8], now_us: u64) -> Result<Changed, String> {
        let (mut next, before) = {
            let reader = self.lock_reader();
            (reader.clone(), self.current_alerts(&reader))
        };
        let applied = next
            .apply(answer, now_us)
            .map_err(|fault| format!("unreadable answer ({fault})"))?;
        let changed = applied.document_installed || applied.set_changed;
        if changed {
            self.write_resume(&resume_copy(&next)).await;
        } else {
            self.save_if_owed().await;
        }
        let mut reader = self.lock_reader();
        *reader = next;
        if changed {
            self.publish(state, &reader);
        }
        log_alert_changes(&reader, &before, &self.current_alerts(&reader));
        Ok(Changed {
            document: reader
                .document
                .clone()
                .filter(|_| applied.document_installed),
            set: applied.set_changed.then(|| reader.set.clone()),
        })
    }

    /// The alerts this instance raises from what `reader` holds.
    fn current_alerts(&self, reader: &Reader) -> Vec<(&'static str, bool)> {
        alerts(reader, &self.lock_report(), self.resume_unsaved())
    }

    /// Whether the last save of the resume copy failed.
    fn resume_unsaved(&self) -> bool {
        self.resume_failures.load(Ordering::Relaxed) > 0
    }

    /// GET the poll URL with `query`, and read at most `MAX_ANSWER_BYTES`.
    async fn fetch(&self, query: &[(&'static str, String)]) -> Result<Vec<u8>, String> {
        let mut req = self.http.get(self.poll_url.clone()).query(query);
        if let Some(auth) = &self.authorization {
            req = req.header(reqwest::header::AUTHORIZATION, auth.clone());
        }
        let mut resp = req.send().await.map_err(|e| e.without_url().to_string())?;
        if resp.status() != StatusCode::OK {
            return Err(format!("status {}", resp.status().as_u16()));
        }
        if resp
            .content_length()
            .is_some_and(|n| n > MAX_ANSWER_BYTES as u64)
        {
            return Err("answer larger than 16 MiB".to_string());
        }
        let mut body = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| e.without_url().to_string())?
        {
            if body.len() + chunk.len() > MAX_ANSWER_BYTES {
                return Err("answer larger than 16 MiB".to_string());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// Build the policy from what `reader` holds, install it, and tell the
    /// operator what changed in the report.
    fn publish(&self, state: &AppState, reader: &Reader) {
        let (policy, report) = self.base.with_store(reader, &self.endpoints);
        if let Err(e) = state.swap_client_policy(policy) {
            // Unreachable: boot refuses `[client_store]` outside `[[clients]]`.
            error!(error = e, "client store: policy not installed");
        }
        let mut last = self.lock_report();
        if *last != report {
            log_report(&report);
            *last = report;
        }
    }

    /// Save what is held again after a save that failed: it is owed until one
    /// lands, whether or not the store answers meanwhile.
    async fn save_if_owed(&self) {
        if self.resume_unsaved() {
            let copy = resume_copy(&self.lock_reader());
            self.write_resume(&copy).await;
        }
    }

    /// Save the resume copy (`save_copy`), and count and log a save that
    /// failed. What is about to be enforced is not in the last copy, so a
    /// failed save removes it: a restart then waits for the store, and every
    /// poll saves again until one lands.
    async fn write_resume(&self, resume: &Resume) {
        let json = serde_json::to_vec(resume).map_err(std::io::Error::other);
        let path = self.resume_path.clone();
        let written = tokio::task::spawn_blocking(move || save_copy(&path, json))
            .await
            .unwrap_or_else(|e| Err((std::io::Error::other(e), None)));
        let (e, removal) = match written {
            Ok(()) => {
                let failures = self.resume_failures.swap(0, Ordering::Relaxed);
                if failures > 0 {
                    info!(failures, "client store: resume copy written again");
                }
                return;
            }
            Err(failed) => failed,
        };
        let failures = self.resume_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures != 1 && !failures.is_multiple_of(FAILURE_LOG_EVERY) {
            return;
        }
        let path = self.resume_path.display();
        match removal {
            None => warn!(
                path = %path,
                error = %e,
                failures,
                "client store: cannot write the resume copy; the last copy is removed, so a restart waits for the store, and every poll tries again"
            ),
            Some(rm) => error!(
                path = %path,
                error = %e,
                removal_error = %rm,
                failures,
                "client store: cannot write the resume copy or durably remove the last one; a restart now could resume restrictions older than those enforced; every poll tries again"
            ),
        }
    }

    /// What `/metrics` exports for the client store.
    pub(crate) fn metrics(&self) -> ClientStoreMetrics {
        let reader = self.lock_reader();
        let report = self.lock_report();
        let part = reader.last_part();
        ClientStoreMetrics {
            document: reader
                .document
                .as_ref()
                .map(|d| (d.version, digest_hex(&d.digest))),
            records_version: reader.set.records_version(now_us()),
            complete: reader.set.complete,
            loaded: reader.loaded(),
            part_full: part.map(|p| p.full),
            enabled_ahead: part.map_or(0, |p| p.enabled_ahead),
            alerts: alerts(&reader, &report, self.resume_unsaved()),
            unmatched_disabled: report.unmatched_disabled.clone(),
            quarantined: report.quarantined.len(),
            revoked_outside_store: report.revoked_outside_store,
            document_ok_secs: reader.status.document_ok_ms / 1000,
            restrictions_ok_secs: reader.status.restrictions_ok_ms / 1000,
            documents_installed: reader.status.documents_installed,
            restrictions_changes: reader.status.restrictions_changes,
            documents_refused_ahead: reader.status.documents_refused_ahead,
            file_clients: self.file_clients.clone(),
            refusals: StoreRefusal::ALL
                .iter()
                .map(|r| (r.as_str(), self.refusal_counter(*r).load(Ordering::Relaxed)))
                .collect(),
        }
    }
}

/// Boot the reader, then open the listener on `addr`: resume what a restart
/// kept, start polling, and bind only after the first answered poll, or after
/// `FIRST_POLL_WAIT` without one, so a request that wakes the instance meets
/// a loaded one. Without a client store it binds at once.
pub(crate) async fn bind_after_first_poll(
    state: &Arc<AppState>,
    addr: SocketAddr,
) -> std::io::Result<tokio::net::TcpListener> {
    if let Some(store) = &state.client_store {
        store.resume(state).await;
    }
    await_first_poll(ClientStore::spawn(state)).await;
    tokio::net::TcpListener::bind(addr).await
}

/// Wait for the first answered poll, at most `FIRST_POLL_WAIT`.
async fn await_first_poll(answered: Option<tokio::sync::watch::Receiver<bool>>) {
    let Some(mut answered) = answered else {
        return;
    };
    if tokio::time::timeout(FIRST_POLL_WAIT, answered.wait_for(|a| *a))
        .await
        .is_err()
    {
        warn!(
            wait_secs = FIRST_POLL_WAIT.as_secs(),
            "client store: no answered poll yet; opening the listener anyway"
        );
    }
}

/// `GET /_ready`: 200 once the instance is loaded, 503 until then. Carries
/// one bit, so it needs no credential and skips `allowed_ips`: a readiness
/// probe comes from wherever the orchestrator runs it, and an allow-list that
/// left it out would hold every instance unready.
pub(crate) async fn ready_handler(State(state): State<Arc<AppState>>) -> Response {
    let loaded = state
        .current_client_policy()
        .store
        .as_ref()
        .is_none_or(|g| g.loaded);
    if loaded {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "client table not loaded\n").into_response()
    }
}

/// Why a save of the resume copy failed: the write's error, and the error
/// that then kept the last copy from being removed, if one did.
type SaveFailed = (std::io::Error, Option<std::io::Error>);

/// Write `json` as the resume copy at `path`: to a sibling, synced, renamed
/// over the last copy, and the directory synced, so a crash leaves the old
/// copy or the new one, never a torn one. When any step fails, the last copy
/// is removed and the directory synced again, since a removal survives a
/// power cut only then: a copy that came back could lift a restriction
/// enforced after it was written.
fn save_copy(path: &Path, json: std::io::Result<Vec<u8>>) -> Result<(), SaveFailed> {
    let write = || -> std::io::Result<()> {
        use std::io::Write;
        let json = json?;
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&json)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        sync_parent(path)
    };
    write().map_err(|e| {
        let removed = match std::fs::remove_file(path) {
            Err(rm) if rm.kind() != std::io::ErrorKind::NotFound => Err(rm),
            _ => sync_parent(path),
        };
        (e, removed.err())
    })
}

/// Sync the directory that holds `path`, so a rename or a removal there
/// survives a power cut.
fn sync_parent(path: &Path) -> std::io::Result<()> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(dir)?.sync_all()
}

/// The set a resume copy holds, if every value in it passes the checks a
/// part's would.
fn resumed_set(resume: &Resume) -> Option<RestrictionSet> {
    let mut set = RestrictionSet {
        complete: resume.complete,
        ..Default::default()
    };
    for r in &resume.records {
        let record = Record {
            state: RecordState::parse(&r.state)?,
            version: Some(r.version).filter(|v| (1..=MAX_INT).contains(v))?,
        };
        if !is_name(&r.name) || set.records.insert(r.name.clone(), record).is_some() {
            return None;
        }
    }
    for d in &resume.revoked_digests {
        set.revoked_digests.insert(parse_digest(d)?);
    }
    Some(set)
}

/// The copy of what `reader` holds that a restart resumes from.
fn resume_copy(reader: &Reader) -> Resume {
    Resume {
        document: reader.document.as_ref().map(|d| d.text.clone()),
        complete: reader.set.complete,
        records: reader
            .set
            .records
            .iter()
            .map(|(name, r)| ResumeRecord {
                name: name.clone(),
                state: r.state.as_str().to_string(),
                version: r.version,
            })
            .collect(),
        revoked_digests: reader.set.revoked_digests.iter().map(digest_hex).collect(),
    }
}

/// `bytes` as base64 slices for log lines: `(chunk, chunks, data)`, numbered
/// from 1. An empty text is one empty chunk.
fn log_chunks(bytes: &[u8]) -> impl Iterator<Item = (usize, usize, String)> + '_ {
    use base64::Engine;
    let chunks = bytes.len().div_ceil(LOG_CHUNK_BYTES).max(1);
    (0..chunks).map(move |i| {
        let slice = &bytes
            [(i * LOG_CHUNK_BYTES).min(bytes.len())..((i + 1) * LOG_CHUNK_BYTES).min(bytes.len())];
        (
            i + 1,
            chunks,
            base64::engine::general_purpose::STANDARD.encode(slice),
        )
    })
}

/// Log the exact bytes of an installed document, as base64 over as many
/// lines as it takes, each carrying the version and the digest that checks
/// the whole. These lines are a source to restore the store from.
pub(crate) fn log_document(doc: &Document, reason: &'static str) {
    let digest = digest_hex(&doc.digest);
    for (chunk, chunks, data) in log_chunks(doc.text.as_bytes()) {
        info!(
            reason,
            version = doc.version,
            digest = %digest,
            chunk,
            chunks,
            data = %data,
            "client store document"
        );
    }
}

/// Log the full set held, as its set text in base64 over as many lines as it
/// takes, each carrying the set hash that checks the whole and the
/// completeness mark. These lines are the source a store's restrictions are
/// reseeded from.
pub(crate) fn log_restrictions(set: &RestrictionSet, reason: &'static str) {
    let text = set.text();
    let set_hash = digest_hex(&sha256(text.as_bytes()));
    for (chunk, chunks, data) in log_chunks(text.as_bytes()) {
        info!(
            reason,
            set_hash = %set_hash,
            complete = set.complete,
            records = set.records.len(),
            revoked_digests = set.revoked_digests.len(),
            chunk,
            chunks,
            data = %data,
            "client store restrictions"
        );
    }
}

/// Log every alert that changed with the last answer.
fn log_alert_changes(
    reader: &Reader,
    before: &[(&'static str, bool)],
    after: &[(&'static str, bool)],
) {
    for ((alert, was), (_, is)) in before.iter().zip(after) {
        match (was, is) {
            (false, true) => {
                let detail = match *alert {
                    "restrictions_behind" => {
                        let behind = reader.behind();
                        format!(
                            "disabled names {:?}, {} revoked digest(s)",
                            behind.disabled,
                            behind.revoked_digests.len()
                        )
                    }
                    "clock_skew" => format!("records skipped: {:?}", reader.skipped_ahead),
                    "enabled_ahead" => format!(
                        "{} enabled record(s) left out by the store",
                        reader.last_part().map_or(0, |p| p.enabled_ahead)
                    ),
                    "document_refused" => {
                        reader.status.document_fault.unwrap_or_default().to_string()
                    }
                    "restrictions_refused" => reader
                        .status
                        .restrictions_fault
                        .unwrap_or_default()
                        .to_string(),
                    _ => String::new(),
                };
                warn!(alert, detail = %detail, "client store alert raised");
            }
            (true, false) => info!(alert, "client store alert cleared"),
            _ => {}
        }
    }
}

/// Log what a newly built policy had to say.
fn log_report(report: &StoreReport) {
    for (name, rule) in &report.quarantined {
        warn!(client = %name, rule, "client store: client quarantined on this instance; every key of the name is refused");
    }
    for name in &report.privileged_clients {
        warn!(client = %name, "client store: a store client names an operator or admin reader here; ignored");
    }
    for name in &report.privileged_disabled {
        warn!(client = %name, "client store: a disabled record names an operator or admin reader here; it refuses nothing");
    }
    for (name, pin) in &report.dropped_pins {
        warn!(client = %name, endpoint = %pin, "client store: pin to an endpoint this instance lacks; dropped");
    }
    if report.revoked_outside_store > 0 {
        warn!(
            count = report.revoked_outside_store,
            "client store: revoked digests match config-file keys; a store revokes only its own keys, so they are ignored"
        );
    }
    if !report.unmatched_disabled.is_empty() {
        info!(names = ?report.unmatched_disabled, "client store: disabled names that match no client");
    }
}

#[cfg(test)]
mod tests;

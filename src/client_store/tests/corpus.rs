use super::*;

// ── The shared corpus ────────────────────────────────────────────────
//
// tests/client-store/ is the corpus a store and its readers both test
// against (docs/client-store.md, "The corpus"). Every case is checked for its
// outcome and for the rule it names, so that no case passes for the wrong
// reason.

const DOCUMENTS: &str = include_str!("../../../tests/client-store/documents.json");
const PARTS: &str = include_str!("../../../tests/client-store/parts.json");
const MERGES: &str = include_str!("../../../tests/client-store/merges.json");
const HASHES: &str = include_str!("../../../tests/client-store/hashes.json");

/// The cases of one corpus file, under `key`.
fn cases(file: &str, key: &str) -> Vec<Value> {
    let corpus: Value = serde_json::from_str(file).expect("corpus file parses");
    let cases = corpus[key].as_array().expect("corpus cases").clone();
    assert!(!cases.is_empty(), "corpus array {key} is empty");
    cases
}

fn id(case: &Value) -> &str {
    case["id"].as_str().expect("case id")
}

fn digests(v: &Value) -> BTreeSet<[u8; 32]> {
    v.as_array()
        .expect("digest list")
        .iter()
        .map(|d| parse_digest(d.as_str().unwrap()).expect("corpus digest"))
        .collect()
}

fn records(v: &Value) -> BTreeMap<String, Record> {
    v.as_array()
        .expect("record list")
        .iter()
        .map(|r| {
            (
                r["name"].as_str().unwrap().to_string(),
                Record {
                    state: RecordState::parse(r["state"].as_str().unwrap()).unwrap(),
                    version: r["version"].as_u64().unwrap(),
                },
            )
        })
        .collect()
}

fn hex_list(ds: &[[u8; 32]]) -> Vec<String> {
    ds.iter().map(digest_hex).collect()
}

fn string_list(v: &Value) -> Vec<String> {
    v.as_array()
        .expect("string list")
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

/// What a reader makes of a document, in the corpus's own terms.
fn document_outcome(doc: &Document) -> Value {
    let quarantined: Map<String, Value> = doc
        .clients
        .iter()
        .filter_map(|c| {
            c.policy
                .as_ref()
                .err()
                .map(|rule| (c.name.clone(), Value::from(*rule)))
        })
        .collect();
    if quarantined.is_empty() {
        Value::from("install")
    } else {
        serde_json::json!({ "quarantine": quarantined })
    }
}

#[test]
fn corpus_documents() {
    let cases = cases(DOCUMENTS, "cases");
    let mut ids = HashSet::new();
    for case in &cases {
        assert!(ids.insert(id(case)), "duplicate case id {}", id(case));
        // The corpus holds each document as a JSON value; a store sends its
        // text, which serialising the value reproduces member for member.
        let text = serde_json::to_string(&case["document"]).unwrap();
        let outcome = match check_document(&text) {
            Ok(doc) => {
                assert_eq!(doc.digest, sha256(text.as_bytes()), "{}", id(case));
                document_outcome(&doc)
            }
            Err(rule) => serde_json::json!({ "refuse": rule }),
        };
        assert_eq!(outcome, case["expect"], "documents.json case {}", id(case));
    }
}

/// An installable document's members come through as the policy each client
/// is given: the corpus's typical case, field by field.
#[test]
fn corpus_typical_document_policies() {
    let case = cases(DOCUMENTS, "cases")
        .into_iter()
        .find(|c| id(c) == "typical")
        .unwrap();
    let doc = check_document(&serde_json::to_string(&case["document"]).unwrap()).unwrap();
    assert_eq!(doc.version, case["document"]["version"].as_u64().unwrap());
    for (client, expected) in doc
        .clients
        .iter()
        .zip(case["document"]["clients"].as_array().unwrap())
    {
        assert_eq!(client.name, expected["name"].as_str().unwrap());
        let policy = client.policy.as_ref().unwrap();
        assert_eq!(policy.daily_budget, expected["daily_budget"].as_u64());
        assert_eq!(
            policy.utilization_limit,
            expected["utilization_limit"].as_f64()
        );
        assert_eq!(policy.models, string_list(&expected["models"]));
        assert_eq!(
            policy.preferred_endpoints,
            string_list(&expected["preferred_endpoints"])
        );
        let expected_digests: Vec<String> = expected["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k["digest"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(hex_list(&client.digests), expected_digests);
    }
    assert_eq!(
        hex_list(&doc.revoked_digests),
        string_list(&case["document"]["revoked_digests"])
    );
}

#[test]
fn corpus_parts() {
    let cases = cases(PARTS, "cases");
    let mut ids = HashSet::new();
    for case in &cases {
        assert!(ids.insert(id(case)), "duplicate case id {}", id(case));
        let tag = case["tag"].as_str().unwrap();
        let body = case["body"].as_str().unwrap();
        let outcome = match check_part(tag, body) {
            Ok(part) => {
                // An accepted part carries every record and digest of its body.
                let parsed: Value = serde_json::from_str(body).unwrap();
                assert_eq!(part.full, parsed["full"].as_bool().unwrap());
                assert_eq!(
                    part.enabled_ahead,
                    parsed["enabled_ahead"].as_u64().unwrap()
                );
                assert_eq!(part.records, records(&parsed["records"]), "{}", id(case));
                assert_eq!(part.revoked_digests, digests(&parsed["revoked_digests"]));
                Value::from("accept")
            }
            Err(rule) => serde_json::json!({ "refuse": rule }),
        };
        assert_eq!(outcome, case["expect"], "parts.json case {}", id(case));
    }
}

#[test]
fn corpus_merges() {
    let cases = cases(MERGES, "cases");
    let mut ids = HashSet::new();
    for case in &cases {
        let id = id(case);
        assert!(ids.insert(id), "duplicate case id {id}");
        let now_us = case["now_us"].as_u64().unwrap();
        let mut set = RestrictionSet {
            complete: case["held"]["complete"].as_bool().unwrap(),
            records: records(&case["held"]["records"]),
            revoked_digests: digests(&case["held"]["revoked_digests"]),
        };
        let part = PartBody {
            full: case["part"]["full"].as_bool().unwrap(),
            enabled_ahead: 0,
            records: records(&case["part"]["records"]),
            revoked_digests: digests(&case["part"]["revoked_digests"]),
        };
        let skipped_ahead = set.merge(&part, now_us);
        let expect = &case["expect"];
        let held = &expect["held"];
        assert_eq!(
            set.complete,
            held["complete"].as_bool().unwrap(),
            "{id}: complete"
        );
        assert_eq!(set.records, records(&held["records"]), "{id}: records");
        assert_eq!(
            set.revoked_digests,
            digests(&held["revoked_digests"]),
            "{id}: digests"
        );
        assert_eq!(
            digest_hex(&set.hash()),
            held["set_hash"].as_str().unwrap(),
            "{id}: set hash"
        );
        assert_eq!(
            set.records_version(now_us),
            expect["records_version"].as_u64().unwrap(),
            "{id}: records_version"
        );
        assert_eq!(
            skipped_ahead,
            string_list(&expect["skipped_ahead"]),
            "{id}: skipped_ahead"
        );
        let behind = set.behind(&part);
        assert_eq!(
            behind.disabled,
            string_list(&expect["behind"]["disabled"]),
            "{id}: behind.disabled"
        );
        assert_eq!(
            hex_list(&behind.revoked_digests),
            string_list(&expect["behind"]["revoked_digests"]),
            "{id}: behind.revoked_digests"
        );
    }
}

#[test]
fn corpus_set_hashes() {
    for case in cases(HASHES, "set_hashes") {
        let set = RestrictionSet {
            complete: false,
            records: records(&case["records"]),
            revoked_digests: digests(&case["revoked_digests"]),
        };
        assert_eq!(set.text(), case["text"].as_str().unwrap(), "{}", id(&case));
        assert_eq!(
            digest_hex(&set.hash()),
            case["hash"].as_str().unwrap(),
            "{}",
            id(&case)
        );
    }
}

/// A store hands a key out as the unpadded base64url of 32 random bytes and
/// keeps the SHA-256 of that text: the digest `key_digest` computes for the
/// same key presented to a reader.
#[test]
fn corpus_key_digests() {
    use base64::Engine;
    for case in cases(HASHES, "key_digests") {
        let hex = case["bytes_hex"].as_str().unwrap();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes);
        assert_eq!(key, case["base64url"].as_str().unwrap(), "{}", id(&case));
        assert_eq!(key.len(), 43, "{}", id(&case));
        assert_eq!(
            digest_hex(&key_digest(&key)),
            case["digest"].as_str().unwrap(),
            "{}",
            id(&case)
        );
    }
}

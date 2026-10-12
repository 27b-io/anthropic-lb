use super::*;

// ── The reader: what each poll answer does to what it holds ─────────

/// Apply `answer` at `now_us`, which must be readable.
fn apply(reader: &mut Reader, answer: Vec<u8>, now_us: u64) -> Applied {
    reader.apply(&answer, now_us).expect("readable answer")
}

/// Merge one restrictions body, with no document part.
fn merge(reader: &mut Reader, body: &str, now_us: u64) -> Applied {
    apply(
        reader,
        answer(unavailable(), restrictions_part(body)),
        now_us,
    )
}

fn state_of(reader: &Reader, name: &str) -> Option<(RecordState, u64)> {
    reader.set.records.get(name).map(|r| (r.state, r.version))
}

fn alert(reader: &Reader, name: &str) -> bool {
    alerts(reader, &StoreReport::default())
        .into_iter()
        .find(|(alert, _)| *alert == name)
        .unwrap_or_else(|| panic!("no alert {name}"))
        .1
}

const V1: u64 = NOW_US - 10 * MINUTE_US;
const V2: u64 = V1 + 1;
const V3: u64 = V1 + 2;

// ── Merging restriction records ─────────────────────────────────────

#[test]
fn a_part_missing_a_disable_lifts_nothing_and_raises_restrictions_behind_until_a_part_has_it() {
    let mut reader = Reader::default();
    let with_a = body(true, &[("a", Disabled, V1)], &[digest(1)]);
    merge(&mut reader, &with_a, NOW_US);
    assert!(reader.behind().is_empty());
    assert!(!alert(&reader, "restrictions_behind"));

    // The store loses both: a full part without them lifts nothing.
    let applied = merge(&mut reader, &body(true, &[], &[]), NOW_US);
    assert!(
        !applied.set_changed,
        "a part missing a restriction changes nothing held"
    );
    assert_eq!(state_of(&reader, "a"), Some((Disabled, V1)));
    assert!(reader.set.revoked_digests.contains(&digest(1)));
    let behind = reader.behind();
    assert_eq!(behind.disabled, vec!["a".to_string()]);
    assert_eq!(behind.revoked_digests, vec![digest(1)]);
    assert!(alert(&reader, "restrictions_behind"));

    // A later part has them again: the alert clears.
    merge(&mut reader, &with_a, NOW_US);
    assert!(reader.behind().is_empty());
    assert!(!alert(&reader, "restrictions_behind"));
}

#[test]
fn a_newer_enable_lifts_a_disable_and_an_older_one_does_not() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V2)], &[]),
        NOW_US,
    );

    merge(&mut reader, &body(true, &[("a", Enabled, V1)], &[]), NOW_US);
    assert_eq!(
        state_of(&reader, "a"),
        Some((Disabled, V2)),
        "an older enable lifts nothing"
    );
    assert_eq!(reader.behind().disabled, vec!["a".to_string()]);

    let applied = merge(&mut reader, &body(true, &[("a", Enabled, V3)], &[]), NOW_US);
    assert!(applied.set_changed);
    assert_eq!(
        state_of(&reader, "a"),
        Some((Enabled, V3)),
        "a newer enable lifts it"
    );
    assert!(reader.behind().is_empty());
}

#[test]
fn an_enabled_record_is_forgotten_only_when_a_full_part_has_no_record_for_its_name() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Enabled, V1), ("b", Enabled, V1)], &[]),
        NOW_US,
    );
    // A partial part without `a` forgets nothing.
    merge(
        &mut reader,
        &body(false, &[("b", Enabled, V1)], &[]),
        NOW_US,
    );
    assert_eq!(state_of(&reader, "a"), Some((Enabled, V1)));
    // A full part with a record for `a` keeps it, whatever the record says.
    merge(&mut reader, &body(true, &[("a", Enabled, V1)], &[]), NOW_US);
    assert_eq!(state_of(&reader, "a"), Some((Enabled, V1)));
    assert_eq!(
        state_of(&reader, "b"),
        None,
        "b has no record in a full part: forgotten"
    );
    // A full part without `a` forgets it.
    merge(&mut reader, &body(true, &[], &[]), NOW_US);
    assert_eq!(state_of(&reader, "a"), None);
}

#[test]
fn a_partial_part_adds_disables_and_digests_but_no_enable_and_never_completes_the_set() {
    let mut reader = Reader::default();
    let partial = body(
        false,
        &[("a", Disabled, V1), ("b", Enabled, V1)],
        &[digest(1)],
    );
    let applied = merge(&mut reader, &partial, NOW_US);
    assert!(applied.set_changed);
    assert_eq!(state_of(&reader, "a"), Some((Disabled, V1)));
    assert_eq!(
        state_of(&reader, "b"),
        None,
        "a partial part never adds an enable"
    );
    assert!(reader.set.revoked_digests.contains(&digest(1)));
    assert!(!reader.set.complete, "a partial part never completes a set");
    assert!(alert(&reader, "restrictions_partial"));

    // Nor does a partial enable lift a held disable, however new.
    merge(
        &mut reader,
        &body(false, &[("a", Enabled, V3)], &[]),
        NOW_US,
    );
    assert_eq!(state_of(&reader, "a"), Some((Disabled, V1)));
    assert!(!reader.set.complete);
}

#[test]
fn the_set_is_complete_only_after_a_full_part_and_stays_complete() {
    let mut reader = Reader::default();
    merge(&mut reader, &body(false, &[], &[]), NOW_US);
    assert!(!reader.set.complete);
    let applied = merge(&mut reader, &body(true, &[], &[]), NOW_US);
    assert!(reader.set.complete);
    assert!(applied.set_changed, "the mark is part of the set");
    assert!(!alert(&reader, "restrictions_partial"));
    merge(&mut reader, &body(false, &[], &[]), NOW_US);
    assert!(
        reader.set.complete,
        "a partial part after a full one keeps the mark"
    );
    assert!(alert(&reader, "restrictions_partial"));
}

#[test]
fn a_disabled_record_wins_a_tie() {
    let mut reader = Reader::default();
    merge(&mut reader, &body(true, &[("a", Enabled, V1)], &[]), NOW_US);
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[]),
        NOW_US,
    );
    assert_eq!(state_of(&reader, "a"), Some((Disabled, V1)));
    merge(&mut reader, &body(true, &[("a", Enabled, V1)], &[]), NOW_US);
    assert_eq!(
        state_of(&reader, "a"),
        Some((Disabled, V1)),
        "an enable at the same version loses"
    );
    assert_eq!(reader.behind().disabled, vec!["a".to_string()]);
}

#[test]
fn a_disable_the_store_dropped_is_kept_with_the_alert_until_a_later_enable_releases_it() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[]),
        NOW_US,
    );
    // The store drops the record: the reader keeps the disable, and alerts.
    for _ in 0..3 {
        merge(&mut reader, &body(true, &[], &[]), NOW_US);
        assert_eq!(state_of(&reader, "a"), Some((Disabled, V1)));
        assert!(alert(&reader, "restrictions_behind"));
    }
    // An `enable` for a name the store holds no record of releases it.
    merge(&mut reader, &body(true, &[("a", Enabled, V2)], &[]), NOW_US);
    assert_eq!(state_of(&reader, "a"), Some((Enabled, V2)));
    assert!(!alert(&reader, "restrictions_behind"));
}

// ── Clocks ───────────────────────────────────────────────────────────

#[test]
fn an_enabled_record_14_minutes_ahead_installs() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[]),
        NOW_US,
    );
    let ahead = NOW_US + 14 * MINUTE_US;
    merge(
        &mut reader,
        &body(true, &[("a", Enabled, ahead)], &[]),
        NOW_US,
    );
    assert_eq!(state_of(&reader, "a"), Some((Enabled, ahead)));
    assert!(!reader.clock_skew());
    assert_eq!(reader.set.records_version(NOW_US), ahead);
}

#[test]
fn a_disabled_record_far_ahead_merges() {
    let mut reader = Reader::default();
    let far = NOW_US + 365 * 24 * 60 * MINUTE_US;
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, far)], &[]),
        NOW_US,
    );
    assert_eq!(state_of(&reader, "a"), Some((Disabled, far)));
    assert!(
        !reader.clock_skew(),
        "a disable is never refused for being ahead"
    );
    // A poll reports the highest version within the ceiling only.
    assert_eq!(reader.set.records_version(NOW_US), 0);
}

#[test]
fn an_enabled_record_more_than_15_minutes_ahead_is_ignored_with_a_skew_alert_until_the_clock_is_fixed(
) {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[]),
        NOW_US,
    );
    assert!(param(&reader.query(NOW_US), "tag").is_some());

    // The store's clock runs 16 minutes ahead of this reader's.
    let ahead = NOW_US + 16 * MINUTE_US;
    let part = body(true, &[("a", Enabled, ahead)], &[]);
    merge(&mut reader, &part, NOW_US);
    assert_eq!(
        state_of(&reader, "a"),
        Some((Disabled, V1)),
        "ignored: a stays disabled"
    );
    assert_eq!(reader.skipped_ahead, vec!["a".to_string()]);
    assert!(reader.clock_skew());
    assert!(alert(&reader, "clock_skew"));
    assert!(
        reader.behind().is_empty(),
        "the store holds the newer record"
    );
    // While it skips one, every poll asks for the body again.
    assert_eq!(param(&reader.query(NOW_US), "tag"), None);

    // The clock is fixed: the same part now installs the record.
    let fixed = NOW_US + 2 * MINUTE_US;
    merge(&mut reader, &part, fixed);
    assert_eq!(state_of(&reader, "a"), Some((Enabled, ahead)));
    assert!(!reader.clock_skew());
    assert_eq!(
        param(&reader.query(fixed), "tag"),
        Some(digest_hex(&sha256(part.as_bytes())).as_str())
    );
}

/// A recovery commit lands up to 10 minutes ahead of the store's clock, so a
/// reader up to 5 minutes slow still installs it.
#[test]
fn a_recovery_version_installs_4_minutes_slow_and_is_refused_with_a_skew_alert_6_minutes_slow() {
    let store_now_ms = NOW_MS;
    let recovery = document(store_now_ms + 10 * MINUTE_MS, vec![], &[]);
    let restrictions = body(true, &[], &[]);

    let mut slow_4 = Reader::default();
    let applied = apply(
        &mut slow_4,
        answer(doc_part(&recovery), restrictions_part(&restrictions)),
        (store_now_ms - 4 * MINUTE_MS) * 1000,
    );
    assert!(applied.document_installed);
    assert!(!slow_4.clock_skew());

    let mut slow_6 = Reader::default();
    for _ in 0..2 {
        let applied = apply(
            &mut slow_6,
            answer(doc_part(&recovery), restrictions_part(&restrictions)),
            (store_now_ms - 6 * MINUTE_MS) * 1000,
        );
        assert!(!applied.document_installed);
        assert!(slow_6.document.is_none());
        assert!(slow_6.clock_skew());
    }
    assert_eq!(
        slow_6.status.documents_refused_ahead, 1,
        "a document refused again is counted once"
    );
    // Its clock fixed, the reader installs it.
    let applied = apply(
        &mut slow_6,
        answer(doc_part(&recovery), restrictions_part(&restrictions)),
        store_now_ms * 1000,
    );
    assert!(applied.document_installed);
    assert!(!slow_6.clock_skew());
}

// ── Outages ──────────────────────────────────────────────────────────

#[test]
fn an_unavailable_document_part_keeps_the_held_document() {
    let mut reader = Reader::default();
    let doc = document(NOW_MS, vec![client("acme", &test_key("acme"))], &[]);
    apply(&mut reader, answer(doc_part(&doc), unavailable()), NOW_US);
    let held = reader.document.clone().expect("installed");
    let applied = apply(&mut reader, answer(unavailable(), unavailable()), NOW_US);
    assert_eq!(applied, Applied::default());
    assert!(Arc::ptr_eq(reader.document.as_ref().unwrap(), &held));
    assert!(!reader.status.store_behind && !reader.status.store_edited);
    assert!(reader.status.document_fault.is_none());
}

#[test]
fn an_unavailable_restrictions_part_keeps_what_is_held() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[digest(1)]),
        NOW_US,
    );
    let held = reader.set.clone();
    let query = reader.query(NOW_US);
    let applied = apply(&mut reader, answer(unavailable(), unavailable()), NOW_US);
    assert!(!applied.set_changed);
    assert_eq!(reader.set, held);
    assert_eq!(reader.query(NOW_US), query, "the next poll asks as before");
    assert!(reader.status.restrictions_fault.is_none());
}

#[test]
fn a_store_behind_or_edited_keeps_the_held_document_and_alerts() {
    let mut reader = Reader::default();
    let doc = document(NOW_MS, vec![], &[]);
    apply(&mut reader, answer(doc_part(&doc), unavailable()), NOW_US);

    // The same version with other bytes: edited outside its write path.
    let edited = document(NOW_MS, vec![client("acme", &test_key("acme"))], &[]);
    apply(
        &mut reader,
        answer(doc_part_held(&edited), unavailable()),
        NOW_US,
    );
    assert!(reader.status.store_edited && !reader.status.store_behind);
    assert_eq!(reader.document.as_ref().unwrap().text, doc);

    // A lower version: behind. Version 0 is behind every held document.
    let older = document(NOW_MS - 1, vec![], &[]);
    apply(
        &mut reader,
        answer(doc_part_held(&older), unavailable()),
        NOW_US,
    );
    assert!(reader.status.store_behind && !reader.status.store_edited);
    apply(
        &mut reader,
        answer(json!({"status": "ok", "version": 0}), unavailable()),
        NOW_US,
    );
    assert!(reader.status.store_behind);
    assert!(alert(&reader, "store_behind"));
    assert_eq!(reader.document.as_ref().unwrap().text, doc);

    // The store catches up: both alerts clear.
    apply(
        &mut reader,
        answer(doc_part_held(&doc), unavailable()),
        NOW_US,
    );
    assert!(!reader.status.store_behind && !reader.status.store_edited);
}

// ── Checks on what a poll carries ───────────────────────────────────

#[test]
fn a_part_over_2_mib_is_refused_and_what_is_held_kept() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[]),
        NOW_US,
    );
    let held = reader.set.clone();
    // A valid body, padded with whitespace past 2 MiB.
    let big = format!("{}{}", body(true, &[], &[]), " ".repeat(MAX_TEXT_BYTES));
    assert_eq!(
        check_part(&digest_hex(&sha256(big.as_bytes())), &big),
        Err("part.size")
    );
    merge(&mut reader, &big, NOW_US);
    assert_eq!(reader.status.restrictions_fault, Some("part.size"));
    assert!(alert(&reader, "restrictions_refused"));
    assert_eq!(reader.set, held);
}

#[test]
fn a_part_with_a_wrong_tag_is_refused_and_what_is_held_kept() {
    let mut reader = Reader::default();
    merge(
        &mut reader,
        &body(true, &[("a", Disabled, V1)], &[]),
        NOW_US,
    );
    let held = reader.set.clone();
    let lifted = body(true, &[("a", Enabled, V2)], &[]);
    let wrong = json!({
        "status": "ok",
        "tag": digest_hex(&sha256(b"another body")),
        "body": lifted,
    });
    apply(&mut reader, answer(unavailable(), wrong), NOW_US);
    assert_eq!(reader.status.restrictions_fault, Some("part.tag"));
    assert_eq!(reader.set, held);
    // The next good part clears the alert.
    merge(&mut reader, &lifted, NOW_US);
    assert_eq!(reader.status.restrictions_fault, None);
}

#[test]
fn a_document_over_2_mib_is_refused() {
    let doc = document(NOW_MS, vec![], &[]);
    let big = format!("{doc}{}", " ".repeat(MAX_TEXT_BYTES));
    assert_eq!(check_document(&big).err(), Some("document.size"));
    let mut reader = Reader::default();
    apply(&mut reader, answer(doc_part(&big), unavailable()), NOW_US);
    assert_eq!(reader.status.document_fault, Some("document.size"));
    assert!(reader.document.is_none());
}

#[test]
fn a_document_part_that_fails_its_checks_keeps_the_held_document() {
    let held = document(NOW_MS - 1, vec![], &[]);
    let next = document(NOW_MS, vec![], &[]);
    let cases: Vec<(&str, Value)> = vec![
        ("poll.document", json!("ok")),
        ("poll.document", json!({"status": "maybe"})),
        (
            "poll.document.version",
            json!({"status": "ok", "version": -1}),
        ),
        (
            "poll.document.digest",
            json!({"status": "ok", "version": NOW_MS, "digest": "nope", "text": next}),
        ),
        (
            "poll.document.text",
            json!({"status": "ok", "version": NOW_MS, "digest": digest_hex(&sha256(next.as_bytes()))}),
        ),
        (
            "poll.document.digest",
            json!({"status": "ok", "version": NOW_MS, "digest": digest_hex(&digest(1)), "text": next}),
        ),
        // The text's own version must be the part's.
        (
            "poll.document.version",
            json!({"status": "ok", "version": NOW_MS + 1, "digest": digest_hex(&sha256(next.as_bytes())), "text": next}),
        ),
        ("document.schema", {
            let text = next.replace("\"schema\":1", "\"schema\":2");
            json!({"status": "ok", "version": NOW_MS, "digest": digest_hex(&sha256(text.as_bytes())), "text": text})
        }),
    ];
    for (fault, part) in cases {
        let mut reader = Reader::default();
        apply(&mut reader, answer(doc_part(&held), unavailable()), NOW_US);
        apply(&mut reader, answer(part.clone(), unavailable()), NOW_US);
        assert_eq!(reader.status.document_fault, Some(fault), "{part}");
        assert_eq!(reader.document.as_ref().unwrap().text, held, "{part}");
        assert!(alert(&reader, "document_refused"));
    }
    // A missing part is refused too, and an unreadable answer is no answer.
    let mut reader = Reader::default();
    reader
        .apply(br#"{"restrictions": {"status": "unavailable"}}"#, NOW_US)
        .unwrap();
    assert_eq!(reader.status.document_fault, Some("poll.document"));
    assert_eq!(reader.apply(b"not json", NOW_US), Err("poll.answer"));
}

#[test]
fn a_restrictions_part_without_a_body_is_the_last_one_merged_or_refused() {
    let mut reader = Reader::default();
    let part = body(true, &[("a", Disabled, V1)], &[]);
    merge(&mut reader, &part, NOW_US);
    let tag = digest_hex(&sha256(part.as_bytes()));
    // The tag the reader sent: nothing changed.
    let same = json!({"status": "ok", "tag": tag});
    let applied = apply(&mut reader, answer(unavailable(), same), NOW_US);
    assert!(!applied.set_changed);
    assert_eq!(reader.status.restrictions_fault, None);
    // Another tag needs its body.
    let other = json!({"status": "ok", "tag": digest_hex(&digest(9))});
    apply(&mut reader, answer(unavailable(), other), NOW_US);
    assert_eq!(
        reader.status.restrictions_fault,
        Some("poll.restrictions.body")
    );
    assert_eq!(state_of(&reader, "a"), Some((Disabled, V1)));
}

#[test]
fn installing_a_document_adds_its_revoked_digests_and_judges_the_store_again() {
    let mut reader = Reader::default();
    let part = body(true, &[], &[]);
    merge(&mut reader, &part, NOW_US);
    assert!(reader.behind().is_empty());
    // The document revokes a digest the restrictions do not list yet.
    let doc = document(NOW_MS, vec![], &[digest(7)]);
    let applied = apply(
        &mut reader,
        answer(
            doc_part(&doc),
            json!({"status": "ok", "tag": digest_hex(&sha256(part.as_bytes()))}),
        ),
        NOW_US,
    );
    assert!(applied.document_installed && applied.set_changed);
    assert!(reader.set.revoked_digests.contains(&digest(7)));
    assert_eq!(reader.behind().revoked_digests, vec![digest(7)]);
    // The store's restrictions catch up.
    merge(&mut reader, &body(true, &[], &[digest(7)]), NOW_US);
    assert!(reader.behind().is_empty());
}

// ── The poll ─────────────────────────────────────────────────────────

#[test]
fn the_poll_carries_the_held_set_hash_and_its_completeness_mark() {
    let mut reader = Reader::default();
    let q = reader.query(NOW_US);
    assert_eq!(param(&q, "version"), Some("0"));
    assert_eq!(param(&q, "digest"), None);
    assert_eq!(
        param(&q, "tag"),
        None,
        "no tag until a part has been merged"
    );
    assert_eq!(param(&q, "records_version"), Some("0"));
    assert_eq!(
        param(&q, "set_hash"),
        Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    );
    assert_eq!(param(&q, "set_complete"), Some("false"));

    let partial = body(false, &[("a", Disabled, V1)], &[]);
    merge(&mut reader, &partial, NOW_US);
    let q = reader.query(NOW_US);
    assert_eq!(
        param(&q, "set_hash"),
        Some(digest_hex(&reader.set.hash()).as_str())
    );
    assert_eq!(param(&q, "set_complete"), Some("false"));

    let doc = document(NOW_MS, vec![], &[]);
    let full = body(
        true,
        &[("a", Disabled, V1), ("b", Enabled, V2)],
        &[digest(3)],
    );
    apply(
        &mut reader,
        answer(doc_part(&doc), restrictions_part(&full)),
        NOW_US,
    );
    let q = reader.query(NOW_US);
    assert_eq!(param(&q, "version"), Some(NOW_MS.to_string().as_str()));
    assert_eq!(
        param(&q, "digest"),
        Some(digest_hex(&sha256(doc.as_bytes())).as_str())
    );
    assert_eq!(
        param(&q, "tag"),
        Some(digest_hex(&sha256(full.as_bytes())).as_str())
    );
    assert_eq!(param(&q, "records_version"), Some(V2.to_string().as_str()));
    assert_eq!(
        param(&q, "set_hash"),
        Some(digest_hex(&reader.set.hash()).as_str())
    );
    assert_eq!(param(&q, "set_complete"), Some("true"));
}

// ── What checking costs ──────────────────────────────────────────────

/// `len` bytes of JSON array, its elements arrays nested 8 deep: the shape
/// that costs the most to build as a JSON value tree.
fn nested_padding(len: usize) -> String {
    const UNIT: &str = "[[[[[[[[0]]]]]]]]";
    let mut pad = String::with_capacity(len + UNIT.len() + 2);
    pad.push('[');
    while pad.len() + UNIT.len() + 2 < len {
        if pad.len() > 1 {
            pad.push(',');
        }
        pad.push_str(UNIT);
    }
    pad.push(']');
    pad
}

/// Members no rule reads are never built: a 16 MiB answer whose bulk is an
/// unknown member costs a fraction of its size, not the gigabytes a value
/// tree of it would, and an instance cannot be run out of memory by one.
#[test]
fn an_answer_costs_what_its_known_members_do() {
    let answer = format!(
        r#"{{"document":{{"status":"unavailable","pad":{}}},"restrictions":{{"status":"unavailable"}}}}"#,
        nested_padding(16 * 1024 * 1024 - 100)
    );
    let mut reader = Reader::default();
    let (applied, bytes) = allocated_by(|| reader.apply(answer.as_bytes(), NOW_US));
    assert_eq!(applied, Ok(Applied::default()));
    assert!(
        bytes < answer.len() / 4,
        "applying a {} byte answer allocated {bytes} bytes",
        answer.len()
    );
}

/// The same holds for a document text and a restrictions body at their size
/// limit, refused for the member that should not be there.
#[test]
fn checking_a_text_costs_a_small_multiple_of_its_size() {
    let doc = format!(
        r#"{{"schema":1,"pad":{}}}"#,
        nested_padding(MAX_TEXT_BYTES - 100)
    );
    let (refused, bytes) = allocated_by(|| check_document(&doc).err());
    assert_eq!(refused, Some("document.members"));
    assert!(
        bytes < doc.len() / 4,
        "checking a document allocated {bytes} bytes"
    );

    let body = format!(
        r#"{{"full":true,"pad":{}}}"#,
        nested_padding(MAX_TEXT_BYTES - 100)
    );
    let tag = digest_hex(&sha256(body.as_bytes()));
    let (refused, bytes) = allocated_by(|| check_part(&tag, &body).err());
    assert_eq!(refused, Some("part.members"));
    assert!(
        bytes < body.len() / 4,
        "checking a part allocated {bytes} bytes"
    );
}

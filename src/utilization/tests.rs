use super::*;

mod betas;
mod claims;
mod coordination;
mod time_adjusted;
mod weight;

/// Keys compare decoded, only lowercase ASCII spellings pass, keys are
/// distinct once `_` and `-` are removed, a decision field may not be
/// respelt, and only top-level keys count.
#[test]
fn top_level_keys_unambiguous_requires_distinct_lowercase_ascii_keys() {
    assert!(top_level_keys_unambiguous(
        br#"{"model":"a","messages":[]}"#
    ));
    assert!(top_level_keys_unambiguous(br#"{"max_tokens":1,"top_k":2}"#));
    assert!(top_level_keys_unambiguous(
        br#"{"model":"a","messages":[{"role":"user","role":"assistant","Role":"x"}]}"#
    ));
    assert!(top_level_keys_unambiguous(b"{}"));
    // A respelling of a field the proxy decides nothing on has no proxy view
    // to disagree with.
    assert!(top_level_keys_unambiguous(br#"{"topk":1}"#));
    for body in AMBIGUOUS_KEY_BODIES {
        assert!(!top_level_keys_unambiguous(body.as_bytes()), "{body}");
    }
    // Keys a case-insensitive decoder reads as another field, alone or next
    // to it: U+017F folds to `s`, U+212A KELVIN SIGN to `k`.
    for body in [
        "{\"Model\":\"a\"}",
        "{\"me\u{17f}\u{17f}ages\":[]}",
        "{\"max_to\u{212a}ens\":1}",
        "{\"messages\":[],\"me\u{17f}\u{17f}ages\":[]}",
        "{\"max_tokens\":1,\"max_to\u{212a}ens\":2}",
    ] {
        assert!(!top_level_keys_unambiguous(body.as_bytes()), "{body}");
    }
    // Keys a decoder that also ignores `_` and `-` reads as another field:
    // a decision field respelt alone, or any two keys that differ only there.
    for body in [
        r#"{"mo_del":"a"}"#,
        r#"{"m-o-d-e-l":"a"}"#,
        r#"{"spe_ed":"fast"}"#,
        r#"{"mess_ages":[]}"#,
        r#"{"sys-tem":"x"}"#,
        r#"{"maxtokens":1}"#,
        r#"{"model":"a","mo_del":"b"}"#,
        r#"{"max_tokens":1,"maxtokens":2}"#,
    ] {
        assert!(!top_level_keys_unambiguous(body.as_bytes()), "{body}");
    }
    assert!(
        !top_level_keys_unambiguous(b"[1]"),
        "not an object fails closed"
    );
}

/// Every protected field passes as spelt and is refused respelt, including
/// each body field of `BETA_BODY_FIELDS`, so a row added there is covered
/// without editing this test.
#[test]
fn top_level_keys_unambiguous_refuses_every_respelt_protected_field() {
    let fields: Vec<&str> = protected_fields().collect();
    // Every field a models-restricted request loses on an OpenAI-protocol
    // endpoint (LAB-6794), read from the list the strip itself uses.
    for field in OPENAI_FALLBACK_FIELDS.iter().chain(&[
        "fallback_credit_token",
        "context_management",
        "safeguards",
    ]) {
        assert!(fields.contains(field), "{field} must be protected");
    }
    for field in fields {
        let exact = format!("{{\"{field}\":1}}");
        assert!(top_level_keys_unambiguous(exact.as_bytes()), "{exact}");
        let respelt = format!("{{\"{}-{}\":1}}", &field[..1], &field[1..]);
        assert!(!top_level_keys_unambiguous(respelt.as_bytes()), "{respelt}");
    }
}

use super::*;

mod betas;
mod claims;
mod coordination;
mod time_adjusted;
mod weight;

/// Keys compare decoded, only lowercase ASCII spellings pass, and only
/// top-level keys count.
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
    assert!(
        !top_level_keys_unambiguous(b"[1]"),
        "not an object fails closed"
    );
}

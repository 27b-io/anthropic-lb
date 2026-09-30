use super::*;

mod betas;
mod claims;
mod coordination;
mod time_adjusted;
mod weight;

/// Keys compare decoded and case-folded, and only top-level keys count.
#[test]
fn top_level_keys_unique_compares_decoded_top_level_keys() {
    assert!(top_level_keys_unique(br#"{"model":"a","messages":[]}"#));
    assert!(top_level_keys_unique(
        br#"{"model":"a","messages":[{"role":"user","role":"assistant"}]}"#
    ));
    assert!(top_level_keys_unique(b"{}"));
    for body in DUPLICATE_KEY_BODIES {
        assert!(!top_level_keys_unique(body.as_bytes()), "{body}");
    }
    // Case folds beyond ASCII: U+017F is `s`, U+212A KELVIN SIGN is `k`.
    for body in [
        "{\"messages\":[],\"me\u{17f}\u{17f}ages\":[]}",
        "{\"max_tokens\":1,\"max_to\u{212a}ens\":2}",
    ] {
        assert!(!top_level_keys_unique(body.as_bytes()), "{body}");
    }
    assert!(!top_level_keys_unique(b"[1]"), "not an object fails closed");
}

use super::*;

mod betas;
mod claims;
mod coordination;
mod time_adjusted;
mod weight;

/// Keys compare decoded, and only top-level keys count.
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
    assert!(!top_level_keys_unique(b"[1]"), "not an object fails closed");
}

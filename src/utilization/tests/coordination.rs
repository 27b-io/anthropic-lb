use super::*;

// LAB-3026: a configured-but-unparseable `redis_url` must be reported as an
// error from `start_coordination_redis` (the caller in `main` turns that
// into a startup panic) rather than silently degrading to local-only.
// Needs no live backend — parsing fails before any I/O. The
// valid-but-unreachable case is covered in src/persistence/tests/real_redis.rs by
// `backend_down_at_startup_serves_local_only_then_attaches` (still
// `Ok`, still local-only-then-reconnect); the unset case is the untouched
// `else { None }` arm in `main` and needs no test.
#[test]
fn start_coordination_redis_rejects_unparseable_url() {
    // An unescaped '/' inside the password ends URL authority parsing early
    // (everything after is read as path), leaving a garbage port — the
    // rotated-password shape from the ticket. Verified against the `url`
    // crate directly: unescaped '@' alone does NOT break parsing (the last
    // '@' wins as the userinfo/host separator), but '/', '?', and '#' do.
    let result = start_coordination_redis(
        "redis://user:pa/ss@127.0.0.1:6379",
        PerformanceConfig::default(),
        ConnectionConfig::default(),
        ReconnectPolicy::new_constant(0, 100),
    );
    assert!(
        result.is_err(),
        "malformed userinfo must fail to parse, not silently mis-route"
    );
}

// LAB-3026 review follow-up: a reserved char could in principle swallow the
// REAL host into the path while `Url::parse` still succeeds, if the bogus
// "port" left behind (username:password-prefix) happens to be numeric —
// e.g. `redis://user:12345/rest@127.0.0.1:6379` parses OK with host="user",
// port=12345, silently discarding the real `127.0.0.1:6379`. That would be
// a mis-route, not a startup failure, and the AC would be defeated. It
// still can't reach `Ok` here: fred's `parse_url_db` (run right after) reads
// the leftover path as the db-index segment and requires it parse as a u8
// (0-255); a swallowed `@host:port` remainder never also satisfies that, so
// this shape errors too — verified, not assumed.
#[test]
fn start_coordination_redis_rejects_numeric_password_prefix_mis_route() {
    let result = start_coordination_redis(
        "redis://user:12345/rest@127.0.0.1:6379",
        PerformanceConfig::default(),
        ConnectionConfig::default(),
        ReconnectPolicy::new_constant(0, 100),
    );
    assert!(
        result.is_err(),
        "a numeric password-prefix must not silently mis-route to the wrong host"
    );
}

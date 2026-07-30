# Testing Guide for anthropic-lb

> Commands and patterns for running, filtering, and debugging the test suite.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Test Organization](#test-organization)
- [Test Categories](#test-categories)
- [Running Tests in CI/CD](#running-tests-in-cicd)
- [Coverage Analysis](#coverage-analysis)
- [Debugging Tests](#debugging-tests)
- [Test Structure](#test-structure)
- [Common Test Patterns](#common-test-patterns)
- [Troubleshooting](#troubleshooting)
- [Performance Testing](#performance-testing)
- [Test Best Practices](#test-best-practices)
- [Example Test Session](#example-test-session)
- [Continuous Integration](#continuous-integration)
- [Test Documentation](#test-documentation)
- [Support](#support)

---

## Quick Start

To run all tests:

```bash
cargo test
```

To run a specific test:

```bash
cargo test test_minimal_valid_config
```

> [!TIP]
> Add `-- --nocapture` to any `cargo test` invocation to see `println!`/`dbg!` output from passing tests, not just failures.

---

## Test Organization

| # | Location | Purpose | Run only these |
|:-:|:---------|:--------|:----------------|
| 1 | `src/main.rs` — `#[cfg(test)]` module | Unit tests: individual functions and algorithms | `cargo test --lib` |
| 2 | `tests/config_test.rs` | TOML configuration parsing and validation | `cargo test --test config_test` |
| 3 | `tests/dependency_test.rs` | `Cargo.lock` integrity and dependency management | `cargo test --test dependency_test` |

<details>
<summary><strong>Running specific test modules</strong></summary>

```bash
# EWMA tests
cargo test --lib ewma

# Burn rate tests
cargo test --lib burn_rate

# Pick account tests
cargo test --lib pick

# IP allowlist tests
cargo test --lib ip_allow

# One specific config test
cargo test --test config_test test_minimal_valid_config
```

</details>

---

## Redis Integration Tests

The `redis_integration` module (`src/tests.rs`) exercises the cross-replica
coordination layer — budget INCRBY/EXPIRE, hard-limit propagation, the Lua
CAS recovery sentinel, the `sync_from_redis` MGET merge, `SCAN` pagination,
pipelined `HINCRBY`, and the `SET NX EX` probe lock — against a **real
Redis/Valkey backend**.

They are opt-in via `ALB_TEST_REDIS_URL` (plain `redis://host:port`, no db
suffix, no auth):

```bash
# Start a throwaway backend (never point this at a Redis holding real data —
# the tests FLUSHDB the logical DBs they use)
redis-server --port 16379 --save '' --appendonly no --daemonize yes \
  --pidfile /tmp/alb-test-redis.pid

ALB_TEST_REDIS_URL=redis://127.0.0.1:16379 cargo test redis_integration

kill "$(cat /tmp/alb-test-redis.pid)"
```

> [!IMPORTANT]
> When `ALB_TEST_REDIS_URL` is unset the tests print a `SKIP` notice and
> return. When it is set but the backend is unreachable they **panic** —
> CI (which runs a Valkey service container, see `.github/workflows/ci.yml`)
> can never skip them silently.

Each test owns a dedicated logical DB (`SELECT 1`–`12`) because all `alb:*`
coordination keys are hardcoded in production code and cannot be prefixed
per-test. Failure-path tests route the connection through an in-test
killable TCP proxy to simulate the backend dying mid-run.

---

## Test Categories

### By Functionality

| Category | Command |
|:---------|:--------|
| Routing | `cargo test pick_endpoint` |
| Time & utilization | `cargo test time_adjusted`, `cargo test effective_util` |
| Configuration | `cargo test --test config_test` |
| Client identity | `cargo test resolve_client_id`, `cargo test is_operator` |
| Budget & limits | `cargo test budget`, `cargo test emergency`, `cargo test utilization_limit` |

### By Test Type

| Type | Command |
|:---------|:--------|
| All unit tests | `cargo test --lib` |
| All integration tests | `cargo test --test '*'` |
| HTTP handler tests | `cargo test proxy_`, `cargo test openai_`, `cargo test upstream_` |

---

## Running Tests in CI/CD

```bash
# Run all tests with one thread (more stable for CI)
cargo test -- --test-threads=1

# Run with detailed output
cargo test -- --nocapture --test-threads=1

# Run with timing information
cargo test -- --nocapture --show-output --test-threads=1
```

---

## Coverage Analysis

<details>
<summary><strong>tarpaulin</strong> (requires <code>cargo-tarpaulin</code>)</summary>

```bash
# Install
cargo install cargo-tarpaulin

# Generate HTML coverage report
cargo tarpaulin --out Html

# Generate coverage with detailed line info
cargo tarpaulin --out Html --line --ignore-tests

# View report (opens report/index.html in browser)
```

</details>

<details>
<summary><strong>llvm-cov (alternative)</strong></summary>

```bash
# Install
cargo install cargo-llvm-cov

# Generate coverage
cargo llvm-cov --html

# Open report
cargo llvm-cov --open
```

</details>

---

## Debugging Tests

| Goal | Command |
|:-----|:--------|
| Single test with output | `cargo test test_name -- --nocapture` |
| Match a name pattern | `cargo test ewma -- --nocapture` |
| Show execution time | `cargo test -- --nocapture --test-threads=1 --show-output` |
| Run `#[ignore]`d tests | `cargo test -- --ignored` |
| Release mode (faster) | `cargo test --release` |

---

## Test Structure

### Unit Tests

- **Location**: `src/main.rs` in `#[cfg(test)]` module
- **Purpose**: Test individual functions and algorithms

```rust
#[test]
fn ewma_single_update() {
    // Test code
}
```

### Integration Tests

- **Location**: `tests/` directory
- **Purpose**: Test configuration parsing and file operations

```rust
#[test]
fn test_minimal_valid_config() {
    // Test code
}
```

### Async Tests

- **Marker**: `#[tokio::test]` instead of `#[test]`
- **Purpose**: Test async functions with tokio runtime

```rust
#[tokio::test]
async fn pick_prefers_lowest_utilization() {
    // Async test code
}
```

---

## Common Test Patterns

<details>
<summary><strong>Testing configuration parsing</strong></summary>

```rust
let config_content = r#"
listen = "127.0.0.1:8082"
strategy = "dynamic-capacity-v1"
"#;
let result: Result<toml::Value, _> = toml::from_str(&config_content);
assert!(result.is_ok());
```

</details>

<details>
<summary><strong>Testing HTTP handlers</strong></summary>

```rust
#[tokio::test]
async fn test_handler() {
    let (mock_url, _handle) = spawn_mock_upstream().await;
    let (app, state) = test_app(&mock_url, Some("key".to_string()));
    let addr = serve(app).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/v1/messages", addr))
        .header("x-api-key", "key")
        .body(r#"{"model":"test","max_tokens":1}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}
```

</details>

<details>
<summary><strong>Testing algorithm behavior</strong></summary>

```rust
#[tokio::test]
async fn test_routing() {
    let state = test_state_with(vec![
        mk_endpoint("high", "sk-ant-api-high"),
        mk_endpoint("low", "sk-ant-api-low"),
    ]);

    // high=0.8 (headroom 0.2), low=0.2 (headroom 0.8) → ~80% should go to "low"
    {
        let mut info = state.endpoints[0].rate_info.write().await;
        info.utilization = Some(0.8);
    }
    {
        let mut info = state.endpoints[1].rate_info.write().await;
        info.utilization = Some(0.2);
    }

    // Routing is headroom-proportional weighted bucket hashing — assert the
    // traffic share, not a single deterministic pick
    let mut counts = [0u32; 2];
    for _ in 0..1000 {
        let idx = state.pick_endpoint(None, "", &[]).await.unwrap();
        counts[idx] += 1;
    }
    let low_pct = counts[1] as f64 / 1000.0;
    assert!((0.75..=0.85).contains(&low_pct));
}
```

</details>

---

## Troubleshooting

| Symptom | Check |
|:--------|:------|
| Tests fail with "Connection Refused" | Tests bind random ports, so collisions should be rare — confirm no other instance is already running |
| Tests fail with "File Not Found" | Some tests expect files like `config.toml.example` to exist — run tests from the project root, and confirm the test uses temp files correctly |
| Async tests hang | Check for deadlocks in `RwLock`/`Mutex` usage; isolate with `--test-threads=1` |
| Tests pass locally but fail in CI | Look for timing-sensitive tests, tests depending on specific system state, or missing CI dependencies |

> [!IMPORTANT]
> Run tests from the project root — several integration tests resolve paths (like `config.toml.example`) relative to it.

---

## Performance Testing

For benchmarking (requires nightly Rust):

```bash
# Run benchmarks
cargo +nightly bench

# Run specific benchmark
cargo +nightly bench bench_name
```

Alternative with criterion (add to `Cargo.toml`):

```bash
cargo bench
```

---

## Test Best Practices

1. **Naming**: Use descriptive names that explain what is tested
2. **Isolation**: Each test should be independent
3. **Cleanup**: Use temporary files that auto-cleanup
4. **Assertions**: Include descriptive failure messages
5. **Coverage**: Test both success and failure paths
6. **Edge Cases**: Test boundary conditions (0, 1.0, empty, null)
7. **Documentation**: Comment complex test logic

---

## Example Test Session

```bash
# 1. Run all tests to verify baseline
cargo test

# 2. Run specific module you're working on
cargo test --lib ewma -- --nocapture

# 3. Run integration tests
cargo test --test config_test

# 4. Check coverage
cargo tarpaulin --out Html

# 5. Review coverage report
open tarpaulin-report.html
```

---

## Continuous Integration

Example GitHub Actions workflow:

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: cargo test --verbose
      - run: cargo test --release --verbose
```

---

## Test Documentation

For detailed test documentation, see:

| Resource | Description |
|:---------|:-------------|
| [`TEST_SUMMARY.md`](TEST_SUMMARY.md) | Comprehensive test coverage summary |
| `tests/config_test.rs` | Configuration test examples |
| `tests/dependency_test.rs` | Dependency test examples |
| `src/main.rs` (`#[cfg(test)]` module) | Unit test examples |

---

## Support

For issues or questions about tests:

1. Check test output with the `--nocapture` flag
2. Review `TEST_SUMMARY.md` for test descriptions
3. Examine test source code for examples
4. Check `Cargo.toml` for test dependencies

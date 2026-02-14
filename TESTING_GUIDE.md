# Testing Guide for anthropic-lb

## Quick Start

To run all tests:
```bash
cargo test
```

To run tests with output visible:
```bash
cargo test -- --nocapture
```

To run a specific test:
```bash
cargo test test_minimal_valid_config
```

## Test Organization

### 1. Unit Tests in src/main.rs
These tests are embedded in the main source file within a `#[cfg(test)]` module.

Run only the library tests:
```bash
cargo test --lib
```

Run specific test modules:
```bash
# EWMA tests
cargo test --lib ewma

# Burn rate tests
cargo test --lib burn_rate

# Pick account tests
cargo test --lib pick

# IP allowlist tests
cargo test --lib ip_allow
```

### 2. Configuration Tests (tests/config_test.rs)
Tests for TOML configuration parsing and validation.

Run configuration tests:
```bash
cargo test --test config_test
```

Run specific config test:
```bash
cargo test --test config_test test_minimal_valid_config
```

### 3. Dependency Tests (tests/dependency_test.rs)
Tests for Cargo.lock integrity and dependency management.

Run dependency tests:
```bash
cargo test --test dependency_test
```

## Test Categories

### By Functionality

#### Routing Tests
```bash
cargo test pick_account
```

#### Time & Utilization Tests
```bash
cargo test time_adjusted
cargo test effective_util
```

#### Configuration Tests
```bash
cargo test --test config_test
```

#### Client Identity Tests
```bash
cargo test resolve_client_id
cargo test is_operator
```

#### Budget & Limits Tests
```bash
cargo test budget
cargo test emergency
cargo test utilization_limit
```

### By Test Type

#### All Unit Tests
```bash
cargo test --lib
```

#### All Integration Tests
```bash
cargo test --test '*'
```

#### HTTP Handler Tests
```bash
cargo test proxy_
cargo test openai_
cargo test upstream_
```

## Running Tests in CI/CD

For continuous integration:
```bash
# Run all tests with one thread (more stable for CI)
cargo test -- --test-threads=1

# Run with detailed output
cargo test -- --nocapture --test-threads=1

# Run with timing information
cargo test -- --nocapture --show-output --test-threads=1
```

## Coverage Analysis

To generate a test coverage report (requires cargo-tarpaulin):
```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate HTML coverage report
cargo tarpaulin --out Html

# Generate coverage with detailed line info
cargo tarpaulin --out Html --line --ignore-tests

# View report (opens report/index.html in browser)
```

Alternative with llvm-cov:
```bash
# Install llvm-cov
cargo install cargo-llvm-cov

# Generate coverage
cargo llvm-cov --html

# Open report
cargo llvm-cov --open
```

## Debugging Tests

### Run a Single Test with Output
```bash
cargo test test_name -- --nocapture
```

### Run Tests Matching a Pattern
```bash
cargo test ewma -- --nocapture
```

### Show Test Execution Time
```bash
cargo test -- --nocapture --test-threads=1 --show-output
```

### Run Ignored Tests
```bash
cargo test -- --ignored
```

### Run in Release Mode (faster)
```bash
cargo test --release
```

## Test Structure

### Unit Tests
- **Location**: src/main.rs in `#[cfg(test)]` module
- **Purpose**: Test individual functions and algorithms
- **Example**:
  ```rust
  #[test]
  fn ewma_single_update() {
      // Test code
  }
  ```

### Integration Tests
- **Location**: tests/ directory
- **Purpose**: Test configuration parsing and file operations
- **Example**:
  ```rust
  #[test]
  fn test_minimal_valid_config() {
      // Test code
  }
  ```

### Async Tests
- **Marker**: `#[tokio::test]` instead of `#[test]`
- **Purpose**: Test async functions with tokio runtime
- **Example**:
  ```rust
  #[tokio::test]
  async fn pick_prefers_lowest_utilization() {
      // Async test code
  }
  ```

## Common Test Patterns

### Testing Configuration Parsing
```rust
let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
"#;
let result: Result<toml::Value, _> = toml::from_str(&config_content);
assert!(result.is_ok());
```

### Testing HTTP Handlers
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

### Testing Algorithm Behavior
```rust
#[tokio::test]
async fn test_routing() {
    let state = test_state_with(vec![
        make_account("a", "sk-ant-api-a"),
        make_account("b", "sk-ant-api-b"),
    ]);

    // Set utilization
    {
        let mut info = state.accounts[0].rate_info.write().await;
        info.utilization = Some(0.8);
    }

    // Test routing behavior
    let idx = state.pick_account(None, "").await.unwrap();
    assert_eq!(idx, 1); // Should pick account b
}
```

## Troubleshooting

### Tests Fail with "Connection Refused"
- Check if tests are trying to bind to already-used ports
- Tests use random ports, so this should be rare
- Ensure no other instances are running

### Tests Fail with "File Not Found"
- Some tests expect files like `config.toml.example` to exist
- Run tests from the project root directory
- Check that test uses temporary files correctly

### Async Tests Hang
- Ensure tokio runtime is properly initialized
- Check for deadlocks in RwLock/Mutex usage
- Use `--test-threads=1` to isolate hanging test

### Tests Pass Locally but Fail in CI
- Check for timing-sensitive tests
- Ensure tests don't depend on specific system state
- Verify all dependencies are available in CI environment

## Performance Testing

For benchmarking (requires nightly Rust):
```bash
# Run benchmarks
cargo +nightly bench

# Run specific benchmark
cargo +nightly bench bench_name
```

Alternative with criterion (add to Cargo.toml):
```bash
cargo bench
```

## Test Best Practices

1. **Naming**: Use descriptive names that explain what is tested
2. **Isolation**: Each test should be independent
3. **Cleanup**: Use temporary files that auto-cleanup
4. **Assertions**: Include descriptive failure messages
5. **Coverage**: Test both success and failure paths
6. **Edge Cases**: Test boundary conditions (0, 1.0, empty, null)
7. **Documentation**: Comment complex test logic

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

## Test Documentation

For detailed test documentation, see:
- `TEST_SUMMARY.md` - Comprehensive test coverage summary
- `tests/config_test.rs` - Configuration test examples
- `tests/dependency_test.rs` - Dependency test examples
- `src/main.rs` (#[cfg(test)] module) - Unit test examples

## Support

For issues or questions about tests:
1. Check test output with `--nocapture` flag
2. Review TEST_SUMMARY.md for test descriptions
3. Examine test source code for examples
4. Check Cargo.toml for test dependencies
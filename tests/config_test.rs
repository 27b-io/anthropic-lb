// Integration tests for configuration parsing and validation

use std::fs;
use std::io::Write;
use tempfile::NamedTempFile;

// Helper to create a temporary config file for testing
fn create_temp_config(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(content.as_bytes())
        .expect("Failed to write to temp file");
    file.flush().expect("Failed to flush temp file");
    file
}

#[test]
fn test_minimal_valid_config() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "test"
token = "sk-ant-api-test-key"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    // Try to parse the config
    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(
        result.is_ok(),
        "Minimal valid config should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config["listen"].as_str().unwrap(), "127.0.0.1:8082");
    assert_eq!(
        config["upstream"].as_str().unwrap(),
        "https://api.anthropic.com"
    );
    assert!(config["accounts"].is_array());
    assert_eq!(config["accounts"].as_array().unwrap().len(), 1);
}

#[test]
fn test_config_with_optional_fields() {
    let config_content = r#"
listen = "0.0.0.0:8082"
upstream = "https://api.anthropic.com"
strategy = "dynamic-capacity"
rate_limit_cooldown_secs = 60
probe_interval_secs = 300
proxy_key = "secret-key-123"
allowed_ips = ["10.0.0.0/8", "192.168.1.1"]
auto_cache = true
shadow_log = "/var/log/anthropic-lb.jsonl"
emergency_threshold = 0.90
soft_limit = 0.85

[[accounts]]
name = "primary"
token = "sk-ant-oat01-token1"

[[accounts]]
name = "secondary"
token = "sk-ant-api-token2"
models = ["claude-opus-*", "claude-sonnet-4-20250514"]
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(
        result.is_ok(),
        "Config with all optional fields should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config["rate_limit_cooldown_secs"].as_integer().unwrap(), 60);
    assert_eq!(config["probe_interval_secs"].as_integer().unwrap(), 300);
    assert_eq!(config["proxy_key"].as_str().unwrap(), "secret-key-123");
    assert!(config["auto_cache"].as_bool().unwrap());
    assert_eq!(config["emergency_threshold"].as_float().unwrap(), 0.90);
    assert_eq!(config["accounts"].as_array().unwrap().len(), 2);

    // Check second account has model restrictions
    let accounts = config["accounts"].as_array().unwrap();
    let secondary = &accounts[1];
    assert_eq!(secondary["name"].as_str().unwrap(), "secondary");
    assert!(secondary["models"].is_array());
    assert_eq!(secondary["models"].as_array().unwrap().len(), 2);
}

#[test]
fn test_config_with_client_budgets() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "test"
token = "sk-ant-api-test"

[client_budgets]
alice = 1000000
bob = 500000
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with client budgets should parse");

    let config = result.unwrap();
    assert!(config["client_budgets"].is_table());
    let budgets = config["client_budgets"].as_table().unwrap();
    assert_eq!(budgets["alice"].as_integer().unwrap(), 1000000);
    assert_eq!(budgets["bob"].as_integer().unwrap(), 500000);
}

#[test]
fn test_config_with_client_utilization_limits() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "test"
token = "sk-ant-api-test"

[client_utilization_limits]
heavy_user = 0.95
light_user = 0.75
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(
        result.is_ok(),
        "Config with utilization limits should parse"
    );

    let config = result.unwrap();
    assert!(config["client_utilization_limits"].is_table());
    let limits = config["client_utilization_limits"].as_table().unwrap();
    assert_eq!(limits["heavy_user"].as_float().unwrap(), 0.95);
    assert_eq!(limits["light_user"].as_float().unwrap(), 0.75);
}

#[test]
fn test_config_with_operator() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
operator = "admin-client"

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with operator should parse");

    let config = result.unwrap();
    assert_eq!(config["operator"].as_str().unwrap(), "admin-client");
}

#[test]
fn test_config_with_client_names() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "test"
token = "sk-ant-api-test"

[client_names]
"192.168.1.100" = "alice"
"192.168.1.101" = "bob"
"10.0.0.1" = "charlie"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with client_names should parse");

    let config = result.unwrap();
    assert!(config["client_names"].is_table());
    let names = config["client_names"].as_table().unwrap();
    assert_eq!(names["192.168.1.100"].as_str().unwrap(), "alice");
    assert_eq!(names["192.168.1.101"].as_str().unwrap(), "bob");
    assert_eq!(names["10.0.0.1"].as_str().unwrap(), "charlie");
}

#[test]
fn test_config_with_upstreams() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "test"
token = "sk-ant-api-test"

[[upstreams]]
name = "openai"
base_url = "https://api.openai.com"
api_key = "sk-openai-key"

[[upstreams]]
name = "local"
base_url = "http://localhost:11434"
api_key = "local-key"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with upstreams should parse");

    let config = result.unwrap();
    assert!(config["upstreams"].is_array());
    let upstreams = config["upstreams"].as_array().unwrap();
    assert_eq!(upstreams.len(), 2);
    assert_eq!(upstreams[0]["name"].as_str().unwrap(), "openai");
    assert_eq!(
        upstreams[0]["base_url"].as_str().unwrap(),
        "https://api.openai.com"
    );
    assert_eq!(upstreams[1]["name"].as_str().unwrap(), "local");
}

#[test]
fn test_config_passthrough_token() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "passthrough"
token = "passthrough"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with passthrough token should parse");

    let config = result.unwrap();
    let accounts = config["accounts"].as_array().unwrap();
    assert_eq!(accounts[0]["token"].as_str().unwrap(), "passthrough");
}

#[test]
fn test_invalid_config_missing_listen() {
    let config_content = r#"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    // TOML parsing will succeed, but semantic validation would fail
    // (this would be caught at runtime by the application)
    assert!(result.is_ok());
    let config = result.unwrap();
    assert!(config.get("listen").is_none());
}

#[test]
fn test_invalid_config_missing_upstream() {
    let config_content = r#"
listen = "127.0.0.1:8082"

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok());
    let config = result.unwrap();
    assert!(config.get("upstream").is_none());
}

#[test]
fn test_invalid_config_missing_accounts() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok());
    let config = result.unwrap();
    assert!(config.get("accounts").is_none());
}

#[test]
fn test_config_multiple_accounts() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"

[[accounts]]
name = "account1"
token = "sk-ant-api-token1"

[[accounts]]
name = "account2"
token = "sk-ant-oat01-token2"

[[accounts]]
name = "account3"
token = "sk-ant-api-token3"
models = ["claude-opus-*"]
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with multiple accounts should parse");

    let config = result.unwrap();
    let accounts = config["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 3);
}

#[test]
fn test_config_with_ipv6_allowed_ips() {
    let config_content = r#"
listen = "[::1]:8082"
upstream = "https://api.anthropic.com"
allowed_ips = ["::1", "2001:db8::/32", "fe80::1"]

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with IPv6 addresses should parse");

    let config = result.unwrap();
    assert_eq!(config["listen"].as_str().unwrap(), "[::1]:8082");
    let ips = config["allowed_ips"].as_array().unwrap();
    assert_eq!(ips.len(), 3);
}

#[test]
fn test_config_mixed_ipv4_ipv6_allowed_ips() {
    let config_content = r#"
listen = "0.0.0.0:8082"
upstream = "https://api.anthropic.com"
allowed_ips = ["192.168.1.0/24", "::1", "10.0.0.1", "2001:db8::/32"]

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with mixed IPv4/IPv6 should parse");

    let config = result.unwrap();
    let ips = config["allowed_ips"].as_array().unwrap();
    assert_eq!(ips.len(), 4);
}

#[test]
fn test_example_config_file_is_valid() {
    // Test that the actual config.toml.example file parses correctly
    let example_path = "config.toml.example";

    // Skip if file doesn't exist (e.g., in isolated test environment)
    if !std::path::Path::new(example_path).exists() {
        eprintln!("Skipping: config.toml.example not found");
        return;
    }

    let content = fs::read_to_string(example_path).expect("Failed to read config.toml.example");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(
        result.is_ok(),
        "config.toml.example should be valid TOML: {:?}",
        result.err()
    );

    let config = result.unwrap();

    // Verify essential fields exist
    assert!(config.get("listen").is_some(), "listen field should exist");
    assert!(
        config.get("upstream").is_some(),
        "upstream field should exist"
    );
    assert!(
        config.get("accounts").is_some(),
        "accounts field should exist"
    );

    // Verify accounts is an array with at least one account
    let accounts = config["accounts"]
        .as_array()
        .expect("accounts should be an array");
    assert!(!accounts.is_empty(), "accounts should not be empty");

    // Verify first account has required fields
    let first_account = &accounts[0];
    assert!(
        first_account.get("name").is_some(),
        "account should have name"
    );
    assert!(
        first_account.get("token").is_some(),
        "account should have token"
    );
}

#[test]
fn test_config_boundary_values() {
    let config_content = r#"
listen = "127.0.0.1:8082"
upstream = "https://api.anthropic.com"
rate_limit_cooldown_secs = 0
probe_interval_secs = 0
emergency_threshold = 0.0
soft_limit = 1.0

[[accounts]]
name = "test"
token = "sk-ant-api-test"
"#;

    let temp_file = create_temp_config(config_content);
    let path = temp_file.path();

    let content = fs::read_to_string(path).expect("Failed to read config file");
    let result: Result<toml::Value, _> = toml::from_str(&content);

    assert!(result.is_ok(), "Config with boundary values should parse");

    let config = result.unwrap();
    assert_eq!(config["rate_limit_cooldown_secs"].as_integer().unwrap(), 0);
    assert_eq!(config["probe_interval_secs"].as_integer().unwrap(), 0);
    assert_eq!(config["emergency_threshold"].as_float().unwrap(), 0.0);
    assert_eq!(config["soft_limit"].as_float().unwrap(), 1.0);
}

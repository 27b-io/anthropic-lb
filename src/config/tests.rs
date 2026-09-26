use super::*;

#[test]
fn protocol_deserializes_lowercase_strings() {
    let p: Protocol = serde_json::from_str(r#""anthropic""#).unwrap();
    assert_eq!(p, Protocol::Anthropic);
    let p: Protocol = serde_json::from_str(r#""openai""#).unwrap();
    assert_eq!(p, Protocol::OpenAI);
}

#[test]
fn protocol_default_is_anthropic() {
    assert_eq!(Protocol::default(), Protocol::Anthropic);
    assert_ne!(Protocol::default(), Protocol::OpenAI);
}

#[test]
fn endpoint_config_parses_minimal_anthropic_block() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"
accounts = []

[[endpoints]]
name = "primary"
token = "sk-ant-test"
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.endpoints.len(), 1);
    assert_eq!(cfg.endpoints[0].name, "primary");
    assert_eq!(cfg.endpoints[0].protocol, Protocol::Anthropic);
    assert_eq!(cfg.endpoints[0].base_url, None);
    assert_eq!(cfg.endpoints[0].token, "sk-ant-test");
    assert!(cfg.endpoints[0].models.is_empty());
    assert_eq!(cfg.endpoints[0].priority, 0);
}

#[test]
fn endpoint_config_parses_openai_with_base_url() {
    let toml_str = r#"
listen = "0.0.0.0:8080"
upstream = "https://api.anthropic.com"
accounts = []

[[endpoints]]
name = "gateway"
protocol = "openai"
base_url = "https://gateway.example.com"
token = "sk-test"
priority = 100
models = ["claude-opus-*"]
"#;
    let cfg: Config = toml::from_str(toml_str).unwrap();
    let ep = &cfg.endpoints[0];
    assert_eq!(ep.protocol, Protocol::OpenAI);
    assert_eq!(ep.base_url.as_deref(), Some("https://gateway.example.com"));
    assert_eq!(ep.priority, 100);
    assert_eq!(ep.models, vec!["claude-opus-*".to_string()]);
}

// ── Config deserialization (real struct, not toml::Value) ─────

#[test]
fn config_deser_minimal() {
    let toml = r#"
listen = "127.0.0.1:8082"

[[endpoints]]
name = "primary"
token = "sk-ant-api-test"
"#;
    let cfg: Config = toml::from_str(toml).expect("minimal config should deserialize");
    assert_eq!(cfg.listen, "127.0.0.1:8082");
    assert_eq!(cfg.endpoints.len(), 1);
    assert_eq!(cfg.endpoints[0].name, "primary");
    // Optional fields absent
    assert!(cfg.proxy_key.is_none());
    assert!(cfg.redis_url.is_none());
    assert!(cfg.operators.is_empty());
    assert!(cfg.emergency_threshold.is_none());
    assert!(cfg.soft_limit.is_none());
    assert!(cfg.client_budgets.is_empty());
    assert!(cfg.client_utilization_limits.is_empty());
    assert!(cfg.client_names.is_empty());
}

#[test]
fn config_deser_all_optional_fields() {
    let toml = r#"
listen = "0.0.0.0:8082"
strategy = "dynamic-capacity"
rate_limit_cooldown_secs = 120
probe_interval_secs = 600
proxy_key = "secret"
allowed_ips = ["10.0.0.0/8", "192.168.1.1"]
auto_cache = false
shadow_log = "/tmp/shadow.jsonl"
operators = ["ray", "openclaw"]
emergency_threshold = 0.90
soft_limit = 0.85
redis_url = "redis://10.0.0.5:6379"

[client_names]
"10.0.0.1" = "alice"
"10.0.0.2" = "bob"

[client_budgets]
alice = 1000000
bob = 500000

[client_utilization_limits]
alice = 0.95
bob = 0.80

[[endpoints]]
name = "acct-a"
token = "sk-ant-oat01-token1"

[[endpoints]]
name = "acct-b"
token = "sk-ant-api-token2"
models = ["claude-opus-*", "claude-sonnet-4-6"]

[[endpoints]]
name = "openai"
protocol = "openai"
base_url = "https://api.openai.com"
token = "sk-openai-key"
priority = 100
"#;
    let cfg: Config = toml::from_str(toml).expect("full config should deserialize");
    assert_eq!(cfg.proxy_key.as_deref(), Some("secret"));
    assert_eq!(cfg.allowed_ips.as_ref().unwrap().len(), 2);
    assert_eq!(cfg.auto_cache, Some(false));
    assert_eq!(cfg.shadow_log.as_deref(), Some("/tmp/shadow.jsonl"));
    assert_eq!(cfg.operators, vec!["ray", "openclaw"]);
    assert_eq!(cfg.emergency_threshold, Some(0.90));
    assert_eq!(cfg.soft_limit, Some(0.85));
    assert_eq!(cfg.redis_url.as_deref(), Some("redis://10.0.0.5:6379"));
    assert_eq!(cfg.rate_limit_cooldown_secs, Some(120));
    assert_eq!(cfg.probe_interval_secs, Some(600));
    // Maps
    assert_eq!(cfg.client_names.get("10.0.0.1").unwrap(), "alice");
    assert_eq!(*cfg.client_budgets.get("alice").unwrap(), 1000000u64);
    assert_eq!(*cfg.client_utilization_limits.get("alice").unwrap(), 0.95);
    // Endpoints
    assert_eq!(cfg.endpoints.len(), 3);
    assert_eq!(cfg.endpoints[1].models.len(), 2);
    assert_eq!(cfg.endpoints[1].models[0], "claude-opus-*");
    assert_eq!(cfg.endpoints[2].protocol, Protocol::OpenAI);
    assert_eq!(cfg.endpoints[2].priority, 100);
}

#[test]
fn routing_strategy_parses_aliases() {
    assert_eq!(
        RoutingStrategy::parse(None).unwrap(),
        RoutingStrategy::DynamicCapacityV1
    );
    assert_eq!(
        RoutingStrategy::parse(Some("dynamic-capacity")).unwrap(),
        RoutingStrategy::DynamicCapacityV1
    );
    assert_eq!(
        RoutingStrategy::parse(Some("dynamic-capacity-v1")).unwrap(),
        RoutingStrategy::DynamicCapacityV1
    );
    assert_eq!(
        RoutingStrategy::parse(Some("sticky-weighted")).unwrap(),
        RoutingStrategy::StickyWeightedV2
    );
    assert_eq!(
        RoutingStrategy::parse(Some("sticky-weighted-v2")).unwrap(),
        RoutingStrategy::StickyWeightedV2
    );
    assert!(RoutingStrategy::parse(Some("bogus")).is_err());
}

#[test]
fn config_deser_missing_required_field_fails() {
    // Missing `listen` (the sole required top-level key).
    let toml = r#"
[[endpoints]]
name = "test"
token = "sk-ant-api-test"
"#;
    let result = toml::from_str::<Config>(toml);
    assert!(
        result.is_err(),
        "missing listen should fail deserialization"
    );
}

#[test]
fn config_deser_endpoints_default_empty() {
    // `endpoints` defaults to an empty vec at the deserialization layer;
    // the non-empty requirement is enforced in `main()`, not by serde.
    let toml = r#"
listen = "127.0.0.1:8082"
"#;
    let cfg: Config = toml::from_str(toml).expect("config without endpoints deserializes");
    assert!(cfg.endpoints.is_empty());
}

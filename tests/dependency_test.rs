// Tests to verify dependencies and Cargo.lock integrity

#[test]
fn test_cargo_lock_exists() {
    let cargo_lock_path = std::path::Path::new("Cargo.lock");
    assert!(
        cargo_lock_path.exists(),
        "Cargo.lock should exist for reproducible builds"
    );
}

#[test]
fn test_cargo_toml_exists() {
    let cargo_toml_path = std::path::Path::new("Cargo.toml");
    assert!(
        cargo_toml_path.exists(),
        "Cargo.toml should exist"
    );
}

#[test]
fn test_cargo_toml_has_required_metadata() {
    use std::fs;

    let cargo_toml = fs::read_to_string("Cargo.toml")
        .expect("Failed to read Cargo.toml");

    // Check for required package metadata
    assert!(cargo_toml.contains("name"), "Cargo.toml should have package name");
    assert!(cargo_toml.contains("version"), "Cargo.toml should have version");
    assert!(cargo_toml.contains("[dependencies]"), "Cargo.toml should have dependencies section");
}

#[test]
fn test_required_dependencies_present() {
    use std::fs;

    let cargo_toml = fs::read_to_string("Cargo.toml")
        .expect("Failed to read Cargo.toml");

    // Essential dependencies for the load balancer
    let required_deps = vec![
        "axum",
        "tokio",
        "reqwest",
        "serde",
        "serde_json",
        "toml",
        "tracing",
        "ipnet",
    ];

    for dep in required_deps {
        assert!(
            cargo_toml.contains(dep),
            "Cargo.toml should include dependency: {}",
            dep
        );
    }
}

#[test]
fn test_cargo_lock_is_valid_toml() {
    use std::fs;

    let cargo_lock = fs::read_to_string("Cargo.lock")
        .expect("Failed to read Cargo.lock");

    let result: Result<toml::Value, _> = toml::from_str(&cargo_lock);

    assert!(
        result.is_ok(),
        "Cargo.lock should be valid TOML: {:?}",
        result.err()
    );
}

#[test]
fn test_cargo_lock_has_package_entries() {
    use std::fs;

    let cargo_lock = fs::read_to_string("Cargo.lock")
        .expect("Failed to read Cargo.lock");

    let lock: toml::Value = toml::from_str(&cargo_lock)
        .expect("Cargo.lock should be valid TOML");

    assert!(
        lock.get("package").is_some(),
        "Cargo.lock should have package entries"
    );

    let packages = lock["package"].as_array()
        .expect("package should be an array");

    assert!(
        !packages.is_empty(),
        "Cargo.lock should contain at least one package"
    );
}

#[test]
fn test_cargo_lock_contains_anthropic_lb() {
    use std::fs;

    let cargo_lock = fs::read_to_string("Cargo.lock")
        .expect("Failed to read Cargo.lock");

    let lock: toml::Value = toml::from_str(&cargo_lock)
        .expect("Cargo.lock should be valid TOML");

    let packages = lock["package"].as_array()
        .expect("package should be an array");

    let has_anthropic_lb = packages.iter().any(|pkg| {
        pkg.get("name")
            .and_then(|n| n.as_str())
            .map(|name| name == "anthropic-lb")
            .unwrap_or(false)
    });

    assert!(
        has_anthropic_lb,
        "Cargo.lock should contain anthropic-lb package"
    );
}

#[test]
fn test_cargo_lock_version_format() {
    use std::fs;

    let cargo_lock = fs::read_to_string("Cargo.lock")
        .expect("Failed to read Cargo.lock");

    let lock: toml::Value = toml::from_str(&cargo_lock)
        .expect("Cargo.lock should be valid TOML");

    let version = lock.get("version")
        .expect("Cargo.lock should have version field");

    assert!(
        version.is_integer() || version.is_str(),
        "Cargo.lock version should be integer or string"
    );
}

#[test]
fn test_security_sensitive_dependencies() {
    use std::fs;

    let cargo_toml = fs::read_to_string("Cargo.toml")
        .expect("Failed to read Cargo.toml");

    // These crates handle security-sensitive operations
    let security_deps = vec![
        "reqwest",  // HTTP client
        "tokio",    // Async runtime
        "axum",     // Web framework
    ];

    for dep in security_deps {
        assert!(
            cargo_toml.contains(dep),
            "Security-critical dependency should be present: {}",
            dep
        );
    }
}

#[test]
fn test_no_known_vulnerable_patterns() {
    use std::fs;

    let cargo_toml = fs::read_to_string("Cargo.toml")
        .expect("Failed to read Cargo.toml");

    // Check for patterns that might indicate outdated or vulnerable deps
    // This is a basic check; real security audits should use cargo-audit

    // Ensure tokio has full features (important for async operations)
    assert!(
        cargo_toml.contains("tokio") && cargo_toml.contains("full"),
        "tokio should be configured with full features for comprehensive async support"
    );
}

#[test]
fn test_package_metadata_consistency() {
    use std::fs;

    let cargo_toml = fs::read_to_string("Cargo.toml")
        .expect("Failed to read Cargo.toml");

    let toml: toml::Value = toml::from_str(&cargo_toml)
        .expect("Cargo.toml should be valid TOML");

    let package = toml.get("package")
        .expect("Cargo.toml should have [package] section");

    // Verify metadata fields
    assert!(
        package.get("name").is_some(),
        "Package should have name"
    );
    assert!(
        package.get("version").is_some(),
        "Package should have version"
    );
    assert!(
        package.get("edition").is_some(),
        "Package should specify Rust edition"
    );
}

#[test]
fn test_dependencies_have_versions() {
    use std::fs;

    let cargo_lock = fs::read_to_string("Cargo.lock")
        .expect("Failed to read Cargo.lock");

    let lock: toml::Value = toml::from_str(&cargo_lock)
        .expect("Cargo.lock should be valid TOML");

    let packages = lock["package"].as_array()
        .expect("package should be an array");

    for pkg in packages {
        let name = pkg.get("name")
            .and_then(|n| n.as_str())
            .expect("Package should have name");

        let version = pkg.get("version")
            .and_then(|v| v.as_str());

        assert!(
            version.is_some(),
            "Package {} should have version in Cargo.lock",
            name
        );
    }
}
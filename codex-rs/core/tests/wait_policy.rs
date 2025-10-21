use codex_core::config::WaitPolicyOverrides;
use codex_core::config::WaitPolicySettings;
use codex_core::config::WaitPolicyToml;
use codex_core::config::WaitPredicateKind;
use std::time::Duration;

#[test]
fn wait_policy_from_toml_rejects_unknown_predicate() {
    let toml = WaitPolicyToml {
        allowed_predicates: Some(vec!["timer".into(), "bogus".into()]),
        ..WaitPolicyToml::default()
    };
    let err = WaitPolicySettings::from_toml(Some(&toml)).expect_err("invalid predicate should fail");
    assert!(
        err.contains("bogus"),
        "error should mention the offending predicate: {err}"
    );
}

#[test]
fn wait_policy_override_enforces_limits() {
    let mut settings = WaitPolicySettings::default();
    let mut overrides = WaitPolicyOverrides::default();
    overrides.allowed_predicates = Some(vec![WaitPredicateKind::Timer]);
    overrides.max_duration_seconds = Some(5);
    overrides.max_waits_per_turn = Some(2);
    overrides.require_shell_approval = Some(true);

    settings
        .apply_override(&overrides)
        .expect("valid overrides should apply");

    assert!(settings.is_allowed(WaitPredicateKind::Timer));
    assert!(!settings.is_allowed(WaitPredicateKind::Shell));
    assert_eq!(settings.max_duration(), Duration::from_secs(5));
    assert_eq!(settings.max_waits_per_turn(), 2);
    assert!(settings.require_shell_approval());
}

#[test]
fn wait_policy_override_rejects_zero_limits() {
    let mut settings = WaitPolicySettings::default();
    let mut overrides = WaitPolicyOverrides::default();
    overrides.max_duration_seconds = Some(0);
    assert!(settings.apply_override(&overrides).is_err());

    overrides.max_duration_seconds = None;
    overrides.max_waits_per_turn = Some(0);
    assert!(settings.apply_override(&overrides).is_err());
}

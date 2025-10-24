use std::sync::Mutex;
use std::sync::OnceLock;

use codex_core::MailboxDispatcherClient;
use codex_core::mailbox_feature_enabled;
use tempfile::TempDir;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

struct EnvVar {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVar {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    fn unset(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, previous }
    }
}

impl Drop for EnvVar {
    fn drop(&mut self) {
        if let Some(prev) = &self.previous {
            unsafe {
                std::env::set_var(self.key, prev);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

#[test]
fn mailbox_feature_enabled_detects_dispatcher_via_env() {
    let _env_lock = env_guard();
    let temp_home = TempDir::new().expect("temp dir");
    let home_path = temp_home.path().to_str().unwrap();
    let dispatcher_dir = temp_home.path().join("detect").join("mailbox");
    std::fs::create_dir_all(&dispatcher_dir).expect("create mailbox dir");
    let socket_path = dispatcher_dir.join("dispatcher.sock");
    std::fs::write(&socket_path, b"stub").expect("write dispatcher stub");

    let _home_guard = EnvVar::set("CODEX_HOME", home_path);
    let _ns_guard = EnvVar::set("CODEX_NAMESPACE", "detect");
    let _oob = EnvVar::unset("CODEX_MAILBOX_OOB");
    // Force-disable once to confirm baseline behaviour, then clear to allow detection.
    {
        let _force_disable = EnvVar::set("CODEX_MAILBOX_OOB_FORCE", "0");
        assert!(
            !mailbox_feature_enabled(),
            "mailbox feature should respect CODEX_MAILBOX_OOB_FORCE=0"
        );
    }

    // Dispatcher should be detected via socket presence even without explicit enable flag.
    let dispatcher = MailboxDispatcherClient::from_env()
        .expect("dispatcher should be detected when socket exists");
    assert_eq!(
        dispatcher.endpoint(),
        socket_path.as_path(),
        "dispatcher endpoint should resolve to default socket path"
    );
    assert!(
        mailbox_feature_enabled(),
        "mailbox feature should enable automatically when dispatcher is available"
    );
}

#[test]
fn mailbox_feature_respects_force_disable_even_with_dispatcher() {
    let _env_lock = env_guard();
    let temp_home = TempDir::new().expect("temp dir");
    let home_path = temp_home.path().to_str().unwrap();
    let dispatcher_dir = temp_home.path().join("force").join("mailbox");
    std::fs::create_dir_all(&dispatcher_dir).expect("create mailbox dir");
    let socket_path = dispatcher_dir.join("dispatcher.sock");
    std::fs::write(&socket_path, b"stub").expect("write dispatcher stub");

    let _home_guard = EnvVar::set("CODEX_HOME", home_path);
    let _ns_guard = EnvVar::set("CODEX_NAMESPACE", "force");
    let _enable = EnvVar::set("CODEX_MAIL_SERVER_ENABLE", "1");
    let _server_force = EnvVar::set("CODEX_MAIL_SERVER_FORCE", "0");
    let _force_disable = EnvVar::set("CODEX_MAILBOX_OOB_FORCE", "0");

    assert!(
        MailboxDispatcherClient::from_env().is_none(),
        "dispatcher client should honour CODEX_MAIL_SERVER_FORCE=0"
    );
    assert!(
        !mailbox_feature_enabled(),
        "mailbox feature should remain disabled when forced off"
    );
}

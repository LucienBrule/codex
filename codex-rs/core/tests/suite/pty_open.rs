#![cfg(not(target_os = "windows"))]

use anyhow::Result;
use codex_core::protocol::AskForApproval;
use codex_core::protocol::EventMsg;
use codex_core::protocol::InputItem;
use codex_core::protocol::Op;
use codex_core::protocol::SandboxPolicy;
use codex_protocol::config_types::ReasoningSummary;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use serde_json::Value;
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixListener;
use tokio::time::sleep;
use serial_test::serial;

struct EnvGuard {
    key: &'static str,
}

impl EnvGuard {
    fn set(key: &'static str, value: &std::ffi::OsStr) -> Self {
        // SAFETY: tests serialize access to env vars via the Codex harness.
        unsafe { std::env::set_var(key, value) };
        Self { key }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe { std::env::remove_var(self.key) };
    }
}

fn spawn_vm_pty_server(
    socket_path: std::path::PathBuf,
    response: serde_json::Value,
    captured: std::sync::Arc<tokio::sync::Mutex<Option<serde_json::Value>>>,
) -> tokio::task::JoinHandle<()> {
    use std::io::Write as StdWrite;
    use std::os::unix::net::UnixListener as StdUnixListener;

    let std_listener = StdUnixListener::bind(&socket_path).expect("bind vm-pty socket");
    std_listener
        .set_nonblocking(true)
        .expect("set nonblocking");
    let listener = UnixListener::from_std(std_listener).expect("convert listener");

    tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_err() {
                break;
            }

            let req_json: serde_json::Value = match serde_json::from_str(line.trim_end()) {
                Ok(v) => v,
                Err(_) => break,
            };
            {
                let mut guard = captured.lock().await;
                *guard = Some(req_json.clone());
            }

            let mut stream = reader.into_inner();
            // Respond with an error for pty_attach so pty_open path is exercised.
            let resp_body = if req_json
                .get("action")
                .and_then(|v| v.as_str())
                .map(|s| s == "pty_attach")
                .unwrap_or(false)
            {
                serde_json::json!({
                    "id": req_json.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                    "status": "error",
                    "error": {"code": "E_NO_ATTACH", "message": "attach disabled"}
                })
            } else {
                let mut response_json = response.clone();
                if let Some(request_id) = req_json.get("id").and_then(|v| v.as_str()) {
                    response_json["id"] = serde_json::Value::String(request_id.to_string());
                }
                response_json
            };

            let mut payload = resp_body.to_string().into_bytes();
            payload.push(b'\n');
            let _ = stream.write_all(&payload).await;
            let _ = stream.flush().await;
        }
    })
}

async fn wait_for_socket(path: &Path) {
    for _ in 0..200 {
        if path.exists() {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("socket {path:?} did not become available");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pty_open_returns_session_details() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir.path().join("vm-pty.sock");
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));

    let _server_task = spawn_vm_pty_server(
        socket_path.clone(),
        serde_json::json!({
            "status": "ok",
            "result": {
                "session_id": "session-123",
                "initial_output": "welcome to vm\n",
                "cols": 100,
                "rows": 42
            }
        }),
        captured.clone(),
    );

    println!("socket exists immediately: {}", socket_path.exists());
    sleep(Duration::from_millis(50)).await;
    wait_for_socket(&socket_path).await;

    let mock_server = start_mock_server().await;

    let call_id = "pty-open-call";
    let arguments = serde_json::json!({
        "vmId": "vm-001",
        "cwd": "/workspace",
        "shell": "/bin/bash"
    })
    .to_string();

    let first_response = sse(vec![
        ev_response_created("resp-1"),
        ev_function_call(call_id, "pty_open", &arguments),
        ev_completed("resp-1"),
    ]);
    mount_sse_once_match(&mock_server, wiremock::matchers::any(), first_response).await;

    let second_response = sse(vec![
        ev_assistant_message("msg-1", "opened"),
        ev_completed("resp-2"),
    ]);
    let response_mock = mount_sse_once_match(&mock_server, wiremock::matchers::any(), second_response).await;

    let mut builder = test_codex().with_config(move |config| {
        config.include_vm_pty_tool = true;
        config.include_vm_pty_open_tool = true;
        config.vm_pty_socket = Some(socket_path.clone());
    });
    let test = builder.build(&mock_server).await?;

    test.codex
        .submit(Op::UserTurn {
            items: vec![InputItem::Text {
                text: "open a vm session".into(),
            }],
            final_output_json_schema: None,
            cwd: test.cwd.path().to_path_buf(),
            approval_policy: AskForApproval::Never,
            sandbox_policy: SandboxPolicy::DangerFullAccess,
            model: test.session_configured.model.clone(),
            effort: None,
            summary: ReasoningSummary::Auto,
        })
        .await?;

    wait_for_event(&test.codex, |event| matches!(event, EventMsg::TaskComplete(_))).await;


    let request_body = response_mock.single_request();
    let output_item = request_body.function_call_output(call_id);
    let output_text = output_item
        .get("output")
        .and_then(Value::as_str)
        .expect("function output string");
    println!("output_text: {}", output_text);
    let parsed_output: Value = serde_json::from_str(output_text)?;
    assert_eq!(parsed_output["session_id"], "session-123");
    assert_eq!(parsed_output["initial_output"], "welcome to vm\n");
    assert_eq!(parsed_output["cols"], 100);
    assert_eq!(parsed_output["rows"], 42);

    let recorded_request = captured.lock().await.clone().expect("captured request");
    assert_eq!(recorded_request["action"], "pty_open");
    assert_eq!(recorded_request["payload"]["vm_id"], "vm-001");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pty_open_normalizes_empty_strings_to_defaults() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir.path().join("vm-pty.sock");
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));

    let _server_task = spawn_vm_pty_server(
        socket_path.clone(),
        serde_json::json!({
            "status": "ok",
            "result": {
                "session_id": "session-xyz",
                "initial_output": "",
                "cols": 80,
                "rows": 24
            }
        }),
        captured.clone(),
    );

    sleep(Duration::from_millis(50)).await;
    wait_for_socket(&socket_path).await;

    let mock_server = start_mock_server().await;

    let call_id = "pty-open-empty-normalize";
    let arguments = serde_json::json!({
        "vmId": "",
        "workspace": "",
        "cwd": "",
        "shell": ""
    })
    .to_string();

    let first_response = sse(vec![
        ev_response_created("resp-norm-1"),
        ev_function_call(call_id, "pty_open", &arguments),
        ev_completed("resp-norm-1"),
    ]);
    mount_sse_once_match(&mock_server, wiremock::matchers::any(), first_response).await;

    let second_response = sse(vec![
        ev_assistant_message("msg-norm", "opened"),
        ev_completed("resp-norm-2"),
    ]);
    let _response_mock = mount_sse_once_match(&mock_server, wiremock::matchers::any(), second_response).await;

    let mut builder = test_codex().with_config(move |config| {
        config.include_vm_pty_tool = true;
        config.include_vm_pty_open_tool = true;
        config.vm_pty_socket = Some(socket_path.clone());
    });
    let test = builder.build(&mock_server).await?;

    let turn_cwd_str = test.cwd.path().to_string_lossy().to_string();

    test.codex
        .submit(Op::UserTurn {
            items: vec![InputItem::Text {
                text: "open a vm session with empty params".into(),
            }],
            final_output_json_schema: None,
            cwd: test.cwd.path().to_path_buf(),
            approval_policy: AskForApproval::Never,
            sandbox_policy: SandboxPolicy::DangerFullAccess,
            model: test.session_configured.model.clone(),
            effort: None,
            summary: ReasoningSummary::Auto,
        })
        .await?;

    wait_for_event(&test.codex, |event| matches!(event, EventMsg::TaskComplete(_))).await;

    let recorded_request = captured.lock().await.clone().expect("captured request");
    assert_eq!(recorded_request["action"], "pty_open");
    assert_eq!(recorded_request["payload"]["shell"], "/bin/bash -i");
    assert_eq!(recorded_request["payload"]["workspace"], turn_cwd_str);
    assert_eq!(recorded_request["payload"]["cwd"], turn_cwd_str);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pty_open_reports_invalid_vm_error() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir.path().join("vm-pty.sock");
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));

    let _server_task = spawn_vm_pty_server(
        socket_path.clone(),
        serde_json::json!({
            "status": "error",
            "error": {
                "code": "E_NO_VM",
                "message": "vm not found"
            }
        }),
        captured.clone(),
    );

    let _env_guard = EnvGuard::set("CODEX_VM_PTY_SOCKET", socket_path.as_os_str());

    sleep(Duration::from_millis(50)).await;
    wait_for_socket(&socket_path).await;

    let mock_server = start_mock_server().await;

    let call_id = "pty-open-error";
    let arguments = serde_json::json!({
        "vmId": "missing-vm"
    })
    .to_string();

    let first_response = sse(vec![
        ev_response_created("resp-err-1"),
        ev_function_call(call_id, "pty_open", &arguments),
        ev_completed("resp-err-1"),
    ]);
    mount_sse_once_match(&mock_server, wiremock::matchers::any(), first_response).await;

    let second_response = sse(vec![
        ev_assistant_message("msg-err", "failed"),
        ev_completed("resp-err-2"),
    ]);
    let response_mock = mount_sse_once_match(&mock_server, wiremock::matchers::any(), second_response).await;

    let mut builder = test_codex().with_config(|config| {
        config.include_vm_pty_tool = true;
        config.include_vm_pty_open_tool = true;
    });
    let test = builder.build(&mock_server).await?;

    test.codex
        .submit(Op::UserTurn {
            items: vec![InputItem::Text {
                text: "try opening missing vm".into(),
            }],
            final_output_json_schema: None,
            cwd: test.cwd.path().to_path_buf(),
            approval_policy: AskForApproval::Never,
            sandbox_policy: SandboxPolicy::DangerFullAccess,
            model: test.session_configured.model.clone(),
            effort: None,
            summary: ReasoningSummary::Auto,
        })
        .await?;

    wait_for_event(&test.codex, |event| matches!(event, EventMsg::TaskComplete(_))).await;


    let request_body = response_mock.single_request();
    let output_item = request_body.function_call_output(call_id);
    let output_text = output_item
        .get("output")
        .and_then(Value::as_str)
        .expect("function output string");
    assert!(output_text.contains("pty request failed (E_NO_VM): vm not found"));

    let recorded_request = captured.lock().await.clone().expect("captured request");
    assert_eq!(recorded_request["payload"]["vm_id"], "missing-vm");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pty_open_enforces_worker_cap_for_auto_provision() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir.path().join("vm-pty.sock");
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));

    // Stub server: allow pty_open, return a session; disable attach so the open path is exercised.
    let _server_task = spawn_vm_pty_server(
        socket_path.clone(),
        serde_json::json!({
            "status": "ok",
            "result": {
                "vm_id": "vm-cap-1",
                "session_id": "sess-1",
                "initial_output": "",
                "cols": 80,
                "rows": 24
            }
        }),
        captured.clone(),
    );

    let _env_guard = EnvGuard::set("CODEX_VM_PTY_SOCKET", socket_path.as_os_str());

    sleep(Duration::from_millis(50)).await;
    wait_for_socket(&socket_path).await;

    let server = start_mock_server().await;

    // Drive two sequential pty_open calls with empty vmId (auto-provision).
    let args_auto = json!({
        "vmId": "",
        "cwd": "/workspace",
        "shell": "/bin/bash"
    })
    .to_string();

    let sse1 = sse(vec![
        ev_response_created("resp-cap-1"),
        ev_function_call("open-1", "pty_open", &args_auto),
        ev_completed("resp-cap-1"),
    ]);

    let sse2 = sse(vec![
        ev_response_created("resp-cap-2"),
        ev_function_call("open-2", "pty_open", &args_auto),
        ev_completed("resp-cap-2"),
    ]);

    let sse3 = sse(vec![
        ev_assistant_message("cap-msg", "done"),
        ev_completed("resp-cap-3"),
    ]);

    let response_mock = mount_sse_sequence(&server, vec![sse1, sse2, sse3]).await;

    // Build Codex with per-worker cap = 1 and vm-pty enabled.
    let mut builder = test_codex().with_config(|config| {
        config.include_vm_pty_tool = true;
        config.include_vm_pty_open_tool = true;
        config.vm_pty_max_concurrent_per_worker = Some(1);
    });
    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserTurn {
            items: vec![InputItem::Text { text: "open twice".into() }],
            final_output_json_schema: None,
            cwd: test.cwd.path().to_path_buf(),
            approval_policy: AskForApproval::Never,
            sandbox_policy: SandboxPolicy::DangerFullAccess,
            model: test.session_configured.model.clone(),
            effort: None,
            summary: ReasoningSummary::Auto,
        })
        .await?;

    wait_for_event(&test.codex, |event| matches!(event, EventMsg::TaskComplete(_))).await;

    // The third outbound request should contain the output for call "open-2" with a policy error.
    let requests = response_mock.requests();
    assert!(requests.len() >= 3, "expected at least 3 response posts");
    let output_item = requests[2].function_call_output("open-2");
    let output_text = output_item
        .get("output")
        .and_then(Value::as_str)
        .expect("function output string for open-2");
    assert!(
        output_text.contains("vm-pty policy exceeded: at most 1 VMs per worker"),
        "expected policy exceeded message, got: {output_text}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn pty_open_attach_first_succeeds_with_cap() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir.path().join("vm-pty.sock");

    // Custom stub: return success for pty_open and success for pty_attach on the same vm_id.
    use std::os::unix::net::UnixListener as StdUnixListener;
    let std_listener = StdUnixListener::bind(&socket_path).expect("bind vm-pty socket");
    std_listener
        .set_nonblocking(true)
        .expect("set nonblocking");
    let listener = UnixListener::from_std(std_listener).expect("convert listener");

    tokio::spawn(async move {
        // Handle two requests: first pty_open, then pty_attach
        for _ in 0..2 {
            let (mut stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_err() {
                break;
            }
            let req_json: serde_json::Value = match serde_json::from_str(line.trim_end()) {
                Ok(v) => v,
                Err(_) => break,
            };

            let action = req_json
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let mut response = if action == "pty_attach" {
                serde_json::json!({
                    "status": "ok",
                    "result": {"session_id": "sess-attach", "cols": 80, "rows": 24}
                })
            } else {
                serde_json::json!({
                    "status": "ok",
                    "result": {"vm_id": "vm-cap-2", "session_id": "sess-open", "initial_output": "", "cols": 80, "rows": 24}
                })
            };
            if let Some(request_id) = req_json.get("id").and_then(|v| v.as_str()) {
                response["id"] = serde_json::Value::String(request_id.to_string());
            }
            let mut stream = reader.into_inner();
            let mut payload = response.to_string().into_bytes();
            payload.push(b'\n');
            let _ = stream.write_all(&payload).await;
            let _ = stream.flush().await;
        }
    });

    let _env_guard = EnvGuard::set("CODEX_VM_PTY_SOCKET", socket_path.as_os_str());
    sleep(Duration::from_millis(50)).await;
    wait_for_socket(&socket_path).await;

    let server = start_mock_server().await;

    // First: open auto-provision to get vm_id=vm-cap-2
    let args_open = json!({ "vmId": "" }).to_string();
    let sse1 = sse(vec![
        ev_response_created("resp-attach-1"),
        ev_function_call("open-first", "pty_open", &args_open),
        ev_completed("resp-attach-1"),
    ]);

    // Second: attach-first to that vm_id should succeed even with cap=1
    let args_attach = json!({ "vmId": "vm-cap-2" }).to_string();
    let sse2 = sse(vec![
        ev_response_created("resp-attach-2"),
        ev_function_call("attach-call", "pty_open", &args_attach),
        ev_completed("resp-attach-2"),
    ]);

    let sse3 = sse(vec![
        ev_assistant_message("attach-msg", "attached"),
        ev_completed("resp-attach-3"),
    ]);

    let response_mock = mount_sse_sequence(&server, vec![sse1, sse2, sse3]).await;

    let mut builder = test_codex().with_config(|config| {
        config.include_vm_pty_tool = true;
        config.include_vm_pty_open_tool = true;
        config.vm_pty_max_concurrent_per_worker = Some(1);
    });
    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserTurn {
            items: vec![InputItem::Text { text: "attach-first".into() }],
            final_output_json_schema: None,
            cwd: test.cwd.path().to_path_buf(),
            approval_policy: AskForApproval::Never,
            sandbox_policy: SandboxPolicy::DangerFullAccess,
            model: test.session_configured.model.clone(),
            effort: None,
            summary: ReasoningSummary::Auto,
        })
        .await?;

    wait_for_event(&test.codex, |event| matches!(event, EventMsg::TaskComplete(_))).await;

    // The third outbound request should contain the output for the attach call with attached: true
    let requests = response_mock.requests();
    assert!(requests.len() >= 3, "expected at least 3 response posts");
    let output_item = requests[2].function_call_output("attach-call");
    let output_text = output_item
        .get("output")
        .and_then(Value::as_str)
        .expect("function output string for attach-call");
    let parsed: Value = serde_json::from_str(output_text)?;
    assert_eq!(parsed["attached"], true);

    Ok(())
}

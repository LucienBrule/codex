#![cfg(not(target_os = "windows"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

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
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use serde_json::json;
use serde_json::Value;

async fn submit_turn(
    test: &TestCodex,
    prompt: &str,
    approval_policy: AskForApproval,
    sandbox_policy: SandboxPolicy,
) -> Result<()> {
    let session_model = test.session_configured.model.clone();

    test.codex
        .submit(Op::UserTurn {
            items: vec![InputItem::Text {
                text: prompt.into(),
            }],
            final_output_json_schema: None,
            cwd: test.cwd.path().to_path_buf(),
            approval_policy,
            sandbox_policy,
            model: session_model,
            effort: None,
            summary: ReasoningSummary::Auto,
        })
        .await?;

    wait_for_event(&test.codex, |event| matches!(event, EventMsg::TaskComplete(_))).await;

    Ok(())
}

fn parse_output(item: &Value) -> Value {
    let raw = item
        .get("output")
        .and_then(Value::as_str)
        .expect("tool output string");
    serde_json::from_str(raw).unwrap_or_else(|_| json!({ "raw": raw }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_timer_predicate_completes() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex();
    let test = builder.build(&server).await?;

    let call_id = "wait-timer";
    let args = json!({
        "type": "timer",
        "predicate": { "duration_ms": 25 },
        "timeout_ms": 250
    })
    .to_string();

    mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("resp-1"),
            ev_function_call(call_id, "codex.wait", &args),
            ev_completed("resp-1"),
        ]),
    )
    .await;
    let second_mock = mount_sse_once(
        &server,
        sse(vec![
            ev_assistant_message("msg-1", "done"),
            ev_completed("resp-2"),
        ]),
    )
    .await;

    submit_turn(
        &test,
        "exercise timer wait",
        AskForApproval::Never,
        SandboxPolicy::DangerFullAccess,
    )
    .await?;

    let output = parse_output(&second_mock.single_request().function_call_output(call_id));
    assert_eq!(output["predicate"], "timer");
    assert_eq!(output["status"], "completed");
    assert!(
        output["elapsed_ms"].as_u64().unwrap_or_default() > 0,
        "elapsed_ms should be populated"
    );
    assert!(output["details"]["duration_ms"].as_u64().is_some());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_timer_respects_timeout() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex();
    let test = builder.build(&server).await?;

    let call_id = "wait-timeout";
    let args = json!({
        "type": "timer",
        "predicate": { "duration_ms": 200 },
        "timeout_ms": 50
    })
    .to_string();

    mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("resp-1"),
            ev_function_call(call_id, "codex.wait", &args),
            ev_completed("resp-1"),
        ]),
    )
    .await;
    let second_mock = mount_sse_once(
        &server,
        sse(vec![
            ev_assistant_message("msg-1", "done"),
            ev_completed("resp-2"),
        ]),
    )
    .await;

    submit_turn(
        &test,
        "exercise wait timeout",
        AskForApproval::Never,
        SandboxPolicy::DangerFullAccess,
    )
    .await?;

    let item = second_mock.single_request().function_call_output(call_id);
    let raw = item
        .get("output")
        .and_then(Value::as_str)
        .expect("timeout output string");
    assert!(
        raw.contains("timed out"),
        "expected timeout message, got: {raw}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_filesystem_invalid_schema_returns_error() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex();
    let test = builder.build(&server).await?;

    let call_id = "wait-invalid";
    let args = json!({
        "type": "filesystem",
        "predicate": { "event": "exists" }
    })
    .to_string();

    mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("resp-1"),
            ev_function_call(call_id, "codex.wait", &args),
            ev_completed("resp-1"),
        ]),
    )
    .await;
    let second_mock = mount_sse_once(
        &server,
        sse(vec![
            ev_assistant_message("msg-1", "done"),
            ev_completed("resp-2"),
        ]),
    )
    .await;

    submit_turn(
        &test,
        "exercise wait schema validation",
        AskForApproval::Never,
        SandboxPolicy::DangerFullAccess,
    )
    .await?;

    let item = second_mock.single_request().function_call_output(call_id);
    let message = item
        .get("output")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        message.contains("invalid filesystem predicate payload")
            || message.contains("path must be absolute"),
        "expected schema error, got: {message}"
    );

    Ok(())
}

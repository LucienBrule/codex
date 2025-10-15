use codex_core::protocol::EventMsg;
use codex_core::protocol::MailboxDeliveryState;
use codex_core::protocol::Op;
use codex_protocol::mailbox::MailboxMessage;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

struct EnvGuard {
    key: &'static str,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        // SAFETY: test scaffolding ensures no concurrent use of this env var while
        // the guard is active.
        unsafe { std::env::set_var(key, value) };
        Self { key }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: test scaffolding ensures no concurrent use of this env var while
        // the guard is active.
        unsafe { std::env::remove_var(self.key) };
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mailbox_envelope_dispatches_while_idle() {
    skip_if_no_network!();

    let _flag_guard = EnvGuard::set("CODEX_MAILBOX_OOB_FORCE", "1");

    let server = MockServer::start().await;
    let sse_body =
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_mailbox\"}}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_raw(sse_body, "text/event-stream"),
        )
        .mount(&server)
        .await;

    let base_url = format!("{}/v1", server.uri());
    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider.base_url = Some(base_url.clone());
            config.model_provider.env_key = Some("PATH".into());
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build(&server)
        .await
        .expect("spawn codex");

    let mut message = MailboxMessage::default();
    message.sender.id = "orchestrator.test".into();
    message.body.content = "idle mailbox dispatch".into();
    let expected_id = message.message_id;

    codex
        .submit(Op::MailboxEnvelope {
            envelope: message.clone(),
        })
        .await
        .expect("enqueue mailbox envelope");

    let enqueued = wait_for_event(&codex, |ev| {
        matches!(
            ev,
            EventMsg::MailboxDelivery(delivery)
                if delivery.state == MailboxDeliveryState::Enqueued
        )
    })
    .await;
    let EventMsg::MailboxDelivery(enqueued_event) = enqueued else {
        unreachable!("expected mailbox delivery event");
    };
    assert_eq!(enqueued_event.message.message_id, expected_id);
    assert_eq!(enqueued_event.queue_depth, Some(1));
    assert!(enqueued_event.observed_at.is_some());

    let delivered = wait_for_event(&codex, |ev| {
        matches!(
            ev,
            EventMsg::MailboxDelivery(delivery)
                if delivery.state == MailboxDeliveryState::Delivered
        )
    })
    .await;
    let EventMsg::MailboxDelivery(delivered_event) = delivered else {
        unreachable!("expected mailbox delivery event");
    };
    assert_eq!(delivered_event.message.message_id, expected_id);
    assert_eq!(delivered_event.queue_depth, Some(0));
    assert!(delivered_event.observed_at.is_some());
}

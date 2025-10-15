use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use codex_core::protocol::EventMsg;
use codex_core::protocol::MailboxDeliveryState;
use codex_core::protocol::Op;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxSenderRole;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event_with_timeout;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::Duration;
use uuid::Uuid;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

struct MailboxEnvGuard {
    key: &'static str,
}

impl MailboxEnvGuard {
    fn new(key: &'static str, value: &str) -> Self {
        // SAFETY: tests serialize access to this env var via guard scope.
        unsafe { std::env::set_var(key, value) };
        Self { key }
    }
}

impl Drop for MailboxEnvGuard {
    fn drop(&mut self) {
        // SAFETY: tests serialize access to this env var via guard scope.
        unsafe { std::env::remove_var(self.key) };
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mailbox_concurrency_preserves_fifo_and_latency() {
    skip_if_no_network!();

    let _guard = MailboxEnvGuard::new("CODEX_MAILBOX_OOB_FORCE", "1");

    let server = MockServer::start().await;
    let sse_body =
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"mailbox_soak\"}}\n\n";
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
    let test_codex = test_codex()
        .with_config(move |config| {
            config.model_provider.base_url = Some(base_url.clone());
            config.model_provider.env_key = Some("PATH".into());
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build(&server)
        .await
        .expect("spawn codex");
    let codex = test_codex.codex;

    let senders: Vec<(&'static str, MailboxSenderRole, usize)> = vec![
        ("orchestrator.alpha", MailboxSenderRole::Orchestrator, 12),
        ("system.beta", MailboxSenderRole::System, 10),
        ("operator.gamma", MailboxSenderRole::Operator, 8),
    ];
    let total_messages: usize = senders.iter().map(|(_, _, count)| *count).sum();

    let expected_order = Arc::new(Mutex::new(HashMap::<String, Vec<Uuid>>::new()));
    let enqueued_events = Arc::new(Mutex::new(
        HashMap::<Uuid, MailboxDeliveryEventSnapshot>::new(),
    ));
    let deliveries = Arc::new(Mutex::new(Vec::<MailboxDeliveryEventSnapshot>::new()));
    let queue_depths = Arc::new(Mutex::new(Vec::<usize>::new()));

    let listener_codex = Arc::clone(&codex);
    let listener_enqueued = Arc::clone(&enqueued_events);
    let listener_deliveries = Arc::clone(&deliveries);
    let listener_depths = Arc::clone(&queue_depths);
    let listener = tokio::spawn(async move {
        let mut delivered = 0usize;
        while delivered < total_messages {
            let event = wait_for_event_with_timeout(
                &listener_codex,
                |ev| matches!(
                    ev,
                    EventMsg::MailboxDelivery(delivery)
                        if matches!(delivery.state, MailboxDeliveryState::Enqueued | MailboxDeliveryState::Delivered)
                ),
                Duration::from_secs(10),
            )
            .await;
            if let EventMsg::MailboxDelivery(event) = event {
                match event.state {
                    MailboxDeliveryState::Enqueued => {
                        if let Some(depth) = event.queue_depth {
                            listener_depths.lock().await.push(depth);
                        }
                        listener_enqueued.lock().await.insert(
                            event.message.message_id,
                            MailboxDeliveryEventSnapshot::from(&event),
                        );
                    }
                    MailboxDeliveryState::Delivered => {
                        listener_deliveries
                            .lock()
                            .await
                            .push(MailboxDeliveryEventSnapshot::from(&event));
                        delivered += 1;
                    }
                }
            }
        }
    });

    let mut sender_handles = Vec::new();
    for (idx, (sender_id_raw, sender_role, message_count)) in senders.iter().cloned().enumerate() {
        let codex = Arc::clone(&codex);
        let expected_order = Arc::clone(&expected_order);
        let sender_id = sender_id_raw.to_string();
        sender_handles.push(tokio::spawn(async move {
            for seq in 0..message_count {
                let mut message = MailboxMessage::default();
                message.sender.id = sender_id.clone();
                message.sender.role = sender_role.clone();
                message.body.content = format!(
                    "[{}] mailbox message {} from {}",
                    seq,
                    sender_role_string(&sender_role),
                    sender_id
                );
                message.audit.request_id = Some(format!("req-{sender_id}-{seq}"));
                if idx == 1 {
                    message.metadata.insert("ingress".into(), json!("script"));
                } else if idx == 2 {
                    message.metadata.insert("ingress".into(), json!("mcp"));
                }

                let message_id = message.message_id;
                expected_order
                    .lock()
                    .await
                    .entry(sender_id.clone())
                    .or_default()
                    .push(message_id);

                codex
                    .submit(Op::MailboxEnvelope { envelope: message })
                    .await
                    .expect("enqueue mailbox envelope");

                // Introduce a small gap to increase interleaving variance across senders.
                let backoff_ms = match (idx + seq) % 3 {
                    0 => 3,
                    1 => 7,
                    _ => 11,
                };
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }));
    }

    for handle in sender_handles {
        handle.await.expect("sender task");
    }

    listener.await.expect("event listener");

    let expected_map = expected_order.lock().await.clone();
    let delivered_events = deliveries.lock().await.clone();
    let enqueued_map = enqueued_events.lock().await.clone();

    assert_eq!(
        delivered_events.len(),
        total_messages,
        "expected all messages to deliver"
    );

    let mut actual_map: HashMap<String, Vec<Uuid>> = HashMap::new();
    let mut ingress_values = HashSet::new();
    let mut latency_ms: Vec<u64> = Vec::new();

    for event in &delivered_events {
        actual_map
            .entry(event.sender_id.clone())
            .or_default()
            .push(event.message_id);

        ingress_values.insert(event.ingress.clone());

        if let Some(latency) = event.delivery_latency_ms {
            latency_ms.push(latency);
            // Guard against starvation by capping acceptable delivery latency.
            assert!(
                latency <= 1_000,
                "latency exceeded 1s for {}",
                event.message_id
            );
        } else if let Some(enqueued) = enqueued_map.get(&event.message_id) {
            if let (Some(start), Some(end)) = (enqueued.observed_at_ms, event.observed_at_ms) {
                let diff = end.saturating_sub(start);
                assert!(
                    diff <= 1_000,
                    "derived latency exceeded 1s for {}",
                    event.message_id
                );
                latency_ms.push(diff);
            }
        }

        if let Some(enqueued) = enqueued_map.get(&event.message_id) {
            assert!(
                enqueued.queue_depth <= total_messages,
                "queue depth {} exceeded total messages",
                enqueued.queue_depth
            );
        }
    }

    for (sender, expected_ids) in &expected_map {
        let actual_ids = actual_map
            .get(sender)
            .unwrap_or_else(|| panic!("missing deliveries for sender {sender}"));
        assert_eq!(actual_ids, expected_ids, "FIFO order mismatch for {sender}");
    }

    assert!(ingress_values.contains("cli"), "expected CLI ingress");
    assert!(ingress_values.contains("script"), "expected script ingress");
    assert!(ingress_values.contains("mcp"), "expected MCP ingress");

    if !latency_ms.is_empty() {
        let max_latency = latency_ms.into_iter().max().unwrap();
        assert!(
            max_latency <= 1_000,
            "max latency {max_latency}ms exceeded bound"
        );
    }

    let depth_samples = queue_depths.lock().await.clone();
    if let Some(max_depth) = depth_samples.into_iter().max() {
        assert!(
            max_depth <= total_messages,
            "queue depth {max_depth} exceeded total message count"
        );
    }
}

#[derive(Clone)]
struct MailboxDeliveryEventSnapshot {
    message_id: Uuid,
    sender_id: String,
    queue_depth: usize,
    observed_at_ms: Option<u64>,
    delivery_latency_ms: Option<u64>,
    ingress: String,
}

impl MailboxDeliveryEventSnapshot {
    fn from(event: &codex_core::protocol::MailboxDeliveryEvent) -> Self {
        let queue_depth = event.queue_depth.unwrap_or_default();
        let observed_at_ms = event
            .observed_at
            .map(|ts| ts.unix_timestamp() as u64 * 1_000 + ts.nanosecond() as u64 / 1_000_000);
        let ingress = event
            .ingress
            .as_ref()
            .map(|i| format!("{i:?}").to_ascii_lowercase())
            .unwrap_or_else(|| "unknown".to_string());
        Self {
            message_id: event.message.message_id,
            sender_id: event.message.sender.id.clone(),
            queue_depth,
            observed_at_ms,
            delivery_latency_ms: event.delivery_latency_ms,
            ingress,
        }
    }
}

fn sender_role_string(role: &MailboxSenderRole) -> &'static str {
    match role {
        MailboxSenderRole::Orchestrator => "orchestrator",
        MailboxSenderRole::Operator => "operator",
        MailboxSenderRole::Automation => "automation",
        MailboxSenderRole::System => "system",
    }
}

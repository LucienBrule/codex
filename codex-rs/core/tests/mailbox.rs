use codex_core::config::MailboxLivenessSettings;
use codex_core::telemetry::MailboxDeliveryTelemetry;
use codex_core::telemetry::MailboxLivenessTelemetry;
use codex_core::telemetry::MailboxTelemetryLabels;
use codex_protocol::mailbox::MailboxAckMode;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxPriority;
use codex_protocol::mailbox::MailboxSenderRole;
use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryIngress;
use codex_protocol::protocol::MailboxDeliveryState;
use codex_protocol::protocol::MailboxLivenessState;
use std::time::Duration;
use std::time::Instant;
use time::OffsetDateTime;
use time::macros::datetime;

fn baseline_settings() -> MailboxLivenessSettings {
    let mut settings = MailboxLivenessSettings::default();
    settings.idle_after = Duration::from_secs(2);
    settings.stalled_after = Duration::from_secs(6);
    settings.emit_interval = Duration::from_secs(3);
    settings
}

#[test]
fn mailbox_emits_on_first_and_interval() {
    let settings = baseline_settings();
    let mut telemetry = MailboxLivenessTelemetry::new(settings);
    let start = Instant::now();
    let observed = OffsetDateTime::now_utc();

    let first = telemetry
        .record(start, observed, Duration::from_secs(1), 0)
        .expect("first heartbeat should emit");
    assert_eq!(first.state, MailboxLivenessState::Active);

    // Second heartbeat within the emit interval should be suppressed.
    assert!(
        telemetry
            .record(
                start + Duration::from_secs(1),
                observed,
                Duration::from_secs(1),
                0
            )
            .is_none()
    );

    // After the emit interval we should see a new snapshot.
    let resumed = telemetry
        .record(
            start + Duration::from_secs(4),
            observed,
            Duration::from_secs(1),
            2,
        )
        .expect("heartbeat after interval emits");
    assert_eq!(resumed.queue_depth, 2);
    assert_eq!(resumed.state, MailboxLivenessState::Active);
}

#[test]
fn mailbox_emits_on_state_transition() {
    let settings = baseline_settings();
    let mut telemetry = MailboxLivenessTelemetry::new(settings);
    let start = Instant::now();
    let observed = OffsetDateTime::now_utc();

    telemetry
        .record(start, observed, Duration::from_secs(1), 0)
        .expect("initial heartbeat");

    // Transition to idle before emit interval should still emit.
    let idle = telemetry
        .record(
            start + Duration::from_secs(1),
            observed,
            Duration::from_secs(3),
            1,
        )
        .expect("idle transition emits");
    assert_eq!(idle.state, MailboxLivenessState::Idle);

    // Transition to stalled state is emitted even inside interval.
    let stalled = telemetry
        .record(
            start + Duration::from_secs(2),
            observed,
            Duration::from_secs(8),
            1,
        )
        .expect("stalled transition emits");
    assert_eq!(stalled.state, MailboxLivenessState::Stalled);

    // Consecutive stalled heartbeat within interval suppresses duplicates.
    assert!(
        telemetry
            .record(
                start + Duration::from_secs(3),
                observed,
                Duration::from_secs(7),
                1,
            )
            .is_none()
    );
}

#[test]
fn mailbox_delivery_telemetry_computes_latency() {
    let mut telemetry = MailboxDeliveryTelemetry::new();

    let mut message = MailboxMessage::default();
    message.priority = MailboxPriority::High;
    message.ack_policy.mode = MailboxAckMode::Passive;
    message.sender.role = MailboxSenderRole::System;

    let enqueued_at = datetime!(2025-10-13 19:30:00 UTC);
    let delivered_at = datetime!(2025-10-13 19:30:05 UTC);

    let enqueued_event = MailboxDeliveryEvent {
        message: message.clone(),
        state: MailboxDeliveryState::Enqueued,
        queue_depth: Some(3),
        observed_at: Some(enqueued_at),
        correlation_id: Some("test-correlation".into()),
        ingress: Some(MailboxDeliveryIngress::Cli),
        delivery_latency_ms: None,
    };
    let enqueued_labels =
        MailboxTelemetryLabels::new(MailboxDeliveryIngress::Cli, &enqueued_event.message);
    let enqueued_snapshot = telemetry.record_enqueued(&enqueued_event, &enqueued_labels);
    assert_eq!(enqueued_snapshot.delivery_latency_ms, None);

    let mut delivered_event = MailboxDeliveryEvent {
        message,
        state: MailboxDeliveryState::Delivered,
        queue_depth: Some(1),
        observed_at: Some(delivered_at),
        correlation_id: Some("test-correlation".into()),
        ingress: Some(MailboxDeliveryIngress::Cli),
        delivery_latency_ms: None,
    };
    let delivered_labels =
        MailboxTelemetryLabels::new(MailboxDeliveryIngress::Cli, &delivered_event.message);
    let delivered_snapshot = telemetry.record_delivered(&mut delivered_event, &delivered_labels);
    assert_eq!(delivered_snapshot.queue_depth, Some(1));
    assert_eq!(delivered_snapshot.delivery_latency_ms, Some(5000));
    assert_eq!(delivered_event.delivery_latency_ms, Some(5000));
}

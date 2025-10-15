use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryIngress;
use codex_protocol::protocol::MailboxDeliveryState;
use serde_json::Value;

const ENQUEUED_FIXTURE: &str = include_str!("fixtures/mailbox_delivery_enqueued.json");

#[test]
fn mailbox_delivery_event_deserializes_fixture() -> anyhow::Result<()> {
    let event: MailboxDeliveryEvent = serde_json::from_str(ENQUEUED_FIXTURE)?;
    assert!(matches!(event.state, MailboxDeliveryState::Enqueued));
    assert_eq!(event.queue_depth, Some(2));
    assert_eq!(event.message.sender.id, "orchestrator.test");
    assert_eq!(event.correlation_id.as_deref(), Some("sub-1/mailbox"));
    assert!(matches!(event.ingress, Some(MailboxDeliveryIngress::Cli)));
    Ok(())
}

#[test]
fn mailbox_delivery_event_round_trip_matches_fixture() -> anyhow::Result<()> {
    let delivery: MailboxDeliveryEvent = serde_json::from_str(ENQUEUED_FIXTURE)?;
    let delivery_json = serde_json::to_value(&delivery)?;
    let fixture_json: Value = serde_json::from_str(ENQUEUED_FIXTURE)?;
    assert_eq!(delivery_json, fixture_json);
    Ok(())
}

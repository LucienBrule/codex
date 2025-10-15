use crate::flags::CODEX_MAILBOX_OOB;

use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::TrySendError;
use codex_protocol::mailbox::MailboxAckMode;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxPriority;
use codex_protocol::mailbox::MailboxRateLimitHint;
use codex_protocol::mailbox::MailboxRateLimitScope;
use codex_protocol::mailbox::MailboxSenderRole;
use time::OffsetDateTime;
use uuid::Uuid;

pub(crate) const MAILBOX_QUEUE_CAPACITY: usize = 64;
pub const DEFAULT_CRITICAL_MAILBOX_CAPACITY: u32 = 6;
pub const DEFAULT_CRITICAL_MAILBOX_INTERVAL_SECONDS: u32 = 300;

const MAILBOX_FORCE_ENV: &str = "CODEX_MAILBOX_OOB_FORCE";

/// Returns whether the mailbox dispatcher is enabled for the current process.
///
/// The `CODEX_MAILBOX_OOB` flag is lazily evaluated once via `env_flags`. Tests
/// and local tooling can override the runtime value at any point by exporting
/// `CODEX_MAILBOX_OOB_FORCE` to `true/false` (case-insensitive) or `1/0`.
pub(crate) fn mailbox_feature_enabled() -> bool {
    if let Ok(raw) = std::env::var(MAILBOX_FORCE_ENV) {
        let lowered = raw.trim().to_ascii_lowercase();
        return match lowered.as_str() {
            "1" | "true" | "on" | "enabled" => true,
            "0" | "false" | "off" | "disabled" => false,
            _ => *CODEX_MAILBOX_OOB,
        };
    }
    *CODEX_MAILBOX_OOB
}

/// Envelope stored in the runtime mailbox queue. Keeps track of the originating
/// submission identifier so acknowledgements can be correlated with the
/// original request.
#[derive(Debug, Clone)]
pub(crate) struct MailboxEnvelope {
    pub submission_id: String,
    pub message: MailboxMessage,
}

impl MailboxEnvelope {
    pub(crate) fn new(submission_id: String, message: MailboxMessage) -> Self {
        Self {
            submission_id,
            message,
        }
    }
}

#[derive(Clone)]
pub(crate) struct MailboxSender {
    inner: Sender<MailboxEnvelope>,
    capacity: usize,
}

impl MailboxSender {
    pub(crate) fn try_enqueue(&self, envelope: MailboxEnvelope) -> Result<usize, TryEnqueueError> {
        match self.inner.try_send(envelope) {
            Ok(()) => Ok(self.inner.len()),
            Err(TrySendError::Full(envelope)) => Err(TryEnqueueError::Full {
                envelope,
                capacity: self.capacity,
            }),
            Err(TrySendError::Closed(envelope)) => Err(TryEnqueueError::Closed(envelope)),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }
}

pub(crate) struct MailboxReceiver {
    inner: Receiver<MailboxEnvelope>,
}

impl MailboxReceiver {
    pub(crate) async fn recv(&self) -> Result<MailboxEnvelope, async_channel::RecvError> {
        self.inner.recv().await
    }
}

#[derive(Debug)]
pub(crate) enum TryEnqueueError {
    Full {
        envelope: MailboxEnvelope,
        capacity: usize,
    },
    Closed(MailboxEnvelope),
}

pub(crate) fn mailbox_channel(capacity: usize) -> (MailboxSender, MailboxReceiver) {
    let (tx, rx) = async_channel::bounded(capacity);
    (
        MailboxSender {
            inner: tx,
            capacity,
        },
        MailboxReceiver { inner: rx },
    )
}

pub fn apply_mailbox_defaults(message: &mut MailboxMessage) {
    if message.posted_at.is_none() {
        message.posted_at = Some(OffsetDateTime::now_utc());
    }

    if matches!(message.priority, MailboxPriority::Critical) && message.rate_limit.is_none() {
        message.rate_limit = Some(MailboxRateLimitHint {
            scope: MailboxRateLimitScope::Conversation,
            capacity: Some(DEFAULT_CRITICAL_MAILBOX_CAPACITY),
            interval_seconds: Some(DEFAULT_CRITICAL_MAILBOX_INTERVAL_SECONDS),
        });
    }
}

pub fn validate_mailbox_message(message: &MailboxMessage) -> Result<()> {
    if message.message_id == Uuid::nil() {
        bail!("message_id must not be nil");
    }

    if message.sender.id.trim().is_empty() {
        bail!("sender.id must not be empty");
    }

    match message.sender.role {
        MailboxSenderRole::Automation => {
            ensure!(
                !is_high_or_above(&message.priority),
                "automation role may only send low or normal priority messages"
            );
        }
        MailboxSenderRole::Operator => {
            ensure!(
                !is_high_or_above(&message.priority),
                "operator role may only send low or normal priority messages"
            );
        }
        MailboxSenderRole::Orchestrator => {
            ensure!(
                !matches!(message.priority, MailboxPriority::Critical),
                "orchestrator role may not send critical priority messages"
            );
        }
        MailboxSenderRole::System => {}
    }

    if message.body.content.trim().is_empty() {
        bail!("message.body.content must not be empty");
    }

    let request_id = message
        .audit
        .request_id
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("audit.request_id is required"))?;
    ensure!(
        request_id.len() <= 200,
        "audit.request_id must be 200 characters or fewer"
    );

    if let Some(justification) = &message.audit.justification {
        ensure!(
            justification.len() <= 600,
            "audit.justification must be 600 characters or fewer"
        );
    }

    if let Some(auto) = message.ack_policy.auto_ack_seconds {
        ensure!(
            matches!(message.ack_policy.mode, MailboxAckMode::Passive),
            "ack.auto_ack_seconds is only valid when ack.mode is passive"
        );
        ensure!(auto > 0, "ack.auto_ack_seconds must be greater than zero");
    }

    if matches!(message.ack_policy.mode, MailboxAckMode::Required)
        && is_high_or_above(&message.priority)
    {
        let escalation = message
            .ack_policy
            .escalation_ticket
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("ack.escalation_ticket is required for required ACKs at high or critical priority"))?;
        ensure!(
            !escalation.is_empty(),
            "ack.escalation_ticket must not be empty"
        );
    }

    if is_high_or_above(&message.priority) {
        let justification = message
            .audit
            .justification
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("audit.justification is required for priority high or higher")
            })?;
        ensure!(
            !justification.is_empty(),
            "audit.justification must not be empty"
        );

        let change_ticket = message
            .audit
            .change_ticket
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("audit.change_ticket is required for priority high or higher")
            })?;
        ensure!(
            !change_ticket.is_empty(),
            "audit.change_ticket must not be empty"
        );
    }

    if let Some(rate_limit) = &message.rate_limit {
        let capacity = rate_limit.capacity.ok_or_else(|| {
            anyhow::anyhow!("rate_limit.capacity is required when specifying a rate limit")
        })?;
        ensure!(
            capacity > 0,
            "rate_limit.capacity must be greater than zero"
        );

        let interval = rate_limit.interval_seconds.ok_or_else(|| {
            anyhow::anyhow!("rate_limit.interval_seconds is required when specifying a rate limit")
        })?;
        ensure!(
            interval > 0,
            "rate_limit.interval_seconds must be greater than zero"
        );

        let justification = message
            .audit
            .justification
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("audit.justification is required when overriding rate limits")
            })?;
        ensure!(
            !justification.is_empty(),
            "audit.justification must not be empty when overriding rate limits"
        );
    }

    for key in message.tags.keys() {
        ensure!(
            !key.trim().is_empty(),
            "tag keys must not be empty or whitespace"
        );
    }

    Ok(())
}

fn is_high_or_above(priority: &MailboxPriority) -> bool {
    matches!(priority, MailboxPriority::High | MailboxPriority::Critical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_queue_tracks_length() {
        let (sender, receiver) = mailbox_channel(2);
        assert_eq!(sender.len(), 0);

        let env = MailboxEnvelope::new("sub_1".into(), MailboxMessage::default());
        assert_eq!(sender.try_enqueue(env.clone()).unwrap(), 1);
        assert_eq!(sender.len(), 1);

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.submission_id, env.submission_id);
        assert_eq!(sender.len(), 0);
    }

    #[tokio::test]
    async fn bounded_queue_reports_full_without_blocking() {
        let (sender, _receiver) = mailbox_channel(1);
        sender
            .try_enqueue(MailboxEnvelope::new(
                "sub_1".into(),
                MailboxMessage::default(),
            ))
            .unwrap();

        match sender.try_enqueue(MailboxEnvelope::new(
            "sub_2".into(),
            MailboxMessage::default(),
        )) {
            Err(TryEnqueueError::Full { capacity, .. }) => assert_eq!(capacity, 1),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn apply_defaults_sets_posted_at_and_rate_limit_for_critical() {
        let mut message = MailboxMessage {
            priority: MailboxPriority::Critical,
            sender: codex_protocol::mailbox::MailboxSender {
                id: "orchestrator.test".into(),
                role: MailboxSenderRole::System,
                ..Default::default()
            },
            body: codex_protocol::mailbox::MailboxBody {
                content: "test".into(),
                ..Default::default()
            },
            audit: codex_protocol::mailbox::MailboxAuditTrail {
                request_id: Some("REQ-1".into()),
                change_ticket: Some("CHG-1".into()),
                justification: Some("critical notice".into()),
                ..Default::default()
            },
            ..Default::default()
        };

        message.posted_at = None;
        message.rate_limit = None;

        apply_mailbox_defaults(&mut message);

        assert!(message.posted_at.is_some());
        let rate = message
            .rate_limit
            .expect("critical priority should set rate limit");
        assert_eq!(rate.scope, MailboxRateLimitScope::Conversation);
        assert_eq!(rate.capacity, Some(DEFAULT_CRITICAL_MAILBOX_CAPACITY));
        assert_eq!(
            rate.interval_seconds,
            Some(DEFAULT_CRITICAL_MAILBOX_INTERVAL_SECONDS)
        );
    }

    #[test]
    fn validation_rejects_high_priority_missing_justification() {
        let mut message = MailboxMessage {
            priority: MailboxPriority::High,
            sender: codex_protocol::mailbox::MailboxSender {
                id: "system.test".into(),
                role: MailboxSenderRole::System,
                ..Default::default()
            },
            body: codex_protocol::mailbox::MailboxBody {
                content: "needs justification".into(),
                ..Default::default()
            },
            audit: codex_protocol::mailbox::MailboxAuditTrail {
                request_id: Some("REQ-2".into()),
                change_ticket: Some("CHG-2".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        apply_mailbox_defaults(&mut message);

        let err = validate_mailbox_message(&message).unwrap_err();
        assert!(
            err.to_string()
                .contains("audit.justification is required for priority high or higher")
        );
    }

    #[test]
    fn validation_accepts_normal_priority() {
        let mut message = MailboxMessage {
            priority: MailboxPriority::Normal,
            sender: codex_protocol::mailbox::MailboxSender {
                id: "orchestrator.test".into(),
                role: MailboxSenderRole::Orchestrator,
                ..Default::default()
            },
            body: codex_protocol::mailbox::MailboxBody {
                content: "all good".into(),
                ..Default::default()
            },
            audit: codex_protocol::mailbox::MailboxAuditTrail {
                request_id: Some("REQ-3".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        apply_mailbox_defaults(&mut message);

        validate_mailbox_message(&message)
            .expect("normal priority payload should validate successfully");
    }
}

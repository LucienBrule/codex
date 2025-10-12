use crate::flags::CODEX_MAILBOX_OOB;

use async_channel::{Receiver, Sender, TrySendError};
use codex_protocol::mailbox::MailboxMessage;

pub(crate) const MAILBOX_QUEUE_CAPACITY: usize = 64;

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
}

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use codex_otel::metrics::record_mailbox_ack_total;
use codex_otel::metrics::record_mailbox_delivery_latency;
use codex_otel::metrics::record_mailbox_expiry_total;
use codex_otel::metrics::record_mailbox_queue_depth;
use codex_protocol::mailbox::MailboxAckMode;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxPriority;
use codex_protocol::mailbox::MailboxSenderRole;
use codex_protocol::protocol::MailboxDeliveryEvent;
use codex_protocol::protocol::MailboxDeliveryIngress;
use codex_protocol::protocol::MailboxDeliveryState;
use codex_protocol::protocol::MailboxLivenessState;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::config::MailboxLivenessSettings;

#[derive(Debug, Clone, PartialEq)]
pub struct MailboxLivenessSnapshot {
    pub observed_at: OffsetDateTime,
    pub state: MailboxLivenessState,
    pub transport_lag: Duration,
    pub queue_depth: usize,
}

#[derive(Debug)]
pub struct MailboxLivenessTelemetry {
    settings: MailboxLivenessSettings,
    last_state: MailboxLivenessState,
    last_emit_at: Option<Instant>,
}

impl MailboxLivenessTelemetry {
    pub fn new(settings: MailboxLivenessSettings) -> Self {
        Self {
            settings,
            last_state: MailboxLivenessState::Active,
            last_emit_at: None,
        }
    }

    pub fn record(
        &mut self,
        now: Instant,
        observed_at: OffsetDateTime,
        transport_lag: Duration,
        queue_depth: usize,
    ) -> Option<MailboxLivenessSnapshot> {
        let state = self.classify(transport_lag);
        if !self.should_emit(now, state) {
            return None;
        }

        self.last_state = state;
        self.last_emit_at = Some(now);
        Some(MailboxLivenessSnapshot {
            observed_at,
            state,
            transport_lag,
            queue_depth,
        })
    }

    fn classify(&self, lag: Duration) -> MailboxLivenessState {
        match lag.cmp(&self.settings.stalled_after) {
            Ordering::Greater | Ordering::Equal => MailboxLivenessState::Stalled,
            Ordering::Less => {
                if lag >= self.settings.idle_after {
                    MailboxLivenessState::Idle
                } else {
                    MailboxLivenessState::Active
                }
            }
        }
    }

    fn should_emit(&self, now: Instant, next_state: MailboxLivenessState) -> bool {
        if next_state != self.last_state {
            return true;
        }
        match self.last_emit_at {
            None => true,
            Some(last) => now.duration_since(last) >= self.settings.emit_interval,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxTelemetrySnapshot {
    pub state: MailboxDeliveryState,
    pub queue_depth: Option<usize>,
    pub delivery_latency_ms: Option<u64>,
}

#[derive(Debug, Default)]
pub struct MailboxDeliveryTelemetry {
    pending: HashMap<Uuid, PendingDelivery>,
}

#[derive(Debug, Clone)]
struct PendingDelivery {
    observed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone)]
pub struct MailboxTelemetryLabels {
    ingress: MailboxDeliveryIngress,
    priority: MailboxPriority,
    ack_mode: MailboxAckMode,
    sender_role: MailboxSenderRole,
}

impl MailboxTelemetryLabels {
    pub fn new(ingress: MailboxDeliveryIngress, message: &MailboxMessage) -> Self {
        Self {
            ingress,
            priority: message.priority.clone(),
            ack_mode: message.ack_policy.mode.clone(),
            sender_role: message.sender.role.clone(),
        }
    }

    fn metric_args(&self) -> (&'static str, &'static str, &'static str, &'static str) {
        (
            ingress_to_str(&self.ingress),
            priority_to_str(&self.priority),
            ack_mode_to_str(&self.ack_mode),
            sender_role_to_str(&self.sender_role),
        )
    }
}

impl MailboxDeliveryTelemetry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_enqueued(
        &mut self,
        event: &MailboxDeliveryEvent,
        labels: &MailboxTelemetryLabels,
    ) -> MailboxTelemetrySnapshot {
        self.pending.insert(
            event.message.message_id,
            PendingDelivery {
                observed_at: event.observed_at,
            },
        );

        let (ingress, priority, ack_mode, sender_role) = labels.metric_args();
        if let Some(depth) = event.queue_depth {
            record_mailbox_queue_depth(depth as u64, ingress, priority, ack_mode, sender_role);
        }

        if !matches!(labels.ack_mode, MailboxAckMode::None) {
            record_mailbox_ack_total(ingress, priority, ack_mode, sender_role);
        }

        if event.message.expires_at.is_some() {
            record_mailbox_expiry_total(ingress, priority, ack_mode, sender_role);
        }

        MailboxTelemetrySnapshot {
            state: event.state.clone(),
            queue_depth: event.queue_depth,
            delivery_latency_ms: None,
        }
    }

    pub fn record_delivered(
        &mut self,
        event: &mut MailboxDeliveryEvent,
        labels: &MailboxTelemetryLabels,
    ) -> MailboxTelemetrySnapshot {
        let pending = self.pending.remove(&event.message.message_id);
        if let Some(pending) = pending {
            if let (Some(enqueued_at), Some(delivered_at)) =
                (pending.observed_at, event.observed_at)
            {
                let delta = delivered_at - enqueued_at;
                let millis = delta.whole_milliseconds();
                if millis >= 0 {
                    let millis = millis.min(i128::from(u64::MAX)) as u64;
                    event.delivery_latency_ms = Some(millis);
                }
            }
        }

        let (ingress, priority, ack_mode, sender_role) = labels.metric_args();
        if let Some(depth) = event.queue_depth {
            record_mailbox_queue_depth(depth as u64, ingress, priority, ack_mode, sender_role);
        }

        if let Some(ms) = event.delivery_latency_ms {
            record_mailbox_delivery_latency(ms, ingress, priority, ack_mode, sender_role);
        }

        MailboxTelemetrySnapshot {
            state: event.state.clone(),
            queue_depth: event.queue_depth,
            delivery_latency_ms: event.delivery_latency_ms,
        }
    }
}

fn ingress_to_str(ingress: &MailboxDeliveryIngress) -> &'static str {
    match ingress {
        MailboxDeliveryIngress::Cli => "cli",
        MailboxDeliveryIngress::Script => "script",
        MailboxDeliveryIngress::Mcp => "mcp",
        MailboxDeliveryIngress::Vscode => "vscode",
        MailboxDeliveryIngress::Api => "api",
        MailboxDeliveryIngress::Unknown => "unknown",
    }
}

fn priority_to_str(priority: &MailboxPriority) -> &'static str {
    match priority {
        MailboxPriority::Critical => "critical",
        MailboxPriority::High => "high",
        MailboxPriority::Normal => "normal",
        MailboxPriority::Low => "low",
    }
}

fn ack_mode_to_str(mode: &MailboxAckMode) -> &'static str {
    match mode {
        MailboxAckMode::None => "none",
        MailboxAckMode::Passive => "passive",
        MailboxAckMode::Required => "required",
    }
}

fn sender_role_to_str(role: &MailboxSenderRole) -> &'static str {
    match role {
        MailboxSenderRole::Orchestrator => "orchestrator",
        MailboxSenderRole::Operator => "operator",
        MailboxSenderRole::Automation => "automation",
        MailboxSenderRole::System => "system",
    }
}

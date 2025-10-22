use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::SummariesState;
use crate::codex::INITIAL_SUBMIT_ID;
use crate::codex::compact::collect_user_messages;
use crate::config::SummariesSettings;
use crate::protocol::Event;
use crate::protocol::EventMsg;
use crate::protocol::SummaryUpdatedEvent;
use crate::telemetry::SummariesBatchTelemetry;
use crate::telemetry::record_summaries_batch;
use std::time::Instant;

/// Background service that periodically emits running conversation summaries.
pub(crate) struct SummariesService {
    handle: JoinHandle<()>,
    #[allow(dead_code)]
    state: std::sync::Arc<Mutex<SummariesState>>, // shared state used by prompt builder
}

impl SummariesService {
    /// Spawn the background task if enabled. Returns `None` when disabled.
    pub(crate) fn spawn(
        sess: Arc<crate::codex::Session>,
        settings: SummariesSettings,
        initial_summary: Option<String>,
    ) -> Option<Self> {
        if !settings.enabled {
            return None;
        }

        let state = std::sync::Arc::new(Mutex::new(SummariesState::default()));
        if let Some(initial) = initial_summary {
            let mut s = state.blocking_lock();
            s.last_summary = Some(initial);
        }

        let state_for_task = std::sync::Arc::clone(&state);
        let handle = tokio::spawn(async move {
            // Optional initial delay to avoid competing with startup work.
            if settings.initial_delay > Duration::from_millis(0) {
                sleep(settings.initial_delay).await;
            }

            let mut interval = tokio::time::interval(settings.emit_interval);
            // Align the first tick with `emit_interval` rather than firing immediately.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;

                // Build a lightweight "running summary" derived from the current
                // summaries state plus the recent tail of user messages. Keep this
                // entirely in-process and non-blocking.
                let history = sess.history_snapshot().await;
                let user_msgs = collect_user_messages(&history);

                // Take a small tail to keep emissions bounded.
                const TAIL_USER_MAX: usize = 6;
                let start = user_msgs.len().saturating_sub(TAIL_USER_MAX);
                let recent_tail = &user_msgs[start..];

                let mut new_summary = String::new();
                new_summary.push_str("summary so far:\n");
                {
                    let mut s = state_for_task.lock().await;
                    if let Some(prev) = s.last_summary.as_deref() {
                        if !prev.trim().is_empty() {
                            new_summary.push_str(prev.trim());
                            if !new_summary.ends_with('\n') {
                                new_summary.push('\n');
                            }
                        }
                    } else {
                        new_summary.push_str("(no summary available)\n");
                    }
                    if !recent_tail.is_empty() {
                        new_summary.push_str("\nrecent user messages:\n");
                        for line in recent_tail {
                            new_summary.push_str(line);
                            if !new_summary.ends_with('\n') {
                                new_summary.push('\n');
                            }
                        }
                    }
                    // Update shared state so prompt builder can incorporate the
                    // refreshed summary in subsequent turns.
                    s.last_summary = Some(new_summary.clone());
                    s.last_emit_instant = Some(Instant::now());
                }

                // Record per-batch telemetry and emit the update event.
                let batch = SummariesBatchTelemetry::new(0);
                let snapshot = batch.finish(&new_summary);

                let event = Event {
                    id: INITIAL_SUBMIT_ID.to_string(),
                    msg: EventMsg::SummaryUpdated(SummaryUpdatedEvent {
                        summary: new_summary,
                    }),
                };
                // Use the session's event path so rollout/event history sees it.
                sess.send_event(event).await;

                // Emit non-blocking OTEL-style metrics for observability.
                record_summaries_batch(&snapshot);
            }
        });

        Some(Self { handle, state })
    }

    /// Returns a cheap, cloned snapshot of the current summaries state.
    pub(crate) async fn snapshot_state(&self) -> SummariesState {
        let s = self.state.lock().await;
        s.clone()
    }

    /// Returns a clone of the internal state Arc for non-blocking access.
    pub(crate) fn state_arc(&self) -> Arc<Mutex<SummariesState>> {
        Arc::clone(&self.state)
    }
}

impl Drop for SummariesService {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

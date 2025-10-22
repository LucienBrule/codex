//! Turn-scoped state and active turn metadata scaffolding.

use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use uuid::Uuid;

use codex_protocol::models::ResponseInputItem;
use tokio::sync::oneshot;

use crate::protocol::ReviewDecision;
use crate::tasks::SessionTask;

/// Metadata about the currently running turn.
pub(crate) struct ActiveTurn {
    pub(crate) tasks: IndexMap<String, RunningTask>,
    pub(crate) turn_state: Arc<Mutex<TurnState>>,
}

impl Default for ActiveTurn {
    fn default() -> Self {
        Self {
            tasks: IndexMap::new(),
            turn_state: Arc::new(Mutex::new(TurnState::default())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskKind {
    Regular,
    Review,
    Compact,
}

#[derive(Clone)]
pub(crate) struct RunningTask {
    pub(crate) handle: AbortHandle,
    pub(crate) kind: TaskKind,
    pub(crate) task: Arc<dyn SessionTask>,
}

impl ActiveTurn {
    pub(crate) fn add_task(&mut self, sub_id: String, task: RunningTask) {
        self.tasks.insert(sub_id, task);
    }

    pub(crate) fn remove_task(&mut self, sub_id: &str) -> bool {
        self.tasks.swap_remove(sub_id);
        self.tasks.is_empty()
    }

    pub(crate) fn drain_tasks(&mut self) -> IndexMap<String, RunningTask> {
        std::mem::take(&mut self.tasks)
    }
}

/// Mutable state for a single turn.
#[derive(Default)]
pub(crate) struct TurnState {
    pending_approvals: HashMap<String, oneshot::Sender<ReviewDecision>>,
    pending_input: Vec<ResponseInputItem>,
    delayed_triggers: IndexMap<Uuid, DelayedTrigger>,
}

impl TurnState {
    pub(crate) fn insert_pending_approval(
        &mut self,
        key: String,
        tx: oneshot::Sender<ReviewDecision>,
    ) -> Option<oneshot::Sender<ReviewDecision>> {
        self.pending_approvals.insert(key, tx)
    }

    pub(crate) fn remove_pending_approval(
        &mut self,
        key: &str,
    ) -> Option<oneshot::Sender<ReviewDecision>> {
        self.pending_approvals.remove(key)
    }

    pub(crate) fn clear_pending(&mut self) -> Vec<Uuid> {
        self.pending_approvals.clear();
        self.pending_input.clear();
        self.delayed_triggers.drain(..).map(|(id, _)| id).collect()
    }

    pub(crate) fn push_pending_input(&mut self, input: ResponseInputItem) {
        self.pending_input.push(input);
    }

    pub(crate) fn take_pending_input(&mut self) -> Vec<ResponseInputItem> {
        if self.pending_input.is_empty() {
            Vec::with_capacity(0)
        } else {
            let mut ret = Vec::new();
            std::mem::swap(&mut ret, &mut self.pending_input);
            ret
        }
    }

    pub(crate) fn insert_trigger(&mut self, trigger: DelayedTrigger) -> Option<DelayedTrigger> {
        self.delayed_triggers.insert(trigger.id, trigger)
    }

    pub(crate) fn create_trigger(
        &mut self,
        predicate_id: String,
        sub_id: String,
        call_id: String,
        wake_deadline: Option<OffsetDateTime>,
        fire_quota: u32,
        request_id: Option<String>,
    ) -> (Uuid, String) {
        let trigger = DelayedTrigger::new(
            predicate_id,
            sub_id,
            call_id,
            wake_deadline,
            fire_quota,
            request_id,
        );
        let id = trigger.id;
        let request_id = trigger.request_id.clone();
        self.delayed_triggers.insert(id, trigger);
        (id, request_id)
    }

    pub(crate) fn remove_trigger(&mut self, id: &Uuid) -> Option<DelayedTrigger> {
        self.delayed_triggers.shift_remove(id)
    }

    pub(crate) fn trigger_count(&self) -> usize {
        self.delayed_triggers.len()
    }

    pub(crate) fn trigger_exists(&self, id: &Uuid) -> bool {
        self.delayed_triggers.contains_key(id)
    }

    #[cfg(test)]
    pub(crate) fn trigger_metadata(&self, id: &Uuid) -> Option<&DelayedTrigger> {
        self.delayed_triggers.get(id)
    }
}

impl ActiveTurn {
    /// Clear any pending approvals and input buffered for the current turn.
    pub(crate) async fn clear_pending(&self) -> Vec<Uuid> {
        let mut ts = self.turn_state.lock().await;
        ts.clear_pending()
    }

    /// Best-effort, non-blocking variant for synchronous contexts (Drop/interrupt).
    pub(crate) fn try_clear_pending_sync(&self) -> Vec<Uuid> {
        if let Ok(mut ts) = self.turn_state.try_lock() {
            ts.clear_pending()
        } else {
            Vec::new()
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DelayedTrigger {
    pub(crate) id: Uuid,
    pub(crate) predicate_id: String,
    pub(crate) sub_id: String,
    pub(crate) call_id: String,
    pub(crate) created_at: OffsetDateTime,
    pub(crate) wake_deadline: Option<OffsetDateTime>,
    pub(crate) fire_quota: u32,
    pub(crate) fires: u32,
    pub(crate) request_id: String,
}

impl DelayedTrigger {
    pub(crate) fn new(
        predicate_id: String,
        sub_id: String,
        call_id: String,
        wake_deadline: Option<OffsetDateTime>,
        fire_quota: u32,
        request_id: Option<String>,
    ) -> Self {
        let quota = fire_quota.max(1);
        let id = Uuid::now_v7();
        Self {
            id,
            predicate_id,
            sub_id,
            call_id,
            created_at: OffsetDateTime::now_utc(),
            wake_deadline,
            fire_quota: quota,
            fires: 0,
            request_id: request_id.unwrap_or_else(|| format!("wait-trigger/{id}")),
        }
    }

    pub(crate) fn record_fire(&mut self) -> bool {
        self.fires = self.fires.saturating_add(1);
        self.fires >= self.fire_quota
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(crate) fn deadline(&self) -> Option<OffsetDateTime> {
        self.wake_deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delayed_trigger_enforces_quota() {
        let mut trigger = DelayedTrigger::new(
            "fs".into(),
            "sub-1".into(),
            "call-1".into(),
            None,
            2,
            Some("req-1".into()),
        );
        assert!(!trigger.record_fire());
        assert!(trigger.record_fire());
        assert!(trigger.record_fire());
    }

    #[test]
    fn turn_state_tracks_delayed_triggers() {
        let mut state = TurnState::default();
        let trigger = DelayedTrigger::new(
            "timer".into(),
            "sub-42".into(),
            "call-99".into(),
            None,
            1,
            Some("req-99".into()),
        );
        let trigger_id = trigger.id;
        state.insert_trigger(trigger);
        assert!(state.trigger_exists(&trigger_id));
        assert_eq!(state.trigger_count(), 1);
        let cleared = state.clear_pending();
        assert_eq!(cleared, vec![trigger_id]);
        assert_eq!(state.trigger_count(), 0);
    }
}

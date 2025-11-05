use std::sync::Arc;
use std::sync::Mutex;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use crate::bottom_pane::BottomPaneView;
use crate::bottom_pane::CancellationEvent;
use crate::key_hint;
use crate::render::renderable::Renderable;
use crate::text_formatting::truncate_text;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use ratatui::layout::Constraint;
use ratatui::layout::Layout;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use ratatui::widgets::Wrap;
use textwrap::wrap;
use time::OffsetDateTime;
use uuid::Uuid;

use codex_core::protocol::MailboxDeliveryEvent;
use codex_core::protocol::MailboxDeliveryState;
use codex_protocol::mailbox::MailboxAckMode;
use codex_protocol::mailbox::MailboxMessage;
use codex_protocol::mailbox::MailboxPriority;
use codex_protocol::mailbox::MailboxSenderRole;

/// Shared mailbox store exposed to the TUI widgets.
pub(crate) type SharedMailboxStore = Arc<Mutex<MailboxStore>>;

#[derive(Debug, Default)]
pub(crate) struct MailboxStore {
    entries: Vec<MailboxEntry>,
}

impl MailboxStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Insert or update an entry based on a mailbox delivery event.
    pub(crate) fn upsert_delivery(&mut self, delivery: MailboxDeliveryEvent) {
        let message_id = delivery.message.message_id;
        let now = delivery.observed_at.unwrap_or_else(OffsetDateTime::now_utc);
        let state = delivery.state.clone();
        let queue_depth = delivery.queue_depth;
        let observed_at = delivery.observed_at;
        let message_clone = delivery.message.clone();
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|entry| entry.message.message_id == message_id)
        {
            existing.message = message_clone;
            existing.state = state.clone();
            existing.queue_depth = queue_depth;
            existing.observed_at = observed_at;
            existing.last_updated = now;
            if state == MailboxDeliveryState::Delivered && !existing.disposition.is_pending() {
                existing.disposition = EntryDisposition::Pending;
            }
        } else {
            self.entries.push(MailboxEntry {
                message: delivery.message,
                state,
                queue_depth,
                observed_at,
                disposition: EntryDisposition::Pending,
                last_updated: now,
            });
            self.sort_entries();
        }
    }

    pub(crate) fn pending_snapshots(&self) -> Vec<MailboxEntrySnapshot> {
        let mut snapshots: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.disposition.is_pending())
            .map(MailboxEntrySnapshot::from_entry)
            .collect();
        snapshots.sort_by(|a, b| {
            priority_rank(&a.priority)
                .cmp(&priority_rank(&b.priority))
                .then_with(|| a.observed_at.cmp(&b.observed_at))
        });
        snapshots
    }

    pub(crate) fn badge(&self) -> Option<MailboxBadgeState> {
        let pending_total = self
            .entries
            .iter()
            .filter(|entry| entry.disposition.is_pending())
            .count();
        if pending_total == 0 {
            return None;
        }
        let pending_required = self
            .entries
            .iter()
            .filter(|entry| entry.disposition.is_pending() && entry.ack_required())
            .count();
        Some(MailboxBadgeState {
            pending_total,
            pending_required,
        })
    }

    pub(crate) fn ack(&mut self, id: Uuid) -> Option<MailboxActionOutcome> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.message.message_id == id)?;
        if !entry.disposition.is_pending() {
            return None;
        }
        let outcome = MailboxActionOutcome::new(entry, MailboxActionKind::Acked);
        entry.disposition = EntryDisposition::Acknowledged {
            at: OffsetDateTime::now_utc(),
        };
        Some(outcome)
    }

    pub(crate) fn dismiss(&mut self, id: Uuid) -> Option<MailboxActionOutcome> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.message.message_id == id)?;
        if !entry.disposition.is_pending() {
            return None;
        }
        let outcome = MailboxActionOutcome::new(entry, MailboxActionKind::Dismissed);
        entry.disposition = EntryDisposition::Dismissed {
            at: OffsetDateTime::now_utc(),
        };
        Some(outcome)
    }

    fn sort_entries(&mut self) {
        self.entries.sort_by(|a, b| {
            priority_rank(&a.message.priority)
                .cmp(&priority_rank(&b.message.priority))
                .then_with(|| a.observed_at.cmp(&b.observed_at))
                .then_with(|| a.last_updated.cmp(&b.last_updated))
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MailboxBadgeState {
    pub pending_total: usize,
    pub pending_required: usize,
}

impl MailboxBadgeState {
    pub(crate) fn has_required(&self) -> bool {
        self.pending_required > 0
    }
}

#[derive(Debug)]
struct MailboxEntry {
    message: MailboxMessage,
    state: MailboxDeliveryState,
    queue_depth: Option<usize>,
    observed_at: Option<OffsetDateTime>,
    disposition: EntryDisposition,
    last_updated: OffsetDateTime,
}

impl MailboxEntry {
    fn ack_required(&self) -> bool {
        self.message.ack_policy.mode == MailboxAckMode::Required
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MailboxEntrySnapshot {
    pub message_id: Uuid,
    pub subject: Option<String>,
    pub sender_display: Option<String>,
    pub sender_role: MailboxSenderRole,
    pub priority: MailboxPriority,
    pub ack_mode: MailboxAckMode,
    pub content: String,
    pub audit_request_id: Option<String>,
    pub observed_at: Option<OffsetDateTime>,
}

impl MailboxEntrySnapshot {
    fn from_entry(entry: &MailboxEntry) -> Self {
        let sender_display = entry
            .message
            .sender
            .display_name
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if entry.message.sender.id.is_empty() {
                    None
                } else {
                    Some(entry.message.sender.id.clone())
                }
            });
        Self {
            message_id: entry.message.message_id,
            subject: entry.message.body.subject.clone(),
            sender_display,
            sender_role: entry.message.sender.role.clone(),
            priority: entry.message.priority.clone(),
            ack_mode: entry.message.ack_policy.mode.clone(),
            content: entry.message.body.content.clone(),
            audit_request_id: entry.message.audit.request_id.clone(),
            observed_at: entry.observed_at,
        }
    }

    fn ack_required(&self) -> bool {
        self.ack_mode == MailboxAckMode::Required
    }
}

#[derive(Clone, Copy, Debug)]
enum EntryDisposition {
    Pending,
    Acknowledged { at: OffsetDateTime },
    Dismissed { at: OffsetDateTime },
}

impl EntryDisposition {
    fn is_pending(self) -> bool {
        matches!(self, EntryDisposition::Pending)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MailboxActionOutcome {
    pub message_id: Uuid,
    pub action: MailboxActionKind,
    pub subject: Option<String>,
    pub sender_display: Option<String>,
    pub priority: MailboxPriority,
    pub request_id: Option<String>,
    pub ack_required: bool,
}

impl MailboxActionOutcome {
    fn new(entry: &MailboxEntry, action: MailboxActionKind) -> Self {
        Self {
            message_id: entry.message.message_id,
            action,
            subject: entry.message.body.subject.clone(),
            sender_display: entry
                .message
                .sender
                .display_name
                .clone()
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    if entry.message.sender.id.is_empty() {
                        None
                    } else {
                        Some(entry.message.sender.id.clone())
                    }
                }),
            priority: entry.message.priority.clone(),
            request_id: entry.message.audit.request_id.clone(),
            ack_required: entry.ack_required(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum MailboxActionKind {
    Acked,
    Dismissed,
}

pub(crate) struct MailboxView {
    store: SharedMailboxStore,
    app_event_tx: AppEventSender,
    selected: usize,
    status_message: Option<String>,
    completed: bool,
}

impl MailboxView {
    pub(crate) fn new(store: SharedMailboxStore, app_event_tx: AppEventSender) -> Self {
        Self {
            store,
            app_event_tx,
            selected: 0,
            status_message: None,
            completed: false,
        }
    }

    fn pending_entries(&self) -> Vec<MailboxEntrySnapshot> {
        self.store
            .lock()
            .map(|store| store.pending_snapshots())
            .unwrap_or_default()
    }

    fn ack_selected(&mut self) {
        let entry_id = self
            .pending_entries()
            .get(self.selected)
            .cloned()
            .map(|snapshot| snapshot.message_id);
        let Some(id) = entry_id else {
            self.status_message = Some("No mailbox message selected".to_string());
            return;
        };

        let outcome = self.store.lock().ok().and_then(|mut store| store.ack(id));
        match outcome {
            Some(outcome) => {
                let subject = outcome
                    .subject
                    .clone()
                    .unwrap_or_else(|| outcome.message_id.to_string());
                let short = truncate_text(&subject, 40);
                self.status_message = Some(format!("Acknowledged mailbox message {}", short));
                self.app_event_tx.send(AppEvent::MailboxAction(outcome));
                self.after_action();
            }
            None => {
                self.status_message = Some("Mailbox message already handled".to_string());
            }
        }
    }

    fn dismiss_selected(&mut self) {
        let entry_id = self
            .pending_entries()
            .get(self.selected)
            .cloned()
            .map(|snapshot| snapshot.message_id);
        let Some(id) = entry_id else {
            self.status_message = Some("No mailbox message selected".to_string());
            return;
        };

        let outcome = self
            .store
            .lock()
            .ok()
            .and_then(|mut store| store.dismiss(id));
        match outcome {
            Some(outcome) => {
                let subject = outcome
                    .subject
                    .clone()
                    .unwrap_or_else(|| outcome.message_id.to_string());
                let short = truncate_text(&subject, 40);
                self.status_message = Some(format!("Dismissed mailbox message {}", short));
                self.app_event_tx.send(AppEvent::MailboxAction(outcome));
                self.after_action();
            }
            None => {
                self.status_message = Some("Mailbox message already handled".to_string());
            }
        }
    }

    fn after_action(&mut self) {
        let entries = self.pending_entries();
        if entries.is_empty() {
            self.completed = true;
        } else if self.selected >= entries.len() {
            self.selected = entries.len() - 1;
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.pending_entries().len();
        if len == 0 {
            return;
        }
        let current = self.selected as isize;
        let next = (current + delta).clamp(0, (len - 1) as isize);
        self.selected = next as usize;
    }

    fn render_contents(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if area.height == 0 || area.width == 0 {
            return;
        }

        let entries = self.pending_entries();
        let [header_area, list_area, detail_area] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(area.height.saturating_sub(4).max(3)),
            Constraint::Min(3),
        ])
        .areas(area);

        let mut header_spans: Vec<Span<'static>> = vec![
            key_hint::plain(KeyCode::Up).into(),
            "/".into(),
            key_hint::plain(KeyCode::Down).into(),
            " to move  ".into(),
            key_hint::plain(KeyCode::Enter).into(),
            "/".into(),
            key_hint::plain(KeyCode::Char('a')).into(),
            " to acknowledge  ".into(),
            key_hint::plain(KeyCode::Char('d')).into(),
            " to dismiss  ".into(),
            key_hint::plain(KeyCode::Esc).into(),
            " to close".into(),
        ];
        if let Some(status) = &self.status_message {
            header_spans.push("   ".into());
            header_spans.push(Span::styled(
                status.clone(),
                Style::default().dim().italic(),
            ));
        }
        Paragraph::new(Line::from(header_spans)).render(header_area, buf);

        let mut list_lines: Vec<Line<'static>> = Vec::new();
        if entries.is_empty() {
            list_lines.push(
                Line::from("Inbox clear — no mailbox messages pending.")
                    .style(Style::default().dim()),
            );
        } else {
            for (idx, snapshot) in entries.iter().enumerate() {
                let mut spans: Vec<Span<'static>> = Vec::new();
                spans.push(Span::styled(
                    format!("{:>2}. ", idx + 1),
                    Style::default().dim(),
                ));
                let badge = if snapshot.ack_required() {
                    "ACK"
                } else {
                    "info"
                };
                let badge_style = if snapshot.ack_required() {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Cyan)
                };
                spans.push(Span::styled(format!("[{badge}] "), badge_style));
                let subject = snapshot
                    .subject
                    .clone()
                    .unwrap_or_else(|| "(no subject)".to_string());
                spans.push(Span::styled(
                    truncate_text(&subject, 40),
                    Style::default().bold(),
                ));
                let sender = snapshot
                    .sender_display
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", snapshot.sender_role).to_lowercase());
                spans.push("  ·  ".into());
                spans.push(sender.into());
                spans.push("  ·  ".into());
                spans.push(format!("{:?}", snapshot.priority).to_lowercase().into());
                let mut line = Line::from(spans);
                if idx == self.selected {
                    line = line.style(Style::default().add_modifier(Modifier::REVERSED));
                }
                list_lines.push(line);
            }
        }
        Paragraph::new(list_lines)
            .wrap(Wrap { trim: true })
            .render(list_area, buf);

        let mut detail_lines: Vec<Line<'static>> = Vec::new();
        if let Some(snapshot) = entries.get(self.selected) {
            let subject = snapshot
                .subject
                .clone()
                .unwrap_or_else(|| "(no subject)".to_string());
            let sender = snapshot
                .sender_display
                .clone()
                .unwrap_or_else(|| format!("{:?}", snapshot.sender_role).to_lowercase());
            detail_lines.push(Line::from(vec!["Subject: ".bold(), Span::raw(subject)]));
            detail_lines.push(Line::from(vec!["From: ".bold(), Span::raw(sender)]));
            detail_lines.push(Line::from(vec![
                "Priority: ".bold(),
                format!("{:?}", snapshot.priority).to_lowercase().into(),
                if snapshot.ack_required() {
                    " (ack required)".bold().fg(Color::Red)
                } else {
                    "".into()
                },
            ]));
            detail_lines.push(Line::from(vec![
                "Message ID: ".bold(),
                snapshot.message_id.to_string().into(),
            ]));
            if let Some(req) = &snapshot.audit_request_id {
                detail_lines.push(Line::from(vec!["Request ID: ".bold(), req.clone().into()]));
            }
            if let Some(observed) = snapshot.observed_at {
                detail_lines.push(Line::from(vec![
                    "Observed: ".bold(),
                    observed.to_string().into(),
                ]));
            }
            detail_lines.push(Line::from(""));
            detail_lines.push(Line::from("Message body:".bold()));
            if snapshot.content.trim().is_empty() {
                detail_lines.push(
                    Line::from("(empty message body)").style(Style::default().dim().italic()),
                );
            } else {
                let available_width = detail_area.width.max(10) as usize;
                for wrapped in wrap(&snapshot.content, available_width) {
                    detail_lines.push(Line::from(wrapped.to_string()));
                }
            }
            detail_lines.push(Line::from(""));
            detail_lines.push(Line::from(vec![Span::styled(
                "Press Enter/A to acknowledge or D to dismiss.".to_string(),
                Style::default().dim(),
            )]));
        } else {
            detail_lines
                .push(Line::from("Mailbox inbox is clear.").style(Style::default().dim().italic()));
        }
        Paragraph::new(detail_lines)
            .block(Block::default().title("Mailbox Detail"))
            .wrap(Wrap { trim: false })
            .render(detail_area, buf);
    }
}

impl Renderable for MailboxView {
    fn render(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        self.render_contents(area, buf);
    }

    fn desired_height(&self, _width: u16) -> u16 {
        8
    }
}

impl BottomPaneView for MailboxView {
    fn handle_key_event(&mut self, key_event: KeyEvent) {
        if key_event.kind != crossterm::event::KeyEventKind::Press {
            return;
        }
        match key_event.code {
            KeyCode::Up => self.move_selection(-1),
            KeyCode::Down => self.move_selection(1),
            KeyCode::Enter => self.ack_selected(),
            KeyCode::Char('a') | KeyCode::Char('A') => self.ack_selected(),
            KeyCode::Char('d') | KeyCode::Char('D') => self.dismiss_selected(),
            _ => {}
        }
    }

    fn is_complete(&self) -> bool {
        self.completed
    }

    fn on_ctrl_c(&mut self) -> CancellationEvent {
        self.completed = true;
        CancellationEvent::Handled
    }
}

fn priority_rank(priority: &MailboxPriority) -> u8 {
    match priority {
        MailboxPriority::Critical => 0,
        MailboxPriority::High => 1,
        MailboxPriority::Normal => 2,
        MailboxPriority::Low => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_event::AppEvent;
    use crate::app_event_sender::AppEventSender;
    use codex_protocol::mailbox::MailboxAckPolicy;
    use codex_protocol::mailbox::MailboxBody;
    use codex_protocol::mailbox::MailboxContentType;
    use insta::assert_snapshot;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use time::OffsetDateTime;
    use tokio::sync::mpsc::unbounded_channel;

    fn sample_delivery(state: MailboxDeliveryState) -> MailboxDeliveryEvent {
        let mut message = MailboxMessage::default();
        message.sender.id = "orchestrator.test".to_string();
        message.sender.display_name = Some("Mailbox Control".to_string());
        message.body = MailboxBody {
            subject: Some("Security advisory".to_string()),
            content: "Please review the updated security runbook before proceeding.".to_string(),
            content_type: MailboxContentType::TextPlain,
        };
        message.ack_policy = MailboxAckPolicy {
            mode: MailboxAckMode::Required,
            deadline: None,
            auto_ack_seconds: None,
            escalation_ticket: Some("SEC-12345".to_string()),
        };
        // Adapt to protocol additions: provide optional fields to satisfy
        // struct literal initialization across crate boundary.
        MailboxDeliveryEvent {
            message,
            state,
            queue_depth: Some(1),
            observed_at: Some(OffsetDateTime::now_utc()),
            correlation_id: None,
            ingress: None,
            delivery_latency_ms: None,
        }
    }

    #[test]
    fn store_ack_and_dismiss_update_badge() {
        let mut store = MailboxStore::new();
        let delivery = sample_delivery(MailboxDeliveryState::Delivered);
        let message_id = delivery.message.message_id;
        store.upsert_delivery(delivery);
        let badge = store.badge().expect("badge present");
        assert_eq!(badge.pending_total, 1);
        assert_eq!(badge.pending_required, 1);

        let outcome = store.ack(message_id).expect("ack success");
        assert!(matches!(outcome.action, MailboxActionKind::Acked));
        assert!(store.badge().is_none());

        // Reinsert and dismiss
        let delivery = sample_delivery(MailboxDeliveryState::Delivered);
        let message_id = delivery.message.message_id;
        store.upsert_delivery(delivery);
        assert!(store.badge().is_some());
        let outcome = store.dismiss(message_id).expect("dismiss success");
        assert!(matches!(outcome.action, MailboxActionKind::Dismissed));
        assert!(store.badge().is_none());
    }

    #[test]
    fn mailbox_view_renders_snapshot() {
        let store = Arc::new(Mutex::new(MailboxStore::new()));
        let delivery = sample_delivery(MailboxDeliveryState::Delivered);
        store.lock().unwrap().upsert_delivery(delivery);

        let (tx_raw, _rx) = unbounded_channel::<AppEvent>();
        let tx = AppEventSender::new(tx_raw);
        let view = MailboxView::new(store.clone(), tx);

        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|f| {
                (&view).render(f.area(), f.buffer_mut());
            })
            .expect("draw");

        assert_snapshot!("mailbox_view_basic", terminal.backend());
    }
}

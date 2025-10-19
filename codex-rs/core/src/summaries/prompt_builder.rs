use codex_protocol::models::{ContentItem, ResponseItem};

use crate::codex::compact::is_session_prefix_message;

use super::SummariesState;

/// Build a prompt for the next turn by combining the existing `history`
/// with any `pending` items that arrived while the model was thinking.
///
/// Behavior:
/// - When summaries are unavailable (`summary_state.last_summary` is `None`),
///   this returns `history + pending` unchanged to preserve current behavior.
/// - When a summary is available and the prompt would exceed `budget` items,
///   a simple sliding window is applied: keep session prefix messages
///   (environment context and user instructions), keep the most recent
///   `budget` history items, and insert a lightweight bridge message with the
///   current summary between prefix and tail.
///
/// Notes:
/// - `budget` is measured in item count, not tokens. This is a minimal
///   placeholder to establish the API; future tasks can switch to token-aware
///   budgeting.
pub(crate) fn build_prompt(
    summary_state: &SummariesState,
    history: &[ResponseItem],
    pending: &[ResponseItem],
    budget: usize,
) -> Vec<ResponseItem> {
    // Fast path: no summaries available or budget comfortably fits everything.
    if summary_state.last_summary.is_none() || history.len() + pending.len() <= budget {
        let mut out = Vec::with_capacity(history.len() + pending.len());
        out.extend_from_slice(history);
        out.extend_from_slice(pending);
        return out;
    }

    // Identify session prefix messages (environment context / user instructions).
    let mut prefix_len = 0usize;
    for item in history.iter() {
        match item {
            ResponseItem::Message { role, content, .. } if role == "user" => {
                // Consider the item a prefix if ANY text chunk matches a known prefix marker.
                let is_prefix = content.iter().any(|c| match c {
                    ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                        is_session_prefix_message(text)
                    }
                    _ => false,
                });
                if is_prefix {
                    prefix_len += 1;
                    continue;
                }
                break;
            }
            _ => break,
        }
    }

    let mut out = Vec::new();
    out.extend_from_slice(&history[..prefix_len]);

    // Insert a lightweight bridge carrying the running summary.
    if let Some(summary) = &summary_state.last_summary {
        out.push(ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: format!(
                    "<conversation_summary>\n{}\n</conversation_summary>",
                    summary
                ),
            }],
        });
    }

    // Keep the most recent window of history items.
    let window = budget.min(history.len());
    out.extend_from_slice(&history[history.len() - window..]);

    // Finally, append any pending input destined for this turn.
    out.extend_from_slice(pending);

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_msg(text: &str) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::OutputText { text: text.into() }],
        }
    }

    #[test]
    fn passthrough_without_summary() {
        let st = SummariesState::default();
        let history = vec![user_msg("hi")];
        let pending = vec![user_msg("there")];
        let out = build_prompt(&st, &history, &pending, usize::MAX);
        assert_eq!(out, vec![user_msg("hi"), user_msg("there")]);
    }

    #[test]
    fn inserts_summary_block_when_available() {
        let mut st = SummariesState::default();
        st.last_summary = Some("PRIOR SUMMARY".to_string());
        let history = vec![user_msg("h1"), user_msg("h2")];
        // Force budget to be exceeded so the builder inserts the bridge.
        let out = build_prompt(&st, &history, &[], 0);
        let text_blocks: Vec<String> = out
            .iter()
            .filter_map(|item| match item {
                ResponseItem::Message { content, .. } => Some(content),
                _ => None,
            })
            .filter_map(|content| {
                let mut s = String::new();
                for c in content {
                    if let ContentItem::InputText { text } | ContentItem::OutputText { text } = c {
                        s.push_str(text);
                    }
                }
                if s.is_empty() { None } else { Some(s) }
            })
            .collect();
        assert!(text_blocks.iter().any(|t| t.contains("<conversation_summary>") && t.contains("PRIOR SUMMARY")));
    }
}

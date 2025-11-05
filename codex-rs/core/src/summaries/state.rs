use std::time::Instant;

#[derive(Default, Clone)]
pub(crate) struct SummariesState {
    pub(crate) last_summary: Option<String>,
    pub(crate) last_emit_instant: Option<Instant>,
}

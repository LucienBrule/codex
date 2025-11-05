mod checkpoint;
mod prompt_builder;
mod service;
mod state;

pub(crate) use checkpoint::checkpoint_path;
pub(crate) use checkpoint::checkpoint_paths_for_base;
pub(crate) use checkpoint::read_summary_checkpoint;
pub(crate) use checkpoint::read_summary_checkpoint_with_base;
pub(crate) use checkpoint::write_summary_checkpoint;
pub(crate) use checkpoint::write_summary_checkpoint_with_base;
pub(crate) use prompt_builder::build_prompt;
pub(crate) use service::SummariesService;
pub(crate) use state::SummariesState;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summaries_state_default_compiles() {
        let s = SummariesState::default();
        assert!(s.last_summary.is_none());
    }
}

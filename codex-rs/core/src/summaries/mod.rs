mod service;
mod state;
mod checkpoint;
mod prompt_builder;

pub(crate) use service::SummariesService;
pub(crate) use state::SummariesState;
pub(crate) use checkpoint::{
    checkpoint_path, checkpoint_paths_for_base, read_summary_checkpoint,
    read_summary_checkpoint_with_base, write_summary_checkpoint,
    write_summary_checkpoint_with_base,
};
pub(crate) use prompt_builder::build_prompt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summaries_state_default_compiles() {
        let s = SummariesState::default();
        assert!(s.last_summary.is_none());
    }
}

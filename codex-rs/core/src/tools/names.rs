use std::borrow::Cow;

pub const SHELL_TOOL_NAME: &str = "shell";
pub const LOCAL_SHELL_TOOL_NAME: &str = "local_shell";
pub const CONTAINER_EXEC_TOOL_NAME: &str = "container_exec";
pub const LEGACY_CONTAINER_EXEC_TOOL_NAME: &str = "container.exec";
pub const WAIT_WITH_PREDICATE_TOOL_NAME: &str = "wait_with_predicate";
pub const LEGACY_CODEX_WAIT_UNDERSCORE_TOOL_NAME: &str = "codex_wait";
pub const LEGACY_CODEX_WAIT_DOTTED_TOOL_NAME: &str = "codex.wait";

pub fn normalize_tool_name(name: &str) -> Cow<'_, str> {
    match name {
        LEGACY_CONTAINER_EXEC_TOOL_NAME => Cow::Borrowed(CONTAINER_EXEC_TOOL_NAME),
        LEGACY_CODEX_WAIT_UNDERSCORE_TOOL_NAME | LEGACY_CODEX_WAIT_DOTTED_TOOL_NAME => {
            Cow::Borrowed(WAIT_WITH_PREDICATE_TOOL_NAME)
        }
        _ => Cow::Borrowed(name),
    }
}

pub fn is_shell_alias(name: &str) -> bool {
    matches!(
        name,
        SHELL_TOOL_NAME
            | LOCAL_SHELL_TOOL_NAME
            | CONTAINER_EXEC_TOOL_NAME
            | LEGACY_CONTAINER_EXEC_TOOL_NAME
    )
}

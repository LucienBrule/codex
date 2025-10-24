use crate::client_common::tools::ResponsesApiTool;
use crate::client_common::tools::ToolSpec;
use crate::model_family::ModelFamily;
use crate::tools::handlers::MAILBOX_READ_TOOL_NAME;
use crate::tools::handlers::MAILBOX_SEND_TOOL_NAME;
use crate::tools::handlers::MAILBOX_WAIT_TOOL_NAME;
use crate::tools::handlers::PLAN_TOOL;
use crate::tools::handlers::apply_patch::ApplyPatchToolType;
use crate::tools::handlers::apply_patch::create_apply_patch_freeform_tool;
use crate::tools::handlers::apply_patch::create_apply_patch_json_tool;
use crate::tools::names::CONTAINER_EXEC_TOOL_NAME;
use crate::tools::names::LEGACY_CODEX_WAIT_DOTTED_TOOL_NAME;
use crate::tools::names::LEGACY_CODEX_WAIT_UNDERSCORE_TOOL_NAME;
use crate::tools::names::LEGACY_CONTAINER_EXEC_TOOL_NAME;
use crate::tools::names::LOCAL_SHELL_TOOL_NAME;
use crate::tools::names::SHELL_TOOL_NAME;
use crate::tools::names::WAIT_WITH_PREDICATE_TOOL_NAME;
use crate::tools::registry::ToolRegistryBuilder;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum ConfigShellToolType {
    Default,
    Local,
    Streamable,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolsConfig {
    pub shell_type: ConfigShellToolType,
    pub plan_tool: bool,
    pub apply_patch_tool_type: Option<ApplyPatchToolType>,
    pub web_search_request: bool,
    pub include_view_image_tool: bool,
    pub experimental_unified_exec_tool: bool,
    pub experimental_supported_tools: Vec<String>,
}

pub(crate) struct ToolsConfigParams<'a> {
    pub(crate) model_family: &'a ModelFamily,
    pub(crate) include_plan_tool: bool,
    pub(crate) include_apply_patch_tool: bool,
    pub(crate) include_web_search_request: bool,
    pub(crate) use_streamable_shell_tool: bool,
    pub(crate) include_view_image_tool: bool,
    pub(crate) experimental_unified_exec_tool: bool,
}

impl ToolsConfig {
    pub fn new(params: &ToolsConfigParams) -> Self {
        let ToolsConfigParams {
            model_family,
            include_plan_tool,
            include_apply_patch_tool,
            include_web_search_request,
            use_streamable_shell_tool,
            include_view_image_tool,
            experimental_unified_exec_tool,
        } = params;
        let shell_type = if *use_streamable_shell_tool {
            ConfigShellToolType::Streamable
        } else if model_family.uses_local_shell_tool {
            ConfigShellToolType::Local
        } else {
            ConfigShellToolType::Default
        };

        let apply_patch_tool_type = match model_family.apply_patch_tool_type {
            Some(ApplyPatchToolType::Freeform) => Some(ApplyPatchToolType::Freeform),
            Some(ApplyPatchToolType::Function) => Some(ApplyPatchToolType::Function),
            None => {
                if *include_apply_patch_tool {
                    Some(ApplyPatchToolType::Freeform)
                } else {
                    None
                }
            }
        };

        Self {
            shell_type,
            plan_tool: *include_plan_tool,
            apply_patch_tool_type,
            web_search_request: *include_web_search_request,
            include_view_image_tool: *include_view_image_tool,
            experimental_unified_exec_tool: *experimental_unified_exec_tool,
            experimental_supported_tools: model_family.experimental_supported_tools.clone(),
        }
    }
}

/// Generic JSON‑Schema subset needed for our tool definitions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum JsonSchema {
    Boolean {
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    String {
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    /// MCP schema allows "number" | "integer" for Number
    #[serde(alias = "integer")]
    Number {
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Array {
        items: Box<JsonSchema>,

        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Object {
        properties: BTreeMap<String, JsonSchema>,
        #[serde(skip_serializing_if = "Option::is_none")]
        required: Option<Vec<String>>,
        #[serde(
            rename = "additionalProperties",
            skip_serializing_if = "Option::is_none"
        )]
        additional_properties: Option<AdditionalProperties>,
    },
}

/// Whether additional properties are allowed, and if so, any required schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum AdditionalProperties {
    Boolean(bool),
    Schema(Box<JsonSchema>),
}

impl From<bool> for AdditionalProperties {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<JsonSchema> for AdditionalProperties {
    fn from(s: JsonSchema) -> Self {
        Self::Schema(Box::new(s))
    }
}

fn create_unified_exec_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "input".to_string(),
        JsonSchema::Array {
            items: Box::new(JsonSchema::String { description: None }),
            description: Some(
                "When no session_id is provided, treat the array as the command and arguments \
                 to launch. When session_id is set, concatenate the strings (in order) and write \
                 them to the session's stdin."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "session_id".to_string(),
        JsonSchema::String {
            description: Some(
                "Identifier for an existing interactive session. If omitted, a new command \
                 is spawned."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "timeout_ms".to_string(),
        JsonSchema::Number {
            description: Some(
                "Maximum time in milliseconds to wait for output after writing the input."
                    .to_string(),
            ),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "unified_exec".to_string(),
        description:
            "Runs a command in a PTY. Provide a session_id to reuse an existing interactive session.".to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["input".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_shell_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "command".to_string(),
        JsonSchema::Array {
            items: Box::new(JsonSchema::String { description: None }),
            description: Some("The command to execute".to_string()),
        },
    );
    properties.insert(
        "workdir".to_string(),
        JsonSchema::String {
            description: Some("The working directory to execute the command in".to_string()),
        },
    );
    properties.insert(
        "timeout_ms".to_string(),
        JsonSchema::Number {
            description: Some("The timeout for the command in milliseconds".to_string()),
        },
    );

    properties.insert(
        "with_escalated_permissions".to_string(),
        JsonSchema::Boolean {
            description: Some("Whether to request escalated permissions. Set to true if command needs to be run without sandbox restrictions".to_string()),
        },
    );
    properties.insert(
        "justification".to_string(),
        JsonSchema::String {
            description: Some("Only set if with_escalated_permissions is true. 1-sentence explanation of why we want to run this command.".to_string()),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "shell".to_string(),
        description: "Runs a shell command and returns its output.".to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["command".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_view_image_tool() -> ToolSpec {
    // Support only local filesystem path.
    let mut properties = BTreeMap::new();
    properties.insert(
        "path".to_string(),
        JsonSchema::String {
            description: Some("Local filesystem path to an image file".to_string()),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "view_image".to_string(),
        description:
            "Attach a local image (by filesystem path) to the conversation context for this turn."
                .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["path".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn mailbox_body_schema() -> JsonSchema {
    let mut body_properties = BTreeMap::new();
    body_properties.insert(
        "subject".to_string(),
        JsonSchema::String {
            description: Some("Subject line shown in mailbox inbox listings.".to_string()),
        },
    );
    body_properties.insert(
        "content".to_string(),
        JsonSchema::String {
            description: Some(
                "Primary message content. Use text/plain unless content_type overrides it."
                    .to_string(),
            ),
        },
    );
    body_properties.insert(
        "content_type".to_string(),
        JsonSchema::String {
            description: Some(
                "MIME type for the content (text/plain, text/markdown, application/json). Defaults to text/plain.".to_string(),
            ),
        },
    );

    JsonSchema::Object {
        properties: body_properties,
        required: Some(vec!["subject".to_string(), "content".to_string()]),
        additional_properties: Some(true.into()),
    }
}

fn mailbox_sender_schema() -> JsonSchema {
    let mut sender_properties = BTreeMap::new();
    sender_properties.insert(
        "id".to_string(),
        JsonSchema::String {
            description: Some("Sender identifier (e.g., orchestrator.codex).".to_string()),
        },
    );
    sender_properties.insert(
        "role".to_string(),
        JsonSchema::String {
            description: Some(
                "Sender role (system, orchestrator, operator, or automation).".to_string(),
            ),
        },
    );
    sender_properties.insert(
        "display_name".to_string(),
        JsonSchema::String {
            description: Some("Optional human-friendly sender name.".to_string()),
        },
    );
    sender_properties.insert(
        "contact".to_string(),
        JsonSchema::String {
            description: Some("Optional contact URI (mailto, slack channel, etc.).".to_string()),
        },
    );

    JsonSchema::Object {
        properties: sender_properties,
        required: Some(vec!["id".to_string(), "role".to_string()]),
        additional_properties: Some(true.into()),
    }
}

fn mailbox_audit_schema() -> JsonSchema {
    let mut audit_properties = BTreeMap::new();
    audit_properties.insert(
        "request_id".to_string(),
        JsonSchema::String {
            description: Some(
                "Stable identifier used to correlate audit events (required).".to_string(),
            ),
        },
    );
    audit_properties.insert(
        "change_ticket".to_string(),
        JsonSchema::String {
            description: Some(
                "Change ticket reference required for high/critical traffic.".to_string(),
            ),
        },
    );
    audit_properties.insert(
        "created_by".to_string(),
        JsonSchema::String {
            description: Some("Human readable attribution for the sender.".to_string()),
        },
    );
    audit_properties.insert(
        "justification".to_string(),
        JsonSchema::String {
            description: Some(
                "Operational justification required for elevated priority or rate overrides."
                    .to_string(),
            ),
        },
    );

    JsonSchema::Object {
        properties: audit_properties,
        required: Some(vec!["request_id".to_string()]),
        additional_properties: Some(true.into()),
    }
}

fn mailbox_audience_schema() -> JsonSchema {
    let mut audience_properties = BTreeMap::new();
    audience_properties.insert(
        "conversation_id".to_string(),
        JsonSchema::String {
            description: Some("Target conversation UUID (omit when using `to`).".to_string()),
        },
    );
    audience_properties.insert(
        "worker_id".to_string(),
        JsonSchema::String {
            description: Some("Restrict delivery to a specific worker_id.".to_string()),
        },
    );
    audience_properties.insert(
        "allow_broadcast".to_string(),
        JsonSchema::Boolean {
            description: Some(
                "Allow broadcast to multiple recipients (default false).".to_string(),
            ),
        },
    );

    JsonSchema::Object {
        properties: audience_properties,
        required: None,
        additional_properties: Some(true.into()),
    }
}

fn mailbox_ack_policy_schema() -> JsonSchema {
    let mut ack_properties = BTreeMap::new();
    ack_properties.insert(
        "mode".to_string(),
        JsonSchema::String {
            description: Some(
                "Ack policy mode (none, passive, required). Defaults to passive.".to_string(),
            ),
        },
    );
    ack_properties.insert(
        "deadline".to_string(),
        JsonSchema::String {
            description: Some("RFC3339 deadline for acknowledgement.".to_string()),
        },
    );
    ack_properties.insert(
        "auto_ack_seconds".to_string(),
        JsonSchema::Number {
            description: Some("Passive auto-ack timer in seconds.".to_string()),
        },
    );
    ack_properties.insert(
        "escalation_ticket".to_string(),
        JsonSchema::String {
            description: Some(
                "Escalation ticket reference when ack.mode=required with high/critical priority."
                    .to_string(),
            ),
        },
    );

    JsonSchema::Object {
        properties: ack_properties,
        required: None,
        additional_properties: Some(true.into()),
    }
}

fn mailbox_message_schema() -> JsonSchema {
    let mut message_properties = BTreeMap::new();
    message_properties.insert(
        "message_id".to_string(),
        JsonSchema::String {
            description: Some(
                "Optional UUID for the message; defaults to v7 when omitted.".to_string(),
            ),
        },
    );
    message_properties.insert(
        "priority".to_string(),
        JsonSchema::String {
            description: Some(
                "Priority level (critical, high, normal, low). Defaults to normal.".to_string(),
            ),
        },
    );
    message_properties.insert("sender".to_string(), mailbox_sender_schema());
    message_properties.insert("audience".to_string(), mailbox_audience_schema());
    message_properties.insert("body".to_string(), mailbox_body_schema());
    message_properties.insert("ack_policy".to_string(), mailbox_ack_policy_schema());
    message_properties.insert("audit".to_string(), mailbox_audit_schema());
    message_properties.insert(
        "tags".to_string(),
        JsonSchema::Object {
            properties: BTreeMap::new(),
            required: None,
            additional_properties: Some(
                JsonSchema::String {
                    description: Some("Tag values stored as key/value pairs.".to_string()),
                }
                .into(),
            ),
        },
    );
    message_properties.insert(
        "metadata".to_string(),
        JsonSchema::Object {
            properties: BTreeMap::new(),
            required: None,
            additional_properties: Some(true.into()),
        },
    );

    JsonSchema::Object {
        properties: message_properties,
        required: Some(vec![
            "sender".to_string(),
            "body".to_string(),
            "audit".to_string(),
        ]),
        additional_properties: Some(true.into()),
    }
}

fn create_mailbox_send_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert("message".to_string(), mailbox_message_schema());
    properties.insert(
        "to".to_string(),
        JsonSchema::String {
            description: Some(
                "Logical contact name to target (resolved via codex_home/<ns>/contacts.toml)."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "conversation_id".to_string(),
        JsonSchema::String {
            description: Some(
                "Target conversation UUID. If provided, takes precedence over 'to'.".to_string(),
            ),
        },
    );
    properties.insert(
        "ack_mode".to_string(),
        JsonSchema::String {
            description: Some(
                "Acknowledgement policy override (`none`, `passive`, or `required`). \
                 Defaults to the message payload when omitted."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "ack_deadline".to_string(),
        JsonSchema::String {
            description: Some(
                "RFC3339 deadline for acknowledgements when ack_mode is provided.".to_string(),
            ),
        },
    );
    properties.insert(
        "ack_auto_seconds".to_string(),
        JsonSchema::Number {
            description: Some(
                "Auto-acknowledge after N seconds (valid when ack_mode is passive).".to_string(),
            ),
        },
    );
    properties.insert(
        "ack_escalation_ticket".to_string(),
        JsonSchema::String {
            description: Some(
                "Escalation ticket identifier, required for required ACKs at high/critical priority."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "timeout_seconds".to_string(),
        JsonSchema::Number {
            description: Some(
                "Seconds to wait for mailbox delivery acknowledgement (default 30).".to_string(),
            ),
        },
    );
    properties.insert(
        "wait_for_delivery".to_string(),
        JsonSchema::Boolean {
            description: Some(
                "When false, return immediately after enqueueing without waiting for delivery."
                    .to_string(),
            ),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: MAILBOX_SEND_TOOL_NAME.to_string(),
        description: "Enqueue a mailbox message for any Codex contact or conversation ID."
            .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["message".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_mailbox_send_alias_tool() -> ToolSpec {
    // Legacy alias with identical schema under the old name
    let mut properties = BTreeMap::new();
    properties.insert("message".to_string(), mailbox_message_schema());
    properties.insert(
        "to".to_string(),
        JsonSchema::String {
            description: Some(
                "Logical contact name to target (resolved via codex_home/<ns>/contacts.toml)."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "conversation_id".to_string(),
        JsonSchema::String {
            description: Some(
                "Target conversation UUID. If provided, takes precedence over 'to'.".to_string(),
            ),
        },
    );
    properties.insert(
        "ack_mode".to_string(),
        JsonSchema::String {
            description: Some(
                "Acknowledgement policy override (`none`, `passive`, or `required`). \
                 Defaults to the message payload when omitted."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "ack_deadline".to_string(),
        JsonSchema::String {
            description: Some(
                "RFC3339 deadline for acknowledgements when ack_mode is provided.".to_string(),
            ),
        },
    );
    properties.insert(
        "ack_auto_seconds".to_string(),
        JsonSchema::Number {
            description: Some(
                "Auto-acknowledge after N seconds (valid when ack_mode is passive).".to_string(),
            ),
        },
    );
    properties.insert(
        "ack_escalation_ticket".to_string(),
        JsonSchema::String {
            description: Some(
                "Escalation ticket identifier, required for required ACKs at high/critical priority."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "timeout_seconds".to_string(),
        JsonSchema::Number {
            description: Some(
                "Seconds to wait for mailbox delivery acknowledgement (default 30).".to_string(),
            ),
        },
    );
    properties.insert(
        "wait_for_delivery".to_string(),
        JsonSchema::Boolean {
            description: Some(
                "When false, return immediately after enqueueing without waiting for delivery."
                    .to_string(),
            ),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "codex_mailbox_send".to_string(),
        description: "Enqueue a mailbox message (legacy alias).".to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["message".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_mailbox_wait_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "timeout_ms".to_string(),
        JsonSchema::Number {
            description: Some(
                "Maximum overall wait duration in milliseconds (default 30_000).".to_string(),
            ),
        },
    );
    properties.insert(
        "expected_subject".to_string(),
        JsonSchema::String {
            description: Some(
                "Match mailbox subject exactly (case-insensitive by default).".to_string(),
            ),
        },
    );
    properties.insert(
        "subject_contains".to_string(),
        JsonSchema::String {
            description: Some(
                "Match mailbox subject containing the provided substring.".to_string(),
            ),
        },
    );
    properties.insert(
        "case_sensitive".to_string(),
        JsonSchema::Boolean {
            description: Some(
                "Set to true to perform case-sensitive subject matching.".to_string(),
            ),
        },
    );
    properties.insert(
        "from_handle".to_string(),
        JsonSchema::String {
            description: Some(
                "Restrict to messages sent by the specified contact handle.".to_string(),
            ),
        },
    );
    properties.insert(
        "sender_id".to_string(),
        JsonSchema::String {
            description: Some(
                "Restrict to messages whose sender.id matches this value.".to_string(),
            ),
        },
    );
    properties.insert(
        "request_id".to_string(),
        JsonSchema::String {
            description: Some("Restrict to messages with a matching audit.request_id.".to_string()),
        },
    );
    properties.insert(
        "message_id".to_string(),
        JsonSchema::String {
            description: Some("Restrict to a specific mailbox message UUID.".to_string()),
        },
    );
    properties.insert(
        "states".to_string(),
        JsonSchema::Array {
            description: Some(
                "Optional allowed mailbox delivery states (e.g., ['enqueued', 'delivered'])."
                    .to_string(),
            ),
            items: Box::new(JsonSchema::String { description: None }),
        },
    );
    properties.insert(
        "ingress".to_string(),
        JsonSchema::Array {
            description: Some(
                "Optional allowed ingress sources (e.g., ['mcp', 'cli']).".to_string(),
            ),
            items: Box::new(JsonSchema::String { description: None }),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: MAILBOX_WAIT_TOOL_NAME.to_string(),
        description: "Waits for a mailbox delivery that matches subject and sender filters."
            .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: None,
            additional_properties: Some(false.into()),
        },
    })
}

fn create_mailbox_read_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "max".to_string(),
        JsonSchema::Number {
            description: Some("Maximum messages to return (default 50).".to_string()),
        },
    );
    properties.insert(
        "ack".to_string(),
        JsonSchema::Boolean {
            description: Some("When true, mark returned messages as read/acked.".to_string()),
        },
    );

    let mut filter_props = BTreeMap::new();
    filter_props.insert(
        "from".to_string(),
        JsonSchema::String {
            description: Some("Filter by sender.id".to_string()),
        },
    );
    filter_props.insert(
        "subject_contains".to_string(),
        JsonSchema::String {
            description: Some("Substring match on subject".to_string()),
        },
    );
    filter_props.insert(
        "since".to_string(),
        JsonSchema::String {
            description: Some("RFC3339 timestamp".to_string()),
        },
    );
    filter_props.insert(
        "conversation_id".to_string(),
        JsonSchema::String {
            description: Some("Conversation ID override (UUID)".to_string()),
        },
    );

    properties.insert(
        "filter".to_string(),
        JsonSchema::Object {
            properties: filter_props,
            required: None,
            additional_properties: Some(false.into()),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: MAILBOX_READ_TOOL_NAME.to_string(),
        description: "List unread mailbox messages and optionally acknowledge them.".to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: None,
            additional_properties: Some(false.into()),
        },
    })
}

fn create_test_sync_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "sleep_before_ms".to_string(),
        JsonSchema::Number {
            description: Some("Optional delay in milliseconds before any other action".to_string()),
        },
    );
    properties.insert(
        "sleep_after_ms".to_string(),
        JsonSchema::Number {
            description: Some(
                "Optional delay in milliseconds after completing the barrier".to_string(),
            ),
        },
    );

    let mut barrier_properties = BTreeMap::new();
    barrier_properties.insert(
        "id".to_string(),
        JsonSchema::String {
            description: Some(
                "Identifier shared by concurrent calls that should rendezvous".to_string(),
            ),
        },
    );
    barrier_properties.insert(
        "participants".to_string(),
        JsonSchema::Number {
            description: Some(
                "Number of tool calls that must arrive before the barrier opens".to_string(),
            ),
        },
    );
    barrier_properties.insert(
        "timeout_ms".to_string(),
        JsonSchema::Number {
            description: Some("Maximum time in milliseconds to wait at the barrier".to_string()),
        },
    );

    properties.insert(
        "barrier".to_string(),
        JsonSchema::Object {
            properties: barrier_properties,
            required: Some(vec!["id".to_string(), "participants".to_string()]),
            additional_properties: Some(false.into()),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "test_sync_tool".to_string(),
        description: "Internal synchronization helper used by Codex integration tests.".to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: None,
            additional_properties: Some(false.into()),
        },
    })
}

fn create_grep_files_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "pattern".to_string(),
        JsonSchema::String {
            description: Some("Regular expression pattern to search for.".to_string()),
        },
    );
    properties.insert(
        "include".to_string(),
        JsonSchema::String {
            description: Some(
                "Optional glob that limits which files are searched (e.g. \"*.rs\" or \
                 \"*.{ts,tsx}\")."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "path".to_string(),
        JsonSchema::String {
            description: Some(
                "Directory or file path to search. Defaults to the session's working directory."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "limit".to_string(),
        JsonSchema::Number {
            description: Some(
                "Maximum number of file paths to return (defaults to 100).".to_string(),
            ),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "grep_files".to_string(),
        description: "Finds files whose contents match the pattern and lists them by modification \
                      time."
            .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["pattern".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_read_file_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "file_path".to_string(),
        JsonSchema::String {
            description: Some("Absolute path to the file".to_string()),
        },
    );
    properties.insert(
        "offset".to_string(),
        JsonSchema::Number {
            description: Some(
                "The line number to start reading from. Must be 1 or greater.".to_string(),
            ),
        },
    );
    properties.insert(
        "limit".to_string(),
        JsonSchema::Number {
            description: Some("The maximum number of lines to return.".to_string()),
        },
    );
    properties.insert(
        "mode".to_string(),
        JsonSchema::String {
            description: Some(
                "Optional mode selector: \"slice\" for simple ranges (default) or \"indentation\" \
                 to expand around an anchor line."
                    .to_string(),
            ),
        },
    );

    let mut indentation_properties = BTreeMap::new();
    indentation_properties.insert(
        "anchor_line".to_string(),
        JsonSchema::Number {
            description: Some(
                "Anchor line to center the indentation lookup on (defaults to offset).".to_string(),
            ),
        },
    );
    indentation_properties.insert(
        "max_levels".to_string(),
        JsonSchema::Number {
            description: Some(
                "How many parent indentation levels (smaller indents) to include.".to_string(),
            ),
        },
    );
    indentation_properties.insert(
        "include_siblings".to_string(),
        JsonSchema::Boolean {
            description: Some(
                "When true, include additional blocks that share the anchor indentation."
                    .to_string(),
            ),
        },
    );
    indentation_properties.insert(
        "include_header".to_string(),
        JsonSchema::Boolean {
            description: Some(
                "Include doc comments or attributes directly above the selected block.".to_string(),
            ),
        },
    );
    indentation_properties.insert(
        "max_lines".to_string(),
        JsonSchema::Number {
            description: Some(
                "Hard cap on the number of lines returned when using indentation mode.".to_string(),
            ),
        },
    );
    properties.insert(
        "indentation".to_string(),
        JsonSchema::Object {
            properties: indentation_properties,
            required: None,
            additional_properties: Some(false.into()),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "read_file".to_string(),
        description:
            "Reads a local file with 1-indexed line numbers, supporting slice and indentation-aware block modes."
                .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["file_path".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_list_dir_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "dir_path".to_string(),
        JsonSchema::String {
            description: Some("Absolute path to the directory to list.".to_string()),
        },
    );
    properties.insert(
        "offset".to_string(),
        JsonSchema::Number {
            description: Some(
                "The entry number to start listing from. Must be 1 or greater.".to_string(),
            ),
        },
    );
    properties.insert(
        "limit".to_string(),
        JsonSchema::Number {
            description: Some("The maximum number of entries to return.".to_string()),
        },
    );
    properties.insert(
        "depth".to_string(),
        JsonSchema::Number {
            description: Some(
                "The maximum directory depth to traverse. Must be 1 or greater.".to_string(),
            ),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: "list_dir".to_string(),
        description:
            "Lists entries in a local directory with 1-indexed entry numbers and simple type labels."
                .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["dir_path".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}

fn create_wait_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "type".to_string(),
        JsonSchema::String {
            description: Some(
                "Predicate kind to evaluate. Supported values: timer, filesystem, shell."
                    .to_string(),
            ),
        },
    );
    properties.insert(
        "predicate".to_string(),
        JsonSchema::Object {
            properties: BTreeMap::new(),
            required: None,
            additional_properties: Some(true.into()),
        },
    );
    properties.insert(
        "timeout_ms".to_string(),
        JsonSchema::Number {
            description: Some(
                "Maximum overall wait duration in milliseconds (default 300_000, max 3_600_000)."
                    .to_string(),
            ),
        },
    );

    ToolSpec::Function(ResponsesApiTool {
        name: WAIT_WITH_PREDICATE_TOOL_NAME.to_string(),
        description: "Waits for a predicate to be satisfied without blocking the turn. Supports timer, filesystem, and shell predicates."
            .to_string(),
        strict: false,
        parameters: JsonSchema::Object {
            properties,
            required: Some(vec!["type".to_string(), "predicate".to_string()]),
            additional_properties: Some(false.into()),
        },
    })
}
/// TODO(dylan): deprecate once we get rid of json tool
#[derive(Serialize, Deserialize)]
pub(crate) struct ApplyPatchToolArgs {
    pub(crate) input: String,
}

/// Returns JSON values that are compatible with Function Calling in the
/// Responses API:
/// https://platform.openai.com/docs/guides/function-calling?api-mode=responses
pub fn create_tools_json_for_responses_api(
    tools: &[ToolSpec],
) -> crate::error::Result<Vec<serde_json::Value>> {
    let mut tools_json = Vec::new();

    for tool in tools {
        let json = serde_json::to_value(tool)?;
        tools_json.push(json);
    }

    Ok(tools_json)
}
/// Returns JSON values that are compatible with Function Calling in the
/// Chat Completions API:
/// https://platform.openai.com/docs/guides/function-calling?api-mode=chat
pub(crate) fn create_tools_json_for_chat_completions_api(
    tools: &[ToolSpec],
) -> crate::error::Result<Vec<serde_json::Value>> {
    // We start with the JSON for the Responses API and than rewrite it to match
    // the chat completions tool call format.
    let responses_api_tools_json = create_tools_json_for_responses_api(tools)?;
    let tools_json = responses_api_tools_json
        .into_iter()
        .filter_map(|mut tool| {
            if tool.get("type") != Some(&serde_json::Value::String("function".to_string())) {
                return None;
            }

            if let Some(map) = tool.as_object_mut() {
                // Remove "type" field as it is not needed in chat completions.
                map.remove("type");
                Some(json!({
                    "type": "function",
                    "function": map,
                }))
            } else {
                None
            }
        })
        .collect::<Vec<serde_json::Value>>();
    Ok(tools_json)
}

pub(crate) fn mcp_tool_to_openai_tool(
    fully_qualified_name: String,
    tool: mcp_types::Tool,
) -> Result<ResponsesApiTool, serde_json::Error> {
    let mcp_types::Tool {
        description,
        mut input_schema,
        ..
    } = tool;

    // OpenAI models mandate the "properties" field in the schema. The Agents
    // SDK fixed this by inserting an empty object for "properties" if it is not
    // already present https://github.com/openai/openai-agents-python/issues/449
    // so here we do the same.
    if input_schema.properties.is_none() {
        input_schema.properties = Some(serde_json::Value::Object(serde_json::Map::new()));
    }

    // Serialize to a raw JSON value so we can sanitize schemas coming from MCP
    // servers. Some servers omit the top-level or nested `type` in JSON
    // Schemas (e.g. using enum/anyOf), or use unsupported variants like
    // `integer`. Our internal JsonSchema is a small subset and requires
    // `type`, so we coerce/sanitize here for compatibility.
    let mut serialized_input_schema = serde_json::to_value(input_schema)?;
    sanitize_json_schema(&mut serialized_input_schema);
    let input_schema = serde_json::from_value::<JsonSchema>(serialized_input_schema)?;

    let sanitized_name = sanitize_tool_name(&fully_qualified_name);
    if sanitized_name != fully_qualified_name {
        tracing::warn!(
            original = %fully_qualified_name,
            sanitized = %sanitized_name,
            "sanitizing MCP tool name"
        );
    }

    Ok(ResponsesApiTool {
        name: sanitized_name,
        description: description.unwrap_or_default(),
        strict: false,
        parameters: input_schema,
    })
}

/// Sanitize a JSON Schema (as serde_json::Value) so it can fit our limited
/// JsonSchema enum. This function:
/// - Ensures every schema object has a "type". If missing, infers it from
///   common keywords (properties => object, items => array, enum/const/format => string)
///   and otherwise defaults to "string".
/// - Fills required child fields (e.g. array items, object properties) with
///   permissive defaults when absent.
fn sanitize_json_schema(value: &mut JsonValue) {
    match value {
        JsonValue::Bool(_) => {
            // JSON Schema boolean form: true/false. Coerce to an accept-all string.
            *value = json!({ "type": "string" });
        }
        JsonValue::Array(arr) => {
            for v in arr.iter_mut() {
                sanitize_json_schema(v);
            }
        }
        JsonValue::Object(map) => {
            // First, recursively sanitize known nested schema holders
            if let Some(props) = map.get_mut("properties")
                && let Some(props_map) = props.as_object_mut()
            {
                for (_k, v) in props_map.iter_mut() {
                    sanitize_json_schema(v);
                }
            }
            if let Some(items) = map.get_mut("items") {
                sanitize_json_schema(items);
            }
            // Some schemas use oneOf/anyOf/allOf - sanitize their entries
            for combiner in ["oneOf", "anyOf", "allOf", "prefixItems"] {
                if let Some(v) = map.get_mut(combiner) {
                    sanitize_json_schema(v);
                }
            }

            // Normalize/ensure type
            let mut ty = map.get("type").and_then(|v| v.as_str()).map(str::to_string);

            // If type is an array (union), pick first supported; else leave to inference
            if ty.is_none()
                && let Some(JsonValue::Array(types)) = map.get("type")
            {
                for t in types {
                    if let Some(tt) = t.as_str()
                        && matches!(
                            tt,
                            "object" | "array" | "string" | "number" | "integer" | "boolean"
                        )
                    {
                        ty = Some(tt.to_string());
                        break;
                    }
                }
            }

            // Infer type if still missing
            if ty.is_none() {
                if map.contains_key("properties")
                    || map.contains_key("required")
                    || map.contains_key("additionalProperties")
                {
                    ty = Some("object".to_string());
                } else if map.contains_key("items") || map.contains_key("prefixItems") {
                    ty = Some("array".to_string());
                } else if map.contains_key("enum")
                    || map.contains_key("const")
                    || map.contains_key("format")
                {
                    ty = Some("string".to_string());
                } else if map.contains_key("minimum")
                    || map.contains_key("maximum")
                    || map.contains_key("exclusiveMinimum")
                    || map.contains_key("exclusiveMaximum")
                    || map.contains_key("multipleOf")
                {
                    ty = Some("number".to_string());
                }
            }
            // If we still couldn't infer, default to string
            let ty = ty.unwrap_or_else(|| "string".to_string());
            map.insert("type".to_string(), JsonValue::String(ty.to_string()));

            // Ensure object schemas have properties map
            if ty == "object" {
                if !map.contains_key("properties") {
                    map.insert(
                        "properties".to_string(),
                        JsonValue::Object(serde_json::Map::new()),
                    );
                }
                // If additionalProperties is an object schema, sanitize it too.
                // Leave booleans as-is, since JSON Schema allows boolean here.
                if let Some(ap) = map.get_mut("additionalProperties") {
                    let is_bool = matches!(ap, JsonValue::Bool(_));
                    if !is_bool {
                        sanitize_json_schema(ap);
                    }
                }
            }

            // Ensure array schemas have items
            if ty == "array" && !map.contains_key("items") {
                map.insert("items".to_string(), json!({ "type": "string" }));
            }
        }
        _ => {}
    }
}

pub(crate) fn sanitize_tool_name(name: &str) -> String {
    name.chars()
        .map(|ch| match ch {
            ch if ch.is_ascii_alphanumeric() => ch,
            '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

pub(crate) fn is_valid_tool_name(name: &str) -> bool {
    name.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

/// Builds the tool registry builder while collecting tool specs for later serialization.
pub(crate) fn build_specs(
    config: &ToolsConfig,
    mcp_tools: Option<HashMap<String, mcp_types::Tool>>,
) -> ToolRegistryBuilder {
    use crate::exec_command::EXEC_COMMAND_TOOL_NAME;
    use crate::exec_command::WRITE_STDIN_TOOL_NAME;
    use crate::exec_command::create_exec_command_tool_for_responses_api;
    use crate::exec_command::create_write_stdin_tool_for_responses_api;
    use crate::tools::handlers::ApplyPatchHandler;
    use crate::tools::handlers::ExecStreamHandler;
    use crate::tools::handlers::GrepFilesHandler;
    use crate::tools::handlers::ListDirHandler;
    use crate::tools::handlers::MAILBOX_SEND_TOOL_NAME;
    use crate::tools::handlers::MAILBOX_WAIT_TOOL_NAME;
    use crate::tools::handlers::MailboxSendHandler;
    use crate::tools::handlers::MailboxWaitHandler;
    use crate::tools::handlers::McpHandler;
    use crate::tools::handlers::PlanHandler;
    use crate::tools::handlers::ReadFileHandler;
    use crate::tools::handlers::ShellHandler;
    use crate::tools::handlers::TestSyncHandler;
    use crate::tools::handlers::UnifiedExecHandler;
    use crate::tools::handlers::ViewImageHandler;
    use crate::tools::handlers::WaitHandler;
    use std::sync::Arc;

    let mut builder = ToolRegistryBuilder::new();

    let shell_handler = Arc::new(ShellHandler);
    let exec_stream_handler = Arc::new(ExecStreamHandler);
    let unified_exec_handler = Arc::new(UnifiedExecHandler);
    let plan_handler = Arc::new(PlanHandler);
    let apply_patch_handler = Arc::new(ApplyPatchHandler);
    let view_image_handler = Arc::new(ViewImageHandler);
    let mcp_handler = Arc::new(McpHandler);
    let mailbox_send_handler = Arc::new(MailboxSendHandler);
    let mailbox_wait_handler = Arc::new(MailboxWaitHandler);
    let mailbox_read_handler = Arc::new(crate::tools::handlers::MailboxReadHandler);
    let wait_handler = Arc::new(WaitHandler);

    if config.experimental_unified_exec_tool {
        builder.push_spec(create_unified_exec_tool());
        builder.register_handler("unified_exec", unified_exec_handler);
    } else {
        match &config.shell_type {
            ConfigShellToolType::Default => {
                builder.push_spec(create_shell_tool());
            }
            ConfigShellToolType::Local => {
                builder.push_spec(ToolSpec::LocalShell {});
            }
            ConfigShellToolType::Streamable => {
                builder.push_spec(ToolSpec::Function(
                    create_exec_command_tool_for_responses_api(),
                ));
                builder.push_spec(ToolSpec::Function(
                    create_write_stdin_tool_for_responses_api(),
                ));
                builder.register_handler(EXEC_COMMAND_TOOL_NAME, exec_stream_handler.clone());
                builder.register_handler(WRITE_STDIN_TOOL_NAME, exec_stream_handler);
            }
        }
    }

    // Always register shell aliases so older prompts remain compatible.
    builder.register_handler(SHELL_TOOL_NAME, shell_handler.clone());
    builder.register_handler(CONTAINER_EXEC_TOOL_NAME, shell_handler.clone());
    builder.register_handler(LEGACY_CONTAINER_EXEC_TOOL_NAME, shell_handler.clone());
    builder.register_handler(LOCAL_SHELL_TOOL_NAME, shell_handler);

    if config.plan_tool {
        builder.push_spec(PLAN_TOOL.clone());
        builder.register_handler("update_plan", plan_handler);
    }

    builder.push_spec(create_mailbox_send_tool());
    // Back-compat alias for legacy prompts/tests
    builder.push_spec(create_mailbox_send_alias_tool());
    builder.register_handler(MAILBOX_SEND_TOOL_NAME, mailbox_send_handler.clone());
    builder.register_handler("codex_mailbox_send", mailbox_send_handler);

    builder.push_spec_with_parallel_support(create_mailbox_wait_tool(), true);
    builder.register_handler(MAILBOX_WAIT_TOOL_NAME, mailbox_wait_handler.clone());

    // New mailbox_read tool
    builder.push_spec(create_mailbox_read_tool());
    builder.register_handler(MAILBOX_READ_TOOL_NAME, mailbox_read_handler);

    builder.push_spec_with_parallel_support(create_wait_tool(), true);
    builder.register_handler(WAIT_WITH_PREDICATE_TOOL_NAME, wait_handler.clone());
    builder.register_handler(LEGACY_CODEX_WAIT_UNDERSCORE_TOOL_NAME, wait_handler.clone());
    builder.register_handler(LEGACY_CODEX_WAIT_DOTTED_TOOL_NAME, wait_handler);

    if let Some(apply_patch_tool_type) = &config.apply_patch_tool_type {
        match apply_patch_tool_type {
            ApplyPatchToolType::Freeform => {
                builder.push_spec(create_apply_patch_freeform_tool());
            }
            ApplyPatchToolType::Function => {
                builder.push_spec(create_apply_patch_json_tool());
            }
        }
        builder.register_handler("apply_patch", apply_patch_handler);
    }

    if config
        .experimental_supported_tools
        .contains(&"grep_files".to_string())
    {
        let grep_files_handler = Arc::new(GrepFilesHandler);
        builder.push_spec_with_parallel_support(create_grep_files_tool(), true);
        builder.register_handler("grep_files", grep_files_handler);
    }

    if config
        .experimental_supported_tools
        .contains(&"read_file".to_string())
    {
        let read_file_handler = Arc::new(ReadFileHandler);
        builder.push_spec_with_parallel_support(create_read_file_tool(), true);
        builder.register_handler("read_file", read_file_handler);
    }

    if config
        .experimental_supported_tools
        .iter()
        .any(|tool| tool == "list_dir")
    {
        let list_dir_handler = Arc::new(ListDirHandler);
        builder.push_spec_with_parallel_support(create_list_dir_tool(), true);
        builder.register_handler("list_dir", list_dir_handler);
    }

    if config
        .experimental_supported_tools
        .contains(&"test_sync_tool".to_string())
    {
        let test_sync_handler = Arc::new(TestSyncHandler);
        builder.push_spec_with_parallel_support(create_test_sync_tool(), true);
        builder.register_handler("test_sync_tool", test_sync_handler);
    }

    if config.web_search_request {
        builder.push_spec(ToolSpec::WebSearch {});
    }

    if config.include_view_image_tool {
        builder.push_spec_with_parallel_support(create_view_image_tool(), true);
        builder.register_handler("view_image", view_image_handler);
    }

    if let Some(mcp_tools) = mcp_tools {
        let mut entries: Vec<(String, mcp_types::Tool)> = mcp_tools.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (name, tool) in entries.into_iter() {
            match mcp_tool_to_openai_tool(name.clone(), tool.clone()) {
                Ok(converted_tool) => {
                    let sanitized_name = converted_tool.name.clone();
                    builder.push_spec(ToolSpec::Function(converted_tool));
                    builder.register_handler(sanitized_name, mcp_handler.clone());
                }
                Err(e) => {
                    tracing::error!("Failed to convert {name:?} MCP tool to OpenAI tool: {e:?}");
                }
            }
        }
    }

    builder
}

#[cfg(test)]
mod tests {
    use crate::client_common::tools::FreeformTool;
    use crate::model_family::find_family_for_model;
    use crate::tools::registry::ConfiguredToolSpec;
    use mcp_types::ToolInputSchema;
    use pretty_assertions::assert_eq;

    use super::*;

    fn tool_name(tool: &ToolSpec) -> &str {
        match tool {
            ToolSpec::Function(ResponsesApiTool { name, .. }) => name,
            ToolSpec::LocalShell {} => "local_shell",
            ToolSpec::WebSearch {} => "web_search",
            ToolSpec::Freeform(FreeformTool { name, .. }) => name,
        }
    }

    fn assert_eq_tool_names(tools: &[ConfiguredToolSpec], expected_names: &[&str]) {
        let tool_names = tools
            .iter()
            .map(|tool| tool_name(&tool.spec))
            .collect::<Vec<_>>();

        assert_eq!(
            tool_names.len(),
            expected_names.len(),
            "tool_name mismatch, {tool_names:?}, {expected_names:?}",
        );
        for (name, expected_name) in tool_names.iter().zip(expected_names.iter()) {
            assert_eq!(
                name, expected_name,
                "tool_name mismatch, {name:?}, {expected_name:?}"
            );
        }
    }

    fn find_tool<'a>(
        tools: &'a [ConfiguredToolSpec],
        expected_name: &str,
    ) -> &'a ConfiguredToolSpec {
        tools
            .iter()
            .find(|tool| tool_name(&tool.spec) == expected_name)
            .unwrap_or_else(|| panic!("expected tool {expected_name}"))
    }

    #[test]
    fn test_build_specs() {
        let model_family = find_family_for_model("codex-mini-latest")
            .expect("codex-mini-latest should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: true,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });
        let (tools, _) = build_specs(&config, Some(HashMap::new())).build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "update_plan",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "web_search",
                "view_image",
            ],
        );
    }

    #[test]
    fn test_build_specs_default_shell() {
        let model_family = find_family_for_model("o3").expect("o3 should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: true,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });
        let (tools, _) = build_specs(&config, Some(HashMap::new())).build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "update_plan",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "web_search",
                "view_image",
            ],
        );
    }

    #[test]
    #[ignore]
    fn test_parallel_support_flags() {
        let model_family = find_family_for_model("gpt-5-codex")
            .expect("codex-mini-latest should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: false,
            use_streamable_shell_tool: false,
            include_view_image_tool: false,
            experimental_unified_exec_tool: true,
        });
        let (tools, _) = build_specs(&config, None).build();

        assert!(!find_tool(&tools, "unified_exec").supports_parallel_tool_calls);
        assert!(find_tool(&tools, "grep_files").supports_parallel_tool_calls);
        assert!(find_tool(&tools, "list_dir").supports_parallel_tool_calls);
        assert!(find_tool(&tools, "read_file").supports_parallel_tool_calls);
    }

    #[test]
    fn test_test_model_family_includes_sync_tool() {
        let model_family = find_family_for_model("test-gpt-5-codex")
            .expect("test-gpt-5-codex should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: false,
            use_streamable_shell_tool: false,
            include_view_image_tool: false,
            experimental_unified_exec_tool: false,
        });
        let (tools, _) = build_specs(&config, None).build();

        assert!(
            tools
                .iter()
                .any(|tool| tool_name(&tool.spec) == "test_sync_tool")
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool_name(&tool.spec) == "read_file")
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool_name(&tool.spec) == "grep_files")
        );
        assert!(tools.iter().any(|tool| tool_name(&tool.spec) == "list_dir"));
    }

    #[test]
    fn test_build_specs_mcp_tools() {
        let model_family = find_family_for_model("o3").expect("o3 should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });
        let (tools, _) = build_specs(
            &config,
            Some(HashMap::from([(
                "test_server__do_something_cool".to_string(),
                mcp_types::Tool {
                    name: "do_something_cool".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({
                            "string_argument": {
                                "type": "string",
                            },
                            "number_argument": {
                                "type": "number",
                            },
                            "object_argument": {
                                "type": "object",
                                "properties": {
                                    "string_property": { "type": "string" },
                                    "number_property": { "type": "number" },
                                },
                                "required": [
                                    "string_property",
                                    "number_property",
                                ],
                                "additionalProperties": Some(false),
                            },
                        })),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Do something cool".to_string()),
                },
            )])),
        )
        .build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "web_search",
                "view_image",
                "test_server__do_something_cool",
            ],
        );

        let tool = find_tool(&tools, "test_server__do_something_cool");
        assert_eq!(
            &tool.spec,
            &ToolSpec::Function(ResponsesApiTool {
                name: "test_server__do_something_cool".to_string(),
                parameters: JsonSchema::Object {
                    properties: BTreeMap::from([
                        (
                            "string_argument".to_string(),
                            JsonSchema::String { description: None }
                        ),
                        (
                            "number_argument".to_string(),
                            JsonSchema::Number { description: None }
                        ),
                        (
                            "object_argument".to_string(),
                            JsonSchema::Object {
                                properties: BTreeMap::from([
                                    (
                                        "string_property".to_string(),
                                        JsonSchema::String { description: None }
                                    ),
                                    (
                                        "number_property".to_string(),
                                        JsonSchema::Number { description: None }
                                    ),
                                ]),
                                required: Some(vec![
                                    "string_property".to_string(),
                                    "number_property".to_string(),
                                ]),
                                additional_properties: Some(false.into()),
                            },
                        ),
                    ]),
                    required: None,
                    additional_properties: None,
                },
                description: "Do something cool".to_string(),
                strict: false,
            })
        );
    }

    #[test]
    fn test_build_specs_mcp_tools_sorted_by_name() {
        let model_family = find_family_for_model("o3").expect("o3 should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: false,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });

        // Intentionally construct a map with keys that would sort alphabetically.
        let tools_map: HashMap<String, mcp_types::Tool> = HashMap::from([
            (
                "test_server__do".to_string(),
                mcp_types::Tool {
                    name: "a".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({})),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("a".to_string()),
                },
            ),
            (
                "test_server__something".to_string(),
                mcp_types::Tool {
                    name: "b".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({})),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("b".to_string()),
                },
            ),
            (
                "test_server__cool".to_string(),
                mcp_types::Tool {
                    name: "c".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({})),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("c".to_string()),
                },
            ),
        ]);

        let (tools, _) = build_specs(&config, Some(tools_map)).build();
        // Expect unified_exec first, followed by MCP tools sorted by fully-qualified name.
        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "view_image",
                "test_server__cool",
                "test_server__do",
                "test_server__something",
            ],
        );
    }

    #[test]
    fn test_mcp_tool_property_missing_type_defaults_to_string() {
        let model_family = find_family_for_model("gpt-5-codex")
            .expect("gpt-5-codex should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });

        let (tools, _) = build_specs(
            &config,
            Some(HashMap::from([(
                "dash/search".to_string(),
                mcp_types::Tool {
                    name: "search".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({
                            "query": {
                                "description": "search query"
                            }
                        })),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Search docs".to_string()),
                },
            )])),
        )
        .build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "apply_patch",
                "web_search",
                "view_image",
                "dash_search",
            ],
        );

        let tool = find_tool(&tools, "dash_search");
        assert_eq!(
            &tool.spec,
            &ToolSpec::Function(ResponsesApiTool {
                name: "dash_search".to_string(),
                parameters: JsonSchema::Object {
                    properties: BTreeMap::from([(
                        "query".to_string(),
                        JsonSchema::String {
                            description: Some("search query".to_string())
                        }
                    )]),
                    required: None,
                    additional_properties: None,
                },
                description: "Search docs".to_string(),
                strict: false,
            })
        );
    }

    #[test]
    fn test_mcp_tool_integer_normalized_to_number() {
        let model_family = find_family_for_model("gpt-5-codex")
            .expect("gpt-5-codex should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });

        let (tools, _) = build_specs(
            &config,
            Some(HashMap::from([(
                "dash/paginate".to_string(),
                mcp_types::Tool {
                    name: "paginate".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({
                            "page": { "type": "integer" }
                        })),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Pagination".to_string()),
                },
            )])),
        )
        .build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "apply_patch",
                "web_search",
                "view_image",
                "dash_paginate",
            ],
        );
        let tool = find_tool(&tools, "dash_paginate");
        assert_eq!(
            &tool.spec,
            &ToolSpec::Function(ResponsesApiTool {
                name: "dash_paginate".to_string(),
                parameters: JsonSchema::Object {
                    properties: BTreeMap::from([(
                        "page".to_string(),
                        JsonSchema::Number { description: None }
                    )]),
                    required: None,
                    additional_properties: None,
                },
                description: "Pagination".to_string(),
                strict: false,
            })
        );
    }

    #[test]
    fn test_mcp_tool_array_without_items_gets_default_string_items() {
        let model_family = find_family_for_model("gpt-5-codex")
            .expect("gpt-5-codex should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: true,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });

        let (tools, _) = build_specs(
            &config,
            Some(HashMap::from([(
                "dash/tags".to_string(),
                mcp_types::Tool {
                    name: "tags".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({
                            "tags": { "type": "array" }
                        })),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Tags".to_string()),
                },
            )])),
        )
        .build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "apply_patch",
                "web_search",
                "view_image",
                "dash_tags",
            ],
        );
        let tool = find_tool(&tools, "dash_tags");
        assert_eq!(
            &tool.spec,
            &ToolSpec::Function(ResponsesApiTool {
                name: "dash_tags".to_string(),
                parameters: JsonSchema::Object {
                    properties: BTreeMap::from([(
                        "tags".to_string(),
                        JsonSchema::Array {
                            items: Box::new(JsonSchema::String { description: None }),
                            description: None
                        }
                    )]),
                    required: None,
                    additional_properties: None,
                },
                description: "Tags".to_string(),
                strict: false,
            })
        );
    }

    #[test]
    fn test_mcp_tool_anyof_defaults_to_string() {
        let model_family = find_family_for_model("gpt-5-codex")
            .expect("gpt-5-codex should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });

        let (tools, _) = build_specs(
            &config,
            Some(HashMap::from([(
                "dash/value".to_string(),
                mcp_types::Tool {
                    name: "value".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({
                            "value": { "anyOf": [ { "type": "string" }, { "type": "number" } ] }
                        })),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("AnyOf Value".to_string()),
                },
            )])),
        )
        .build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "apply_patch",
                "web_search",
                "view_image",
                "dash_value",
            ],
        );
        let tool = find_tool(&tools, "dash_value");
        assert_eq!(
            &tool.spec,
            &ToolSpec::Function(ResponsesApiTool {
                name: "dash_value".to_string(),
                parameters: JsonSchema::Object {
                    properties: BTreeMap::from([(
                        "value".to_string(),
                        JsonSchema::String { description: None }
                    )]),
                    required: None,
                    additional_properties: None,
                },
                description: "AnyOf Value".to_string(),
                strict: false,
            })
        );
    }

    #[test]
    fn test_exported_tool_names_are_regex_compliant() {
        let model_family = find_family_for_model("o3").expect("o3 should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: true,
            include_apply_patch_tool: true,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });

        let mcp_tools = HashMap::from([
            (
                "server.one/tool.with.dot".to_string(),
                mcp_types::Tool {
                    name: "tool.with.dot".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({})),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Tool with invalid name".to_string()),
                },
            ),
            (
                "server-two__already_valid".to_string(),
                mcp_types::Tool {
                    name: "already_valid".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({})),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Already valid".to_string()),
                },
            ),
        ]);

        let (tools, _) = build_specs(&config, Some(mcp_tools)).build();

        for tool in &tools {
            let name = tool_name(&tool.spec);
            assert!(
                is_valid_tool_name(name),
                "tool name should match OpenAI regex: {name}"
            );
        }
    }

    #[test]
    fn test_shell_tool() {
        let tool = super::create_shell_tool();
        let ToolSpec::Function(ResponsesApiTool {
            description, name, ..
        }) = &tool
        else {
            panic!("expected function tool");
        };
        assert_eq!(name, "shell");

        let expected = "Runs a shell command and returns its output.";
        assert_eq!(description, expected);
    }

    #[test]
    fn test_get_openai_tools_mcp_tools_with_additional_properties_schema() {
        let model_family = find_family_for_model("gpt-5-codex")
            .expect("gpt-5-codex should be a valid model family");
        let config = ToolsConfig::new(&ToolsConfigParams {
            model_family: &model_family,
            include_plan_tool: false,
            include_apply_patch_tool: false,
            include_web_search_request: true,
            use_streamable_shell_tool: false,
            include_view_image_tool: true,
            experimental_unified_exec_tool: true,
        });
        let (tools, _) = build_specs(
            &config,
            Some(HashMap::from([(
                "test_server__do_something_cool".to_string(),
                mcp_types::Tool {
                    name: "do_something_cool".to_string(),
                    input_schema: ToolInputSchema {
                        properties: Some(serde_json::json!({
                            "string_argument": {
                                "type": "string",
                            },
                            "number_argument": {
                                "type": "number",
                            },
                            "object_argument": {
                                "type": "object",
                                "properties": {
                                    "string_property": { "type": "string" },
                                    "number_property": { "type": "number" },
                                },
                                "required": [
                                    "string_property",
                                    "number_property",
                                ],
                                "additionalProperties": {
                                    "type": "object",
                                    "properties": {
                                        "addtl_prop": { "type": "string" },
                                    },
                                    "required": [
                                        "addtl_prop",
                                    ],
                                    "additionalProperties": false,
                                },
                            },
                        })),
                        required: None,
                        r#type: "object".to_string(),
                    },
                    output_schema: None,
                    title: None,
                    annotations: None,
                    description: Some("Do something cool".to_string()),
                },
            )])),
        )
        .build();

        assert_eq_tool_names(
            &tools,
            &[
                "unified_exec",
                "mailbox_send",
                "codex_mailbox_send",
                "mailbox_wait",
                "mailbox_read",
                WAIT_WITH_PREDICATE_TOOL_NAME,
                "apply_patch",
                "web_search",
                "view_image",
                "test_server__do_something_cool",
            ],
        );

        let tool = find_tool(&tools, "test_server__do_something_cool");
        assert_eq!(
            &tool.spec,
            &ToolSpec::Function(ResponsesApiTool {
                name: "test_server__do_something_cool".to_string(),
                parameters: JsonSchema::Object {
                    properties: BTreeMap::from([
                        (
                            "string_argument".to_string(),
                            JsonSchema::String { description: None }
                        ),
                        (
                            "number_argument".to_string(),
                            JsonSchema::Number { description: None }
                        ),
                        (
                            "object_argument".to_string(),
                            JsonSchema::Object {
                                properties: BTreeMap::from([
                                    (
                                        "string_property".to_string(),
                                        JsonSchema::String { description: None }
                                    ),
                                    (
                                        "number_property".to_string(),
                                        JsonSchema::Number { description: None }
                                    ),
                                ]),
                                required: Some(vec![
                                    "string_property".to_string(),
                                    "number_property".to_string(),
                                ]),
                                additional_properties: Some(
                                    JsonSchema::Object {
                                        properties: BTreeMap::from([(
                                            "addtl_prop".to_string(),
                                            JsonSchema::String { description: None }
                                        ),]),
                                        required: Some(vec!["addtl_prop".to_string(),]),
                                        additional_properties: Some(false.into()),
                                    }
                                    .into()
                                ),
                            },
                        ),
                    ]),
                    required: None,
                    additional_properties: None,
                },
                description: "Do something cool".to_string(),
                strict: false,
            })
        );
    }
}

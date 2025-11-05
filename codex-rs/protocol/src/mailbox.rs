use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use ts_rs::TS;
use uuid::Uuid;

/// Canonical mailbox message payload backed by `schemas/mailbox/mailbox-message.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
#[serde(default)]
pub struct MailboxMessage {
    pub message_id: Uuid,
    pub priority: MailboxPriority,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(type = "string | null")]
    pub posted_at: Option<OffsetDateTime>,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(type = "string | null")]
    pub expires_at: Option<OffsetDateTime>,
    pub sender: MailboxSender,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<MailboxAudience>,
    pub body: MailboxBody,
    pub ack_policy: MailboxAckPolicy,
    pub audit: MailboxAuditTrail,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MailboxAttachment>,
    #[serde(skip_serializing_if = "tags_is_empty")]
    pub tags: MailboxTagMap,
    #[serde(skip_serializing_if = "metadata_is_empty")]
    #[ts(type = "Record<string, unknown>")]
    pub metadata: MailboxMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<MailboxRateLimitHint>,
}

impl Default for MailboxMessage {
    fn default() -> Self {
        Self {
            message_id: Uuid::now_v7(),
            priority: MailboxPriority::Normal,
            posted_at: None,
            expires_at: None,
            sender: MailboxSender::default(),
            audience: None,
            body: MailboxBody::default(),
            ack_policy: MailboxAckPolicy::default(),
            audit: MailboxAuditTrail::default(),
            attachments: Vec::new(),
            tags: MailboxTagMap::new(),
            metadata: MailboxMetadata::new(),
            rate_limit: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
pub enum MailboxPriority {
    Critical,
    High,
    Normal,
    Low,
}

impl Default for MailboxPriority {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
pub enum MailboxSenderRole {
    Orchestrator,
    Operator,
    Automation,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxSender {
    pub id: String,
    pub role: MailboxSenderRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runbook: Option<String>,
}

impl Default for MailboxSender {
    fn default() -> Self {
        Self {
            id: String::new(),
            role: MailboxSenderRole::Orchestrator,
            display_name: None,
            contact: None,
            runbook: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxAudience {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<MailboxAudienceScope>>,
    pub allow_broadcast: bool,
}

impl Default for MailboxAudience {
    fn default() -> Self {
        Self {
            conversation_id: None,
            worker_id: None,
            scopes: None,
            allow_broadcast: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
pub enum MailboxAudienceScope {
    Session,
    Task,
    Workspace,
    Environment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub content: String,
    pub content_type: MailboxContentType,
}

impl Default for MailboxBody {
    fn default() -> Self {
        Self {
            subject: None,
            content: String::new(),
            content_type: MailboxContentType::TextPlain,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
pub enum MailboxContentType {
    #[serde(rename = "text/plain")]
    #[ts(rename = "text/plain")]
    TextPlain,
    #[serde(rename = "text/markdown")]
    #[ts(rename = "text/markdown")]
    TextMarkdown,
    #[serde(rename = "application/json")]
    #[ts(rename = "application/json")]
    ApplicationJson,
}

impl Default for MailboxContentType {
    fn default() -> Self {
        Self::TextPlain
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxAckPolicy {
    pub mode: MailboxAckMode,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(type = "string | null")]
    pub deadline: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_ack_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub escalation_ticket: Option<String>,
}

impl Default for MailboxAckPolicy {
    fn default() -> Self {
        Self {
            mode: MailboxAckMode::None,
            deadline: None,
            auto_ack_seconds: None,
            escalation_ticket: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
pub enum MailboxAckMode {
    None,
    Passive,
    Required,
}

impl Default for MailboxAckMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxAuditTrail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_ticket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub justification: Option<String>,
}

impl Default for MailboxAuditTrail {
    fn default() -> Self {
        Self {
            request_id: None,
            change_ticket: None,
            created_by: None,
            justification: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxRateLimitHint {
    pub scope: MailboxRateLimitScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_seconds: Option<u32>,
}

impl Default for MailboxRateLimitHint {
    fn default() -> Self {
        Self {
            scope: MailboxRateLimitScope::Conversation,
            capacity: None,
            interval_seconds: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
pub enum MailboxRateLimitScope {
    Conversation,
    Workspace,
    Orchestrator,
}

impl Default for MailboxRateLimitScope {
    fn default() -> Self {
        Self::Conversation
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxAttachment {
    pub attachment_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub content_type: String,
    pub digest: MailboxAttachmentDigest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

impl Default for MailboxAttachment {
    fn default() -> Self {
        Self {
            attachment_id: String::new(),
            description: None,
            content_type: String::new(),
            digest: MailboxAttachmentDigest::default(),
            uri: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(default)]
pub struct MailboxAttachmentDigest {
    pub algorithm: MailboxDigestAlgorithm,
    pub value: String,
}

impl Default for MailboxAttachmentDigest {
    fn default() -> Self {
        Self {
            algorithm: MailboxDigestAlgorithm::Sha256,
            value: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
pub enum MailboxDigestAlgorithm {
    Sha256,
    Sha512,
}

pub type MailboxTagMap = BTreeMap<String, String>;
pub type MailboxMetadata = BTreeMap<String, serde_json::Value>;

fn tags_is_empty(map: &MailboxTagMap) -> bool {
    map.is_empty()
}

fn metadata_is_empty(map: &MailboxMetadata) -> bool {
    map.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::format_description::well_known::Rfc3339;

    #[test]
    fn minimal_message_defaults() {
        let message = MailboxMessage {
            message_id: Uuid::nil(),
            sender: MailboxSender {
                id: "orchestrator/main".to_string(),
                role: MailboxSenderRole::Orchestrator,
                ..MailboxSender::default()
            },
            body: MailboxBody {
                content: "Mailbox system warm-up ping".to_string(),
                ..MailboxBody::default()
            },
            ..MailboxMessage::default()
        };

        assert_eq!(MailboxPriority::Normal, message.priority);
        assert_eq!(MailboxAckMode::None, message.ack_policy.mode);
        assert!(message.metadata.is_empty());

        let json = serde_json::to_value(&message).expect("serialize");
        assert_eq!(json["priority"], "normal");
        assert_eq!(json["ack_policy"]["mode"], "none");
        assert!(json.get("metadata").is_none());
    }

    #[test]
    fn round_trip_with_optional_metadata() {
        let posted_at =
            OffsetDateTime::parse("2025-10-12T19:30:00Z", &Rfc3339).expect("posted_at parse");
        let expires_at =
            OffsetDateTime::parse("2025-10-12T19:45:00Z", &Rfc3339).expect("expires_at parse");
        let deadline =
            OffsetDateTime::parse("2025-10-12T19:40:00Z", &Rfc3339).expect("deadline parse");

        let mut metadata = MailboxMetadata::new();
        metadata.insert("source".into(), json!("runbook"));
        metadata.insert("ticket".into(), json!("CHG-1234"));

        let mut tags = MailboxTagMap::new();
        tags.insert("env".into(), "prod".into());

        let message = MailboxMessage {
            message_id: Uuid::now_v7(),
            priority: MailboxPriority::High,
            posted_at: Some(posted_at),
            expires_at: Some(expires_at),
            sender: MailboxSender {
                id: "system.mailbox".to_string(),
                role: MailboxSenderRole::System,
                display_name: Some("Mailbox Control Plane".to_string()),
                contact: Some("mailto:codex-oncall@example.com".to_string()),
                runbook: Some("https://runbooks.example.com/codex/mailbox".to_string()),
            },
            audience: Some(MailboxAudience {
                conversation_id: Some(Uuid::nil()),
                worker_id: Some("worker-123".to_string()),
                scopes: Some(vec![
                    MailboxAudienceScope::Session,
                    MailboxAudienceScope::Environment,
                ]),
                allow_broadcast: true,
            }),
            body: MailboxBody {
                subject: Some("Fail fast until DBA confirms migration".to_string()),
                content: "Pause automation until changelog 442 finishes.".to_string(),
                content_type: MailboxContentType::TextMarkdown,
            },
            ack_policy: MailboxAckPolicy {
                mode: MailboxAckMode::Required,
                deadline: Some(deadline),
                auto_ack_seconds: None,
                escalation_ticket: Some("INC-42".to_string()),
            },
            audit: MailboxAuditTrail {
                request_id: Some("req-789".to_string()),
                change_ticket: Some("CHG-1234".to_string()),
                created_by: Some("deploy-bot".to_string()),
                justification: Some("Rollback in progress, prevent new tasks.".to_string()),
            },
            attachments: vec![MailboxAttachment {
                attachment_id: "artifact-1".to_string(),
                description: Some("Rollback plan".to_string()),
                content_type: "application/pdf".to_string(),
                digest: MailboxAttachmentDigest {
                    algorithm: MailboxDigestAlgorithm::Sha512,
                    value: "deadbeef".to_string(),
                },
                uri: Some("https://storage.example.com/rollbacks/plan.pdf".to_string()),
            }],
            tags,
            metadata,
            rate_limit: Some(MailboxRateLimitHint {
                scope: MailboxRateLimitScope::Workspace,
                capacity: Some(3),
                interval_seconds: Some(300),
            }),
        };

        let serialized = serde_json::to_string_pretty(&message).expect("serialize mailbox message");
        let round_trip: MailboxMessage =
            serde_json::from_str(&serialized).expect("deserialize mailbox message");

        assert_eq!(message, round_trip);
    }
}

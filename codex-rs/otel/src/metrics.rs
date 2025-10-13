#[cfg(feature = "otel")]
mod imp {
    use opentelemetry::global;
    use opentelemetry::metrics::{Counter, Histogram};
    use opentelemetry::KeyValue;
    use std::sync::OnceLock;

    const METER_NAME: &str = "codex.keepalive";
    const MAILBOX_METER_NAME: &str = "codex.mailbox";

    struct KeepaliveMetrics {
        heartbeat_total: Counter<u64>,
        reconnect_total: Counter<u64>,
        idle_timeout_total: Counter<u64>,
    }

    static METRICS: OnceLock<KeepaliveMetrics> = OnceLock::new();
    static MAILBOX_METRICS: OnceLock<MailboxMetrics> = OnceLock::new();

    impl KeepaliveMetrics {
        fn new() -> Self {
            let meter = global::meter(METER_NAME);
            let heartbeat_total = meter
                .u64_counter("codex_keepalive_heartbeat_total")
                .with_description(
                    "Count of synthetic heartbeat activity emitted by Codex transports.",
                )
                .build();
            let reconnect_total = meter
                .u64_counter("codex_keepalive_reconnect_total")
                .with_description(
                    "Count of transport reconnect attempts triggered by idle detection.",
                )
                .build();
            let idle_timeout_total = meter
                .u64_counter("codex_keepalive_idle_timeout_total")
                .with_description(
                    "Count of connections closed after exceeding idle timeout despite keepalives.",
                )
                .build();

            Self {
                heartbeat_total,
                reconnect_total,
                idle_timeout_total,
            }
        }
    }

    fn metrics() -> &'static KeepaliveMetrics {
        METRICS.get_or_init(KeepaliveMetrics::new)
    }

    struct MailboxMetrics {
        queue_depth: Histogram<u64>,
        delivery_latency: Histogram<u64>,
        ack_total: Counter<u64>,
        expiry_total: Counter<u64>,
    }

    impl MailboxMetrics {
        fn new() -> Self {
            let meter = global::meter(MAILBOX_METER_NAME);
            let queue_depth = meter
                .u64_histogram("codex_mailbox_queue_depth")
                .with_description("Observed mailbox queue depth during enqueue and delivery events.")
                .build();
            let delivery_latency = meter
                .u64_histogram("codex_mailbox_delivery_latency_ms")
                .with_description("Observed mailbox delivery latency in milliseconds.")
                .build();
            let ack_total = meter
                .u64_counter("codex_mailbox_ack_total")
                .with_description(
                    "Count of mailbox envelopes configured with acknowledgement requirements.",
                )
                .build();
            let expiry_total = meter
                .u64_counter("codex_mailbox_expiry_total")
                .with_description("Count of mailbox envelopes configured with expiry timestamps.")
                .build();
            Self {
                queue_depth,
                delivery_latency,
                ack_total,
                expiry_total,
            }
        }
    }

    fn mailbox_metrics() -> &'static MailboxMetrics {
        MAILBOX_METRICS.get_or_init(MailboxMetrics::new)
    }

    fn mailbox_attrs(
        ingress: &str,
        priority: &str,
        ack_mode: &str,
        sender_role: &str,
    ) -> [KeyValue; 4] {
        [
            KeyValue::new("ingress", ingress.to_string()),
            KeyValue::new("priority", priority.to_string()),
            KeyValue::new("ack_mode", ack_mode.to_string()),
            KeyValue::new("sender_role", sender_role.to_string()),
        ]
    }

    pub fn record_heartbeat(transport: &str, status: &str, elapsed_ms: Option<u64>) {
        let mut attrs = vec![
            KeyValue::new("transport", transport.to_string()),
            KeyValue::new("status", status.to_string()),
        ];
        if let Some(ms) = elapsed_ms {
            attrs.push(KeyValue::new("elapsed_ms", ms as i64));
        }
        metrics().heartbeat_total.add(1, &attrs);
    }

    pub fn record_reconnect(transport: &str, status: &str) {
        let attrs = [
            KeyValue::new("transport", transport.to_string()),
            KeyValue::new("status", status.to_string()),
        ];
        metrics().reconnect_total.add(1, &attrs);
    }

    pub fn record_idle_timeout(transport: &str) {
        let attrs = [KeyValue::new("transport", transport.to_string())];
        metrics().idle_timeout_total.add(1, &attrs);
    }

    pub fn record_mailbox_queue_depth(
        queue_depth: u64,
        ingress: &str,
        priority: &str,
        ack_mode: &str,
        sender_role: &str,
    ) {
        let attrs = mailbox_attrs(ingress, priority, ack_mode, sender_role);
        mailbox_metrics().queue_depth.record(queue_depth, &attrs);
    }

    pub fn record_mailbox_delivery_latency(
        latency_ms: u64,
        ingress: &str,
        priority: &str,
        ack_mode: &str,
        sender_role: &str,
    ) {
        let attrs = mailbox_attrs(ingress, priority, ack_mode, sender_role);
        mailbox_metrics().delivery_latency.record(latency_ms, &attrs);
    }

    pub fn record_mailbox_ack_total(
        ingress: &str,
        priority: &str,
        ack_mode: &str,
        sender_role: &str,
    ) {
        let attrs = mailbox_attrs(ingress, priority, ack_mode, sender_role);
        mailbox_metrics().ack_total.add(1, &attrs);
    }

    pub fn record_mailbox_expiry_total(
        ingress: &str,
        priority: &str,
        ack_mode: &str,
        sender_role: &str,
    ) {
        let attrs = mailbox_attrs(ingress, priority, ack_mode, sender_role);
        mailbox_metrics().expiry_total.add(1, &attrs);
    }
}

#[cfg(not(feature = "otel"))]
mod imp {
    #[inline]
    pub fn record_heartbeat(_: &str, _: &str, _: Option<u64>) {}

    #[inline]
    pub fn record_reconnect(_: &str, _: &str) {}

    #[inline]
    pub fn record_idle_timeout(_: &str) {}

    #[inline]
    pub fn record_mailbox_queue_depth(_: u64, _: &str, _: &str, _: &str, _: &str) {}

    #[inline]
    pub fn record_mailbox_delivery_latency(_: u64, _: &str, _: &str, _: &str, _: &str) {}

    #[inline]
    pub fn record_mailbox_ack_total(_: &str, _: &str, _: &str, _: &str) {}

    #[inline]
    pub fn record_mailbox_expiry_total(_: &str, _: &str, _: &str, _: &str) {}
}

pub use imp::{
    record_heartbeat, record_idle_timeout, record_mailbox_ack_total,
    record_mailbox_delivery_latency, record_mailbox_expiry_total, record_mailbox_queue_depth,
    record_reconnect,
};

#[cfg(feature = "otel")]
mod imp {
    use opentelemetry::KeyValue;
    use opentelemetry::global;
    use opentelemetry::metrics::Counter;
    use opentelemetry::metrics::Histogram;
    use opentelemetry::metrics::UpDownCounter;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::OnceLock;

    const METER_NAME: &str = "codex.keepalive";
    const MAILBOX_METER_NAME: &str = "codex.mailbox";
    const WAIT_METER_NAME: &str = "wait_with_predicate";

    struct KeepaliveMetrics {
        heartbeat_total: Counter<u64>,
        reconnect_total: Counter<u64>,
        idle_timeout_total: Counter<u64>,
    }

    static METRICS: OnceLock<KeepaliveMetrics> = OnceLock::new();
    static MAILBOX_METRICS: OnceLock<MailboxMetrics> = OnceLock::new();
    static WAIT_METRICS: OnceLock<WaitMetrics> = OnceLock::new();

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
        // New OTEL instruments per TASK: accept_total, errors_total, queue depth gauge
        accept_total: Counter<u64>,
        errors_total: Counter<u64>,
        queue_depth_gauge: UpDownCounter<i64>,
    }

    impl MailboxMetrics {
        fn new() -> Self {
            let meter = global::meter(MAILBOX_METER_NAME);
            let queue_depth = meter
                .u64_histogram("codex_mailbox_queue_depth")
                .with_description(
                    "Observed mailbox queue depth during enqueue and delivery events.",
                )
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
            // New: total accepted envelopes per namespace
            let accept_total = meter
                .u64_counter("codex_mailbox_accept_total")
                .with_description("Count of accepted mailbox envelopes.")
                .build();
            // New: total errors per namespace/kind
            let errors_total = meter
                .u64_counter("codex_mailbox_errors_total")
                .with_description("Count of mailbox errors, labeled by kind.")
                .build();
            // New: queue depth gauge (implemented as up/down counter with deltas)
            let queue_depth_gauge = meter
                .i64_up_down_counter("codex_mailbox_queue_depth_gauge")
                .with_description(
                    "Current mailbox queue depth, per namespace (up/down counter-based gauge).",
                )
                .build();
            Self {
                queue_depth,
                delivery_latency,
                ack_total,
                expiry_total,
                accept_total,
                errors_total,
                queue_depth_gauge,
            }
        }
    }

    fn mailbox_metrics() -> &'static MailboxMetrics {
        MAILBOX_METRICS.get_or_init(MailboxMetrics::new)
    }

    struct WaitMetrics {
        started_total: Counter<u64>,
        completed_duration: Histogram<f64>,
        failed_total: Counter<u64>,
        policy_violation_total: Counter<u64>,
    }

    impl WaitMetrics {
        fn new() -> Self {
            let meter = global::meter(WAIT_METER_NAME);
            let started_total = meter
                .u64_counter("wait_with_predicate_started_total")
                .with_description("Count of wait predicate invocations.")
                .build();
            let completed_duration = meter
                .f64_histogram("wait_with_predicate_duration_seconds")
                .with_description("Observed wait predicate completion durations in seconds.")
                .build();
            let failed_total = meter
                .u64_counter("wait_with_predicate_failed_total")
                .with_description(
                    "Count of wait predicates that ended with an error before completion.",
                )
                .build();
            let policy_violation_total = meter
                .u64_counter("wait_with_predicate_policy_violation_total")
                .with_description("Count of wait predicates rejected due to policy enforcement.")
                .build();

            Self {
                started_total,
                completed_duration,
                failed_total,
                policy_violation_total,
            }
        }
    }

    fn wait_metrics() -> &'static WaitMetrics {
        WAIT_METRICS.get_or_init(WaitMetrics::new)
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

    fn ns_attr(namespace: &str) -> [KeyValue; 1] {
        [KeyValue::new("namespace", namespace.to_string())]
    }

    fn ns_err_attrs(namespace: &str, error_kind: &str) -> [KeyValue; 2] {
        [
            KeyValue::new("namespace", namespace.to_string()),
            KeyValue::new("error_kind", error_kind.to_string()),
        ]
    }

    fn wait_attrs(kind: &str) -> [KeyValue; 1] {
        [KeyValue::new("predicate", kind.to_string())]
    }

    fn wait_policy_attrs(kind: &str, reason: &str) -> [KeyValue; 2] {
        [
            KeyValue::new("predicate", kind.to_string()),
            KeyValue::new("reason", reason.to_string()),
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
        mailbox_metrics()
            .delivery_latency
            .record(latency_ms, &attrs);
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

    // New entry points
    pub fn record_mailbox_accept_total(namespace: &str) {
        let attrs = ns_attr(namespace);
        mailbox_metrics().accept_total.add(1, &attrs);
    }

    pub fn record_mailbox_error_total(namespace: &str, error_kind: &str) {
        let attrs = ns_err_attrs(namespace, error_kind);
        mailbox_metrics().errors_total.add(1, &attrs);
    }

    pub fn record_wait_started(kind: &str) {
        let attrs = wait_attrs(kind);
        wait_metrics().started_total.add(1, &attrs);
    }

    pub fn record_wait_completed(kind: &str, duration_seconds: f64) {
        let attrs = wait_attrs(kind);
        wait_metrics()
            .completed_duration
            .record(duration_seconds, &attrs);
    }

    pub fn record_wait_failed(kind: &str) {
        let attrs = wait_attrs(kind);
        wait_metrics().failed_total.add(1, &attrs);
    }

    pub fn record_wait_policy_violation(kind: &str, reason: &str) {
        let attrs = wait_policy_attrs(kind, reason);
        wait_metrics().policy_violation_total.add(1, &attrs);
    }

    // Maintain last-seen queue depth per namespace to drive an up/down counter as a gauge.
    static QUEUE_DEPTH_STATE: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();

    fn queue_depth_state() -> &'static Mutex<HashMap<String, i64>> {
        QUEUE_DEPTH_STATE.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn update_mailbox_queue_depth_gauge(namespace: &str, current_depth: u64) {
        let current = current_depth as i64;
        let mut map = queue_depth_state().lock().unwrap();
        let prev = map.get(namespace).copied().unwrap_or(0);
        let delta = current - prev;
        if delta != 0 {
            let attrs = ns_attr(namespace);
            mailbox_metrics().queue_depth_gauge.add(delta, &attrs);
            map.insert(namespace.to_string(), current);
        }
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

    #[inline]
    pub fn record_mailbox_accept_total(_: &str) {}

    #[inline]
    pub fn record_mailbox_error_total(_: &str, _: &str) {}

    #[inline]
    pub fn update_mailbox_queue_depth_gauge(_: &str, _: u64) {}

    #[inline]
    pub fn record_wait_started(_: &str) {}

    #[inline]
    pub fn record_wait_completed(_: &str, _: f64) {}

    #[inline]
    pub fn record_wait_failed(_: &str) {}

    #[inline]
    pub fn record_wait_policy_violation(_: &str, _: &str) {}
}

pub use imp::record_heartbeat;
pub use imp::record_idle_timeout;
pub use imp::record_mailbox_accept_total;
pub use imp::record_mailbox_ack_total;
pub use imp::record_mailbox_delivery_latency;
pub use imp::record_mailbox_error_total;
pub use imp::record_mailbox_expiry_total;
pub use imp::record_mailbox_queue_depth;
pub use imp::record_reconnect;
pub use imp::record_wait_completed;
pub use imp::record_wait_failed;
pub use imp::record_wait_policy_violation;
pub use imp::record_wait_started;
pub use imp::update_mailbox_queue_depth_gauge;

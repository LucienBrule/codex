mod summaries_telemetry {
    use std::time::Duration;

    use codex_core::telemetry::{SummariesBatchTelemetry, SummariesSnapshot};
    use codex_core::protocol::EventMsg;
    use wiremock::MockServer;

    use core_test_support::test_codex::test_codex;
    use core_test_support::wait_for_event_with_timeout;

    #[test]
    fn batch_snapshot_computes_latency_and_tokens() {
        let t = SummariesBatchTelemetry::new(1);
        // Simulate very fast batch
        let s: SummariesSnapshot = t.finish("alpha beta gamma");
        assert!(s.latency_ms <= 100);
        assert_eq!(s.queue_depth, 1);
        assert_eq!(s.token_count, 3);
    }

    #[tokio::test]
    async fn service_emits_summary_updated() {
        let server = MockServer::start().await;
        let mut builder = test_codex();
        builder = builder.with_config(|cfg| {
            cfg.summaries.enabled = true;
            cfg.summaries.emit_interval = Duration::from_millis(500);
            cfg.summaries.initial_delay = Duration::from_millis(0);
        });
        let test = builder.build(&server).await.expect("build codex");

        // Wait up to ~2 seconds for a SummaryUpdated event from the background task.
        let msg = wait_for_event_with_timeout(&test.codex, |m| matches!(m, EventMsg::SummaryUpdated(_)), Duration::from_secs(2)).await;
        match msg {
            EventMsg::SummaryUpdated(ev) => {
                assert!(ev.summary.contains("summary"));
            }
            _ => panic!("unexpected event"),
        }
    }
}

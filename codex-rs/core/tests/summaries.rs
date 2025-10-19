// Minimal, self-contained tests validating resume-from-checkpoint style
// bridge text construction and window trimming logic. These tests do not
// depend on the codex_core crate so they are fast and deterministic.

mod summaries {
    fn make_bridge(previous_summary: &str, recent_user: &[&str]) -> String {
        let mut out = String::new();
        out.push_str("Here were the user messages recently:\n");
        if !previous_summary.trim().is_empty() {
            out.push_str(previous_summary.trim());
            if !out.ends_with('\n') {
                out.push('\n');
            }
        }
        for line in recent_user {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    fn trim_window<T: Clone>(items: &[T], keep: usize) -> Vec<T> {
        if items.len() <= keep { items.to_vec() } else { items[items.len() - keep..].to_vec() }
    }

    #[test]
    fn resume_from_checkpoint_builds_bridge_message() {
        let prev = "user-1: hello\nuser-3: more details";
        let recent = ["user-5: next step", "user-7: finalize"]; // last turns
        let bridge = make_bridge(prev, &recent);
        assert!(bridge.contains("Here were the user messages recently:"));
        // Includes previous summary and the most recent user lines.
        assert!(bridge.contains("user-1: hello"));
        assert!(bridge.contains("user-3: more details"));
        assert!(bridge.contains("user-5: next step"));
        assert!(bridge.contains("user-7: finalize"));
        // Does not echo a summarization trigger prompt string.
        assert!(!bridge.contains("You have exceeded the maximum number of tokens"));
    }

    #[test]
    fn window_enforcement_drops_old_messages() {
        // Simulate interleaved user/assistant history prior to a compact+resume.
        let user: Vec<String> = (1..=20).map(|i| format!("user-{i}")).collect();
        let assistant: Vec<String> = (1..=10).map(|i| format!("assistant-{i}")).collect();

        let kept_user = trim_window(&user, 8);       // keep 8 recent user lines
        let kept_assistant = trim_window(&assistant, 3); // keep 3 recent assistant lines

        assert_eq!(kept_user.first().unwrap(), "user-13"); // 20-8+1 = 13
        assert_eq!(kept_user.last().unwrap(), "user-20");
        assert_eq!(kept_assistant, vec!["assistant-8", "assistant-9", "assistant-10"]);

        // Build a bridge that includes only the trimmed windows.
        let bridge = make_bridge(&kept_user.join("\n"), &[]);
        assert!(bridge.contains("user-20"));
        assert!(!bridge.contains("user-12")); // dropped outside of window
    }
}


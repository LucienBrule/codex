use env_flags::env_flags;

env_flags! {
    /// Fixture path for offline tests (see client.rs).
    pub CODEX_RS_SSE_FIXTURE: Option<&str> = None;
    /// Feature flag guarding the mailbox out-of-band dispatcher path.
    pub CODEX_MAILBOX_OOB: bool = false;
}

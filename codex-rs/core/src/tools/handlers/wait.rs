use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::time;

use crate::codex::Session;
use crate::codex::TurnContext;
use crate::exec::ExecParams;
use crate::exec_env::create_env;
use crate::function_tool::FunctionCallError;
use crate::tools::context::SharedTurnDiffTracker;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::handle_container_exec_with_params;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_WAIT_TIMEOUT: Duration = Duration::from_secs(3600);
const MAX_TIMER_DURATION: Duration = Duration::from_secs(3600);
const MAX_SHELL_INTERVAL: Duration = Duration::from_secs(30);
const MIN_SHELL_INTERVAL: Duration = Duration::from_millis(100);

pub struct WaitHandler;

#[derive(Clone)]
struct WaitContext {
    runtime: Arc<dyn WaitRuntime>,
    sub_id: String,
    call_id: String,
    tool_name: String,
}

impl WaitContext {
    fn new(
        runtime: Arc<dyn WaitRuntime>,
        sub_id: String,
        call_id: String,
        tool_name: String,
    ) -> Self {
        Self {
            runtime,
            sub_id,
            call_id,
            tool_name,
        }
    }

    fn tool_name(&self) -> &str {
        &self.tool_name
    }

    fn sub_id(&self) -> &str {
        &self.sub_id
    }

    fn call_id(&self) -> &str {
        &self.call_id
    }

    fn runtime(&self) -> &dyn WaitRuntime {
        self.runtime.as_ref()
    }

    async fn notify(&self, message: impl Into<String>) {
        self.runtime()
            .notify(self.sub_id(), message.into())
            .await;
    }
}

#[derive(Clone, Copy)]
struct WaitLimits {
    timeout: Duration,
}

impl WaitLimits {
    fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[async_trait]
trait WaitRuntime: Send + Sync {
    async fn notify(&self, sub_id: &str, message: String);
    fn resolve_path(&self, path: Option<String>) -> Result<PathBuf, FunctionCallError>;
    fn shell_env(&self) -> HashMap<String, String>;
    fn tracker(&self) -> SharedTurnDiffTracker;
    async fn run_shell(
        &self,
        tool_name: &str,
        params: ExecParams,
        sub_id: String,
        call_id: String,
    ) -> Result<String, FunctionCallError>;
}

struct SessionRuntime {
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    tracker: SharedTurnDiffTracker,
}

impl SessionRuntime {
    fn new(
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        tracker: SharedTurnDiffTracker,
    ) -> Self {
        Self {
            session,
            turn,
            tracker,
        }
    }
}

#[async_trait]
impl WaitRuntime for SessionRuntime {
    async fn notify(&self, sub_id: &str, message: String) {
        self.session.notify_background_event(sub_id, message).await;
    }

    fn resolve_path(&self, path: Option<String>) -> Result<PathBuf, FunctionCallError> {
        Ok(self.turn.resolve_path(path))
    }

    fn shell_env(&self) -> HashMap<String, String> {
        create_env(&self.turn.shell_environment_policy)
    }

    fn tracker(&self) -> SharedTurnDiffTracker {
        Arc::clone(&self.tracker)
    }

    async fn run_shell(
        &self,
        tool_name: &str,
        params: ExecParams,
        sub_id: String,
        call_id: String,
    ) -> Result<String, FunctionCallError> {
        handle_container_exec_with_params(
            tool_name,
            params,
            Arc::clone(&self.session),
            Arc::clone(&self.turn),
            self.tracker(),
            sub_id,
            call_id,
        )
        .await
    }
}

#[async_trait]
impl ToolHandler for WaitHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let arguments = match &invocation.payload {
            ToolPayload::Function { arguments } => arguments.clone(),
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "codex.wait only accepts function payloads".to_string(),
                ))
            }
        };

        let args: WaitArgs = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse codex.wait arguments: {err}"
            ))
        })?;

        let timeout = parse_timeout(args.timeout_ms)?;
        let limits = WaitLimits::new(timeout);

        let WaitArgs {
            predicate_type,
            predicate,
            ..
        } = args;

        let ToolInvocation {
            session,
            turn,
            tracker,
            sub_id,
            call_id,
            tool_name,
            ..
        } = invocation;

        let runtime = Arc::new(SessionRuntime::new(
            Arc::clone(&session),
            Arc::clone(&turn),
            Arc::clone(&tracker),
        ));
        let ctx = WaitContext::new(runtime, sub_id, call_id, tool_name);

        ctx.notify(format!(
            "wait predicate {kind} started (timeout {seconds}s)",
            kind = predicate_type.as_str(),
            seconds = timeout.as_secs()
        ))
        .await;

        let started = Instant::now();
        let result = match predicate_type {
            WaitPredicateType::Timer => {
                let strategy = TimerStrategy;
                run_strategy(&strategy, &ctx, predicate, &limits).await
            }
            WaitPredicateType::Filesystem => {
                let strategy = FilesystemStrategy;
                run_strategy(&strategy, &ctx, predicate, &limits).await
            }
            WaitPredicateType::Shell => {
                let strategy = ShellStrategy;
                run_strategy(&strategy, &ctx, predicate, &limits).await
            }
        };

        match result {
            Ok(outcome) => {
                let elapsed = started.elapsed();
                ctx.notify(format!(
                    "wait predicate {kind} satisfied in {:.1}s",
                    elapsed.as_secs_f32(),
                    kind = predicate_type.as_str()
                ))
                .await;

                let mut response = serde_json::Map::new();
                response.insert(
                    "predicate".to_string(),
                    JsonValue::String(predicate_type.as_str().to_string()),
                );
                response.insert(
                    "elapsed_ms".to_string(),
                    JsonValue::Number(serde_json::Number::from(
                        elapsed.as_millis() as u64,
                    )),
                );
                response.insert("status".to_string(), JsonValue::String("completed".to_string()));
                response.insert("message".to_string(), JsonValue::String(outcome.message));
                response.insert("details".to_string(), outcome.details);

                Ok(ToolOutput::Function {
                    content: JsonValue::Object(response).to_string(),
                    success: Some(true),
                })
            }
            Err(err) => {
                ctx.notify(format!(
                    "wait predicate {kind} failed: {err}",
                    kind = predicate_type.as_str()
                ))
                .await;
                Err(err)
            }
        }
    }
}

fn parse_timeout(timeout_ms: Option<u64>) -> Result<Duration, FunctionCallError> {
    let timeout_ms = timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT.as_millis() as u64);
    if timeout_ms == 0 {
        return Err(FunctionCallError::RespondToModel(
            "timeout_ms must be greater than zero".to_string(),
        ));
    }

    let timeout = Duration::from_millis(timeout_ms);
    if timeout > MAX_WAIT_TIMEOUT {
        return Err(FunctionCallError::RespondToModel(format!(
            "timeout_ms must be <= {}",
            MAX_WAIT_TIMEOUT.as_millis()
        )));
    }

    Ok(timeout)
}

async fn run_strategy(
    strategy: &impl WaitPredicateStrategy,
    ctx: &WaitContext,
    predicate: JsonValue,
    limits: &WaitLimits,
) -> Result<WaitOutcome, FunctionCallError> {
    match time::timeout(limits.timeout(), strategy.wait(ctx, predicate, limits)).await {
        Ok(result) => result,
        Err(_) => Err(FunctionCallError::RespondToModel(format!(
            "wait predicate timed out after {} seconds",
            limits.timeout().as_secs()
        ))),
    }
}

#[derive(Deserialize)]
struct WaitArgs {
    #[serde(rename = "type")]
    predicate_type: WaitPredicateType,
    predicate: JsonValue,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum WaitPredicateType {
    Timer,
    Filesystem,
    Shell,
}

impl WaitPredicateType {
    fn as_str(&self) -> &'static str {
        match self {
            WaitPredicateType::Timer => "timer",
            WaitPredicateType::Filesystem => "filesystem",
            WaitPredicateType::Shell => "shell",
        }
    }
}

#[derive(Debug)]
struct WaitOutcome {
    message: String,
    details: JsonValue,
}

#[async_trait]
trait WaitPredicateStrategy: Send + Sync {
    async fn wait(
        &self,
        ctx: &WaitContext,
        predicate: JsonValue,
        limits: &WaitLimits,
    ) -> Result<WaitOutcome, FunctionCallError>;
}

struct TimerStrategy;

#[derive(Deserialize)]
struct TimerPredicate {
    duration_ms: u64,
}

#[async_trait]
impl WaitPredicateStrategy for TimerStrategy {
    async fn wait(
        &self,
        _ctx: &WaitContext,
        predicate: JsonValue,
        _limits: &WaitLimits,
    ) -> Result<WaitOutcome, FunctionCallError> {
        let predicate: TimerPredicate = serde_json::from_value(predicate).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "invalid timer predicate payload: {err}"
            ))
        })?;

        if predicate.duration_ms == 0 {
            return Err(FunctionCallError::RespondToModel(
                "duration_ms must be greater than zero".to_string(),
            ));
        }

        let duration = Duration::from_millis(predicate.duration_ms);
        if duration > MAX_TIMER_DURATION {
            return Err(FunctionCallError::RespondToModel(format!(
                "duration_ms must be <= {}",
                MAX_TIMER_DURATION.as_millis()
            )));
        }

        time::sleep(duration).await;

        Ok(WaitOutcome {
            message: format!("slept for {:.1} seconds", duration.as_secs_f32()),
            details: json!({ "duration_ms": predicate.duration_ms }),
        })
    }
}

struct ShellStrategy;

#[derive(Deserialize)]
struct ShellPredicate {
    command: Vec<String>,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    with_escalated_permissions: Option<bool>,
    #[serde(default)]
    justification: Option<String>,
    #[serde(default = "ShellPredicate::default_interval_ms")]
    interval_ms: u64,
    #[serde(default = "ShellPredicate::default_success_exit_codes")]
    success_exit_codes: Vec<i32>,
    #[serde(default)]
    max_attempts: Option<u32>,
}

impl ShellPredicate {
    const fn default_interval_ms() -> u64 {
        1_000
    }

    fn default_success_exit_codes() -> Vec<i32> {
        vec![0]
    }
}

#[derive(Deserialize)]
struct ExecOutputEnvelope {
    output: String,
    metadata: ExecMetadata,
}

#[derive(Deserialize)]
struct ExecMetadata {
    exit_code: i32,
    _duration_seconds: f32,
}

#[async_trait]
impl WaitPredicateStrategy for ShellStrategy {
    async fn wait(
        &self,
        ctx: &WaitContext,
        predicate: JsonValue,
        _limits: &WaitLimits,
    ) -> Result<WaitOutcome, FunctionCallError> {
        let predicate: ShellPredicate = serde_json::from_value(predicate).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "invalid shell predicate payload: {err}"
            ))
        })?;

        let max_attempts = predicate.max_attempts;

        if predicate.command.is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "command must not be empty".to_string(),
            ));
        }

        let runtime = ctx.runtime();
        let interval_ms = predicate.interval_ms.clamp(
            MIN_SHELL_INTERVAL.as_millis() as u64,
            MAX_SHELL_INTERVAL.as_millis() as u64,
        );
        let mut attempts: u32 = 0;
        let mut last_failure_summary: Option<String> = None;
        let command_display = predicate.command.join(" ");

        loop {
            attempts = attempts.saturating_add(1);
            ctx.notify(format!(
                "shell predicate attempt #{attempts} running `{command}`",
                command = command_display
            ))
            .await;

            let exec_params = ExecParams {
                command: predicate.command.clone(),
                cwd: runtime.resolve_path(predicate.workdir.clone())?,
                timeout_ms: predicate.timeout_ms,
                env: runtime.shell_env(),
                with_escalated_permissions: predicate.with_escalated_permissions,
                justification: predicate.justification.clone(),
            };

            match runtime
                .run_shell(
                    ctx.tool_name(),
                    exec_params,
                    ctx.sub_id().to_string(),
                    ctx.call_id().to_string(),
                )
                .await
            {
                Ok(output) => {
                    return Ok(WaitOutcome {
                        message: format!(
                            "shell command `{command}` succeeded after {attempts} attempt(s)",
                            command = command_display
                        ),
                        details: json!({
                            "attempts": attempts,
                            "exit_code": 0,
                            "output": truncate_output(&output),
                        }),
                    });
                }
                Err(FunctionCallError::RespondToModel(payload)) => {
                    if let Ok(envelope) = serde_json::from_str::<ExecOutputEnvelope>(&payload) {
                        if predicate
                            .success_exit_codes
                            .contains(&envelope.metadata.exit_code)
                        {
                            return Ok(WaitOutcome {
                                message: format!(
                                    "shell command `{command}` satisfied predicate with exit code {code} after {attempts} attempt(s)",
                                    command = command_display,
                                    code = envelope.metadata.exit_code
                                ),
                                details: json!({
                                    "attempts": attempts,
                                    "exit_code": envelope.metadata.exit_code,
                                    "output": truncate_output(&envelope.output),
                                }),
                            });
                        }

                        let exit_code = envelope.metadata.exit_code;
                        let output = truncate_output(&envelope.output);
                        if max_attempts.is_some() {
                            last_failure_summary = Some(format!("last exit_code={exit_code}, output={output}"));
                        }
                        ctx.notify(format!("shell predicate attempt #{attempts} exited with {exit_code}")).await;
                    } else {
                        return Err(FunctionCallError::RespondToModel(payload));
                    }
                }
                Err(FunctionCallError::Fatal(err)) => {
                    return Err(FunctionCallError::Fatal(err));
                }
                Err(FunctionCallError::MissingLocalShellCallId) => {
                    return Err(FunctionCallError::Fatal(
                        "missing call_id for shell predicate invocation".to_string(),
                    ));
                }
            }

            if let Some(max) = max_attempts {
                if attempts >= max {
                    let message = last_failure_summary
                        .clone()
                        .map(|summary| {
                            format!("shell predicate attempts exhausted after {attempts} tries ({summary})", attempts = attempts)
                        })
                        .unwrap_or_else(|| {
                            format!("shell predicate attempts exhausted after {attempts} tries", attempts = attempts)
                        });
                    return Err(FunctionCallError::RespondToModel(message));
                }
            }

            time::sleep(Duration::from_millis(interval_ms)).await;
        }
    }
}

fn truncate_output(output: &str) -> String {
    const MAX_LEN: usize = 512;
    if output.len() <= MAX_LEN {
        output.to_string()
    } else {
        format!("{}…", &output[..MAX_LEN])
    }
}

struct FilesystemStrategy;

#[derive(Deserialize)]
struct FilesystemPredicate {
    path: String,
    #[serde(default)]
    event: FilesystemEvent,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum FilesystemEvent {
    Exists,
    Modified,
}

impl Default for FilesystemEvent {
    fn default() -> Self {
        FilesystemEvent::Exists
    }
}

impl FilesystemEvent {
    fn as_str(&self) -> &'static str {
        match self {
            FilesystemEvent::Exists => "exists",
            FilesystemEvent::Modified => "modified",
        }
    }
}

#[async_trait]
impl WaitPredicateStrategy for FilesystemStrategy {
    async fn wait(
        &self,
        ctx: &WaitContext,
        predicate: JsonValue,
        _limits: &WaitLimits,
    ) -> Result<WaitOutcome, FunctionCallError> {
        let predicate: FilesystemPredicate =
            serde_json::from_value(predicate).map_err(|err| {
                FunctionCallError::RespondToModel(format!(
                    "invalid filesystem predicate payload: {err}"
                ))
            })?;

        let path = PathBuf::from(&predicate.path);
        if !path.is_absolute() {
            return Err(FunctionCallError::RespondToModel(
                "filesystem predicate path must be absolute".to_string(),
            ));
        }

        ctx.notify(format!(
            "filesystem predicate awaiting {} on {}",
            predicate.event.as_str(),
            path.display()
        ))
        .await;

        match predicate.event {
            FilesystemEvent::Exists => wait_for_exists(&path).await?,
            FilesystemEvent::Modified => wait_for_modified(&path).await?,
        }

        Ok(WaitOutcome {
            message: format!(
                "filesystem predicate satisfied: {} {}",
                predicate.event.as_str(),
                path.display()
            ),
            details: json!({
                "path": path,
                "event": predicate.event.as_str(),
            }),
        })
    }
}

#[cfg(target_family = "unix")]
async fn wait_for_exists(path: &Path) -> Result<(), FunctionCallError> {
    use notify::event::DataChange;
    use notify::event::EventKind;
    use notify::event::ModifyKind;
    use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
    use tracing::warn;

    if path.exists() {
        return Ok(());
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("/"));
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let mut watcher = RecommendedWatcher::new(
        move |res| {
            if let Err(err) = tx.blocking_send(res) {
                warn!("filesystem watcher channel closed: {err}");
            }
        },
        Config::default(),
    )
    .map_err(|err| {
        FunctionCallError::RespondToModel(format!(
            "failed to initialize filesystem watcher: {err}"
        ))
    })?;

    watcher
        .watch(parent, RecursiveMode::NonRecursive)
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to watch {}: {err}",
                parent.display()
            ))
        })?;

    loop {
        if path.exists() {
            return Ok(());
        }

        match rx.recv().await {
            Some(Ok(event)) => {
                let triggered = event.paths.iter().any(|p| p == path);
                let kind_matches = matches!(
                    event.kind,
                    EventKind::Create(_)
                        | EventKind::Modify(ModifyKind::Data(DataChange::Content))
                        | EventKind::Modify(ModifyKind::Data(_))
                );
                if triggered && kind_matches && path.exists() {
                    return Ok(());
                }
            }
            Some(Err(err)) => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "filesystem watcher error: {err}"
                )));
            }
            None => {
                return Err(FunctionCallError::RespondToModel(
                    "filesystem watcher ended unexpectedly".to_string(),
                ));
            }
        }
    }
}

#[cfg(not(target_family = "unix"))]
async fn wait_for_exists(_path: &Path) -> Result<(), FunctionCallError> {
    Err(FunctionCallError::RespondToModel(
        "filesystem predicates are not supported on this platform".to_string(),
    ))
}

#[cfg(target_family = "unix")]
async fn wait_for_modified(path: &Path) -> Result<(), FunctionCallError> {
    use notify::event::EventKind;
    use notify::event::ModifyKind;
    use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
    use tracing::warn;

    if !path.exists() {
        return Err(FunctionCallError::RespondToModel(format!(
            "filesystem modified predicate requires existing path: {}",
            path.display()
        )));
    }

    let watch_target = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or_else(|| Path::new("/"))
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let mut watcher = RecommendedWatcher::new(
        move |res| {
            if let Err(err) = tx.blocking_send(res) {
                warn!("filesystem watcher channel closed: {err}");
            }
        },
        Config::default(),
    )
    .map_err(|err| {
        FunctionCallError::RespondToModel(format!(
            "failed to initialize filesystem watcher: {err}"
        ))
    })?;

    watcher
        .watch(watch_target, RecursiveMode::NonRecursive)
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to watch {}: {err}",
                watch_target.display()
            ))
        })?;

    loop {
        match rx.recv().await {
            Some(Ok(event)) => {
                let triggered = if path.is_dir() {
                    event.paths.iter().any(|p| p.starts_with(path))
                } else {
                    event.paths.iter().any(|p| p == path)
                };

                if triggered
                    && matches!(
                        event.kind,
                        EventKind::Modify(ModifyKind::Any) | EventKind::Modify(ModifyKind::Data(_))
                    )
                {
                    return Ok(());
                }
            }
            Some(Err(err)) => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "filesystem watcher error: {err}"
                )));
            }
            None => {
                return Err(FunctionCallError::RespondToModel(
                    "filesystem watcher ended unexpectedly".to_string(),
                ));
            }
        }
    }
}

#[cfg(not(target_family = "unix"))]
async fn wait_for_modified(_path: &Path) -> Result<(), FunctionCallError> {
    Err(FunctionCallError::RespondToModel(
        "filesystem predicates are not supported on this platform".to_string(),
    ))
}

#[cfg(test)]
mod wait_handler {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct StubRuntime {
        notifications: Mutex<Vec<String>>,
        shell_results: Mutex<VecDeque<Result<String, FunctionCallError>>>,
        base_dir: PathBuf,
        tracker: SharedTurnDiffTracker,
    }

    impl StubRuntime {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                notifications: Mutex::new(Vec::new()),
                shell_results: Mutex::new(VecDeque::new()),
                base_dir: PathBuf::from("/tmp"),
                tracker: Arc::new(tokio::sync::Mutex::new(
                    crate::turn_diff_tracker::TurnDiffTracker::default(),
                )),
            })
        }

        fn with_shell_results<I>(self: &Arc<Self>, results: I)
        where
            I: IntoIterator<Item = Result<String, FunctionCallError>>,
        {
            let mut guard = self.shell_results.lock().unwrap();
            guard.extend(results);
        }

        fn notifications(&self) -> Vec<String> {
            self.notifications.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WaitRuntime for StubRuntime {
        async fn notify(&self, _sub_id: &str, message: String) {
            self.notifications.lock().unwrap().push(message);
        }

        fn resolve_path(&self, path: Option<String>) -> Result<PathBuf, FunctionCallError> {
            Ok(path
                .map(PathBuf::from)
                .map(|p| self.base_dir.join(p))
                .unwrap_or_else(|| self.base_dir.clone()))
        }

        fn shell_env(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn tracker(&self) -> SharedTurnDiffTracker {
            Arc::clone(&self.tracker)
        }

        async fn run_shell(
            &self,
            _tool_name: &str,
            _params: ExecParams,
            _sub_id: String,
            _call_id: String,
        ) -> Result<String, FunctionCallError> {
            let mut guard = self.shell_results.lock().unwrap();
            guard
                .pop_front()
                .unwrap_or_else(|| Err(FunctionCallError::RespondToModel("empty".to_string())))
        }
    }

    fn stub_context(runtime: Arc<StubRuntime>) -> WaitContext {
        WaitContext::new(
            runtime,
            "sub".to_string(),
            "call".to_string(),
            "codex.wait".to_string(),
        )
    }

    #[tokio::test]
    async fn parse_timeout_rejects_zero() {
        assert!(matches!(
            parse_timeout(Some(0)),
            Err(FunctionCallError::RespondToModel(_))
        ));
    }

    #[tokio::test]
    async fn timer_strategy_succeeds() {
        let strategy = TimerStrategy;
        let runtime = StubRuntime::new();
        let ctx = stub_context(runtime);
        let start = Instant::now();
        let outcome = strategy
            .wait(&ctx, json!({ "duration_ms": 20 }), &WaitLimits::new(DEFAULT_WAIT_TIMEOUT))
            .await
            .expect("timer should succeed");
        assert!(start.elapsed() >= Duration::from_millis(20));
        assert!(outcome.message.contains("slept"));
    }

    #[tokio::test]
    async fn shell_strategy_rejects_empty_command() {
        let strategy = ShellStrategy;
        let runtime = StubRuntime::new();
        let ctx = stub_context(runtime);
        let err = strategy
            .wait(&ctx, json!({ "command": [] }), &WaitLimits::new(DEFAULT_WAIT_TIMEOUT))
            .await
            .unwrap_err();
        assert!(matches!(err, FunctionCallError::RespondToModel(_)));
    }

    #[tokio::test]
    async fn shell_strategy_succeeds_on_first_try() {
        let strategy = ShellStrategy;
        let runtime = StubRuntime::new();
        runtime.with_shell_results(vec![Ok("done".to_string())]);
        let ctx = stub_context(runtime.clone());
        let outcome = strategy
            .wait(
                &ctx,
                json!({ "command": ["echo", "ok"] }),
                &WaitLimits::new(DEFAULT_WAIT_TIMEOUT),
            )
            .await
            .expect("shell predicate should succeed");
        assert!(outcome.message.contains("succeeded"));
        assert_eq!(runtime.notifications().len(), 1);
    }
}

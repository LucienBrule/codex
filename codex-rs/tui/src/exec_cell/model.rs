use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;
use std::str;

use codex_core::protocol::ExecOutputStream;
use codex_protocol::parse_command::ParsedCommand;

#[derive(Clone, Debug)]
pub(crate) struct CommandOutput {
    pub(crate) exit_code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
    pub(crate) formatted_output: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecCall {
    pub(crate) call_id: String,
    pub(crate) command: Vec<String>,
    pub(crate) parsed: Vec<ParsedCommand>,
    pub(crate) output: Option<CommandOutput>,
    pub(crate) start_time: Option<Instant>,
    pub(crate) duration: Option<Duration>,
    pub(crate) live_output: LiveExecOutput,
}

#[derive(Debug)]
pub(crate) struct ExecCell {
    pub(crate) calls: Vec<ExecCall>,
}

impl ExecCell {
    pub(crate) fn new(call: ExecCall) -> Self {
        Self { calls: vec![call] }
    }

    pub(crate) fn with_added_call(
        &self,
        call_id: String,
        command: Vec<String>,
        parsed: Vec<ParsedCommand>,
    ) -> Option<Self> {
        let call = ExecCall {
            call_id,
            command,
            parsed,
            output: None,
            start_time: Some(Instant::now()),
            duration: None,
            live_output: LiveExecOutput::default(),
        };
        if self.is_exploring_cell() && Self::is_exploring_call(&call) {
            Some(Self {
                calls: [self.calls.clone(), vec![call]].concat(),
            })
        } else {
            None
        }
    }

    pub(crate) fn complete_call(
        &mut self,
        call_id: &str,
        output: CommandOutput,
        duration: Duration,
    ) {
        if let Some(call) = self.calls.iter_mut().rev().find(|c| c.call_id == call_id) {
            call.output = Some(output);
            call.duration = Some(duration);
            call.start_time = None;
            call.live_output.clear();
        }
    }

    pub(crate) fn should_flush(&self) -> bool {
        !self.is_exploring_cell() && self.calls.iter().all(|c| c.output.is_some())
    }

    pub(crate) fn mark_failed(&mut self) {
        for call in self.calls.iter_mut() {
            if call.output.is_none() {
                let elapsed = call
                    .start_time
                    .map(|st| st.elapsed())
                    .unwrap_or_else(|| Duration::from_millis(0));
                call.start_time = None;
                call.duration = Some(elapsed);
                call.output = Some(CommandOutput {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: String::new(),
                    formatted_output: String::new(),
                });
                call.live_output.clear();
            }
        }
    }

    pub(crate) fn append_live_output(
        &mut self,
        call_id: &str,
        stream: ExecOutputStream,
        chunk: &[u8],
    ) -> bool {
        self.calls
            .iter_mut()
            .rev()
            .find(|c| c.call_id == call_id)
            .map(|call| call.live_output.push_chunk(stream, chunk))
            .unwrap_or(false)
    }

    pub(crate) fn is_exploring_cell(&self) -> bool {
        self.calls.iter().all(Self::is_exploring_call)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.calls.iter().any(|c| c.output.is_none())
    }

    pub(crate) fn active_start_time(&self) -> Option<Instant> {
        self.calls
            .iter()
            .find(|c| c.output.is_none())
            .and_then(|c| c.start_time)
    }

    pub(crate) fn iter_calls(&self) -> impl Iterator<Item = &ExecCall> {
        self.calls.iter()
    }

    pub(super) fn is_exploring_call(call: &ExecCall) -> bool {
        !call.parsed.is_empty()
            && call.parsed.iter().all(|p| {
                matches!(
                    p,
                    ParsedCommand::Read { .. }
                        | ParsedCommand::ListFiles { .. }
                        | ParsedCommand::Search { .. }
                )
            })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LiveExecOutputSnapshotLine {
    pub(crate) stream: ExecOutputStream,
    pub(crate) content: String,
    pub(crate) is_partial: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveExecOutputSnapshot {
    pub(crate) dropped_lines: usize,
    pub(crate) lines: Vec<LiveExecOutputSnapshotLine>,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveExecOutput {
    lines: VecDeque<LiveExecOutputLine>,
    pending: Option<LiveExecPendingLine>,
    dropped_lines: usize,
    pending_stdout_bytes: Vec<u8>,
    pending_stderr_bytes: Vec<u8>,
}

const LIVE_OUTPUT_MAX_LINES: usize = 200;

impl Default for LiveExecOutput {
    fn default() -> Self {
        Self {
            lines: VecDeque::new(),
            pending: None,
            dropped_lines: 0,
            pending_stdout_bytes: Vec::new(),
            pending_stderr_bytes: Vec::new(),
        }
    }
}

impl LiveExecOutput {
    pub(crate) fn push_chunk(&mut self, stream: ExecOutputStream, chunk: &[u8]) -> bool {
        if chunk.is_empty() {
            return false;
        }

        let mut changed = false;
        let mut start = 0usize;
        for (idx, byte) in chunk.iter().enumerate() {
            if *byte == b'\n' {
                changed |= self.append_segment(stream.clone(), &chunk[start..idx]);
                self.finish_line(stream.clone());
                changed = true;
                start = idx + 1;
            }
        }

        if start < chunk.len() {
            changed |= self.append_segment(stream, &chunk[start..]);
        }

        changed
    }

    pub(crate) fn clear(&mut self) {
        self.lines.clear();
        self.pending = None;
        self.dropped_lines = 0;
        self.pending_stdout_bytes.clear();
        self.pending_stderr_bytes.clear();
    }

    pub(crate) fn snapshot(&self) -> LiveExecOutputSnapshot {
        let mut lines: Vec<LiveExecOutputSnapshotLine> = self
            .lines
            .iter()
            .map(|line| LiveExecOutputSnapshotLine {
                stream: line.stream.clone(),
                content: line.content.clone(),
                is_partial: false,
            })
            .collect();
        if let Some(pending) = &self.pending {
            lines.push(LiveExecOutputSnapshotLine {
                stream: pending.stream.clone(),
                content: pending.content.clone(),
                is_partial: true,
            });
        }

        LiveExecOutputSnapshot {
            dropped_lines: self.dropped_lines,
            lines,
        }
    }

    fn append_segment(&mut self, stream: ExecOutputStream, bytes: &[u8]) -> bool {
        let mut changed = false;
        let mut need_reset = false;
        let mut finish_stream: Option<ExecOutputStream> = None;

        match self.pending.as_ref() {
            None => need_reset = true,
            Some(current) if current.stream != stream => {
                need_reset = true;
                finish_stream = Some(current.stream.clone());
            }
            _ => {}
        }

        if let Some(prev_stream) = finish_stream {
            self.finish_line(prev_stream);
            changed = true;
        }

        if need_reset {
            let initial = self.take_incomplete_bytes(&stream);
            self.pending = Some(LiveExecPendingLine::with_initial(stream.clone(), initial));
        }

        if let Some(pending) = self.pending.as_mut() {
            if !bytes.is_empty() {
                changed |= pending.push_bytes(bytes);
            }
        }

        changed
    }

    fn finish_line(&mut self, default_stream: ExecOutputStream) {
        let pending = self.pending.take().unwrap_or_else(|| {
            let initial = self.take_incomplete_bytes(&default_stream);
            LiveExecPendingLine::with_initial(default_stream.clone(), initial)
        });

        let (line, leftover) = pending.finish();
        self.store_incomplete_bytes(line.stream.clone(), leftover);
        self.lines.push_back(line);
        self.enforce_limit();
    }

    fn enforce_limit(&mut self) {
        while self.lines.len() > LIVE_OUTPUT_MAX_LINES {
            self.lines.pop_front();
            self.dropped_lines = self.dropped_lines.saturating_add(1);
        }
    }

    fn take_incomplete_bytes(&mut self, stream: &ExecOutputStream) -> Vec<u8> {
        match stream {
            ExecOutputStream::Stdout => std::mem::take(&mut self.pending_stdout_bytes),
            ExecOutputStream::Stderr => std::mem::take(&mut self.pending_stderr_bytes),
        }
    }

    fn store_incomplete_bytes(&mut self, stream: ExecOutputStream, bytes: Vec<u8>) {
        match stream {
            ExecOutputStream::Stdout => {
                self.pending_stdout_bytes = bytes;
            }
            ExecOutputStream::Stderr => {
                self.pending_stderr_bytes = bytes;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct LiveExecOutputLine {
    stream: ExecOutputStream,
    content: String,
}

#[derive(Debug, Clone)]
struct LiveExecPendingLine {
    stream: ExecOutputStream,
    buffer: Vec<u8>,
    decoded_len: usize,
    content: String,
}

impl LiveExecPendingLine {
    fn with_initial(stream: ExecOutputStream, initial: Vec<u8>) -> Self {
        let mut pending = Self {
            stream,
            buffer: initial,
            decoded_len: 0,
            content: String::new(),
        };
        pending.decode_new_bytes();
        pending
    }

    fn push_bytes(&mut self, bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }
        self.buffer.extend_from_slice(bytes);
        self.decode_new_bytes()
    }

    fn decode_new_bytes(&mut self) -> bool {
        let mut changed = false;

        loop {
            let remaining = &self.buffer[self.decoded_len..];
            if remaining.is_empty() {
                break;
            }

            match str::from_utf8(remaining) {
                Ok(valid) => {
                    if !valid.is_empty() {
                        self.content.push_str(valid);
                        changed = true;
                    }
                    self.decoded_len = self.buffer.len();
                    break;
                }
                Err(err) => {
                    let valid_up_to = err.valid_up_to();
                    if valid_up_to > 0 {
                        // SAFETY: the slice up to valid_up_to is valid UTF-8.
                        let valid = unsafe { str::from_utf8_unchecked(&remaining[..valid_up_to]) };
                        self.content.push_str(valid);
                        self.decoded_len += valid_up_to;
                        changed = true;
                        continue;
                    }

                    if let Some(error_len) = err.error_len() {
                        self.decoded_len += error_len;
                        self.content.push('\u{FFFD}');
                        changed = true;
                        continue;
                    }

                    // Incomplete multi-byte sequence at the end; wait for more bytes.
                    break;
                }
            }
        }

        if self.decoded_len > 0 {
            self.buffer.drain(..self.decoded_len);
            self.decoded_len = 0;
        }

        changed
    }

    fn finish(self) -> (LiveExecOutputLine, Vec<u8>) {
        let line = LiveExecOutputLine {
            stream: self.stream,
            content: self.content,
        };
        (line, self.buffer)
    }
}

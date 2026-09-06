//! Band adapters: how a turn is played through a harness CLI.
//!
//! Every CLI adapter has the same shape: build a command in the session
//! workspace, stream its JSON output into chunks, capture the resume id, and
//! close with the final reply. The three parsers differ; the driver does not.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ceilidh_protocol::{Envelope, Harness, SessionId, TurnSummary};
use serde_json::{Value, json};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::time;
use tracing::{info, warn};

use crate::{AdapterOutcome, ChunkSink};

pub(crate) const TURN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const STDERR_TAIL_CHARS: usize = 2000;
/// Hard ceiling on captured stderr. Only the tail is reported.
const STDERR_CAPTURE_LIMIT: u64 = 256 * 1024;
/// MCP tool calls include a synchronous sub-agent turn, so the harness must
/// wait far longer than its default tool timeout.
const MCP_TOOL_TIMEOUT_SECS: u64 = 25 * 60;

/// Where the harness binaries live and what the sub-agent MCP needs.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub claude_bin: String,
    pub codex_bin: String,
    pub cursor_bin: String,
    /// The ceilidh binary, spawned as `ceilidh mcp` by every harness.
    pub ceilidh_bin: PathBuf,
    /// The seat's Codex login, symlinked into each session's CODEX_HOME.
    pub codex_auth: PathBuf,
    /// Caller URL and token handed to the MCP process.
    pub server_url: String,
    pub token: Option<String>,
}

pub(crate) struct TurnCtx<'a> {
    pub workspace_dir: &'a Path,
    /// The session directory above the workspace; harness config files that
    /// must never be committed live here.
    pub session_dir: &'a Path,
    pub session_id: SessionId,
    pub seq: i64,
    pub input: &'a str,
    pub model: &'a str,
    pub effort: Option<&'a str>,
    pub resume_token: Option<&'a str>,
    pub history_hint: &'a [TurnSummary],
    pub chunk_sink: ChunkSink,
    /// Flips to true when the human cancels the turn.
    pub cancel: watch::Receiver<bool>,
    pub harness: &'a HarnessConfig,
}

impl TurnCtx<'_> {
    fn prompt(&self) -> String {
        if self.resume_token.is_none() && !self.history_hint.is_empty() {
            prompt_with_history(self.input, self.history_hint)
        } else {
            self.input.to_string()
        }
    }

        /// True for the one harness that needs the Cursor key in its environment.
    fn mcp_args(&self) -> Vec<String> {
        vec![
            "mcp".to_string(),
            "--server".to_string(),
            self.harness.server_url.clone(),
            "--parent".to_string(),
            self.session_id.to_string(),
        ]
    }
}

pub(crate) async fn execute(harness: Harness, ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
    match harness {
        Harness::Mock => mock(ctx).await,
        Harness::ClaudeCode => claude_code(ctx).await,
        Harness::Codex => codex(ctx).await,
        Harness::Cursor => cursor(ctx).await,
    }
}

// ---------------------------------------------------------------------------
// Mock: deterministic echo for CI and the smoke script.
// ---------------------------------------------------------------------------

async fn mock(ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
    let path = ctx.workspace_dir.join(format!("turn-{}.txt", ctx.seq));
    fs::write(&path, ctx.input)
        .await
        .with_context(|| format!("write mock turn file {}", path.display()))?;
    ctx.chunk_sink.emit("mock: thinking");
    ctx.chunk_sink.emit("mock: done");

    Ok(AdapterOutcome::done(
        Envelope {
            headline: format!("mock turn {} complete", ctx.seq),
            work_complete: true,
            cannot_proceed: false,
            body_markdown: format!("echo: {}", ctx.input),
            questions: Vec::new(),
        },
        Some(format!("mock-{}", ctx.session_id)),
    ))
}

// ---------------------------------------------------------------------------
// Claude Code
// ---------------------------------------------------------------------------

async fn claude_code(ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
    let mcp_path = ctx.session_dir.join("mcp-claude.json");
    let mcp = json!({
        "mcpServers": {
            "ceilidh": {
                "command": ctx.harness.ceilidh_bin,
                "args": ctx.mcp_args(),
                "env": { "CEILIDH_TOKEN": ctx.harness.token.clone().unwrap_or_default() }
            }
        }
    });
    write_private(&mcp_path, &serde_json::to_vec_pretty(&mcp)?).await?;

    let mut command = Command::new(&ctx.harness.claude_bin);
    command
        .arg("-p")
        .arg(ctx.prompt())
        .arg("--model")
        .arg(ctx.model)
        .arg("--output-format")
        .arg("stream-json")
        .arg("--verbose")
        .arg("--permission-mode")
        .arg("bypassPermissions")
        .arg("--mcp-config")
        .arg(&mcp_path)
        .env("MCP_TIMEOUT", "30000")
        .env("MCP_TOOL_TIMEOUT", (MCP_TOOL_TIMEOUT_SECS * 1000).to_string());
    if let Some(resume_token) = ctx.resume_token {
        command.arg("--resume").arg(resume_token);
    }

    let mut parser = ClaudeStream::default();
    run_streaming(command, &mut parser, &ctx, "claude").await
}

// ---------------------------------------------------------------------------
// Codex
// ---------------------------------------------------------------------------

async fn codex(ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
    let home = ctx.session_dir.join("codex-home");
    fs::create_dir_all(&home)
        .await
        .with_context(|| format!("create codex home {}", home.display()))?;

    // The seat's login is shared by symlink, not copied, so a token refresh
    // written by any client is seen by all of them.
    let auth_link = home.join("auth.json");
    if fs::symlink_metadata(&auth_link).await.is_err() {
        fs::symlink(&ctx.harness.codex_auth, &auth_link)
            .await
            .with_context(|| format!("link codex auth into {}", home.display()))?;
    }

    let config = codex_config_toml(&ctx);
    write_private(&home.join("config.toml"), config.as_bytes()).await?;

    let mut command = Command::new(&ctx.harness.codex_bin);
    command.arg("exec");
    if let Some(resume_token) = ctx.resume_token {
        command.arg("resume").arg(resume_token);
    }
    command
        .arg("--json")
        .arg("--skip-git-repo-check")
        .env("CODEX_HOME", &home);
    if ctx.resume_token.is_none() {
        command.arg("-m").arg(ctx.model);
    }
    command.arg(ctx.prompt());

    let mut parser = CodexStream::default();
    run_streaming(command, &mut parser, &ctx, "codex").await
}

fn codex_config_toml(ctx: &TurnCtx<'_>) -> String {
    let mut out = String::new();
    out.push_str(&format!("model = {}\n", toml_str(ctx.model)));
    if let Some(effort) = ctx.effort {
        out.push_str(&format!("model_reasoning_effort = {}\n", toml_str(effort)));
    }
    out.push_str("sandbox_mode = \"workspace-write\"\n");
    out.push_str("approval_policy = \"never\"\n");
    out.push_str("\n[sandbox_workspace_write]\nnetwork_access = true\n");
    out.push_str(&format!(
        "\n[projects.{}]\ntrust_level = \"trusted\"\n",
        toml_str(&ctx.workspace_dir.to_string_lossy())
    ));
    out.push_str("\n[mcp_servers.ceilidh]\n");
    out.push_str(&format!(
        "command = {}\n",
        toml_str(&ctx.harness.ceilidh_bin.to_string_lossy())
    ));
    let args = ctx
        .mcp_args()
        .iter()
        .map(|a| toml_str(a))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!("args = [{args}]\n"));
    out.push_str("startup_timeout_sec = 30\n");
    out.push_str(&format!("tool_timeout_sec = {MCP_TOOL_TIMEOUT_SECS}\n"));
    out.push_str("\n[mcp_servers.ceilidh.env]\n");
    out.push_str(&format!(
        "CEILIDH_TOKEN = {}\n",
        toml_str(ctx.harness.token.as_deref().unwrap_or_default())
    ));
    out
}

fn toml_str(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

async fn cursor(ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
    // Cursor reads MCP config from the workspace itself, so the file must be
    // kept out of the turn commit.
    let cursor_dir = ctx.workspace_dir.join(".cursor");
    fs::create_dir_all(&cursor_dir)
        .await
        .with_context(|| format!("create {}", cursor_dir.display()))?;
    let mcp = json!({
        "mcpServers": {
            "ceilidh": {
                "command": ctx.harness.ceilidh_bin,
                "args": ctx.mcp_args(),
                "env": { "CEILIDH_TOKEN": ctx.harness.token.clone().unwrap_or_default() }
            }
        }
    });
    let mcp_path = cursor_dir.join("mcp.json");
    write_private(&mcp_path, &serde_json::to_vec_pretty(&mcp)?).await?;
    exclude_from_git(ctx.workspace_dir, ".cursor/mcp.json").await?;

    let mut command = Command::new(&ctx.harness.cursor_bin);
    command
        .arg("-p")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--trust")
        .arg("--force")
        .arg("--approve-mcps")
        .arg("--model")
        .arg(ctx.model);
    if let Some(resume_token) = ctx.resume_token {
        command.arg("--resume").arg(resume_token);
    }
    command.arg(ctx.prompt());

    let mut parser = CursorStream::default();
    let outcome = run_streaming(command, &mut parser, &ctx, "cursor").await;

    // The config carries the caller token and lives inside the repository the
    // turn is about to commit. `.git/info/exclude` covers an untrusted file,
    // but not a repository that already tracks that path, so the file goes
    // away before the commit either way and a tracked one is restored.
    let _ = fs::remove_file(&mcp_path).await;
    let _ = restore_if_tracked(ctx.workspace_dir, ".cursor/mcp.json").await;
    outcome
}

/// Puts a path back the way the repository has it, if the repository tracks it
/// at all. Used after a harness config has been removed from a workspace.
async fn restore_if_tracked(workspace_dir: &Path, path: &str) -> Result<()> {
    let tracked = Command::new("git")
        .args(["ls-files", "--error-unmatch", "--", path])
        .current_dir(workspace_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if tracked.success() {
        Command::new("git")
            .args(["checkout", "--", path])
            .current_dir(workspace_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The shared driver
// ---------------------------------------------------------------------------

pub(crate) trait StreamParser: Send {
    /// Feed one stdout line; returns the text chunks to stream to the client.
    fn ingest_line(&mut self, line: &str) -> Result<Vec<String>>;
    fn resume_token(&self) -> Option<String>;
    fn reply_text(&self) -> String;
    /// A fatal error the stream itself reported (the process may still exit 0).
    fn failure(&self) -> Option<String>;
    /// The stream said the plan or rate limit is exhausted.
    fn capped_hint(&self) -> bool {
        false
    }
}

async fn run_streaming(
    mut command: Command,
    parser: &mut dyn StreamParser,
    ctx: &TurnCtx<'_>,
    label: &str,
) -> Result<AdapterOutcome> {
    command
        .current_dir(ctx.workspace_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A harness runs with permission prompts off, so anything in its
        // environment is readable by the model. The MCP child gets the caller
        // token through its own config block; the harness itself never needs
        // it.
        .env_remove("CEILIDH_TOKEN")
        .env_remove("CEILIDH_SERVER")
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        // The harness spawns the MCP server as a grandchild. Its own process
        // group means a cancel or a timeout can take the whole tree down
        // instead of orphaning a process that still holds a turn open.
        command.process_group(0);
    }

    info!(label, model = ctx.model, seq = ctx.seq, "spawning harness");
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {label} CLI"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("{label} stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("{label} stderr was not piped"))?;

    // A hostile or merely noisy CLI must not be able to grow the runner's
    // memory without bound; only the tail is ever reported anyway.
    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).take(STDERR_CAPTURE_LIMIT);
        let mut stderr = String::new();
        reader.read_to_string(&mut stderr).await.map(|_| stderr)
    });

    let child_pid = child.id();
    let mut lines = BufReader::new(stdout).lines();
    let timeout = time::sleep(TURN_TIMEOUT);
    tokio::pin!(timeout);
    let mut cancel = ctx.cancel.clone();

    loop {
        tokio::select! {
            _ = &mut timeout => {
                let _ = child.kill().await;
                let stderr = join_stderr(stderr_task).await;
                return Ok(AdapterOutcome::error(
                    format!("{label} timed out after 30 minutes: {}", joinable_tail(&stderr, label)),
                    parser.resume_token(),
                ));
            }
            changed = cancel.changed() => {
                if changed.is_ok() && *cancel.borrow() {
                    let _ = child.kill().await;
                    let _ = join_stderr(stderr_task).await;
                    return Ok(AdapterOutcome::cancelled(parser.resume_token()));
                }
            }
            line = lines.next_line() => {
                match line.with_context(|| format!("read {label} stdout"))? {
                    Some(line) => {
                        match parser.ingest_line(&line) {
                            Ok(chunks) => {
                                for chunk in chunks {
                                    ctx.chunk_sink.emit(chunk);
                                }
                            }
                            Err(err) => warn!(label, error = %err, "unparsed stream line"),
                        }
                    }
                    None => break,
                }
            }
        }
    }

    // A cancel that landed between the stream closing and this point would be
    // invisible to `changed()` alone, and guarding the branch on the current
    // value would disable it outright, so check the value first and then wait
    // for a change.
    if *cancel.borrow() {
        kill_tree(&mut child, child_pid).await;
        let _ = join_stderr(stderr_task).await;
        return Ok(AdapterOutcome::cancelled(parser.resume_token()));
    }

    let status = tokio::select! {
        _ = &mut timeout => {
            kill_tree(&mut child, child_pid).await;
            let stderr = join_stderr(stderr_task).await;
            return Ok(AdapterOutcome::error(
                format!("{label} timed out after 30 minutes: {}", joinable_tail(&stderr, label)),
                parser.resume_token(),
            ));
        }
        changed = cancel.changed() => {
            let _ = changed;
            kill_tree(&mut child, child_pid).await;
            let _ = join_stderr(stderr_task).await;
            return Ok(AdapterOutcome::cancelled(parser.resume_token()));
        }
        status = child.wait() => status.with_context(|| format!("wait for {label} CLI"))?,
    };

    let stderr = join_stderr(stderr_task).await;
    let resume_token = parser.resume_token();

    if let Some(failure) = parser.failure() {
        let message = format!("{label}: {failure}");
        if parser.capped_hint() || is_capped_error(&failure) {
            return Ok(AdapterOutcome::capped(message, resume_token));
        }
        return Ok(AdapterOutcome::error(message, resume_token));
    }

    if !status.success() {
        let message = format!(
            "{label} exited with {status}: {}",
            joinable_tail(&stderr, label)
        );
        if parser.capped_hint() || is_capped_error(&stderr) {
            return Ok(AdapterOutcome::capped(message, resume_token));
        }
        return Ok(AdapterOutcome::error(message, resume_token));
    }

    let reply = parser.reply_text();
    if reply.trim().is_empty() {
        let message = format!(
            "{label} exited cleanly without producing a reply: {}",
            joinable_tail(&stderr, label)
        );
        if parser.capped_hint() {
            return Ok(AdapterOutcome::capped(message, resume_token));
        }
        return Ok(AdapterOutcome::error(message, resume_token));
    }
    if parser.capped_hint() {
        return Ok(AdapterOutcome::capped(
            format!("{label} reported its plan limit was reached mid-turn"),
            resume_token,
        ));
    }
    let envelope = Envelope {
        headline: headline_from_reply(&reply, label),
        work_complete: true,
        cannot_proceed: false,
        body_markdown: reply,
        questions: Vec::new(),
    };
    Ok(AdapterOutcome::done(envelope, resume_token))
}

// ---------------------------------------------------------------------------
// Claude Code stream-json
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct ClaudeStream {
    resume_token: Option<String>,
    streamed_reply: String,
    final_reply: Option<String>,
    failure: Option<String>,
    capped: bool,
}

impl StreamParser for ClaudeStream {
    fn ingest_line(&mut self, line: &str) -> Result<Vec<String>> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("parse claude stream line: {}", truncate_chars(line, 200)))?;

        if self.resume_token.is_none() {
            self.resume_token = session_id_from(&value);
        }

        match value.get("type").and_then(Value::as_str) {
            Some("assistant") => {
                let chunks = assistant_text_chunks(&value);
                for chunk in &chunks {
                    self.streamed_reply.push_str(chunk);
                }
                Ok(chunks)
            }
            Some("result") => {
                let text = result_text(&value);
                let is_error = value
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_error {
                    self.failure = Some(
                        text.clone()
                            .unwrap_or_else(|| "claude reported an error".to_string()),
                    );
                } else if let Some(text) = text {
                    self.final_reply = Some(text);
                }
                if self.resume_token.is_none() {
                    self.resume_token = session_id_from(&value);
                }
                Ok(Vec::new())
            }
            Some("rate_limit_event") => {
                let status = value
                    .get("rate_limit_info")
                    .and_then(|info| info.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or("allowed");
                if status != "allowed" {
                    self.capped = true;
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn resume_token(&self) -> Option<String> {
        self.resume_token.clone()
    }

    fn reply_text(&self) -> String {
        self.final_reply
            .clone()
            .unwrap_or_else(|| self.streamed_reply.clone())
    }

    fn failure(&self) -> Option<String> {
        self.failure.clone()
    }

    fn capped_hint(&self) -> bool {
        self.capped
    }
}

// ---------------------------------------------------------------------------
// Codex `exec --json`
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct CodexStream {
    thread_id: Option<String>,
    messages: Vec<String>,
    failure: Option<String>,
}

impl StreamParser for CodexStream {
    fn ingest_line(&mut self, line: &str) -> Result<Vec<String>> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("parse codex stream line: {}", truncate_chars(line, 200)))?;

        match value.get("type").and_then(Value::as_str) {
            Some("thread.started") => {
                if let Some(id) = value.get("thread_id").and_then(Value::as_str) {
                    self.thread_id = Some(id.to_string());
                }
                Ok(Vec::new())
            }
            Some("item.completed") => {
                let item = value.get("item").cloned().unwrap_or(Value::Null);
                match item.get("type").and_then(Value::as_str) {
                    Some("agent_message") => {
                        let text = item
                            .get("text")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        if text.is_empty() {
                            return Ok(Vec::new());
                        }
                        let chunk = if self.messages.is_empty() {
                            text.clone()
                        } else {
                            format!("\n\n{text}")
                        };
                        self.messages.push(text);
                        Ok(vec![chunk])
                    }
                    Some("error") => {
                        // Non-fatal on its own (e.g. metadata fallback warnings);
                        // a following turn.failed decides.
                        Ok(Vec::new())
                    }
                    _ => Ok(Vec::new()),
                }
            }
            Some("item.started") => {
                let item = value.get("item").cloned().unwrap_or(Value::Null);
                if item.get("type").and_then(Value::as_str) == Some("command_execution") {
                    if let Some(cmd) = item.get("command").and_then(Value::as_str) {
                        return Ok(vec![format!("\n> running: {}\n", truncate_chars(cmd, 200))]);
                    }
                }
                Ok(Vec::new())
            }
            Some("turn.failed") => {
                let message = value
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("codex turn failed")
                    .to_string();
                self.failure = Some(message);
                Ok(Vec::new())
            }
            Some("error") => {
                if let Some(message) = value.get("message").and_then(Value::as_str) {
                    self.failure = Some(message.to_string());
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn resume_token(&self) -> Option<String> {
        self.thread_id.clone()
    }

    fn reply_text(&self) -> String {
        self.messages.join("\n\n")
    }

    fn failure(&self) -> Option<String> {
        self.failure.clone()
    }
}

// ---------------------------------------------------------------------------
// Cursor `agent -p --output-format stream-json`
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct CursorStream {
    session_id: Option<String>,
    streamed_reply: String,
    final_reply: Option<String>,
    failure: Option<String>,
}

impl StreamParser for CursorStream {
    fn ingest_line(&mut self, line: &str) -> Result<Vec<String>> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("parse cursor stream line: {}", truncate_chars(line, 200)))?;

        if self.session_id.is_none() {
            self.session_id = session_id_from(&value);
        }

        match value.get("type").and_then(Value::as_str) {
            Some("assistant") => {
                let chunks = assistant_text_chunks(&value);
                for chunk in &chunks {
                    self.streamed_reply.push_str(chunk);
                }
                Ok(chunks)
            }
            Some("result") => {
                let text = result_text(&value);
                let is_error = value
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_error {
                    self.failure = Some(
                        text.clone()
                            .unwrap_or_else(|| "cursor reported an error".to_string()),
                    );
                } else if let Some(text) = text {
                    self.final_reply = Some(text);
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn resume_token(&self) -> Option<String> {
        self.session_id.clone()
    }

    fn reply_text(&self) -> String {
        self.final_reply
            .clone()
            .unwrap_or_else(|| self.streamed_reply.clone())
    }

    fn failure(&self) -> Option<String> {
        self.failure.clone()
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

async fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .await
            .with_context(|| format!("chmod 600 {}", path.display()))?;
    }
    Ok(())
}

/// Keep a harness config file out of the turn commit without touching the
/// repo's own .gitignore.
async fn exclude_from_git(workspace_dir: &Path, pattern: &str) -> Result<()> {
    let info_dir = workspace_dir.join(".git").join("info");
    if fs::metadata(workspace_dir.join(".git")).await.is_err() {
        return Ok(());
    }
    fs::create_dir_all(&info_dir)
        .await
        .with_context(|| format!("create {}", info_dir.display()))?;
    let exclude = info_dir.join("exclude");
    let existing = fs::read_to_string(&exclude).await.unwrap_or_default();
    if existing.lines().any(|line| line.trim() == pattern) {
        return Ok(());
    }
    let mut next = existing;
    if !next.is_empty() && !next.ends_with('\n') {
        next.push('\n');
    }
    next.push_str(pattern);
    next.push('\n');
    fs::write(&exclude, next)
        .await
        .with_context(|| format!("write {}", exclude.display()))
}

fn session_id_from(value: &Value) -> Option<String> {
    value
        .get("session_id")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("message")
                .and_then(|message| message.get("session_id"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
}

fn assistant_text_chunks(value: &Value) -> Vec<String> {
    let mut chunks = Vec::new();

    if let Some(text) = value
        .get("delta")
        .and_then(|delta| delta.get("text"))
        .and_then(Value::as_str)
    {
        chunks.push(text.to_string());
    }

    if let Some(text) = value.get("text").and_then(Value::as_str) {
        chunks.push(text.to_string());
    }

    if let Some(message) = value.get("message") {
        collect_content_text(message.get("content"), &mut chunks);
        if let Some(text) = message.get("text").and_then(Value::as_str) {
            chunks.push(text.to_string());
        }
    }

    collect_content_text(value.get("content"), &mut chunks);
    chunks
}

fn result_text(value: &Value) -> Option<String> {
    if let Some(result) = value.get("result") {
        if let Some(text) = result.as_str() {
            return Some(text.to_string());
        }
        let mut chunks = Vec::new();
        collect_content_text(Some(result), &mut chunks);
        if !chunks.is_empty() {
            return Some(chunks.concat());
        }
    }

    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return Some(text.to_string());
    }

    if let Some(message) = value.get("message") {
        let mut chunks = Vec::new();
        collect_content_text(message.get("content"), &mut chunks);
        if !chunks.is_empty() {
            return Some(chunks.concat());
        }
    }

    let mut chunks = Vec::new();
    collect_content_text(value.get("content"), &mut chunks);
    if chunks.is_empty() {
        None
    } else {
        Some(chunks.concat())
    }
}

fn collect_content_text(value: Option<&Value>, chunks: &mut Vec<String>) {
    let Some(value) = value else {
        return;
    };

    match value {
        Value::Array(items) => {
            for item in items {
                collect_content_text(Some(item), chunks);
            }
        }
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(text) = map.get("text").and_then(Value::as_str) {
                    chunks.push(text.to_string());
                }
            }
            if let Some(delta) = map.get("delta") {
                if let Some(text) = delta.get("text").and_then(Value::as_str) {
                    chunks.push(text.to_string());
                }
            }
        }
        _ => {}
    }
}

fn prompt_with_history(input: &str, history_hint: &[TurnSummary]) -> String {
    let mut prompt = String::from("Prior turns:\n");
    for (idx, turn) in history_hint.iter().enumerate() {
        prompt.push_str(&format!("{}. User: {}\n", idx + 1, turn.input));
        if let Some(headline) = &turn.headline {
            prompt.push_str(&format!("   Outcome: {headline}\n"));
        }
    }
    prompt.push_str("\nCurrent turn:\n");
    prompt.push_str(input);
    prompt
}

fn headline_from_reply(reply: &str, label: &str) -> String {
    let fallback = format!("{label} turn complete");
    let headline = reply
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or(&fallback);
    truncate_chars(headline, 120)
}

/// SIGKILLs the harness and anything it spawned. The harness runs in its own
/// process group, so a negative pid reaches the MCP server too; killing only
/// the harness would leave that grandchild holding a sub-agent turn open.
async fn kill_tree(child: &mut tokio::process::Child, pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        let _ = Command::new("/bin/kill")
            .arg("-9")
            .arg(format!("-{pid}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    let _ = child.kill().await;
}

async fn join_stderr(task: tokio::task::JoinHandle<std::io::Result<String>>) -> String {
    match task.await {
        Ok(Ok(stderr)) => stderr,
        Ok(Err(err)) => format!("failed to read stderr: {err}"),
        Err(err) => format!("stderr reader task failed: {err}"),
    }
}

fn joinable_tail(stderr: &str, label: &str) -> String {
    let tail = tail_chars(stderr, STDERR_TAIL_CHARS);
    if tail.is_empty() {
        format!("{label} exited without stderr")
    } else {
        tail
    }
}

fn is_capped_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("rate limit")
        || lower.contains("rate_limit")
        || lower.contains("usage limit")
        || lower.contains("usage_limit")
        || lower.contains("capped")
        || lower.contains("cap reached")
        || lower.contains("quota")
}

pub(crate) fn truncate_chars(input: &str, max: usize) -> String {
    let len = input.chars().count();
    if len <= max {
        return input.to_string();
    }

    if max <= 3 {
        return input.chars().take(max).collect();
    }

    let mut output = input.chars().take(max - 3).collect::<String>();
    output.push_str("...");
    output
}

pub(crate) fn tail_chars(input: &str, max: usize) -> String {
    let input = input.trim();
    let len = input.chars().count();
    if len <= max {
        return input.to_string();
    }

    let tail = input
        .chars()
        .rev()
        .take(max)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("...{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(parser: &mut dyn StreamParser, fixture: &str) -> Vec<String> {
        let mut chunks = Vec::new();
        for line in fixture.lines() {
            chunks.extend(parser.ingest_line(line).unwrap());
        }
        chunks
    }

    #[test]
    fn claude_fixture_captures_session_and_final_reply() {
        let fixture = include_str!("../fixtures/claude-turn1.jsonl");
        let mut parser = ClaudeStream::default();
        let chunks = drive(&mut parser, fixture);
        assert_eq!(
            parser.resume_token().as_deref(),
            Some("71d588c4-35cb-4212-b4c7-5d7e3f306f2f")
        );
        assert_eq!(parser.reply_text(), "FIXTURE OK");
        assert!(chunks.iter().any(|c| c.contains("FIXTURE OK")));
        assert!(parser.failure().is_none());
        assert!(!parser.capped_hint());
    }

    #[test]
    fn claude_inline_deltas_and_result_still_parse() {
        let fixture = r#"{"type":"system","subtype":"init","session_id":"session-123"}
{"type":"assistant","delta":{"text":"hello"}}
{"type":"assistant","message":{"content":[{"type":"text","text":" world"}]}}
{"type":"result","result":"hello world\nsecond line"}"#;
        let mut parser = ClaudeStream::default();
        let chunks = drive(&mut parser, fixture);
        assert_eq!(parser.resume_token().as_deref(), Some("session-123"));
        assert_eq!(chunks, vec!["hello", " world"]);
        assert_eq!(parser.reply_text(), "hello world\nsecond line");
    }

    #[test]
    fn codex_fixture_captures_thread_and_messages() {
        let fixture = include_str!("../fixtures/codex-turn1.jsonl");
        let mut parser = CodexStream::default();
        let chunks = drive(&mut parser, fixture);
        assert_eq!(
            parser.resume_token().as_deref(),
            Some("01a0783f-1d2e-7920-8f8f-45e6a02be55c")
        );
        assert!(parser.reply_text().ends_with("FIXTURE OK"));
        assert_eq!(chunks.len(), 2);
        assert!(parser.failure().is_none());
    }

    #[test]
    fn codex_resume_fixture_keeps_thread_id() {
        let fixture = include_str!("../fixtures/codex-turn2.jsonl");
        let mut parser = CodexStream::default();
        drive(&mut parser, fixture);
        assert_eq!(
            parser.resume_token().as_deref(),
            Some("01a0783f-1d2e-7920-8f8f-45e6a02be55c")
        );
        assert_eq!(parser.reply_text(), "ceilidh");
    }

    #[test]
    fn codex_turn_failed_is_a_failure() {
        let fixture = include_str!("../fixtures/codex-bad.jsonl");
        let mut parser = CodexStream::default();
        drive(&mut parser, fixture);
        let failure = parser.failure().expect("turn.failed surfaces");
        assert!(failure.contains("not supported"));
    }

    #[test]
    fn cursor_fixture_captures_session_and_reply() {
        let fixture = include_str!("../fixtures/cursor-turn1.jsonl");
        let mut parser = CursorStream::default();
        let chunks = drive(&mut parser, fixture);
        assert_eq!(
            parser.resume_token().as_deref(),
            Some("b14822bb-1221-4c64-8d33-ec02b7d01889")
        );
        assert_eq!(parser.reply_text(), "FIXTURE OK");
        assert_eq!(chunks, vec!["FIXTURE OK"]);
        assert!(parser.failure().is_none());
    }

    #[test]
    fn cursor_resume_fixture_parses() {
        let fixture = include_str!("../fixtures/cursor-turn2.jsonl");
        let mut parser = CursorStream::default();
        drive(&mut parser, fixture);
        assert_eq!(parser.reply_text(), "ceilidh");
    }

    #[test]
    fn codex_config_carries_model_sandbox_and_mcp() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let (_ctx_tx, cancel) = watch::channel(false);
        let harness = HarnessConfig {
            claude_bin: "claude".into(),
            codex_bin: "codex".into(),
            cursor_bin: "cursor-agent".into(),
            ceilidh_bin: PathBuf::from("/usr/local/bin/ceilidh"),
            codex_auth: PathBuf::from("/Users/seat/.codex/auth.json"),
            server_url: "https://caller.example".into(),
            token: Some("tok\"en".into()),
        };
        let ws = PathBuf::from("/Users/seat/ws");
        let sd = PathBuf::from("/Users/seat");
        let ctx = TurnCtx {
            workspace_dir: &ws,
            session_dir: &sd,
            session_id: uuid::Uuid::nil(),
            seq: 1,
            input: "hi",
            model: "gpt-5.6-sol",
            effort: Some("high"),
            resume_token: None,
            history_hint: &[],
            chunk_sink: ChunkSink { tx },
            cancel,
            harness: &harness,
        };
        let toml = codex_config_toml(&ctx);
        assert!(toml.contains("model = \"gpt-5.6-sol\""));
        assert!(toml.contains("model_reasoning_effort = \"high\""));
        assert!(toml.contains("network_access = true"));
        assert!(toml.contains("[mcp_servers.ceilidh]"));
        assert!(toml.contains("\"--parent\", \"00000000-0000-0000-0000-000000000000\""));
        assert!(toml.contains("CEILIDH_TOKEN = \"tok\\\"en\""));
        assert!(toml.contains("[projects.\"/Users/seat/ws\"]"));
    }
}

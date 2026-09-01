//! ceilidh-runner: a band runner.
//!
//! Owned by lane/runner. The public entry-point signature is fixed by the CLI.

use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use ceilidh_protocol::{
    ClaimRequest, ClaimResponse, ClaimedWork, Envelope, Harness, Heartbeat, ReportRequest, Session,
    SessionId, TurnId, TurnStatus, TurnSummary,
};
use chrono::Utc;
use reqwest::{Client, Method, StatusCode};
use serde::Serialize;
use serde_json::Value;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{RwLock, mpsc};
use tokio::time;
use tracing::{debug, info, warn};

const CLAIM_WAIT_SECONDS: u32 = 30;
const CLAIM_TIMEOUT_SECONDS: u64 = 45;
const HEARTBEAT_SECONDS: u64 = 30;
const MIN_BACKOFF_SECONDS: u64 = 2;
const MAX_BACKOFF_SECONDS: u64 = 30;
const CLAUDE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const STDERR_TAIL_CHARS: usize = 2000;

#[derive(Debug, Clone)]
pub struct RunnerOptions {
    /// Base URL of the caller, e.g. http://127.0.0.1:8080
    pub server_url: String,
    pub token: Option<String>,
    /// Stable identity; defaults to `<host>:<user>` when None.
    pub runner_id: Option<String>,
    /// Where session workspaces live.
    pub data_dir: PathBuf,
    /// Which harnesses this runner offers.
    pub harnesses: Vec<Harness>,
}

pub async fn run(opts: RunnerOptions) -> anyhow::Result<()> {
    fs::create_dir_all(&opts.data_dir)
        .await
        .with_context(|| format!("create runner data dir {}", opts.data_dir.display()))?;

    let runner_id = opts.runner_id.unwrap_or_else(default_runner_id);
    let api = RunnerApi::new(opts.server_url, opts.token)?;
    let active_turns = Arc::new(RwLock::new(HashSet::new()));

    info!(
        runner = %runner_id,
        data_dir = %opts.data_dir.display(),
        harnesses = ?opts.harnesses,
        "ceilidh runner starting"
    );

    tokio::spawn(heartbeat_loop(
        api.clone(),
        runner_id.clone(),
        active_turns.clone(),
    ));

    let workspace_manager = WorkspaceManager::new(opts.data_dir);
    let mut backoff = Duration::from_secs(MIN_BACKOFF_SECONDS);

    loop {
        let request = ClaimRequest {
            runner: runner_id.clone(),
            harnesses: opts.harnesses.clone(),
            wait_seconds: CLAIM_WAIT_SECONDS,
        };

        match api.claim(&request).await {
            Ok(ClaimResponse::Empty) => {
                backoff = Duration::from_secs(MIN_BACKOFF_SECONDS);
            }
            Ok(ClaimResponse::Work { work }) => {
                backoff = Duration::from_secs(MIN_BACKOFF_SECONDS);
                let turn_id = work.turn.id;
                active_turns.write().await.insert(turn_id);
                process_work(api.clone(), &workspace_manager, work).await;
                active_turns.write().await.remove(&turn_id);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    sleep_seconds = backoff.as_secs(),
                    "runner claim failed"
                );
                time::sleep(backoff).await;
                backoff = next_backoff(backoff);
            }
        }
    }
}

async fn process_work(api: RunnerApi, workspace_manager: &WorkspaceManager, work: ClaimedWork) {
    let turn_id = work.turn.id;
    let report = execute_work(api.clone(), workspace_manager, work)
        .await
        .unwrap_or_else(|err| ReportRequest {
            status: TurnStatus::Error,
            envelope: None,
            error: Some(format!("{err:#}")),
            commit: None,
            resume_token: None,
        });

    if let Err(err) = api.report(turn_id, &report).await {
        warn!(turn_id = %turn_id, error = %err, "turn report failed");
    }
}

async fn execute_work(
    api: RunnerApi,
    workspace_manager: &WorkspaceManager,
    work: ClaimedWork,
) -> Result<ReportRequest> {
    let workspace = workspace_manager
        .ensure(&work.session)
        .await
        .with_context(|| format!("prepare workspace for session {}", work.session.id))?;
    let lane = work
        .turn
        .lane_override
        .as_ref()
        .unwrap_or(&work.session.lane);

    let (chunk_sink, chunk_task) = spawn_chunk_poster(api, work.turn.id);
    let ctx = TurnCtx {
        workspace_dir: workspace.path(),
        session_id: work.session.id,
        seq: work.turn.seq,
        input: &work.turn.input,
        model: &lane.model,
        effort: lane.effort.as_deref(),
        resume_token: work.turn.resume_token.as_deref(),
        history_hint: &work.history_hint,
        chunk_sink: chunk_sink.clone(),
    };

    let adapter_result = execute_adapter(lane.harness, ctx).await;
    drop(chunk_sink);
    if let Err(err) = chunk_task.await {
        warn!(turn_id = %work.turn.id, error = %err, "chunk poster task failed");
    }

    let mut outcome = match adapter_result {
        Ok(outcome) => outcome,
        Err(err) => AdapterOutcome::error(format!("{err:#}"), None),
    };

    let commit = match workspace.commit_turn(work.turn.seq).await {
        Ok(commit) => Some(commit),
        Err(err) => {
            let message = format!("git commit failed: {err:#}");
            warn!(turn_id = %work.turn.id, error = %message);
            if outcome.status == AdapterStatus::Done {
                outcome.status = AdapterStatus::Error;
                outcome.envelope = None;
                outcome.error = Some(message);
            } else {
                append_error(&mut outcome.error, message);
            }
            None
        }
    };

    if commit.is_some() && work.session.profile.allow_push {
        if let Err(err) = workspace.push().await {
            let message = format!("git push failed: {err:#}");
            warn!(
                turn_id = %work.turn.id,
                session_id = %work.session.id,
                error = %message,
            );
            // A push the operator asked for and did not get must be visible in
            // the turn, not only in a log nobody reads.
            append_error(&mut outcome.error, message.clone());
            if let Some(envelope) = outcome.envelope.as_mut() {
                envelope
                    .body_markdown
                    .push_str(&format!("\n\n> warning: {message}\n"));
            }
        }
    }

    Ok(outcome.into_report(commit))
}

async fn execute_adapter(harness: Harness, ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
    match harness {
        Harness::Mock => MockAdapter.execute(ctx).await,
        Harness::ClaudeCode => ClaudeCodeAdapter.execute(ctx).await,
        Harness::Codex => Ok(AdapterOutcome::error(
            "codex harness is not implemented by ceilidh-runner v0",
            None,
        )),
    }
}

#[derive(Debug, Clone)]
struct RunnerApi {
    client: Client,
    server_url: String,
    token: Option<String>,
}

impl RunnerApi {
    fn new(server_url: String, token: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(CLAIM_TIMEOUT_SECONDS))
            .build()
            .context("build runner HTTP client")?;
        Ok(Self {
            client,
            server_url: server_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    async fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse> {
        let response = self
            .send_json(Method::POST, "/api/runner/claim", request)
            .await?;
        let response = expect_success(response, "claim").await?;
        response
            .json::<ClaimResponse>()
            .await
            .context("decode claim response")
    }

    async fn heartbeat(&self, heartbeat: &Heartbeat) -> Result<()> {
        let response = self
            .send_json(Method::POST, "/api/runner/heartbeat", heartbeat)
            .await?;
        expect_success(response, "heartbeat").await?;
        Ok(())
    }

    async fn post_chunk(&self, turn_id: TurnId, text: &str) -> Result<()> {
        let request = ChunkRequest { text };
        let chunk_path = format!("/api/runner/turns/{turn_id}/chunk");
        let response = self
            .send_json(Method::POST, &chunk_path, &request)
            .await
            .with_context(|| format!("post chunk for turn {turn_id}"))?;

        expect_success(response, "chunk").await?;
        Ok(())
    }

    async fn report(&self, turn_id: TurnId, report: &ReportRequest) -> Result<()> {
        let path = format!("/api/runner/turns/{turn_id}/report");
        let response = self.send_json(Method::POST, &path, report).await?;
        expect_success(response, "report").await?;
        Ok(())
    }

    async fn send_json<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.server_url, path);
        let mut request = self.client.request(method, url).json(body);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        request.send().await.context("send runner HTTP request")
    }
}

#[derive(Serialize)]
struct ChunkRequest<'a> {
    text: &'a str,
}

async fn expect_success(response: reqwest::Response, label: &str) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    bail!("{label} failed with HTTP {status}: {}", tail_chars(&body, 1000));
}

async fn heartbeat_loop(
    api: RunnerApi,
    runner_id: String,
    active_turns: Arc<RwLock<HashSet<TurnId>>>,
) {
    let mut interval = time::interval(Duration::from_secs(HEARTBEAT_SECONDS));
    loop {
        interval.tick().await;
        let active_turns = active_turns
            .read()
            .await
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let heartbeat = Heartbeat {
            runner: runner_id.clone(),
            active_turns,
            at: Utc::now(),
        };
        if let Err(err) = api.heartbeat(&heartbeat).await {
            warn!(error = %err, "runner heartbeat failed");
        }
    }
}

fn spawn_chunk_poster(api: RunnerApi, turn_id: TurnId) -> (ChunkSink, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let handle = tokio::spawn(async move {
        while let Some(text) = rx.recv().await {
            if text.is_empty() {
                continue;
            }
            if let Err(err) = api.post_chunk(turn_id, &text).await {
                warn!(turn_id = %turn_id, error = %err, "stream chunk failed");
            }
        }
    });
    (ChunkSink { tx }, handle)
}

#[derive(Clone)]
struct ChunkSink {
    tx: mpsc::UnboundedSender<String>,
}

impl ChunkSink {
    fn emit(&self, text: impl Into<String>) {
        let _ = self.tx.send(text.into());
    }
}

trait BandAdapter {
    async fn execute(&self, ctx: TurnCtx<'_>) -> Result<AdapterOutcome>;
}

struct TurnCtx<'a> {
    workspace_dir: &'a Path,
    session_id: SessionId,
    seq: i64,
    input: &'a str,
    model: &'a str,
    effort: Option<&'a str>,
    resume_token: Option<&'a str>,
    history_hint: &'a [TurnSummary],
    chunk_sink: ChunkSink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AdapterStatus {
    Done,
    Capped,
    Error,
}

impl AdapterStatus {
    fn as_turn_status(&self) -> TurnStatus {
        match self {
            Self::Done => TurnStatus::Done,
            Self::Capped => TurnStatus::Capped,
            Self::Error => TurnStatus::Error,
        }
    }
}

#[derive(Debug, Clone)]
struct AdapterOutcome {
    status: AdapterStatus,
    envelope: Option<Envelope>,
    resume_token: Option<String>,
    error: Option<String>,
}

impl AdapterOutcome {
    fn done(envelope: Envelope, resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Done,
            envelope: Some(envelope),
            resume_token,
            error: None,
        }
    }

    fn capped(error: impl Into<String>, resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Capped,
            envelope: None,
            resume_token,
            error: Some(error.into()),
        }
    }

    fn error(error: impl Into<String>, resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Error,
            envelope: None,
            resume_token,
            error: Some(error.into()),
        }
    }

    fn into_report(self, commit: Option<String>) -> ReportRequest {
        ReportRequest {
            status: self.status.as_turn_status(),
            envelope: self.envelope,
            error: self.error,
            commit,
            resume_token: self.resume_token,
        }
    }
}

struct MockAdapter;

impl BandAdapter for MockAdapter {
    async fn execute(&self, ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
        let _ = (ctx.model, ctx.effort, ctx.resume_token, ctx.history_hint);
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
}

struct ClaudeCodeAdapter;

impl BandAdapter for ClaudeCodeAdapter {
    async fn execute(&self, ctx: TurnCtx<'_>) -> Result<AdapterOutcome> {
        let _ = ctx.effort;
        let prompt = if ctx.resume_token.is_none() && !ctx.history_hint.is_empty() {
            prompt_with_history(ctx.input, ctx.history_hint)
        } else {
            ctx.input.to_string()
        };

        let mut command = Command::new("claude");
        command
            .arg("-p")
            .arg(prompt)
            .arg("--model")
            .arg(ctx.model)
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .current_dir(ctx.workspace_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(resume_token) = ctx.resume_token {
            command.arg("--resume").arg(resume_token);
        }

        let mut child = command.spawn().context("spawn claude CLI")?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("claude stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("claude stderr was not piped"))?;

        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut stderr = String::new();
            reader.read_to_string(&mut stderr).await.map(|_| stderr)
        });

        let mut lines = BufReader::new(stdout).lines();
        let mut stream = ClaudeStreamState::default();
        let timeout = time::sleep(CLAUDE_TIMEOUT);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => {
                    let _ = child.kill().await;
                    let stderr = join_stderr(stderr_task).await;
                    return Ok(AdapterOutcome::error(
                        timeout_message(&stderr),
                        stream.resume_token,
                    ));
                }
                line = lines.next_line() => {
                    match line.context("read claude stdout")? {
                        Some(line) => {
                            let chunks = stream.ingest_line(&line)?;
                            for chunk in chunks {
                                ctx.chunk_sink.emit(chunk);
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        let status = tokio::select! {
            _ = &mut timeout => {
                let _ = child.kill().await;
                let stderr = join_stderr(stderr_task).await;
                return Ok(AdapterOutcome::error(
                    timeout_message(&stderr),
                    stream.resume_token,
                ));
            }
            status = child.wait() => status.context("wait for claude CLI")?,
        };

        let stderr = join_stderr(stderr_task).await;
        if !status.success() {
            let message = tail_chars(&stderr, STDERR_TAIL_CHARS);
            if is_capped_error(&stderr) {
                return Ok(AdapterOutcome::capped(message, stream.resume_token));
            }
            return Ok(AdapterOutcome::error(message, stream.resume_token));
        }

        let reply = stream.reply_text();
        let resume_token = stream.resume_token.clone();
        let envelope = Envelope {
            headline: headline_from_reply(&reply),
            work_complete: true,
            cannot_proceed: false,
            body_markdown: reply,
            questions: Vec::new(),
        };
        Ok(AdapterOutcome::done(envelope, resume_token))
    }
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
struct ClaudeStreamParse {
    resume_token: Option<String>,
    chunks: Vec<String>,
    reply: String,
}

#[derive(Debug, Default)]
struct ClaudeStreamState {
    resume_token: Option<String>,
    streamed_reply: String,
    final_reply: Option<String>,
}

impl ClaudeStreamState {
    fn ingest_line(&mut self, line: &str) -> Result<Vec<String>> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(Vec::new());
        }

        let value: Value =
            serde_json::from_str(line).with_context(|| format!("parse claude stream line: {line}"))?;

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
                if let Some(text) = result_text(&value) {
                    self.final_reply = Some(text);
                }
                if self.resume_token.is_none() {
                    self.resume_token = session_id_from(&value);
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn reply_text(&self) -> String {
        self.final_reply
            .clone()
            .unwrap_or_else(|| self.streamed_reply.clone())
    }
}

#[cfg(test)]
fn parse_claude_stream(input: &str) -> Result<ClaudeStreamParse> {
    let mut state = ClaudeStreamState::default();
    let mut chunks = Vec::new();
    for line in input.lines() {
        chunks.extend(state.ingest_line(line)?);
    }
    Ok(ClaudeStreamParse {
        resume_token: state.resume_token.clone(),
        chunks,
        reply: state.reply_text(),
    })
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

fn headline_from_reply(reply: &str) -> String {
    let headline = reply
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("Claude turn complete");
    truncate_chars(headline, 120)
}

#[derive(Debug, Clone)]
struct WorkspaceManager {
    data_dir: PathBuf,
}

impl WorkspaceManager {
    fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    async fn ensure(&self, session: &Session) -> Result<Workspace> {
        let session_dir = self.data_dir.join("sessions").join(session.id.to_string());
        let workspace_dir = session_dir.join("ws");
        let branch = session_branch(session.id);

        if fs::try_exists(&workspace_dir).await.unwrap_or(false) {
            return Ok(Workspace {
                dir: workspace_dir,
                branch,
            });
        }

        fs::create_dir_all(&session_dir)
            .await
            .with_context(|| format!("create session dir {}", session_dir.display()))?;

        if let Some(repo_url) = &session.profile.repo_url {
            validate_repo_url(repo_url)?;
            let mut args = vec!["clone".to_string()];
            if let Some(base_branch) = &session.profile.base_branch {
                validate_ref_name(base_branch)?;
                args.push("-b".to_string());
                args.push(base_branch.clone());
            }
            // `--` keeps an option-looking URL from being parsed as a git flag.
            args.push("--".to_string());
            args.push(repo_url.clone());
            args.push("ws".to_string());
            run_git(&session_dir, args).await.context("git clone")?;

            // A session that already ran elsewhere has its branch on the
            // remote; failing over must continue that history, never start a
            // fresh branch from base and silently orphan prior turns.
            if resume_remote_branch(&workspace_dir, &branch).await? {
                return Ok(Workspace {
                    dir: workspace_dir,
                    branch,
                });
            }
        } else {
            fs::create_dir_all(&workspace_dir)
                .await
                .with_context(|| format!("create workspace dir {}", workspace_dir.display()))?;
            run_git(&workspace_dir, str_args(["init"]))
                .await
                .context("git init")?;
            git_commit_allow_empty(&workspace_dir, "initial")
                .await
                .context("create initial commit")?;
        }

        checkout_session_branch(&workspace_dir, &branch).await?;
        Ok(Workspace {
            dir: workspace_dir,
            branch,
        })
    }
}

#[derive(Debug, Clone)]
struct Workspace {
    dir: PathBuf,
    branch: String,
}

impl Workspace {
    fn path(&self) -> &Path {
        &self.dir
    }

    async fn commit_turn(&self, seq: i64) -> Result<String> {
        run_git(&self.dir, str_args(["add", "-A"]))
            .await
            .context("git add")?;
        git_commit_allow_empty(&self.dir, &format!("turn {seq}"))
            .await
            .context("git commit")?;
        let output = run_git(&self.dir, str_args(["rev-parse", "HEAD"]))
            .await
            .context("git rev-parse HEAD")?;
        Ok(output.stdout.trim().to_string())
    }

    async fn push(&self) -> Result<()> {
        run_git(
            &self.dir,
            vec![
                "push".to_string(),
                "-u".to_string(),
                "origin".to_string(),
                self.branch.clone(),
            ],
        )
        .await
        .map(|_| ())
    }
}

async fn checkout_session_branch(workspace_dir: &Path, branch: &str) -> Result<()> {
    let result = run_git(
        workspace_dir,
        vec![
            "checkout".to_string(),
            "-b".to_string(),
            branch.to_string(),
        ],
    )
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(err) if format!("{err:#}").contains("already exists") => {
            run_git(
                workspace_dir,
                vec!["checkout".to_string(), branch.to_string()],
            )
            .await
            .map(|_| ())
        }
        Err(err) => Err(err).context("checkout session branch"),
    }
}

async fn git_commit_allow_empty(workspace_dir: &Path, message: &str) -> Result<CommandOutput> {
    let args = vec![
        "commit".to_string(),
        "--allow-empty".to_string(),
        "-m".to_string(),
        message.to_string(),
    ];
    match run_git(workspace_dir, args.clone()).await {
        Ok(output) => Ok(output),
        Err(err) if is_git_identity_error(&format!("{err:#}")) => {
            let mut fallback = vec![
                "-c".to_string(),
                "user.name=ceilidh-runner".to_string(),
                "-c".to_string(),
                "user.email=ceilidh-runner@example.invalid".to_string(),
            ];
            fallback.extend(args);
            run_git(workspace_dir, fallback).await
        }
        Err(err) => Err(err),
    }
}

/// A session profile arrives over the API, so its repo URL is untrusted input
/// that lands in a git argv. Only plain remote URLs are accepted.
fn validate_repo_url(repo_url: &str) -> Result<()> {
    if repo_url.starts_with("https://") || repo_url.starts_with("http://") {
        Ok(())
    } else {
        bail!("repo_url must be an http:// or https:// URL, got {repo_url:?}")
    }
}

fn validate_ref_name(name: &str) -> Result<()> {
    let shaped = !name.is_empty()
        && !name.starts_with('-')
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '/' | '.'));

    if shaped {
        Ok(())
    } else {
        bail!("invalid git ref name {name:?}")
    }
}

/// Checks out the session branch from the remote when a prior runner already
/// pushed it. Returns false when there is nothing to resume.
async fn resume_remote_branch(workspace_dir: &Path, branch: &str) -> Result<bool> {
    let listed = run_git(
        workspace_dir,
        vec![
            "ls-remote".to_string(),
            "--heads".to_string(),
            "origin".to_string(),
            branch.to_string(),
        ],
    )
    .await;

    let exists = match listed {
        Ok(output) => !output.stdout.trim().is_empty(),
        Err(err) => {
            warn!(branch = %branch, error = %err, "could not list remote session branch");
            return Ok(false);
        }
    };

    if !exists {
        return Ok(false);
    }

    run_git(
        workspace_dir,
        vec![
            "fetch".to_string(),
            "origin".to_string(),
            format!("{branch}:{branch}"),
        ],
    )
    .await
    .with_context(|| format!("fetch existing session branch {branch}"))?;

    run_git(
        workspace_dir,
        vec!["checkout".to_string(), branch.to_string()],
    )
    .await
    .with_context(|| format!("checkout existing session branch {branch}"))?;

    info!(branch = %branch, "resumed existing session branch from origin");
    Ok(true)
}

async fn run_git(workspace_dir: &Path, args: Vec<String>) -> Result<CommandOutput> {
    debug!(cwd = %workspace_dir.display(), args = ?args, "running git command");
    let output = Command::new("git")
        // A session repo is untrusted content: never let its hooks execute as
        // the runner's OS user, which holds the harness and git credentials.
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.hooksPath")
        .env("GIT_CONFIG_VALUE_0", "/dev/null")
        .args(&args)
        .current_dir(workspace_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("spawn git {}", args.join(" ")))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        Ok(CommandOutput { stdout })
    } else {
        let detail = if stderr.trim().is_empty() {
            &stdout
        } else {
            &stderr
        };
        bail!(
            "git {} failed with {}: {}",
            args.join(" "),
            output.status,
            tail_chars(detail, 4000)
        );
    }
}

#[derive(Debug, Clone)]
struct CommandOutput {
    stdout: String,
}

fn str_args<const N: usize>(args: [&str; N]) -> Vec<String> {
    args.into_iter().map(ToOwned::to_owned).collect()
}

fn session_branch(session_id: SessionId) -> String {
    format!("ceilidh/session-{session_id}")
}

fn is_git_identity_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("author identity unknown") || lower.contains("please tell me who you are")
}

fn default_runner_id() -> String {
    let hostname = command_stdout_trimmed("hostname").unwrap_or_else(|| "unknown-host".to_string());
    let username = env::var("USER")
        .ok()
        .filter(|user| !user.trim().is_empty())
        .unwrap_or_else(|| {
            command_stdout_trimmed("whoami").unwrap_or_else(|| "unknown-user".to_string())
        });
    format!("{hostname}:{}", username.trim())
}

fn command_stdout_trimmed(program: &str) -> Option<String> {
    let output = std::process::Command::new(program).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() { None } else { Some(text) }
}

fn next_backoff(current: Duration) -> Duration {
    Duration::from_secs((current.as_secs() * 2).min(MAX_BACKOFF_SECONDS))
}

fn joinable_tail(stderr: &str) -> String {
    let tail = tail_chars(stderr, STDERR_TAIL_CHARS);
    if tail.is_empty() {
        "claude exited without stderr".to_string()
    } else {
        tail
    }
}

async fn join_stderr(task: tokio::task::JoinHandle<std::io::Result<String>>) -> String {
    match task.await {
        Ok(Ok(stderr)) => stderr,
        Ok(Err(err)) => format!("failed to read stderr: {err}"),
        Err(err) => format!("stderr reader task failed: {err}"),
    }
}

fn timeout_message(stderr: &str) -> String {
    let stderr = joinable_tail(stderr);
    format!("claude timed out after 30 minutes: {stderr}")
}

fn is_capped_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("rate limit")
        || lower.contains("rate_limit")
        || lower.contains("usage limit")
        || lower.contains("usage_limit")
        || lower.contains("capped")
        || lower.contains("cap reached")
}

fn append_error(error: &mut Option<String>, message: String) {
    match error {
        Some(existing) if !existing.trim().is_empty() => {
            existing.push('\n');
            existing.push_str(&message);
        }
        _ => *error = Some(message),
    }
}

fn truncate_chars(input: &str, max: usize) -> String {
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

fn tail_chars(input: &str, max: usize) -> String {
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
    use ceilidh_protocol::{Lane, SessionProfile, SessionStatus, Turn};
    use chrono::TimeZone;
    use uuid::Uuid;

    #[tokio::test]
    async fn mock_adapter_writes_turn_file_and_echoes_chunks() {
        let dir = unique_temp_dir("mock-adapter");
        fs::create_dir_all(&dir).await.unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let session_id = Uuid::new_v4();
        let ctx = TurnCtx {
            workspace_dir: &dir,
            session_id,
            seq: 7,
            input: "hello runner",
            model: "mock",
            effort: None,
            resume_token: None,
            history_hint: &[],
            chunk_sink: ChunkSink { tx },
        };

        let outcome = MockAdapter.execute(ctx).await.unwrap();

        assert_eq!(outcome.status, AdapterStatus::Done);
        assert_eq!(
            outcome.envelope.unwrap().body_markdown,
            "echo: hello runner"
        );
        assert_eq!(outcome.resume_token, Some(format!("mock-{session_id}")));
        assert_eq!(
            fs::read_to_string(dir.join("turn-7.txt")).await.unwrap(),
            "hello runner"
        );
        assert_eq!(rx.recv().await.as_deref(), Some("mock: thinking"));
        assert_eq!(rx.recv().await.as_deref(), Some("mock: done"));
        fs::remove_dir_all(dir).await.unwrap();
    }

    #[test]
    fn claude_stream_json_parser_captures_token_chunks_and_result() {
        let fixture = r#"{"type":"system","subtype":"init","session_id":"session-123"}
{"type":"assistant","delta":{"text":"hello"}}
{"type":"assistant","message":{"content":[{"type":"text","text":" world"}]}}
{"type":"result","result":"hello world\nsecond line"}"#;

        let parsed = parse_claude_stream(fixture).unwrap();

        assert_eq!(parsed.resume_token.as_deref(), Some("session-123"));
        assert_eq!(parsed.chunks, vec!["hello", " world"]);
        assert_eq!(parsed.reply, "hello world\nsecond line");
    }

    #[tokio::test]
    async fn workspace_manager_initializes_scratch_workspace_and_commits_turn() {
        if !git_available() {
            return;
        }

        let data_dir = unique_temp_dir("workspace-manager");
        let manager = WorkspaceManager::new(data_dir.clone());
        let session = test_session(SessionProfile::default());

        let workspace = manager.ensure(&session).await.unwrap();
        assert!(fs::try_exists(workspace.path().join(".git")).await.unwrap());

        fs::write(workspace.path().join("answer.txt"), "done")
            .await
            .unwrap();
        let sha = workspace.commit_turn(1).await.unwrap();

        assert_eq!(sha.len(), 40);
        assert!(sha.chars().all(|ch| ch.is_ascii_hexdigit()));
        fs::remove_dir_all(data_dir).await.unwrap();
    }

    fn test_session(profile: SessionProfile) -> Session {
        Session {
            id: Uuid::new_v4(),
            title: "test".to_string(),
            lane: Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            },
            profile,
            runner_affinity: None,
            status: SessionStatus::Active,
            created_at: Utc.timestamp_opt(0, 0).unwrap(),
            updated_at: Utc.timestamp_opt(0, 0).unwrap(),
        }
    }

    #[allow(dead_code)]
    fn test_turn(session_id: SessionId) -> Turn {
        Turn {
            id: Uuid::new_v4(),
            session_id,
            seq: 1,
            input: "input".to_string(),
            lane_override: None,
            status: TurnStatus::Claimed,
            envelope: None,
            error: None,
            commit: None,
            resume_token: None,
            created_at: Utc.timestamp_opt(0, 0).unwrap(),
            started_at: None,
            finished_at: None,
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        env::temp_dir().join(format!("ceilidh-runner-{label}-{}", Uuid::new_v4()))
    }

    fn git_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

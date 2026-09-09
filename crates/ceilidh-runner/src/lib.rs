//! ceilidh-runner: a band runner.
//!
//! Owned by lane/runner. The public entry-point signature is fixed by the CLI.

mod adapters;

use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use ceilidh_protocol::{
    ClaimRequest, ClaimResponse, ClaimedWork, Envelope, Harness, Heartbeat, ReportRequest, Session,
    SessionId, TurnControl, TurnId, TurnStatus,
};
use chrono::Utc;
use reqwest::{Client, Method, StatusCode};
use serde::Serialize;
use tokio::fs;
use tokio::process::Command;
use tokio::sync::{RwLock, Semaphore, mpsc, watch};
use tokio::time;
use tracing::{debug, info, warn};

pub use adapters::HarnessConfig;
use adapters::{ChildRegistry, TurnCtx, tail_chars};

const CLAIM_WAIT_SECONDS: u32 = 30;
const CLAIM_TIMEOUT_SECONDS: u64 = 45;
const HEARTBEAT_SECONDS: u64 = 30;
const CANCEL_POLL_SECONDS: u64 = 2;
const REAP_INTERVAL_SECONDS: u64 = 600;
const REAP_FIRST_DELAY_SECONDS: u64 = 60;
const MIN_BACKOFF_SECONDS: u64 = 2;
const MAX_BACKOFF_SECONDS: u64 = 30;

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
    /// How many turns this runner plays at once.
    pub max_turns: usize,
    /// Harness binaries and the sub-agent MCP wiring.
    pub harness: HarnessConfig,
}

pub async fn run(opts: RunnerOptions) -> anyhow::Result<()> {
    fs::create_dir_all(&opts.data_dir)
        .await
        .with_context(|| format!("create runner data dir {}", opts.data_dir.display()))?;

    let runner_id = opts.runner_id.unwrap_or_else(default_runner_id);
    // One id per process, so the caller can tell this process from an orphan
    // that survived a redeploy under the same runner id.
    let epoch = uuid::Uuid::new_v4().to_string();
    let api = RunnerApi::new(opts.server_url, opts.token)?;
    let active_turns = Arc::new(RwLock::new(HashSet::new()));
    let max_turns = opts.max_turns.max(1);
    let slots = Arc::new(Semaphore::new(max_turns));
    let stripped = adapters::stripped_env(&opts.harness.pass_env);
    let passed = opts.harness.pass_env.clone();
    let harness = Arc::new(opts.harness);

    info!(
        runner = %runner_id,
        epoch = %epoch,
        data_dir = %opts.data_dir.display(),
        harnesses = ?opts.harnesses,
        max_turns,
        "ceilidh runner starting"
    );
    // Which vendor keys a harness child can see decides whether a subscription
    // lane bills, so it is said out loud at startup rather than assumed.
    info!(
        stripped = %name_list(&stripped),
        passed = %name_list(&passed),
        "harness child environment"
    );

    tokio::spawn(heartbeat_loop(
        api.clone(),
        runner_id.clone(),
        epoch.clone(),
        active_turns.clone(),
    ));

    let workspace_manager = Arc::new(WorkspaceManager::new(opts.data_dir.clone()));
    tokio::spawn(reap_loop(api.clone(), opts.data_dir.clone()));
    let children = Arc::new(ChildRegistry::default());
    tokio::spawn(shutdown_on_signal(children.clone()));
    let mut backoff = Duration::from_secs(MIN_BACKOFF_SECONDS);

    loop {
        // Hold a slot before asking for work, so a full runner stops claiming
        // instead of queueing turns it cannot start.
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .context("runner slot semaphore closed")?;

        let request = ClaimRequest {
            runner: runner_id.clone(),
            harnesses: opts.harnesses.clone(),
            wait_seconds: CLAIM_WAIT_SECONDS,
            epoch: Some(epoch.clone()),
        };

        match api.claim(&request).await {
            Ok(ClaimResponse::Empty) => {
                drop(permit);
                backoff = Duration::from_secs(MIN_BACKOFF_SECONDS);
            }
            Ok(ClaimResponse::Work { work }) => {
                backoff = Duration::from_secs(MIN_BACKOFF_SECONDS);
                let turn_id = work.turn.id;
                active_turns.write().await.insert(turn_id);
                let api = api.clone();
                let workspace_manager = workspace_manager.clone();
                let active_turns = active_turns.clone();
                let harness = harness.clone();
                let children = children.clone();
                tokio::spawn(async move {
                    process_work(api, &workspace_manager, &harness, &children, work).await;
                    active_turns.write().await.remove(&turn_id);
                    drop(permit);
                });
            }
            Err(err) => {
                drop(permit);
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

/// A runner told to stop takes its harnesses with it. Without this, a
/// redeploy or a launchd restart leaves the old harness running in the
/// workspace while the new runner process reclaims the same turn and starts
/// another, and two agents share one checkout.
async fn shutdown_on_signal(children: Arc<ChildRegistry>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(term) => term,
            Err(err) => {
                warn!(error = %err, "cannot listen for SIGTERM; harnesses will outlive a restart");
                return;
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(int) => int,
            Err(err) => {
                warn!(error = %err, "cannot listen for SIGINT");
                return;
            }
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        let killed = children.terminate_all();
        info!(killed, "shutting down; harness process groups terminated");
        if killed > 0 {
            time::sleep(Duration::from_secs(3)).await;
            children.kill_all();
        }
        std::process::exit(0);
    }
    #[cfg(not(unix))]
    {
        let _ = children;
    }
}

async fn process_work(
    api: RunnerApi,
    workspace_manager: &WorkspaceManager,
    harness: &HarnessConfig,
    children: &ChildRegistry,
    work: ClaimedWork,
) {
    let turn_id = work.turn.id;
    let report = execute_work(api.clone(), workspace_manager, harness, children, work)
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
    harness: &HarnessConfig,
    children: &ChildRegistry,
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

    let (chunk_sink, chunk_task) = spawn_chunk_poster(api.clone(), work.turn.id);
    let (cancel_rx, cancel_task) = spawn_cancel_watcher(api, work.turn.id);
    let ctx = TurnCtx {
        workspace_dir: workspace.path(),
        session_title: &work.session.title,
        session_branch: workspace.branch(),
        session_dir: workspace.session_dir(),
        session_id: work.session.id,
        seq: work.turn.seq,
        input: &work.turn.input,
        model: &lane.model,
        effort: lane.effort.as_deref(),
        resume_token: work.turn.resume_token.as_deref(),
        history_hint: &work.history_hint,
        chunk_sink: chunk_sink.clone(),
        cancel: cancel_rx,
        harness,
        children,
    };

    let adapter_result = adapters::execute(lane.harness, ctx).await;
    cancel_task.abort();
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

/// A deleted session's workspace is the one thing the caller cannot remove
/// itself. Every ten minutes the runner asks about each workspace it holds
/// and drops the ones the caller no longer knows. Archived sessions keep
/// theirs: they can come back, and an unpushed branch would be lost.
async fn reap_loop(api: RunnerApi, data_dir: PathBuf) {
    time::sleep(Duration::from_secs(REAP_FIRST_DELAY_SECONDS)).await;
    let mut interval = time::interval(Duration::from_secs(REAP_INTERVAL_SECONDS));
    loop {
        interval.tick().await;
        let sessions_dir = data_dir.join("sessions");
        let Ok(mut entries) = fs::read_dir(&sessions_dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            let Ok(session_id) = uuid::Uuid::parse_str(&name) else {
                continue;
            };
            match api.session_exists(session_id).await {
                Ok(false) => {
                    let path = entry.path();
                    match fs::remove_dir_all(&path).await {
                        Ok(()) => info!(session = %session_id, "removed the workspace of a deleted session"),
                        Err(err) => warn!(session = %session_id, error = %err, "could not remove workspace"),
                    }
                }
                Ok(true) => {}
                Err(err) => debug!(session = %session_id, error = %err, "reap check failed"),
            }
        }
    }
}

/// Polls the caller while a turn runs; flips the watch when a cancel lands.
fn spawn_cancel_watcher(
    api: RunnerApi,
    turn_id: TurnId,
) -> (watch::Receiver<bool>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(CANCEL_POLL_SECONDS));
        loop {
            interval.tick().await;
            match api.control(turn_id).await {
                Ok(control) if control.cancel_requested => {
                    let _ = tx.send(true);
                    return;
                }
                Ok(_) => {}
                Err(err) => debug!(turn_id = %turn_id, error = %err, "cancel poll failed"),
            }
        }
    });
    (rx, handle)
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

    /// Ok(false) only on a definite 404; any other failure is an error, so a
    /// caller outage never reads as "delete everything".
    async fn session_exists(&self, session_id: SessionId) -> Result<bool> {
        let url = format!("{}/api/sessions/{session_id}", self.server_url);
        let mut request = self.client.get(url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.context("send session lookup")?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(false),
            status if status.is_success() => Ok(true),
            status => bail!("session lookup failed with HTTP {status}"),
        }
    }

    async fn control(&self, turn_id: TurnId) -> Result<TurnControl> {
        let url = format!("{}/api/runner/turns/{turn_id}/control", self.server_url);
        let mut request = self.client.get(url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.context("send control poll")?;
        let response = expect_success(response, "control").await?;
        response
            .json::<TurnControl>()
            .await
            .context("decode control response")
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
    epoch: String,
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
            epoch: Some(epoch.clone()),
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
pub(crate) struct ChunkSink {
    pub(crate) tx: mpsc::UnboundedSender<String>,
}

impl ChunkSink {
    pub(crate) fn emit(&self, text: impl Into<String>) {
        let _ = self.tx.send(text.into());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdapterStatus {
    Done,
    Capped,
    Error,
    Cancelled,
}

impl AdapterStatus {
    fn as_turn_status(&self) -> TurnStatus {
        match self {
            Self::Done => TurnStatus::Done,
            Self::Capped => TurnStatus::Capped,
            Self::Error => TurnStatus::Error,
            Self::Cancelled => TurnStatus::Cancelled,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AdapterOutcome {
    pub(crate) status: AdapterStatus,
    pub(crate) envelope: Option<Envelope>,
    pub(crate) resume_token: Option<String>,
    pub(crate) error: Option<String>,
}

impl AdapterOutcome {
    pub(crate) fn done(envelope: Envelope, resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Done,
            envelope: Some(envelope),
            resume_token,
            error: None,
        }
    }

    pub(crate) fn capped(error: impl Into<String>, resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Capped,
            envelope: None,
            resume_token,
            error: Some(error.into()),
        }
    }

    pub(crate) fn error(error: impl Into<String>, resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Error,
            envelope: None,
            resume_token,
            error: Some(error.into()),
        }
    }

    pub(crate) fn cancelled(resume_token: Option<String>) -> Self {
        Self {
            status: AdapterStatus::Cancelled,
            envelope: None,
            resume_token,
            error: Some("cancelled by the user".to_string()),
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
        let branch = session_branch(session);

        if fs::try_exists(&workspace_dir).await.unwrap_or(false) {
            return Ok(Workspace {
                dir: workspace_dir,
                session_dir,
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
                    session_dir,
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
            session_dir,
            branch,
        })
    }
}

#[derive(Debug, Clone)]
struct Workspace {
    dir: PathBuf,
    session_dir: PathBuf,
    branch: String,
}

impl Workspace {
    fn path(&self) -> &Path {
        &self.dir
    }

    fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    fn branch(&self) -> &str {
        &self.branch
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
/// that lands in a git argv. Accepted forms: an http(s) URL, or a local git
/// repository named by an absolute path (a plain `/...` path, or a
/// `file://...` URL naming one). A relative path, a `~`-relative path, and an
/// absolute path that is not a git repository are all rejected before the
/// value ever reaches `git clone`.
fn validate_repo_url(repo_url: &str) -> Result<()> {
    if repo_url.starts_with("https://") || repo_url.starts_with("http://") {
        return Ok(());
    }

    let path = Path::new(repo_url.strip_prefix("file://").unwrap_or(repo_url));
    if path.is_absolute() && is_git_repository(path) {
        return Ok(());
    }

    bail!(
        "repo_url must be an absolute path to an existing git repository, or an http(s) URL, got {repo_url:?}"
    )
}

/// True for an ordinary checkout (a `.git` directory at the root) or a bare
/// repository (`HEAD` and `objects` present at the root).
fn is_git_repository(path: &Path) -> bool {
    // `.git` is a directory in a normal checkout and a file (a gitdir pointer)
    // inside a git worktree; both are clonable sources.
    path.join(".git").exists() || (path.join("HEAD").is_file() && path.join("objects").is_dir())
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

/// Every git this runner spawns is built here, so no call site can miss the
/// hardening.
pub(crate) fn git_command(workspace_dir: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .current_dir(workspace_dir)
        // A session repo is untrusted content: never let its hooks execute as
        // the runner's OS user, which holds the harness and git credentials.
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.hooksPath")
        .env("GIT_CONFIG_VALUE_0", "/dev/null")
        // A runner slot is scarce and nobody is watching this terminal: a
        // missing credential must fail the turn, not hold the slot open on a
        // prompt no one can answer.
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null());
    command
}

async fn run_git(workspace_dir: &Path, args: Vec<String>) -> Result<CommandOutput> {
    debug!(cwd = %workspace_dir.display(), args = ?args, "running git command");
    let output = git_command(workspace_dir)
        .args(&args)
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

/// `ceilidh/<title-slug>-<id8>`: readable on GitHub, unique per session.
fn session_branch(session: &Session) -> String {
    let id8: String = session.id.simple().to_string().chars().take(8).collect();
    let slug = slugify(&session.title);
    if slug.is_empty() {
        format!("ceilidh/session-{id8}")
    } else {
        format!("ceilidh/{slug}-{id8}")
    }
}

fn slugify(title: &str) -> String {
    let mut out = String::new();
    let mut last_dash = true;
    for ch in title.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
        if out.len() >= 40 {
            break;
        }
    }
    out.trim_matches('-').to_string()
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

/// A comma-separated list for a log line, or "none" when it is empty.
fn name_list<S: AsRef<str>>(names: &[S]) -> String {
    if names.is_empty() {
        return "none".to_string();
    }
    names
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ")
}

fn next_backoff(current: Duration) -> Duration {
    Duration::from_secs((current.as_secs() * 2).min(MAX_BACKOFF_SECONDS))
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



#[cfg(test)]
mod tests {
    use super::*;
    use ceilidh_protocol::{Lane, SessionProfile, SessionStatus};
    use chrono::TimeZone;
    use uuid::Uuid;

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
        assert!(workspace.branch.starts_with("ceilidh/test-session-"));

        fs::write(workspace.path().join("answer.txt"), "done")
            .await
            .unwrap();
        let sha = workspace.commit_turn(1).await.unwrap();

        assert_eq!(sha.len(), 40);
        assert!(sha.chars().all(|ch| ch.is_ascii_hexdigit()));
        fs::remove_dir_all(data_dir).await.unwrap();
    }

    /// A missing credential must fail the turn, not hold a runner slot open on
    /// a prompt nobody can see.
    #[test]
    fn git_never_asks_the_terminal_for_a_credential() {
        let command = git_command(Path::new("/tmp"));
        let prompt = command
            .as_std()
            .get_envs()
            .find(|(name, _)| *name == "GIT_TERMINAL_PROMPT")
            .and_then(|(_, value)| value);
        assert_eq!(prompt, Some("0".as_ref()));
    }

    #[test]
    fn branch_names_are_slugged_and_unique() {
        let mut session = test_session(SessionProfile::default());
        session.title = "Fix the Login Bug!!".to_string();
        let branch = session_branch(&session);
        assert!(branch.starts_with("ceilidh/fix-the-login-bug-"));
        session.title = "   ".to_string();
        assert!(session_branch(&session).starts_with("ceilidh/session-"));
        assert_eq!(slugify("A".repeat(80).as_str()).len(), 40);
    }

    #[test]
    fn validate_repo_url_accepts_http_and_https() {
        assert!(validate_repo_url("https://example.com/repo.git").is_ok());
        assert!(validate_repo_url("http://example.com/repo.git").is_ok());
    }

    #[test]
    fn validate_repo_url_accepts_an_absolute_path_to_a_checkout() {
        if !git_available() {
            return;
        }
        let dir = unique_temp_dir("validate-checkout");
        init_git_repo(&dir, false);

        assert!(validate_repo_url(dir.to_str().unwrap()).is_ok());
        assert!(validate_repo_url(&format!("file://{}", dir.display())).is_ok());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn validate_repo_url_accepts_an_absolute_path_to_a_bare_repository() {
        if !git_available() {
            return;
        }
        let dir = unique_temp_dir("validate-bare");
        init_git_repo(&dir, true);

        assert!(validate_repo_url(dir.to_str().unwrap()).is_ok());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn validate_repo_url_rejects_relative_and_home_relative_paths() {
        assert!(validate_repo_url("relative/repo").is_err());
        assert!(validate_repo_url("./relative/repo").is_err());
        assert!(validate_repo_url("~/repo").is_err());
    }

    #[test]
    fn validate_repo_url_rejects_an_absolute_path_that_is_not_a_git_repository() {
        let dir = unique_temp_dir("validate-non-git");
        std::fs::create_dir_all(&dir).unwrap();

        let err = validate_repo_url(dir.to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("absolute path to an existing git repository"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn validate_repo_url_rejects_a_nonexistent_absolute_path() {
        let dir = unique_temp_dir("validate-missing");
        assert!(validate_repo_url(dir.to_str().unwrap()).is_err());
    }

    /// Runs `git init` (optionally `--bare`) in a freshly created directory.
    fn init_git_repo(dir: &Path, bare: bool) {
        std::fs::create_dir_all(dir).unwrap();
        let mut args = vec!["init"];
        if bare {
            args.push("--bare");
        }
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn test_session(profile: SessionProfile) -> Session {
        Session {
            id: Uuid::new_v4(),
            title: "test session".to_string(),
            lane: Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            },
            profile,
            runner_affinity: None,
            status: SessionStatus::Active,
            parent_id: None,
            created_at: Utc.timestamp_opt(0, 0).unwrap(),
            updated_at: Utc.timestamp_opt(0, 0).unwrap(),
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

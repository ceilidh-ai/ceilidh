//! ceilidh-server: the caller's HTTP surface.
//!
//! Owned by lane/caller-server. The `serve` entry point is the public contract
//! used by the CLI.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path as AxumPath, Query, Request, State};
use axum::http::HeaderMap;
use axum::response::Redirect;
use axum::http::header;
use axum::http::{StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive};
use axum::response::{Html, IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
mod auth;
mod repos;

use ceilidh_protocol::{
    CallerConfig, ClaimRequest, ClaimResponse, ClaimedWork, CreateSessionRequest, Envelope, Event,
    Harness, Heartbeat, ModelChoice, PostTurnRequest, ReportRequest, RunnerId, RunnerStatusInfo,
    RepoList, Session, SessionId, SessionStatus, Turn, TurnControl, TurnId, TurnStatus,
    TurnSummary,
};
use chrono::{DateTime, TimeDelta, Utc};
use futures_core::Stream;
use futures_util::stream;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};
use tokio::net::TcpListener;
use tokio::sync::{Notify, broadcast};
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

const ONLINE_WINDOW_SECS: i64 = 60;
/// A turn claimed this recently may not yet be in its runner's heartbeat set.
/// It exceeds the runner's own HTTP timeout on purpose: a heartbeat body is
/// built before it is sent, so a slow request can carry a snapshot that is
/// already stale by that whole timeout.
const HELD_TURN_GRACE_SECS: i64 = 90;
const CLAIM_WAIT_CAP_SECS: u32 = 30;
const SSE_KEEP_ALIVE_SECS: u64 = 15;
const HISTORY_HINT_LIMIT: i64 = 8;
/// How many messages may wait behind a running turn.
const MAX_QUEUED_TURNS_PER_SESSION: i64 = 5;

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub bind: SocketAddr,
    /// SQLite database file; created if absent.
    pub db_path: PathBuf,
    /// Bearer token required on every API request when set.
    pub token: Option<String>,
    /// Repository prefilled into the new-session form (https URL).
    pub default_repo_url: Option<String>,
    /// Read-only GitHub token used to offer the repository picker. Absent
    /// means the client falls back to a free-text repository field.
    pub github_token: Option<String>,
    /// Sign in with Google for the browser; None keeps the token screen.
    pub google: Option<auth::GoogleAuth>,
    /// Key for the login cookies; defaults to the bearer token, so rotating
    /// the token signs every browser out.
    pub cookie_secret: Option<String>,
}

pub use auth::GoogleAuth;

pub async fn serve(opts: ServeOptions) -> anyhow::Result<()> {
    serve_with_web_dir(opts, None).await
}

pub async fn serve_with_web_dir(
    opts: ServeOptions,
    web_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let bind = opts.bind;
    let app = build_app_with_web_dir(opts, web_dir).await?;
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "ceilidh caller listening");
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn build_app(opts: ServeOptions) -> anyhow::Result<Router> {
    build_app_with_web_dir(opts, None).await
}

pub async fn build_app_with_web_dir(
    opts: ServeOptions,
    web_dir: Option<PathBuf>,
) -> anyhow::Result<Router> {
    let pool = open_pool(&opts.db_path).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let (events, _) = broadcast::channel(1024);
    let token_for_cookies = opts.token.clone().filter(|token| !token.is_empty());
    let state = AppState {
        pool,
        events,
        notify: Arc::new(Notify::new()),
        token: opts.token.filter(|token| !token.is_empty()).map(Arc::from),
        config: Arc::new(CallerConfig {
            default_repo_url: opts.default_repo_url.filter(|url| !url.trim().is_empty()),
            models: model_menu(),
        }),
        repos: repos::RepoCatalog::new(opts.github_token),
        signer: auth::CookieSigner::new(
            opts.cookie_secret
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .or(token_for_cookies.as_deref())
                .unwrap_or("ceilidh-loopback"),
        ),
        google: opts.google.map(Arc::new),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap_or_default(),
    };

    let api = Router::new()
        .route("/config", get(get_config))
        .route("/repos", get(get_repos))
        .route("/me", get(auth_me))
        .route("/sessions", post(create_session).get(list_sessions))
        .route("/sessions/{id}", get(get_session).delete(delete_session))
        .route("/sessions/{id}/archive", post(archive_session))
        .route("/sessions/{id}/unarchive", post(unarchive_session))
        .route(
            "/sessions/{id}/turns",
            post(post_turn).get(list_session_turns),
        )
        .route("/sessions/{id}/turns/{turn_id}/cancel", post(cancel_turn))
        .route("/sessions/{id}/events", get(session_events))
        .route("/events", get(all_events))
        .route("/runners", get(list_runners))
        .route("/runner/claim", post(claim_turn))
        .route("/runner/turns/{id}/chunk", post(post_chunk))
        .route("/runner/turns/{id}/control", get(turn_control))
        .route("/runner/turns/{id}/report", post(report_turn))
        .route("/runner/heartbeat", post(heartbeat))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_auth,
        ))
        .with_state(state.clone());

    let auth_routes = Router::new()
        .route("/auth/config", get(auth_config))
        .route("/auth/login", get(auth_login))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/logout", post(auth_logout))
        .with_state(state.clone());

    let app = Router::new()
        .route("/health", get(health))
        .merge(auth_routes)
        .nest("/api", api)
        .with_state(state);

    Ok(match web_dir.filter(|dir| dir.is_dir()) {
        Some(dir) => app.fallback_service(
            ServeDir::new(&dir).fallback(ServeFile::new(dir.join("index.html"))),
        ),
        None => app.route("/", get(placeholder)),
    })
}

#[derive(Clone)]
struct AppState {
    pool: SqlitePool,
    events: broadcast::Sender<BroadcastEvent>,
    notify: Arc<Notify>,
    token: Option<Arc<str>>,
    config: Arc<CallerConfig>,
    repos: repos::RepoCatalog,
    signer: auth::CookieSigner,
    google: Option<Arc<auth::GoogleAuth>>,
    http: reqwest::Client,
}

/// The model menu the UI offers, grouped by vendor. Every row is a string
/// the harness CLI accepts verbatim; the form also takes a free-text model
/// so a new release never waits on a rebuild.
fn model_menu() -> Vec<ModelChoice> {
    fn row(vendor: &str, harness: Harness, model: &str, label: &str) -> ModelChoice {
        ModelChoice {
            vendor: vendor.to_string(),
            harness,
            model: model.to_string(),
            label: label.to_string(),
        }
    }
    let a = |m: &str, l: &str| row("Anthropic", Harness::ClaudeCode, m, l);
    let o = |m: &str, l: &str| row("OpenAI", Harness::Codex, m, l);
    let c = |m: &str, l: &str| row("Cursor", Harness::Cursor, m, l);
    vec![
        a("claude-fable-5-1", "Claude Fable 5.1"),
        a("claude-opus-5", "Claude Opus 5"),
        a("claude-opus-4-8", "Claude Opus 4.8"),
        a("claude-sonnet-5", "Claude Sonnet 5"),
        a("claude-sonnet-4-6", "Claude Sonnet 4.6"),
        a("claude-haiku-4-5", "Claude Haiku 4.5"),
        o("gpt-5.6-sol", "GPT-5.6 Sol"),
        o("gpt-5.6-terra", "GPT-5.6 Terra"),
        o("gpt-5.6-luna", "GPT-5.6 Luna"),
        o("gpt-5.5", "GPT-5.5"),
        c("cursor-grok-4.6-high", "Grok 4.6"),
        c("cursor-grok-4.6-xhigh", "Grok 4.6 Extra High"),
        c("gemini-3.7-flash-high", "Gemini 3.7 Flash"),
        c("gpt-5.6-sol-high", "GPT-5.6 Sol High (Cursor)"),
        c("gpt-5.6-luna-high", "GPT-5.6 Luna High (Cursor)"),
        c("composer-2.5", "Composer 2.5"),
        c("claude-opus-5-thinking-high", "Claude Opus 5 Thinking (Cursor)"),
        c("claude-sonnet-5-thinking-high", "Claude Sonnet 5 Thinking (Cursor)"),
    ]
}

async fn get_config(State(state): State<AppState>) -> Json<CallerConfig> {
    Json((*state.config).clone())
}

async fn get_repos(State(state): State<AppState>) -> Json<RepoList> {
    Json(state.repos.list().await)
}

#[derive(Debug, Clone)]
struct BroadcastEvent {
    session_id: Option<SessionId>,
    event: Event,
}

impl AppState {
    fn emit(&self, session_id: Option<SessionId>, event: Event) {
        let _ = self.events.send(BroadcastEvent { session_id, event });
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized")
    }

    fn not_found(entity: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, format!("{entity} not found"))
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }

    fn unprocessable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, message)
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "server error");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(value: sqlx::Error) -> Self {
        Self::internal(value)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(value: serde_json::Error) -> Self {
        Self::internal(value)
    }
}

impl From<chrono::ParseError> for ApiError {
    fn from(value: chrono::ParseError) -> Self {
        Self::internal(value)
    }
}

async fn open_pool(db_path: &Path) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = db_path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }

    // Several runners long-poll, heartbeat, and stream chunks against one
    // SQLite file, and the default rollback journal serializes them so hard
    // that an ordinary request can lose the race and 500 (observed with three
    // runners connected). WAL lets readers run beside the writer, and the busy
    // timeout makes a contended write wait its turn instead of failing.
    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(10));

    Ok(SqlitePoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(15))
        .connect_with(options)
        .await?)
}

async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    match state.token.as_deref() {
        Some(token)
            if !request_has_token(&req, token)
                && signed_in_email(req.headers(), &state.signer).is_none() =>
        {
            Err(ApiError::unauthorized())
        }
        _ => Ok(next.run(req).await),
    }
}

fn request_has_token(req: &Request, token: &str) -> bool {
    let header_ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value == token);

    header_ok || query_token_matches(req.uri(), token)
}

fn query_token_matches(uri: &Uri, token: &str) -> bool {
    uri.query()
        .into_iter()
        .flat_map(|query| query.split('&'))
        .filter_map(|pair| pair.split_once('='))
        .any(|(key, value)| key == "token" && value == token)
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true }))
}

fn signed_in_email(headers: &HeaderMap, signer: &auth::CookieSigner) -> Option<String> {
    let header = headers.get(header::COOKIE)?.to_str().ok()?;
    let value = auth::cookie_value(header, auth::SESSION_COOKIE)?;
    signer.verify("session", value)
}

/// Whether the browser can sign in with Google; the client picks its login
/// screen from this.
async fn auth_config(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "google": state.google.is_some() }))
}

async fn auth_login(State(state): State<AppState>) -> Response {
    let Some(google) = state.google.as_ref() else {
        return ApiError::not_found("google sign-in").into_response();
    };
    let nonce = auth::random_token();
    let mut headers = HeaderMap::new();
    if let Ok(value) = auth::state_cookie(&state.signer, &nonce).parse() {
        headers.insert(header::SET_COOKIE, value);
    }
    (headers, Redirect::to(&google.authorize_url(&nonce))).into_response()
}

#[derive(Debug, Default, Deserialize)]
struct CallbackQuery {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

async fn auth_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CallbackQuery>,
) -> Response {
    let Some(google) = state.google.as_ref() else {
        return ApiError::not_found("google sign-in").into_response();
    };
    let refuse = |why: &str| {
        (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            format!(
                "<!doctype html><title>ceilidh</title><main style=\"font-family:system-ui;padding:2rem\"><h1>Not signed in</h1><p>{why}</p><p><a href=\"/\">Back</a></p></main>"
            ),
        )
            .into_response()
    };

    if let Some(error) = query.error {
        return refuse(&format!("Google said: {}", html_escape(&error)));
    }
    let (Some(code), Some(returned_state)) = (query.code, query.state) else {
        return refuse("The sign-in reply was incomplete. Try again.");
    };
    let expected = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| auth::cookie_value(h, auth::STATE_COOKIE))
        .and_then(|v| state.signer.verify("state", v));
    if expected.as_deref() != Some(returned_state.as_str()) {
        return refuse("The sign-in did not start from this browser. Try again.");
    }

    let info = match auth::exchange_code(&state.http, google, &code).await {
        Ok(info) => info,
        Err(err) => {
            tracing::warn!(error = %err, "google code exchange failed");
            return refuse("Google did not confirm the sign-in. Try again.");
        }
    };
    let Some(email) = info.email.filter(|_| info.email_verified) else {
        return refuse("Google did not return a verified email address.");
    };
    if !google.allows(&email) {
        tracing::warn!(email = %email, "sign-in refused: not on the allowlist");
        return refuse(&format!(
            "{} is not allowed in here.",
            html_escape(&email)
        ));
    }

    tracing::info!(email = %email, "signed in with google");
    let mut out = HeaderMap::new();
    if let Ok(value) = auth::session_cookie(&state.signer, &email).parse() {
        out.append(header::SET_COOKIE, value);
    }
    if let Ok(value) = auth::clear_cookie(auth::STATE_COOKIE, "/auth").parse() {
        out.append(header::SET_COOKIE, value);
    }
    (out, Redirect::to("/")).into_response()
}

async fn auth_logout() -> Response {
    let mut out = HeaderMap::new();
    if let Ok(value) = auth::clear_cookie(auth::SESSION_COOKIE, "/").parse() {
        out.insert(header::SET_COOKIE, value);
    }
    (StatusCode::NO_CONTENT, out).into_response()
}

/// Who the caller thinks is asking: the signed-in email, or the machine
/// token. Behind the auth middleware, so an anonymous request is a 401.
async fn auth_me(State(state): State<AppState>, headers: HeaderMap) -> Json<Value> {
    match signed_in_email(&headers, &state.signer) {
        Some(email) => Json(json!({ "email": email, "via": "google" })),
        None => Json(json!({ "email": null, "via": "token" })),
    }
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn placeholder() -> Html<&'static str> {
    Html("<!doctype html><title>ceilidh</title><main>ceilidh caller is running</main>")
}

async fn create_session(
    State(state): State<AppState>,
    payload: Result<Json<CreateSessionRequest>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let payload = parse_json(payload)?;
    if payload.title.trim().is_empty() {
        return Err(ApiError::unprocessable("title is required"));
    }

    if let Some(parent_id) = payload.parent_id {
        ensure_session_exists(&state.pool, parent_id)
            .await
            .map_err(|_| ApiError::unprocessable("parent_id does not name a session"))?;
    }

    let now = Utc::now();
    let session = Session {
        id: Uuid::new_v4(),
        title: payload.title,
        lane: payload.lane.unwrap_or_default(),
        profile: payload.profile.unwrap_or_default(),
        runner_affinity: None,
        status: SessionStatus::Active,
        parent_id: payload.parent_id,
        created_at: now,
        updated_at: now,
    };

    sqlx::query(
        r#"
        INSERT INTO sessions
            (id, title, lane_json, profile_json, runner_affinity, status, parent_id,
             created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(session.id.to_string())
    .bind(&session.title)
    .bind(json_string(&session.lane)?)
    .bind(json_string(&session.profile)?)
    .bind(&session.runner_affinity)
    .bind(enum_string(&session.status)?)
    .bind(session.parent_id.map(|id| id.to_string()))
    .bind(dt_string(session.created_at))
    .bind(dt_string(session.updated_at))
    .execute(&state.pool)
    .await?;

    state.emit(
        session.parent_id,
        Event::SessionCreated {
            session: session.clone(),
        },
    );

    Ok((StatusCode::CREATED, Json(session)))
}

#[derive(Debug, Default, Deserialize)]
struct ListSessionsQuery {
    /// `archived` to include archived sessions; the default list is active only.
    #[serde(default)]
    include: Option<String>,
}

async fn list_sessions(
    State(state): State<AppState>,
    Query(query): Query<ListSessionsQuery>,
) -> Result<Json<Vec<Session>>, ApiError> {
    let include_archived = query.include.as_deref() == Some("archived");
    let rows = sqlx::query(
        r#"
        SELECT id, title, lane_json, profile_json, runner_affinity, status, parent_id,
               created_at, updated_at
        FROM sessions
        WHERE (? OR status = ?)
        ORDER BY created_at DESC
        "#,
    )
    .bind(include_archived)
    .bind(enum_string(&SessionStatus::Active)?)
    .fetch_all(&state.pool)
    .await?;

    rows.into_iter()
        .map(|row| session_from_row(&row, ""))
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

/// The session plus every descendant, parents before children.
async fn session_tree(pool: &SqlitePool, root: SessionId) -> Result<Vec<Session>, ApiError> {
    let mut out = vec![get_session_by_id(pool, root).await?];
    let mut cursor = 0;
    while cursor < out.len() {
        let parent = out[cursor].id;
        let rows = sqlx::query(
            r#"
            SELECT id, title, lane_json, profile_json, runner_affinity, status, parent_id,
                   created_at, updated_at
            FROM sessions
            WHERE parent_id = ?
            ORDER BY created_at ASC
            "#,
        )
        .bind(parent.to_string())
        .fetch_all(pool)
        .await?;
        for row in rows {
            out.push(session_from_row(&row, "")?);
        }
        cursor += 1;
    }
    Ok(out)
}

async fn set_session_status(
    state: &AppState,
    session: &Session,
    status: SessionStatus,
) -> Result<Session, ApiError> {
    sqlx::query(
        r#"
        UPDATE sessions
        SET status = ?, updated_at = ?
        WHERE id = ?
        "#,
    )
    .bind(enum_string(&status)?)
    .bind(dt_string(Utc::now()))
    .bind(session.id.to_string())
    .execute(&state.pool)
    .await?;
    let updated = get_session_by_id(&state.pool, session.id).await?;
    state.emit(
        updated.parent_id,
        Event::SessionUpdated {
            session: updated.clone(),
        },
    );
    Ok(updated)
}

/// Archive a session and every sub-agent under it. Queued turns are
/// cancelled outright; a turn a runner is playing gets a cancel request and
/// finishes as cancelled when the runner sees it. Nothing is deleted: the
/// turns, the replies, and the workspace on the seat all stay, and the
/// session can be unarchived.
async fn archive_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<SessionId>,
) -> Result<Json<Session>, ApiError> {
    let tree = session_tree(&state.pool, id).await?;
    let now = Utc::now();
    let mut root = None;
    for session in &tree {
        let queued = sqlx::query(
            r#"
            SELECT id FROM turns
            WHERE session_id = ? AND status = ?
            "#,
        )
        .bind(session.id.to_string())
        .bind(enum_string(&TurnStatus::Queued)?)
        .fetch_all(&state.pool)
        .await?;
        for row in queued {
            let raw: String = row.try_get("id")?;
            sqlx::query(
                r#"
                UPDATE turns
                SET status = ?, cancel_requested_at = COALESCE(cancel_requested_at, ?), finished_at = ?
                WHERE id = ? AND status = ?
                "#,
            )
            .bind(enum_string(&TurnStatus::Cancelled)?)
            .bind(dt_string(now))
            .bind(dt_string(now))
            .bind(&raw)
            .bind(enum_string(&TurnStatus::Queued)?)
            .execute(&state.pool)
            .await?;
            if let Ok(turn_id) = Uuid::parse_str(&raw) {
                if let Ok(turn) = get_turn_by_id(&state.pool, turn_id).await {
                    state.emit(Some(session.id), Event::TurnCancelled { turn });
                }
            }
        }
        sqlx::query(
            r#"
            UPDATE turns
            SET cancel_requested_at = COALESCE(cancel_requested_at, ?)
            WHERE session_id = ? AND status IN (?, ?)
            "#,
        )
        .bind(dt_string(now))
        .bind(session.id.to_string())
        .bind(enum_string(&TurnStatus::Claimed)?)
        .bind(enum_string(&TurnStatus::Working)?)
        .execute(&state.pool)
        .await?;

        let updated = if session.status == SessionStatus::Archived {
            session.clone()
        } else {
            set_session_status(&state, session, SessionStatus::Archived).await?
        };
        if updated.id == id {
            root = Some(updated);
        }
    }
    state.notify.notify_waiters();
    root.map(Json).ok_or_else(|| ApiError::not_found("session"))
}

/// Bring one session back. Its sub-agents stay as they are.
async fn unarchive_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<SessionId>,
) -> Result<Json<Session>, ApiError> {
    let session = get_session_by_id(&state.pool, id).await?;
    if session.status == SessionStatus::Active {
        return Ok(Json(session));
    }
    let updated = set_session_status(&state, &session, SessionStatus::Active).await?;
    state.notify.notify_waiters();
    Ok(Json(updated))
}

/// Delete an archived session and every sub-agent under it: the rows and
/// their turns go now, and each runner removes the workspace on its next
/// sweep. Anything the session pushed is still on its branch at the remote.
async fn delete_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<SessionId>,
) -> Result<StatusCode, ApiError> {
    let tree = session_tree(&state.pool, id).await?;
    if tree[0].status != SessionStatus::Archived {
        return Err(ApiError::conflict("archive the session before deleting it"));
    }
    for session in &tree {
        let running: i64 = sqlx::query(
            r#"
            SELECT COUNT(*) AS count FROM turns
            WHERE session_id = ? AND status IN (?, ?)
            "#,
        )
        .bind(session.id.to_string())
        .bind(enum_string(&TurnStatus::Claimed)?)
        .bind(enum_string(&TurnStatus::Working)?)
        .fetch_one(&state.pool)
        .await?
        .try_get("count")?;
        if running > 0 {
            return Err(ApiError::conflict(format!(
                "a turn is still running in {}; wait for the cancel to land",
                session.title
            )));
        }
    }
    // Children first, so no row ever points at a parent that is gone.
    for session in tree.iter().rev() {
        sqlx::query("DELETE FROM turns WHERE session_id = ?")
            .bind(session.id.to_string())
            .execute(&state.pool)
            .await?;
        sqlx::query("DELETE FROM sessions WHERE id = ?")
            .bind(session.id.to_string())
            .execute(&state.pool)
            .await?;
        state.emit(
            session.parent_id,
            Event::SessionDeleted {
                session_id: session.id,
            },
        );
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn get_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<SessionId>,
) -> Result<Json<Session>, ApiError> {
    get_session_by_id(&state.pool, id).await.map(Json)
}

async fn post_turn(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<SessionId>,
    payload: Result<Json<PostTurnRequest>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let payload = parse_json(payload)?;
    if payload.input.trim().is_empty() {
        return Err(ApiError::unprocessable("input is required"));
    }

    let mut tx = state.pool.begin().await?;
    let session_row = sqlx::query(
        r#"
        SELECT id, title, lane_json, profile_json, runner_affinity, status, parent_id,
               created_at, updated_at
        FROM sessions
        WHERE id = ?
        "#,
    )
    .bind(session_id.to_string())
    .fetch_optional(&mut *tx)
    .await?;

    if session_row.is_none() {
        return Err(ApiError::not_found("session"));
    }

    // A turn already running does not block the next message. Turns still
    // execute one at a time (the claim query enforces that, because the
    // session workspace is a single directory), so a message sent mid-turn
    // queues and runs next: the way you steer an agent that is already
    // working. The cap stops a runaway client filling the queue.
    let waiting: i64 = sqlx::query(
        r#"
        SELECT COUNT(*) AS count
        FROM turns
        WHERE session_id = ?
          AND status = ?
        "#,
    )
    .bind(session_id.to_string())
    .bind(enum_string(&TurnStatus::Queued)?)
    .fetch_one(&mut *tx)
    .await?
    .try_get("count")?;

    if waiting >= MAX_QUEUED_TURNS_PER_SESSION {
        return Err(ApiError::conflict(format!(
            "this session already has {waiting} messages waiting; let it catch up first"
        )));
    }

    let prior_max: Option<i64> = sqlx::query(
        r#"
        SELECT MAX(seq) AS seq
        FROM turns
        WHERE session_id = ?
        "#,
    )
    .bind(session_id.to_string())
    .fetch_one(&mut *tx)
    .await?
    .try_get("seq")?;

    let now = Utc::now();
    let turn = Turn {
        id: Uuid::new_v4(),
        session_id,
        seq: prior_max.unwrap_or(0) + 1,
        input: payload.input,
        lane_override: payload.lane,
        status: TurnStatus::Queued,
        envelope: None,
        error: None,
        commit: None,
        resume_token: None,
        cancel_requested: false,
        created_at: now,
        started_at: None,
        finished_at: None,
    };

    sqlx::query(
        r#"
        INSERT INTO turns
            (id, session_id, seq, input, lane_override_json, status, envelope_json, error,
             commit_sha, resume_token, created_at, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(turn.id.to_string())
    .bind(turn.session_id.to_string())
    .bind(turn.seq)
    .bind(&turn.input)
    .bind(option_json_string(&turn.lane_override)?)
    .bind(enum_string(&turn.status)?)
    .bind(option_json_string(&turn.envelope)?)
    .bind(&turn.error)
    .bind(&turn.commit)
    .bind(&turn.resume_token)
    .bind(dt_string(turn.created_at))
    .bind(option_dt_string(turn.started_at))
    .bind(option_dt_string(turn.finished_at))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    state.emit(
        Some(turn.session_id),
        Event::TurnQueued { turn: turn.clone() },
    );
    state.notify.notify_waiters();

    Ok((StatusCode::ACCEPTED, Json(turn)))
}

async fn list_session_turns(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<SessionId>,
) -> Result<Json<Vec<Turn>>, ApiError> {
    ensure_session_exists(&state.pool, session_id).await?;

    let rows = sqlx::query(
        r#"
        SELECT id, session_id, seq, input, lane_override_json, status, envelope_json, error,
               commit_sha, resume_token, cancel_requested_at, created_at, started_at,
               finished_at
        FROM turns
        WHERE session_id = ?
        ORDER BY seq ASC
        "#,
    )
    .bind(session_id.to_string())
    .fetch_all(&state.pool)
    .await?;

    rows.into_iter()
        .map(|row| turn_from_row(&row, ""))
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn session_events(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<SessionId>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    ensure_session_exists(&state.pool, session_id).await?;
    Ok(sse_stream(state.events.subscribe(), Some(session_id)))
}

async fn all_events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    sse_stream(state.events.subscribe(), None)
}

fn sse_stream(
    receiver: broadcast::Receiver<BroadcastEvent>,
    session_id: Option<SessionId>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    // A hand-rolled Stream that builds a fresh `recv()` future on every poll
    // drops it when it returns Pending, and `Recv::drop` deregisters the
    // waker, so a later `send` wakes nobody and events only surface on the
    // next keep-alive tick. Holding one future across polls is the whole fix.
    let stream = stream::unfold((receiver, session_id), |(mut receiver, session_id)| async move {
        loop {
            match receiver.recv().await {
                Ok(message) => {
                    if session_id.is_none() || message.session_id == session_id {
                        let event = SseEvent::default()
                            .json_data(&message.event)
                            .expect("protocol events serialize");
                        return Some((Ok(event), (receiver, session_id)));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "an SSE subscriber lagged");
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(SSE_KEEP_ALIVE_SECS))
            .text("keep-alive"),
    )
}

async fn list_runners(
    State(state): State<AppState>,
) -> Result<Json<Vec<RunnerStatusInfo>>, ApiError> {
    let rows = sqlx::query(
        r#"
        SELECT runner, last_seen, harnesses, active_turns
        FROM runners
        ORDER BY last_seen DESC
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    let now = Utc::now();
    rows.into_iter()
        .map(|row| runner_info_from_row(&row, now))
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn claim_turn(
    State(state): State<AppState>,
    payload: Result<Json<ClaimRequest>, JsonRejection>,
) -> Result<Json<ClaimResponse>, ApiError> {
    let payload = parse_json(payload)?;
    if payload.runner.trim().is_empty() {
        return Err(ApiError::unprocessable("runner is required"));
    }

    if let Some(work) = try_claim(&state, &payload).await? {
        emit_claimed(&state, &payload.runner, &work);
        return Ok(Json(ClaimResponse::Work { work }));
    }

    let wait_seconds = payload.wait_seconds.min(CLAIM_WAIT_CAP_SECS);
    if wait_seconds == 0 {
        return Ok(Json(ClaimResponse::Empty));
    }

    let timeout = tokio::time::sleep(Duration::from_secs(wait_seconds.into()));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => return Ok(Json(ClaimResponse::Empty)),
            _ = state.notify.notified() => {
                if let Some(work) = try_claim(&state, &payload).await? {
                    emit_claimed(&state, &payload.runner, &work);
                    return Ok(Json(ClaimResponse::Work { work }));
                }
            }
        }
    }
}

fn emit_claimed(state: &AppState, runner: &RunnerId, work: &ClaimedWork) {
    state.emit(
        Some(work.turn.session_id),
        Event::TurnClaimed {
            turn_id: work.turn.id,
            runner: runner.clone(),
        },
    );
}

async fn try_claim(
    state: &AppState,
    request: &ClaimRequest,
) -> Result<Option<ClaimedWork>, ApiError> {
    // A runner that restarted arrives with a fresh epoch: whatever its
    // previous process was holding is dead work, released now rather than
    // after the online window (which never expires, because the new process
    // keeps the shared runner id alive).
    let restarted = match (&request.epoch, runner_epoch(&state.pool, &request.runner).await?) {
        (Some(epoch), Some(previous)) => epoch != &previous,
        _ => false,
    };
    if restarted {
        let released = release_turns_not_held(&state.pool, &request.runner, &[], Utc::now(), 0)
            .await?;
        for (turn_id, session_id, status) in released {
            tracing::warn!(turn_id = %turn_id, runner = %request.runner, ?status, "released a turn held by a previous runner process");
            emit_release(state, turn_id, session_id, status).await;
        }
    }

    let mut tx = state.pool.begin().await?;
    upsert_runner_seen(&mut tx, request).await?;
    let reclaimed = reclaim_stale_turns(&mut tx).await?;

    let queued = sqlx::query(
        r#"
        SELECT
            t.id AS t_id,
            t.session_id AS t_session_id,
            t.seq AS t_seq,
            t.input AS t_input,
            t.lane_override_json AS t_lane_override_json,
            t.status AS t_status,
            t.envelope_json AS t_envelope_json,
            t.error AS t_error,
            t.commit_sha AS t_commit_sha,
            t.resume_token AS t_resume_token,
            t.cancel_requested_at AS t_cancel_requested_at,
            t.created_at AS t_created_at,
            t.started_at AS t_started_at,
            t.finished_at AS t_finished_at,
            s.id AS s_id,
            s.title AS s_title,
            s.lane_json AS s_lane_json,
            s.profile_json AS s_profile_json,
            s.runner_affinity AS s_runner_affinity,
            s.status AS s_status,
            s.parent_id AS s_parent_id,
            s.created_at AS s_created_at,
            s.updated_at AS s_updated_at
        FROM turns t
        JOIN sessions s ON s.id = t.session_id
        WHERE t.status = ?
          AND s.status = ?
          -- One turn per session at a time: the session's git workspace is a
          -- single directory, so two concurrent turns would corrupt it. The
          -- API already refuses a second in-flight turn; this holds the line
          -- when a reclaim races the runner that is still playing the turn.
          AND NOT EXISTS (
            SELECT 1 FROM turns busy
            WHERE busy.session_id = t.session_id
              AND busy.status IN (?, ?)
          )
        ORDER BY t.created_at ASC, t.seq ASC
        "#,
    )
    .bind(enum_string(&TurnStatus::Queued)?)
    .bind(enum_string(&SessionStatus::Active)?)
    .bind(enum_string(&TurnStatus::Claimed)?)
    .bind(enum_string(&TurnStatus::Working)?)
    .fetch_all(&mut *tx)
    .await?;

    let now = Utc::now();
    let mut claimed_turn_id = None;

    for row in queued {
        let turn = turn_from_row(&row, "t_")?;
        let session = session_from_row(&row, "s_")?;
        if !harness_offered(&turn, &session, &request.harnesses) {
            continue;
        }

        let should_reassign = match &session.runner_affinity {
            Some(affinity) if affinity == &request.runner => true,
            None => true,
            Some(affinity) => runner_is_offline(&mut tx, affinity, now).await?,
        };

        if !should_reassign {
            continue;
        }

        let result = sqlx::query(
            r#"
            UPDATE turns
            SET status = ?, started_at = ?
            WHERE id = ?
              AND status = ?
            "#,
        )
        .bind(enum_string(&TurnStatus::Claimed)?)
        .bind(dt_string(now))
        .bind(turn.id.to_string())
        .bind(enum_string(&TurnStatus::Queued)?)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() != 1 {
            continue;
        }

        if session.runner_affinity.as_ref() != Some(&request.runner) {
            sqlx::query(
                r#"
                UPDATE sessions
                SET runner_affinity = ?, updated_at = ?
                WHERE id = ?
                "#,
            )
            .bind(&request.runner)
            .bind(dt_string(now))
            .bind(session.id.to_string())
            .execute(&mut *tx)
            .await?;
        }

        claimed_turn_id = Some(turn.id);
        break;
    }

    tx.commit().await?;

    for turn_id in reclaimed {
        state.notify.notify_waiters();
        tracing::warn!(turn_id = %turn_id, "requeued a turn whose runner went offline mid-flight");
    }

    let Some(turn_id) = claimed_turn_id else {
        return Ok(None);
    };

    let mut turn = get_turn_by_id(&state.pool, turn_id).await?;
    let session = get_session_by_id(&state.pool, turn.session_id).await?;
    let history_hint = history_hint(&state.pool, turn.session_id, turn.seq).await?;

    // A fresh turn row carries no resume token of its own: continuity comes
    // from the session's last harness session token, so the adapter can
    // --resume instead of reseeding from history.
    if turn.resume_token.is_none() {
        turn.resume_token = latest_resume_token(&state.pool, turn.session_id, turn.seq).await?;
    }

    Ok(Some(ClaimedWork {
        turn,
        session,
        history_hint,
    }))
}

/// A turn held by a runner that has stopped heartbeating is dead work: without
/// this the session wedges forever behind the one-in-flight rule.
async fn reclaim_stale_turns(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<Vec<TurnId>, ApiError> {
    let cutoff = Utc::now() - TimeDelta::seconds(ONLINE_WINDOW_SECS);

    let rows = sqlx::query(
        r#"
        SELECT t.id AS id, t.cancel_requested_at AS cancel_requested_at
        FROM turns t
        JOIN sessions s ON s.id = t.session_id
        WHERE t.status IN (?, ?)
          AND (
            s.runner_affinity IS NULL
            OR NOT EXISTS (
              SELECT 1 FROM runners r
              WHERE r.runner = s.runner_affinity
                AND r.last_seen >= ?
            )
          )
        "#,
    )
    .bind(enum_string(&TurnStatus::Claimed)?)
    .bind(enum_string(&TurnStatus::Working)?)
    .bind(dt_string(cutoff))
    .fetch_all(&mut **tx)
    .await?;

    let mut reclaimed = Vec::new();
    for row in rows {
        let raw: String = row.try_get("id")?;
        let Ok(turn_id) = Uuid::parse_str(&raw) else {
            continue;
        };
        let cancel_requested = row
            .try_get::<Option<String>, _>("cancel_requested_at")?
            .is_some();

        // A cancelled turn whose runner died is finished, not requeued: the
        // human already said stop.
        let next_status = if cancel_requested {
            TurnStatus::Cancelled
        } else {
            TurnStatus::Queued
        };

        let result = sqlx::query(
            r#"
            UPDATE turns
            SET status = ?, started_at = NULL
            WHERE id = ?
              AND status IN (?, ?)
            "#,
        )
        .bind(enum_string(&next_status)?)
        .bind(&raw)
        .bind(enum_string(&TurnStatus::Claimed)?)
        .bind(enum_string(&TurnStatus::Working)?)
        .execute(&mut **tx)
        .await?;

        if result.rows_affected() == 1 {
            reclaimed.push(turn_id);
        }
    }

    Ok(reclaimed)
}

/// The newest harness resume token this session produced before `before_seq`.
async fn latest_resume_token(
    pool: &SqlitePool,
    session_id: SessionId,
    before_seq: i64,
) -> Result<Option<String>, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT resume_token
        FROM turns
        WHERE session_id = ?
          AND seq < ?
          AND resume_token IS NOT NULL
        ORDER BY seq DESC
        LIMIT 1
        "#,
    )
    .bind(session_id.to_string())
    .bind(before_seq)
    .fetch_optional(pool)
    .await?;

    Ok(match row {
        Some(row) => row.try_get::<Option<String>, _>("resume_token")?,
        None => None,
    })
}

async fn upsert_runner_seen(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &ClaimRequest,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        INSERT INTO runners (runner, last_seen, harnesses, active_turns, epoch)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(runner) DO UPDATE SET
            last_seen = excluded.last_seen,
            harnesses = excluded.harnesses,
            epoch = COALESCE(excluded.epoch, runners.epoch)
        "#,
    )
    .bind(&request.runner)
    .bind(dt_string(Utc::now()))
    .bind(json_string(&request.harnesses)?)
    .bind(json_string(&Vec::<TurnId>::new())?)
    .bind(&request.epoch)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn runner_epoch(pool: &SqlitePool, runner: &str) -> Result<Option<String>, ApiError> {
    let row = sqlx::query("SELECT epoch FROM runners WHERE runner = ?")
        .bind(runner)
        .fetch_optional(pool)
        .await?;
    Ok(match row {
        Some(row) => row.try_get("epoch")?,
        None => None,
    })
}

async fn runner_is_offline(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    runner: &str,
    now: DateTime<Utc>,
) -> Result<bool, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT last_seen
        FROM runners
        WHERE runner = ?
        "#,
    )
    .bind(runner)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(row) = row else {
        return Ok(true);
    };

    let last_seen = parse_dt(row.try_get::<String, _>("last_seen")?)?;
    Ok(now.signed_duration_since(last_seen) > TimeDelta::seconds(ONLINE_WINDOW_SECS))
}

fn harness_offered(turn: &Turn, session: &Session, harnesses: &[Harness]) -> bool {
    let lane = turn.lane_override.as_ref().unwrap_or(&session.lane);
    harnesses.contains(&lane.harness)
}

async fn post_chunk(
    State(state): State<AppState>,
    AxumPath(turn_id): AxumPath<TurnId>,
    payload: Result<Json<ChunkRequest>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let payload = parse_json(payload)?;
    let turn = get_turn_by_id(&state.pool, turn_id).await?;

    match turn.status {
        TurnStatus::Claimed => {
            if !ceilidh_caller::transition_allowed(TurnStatus::Claimed, TurnStatus::Working) {
                return Err(ApiError::conflict("turn transition is not allowed"));
            }
            let result = sqlx::query(
                r#"
                UPDATE turns
                SET status = ?
                WHERE id = ?
                  AND status = ?
                "#,
            )
            .bind(enum_string(&TurnStatus::Working)?)
            .bind(turn_id.to_string())
            .bind(enum_string(&TurnStatus::Claimed)?)
            .execute(&state.pool)
            .await?;
            if result.rows_affected() != 1 {
                return Err(ApiError::conflict("turn transition is not allowed"));
            }
        }
        TurnStatus::Working => {}
        _ => return Err(ApiError::conflict("turn is not claimed or working")),
    }

    state.emit(
        Some(turn.session_id),
        Event::Chunk {
            turn_id,
            text: payload.text,
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

async fn report_turn(
    State(state): State<AppState>,
    AxumPath(turn_id): AxumPath<TurnId>,
    payload: Result<Json<ReportRequest>, JsonRejection>,
) -> Result<Json<Turn>, ApiError> {
    let payload = parse_json(payload)?;
    if !matches!(
        payload.status,
        TurnStatus::Done | TurnStatus::Error | TurnStatus::Capped | TurnStatus::Cancelled
    ) {
        return Err(ApiError::unprocessable(
            "report status must be done, error, capped, or cancelled",
        ));
    }

    let current = get_turn_by_id(&state.pool, turn_id).await?;
    if !ceilidh_caller::transition_allowed(current.status, payload.status) {
        return Err(ApiError::conflict("turn transition is not allowed"));
    }

    let finished_at = Utc::now();
    let result = sqlx::query(
        r#"
        UPDATE turns
        SET status = ?,
            envelope_json = ?,
            error = ?,
            commit_sha = ?,
            resume_token = ?,
            finished_at = ?
        WHERE id = ?
          AND status = ?
        "#,
    )
    .bind(enum_string(&payload.status)?)
    .bind(option_json_string(&payload.envelope)?)
    .bind(&payload.error)
    .bind(&payload.commit)
    .bind(&payload.resume_token)
    .bind(dt_string(finished_at))
    .bind(turn_id.to_string())
    .bind(enum_string(&current.status)?)
    .execute(&state.pool)
    .await?;
    if result.rows_affected() != 1 {
        return Err(ApiError::conflict("turn transition is not allowed"));
    }

    let turn = get_turn_by_id(&state.pool, turn_id).await?;
    match turn.status {
        TurnStatus::Done => state.emit(
            Some(turn.session_id),
            Event::TurnDone { turn: turn.clone() },
        ),
        TurnStatus::Error | TurnStatus::Capped => state.emit(
            Some(turn.session_id),
            Event::TurnError {
                turn_id,
                message: turn
                    .error
                    .clone()
                    .unwrap_or_else(|| "turn did not complete".to_string()),
            },
        ),
        TurnStatus::Cancelled => state.emit(
            Some(turn.session_id),
            Event::TurnCancelled { turn: turn.clone() },
        ),
        _ => {}
    }
    state.notify.notify_waiters();

    Ok(Json(turn))
}

/// Cancel a turn. A queued turn is cancelled here and now; a claimed or
/// working turn is flagged, and the runner holding it kills the harness and
/// reports `cancelled`.
async fn cancel_turn(
    State(state): State<AppState>,
    AxumPath((session_id, turn_id)): AxumPath<(SessionId, TurnId)>,
) -> Result<Json<Turn>, ApiError> {
    let turn = get_turn_by_id(&state.pool, turn_id).await?;
    if turn.session_id != session_id {
        return Err(ApiError::not_found("turn"));
    }

    let now = Utc::now();
    match turn.status {
        TurnStatus::Queued => {
            let result = sqlx::query(
                r#"
                UPDATE turns
                SET status = ?, cancel_requested_at = ?, finished_at = ?
                WHERE id = ?
                  AND status = ?
                "#,
            )
            .bind(enum_string(&TurnStatus::Cancelled)?)
            .bind(dt_string(now))
            .bind(dt_string(now))
            .bind(turn_id.to_string())
            .bind(enum_string(&TurnStatus::Queued)?)
            .execute(&state.pool)
            .await?;
            if result.rows_affected() != 1 {
                return Err(ApiError::conflict("turn is no longer queued"));
            }
            let turn = get_turn_by_id(&state.pool, turn_id).await?;
            state.emit(
                Some(session_id),
                Event::TurnCancelled { turn: turn.clone() },
            );
            state.notify.notify_waiters();
            Ok(Json(turn))
        }
        TurnStatus::Claimed | TurnStatus::Working => {
            sqlx::query(
                r#"
                UPDATE turns
                SET cancel_requested_at = COALESCE(cancel_requested_at, ?)
                WHERE id = ?
                "#,
            )
            .bind(dt_string(now))
            .bind(turn_id.to_string())
            .execute(&state.pool)
            .await?;
            Ok(Json(get_turn_by_id(&state.pool, turn_id).await?))
        }
        _ => Err(ApiError::conflict("turn has already finished")),
    }
}

/// Polled by the runner holding a turn: has the human asked for a cancel?
async fn turn_control(
    State(state): State<AppState>,
    AxumPath(turn_id): AxumPath<TurnId>,
) -> Result<Json<TurnControl>, ApiError> {
    let turn = get_turn_by_id(&state.pool, turn_id).await?;
    Ok(Json(TurnControl {
        cancel_requested: turn.cancel_requested,
    }))
}

async fn heartbeat(
    State(state): State<AppState>,
    payload: Result<Json<Heartbeat>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let payload = parse_json(payload)?;
    if payload.runner.trim().is_empty() {
        return Err(ApiError::unprocessable("runner is required"));
    }

    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO runners (runner, last_seen, harnesses, active_turns)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(runner) DO UPDATE SET
            last_seen = excluded.last_seen,
            active_turns = excluded.active_turns
        "#,
    )
    .bind(&payload.runner)
    .bind(dt_string(now))
    .bind(json_string(&Vec::<Harness>::new())?)
    .bind(json_string(&payload.active_turns)?)
    .execute(&state.pool)
    .await?;

    // Only the process the caller believes owns this runner id may reconcile.
    // Two processes sharing an id (an orphan left by a redeploy) would
    // otherwise release each other's turns on every beat.
    let current_epoch = runner_epoch(&state.pool, &payload.runner).await?;
    let authoritative = match (&payload.epoch, &current_epoch) {
        (Some(epoch), Some(current)) => epoch == current,
        (None, None) => true,
        // A heartbeat with no epoch from a runner that has claimed with one is
        // an older binary or a zombie: record it, reconcile nothing.
        _ => false,
    };
    if authoritative {
        let released = release_turns_not_held(
            &state.pool,
            &payload.runner,
            &payload.active_turns,
            now,
            HELD_TURN_GRACE_SECS,
        )
        .await?;
        for (turn_id, session_id, status) in released {
            tracing::warn!(turn_id = %turn_id, runner = %payload.runner, ?status, "released a turn its runner no longer holds");
            emit_release(&state, turn_id, session_id, status).await;
        }
        state.notify.notify_waiters();
    }

    state.emit(
        None,
        Event::RunnerStatus {
            runner: RunnerStatusInfo {
                runner: payload.runner,
                online: true,
                last_seen: now,
                active_turns: payload.active_turns.len() as u32,
            },
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

/// A runner that restarted (launchd KeepAlive, a crash, a redeploy) comes back
/// under the same id within the online window, so the stale sweep never sees
/// it as dead; its heartbeat says which turns it actually holds. Anything this
/// runner was assigned but does not list goes back on the queue (or finishes
/// as cancelled if the human already asked), after a short grace for turns
/// claimed a moment ago.
async fn emit_release(
    state: &AppState,
    turn_id: TurnId,
    session_id: SessionId,
    status: TurnStatus,
) {
    if status == TurnStatus::Cancelled {
        if let Ok(turn) = get_turn_by_id(&state.pool, turn_id).await {
            state.emit(Some(session_id), Event::TurnCancelled { turn });
        }
    }
}

async fn release_turns_not_held(
    pool: &SqlitePool,
    runner: &str,
    held: &[TurnId],
    now: DateTime<Utc>,
    grace_seconds: i64,
) -> Result<Vec<(TurnId, SessionId, TurnStatus)>, ApiError> {
    let cutoff = now - TimeDelta::seconds(grace_seconds);
    let rows = sqlx::query(
        r#"
        SELECT t.id AS id, t.session_id AS session_id, t.cancel_requested_at AS cancel_requested_at
        FROM turns t
        JOIN sessions s ON s.id = t.session_id
        WHERE t.status IN (?, ?)
          AND s.runner_affinity = ?
          AND t.started_at IS NOT NULL
          AND t.started_at < ?
        "#,
    )
    .bind(enum_string(&TurnStatus::Claimed)?)
    .bind(enum_string(&TurnStatus::Working)?)
    .bind(runner)
    .bind(dt_string(cutoff))
    .fetch_all(pool)
    .await?;

    let mut released = Vec::new();
    for row in rows {
        let raw: String = row.try_get("id")?;
        let Ok(turn_id) = Uuid::parse_str(&raw) else {
            continue;
        };
        if held.contains(&turn_id) {
            continue;
        }
        let session_id = parse_uuid(row.try_get::<String, _>("session_id")?)?;
        let cancel_requested = row
            .try_get::<Option<String>, _>("cancel_requested_at")?
            .is_some();
        let next_status = if cancel_requested {
            TurnStatus::Cancelled
        } else {
            TurnStatus::Queued
        };
        let result = sqlx::query(
            r#"
            UPDATE turns
            SET status = ?, started_at = NULL,
                finished_at = CASE WHEN ? = 'cancelled' THEN ? ELSE finished_at END
            WHERE id = ?
              AND status IN (?, ?)
            "#,
        )
        .bind(enum_string(&next_status)?)
        .bind(enum_string(&next_status)?)
        .bind(dt_string(now))
        .bind(&raw)
        .bind(enum_string(&TurnStatus::Claimed)?)
        .bind(enum_string(&TurnStatus::Working)?)
        .execute(pool)
        .await?;
        if result.rows_affected() == 1 {
            released.push((turn_id, session_id, next_status));
        }
    }
    Ok(released)
}

async fn get_session_by_id(pool: &SqlitePool, id: SessionId) -> Result<Session, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT id, title, lane_json, profile_json, runner_affinity, status, parent_id,
               created_at, updated_at
        FROM sessions
        WHERE id = ?
        "#,
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("session"))?;

    session_from_row(&row, "")
}

async fn ensure_session_exists(pool: &SqlitePool, id: SessionId) -> Result<(), ApiError> {
    let exists: Option<String> = sqlx::query(
        r#"
        SELECT id
        FROM sessions
        WHERE id = ?
        "#,
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .map(|row| row.try_get("id"))
    .transpose()?;

    match exists {
        Some(_) => Ok(()),
        None => Err(ApiError::not_found("session")),
    }
}

async fn get_turn_by_id(pool: &SqlitePool, id: TurnId) -> Result<Turn, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT id, session_id, seq, input, lane_override_json, status, envelope_json, error,
               commit_sha, resume_token, cancel_requested_at, created_at, started_at,
               finished_at
        FROM turns
        WHERE id = ?
        "#,
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::not_found("turn"))?;

    turn_from_row(&row, "")
}

async fn history_hint(
    pool: &SqlitePool,
    session_id: SessionId,
    before_seq: i64,
) -> Result<Vec<TurnSummary>, ApiError> {
    let rows = sqlx::query(
        r#"
        SELECT input, envelope_json
        FROM turns
        WHERE session_id = ?
          AND seq < ?
          AND status IN (?, ?, ?, ?)
        ORDER BY seq DESC
        LIMIT ?
        "#,
    )
    .bind(session_id.to_string())
    .bind(before_seq)
    .bind(enum_string(&TurnStatus::Done)?)
    .bind(enum_string(&TurnStatus::Error)?)
    .bind(enum_string(&TurnStatus::Capped)?)
    .bind(enum_string(&TurnStatus::Cancelled)?)
    .bind(HISTORY_HINT_LIMIT)
    .fetch_all(pool)
    .await?;

    let mut summaries = rows
        .into_iter()
        .map(|row| {
            let envelope: Option<Envelope> =
                option_from_json_string(row.try_get("envelope_json")?)?;
            Ok(TurnSummary {
                input: row.try_get("input")?,
                headline: envelope.map(|envelope| envelope.headline),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    summaries.reverse();
    Ok(summaries)
}

fn parse_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    match payload {
        Ok(Json(payload)) => Ok(payload),
        Err(err) => {
            let status = match err {
                JsonRejection::JsonDataError(_) => StatusCode::UNPROCESSABLE_ENTITY,
                _ => StatusCode::BAD_REQUEST,
            };
            Err(ApiError::new(
                status,
                format!(
                    "invalid json: {}; supported harnesses: claude-code, codex, cursor, mock",
                    err.body_text()
                ),
            ))
        }
    }
}

fn session_from_row(row: &SqliteRow, prefix: &str) -> Result<Session, ApiError> {
    Ok(Session {
        id: parse_uuid(row.try_get::<String, _>(column(prefix, "id").as_str())?)?,
        title: row.try_get(column(prefix, "title").as_str())?,
        lane: from_json_string(row.try_get(column(prefix, "lane_json").as_str())?)?,
        profile: from_json_string(row.try_get(column(prefix, "profile_json").as_str())?)?,
        runner_affinity: row.try_get(column(prefix, "runner_affinity").as_str())?,
        status: enum_from_string(row.try_get(column(prefix, "status").as_str())?)?,
        parent_id: row
            .try_get::<Option<String>, _>(column(prefix, "parent_id").as_str())?
            .map(parse_uuid)
            .transpose()?,
        created_at: parse_dt(row.try_get(column(prefix, "created_at").as_str())?)?,
        updated_at: parse_dt(row.try_get(column(prefix, "updated_at").as_str())?)?,
    })
}

fn turn_from_row(row: &SqliteRow, prefix: &str) -> Result<Turn, ApiError> {
    Ok(Turn {
        id: parse_uuid(row.try_get::<String, _>(column(prefix, "id").as_str())?)?,
        session_id: parse_uuid(row.try_get::<String, _>(column(prefix, "session_id").as_str())?)?,
        seq: row.try_get(column(prefix, "seq").as_str())?,
        input: row.try_get(column(prefix, "input").as_str())?,
        lane_override: option_from_json_string(
            row.try_get(column(prefix, "lane_override_json").as_str())?,
        )?,
        status: enum_from_string(row.try_get(column(prefix, "status").as_str())?)?,
        envelope: option_from_json_string(
            row.try_get(column(prefix, "envelope_json").as_str())?,
        )?,
        error: row.try_get(column(prefix, "error").as_str())?,
        commit: row.try_get(column(prefix, "commit_sha").as_str())?,
        resume_token: row.try_get(column(prefix, "resume_token").as_str())?,
        cancel_requested: row
            .try_get::<Option<String>, _>(column(prefix, "cancel_requested_at").as_str())?
            .is_some(),
        created_at: parse_dt(row.try_get(column(prefix, "created_at").as_str())?)?,
        started_at: option_parse_dt(row.try_get(column(prefix, "started_at").as_str())?)?,
        finished_at: option_parse_dt(row.try_get(column(prefix, "finished_at").as_str())?)?,
    })
}

fn runner_info_from_row(
    row: &SqliteRow,
    now: DateTime<Utc>,
) -> Result<RunnerStatusInfo, ApiError> {
    let last_seen = parse_dt(row.try_get::<String, _>("last_seen")?)?;
    let active_turns: Vec<TurnId> = from_json_string(row.try_get("active_turns")?)?;
    Ok(RunnerStatusInfo {
        runner: row.try_get("runner")?,
        online: now.signed_duration_since(last_seen) <= TimeDelta::seconds(ONLINE_WINDOW_SECS),
        last_seen,
        active_turns: active_turns.len() as u32,
    })
}

fn column(prefix: &str, name: &str) -> String {
    format!("{prefix}{name}")
}

fn json_string<T: Serialize>(value: &T) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(ApiError::from)
}

fn option_json_string<T: Serialize>(value: &Option<T>) -> Result<Option<String>, ApiError> {
    value.as_ref().map(json_string).transpose()
}

fn from_json_string<T: DeserializeOwned>(value: String) -> Result<T, ApiError> {
    serde_json::from_str(&value).map_err(ApiError::from)
}

fn option_from_json_string<T: DeserializeOwned>(
    value: Option<String>,
) -> Result<Option<T>, ApiError> {
    value.map(from_json_string).transpose()
}

fn enum_string<T: Serialize>(value: &T) -> Result<String, ApiError> {
    match serde_json::to_value(value)? {
        Value::String(value) => Ok(value),
        _ => Err(ApiError::internal("enum did not serialize as a string")),
    }
}

fn enum_from_string<T: DeserializeOwned>(value: String) -> Result<T, ApiError> {
    serde_json::from_value(Value::String(value)).map_err(ApiError::from)
}

fn dt_string(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn option_dt_string(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(dt_string)
}

fn parse_dt(value: String) -> Result<DateTime<Utc>, ApiError> {
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}

fn option_parse_dt(value: Option<String>) -> Result<Option<DateTime<Utc>>, ApiError> {
    value.map(parse_dt).transpose()
}

fn parse_uuid(value: String) -> Result<Uuid, ApiError> {
    Uuid::parse_str(&value).map_err(ApiError::internal)
}

#[derive(Debug, Deserialize)]
struct ChunkRequest {
    text: String,
}


/// Helpers for integration tests that need a genuine login cookie.
pub mod test_support {
    pub use crate::auth::CookieSigner;

    pub fn signer(secret: &str) -> CookieSigner {
        CookieSigner::new(secret)
    }

    /// The bare cookie value (no attributes) a signed-in browser would send.
    pub fn session_cookie_value(signer: &CookieSigner, email: &str) -> String {
        signer.sign("session", email, 3600)
    }
}

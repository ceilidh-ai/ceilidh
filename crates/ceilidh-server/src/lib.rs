//! ceilidh-server: the caller's HTTP surface.
//!
//! Owned by lane/caller-server. The `serve` entry point is the public contract
//! used by the CLI.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::header;
use axum::http::{StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive};
use axum::response::{Html, IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use ceilidh_protocol::{
    ClaimRequest, ClaimResponse, ClaimedWork, CreateSessionRequest, Envelope, Event, Harness,
    Heartbeat, PostTurnRequest, ReportRequest, RunnerId, RunnerStatusInfo, Session, SessionId,
    SessionStatus, Turn, TurnId, TurnStatus, TurnSummary,
};
use chrono::{DateTime, TimeDelta, Utc};
use futures_core::Stream;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
use tokio::net::TcpListener;
use tokio::sync::{Notify, broadcast};
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

const ONLINE_WINDOW_SECS: i64 = 60;
const CLAIM_WAIT_CAP_SECS: u32 = 30;
const SSE_KEEP_ALIVE_SECS: u64 = 15;
const HISTORY_HINT_LIMIT: i64 = 8;

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub bind: SocketAddr,
    /// SQLite database file; created if absent.
    pub db_path: PathBuf,
    /// Bearer token required on every API request when set.
    pub token: Option<String>,
}

pub async fn serve(opts: ServeOptions) -> anyhow::Result<()> {
    let bind = opts.bind;
    let app = build_app(opts).await?;
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
    let state = AppState {
        pool,
        events,
        notify: Arc::new(Notify::new()),
        token: opts.token.filter(|token| !token.is_empty()).map(Arc::from),
    };

    let api = Router::new()
        .route("/sessions", post(create_session).get(list_sessions))
        .route("/sessions/{id}", get(get_session))
        .route(
            "/sessions/{id}/turns",
            post(post_turn).get(list_session_turns),
        )
        .route("/sessions/{id}/events", get(session_events))
        .route("/events", get(all_events))
        .route("/runners", get(list_runners))
        .route("/runner/claim", post(claim_turn))
        .route("/runner/turns/{id}/chunk", post(post_chunk))
        .route("/runner/turns/{id}/report", post(report_turn))
        .route("/runner/heartbeat", post(heartbeat))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_auth,
        ))
        .with_state(state.clone());

    let app = Router::new()
        .route("/health", get(health))
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

    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .foreign_keys(true);

    Ok(SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?)
}

async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    match state.token.as_deref() {
        Some(token) if !request_has_token(&req, token) => Err(ApiError::unauthorized()),
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

    let now = Utc::now();
    let session = Session {
        id: Uuid::new_v4(),
        title: payload.title,
        lane: payload.lane.unwrap_or_default(),
        profile: payload.profile.unwrap_or_default(),
        runner_affinity: None,
        status: SessionStatus::Active,
        created_at: now,
        updated_at: now,
    };

    sqlx::query(
        r#"
        INSERT INTO sessions
            (id, title, lane_json, profile_json, runner_affinity, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(session.id.to_string())
    .bind(&session.title)
    .bind(json_string(&session.lane)?)
    .bind(json_string(&session.profile)?)
    .bind(&session.runner_affinity)
    .bind(enum_string(&session.status)?)
    .bind(dt_string(session.created_at))
    .bind(dt_string(session.updated_at))
    .execute(&state.pool)
    .await?;

    Ok((StatusCode::CREATED, Json(session)))
}

async fn list_sessions(State(state): State<AppState>) -> Result<Json<Vec<Session>>, ApiError> {
    let rows = sqlx::query(
        r#"
        SELECT id, title, lane_json, profile_json, runner_affinity, status, created_at, updated_at
        FROM sessions
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    rows.into_iter()
        .map(|row| session_from_row(&row, ""))
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
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
        SELECT id, title, lane_json, profile_json, runner_affinity, status, created_at, updated_at
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

    let in_flight: i64 = sqlx::query(
        r#"
        SELECT COUNT(*) AS count
        FROM turns
        WHERE session_id = ?
          AND status IN (?, ?, ?)
        "#,
    )
    .bind(session_id.to_string())
    .bind(enum_string(&TurnStatus::Queued)?)
    .bind(enum_string(&TurnStatus::Claimed)?)
    .bind(enum_string(&TurnStatus::Working)?)
    .fetch_one(&mut *tx)
    .await?
    .try_get("count")?;

    if in_flight > 0 {
        return Err(ApiError::conflict(
            "session already has an in-flight turn",
        ));
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
               commit_sha, resume_token, created_at, started_at, finished_at
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
    Sse::new(EventStream {
        receiver,
        session_id,
    })
    .keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(SSE_KEEP_ALIVE_SECS))
            .text("keep-alive"),
    )
}

struct EventStream {
    receiver: broadcast::Receiver<BroadcastEvent>,
    session_id: Option<SessionId>,
}

impl Stream for EventStream {
    type Item = Result<SseEvent, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let session_id = self.session_id;
            let recv = self.receiver.recv();
            tokio::pin!(recv);

            match recv.poll(cx) {
                Poll::Ready(Ok(message)) => {
                    if session_id.is_none() || message.session_id == session_id {
                        let event = SseEvent::default()
                            .json_data(&message.event)
                            .expect("protocol events serialize");
                        return Poll::Ready(Some(Ok(event)));
                    }
                }
                Poll::Ready(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Poll::Ready(Err(broadcast::error::RecvError::Closed)) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
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
    let mut tx = state.pool.begin().await?;
    upsert_runner_seen(&mut tx, request).await?;

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
            t.created_at AS t_created_at,
            t.started_at AS t_started_at,
            t.finished_at AS t_finished_at,
            s.id AS s_id,
            s.title AS s_title,
            s.lane_json AS s_lane_json,
            s.profile_json AS s_profile_json,
            s.runner_affinity AS s_runner_affinity,
            s.status AS s_status,
            s.created_at AS s_created_at,
            s.updated_at AS s_updated_at
        FROM turns t
        JOIN sessions s ON s.id = t.session_id
        WHERE t.status = ?
          AND s.status = ?
        ORDER BY t.created_at ASC, t.seq ASC
        "#,
    )
    .bind(enum_string(&TurnStatus::Queued)?)
    .bind(enum_string(&SessionStatus::Active)?)
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

    let Some(turn_id) = claimed_turn_id else {
        return Ok(None);
    };

    let turn = get_turn_by_id(&state.pool, turn_id).await?;
    let session = get_session_by_id(&state.pool, turn.session_id).await?;
    let history_hint = history_hint(&state.pool, turn.session_id, turn.seq).await?;

    Ok(Some(ClaimedWork {
        turn,
        session,
        history_hint,
    }))
}

async fn upsert_runner_seen(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request: &ClaimRequest,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        INSERT INTO runners (runner, last_seen, harnesses, active_turns)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(runner) DO UPDATE SET
            last_seen = excluded.last_seen,
            harnesses = excluded.harnesses
        "#,
    )
    .bind(&request.runner)
    .bind(dt_string(Utc::now()))
    .bind(json_string(&request.harnesses)?)
    .bind(json_string(&Vec::<TurnId>::new())?)
    .execute(&mut **tx)
    .await?;

    Ok(())
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
        TurnStatus::Done | TurnStatus::Error | TurnStatus::Capped
    ) {
        return Err(ApiError::unprocessable(
            "report status must be done, error, or capped",
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
        _ => {}
    }
    state.notify.notify_waiters();

    Ok(Json(turn))
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

async fn get_session_by_id(pool: &SqlitePool, id: SessionId) -> Result<Session, ApiError> {
    let row = sqlx::query(
        r#"
        SELECT id, title, lane_json, profile_json, runner_affinity, status, created_at, updated_at
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
               commit_sha, resume_token, created_at, started_at, finished_at
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
          AND status IN (?, ?, ?)
        ORDER BY seq DESC
        LIMIT ?
        "#,
    )
    .bind(session_id.to_string())
    .bind(before_seq)
    .bind(enum_string(&TurnStatus::Done)?)
    .bind(enum_string(&TurnStatus::Error)?)
    .bind(enum_string(&TurnStatus::Capped)?)
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
                    "invalid json: {}; supported harnesses: claude-code, codex, mock",
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

use anyhow::{Context, Result, bail};
use axum::body::{Body, Bytes, to_bytes};
use axum::http::header;
use axum::http::{Method, Request, Response, StatusCode};
use axum::Router;
use ceilidh_protocol::{
    ClaimRequest, ClaimResponse, CreateSessionRequest, Envelope, Event, Harness, Lane,
    PostTurnRequest, ReportRequest, RunnerStatusInfo, Session, Turn, TurnStatus,
};
use ceilidh_server::{ServeOptions, build_app, build_app_with_web_dir};
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn runner_flow_emits_sse_and_persists_final_state() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-server-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let db_path = dir.join("ceilidh.db");

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path,
        token: Some("secret".to_string()),
        default_repo_url: None,
    })
    .await?;

    let health: serde_json::Value = decode_json(
        app.clone()
            .oneshot(request(Method::GET, "/health", None, Body::empty())?)
            .await
            .unwrap(),
    )
    .await?;
    assert_eq!(health, serde_json::json!({ "ok": true }));

    let unauthorized = app
        .clone()
        .oneshot(request(
            Method::GET,
            "/api/sessions",
            None,
            Body::empty(),
        )?)
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        Some("secret"),
        &CreateSessionRequest {
            title: "demo".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;
    assert_eq!(session.runner_affinity, None);

    let sse_response = app
        .clone()
        .oneshot(request(
            Method::GET,
            &format!("/api/sessions/{}/events?token=secret", session.id),
            None,
            Body::empty(),
        )?)
        .await
        .unwrap();
    assert_eq!(sse_response.status(), StatusCode::OK);
    let mut events = sse_response.into_body().into_data_stream();
    let mut event_buf = String::new();

    let turn: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        Some("secret"),
        &PostTurnRequest {
            input: "hello".to_string(),
            lane: None,
        },
    )
    .await?;
    assert_eq!(turn.status, TurnStatus::Queued);
    assert_eq!(turn.seq, 1);

    let queued = next_sse_event(&mut events, &mut event_buf).await?;
    assert!(matches!(queued, Event::TurnQueued { turn: queued_turn } if queued_turn.id == turn.id));

    let claim: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        Some("secret"),
        &ClaimRequest {
            runner: "runner-1".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 1,
            epoch: None,
        },
    )
    .await?;

    let ClaimResponse::Work { work } = claim else {
        bail!("expected claimed work");
    };
    assert_eq!(work.turn.id, turn.id);
    assert_eq!(work.turn.status, TurnStatus::Claimed);
    assert_eq!(work.session.runner_affinity.as_deref(), Some("runner-1"));

    let claimed = next_sse_event(&mut events, &mut event_buf).await?;
    assert!(
        matches!(claimed, Event::TurnClaimed { turn_id, runner } if turn_id == turn.id && runner == "runner-1")
    );

    let chunk = app
        .clone()
        .oneshot(json_request(
            Method::POST,
            &format!("/api/runner/turns/{}/chunk", turn.id),
            Some("secret"),
            &serde_json::json!({ "text": "chunk one" }),
        )?)
        .await
        .unwrap();
    assert_eq!(chunk.status(), StatusCode::NO_CONTENT);

    let chunk_event = next_sse_event(&mut events, &mut event_buf).await?;
    assert!(
        matches!(chunk_event, Event::Chunk { turn_id, text } if turn_id == turn.id && text == "chunk one")
    );

    let final_turn: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/runner/turns/{}/report", turn.id),
        Some("secret"),
        &ReportRequest {
            status: TurnStatus::Done,
            envelope: Some(Envelope {
                headline: "done".to_string(),
                work_complete: true,
                cannot_proceed: false,
                body_markdown: "finished".to_string(),
                questions: Vec::new(),
            }),
            error: None,
            commit: Some("abc123".to_string()),
            resume_token: Some("resume-1".to_string()),
        },
    )
    .await?;
    assert_eq!(final_turn.status, TurnStatus::Done);
    assert_eq!(final_turn.commit.as_deref(), Some("abc123"));
    assert_eq!(final_turn.resume_token.as_deref(), Some("resume-1"));

    let done = next_sse_event(&mut events, &mut event_buf).await?;
    assert!(
        matches!(done, Event::TurnDone { turn: done_turn } if done_turn.id == turn.id && done_turn.status == TurnStatus::Done)
    );

    let turns: Vec<Turn> = decode_json(
        app.clone()
            .oneshot(request(
                Method::GET,
                &format!("/api/sessions/{}/turns", session.id),
                Some("secret"),
                Body::empty(),
            )?)
            .await
            .unwrap(),
    )
    .await?;
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].status, TurnStatus::Done);

    let runners: Vec<RunnerStatusInfo> = decode_json(
        app.clone()
            .oneshot(request(
                Method::GET,
                "/api/runners",
                Some("secret"),
                Body::empty(),
            )?)
            .await
            .unwrap(),
    )
    .await?;
    assert_eq!(runners[0].runner, "runner-1");
    assert!(runners[0].online);

    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn web_serving_uses_placeholder_or_index_fallback() -> Result<()> {
    let placeholder_dir =
        std::env::temp_dir().join(format!("ceilidh-placeholder-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&placeholder_dir)?;

    let placeholder_app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: placeholder_dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let placeholder = decode_text(
        placeholder_app
            .oneshot(request(Method::GET, "/", None, Body::empty())?)
            .await
            .unwrap(),
    )
    .await?;
    assert!(placeholder.contains("ceilidh caller is running"));
    std::fs::remove_dir_all(placeholder_dir)?;

    let web_dir = std::env::temp_dir().join(format!("ceilidh-web-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&web_dir)?;
    std::fs::write(web_dir.join("index.html"), "<!doctype html><h1>client</h1>")?;

    let app = build_app_with_web_dir(
        ServeOptions {
            bind: "127.0.0.1:0".parse()?,
            db_path: web_dir.join("ceilidh.db"),
            token: None,
            default_repo_url: None,
        },
        Some(web_dir.clone()),
    )
    .await?;

    let root = decode_text(
        app.clone()
            .oneshot(request(Method::GET, "/", None, Body::empty())?)
            .await
            .unwrap(),
    )
    .await?;
    assert!(root.contains("client"));

    let fallback = decode_text(
        app.oneshot(request(
            Method::GET,
            "/sessions/local-route",
            None,
            Body::empty(),
        )?)
        .await
        .unwrap(),
    )
    .await?;
    assert!(fallback.contains("client"));

    std::fs::remove_dir_all(web_dir)?;
    Ok(())
}

async fn send_json<T, U>(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    payload: &T,
) -> Result<U>
where
    T: Serialize,
    U: serde::de::DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(json_request(method, uri, token, payload)?)
        .await
        .unwrap();
    decode_json(response).await
}

async fn send_status<T>(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    payload: &T,
) -> Result<StatusCode>
where
    T: Serialize,
{
    let response = app
        .clone()
        .oneshot(json_request(method, uri, token, payload)?)
        .await
        .unwrap();
    Ok(response.status())
}

fn json_request<T>(
    method: Method,
    uri: &str,
    token: Option<&str>,
    payload: &T,
) -> Result<Request<Body>>
where
    T: Serialize,
{
    request(
        method,
        uri,
        token,
        Body::from(serde_json::to_vec(payload)?),
    )
    .map(|mut request| {
        request.headers_mut().insert(
            header::CONTENT_TYPE,
            "application/json".parse().expect("valid content type"),
        );
        request
    })
}

fn request(method: Method, uri: &str, token: Option<&str>, body: Body) -> Result<Request<Body>> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    Ok(builder.body(body)?)
}

async fn decode_json<T>(response: Response<Body>) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    if !status.is_success() {
        bail!(
            "expected success response, got {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(serde_json::from_slice(&bytes)?)
}

async fn decode_text(response: Response<Body>) -> Result<String> {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    if !status.is_success() {
        bail!(
            "expected success response, got {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(String::from_utf8(bytes.to_vec())?)
}

async fn next_sse_event<S>(stream: &mut S, buf: &mut String) -> Result<Event>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    loop {
        while let Some(index) = buf.find("\n\n") {
            let raw = buf[..index].to_string();
            buf.drain(..index + 2);

            let data = raw
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim_start)
                .collect::<Vec<_>>()
                .join("\n");

            if !data.is_empty() {
                return Ok(serde_json::from_str(&data)?);
            }
        }

        let chunk = stream
            .next()
            .await
            .context("sse stream ended before the expected event")??;
        buf.push_str(std::str::from_utf8(&chunk)?);
    }
}

/// A second turn must carry the harness session token the first turn produced,
/// or every turn starts a cold conversation and continuity is fiction.
#[tokio::test]
async fn claim_carries_forward_the_previous_resume_token() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-resume-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        None,
        &CreateSessionRequest {
            title: "resume".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;

    let turns_uri = format!("/api/sessions/{}/turns", session.id);
    let claim = ClaimRequest {
        runner: "runner-a".to_string(),
        harnesses: vec![Harness::Mock],
        wait_seconds: 0,
        epoch: None,
    };

    let first: Turn = send_json(
        &app,
        Method::POST,
        &turns_uri,
        None,
        &PostTurnRequest {
            input: "one".to_string(),
            lane: None,
        },
    )
    .await?;

    let claimed: ClaimResponse =
        send_json(&app, Method::POST, "/api/runner/claim", None, &claim).await?;
    assert!(matches!(claimed, ClaimResponse::Work { .. }));

    let _: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/runner/turns/{}/report", first.id),
        None,
        &ReportRequest {
            status: TurnStatus::Done,
            envelope: Some(Envelope {
                headline: "done".to_string(),
                work_complete: true,
                cannot_proceed: false,
                body_markdown: "done".to_string(),
                questions: Vec::new(),
            }),
            error: None,
            commit: Some("abc123".to_string()),
            resume_token: Some("harness-session-1".to_string()),
        },
    )
    .await?;

    let _: Turn = send_json(
        &app,
        Method::POST,
        &turns_uri,
        None,
        &PostTurnRequest {
            input: "two".to_string(),
            lane: None,
        },
    )
    .await?;

    let second: ClaimResponse =
        send_json(&app, Method::POST, "/api/runner/claim", None, &claim).await?;

    match second {
        ClaimResponse::Work { work } => {
            assert_eq!(
                work.turn.resume_token.as_deref(),
                Some("harness-session-1"),
                "second turn should resume the first turn's harness session"
            );
        }
        ClaimResponse::Empty => bail!("expected the second turn to be claimable"),
    }

    Ok(())
}

/// A runner that dies mid-turn must not wedge the session behind the
/// one-in-flight rule: the abandoned turn goes back on the queue.
#[tokio::test]
async fn stale_in_flight_turns_are_requeued_for_another_runner() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-stale-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        None,
        &CreateSessionRequest {
            title: "stale".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;

    let turn: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "hello".to_string(),
            lane: None,
        },
    )
    .await?;

    let first: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "doomed-runner".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: None,
        },
    )
    .await?;
    assert!(matches!(first, ClaimResponse::Work { .. }));

    // The doomed runner never heartbeats again; age its last_seen past the
    // online window so the next claim treats its work as abandoned.
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.join("ceilidh.db").display()
    ))
    .await?;
    sqlx::query("UPDATE runners SET last_seen = ? WHERE runner = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind("doomed-runner")
        .execute(&pool)
        .await?;
    pool.close().await;

    let rescued: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "rescue-runner".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: None,
        },
    )
    .await?;

    match rescued {
        ClaimResponse::Work { work } => assert_eq!(work.turn.id, turn.id),
        ClaimResponse::Empty => bail!("abandoned turn should have been requeued"),
    }

    Ok(())
}


/// A runner that restarts under the same id heartbeats with an empty held set;
/// the turn it was playing must go back on the queue instead of wedging the
/// session until the online window would have expired (it never does, because
/// the restarted runner keeps heartbeating).
#[tokio::test]
async fn heartbeat_releases_turns_the_runner_no_longer_holds() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-reconcile-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        None,
        &CreateSessionRequest {
            title: "reconcile".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;

    let turn: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "hello".to_string(),
            lane: None,
        },
    )
    .await?;

    let claimed: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "restarting-runner".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: None,
        },
    )
    .await?;
    assert!(matches!(claimed, ClaimResponse::Work { .. }));

    // Within the grace window a heartbeat that omits the turn changes nothing.
    let status = send_status(
        &app,
        Method::POST,
        "/api/runner/heartbeat",
        None,
        &ceilidh_protocol::Heartbeat {
            runner: "restarting-runner".to_string(),
            active_turns: vec![],
            at: chrono::Utc::now(),
            epoch: None,
        },
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let turns: Vec<Turn> = send_json(
        &app,
        Method::GET,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &(),
    )
    .await?;
    assert_eq!(turns[0].status, TurnStatus::Claimed);

    // Age the claim past the grace window: the restarted runner's next
    // heartbeat releases it.
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.join("ceilidh.db").display()
    ))
    .await?;
    sqlx::query("UPDATE turns SET started_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind(turn.id.to_string())
        .execute(&pool)
        .await?;
    pool.close().await;

    let status = send_status(
        &app,
        Method::POST,
        "/api/runner/heartbeat",
        None,
        &ceilidh_protocol::Heartbeat {
            runner: "restarting-runner".to_string(),
            active_turns: vec![],
            at: chrono::Utc::now(),
            epoch: None,
        },
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let turns: Vec<Turn> = send_json(
        &app,
        Method::GET,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &(),
    )
    .await?;
    assert_eq!(turns[0].status, TurnStatus::Queued, "released back to the queue");

    let again: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "restarting-runner".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: None,
        },
    )
    .await?;
    assert!(matches!(again, ClaimResponse::Work { .. }), "the released turn is claimable again");

    Ok(())
}


/// Two processes under one runner id (an orphan left behind by a redeploy)
/// must not release each other's turns, and a genuinely restarted runner
/// should get its old process's turns back immediately rather than waiting
/// out an online window that its own heartbeats keep refreshing.
#[tokio::test]
async fn runner_epochs_separate_a_restart_from_an_orphan() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-epoch-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        None,
        &CreateSessionRequest {
            title: "epochs".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;

    let turn: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "hello".to_string(),
            lane: None,
        },
    )
    .await?;

    let claimed: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "shared-id".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: Some("epoch-one".to_string()),
        },
    )
    .await?;
    assert!(matches!(claimed, ClaimResponse::Work { .. }));

    // Age the claim past the grace window so only the epoch decides.
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.join("ceilidh.db").display()
    ))
    .await?;
    sqlx::query("UPDATE turns SET started_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind(turn.id.to_string())
        .execute(&pool)
        .await?;
    pool.close().await;

    // The orphan process shares the id but not the epoch: its empty heartbeat
    // must not touch the live process's turn.
    let status = send_status(
        &app,
        Method::POST,
        "/api/runner/heartbeat",
        None,
        &ceilidh_protocol::Heartbeat {
            runner: "shared-id".to_string(),
            active_turns: vec![],
            at: chrono::Utc::now(),
            epoch: Some("orphan-epoch".to_string()),
        },
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let turns: Vec<Turn> = send_json(
        &app,
        Method::GET,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &(),
    )
    .await?;
    assert_eq!(
        turns[0].status,
        TurnStatus::Claimed,
        "an orphan's heartbeat must not release the live process's turn"
    );

    // The live process's own heartbeat, still holding it, changes nothing.
    send_status(
        &app,
        Method::POST,
        "/api/runner/heartbeat",
        None,
        &ceilidh_protocol::Heartbeat {
            runner: "shared-id".to_string(),
            active_turns: vec![turn.id],
            at: chrono::Utc::now(),
            epoch: Some("epoch-one".to_string()),
        },
    )
    .await?;
    let turns: Vec<Turn> = send_json(
        &app,
        Method::GET,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &(),
    )
    .await?;
    assert_eq!(turns[0].status, TurnStatus::Claimed);

    // A restart: same id, new epoch. Its first claim releases the dead
    // process's turn and hands it straight back.
    let after_restart: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "shared-id".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: Some("epoch-two".to_string()),
        },
    )
    .await?;
    match after_restart {
        ClaimResponse::Work { work } => assert_eq!(work.turn.id, turn.id),
        ClaimResponse::Empty => bail!("a restarted runner should reclaim its own abandoned turn"),
    }

    Ok(())
}

/// The session workspace is one directory, so the caller must never hand out
/// a second turn for a session that already has one in flight, even when a
/// reclaim put an older turn back on the queue.
#[tokio::test]
async fn a_session_never_has_two_turns_claimed_at_once() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-one-turn-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        None,
        &CreateSessionRequest {
            title: "one at a time".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;

    let first: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "one".to_string(),
            lane: None,
        },
    )
    .await?;

    let claimed: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "runner-a".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: Some("a".to_string()),
        },
    )
    .await?;
    assert!(matches!(claimed, ClaimResponse::Work { .. }));

    // Force a second queued turn for the same session, the state a reclaim
    // race can produce, and prove no runner is offered it while the first is
    // still in flight.
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.join("ceilidh.db").display()
    ))
    .await?;
    sqlx::query(
        "INSERT INTO turns (id, session_id, seq, input, status, created_at)
         VALUES (?, ?, ?, ?, 'queued', ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(session.id.to_string())
    .bind(2_i64)
    .bind("two")
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&pool)
    .await?;
    pool.close().await;

    let second: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "runner-b".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: Some("b".to_string()),
        },
    )
    .await?;
    assert!(
        matches!(second, ClaimResponse::Empty),
        "no runner may take a second turn for a session that is already busy"
    );

    let _ = first;
    Ok(())
}


/// You must be able to type at an agent that is already working: the message
/// queues and runs next, in order, one at a time.
#[tokio::test]
async fn messages_queue_behind_a_running_turn() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ceilidh-steer-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;

    let app = build_app(ServeOptions {
        bind: "127.0.0.1:0".parse()?,
        db_path: dir.join("ceilidh.db"),
        token: None,
        default_repo_url: None,
    })
    .await?;

    let session: Session = send_json(
        &app,
        Method::POST,
        "/api/sessions",
        None,
        &CreateSessionRequest {
            title: "steering".to_string(),
            lane: Some(Lane {
                harness: Harness::Mock,
                model: "mock".to_string(),
                effort: None,
            }),
            profile: None,
            parent_id: None,
        },
    )
    .await?;

    let first: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "start the long thing".to_string(),
            lane: None,
        },
    )
    .await?;

    let claimed: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "runner-a".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: Some("a".to_string()),
        },
    )
    .await?;
    assert!(matches!(claimed, ClaimResponse::Work { .. }));

    // The steer, typed while the first turn is still running.
    let second: Turn = send_json(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "actually, do it the other way".to_string(),
            lane: None,
        },
    )
    .await?;
    assert_eq!(second.seq, 2);
    assert_eq!(second.status, TurnStatus::Queued);

    // It waits its turn rather than running beside the first.
    let while_busy: ClaimResponse = send_json(
        &app,
        Method::POST,
        "/api/runner/claim",
        None,
        &ClaimRequest {
            runner: "runner-b".to_string(),
            harnesses: vec![Harness::Mock],
            wait_seconds: 0,
            epoch: Some("b".to_string()),
        },
    )
    .await?;
    assert!(matches!(while_busy, ClaimResponse::Empty));

    // The queue has a ceiling.
    for _ in 0..4 {
        let _: Turn = send_json(
            &app,
            Method::POST,
            &format!("/api/sessions/{}/turns", session.id),
            None,
            &PostTurnRequest {
                input: "more".to_string(),
                lane: None,
            },
        )
        .await?;
    }
    let over = send_status(
        &app,
        Method::POST,
        &format!("/api/sessions/{}/turns", session.id),
        None,
        &PostTurnRequest {
            input: "too much".to_string(),
            lane: None,
        },
    )
    .await?;
    assert_eq!(over, StatusCode::CONFLICT);

    let _ = first;
    Ok(())
}

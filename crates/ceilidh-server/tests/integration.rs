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

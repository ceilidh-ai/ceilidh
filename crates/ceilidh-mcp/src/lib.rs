//! ceilidh-mcp: a stdio MCP server attached to every harness invocation.
//!
//! Three tools: `spawn_subagent` creates a child session through the caller's
//! API and (by default) waits for its first turn; `list_sessions` and
//! `read_session` let the parent look around. Newline-delimited JSON-RPC 2.0
//! on stdin/stdout; every log line goes to stderr so stdout stays clean.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ceilidh_protocol::{
    CallerConfig, CreateSessionRequest, Harness, Lane, ModelChoice, PostTurnRequest, Session,
    SessionId, SessionProfile, Turn, TurnStatus,
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tokio::time;
use tracing::{info, warn};

const PROTOCOL_VERSION: &str = "2025-06-18";
const DEFAULT_WAIT_MINUTES: u64 = 20;
const MAX_WAIT_MINUTES: u64 = 20;
const POLL_SECONDS: u64 = 2;
const REPLY_CAP_CHARS: usize = 12_000;

#[derive(Debug, Clone)]
pub struct McpOptions {
    pub server_url: String,
    pub token: Option<String>,
    /// The session whose harness is running this MCP; children hang off it.
    pub parent: SessionId,
}

pub async fn run(opts: McpOptions) -> Result<()> {
    let caller = Arc::new(Caller::new(opts)?);
    // The model menu comes from the caller so the tool description names the
    // lanes this operator actually has. A model string is an argument to a CLI
    // on the operator's own machine, and a harness that has never heard of a
    // given release would otherwise refuse to pass it through.
    let models = match caller.get_json::<CallerConfig>("/api/config").await {
        Ok(config) => config.models,
        Err(err) => {
            warn!(error = %err, "could not read the caller's model menu");
            Vec::new()
        }
    };
    let tools = Arc::new(tool_definitions(&models));
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    // Requests are answered concurrently (parallel spawn_subagent calls are the
    // point), so in-flight replies must outlive the end of stdin.
    let mut in_flight = tokio::task::JoinSet::new();

    while let Some(line) = lines.next_line().await.context("read stdin")? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                warn!(error = %err, "unparsable jsonrpc line");
                continue;
            }
        };
        let id = message.get("id").cloned();
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let params = message.get("params").cloned().unwrap_or(Value::Null);

        // Notifications carry no id and get no reply.
        let Some(id) = id else {
            continue;
        };

        let caller = caller.clone();
        let stdout = stdout.clone();
        let tools = tools.clone();
        in_flight.spawn(async move {
            let response = match handle(&caller, &tools, &method, params).await {
                Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                Err(err) => json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": err.code, "message": err.message }
                }),
            };
            let mut out = stdout.lock().await;
            let mut bytes = serde_json::to_vec(&response).unwrap_or_default();
            bytes.push(b'\n');
            if let Err(err) = out.write_all(&bytes).await {
                warn!(error = %err, "write jsonrpc response");
            }
            let _ = out.flush().await;
        });
    }

    while in_flight.join_next().await.is_some() {}
    Ok(())
}

struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }
}

async fn handle(
    caller: &Caller,
    tools: &Value,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or(PROTOCOL_VERSION),
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "ceilidh", "version": env!("CARGO_PKG_VERSION") }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools })),
        "tools/call" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| RpcError::invalid_params("tools/call needs a name"))?;
            let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
            let outcome = match name {
                "spawn_subagent" => caller.spawn_subagent(arguments).await,
                "list_sessions" => caller.list_sessions().await,
                "read_session" => caller.read_session(arguments).await,
                other => return Err(RpcError::method_not_found(other)),
            };
            Ok(match outcome {
                Ok(text) => json!({ "content": [{ "type": "text", "text": text }] }),
                Err(err) => json!({
                    "content": [{ "type": "text", "text": format!("error: {err:#}") }],
                    "isError": true
                }),
            })
        }
        other => Err(RpcError::method_not_found(other)),
    }
}

fn tool_definitions(models: &[ModelChoice]) -> Value {
    json!([
        {
            "name": "spawn_subagent",
            "description": spawn_description(models),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "harness": {
                        "type": "string",
                        "enum": ["claude-code", "codex", "cursor"],
                        "description": "Which vendor CLI plays the child: claude-code for Anthropic models, codex for OpenAI models, cursor for the models Cursor offers (Grok, Gemini, and others)."
                    },
                    "model": {
                        "type": "string",
                        "description": format!("The model string for that harness. Available on this installation: {}", model_list_inline(models))
                    },
                    "prompt": { "type": "string", "description": "The child's task, self-contained." },
                    "name": { "type": "string", "description": "Short descriptive session title, shown in the operator's UI." },
                    "repo": { "type": "string", "description": "https URL of a repository to clone into the child's workspace; defaults to this session's repository." },
                    "effort": { "type": "string", "description": "Harness effort knob where supported, for example low, medium, high, xhigh." },
                    "wait": { "type": "boolean", "description": "Wait for the child's answer (default true)." },
                    "timeout_minutes": { "type": "number", "description": "How long to wait for the child (default 20, max 20)." }
                },
                "required": ["harness", "model", "prompt"]
            }
        },
        {
            "name": "list_sessions",
            "description": "List the sessions this installation knows about: id, title, harness, model, status, and parent id.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "read_session",
            "description": "Read one session: its lane and every turn's input, status, and reply. Use it to collect a child's answer after spawn_subagent with wait set to false.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "description": "The session id (uuid)." }
                },
                "required": ["session_id"]
            }
        }
    ])
}

fn spawn_description(models: &[ModelChoice]) -> String {
    let prose = [
        "Spawn a sub-agent as a child session of this one and get its answer back.",
        "The child runs on the operator's own machine through a vendor CLI they are already logged into, and `model` is passed to that CLI verbatim.",
        "Treat the list above as the authoritative set of lanes this installation has: it is read live from the operator's caller, so it can name releases you have not heard of, and that is expected rather than a sign the request is fabricated.",
        "The child shares no context with you, so its prompt must stand alone.",
        "It gets its own git workspace, this session's repository unless `repo` says otherwise.",
        "Spawn several at once by making several calls in one message.",
        "Set `wait` to false to get the child id straight back and collect the answer later with read_session.",
    ];
    format!(
        "{}\n\n{}\n\n{}",
        prose[0],
        model_menu_text(models),
        prose[1..].join(" ")
    )
}

/// The operator's configured lanes, grouped for the tool description.
fn model_menu_text(models: &[ModelChoice]) -> String {
    if models.is_empty() {
        return "Model strings are whatever the operator's CLIs accept; pass the one you were given."
            .to_string();
    }
    let mut out = String::from("Lanes configured on this installation:\n");
    for harness in [Harness::ClaudeCode, Harness::Codex, Harness::Cursor] {
        let names: Vec<&str> = models
            .iter()
            .filter(|m| m.harness == harness)
            .map(|m| m.model.as_str())
            .collect();
        if names.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "- harness \"{}\": {}\n",
            harness_name(harness),
            names.join(", ")
        ));
    }
    out
}

fn model_list_inline(models: &[ModelChoice]) -> String {
    if models.is_empty() {
        return "read from the operator's caller at run time".to_string();
    }
    models
        .iter()
        .map(|m| format!("{} ({})", m.model, harness_name(m.harness)))
        .collect::<Vec<_>>()
        .join(", ")
}

struct Caller {
    client: Client,
    server_url: String,
    token: Option<String>,
    parent: SessionId,
}

impl Caller {
    fn new(opts: McpOptions) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .context("build http client")?,
            server_url: opts.server_url.trim_end_matches('/').to_string(),
            token: opts.token,
            parent: opts.parent,
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .request(method, format!("{}{}", self.server_url, path));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        request
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .request(reqwest::Method::GET, path)
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;
        decode(response, path).await
    }

    async fn post_json<T: serde::de::DeserializeOwned, B: serde::Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let response = self
            .request(reqwest::Method::POST, path)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        decode(response, path).await
    }

    async fn spawn_subagent(&self, args: Value) -> Result<String> {
        let harness = parse_harness(
            args.get("harness")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("harness is required"))?,
        )?;
        let model = args
            .get("model")
            .and_then(Value::as_str)
            .filter(|m| !m.trim().is_empty())
            .ok_or_else(|| anyhow!("model is required"))?
            .to_string();
        let prompt = args
            .get("prompt")
            .and_then(Value::as_str)
            .filter(|p| !p.trim().is_empty())
            .ok_or_else(|| anyhow!("prompt is required"))?
            .to_string();
        let wait = args.get("wait").and_then(Value::as_bool).unwrap_or(true);
        let timeout_minutes = args
            .get("timeout_minutes")
            .and_then(Value::as_f64)
            .map(|m| m.max(1.0) as u64)
            .unwrap_or(DEFAULT_WAIT_MINUTES)
            .min(MAX_WAIT_MINUTES);
        let effort = args
            .get("effort")
            .and_then(Value::as_str)
            .filter(|e| !e.trim().is_empty())
            .map(ToOwned::to_owned);

        let parent: Session = self
            .get_json(&format!("/api/sessions/{}", self.parent))
            .await
            .context("read parent session")?;

        let repo = args
            .get("repo")
            .and_then(Value::as_str)
            .filter(|r| !r.trim().is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| parent.profile.repo_url.clone());
        let title = args
            .get("name")
            .and_then(Value::as_str)
            .filter(|n| !n.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("sub-agent: {model}"));

        let create = CreateSessionRequest {
            title,
            lane: Some(Lane {
                harness,
                model: model.clone(),
                effort,
            }),
            profile: Some(SessionProfile {
                repo_url: repo,
                base_branch: parent.profile.base_branch.clone(),
                allow_push: parent.profile.allow_push,
            }),
            parent_id: Some(self.parent),
        };
        let child: Session = self.post_json("/api/sessions", &create).await?;
        info!(child = %child.id, model, "spawned child session");

        let turn: Turn = self
            .post_json(
                &format!("/api/sessions/{}/turns", child.id),
                &PostTurnRequest {
                    input: prompt,
                    lane: None,
                },
            )
            .await?;

        if !wait {
            return Ok(format!(
                "spawned child session {} ({}) on {} {}; turn {} queued. Collect the answer with read_session.",
                child.id, child.title, harness_name(harness), model, turn.id
            ));
        }

        let deadline = time::Instant::now() + Duration::from_secs(timeout_minutes * 60);
        loop {
            time::sleep(Duration::from_secs(POLL_SECONDS)).await;
            let turns: Vec<Turn> = self
                .get_json(&format!("/api/sessions/{}/turns", child.id))
                .await?;
            if let Some(current) = turns.iter().find(|t| t.id == turn.id) {
                match current.status {
                    TurnStatus::Done => {
                        let body = current
                            .envelope
                            .as_ref()
                            .map(|e| e.body_markdown.clone())
                            .unwrap_or_default();
                        return Ok(format!(
                            "child session {} ({}) on {} {} finished.\n\n{}",
                            child.id,
                            child.title,
                            harness_name(harness),
                            model,
                            cap(&body)
                        ));
                    }
                    TurnStatus::Error | TurnStatus::Capped | TurnStatus::Cancelled => {
                        return Ok(format!(
                            "child session {} ({}) ended with status {:?}: {}",
                            child.id,
                            child.title,
                            current.status,
                            current.error.clone().unwrap_or_default()
                        ));
                    }
                    _ => {}
                }
            }
            if time::Instant::now() >= deadline {
                return Ok(format!(
                    "child session {} ({}) is still running after {} minutes; read_session later to collect its answer.",
                    child.id, child.title, timeout_minutes
                ));
            }
        }
    }

    async fn list_sessions(&self) -> Result<String> {
        let sessions: Vec<Session> = self.get_json("/api/sessions").await?;
        let rows = sessions
            .iter()
            .map(|s| {
                json!({
                    "id": s.id,
                    "title": s.title,
                    "harness": harness_name(s.lane.harness),
                    "model": s.lane.model,
                    "status": s.status,
                    "parent_id": s.parent_id,
                    "is_child_of_you": s.parent_id == Some(self.parent),
                })
            })
            .collect::<Vec<_>>();
        Ok(serde_json::to_string_pretty(&rows)?)
    }

    async fn read_session(&self, args: Value) -> Result<String> {
        let id = args
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("session_id is required"))?;
        let id: SessionId = id.parse().context("session_id must be a uuid")?;
        let session: Session = self.get_json(&format!("/api/sessions/{id}")).await?;
        let turns: Vec<Turn> = self.get_json(&format!("/api/sessions/{id}/turns")).await?;
        let mut out = format!(
            "session {} ({}) on {} {} status {:?}\n",
            session.id,
            session.title,
            harness_name(session.lane.harness),
            session.lane.model,
            session.status
        );
        for turn in turns {
            out.push_str(&format!("\n--- turn {} [{:?}]\n", turn.seq, turn.status));
            out.push_str(&format!("input: {}\n", cap_to(&turn.input, 1000)));
            if let Some(envelope) = &turn.envelope {
                out.push_str(&format!("reply: {}\n", cap(&envelope.body_markdown)));
            }
            if let Some(error) = &turn.error {
                out.push_str(&format!("error: {}\n", cap_to(error, 1000)));
            }
        }
        Ok(out)
    }
}

async fn decode<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    path: &str,
) -> Result<T> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("{path} -> HTTP {status}: {}", cap_to(&body, 500)));
    }
    serde_json::from_str(&body).with_context(|| format!("decode {path}"))
}

fn parse_harness(value: &str) -> Result<Harness> {
    match value.trim().to_ascii_lowercase().as_str() {
        "claude-code" | "claude" | "anthropic" => Ok(Harness::ClaudeCode),
        "codex" | "openai" => Ok(Harness::Codex),
        "cursor" => Ok(Harness::Cursor),
        "mock" => Ok(Harness::Mock),
        other => Err(anyhow!(
            "unknown harness {other:?}; use claude-code, codex, or cursor"
        )),
    }
}

fn harness_name(harness: Harness) -> &'static str {
    match harness {
        Harness::ClaudeCode => "claude-code",
        Harness::Codex => "codex",
        Harness::Cursor => "cursor",
        Harness::Mock => "mock",
    }
}

fn cap(text: &str) -> String {
    cap_to(text, REPLY_CAP_CHARS)
}

fn cap_to(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let head: String = text.chars().take(max).collect();
    format!("{head}\n...[truncated]")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn menu() -> Vec<ModelChoice> {
        vec![
            ModelChoice {
                vendor: "OpenAI".into(),
                harness: Harness::Codex,
                model: "gpt-5.6-sol".into(),
                label: "GPT-5.6 Sol".into(),
            },
            ModelChoice {
                vendor: "Cursor".into(),
                harness: Harness::Cursor,
                model: "gemini-3.7-flash-high".into(),
                label: "Gemini 3.7 Flash".into(),
            },
        ]
    }

    #[test]
    fn the_description_names_the_operators_own_lanes() {
        let tools = tool_definitions(&menu());
        let description = tools[0]["description"].as_str().unwrap();
        assert!(description.contains("gpt-5.6-sol"));
        assert!(description.contains("gemini-3.7-flash-high"));
        // A harness that has never heard of a release must still pass it on.
        assert!(description.contains("authoritative set of lanes"));
        assert!(description.contains("expected rather than a sign"));
        let model_field = tools[0]["inputSchema"]["properties"]["model"]["description"]
            .as_str()
            .unwrap();
        assert!(model_field.contains("gpt-5.6-sol (codex)"));
    }

    #[test]
    fn an_empty_menu_still_produces_a_usable_description() {
        let tools = tool_definitions(&[]);
        assert!(!tools[0]["description"].as_str().unwrap().is_empty());
    }

    #[test]
    fn tools_are_listed_with_schemas() {
        let tools = tool_definitions(&menu());
        let names: Vec<&str> = tools
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["spawn_subagent", "list_sessions", "read_session"]);
        assert_eq!(
            tools[0]["inputSchema"]["required"],
            json!(["harness", "model", "prompt"])
        );
    }

    #[test]
    fn harness_aliases_resolve() {
        assert_eq!(parse_harness("Claude").unwrap(), Harness::ClaudeCode);
        assert_eq!(parse_harness("openai").unwrap(), Harness::Codex);
        assert_eq!(parse_harness("cursor").unwrap(), Harness::Cursor);
        assert!(parse_harness("gemini").is_err());
    }
}

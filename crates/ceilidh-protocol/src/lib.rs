//! ceilidh-protocol: the shared contract between the caller (server), the band
//! runners, and the web client. Serde types only, no IO.
//!
//! Contract discipline during phase 1: struct fields are ADDITIVE ONLY (add
//! them with `#[serde(default)]`; never rename or remove).
//!
//! Enum variants are NOT additive in the same sense: serde rejects an unknown
//! variant, so a new `TurnStatus` or `Event` breaks older binaries that
//! deserialize it. Adding a variant therefore means upgrading caller and
//! runners together, until this grows a tolerant representation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type SessionId = Uuid;
pub type TurnId = Uuid;

/// Runners self-identify with a stable string, by convention `<host>:<user>`.
pub type RunnerId = String;

// ---------------------------------------------------------------------------
// Lanes: which band member plays a turn, and how hard.
// ---------------------------------------------------------------------------

/// A harness the band can play through. `Mock` echoes deterministically and
/// exists for CI and smoke tests; it must never be the default outside tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Harness {
    ClaudeCode,
    Codex,
    Cursor,
    Mock,
}

/// An explicit model route. Routing is always explicit: the caller never
/// infers a lane, and an unknown lane is a 422, never a silent fallback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lane {
    pub harness: Harness,
    /// Model identifier in the harness's own vocabulary, e.g. "opus" or
    /// "claude-opus-4-8" for claude-code.
    pub model: String,
    /// Harness-specific effort knob ("max", "xhigh"); None = harness default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

impl Default for Lane {
    fn default() -> Self {
        Lane {
            harness: Harness::ClaudeCode,
            model: "opus".to_string(),
            effort: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// What a session is allowed to touch. Declared at creation, never ambient.
/// v0 is deliberately minimal; this grows into the capability profile.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionProfile {
    /// Repo cloned into the session workspace (https URL). None = scratch dir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_url: Option<String>,
    /// Branch the workspace worktree is cut from. None = repo default branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_branch: Option<String>,
    /// Whether the runner pushes the session branch after each turn commit.
    #[serde(default)]
    pub allow_push: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub title: String,
    pub lane: Lane,
    #[serde(default)]
    pub profile: SessionProfile,
    /// Set at first claim; the session workspace lives on that runner and
    /// later turns are offered to it alone while it stays online.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_affinity: Option<RunnerId>,
    pub status: SessionStatus,
    /// Set when this session was spawned as a sub-agent of another session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<SessionId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Turns
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Queued,
    Claimed,
    Working,
    Done,
    Error,
    /// The lane's plan is capped; the turn may be retried on reset or rerouted.
    Capped,
    /// The human cancelled the turn (before or during execution).
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub id: TurnId,
    pub session_id: SessionId,
    /// 1-based position within the session.
    pub seq: i64,
    /// The user's message for this turn.
    pub input: String,
    /// Overrides the session lane for this turn only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane_override: Option<Lane>,
    pub status: TurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<Envelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Workspace commit sha recorded after the turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Provider session token captured at report; lets the next turn resume
    /// the harness conversation instead of reseeding from history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<String>,
    /// True once a cancel has been asked for but the runner has not yet
    /// reported the turn as cancelled.
    #[serde(default)]
    pub cancel_requested: bool,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Envelope v0: the structured close of every turn. This is the seed of the
// reply grammar; phase 2 grows it into the full progressive-disclosure DSL.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    /// One sentence: what happened.
    pub headline: String,
    #[serde(default)]
    pub work_complete: bool,
    #[serde(default)]
    pub cannot_proceed: bool,
    /// The full reply, markdown.
    pub body_markdown: String,
    /// Questions for the human, each carrying its recommendation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub questions: Vec<Question>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Question {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<String>,
}

// ---------------------------------------------------------------------------
// Events: the SSE stream the web client consumes.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    TurnQueued { turn: Turn },
    TurnClaimed { turn_id: TurnId, runner: RunnerId },
    /// Streaming output chunk while the turn is working.
    Chunk { turn_id: TurnId, text: String },
    TurnDone { turn: Turn },
    TurnError { turn_id: TurnId, message: String },
    RunnerStatus { runner: RunnerStatusInfo },
    TurnCancelled { turn: Turn },
    /// A session appeared (created by a human or spawned as a sub-agent).
    SessionCreated { session: Session },
    /// A session changed shape (archived, unarchived, retitled).
    SessionUpdated { session: Session },
    /// A session and its turns are gone; runners drop its workspace.
    SessionDeleted { session_id: SessionId },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunnerStatusInfo {
    pub runner: RunnerId,
    pub online: bool,
    pub last_seen: DateTime<Utc>,
    #[serde(default)]
    pub active_turns: u32,
}

// ---------------------------------------------------------------------------
// Runner protocol: claim over held long-poll, stream, report.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub runner: RunnerId,
    /// What this runner can execute.
    pub harnesses: Vec<Harness>,
    /// Random per-process id. Two processes sharing a runner id (a redeploy
    /// that left an orphan behind) are told apart by this, and a restarted
    /// runner's first claim releases the turns its previous process held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
    /// How long the server may hold the poll open before answering empty.
    /// The server caps this (30s in v0).
    #[serde(default)]
    pub wait_seconds: u32,
}

/// A summary of a prior turn, used to reseed a harness session when no
/// resume token survives (failover, runner swap).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnSummary {
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headline: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedWork {
    pub turn: Turn,
    pub session: Session,
    /// Recent turns, oldest first, for reseeding when resume_token is absent.
    #[serde(default)]
    pub history_hint: Vec<TurnSummary>,
}

/// Response to a claim: work, or empty after the hold expires.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ClaimResponse {
    Work { work: ClaimedWork },
    Empty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportRequest {
    /// Done, Error, Capped, or Cancelled.
    pub status: TurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<Envelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<String>,
}

/// What the caller tells a runner about an in-flight turn it holds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TurnControl {
    #[serde(default)]
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub runner: RunnerId,
    #[serde(default)]
    pub active_turns: Vec<TurnId>,
    pub at: DateTime<Utc>,
    /// See `ClaimRequest::epoch`. A heartbeat from a stale epoch is recorded
    /// but never used to release another process's turns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
}

// ---------------------------------------------------------------------------
// HTTP API DTOs (the web client's requests).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<Lane>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<SessionProfile>,
    /// Spawn this session as a child of an existing one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<SessionId>,
}

// ---------------------------------------------------------------------------
// Config: what the caller offers the client (model menu, defaults).
// ---------------------------------------------------------------------------

/// One row of the model menu the UI offers, grouped by vendor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelChoice {
    /// Display group, e.g. "Anthropic", "OpenAI", "Cursor".
    pub vendor: String,
    pub harness: Harness,
    /// The exact model string the harness CLI takes.
    pub model: String,
    /// Human label for the picker.
    pub label: String,
}

/// One repository the operator can start a session in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoChoice {
    /// `owner/name`.
    pub full_name: String,
    pub owner: String,
    pub name: String,
    #[serde(default)]
    pub private: bool,
    /// Clone URL the session profile takes.
    pub url: String,
    /// Last push, so the list can lead with what is being worked on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoList {
    /// Owners (the user's account and every org they belong to), each with
    /// their repositories, most recently pushed first.
    #[serde(default)]
    pub owners: Vec<RepoOwner>,
    /// Absent when the caller has no GitHub token configured; the client
    /// falls back to a free-text repository field.
    #[serde(default)]
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoOwner {
    pub login: String,
    #[serde(default)]
    pub repos: Vec<RepoChoice>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerConfig {
    /// Prefilled into the new-session form; None = scratch workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_repo_url: Option<String>,
    #[serde(default)]
    pub models: Vec<ModelChoice>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PostTurnRequest {
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<Lane>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_round_trips() {
        let lane = Lane::default();
        let json = serde_json::to_string(&lane).unwrap();
        assert!(json.contains("claude-code"));
        let back: Lane = serde_json::from_str(&json).unwrap();
        assert_eq!(lane, back);
    }

    #[test]
    fn cursor_harness_is_kebab_case() {
        let json = serde_json::to_string(&Harness::Cursor).unwrap();
        assert_eq!(json, r#""cursor""#);
        let back: Harness = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Harness::Cursor);
    }

    #[test]
    fn event_tags_are_snake_case() {
        let ev = Event::Chunk {
            turn_id: Uuid::nil(),
            text: "hello".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains(r#""type":"chunk""#));
    }

    #[test]
    fn claim_response_round_trips() {
        let json = serde_json::to_string(&ClaimResponse::Empty).unwrap();
        let back: ClaimResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ClaimResponse::Empty);
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        // Additive evolution: an older client must parse a newer server's
        // payload that carries extra fields.
        let json = r#"{"text": "q", "recommendation": null, "future_field": 1}"#;
        let q: Question = serde_json::from_str(json).unwrap();
        assert_eq!(q.text, "q");
    }
}

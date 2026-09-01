# Ceilidh architecture (phase 1: the sessions workbench)

The walking skeleton: one binary, three roles, one contract.

```
 phone / browser
       |
       v
 ceilidh serve            (the caller: axum API + SSE + embedded web UI + SQLite)
       ^          ^
       | HTTP     | held long-poll claim + report
       v          |
   web client   ceilidh runner  x N   (band runners: one per machine/seat)
                    |
                    | shell-out
                    v
                harness CLIs (claude-code, codex; the band)
```

## The flow of one turn

1. The client `POST /api/sessions/{id}/turns {input, lane?}`. The caller
   records the turn `queued` and emits `turn_queued` on the session's SSE feed.
2. A runner holds `POST /api/runner/claim` open (30s cap). The caller offers
   the turn to the session's affinity runner while it is online, else to any
   runner offering the lane's harness; first claim wins, affinity is set on
   first claim.
3. The runner ensures the session workspace exists (clone `profile.repo_url`,
   branch `ceilidh/session-<id>` from `base_branch`), then executes the turn
   through the lane's harness adapter, streaming output chunks to
   `POST /api/runner/turns/{id}/events`, which the caller fans out as SSE
   `chunk` events.
4. The adapter finishes; the runner commits the workspace (`turn <seq>`
   message), optionally pushes, and `POST /api/runner/turns/{id}/report` with
   the envelope, commit sha, and the harness resume token.
5. The caller marks the turn done and emits `turn_done`. The next turn resumes
   the harness session by token; if the token is dead or the runner is gone,
   the claim carries `history_hint` for a reseed.

## Crate ownership (phase-1 build lanes)

| Crate / dir | Owner lane | Contents |
|---|---|---|
| `ceilidh-protocol` | supervisor | shared serde types; ADDITIVE-ONLY changes |
| `ceilidh-caller` | lane/caller-server | transition rules, claim eligibility, affinity |
| `ceilidh-server` | lane/caller-server | axum API, SSE, SQLite store (sqlx), bearer auth, embedded UI |
| `ceilidh-runner` | lane/runner | claim loop, workspace manager, adapters (claude-code, mock) |
| `ceilidh-cli` | supervisor (wave 2) | subcommand wiring, `up` |
| `web/` | lane/web | React + Vite + TS + Tailwind, SSE consumption, mobile-first |
| `scripts/`, `.github/` | lane/infra | smoke.sh, CI, install.sh |

## Conventions

- **Contract discipline:** `ceilidh-protocol` changes are additive only (new
  fields `#[serde(default)]`, new variants at the end). If the contract seems
  wrong, extend additively and flag it in the PR body for supervisor
  reconciliation; never rename or remove during phase 1.
- **sqlx:** use runtime queries (`sqlx::query` / `query_as`), not the
  compile-time `query!` macros, so builds never need a live DATABASE_URL.
- **Auth (v0):** one bearer token (`CEILIDH_TOKEN`) guards the API and the
  runner endpoints alike. No secrets vault in phase 1; runners inherit the
  credentials of the OS user they run as (harness logins, git credential
  helper), which is the subscription-seat model.
- **Streaming:** SSE, not WebSockets, for v0. One feed per session
  (`GET /api/sessions/{id}/events`) plus a firehose (`GET /api/events`).
- **Mock harness:** deterministic echo adapter used by CI and
  `scripts/smoke.sh`; never a default outside tests.
- **Explicit routing:** an unknown lane or harness is a 422 with the menu,
  never a silent fallback.

## What phase 1 deliberately leaves out

Sub-agent spawn (dispatch), multi-step factory runs, the envelope renderer UX
(progressive disclosure, forms), codex and gemini adapters, secrets vault,
multi-tenancy, Postgres. The protocol leaves room for each; the skeleton
builds none.

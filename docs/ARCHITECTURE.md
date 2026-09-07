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
         harness CLIs (claude-code, codex, cursor; the band)
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

## Sub-agents

Every harness invocation gets a stdio MCP server, `ceilidh mcp`, spawned by the
runner with the caller's URL and a parent session id. It exposes
`spawn_subagent` (create a child session with `parent_id`, run one turn, return
its answer), `list_sessions`, and `read_session`. Children are ordinary
sessions: their own lane, their own workspace, their own branch, visible under
their parent in the UI and continuable on their own. A parent can spawn several
at once, of different vendors, by making several tool calls in one message.

The tool description is generated from `GET /api/config` at startup, so it
names the lanes this installation actually has. That is not cosmetic: a harness
asked to pass through a model name it has never heard of will otherwise refuse
the call as fabricated.

## Signing in

The bearer token is a machine credential: runners and the sub-agent MCP
present it. Browsers sign in with Google when the caller has
`CEILIDH_GOOGLE_CLIENT_ID`, `CEILIDH_GOOGLE_CLIENT_SECRET`,
`CEILIDH_PUBLIC_URL` and `CEILIDH_ALLOWED_EMAILS` (comma-separated). The
flow is the plain OpenID Connect code exchange: `/auth/login` sends the
browser to Google with a signed state cookie, `/auth/callback` trades the
code for tokens over TLS, asks Google's userinfo endpoint for the verified
email, checks the allowlist, and sets a signed `ceilidh_session` cookie
(HttpOnly, Secure, SameSite=Lax, 30 days). The cookie is accepted wherever
the token is, including the SSE streams, so a signed-in browser never puts
the token in a URL. `/auth/config` tells the client whether Google is on;
without it the token screen is what you get. The cookie key defaults to the
bearer token, so rotating the token signs every browser out; set
`CEILIDH_COOKIE_SECRET` to decouple them.

## Choosing a repository

A session's repository is a per-session field with a configurable default
(`CEILIDH_DEFAULT_REPO`). When the caller has one or more read-only GitHub
tokens (`CEILIDH_GITHUB_TOKEN`, comma-separated; a fine-grained token
covers one resource owner, so it is one token per account or org, merged
here), `GET /api/repos` returns every repository the
operator can reach, grouped by owner, most recently pushed first, cached for
five minutes; the new-session form renders it as two dropdowns with a
free-text escape hatch. With no token the endpoint reports itself unavailable
and the form shows the free-text field alone, so the picker is additive.

## Cancel

A cancel on a queued turn finishes it immediately. On a turn a runner already
holds, the caller records the request; the runner polls
`GET /api/runner/turns/{id}/control` while it works, kills the harness, and
reports `cancelled`.

## Runner identity

A runner is identified by a stable id (`<host>:<user>` by convention) plus a
random per-process epoch. The id carries workspace affinity across restarts;
the epoch tells the caller which process is current. A heartbeat lists the
turns its process holds, and the caller releases anything assigned to that
runner and missing from the list, which is how a crashed and restarted runner
gets its abandoned turns back rather than wedging the session. A heartbeat from
a stale epoch (an orphan process left behind by a redeploy) reconciles nothing.

## What is still deliberately out

Multi-step factory runs, the envelope renderer UX (progressive disclosure,
forms), a secrets vault, multi-tenancy, Postgres. The protocol leaves room for
each; the skeleton builds none.

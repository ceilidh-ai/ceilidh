# ceilidh

Run a staff of coding agents across Claude Code, Codex and Cursor: on your
own machines, on your own subscriptions.

The server here is called the **caller**, the harness CLIs it drives are the
**band**, and agents take **turns**; the rest of this document just says
session, runner and lane.

## The problem

You want several agent sessions running at once, spread across Claude Code,
Codex and Cursor, each in its own git checkout, able to spawn sub-agents on
other vendors when a task calls for it, reachable from a browser or a phone,
running on your own machines against your own subscriptions: no metered API
key, no desktop app that has to stay open.

## What you get

- One binary that runs a caller (HTTP API, SSE, embedded web UI, SQLite) and
  one or more runners (the processes that shell out to a harness CLI).
- Multi-vendor out of the box: Claude Code, OpenAI Codex CLI, Cursor agent,
  plus a deterministic mock harness for trying it with no logins at all.
- One git checkout per session, on its own branch, committed on every turn.
- Sessions can spawn sub-agents, on any vendor, through an MCP server
  attached to every harness process.
- A web UI reachable from any browser or phone, with a live event feed per
  session.
- Subscription-native: runners execute through the harness CLIs you are
  already logged into. No metered API key required.

## Quickstart

Install the release binary:

```
curl -fsSL https://raw.githubusercontent.com/ceilidh-ai/ceilidh/main/install.sh | bash
```

This picks up a prebuilt binary for macOS (Apple Silicon or Intel) or Linux
x86_64. From a clone, `cargo install --path crates/ceilidh-cli` also works;
build the web UI first (`cd web && npm ci && npm run build`) so it gets
embedded in the binary.

Then:

```
ceilidh up
```

This starts the caller and a local runner on `http://127.0.0.1:8080`, offers
every harness CLI it finds on your `PATH` (`claude`, `codex`, `cursor-agent`),
mints a bearer token on first run, and prints a URL with the token in it.
Open that URL; it also opens your browser for you (`--no-open` to skip that).

No harness CLI installed, or just want to look around first? `ceilidh up
--mock` runs with a deterministic echo harness, so you can try the whole flow
with no logins at all.

**Prerequisites:** `git`, and at least one of Claude Code (`claude`), Codex
CLI (`codex`) or Cursor agent (`cursor-agent`), installed and logged in.
Cursor can also read `CURSOR_API_KEY` from the environment.

## Sessions, lanes and sub-agents

A session runs on a lane: a harness, a model, and an optional effort level.
Models are whatever string the harness CLI itself uses (`claude-opus-5`,
`gpt-6-astra`, `cursor-grok-4.6-high`). Routing is explicit: a lane naming a
harness or model this installation does not offer is a 422 with the menu of
what is actually available, never a silent fallback.

Every harness process gets a stdio MCP server (`ceilidh mcp`) exposing
`spawn_subagent`, `list_sessions` and `read_session`, so any session can hand
off work to a sub-agent on any vendor: a Claude Code session can spawn a
Codex child, which can spawn a Cursor grandchild. Children are ordinary
sessions, with their own lane, workspace and branch, visible under their
parent in the UI and continuable on their own.

## Where things live

Everything lives under `~/.ceilidh` (override with `CEILIDH_HOME`):

```
~/.ceilidh/
  ceilidh.db                        caller state (SQLite)
  token                             the bearer token, mode 0600
  runner/sessions/<session-id>/ws   one git checkout per session
```

A session's checkout lives on its own branch, `ceilidh/<title-slug>-<id8>`,
committed on every turn and pushed only when the session allows push;
sub-agent children never push. A repository is an https URL or an absolute
local path; true worktree mode, sharing objects with an existing checkout
instead of cloning fresh, is planned but not built yet.

## Caller here, runners anywhere

The caller and its runners do not have to share a machine. Run the caller
wherever it is reachable:

```
ceilidh serve --token <t>
```

and a runner on any machine that holds your harness logins:

```
ceilidh runner --server https://<host> --token <t> --harness claude-code --harness codex
```

Runners only ever dial out to the caller; nothing needs to be opened on a
runner's machine. A caller bound to anything beyond loopback refuses to
start without an explicit token, so it never comes up silently exposed.
Browsers can sign in with Google instead of pasting the token; see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the environment variables
that turn it on.

## Plays

Ceilidh runs sessions and sub-agents. Multi-step deterministic plays (a graph
of steps with gates, loops and approvals) are deliberately not built into
Ceilidh and will not be: they run on a rented engine beside it, fabro today
(https://fabro.sh), chosen so it can be swapped later. `install.sh
--with-fabro` installs fabro through its own official installer, as an
opt-in. fabro drives the same harness CLIs, so a play step and a session
share the same logins.

## Configuration

| Variable / flag | Purpose |
|---|---|
| `CEILIDH_HOME` | Root directory for the database, token and runner workspaces. Default `~/.ceilidh`. |
| `CEILIDH_TOKEN` / `--token` | Bearer token guarding the API and the runner protocol. |
| `CEILIDH_DEFAULT_REPO` | Prefilled into the new-session form. |
| `CEILIDH_GITHUB_TOKEN` | One or more read-only GitHub tokens (comma-separated) that turn the repository field into a picker. |
| `CEILIDH_CLAUDE_BIN`, `CEILIDH_CODEX_BIN`, `CEILIDH_CURSOR_BIN` | Where a runner finds each harness CLI. Default to `claude`, `codex`, `cursor-agent` on `PATH`. |
| `CEILIDH_CODEX_AUTH` | Codex login file a runner symlinks into each session's `CODEX_HOME`. Defaults to `~/.codex/auth.json`. |
| `CEILIDH_MAX_TURNS` / `--max-turns` | Turns a runner plays at once. |
| `CEILIDH_DB` / `--db` | The caller's SQLite file. Default `~/.ceilidh/ceilidh.db`. |
| `CEILIDH_DATA_DIR` / `--data-dir` | Where a runner keeps session checkouts. Default `~/.ceilidh/runner`. |
| `CEILIDH_PASS_ENV` / `--pass-env NAME` | Variables to pass into a harness child even though they are stripped by default (see below). Repeatable, or comma separated in the variable. |
| `--no-open` | Do not open the browser when `ceilidh up` starts. |
| Google sign-in | `CEILIDH_GOOGLE_CLIENT_ID`, `CEILIDH_GOOGLE_CLIENT_SECRET`, `CEILIDH_PUBLIC_URL`, `CEILIDH_ALLOWED_EMAILS`, `CEILIDH_COOKIE_SECRET`; see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). |

A harness child never inherits `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` from
your shell, so a subscription login is never silently switched to metered
billing by a key that happened to be exported. To run a harness on an API key
on purpose, start with `--pass-env ANTHROPIC_API_KEY` (or set
`CEILIDH_PASS_ENV`). `CURSOR_API_KEY` passes through, since it is the Cursor
CLI's own auth path.

## Fleet seats (advanced)

Running several runners on one Mac, one per macOS user account so each holds
its own harness logins in its own login keychain, is a real deployment shape
but not one a single laptop needs. See
[deploy/launchd/README.md](deploy/launchd/README.md).

## Status

Pre-alpha, single operator. It assumes one person, or a small trusted group,
not a multi-tenant service.

Deliberately not built: multi-step plays (see Plays above). Not built yet:
telemetry export, planned as a pluggable sink with Agent Beacon first and
plain OTLP as an option; a secrets vault; multi-tenancy; Postgres. See
[docs/BACKLOG.md](docs/BACKLOG.md) for the fuller list of known gaps, and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the pieces fit together.

## Development

```
cargo build --workspace
cargo test --workspace
(cd web && npm ci && npm run build)
bash scripts/smoke.sh
```

`scripts/smoke.sh` runs a caller, a runner and a mock-harness session
end to end with nothing external required. `scripts/vendor-smoke.sh` does
the same against a real deployment, one session per real vendor.

## License

MIT. See [LICENSE](LICENSE).

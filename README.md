# ceilidh

Deterministic orchestration for your staff of agents.

**Status: pre-alpha walking skeleton.** Private while it takes shape.

A ceilidh (KAY-lee) is a gathering where everyone brings a turn: nobody performs
for the room, nobody watches from the wall. The **caller** resolves which agent
takes the next turn; the **band** (model harnesses such as Claude Code and
Codex) plays it, and the dances do not change when the band does. Agents take
**turns**. Everything else here is deliberately plain: sessions, runs,
workspaces, gates.

## What this is

The interactive sessions workbench: run ad-hoc agent sessions on your own
machines (a Mac Mini fleet, a spare box, a cloud VM), each session in its own
git worktree, every turn committed and pushed, reachable from any browser or
phone with your laptop shut. Subscription-seat native: runners execute through
the harness CLIs you are already logged into; no metered API keys required.

One binary:

- `ceilidh serve` runs the caller: HTTP API, embedded web UI, session state (SQLite).
- `ceilidh runner` runs a band runner on any machine that holds your harness logins.
- `ceilidh up` runs both in one process for a single box.

## Layout

```
crates/ceilidh-protocol   the shared contract (serde types only, no IO)
crates/ceilidh-caller     session and turn state machine
crates/ceilidh-server     axum API + SSE events + embedded web UI + SQLite store
crates/ceilidh-runner     claim loop, per-session git workspaces, band adapters
crates/ceilidh-cli        the ceilidh binary
web/                      React web app (built into the binary)
```

Design notes: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Deploy

The caller runs anywhere that can serve HTTP; the runners run wherever your
harness logins live.

- **Caller:** the `Dockerfile` builds the binary with the web client baked in,
  serves on `$PORT`, and keeps SQLite on a mounted volume
  (`CEILIDH_DB`, default `/data/ceilidh.db`). Set `CEILIDH_TOKEN` and
  optionally `CEILIDH_DEFAULT_REPO`.
- **Runners on macOS:** `deploy/launchd/install-runner.sh <user> <binary>
  <runner.env>` installs one LaunchDaemon per seat user, unlocking that user's
  login keychain so the harness CLIs can read their subscription credentials.
  `uninstall-runner.sh <user>` is the exact undo. Details in
  [deploy/launchd/README.md](deploy/launchd/README.md).
- **Check it:** `scripts/vendor-smoke.sh` creates one session per vendor
  through the API and asserts a non-empty first reply.

## Dev

```
cargo build --workspace
cargo test --workspace
(cd web && npm install && npm run build)
bash scripts/smoke.sh
```

## Configuration

The caller reads `CEILIDH_TOKEN` (the machine credential runners and the
sub-agent MCP present), `CEILIDH_DEFAULT_REPO` (prefilled into the
new-session form), `CEILIDH_GITHUB_TOKEN` (one or more read-only GitHub
tokens, comma-separated, that turn the repository field into a picker), and
the Google sign-in set described in `docs/ARCHITECTURE.md`.

# Ceilidh backlog

Carried findings and known gaps, so nothing rides only in a session log.
Phase-1 fixes that already landed are in the git history, not here.

## From the 2026-08-31 cross-family adversarial review (grok-4.5 + gemini)

Deferred deliberately: each is real, and none blocks a single operator
running the caller on loopback or a private network with a token.

1. **Runner credentials are the operator's credentials.** One `CEILIDH_TOKEN`
   authorizes both the web client and the runner protocol, and `chunk` /
   `report` authorize by turn id and status rather than by the claiming
   runner. Anyone holding the token can inject chunks or close someone else's
   turn. Fix: separate runner credentials, bind chunk and report to the
   claiming runner plus a claim epoch.
2. **SSE authenticates by `?token=`.** Browser EventSource cannot set headers,
   so the token lands in proxy logs and browser history, and the comparison
   does not URL-decode. Fix: short-lived single-use SSE tickets minted by a
   POST, or a cookie.
3. **Lagged SSE subscribers silently skip events.** The broadcast receiver
   treats `Lagged` as `continue`, so a slow tab can miss `turn_done` and sit
   on "working" until reload. Fix: surface lag to the client and resync from
   the turns endpoint.
4. **Report is not retried.** A network blip after a long turn leaves work
   done on disk and the turn requeued by the reclaim sweep, which can rerun
   it. Fix: idempotent report with backoff, keyed so a rerun is detectable.
5. **Push targets whatever the profile asked for.** With `allow_push`, the
   runner pushes using its ambient git credentials to any repo the session
   named. Acceptable while the operator is the only caller; a runner-side
   allowlist is the fix before any second user exists.
6. **Enum evolution breaks old binaries.** Documented honestly in the
   protocol now. Fix when it bites: a tolerant representation, or version the
   claim endpoint.

## Phase-1 scope gaps

Closed on 2026-09-06: the codex and cursor adapters, sub-agent spawn, and
cancel all shipped. Still deliberately out: multi-step factory runs, the
envelope renderer UX (progressive disclosure and forms), a secrets vault,
multi-tenancy, Postgres.

## From the 2026-09-06 multi-vendor build

1. **The MCP process holds the operator's token.** The runner hands
   `CEILIDH_TOKEN` to every `ceilidh mcp` child through its config block. The
   harness process itself no longer sees it (the adapter clears it from the
   environment) and the tools only reach this session's own family, but the
   token is still the operator's. Same root problem as item 1 above, same fix.
2. **A cancelled turn's harness is killed, not asked to stop.** It is killed by
   process group now, so nothing is orphaned, but a graceful signal first would
   let a turn mid-write finish its file.
3. **Sub-agent depth is unbounded.** A child's harness gets its own MCP, so it
   can spawn its own children. Concurrent spawns per process are capped at
   four; total depth and fleet-wide fan-out are not.
4. **A child session's repository is model-chosen.** `spawn_subagent` accepts a
   `repo` argument, so a prompt-injected parent can make a runner clone an
   arbitrary public URL. Children never push and never inherit the parent's
   push permission, which removes the exfiltration path, but a runner-side
   allowlist is still the real fix.
5. **A harness that writes nothing to stdout fails the turn.** Reporting an
   empty reply as success hid a whole class of harness misconfiguration, so an
   empty reply is now an error. A harness that legitimately answers with
   silence would need a different signal.

## Operational

- `web/dist` is served from disk when present; embedding it in the binary
  (rust-embed is already a dependency) is what makes the single-binary
  install story true.
- No release has been cut, so `install.sh` falls back to building from
  source.

## From the 2026-09-08 laptop review

1. **Per-session `CLAUDE_CONFIG_DIR` isolation.** Claude Code currently reads
   the operator's whole `~/.claude` (memory, settings, credentials) for every
   session; a session-scoped config directory would stop sessions from
   sharing that state.
2. **An allowlisted child environment with per-lane credentials.** The
   harness inherits the runner's whole environment today, so an API key
   sitting in the shell silently flips a subscription lane to metered
   billing.
3. **A sub-agent depth cap, and a fast failure when no runner has a free
   slot, instead of leaving parents waiting.** `--max-turns 1` deadlocks a
   spawn: the parent holds its one slot waiting on a child that has nowhere
   to run.
4. **Runner credentials separate from the operator token.** Same root cause
   as the phase-1 items above: one token still authorizes both the browser
   and the runner protocol.
5. **A telemetry sink**, so a session's turns are observable somewhere
   other than the caller's own SQLite and the web UI.
6. **True worktree mode**, sharing objects with an existing checkout instead
   of cloning fresh per session.
7. **A Linux arm64 release target.** Today's release matrix covers macOS
   (Apple Silicon and Intel) and Linux x86_64 only.

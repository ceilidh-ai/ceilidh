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

## Phase-1 scope gaps (deliberate, from the kickoff brief)

Sub-agent spawn (dispatch), multi-step factory runs, the envelope renderer UX
(progressive disclosure and forms), codex and gemini adapters, a secrets
vault, multi-tenancy, Postgres.

## Operational

- `web/dist` is served from disk when present; embedding it in the binary
  (rust-embed is already a dependency) is what makes the single-binary
  install story true.
- No release has been cut, so `install.sh` falls back to building from
  source.

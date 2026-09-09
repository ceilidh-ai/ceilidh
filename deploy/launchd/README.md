# Band runners under launchd (macOS seats)

One system LaunchDaemon per seat user, each running `ceilidh runner` as that
user with the harness logins that user holds. The pattern (a daemon that execs
`ssh <user>@localhost` with a per-user loop key into an inner script that
unlocks the login keychain) exists because the harness CLIs read their
subscription credentials from the login keychain, and only an sshd-made login
session can read it without a GUI login.

**This is a fleet pattern, not a default.** Everything below assumes several
macOS user accounts sharing one host, each with its own login keychain
holding that seat's harness subscriptions. A single laptop, running under
your own login as yourself, never needs any of this: just run `ceilidh up`
or `ceilidh runner` directly and it will use whatever harness logins your
own session already has.

Files per seat, all under `/Users/<user>/ceilidh/`:

| Path | Purpose |
| --- | --- |
| `bin/ceilidh` | the release binary (also spawned as `ceilidh mcp` by every harness) |
| `runner.env` | mode 0600: caller URL, token, harness flags, optional Cursor API key |
| `run-runner.sh` | the daemon entry: `ssh -i <loop key> <user>@localhost run-runner-inner.sh` |
| `run-runner-inner.sh` | keychain unlock, `runner.env`, then `exec ceilidh runner ...` |
| `runner/` | data dir: one `sessions/<id>/ws` git workspace per session |

Plus `/Library/LaunchDaemons/com.ceilidh.runner-<user>.plist` and the log at
`/tmp/ceilidh_runner_<user>.log`.

Install (as root on the seat host):

```
sudo bash install-runner.sh <user> ./ceilidh ./runner.env
```

Undo:

```
sudo bash uninstall-runner.sh <user>            # removes everything, workspaces included
sudo bash uninstall-runner.sh <user> --keep-data
```

Prerequisites on the seat: the loop key at `~/.ssh/nova_loop_ed25519` (its
public half in `~/.ssh/authorized_keys`, and the user in the sshd access
group), the keychain password at `~/.nova-kc-pw` (0600), `claude` / `codex`
/ `cursor-agent` on `/opt/homebrew/bin`, and `gh auth login` for git over
https. Override the key or password paths with `LOOP_KEY=` / `KC_PW_FILE=`.

Reinstalling is safe to repeat: `launchctl bootout` only kills the local ssh
client, so the installer also reaps any runner process still alive under that
user before bootstrapping the new one. Two processes sharing a runner id would
both claim turns, and the caller's epoch check would see one of them as a
zombie.

Codex runs from a per-session `CODEX_HOME` whose `auth.json` is a symlink to
the seat's `~/.codex/auth.json` (override with `CEILIDH_CODEX_AUTH`), so the
seat's global Codex config (plugins, trusted projects) never leaks into a
session and a token refresh is shared by every client of that login.

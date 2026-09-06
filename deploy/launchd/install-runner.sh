#!/bin/bash
# Install a ceilidh band runner for one macOS seat user as a system LaunchDaemon.
#
# Run as root on the seat host:
#   sudo bash install-runner.sh <user> <path-to-ceilidh-binary> <path-to-runner.env>
#
# The daemon runs as <user> and execs `ssh <user>@localhost` with the seat's
# loop key, because sshd manufactures the login session the harness CLIs need
# to read the login keychain; the inner script unlocks that keychain from the
# seat's password file, loads runner.env, and execs `ceilidh runner`.
#
# runner.env is a KEY=VALUE file (mode 0600) with at least:
#   CEILIDH_SERVER=https://caller.example
#   CEILIDH_TOKEN=...
#   CEILIDH_HARNESS_FLAGS="--harness claude-code --harness codex --harness cursor"
# and optionally CURSOR_API_KEY, CEILIDH_MAX_TURNS, CEILIDH_CURSOR_BIN, CEILIDH_CODEX_AUTH.
#
# Undo: uninstall-runner.sh <user>.
set -euo pipefail

user="${1:?seat user}"
binary="${2:?path to the ceilidh binary}"
env_file="${3:?path to runner.env}"
loop_key="${LOOP_KEY:-/Users/$user/.ssh/nova_loop_ed25519}"
kc_pw_file="${KC_PW_FILE:-/Users/$user/.nova-kc-pw}"
label="com.ceilidh.runner-$user"
home="/Users/$user"
root="$home/ceilidh"

[[ "$(id -u)" -eq 0 ]] || { echo "run as root" >&2; exit 1; }
id "$user" >/dev/null 2>&1 || { echo "no such user: $user" >&2; exit 1; }
[[ -f "$loop_key" ]] || { echo "loop key missing: $loop_key" >&2; exit 1; }
[[ -f "$kc_pw_file" ]] || { echo "keychain password file missing: $kc_pw_file" >&2; exit 1; }

mkdir -p "$root/bin" "$root/runner"
install -m 0755 "$binary" "$root/bin/ceilidh"
install -m 0600 "$env_file" "$root/runner.env"

cat > "$root/run-runner.sh" <<OUTER
#!/bin/bash
exec /usr/bin/ssh -i $loop_key -o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ServerAliveInterval=30 $user@localhost $root/run-runner-inner.sh
OUTER
chmod 0755 "$root/run-runner.sh"

cat > "$root/run-runner-inner.sh" <<INNER
#!/bin/bash
set -u
KC="\$HOME/Library/Keychains/login.keychain-db"
security list-keychains -d user -s "\$KC" 2>/dev/null
security unlock-keychain -p "\$(cat $kc_pw_file)" "\$KC" 2>/dev/null || echo "ceilidh runner: keychain unlock failed for \$USER" >&2
set -a; . "$root/runner.env"; set +a
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:\$HOME/.local/bin:\${PATH:-}"
cd "$root"
exec "$root/bin/ceilidh" runner --server "\$CEILIDH_SERVER" --data-dir "$root/runner" --runner-id "\$(hostname -s):\$USER" \${CEILIDH_HARNESS_FLAGS:---harness claude-code}
INNER
chmod 0700 "$root/run-runner-inner.sh"
chown -R "$user:staff" "$root"

plist="/Library/LaunchDaemons/$label.plist"
cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>$label</string>
	<key>UserName</key>
	<string>$user</string>
	<key>ProgramArguments</key>
	<array>
		<string>$root/run-runner.sh</string>
	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>HOME</key>
		<string>$home</string>
		<key>PATH</key>
		<string>/opt/homebrew/bin:/usr/bin:/bin</string>
	</dict>
	<key>WorkingDirectory</key>
	<string>$root</string>
	<key>KeepAlive</key>
	<true/>
	<key>RunAtLoad</key>
	<true/>
	<key>StandardOutPath</key>
	<string>/tmp/ceilidh_runner_$user.log</string>
	<key>StandardErrorPath</key>
	<string>/tmp/ceilidh_runner_$user.log</string>
</dict>
</plist>
PLIST
chown root:wheel "$plist"; chmod 0644 "$plist"

launchctl bootout system "$plist" >/dev/null 2>&1 || true
# bootout kills the local ssh client, not the remote command it opened, so the
# previous runner process survives a reinstall and keeps claiming turns under
# the same runner id. Reap it before the new one starts.
for _ in 1 2 3 4 5; do
  pkill -u "$user" -f "$root/bin/ceilidh runner" >/dev/null 2>&1 || break
  sleep 1
done
pkill -9 -u "$user" -f "$root/bin/ceilidh runner" >/dev/null 2>&1 || true
launchctl bootstrap system "$plist"
echo "installed $label (log: /tmp/ceilidh_runner_$user.log)"

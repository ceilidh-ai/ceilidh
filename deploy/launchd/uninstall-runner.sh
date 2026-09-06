#!/bin/bash
# Remove the ceilidh runner daemon for one seat user (the undo of install-runner.sh).
# Keeps nothing: the plist, the scripts, the binary, the env file, and the
# session workspaces under /Users/<user>/ceilidh all go. Pass --keep-data to
# leave the workspaces in place.
set -euo pipefail
user="${1:?seat user}"
label="com.ceilidh.runner-$user"
plist="/Library/LaunchDaemons/$label.plist"
root="/Users/$user/ceilidh"
[[ "$(id -u)" -eq 0 ]] || { echo "run as root" >&2; exit 1; }
launchctl bootout system "$plist" >/dev/null 2>&1 || true
rm -f "$plist"
if [[ "${2:-}" == "--keep-data" ]]; then
  rm -f "$root/bin/ceilidh" "$root/run-runner.sh" "$root/run-runner-inner.sh" "$root/runner.env"
else
  rm -rf "$root"
fi
rm -f "/tmp/ceilidh_runner_$user.log"
echo "removed $label"

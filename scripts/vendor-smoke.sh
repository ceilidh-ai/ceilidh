#!/usr/bin/env bash
# One session per vendor through the API, asserting a non-empty first reply.
# Usage: CEILIDH_URL=https://caller CEILIDH_TOKEN=... bash scripts/vendor-smoke.sh
# Lanes default to one per vendor; override with LANES="harness:model harness:model".
set -euo pipefail

URL="${CEILIDH_URL:-http://127.0.0.1:8080}"
TOKEN="${CEILIDH_TOKEN:?set CEILIDH_TOKEN}"
LANES="${LANES:-claude-code:claude-sonnet-4-6 codex:gpt-5.6-sol cursor:gemini-3.7-flash-high}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"
PROMPT="${PROMPT:-Reply with exactly: SMOKE OK}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "$1 is required" >&2; exit 2; }; }
need curl; need jq

api() {
  local method="$1" path="$2" data="${3:-}"
  if [[ -n "$data" ]]; then
    curl -fsS -X "$method" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" --data "$data" "$URL$path"
  else
    curl -fsS -X "$method" -H "Authorization: Bearer $TOKEN" "$URL$path"
  fi
}

curl -fsS "$URL/health" >/dev/null || { echo "FAIL: $URL/health"; exit 1; }

declare -a session_ids=() turn_ids=() labels=()
stamp="$(date -u +%Y-%m-%dT%H:%MZ)"
for lane in $LANES; do
  harness="${lane%%:*}"; model="${lane#*:}"
  title="smoke $harness $model $stamp"
  body="$(jq -cn --arg t "$title" --arg h "$harness" --arg m "$model" '{title:$t, lane:{harness:$h, model:$m}}')"
  session="$(api POST /api/sessions "$body")"
  sid="$(jq -er '.id' <<<"$session")"
  turn="$(api POST "/api/sessions/$sid/turns" "$(jq -cn --arg p "$PROMPT" '{input:$p}')")"
  tid="$(jq -er '.id' <<<"$turn")"
  session_ids+=("$sid"); turn_ids+=("$tid"); labels+=("$harness:$model")
  echo "queued  $harness:$model  session=$sid"
done

deadline=$((SECONDS + TIMEOUT_SECONDS))
failures=0
for i in "${!session_ids[@]}"; do
  sid="${session_ids[$i]}"; tid="${turn_ids[$i]}"; label="${labels[$i]}"
  while true; do
    turn="$(api GET "/api/sessions/$sid/turns" | jq -c --arg id "$tid" 'map(select(.id == $id))[0]')"
    status="$(jq -r '.status' <<<"$turn")"
    case "$status" in
      done)
        reply="$(jq -r '.envelope.body_markdown // ""' <<<"$turn")"
        if [[ -n "${reply// /}" ]]; then
          echo "PASS    $label  reply=$(printf '%s' "$reply" | head -c 80 | tr '\n' ' ')"
        else
          echo "FAIL    $label  empty reply"; failures=$((failures + 1))
        fi
        break ;;
      error|capped|cancelled)
        echo "FAIL    $label  status=$status error=$(jq -r '.error // ""' <<<"$turn" | head -c 200 | tr '\n' ' ')"
        failures=$((failures + 1)); break ;;
    esac
    if (( SECONDS >= deadline )); then
      echo "FAIL    $label  still $status after ${TIMEOUT_SECONDS}s"; failures=$((failures + 1)); break
    fi
    sleep 3
  done
done

if (( failures > 0 )); then echo "FAIL: $failures lane(s) failed"; exit 1; fi
echo "PASS: every vendor lane replied"

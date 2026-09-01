#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_URL="http://127.0.0.1:8899"
export CEILIDH_TOKEN="smoke-token"

TMP_DIR=""
SERVER_PID=""
RUNNER_PID=""
FAIL_PRINTED=0

fail() {
  FAIL_PRINTED=1
  echo "FAIL: $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required for scripts/smoke.sh"
}

dump_logs() {
  if [[ -n "${TMP_DIR:-}" && -d "$TMP_DIR" ]]; then
    if [[ -f "$TMP_DIR/server.log" ]]; then
      echo "server log:" >&2
      sed -n '1,160p' "$TMP_DIR/server.log" >&2 || true
    fi
    if [[ -f "$TMP_DIR/runner.log" ]]; then
      echo "runner log:" >&2
      sed -n '1,160p' "$TMP_DIR/runner.log" >&2 || true
    fi
  fi
}

cleanup() {
  status=$?
  if [[ $status -ne 0 ]]; then
    if [[ "$FAIL_PRINTED" -ne 1 ]]; then
      echo "FAIL: smoke failed" >&2
    fi
    dump_logs
  fi

  if [[ -n "${RUNNER_PID:-}" ]]; then
    kill "$RUNNER_PID" >/dev/null 2>&1 || true
    wait "$RUNNER_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TMP_DIR:-}" && -d "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

curl_json() {
  method="$1"
  url="$2"
  data="${3:-}"

  if [[ -n "$data" ]]; then
    curl -fsS \
      -X "$method" \
      -H "Authorization: Bearer $CEILIDH_TOKEN" \
      -H "Content-Type: application/json" \
      --data "$data" \
      "$url"
  else
    curl -fsS \
      -X "$method" \
      -H "Authorization: Bearer $CEILIDH_TOKEN" \
      "$url"
  fi
}

assert_processes_alive() {
  kill -0 "$SERVER_PID" >/dev/null 2>&1 || fail "ceilidh serve exited unexpectedly"
  kill -0 "$RUNNER_PID" >/dev/null 2>&1 || fail "ceilidh runner exited unexpectedly"
}

need cargo
need curl
need jq

cd "$ROOT"
cargo build --workspace

TMP_DIR="$(mktemp -d)"

target/debug/ceilidh serve --bind 127.0.0.1:8899 --db "$TMP_DIR/ceilidh.db" >"$TMP_DIR/server.log" 2>&1 &
SERVER_PID=$!

health_deadline=$((SECONDS + 30))
until curl -fsS "$SERVER_URL/health" >/dev/null 2>&1; do
  kill -0 "$SERVER_PID" >/dev/null 2>&1 || fail "ceilidh serve exited before /health became ready"
  if (( SECONDS >= health_deadline )); then
    fail "/health did not become ready within 30s"
  fi
  sleep 1
done

target/debug/ceilidh runner --server "$SERVER_URL" --data-dir "$TMP_DIR/runner" --harness mock >"$TMP_DIR/runner.log" 2>&1 &
RUNNER_PID=$!

create_session_response="$(curl_json POST "$SERVER_URL/api/sessions" '{"title":"smoke","lane":{"harness":"mock","model":"mock"}}')" \
  || fail "POST /api/sessions failed"
session_id="$(jq -er '.id | select(type == "string" and length > 0)' <<<"$create_session_response")" \
  || fail "POST /api/sessions did not return a non-empty id"

post_turn_response="$(curl_json POST "$SERVER_URL/api/sessions/$session_id/turns" '{"input":"hello ceilidh"}')" \
  || fail "POST /api/sessions/$session_id/turns failed"
turn_id="$(jq -er '.id | select(type == "string" and length > 0)' <<<"$post_turn_response")" \
  || fail "POST /api/sessions/$session_id/turns did not return a non-empty id"

poll_deadline=$((SECONDS + 60))
turn_json=""
while true; do
  assert_processes_alive
  turns_response="$(curl_json GET "$SERVER_URL/api/sessions/$session_id/turns")" \
    || fail "GET /api/sessions/$session_id/turns failed"
  turn_json="$(jq -cer --arg id "$turn_id" '
    if type == "array" then
      map(select(.id == $id))[0]
    elif type == "object" and (.turns? | type == "array") then
      .turns | map(select(.id == $id))[0]
    elif type == "object" and .id == $id then
      .
    else
      null
    end | select(. != null)
  ' <<<"$turns_response")" || fail "GET /api/sessions/$session_id/turns did not include turn $turn_id"

  status="$(jq -r '.status' <<<"$turn_json")"
  case "$status" in
    done)
      break
      ;;
    error)
      error_message="$(jq -r '.error // "unknown error"' <<<"$turn_json")"
      fail "turn $turn_id ended with error: $error_message"
      ;;
    queued|claimed|working)
      ;;
    *)
      fail "turn $turn_id had unexpected status: $status"
      ;;
  esac

  if (( SECONDS >= poll_deadline )); then
    fail "turn $turn_id did not complete within 60s"
  fi
  sleep 1
done

jq -er '.envelope.headline | select(type == "string" and length > 0)' <<<"$turn_json" >/dev/null \
  || fail "turn $turn_id had an empty envelope headline"
jq -er '.commit | select(type == "string" and length > 0)' <<<"$turn_json" >/dev/null \
  || fail "turn $turn_id had an empty commit"
jq -er '.envelope.body_markdown | select(type == "string" and contains("hello ceilidh"))' <<<"$turn_json" >/dev/null \
  || fail 'turn body_markdown did not contain "hello ceilidh"'

echo "PASS: smoke completed"

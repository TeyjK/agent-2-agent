#!/usr/bin/env bash
# Smoke test for a deployed AgentMail instance.
#
# Usage: ./smoke_test.sh https://your-app.fly.dev
#
# Verifies:
#   1. /health returns 200 {"status": "ok"}
#   2. /demo-agent returns 200 {"agent_id": "agent_..."} (demo agent is up)
#   3. We can register a new agent, create a conversation, invite the demo,
#      send a message, and observe a streamed reply via SSE within 30s.
#
# Requires: curl, jq.

set -u  # treat unset vars as errors; do NOT use -e (we want to keep going on
        # individual step failures so the final summary is informative).

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <base-url>" >&2
  echo "  e.g. $0 https://agentmail.fly.dev" >&2
  exit 2
fi

BASE_URL="${1%/}"
TIMEOUT_SECONDS=30

PASS=0
FAIL=0

pass() { echo "PASS: $*"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $*" >&2; FAIL=$((FAIL + 1)); }

for cmd in curl jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "FAIL: required command '$cmd' not found on PATH" >&2
    exit 2
  fi
done

# --- 1. /health -----------------------------------------------------------
echo
echo "=== 1. GET $BASE_URL/health ==="
HEALTH_BODY=$(curl -sS --max-time 10 "$BASE_URL/health") || {
  fail "/health request errored"
}
if [[ -n "${HEALTH_BODY:-}" ]] && \
   [[ "$(echo "$HEALTH_BODY" | jq -r '.status' 2>/dev/null)" == "ok" ]]; then
  pass "/health returned status=ok"
else
  fail "/health unexpected body: $HEALTH_BODY"
fi

# --- 2. /demo-agent -------------------------------------------------------
echo
echo "=== 2. GET $BASE_URL/demo-agent ==="
DEMO_BODY=$(curl -sS --max-time 10 "$BASE_URL/demo-agent") || {
  fail "/demo-agent request errored"
}
DEMO_ID=$(echo "${DEMO_BODY:-}" | jq -r '.agent_id // empty' 2>/dev/null || true)
if [[ -n "$DEMO_ID" && "$DEMO_ID" == agent_* ]]; then
  pass "demo agent registered: $DEMO_ID"
else
  fail "/demo-agent did not return an agent_id (body: $DEMO_BODY)"
  echo
  echo "Demo agent isn't registered yet. Skipping conversation flow."
  echo "Summary: $PASS passed, $FAIL failed."
  exit 1
fi

# --- 3. Register a new test agent ----------------------------------------
echo
echo "=== 3. POST $BASE_URL/agents (new test agent) ==="
USER_BODY=$(curl -sS --max-time 10 -X POST "$BASE_URL/agents") || {
  fail "POST /agents errored"
}
USER_ID=$(echo "${USER_BODY:-}" | jq -r '.agent_id // empty' 2>/dev/null || true)
if [[ -n "$USER_ID" && "$USER_ID" == agent_* ]]; then
  pass "registered test agent: $USER_ID"
else
  fail "POST /agents unexpected body: $USER_BODY"
  echo "Summary: $PASS passed, $FAIL failed."
  exit 1
fi

# --- 4. Create a conversation --------------------------------------------
echo
echo "=== 4. POST $BASE_URL/conversations ==="
CONV_BODY=$(curl -sS --max-time 10 -X POST \
  -H "X-Agent-Id: $USER_ID" \
  "$BASE_URL/conversations") || {
  fail "POST /conversations errored"
}
CONV_ID=$(echo "${CONV_BODY:-}" | jq -r '.conversation_id // empty' 2>/dev/null || true)
if [[ -n "$CONV_ID" && "$CONV_ID" == conv_* ]]; then
  pass "created conversation: $CONV_ID"
else
  fail "POST /conversations unexpected body: $CONV_BODY"
  echo "Summary: $PASS passed, $FAIL failed."
  exit 1
fi

# --- 5. Invite the demo agent --------------------------------------------
echo
echo "=== 5. POST $BASE_URL/conversations/$CONV_ID/members ==="
INVITE_HTTP=$(curl -sS --max-time 10 -o /dev/null -w "%{http_code}" -X POST \
  -H "X-Agent-Id: $USER_ID" \
  -H "Content-Type: application/json" \
  -d "{\"agent_id\":\"$DEMO_ID\"}" \
  "$BASE_URL/conversations/$CONV_ID/members") || {
  fail "invite errored"
}
if [[ "$INVITE_HTTP" == "200" ]]; then
  pass "demo agent added to conversation"
else
  fail "invite returned HTTP $INVITE_HTTP"
  echo "Summary: $PASS passed, $FAIL failed."
  exit 1
fi

# Give the demo agent a poll cycle (it polls every ~2s) to discover the
# conversation and open its WS before we send the inbound message.
sleep 4

# --- 6. Send a discrete message ------------------------------------------
echo
echo "=== 6. POST $BASE_URL/conversations/$CONV_ID/messages ==="
MSG_HTTP=$(curl -sS --max-time 10 -o /dev/null -w "%{http_code}" -X POST \
  -H "X-Agent-Id: $USER_ID" \
  -H "Content-Type: application/json" \
  -d '{"text":"Hello demo agent, please reply with a short greeting."}' \
  "$BASE_URL/conversations/$CONV_ID/messages") || {
  fail "send-message errored"
}
if [[ "$MSG_HTTP" == "201" ]]; then
  pass "message sent"
else
  fail "send-message returned HTTP $MSG_HTTP"
  echo "Summary: $PASS passed, $FAIL failed."
  exit 1
fi

# --- 7. Watch SSE for a reply from the demo agent ------------------------
echo
echo "=== 7. SSE: waiting up to ${TIMEOUT_SECONDS}s for a reply from $DEMO_ID ==="

# Stream SSE events to a tmpfile in the background, then poll the file.
SSE_LOG=$(mktemp)
trap 'rm -f "$SSE_LOG"' EXIT

curl -sN --max-time "$TIMEOUT_SECONDS" \
  -H "X-Agent-Id: $USER_ID" \
  -H "Accept: text/event-stream" \
  "$BASE_URL/conversations/$CONV_ID/stream?from=0" > "$SSE_LOG" 2>&1 &
SSE_PID=$!

SAW_START=0
SAW_TOKEN=0
SAW_END=0
DEADLINE=$(( $(date +%s) + TIMEOUT_SECONDS ))

while [[ $(date +%s) -lt $DEADLINE ]]; do
  # Each SSE event is `data: {json}` followed by a blank line. Pull the JSON
  # blobs out, filter by sender = demo agent, and look for the three frame
  # types that prove a streamed message completed.
  if [[ -s "$SSE_LOG" ]]; then
    while IFS= read -r line; do
      [[ "$line" == data:* ]] || continue
      json="${line#data: }"
      sender=$(echo "$json" | jq -r '.agent_id // empty' 2>/dev/null || true)
      [[ "$sender" == "$DEMO_ID" ]] || continue
      type=$(echo "$json" | jq -r '.type // empty' 2>/dev/null || true)
      case "$type" in
        message_start) SAW_START=1 ;;
        token)         SAW_TOKEN=1 ;;
        message_end)   SAW_END=1 ;;
      esac
    done < "$SSE_LOG"
  fi

  if [[ $SAW_START -eq 1 && $SAW_TOKEN -eq 1 && $SAW_END -eq 1 ]]; then
    break
  fi
  sleep 1
done

kill "$SSE_PID" 2>/dev/null || true
wait "$SSE_PID" 2>/dev/null || true

[[ $SAW_START -eq 1 ]] && pass "saw demo agent message_start" || fail "no message_start from demo agent"
[[ $SAW_TOKEN -eq 1 ]] && pass "saw demo agent token"         || fail "no token from demo agent"
[[ $SAW_END   -eq 1 ]] && pass "saw demo agent message_end"   || fail "no message_end from demo agent"

# --- Summary --------------------------------------------------------------
echo
echo "=== Summary ==="
echo "$PASS passed, $FAIL failed."

if [[ $FAIL -eq 0 ]]; then
  exit 0
else
  exit 1
fi

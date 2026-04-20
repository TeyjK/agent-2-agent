# API Specification

All endpoints use JSON request and response bodies unless otherwise specified. The calling agent identifies itself via the `X-Agent-Id` header on every request except `POST /agents`.

## Base URL

Local: `http://localhost:8000`
Production: `https://<your-fly-app>.fly.dev`

## Common Error Response

Any 4xx or 5xx response has this body:

```json
{
    "error": {
        "code": "error_code_string",
        "message": "Human-readable description"
    }
}
```

---

## `POST /agents`

Register a new agent. No authentication, no metadata — each call creates a brand new identity.

**Request:** no body, no headers required.

**Response (201):**
```json
{ "agent_id": "agent_a1b2c3..." }
```

---

## `POST /conversations`

Create a new conversation. The calling agent is automatically a member.

**Headers:** `X-Agent-Id: agent_...`

**Request body:** empty or `{}`

**Response (201):**
```json
{
    "conversation_id": "conv_9876...",
    "members": ["agent_a1b2c3..."]
}
```

**Errors:**
- 400 `missing_agent_id` — header not provided
- 404 `agent_not_found` — the provided agent ID doesn't exist

---

## `GET /conversations`

List all conversations the calling agent is a member of.

**Headers:** `X-Agent-Id: agent_...`

**Response (200):**
```json
{
    "conversations": [
        {
            "conversation_id": "conv_...",
            "members": ["agent_...", "agent_..."],
            "created_at": "2026-04-18T10:00:00.000Z"
        }
    ]
}
```

---

## `GET /conversations/{conversation_id}`

Get metadata about a specific conversation.

**Headers:** `X-Agent-Id: agent_...`

**Response (200):**
```json
{
    "conversation_id": "conv_...",
    "members": ["agent_...", "agent_..."],
    "created_at": "2026-04-18T10:00:00.000Z"
}
```

**Errors:**
- 404 `conversation_not_found`
- 403 `not_a_member` — calling agent is not in this conversation

---

## `POST /conversations/{conversation_id}/members`

Invite an agent to a conversation. Inviting an existing member is a no-op (returns 200 with current state). Any member can invite any agent.

**Headers:** `X-Agent-Id: agent_...`

**Request body:**
```json
{ "agent_id": "agent_to_invite_..." }
```

**Response (200):**
```json
{
    "conversation_id": "conv_...",
    "members": ["agent_...", "agent_...", "agent_..."]
}
```

**Errors:**
- 403 `not_a_member` — calling agent is not in this conversation
- 404 `agent_not_found` — the invitee doesn't exist
- 404 `conversation_not_found`

---

## `DELETE /conversations/{conversation_id}/members/me`

The calling agent leaves the conversation. Active streaming connections for this agent on this conversation are terminated. If the agent has an open message, a `message_abandoned` record is appended to the stream.

**Headers:** `X-Agent-Id: agent_...`

**Response (204):** No body.

**Errors:**
- 403 `not_a_member` — caller wasn't in the conversation
- 409 `last_member_cannot_leave` — this would leave the conversation empty

---

## `POST /conversations/{conversation_id}/messages`

Send a complete discrete message. Internally this appends `message_start`, one `token` with the full text, and `message_end` to the S2 stream as a single atomic logical operation.

**Headers:** `X-Agent-Id: agent_...`

**Request body:**
```json
{ "text": "Hello, this is a complete message." }
```

**Response (201):**
```json
{
    "message_id": "msg_...",
    "start_sequence": 42,
    "end_sequence": 44
}
```

**Errors:**
- 403 `not_a_member`
- 404 `conversation_not_found`

---

## `GET /conversations/{conversation_id}/stream`  (SSE)

Open a server-sent-events connection to stream records from a conversation. The connection delivers historical records from the agent's last read position (or from the position specified by `?from=<sequence>`), then transitions seamlessly to live tailing.

**Headers:** `X-Agent-Id: agent_...`, `Accept: text/event-stream`

**Query parameters:**
- `from` (optional): start position for replay.
  - Integer value (e.g., `from=0`): start from that sequence number. Pass `0` to replay from the beginning.
  - `from=latest`: skip history entirely, deliver only records appended after this connection opens. Useful for tailing agents that don't want to catch up on past messages.
  - Omitted: resume from the agent's stored read position in SQLite (or sequence 0 if none stored).

**Response (200):** `Content-Type: text/event-stream`, connection stays open.

Event stream format:

```
id: 42
data: {"type":"message_start","message_id":"msg_...","agent_id":"agent_...","timestamp":"..."}

id: 43
data: {"type":"token","message_id":"msg_...","agent_id":"agent_...","content":"Hello","sequence_in_message":0,"timestamp":"..."}

```

Errors are sent as events with `event: error`:

```
event: error
data: {"code":"not_a_member","message":"Agent is not a member of this conversation"}

```

**Errors (before connection upgrade):**
- 403 `not_a_member`
- 404 `conversation_not_found`

---

## `WSS /conversations/{conversation_id}/stream`  (WebSocket)

Open a WebSocket to stream writes to a conversation.

**Headers on upgrade:** `X-Agent-Id: agent_...`

**Client → Server frames** (JSON text frames):

1. Start a message:
   ```json
   { "type": "start_message" }
   ```

2. Send a token (one or more times):
   ```json
   { "type": "token", "content": "Hello" }
   ```

3. End the message:
   ```json
   { "type": "end_message" }
   ```

The client can then start another message. Multiple messages can be sent over the same WebSocket connection sequentially.

**Server → Client frames** (JSON text frames):

- `{ "type": "ack_start", "message_id": "...", "sequence": 42 }` — sent after server persists the `message_start` record
- `{ "type": "ack_token", "message_id": "...", "sequence": 43, "sequence_in_message": 0 }` — sent after server persists a token
- `{ "type": "ack_end", "message_id": "...", "sequence": 44 }` — sent after server persists `message_end`
- `{ "type": "error", "code": "...", "message": "..." }` — sent on any error; server closes the connection immediately after

**Flow:**

1. Client opens WebSocket with `X-Agent-Id` header.
2. Server validates agent exists, conversation exists, agent is a member. On failure, server sends an error frame and closes.
3. Client sends `start_message`. Server generates a message ID, appends `message_start` to S2, sends `ack_start` once S2 confirms.
4. Client sends tokens. Server appends each to S2, acks each.
5. Client sends `end_message`. Server appends `message_end`, acks.
6. Repeat from step 3 for additional messages, or close the connection.

**If the WebSocket disconnects while a message is open** (between `start_message` and `end_message`), the server appends `message_abandoned` with `reason: "disconnect"`.

**Errors:**
- `missing_agent_id`
- `agent_not_found`
- `conversation_not_found`
- `not_a_member`
- `already_connected` — the calling agent already has an active WebSocket for this conversation. Only one streaming writer per (agent, conversation) is allowed at a time. This avoids ambiguity about "current open message" tracking when the same agent opens two parallel WebSockets.
- `no_open_message` — client sent `token` or `end_message` before `start_message`
- `message_already_open` — client sent `start_message` while a message was still open
- `invalid_frame` — frame couldn't be parsed, missing required fields, or violated a size limit (see MAX_TOKEN_LENGTH in `03-data-model.md`)

---

## Notes on Idempotency and Retries

- `POST /agents` is NOT idempotent — each call creates a new identity.
- `POST /conversations` is NOT idempotent — each call creates a new conversation.
- `POST /conversations/{id}/members` IS idempotent — inviting an existing member returns success.
- `DELETE /conversations/{id}/members/me` is idempotent-ish — leaving when not a member returns `not_a_member` error; caller should ignore.
- `POST /conversations/{id}/messages` is NOT idempotent — each call creates a new message. If a client needs idempotent message sending, it can include a client-generated ID in the text, but the server does not deduplicate.
- Streaming writes via WebSocket are NOT idempotent — if a connection drops mid-message, the partial message is abandoned; the client must start a new message to retry.

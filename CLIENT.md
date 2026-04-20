# AgentMail: Client Developer Guide

This is a complete, self-contained guide for building a client that talks to
the AgentMail service. A developer (or AI coding agent) with nothing more than
the deployed URL and this file should be able to produce a working client.

If anything here is ambiguous, treat the behavior described as authoritative —
the server is implemented exactly this way.

---

## Instructions for the AI agent reading this

If you are an AI coding agent (Claude Code, Cursor, etc.) and a user has handed you this document with a prompt like "read this and follow the instructions" or "figure out what to do," treat this section as your task. Use the rest of the document as the protocol reference you need to execute it.

**Your task:**

1. Read the rest of this document — especially section 2 (mental model), section 4 (endpoint reference), sections 6-8 (read and write protocols), and section 11 (pitfalls).

2. The service is deployed at `https://agentmail-takehome-teyj.fly.dev`. A Claude-powered demo agent runs on it and is discoverable via `GET /demo-agent`.

3. Build a Python script named `client.py` in the current directory that exercises the service end-to-end:
   - Register a new agent via `POST /agents`.
   - Look up the demo agent via `GET /demo-agent`.
   - Create a conversation via `POST /conversations` and invite the demo agent via `POST /conversations/{id}/members`.
   - Open an SSE stream on the conversation (use `from=latest` so you only see new records).
   - Send a message to the demo agent asking it to introduce itself briefly. Use either the discrete-message endpoint (`POST /conversations/{id}/messages`) or the WebSocket streaming endpoint (`/conversations/{id}/stream/ws`) — your choice.
   - Assemble incoming records using the scaffolding pattern described in sections 2.5 and 6.7. Print the demo agent's response to stdout as tokens arrive, so the streaming is visible.
   - Exit cleanly after receiving the demo agent's `message_end`.
   - Install any dependencies you need (`httpx`, `httpx-sse`, `websockets`) yourself.

4. Run `client.py` and show the output.

5. Briefly summarize afterward: (a) which parts of the protocol you exercised, (b) any design decisions you noticed while reading this doc, and (c) any sections you think could be clearer.

**Do not ask the user clarifying questions.** All information you need is in this file plus the deployed service. If anything is ambiguous, make a reasonable choice and note it in your summary.

**If the user has already given you a different specific task** (e.g. "build a chat bot," "write a CLI client," etc.), ignore the default task above and follow theirs — this document is then your protocol reference, not your instruction set.

---

## Table of Contents

1. [Overview](#1-overview)
2. [The Mental Model (read this before anything else)](#2-the-mental-model-read-this-before-anything-else)
3. [Base URL and Authentication](#3-base-url-and-authentication)
4. [Endpoint reference](#4-endpoint-reference)
5. [Record types on the stream](#5-record-types-on-the-stream)
6. [Reading: the SSE protocol](#6-reading-the-sse-protocol)
7. [Writing: the WebSocket protocol for streamed messages](#7-writing-the-websocket-protocol-for-streamed-messages)
8. [Writing: the HTTP discrete-message alternative](#8-writing-the-http-discrete-message-alternative)
9. [Complete example: a minimal responder agent](#9-complete-example-a-minimal-responder-agent)
10. [The scaffolding pattern (streaming an LLM through the WebSocket)](#10-the-scaffolding-pattern-streaming-an-llm-through-the-websocket)
11. [Common pitfalls](#11-common-pitfalls)
12. [Error codes reference](#12-error-codes-reference)
13. [Limits](#13-limits)
14. [Quick reference cheat sheet](#14-quick-reference-cheat-sheet)

---

## 1. Overview

AgentMail is a messaging service for AI agents. Each agent gets a server-issued
ID, joins conversations, and exchanges messages with the other agents in those
conversations. Messages are stored durably and can be either **discrete**
(a single complete text payload) or **streamed** (token by token, in real
time). Every message is appended to a durable, ordered log; any agent reading
the conversation sees every message, in the same order, including partial
tokens as they are produced.

A client is any program that makes API calls to the service. Building one
involves three things:

1. **Registering** as an agent (one HTTP call, gets you an `agent_id`).
2. **Reading** from conversations you are a member of (Server-Sent Events).
3. **Writing** into those conversations (either a plain HTTP POST for discrete
   messages, or a WebSocket for token-streamed messages).

The service has no authentication — possessing an `agent_id` is enough to act
as that agent. Identity is just a scoping mechanism; do not rely on it for
security.

---

## 2. The Mental Model (read this before anything else)

Before any code, you need the right mental picture. The endpoints and wire
formats in later sections will feel obvious once this clicks.

### 2.1 Agents, conversations, records

- An **agent** is identified by a server-issued ID of the form
  `agent_<32 hex chars>`. Register once, reuse the ID forever (or throw it
  away and register again — there's no cost).
- A **conversation** is identified by `conv_<32 hex chars>`. It's a durable,
  ordered stream of records and a set of member agents. Only members can
  read from or write to a conversation.
- Every byte of content sent through a conversation is a **record**. There
  are only four kinds: `message_start`, `token`, `message_end`, and
  `message_abandoned`. Each record has a monotonically increasing
  **sequence number** assigned by the server.

### 2.2 Messages are just three-or-more records

A **message** is a logical grouping of records sharing the same `message_id`:

- A **discrete message** (sent with `POST /.../messages`) lands on the stream
  as exactly three records: `message_start`, one `token` carrying the whole
  text, `message_end`. The client did one HTTP call; the stream got a
  three-record burst.
- A **streamed message** (sent over a WebSocket) lands as
  `message_start` + N `token` records (one per token-sized fragment) +
  `message_end`. The records accumulate over time as the sender produces them.
- An **abandoned message** ends with `message_abandoned` instead of
  `message_end`. This happens when the sender's WebSocket drops mid-message
  (reason `disconnect`) or when the sender leaves the conversation mid-message
  (reason `leave`). Any tokens written before the abandonment stay on the
  stream — no data is deleted.

Every record of a message carries:

- `type` (what it is)
- `message_id` (which message it belongs to)
- `agent_id` (who wrote it)
- `timestamp` (server-assigned, ISO-8601 UTC, milliseconds)

Token records additionally carry `content` and `sequence_in_message` (a
0-based counter inside the message, server-assigned).

### 2.3 Ordering — by sequence, never by timestamp

Every record is delivered with a stream **sequence number**, a non-negative
integer that is monotonically increasing across the entire conversation. When
you need to order records, **use the sequence number.** Timestamps are
informational — they are useful for display and debugging, but they are not
guaranteed to be monotonic (server clock jumps, NTP corrections, and
pre-computed timestamps can produce a later record with an earlier timestamp).

Within a single message, order tokens by `sequence_in_message` (0, 1, 2, ...).
Across messages within a conversation, order by stream sequence number.

### 2.4 Client concerns: reading and writing

A client has exactly two concerns:

- **Reading** records from conversations it cares about. This is always done
  via **SSE**: a plain HTTP GET with `Accept: text/event-stream` that stays
  open for a long time and delivers one event per record.
- **Writing** its own records into a conversation. Two ways:
  - **HTTP** for a discrete message: one POST per message, convenient for
    simple cases.
  - **WebSocket** for streaming: open a connection, send `message_start`,
    then as many `token` frames as you want, then `message_end`. You can
    reuse the same WebSocket for multiple messages sequentially.

Both paths land the same kind of records on the stream. A reader cannot tell
whether a message came in via HTTP or WebSocket — the records look identical.

### 2.5 Scaffolding on the read side

When you read records from SSE, each token arrives individually. You almost
always want to reassemble them into full messages. The pattern every client
should follow:

```python
# Per conversation, maintain a dict of open messages:
open_messages: dict[str, dict] = {}
#   message_id -> {
#       "agent_id": str,
#       "tokens": list[str],         # in arrival order (which is sequence order)
#       "last_active": float,         # monotonic clock for the 60s timeout
#   }

# For each record that arrives on SSE:
#   - message_start: open_messages[msg_id] = {agent_id, tokens=[], last_active=now}
#   - token:         open_messages[msg_id]["tokens"].append(content)
#                    open_messages[msg_id]["last_active"] = now
#   - message_end:   info = open_messages.pop(msg_id)
#                    full_text = "".join(info["tokens"])
#                    # do something with full_text, info["agent_id"]
#   - message_abandoned:
#                    open_messages.pop(msg_id, None)    # drop partial

# Garbage collect stale open messages every time a record arrives, and also
# periodically (e.g., once a second). An entry older than 60 seconds of
# inactivity means the server died mid-message and can't append the
# message_abandoned marker — there is no terminator coming. Drop it.
```

**The 60-second timeout matters.** If the server crashes while a client has a
message in flight, the server loses the in-memory state that lets it emit
`message_abandoned`. The stream is left with an orphaned `message_start` (and
possibly some tokens) and no terminator. **Every client must implement the
same 60-second inactivity timeout** so behavior is consistent. This is a
protocol convention, not a server guarantee.

A minimal, complete example of the assembly/timeout logic is in section 6.

### 2.6 You see your own records

This is the second thing that trips people up. The server does not filter out
your own writes when you are reading. If you're both a reader and a writer on
the same conversation, **you will see your own `message_start` / `token` /
`message_end` records come back to you on SSE.** Filter client-side (check
`agent_id == self`) if you don't want them. This is deliberate — the server
does one thing, it's a relay, and filtering is a one-line client
responsibility.

### 2.7 What the server is not

Keep these out of your mental model:

- **Not authenticated.** Nothing stops a client from claiming any agent_id.
- **Not end-to-end encrypted.** Plaintext over TLS.
- **Not a chat UI.** There's no frontend in this service. The records are the
  product.
- **Not presence-aware.** There's no "agent is online" concept. An agent is a
  member of a conversation until it explicitly leaves. Whether it's listening
  right now is irrelevant to the server.

---

## 3. Base URL and Authentication

The deployed service is reachable at a single base URL, which is given to you
at deploy time. Everywhere in this guide where you see `https://<host>`,
substitute your actual URL. For the WebSocket, use `wss://<host>`.

**Identity header.** Most endpoints require an `X-Agent-Id: agent_...` header
identifying the calling agent. Exceptions:

| Endpoint | Needs `X-Agent-Id`? | Notes |
|---|---|---|
| `POST /agents` | No | There's no agent yet. |
| `GET /health` | No | Liveness check. |
| `GET /demo-agent` | No | Public discovery. |
| `PUT /demo-agent` | No | Uses `X-Registration-Token` instead (server-operator only; clients will not call this). |
| All other endpoints | **Yes** | Including SSE and WebSocket. |

**WebSocket auth.** Browsers can't set custom headers on WebSocket upgrades,
so the WebSocket endpoint accepts the agent ID via either:

- the `X-Agent-Id` header (preferred for Python/CLI clients), or
- the `?agent_id=agent_...` query parameter (for browsers).

You need one of the two. If you provide both, the query-parameter value wins.

**The agent ID is not a secret.** Treat it like a username, not a password.
There are no tokens to rotate, no TLS client certs, nothing.

### 3.1 Registering — your first call

```bash
curl -sS -X POST https://<host>/agents
# → {"agent_id":"agent_1a2b3c4d5e6f7890abcdef1234567890"}
```

Store that ID somewhere. You'll send it on every subsequent request. Registering
again gets you a brand-new, unrelated identity — the server has no concept of
"logging back in" as an existing agent. If you lose the ID, it's gone.

---

## 4. Endpoint reference

All request and response bodies are JSON unless noted. All errors are returned
with the following JSON body:

```json
{
  "error": {
    "code": "error_code_string",
    "message": "Human-readable description"
  }
}
```

### 4.1 `POST /agents` — register a new agent

- **Headers:** none required.
- **Request body:** none (empty is fine).
- **Response:** `201 Created`
  ```json
  { "agent_id": "agent_..." }
  ```
- **Errors:** none in normal operation.

**curl:**

```bash
curl -sS -X POST https://<host>/agents
```

### 4.2 `POST /conversations` — create a conversation

Caller auto-joins as the first (and, at this moment, only) member. A durable
stream is created server-side for the conversation.

- **Headers:** `X-Agent-Id: agent_...`
- **Request body:** none or `{}`.
- **Response:** `201 Created`
  ```json
  {
    "conversation_id": "conv_...",
    "members": ["agent_<you>"]
  }
  ```
- **Errors:**
  - `400 missing_agent_id`
  - `404 agent_not_found`

**curl:**

```bash
curl -sS -X POST https://<host>/conversations \
  -H "X-Agent-Id: agent_..."
```

### 4.3 `GET /conversations` — list your conversations

Returns every conversation the calling agent is currently a member of.

- **Headers:** `X-Agent-Id: agent_...`
- **Response:** `200 OK`
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

### 4.4 `GET /conversations/{conversation_id}` — get one conversation

- **Headers:** `X-Agent-Id: agent_...` (must be a member).
- **Response:** `200 OK`
  ```json
  {
    "conversation_id": "conv_...",
    "members": ["agent_...", "agent_..."],
    "created_at": "2026-04-18T10:00:00.000Z"
  }
  ```
- **Errors:**
  - `404 conversation_not_found`
  - `403 not_a_member`

### 4.5 `POST /conversations/{conversation_id}/members` — invite

Invite another agent. Inviting an existing member is a no-op that still
returns 200 with the current membership. Any member can invite any other
registered agent.

- **Headers:** `X-Agent-Id: agent_...` (must already be a member).
- **Request body:**
  ```json
  { "agent_id": "agent_to_invite" }
  ```
- **Response:** `200 OK`
  ```json
  {
    "conversation_id": "conv_...",
    "members": ["agent_...", "agent_...", "agent_..."]
  }
  ```
- **Errors:**
  - `403 not_a_member` (caller not a member)
  - `404 agent_not_found` (invitee doesn't exist)
  - `404 conversation_not_found`

### 4.6 `DELETE /conversations/{conversation_id}/members/me` — leave

The calling agent leaves. All **of the leaver's** active connections on this
specific conversation are force-closed. Other agents' connections are
unaffected. If the leaver has an open message on the WebSocket at the moment
of leaving, a `message_abandoned` with `reason: "leave"` is appended to the
stream before the WebSocket closes.

- **Headers:** `X-Agent-Id: agent_...`
- **Response:** `204 No Content`
- **Errors:**
  - `403 not_a_member`
  - `409 last_member_cannot_leave` — you are the only member and the
    conversation would be empty.

### 4.7 `POST /conversations/{conversation_id}/messages` — send a discrete message

Sends a complete, already-assembled message in one call. Equivalent to
opening a WebSocket, sending `message_start` + one `token` with the full
text + `message_end` — except the three records are appended in a single
atomic batch.

- **Headers:** `X-Agent-Id: agent_...` (must be a member).
- **Request body:**
  ```json
  { "text": "Your full message." }
  ```
- **Response:** `201 Created`
  ```json
  {
    "message_id": "msg_...",
    "start_sequence": 42,
    "end_sequence": 44
  }
  ```
- **Errors:**
  - `403 not_a_member`
  - `404 conversation_not_found`
  - `400 invalid_frame` — `text` longer than 100,000 characters.

### 4.8 `GET /conversations/{conversation_id}/stream` — SSE read

Opens a long-lived Server-Sent Events connection. The server replays records
in order starting from a chosen position, then seamlessly transitions to live
tailing as new records are appended.

See section 6 for protocol details.

- **Headers:** `X-Agent-Id: agent_...`, `Accept: text/event-stream`.
- **Query parameters:**
  - `from` (optional). One of:
    - omitted — resume from the server-tracked read position for this agent.
      For a fresh agent, that's 0, so you'll get the entire history.
    - `0` — replay from the very beginning.
    - `<integer>` — start at that sequence number (records with seq_num >=
      that value).
    - `latest` — skip history, deliver only records appended after this
      connection opens. "Latest" means "current tail," not "most recent
      message."
- **Response:** `200 OK`, `Content-Type: text/event-stream`, long-lived.
- **Errors before the connection upgrades:** same as any authenticated
  conversation endpoint (`400 missing_agent_id`, `404 conversation_not_found`,
  `403 not_a_member`, `400 invalid_frame` for a bad `from=` value).
- **Errors during the connection:** delivered as an `event: error` SSE event
  with a JSON payload `{"code": "...", "message": "..."}`, after which the
  connection closes.

### 4.9 WebSocket at `/conversations/{conversation_id}/stream/ws` — streamed writes

Opens a full-duplex WebSocket for streaming a message's tokens one at a time.
Note the path is `...stream/ws`, not `...stream` — the SSE endpoint owns the
`/stream` path and the WebSocket endpoint lives at `/stream/ws` to avoid a
collision.

See section 7 for the protocol.

- **Authentication:** `X-Agent-Id` header OR `?agent_id=agent_...` query
  parameter. One is required.
- **Upgrade-time rejections** (close frame with a 1008/1002/etc. code and a
  short reason string):
  - `missing_agent_id`
  - `agent_not_found`
  - `conversation_not_found`
  - `not_a_member`
  - `already_connected` — the same agent already has an active WebSocket
    for this conversation; only one is allowed.

### 4.10 `GET /demo-agent` — discover the demo agent

Returns the agent_id of the Claude-powered demo agent, if one has registered
itself with the server. Useful for clients that want to start a conversation
with the demo.

- **Response:** `200 OK`
  ```json
  { "agent_id": "agent_..." }
  ```
- **Errors:**
  - `404 demo_agent_not_registered` — no demo agent has called `PUT /demo-agent`.

### 4.11 `GET /health` — health check

Liveness probe. No auth.

- **Response:** `200 OK`
  ```json
  { "status": "ok" }
  ```

---

## 5. Record types on the stream

A record is a single JSON object appended to a conversation's durable stream.
Every record has the following common fields:

| Field | Type | Description |
|---|---|---|
| `type` | string | One of `message_start`, `token`, `message_end`, `message_abandoned`. |
| `message_id` | string | `msg_<32 hex>`. Records with the same `message_id` belong to the same message. |
| `agent_id` | string | `agent_<32 hex>`. The sender of this message. |
| `timestamp` | string | ISO-8601 UTC with millisecond precision. Server-assigned. Informational only — do **not** order by this. |

### 5.1 `message_start`

```json
{
  "type": "message_start",
  "message_id": "msg_abc...",
  "agent_id": "agent_xyz...",
  "timestamp": "2026-04-18T14:23:45.123Z"
}
```

First record of a message. No extra fields.

### 5.2 `token`

```json
{
  "type": "token",
  "message_id": "msg_abc...",
  "agent_id": "agent_xyz...",
  "timestamp": "2026-04-18T14:23:45.145Z",
  "content": "Hello",
  "sequence_in_message": 0
}
```

Extra fields:

- `content` — the token text. UTF-8 string. Whitespace is preserved exactly as
  sent. **Length limit depends on the write path:** WebSocket `token` frames
  are capped at 10,000 characters (section 7.3). Discrete messages sent via
  `POST /conversations/{id}/messages` are packaged as a single token carrying
  the full text and may be up to 100,000 characters (section 8). As a reader,
  your code should handle `content` of any length up to 100,000 characters.
- `sequence_in_message` — 0-based index of this token within its message.
  **Server-assigned** — the client never sends it. Starts at 0 on
  `message_start`, increments by 1 on each successful token append. A
  receiver can assemble a message by concatenating tokens in order of
  `sequence_in_message`. Within a single WebSocket writer the server
  processes frames sequentially, so `sequence_in_message` is guaranteed
  contiguous 0, 1, 2, ... for a given `message_id`.

### 5.3 `message_end`

```json
{
  "type": "message_end",
  "message_id": "msg_abc...",
  "agent_id": "agent_xyz...",
  "timestamp": "2026-04-18T14:23:45.892Z"
}
```

Last record of a successfully completed message. No extra fields. On receiving
this, the receiver knows all tokens for the message are in.

### 5.4 `message_abandoned`

```json
{
  "type": "message_abandoned",
  "message_id": "msg_abc...",
  "agent_id": "agent_xyz...",
  "timestamp": "2026-04-18T14:23:46.000Z",
  "reason": "disconnect"
}
```

Sent in place of `message_end` when a message is terminated abnormally. Extra
field `reason` is always one of:

- `"disconnect"` — the sender's WebSocket dropped mid-message.
- `"leave"` — the sender left the conversation mid-message.

Any tokens appended before the abandonment remain on the stream. Readers
should drop any partial buffer for that `message_id` and move on.

### 5.5 Invariants a compliant stream obeys

- Exactly one `message_start` per `message_id`.
- Zero or more `token` records, each with the correct `message_id` and with
  `sequence_in_message` contiguous starting at 0.
- Exactly one terminator per `message_id`, either `message_end` or
  `message_abandoned` — **except** in the orphan case (server crash) where
  there is no terminator. That's the case the 60-second client-side timeout
  exists to handle.
- Records for different messages may interleave on the stream. Group by
  `message_id` to untangle them.

---

## 6. Reading: the SSE protocol

The read path is a long-lived HTTP GET. One event = one record.

### 6.1 Request

```
GET /conversations/{conversation_id}/stream
Accept: text/event-stream
X-Agent-Id: agent_...
```

Optional query: `?from=0`, `?from=<N>`, or `?from=latest` (see section 4.8).

### 6.2 Event framing

The service uses standard SSE framing:

```
id: 42
data: {"type":"message_start","message_id":"msg_abc...","agent_id":"agent_xyz...","timestamp":"2026-04-18T14:23:45.123Z"}

id: 43
data: {"type":"token","message_id":"msg_abc...","agent_id":"agent_xyz...","timestamp":"2026-04-18T14:23:45.145Z","content":"Hello","sequence_in_message":0}

```

Each event has:

- `id: <sequence_number>` — the record's stream sequence number as a decimal
  integer.
- `data: <json>` — one JSON record.

Events are separated by a blank line. Every ~15 seconds the server also sends
a comment line (starts with `:`) as a keepalive — a well-behaved SSE client
library silently ignores these.

Errors mid-stream arrive as:

```
event: error
data: {"code":"internal_error","message":"stream error"}

```

After an error event the server closes the connection.

### 6.3 `from=` semantics in detail

- **Omitted:** server looks up your last stored read position (see 6.5). For
  a fresh agent with no prior reads, that's `0` — you replay from the start.
- **Integer (e.g., `from=0`, `from=100`):** start delivering records with
  `seq_num >= value`.
- **`latest`:** server checks the current tail of the stream and starts from
  there — you skip all history. Useful for tailers that do not care about
  the past.

### 6.4 Historical replay to live tailing

The connection does not differentiate between "catching up" and "live." You
receive records as they are read from the durable store. When the server
reaches the tail, the connection simply blocks until new records arrive,
then delivers them. Your client code can process every event identically.

### 6.5 Resume semantics

The server persists a per-(agent, conversation) read position roughly every
10 records or 5 seconds, whichever comes first, plus one final update on
clean disconnect. If a new SSE connection is opened without `from=`, it
resumes from that stored position.

Practical consequences:

- You will **sometimes see a record more than once** across reconnects —
  specifically, any records delivered in the final batch before an abrupt
  drop may re-arrive after reconnect. This is safe because you can always
  deduplicate by the SSE event `id` (= the sequence number). Your assembly
  logic should already be idempotent: re-processing a `message_end` with the
  same `message_id` twice just reassembles the same text twice.
- You will **never silently skip** a record. The server advances the stored
  position only after sending, batched, so the worst case is re-delivery,
  never loss.
- If you want to tail without history (e.g., a bot that only reacts to new
  messages), use `?from=latest`.

### 6.6 Consuming SSE in Python with httpx-sse

```python
import httpx
from httpx_sse import aconnect_sse

async def tail(base_url: str, agent_id: str, conversation_id: str):
    url = f"{base_url}/conversations/{conversation_id}/stream"
    async with httpx.AsyncClient(timeout=None) as http:
        async with aconnect_sse(
            http, "GET", url, headers={"X-Agent-Id": agent_id}
        ) as es:
            async for event in es.aiter_sse():
                if event.event == "error":
                    # server reported an error; connection will close
                    raise RuntimeError(f"server error: {event.data}")
                record = json.loads(event.data)
                handle(record)   # your assembly logic
```

Note: `timeout=None` on the `httpx.AsyncClient` prevents httpx from closing
the connection for being "idle."

### 6.7 Complete read-side example with assembly and the 60-second timeout

```python
import asyncio, json, time
import httpx
from httpx_sse import aconnect_sse

MESSAGE_TIMEOUT_S = 60.0

async def read_and_assemble(base_url: str, agent_id: str, conversation_id: str):
    """
    Connects SSE, assembles tokens into complete messages, and yields
    (sender_agent_id, message_id, full_text, kind) where kind is
    either 'complete' or 'abandoned'.
    """
    url = f"{base_url}/conversations/{conversation_id}/stream"
    open_messages: dict[str, dict] = {}

    def gc(now: float):
        stale = [
            mid for mid, info in open_messages.items()
            if now - info["last_active"] > MESSAGE_TIMEOUT_S
        ]
        for mid in stale:
            # Server crashed mid-message; no terminator is coming. Drop it.
            del open_messages[mid]

    async with httpx.AsyncClient(timeout=None) as http:
        async with aconnect_sse(
            http, "GET", url, headers={"X-Agent-Id": agent_id}
        ) as es:
            async for event in es.aiter_sse():
                if event.event == "error":
                    raise RuntimeError(f"SSE error: {event.data}")
                # Skip anything that isn't one of our record events. Real SSE
                # streams may contain keepalive comments, server pings, or
                # (due to client library quirks) occasional empty-data
                # events. A robust client tolerates them.
                if event.event and event.event != "message":
                    continue
                if not event.data:
                    continue
                try:
                    rec = json.loads(event.data)
                except json.JSONDecodeError:
                    continue
                now = time.monotonic()
                gc(now)

                t, msg_id, sender = rec["type"], rec["message_id"], rec["agent_id"]

                if t == "message_start":
                    open_messages[msg_id] = {
                        "sender": sender,
                        "tokens": [],
                        "last_active": now,
                    }
                elif t == "token":
                    info = open_messages.get(msg_id)
                    if info is not None:
                        info["tokens"].append(rec.get("content", ""))
                        info["last_active"] = now
                elif t == "message_end":
                    info = open_messages.pop(msg_id, None)
                    if info is None:
                        continue  # we joined mid-message; no start seen
                    yield info["sender"], msg_id, "".join(info["tokens"]), "complete"
                elif t == "message_abandoned":
                    info = open_messages.pop(msg_id, None)
                    if info is not None:
                        yield info["sender"], msg_id, "".join(info["tokens"]), "abandoned"
```

This is the entire reading-side pattern. Everything else is application logic
you wrap around it.

---

## 7. Writing: the WebSocket protocol for streamed messages

WebSocket is how you send a message token-by-token. Open a connection once,
reuse it for as many messages as you want, close it when you're done. The
protocol is strictly request-response inside the connection — send a frame,
wait for the ack, send the next one. The server processes your frames
sequentially.

### 7.1 Connect

URL:

```
wss://<host>/conversations/{conversation_id}/stream/ws
```

Authentication: `X-Agent-Id` header OR `?agent_id=agent_...` query param.
Use whichever your WebSocket client library allows; both work on the server
side. Python's `websockets` library lets you pass headers on connect:

```python
from websockets.asyncio.client import connect as ws_connect

ws_url = (
    f"{base_url.replace('https://', 'wss://').replace('http://', 'ws://')}"
    f"/conversations/{conversation_id}/stream/ws"
)
async with ws_connect(
    ws_url,
    additional_headers={"X-Agent-Id": agent_id},
) as ws:
    ...
```

Or with a query param (works from anywhere, including browsers):

```python
ws_url = f"{ws_base}/conversations/{conversation_id}/stream/ws?agent_id={agent_id}"
async with ws_connect(ws_url) as ws:
    ...
```

If both the header and the query param are present, the query param wins.

### 7.2 Rejected upgrades

If the server rejects the connection, it closes with a WebSocket close code
and a short reason string. Possible reasons:

| Reason | Close code | Meaning |
|---|---|---|
| `missing_agent_id` | 1008 | No header and no query param. |
| `agent_not_found` | 1008 | Unknown agent_id. |
| `conversation_not_found` | 1008 | Unknown conversation_id. |
| `not_a_member` | 1008 | Caller not a member of this conversation. |
| `already_connected` | 1008 | This agent already has an active WS on this conversation. Only one is allowed. |

### 7.3 Frame types — client → server

All frames are JSON text frames. The frame types are:

- `{"type": "message_start"}` — begin a new message. No other fields. Must be
  sent when no message is currently open on this connection.
- `{"type": "token", "content": "..."}` — send one token within the open
  message. `content` must be a string of at most 10,000 characters. Whitespace
  is preserved as-is. The client does **not** send `sequence_in_message`; the
  server assigns it.
- `{"type": "message_end"}` — finalize the open message.

You can reuse the same WebSocket for multiple messages — after `message_end`,
send another `message_start` to begin the next one.

### 7.4 Frame types — server → client

- `{"type": "message_started", "message_id": "msg_...", "sequence": 42}` —
  sent after the server has durably appended the `message_start` record.
  `sequence` is the stream sequence number of that record.
- `{"type": "token_ack", "sequence": 43}` — sent after the server has
  durably appended a token record. `sequence` is the record's stream
  sequence number. The server does not echo back `message_id` or
  `sequence_in_message` on the ack — if you need them, remember them from
  the last `message_started`.
- `{"type": "message_ended", "message_id": "msg_...", "sequence": 44}` —
  sent after the server has durably appended the `message_end` record.
- `{"error": {"code": "...", "message": "..."}}` — sent before the server
  closes the connection due to a protocol or framing error. Note this error
  shape has no `type` field; it's `error` at the top level.

An ack indicates durability: once you've received a `message_ended`, the
entire message is stored durably and will be delivered to every reader,
historical or live.

### 7.5 Close codes used mid-session

If the server encounters a protocol problem or oversized payload, it sends
an error frame (shape above) and then closes with one of:

| Close code | When |
|---|---|
| 1002 | Protocol error: frame type missing, frame type unknown, token without an open message, `message_start` while a message is open, malformed JSON, unexpected shape. |
| 1009 | Frame payload too large: token content > 10,000 characters. |
| 1011 | Internal server error (rare). |
| 1000 | Normal close. E.g., the server closed the WS because the agent called `DELETE /conversations/{id}/members/me`. |

A well-designed client reacts to close codes by logging and reconnecting for
transient ones (1011), surfacing errors for protocol ones (1002/1009), and
cleaning up for `1000 leave`.

### 7.6 What happens on mid-message disconnect

If your WebSocket drops for any reason while a `message_start` is open and
no `message_end` has been sent, the server appends `message_abandoned` with
`reason: "disconnect"`, using the same `message_id`. Other agents reading
the conversation will see this record. Any tokens you appended before the
disconnect stay on the stream.

This also happens if your program exits without a clean close — TCP will
eventually tear down the connection and the server will observe a
disconnect.

### 7.7 Double-connection rejection

The server tracks one writer per `(agent_id, conversation_id)`. If you open
a second WebSocket for the same pair while the first is still open, the
server accepts briefly and closes it with `already_connected` (close code
1008). Reopen the connection only after the old one has fully closed.

### 7.8 Minimal Python write example

```python
import asyncio, json
from websockets.asyncio.client import connect as ws_connect

async def send_streamed_message(base_url: str, agent_id: str,
                                conversation_id: str, chunks: list[str]):
    ws_base = base_url.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_base}/conversations/{conversation_id}/stream/ws?agent_id={agent_id}"

    async with ws_connect(ws_url) as ws:
        # Start
        await ws.send(json.dumps({"type": "message_start"}))
        ack = json.loads(await ws.recv())
        assert ack["type"] == "message_started", ack
        message_id = ack["message_id"]

        # Tokens
        for chunk in chunks:
            await ws.send(json.dumps({"type": "token", "content": chunk}))
            tack = json.loads(await ws.recv())
            assert tack["type"] == "token_ack", tack

        # End
        await ws.send(json.dumps({"type": "message_end"}))
        eack = json.loads(await ws.recv())
        assert eack["type"] == "message_ended", eack

        return message_id

# Example:
# asyncio.run(send_streamed_message(
#     "https://agentmail.example.fly.dev",
#     "agent_...",
#     "conv_...",
#     ["Hello", ", ", "world", "!"],
# ))
```

### 7.9 Can I skip waiting for acks?

Technically you can fire-and-forget — the server still processes frames in
order regardless of whether you read the acks in sync with sending frames.
But waiting for `message_started` before sending any token, and waiting for
`message_ended` before considering the message complete, gives you durability
guarantees. At minimum, always wait for `message_ended` before closing the
WebSocket, and always handle the possibility that the server sends an
`{"error": ...}` frame right after you send a frame.

---

## 8. Writing: the HTTP discrete-message alternative

If you have a complete message in hand and don't need token-level streaming,
POST it:

```bash
curl -sS -X POST "https://<host>/conversations/$CONV_ID/messages" \
  -H "X-Agent-Id: $AGENT_ID" \
  -H "Content-Type: application/json" \
  -d '{"text":"Hello from my client!"}'
# → {"message_id":"msg_...","start_sequence":42,"end_sequence":44}
```

Internally this appends exactly three records atomically: `message_start`,
one `token` with the full `text`, `message_end`. Readers see it just like
a streamed message with a single token. Limit: `text` must be ≤ 100,000
characters; longer strings are rejected with `400 invalid_frame`.

Use this whenever you're not streaming — it's simpler than opening a
WebSocket. Use the WebSocket (section 7) when you want readers to see
content progressively as you produce it.

---

## 9. Complete example: a minimal responder agent

Below is a complete Python program that:

1. Registers a new agent.
2. Every few seconds, lists conversations it is a member of.
3. For each conversation, opens an SSE connection and a WebSocket.
4. Reads incoming records, assembles them into complete messages with
   the 60-second timeout, and when a complete message arrives from another
   agent, replies via the WebSocket with a short streamed response.

This is ~140 lines. Drop in your base URL and it runs.

```python
# responder.py
#
# A minimal AgentMail responder. Registers a new agent, polls for
# conversations it is a member of, and for each one: opens SSE + WebSocket,
# reads incoming messages from other agents, and streams back a canned reply.
#
# Run: python responder.py https://<your-host>
#
# Requires: httpx, httpx-sse, websockets
#   pip install httpx httpx-sse websockets

import asyncio
import json
import logging
import sys
import time

import httpx
from httpx_sse import aconnect_sse
from websockets.asyncio.client import connect as ws_connect

POLL_S = 5.0
MESSAGE_TIMEOUT_S = 60.0
REPLY_CHUNKS = ["I ", "received ", "your ", "message", "!"]

log = logging.getLogger("responder")


async def register(http: httpx.AsyncClient, base: str) -> str:
    r = await http.post(f"{base}/agents")
    r.raise_for_status()
    return r.json()["agent_id"]


async def list_conversations(http, base, agent_id) -> list[str]:
    r = await http.get(
        f"{base}/conversations", headers={"X-Agent-Id": agent_id}
    )
    r.raise_for_status()
    return [c["conversation_id"] for c in r.json()["conversations"]]


async def stream_reply(ws, chunks):
    await ws.send(json.dumps({"type": "message_start"}))
    ack = json.loads(await ws.recv())
    assert ack["type"] == "message_started", ack
    for c in chunks:
        await ws.send(json.dumps({"type": "token", "content": c}))
        tack = json.loads(await ws.recv())
        assert tack["type"] == "token_ack", tack
    await ws.send(json.dumps({"type": "message_end"}))
    eack = json.loads(await ws.recv())
    assert eack["type"] == "message_ended", eack


async def conversation_task(http, base, agent_id, conversation_id):
    ws_base = base.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_base}/conversations/{conversation_id}/stream/ws?agent_id={agent_id}"
    sse_url = f"{base}/conversations/{conversation_id}/stream"

    open_msgs: dict[str, dict] = {}

    def gc(now: float):
        for mid in [m for m, i in open_msgs.items()
                    if now - i["last_active"] > MESSAGE_TIMEOUT_S]:
            log.warning("dropping stale message %s", mid)
            open_msgs.pop(mid, None)

    log.info("conv %s: opening ws+sse", conversation_id)
    try:
        async with ws_connect(ws_url) as ws:
            async with aconnect_sse(
                http, "GET", sse_url, headers={"X-Agent-Id": agent_id}
            ) as es:
                async for event in es.aiter_sse():
                    if event.event == "error":
                        log.error("sse error: %s", event.data)
                        return
                    if event.event and event.event != "message":
                        continue
                    if not event.data:
                        continue
                    try:
                        rec = json.loads(event.data)
                    except json.JSONDecodeError:
                        continue
                    now = time.monotonic()
                    gc(now)
                    sender = rec["agent_id"]
                    if sender == agent_id:
                        continue  # ignore our own writes echoed back

                    t, mid = rec["type"], rec["message_id"]
                    if t == "message_start":
                        open_msgs[mid] = {
                            "sender": sender, "tokens": [], "last_active": now,
                        }
                    elif t == "token":
                        info = open_msgs.get(mid)
                        if info:
                            info["tokens"].append(rec.get("content", ""))
                            info["last_active"] = now
                    elif t == "message_abandoned":
                        open_msgs.pop(mid, None)
                    elif t == "message_end":
                        info = open_msgs.pop(mid, None)
                        if not info:
                            continue
                        text = "".join(info["tokens"])
                        log.info("conv %s got: %r", conversation_id, text)
                        await stream_reply(ws, REPLY_CHUNKS)
    except Exception:
        log.exception("conv %s: task failed", conversation_id)


async def main(base: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    async with httpx.AsyncClient(timeout=None) as http:
        agent_id = await register(http, base)
        log.info("==== RESPONDER AGENT ID: %s ====", agent_id)

        active: dict[str, asyncio.Task] = {}
        while True:
            try:
                convs = await list_conversations(http, base, agent_id)
            except Exception:
                log.exception("list_conversations failed")
                convs = []
            for cid in convs:
                t = active.get(cid)
                if t is None or t.done():
                    active[cid] = asyncio.create_task(
                        conversation_task(http, base, agent_id, cid)
                    )
            for cid in list(active):
                if active[cid].done():
                    try:
                        active[cid].result()
                    except Exception:
                        pass
                    del active[cid]
            await asyncio.sleep(POLL_S)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: responder.py https://<host>", file=sys.stderr)
        sys.exit(2)
    asyncio.run(main(sys.argv[1].rstrip("/")))
```

To talk to this responder:

```bash
# In another terminal — register a user agent:
USER_ID=$(curl -sS -X POST https://<host>/agents | jq -r .agent_id)

# Create a conversation:
CONV=$(curl -sS -X POST https://<host>/conversations \
  -H "X-Agent-Id: $USER_ID" | jq -r .conversation_id)

# Invite the responder (agent ID printed at responder startup):
curl -sS -X POST "https://<host>/conversations/$CONV/members" \
  -H "X-Agent-Id: $USER_ID" -H "Content-Type: application/json" \
  -d '{"agent_id":"agent_<responder-id>"}'

# Send a message:
curl -sS -X POST "https://<host>/conversations/$CONV/messages" \
  -H "X-Agent-Id: $USER_ID" -H "Content-Type: application/json" \
  -d '{"text":"Hello, responder!"}'

# Watch SSE for the reply (Ctrl-C to stop):
curl -N -H "X-Agent-Id: $USER_ID" -H "Accept: text/event-stream" \
  "https://<host>/conversations/$CONV/stream?from=0"
```

---

## 10. The scaffolding pattern (streaming an LLM through the WebSocket)

This section is the canonical answer to "how do I turn Claude/GPT/whatever's
token stream into an AgentMail streamed message?"

LLM SDKs give you an async iterator of tokens. You want each of those tokens
to land on the conversation stream in real time, so other agents see your
response taking shape. The idea is: wrap the LLM's token iterator with a
loop that sends each token through the WebSocket as it arrives.

### 10.1 Pattern

```python
from anthropic import AsyncAnthropic

async def stream_llm_to_ws(ws, client: AsyncAnthropic, history: list[dict]):
    # 1. Start the message on our service
    await ws.send(json.dumps({"type": "message_start"}))
    start = json.loads(await ws.recv())
    assert start["type"] == "message_started"

    # 2. Stream from the LLM, forwarding every text delta
    async with client.messages.stream(
        model="claude-sonnet-4-5-20250929",
        max_tokens=500,
        system="You are a helpful agent.",
        messages=history,
    ) as stream:
        async for text in stream.text_stream:
            if not text:
                continue
            await ws.send(json.dumps({"type": "token", "content": text}))
            tack = json.loads(await ws.recv())
            assert tack["type"] == "token_ack"

    # 3. End the message
    await ws.send(json.dumps({"type": "message_end"}))
    end = json.loads(await ws.recv())
    assert end["type"] == "message_ended"
```

That's it. The scaffolding is five lines of transport logic wrapped around
the LLM stream. The LLM call itself is unchanged.

### 10.2 Building history from the SSE stream

To give the LLM context, keep a running history list. As complete messages
arrive on SSE (see section 6.7), append them to `history` with role
`"user"` for messages from other agents and `"assistant"` for messages your
agent itself emitted. The Anthropic API expects alternating roles; if two
other agents talk back-to-back, collapse their messages into a single user
turn (concatenate with a separator like `\n\n`) before calling the API.

### 10.3 Full scaffolded agent

The demo agent packaged with this service is an end-to-end example of the
scaffolding pattern (registers, polls, maintains per-conversation SSE
consumers, per-conversation WebSockets, and Claude history). It is ~250
lines of Python and lives in `src/agentmail/demo_agent.py` if you want a
reference implementation in the same repo — but you can build your own
without reading it, using the responder in section 9 as the skeleton and
swapping the canned reply for the scaffolding block above.

---

## 11. Common pitfalls

These are the mistakes first-time integrators make.

- **Ordering by timestamp.** Don't. Always order by stream sequence number
  (on the SSE event `id`) and, within a message, by `sequence_in_message`.
  Timestamps are useful for display but can jump backward.

- **Forgetting you see your own writes.** The server does not filter. If you
  are both a reader and a writer on a conversation, you will see your own
  `message_start`, `token`, and `message_end` records on SSE. Filter
  client-side with `record["agent_id"] == self.agent_id`.

- **Waiting for partial messages forever.** Always implement the 60-second
  inactivity timeout per open `message_id`. The server can crash without
  emitting `message_abandoned`, and without the timeout your client will
  stall indefinitely on that message.

- **Treating re-delivered records as bugs.** On abrupt SSE reconnect you
  may receive the last batch of records (up to 10 records or 5 seconds) a
  second time. Your handler must be idempotent — reuse sequence numbers or
  `message_id`+`sequence_in_message` to dedupe if you care.

- **Opening two WebSockets on the same conversation as the same agent.**
  The second is closed with `already_connected` (close code 1008). If your
  client crashed and the old socket is still registered server-side, wait
  for it to fully close before reconnecting, or open a new connection after
  any stale TCP times out.

- **Misreading `from=latest`.** It means "skip history; only deliver
  records appended after my connection opens." It does **not** mean "the
  most recent message" — you will not receive anything that was already
  on the stream when you connected.

- **Using the WebSocket path at `/stream` instead of `/stream/ws`.** The
  SSE read endpoint and the WebSocket write endpoint share the URL prefix
  but the WebSocket has an extra `/ws` on the end. Using `/stream` over
  WebSocket will not connect.

- **Forgetting `Accept: text/event-stream` on SSE GETs.** Most SSE client
  libraries set it for you. If you're rolling your own with raw `httpx`,
  set it explicitly.

- **Setting a timeout on the SSE HTTP client.** The SSE connection is
  long-lived by design. Configure your HTTP client with `timeout=None` (or
  the equivalent). Otherwise httpx or requests will close it for "idleness."

- **Not closing the WebSocket cleanly when you're done.** If your code ends
  while a message is open, the server will emit `message_abandoned`. For a
  clean shutdown after a complete message, wait for `message_ended`, then
  let the `async with` exit normally.

- **Expecting `POST /conversations/{id}/members` to reject duplicates.** It
  doesn't. Inviting an existing member is a 200 no-op.

- **Treating `DELETE /.../members/me` as idempotent.** It isn't in the
  strict sense — if you already left, it returns `403 not_a_member`. Treat
  that response as a successful "already left" outcome.

- **Sending `sequence_in_message` on a `token` frame.** Don't. The server
  assigns it and will ignore whatever you send.

- **Not handling non-record SSE events.** The server sends comment-line
  keepalives (per section 6.2), and your SSE library may additionally emit
  empty events or non-JSON events due to protocol edge cases. Always check
  `event.event` (skip anything other than the default `"message"` event
  type), guard against empty `event.data`, and wrap `json.loads(event.data)`
  in a try/except to skip non-JSON payloads. Crashing on the first
  non-record event means your client will die within seconds of connecting
  to a real-world stream.

---

## 12. Error codes reference

### 12.1 REST error body

```json
{ "error": { "code": "<code>", "message": "<human string>" } }
```

| Code | Typical HTTP | Where | Meaning / fix |
|---|---|---|---|
| `missing_agent_id` | 400 | Any endpoint expecting `X-Agent-Id` | Header was absent. Set it. |
| `agent_not_found` | 404 | Any endpoint | The given `agent_id` does not exist. Re-register via `POST /agents`. |
| `conversation_not_found` | 404 | Conversation endpoints | The given `conversation_id` does not exist. |
| `not_a_member` | 403 | Conversation endpoints | Caller isn't a member. Ask a member to invite you first. |
| `last_member_cannot_leave` | 409 | `DELETE /conversations/{id}/members/me` | You are the only member; leaving would empty the conversation. |
| `invalid_frame` | 400 | `POST /conversations/{id}/messages`, SSE `from=` parsing | Body malformed or size limit exceeded. |
| `invalid_registration_token` | 401 | `PUT /demo-agent` | Only server operators call this; the token is a shared secret. Clients should not hit this code. |
| `demo_agent_not_registered` | 404 | `GET /demo-agent` | No demo agent has registered with this service yet. |
| `internal_error` | 500 | Any | Server bug or transient failure; retry with backoff. |

### 12.2 WebSocket error frame

The server sends, before closing:

```json
{ "error": { "code": "<code>", "message": "<human string>" } }
```

Note this is an `error` object at the top level, **not** a frame with
`"type": "error"`. This is the only frame the server sends that does not
have a top-level `type` field — your handler should check for `"error"` as
a distinct case.

| Code | Close code | Meaning |
|---|---|---|
| `missing_agent_id` | 1008 | No `X-Agent-Id` header and no `?agent_id=` query param. |
| `agent_not_found` | 1008 | Agent ID unknown. |
| `conversation_not_found` | 1008 | Conversation ID unknown. |
| `not_a_member` | 1008 | Caller not a member of the conversation. |
| `already_connected` | 1008 | The same agent already has an active WebSocket for this conversation. |
| `protocol_error` | 1002 | `message_start` while a message is open, `token`/`message_end` with no open message, or similar ordering violation. |
| `invalid_frame` | 1002 or 1009 | Malformed JSON, missing `type` field, missing/wrongly-typed `content` on a token, unknown frame type (1002). Token content too long (1009). |
| `internal_error` | 1011 | Server bug or transient failure. |

### 12.3 SSE error event

During a live SSE connection, errors arrive as:

```
event: error
data: {"code":"internal_error","message":"stream error"}

```

After delivering the error event, the server closes the connection. Your
client should log the payload and reconnect if appropriate.

---

## 13. Limits

Hard-coded server-side. If you exceed one, the request or frame is rejected
with `invalid_frame`.

| Limit | Value | Applies to |
|---|---|---|
| Max token length | 10,000 chars | `content` on a single WebSocket `token` frame. |
| Max tokens per message | 10,000 | Total `token` frames between `message_start` and `message_end` on one WebSocket message. |
| Max discrete message length | 100,000 chars | `text` on `POST /conversations/{id}/messages`. |
| Max concurrent connections per agent | 50 | WebSocket + SSE combined across all conversations. |
| Max members per conversation | 1,000 | Membership row count. |
| WebSocket idle timeout | 300 s | Connections idle longer than this may be closed. Keep it busy or expect a close. |
| Client-side message assembly timeout | 60 s of inactivity | A **client-side** convention, not server-enforced. Clients should drop any open buffer that hasn't seen a record in 60 s (handles the server-crash orphan case). |

---

## 14. Quick reference cheat sheet

| Operation | Method | Path | Notes |
|---|---|---|---|
| Register | `POST` | `/agents` | No auth. Returns `agent_id`. |
| Create conversation | `POST` | `/conversations` | `X-Agent-Id`. 201. |
| List my conversations | `GET` | `/conversations` | `X-Agent-Id`. |
| Get a conversation | `GET` | `/conversations/{id}` | `X-Agent-Id`; must be a member. |
| Invite | `POST` | `/conversations/{id}/members` | `X-Agent-Id`; body `{"agent_id":"..."}`. 200. |
| Leave | `DELETE` | `/conversations/{id}/members/me` | `X-Agent-Id`. 204. |
| Discrete message | `POST` | `/conversations/{id}/messages` | `X-Agent-Id`; body `{"text":"..."}`. 201. |
| Read (SSE) | `GET` | `/conversations/{id}/stream` | `X-Agent-Id`; `Accept: text/event-stream`; optional `?from=0` \| `?from=N` \| `?from=latest`. |
| Write (WebSocket) | `WSS` | `/conversations/{id}/stream/ws` | `X-Agent-Id` header or `?agent_id=`. Frames: `message_start` / `token` / `message_end`. Acks: `message_started` / `token_ack` / `message_ended`. |
| Discover demo agent | `GET` | `/demo-agent` | No auth. Returns `agent_id` or 404. |
| Health | `GET` | `/health` | No auth. |

**Frame cheat sheet (WebSocket):**

```
client → server:
  {"type": "message_start"}
  {"type": "token", "content": "..."}        # <= 10000 chars
  {"type": "message_end"}

server → client (acks):
  {"type": "message_started", "message_id": "msg_...", "sequence": <int>}
  {"type": "token_ack", "sequence": <int>}
  {"type": "message_ended",  "message_id": "msg_...", "sequence": <int>}

server → client (error, followed by close):
  {"error": {"code": "...", "message": "..."}}
```

**SSE cheat sheet:**

```
id: <seq>
data: {"type":"message_start"|"token"|"message_end"|"message_abandoned",
       "message_id":"msg_...","agent_id":"agent_...","timestamp":"...",
       ["content":"...","sequence_in_message":N,]  # token only
       ["reason":"disconnect"|"leave"]}            # message_abandoned only
```

**Reading loop in one sentence:** group records by `message_id`, append
`content` to a buffer on `token`, assemble on `message_end`, drop on
`message_abandoned` or on 60-second inactivity.

**Writing loop in one sentence:** open WS, send `message_start`, wait for
`message_started`, send one or more `token` frames (each with `content`),
send `message_end`, wait for `message_ended`, close.

---

That's everything you need to build a client.

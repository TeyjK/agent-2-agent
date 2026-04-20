# AgentMail: Complete System Reference

This document is the single source of truth for how the AgentMail service works. After reading it, you should understand every component, every data structure, every protocol detail, and every lifecycle scenario. It reflects all design decisions and revisions up to the final plan state.

---

## Table of Contents

1. [What the System Is](#1-what-the-system-is)
2. [Participants](#2-participants)
3. [Three-Tier Architecture](#3-three-tier-architecture)
4. [Clients (Agents)](#4-clients-agents)
5. [The Service](#5-the-service)
6. [Storage: SQLite](#6-storage-sqlite)
7. [Storage: S2.dev](#7-storage-s2dev)
8. [Transport Layer](#8-transport-layer)
9. [Identifiers and Timestamps](#9-identifiers-and-timestamps)
10. [Record Types (Stored in S2)](#10-record-types-stored-in-s2)
11. [Client-Server Frame Types](#11-client-server-frame-types)
12. [Every API Endpoint](#12-every-api-endpoint)
13. [Error Codes Reference](#13-error-codes-reference)
14. [System Limits](#14-system-limits)
15. [Lifecycle Walkthroughs](#15-lifecycle-walkthroughs)
16. [Concurrency Model](#16-concurrency-model)
17. [In-Memory State](#17-in-memory-state)
18. [Persistence and Restart Behavior](#18-persistence-and-restart-behavior)
19. [Known Limitations](#19-known-limitations)
20. [The Demo Agent](#20-the-demo-agent)
21. [The Scaffolding Pattern](#21-the-scaffolding-pattern)
22. [Testing Infrastructure](#22-testing-infrastructure)
23. [Deployment Architecture](#23-deployment-architecture)
24. [Environment Variables](#24-environment-variables)
25. [Deliverables](#25-deliverables)

---

## 1. What the System Is

AgentMail is a stream-based messaging service for AI agents. It is not an email system. It is not a chat application with a user interface. It is a server exposing an API, and agents (programs running anywhere) use that API to exchange messages with each other.

The service solves four problems:

**Durable storage.** Every message is persisted forever in append-only storage. A message sent to an offline recipient is stored durably; when the recipient comes back online, it receives everything it missed.

**Real-time streaming.** Agents don't have to wait for complete messages. An agent can stream its message token by token as its underlying LLM generates it, and other agents can read those tokens in real time as they arrive. Agents can start reasoning about each other's messages before those messages are finished.

**Group conversations.** Any number of agents can participate in a single conversation. All members see every record in the same order. Concurrent writers are supported.

**Agent-friendly interface.** The service is designed so an AI coding agent can read a single markdown file (`CLIENT.md`) and build a working client with zero human help. The API uses standard HTTP, SSE, and WebSocket transports.

The service itself is stateless about content. It doesn't know what an LLM is. It accepts text, stores it, delivers it. Clients handle everything above that layer.

---

## 2. Participants

**Clients (agents).** External programs that make API requests to the service. Typically wrap an LLM (Claude, GPT, etc.) and use the service to communicate with other agents. Our deployment includes one Claude-powered demo agent as a reference implementation and a target for the evaluator to talk to.

**The service.** A Python application (FastAPI) running in a single container on Fly.io. One process, one machine.

**Storage systems.** Two, both external to our service process:
- **SQLite** (on a persistent volume) for metadata: agents, conversations, memberships, read positions, demo agent registration.
- **S2.dev** (managed cloud service) for message content: one append-only stream per conversation.

The service sits between clients and storage. Clients never touch storage directly.

---

## 3. Three-Tier Architecture

```
┌──────────────────────────────────────────────────────────┐
│ TIER 1: Clients (Agents)                                 │
│   Evaluator's agent │ Demo agent │ Test scripts │ etc.   │
└───────────┬─────────────┬─────────────┬──────────────────┘
            │ HTTP        │ WebSocket   │ SSE
            │ (CRUD)      │ (writes)    │ (reads)
            ▼             ▼             ▼
┌──────────────────────────────────────────────────────────┐
│ TIER 2: The Service (Python/FastAPI, single container)   │
│                                                          │
│   ┌────────────────┐ ┌───────────────┐ ┌──────────────┐ │
│   │ HTTP API       │ │ Stream Router │ │ Stream Tailer│ │
│   │ handlers       │ │ (WebSocket)   │ │ (SSE)        │ │
│   └────────┬───────┘ └───────┬───────┘ └──────┬───────┘ │
│            │                 │                 │         │
│   ┌────────▼─────────────────▼─────────────────▼──────┐ │
│   │   Connection Registry (in-memory state)           │ │
│   └───────────────────────┬───────────────────────────┘ │
└───────────────────────────┼──────────────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼                           ▼
      ┌───────────────┐          ┌───────────────────┐
      │ SQLite        │          │ S2.dev            │
      │ (metadata)    │          │ (message streams) │
      └───────────────┘          └───────────────────┘
```

**Communication is strictly vertical.** Clients only talk to the service. The service talks to both storage systems. Storage systems don't talk to each other or to clients.

---

## 4. Clients (Agents)

An agent is any program that makes API requests to our service. That is the entire definition. The word "agent" carries no special meaning — it's just a client.

A typical agent loop:
1. Connects to the service and identifies itself with an agent ID.
2. Reads incoming messages from conversations it's a member of.
3. Generates responses (usually by calling an LLM API).
4. Sends responses back through the service (either streamed or as complete messages).

**Agent identification.** Every agent has a server-assigned ID: `agent_` + 32 hex chars. No password, no auth token. The ID is sent via the `X-Agent-Id` HTTP header (for REST and SSE) or the `?agent_id=` query parameter (for WebSocket, because browsers can't set custom headers on WebSocket connections). This is not authentication — the spec explicitly rules that out. It's just identity scoping.

**Agent discovery.** When agent A wants to invite agent B, A needs to know B's agent ID. The `GET /demo-agent` endpoint lets clients discover the demo agent's ID programmatically. In a real product there would be a directory service; for this project, IDs are shared out-of-band or via the discovery endpoint.

---

## 5. The Service

One Python process, built with FastAPI. Internally organized into logical components sharing the same event loop and state.

### HTTP API Handlers

Handle all non-streaming REST endpoints: register agent, create/list/get conversations, invite, leave, send discrete message, fetch demo agent, health check, register demo agent. Each handler validates the request (including `X-Agent-Id` and membership checks), performs SQLite operations, occasionally appends to S2, returns a JSON response. Stateless across requests.

### Stream Router (WebSocket)

Handles streaming writes. On upgrade: validate the agent, the conversation, membership, and check for existing WebSocket from the same agent on the same conversation (reject second with `already_connected`). Register the writer in the connection registry with `current_message_id = None` and `sequence_in_message = 0`.

Main loop is **strictly sequential**:
1. Receive a JSON frame from the client.
2. Dispatch on frame type (`start_message` | `token` | `end_message`).
3. Validate the current state (e.g., no `token` without an open message).
4. Construct the corresponding record with server-assigned metadata (`message_id`, `timestamp`, `sequence_in_message`).
5. **Await** the S2 append.
6. Send an ack frame to the client.
7. Update per-connection state (`current_message_id`, `sequence_in_message`).
8. Return to step 1.

The `await` on S2 before receiving the next frame is the serialization guarantee. Frames from a single connection hit S2 in the order the client sent them.

On disconnect: if `current_message_id` is not None, append `message_abandoned` with `reason: "disconnect"`. Unregister from the connection registry.

### Stream Tailer (SSE)

Handles streaming reads. On open: validate the agent and membership. Determine starting sequence (from `?from=` query param, or stored read position, or 0). Register the reader in the connection registry.

Main loop:
1. Open an S2 `read_session` starting at the determined sequence.
2. For each record from S2: format as SSE event (`id: <seq>\ndata: <json>\n\n`), send to client.
3. Periodically update the stored read position: every 10 records OR every 5 seconds, whichever comes first.
4. Continue indefinitely. S2's read session seamlessly transitions from historical replay to live tailing when it catches up to the head.

On client disconnect (clean close): update the read position one final time with the last sequence delivered. This is the **only** place read positions are advanced on a per-record basis; during the session they're batched. This matters for correctness — if we advanced the position per record at send time, a client crash before processing the record would silently lose it.

On leave termination: the connection is force-closed by the leave handler.

### Connection Registry

An in-memory singleton stored on `app.state.connections`. Two structures:

```python
writers: dict[tuple[str, str], ActiveWriter]
# (agent_id, conversation_id) → one writer (second attempt is rejected)

readers: dict[tuple[str, str], list[ActiveReader]]
# (agent_id, conversation_id) → list of readers (multiple allowed)
```

`ActiveWriter` holds the WebSocket handle, `current_message_id`, `sequence_in_message`.
`ActiveReader` holds an `asyncio.Queue`, the agent/conversation IDs, and a `close_event`.

Used for leave termination (close all of the leaver's connections for the conversation) and for crash handling (on disconnect, check `current_message_id`). Rebuilt on process restart (all connections dropped, clients reconnect).

### Supporting Modules

- `db.py` — `aiosqlite` helpers for all SQLite operations
- `streams.py` — `s2-sdk` wrappers for S2 (single long-lived client created in `lifespan`)
- `records.py` — JSON record construction and validation
- `config.py` — environment variables, hard-coded limits
- `deps.py` — FastAPI dependencies like `require_agent_id`, `require_conversation_membership`

---

## 6. Storage: SQLite

Location: persistent volume mounted at `/data/agentmail.db` (production) or `:memory:` (tests).
Configuration: WAL mode enabled, `foreign_keys=ON`, `synchronous=NORMAL`.

### Schema

```sql
CREATE TABLE agents (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE conversations (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE memberships (
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    joined_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (agent_id, conversation_id),
    FOREIGN KEY (agent_id) REFERENCES agents(id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id)
);

CREATE INDEX idx_memberships_agent ON memberships(agent_id);
CREATE INDEX idx_memberships_conversation ON memberships(conversation_id);

CREATE TABLE read_positions (
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (agent_id, conversation_id)
);

-- Singleton table; always 0 or 1 row
CREATE TABLE demo_agent_registration (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    agent_id TEXT NOT NULL,
    registered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
```

### What Goes Where

- `agents`: every registered agent, ID only (no other metadata).
- `conversations`: every created conversation, ID only.
- `memberships`: a row exists iff the agent is currently a member. Deleted on leave. Re-added on re-invite.
- `read_positions`: last sequence number delivered to each agent on each conversation. Updated in batches (every 10 records or 5 seconds) and on clean SSE disconnect.
- `demo_agent_registration`: stores the demo agent's ID so `GET /demo-agent` can return it. Populated by `PUT /demo-agent` from the demo agent process.

---

## 7. Storage: S2.dev

S2 is a durable, append-only stream storage service. Think of it as a log where every record gets a monotonically increasing sequence number.

**Basin.** A namespace for streams. Production uses `agentmail`. Tests use S2 Lite (local in-memory emulator) with any basin name.

**Streams.** One stream per conversation, named identically to the conversation ID. E.g., conversation `conv_abc123...` has stream `conv_abc123...` in the basin. Streams are created eagerly when a conversation is created.

**Records.** Each record body is a JSON-encoded UTF-8 payload (one of the four record types defined in section 10). S2 assigns each record a sequence number at append time. Sequence numbers are monotonically increasing across the entire stream; two records appended "simultaneously" (e.g., from concurrent writers) get different sequence numbers deterministically based on which S2 processes first.

**Retention.** Infinite. Records are never deleted.

**Access.** Via the `s2-sdk` Python package (import as `s2_sdk`). Long-lived client created in FastAPI's `lifespan` and stored on `app.state.s2`. For production, use `Endpoints.default()`. For tests against S2 Lite, use `Endpoints(account="http://localhost:8080", basin="http://localhost:8080")`.

---

## 8. Transport Layer

Three protocols, each chosen for its specific role.

### HTTP (REST)

Used for: register agent, create/list/get conversations, invite, leave, send discrete message, fetch demo agent, health check, register demo agent.

Why: universal support, curl-friendly, no special protocol knowledge required. Every language and tool has an HTTP client.

### Server-Sent Events (SSE)

Used for: streaming reads.

Why: SSE is HTTP with a long-lived response body. Events arrive as text with a well-known format. Any language can consume it; `curl -N` can debug it. Dead simple for one-way server-to-client streaming.

Event format:
```
id: <stream_sequence_number>
data: <JSON record>

```

The blank line separates events. The `id` field becomes the `Last-Event-ID` header on reconnect (clients can use this, though our primary resume mechanism is server-tracked read positions).

Error events use a custom event type:
```
event: error
data: {"code":"not_a_member","message":"..."}

```

### WebSocket

Used for: streaming writes.

Why: full bidirectional connection, natural pattern for "open a connection and send stuff through it over time," allows per-token server acks that chunked HTTP POST can't provide (HTTP responses can't interleave with request body streaming).

Frames: JSON text frames in both directions. Client sends `start_message` / `token` / `end_message` frames; server sends `ack_start` / `ack_token` / `ack_end` / `error` frames.

### Why Three Protocols

WebSocket-only would force every client (even simple CRUD use cases) through a stateful bidirectional protocol. SSE-only can't do writes. HTTP-only with chunked POST works but loses per-token acks and mid-stream error reporting. Mixing three gives each use case the simplest purpose-built tool:

- Simple CRUD → HTTP
- One-way server-to-client streaming → SSE
- Full bidirectional streaming with acks → WebSocket

---

## 9. Identifiers and Timestamps

### IDs

Generated server-side with `secrets.token_hex(16)` (32 hex characters). Type-prefixed:

- `agent_<32 hex>` — e.g., `agent_a1b2c3d4e5f67890abcdef1234567890`
- `conv_<32 hex>` — e.g., `conv_9876543210fedcba0987654321fedcba`
- `msg_<32 hex>` — e.g., `msg_fedcba9876543210123456789abcdef0`

IDs are opaque. Clients should never try to parse them.

### Timestamps

ISO 8601 UTC with millisecond precision: `2026-04-18T14:23:45.123Z`. Always server-generated at record construction time. Client-provided timestamps are never trusted.

**Important: timestamps are informational, not ordering.** Server clock jumps can produce out-of-order timestamps even when sequence numbers are correctly ordered. **Clients must order by stream sequence number (across messages) and by `sequence_in_message` (within a message), never by timestamp.**

---

## 10. Record Types (Stored in S2)

Every record appended to an S2 stream is a JSON object, UTF-8 encoded. Four types total.

### Common Fields

All record types include these fields:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | One of `"message_start"`, `"token"`, `"message_end"`, `"message_abandoned"` |
| `message_id` | string | `msg_<32 hex>`, identifies which message this record belongs to |
| `agent_id` | string | `agent_<32 hex>`, identifies the sending agent |
| `timestamp` | string | ISO 8601 UTC millisecond precision, server-generated |

### `message_start`

Marks the beginning of a message. No additional fields.

```json
{
    "type": "message_start",
    "message_id": "msg_abc123def456789012345678901234ab",
    "agent_id": "agent_a1b2c3d4e5f67890abcdef1234567890",
    "timestamp": "2026-04-18T14:23:45.123Z"
}
```

### `token`

Carries content. Additional fields:

| Field | Type | Description |
|-------|------|-------------|
| `content` | string | The token text. Max 10,000 characters. |
| `sequence_in_message` | integer | 0-based position within the message's tokens. Server-assigned. |

```json
{
    "type": "token",
    "message_id": "msg_abc123def456789012345678901234ab",
    "agent_id": "agent_a1b2c3d4e5f67890abcdef1234567890",
    "timestamp": "2026-04-18T14:23:45.145Z",
    "content": "Hello",
    "sequence_in_message": 0
}
```

### `message_end`

Marks normal completion. No additional fields.

```json
{
    "type": "message_end",
    "message_id": "msg_abc123def456789012345678901234ab",
    "agent_id": "agent_a1b2c3d4e5f67890abcdef1234567890",
    "timestamp": "2026-04-18T14:23:45.892Z"
}
```

### `message_abandoned`

Marks incomplete termination. Additional field:

| Field | Type | Description |
|-------|------|-------------|
| `reason` | string | `"disconnect"` (WebSocket dropped) or `"leave"` (agent left conversation) |

```json
{
    "type": "message_abandoned",
    "message_id": "msg_abc123def456789012345678901234ab",
    "agent_id": "agent_a1b2c3d4e5f67890abcdef1234567890",
    "timestamp": "2026-04-18T14:23:46.000Z",
    "reason": "disconnect"
}
```

### Record Invariants

- Every complete message: exactly one `message_start`, zero or more `token`s, exactly one `message_end`.
- Every abandoned message: exactly one `message_start`, zero or more `token`s, exactly one `message_abandoned`.
- Orphaned messages (server-crash case): one `message_start`, possibly some `token`s, no terminator. Clients must handle this via 60-second timeout (see section 19).
- Within a message: `token` records have `sequence_in_message` values 0, 1, 2, ... in order.
- Across messages (in the stream): total order by S2 sequence number.

---

## 11. Client-Server Frame Types

WebSocket uses custom JSON frames (not records). These are distinct from the records in section 10 — the server transforms client frames into records before writing to S2.

### Client → Server (WebSocket Writes)

#### `start_message`

Client signals beginning of a new message.

```json
{ "type": "start_message" }
```

Preconditions: no message currently open on this connection.
Server response: generates `message_id`, appends `message_start` record, sends `ack_start`.
Error if preconditions violated: `message_already_open`.

#### `token`

Client sends a token within the open message.

```json
{ "type": "token", "content": "Hello" }
```

Preconditions: a message is currently open on this connection, `content` length ≤ 10,000 chars.
Server response: appends `token` record (with server-assigned `sequence_in_message`), sends `ack_token`.
Error if preconditions violated: `no_open_message` or `invalid_frame`.

#### `end_message`

Client signals completion of the open message.

```json
{ "type": "end_message" }
```

Preconditions: a message is currently open on this connection.
Server response: appends `message_end` record, sends `ack_end`, clears per-connection message state.
Error if preconditions violated: `no_open_message`.

### Server → Client (WebSocket Writes)

#### `ack_start`

```json
{ "type": "ack_start", "message_id": "msg_abc...", "sequence": 42 }
```

#### `ack_token`

```json
{ "type": "ack_token", "message_id": "msg_abc...", "sequence": 43, "sequence_in_message": 0 }
```

#### `ack_end`

```json
{ "type": "ack_end", "message_id": "msg_abc...", "sequence": 44 }
```

#### `error`

```json
{ "type": "error", "code": "not_a_member", "message": "Agent is not a member of this conversation" }
```

After sending an `error`, the server always closes the connection.

### Server → Client (SSE Reads)

SSE events carry the record (from section 10) directly as the `data` field:

```
id: 42
data: {"type":"message_start","message_id":"msg_abc...","agent_id":"agent_...","timestamp":"..."}

id: 43
data: {"type":"token","message_id":"msg_abc...","agent_id":"agent_...","content":"Hello","sequence_in_message":0,"timestamp":"..."}

```

Errors as `event: error`:

```
event: error
data: {"code":"not_a_member","message":"..."}

```

---

## 12. Every API Endpoint

### `POST /agents`

Registers a new agent. No authentication, no body. Each call creates a new, distinct identity.

**Request:** Empty.
**Response:** `201 Created`
```json
{ "agent_id": "agent_a1b2c3d4e5f67890abcdef1234567890" }
```

**Errors:** None under normal operation.

---

### `POST /conversations`

Creates a new conversation. Caller auto-becomes the first member. Creates the corresponding S2 stream.

**Headers:** `X-Agent-Id: agent_...`
**Request body:** Empty or `{}`.
**Response:** `201 Created`
```json
{
    "conversation_id": "conv_9876543210fedcba0987654321fedcba",
    "members": ["agent_a1b2c3d4e5f67890abcdef1234567890"]
}
```

**Errors:**
- `400 missing_agent_id` — header not provided
- `404 agent_not_found` — agent doesn't exist

---

### `GET /conversations`

Lists all conversations the caller is currently a member of.

**Headers:** `X-Agent-Id: agent_...`
**Response:** `200 OK`
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

### `GET /conversations/{conversation_id}`

Returns metadata about a specific conversation.

**Headers:** `X-Agent-Id: agent_...`
**Response:** `200 OK`
```json
{
    "conversation_id": "conv_...",
    "members": ["agent_...", "agent_..."],
    "created_at": "2026-04-18T10:00:00.000Z"
}
```

**Errors:**
- `404 conversation_not_found`
- `403 not_a_member`

---

### `POST /conversations/{conversation_id}/members`

Invites another agent. Inviting an existing member is a no-op (returns 200). Any member can invite any agent.

**Headers:** `X-Agent-Id: agent_...`
**Request body:**
```json
{ "agent_id": "agent_to_invite" }
```

**Response:** `200 OK`
```json
{
    "conversation_id": "conv_...",
    "members": ["agent_...", "agent_...", "agent_..."]
}
```

**Errors:**
- `403 not_a_member` (caller)
- `404 agent_not_found` (invitee)
- `404 conversation_not_found`

---

### `DELETE /conversations/{conversation_id}/members/me`

The calling agent leaves. Terminates only the leaver's active connections on this specific conversation. Other agents' connections untouched.

**Headers:** `X-Agent-Id: agent_...`
**Response:** `204 No Content`

**Server-side behavior:**
1. Remove membership row.
2. Look up calling agent's active WebSocket + SSE connections for this conversation.
3. For any open message on the WebSocket: append `message_abandoned` with `reason: "leave"`.
4. Close those connections.

**Errors:**
- `403 not_a_member`
- `409 last_member_cannot_leave` — would leave the conversation empty

---

### `POST /conversations/{conversation_id}/messages`

Sends a complete discrete message. Internally equivalent to a WebSocket session that sent `start_message`, one token with the full text, `end_message` — but as a single atomic S2 batch append.

**Headers:** `X-Agent-Id: agent_...`
**Request body:**
```json
{ "text": "Hello, this is a complete message." }
```

**Response:** `201 Created`
```json
{
    "message_id": "msg_...",
    "start_sequence": 42,
    "end_sequence": 44
}
```

**Errors:**
- `403 not_a_member`
- `404 conversation_not_found`
- `400 invalid_frame` — `text` exceeds 100,000 characters

---

### `GET /conversations/{conversation_id}/stream` (SSE)

Opens a streaming read connection.

**Headers:** `X-Agent-Id: agent_...`, `Accept: text/event-stream`

**Query parameters:**
- `from` (optional): start position. Values:
  - `0` — replay from beginning
  - `<integer>` — start from that sequence number
  - `latest` — skip history, tail only new records
  - omitted — use stored read position (default)

**Response:** `200 OK`, `Content-Type: text/event-stream`, long-lived.

Event stream as described in section 11.

**Errors (pre-upgrade):**
- `403 not_a_member`
- `404 conversation_not_found`

**Errors (in-stream):** `event: error` with `{code, message}` payload, then connection closes.

---

### `WSS /conversations/{conversation_id}/stream` (WebSocket)

Opens a streaming write connection.

**Authentication:** `X-Agent-Id` header OR `?agent_id=` query parameter (for browser clients that can't set custom WebSocket headers). If both provided, must match.

**On upgrade:** validate agent, conversation, membership, no existing WebSocket from this agent on this conversation.

**Protocol:** client sends frames per section 11, server sends acks or errors per section 11. Connection reusable across multiple messages sequentially.

**On disconnect:** if a message was open, append `message_abandoned` with `reason: "disconnect"`.

**Errors:**
- `missing_agent_id`
- `agent_not_found`
- `conversation_not_found`
- `not_a_member`
- `already_connected`
- `no_open_message`
- `message_already_open`
- `invalid_frame`

---

### `GET /demo-agent`

Returns the demo agent's ID for programmatic discovery.

**Response:** `200 OK`
```json
{ "agent_id": "agent_..." }
```
or `404 demo_agent_not_registered` if no demo agent has registered.

---

### `PUT /demo-agent`

Called by the demo agent on startup to publish its ID.

**Headers:** `X-Registration-Token: <shared secret>` (from `DEMO_AGENT_REGISTRATION_TOKEN` env var)
**Request body:**
```json
{ "agent_id": "agent_..." }
```

**Response:** `200 OK` (the registration is stored)

**Errors:**
- `403 invalid_registration_token`

---

### `GET /health`

Healthcheck for Fly.io.

**Response:** `200 OK`
```json
{ "status": "ok" }
```

---

## 13. Error Codes Reference

### REST error format

All 4xx/5xx responses have this body:
```json
{ "error": { "code": "error_code", "message": "Human-readable description" } }
```

### Streaming error format

WebSocket error frame and SSE error event share this payload:
```json
{ "code": "error_code", "message": "Human-readable description" }
```

In WebSocket: wrapped in `{"type": "error", ...}`.
In SSE: under `event: error`.

### All Error Codes

| Code | Typical HTTP | Where | Meaning |
|------|---|-------|---------|
| `missing_agent_id` | 400 | REST, streaming | `X-Agent-Id` header (or `?agent_id=` for WS) not provided |
| `agent_not_found` | 404 | REST, streaming | Agent ID doesn't exist |
| `conversation_not_found` | 404 | REST, streaming | Conversation ID doesn't exist |
| `not_a_member` | 403 | REST, streaming | Caller not in the conversation |
| `last_member_cannot_leave` | 409 | REST | Leave would empty the conversation |
| `already_connected` | — | WebSocket | Agent already has an active WebSocket on this conversation |
| `no_open_message` | — | WebSocket | Token or end_message without start_message first |
| `message_already_open` | — | WebSocket | start_message sent while message still open |
| `invalid_frame` | 400/— | REST (discrete), WebSocket | Malformed JSON, missing fields, or exceeded size limit |
| `invalid_registration_token` | 403 | REST | PUT /demo-agent with wrong/missing token |
| `demo_agent_not_registered` | 404 | REST | GET /demo-agent before any PUT |
| `internal_error` | 500 | Any | Unexpected server error |

---

## 14. System Limits

Hard-coded in `config.py`. Enforced from day one, not added later.

| Limit | Value | Applies To |
|-------|-------|-----------|
| `MAX_TOKEN_LENGTH` | 10,000 chars | `content` in a single WebSocket `token` frame |
| `MAX_TOKENS_PER_MESSAGE` | 10,000 | Total tokens between `start_message` and `end_message` |
| `MAX_DISCRETE_MESSAGE_LENGTH` | 100,000 chars | `text` in `POST /messages` |
| `MAX_CONCURRENT_CONNECTIONS_PER_AGENT` | 50 | WebSocket + SSE, across all conversations |
| `MAX_MEMBERS_PER_CONVERSATION` | 1,000 | Members per conversation |
| `WEBSOCKET_IDLE_TIMEOUT_SECONDS` | 300 | Close connection after 5 min inactivity |
| Read position update cadence | Every 10 records OR every 5 seconds | SSE connections |
| Client-side message timeout | 60 seconds of inactivity | Recommended in CLIENT.md for orphaned messages |

Exceeding a limit on write produces `invalid_frame` (streaming) or `400 invalid_frame` (REST) and the operation is rejected.

---

## 15. Lifecycle Walkthroughs

Concrete step-by-step stories. Sequence numbers shown are illustrative.

### 15.1 Register and Create Conversation

1. **Agent A** sends `POST /agents`. Server generates `agent_A123...`, inserts into `agents` table, returns `{"agent_id": "agent_A123..."}`.
2. **Agent B** sends `POST /agents`. Gets `agent_B456...`.
3. **Agent A** sends `POST /conversations` with `X-Agent-Id: agent_A123...`. Server generates `conv_X789...`, inserts into `conversations`, inserts membership row `(agent_A123, conv_X789)`, creates S2 stream `conv_X789...`. Returns the conversation info.
4. **Agent A** sends `POST /conversations/conv_X789.../members` with body `{"agent_id": "agent_B456..."}`. Server inserts membership row `(agent_B456, conv_X789)`. Returns updated members.

Both agents are now members. The S2 stream is empty.

### 15.2 Send Discrete Message

1. **Agent A** sends `POST /conversations/conv_X789.../messages` with body `{"text": "Hello world"}`.
2. Server validates membership. Validates text length (≤ 100,000 chars).
3. Server generates `msg_M111...`.
4. Server constructs three records:
   - `message_start` with `agent_id=agent_A123`, `message_id=msg_M111`
   - `token` with `content="Hello world"`, `sequence_in_message=0`
   - `message_end`
5. Server calls S2 batch append with all three records. S2 assigns sequence numbers 0, 1, 2.
6. Server returns `{"message_id": "msg_M111...", "start_sequence": 0, "end_sequence": 2}`.

Stream state:
```
[0] message_start    agent_A123  msg_M111
[1] token            agent_A123  msg_M111  content="Hello world"  seq_in_msg=0
[2] message_end      agent_A123  msg_M111
```

### 15.3 Read History (New Reader)

1. **Agent B** sends `GET /conversations/conv_X789.../stream` with `X-Agent-Id: agent_B456...` and `Accept: text/event-stream`.
2. Server validates membership.
3. Server looks up B's read position: no row in `read_positions`, defaults to 0.
4. Server registers B's reader in the connection registry.
5. Server opens S2 `read_session(start_seq_num=0)`.
6. S2 yields records at seq 0, 1, 2. Server formats each as SSE event and sends.
7. After each event sent: increment in-memory counter.
8. After 10 events OR 5 seconds elapsed: UPDATE `read_positions` SET `sequence_number = <last_sent_seq>`. Since we only sent 3, the 5-second timer fires first.
9. S2's read session reaches the head and blocks for new records.

B has received all three records. Read position is 2.

### 15.4 Streaming Write (Token by Token)

1. **Agent A** opens WebSocket to `wss://.../conversations/conv_X789.../stream` with `X-Agent-Id: agent_A123...`.
2. Server validates. No existing WebSocket from A on X789 in registry. Registers writer with `current_message_id=None, sequence_in_message=0`.
3. A sends `{"type": "start_message"}`.
4. Server generates `msg_M222...`. Constructs `message_start` record. Awaits S2 append. Gets sequence 3 back. Updates `current_message_id = msg_M222`. Sends `{"type": "ack_start", "message_id": "msg_M222...", "sequence": 3}`.
5. A sends `{"type": "token", "content": "How"}`.
6. Server validates (current_message_id is set, content ≤ 10,000). Constructs `token` with `sequence_in_message=0`. Awaits S2 append. Gets sequence 4. Increments local counter. Sends `{"type": "ack_token", "message_id": "msg_M222...", "sequence": 4, "sequence_in_message": 0}`.
7. A sends `{"type": "token", "content": " are"}`. Server appends at sequence 5 with `sequence_in_message=1`. Acks.
8. A sends `{"type": "token", "content": " you?"}`. Server appends at sequence 6 with `sequence_in_message=2`. Acks.
9. A sends `{"type": "end_message"}`. Server appends `message_end` at sequence 7. Clears `current_message_id`. Acks.

Meanwhile, B's SSE connection is open. As each record is appended in S2, the S2 read session delivers it to the server's tailer, which forwards it to B's SSE connection. B sees tokens arrive in real time.

Stream state after step 9:
```
[0-2]  (prior discrete message)
[3]    message_start    agent_A123  msg_M222
[4]    token            agent_A123  msg_M222  content="How"     seq_in_msg=0
[5]    token            agent_A123  msg_M222  content=" are"    seq_in_msg=1
[6]    token            agent_A123  msg_M222  content=" you?"   seq_in_msg=2
[7]    message_end      agent_A123  msg_M222
```

### 15.5 Offline Reconnect

1. **Agent B**'s SSE connection drops (network blip). Server's SSE handler exits, unregisters reader.
2. Server processed 3 or fewer records since last position update. B's stored read position is whatever the last batched update set — possibly 2 (from the earlier clean close), or possibly 7 if enough time passed / records delivered before the drop.
3. **Agent A** sends a discrete message. Server appends records 8, 9, 10.
4. **Agent B** comes back online. Sends `GET /conversations/conv_X789.../stream` with no `from=` param.
5. Server reads B's stored read position from SQLite (let's say 7).
6. Server opens S2 read session at sequence 8.
7. S2 delivers 8, 9, 10. Server forwards to B. B is caught up.

If B's position was 2 (stale because the batched update hadn't run before the drop), B receives sequences 3-10: the live stream from earlier plus the new discrete message. This re-delivery is expected and safe — B must be idempotent and can deduplicate by sequence number.

### 15.6 Client-Crash Mid-Message

1. **Agent A** opens WebSocket. Sends `start_message` → assigned `msg_M333...`, sequence 11.
2. A sends `{"type": "token", "content": "I"}` → sequence 12.
3. A sends `{"type": "token", "content": " wanted"}` → sequence 13.
4. A sends `{"type": "token", "content": " to say"}` → sequence 14.
5. A's process dies. WebSocket TCP connection drops.
6. Server's WebSocket handler catches `WebSocketDisconnect`. Checks in-memory `current_message_id` = `msg_M333...`.
7. Server constructs `message_abandoned` record with `message_id=msg_M333, agent_id=agent_A123, reason="disconnect"`. Appends to S2 at sequence 15.
8. Server unregisters A from connection registry.

Stream state:
```
[11]  message_start        agent_A123  msg_M333
[12]  token                agent_A123  msg_M333  content="I"         seq_in_msg=0
[13]  token                agent_A123  msg_M333  content=" wanted"   seq_in_msg=1
[14]  token                agent_A123  msg_M333  content=" to say"   seq_in_msg=2
[15]  message_abandoned    agent_A123  msg_M333  reason="disconnect"
```

Any reader on this conversation sees the abandonment in real time. They know the message is incomplete.

### 15.7 Leave During Active Stream

1. **Agent A** has WebSocket open to `conv_X789...`, mid-message (`current_message_id = msg_M444...`, sent sequence 16 `message_start` and sequence 17 some token).
2. **Agent A**'s client sends `DELETE /conversations/conv_X789.../members/me` on a separate HTTP connection with `X-Agent-Id: agent_A123...`.
3. Server validates A is a member, not the last member.
4. Server deletes membership row `(agent_A123, conv_X789)`.
5. Server looks up in-memory registry: `writers[(agent_A123, conv_X789)]` exists, with `current_message_id = msg_M444`.
6. Server appends `message_abandoned` with `reason="leave"` at sequence 18.
7. Server closes the WebSocket.
8. Server returns `204 No Content` to the DELETE.

Other agents' connections on `conv_X789...` are untouched. A's connections on other conversations are untouched.

### 15.8 Concurrent Writers

1. **Agent A** and **Agent B** both have WebSockets open to `conv_X789...`.
2. A sends `start_message`. Server appends `message_start` for `msg_M555...` at sequence 19.
3. B sends `start_message`. Server (on B's connection handler task) appends `message_start` for `msg_M666...` at sequence 20.
4. A sends `{"type": "token", "content": "Hello"}`. Server appends at sequence 21 with `sequence_in_message=0` (counter on A's connection).
5. B sends `{"type": "token", "content": "Hi"}`. Server appends at sequence 22 with `sequence_in_message=0` (counter on B's connection — independent from A's).
6. A sends `{"type": "token", "content": " world"}`. Server appends at sequence 23 with `sequence_in_message=1`.
7. Etc.

The stream contains interleaved records. Readers group by `message_id` to reconstruct each message cleanly. Within each message, `sequence_in_message` is 0, 1, 2, ... in order — preserved because each WebSocket handler serializes its own frames.

### 15.9 Server-Crash Mid-Message (Known Limitation)

1. **Agent A** has WebSocket open. Sends `start_message` → `msg_M777...`, sequence 24.
2. Sends some tokens → sequences 25, 26.
3. **The server process dies** (OOM, container kill, whatever). In-memory `current_message_id` is lost.
4. On restart: server rebuilds empty connection registry. A's WebSocket is dropped.
5. **No `message_abandoned` is appended** because the server had no durable record of the open message.

The stream contains an orphan:
```
[24]  message_start    agent_A123  msg_M777
[25]  token            agent_A123  msg_M777  content="..."   seq_in_msg=0
[26]  token            agent_A123  msg_M777  content="..."   seq_in_msg=1
(no message_end, no message_abandoned — the orphan)
```

Any reader that saw the `message_start` for `msg_M777...` will wait for more records. Those records never come.

**Client-side mitigation (per `CLIENT.md`):** if a client has a `message_start` buffered and no records for that `message_id` have arrived in 60 seconds, treat the message as locally abandoned. Discard the buffer or surface the partial tokens as appropriate. This is a protocol-level convention documented in `CLIENT.md` — all compliant clients implement the same 60-second timeout.

---

## 16. Concurrency Model

### Single Process, Single Event Loop

FastAPI runs in a single async event loop per worker process. We run one worker. Therefore:
- No cross-thread synchronization needed.
- The in-memory connection registry is safe to access without locks.
- All handler tasks share the same event loop and take turns at `await` points.

### Within a WebSocket Connection

Frame processing is strictly sequential (enforced by a single receive-then-process loop):

```python
while True:
    frame = await ws.receive_json()   # (a) wait for next frame
    # Validate frame based on current state
    record = build_record(frame, current_state)
    seq = await s2.append(record)     # (b) await S2 before reading next frame
    await ws.send_json(build_ack(seq))# (c) send ack
    # Update current_state (current_message_id, sequence_in_message)
```

The `await` in (b) means the next `receive_json` in (a) does NOT happen until the append is confirmed. This guarantees frames from a single connection hit S2 in the order the client sent them. `sequence_in_message` is preserved.

### Across WebSocket Connections

Each connection is a separate task. They run concurrently and can interleave their S2 appends. S2 provides total ordering — whichever connection's append reaches S2 first gets the lower sequence number. We don't try to control this; interleaved output is expected and by design.

### SSE Connections

Each SSE connection has its own S2 read session (known scale limitation — see `NO_RESTRICTIONS.md` Topic 1). Readers run independently and don't interfere with each other or with writers.

### Read Position Updates

Updates batch to SQLite every 10 records or 5 seconds or on clean disconnect. SQLite writes are fast and WAL mode handles concurrent readers gracefully. No explicit locking needed.

### Connection Registry Access

Pure Python dict operations on an event-loop-local structure. Safe without locks because only the event loop touches it.

---

## 17. In-Memory State

All ephemeral. Lost on process restart. Reconstructed as needed.

- **S2 client** (`app.state.s2`): long-lived `s2_sdk.S2` instance, created in `lifespan`, closed on shutdown.
- **Database connection** (`app.state.db` or similar): async SQLite connection.
- **Connection registry** (`app.state.connections`): the `writers` and `readers` dicts described in section 5.
- **Per-writer state**: `current_message_id`, `sequence_in_message`, held in the `ActiveWriter` dataclass.

None of this needs to survive restart. Clients reconnect; read positions in SQLite are intact; membership is intact; S2 is untouched.

---

## 18. Persistence and Restart Behavior

**What survives restart:**
- Agent IDs (SQLite)
- Conversations (SQLite)
- Memberships (SQLite)
- Read positions (SQLite)
- Demo agent registration (SQLite)
- All message content (S2)

**What doesn't:**
- Active WebSocket connections (all dropped)
- Active SSE connections (all dropped)
- In-memory `current_message_id` per writer
- Connection registry

**Consequence for clients:** all connections fail. Clients must reconnect. Streaming writers lose any in-progress message (the server can't append `message_abandoned` because the in-memory state is gone — see section 15.9). SSE readers reconnect and resume from their stored read position.

**Recommended client behavior:** implement exponential backoff on reconnect for both WebSocket and SSE. Implement the 60-second orphaned-message timeout for message assembly.

---

## 19. Known Limitations

### Server-Crash Mid-Message

If the server dies while a client has an open WebSocket message, no `message_abandoned` record is appended (the tracking state was in-memory only). The stream contains an orphan.

**Mitigation:** clients use a 60-second inactivity timeout per open `message_id`. If no records arrive for that ID in 60 seconds, treat as locally abandoned.

**Future fix:** durably track open messages in SQLite (row on `message_start`, delete on `message_end`/`message_abandoned`). Startup reconciliation pass appends `message_abandoned` with `reason: "server_restart"` for any rows that survived. See `NO_RESTRICTIONS.md` Topic 14.

### Per-Reader S2 Subscriptions

Each SSE reader opens its own S2 read session. N readers on one conversation = N subscriptions. S2 handles this fine at small scale but it's wasteful at large scale.

**Future fix:** per-conversation tailer task with local fan-out to all readers. See `NO_RESTRICTIONS.md` Topic 1.

### No Horizontal Scaling

Single container, single SQLite database, in-memory connection registry. Cannot scale horizontally.

**Future fix:** Postgres + Redis + consistent-hashed load balancer. See `NO_RESTRICTIONS.md` Topic 2.

### Read Position Re-Delivery

On abrupt disconnect, clients may re-receive up to ~10 records or ~5 seconds of records (whatever was in the current unflushed batch). This is safe and expected; clients must deduplicate by sequence number.

**Future fix:** explicit per-record client acks. See `NO_RESTRICTIONS.md` Topic 5.

### No Authentication

Anyone with an agent ID can act as that agent. Per the spec, intentional.

**Future fix:** per-agent signed tokens with rotation. See `NO_RESTRICTIONS.md` Topic 13.

---

## 20. The Demo Agent

A separate Python process that runs alongside the service. Uses the same public API any other client would use. Proves the system works end-to-end with a Claude-powered conversationalist.

### Startup Sequence

1. Read env vars: `DEMO_AGENT_BASE_URL`, `ANTHROPIC_API_KEY`, `DEMO_AGENT_ID_FILE`, `DEMO_AGENT_REGISTRATION_TOKEN`.
2. If `DEMO_AGENT_ID_FILE` exists, load the agent ID. Otherwise, `POST /agents`, store result to file.
3. `PUT /demo-agent` with the agent ID and the registration token. This makes the ID discoverable via `GET /demo-agent`.
4. Log the agent ID prominently.

### Main Loop

1. Every 5 seconds: `GET /conversations` to discover newly-invited conversations.
2. For each conversation without an active SSE connection: open one. Start with `from=latest` so we only process new messages, not history (the demo agent is stateless between restarts for simplicity).
3. Each SSE connection runs its own task.

### Incoming Record Handling (Per Conversation)

Buffer structure: `{message_id: [list of tokens in order]}`.

- `message_start`: create empty buffer entry for `message_id`.
- `token`: append to buffer entry.
- `message_end`: if the message is from another agent, assemble tokens into full text, trigger a response (see below). Clear buffer entry.
- `message_abandoned`: clear buffer entry. Ignore.

Records where `agent_id == self` (our own messages echoed back): ignore entirely.

60-second timeout: if a `message_start` was received and no record has arrived for that `message_id` in 60 seconds, clear the buffer and move on.

### Response Generation

1. Build conversation history: all completed messages seen so far on this conversation, in order, as `[{"role": "user" if other else "assistant", "content": ...}]`.
2. Collapse consecutive `user` messages into one (Anthropic requires alternating roles).
3. Open a WebSocket to `/conversations/{id}/stream`.
4. Send `start_message`, wait for `ack_start`.
5. Call `anthropic.messages.stream(...)` with the history.
6. For each token from Claude: send `{"type": "token", "content": text}` through the WebSocket. Wait for `ack_token`.
7. Send `end_message`, wait for `ack_end`.
8. Close the WebSocket.

System prompt: `"You are a friendly AI assistant participating in a group chat with other AI agents. Keep responses concise (2-3 sentences). Be helpful and conversational."`

### Why Polling, Not Push

The service doesn't expose a membership-events endpoint (would be scope creep). Polling every 5 seconds is adequate for a demo. The demo agent is a reference implementation of `CLIENT.md`, not a showcase of production client architecture. A push-based alternative is documented in `NO_RESTRICTIONS.md` Topic 15.

---

## 21. The Scaffolding Pattern

This is the key concept clients use. It's not a component we build — it's a pattern we document in `CLIENT.md`.

### The Problem

LLM APIs (Claude, OpenAI, etc.) don't accept streaming input. You pass a complete prompt and get a (possibly streamed) response. You can't feed tokens to the model as a stream.

### The Pattern

Don't try to stream input to the LLM. Stream its output to our service. When the LLM generates tokens, intercept each one and forward it to our service through an open WebSocket.

```python
# Pseudocode — real example is in CLIENT.md
async with ws_connect(conv_stream_url, headers={"X-Agent-Id": my_id}) as ws:
    await ws.send_json({"type": "start_message"})
    await ws.recv()  # ack_start

    async with anthropic_client.messages.stream(...) as claude_stream:
        async for text in claude_stream.text_stream:
            await ws.send_json({"type": "token", "content": text})
            await ws.recv()  # ack_token

    await ws.send_json({"type": "end_message"})
    await ws.recv()  # ack_end
```

The scaffolding is the loop that tees Claude's output to the WebSocket. ~5 lines of real logic. The agent's LLM usage is unchanged; the scaffolding is just networking glue.

### Who Writes Scaffolding

The developer (or coding agent) building an agent writes it. We write it for our demo agent; we document the pattern in `CLIENT.md`; the evaluator's Claude Code agent will write it for his client based on our docs.

### Why It Matters

- No per-token tool calls (would be catastrophically slow).
- No modification to the LLM library.
- Future-proof: when LLM APIs support streaming input, the same pattern works — the input stream is the tee point.

---

## 22. Testing Infrastructure

### Test Stack

- `pytest` + `pytest-asyncio`
- `httpx.AsyncClient` for HTTP
- `websockets` for WebSocket clients
- `httpx-sse` for SSE consumption

### S2 Lite (Primary Test Backend)

Routine tests use S2 Lite — the official MIT-licensed in-memory S2 emulator distributed as a Docker image. The `s2-sdk` package points at it via the `Endpoints` construct:

```bash
docker run --rm -d -p 8080:80 --name s2-lite-test ghcr.io/s2-streamstore/s2 lite
```

```python
# In conftest.py, either set env vars and use Endpoints.from_env()...
os.environ["S2_ACCOUNT_ENDPOINT"] = "http://localhost:8080"
os.environ["S2_BASIN_ENDPOINT"] = "http://localhost:8080"
os.environ["S2_ACCESS_TOKEN"] = "ignored"

# ...or construct Endpoints directly:
from s2_sdk import S2, Endpoints
endpoints = Endpoints(account="http://localhost:8080", basin="http://localhost:8080")
client = S2(access_token="ignored", endpoints=endpoints)
```

Same SDK, same behaviors, no network, no credentials, no flakiness.

### Real S2 (Smoke Tests)

Small set of `@pytest.mark.real_s2` tests run against real S2 manually before deployment. Verifies S2 Lite's behavior matches production.

### Database

In-memory SQLite (`:memory:`) per test. No file cleanup needed.

### Test Agents

Always scripted, never Claude-powered. Hardcoded text. Determinism and speed matter.

### Coverage

61 integration test scenarios covering:
- Basic flow (register, create, invite, list, leave, send/read)
- Streaming writes (happy path, all error cases, size limit, double-connection rejection, ack ordering, sequence_in_message preservation)
- Streaming reads (historical, live, from=0, from=latest, stored position, batched updates, clean close, transition historical→live, non-member)
- Concurrent writers (interleaving, attribution, multi-reader fan-out)
- Offline/reconnect (catch-up, partial read, during concurrent write)
- Crashes (client-crash abandonment, partial token preservation, visibility to readers)
- Server-crash (orphan detection)
- Group conversations (5 agents, 3 concurrent writers, invite mid-conversation)
- Leave (own WebSocket, own SSE, doesn't affect others, rejoin sees history)
- Demo agent endpoints (PUT with/without token, GET before/after)
- Attribution (every record has correct agent_id, self-messages delivered)
- Message structure (unique IDs, monotonic sequence_in_message, monotonic stream sequence)

---

## 23. Deployment Architecture

### Fly.io Container

Base: `python:3.11-slim`.

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .
COPY src/ ./src/
ENV DATABASE_PATH=/data/agentmail.db
ENV PORT=8080
EXPOSE 8080
CMD ["uvicorn", "agentmail.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### fly.toml Key Settings

- `auto_stop_machines = false` (long-lived connections require always-on)
- `min_machines_running = 1`
- Mount `agentmail_data` volume at `/data` for SQLite
- Healthcheck on `/health`

### Process Layout

Option B (sidecar) is default:
```toml
[processes]
  app = "uvicorn agentmail.main:app --host 0.0.0.0 --port 8080"
  demo_agent = "python -m agentmail.demo_agent"
```

Both processes in same container. Demo agent has access to the SQLite file (but doesn't touch it directly — uses the public API via `DEMO_AGENT_BASE_URL=http://localhost:8080`).

Option A (separate Fly app) also supported because `PUT /demo-agent` works across network.

### Secrets

Set via `fly secrets set`:
- `ANTHROPIC_API_KEY` — for demo agent
- `S2_ACCESS_TOKEN` — for service's S2 client
- `DEMO_AGENT_REGISTRATION_TOKEN` — shared secret for `PUT /demo-agent`

---

## 24. Environment Variables

| Variable | Required | Where | Description |
|----------|----------|-------|-------------|
| `DATABASE_PATH` | Yes | Service | Path to SQLite file, or `:memory:` for tests |
| `S2_ACCESS_TOKEN` | Yes | Service | S2.dev access token (or `"ignored"` for S2 Lite) |
| `S2_BASIN` | Yes | Service | Basin name, e.g., `agentmail` |
| `S2_ACCOUNT_ENDPOINT` | No | Service | Override for S2 Lite: `http://localhost:8080` |
| `S2_BASIN_ENDPOINT` | No | Service | Override for S2 Lite: `http://localhost:8080` |
| `PORT` | No (default 8080) | Service | HTTP bind port |
| `DEMO_AGENT_REGISTRATION_TOKEN` | Yes | Both | Shared secret for `PUT /demo-agent` |
| `ANTHROPIC_API_KEY` | Yes | Demo agent | For Claude streaming calls |
| `DEMO_AGENT_BASE_URL` | Yes | Demo agent | Service URL, e.g., `http://localhost:8080` |
| `DEMO_AGENT_ID_FILE` | No (default `/data/demo_agent_id.txt`) | Demo agent | Persist agent ID across restarts |

---

## 25. Deliverables

Five things get delivered to the evaluator:

1. **Codebase** — complete working implementation with tests, pushed to the GitHub repo they created.
2. **`DESIGN.md`** — every design decision, reasoning, and tradeoffs. Derived from `plans/02-design-decisions.md`.
3. **`NO_RESTRICTIONS.md`** — how we'd improve with unlimited time/resources, 15 topics ordered by impact. Derived from `plans/10-no-restrictions-design.md`.
4. **`CLIENT.md`** — self-contained client documentation. When handed to Claude Code with no other context, Claude Code must be able to build a working client. Includes complete scaffolding pattern example. Derived from `plans/09-client-docs.md`.
5. **Live service** — deployed on Fly.io, reachable, with the demo agent running and responsive. Evaluator connects their own agent to it during the evaluation call.

**Validation step** before submission: open a fresh Claude Code session, give it only `CLIENT.md`, ask it to build a client that talks to the service. If it succeeds without asking questions, `CLIENT.md` passes. If it asks anything, add that information and try again. This is literally how the evaluator will test it.
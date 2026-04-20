# Architecture

## System Components

The service has three logical components running in a single Python process:

### 1. HTTP API
Handles all non-streaming operations:
- Agent registration
- Conversation creation, listing, invite, leave
- Discrete message send (for clients that don't need streaming)
- Reads and writes metadata to SQLite
- Appends discrete messages to S2 as self-contained record triples (start, single-token, end)

### 2. Stream Router (WebSocket write handler)
- Accepts WebSocket connections for streaming writes
- Validates the calling agent is a member of the target conversation
- Wraps each incoming token in a JSON envelope with metadata
- Appends records to the S2 stream for that conversation
- On disconnect: appends `message_abandoned` if a message was in progress
- Acknowledges writes after S2 confirms durable storage

### 3. Stream Tailer (SSE read handler)
- Accepts SSE connections for streaming reads
- Resolves the starting position: either from the agent's stored read position (default) or from a client-provided `from` query param
- Subscribes to the S2 stream at that position
- Forwards records to the SSE connection as JSON events
- Updates the agent's read position as records are delivered
- Handles the transition from historical replay to live tailing seamlessly

## Data Stores

### SQLite (metadata)
Tables:
- `agents(id TEXT PRIMARY KEY, created_at TIMESTAMP)`
- `conversations(id TEXT PRIMARY KEY, created_at TIMESTAMP)`
- `memberships(agent_id TEXT, conversation_id TEXT, joined_at TIMESTAMP, PRIMARY KEY (agent_id, conversation_id))`
- `read_positions(agent_id TEXT, conversation_id TEXT, sequence_number INTEGER, PRIMARY KEY (agent_id, conversation_id))`

### S2 (messages)
- One stream per conversation, named by conversation ID
- Append-only
- Records are JSON-encoded bytes
- S2 assigns a monotonically increasing sequence number to every append
- We never delete records

## Full Data Flow Example

**Scenario:** Agent A streams a message to a conversation that Agent B is tailing.

1. Agent A opens WebSocket to `wss://host/conversations/{conv_id}/stream` with header `X-Agent-Id: agent_a`.
2. Server validates: agent exists, conversation exists, agent is a member. If any fail, close connection with an error frame.
3. Agent A sends `{"type": "message_start"}`. Server generates a `message_id`, appends `{"type": "message_start", "message_id": "...", "agent_id": "agent_a", "timestamp": "..."}` to the S2 stream. Server sends back `{"type": "ack", "message_id": "...", "sequence": 42}`.
4. Agent A sends tokens: `{"type": "token", "content": "Hello"}`. Server appends `{"type": "token", "message_id": "...", "agent_id": "agent_a", "content": "Hello", "sequence_in_message": 0, "timestamp": "..."}` to S2. Acks back once S2 confirms.
5. Meanwhile, Agent B's SSE connection to `/conversations/{conv_id}/stream` is tailing the S2 stream. As each record is appended, the stream tailer reads it and forwards it to Agent B as an SSE event.

**Per-connection append serialization.** Inside the WebSocket write handler, token appends for a single connection MUST be serialized, not processed concurrently. If the handler awaits S2 appends while also reading new frames, tokens can hit S2 out of order, corrupting `sequence_in_message` and the reconstructed message. Implementation: process frames one at a time within the WebSocket handler's receive loop — receive a frame, await its S2 append, send the ack, then receive the next frame. The handler must not spawn concurrent tasks per frame. This is a correctness requirement, not a performance tuning option.
6. Agent A sends `{"type": "message_end"}`. Server appends `message_end` record, acks, closes the message (but keeps the WebSocket open for the next message if the agent wants).
7. Agent B's SSE connection receives the `message_end` record and knows the message is complete.

## Concurrent Write Handling

Multiple agents can have WebSockets open and be streaming tokens simultaneously. Each WebSocket connection is an independent task; tokens from different connections can interleave at the S2 stream level in whatever order S2 processes the appends. S2's total ordering gives us the canonical interleaved stream.

**Within a single WebSocket connection**, token appends are serialized (see "Per-connection append serialization" above) — the handler processes one frame at a time. This ensures tokens from a single message hit S2 in the order the client sent them, preserving `sequence_in_message` correctness.

**Across different WebSocket connections**, no cross-connection locking is used — S2 handles the ordering. If Agent A and Agent B are both writing, the S2 stream might look like:

```
start(A, msg_1)
token(A, msg_1, "Hello")
start(B, msg_2)
token(A, msg_1, " world")
token(B, msg_2, "Hi")
end(A, msg_1)
token(B, msg_2, " there")
end(B, msg_2)
```

Any reader can trivially de-interleave by grouping on `message_id`.

## Reconnection Flow

**Scenario:** Agent B was offline; now it reconnects.

1. Agent B opens SSE to `/conversations/{conv_id}/stream` with `X-Agent-Id: agent_b`.
2. Server looks up Agent B's read position in SQLite: sequence 128.
3. Server opens the S2 stream at sequence 129.
4. Server streams every record from sequence 129 to the current head of the stream through the SSE connection.
5. Once caught up, the same SSE connection seamlessly continues tailing for new records.
6. **Read position update policy:** the server updates the stored read position periodically during the session — after every 10 records delivered OR every 5 seconds of activity, whichever comes first — and also on clean disconnect (when the client closes the connection or the generator exits normally). The server does NOT update the read position as each individual record is delivered. This prevents the race where a record is marked read at send time but never actually processed by the client before a crash. The tradeoff is that on abrupt disconnect, the last batch of records (up to 10 or 5 seconds' worth) may be re-delivered on reconnect — but re-delivery is safe because clients must be idempotent anyway (records carry sequence numbers, clients can deduplicate if needed).

From Agent B's perspective, there's no distinction between "history" and "live" — it's just a stream of records.

## Crash Handling

**Scenario:** Agent A is streaming a message. WebSocket connection drops.

1. Server detects the WebSocket disconnect.
2. Server checks: does Agent A have an open message in this conversation (message_start without message_end)?
3. If yes, server appends `{"type": "message_abandoned", "message_id": "...", "agent_id": "agent_a", "timestamp": "..."}` to the S2 stream.
4. Any readers tailing the stream see the abandoned record and know the message won't be completed.

## Server-Crash Mid-Message (Known Limitation)

If the server itself crashes (not the client) while a message is open, the `message_abandoned` record cannot be written because the server is dead. On restart, the stream contains an orphaned `message_start` followed by some tokens, with no terminator.

Readers cannot distinguish this case from "the message is still being written" without a heuristic. To handle this cleanly, clients implementing message assembly should use a timeout: if a `message_start` is received and no new records (tokens, message_end, or message_abandoned) arrive for that `message_id` within **60 seconds**, the client should treat the message as locally abandoned — stop waiting for it, assemble whatever tokens it received as a partial message (or discard), and surface this to its own application logic.

This 60-second timeout is documented in `CLIENT.md` as the recommended handling. A server-side fix would require durably tracking open messages and emitting `message_abandoned` on startup reconciliation. This is documented as a future improvement in `NO_RESTRICTIONS.md`.

## Leave Handling

**Scenario:** Agent A leaves the conversation while it has active connections.

1. HTTP DELETE request arrives at `/conversations/{conv_id}/members/me` with `X-Agent-Id: agent_a`.
2. Server removes the membership from SQLite.
3. Server finds all active WebSocket and SSE connections **owned by Agent A for this specific conversation** (not other agents' connections, not A's connections on other conversations).
4. For each of A's open messages in this conversation, append `message_abandoned` with `reason: "leave"` to S2.
5. Close only those connections.
6. Return 200 to the HTTP request.

Other agents' connections on the same conversation are untouched.

## In-Memory State

The service maintains in-memory bookkeeping:
- **Active WebSocket writers**: map of `(agent_id, conversation_id) -> WebSocket connection`, with metadata like "current open message_id" to detect incomplete messages on disconnect.
- **Active SSE readers**: map of `(agent_id, conversation_id) -> SSE connection`, used when we need to terminate connections on leave.

This state is rebuilt on restart (all connections are closed on restart; clients reconnect).

## Durability Model

- Metadata (agents, conversations, memberships, read positions) lives in SQLite on a persistent volume.
- Message content lives in S2 (fully managed, durable by S2's design).
- No in-memory-only state is critical — restart behavior is: drop all connections, clients reconnect, resume from stored read positions.

**Consistency between stores.** Read positions live in SQLite; messages live in S2. If a SQLite write fails but the corresponding S2 read succeeded, the client might re-receive records it has already processed on reconnect. If an S2 append succeeds but the metadata operation (e.g., conversation creation) fails, we get an orphaned stream. Both cases are survivable: re-delivery is idempotent because clients can deduplicate by sequence number, and orphaned streams are harmless (they just sit unused). We don't attempt two-phase commit across these stores — the complexity isn't warranted.

**Ordering truth.** Stream sequence numbers (assigned by S2) are the canonical order of events. Timestamps are informational only — they're generated by the server at record-construction time, and server clock jumps can produce out-of-order timestamps even for records with correctly ordered sequence numbers. Clients should always order by sequence number, never by timestamp.

## Deployment

- Single container built from a Python base image.
- Deployed to Fly.io with a persistent volume for the SQLite file.
- Environment variables: `ANTHROPIC_API_KEY`, `S2_ACCESS_TOKEN`, `S2_BASIN`, `DATABASE_PATH`.
- The demo agent runs as a separate process in the same container (or as a separate Fly.io app — decide during deployment).

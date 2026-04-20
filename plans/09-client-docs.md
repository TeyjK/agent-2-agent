# Client Documentation

This plan describes the structure of `CLIENT.md`, which is the deliverable that goes to the evaluator's Claude Code agent. When the agent reads this file, it must be able to build a working client without any human help.

`CLIENT.md` must be:
- **Self-contained** — no references to external docs
- **Comprehensive** — every endpoint, every frame type, every error code
- **Example-rich** — show curl commands and Python code for every operation
- **Unambiguous** — no "depending on" or "typically" language; state exactly what happens

## Structure of `CLIENT.md`

### Section 1: What This Service Is

One paragraph. "A messaging service for AI agents. Agents are identified by server-assigned IDs and can converse in conversations. Messages can be sent as complete text (simple) or streamed token by token (real-time). All messages are durably stored."

### Section 2: The Scaffolding Pattern (Critical — Read First)

This is what most implementors will actually need, so it goes second, right after the intro. Most AI coding agents reading this doc top-down should hit this section before any reference material.

Explain that the service supports token-level streaming, but LLM APIs typically don't accept streaming input. Show the scaffolding pattern:

"When your agent generates text via an LLM API (like Anthropic or OpenAI) in streaming mode, you wrap the stream in code that pipes each token to our service as it arrives from the LLM. Your agent itself doesn't know this is happening — it just generates text normally. The wrapper code handles the networking."

Include a complete, copy-pasteable Python example that:
1. Opens a WebSocket to our service
2. Sends `start_message`
3. Starts an Anthropic streaming completion
4. For each token from Anthropic, sends a `token` frame through the WebSocket
5. Sends `end_message`
6. Closes the WebSocket

The example should be ~40 lines of clear, commented Python. Show both the sending side (streaming write) AND a minimal listener (SSE consumer with token buffering) because any real agent needs both.

### Section 3: Base URL and Agent Identity

- State the base URL (filled in at deploy time): `https://<your-app>.fly.dev`
- Explain that every request except `POST /agents` requires an `X-Agent-Id` header
- Show how to register and obtain an agent ID

### Section 4: Complete API Reference

For each endpoint: HTTP method + path, purpose in one sentence, request format (headers and body), response format (status code and body), error cases. Include working curl examples.

Endpoints:
- `POST /agents`
- `POST /conversations`
- `GET /conversations`
- `GET /conversations/{id}`
- `POST /conversations/{id}/members`
- `DELETE /conversations/{id}/members/me`
- `POST /conversations/{id}/messages`
- `GET /conversations/{id}/stream` (SSE)
- `WSS /conversations/{id}/stream` (WebSocket)
- `GET /demo-agent` (to discover the demo agent)

### Section 5: Record Format

Explain what a "record" is (a unit appended to a conversation's stream). Show the schema for each record type with a JSON example:

- `message_start`
- `token`
- `message_end`
- `message_abandoned`

Explain the common fields and the per-type fields. Explain how to group tokens by `message_id` to reconstruct complete messages. **Emphasize:** clients must order by `sequence_in_message` within a message (not by timestamp), and by stream sequence number across messages.

### Section 6: Streaming Writes (WebSocket) — Detailed Reference

Explain the WebSocket frame protocol in detail:
- Open connection with `X-Agent-Id` header (or `?agent_id=` query param) to `/conversations/{id}/stream`
- Send `start_message` frame, receive `ack_start`
- Send one or more `token` frames, each gets an `ack_token`
- Send `end_message` frame, receive `ack_end`
- Connection can be reused for multiple messages
- Only one WebSocket per (agent, conversation) at a time — opening a second returns `already_connected` error
- On error, server sends `error` frame and closes

Include a full Python example using the `websockets` library.

### Section 7: Streaming Reads (SSE) — Detailed Reference

Explain the SSE format:
- GET to `/conversations/{id}/stream` with `Accept: text/event-stream` and `X-Agent-Id` header
- Server sends historical records from agent's last read position (or `?from=` if specified)
- `from=0` replays from the beginning, `from=latest` skips history and tails only, `from=<N>` starts from sequence N
- Seamlessly transitions to live records
- Each event has the S2 sequence number as the event ID
- Errors delivered as `event: error` events

Include a full Python example using `httpx` or `aiohttp` to consume SSE.

### Section 8: Handling Incoming Messages (Message Assembly)

Explain how to listen for messages from other agents:
1. Open SSE connection to `/conversations/{id}/stream`
2. Parse each incoming event as JSON
3. Buffer tokens by `message_id`
4. When a `message_end` arrives with a matching `message_id`, the message is complete — assemble tokens into full text and process
5. When a `message_abandoned` arrives, clean up the buffer and either discard or surface as partial

**Timeout handling for server-side crashes:** if a `message_start` is received but no subsequent record (token, message_end, or message_abandoned) for that `message_id` arrives within **60 seconds**, the client should treat the message as locally abandoned — stop waiting for it, clean up the buffer, and optionally surface what was received as a partial message. This handles the edge case where the service itself crashes mid-stream and cannot emit `message_abandoned`. All clients should implement this timeout identically (60s of inactivity per open message) so behavior is consistent across the ecosystem.

Include a complete Python example of a listener with both the assembly logic and the timeout handling.

### Section 9: End-to-End Example

A full working example: a script that registers as an agent, invites the demo agent to a new conversation, sends a streaming message, and prints the streaming response. ~80 lines of Python. Runnable as-is with just the base URL and an Anthropic API key.

### Section 10: Error Codes Reference

Table of all error codes with descriptions and common causes:
- `missing_agent_id`
- `agent_not_found`
- `conversation_not_found`
- `not_a_member`
- `last_member_cannot_leave`
- `no_open_message`
- `message_already_open`
- `already_connected`
- `invalid_frame`
- `internal_error`

### Section 11: Demo Agent

How to discover the demo agent:
```bash
curl https://<your-app>.fly.dev/demo-agent
# → {"agent_id": "agent_..."}
```

And how to start a conversation with it: invite its agent ID to a conversation, then send messages. The demo agent will respond via streaming writes to the same conversation.

### Section 12: Quick Reference Cheat Sheet

A compact table of all operations with their HTTP method, path, and one-line description. For the AI coding agent to scan quickly.

---

## What Must NOT Be in `CLIENT.md`

- Internal architecture details (S2, SQLite — not relevant to clients)
- Design decisions or reasoning (those are in `DESIGN.md`)
- Implementation details about how the server works
- References to other documentation files

The client doc is strictly about **how to use the service**, not how it's built.

## Validation

The test for `CLIENT.md`: open a fresh Claude Code session, give it `CLIENT.md`, ask it to "build a client that registers an agent, creates a conversation, invites the demo agent, and has a streaming conversation with it." If it succeeds without asking questions, the doc passes. If it asks anything, add that information to the doc.

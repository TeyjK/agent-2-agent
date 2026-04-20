# Design Decisions

This document captures every design decision made for the messaging service, along with the tradeoffs considered and the reasoning behind each choice. This is also the reference for the `DESIGN.md` deliverable.

---

## Decision 1: Transport Protocol

**Decision:** HTTP REST for all non-streaming operations. SSE for streaming reads. WebSocket for streaming writes.

**Reasoning:** The primary clients are AI coding agents that need to use the service from documentation alone. HTTP is universally understood and curl-friendly — registering an agent, creating a conversation, inviting, and leaving are just standard REST calls. SSE for reads is the simplest possible streaming mechanism: it's just HTTP that stays open, with a well-known text-based event format that any language can consume. WebSocket for writes is the standard pattern for sending data to a server over time — the scaffolding wrapping the agent opens a WebSocket, pipes tokens through it, and closes when done. The tradeoff is that clients must understand three connection types instead of one, but each is simpler and more purpose-built than a single unified protocol would be. gRPC was rejected because it requires protobuf codegen and can't be used with curl, which would make it nearly impossible for an AI coding agent to use from documentation. WebSocket-for-everything was rejected because it forces even simple CRUD operations through a more complex transport. HTTP-only was rejected because chunked transfer encoding works but is awkward and less idiomatic than purpose-built streaming transports.

---

## Decision 2: Stream Architecture

**Decision:** One S2 stream per conversation. All messages from all agents in a conversation are appended to a single ordered stream.

**Reasoning:** S2 gives us total ordering for free on a single stream — whatever is appended first comes first. Readers only need to tail one stream to see the entire conversation, including all participants. This dramatically simplifies both the server implementation (no merging logic) and the client implementation (no need to reconcile multiple streams). Alternative approaches considered: one stream per agent per conversation would eliminate write contention but would require readers to merge streams and reason about ordering across them, which is significantly more complex. One stream per message would create massive overhead and require an index. The tradeoff with our choice is that concurrent writes to the same conversation compete for the same stream, but for agent conversations measured in tokens per second, this is well within S2's capabilities. If this ever became a bottleneck at scale, sharding to per-agent streams with a merge layer would be the natural evolution — documented in the no-restrictions design.

---

## Decision 3: Message Structure on the Wire

**Decision:** JSON envelope per record. Every record appended to the S2 stream is a small JSON object containing agent ID, message ID, sequence-in-message, timestamp, type (`message_start` | `token` | `message_end` | `message_abandoned`), and content.

**Reasoning:** Every record is fully self-describing — any reader can jump to any position in the stream and immediately understand what they're looking at without needing context from prior records. This is critical for reconnection (the server can resume from any sequence number), for debugging (the stream is directly inspectable), and for fault tolerance (a corrupted or skipped record doesn't invalidate everything that follows). The tradeoff is bandwidth — wrapping each token in a JSON object adds overhead relative to the token itself. For plaintext agent communication, the JSON metadata is small in absolute terms, and the operational benefits of a self-describing stream far outweigh the bandwidth cost. Raw text was rejected because it makes attribution and ordering impossible. Metadata only at message boundaries was rejected because mid-stream readers would be unable to identify what they're reading.

---

## Decision 4: Message Boundaries

**Decision:** Explicit `message_start` and `message_end` markers. Every message begins with a `message_start` record and ends with a `message_end` record, with `token` records in between. Incomplete messages get a `message_abandoned` record instead of `message_end`.

**Reasoning:** Explicit markers give unambiguous boundaries that make history replay and message reconstruction trivial. A client processing the stream always knows exactly when a message begins and ends. Crash detection is also unambiguous — a `message_start` without a matching `message_end` (and no `message_abandoned`) is definitively incomplete. The tradeoff is a small protocol obligation on clients — they must send a start before tokens and an end when done — but this is natural, and the server enforces it by rejecting tokens that arrive outside an open message. Implicit boundaries via message ID transitions were rejected because they make crash detection require timeouts. No boundaries at all were rejected because they make history replay as discrete messages impossible.

---

## Decision 5: Message Ordering

**Decision:** Total order across all records in a conversation, provided by S2's append sequence number.

**Reasoning:** Since all records for a conversation go to a single S2 stream (Decision 2), we get a monotonically increasing sequence number assigned by S2 for free. Every reader sees exactly the same order. This is the strongest and simplest ordering model — there's no ambiguity about what happened when. The tradeoff is that concurrent writers have their tokens interleaved in the order records arrive at the server, which could look messy if you tried to read the raw stream as a conversation transcript. But every token carries a `message_id` (Decision 3), so de-interleaving is a trivial client-side operation. Per-agent ordering was rejected because it contradicts our single-stream architecture and pushes complexity onto clients. Per-message ordering was rejected as strictly weaker than what we already get for free.

---

## Decision 6: Concurrent Writes

**Decision:** Allow free interleaving across different writer connections. Within a single writer connection, serialize all frame processing — one frame at a time, sequentially. Multiple agents can have open messages simultaneously. Tokens from different connections are appended to the S2 stream in the order they arrive at the server.

**Reasoning:** This directly supports the evaluator's stated goal of maximizing conversation throughput — agents can read and reason about incoming tokens while still composing their own responses. The stream becomes a faithful real-time record of everything happening in the conversation. The interleaving across connections is handled at the record level: every token carries a message ID and agent ID, so any client can cleanly group tokens by message. **Within a single connection, however, concurrent frame processing would be a correctness bug** — if the handler awaited S2 appends concurrently across frames from the same WebSocket, tokens from one message could be reordered at the S2 level, breaking `sequence_in_message`. So the WebSocket handler's receive loop must be strictly sequential: receive frame, await S2 append, send ack, receive next frame. This is the only correct approach given that `sequence_in_message` is server-assigned. Serialization was rejected at the cross-connection level because it would kill throughput for the real-time use case. Server-side buffering to produce non-interleaved output would add latency, consume memory per-agent, and break the append-only model. The tradeoff we accept is that the raw stream is not a clean human-readable transcript, but clients are programs that trivially handle grouping by message ID.

---

## Decision 7: Delivery Guarantees

**Decision:** A write is considered successful once S2 has durably stored the record and the server has received confirmation from S2. The writer is acknowledged at that point. Readers get at-least-once delivery: every record is delivered eventually, either in real time if they're connected or on reconnection replay if they're not.

**Reasoning:** Acknowledging on S2 append gives the writer a strong durability guarantee — once confirmed, the record will not be lost, even if the server crashes immediately after. This is essential for a messaging service where losing messages is unacceptable. The tradeoff is that writes wait for the S2 round-trip before being acknowledged, adding latency, but S2 is designed for low-latency appends and this should be imperceptible in the context of token-by-token streaming. Acknowledging on server receipt (before S2 persistence) was rejected because it creates a window where data could be lost after the writer was told the write succeeded. Acknowledging on reader delivery was rejected because it's incompatible with offline readers and would make streaming writes block on slow readers.

---

## Decision 8: Read Position Tracking

**Decision:** Server-side tracking with client override. The server stores each agent's last-read sequence number per conversation in SQLite. Updates happen periodically during an active SSE session — after every 10 records delivered OR every 5 seconds, whichever comes first — and on clean disconnect. Updates do NOT happen on every record. On reconnect, the server automatically resumes from the stored position. Clients can optionally pass a `from=<sequence>` or `from=latest` query parameter to override.

**Reasoning:** Server-side tracking is the most reliable — an agent that crashes and loses its in-memory state doesn't need to remember where it was. This makes the client documentation simpler. The batched update cadence (every 10 records or 5 seconds) is a deliberate correctness decision, not an optimization. Updating the position on every record delivery would create a race condition: a record could be marked read at the moment the server sends it to the socket, but the client could crash before actually processing it, and that record would be skipped on reconnect — silently losing a message. By updating periodically and on clean close, we ensure that abrupt disconnects only risk re-delivering the last batch (which is safe because clients must be idempotent anyway), not losing records entirely. The client override option (`from=0` to replay from beginning, `from=latest` to skip history, `from=<N>` to start from a specific point) provides flexibility for debugging and for agents that want to tail without catching up. The tradeoff is a small persistent store is required beyond S2, but this is just a table of `(agent_id, conversation_id, sequence_number)` tuples — trivial in SQLite. Updating on every record was rejected as a correctness bug masquerading as simplicity. Never updating (only on close) was rejected because long-lived agents would rarely disconnect cleanly, making reconnect replay unnecessarily large.

---

## Decision 9: Reconnection Behavior

**Decision:** Replay from the agent's last read position using the exact same record format as live streaming. The SSE connection delivers historical records in order, then seamlessly transitions to live tailing once caught up.

**Reasoning:** Using one code path and one format for both historical and live delivery is dramatically simpler than maintaining two. The client processes every incoming record identically — it doesn't need to know whether it's catching up or live. The tradeoff is that a long-offline agent replays every individual token record rather than receiving reconstructed complete messages, which is less bandwidth-efficient. But this preserves full fidelity (including partial messages from crashes, concurrent interleaving, everything) and keeps the system maximally simple. Adding an optional `format=messages` query parameter to return reconstructed messages would be a natural optimization at scale, documented in the no-restrictions design.

---

## Decision 10: Conversation History Format

**Decision:** History is delivered in the same format as the live stream — token-level records, same JSON envelopes, same order. Newly invited agents get access to the full history via the same SSE endpoint starting from sequence 0 (or any position they specify).

**Reasoning:** One canonical representation of a conversation. Every agent sees the same records in the same format regardless of when they joined or how they're reading. This keeps the system consistent and the API minimal — there's no separate "history" endpoint with a different format. The tradeoff is efficiency for very long conversations, where token-level replay could be slow. Message reconstruction as an optional output format is a natural future optimization but not needed for the scope of this implementation.

---

## Decision 11: Crash and Disconnect Handling

**Decision:** When a WebSocket disconnects while an agent has an open message, the server appends a `message_abandoned` record to the stream. Partial tokens remain. This handles the **client-crash** case. The **server-crash** case — where the server itself dies mid-message and can't emit `message_abandoned` — is handled client-side: clients implementing message assembly should use a 60-second inactivity timeout per open message and treat messages as locally abandoned if no record arrives for that message ID within that window.

**Reasoning:** Preserving partial tokens with an abandoned marker is honest, preserves data, respects S2's append-only model, and gives clients complete information about the failure. Discarding partial tokens was rejected because it violates append-only semantics and loses potentially useful content. Auto-closing the message was rejected because it's dishonest — the message wasn't actually completed. The server-crash case is a genuine gap we acknowledge as a known limitation rather than fully solve, because solving it server-side would require durably tracking open messages (another storage commitment) and running a reconciliation pass on startup. The client-side timeout is a pragmatic compromise: it bounds the worst-case waiting time and makes behavior predictable. Documenting the 60-second timeout in `CLIENT.md` ensures all clients handle this consistently rather than each implementing its own heuristic. The durable-tracking-and-reconciliation approach is documented as a future improvement in `NO_RESTRICTIONS.md`.

---

## Decision 12: Server-Side vs Client-Side Responsibility

**Decision:** Thin server. The service is a minimal durable ordered stream relay. It accepts records, appends them to S2, delivers them to readers, tracks read positions, and enforces membership. It does not assemble tokens into messages, does not filter, does not transform.

**Reasoning:** Keeping the server thin keeps it simple and reliable, with fewer places for bugs to hide. It also avoids making assumptions about what clients want — a real-time agent that wants to process tokens as they arrive gets that; an agent that wants complete messages groups by `message_id` in a few lines of code. The server does one thing well: durably ordered stream relay. Every other concern is pushed to the client. The tradeoff is that the client documentation must clearly explain how to group tokens and handle interleaving, but this is straightforward and actually demonstrates the thoughtfulness of the design. A thick server was rejected because it would force every client through one interpretation of what "a message" means, reducing flexibility and adding unnecessary server complexity.

---

## Decision 13: Future-Proofing for Discrete and Streamed Messages

**Decision:** Streaming-first protocol with discrete messages as a special case. A discrete message sent via `POST /conversations/{id}/messages` is internally handled by appending a `message_start` record, a single `token` record with the full text, and a `message_end` record.

**Reasoning:** One model, one code path. Every message — whether originally streamed token-by-token or sent whole — looks identical in the S2 stream and identical to every reader. This means storage, delivery, reconnection, history replay, and concurrent handling all have one implementation regardless of how the message was sent. The tradeoff is that discrete messages carry the slight overhead of start/end markers, but the overhead is trivial and the consistency benefit is enormous. When LLM APIs eventually support streaming input, nothing about our protocol needs to change — clients that were previously sending discrete messages can simply start using the WebSocket streaming path with no server changes.

---

## Decision 14: Group Conversation Semantics

**Decision:** Fan-out everything to everyone, including the sender's own tokens. Every agent tailing a conversation receives every record, regardless of who wrote it. No filtering.

**Reasoning:** One canonical stream, everyone sees the same thing. The server does no filtering, keeping it maximally simple. An agent receives its own tokens back, which serves as a built-in confirmation that the tokens were successfully persisted and are visible to everyone else. If a client wants to ignore its own messages, that's a one-line filter on agent ID. The tradeoff is slightly redundant bandwidth for the sender, but token text is tiny and the consistency benefit is significant. Exclude-self was rejected because it creates an inconsistency where each agent sees a different stream, complicating debugging and reasoning. Filtered subscriptions were rejected as added complexity for marginal benefit.

---

## Decision 15: Leave Behavior During Active Streams

**Decision:** Immediate termination of all active connections on leave. If the agent had an open message, append `message_abandoned` to the stream. Aligns with the spec's explicit requirement.

**Reasoning:** The spec explicitly states that active streaming connections are terminated on leave, so this directly implements that requirement. From other agents' perspective, a leave mid-message looks identical to a crash mid-message — same `message_abandoned` record, same partial tokens preserved. This is consistent and predictable. The tradeoff is that an agent could lose the tail of a message if it leaves at an unfortunate moment, but leaving is a deliberate action and agents should finish their messages before leaving. If they don't, the system handles it gracefully.

---

## Decision 16: Message Attribution

**Decision:** Agent ID embedded in every record — start, token, end, abandoned. Attribution is always available regardless of membership changes or stream position.

**Reasoning:** The stream is an immutable self-describing log. When an agent leaves and is re-invited, its prior messages remain attributed to it because the agent ID is baked into every record. No lookups, no reconciliation needed. The tradeoff is a few extra bytes per record for a repeated field, but this is negligible and the benefits are exactly what we need: any record can be understood in isolation, and attribution survives any membership change. Agent ID only on start was rejected because mid-stream readers would need to trace back to the start marker to identify the sender. A separate membership log was rejected as unnecessary complexity.

---

## Decision 17: Backpressure and Flow Control

**Decision:** No backpressure. Writers are never throttled or blocked by slow readers. Each reader tails the S2 stream at its own pace.

**Reasoning:** S2 is a durable log, so records are never lost regardless of reader speed — a slow reader simply has a read position that's further behind the head. This fits naturally with our architecture: the server is a relay, not a broker, and doesn't buffer records in memory per reader. S2 handles delivery. Writer throttling was rejected because it would punish fast writers and every other reader for the slowest participant, directly contradicting the throughput goals. The tradeoff is that a very slow reader could fall arbitrarily far behind with no server-side mechanism to alert it, but for the scope of this service this is acceptable. Adding staleness metrics and configurable maximum lag is documented as a future improvement.

---

## Decision 18: Storage Retention and Compaction

**Decision:** Keep everything forever. No compaction, no deletion, no retention policy. The S2 stream is an immutable append-only log that stays that way.

**Reasoning:** This is the simplest possible approach — no background processes, no state mutations, full fidelity preserved indefinitely. Any agent at any time can replay the complete history of a conversation at the token level. The tradeoff is storage growth over time, but plaintext tokens are tiny, S2 is designed for high-volume appends, and storage is cheap. For the scale of this project, this will never be an issue. At scale, a compacted materialized view of completed messages alongside the raw token stream would be a natural evolution — documented in the no-restrictions design.

---

## Decision 19: Error Handling Semantics

**Decision:** In-band error records for streaming connections; standard HTTP status codes for REST endpoints. WebSocket write errors and SSE read errors are delivered as JSON records with `type: "error"` on the same connection, then the connection closes.

**Reasoning:** In-band errors are immediate and unmissable — the client is already watching the connection for data, and errors arrive on the same channel. Since clients already check the `type` field on every incoming record (to distinguish between `token`, `message_start`, etc.), adding `error` as another type is trivial. For REST endpoints, standard HTTP status codes with a JSON error body (`{"error": {"code": "...", "message": "..."}}`) is what every developer and AI agent already expects. The tradeoff is clients must handle the error type in their record processor, but this is already part of the protocol. Out-of-band error channels were rejected as easy to miss. Connection termination without explanation was rejected as insufficiently diagnostic.

---

## Decision 20: API Design for Agent Usability

**Decision:** RESTful resource-based URLs with a hybrid accommodation for streaming. The calling agent is identified via an `X-Agent-Id` header on every request.

**Endpoints:**
- `POST /agents` — register a new agent, returns `{ "agent_id": "..." }`
- `POST /conversations` — create a conversation (requires `X-Agent-Id`), returns `{ "conversation_id": "...", "members": [...] }`
- `GET /conversations` — list conversations for the calling agent
- `GET /conversations/{id}` — get conversation metadata
- `POST /conversations/{id}/members` — invite an agent, body: `{ "agent_id": "..." }`
- `DELETE /conversations/{id}/members/me` — leave the conversation
- `POST /conversations/{id}/messages` — send a complete discrete message, body: `{ "text": "..." }`
- `GET /conversations/{id}/stream?from=<sequence>` — SSE endpoint for streaming reads (optional `from` param)
- `WSS /conversations/{id}/stream` — WebSocket endpoint for streaming writes

**Reasoning:** RESTful URLs are maximally predictable — an AI coding agent reading the docs can predict endpoints before seeing them. Standard HTTP verbs map to standard actions. The `X-Agent-Id` header is a clean way to identify the calling agent without requiring authentication (per the spec). Using the same `/conversations/{id}/stream` path for both WebSocket writes and SSE reads keeps related concepts together — the protocol prefix (wss:// vs https://) disambiguates. Errors use consistent JSON structure across REST and streaming endpoints. The tradeoff is that agents must set the header on every request, but this is a single line of code in any HTTP client and is standard practice.

---

## Decision 21: Deployment and Infrastructure

**Decision:** Single Python container deployed on Fly.io. SQLite on a persistent volume for metadata. S2 for message streams.

**Reasoning:** This is the simplest deployment that meets all requirements. One process, one file for metadata, one external service (S2). Fast to provision, cheap, trivially debuggable. SQLite is more than sufficient for the scale this project will ever see and eliminates the operational burden of a separate database. Fly.io has excellent support for WebSocket and SSE connections and handles Python apps well. The tradeoff is no horizontal scaling (SQLite is single-writer) and in-flight connections are lost on restart, but both are acceptable for this scope. At scale, switching to Postgres with a load balancer and sticky sessions would be the natural evolution — documented in the no-restrictions design.

---

## Decision 22: Testing Strategy

**Decision:** Unit tests for isolated logic + integration tests for end-to-end behavior against real S2 and real SQLite. All test agents are dumb scripted clients (no Claude calls) for determinism. The live Claude-powered demo agent is separate and not part of the test suite.

**Reasoning:** Integration tests exercise the full system and inspire confidence in correctness. Determinism matters for a reliable test suite — Claude's responses are non-deterministic, would slow tests dramatically, and would burn API credits on every run. The scripted clients exercise exactly the same code paths real LLM agents would, because our service doesn't know or care where the text comes from. Unit tests cover isolated logic like JSON parsing and sequence assignment for fast feedback during development. Key integration scenarios: basic flow, streaming write + live read, concurrent writers, offline reconnect, crash mid-message, group conversations, membership edge cases, leave during active stream. The live Claude-powered demo proves end-to-end viability in a separate, non-automated context.

# No-Restrictions Design

This plan describes the structure of `NO_RESTRICTIONS.md`, one of the required deliverables. This file answers: "How would you revisit your design decisions given no restrictions on time and resources? This is not about adding features — it's about infrastructure, reliability, scale, and the overall design quality of the core feature set you chose to implement."

The evaluator specifically called out that thoughtfulness here — especially around infrastructure, reliability, and scale — is a key evaluation criterion.

## Structure of `NO_RESTRICTIONS.md`

### Framing

Lead with: "The implementation I shipped is deliberately minimal. These are the places where, given unlimited time and resources, I would make different choices for reliability, scale, and quality of the core feature set."

### Topic 1: Per-Conversation Tailer Multiplexing

**Current state:** Each SSE reader opens its own S2 `read_session` against S2. If 10 agents are tailing the same conversation, we have 10 independent S2 subscriptions to the same stream.

**What I'd change:** A per-conversation tailer task that maintains a single S2 subscription per conversation and fans out records to all local SSE readers in-process. When the first SSE reader opens for a conversation, spin up the tailer. When the last reader closes, tear it down. All readers share the same records coming off S2, dramatically reducing S2 subscription count at scale. This also creates a natural place to add features like rate-limiting, slow-reader detection, and backpressure signaling.

**Tradeoff I'm accepting by not doing this:** With N readers per conversation, we're doing N times the S2 read work. S2 is designed to handle this, but it's wasteful and doesn't scale elegantly. For a handful of readers in demo scenarios this is fine; for a real deployment with hundreds of agents per conversation it's not.

### Topic 2: Horizontal Scalability

**Current state:** Single container, SQLite, in-memory connection registry. No horizontal scaling.

**What I'd change:** Replace SQLite with Postgres (or FoundationDB for stronger ordering guarantees across shards). Move the in-memory connection registry to Redis or similar so connections on any instance can be discovered and managed from any other instance. Put a load balancer with consistent hashing on conversation ID in front of multiple instances so WebSocket connections for a given conversation land on the same instance (for efficient SSE fan-out — one S2 tailer per conversation per instance instead of N). Add a control plane for the demo agent so it can run as a pool rather than a singleton.

**Tradeoff I'm accepting by not doing this:** A single-instance deployment caps throughput and concurrent connections. For the evaluator's demo purposes this is fine; for real production traffic it would not be.

### Topic 3: Stream Sharding

**Current state:** One S2 stream per conversation. All writers contend for the same stream.

**What I'd change:** For very high-throughput conversations (many concurrent agents writing simultaneously), shard by writer: one stream per agent per conversation. Readers subscribe to all per-agent streams for a conversation and merge records client-side (or via a server-side merge layer) using a consistent ordering key like a hybrid logical clock or a lamport timestamp. This eliminates write contention and allows per-writer throughput scaling.

**Tradeoff I'm accepting by not doing this:** Single-stream architecture is simpler and sufficient for realistic agent-to-agent throughput, but has a theoretical ceiling on concurrent write throughput per conversation.

### Topic 4: Reader Efficiency and Compacted History

**Current state:** History replay means streaming every individual token record. For a conversation with 10,000 messages and 100 tokens each, that's 1M+ records replayed sequentially.

**What I'd change:** Maintain a compacted materialized view alongside the raw stream. A background process watches for `message_end` records and writes a corresponding "complete message" record to a separate stream (or a Postgres table) containing the message_id, agent_id, timestamps, and full assembled text. Clients can request history in either format via a `format=` query parameter: `tokens` returns the raw stream, `messages` returns reconstructed messages (much faster for long histories). The live tailing endpoint always returns raw tokens for real-time fidelity.

**Tradeoff I'm accepting by not doing this:** Historical replay of long conversations is bandwidth- and latency-heavy.

### Topic 5: Delivery Acknowledgments to Readers

**Current state:** Readers get at-least-once delivery by replaying from the server-tracked read position. We update the position as records are delivered, which means the client might acknowledge receipt by virtue of the server having sent it.

**What I'd change:** Explicit reader-side acknowledgments. The server sends records, the client acks each sequence number, only then does the server advance the read position. This handles the case where the server sends a record but the client crashes before processing it — with the current design, that record might be considered "read" even though the client didn't actually process it. Adding explicit acks makes read positions reflect successful processing.

**Tradeoff I'm accepting by not doing this:** A client that crashes between receiving a record and processing it may miss that record on reconnect. For simple demos this is tolerable; for production it's a correctness issue.

### Topic 6: Backpressure and Slow Reader Detection

**Current state:** No backpressure. A slow reader just falls behind; we do nothing about it.

**What I'd change:** Track each reader's lag (current head sequence minus reader's position). Emit metrics. If a reader falls below a configured threshold (e.g., 5 minutes behind), either send a warning event on their SSE connection or disconnect them and force reconnection. Also add optional flow control on the WebSocket write side: if the server is overloaded, it can reject new `start_message` frames with a retry-after.

**Tradeoff I'm accepting by not doing this:** A broken or misconfigured reader could silently fall indefinitely behind, and a sudden write spike could overwhelm the server without any way to shed load.

### Topic 7: Observability

**Current state:** Basic logging only.

**What I'd change:** Structured logs (JSON to stdout) with correlation IDs threading through the request/connection lifecycle. Metrics via OpenTelemetry: records appended per conversation, SSE fanout latency, WebSocket connection counts, S2 append latency, SQLite query latency, reader lag distribution. Distributed tracing for multi-step operations. Dashboards on Grafana or similar.

**Tradeoff I'm accepting by not doing this:** Debugging issues in the deployed service requires reading logs by hand, and performance regressions are invisible.

### Topic 8: Schema Evolution

**Current state:** JSON records have no version field. If we add a field, clients must handle missing fields gracefully, and if we change a field's meaning, we have a breaking change.

**What I'd change:** Add a `schema_version` field to every record. Document a versioning policy: additive changes (new optional fields) don't bump the version; breaking changes bump the version. Server can emit multiple schema versions to different clients based on a capability negotiation on connection. Over time, old records remain in their original schema; new records use the new schema. Clients know which version they're receiving.

**Tradeoff I'm accepting by not doing this:** The protocol is frozen. Any changes will either break existing clients or require careful coordination.

### Topic 9: Message Size and Rate Limits

**Current state:** No enforced limits beyond what the infrastructure happens to enforce.

**What I'd change:** Per-agent rate limits (tokens per second, messages per minute, concurrent connections). Per-message size limits (max tokens per message, max token length). Per-conversation limits (max members, max messages per day). Configurable, with sensible defaults. Return proper 429 responses with retry-after headers.

**Tradeoff I'm accepting by not doing this:** A misbehaving or malicious client could exhaust server resources.

### Topic 10: Connection Reliability

**Current state:** If a WebSocket or SSE connection drops, the client must reconnect from scratch. The server might have already appended records that the client didn't acknowledge (SSE side).

**What I'd change:** WebSocket reconnection with resumption: if a writer's connection drops mid-message, allow the client to reconnect with a `message_id` and resume where it left off (instead of requiring a new message). SSE reconnection via the standard `Last-Event-ID` header so clients can reconnect with byte-perfect resumption. Implement heartbeats on both connection types.

**Tradeoff I'm accepting by not doing this:** Network blips cause full message abandonment (for writes) or full catchup cycles (for reads).

### Topic 11: Testing Beyond Happy Paths

**Current state:** Integration tests cover correctness of well-formed scenarios.

**What I'd change:** Chaos testing: randomly inject network delays, drop S2 responses, kill processes mid-request. Property-based testing with Hypothesis: generate random sequences of client operations and assert invariants (stream ordering, durability, membership consistency). Load testing with realistic traffic patterns (many small conversations, a few huge group conversations, long-tailed message sizes). Soak tests running for hours to catch leaks and slow degradation.

**Tradeoff I'm accepting by not doing this:** I'm only confident about correctness under normal conditions; rare failure modes are unverified.

### Topic 12: Demo Agent Quality

**Current state:** The demo agent handles one-at-a-time complete messages. It doesn't stream-read or process tokens as they arrive.

**What I'd change:** Make the demo agent truly stream-aware: as tokens arrive from other agents, pipe them into a running Claude inference (when that API exists) or into an embedding model that updates a summary of what's being said. Demonstrate the throughput benefits of streaming communication between agents, not just token-level delivery.

**Tradeoff I'm accepting by not doing this:** The demo doesn't fully showcase what this protocol enables — it only exercises the transport layer, not the use case.

### Topic 13: Authentication (Acknowledge, Don't Implement)

**Current state:** No authentication. Any client knowing an agent ID can act as that agent. Per the spec, this is intentional for scope reasons.

**What I'd change in a production system:** Per-agent API keys or signed tokens, rotated regularly. Scoped permissions (read-only vs read-write) per conversation. Audit logs for every action.

**Why I didn't:** The spec explicitly said not to. But in a real product this is table stakes.

### Topic 14: Durable Open-Message Tracking and Server-Side Recovery

**Current state:** When a client's WebSocket disconnects while a message is open, the server appends `message_abandoned` based on in-memory state. But when the server itself crashes mid-message, that in-memory state is lost, leaving orphaned `message_start` records with no terminator. Clients must use a 60-second timeout heuristic (documented in `CLIENT.md`) to work around this.

**What I'd change:** Durably track every open message in a dedicated SQLite table — row inserted when `message_start` is appended, row deleted when `message_end` or `message_abandoned` is appended. On service startup, run a reconciliation pass: for every row still present in the table, append a `message_abandoned` record with `reason: "server_restart"` and delete the row. This closes the orphan cleanly and eliminates the need for the client-side timeout.

**Tradeoff I'm accepting by not doing this:** An additional write on every `message_start` and `message_end` (small SQLite overhead, batched with the S2 acks). And the client-side timeout is a leaky abstraction — every client implementation has to handle it. Getting this right server-side is worth the small added complexity in a real deployment.

### Topic 15: Push-Based Membership Events

**Current state:** The demo agent polls `GET /conversations` every 5 seconds to discover when it's been invited to new conversations. This is fine for a demo but isn't the pattern you'd want for a production agent.

**What I'd change:** A per-agent SSE endpoint (`GET /agents/me/events`) that streams events when the agent is added to or removed from conversations. The agent opens one long-lived connection on startup and receives push notifications instead of polling. This eliminates polling latency (up to 5 seconds currently) and server load from constant GET requests.

**Tradeoff I'm accepting by not doing this:** It's an additional API surface, and the evaluator specifically said not to add features. For a demo, polling is adequate and easier to document.

## Closing

"This list is ordered roughly by impact on the core feature set. Items 1-5 meaningfully change system behavior (per-conversation tailer multiplexing, horizontal scaling, sharding, compacted history, explicit reader acks). Items 6-11 improve operational quality (backpressure, observability, schema evolution, limits, connection reliability, better testing). Items 12-15 are about polish, completeness, and eliminating known limitations (demo agent quality, authentication, durable open-message tracking, push-based membership events). If I had a week instead of a weekend, I'd start with item 1 (tailer multiplexing) and item 5 (reader acknowledgments) because they directly address scaling bottlenecks and correctness edges in the current design. Then item 14 (durable open-message tracking) to eliminate the server-crash limitation that currently requires client-side workaround."

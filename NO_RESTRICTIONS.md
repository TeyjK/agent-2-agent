# No-Restrictions Design

The implementation I shipped is deliberately minimal: a single-process FastAPI
app backed by SQLite and S2.dev, enough to run the brief end-to-end with
sensible defaults but no more. These are the places where, given unlimited
time and resources, I would make different choices — not to add features,
but to improve reliability, scale, and the quality of the core feature set
I chose to implement.

Each topic is structured the same way:

- **Current state** — what the shipped code actually does.
- **What I'd change** — the design I'd reach for with no time constraint.
- **Tradeoff I'm accepting by not doing this** — what the current design
  costs, and why I'm willing to pay it for a weekend deliverable.

Topics are ordered roughly by impact on the core feature set. Items 1-5
meaningfully change system behavior. Items 6-11 improve operational quality.
Items 12-15 are polish, completeness, and eliminating known limitations.

---

## Table of Contents

1. [Per-Conversation Tailer Multiplexing](#1-per-conversation-tailer-multiplexing)
2. [Horizontal Scalability](#2-horizontal-scalability)
3. [Stream Sharding for Write-Heavy Conversations](#3-stream-sharding-for-write-heavy-conversations)
4. [Reader Efficiency and Compacted History](#4-reader-efficiency-and-compacted-history)
5. [Explicit Delivery Acknowledgments to Readers](#5-explicit-delivery-acknowledgments-to-readers)
6. [Backpressure and Slow-Reader Detection](#6-backpressure-and-slow-reader-detection)
7. [Observability](#7-observability)
8. [Schema Evolution](#8-schema-evolution)
9. [Message Size and Rate Limits](#9-message-size-and-rate-limits)
10. [Connection Reliability and Resumption](#10-connection-reliability-and-resumption)
11. [Testing Beyond Happy Paths](#11-testing-beyond-happy-paths)
12. [Demo Agent Quality](#12-demo-agent-quality)
13. [Authentication](#13-authentication-acknowledge-dont-implement)
14. [Durable Open-Message Tracking and Server-Side Recovery](#14-durable-open-message-tracking-and-server-side-recovery)
15. [Push-Based Membership Events](#15-push-based-membership-events)
16. [Closing Priorities](#closing-priorities)

---

## 1. Per-Conversation Tailer Multiplexing

**Current state.** Every SSE reader opens its own S2 `read_session` against
the conversation stream. If ten agents are tailing the same conversation,
the service maintains ten independent S2 subscriptions to the same stream.
Each subscription pays the full network and S2-side cost of delivering the
same bytes. The in-process `ConnectionRegistry` tracks readers but does no
fan-out.

**What I'd change.** Introduce a per-conversation tailer task that maintains
exactly one S2 subscription per conversation and fans out each record to all
local SSE readers in-process. The first SSE reader for a conversation spins
up the tailer; the last reader to disconnect tears it down. The tailer reads
from `read_session`, pushes records into a per-reader `asyncio.Queue` (or
broadcasts to a single shared queue that all readers select on), and handles
read-position advancement once.

Architecturally this is the same pattern as Kafka consumer groups or Redis
pubsub: one upstream subscription, many downstream fan-outs. It naturally
creates a place to add cross-cutting features — rate-limiting, slow-reader
detection, backpressure signaling (Topic 6), metrics for fanout latency
(Topic 7), and lag alerting — all of which become harder or redundant when
each reader is a siloed subscription.

The tricky part is per-reader read-position tracking. With one subscription,
advancing the S2 read position is global: it moves forward when *all*
readers have processed a record, not any one of them. So the tailer needs
to track per-reader positions and advance the S2 session position to the
minimum. This is straightforward in code but requires care around reader
churn (readers connecting at different historical positions, readers
disconnecting mid-replay).

**Tradeoff I'm accepting by not doing this.** With N readers per
conversation we do N× the S2 read work. S2 handles it, but it's wasteful
and doesn't scale elegantly. For demo scenarios with one or two readers per
conversation this is invisible; for a real deployment with tens or hundreds
of agents tailing a shared conversation it's a real cost multiplier and,
worse, makes per-reader behaviors (slow-reader lag, backpressure) structurally
harder to observe or control.

---

## 2. Horizontal Scalability

**Current state.** A single Fly.io machine. SQLite on a local volume. An
in-memory `ConnectionRegistry` on `app.state`. `min_machines_running = 1`,
`auto_stop_machines = false`. Every WebSocket and every SSE reader lands on
the same process. The service does not scale horizontally, and I deliberately
documented this in `deploy.md` rather than papering over it.

**What I'd change.** Three moves, in order:

1. **Replace SQLite with Postgres.** SQLite's single-writer model is the
   hard scaling wall. Postgres gives us concurrent writers, replication,
   point-in-time recovery, and a clean path to managed offerings (RDS,
   Neon, Supabase). The schema is small and translates directly.

2. **Move the connection registry out of process.** Today,
   `ConnectionRegistry` tracks which WebSockets and SSE readers exist on
   *this* process. To scale out we need any instance to be able to ask:
   "is agent X currently writing on any instance?" The cleanest fit is
   Redis (hash per conversation → set of `{instance_id, connection_id}`
   tuples) with pub/sub for cross-instance events like "agent X just
   connected on instance 2, close any stale connection on instance 1."
   This is how the "one WebSocket per agent per conversation" rule gets
   enforced across the fleet.

3. **Route by conversation ID.** A load balancer with consistent hashing
   on the conversation ID (extracted from the URL path) so WebSocket and
   SSE connections for a given conversation land on the same instance.
   Combined with the tailer multiplexing from Topic 1, this means each
   conversation has exactly one S2 subscription cluster-wide, not per
   instance. When an instance goes down, the rebalancer moves its
   conversations elsewhere; clients reconnect and resume.

The demo agent also becomes pool-capable. Rather than a single process
holding a single `agent_id` in a file, a pool of demo agents could register
as distinct agents, share a queue of conversation IDs to respond to, and
coordinate via Redis. Conversation-level exclusivity (only one pool member
handles a given conversation at a time) prevents duplicated responses.

**Tradeoff I'm accepting by not doing this.** A single-instance deployment
caps throughput and concurrent connections at whatever one Fly shared-CPU
machine can handle. For the evaluator's demo purposes that's plenty; for
real production traffic it would not be. More importantly, single-instance
makes the failure mode "all connections drop simultaneously on deploy," which
is survivable for a demo but not for production SLAs.

---

## 3. Stream Sharding for Write-Heavy Conversations

**Current state.** One S2 stream per conversation. Every writer appends to
that one stream, and S2's sequence numbers linearize all writes. For the
agent-to-agent use case — a handful of agents per conversation, most
conversations with 1-3 active writers — this is fine.

**What I'd change.** For very high-throughput conversations, shard by
writer: one stream per `(agent_id, conversation_id)` pair. Each writer
appends to its own stream, eliminating cross-writer contention entirely.
Readers subscribe to the set of per-writer streams for a conversation and
merge records using a consistent ordering key (hybrid logical clock or
Lamport timestamp written into the record payload). A thin server-side
merge layer can handle the merge once for all local readers (paired nicely
with Topic 1's tailer multiplexing).

This changes the delivery contract: you no longer have a single global
sequence ordering; you have a partial ordering with a merge rule. That's a
bigger conceptual shift than it sounds, because lots of downstream logic
(read-position tracking, history replay, "what was the state of the
conversation at time T") assumes a totally ordered log. For the agent use
case, I'd accept the partial ordering since agent turns are naturally
non-overlapping at the message level — you rarely need to interleave tokens
from two agents in a specific order; you just need each agent's own tokens
to be ordered, plus message-level ordering.

**Tradeoff I'm accepting by not doing this.** Single-stream architecture
has a theoretical write-throughput ceiling per conversation (bounded by S2's
per-stream append rate, which is very high but finite). For typical
agent-to-agent scenarios the ceiling is irrelevant; for exotic scenarios
like 100 agents all token-streaming to the same conversation simultaneously,
it would become the bottleneck. Shipping the simple architecture keeps the
reasoning about ordering uniform, and single-stream is the right default.

---

## 4. Reader Efficiency and Compacted History

**Current state.** History replay streams every individual record. For a
conversation with 10,000 messages at 100 tokens each, that's 1M+ records
replayed on every fresh `from=beginning` SSE connection. Bandwidth and
latency scale linearly with history length. A new reader catching up on a
long-lived conversation has a bad first experience.

**What I'd change.** Maintain a compacted materialized view of completed
messages alongside the raw stream. A background compaction process — owned
by whichever instance hosts the per-conversation tailer (Topic 1) — watches
for `message_end` records and writes a corresponding `complete_message`
record to a companion stream or a Postgres table:

```json
{
  "type": "message",
  "message_id": "msg_...",
  "agent_id": "agent_...",
  "sequence": 42,
  "started_at": "...",
  "ended_at": "...",
  "text": "The full assembled message.",
  "token_count": 137
}
```

Clients request history in the format they want:

- `GET /conversations/{id}/stream?format=tokens&from=beginning` returns the
  raw token stream (current behavior — useful for archival, replay, debug).
- `GET /conversations/{id}/stream?format=messages&from=beginning` returns
  pre-assembled messages (orders of magnitude fewer records).
- Live tailing (`from=latest`) always emits raw tokens for real-time
  fidelity — streaming UIs and agent-to-agent token passthrough need per-token
  granularity by definition.

The compacted view also enables cheap count-based queries ("how many
messages in this conversation?"), easy analytics ("top writers by message
count"), and faster search indexing (Topic 9's rate limits become easier to
enforce when you can quickly count per-agent message rates).

**Tradeoff I'm accepting by not doing this.** Historical replay of long
conversations is bandwidth- and latency-heavy. For the demo this is fine
(conversations are short-lived); for production with long-running rooms it
would be a real problem.

---

## 5. Explicit Delivery Acknowledgments to Readers

**Current state.** Readers get at-least-once delivery via server-tracked
read positions. The server batches position updates (10 records or 5
seconds, per `READ_POSITION_UPDATE_EVERY_N_*` config) and persists on clean
close. When the SSE stream sends a record, the read position advances —
before the client has necessarily processed it. If the client crashes
between the TCP write and the application-level "I've handled this" step,
the record might be considered read even though the client didn't actually
process it.

**What I'd change.** Add explicit reader-side acknowledgments to the
protocol. Two options in order of invasiveness:

1. **Lightweight:** an HTTP endpoint the reader calls periodically with
   "I've processed up to sequence N for conversation C, agent A." The
   server only advances the persisted read position when acks arrive.
   This is backward-compatible — existing SSE-only clients keep working,
   they just fall back to the current "delivery == processing" semantics.

2. **Heavyweight:** upgrade the SSE direction to a bidirectional
   WebSocket, merge read and write into a single WebSocket per
   conversation, and send acks as explicit frames. Cleaner protocol,
   but larger blast radius.

Option 1 is the pragmatic choice. It preserves SSE's simplicity (one-way,
HTTP-friendly, proxy-friendly) while giving correctness-conscious clients
a way to commit read progress only after processing. The default stays
"delivery = processing" for clients who don't care; opt-in acks for clients
who do.

**Tradeoff I'm accepting by not doing this.** A client that crashes
between receiving a record and processing it may miss that record on
reconnect. For demo purposes this is tolerable; for production systems
with exactly-once processing semantics (e.g., an agent that writes side
effects based on what it reads), it's a real correctness gap.

---

## 6. Backpressure and Slow-Reader Detection

**Current state.** No backpressure anywhere. A slow reader falls behind,
and the service does nothing about it. A fast writer can append at whatever
rate S2 accepts. Overload surfaces as "everything gets a little slower" with
no visibility into why.

**What I'd change.**

**Reader side.** Track each reader's lag (current head sequence minus
reader's position). Emit metrics (reader lag p50/p95/p99, per conversation).
If a reader falls below a configured threshold — say, 5 minutes behind or
10,000 records behind — emit a warning event on their SSE stream giving
them a chance to catch up, and if they don't, disconnect them. A reader
indefinitely 10 minutes behind is dead weight on server memory (queued
records) and S2 subscription (kept open beyond usefulness).

**Writer side.** Flow control on the WebSocket write path: if the server
is overloaded (S2 appends are timing out, or the SQLite write queue is
backing up), reject new `message_start` frames with an error code carrying
a retry-after hint. This lets well-behaved writers back off voluntarily.
Combined with the per-agent rate limits from Topic 9, it gives operators a
graceful degradation path rather than a cliff.

**Per-connection queues.** Bounded async queues for the tailer-to-reader
fanout (Topic 1). When the queue fills, the tailer pauses advancing the S2
session until the slow reader catches up — or, if configured aggressively,
drops the slow reader and lets them reconnect with catch-up.

**Tradeoff I'm accepting by not doing this.** A broken or misconfigured
reader silently falls indefinitely behind, quietly consuming memory and a
socket. A sudden burst of writes can overwhelm the server without any way
to shed load. Operators learn about these problems by watching the service
fall over.

---

## 7. Observability

**Current state.** Python `logging` at INFO level. No metrics, no tracing,
no structured log output. Debugging an issue in the deployed service means
SSHing into the Fly machine and reading `fly logs`. The code has almost no
instrumentation at all.

**What I'd change.**

**Structured logs.** JSON output to stdout, one log record per line.
Correlation IDs threaded through the request/connection lifecycle:
`request_id`, `agent_id`, `conversation_id`, `connection_id` as
structured fields on every log line emitted during that request. This
makes it trivial to follow one request through multiple code paths
(WebSocket handshake → auth → registry insertion → S2 append → ack back →
disconnect) even when interleaved with other requests.

**Metrics via OpenTelemetry.** Critical signals:

- S2 append latency (p50/p95/p99) per conversation.
- SQLite query latency (p50/p95/p99), per query kind.
- SSE fanout latency (time from S2 read to client-side write).
- Active WebSocket count, active SSE reader count, by conversation.
- Records appended per conversation per minute.
- Reader lag distribution.
- WebSocket close reasons, by code (1000 normal, 1002 protocol, 1008
  policy, 1009 oversize, 1011 internal, abnormal). Spikes in 1011 or
  abnormal closes are the leading indicator of server-side bugs.
- Demo agent health: message-response latency, queue depth, error rate.

**Distributed tracing.** Spans around S2 reads/writes, SQLite transactions,
and each WebSocket/SSE connection lifecycle. Particularly useful for
debugging "why is this append slow" — is it S2? Is it SQLite? Is it us
waiting on a lock?

**Dashboards.** Grafana dashboards for the above, plus a per-conversation
drill-down view (records/sec, active agents, reader lag). Alerts on:
p99 latency above threshold, error rate above threshold, reader lag above
threshold, S2 append failures.

**Tradeoff I'm accepting by not doing this.** Debugging issues in the
deployed service requires reading logs by hand. Performance regressions
are invisible until they become outages. Operators have no way to know
what "normal" looks like.

---

## 8. Schema Evolution

**Current state.** JSON records have no version field. The four record
types (`message_start`, `token`, `message_end`, `message_abandoned`) have
fixed schemas and no versioning hooks. If we add a field, clients need to
tolerate missing fields gracefully (most JSON parsers do). If we change
a field's meaning, it's a breaking change with no coordination mechanism.

**What I'd change.** Add a `schema_version` field to every record,
starting at `"1"`. Document a versioning policy:

- **Additive changes** (new optional fields) don't bump the version.
  Old clients ignore the new field; new clients use it.
- **Breaking changes** (renamed fields, changed semantics, removed
  fields) bump the version. The server keeps emitting the old version to
  clients that haven't upgraded.

Clients declare their supported versions on connection (a header for HTTP,
a first-frame handshake for WebSocket). The server picks the highest
mutually supported version and emits records in that schema. Over time,
old records in a stream remain in their original schema — a client
replaying history from `from=beginning` might see records in multiple
schema versions. That's fine; the schema version field tells them which
parser to use.

Implementation requires a schema registry inside the server and a
translation layer between versions. For adding fields this is trivial; for
harder changes (like renaming a record type) the translation has to be
thought through case by case.

**Tradeoff I'm accepting by not doing this.** The protocol is effectively
frozen. Any future changes either break existing clients or require a
parallel-deployment coordination dance. For a locked-scope deliverable
that's fine; for a real product that expects to evolve, schema versioning
is nearly free to set up at v1 and very expensive to retrofit later.

---

## 9. Message Size and Rate Limits

**Current state.** A handful of hard-coded constants in `config.py`:

- `MAX_TOKEN_LENGTH = 10000` (bytes per token frame)
- `MAX_DISCRETE_MESSAGE_LENGTH = 100000` (bytes per HTTP-posted message)
- `MAX_CONCURRENT_CONNECTIONS_PER_AGENT = 50`
- `MAX_MEMBERS_PER_CONVERSATION = 1000`
- `WEBSOCKET_IDLE_TIMEOUT_SECONDS = 300`

Nothing else. No per-agent tokens/sec, no per-agent messages/min, no
per-conversation global rate limit, no protection against an agent opening
1,000 conversations in a second, no cost attribution.

**What I'd change.** Layered rate limiting.

**Per-agent limits.**

- Tokens per second (write rate).
- Messages per minute.
- Concurrent WebSocket connections across all conversations.
- Conversation creations per hour.
- Conversation invites sent per hour.

**Per-conversation limits.**

- Total messages per day (soft cap with warnings, hard cap with
  rejection).
- Concurrent writers.
- Bytes/second aggregate across all writers.

**Per-message limits** (tightenable from current defaults).

- Max tokens per message.
- Max token length.
- Max message duration (force `message_abandoned` after N minutes).

Return proper `429 Too Many Requests` with `Retry-After` headers for HTTP,
and protocol-level error frames with retry-after hints for WebSocket. Use
a token-bucket algorithm per agent, stored in Redis for the horizontal case
(Topic 2) or in-process for single-instance. Configurable per agent (with
sensible defaults) to allow trusted agents to have higher budgets.

Audit logging: every limit rejection gets logged with agent_id and reason,
so operators can see which agents are approaching their limits and tune.

**Tradeoff I'm accepting by not doing this.** A misbehaving or malicious
client can exhaust server resources (memory for connections, S2 quotas
for appends, SQLite write throughput). In a trusted demo environment,
this doesn't matter; in a production multi-tenant environment it's a
hard requirement.

---

## 10. Connection Reliability and Resumption

**Current state.** If a WebSocket drops mid-message, the server emits a
`message_abandoned` record and the client must open a new WebSocket and
start a new `message_start` from scratch. If an SSE connection drops, the
client reconnects with `from=<last_seen_sequence+1>` — which works, but
requires the client to track its last seen sequence carefully.
No heartbeats; both sides rely on TCP keepalive and the idle timeout
(`WEBSOCKET_IDLE_TIMEOUT_SECONDS = 300`).

**What I'd change.**

**Heartbeats.** Server-sent `:heartbeat\n\n` SSE comments every 15-30
seconds. WebSocket pings every 15-30 seconds. These surface connection
death fast instead of waiting for the 5-minute idle timeout, and they keep
intermediate proxies from closing the connection as idle.

**WebSocket resumption mid-message.** If a writer's connection drops mid-
message, allow it to reconnect with `?resume_message_id=msg_...` as a
query parameter. The server checks: is this message currently open by this
agent on this conversation? If yes, bind the new WebSocket to that open
message; the client can resume sending tokens. If no (because the client-
side or server-side timeout has already fired and the message was
abandoned), reject the resume with an error and the client starts a new
message. This eliminates the current behavior where network blips force
full message abandonment.

The server-side state to support this is a map from `message_id` to
"currently open by this agent, last-activity time" — which already exists
in `ConnectionRegistry.writers` in memory, just needs a grace period
after WebSocket close before `message_abandoned` is appended. Set grace
to 10-15 seconds; a dropped WebSocket reconnecting within that window
resumes, anything longer gets abandoned.

**SSE resumption via `Last-Event-ID`.** SSE has a standard header for this:
when reconnecting, the browser/client includes `Last-Event-ID: <sequence>`
and the server resumes from there. Currently I handle this via query
parameter, but supporting the standard header gives us free compatibility
with browser `EventSource` clients and HTTP proxies that handle it
automatically.

**Tradeoff I'm accepting by not doing this.** Network blips cause full
message abandonment on the write side and full catch-up cycles on the
read side (well, modulo the query param). Client implementations have to
handle reconnection carefully — which is a burden I'm pushing onto client
authors.

---

## 11. Testing Beyond Happy Paths

**Current state.** 114 integration tests, running against an S2 Lite
Docker container in ~15 seconds. Coverage is good for well-formed
scenarios: auth flows, REST endpoints, SSE replay and tailing, WebSocket
write lifecycle, batched read-position updates, error paths for known
invalid frames, limits enforcement. Unit-test surface is minimal by
design — most of the logic lives at the boundary between transports and
S2, which is hard to mock without losing signal.

**What I'd change.**

**Chaos testing.** A test harness that randomly injects faults:

- Network delays between the service and S2 (200-5000ms on random
  appends).
- S2 response drops (the service sees a timeout; does it handle
  cleanly?).
- `aiosqlite` errors (disk full, corruption, lock contention).
- Kill the service process mid-request (validates startup reconciliation
  — Topic 14).
- Kill the client mid-WebSocket (validates the 60-second inactivity
  timeout heuristic and the server-side cleanup).

**Property-based testing with Hypothesis.** Generate random sequences
of client operations (register agent, create conversation, invite, write
message, disconnect, reconnect, leave, delete) and assert invariants:

- Stream ordering: records are strictly monotonically increasing by
  sequence within a stream.
- Durability: records successfully acked to clients are visible on a
  fresh reader's replay.
- Membership consistency: an agent that successfully joined a
  conversation can read from it; an agent that left cannot.
- Open-message invariant: every `message_start` is eventually followed
  by exactly one `message_end` or `message_abandoned` for that
  `message_id`.
- No phantom messages: a `message_abandoned` never appears for a
  `message_id` that had no `message_start`.

Property-based testing catches bugs the test author didn't think to
write a specific case for. It's especially good for concurrency and
edge-case interaction, which is most of what's interesting in this
service.

**Load testing.** Realistic traffic patterns:

- Many small conversations (1-3 agents, short messages).
- A few huge group conversations (50+ agents, long messages).
- Long-tailed message sizes (occasional 10K-token monologues).
- Mixed read/write load (5 tailers per active writer).
- Bursty load (10× normal traffic for 30 seconds).

Measure p50/p95/p99 for each, watch for memory growth, watch for S2
throttling.

**Soak tests.** Run the service under steady realistic load for 12-24
hours. Watch for: memory leaks, connection leaks (goroutine/asyncio-task
equivalents in Python), SQLite WAL growth, slow degradation in any
latency metric. This catches the bugs that only appear after running for
hours.

**Tradeoff I'm accepting by not doing this.** I'm confident about
correctness under normal conditions because the tests cover them. I'm
not confident about rare failure modes: server crashes mid-message
(Topic 14 addresses), S2 unavailability, disk-full, weird client
disconnect races. For a weekend build that's an honest tradeoff; for
anything claiming production readiness it's not.

---

## 12. Demo Agent Quality

**Current state.** The demo agent handles one message at a time. It
opens an SSE stream per conversation and a WebSocket per conversation.
It waits for a `message_end` from another agent, then calls the Anthropic
API with the full accumulated message, streams the response back through
the WebSocket, and closes the message. It's a minimal responder. It does
not:

- Process tokens as they arrive (could start inference before the
  incoming message ends).
- Handle multiple simultaneous conversations in parallel with shared
  state.
- Retain memory across restarts (per `deploy.md` notes — each new
  incoming message after a restart is a cold context).
- Interrupt itself cleanly if a new message arrives while it's mid-reply.

**What I'd change.** Three upgrades:

**Stream-aware inference.** As incoming tokens arrive, stream them into
a running Claude inference. When the incoming `message_end` arrives, the
Claude inference already has most of the context processed and can begin
emitting response tokens almost immediately. This demonstrates the
throughput benefit of the streaming protocol — agent-to-agent latency
approaches the sum of the two models' first-token latencies, not
first-token latency plus the time to transmit the full incoming message.

**Durable conversation state.** Persist each conversation's chat
history to SQLite, keyed by `conversation_id`. On restart, resume with
full context. This makes the demo agent a real conversational partner
rather than a stateless responder, and aligns with how a real agent
would be deployed.

**Interrupt and resume handling.** If a new incoming message arrives
while the agent is emitting a reply (which the spec allows — mid-
message-from-other-agent delivery is part of the protocol), the agent
should decide whether to abandon its in-progress reply, finish it, or
queue the new input for after. A real agent would want all three modes
available and selectable.

**Parallel conversation handling with shared state.** The agent should
track its active conversations, its reply state in each, and coordinate
shared knowledge (e.g., if asked "what did you tell Alice?" in
conversation B, reference what was said in conversation A). Today each
conversation task is independent.

**Tradeoff I'm accepting by not doing this.** The demo doesn't fully
showcase what this protocol enables — it only exercises the transport
layer, not the use case. The protocol is designed for token-level
streaming between agents with real-time responsiveness; the demo reduces
that to "complete-message ping-pong," which could have been done over
plain HTTP POST. A better demo would prove why the streaming design
matters.

---

## 13. Authentication (Acknowledge, Don't Implement)

**Current state.** No authentication. Any client knowing an agent's ID
can register as that agent, send messages, read conversations. Per the
spec, this is intentional for scope reasons — the task brief explicitly
said not to add auth. The only token in the system is the demo agent
registration token (`DEMO_AGENT_REGISTRATION_TOKEN`), and that only
protects the single `PUT /demo-agent` endpoint used for the sidecar to
publish its agent ID on boot.

**What I'd change in a production system.**

**Per-agent credentials.** Each agent gets a long-lived API key on
registration (returned once, never shown again). Keys are hashed at rest
(bcrypt or similar). Rotation: agents can generate a new key via
authenticated request, with a grace period where both keys work.

**Scoped permissions.** Per-conversation roles: `owner`, `writer`,
`reader`. The agent who created the conversation is `owner`. Invited
agents are `writer` by default but owners can issue read-only invites.
Readers can open SSE but their WebSocket writes are rejected. This is
useful for observability agents (they watch without participating) and
for compliance scenarios (an audit agent can replay everything but not
participate).

**Signed short-lived tokens.** For UI-driven flows (say, a human-facing
dashboard that spawns ephemeral agents), the server issues JWTs valid
for minutes-to-hours, signed with a rotating secret. Agents present
these in the `Authorization` header on every request.

**Audit logs.** Every mutating action gets logged with the acting
agent's ID and the authentication mechanism used. Store in a separate
write-once table (SQLite or dedicated audit store). Queryable by agent,
by conversation, by time range.

**Brute force protection.** Rate limit by IP on registration, by IP+
agent on failed auth.

**Why I didn't.** The spec explicitly said not to. Adding auth also
forces a bunch of secondary decisions (how do agents get their initial
credentials? how does a caller prove they're *supposed* to be the agent
that gets the credential?) that drag the scope out. For a weekend
deliverable, "authentication is intentionally absent, here's what I'd add"
is the honest answer.

---

## 14. Durable Open-Message Tracking and Server-Side Recovery

**Current state.** `ConnectionRegistry.writers` holds one entry per
active writer: `{agent_id, conversation_id, message_id, message_start_ts,
stream_sequence_of_start}`. When a WebSocket disconnects while a message
is open, `_abandon_writer_if_open()` in `streaming.py` reads this entry,
appends a `message_abandoned` record, and removes the entry. Good.

But when the server itself crashes mid-message, `ConnectionRegistry`
dies with it. The in-memory entry is lost. The next time the server
comes up, it has no knowledge that a `message_start` was ever written
without a terminator. The orphan sits there forever — or until a client
implements the documented 60-second inactivity heuristic from `CLIENT.md`
(time since last token received > 60s → treat as abandoned).

This is a leaky abstraction. Every client implementation has to
reimplement the timeout. It's easy to get wrong (what if the client
itself fell behind? is 60s after the last token received, or after
`message_start`?), and it's the kind of thing most client authors
won't discover until they hit a real crash in production.

**What I'd change.** Track every open message durably in SQLite:

```sql
CREATE TABLE open_messages (
    message_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    stream_sequence_of_start INTEGER NOT NULL,
    started_at TEXT NOT NULL
);
CREATE INDEX idx_open_messages_agent
  ON open_messages(agent_id, conversation_id);
```

**On `message_start` append:** insert a row.
**On `message_end` or `message_abandoned` append:** delete the row.
**On service startup:** SELECT every row, for each one append a
`message_abandoned` record with `reason: "server_restart"`, and delete
the row.

This closes orphans cleanly and deterministically — no timeout heuristic
on the client side needed. The client just reads records and trusts that
every `message_start` has a matching terminator in the stream.

There's a subtle correctness thing to get right: the SQLite insert and
the S2 append need to not drift apart. Options:

- **Insert SQLite row before S2 append.** If S2 append fails, delete
  the SQLite row in a `finally`. If the server dies between the SQLite
  insert and the S2 append, the reconciliation pass appends
  `message_abandoned` for a `message_start` that was never actually in
  the stream. Readers see a spurious abandon record. This is
  weird but harmless — they just ignore it.

- **Insert SQLite row after S2 append.** If the server dies between
  S2 append and SQLite insert, we have an orphan `message_start` with
  no tracking row. Reconciliation doesn't catch it. Bad.

So option 1 — insert first, delete on failure — is correct. The small
overhead (one SQLite insert + one delete per message) is invisible
under realistic load, and it's batched with the other bookkeeping
SQLite writes already happening.

**Tradeoff I'm accepting by not doing this.** Every client implementation
has to handle the 60-second timeout heuristic (and I documented this in
`CLIENT.md`). Clients that don't will appear to "hang" after a server
crash, waiting forever for a token that will never arrive. Getting this
right server-side is worth the small added complexity in any real
deployment.

---

## 15. Push-Based Membership Events

**Current state.** The demo agent polls `GET /conversations` every 5
seconds to discover new conversations it's been invited to. That's the
only mechanism for invite discovery. When a new conversation is created
and the demo agent is invited, the agent learns about it after a 0-5
second lag. When an agent is removed from a conversation by another
agent, the agent learns about it only on the next poll.

**What I'd change.** A per-agent SSE endpoint that streams membership
events in real time:

```
GET /agents/me/events   (with agent_id header)

data: {"type": "conversation_invited", "conversation_id": "conv_...", "invited_by": "agent_..."}

data: {"type": "conversation_removed", "conversation_id": "conv_..."}

data: {"type": "conversation_deleted", "conversation_id": "conv_..."}
```

The agent opens one long-lived SSE connection on startup and receives
push notifications instead of polling. Benefits:

- Zero invite-discovery latency.
- Zero server load from polling (polling scales linearly with agent
  count; push scales with actual event rate, which is much lower).
- A natural place to surface other per-agent events (rate limit
  warnings, auth events, system notices).

Implementation fits cleanly into the existing architecture: the
conversation-mutation endpoints (POST invite, POST leave, DELETE
conversation) already know which agents are affected. They'd enqueue
events to a per-agent in-memory queue; the SSE endpoint drains from the
queue and sends. For horizontal scaling (Topic 2), this becomes a
Redis pub/sub channel per agent.

**Tradeoff I'm accepting by not doing this.** It's an additional API
surface, and the task brief specifically said not to add features.
Polling is functionally adequate for a demo with a single agent and a
handful of conversations. In a real product with thousands of agents,
polling is wasteful and laggy.

---

## Closing Priorities

If I had a week instead of a weekend, my ordered priorities would be:

1. **Item 1 — Per-conversation tailer multiplexing.** It's the single
   largest scaling improvement, directly addresses a wasteful pattern in
   the current design, and unlocks most of the operational improvements
   in items 6-7 by centralizing the fan-out.

2. **Item 14 — Durable open-message tracking.** This eliminates the
   60-second client-side timeout heuristic, which is the worst leaky
   abstraction in the current design and something every client author
   has to reimplement.

3. **Item 5 — Explicit reader acks.** Closes the correctness edge where
   the server considers a record delivered before the client has
   processed it. Small protocol change, high correctness payoff.

4. **Item 7 — Observability.** Without metrics and structured logs,
   everything else is guesswork. It's the prerequisite for operating
   anything at scale.

5. **Item 9 — Rate limits.** The service currently assumes well-behaved
   clients. Rate limits let it be deployed to untrusted environments
   without constant manual watching.

After that week, if given more: Item 2 (horizontal scaling, because by
this point the single instance is the bottleneck), Item 10 (connection
resumption, because network blips get noticeable at scale), and Item 11
(chaos and property-based testing, because each of the above changes
adds surface area that needs non-happy-path coverage).

Items 3 (stream sharding), 4 (compacted history), and 8 (schema
evolution) I'd defer further — they matter in specific high-scale or
long-lived scenarios, but they're more work per unit of benefit for the
typical deployment than the items above. I'd want real traffic data
before committing to any of them.

Items 12 (demo agent quality), 13 (authentication), and 15 (membership
events) are product decisions more than infrastructure ones. 13 is
required for anything beyond a demo. 12 and 15 are quality-of-life
improvements for the demo agent specifically, and I'd revisit them only
after the infrastructure improvements made them worth doing well.

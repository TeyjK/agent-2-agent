# AgentMail: Design Overview

This document explains how AgentMail is built and why. It's written for the
evaluator reviewing the submission — not for a developer building a client
(that's [CLIENT.md](CLIENT.md)) and not for an exhaustive runtime reference
(that's [SYSTEM.md](SYSTEM.md)).

The thesis: a production-quality agent-to-agent messaging service can be
built on a durable append-only log, a thin metadata database, and a single
stateless relay process, using three well-chosen transports (HTTP, SSE,
WebSocket) and exactly four record types. The design gets almost everything
interesting "for free" from the underlying log (ordering, durability, replay,
live tailing), which means the service itself stays small and obvious.

---

## Table of Contents

1. [What it is](#1-what-it-is)
2. [Architecture](#2-architecture)
3. [Why S2](#3-why-s2)
4. [Why SQLite](#4-why-sqlite)
5. [Why SSE for reads and WebSocket for writes](#5-why-sse-for-reads-and-websocket-for-writes)
6. [Key design decisions](#6-key-design-decisions)
7. [Testing strategy](#7-testing-strategy)
8. [Tradeoffs taken](#8-tradeoffs-taken)
9. [Out of scope](#9-out-of-scope)

---

## 1. What it is

AgentMail is a messaging service for AI agents. Clients (programs, typically
wrapping an LLM) register as agents, join conversations, and exchange
messages. Messages can be sent either as complete discrete payloads (one
HTTP POST) or streamed token-by-token in real time (WebSocket). Every
message is appended to a durable, ordered log, and every member of a
conversation sees the same records in the same order, live or on reconnect.
The service has no user-visible frontend — its interface is a small HTTP,
SSE, and WebSocket API documented in [CLIENT.md](CLIENT.md).

The system solves four problems:

- **Durable storage** — conversations are append-only logs, retained
  indefinitely, replayable from any offset.
- **Real-time streaming** — token-level delivery, so agents can reason
  about each other's partial output as it's being produced.
- **Group participation** — any number of agents can share a conversation;
  all see every record; concurrent writes are supported.
- **Agent-friendly surface** — the protocol is built on transports every
  language and LLM already speaks (HTTP, SSE, WebSocket), documented in a
  single self-contained markdown file so an AI coding agent can build a
  client from it with zero human help.

---

## 2. Architecture

The system has three tiers. Clients are the top tier. The middle tier is a
single Python process. Storage is two external systems: one for metadata,
one for message content.

```
┌──────────────────────────────────────────────────────────────┐
│ Tier 1: Clients (agents)                                     │
│   Demo agent │ Evaluator's agent │ Test scripts │ curl       │
└───────────┬────────────────┬─────────────────┬───────────────┘
            │ HTTP (CRUD)    │ WebSocket       │ SSE
            │ POST/GET/…     │ (writes)        │ (reads)
            ▼                ▼                 ▼
┌──────────────────────────────────────────────────────────────┐
│ Tier 2: The service  (Python / FastAPI, single container)    │
│                                                              │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────────┐    │
│  │ HTTP routes  │  │ WS stream     │  │ SSE stream      │    │
│  │ (REST)       │  │ router        │  │ tailer          │    │
│  └──────┬───────┘  └───────┬───────┘  └────────┬────────┘    │
│         │                  │                   │             │
│         └────── app.state.connections (in-memory) ──────┐    │
│                 writers: (agent,conv) -> ActiveWriter   │    │
│                 readers: (agent,conv) -> [ActiveReader] │    │
│         ┌──────────────────────────────────────────────┘    │
│         │                                                    │
└─────────┼────────────────────────────────────────────────────┘
          │
   ┌──────┴───────────┐
   ▼                  ▼
┌────────┐    ┌───────────────────┐
│ SQLite │    │ S2.dev            │
│        │    │ one stream per    │
│ meta   │    │ conversation      │
└────────┘    └───────────────────┘
```

**Clients only talk to the service.** The service talks to both storage
systems. Storage systems never talk to each other or to clients. Within
the service, everything runs on a single asyncio event loop in a single
process; there is no cross-thread or cross-process coordination to worry
about.

### 2.1 What lives where

- **SQLite** — `agents`, `conversations`, `memberships`, `read_positions`,
  and a singleton `demo_agent_registration` row. No message content. Five
  tables total. Mounted on a persistent volume at `/data/agentmail.db`.
- **S2** — one stream per conversation, named verbatim with the
  conversation ID. Each record is a ~200-byte JSON blob representing a
  single `message_start`, `token`, `message_end`, or `message_abandoned`.
  No schema changes, no secondary indexes, no compaction.
- **Service in-memory** — the connection registry (a pair of dicts mapping
  `(agent_id, conversation_id)` to active WebSocket/SSE state) and the
  per-writer "currently-open message" counter. Lost on restart and
  reconstructed by clients reconnecting.

### 2.2 Request shapes

- REST calls (register, create/list conversation, invite, leave, discrete
  message, health, demo-agent registration and discovery) are ordinary
  FastAPI routes. Each handler reads/writes SQLite and occasionally S2,
  then returns JSON.
- SSE reads open a long-lived HTTP GET. The handler resolves a start
  sequence (from the query, the stored read position, or the tail),
  opens an S2 `read_session`, and yields each record as an SSE event.
  Read-position updates are batched (every 10 records or 5 seconds, plus
  one final write on clean disconnect) for correctness reasons explained
  below.
- WebSocket writes accept JSON frames, and for each frame the handler:
  (1) validates current state, (2) awaits the S2 append, (3) sends an
  ack, (4) loops for the next frame. This single sequential loop is the
  serialization guarantee — `sequence_in_message` never gets out of order.

---

## 3. Why S2

The central decision was picking a storage layer for the conversation
streams. S2 (https://s2.dev) is a managed, durable, append-only stream
service with these properties:

- **Monotonically increasing sequence numbers** assigned at append time.
- **`read_session` that seamlessly transitions from historical replay to
  live tail** — one code path on the service reads both, which means one
  code path on every client.
- **Multi-record atomic appends.** Three records for a discrete message
  land together, and their sequence numbers are contiguous.
- **Managed.** Nothing to operate in production.

Given those primitives, most of what a messaging service wants falls out
automatically:

| Requirement | How S2 gives it to us |
|---|---|
| Total order within a conversation | One stream per conversation, S2 assigns seq_num at append time. |
| Durability | S2 durably persists each append before acking. |
| Replay from any offset | `read_session(start=SeqNum(N))`. |
| Live tailing without custom infra | Same API, transitions when catching up to tail. |
| Fan-out to multiple readers | Each SSE connection opens its own `read_session`. S2 handles the rest. |

The first-version redundancy — N readers on the same conversation =
N independent S2 subscriptions — is acceptable at this scale and
documented in [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) as the first thing
to fix (per-conversation tailer multiplexing).

**Alternatives I considered and rejected for this use case:**

- **Postgres LISTEN/NOTIFY.** Great for sub-Kilobyte coordination events,
  but it is not a durable log. Notifications are fire-and-forget: late
  subscribers don't see past events, and a disconnect during notify means
  lost records. We'd have to build a companion `events` table for history
  replay and reimplement tailing on top of it — effectively rewriting S2
  by hand, worse.
- **Redis Streams.** Has sequence numbers and long polling, but adds
  operational load (Redis cluster, eviction, persistence tuning) for
  functionality S2 already delivers as a managed service. Also less
  durable out of the box (append-only file tradeoffs).
- **Kafka.** The right long-term answer for a much larger system. Massive
  overkill for a single-container demo; operating Kafka dwarfs everything
  else in the submission.
- **Roll-our-own log on top of S3 + SQLite.** Two weeks of work to get
  something half as good as S2, and a lot of novel surface area to get
  wrong (concurrent append ordering, fsync semantics, sequence numbering).

S2 is the clearest match for exactly what this service does: append-only
ordered logs, one per conversation, with tail-aware readers. Everything
else is a step down.

---

## 4. Why SQLite

For metadata we need a small amount of structured state: agent rows,
conversation rows, membership rows, one read-position row per agent-
conversation pair, and a singleton row for the demo agent's registered ID.
The total row count for a typical deployment is in the low thousands even
with long-lived use; the total data volume is kilobytes.

SQLite is the obvious fit here:

- **Zero operational load.** It's a file on a persistent volume. No
  daemon to run, no network, no credentials, no version drift.
- **Transactional.** `ON CONFLICT DO UPDATE` handles the "insert-or-update
  read position" path idempotently. `foreign_keys=ON` enforces referential
  integrity.
- **WAL mode** gives us safe concurrent reads alongside serialized writes,
  which is all the concurrency a single-process event loop actually needs.
- **Fast enough.** Read-position updates batch to 1 SQL statement per ~10
  records delivered; membership checks are ~1 ms. Nothing in the metadata
  path is on the critical latency budget.

**What we give up:** horizontal scaling. SQLite is single-writer per file;
you can't run two instances of the service pointing at the same file. That
is deliberately a non-goal of this submission, and
[NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 2 describes the Postgres
(or FoundationDB) migration that would replace it in a multi-instance
deployment.

**Alternatives I considered and rejected:**

- **Postgres.** Right answer for a scaled deployment, premature here. Adds
  an external dependency and credentials management for no win on a single
  machine.
- **Another append-only log (S2) for metadata.** Possible in principle but
  forces every membership check through a stream replay — wrong primitive
  for "is agent X a member of conv Y right now?" type queries.
- **In-memory only.** Cannot survive restart. Read positions would reset;
  demo-agent registration would be lost.

---

## 5. Why SSE for reads and WebSocket for writes

Reads and writes have asymmetric requirements, so they get asymmetric
transports.

### Reads are bulk replay + continuous tail

A reader's needs are:

- Deliver a backlog of records (possibly many, possibly zero).
- Then deliver new records as they appear, indefinitely.
- One-way. The reader never writes.
- Survive intermediary proxies (Fly's load balancer, corporate proxies,
  etc.).

**SSE** is the simplest protocol that satisfies this: plain HTTP with a
`Content-Type: text/event-stream`, a long-open response body, events
separated by blank lines, built-in reconnect semantics with
`Last-Event-ID`. Any language can consume it. `curl -N` can debug it. No
handshake, no framing protocol, no upgrade. The cost of adding WebSocket
complexity to the read path is all overhead for no win — SSE is already a
superset of the read needs.

### Writes are a bidirectional protocol with per-token acks

A writer's needs are:

- Send structured frames (`message_start`, `token`, `message_end`) in a
  specific sequence.
- Receive acks after each frame is durably persisted, so the client knows
  when it's safe to proceed to the next frame.
- Detect mid-stream errors immediately (`invalid_frame`, `protocol_error`,
  `already_connected`) without waiting for an HTTP response to complete.
- Optionally reuse the same connection for many messages back-to-back.

**WebSocket** fits: full duplex, framed, natural home for a
request/response sub-protocol within a single long-lived connection.
The per-frame ack pattern is also what the demo agent needs to implement
the **scaffolding pattern** — it cannot stream the LLM's tokens faster
than S2 durably stores them without risking data loss if the socket drops.

### Why not one protocol for both?

- **WebSocket for everything.** Would force every trivial CRUD operation
  through a stateful bidirectional protocol. Awkward for `curl`, harder
  to document, harder for an AI agent to build against.
- **SSE for everything.** Can't carry per-frame acks cleanly; can't signal
  mid-stream errors back to the client without inventing a side channel
  (another POST to "cancel my open message"?). Half-duplex is the wrong
  shape for streaming writes.
- **Chunked HTTP POST for writes.** Works, but the response body can't
  interleave with the request body in any standard way — so per-token
  acks aren't feasible. You'd get one ack at the end of the whole
  message, which removes the point of streaming.

Each protocol does the job it's designed for. The cost — three protocols
in the docs instead of one — is paid once in CLIENT.md and never again.

---

## 6. Key design decisions

This section summarizes the handful of decisions that most shape the
system's behavior. For the full list of 22 decisions (including things
like message attribution, discrete-message modeling, retention, and
authentication-scope) see `plans/02-design-decisions.md` in the repo.

### 6.1 One stream per conversation (not per agent, not per message)

S2 provides total order per stream, and conversations need a total order
across all their messages. One stream per conversation gives us that
directly, with no merge logic on the server or client. Alternatives:

- Per-agent-per-conversation streams would avoid write contention but
  force readers to merge across streams with a cross-stream ordering key.
  Significantly more client complexity.
- Per-message streams create absurd overhead and require an index to find
  messages.

The only thing we give up is per-conversation write throughput, bounded
by how fast S2 can append to a single stream. For agents exchanging text
this is generously above the real demand.

### 6.2 Server-assigned `sequence_in_message`

Tokens within a streamed message are indexed 0, 1, 2, … The server
assigns this counter. Reasoning:

- The server already serializes frame processing per WebSocket (one
  receive → one await-append → one ack → next receive), so a monotonically
  increasing counter falls out naturally.
- If the client assigned the index, it would have to pipeline sends and
  wait for acks to determine whether its n+1th token actually got index
  n+1 or was rejected, which is a strictly worse protocol.
- It means the client's frame is dead simple:
  `{"type": "token", "content": "..."}`. No ordering field, no state to
  carry.

Server-side assignment is enforced by the per-connection sequential loop
(see section 2.2).

### 6.3 Scaffolding on the read side, not in the protocol

The protocol deliberately does not assemble messages. It ships tokens
individually. **The assembly logic is documented as a pattern clients
implement in five lines** (open dict, append on token, finalize on
`message_end`, drop on `message_abandoned`, 60-s timeout for orphans).

This is the "thin server, thin client" split:

- The server is a minimal durable relay. One thing, done well.
- Every client writes the same five lines. Documenting the pattern once
  costs less than building it into the server and harder to customize.
- A real-time agent can react to tokens as they arrive without touching
  the assembly logic. A batch agent can wait for `message_end` and ignore
  partial state.

Building assembly into the server would be the wrong call: it commits the
server to an opinion about what "a message" means, which is exactly what
clients will want to override later (e.g., streaming the partial content
directly into an LLM, using tokens as summarization triggers, etc.).

### 6.4 Batched read-position updates (not per-record)

The server tracks each agent's last-delivered sequence per conversation
in SQLite, updated **every 10 records or 5 seconds** during an SSE session
(whichever comes first), plus one final write on clean disconnect.

This is the single subtlest correctness decision in the system. The
obvious alternative — update the position on every record delivery — is
a silent data-loss bug:

- Server sends record N over SSE → server immediately updates stored
  position to N+1.
- Client crashes before actually processing record N.
- Client reconnects, resumes from N+1. Record N is silently skipped.

Batched updates mean the worst case on abrupt reconnect is **replaying**
the last batch of up to ~10 records or ~5 seconds, not losing any.
Clients must already be idempotent (every record carries a stable
sequence number), so re-delivery is safe. Loss is not.

The `from=` override (`0`, `<N>`, `latest`) lets clients bypass the
stored position when they want to.

### 6.5 Ordering by sequence, not by timestamp

Every record has a timestamp and a sequence number. Readers are told, in
the CLIENT docs, to order by sequence and treat timestamps as
informational. Reasons:

- Server clock jumps happen. NTP corrections, container migrations, clock
  drift on shared virtualization — all can produce records where the
  later sequence has the earlier timestamp.
- The sequence number is the only monotonically ordered field the system
  guarantees.

This guidance is explicit in CLIENT.md section 2.3 and 11.

### 6.6 `from=latest` as a first-class option

SSE's `?from=` parameter takes integers, but it also accepts the literal
string `latest`. This returns only records appended after the connection
opens — "tail only, skip history." It is distinct from omitting the
parameter (which resumes from the stored position). Tailing agents that
don't care about the past set `from=latest` and avoid an irrelevant
history replay.

This came up specifically because the demo agent, in early iterations,
was replaying long histories on boot and confusing readers. `from=latest`
is what its SSE connections use.

### 6.7 Double-WebSocket rejection

At most one WebSocket per `(agent_id, conversation_id)`. The second
attempt is accepted just long enough to send an error and close with
`already_connected`. Reasoning: the connection registry tracks "currently
open message" per writer. Two concurrent writers on the same pair create
ambiguity we don't want to resolve on the server. If a client needs to
send two messages at once, it can interleave tokens within a single
connection (we process them sequentially and they'll get separate
message_ids) — or it can genuinely go through two different agents.

### 6.8 Leave terminates only the leaver's connections

When agent A leaves conversation X:

- Membership row is deleted.
- A's WebSocket on X (if any) gets `message_abandoned` with reason
  `"leave"` (if mid-message), then is closed.
- A's SSE on X is signalled to close.
- Other agents' connections on X are untouched. A's connections on other
  conversations are untouched.

Scoped by `(agent_id, conversation_id)` exactly. This is the minimum
invasive behavior and matches what the spec describes. It also means a
leaving agent can't accidentally disrupt an unrelated conversation or
disconnect other participants.

### 6.9 Discrete messages go through the same code path

`POST /conversations/{id}/messages` with a `text` body becomes three
records (`message_start` + one `token` with the full text +
`message_end`) appended in a single atomic S2 batch append. This means:

- Readers can't tell discrete from streamed messages apart at the
  protocol level (they look the same on the wire).
- There's exactly one code path for message storage/replay/reconstruction.
- A client that starts with discrete messages and later wants to stream
  doesn't have to migrate its readers — they already handle the streamed
  shape.

### 6.10 No authentication, no encryption beyond TLS

The spec explicitly places authentication out of scope. We take that
literally: possession of an agent ID is the only identity mechanism.
Treating this as a design choice (not an oversight) lets us keep the docs
and code simple. `NO_RESTRICTIONS.md` Topic 13 describes what real auth
would look like.

---

## 7. Testing strategy

The submission ships with **114 tests** covering every public code path
and interesting edge case. They run in ~15 seconds and pass cleanly
against **real S2 Lite**, the official in-memory S2 emulator distributed
as a Docker image by the S2 team.

### 7.1 Against real infrastructure, not mocks

- **S2 Lite (Docker)** for all integration tests. Same SDK, same API
  semantics, same append/read_session behavior — just in-memory instead
  of durable object storage. Tests exercise the real `s2-sdk` code path.
- **In-memory SQLite** (`:memory:`) per test fixture. No file cleanup
  needed; schema is set up fresh for each test run.
- **Real FastAPI + uvicorn** for the HTTP tests, real `websockets`
  client for the WebSocket tests, real `httpx-sse` for the SSE tests.

No mocks of S2, no mocks of the database, no mocks of FastAPI. The only
mocked-out dependency in the entire suite is Anthropic, because the
"test agents" in each scenario send deterministic scripted text.

### 7.2 What the tests cover

Grouped by `tests/test_*.py`:

- `test_agents.py`, `test_conversations.py`, `test_messages.py` — basic
  CRUD, authentication header behavior, membership checks, discrete
  messages, size limits, self-invite and duplicate-invite no-ops.
- `test_sse.py` — historical replay, live tailing, `from=0`, `from=latest`,
  stored-position resume, batched read-position updates, clean vs abrupt
  disconnect, the historical→live transition, non-member rejection, and
  error event delivery.
- `test_websocket.py` — happy-path streamed message, every frame-error
  case (`protocol_error`, `invalid_frame`, too-long token),
  double-connection rejection, ack ordering, `sequence_in_message`
  preservation under concurrent writers, mid-message disconnect
  (`message_abandoned` with reason `disconnect`), and the leave handler's
  reason-`leave` abandonment path.
- `test_demo_agent.py`, `test_demo_agent_integration.py` — demo agent
  registration via `PUT /demo-agent`, discovery via `GET /demo-agent`,
  registration token rejection, and an end-to-end scenario where a
  scripted agent talks to the real Claude-powered demo agent (scripted
  test agents only; Claude is only invoked by the demo agent itself).
- `test_connections.py` — the connection registry dataclass behavior.
- `test_db.py`, `test_records.py`, `test_streams.py` — lower-level unit
  tests on the storage helpers.

Notable scenarios are covered by explicit named tests:

- Live tailing (writer writes, reader sees it arrive).
- Disconnect mid-message on the WebSocket side (server emits
  `message_abandoned`).
- Leave mid-message (server emits `message_abandoned` with reason
  `"leave"` and closes the WS, then returns 204 from the DELETE).
- Leave terminates only the leaver's connections (others are unaffected).
- End-to-end Claude integration — a scripted test agent sends a message,
  invites the real demo agent, and waits for a streamed reply.

### 7.3 The fast feedback loop

Because S2 Lite runs as a Docker container with no persistence and no
network boundary, the entire suite finishes in ~15 seconds. That is the
single most important property of the test suite for iterative
development — any change that broke the wire contract or the S2 read/write
path turns red immediately.

The `@pytest.mark.real_s2` marker gates a small smoke-test-style subset
that hits real S2 (requires `S2_ACCESS_TOKEN`). It's run manually before
deployment, not on every commit.

---

## 8. Tradeoffs taken

Things I accepted the downside of, to keep the submission in scope.

- **Single-instance deployment.** One Fly.io machine, SQLite on a volume,
  in-memory connection registry. Cannot scale horizontally. For the
  evaluator demo and the bounded traffic envelope of this project, a
  single machine is ample — but it is the ceiling. The remedy
  (Postgres + Redis + consistent-hashed load balancer) is in
  [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 2.

- **Per-reader S2 subscriptions.** Each SSE client opens its own
  `read_session`. N readers on one conversation = N S2 subscriptions to
  the same stream. S2 is designed for this and handles it easily at demo
  scale, but it's wasteful at large scale. Fix: per-conversation tailer
  multiplexing — one S2 read session per conversation, fanning out in
  process to all local readers.
  [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 1.

- **Server-crash orphans.** If the server dies while an agent has an
  open message, no `message_abandoned` record gets appended — the
  in-memory tracking is lost. Clients handle this with a 60-second
  inactivity timeout documented in CLIENT.md. That's a leaky abstraction
  leaking into every client. Fix: durably track open messages in SQLite
  with a startup reconciliation pass.
  [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 14.

- **Demo agent conversation state is ephemeral per process.** The demo
  agent keeps its per-conversation Claude history in RAM. On restart, each
  conversation's SSE resumes from the stored read position — meaning the
  demo agent does *not* re-consume past tokens — so effectively the demo
  agent starts each conversation over after a restart. Scripted tests
  don't exercise long histories across restarts, and the fix (persisting
  or reconstructing history from stream replay) is documented but not
  implemented.

- **Ordering guarantees are per-conversation only.** Records from
  different conversations have no global order. This is inherent to the
  "one stream per conversation" model and not a limitation we intend to
  fix — cross-conversation ordering isn't a semantic clients ask for.

- **Acks indicate durability, not delivery to readers.** A writer knows
  that its tokens hit S2 after `token_ack`, but doesn't know whether the
  other members have received them yet. That is by design —
  writer throughput must not be coupled to the slowest reader — but it
  means we do not offer a "confirmed read" signal. Reader-side explicit
  acks would tighten the semantics ([NO_RESTRICTIONS.md](NO_RESTRICTIONS.md)
  Topic 5).

- **Read-position re-delivery on abrupt disconnect.** As discussed in
  6.4, the ~last batch of records can be re-delivered after a crash.
  Clients must be idempotent.

- **No observability beyond stdout logging.** Structured logs, metrics,
  traces are all future work. [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md)
  Topic 7.

---

## 9. Out of scope

Deliberately **not** implemented, in rough order of "why not":

- **Authentication.** Explicitly ruled out by the spec. Every endpoint
  treats the `X-Agent-Id` header as an identity claim, not a credential.
- **Rate limiting.** No token-bucket, no per-agent or per-connection
  caps beyond the hard-coded size limits (token ≤ 10,000 chars,
  discrete message ≤ 100,000 chars). A misbehaving client can flood the
  service.
- **Frontend.** Not in the brief. Everything is documented for
  programmatic clients.
- **Cross-conversation search / indexing.** No endpoint walks multiple
  streams.
- **Multi-tenant basin isolation.** All conversations live in the one
  S2 basin configured for the deployment. A multi-tenant version would
  namespace by basin.
- **Push-based membership events.** The demo agent discovers new
  conversations by polling `GET /conversations` every ~2 seconds. A
  push-based approach (`GET /agents/me/events` SSE) is sketched in
  [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 15 but not implemented.
- **Message edit / delete / reactions.** Records are immutable by design
  of the append-only log. Add only.
- **Multi-modal content.** Records carry text only. Images, tool calls,
  etc., would require either a new record type or an opaque-bytes
  `content` field with a `mime_type`. Future.
- **Conversation archiving or retention policies.** Streams retain
  forever. No TTL, no compaction.
- **Horizontal scaling.** Single instance only. See
  [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 2.
- **Observability beyond logs.** No OpenTelemetry, no metrics, no
  dashboards. See [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) Topic 7.

These are all conscious scope choices, not oversights. Each of them has
an entry in [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) describing how I'd
build it in a production system.

---

If you're evaluating this submission and want more detail on any specific
point, see:

- [CLIENT.md](CLIENT.md) — the reference I'd hand to a developer building
  a client.
- [SYSTEM.md](SYSTEM.md) — the internal spec covering every endpoint,
  record, lifecycle, and invariant.
- [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) — what I'd do differently with
  another month and no constraints.
- `plans/02-design-decisions.md` — all 22 design decisions in full, with
  rejected alternatives.

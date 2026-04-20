# Open Questions and Clarifications

This file resolves every ambiguity in the other plan files. Where a decision is made here, treat it as binding — do not second-guess. Where this file explicitly says "use your judgment," you have freedom to choose, but document what you chose and why in `DESIGN.md`.

**If anything in this file contradicts the other plan files, this file wins** — these are the latest corrections.

---

## Documentation You Must Read Before Starting

Before writing any code, read these docs. Do not skip this step — outdated assumptions will waste hours.

1. **S2 Python SDK (`s2-sdk`):** https://s2.dev/docs/sdk/python and the PyPI page at https://pypi.org/project/s2-sdk/. Note: `streamstore` is the OLD deprecated package name — do NOT use it. The current supported package is `s2-sdk` (install via `pip install s2-sdk`, import via `from s2_sdk import ...`). You need to understand: how to create a basin, how to create a stream, how to construct `S2` with custom `Endpoints` for pointing at S2 Lite, how `append` and `read_session` work, what fields are on acknowledgments (`ack.start.seq_num`, `ack.end.seq_num`), and how tailing works.

2. **Anthropic Python SDK streaming:** https://github.com/anthropics/anthropic-sdk-python/blob/main/helpers.md. You need: `AsyncAnthropic`, `client.messages.stream(...)`, and the `async for text in stream.text_stream` pattern.

3. **FastAPI WebSockets:** https://fastapi.tiangolo.com/advanced/websockets/. You need: `@app.websocket(...)`, `WebSocket.accept()`, `WebSocket.receive_text()`, `WebSocket.send_text()`, `WebSocketDisconnect`, and WebSocket close codes.

4. **sse-starlette:** https://github.com/sysid/sse-starlette. You need: `EventSourceResponse`, how to yield events, how to format them with `event`, `data`, and `id` fields.

5. **S2 REST API (fallback):** https://s2.dev/docs — if the Python SDK is missing a feature you need, you can fall back to calling the S2 REST API directly with `httpx`.

If any of these docs change the approach described in other plan files, update the relevant plan file with a note, and use the current docs as ground truth.

---

## Package and Version Pinning

Use these exact packages. The coding agent should pin compatible versions in `pyproject.toml`.

```toml
[project]
name = "agentmail"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.110",
    "uvicorn[standard]>=0.27",
    "aiosqlite>=0.19",
    "s2-sdk>=0.1.3",              # S2 Python SDK. NOT `streamstore` (deprecated), NOT `s2-py` (does not exist).
    "anthropic>=0.40",
    "sse-starlette>=2.0",
    "httpx>=0.27",                # For S2 REST fallback and tests
    "websockets>=12.0",           # For tests (client-side WebSocket)
    "pydantic>=2.0",
    "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "httpx-sse>=0.4",             # For tests to consume SSE streams
]
```

**Note on the S2 Python SDK:** The correct package is `s2-sdk` (import as `from s2_sdk import ...`). The `streamstore` package is deprecated and crucially CANNOT work against S2 Lite — it hardcodes gRPC with TLS which fails against S2 Lite's plain HTTP. `s2-sdk` supports both real S2 (HTTPS) and S2 Lite (HTTP) via the `Endpoints` construct. There is no package named `s2-py`.

---

## Resolutions for Specific Ambiguities

### Resolution 1: S2 SDK Usage Pattern

Use the `s2-sdk` Python package (import as `s2_sdk`). The patterns below were verified against the actual SDK (v0.1.3) running both against S2 Lite and validated for use with production S2.

```python
from s2_sdk import S2, Endpoints, AppendInput, Record, SeqNum, TailOffset

# --- Constructing Endpoints ---
# For production (real S2): use the default endpoints
endpoints = Endpoints.default()

# For tests (S2 Lite on localhost): use custom HTTP endpoints
endpoints = Endpoints(
    account="http://localhost:8080",
    basin="http://localhost:8080",
)

# Alternatively, Endpoints.from_env() reads S2_ACCOUNT_ENDPOINT and S2_BASIN_ENDPOINT

# --- Creating the client ---
client = S2(access_token=config.S2_ACCESS_TOKEN, endpoints=endpoints)
# For S2 Lite, access_token can be any string (e.g., "ignored") — auth is not enforced

# --- Basin and stream operations ---
# Create a basin (once per service deployment; idempotent handling needed)
try:
    await client.create_basin("agentmail")
except S2ServerError as e:
    if "already exists" in str(e):
        pass  # Fine, basin already there
    else:
        raise

basin = client.basin("agentmail")

# Create a stream (once per conversation)
try:
    await basin.create_stream(conversation_id)
except S2ServerError as e:
    if "already exists" in str(e):
        pass
    else:
        raise

stream = basin.stream(conversation_id)

# --- Append records ---
ack = await stream.append(AppendInput(records=[
    Record(body=json.dumps(record_dict).encode("utf-8")),
]))
# ack is AppendAck with:
#   ack.start: StreamPosition(seq_num, timestamp)  -- first appended record
#   ack.end:   StreamPosition(seq_num, timestamp)  -- exclusive end
#   ack.tail:  StreamPosition(seq_num, timestamp)  -- stream's new tail
# For a single-record append: ack.start.seq_num is the record's sequence.
# For a batch of N records: seq_nums are ack.start.seq_num, +1, +2, ..., +(N-1).

# --- Read a batch (one-shot) ---
batch = await stream.read(start=SeqNum(value=0))
# batch.records is list[SequencedRecord], each with .seq_num, .body (bytes), .timestamp, .headers

# --- Read session (historical + live tailing) ---
async for batch in stream.read_session(start=SeqNum(value=from_sequence)):
    for record in batch.records:
        seq = record.seq_num
        data = json.loads(record.body)
        # yield (seq, data)
# The session auto-transitions from historical replay to live tailing when it catches up.

# --- Check tail ---
tail = await stream.check_tail()  # returns StreamPosition
current_head_seq = tail.seq_num

# --- Close the client on shutdown ---
await client.close()
# Or use `async with S2(...) as client:` as a context manager
```

**Client lifecycle — long-lived via FastAPI lifespan:**

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from s2_sdk import S2, Endpoints

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Build Endpoints
    if config.S2_ACCOUNT_ENDPOINT and config.S2_BASIN_ENDPOINT:
        endpoints = Endpoints(
            account=config.S2_ACCOUNT_ENDPOINT,
            basin=config.S2_BASIN_ENDPOINT,
        )
    else:
        endpoints = Endpoints.default()

    app.state.s2 = S2(access_token=config.S2_ACCESS_TOKEN, endpoints=endpoints)
    # Ensure basin exists
    try:
        await app.state.s2.create_basin(config.S2_BASIN)
    except S2ServerError as e:
        if "already exists" not in str(e):
            raise
    yield
    await app.state.s2.close()

app = FastAPI(lifespan=lifespan)
```

---

### Resolution 2: Anthropic SDK Streaming Pattern

For the demo agent, use the async streaming helper. This is verified.

```python
from anthropic import AsyncAnthropic

client = AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)

async with client.messages.stream(
    model="claude-sonnet-4-5-20250929",  # Or whatever current Sonnet is
    max_tokens=500,
    system=SYSTEM_PROMPT,
    messages=history,  # [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
) as stream:
    async for text_delta in stream.text_stream:
        # Forward each token to our service via WebSocket
        await ws.send(json.dumps({"type": "token", "content": text_delta}))
        # Optionally wait for ack_token, or pipeline
```

Use `claude-sonnet-4-5-20250929` as the model for the demo agent unless you find a more current equivalent. It's fast, cheap, and more than sufficient for a demo conversation agent.

---

### Resolution 3: WebSocket Header vs Query Parameter for Agent ID

**Ambiguity:** Earlier plan files said `X-Agent-Id` header for WebSocket, but browsers can't set custom headers on WebSocket connections.

**Resolution:** Support both. The WebSocket endpoint accepts the agent ID via either:
- The `X-Agent-Id` HTTP header (preferred, used by proper WebSocket libraries)
- The `?agent_id=...` query parameter (fallback, used by browsers)

If both are provided, they must match, else send an error frame and close. If neither is provided, send an error frame and close.

FastAPI implementation:

```python
@app.websocket("/conversations/{conversation_id}/stream")
async def stream_write(
    websocket: WebSocket,
    conversation_id: str,
    agent_id: str | None = Query(None),
):
    # Check header as well
    header_agent_id = websocket.headers.get("x-agent-id")
    effective_agent_id = agent_id or header_agent_id

    if not effective_agent_id:
        await websocket.accept()
        await websocket.send_json({
            "type": "error",
            "code": "missing_agent_id",
            "message": "Provide X-Agent-Id header or ?agent_id= query param"
        })
        await websocket.close(code=1008)
        return

    # ... proceed with validation and main loop
```

This applies to both the WebSocket write endpoint and the SSE read endpoint (SSE is HTTP, so the header works fine — but support the query param for symmetry and for browser-based tools that can't set headers with `EventSource`).

---

### Resolution 4: In-Memory Connection Registry

**Ambiguity:** "Maintain an in-memory registry" was vague.

**Resolution:** Create a module-level singleton class, stored on `app.state.connections`. It's an in-memory object, no need for external storage. Access is async-safe because FastAPI runs in a single event loop per process.

```python
# src/agentmail/connections.py
import asyncio
from dataclasses import dataclass, field
from fastapi import WebSocket

@dataclass
class ActiveWriter:
    websocket: WebSocket
    agent_id: str
    conversation_id: str
    current_message_id: str | None = None

@dataclass
class ActiveReader:
    queue: asyncio.Queue  # records are pushed to this queue; SSE handler drains it
    agent_id: str
    conversation_id: str
    close_event: asyncio.Event = field(default_factory=asyncio.Event)

class ConnectionRegistry:
    def __init__(self):
        self.writers: dict[tuple[str, str], ActiveWriter] = {}
        # multiple readers per (agent, conversation) possible if a client opens two tabs
        self.readers: dict[tuple[str, str], list[ActiveReader]] = {}

    def register_writer(self, writer: ActiveWriter) -> None:
        self.writers[(writer.agent_id, writer.conversation_id)] = writer

    def unregister_writer(self, agent_id: str, conversation_id: str) -> None:
        self.writers.pop((agent_id, conversation_id), None)

    def get_writer(self, agent_id: str, conversation_id: str) -> ActiveWriter | None:
        return self.writers.get((agent_id, conversation_id))

    def register_reader(self, reader: ActiveReader) -> None:
        self.readers.setdefault((reader.agent_id, reader.conversation_id), []).append(reader)

    def unregister_reader(self, reader: ActiveReader) -> None:
        key = (reader.agent_id, reader.conversation_id)
        if key in self.readers:
            self.readers[key] = [r for r in self.readers[key] if r is not reader]

    def get_readers(self, agent_id: str, conversation_id: str) -> list[ActiveReader]:
        return list(self.readers.get((agent_id, conversation_id), []))
```

On leave: close all connections for that (agent, conversation) pair.

---

### Resolution 5: SSE Fan-Out Architecture

**Ambiguity:** Each SSE reader subscribes to S2. If 5 readers are on the same conversation, we have 5 redundant S2 subscriptions.

**Resolution:** For the first version, accept the redundancy — each SSE reader opens its own `read_session` against S2. This is simpler and S2 is designed for many concurrent readers. Do NOT build a fan-out layer that multiplexes S2 reads through a single subscription.

Document in `NO_RESTRICTIONS.md` that at scale you'd want a per-conversation tailer that broadcasts to all SSE readers locally, reducing S2 subscription count.

---

### Resolution 6: Read Position Update Frequency

**Ambiguity:** How often do we update read position in SQLite? The original plan said "every record, fire-and-forget" which is actually a correctness bug (records get marked read at send time, not processing time, so an abrupt client disconnect can lose records).

**Resolution:** Batched updates. Update the read position:
- After every **10 records** delivered on an SSE connection, OR
- After **5 seconds** of activity since the last update, whichever comes first
- AND on clean disconnect (when the SSE generator exits normally because the client closed the connection)

Do NOT update on every record.

**Stored value semantics:** The stored read position represents the NEXT sequence number to read, not the last sequence seen. When the SSE generator yields a record at seq N, it should persist N+1 as the read position. When resuming without a `from=` parameter, the stored value is passed directly to `streams.read_session(start=SeqNum(value=stored))` without any +1 offset.

Implementation sketch:
```python
next_seq_to_read = None
records_since_update = 0
last_update_time = time.monotonic()

async for seq, record in s2_read_session:
    yield format_sse(record)
    next_seq_to_read = seq + 1
    records_since_update += 1
    now = time.monotonic()

    if records_since_update >= 10 or now - last_update_time >= 5:
        await db.update_read_position(agent_id, conversation_id, next_seq_to_read)
        records_since_update = 0
        last_update_time = now

# On clean exit (client closed connection gracefully):
if next_seq_to_read is not None:
    await db.update_read_position(agent_id, conversation_id, next_seq_to_read)
```

The tradeoff is that clients may see up to ~10 records or ~5 seconds of re-delivery on abrupt reconnect. This is safe because clients must be idempotent anyway — records carry sequence numbers, and duplicate records are easy to detect and discard. The alternative (per-record updates) is a silent data-loss bug waiting to happen.

---

### Resolution 7: Discrete Message Atomicity

**Ambiguity:** "Atomically append three records" for discrete messages.

**Resolution:** Use S2's batch append — append all three records in a single `stream.append([start, token, end])` call. S2 will assign sequential sequence numbers to them, and the entire batch is atomic at the S2 level. This is verified from S2 docs: `Records: []AppendRecord{...}` can contain multiple records.

If the append fails, the whole discrete message fails and the client gets an error response. Partial appends are not a concern because S2's append is atomic per call.

---

### Resolution 8: Ack Pipelining on WebSocket Writes

**Ambiguity:** Does the client need to wait for `ack_token` before sending the next token?

**Resolution:** No. The client sends tokens as fast as it wants. The server processes and acks them in order. The client can ignore acks entirely if it doesn't need the sequence numbers.

However, the demo agent SHOULD wait for `ack_end` before considering the message complete (so it knows the message was durably stored). Waiting for per-token acks is unnecessary.

This means the server must:
- Process incoming frames in order
- Append to S2 in order
- Send acks in order

This happens naturally if the server uses a single async loop per WebSocket connection: receive frame → await S2 append → send ack → receive next frame.

---

### Resolution 9: S2 Stream Naming

**Ambiguity:** What exactly do we name the S2 streams?

**Resolution:** The stream name is the conversation ID, verbatim. E.g., `conv_9876543210fedcba0987654321fedcba`.

S2 stream names can contain slashes and certain characters. Since conversation IDs are alphanumeric with underscores, they are valid S2 stream names without modification.

---

### Resolution 10: ID Generation

**Ambiguity:** UUID method and format.

**Resolution:**
- Use `secrets.token_hex(16)` for agent and conversation IDs — this gives 32 hex characters, cryptographically random, and is collision-safe.
- Prefix with the type: `agent_` or `conv_`.
- Message IDs are server-generated the same way, prefixed with `msg_`.

```python
import secrets

def new_agent_id() -> str:
    return f"agent_{secrets.token_hex(16)}"

def new_conversation_id() -> str:
    return f"conv_{secrets.token_hex(16)}"

def new_message_id() -> str:
    return f"msg_{secrets.token_hex(16)}"
```

---

### Resolution 11: Test Infrastructure for S2

**Ambiguity:** How do tests use S2 without polluting production data?

**Resolution:** Use a separate test basin. Set `S2_BASIN=agentmail-test` in the test environment. At the start of each test, generate a unique stream prefix (e.g., `test_<random_hex>_conv_...`) so streams don't collide across test runs. Clean up streams after each test by calling `basin.delete_stream(stream_name)`.

If S2 has significant latency that slows tests, consider using S2's `s2 lite` in-memory emulator (mentioned in search results: run with `s2 lite` and no `--bucket` for in-memory mode). But first try against real S2 — for this project the test suite probably runs in under a minute.

---

### Resolution 12: What Counts as a Membership Check

**Ambiguity:** "Validate agent is a member" — is this every request or once per connection?

**Resolution:**
- For REST endpoints: check on every request.
- For WebSocket: check on connection upgrade only. Do not re-check on every frame. If the agent leaves while the WebSocket is open, the leave handler terminates the connection — so the only way to write without membership is if there's a race window, which is acceptable for this scope.
- For SSE: check on connection open only. Same rationale.

---

### Resolution 13: Timestamp Generation

**Ambiguity:** Where do timestamps come from?

**Resolution:** All timestamps are generated server-side at the moment the server processes the record. Use UTC with millisecond precision:

```python
from datetime import datetime, timezone

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
```

Or more simply:

```python
from datetime import datetime, timezone
def now_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
```

Don't trust client-provided timestamps — if a client ever sends one, ignore it.

---

### Resolution 14: Error Response Format Consistency

**Ambiguity:** Some places say `{"error": {...}}`, some say flat.

**Resolution:** All REST error responses use nested form:

```json
{ "error": { "code": "agent_not_found", "message": "No agent with ID agent_xxx" } }
```

All streaming error frames (WebSocket and SSE) use flat form (they're already inside a JSON object with `type: "error"`):

```json
{ "type": "error", "code": "not_a_member", "message": "..." }
```

This distinction exists because the streaming frames already have a `type` discriminator and don't need an outer wrapper.

---

### Resolution 15: Max Message Size and Connection Limits

**Ambiguity:** I said "reasonable defaults" but didn't specify.

**Resolution:** Hard-coded constants in `config.py`:

```python
MAX_TOKEN_LENGTH = 10_000            # chars in a single token
MAX_TOKENS_PER_MESSAGE = 10_000      # tokens in a single message
MAX_CONCURRENT_CONNECTIONS_PER_AGENT = 50  # total across all conversations
MAX_MEMBERS_PER_CONVERSATION = 1_000
WEBSOCKET_IDLE_TIMEOUT_SECONDS = 300  # close after 5 min of no activity
```

Enforce these server-side. Exceeding a limit is an error.

---

### Resolution 16: SQLite WAL Mode

**Important detail:** Enable SQLite WAL mode for better concurrency:

```python
async with aiosqlite.connect(db_path) as db:
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA synchronous=NORMAL")  # faster, still durable with WAL
```

Do this once on startup.

---

### Resolution 17: FastAPI Dependency for `X-Agent-Id`

Create a reusable dependency:

```python
# src/agentmail/deps.py
from fastapi import Header, HTTPException, Depends
from .db import get_agent

async def require_agent_id(x_agent_id: str | None = Header(None)) -> str:
    if not x_agent_id:
        raise HTTPException(
            status_code=400,
            detail={"error": {"code": "missing_agent_id", "message": "X-Agent-Id header required"}}
        )
    agent = await get_agent(x_agent_id)
    if not agent:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "agent_not_found", "message": f"No agent {x_agent_id}"}}
        )
    return x_agent_id

# Use in endpoints:
@router.post("/conversations")
async def create_conversation(agent_id: str = Depends(require_agent_id)):
    ...
```

Also create a dependency for conversation membership:

```python
async def require_conversation_membership(
    conversation_id: str,
    agent_id: str = Depends(require_agent_id),
) -> tuple[str, str]:
    is_member = await check_membership(agent_id, conversation_id)
    if not is_member:
        raise HTTPException(
            status_code=403,
            detail={"error": {"code": "not_a_member", "message": "..."}}
        )
    return agent_id, conversation_id
```

---

### Resolution 18: Handling FastAPI HTTPException Error Format

FastAPI wraps `HTTPException.detail` as-is in the response body. Since we pass `detail={"error": {...}}`, the response body will be `{"error": {...}}` — this is what we want. Don't wrap it further.

---

### Resolution 19: Demo Agent Conversation History Handling

**Ambiguity:** How does the demo agent assemble conversation history for Claude?

**Resolution:** The demo agent maintains an in-memory per-conversation history. When it receives `message_end` records, it assembles the full message text from the buffered tokens with the matching `message_id`, and appends to the history list. History format:

```python
history = [
    {"role": "user", "content": "message text from another agent"},
    {"role": "assistant", "content": "demo agent's own previous response"},
    ...
]
```

The role is `"user"` for messages from any other agent, `"assistant"` for the demo agent's own messages. This matches what the Anthropic API expects.

On restart, the demo agent reconnects via SSE from sequence 0 of each conversation and rebuilds history from the replay. No persistent history storage needed.

---

### Resolution 20: Deployment Healthcheck

Add a `/health` endpoint that returns `{"status": "ok"}` with 200. Configure Fly.io's healthcheck to hit this. This is already in plan 05 step 1 but worth reinforcing.

---

### Resolution 21: Per-Connection Append Serialization

**Ambiguity:** Plan 01 originally said "no locking is needed" for concurrent writes. That's true across connections but FALSE within a single connection.

**Resolution:** Within a single WebSocket writer connection, frame processing MUST be sequential. The handler's receive loop looks like:

```python
async def websocket_handler(ws, agent_id, conversation_id):
    current_message_id = None
    sequence_in_message = 0

    while True:
        frame = await ws.receive_json()  # Blocks until next frame

        if frame["type"] == "start_message":
            if current_message_id is not None:
                await send_error(ws, "message_already_open")
                await ws.close()
                return
            current_message_id = new_message_id()
            sequence_in_message = 0
            seq = await append_to_s2(build_start_record(...))  # AWAIT here
            await ws.send_json({"type": "ack_start", "message_id": current_message_id, "sequence": seq})

        elif frame["type"] == "token":
            if current_message_id is None:
                await send_error(ws, "no_open_message")
                await ws.close()
                return
            seq = await append_to_s2(build_token_record(sequence_in_message, ...))  # AWAIT here
            await ws.send_json({"type": "ack_token", "sequence": seq, "sequence_in_message": sequence_in_message})
            sequence_in_message += 1

        elif frame["type"] == "end_message":
            if current_message_id is None:
                await send_error(ws, "no_open_message")
                await ws.close()
                return
            seq = await append_to_s2(build_end_record(...))
            await ws.send_json({"type": "ack_end", "message_id": current_message_id, "sequence": seq})
            current_message_id = None
            sequence_in_message = 0
```

The key: awaiting `append_to_s2` before calling `receive_json` again guarantees serialization. Do NOT spawn tasks to process frames concurrently. Do NOT use a queue drained concurrently. The receive loop IS the serializer.

Across different WebSocket connections, no cross-connection locking is needed — S2 handles that ordering.

---

### Resolution 22: Sequence-In-Message Assignment

**Ambiguity:** Who assigns the `sequence_in_message` field?

**Resolution:** Server-side, per-connection, per-open-message. The WebSocket handler maintains a counter that resets to 0 on each `start_message` and increments on each `token`. The client's `token` frame does NOT carry `sequence_in_message` — only `content`. See the code sketch in Resolution 21.

---

### Resolution 23: Double WebSocket Rejection

**Ambiguity:** What happens if an agent opens two WebSocket connections to the same conversation simultaneously?

**Resolution:** Reject the second one. The server maintains a map `{(agent_id, conversation_id): WebSocket}` in the connection registry. When a new WebSocket upgrade arrives, check if there's already an entry for that (agent, conversation). If yes, accept the new connection briefly, send an `{"type": "error", "code": "already_connected", ...}` frame, close. This avoids ambiguity in "current open message" tracking.

Multiple SSE read connections from the same agent to the same conversation ARE allowed (readers don't have open-message state to conflict over). The map for readers is `{(agent_id, conversation_id): [list of readers]}`.

---

### Resolution 24: Demo Agent Discovery Mechanism

**Ambiguity:** The original plan used file-based discovery (`/data/demo_agent_id.txt`), which breaks when demo agent runs as a separate Fly app.

**Resolution:** Use a dedicated endpoint `PUT /demo-agent` with a shared-secret header. On startup, the demo agent:
1. Registers itself (or loads cached ID from `DEMO_AGENT_ID_FILE` for its own reboot persistence)
2. Calls `PUT /demo-agent` with header `X-Registration-Token: <shared secret>` and body `{"agent_id": "..."}`

The server stores the registered ID (in-memory singleton with periodic SQLite persistence is fine). `GET /demo-agent` returns the stored ID.

Shared secret is set via `DEMO_AGENT_REGISTRATION_TOKEN` env var on both processes. If the header is missing or mismatched on PUT, return 403 `invalid_registration_token`. This is not real auth, just a trivial guardrail.

This approach works for both deployment options (sidecar in same container, or separate Fly app).

---

### Resolution 25: Server-Side Crash Mid-Message

**Ambiguity:** What happens if the server dies while a client has an open message?

**Resolution:** This is a known limitation. The in-memory `current_message_id` tracking is lost when the server dies, so no `message_abandoned` can be appended. The stream will contain an orphaned `message_start` (and possibly tokens) with no terminator.

Clients handle this via timeout: if a `message_start` is received but no subsequent record for that `message_id` arrives within **60 seconds**, treat the message as locally abandoned. This is documented in `CLIENT.md` section 8.

A server-side fix (durably tracking open messages in SQLite and running a startup reconciliation pass) is documented in `NO_RESTRICTIONS.md` Topic 14 as a future improvement.

---

### Resolution 26: S2 Lite for Testing

**Ambiguity:** Original testing plan said "real S2 for every test" which is slow, flaky, and requires credentials.

**Resolution:** Use **S2 Lite** (the official in-memory S2 emulator) for the routine test suite. S2 Lite is MIT-licensed, maintained by the S2 team, and distributed as a Docker image:

```bash
docker run --rm -d -p 8080:80 --name s2-lite-test ghcr.io/s2-streamstore/s2 lite
```

The `s2-sdk` Python package supports endpoint overrides, so pointing it at S2 Lite is straightforward:

```python
# In tests (conftest.py)
os.environ["S2_ACCOUNT_ENDPOINT"] = "http://localhost:8080"
os.environ["S2_BASIN_ENDPOINT"] = "http://localhost:8080"
os.environ["S2_ACCESS_TOKEN"] = "ignored"
```

The S2 client code is identical — same SDK, same behaviors, no mocks. The only difference is in-memory storage vs durable object storage.

A `conftest.py` fixture ensures S2 Lite is running (starts the container if needed, or connects to an already-running one). Tests clean up streams after each test.

**Keep a small end-to-end suite against real S2.** Gate it with `@pytest.mark.real_s2` so it only runs manually (before deployment, for example). This verifies S2 Lite's behavior matches real S2 for the critical code paths.

---

### Resolution 27: Token Size Limits Are First-Class

**Ambiguity:** Original plan treated size limits as polish to add in step 10.

**Resolution:** Size limits are part of the data model and enforced from day one. A `token` frame with `content` longer than `MAX_TOKEN_LENGTH` (10,000 chars) is rejected with `invalid_frame`. A discrete message with `text` longer than `MAX_DISCRETE_MESSAGE_LENGTH` (100,000 chars) is rejected with `400 invalid_frame`. The limits are in `config.py` as constants and checked in the handler before any S2 appends. This is not optional — unbounded token sizes are a DoS vector and must be enforced from the start.

---

### Resolution 28: Timestamp vs Sequence Ordering

**Ambiguity:** Clients shouldn't use timestamps for ordering but the plans didn't say that.

**Resolution:** `CLIENT.md` must explicitly state: **order records by stream sequence number, never by timestamp**. Timestamps are informational (useful for display, debugging, analytics) but not monotonic — a server clock jump can produce out-of-order timestamps even when sequence numbers are correctly ordered. Within a message, order tokens by `sequence_in_message`. Across messages, order by stream sequence number.

---

### Resolution 29: from=latest Query Parameter

**Ambiguity:** Original plan only mentioned `from=<integer>` for SSE.

**Resolution:** Support `?from=latest` as a special value that means "skip history entirely, deliver only records appended after this connection opens." Useful for tailing agents that don't need to catch up. Implementation: when `from=latest`, query S2 for the current tail sequence number and start the read session from there.

Accepted values for `from`:
- `0` → replay from the start
- `<integer>` → start from that sequence number
- `latest` → skip history, tail only
- omitted → use stored read position (default)

---

### Resolution 30: Leave-Abandoned Scope

**Ambiguity:** When agent A leaves, whose messages get `message_abandoned`?

**Resolution:** Only A's own open messages in that specific conversation. Other agents' open messages in the same conversation are untouched. A's open messages in OTHER conversations are untouched. The leave handler looks up only `(agent_a, conversation_x)` connections in the registry.

---

## Things the Coding Agent Must Decide (Document in DESIGN.md)

For each of these, use your judgment, pick one, and briefly document the choice:

1. **Logging library and format.** Use stdlib `logging` with a simple format, or use `structlog` for structured logs. Either is fine.

2. **Test organization.** One file per scenario group, or grouped by endpoint? Your call.

3. **SSE heartbeat interval.** SSE connections benefit from periodic keep-alive events (e.g., a comment line every 30 seconds) to prevent intermediate proxies from closing idle connections. Pick a value; 15-30 seconds is reasonable.

4. **Whether to implement the `GET /demo-agent` endpoint.** Recommended yes (makes client docs self-contained) but not strictly required.

5. **Demo agent polling interval for new conversations.** 5 seconds is reasonable; adjust if it feels off.

---

## Things the Coding Agent Should NOT Do

1. **Do not add features beyond the spec.** No rich text, no attachments, no multimedia, no authentication.
2. **Do not implement horizontal scaling, Redis fan-out, or similar.** Document as a future improvement in `NO_RESTRICTIONS.md` instead.
3. **Do not mock S2 in integration tests.** Use real S2 with a test basin.
4. **Do not use Claude in the automated test suite.** Scripted text only.
5. **Do not build a frontend.** Documentation-only for the CLIENT.md deliverable.
6. **Do not implement complex retry logic on S2 calls.** Let errors propagate. The S2 SDK has its own retry behavior.
7. **Do not persist the demo agent's conversation history to disk.** Rebuild from stream replay on restart.

---

## When to Ask Questions

Do not ask questions. Make reasonable decisions and document them. If a decision is significant enough that the evaluator might challenge it, document the alternatives considered in `DESIGN.md`. The evaluator explicitly said this project is about making decisions and justifying them — the worst thing to do is stall on a decision.

If something in the docs you read (S2, FastAPI, Anthropic) contradicts what's in these plan files, trust the official docs and update the plan files inline with a note explaining the change.

---

## Final Build Order Refinement

Rework of plan 05's steps in light of the above resolutions:

1. Project scaffolding + `pyproject.toml` with correct package names (`s2-sdk`, NOT `streamstore` which is deprecated, NOT `s2-py` which does not exist)
2. Config module (env vars including `DEMO_AGENT_REGISTRATION_TOKEN`, limits from Resolution 15 and 27)
3. SQLite setup with WAL mode (Resolution 16)
4. Test infrastructure: S2 Lite Docker container, conftest.py fixtures (Resolution 26)
5. Agent registration endpoint
6. `require_agent_id` dependency (Resolution 17)
7. Conversation CRUD with `require_conversation_membership` dependency
8. S2 client setup in lifespan (Resolution 1)
9. Discrete message endpoint using batch append (Resolution 7), with `MAX_DISCRETE_MESSAGE_LENGTH` enforcement (Resolution 27)
10. Connection registry (Resolution 4, 23) — must exist before streaming endpoints
11. SSE streaming reads (with heartbeat, batched position tracking per Resolution 6, `from=latest` per Resolution 29)
12. WebSocket streaming writes (strict sequential frame processing per Resolution 21, server-assigned `sequence_in_message` per Resolution 22, `MAX_TOKEN_LENGTH` enforcement per Resolution 27, double-WebSocket rejection per Resolution 23, dual auth per Resolution 3)
13. Leave handler that terminates only the leaver's connections (Resolution 30)
14. `PUT /demo-agent` and `GET /demo-agent` endpoints (Resolution 24)
15. Demo agent process (uses PUT to register its ID)
16. Tests (integration tests against S2 Lite per Resolution 26; all test scenarios in plan 06)
17. Deployment (fly.toml, Dockerfile, secrets including `DEMO_AGENT_REGISTRATION_TOKEN`)
18. Documentation deliverables (CLIENT.md with scaffolding pattern as section 2 + timeout guidance per Resolution 25 + ordering guidance per Resolution 28, DESIGN.md, NO_RESTRICTIONS.md, README.md)

Build in this order. Don't skip ahead. Each step must pass tests before the next.

# Implementation Plan

Build the service in this order. Each step produces a working increment that can be tested before moving on.

**This plan reflects plan 11's authoritative build order and all design revisions.** Where this plan mentions a "Resolution," it refers to `plans/11-open-questions-and-clarifications.md`. That file is the single source of truth if anything here is ambiguous.

## Step 1: Project Scaffolding

- Create `pyproject.toml` with dependencies: `fastapi`, `uvicorn[standard]`, `aiosqlite`, `s2-sdk` (the S2 Python SDK — NOT `streamstore` which is deprecated, NOT `s2-py`), `anthropic`, `pytest`, `pytest-asyncio`, `httpx`, `httpx-sse`, `websockets`, `sse-starlette`, `python-dotenv`.
- Create the directory structure from `00-overview.md`:
  ```
  src/agentmail/
    __init__.py
    main.py
    config.py
    db.py
    streams.py
    records.py
    connections.py
    deps.py
    demo_agent.py
    routes/
      __init__.py
      agents.py
      conversations.py
      messages.py
      streaming.py
      demo_agent.py
  tests/
    __init__.py
    conftest.py
    ... (per testing plan)
  ```
- Create `.env.example` with placeholders for every env var in the table in plan 11 Resolution 15.

## Step 2: Config Module

- Create `src/agentmail/config.py` with module-level constants for every env var per plan 11 Resolution 15 and every limit per Resolution 27:
  - `DATABASE_PATH`, `S2_ACCESS_TOKEN`, `S2_BASIN`, `S2_ACCOUNT_ENDPOINT` (optional), `S2_BASIN_ENDPOINT` (optional), `PORT`, `DEMO_AGENT_REGISTRATION_TOKEN`, `ANTHROPIC_API_KEY`, `DEMO_AGENT_BASE_URL`, `DEMO_AGENT_ID_FILE`
  - Limits: `MAX_TOKEN_LENGTH = 10_000`, `MAX_TOKENS_PER_MESSAGE = 10_000`, `MAX_DISCRETE_MESSAGE_LENGTH = 100_000`, `MAX_CONCURRENT_CONNECTIONS_PER_AGENT = 50`, `MAX_MEMBERS_PER_CONVERSATION = 1_000`, `WEBSOCKET_IDLE_TIMEOUT_SECONDS = 300`
- Simple module with constants, not a class.

## Step 3: SQLite Setup with WAL

- Implement `src/agentmail/db.py` using `aiosqlite` per Resolution 16:
  - `init_db()` creates the schema from `03-data-model.md` (all 5 tables: agents, conversations, memberships, read_positions, demo_agent_registration, plus the two indexes)
  - Enable WAL mode, `foreign_keys=ON`, `synchronous=NORMAL` at connection time
  - Helper functions for every query needed:
    - agents: create_agent, get_agent
    - conversations: create_conversation, get_conversation, list_conversations_for_agent
    - members: add_member, remove_member, check_membership, list_members_of_conversation, count_members
    - read positions: get_read_position, set_read_position
    - demo agent: set_demo_agent_id, get_demo_agent_id
- Wire up DB initialization in lifespan in `main.py`.
- Unit tests for each helper in `tests/test_db.py` using `:memory:` SQLite.

## Step 4: Test Infrastructure (S2 Lite)

Per Resolution 26: use S2 Lite, the official in-memory S2 emulator, as the primary test backend.

- Create `tests/conftest.py`:
  - Session-scoped fixture that ensures S2 Lite Docker container is running (`docker run --rm -d -p 8080:80 --name s2-lite-test ghcr.io/s2-streamstore/s2 lite`)
  - Set `S2_ACCOUNT_ENDPOINT=http://localhost:8080`, `S2_BASIN_ENDPOINT=http://localhost:8080`, `S2_ACCESS_TOKEN=ignored`
  - Function-scoped fixture for a fresh FastAPI app with `:memory:` SQLite
  - Function-scoped fixture for an `httpx.AsyncClient` pointed at the app
  - Helper: `register_test_agent(client)` that creates an agent and returns its ID
- Mark real-S2 tests with `@pytest.mark.real_s2` so they can be skipped by default (they run manually before deployment).

## Step 5: Agent Registration Endpoint

- Implement `POST /agents` in `src/agentmail/routes/agents.py`:
  - Generate agent ID using `secrets.token_hex(16)` per Resolution 10, prefixed with `agent_`
  - Insert into agents table
  - Return `{"agent_id": "..."}` with 201 status
- Register the router in `main.py`.
- Tests: POST returns 201 with correct ID format; repeated POSTs return distinct IDs.

## Step 6: `require_agent_id` and `require_conversation_membership` Dependencies

Per Resolution 17.

- Create `src/agentmail/deps.py`:
  - `require_agent_id` — extracts `X-Agent-Id` header, validates the agent exists in SQLite, returns the agent ID. Returns 400 `missing_agent_id` if header absent, 404 `agent_not_found` if invalid.
  - `require_conversation_membership` — depends on `require_agent_id`, checks membership in the path-parameter conversation, returns the (agent_id, conversation_id) tuple. Returns 404 `conversation_not_found` or 403 `not_a_member` as appropriate.

## Step 7: Conversation CRUD Endpoints

- Implement `src/agentmail/routes/conversations.py` with all endpoints per `04-api-spec.md`:
  - `POST /conversations` — creates conversation, adds caller as member, CREATES S2 STREAM (see Step 8)
  - `GET /conversations` — lists conversations where caller is a member
  - `GET /conversations/{id}` — returns members and metadata, 403 if not a member
  - `POST /conversations/{id}/members` — invites another agent; no-op if already a member (200); 404 if invitee doesn't exist
  - `DELETE /conversations/{id}/members/me` — leaves; 409 if last member
- Use the dependencies from Step 6 for validation.
- Tests covering every happy path and every error code listed in the API spec.

## Step 8: S2 Integration

Per Resolution 1.

- Implement `src/agentmail/streams.py` using the `s2-sdk` package (import as `from s2_sdk import S2, Endpoints, AppendInput, Record, SeqNum, TailOffset`):
  - Create a long-lived S2 client in `lifespan`, store on `app.state.s2`
  - `create_stream(conv_id)` — creates a new stream in the configured basin
  - `append_record(conv_id, record: dict) -> int` — serializes to JSON bytes, appends, returns sequence number
  - `append_records_batch(conv_id, records: list[dict]) -> list[int]` — batch append, returns list of sequence numbers
  - `read_session(conv_id, start_seq: int)` — returns an async iterator yielding `(sequence, record_dict)` tuples, automatically transitioning from historical to live
- Modify `POST /conversations` from Step 7 to also create the S2 stream.

## Step 9: Records Module + Discrete Message Endpoint

- Create `src/agentmail/records.py`:
  - Functions to construct each of the four record types per `03-data-model.md`: `build_message_start`, `build_token`, `build_message_end`, `build_message_abandoned`
  - Each builder adds server-generated `agent_id`, `message_id`, `timestamp`, and (for tokens) `sequence_in_message`
  - Serialization helpers: `record_to_bytes`, `record_from_bytes`
- Implement `POST /conversations/{id}/messages` in `src/agentmail/routes/messages.py`:
  - Validate `text` length against `MAX_DISCRETE_MESSAGE_LENGTH` per Resolution 27; reject with 400 `invalid_frame` if over
  - Generate a `msg_...` ID
  - Construct three records (start, token with full text and `sequence_in_message=0`, end)
  - Use `append_records_batch` per Resolution 7 for atomicity
  - Return `{"message_id": "...", "start_sequence": N, "end_sequence": N+2}` with 201
- Tests:
  - Happy path: send message, read back from S2 directly, verify all three records
  - Oversize message rejected with 400
  - Non-member rejected with 403
  - Message from A is visible to B as a member of the same conversation with correct attribution

## Step 10: Connection Registry

Per Resolution 4, Resolution 23.

- Create `src/agentmail/connections.py`:
  - `ConnectionRegistry` class with:
    - `writers: dict[tuple[str, str], ActiveWriter]` (one writer per agent per conversation)
    - `readers: dict[tuple[str, str], list[ActiveReader]]` (multiple readers allowed)
  - `ActiveWriter` dataclass: websocket handle, `current_message_id`, `sequence_in_message`
  - `ActiveReader` dataclass: agent_id, conv_id, a close callback
  - Methods: `register_writer`, `unregister_writer`, `register_reader`, `unregister_reader`, `get_writer`, `get_readers_for`, `close_agent_connections_in_conversation`
- Instantiate once in `lifespan`, attach to `app.state.connections`.

## Step 11: SSE Streaming Reads

- Implement `GET /conversations/{id}/stream` in `src/agentmail/routes/streaming.py` using `sse-starlette`:
  - Validate membership with the dependency
  - Determine starting sequence per Resolution 29:
    - `?from=<int>` → that sequence
    - `?from=latest` → current head of the S2 stream
    - omitted → stored read position (default 0)
  - Register the reader in the connection registry
  - Loop:
    - For each record yielded by `read_session`:
      - Format as SSE event: `id: <seq>\ndata: <json>\n\n`
      - Send to client
      - Increment counter, update "time since last position save"
      - Every 10 records OR 5 seconds elapsed: update stored read position (Resolution 6)
    - On clean disconnect: one final position update with the last sequence delivered
  - Emit an SSE heartbeat comment every ~15 seconds to prevent intermediate proxy timeouts
  - Unregister the reader on close
- Tests:
  - Historical: send records, open SSE, verify replay
  - Live: open SSE, send records, verify real-time delivery
  - `from=0`: replay from start
  - `from=latest`: skip history, tail only
  - Stored position: resume works
  - Batched update: abrupt disconnect re-delivers recent records (NOT all of them)
  - Clean disconnect: no re-delivery
  - Non-member: 403 pre-upgrade
  - Historical-to-live transition is seamless

## Step 12: WebSocket Streaming Writes

Per Resolutions 21, 22, 23, 3, 27.

- Add the WebSocket handler to `src/agentmail/routes/streaming.py`:
  - Accept auth via `X-Agent-Id` header OR `?agent_id=` query param (Resolution 3)
  - Validate agent, conversation, membership
  - **Double-WebSocket rejection** (Resolution 23): if the connection registry already has a writer for `(agent_id, conv_id)`, accept briefly, send `error` frame with code `already_connected`, close
  - Register writer in the connection registry with `current_message_id=None, sequence_in_message=0`
  - **STRICTLY SEQUENTIAL** receive loop per Resolution 21 — see the code sketch in that resolution. The loop is: receive frame, await S2 append, send ack, update state, loop. DO NOT spawn concurrent tasks for frames from the same connection.
  - Handle each frame type:
    - `start_message`: reject with `message_already_open` if one is open; generate `msg_...`, append `message_start`, send `ack_start`, set `current_message_id`, reset `sequence_in_message=0`
    - `token`: reject with `no_open_message` if none open; reject with `invalid_frame` if `content` > `MAX_TOKEN_LENGTH` (Resolution 27); append `token` record with SERVER-ASSIGNED `sequence_in_message` (Resolution 22); send `ack_token`; increment counter
    - `end_message`: reject with `no_open_message` if none open; append `message_end`, send `ack_end`, clear `current_message_id`
  - On disconnect while `current_message_id` is set: append `message_abandoned` with `reason: "disconnect"`
  - Unregister from the connection registry
- Tests per Resolution-tagged scenarios in `06-testing-plan.md`:
  - Happy path
  - All frame-order error cases
  - Oversize token
  - Double-WebSocket rejection
  - Sequential serialization: rapid tokens preserve `sequence_in_message` order
  - Client-crash mid-message emits `message_abandoned`
  - Ack frames come in monotonic sequence order

## Step 13: Leave Handler Terminates Own Connections

Per Resolution 30.

- Modify `DELETE /conversations/{id}/members/me`:
  - Remove membership row in SQLite
  - Look up ONLY the leaver's connections for THIS conversation via `connection_registry.get_writer((agent_id, conv_id))` and `get_readers_for((agent_id, conv_id))`
  - If the writer has `current_message_id` set, append `message_abandoned` with `reason: "leave"`
  - Close those specific connections (not others' connections on this conversation, not the leaver's on other conversations)
- Tests:
  - Leave while mid-write emits `message_abandoned` with `reason: "leave"`
  - Leave doesn't affect other agents' connections
  - Leave doesn't affect the leaver's connections on other conversations

## Step 14: Demo Agent Discovery Endpoints

Per Resolution 24.

- Create `src/agentmail/routes/demo_agent.py`:
  - `PUT /demo-agent`: requires `X-Registration-Token` header matching `DEMO_AGENT_REGISTRATION_TOKEN` env var; body `{"agent_id": "..."}`; stores in `demo_agent_registration` table; returns 200; returns 403 `invalid_registration_token` on mismatch
  - `GET /demo-agent`: returns `{"agent_id": "..."}` with 200, or 404 `demo_agent_not_registered` if not yet set
- Tests:
  - PUT with correct token → 200
  - PUT with wrong token → 403
  - PUT with no token → 403
  - GET before any PUT → 404
  - GET after PUT → 200 with registered ID

## Step 15: Demo Agent Process

Per `08-demo-agent-plan.md` (revised) and Resolution 25.

- Create `src/agentmail/demo_agent.py` as a standalone Python module (runnable with `python -m agentmail.demo_agent`):
  - Startup:
    - Read env vars
    - Load agent ID from `DEMO_AGENT_ID_FILE` if present; else `POST /agents` and save the ID
    - `PUT /demo-agent` with the registration token
    - Log agent ID prominently
  - Main loop: poll `GET /conversations` every 5 seconds; for new conversations, open SSE with `?from=latest`
  - Per-conversation SSE handler:
    - Buffer tokens by `message_id`
    - On `message_start`: create buffer
    - On `token`: append to buffer
    - On `message_end` (if from another agent): assemble tokens, trigger response, clear buffer
    - On `message_abandoned`: clear buffer
    - Apply 60-second inactivity timeout per open `message_id` (Resolution 25)
    - Skip own messages (where `agent_id == self`)
  - Response generation: build history, call `anthropic.messages.stream()`, pipe tokens through a WebSocket to the conversation (the scaffolding pattern)
  - System prompt: `"You are a friendly AI assistant participating in a group chat with other AI agents. Keep responses concise (2-3 sentences). Be helpful and conversational."`

## Step 16: Full Test Suite

- Cover all 61 scenarios from `06-testing-plan.md`. If any are not already covered by tests written in earlier steps, add them now.
- Run the full suite; everything must pass before moving to Step 17.

## Step 17: Deployment

Per `07-deployment-plan.md`.

- Write `Dockerfile` (`python:3.11-slim`, install deps, copy code, `uvicorn` command)
- Write `fly.toml`:
  - `auto_stop_machines = false`, `min_machines_running = 1`
  - Persistent volume `agentmail_data` mounted at `/data`
  - `[processes]` section for sidecar demo agent
  - Healthcheck on `/health`
- Set secrets via `fly secrets set`: `ANTHROPIC_API_KEY`, `S2_ACCESS_TOKEN`, `DEMO_AGENT_REGISTRATION_TOKEN`
- Create `deploy.md` with the exact commands to run
- Create `smoke_test.sh` (or Python script) that hits `/health`, registers an agent, creates a conversation, invites the demo agent, sends a message, verifies the demo agent responds

## Step 18: Documentation Deliverables

- Write `CLIENT.md` per `09-client-docs.md`. Scaffolding pattern is section 2 (per the revised plan). Include the 60-second timeout heuristic (Resolution 25), the sequence-not-timestamp ordering guidance (Resolution 28), and the `from=latest` documentation (Resolution 29).
- Write `DESIGN.md` by consolidating `02-design-decisions.md` for submission.
- Write `NO_RESTRICTIONS.md` by consolidating `10-no-restrictions-design.md`.
- Update `README.md` with: what the service is, how to run locally, how to test, how to deploy, links to the three main docs.

## Step 19: Validation Loop for CLIENT.md

- Open a fresh Claude Code (or Claude.ai) session.
- Paste ONLY `CLIENT.md` as context.
- Ask: "Build a Python client that registers as an agent, invites the demo agent to a new conversation, sends a streaming message, and prints the demo agent's streaming response."
- If Claude Code succeeds with no questions, `CLIENT.md` passes.
- If Claude Code asks ANY question, add that information to `CLIENT.md` and try again.
- Budget 2-3 iterations.

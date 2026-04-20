# AgentMail

Stream-based agent-to-agent messaging. AI agents exchange messages through a
durable, ordered relay backed by [S2.dev](https://s2.dev) streams and SQLite
metadata. Reads flow over SSE (replay + live tail); writes flow over
WebSocket (bidirectional, per-token streaming with acks); a discrete
HTTP endpoint exists for agents that just want to POST a complete message.

The point of the design is that an agent can begin writing a message
incrementally (e.g. piping tokens from a Claude inference directly into
the outgoing stream), and the receiving agent sees those tokens land in
order, in near-real-time, with the conversation's history durably
preserved for anyone who joins later.

---

## Quick start

Requirements: Python 3.11+, Docker (for the S2 Lite test container), an
[Anthropic API key](https://console.anthropic.com/), and an
[S2.dev](https://s2.dev) account for deployment (tests don't need it).

```bash
# Install
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Configure
cp .env.example .env    # if present; otherwise set the vars below
export ANTHROPIC_API_KEY=sk-ant-...
export S2_ACCESS_TOKEN=...               # only for deploy, not tests
export S2_BASIN=agentmail                # your S2 basin name
export DATABASE_PATH=./agentmail.db

# Run the API server
uvicorn agentmail.main:app --reload --port 8080

# In another terminal, run the demo agent (optional, for interactive use)
python -m agentmail.demo_agent
```

Health check:

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

End-to-end smoke test (registers two agents, opens a conversation, sends
a message, waits for a streamed reply from the demo agent):

```bash
./smoke_test.sh http://localhost:8080
```

---

## Documentation

- **[CLIENT.md](CLIENT.md)** — How to build a client. Self-contained guide
  covering the mental model, every endpoint, all four record types, the
  SSE read protocol, the WebSocket write protocol (with close codes and
  error-frame shape), and a complete runnable Python example. If you're
  writing an agent that talks to this service, this is the file to read.

- **[DESIGN.md](DESIGN.md)** — Design overview. Architecture diagram,
  why S2 for the conversation log, why SQLite for metadata, why
  asymmetric transports (SSE for reads, WebSocket for writes), the 10
  key design decisions with their rejected alternatives, testing
  strategy, tradeoffs, and what's explicitly out of scope.

- **[NO_RESTRICTIONS.md](NO_RESTRICTIONS.md)** — Revisiting the shipped
  design under an unlimited time budget. 15 topics covering
  infrastructure, reliability, and scale improvements I'd make given
  unlimited engineering time. Each with current state, proposed change,
  and the tradeoff accepted by not doing it.

- **[deploy.md](deploy.md)** — Deploying to Fly.io: prerequisites, secret
  setup, volume creation, and verification.

- **[SYSTEM.md](SYSTEM.md)** — Authoritative internal reference. Full
  spec of every record, every frame, every error path. Primary source if
  CLIENT.md or DESIGN.md is ambiguous. Mostly intended for contributors.

---

## Project structure

```
agentmail_takehome/
├── src/agentmail/
│   ├── main.py            # FastAPI app + lifespan (DB + S2 + registry init)
│   ├── config.py          # Limits, timeouts, env wiring
│   ├── db.py              # aiosqlite connection, WAL setup, FK enforcement
│   ├── streams.py         # S2 client wrapper (append, read_session, init_stream)
│   ├── records.py         # Record builders (message_start, token, end, abandoned)
│   ├── connections.py     # ConnectionRegistry: in-memory reader/writer tracking
│   ├── deps.py            # FastAPI dependency accessors for app.state
│   ├── demo_agent.py      # Standalone responder process (sidecar-deployable)
│   └── routes/
│       ├── agents.py      # POST /agents, GET /agents/me, DELETE /agents/me
│       ├── conversations.py  # CRUD + invite/leave for conversations
│       ├── messages.py    # POST /conversations/{id}/messages (discrete path)
│       ├── streaming.py   # GET /stream (SSE) and /stream/ws (WebSocket)
│       └── demo_agent.py  # GET /demo-agent, PUT /demo-agent (sidecar publish)
├── tests/                 # 114 integration tests against S2 Lite (Docker)
│   ├── conftest.py        # S2 Lite container fixtures, test client setup
│   ├── test_agents.py
│   ├── test_conversations.py
│   ├── test_messages.py
│   ├── test_sse.py
│   ├── test_websocket.py
│   ├── test_streams.py
│   ├── test_connections.py
│   ├── test_records.py
│   ├── test_db.py
│   ├── test_demo_agent.py
│   └── test_demo_agent_integration.py
├── plans/                 # Design plans (preserved for reference)
├── fly.toml               # Fly.io deployment config
├── Dockerfile             # Build for deploy
├── Procfile               # Two processes: web + demo_agent (honcho)
├── smoke_test.sh          # End-to-end sanity check against a deployed instance
├── CLIENT.md
├── DESIGN.md
├── NO_RESTRICTIONS.md
├── deploy.md
├── SYSTEM.md
└── pyproject.toml
```

---

## Tests

```bash
pytest
```

All 114 tests run against a real S2 Lite container (started automatically
by `conftest.py` via Docker), not a mock. They complete in ~15 seconds.
This gives real durability and ordering behavior under test — the whole
point of using S2 — without needing an external account.

If you prefer to run tests without Docker, `conftest.py` documents the
environment variables for pointing at a remote S2 instance instead (not
the default path; Docker is the expected workflow).

---

## Deployment

See [deploy.md](deploy.md). The repo ships `fly.toml`, a `Dockerfile`,
and a `Procfile` that runs the API server and the demo agent as a
sidecar pair via `honcho`. One Fly machine, one persistent volume for
the SQLite database and the demo agent's cached ID.

---

## License & credits

Take-home project. Not intended for production use without the
improvements described in [NO_RESTRICTIONS.md](NO_RESTRICTIONS.md) —
notably horizontal scaling, authentication, observability, and durable
open-message tracking.

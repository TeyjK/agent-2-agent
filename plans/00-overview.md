# AgentMail Take-Home: Project Overview

## What We're Building

A stream-based agent-to-agent messaging service. AI agents (programs that make API calls) use this service to communicate with each other. The service stores messages durably, supports real-time token-by-token streaming in both directions, and handles both online and offline agents.

## Key Principle

The service is a **minimal, durable, ordered stream relay**. It does not know or care what an LLM is. It accepts text (streamed or whole), stores it, and delivers it. Clients handle everything above that layer.

## Success Criteria

1. An AI coding agent (e.g., Claude Code) can read our client documentation and successfully build a client that uses our service — with zero human help.
2. Two agents can have a real-time token-by-token streaming conversation through our service.
3. An agent that disconnects and reconnects receives all messages it missed.
4. Multiple agents in the same conversation can write concurrently without data loss.
5. Our deployed service has at least one Claude-powered demo agent running and reachable.
6. Every design decision is documented with reasoning and tradeoffs.

## Tech Stack

- **Language:** Python 3.11+
- **Web framework:** FastAPI (supports HTTP, WebSocket, SSE natively)
- **ASGI server:** Uvicorn
- **Metadata storage:** SQLite (via `sqlite3` stdlib or `aiosqlite` for async)
- **Message storage:** S2.dev (via S2 Python SDK)
- **LLM:** Anthropic Python SDK (for demo agent only)
- **Deployment:** Fly.io (single container, persistent volume for SQLite)
- **Testing:** pytest + httpx for HTTP + websockets for WebSocket + sse-starlette's test client for SSE

## What This Project Is NOT

- Not an email system
- Not a chat app with a UI
- Not an authentication/authorization system
- Not a multi-tenant SaaS
- Not a feature-rich messaging platform

It's a minimal, correct, well-designed messaging **protocol and service** — nothing more.

## Deliverables Checklist

- [ ] Working codebase with tests
- [ ] Deployed live service (Fly.io)
- [ ] Claude-powered demo agent running on the service
- [ ] `DESIGN.md` — all design decisions with reasoning
- [ ] `NO_RESTRICTIONS.md` — how we'd improve things with unlimited time/resources
- [ ] `CLIENT.md` — self-contained client documentation for AI coding agents
- [ ] `README.md` — project overview, how to run locally, how to test

## File Structure

```
agentmail/
├── README.md
├── DESIGN.md
├── NO_RESTRICTIONS.md
├── CLIENT.md
├── pyproject.toml
├── fly.toml
├── Dockerfile
├── src/
│   └── agentmail/
│       ├── __init__.py
│       ├── main.py              # FastAPI app
│       ├── config.py            # env config
│       ├── db.py                # SQLite setup and queries
│       ├── streams.py           # S2 integration
│       ├── records.py           # JSON record types (message_start, token, etc.)
│       ├── routes/
│       │   ├── agents.py        # POST /agents
│       │   ├── conversations.py # CRUD on conversations + membership
│       │   ├── messages.py      # POST discrete messages
│       │   └── streaming.py     # WebSocket write + SSE read
│       └── demo_agent.py        # Claude-powered demo agent
├── tests/
│   ├── conftest.py
│   ├── test_basic_flow.py
│   ├── test_streaming.py
│   ├── test_concurrent.py
│   ├── test_offline_reconnect.py
│   ├── test_crash.py
│   ├── test_group.py
│   └── test_membership.py
└── plans/                       # (this directory — planning docs)
```

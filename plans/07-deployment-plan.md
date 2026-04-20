# Deployment Plan

## Target Platform: Fly.io

Fly.io has excellent support for long-lived connections (WebSocket, SSE), simple deployment via a CLI, built-in persistent volumes for SQLite, and a generous free tier.

## Prerequisites

- Fly.io account and CLI installed (`flyctl`)
- S2.dev account with an API token and a basin created
- Anthropic API key (provided)

## Files to Create

### `Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (none expected for this project)

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY src/ ./src/

# Default SQLite path is on a mounted volume
ENV DATABASE_PATH=/data/agentmail.db
ENV PORT=8080

EXPOSE 8080

CMD ["uvicorn", "agentmail.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### `fly.toml`

```toml
app = "agentmail-<your-unique-suffix>"
primary_region = "iad"  # or closest to you

[build]

[env]
  DATABASE_PATH = "/data/agentmail.db"
  S2_BASIN = "agentmail"
  PORT = "8080"

[mounts]
  source = "agentmail_data"
  destination = "/data"

[http_service]
  internal_port = 8080
  force_https = true
  auto_stop_machines = false  # important — we need long-lived connections
  auto_start_machines = true
  min_machines_running = 1

[[vm]]
  memory = "512mb"
  cpu_kind = "shared"
  cpus = 1
```

Key settings:
- `auto_stop_machines = false` — don't idle the machine, because we have long-lived WebSocket/SSE connections
- `min_machines_running = 1` — always have at least one machine running so the demo agent stays reachable

## Deployment Steps

```bash
# From project root
fly launch --no-deploy   # interactive setup, generates fly.toml (override as above)
fly volumes create agentmail_data --region iad --size 1

# Set secrets (not committed to fly.toml)
fly secrets set ANTHROPIC_API_KEY=sk-ant-...
fly secrets set S2_ACCESS_TOKEN=...

fly deploy
```

## Post-Deployment Verification

```bash
# Health check
curl https://<your-app>.fly.dev/health
# Expected: {"status": "ok"}

# Register an agent
curl -X POST https://<your-app>.fly.dev/agents
# Expected: {"agent_id": "agent_..."}

# Create a conversation
curl -X POST https://<your-app>.fly.dev/conversations \
  -H "X-Agent-Id: agent_..."
# Expected: {"conversation_id": "conv_...", "members": ["agent_..."]}
```

## Demo Agent Deployment

Two options:

### Option A: Separate Fly App (recommended)

Deploy the demo agent as its own Fly app. It registers with the main service on startup, stores its agent ID in a Fly secret (set manually after first run), and runs a long-lived process that listens for SSE and responds via WebSocket.

Benefits: clean separation, easy to restart one without the other, clearer logs.

### Option B: Sidecar Process

Add the demo agent as a second process in the same container using Fly's `[processes]` config. Both run under the same app.

Benefits: one deployment, no separate app to manage.

**Recommendation:** Start with Option B for simplicity. If the demo agent becomes flaky or needs independent restart, split to Option A.

### Sidecar `fly.toml` addition:

```toml
[processes]
  app = "uvicorn agentmail.main:app --host 0.0.0.0 --port 8080"
  demo_agent = "python -m agentmail.demo_agent"
```

## Environment Variables Summary

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_PATH` | Yes | Path to SQLite file (or `:memory:` for tests) |
| `S2_ACCESS_TOKEN` | Yes | S2.dev API token (secret) |
| `S2_BASIN` | Yes | S2 basin name |
| `ANTHROPIC_API_KEY` | Yes (demo agent only) | Anthropic API key (secret) |
| `PORT` | No (defaults to 8080) | Port to bind HTTP server |
| `DEMO_AGENT_REGISTRATION_TOKEN` | Yes | Shared secret between main service and demo agent for the `PUT /demo-agent` endpoint. Any random string. Set on both processes. |
| `DEMO_AGENT_BASE_URL` | Yes (demo agent only) | URL of the main service (e.g., `https://your-app.fly.dev`) |
| `DEMO_AGENT_ID_FILE` | No (defaults to `/data/demo_agent_id.txt`) | File to persist the demo agent's ID locally across restarts |

## Monitoring and Debugging

- `fly logs` — stream logs from the running container
- `fly ssh console` — SSH into the container to inspect state
- `fly status` — check deployment status

## Publishing the Demo Agent ID

The demo agent registers its identity with the main service on startup using a dedicated endpoint:

```
PUT /demo-agent
Header: X-Registration-Token: <shared secret>
Body: { "agent_id": "agent_..." }
```

The server stores this in SQLite (or an in-memory singleton with periodic persistence — your choice). Then:

```
GET /demo-agent
→ 200 {"agent_id": "agent_..."}
→ 404 if no demo agent has registered yet
```

**Why a PUT endpoint instead of file-based discovery:** file-based discovery only works if the demo agent and the main service share a filesystem. That's only true for the sidecar deployment (Option B); it's broken if the demo agent runs as a separate Fly app (Option A). Using an API call works in both cases.

**Registration token:** the `X-Registration-Token` header is a simple guardrail against anyone overwriting the published agent ID. Set via a shared env var `DEMO_AGENT_REGISTRATION_TOKEN` on both the main service and the demo agent. If the header is missing or doesn't match, the PUT returns 403. This is not real authentication — it's a trivial fence against accidents and trivial mischief. Don't overthink it.

Both deployment options (sidecar in one container, or separate Fly app) now work identically from the discovery standpoint.

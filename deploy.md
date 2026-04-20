# Deploying AgentMail to Fly.io

This deploys both the API server and the demo agent as a single Fly app
(sidecar deployment via `honcho` and a `Procfile`). Both processes share a
persistent volume mounted at `/data` for the SQLite DB and the demo agent's
cached ID.

## 1. Prerequisites

- An [S2.dev](https://s2.dev) account
  - Create a basin (e.g. named `agentmail`). Whatever you pick, set it as the
    `S2_BASIN` secret below.
  - Generate an access token.
- An [Anthropic API key](https://console.anthropic.com/) for the demo agent.
- A [Fly.io](https://fly.io) account.
- The `flyctl` CLI installed and authenticated:
  ```bash
  brew install flyctl       # macOS
  curl -L https://fly.io/install.sh | sh   # Linux
  fly auth login
  ```
- Docker (only required if you want to test the image locally before deploy).

## 2. Generate a registration token

The demo agent uses a shared secret to publish its agent ID to the server. Any
random string works — generate one and keep it handy:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

## 3. Create the Fly app

The repo ships with a `fly.toml`. Edit `app = "agentmail"` to a globally unique
name (Fly will reject duplicates), and adjust `primary_region` if needed.

```bash
fly apps create agentmail-<your-suffix>
# OR, if you'd rather have flyctl walk you through it:
#   fly launch --copy-config --no-deploy
```

If you used `fly launch`, it may overwrite `fly.toml` — re-apply the volume
mount, `auto_stop_machines = false`, and the `[env]` block from the version
in this repo.

## 4. Create the persistent volume

Pick the same region as `primary_region` in `fly.toml`:

```bash
fly volumes create agentmail_data --region iad --size 1
```

`size` is in GB. 1 GB is generous for SQLite plus the small ID file.

## 5. Set the secrets

```bash
fly secrets set \
  S2_ACCESS_TOKEN=<your-s2-token> \
  ANTHROPIC_API_KEY=sk-ant-... \
  DEMO_AGENT_REGISTRATION_TOKEN=<the-token-from-step-2>
```

`S2_BASIN` is already set in `fly.toml` under `[env]`, so you do NOT need to
include it in the secrets command. If you want to change the basin without
redeploying, you can set it as a secret instead and remove it from `fly.toml`
— secrets take precedence.

## 6. Deploy

```bash
fly deploy
```

The first deploy builds the Docker image, ships it, mounts the volume, and
starts the machine. Subsequent deploys reuse the volume.

## 7. Verify

Replace `<app>` with your app name (e.g. `agentmail-yourname`).

```bash
# Health check
curl https://<app>.fly.dev/health
# → {"status":"ok"}

# Demo agent should have self-registered within a few seconds of boot.
curl https://<app>.fly.dev/demo-agent
# → {"agent_id":"agent_..."}
```

For an end-to-end check (registers a fresh agent, opens a conversation with
the demo agent, sends a message, watches for a streamed reply):

```bash
./smoke_test.sh https://<app>.fly.dev
```

`smoke_test.sh` needs `curl` and `jq`, and may need `chmod +x smoke_test.sh`
the first time you run it.

## 8. Operations

```bash
# Stream logs (both web and demo processes are interleaved)
fly logs

# Open a shell in the running container
fly ssh console

# Inspect the SQLite DB on the volume
fly ssh console -C "sqlite3 /data/agentmail.db '.tables'"

# Inspect the demo agent's persisted ID
fly ssh console -C "cat /data/demo_agent_id.txt"

# Redeploy after code changes
fly deploy

# Roll back to a prior release
fly releases
fly deploy --image registry.fly.io/<app>:deployment-<id>
```

## 9. Notes

- **One machine only.** `min_machines_running = 1` and `auto_stop_machines =
  false` keep exactly one instance alive at all times so WebSocket and SSE
  connections stay attached to the same process. Scaling out is
  documented as a future improvement (it'd need a fan-out layer for
  cross-machine SSE delivery).
- **Demo agent state is ephemeral per process.** Each conversation task holds
  its chat history (list of user/assistant turns) in memory. On restart, the
  demo agent resumes reading each active conversation from the stored read
  position in SQLite — it does NOT re-consume the full history and regenerate
  responses. The consequence: after a restart, the demo agent has no memory of
  prior turns in the conversation and will treat the next incoming message as
  a fresh context. This is a known limitation and is documented in
  `NO_RESTRICTIONS.md` as a future improvement.
- **Registration token churn.** If you rotate
  `DEMO_AGENT_REGISTRATION_TOKEN`, redeploy with the new value — the demo
  agent re-publishes its ID on every boot using whatever token is set.

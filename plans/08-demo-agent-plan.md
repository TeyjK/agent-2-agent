# Demo Agent Plan

## Purpose

The demo agent is a Claude-powered client that runs alongside our service, ready to converse with any agent the evaluator connects. It proves the full streaming flow works end-to-end: receive streaming tokens, think with Claude, stream response tokens back.

It is NOT part of the service's test suite. It's a separate process that uses the service via the same public API documented in `CLIENT.md`.

## Behavior

1. **On startup:**
   - Read configuration from env vars (`DEMO_AGENT_BASE_URL`, `ANTHROPIC_API_KEY`, `DEMO_AGENT_ID_FILE`, `DEMO_AGENT_REGISTRATION_TOKEN`)
   - If the ID file exists, load the agent ID from it. Otherwise, register a new agent via `POST /agents`, persist the ID to the file.
   - Publish the agent ID to the main service via `PUT /demo-agent` with the registration token. This makes the ID discoverable via `GET /demo-agent` for any client (including the evaluator's).
   - Log the agent ID prominently

2. **Main loop:**
   - Poll `GET /conversations` every 5 seconds to discover newly-invited conversations
   - For each conversation the agent is a member of but doesn't have an active SSE connection to, open one
   - For each SSE connection, process incoming records

3. **Handling incoming records:**
   - Ignore records where `agent_id == self` (don't respond to own messages)
   - Ignore `token` and `message_start` records (wait for complete messages only, to keep the demo simple)
   - Ignore `message_abandoned` records
   - When a `message_end` arrives: collect all tokens with the same `message_id` that came before it (maintained in a per-conversation buffer), assemble them into the full message text, then generate a response

4. **Generating a response:**
   - Build a conversation history from the records buffer (all completed messages in the conversation so far)
   - Call Anthropic API with streaming enabled, using a system prompt like: "You are a friendly AI assistant participating in a group chat with other AI agents. Keep responses concise (2-3 sentences max). Be helpful and conversational."
   - As Claude streams tokens back, pipe each token through a WebSocket to `/conversations/{id}/stream`
   - Wait for ack frames before continuing to the next token (or pipeline if performance matters)

## Scaffolding Pattern (Critical)

This is the pattern the evaluator said we should document. The agent's core loop looks like this:

```python
async def respond_to_conversation(conv_id: str, history: list[dict]):
    # Open a WebSocket to our service
    async with websockets.connect(f"{ws_base}/conversations/{conv_id}/stream",
                                   extra_headers={"X-Agent-Id": self.agent_id}) as ws:
        # Signal message start
        await ws.send(json.dumps({"type": "start_message"}))
        ack = json.loads(await ws.recv())
        assert ack["type"] == "ack_start"

        # Stream from Claude
        async with anthropic.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=500,
            system=SYSTEM_PROMPT,
            messages=history,
        ) as stream:
            # For each token from Claude, forward to our service
            async for text in stream.text_stream:
                await ws.send(json.dumps({"type": "token", "content": text}))
                # Optionally wait for ack_token, or pipeline
                await ws.recv()

        # Signal message end
        await ws.send(json.dumps({"type": "end_message"}))
        await ws.recv()  # ack_end
```

This is exactly what the evaluator's Claude Code agent will need to write from `CLIENT.md`. The demo agent is a reference implementation of this pattern.

## File Layout

- `src/agentmail/demo_agent.py` — entry point, `__main__` runs the agent
- Reuses config from `src/agentmail/config.py` for env vars

## Running Locally

```bash
export DEMO_AGENT_BASE_URL=http://localhost:8080
export ANTHROPIC_API_KEY=sk-ant-...
export DEMO_AGENT_ID_FILE=./demo_agent_id.txt
python -m agentmail.demo_agent
```

Log output includes the agent ID for easy discovery.

## Demo Agent ID Endpoint

Add to the main service (not the demo agent itself):

```
PUT /demo-agent
Header: X-Registration-Token: <shared secret>
Body: { "agent_id": "agent_..." }
→ 200 on success, 403 if token is wrong

GET /demo-agent
→ 200 {"agent_id": "agent_..."}
→ 404 if no demo agent is registered
```

Implementation: the demo agent calls `PUT /demo-agent` on startup with the shared `DEMO_AGENT_REGISTRATION_TOKEN` in the `X-Registration-Token` header. The service stores the ID in the `demo_agent_registration` SQLite table. `GET /demo-agent` reads from that table. This works for both deployment options (sidecar in same container, or separate Fly app) because it goes through the public HTTP API rather than relying on a shared filesystem. See Resolution 24 in plan 11 for the full rationale.

## Testing the Demo Agent

Manually, not automated:

1. Deploy service and demo agent
2. Fetch the demo agent ID: `curl https://<app>.fly.dev/demo-agent`
3. Register a test agent: `curl -X POST https://<app>.fly.dev/agents`
4. Create a conversation and invite the demo agent
5. Open an SSE connection to the conversation (e.g., using `curl -N`)
6. Send a message to the conversation (discrete via POST, or streaming via WebSocket)
7. Observe the demo agent's streaming response arriving on the SSE connection

## What the Demo Agent Does NOT Do

- Does not use tool calls (as per the evaluator's guidance)
- Does not implement sophisticated conversation memory — just uses the conversation history as received
- Does not handle every edge case flawlessly — it's a demo, not production code
- Does not need to be fast, efficient, or scalable — just needs to work for demonstration

**On polling for new conversations:** the demo agent polls `GET /conversations` rather than receiving push notifications because the service does not expose a membership-events endpoint (and adding one is out of scope per the evaluator's "no extra features" guidance). The demo agent is a reference implementation of the client documentation, not a showcase of good client architecture. A production agent built against this service would also need to poll, which is an honest reflection of a limitation in the current API surface — one that's documented in `NO_RESTRICTIONS.md` as a future improvement.

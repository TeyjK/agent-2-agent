import asyncio
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx
import pytest
from httpx_sse import aconnect_sse

from tests.conftest import register_test_agent

pytestmark = pytest.mark.real_anthropic


REPO_ROOT = Path(__file__).resolve().parent.parent


def _spawn_demo_agent(server_url: str, id_file: Path) -> subprocess.Popen:
    env = os.environ.copy()
    env["DEMO_AGENT_BASE_URL"] = server_url
    env["DEMO_AGENT_ID_FILE"] = str(id_file)
    env["DEMO_AGENT_REGISTRATION_TOKEN"] = "test-token"
    if not env.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set in environment")

    return subprocess.Popen(
        [sys.executable, "-m", "agentmail.demo_agent"],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


async def _await_demo_agent_registered(
    client: httpx.AsyncClient, timeout: float = 15.0
) -> str:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        r = await client.get("/demo-agent")
        if r.status_code == 200:
            return r.json()["agent_id"]
        await asyncio.sleep(0.3)
    raise TimeoutError("demo agent did not register within timeout")


async def test_demo_agent_registers_on_startup(live_client, server_url):
    with tempfile.TemporaryDirectory() as tmp:
        id_file = Path(tmp) / "demo_agent_id.txt"
        proc = _spawn_demo_agent(server_url, id_file)
        try:
            agent_id = await _await_demo_agent_registered(live_client)
            assert agent_id.startswith("agent_")
            # Persisted to the id file
            assert id_file.exists()
            assert id_file.read_text().strip() == agent_id
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


async def test_demo_agent_responds_to_message(live_client, server_url):
    with tempfile.TemporaryDirectory() as tmp:
        id_file = Path(tmp) / "demo_agent_id.txt"
        proc = _spawn_demo_agent(server_url, id_file)
        try:
            demo_id = await _await_demo_agent_registered(live_client)

            user_id = await register_test_agent(live_client)
            r = await live_client.post(
                "/conversations", headers={"X-Agent-Id": user_id}
            )
            assert r.status_code == 201, r.text
            conv_id = r.json()["conversation_id"]

            r = await live_client.post(
                f"/conversations/{conv_id}/members",
                headers={"X-Agent-Id": user_id},
                json={"agent_id": demo_id},
            )
            assert r.status_code == 200, r.text

            # Give the demo agent a poll cycle to discover the conversation
            # and open its WS/SSE before we send the inbound message.
            await asyncio.sleep(3.0)

            r = await live_client.post(
                f"/conversations/{conv_id}/messages",
                headers={"X-Agent-Id": user_id},
                json={"text": "Hello, please reply with a short greeting."},
            )
            assert r.status_code == 201, r.text

            # Watch SSE for a complete message from the demo agent.
            saw_start = False
            saw_token = False
            saw_end = False
            async with aconnect_sse(
                live_client,
                "GET",
                f"/conversations/{conv_id}/stream?from=0",
                headers={"X-Agent-Id": user_id},
            ) as es:
                async def _wait():
                    nonlocal saw_start, saw_token, saw_end
                    async for sse in es.aiter_sse():
                        rec = json.loads(sse.data)
                        if rec.get("agent_id") != demo_id:
                            continue
                        if rec["type"] == "message_start":
                            saw_start = True
                        elif rec["type"] == "token":
                            saw_token = True
                        elif rec["type"] == "message_end":
                            saw_end = True
                            return

                await asyncio.wait_for(_wait(), timeout=45.0)

            assert saw_start and saw_token and saw_end
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

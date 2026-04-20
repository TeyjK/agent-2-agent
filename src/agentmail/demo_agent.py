import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import httpx
from anthropic import AsyncAnthropic
from httpx_sse import aconnect_sse
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from . import config

logger = logging.getLogger("agentmail.demo_agent")

POLL_INTERVAL_SECONDS = 2.0
MESSAGE_TIMEOUT_SECONDS = 60.0
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"
CLAUDE_MAX_TOKENS = 500
SYSTEM_PROMPT = (
    "You are a friendly AI assistant participating in a group chat with other "
    "AI agents. Keep responses concise (2-3 sentences max). Be helpful and "
    "conversational."
)


async def ensure_agent_id(
    http: httpx.AsyncClient, base_url: str, id_file: str, token: str
) -> str:
    p = Path(id_file)
    if p.exists():
        agent_id = p.read_text().strip()
        logger.info("loaded existing demo agent id from %s", id_file)
    else:
        r = await http.post(f"{base_url}/agents")
        r.raise_for_status()
        agent_id = r.json()["agent_id"]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(agent_id)
        logger.info("registered new demo agent and persisted id to %s", id_file)

    r = await http.put(
        f"{base_url}/demo-agent",
        headers={"X-Registration-Token": token},
        json={"agent_id": agent_id},
    )
    r.raise_for_status()
    return agent_id


async def list_conversation_ids(
    http: httpx.AsyncClient, base_url: str, agent_id: str
) -> list[str]:
    r = await http.get(
        f"{base_url}/conversations", headers={"X-Agent-Id": agent_id}
    )
    r.raise_for_status()
    return [c["conversation_id"] for c in r.json()["conversations"]]


def _gc_stale_messages(open_messages: dict, now: float) -> None:
    stale = [
        mid
        for mid, info in open_messages.items()
        if now - info["last_active"] > MESSAGE_TIMEOUT_SECONDS
    ]
    for mid in stale:
        logger.warning("dropping stale open message %s (no activity in 60s)", mid)
        del open_messages[mid]


async def _stream_response_via_ws(
    ws,
    anthropic: AsyncAnthropic,
    history: list[dict],
) -> str:
    await ws.send(json.dumps({"type": "message_start"}))
    ack = json.loads(await ws.recv())
    if ack.get("type") != "message_started":
        raise RuntimeError(f"unexpected message_started ack: {ack}")

    assembled: list[str] = []
    async with anthropic.messages.stream(
        model=CLAUDE_MODEL,
        max_tokens=CLAUDE_MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=history,
    ) as stream:
        async for text in stream.text_stream:
            if not text:
                continue
            await ws.send(json.dumps({"type": "token", "content": text}))
            tack = json.loads(await ws.recv())
            if tack.get("type") != "token_ack":
                raise RuntimeError(f"unexpected token_ack: {tack}")
            assembled.append(text)

    await ws.send(json.dumps({"type": "message_end"}))
    eack = json.loads(await ws.recv())
    if eack.get("type") != "message_ended":
        raise RuntimeError(f"unexpected message_ended ack: {eack}")

    return "".join(assembled)


async def conversation_task(
    http: httpx.AsyncClient,
    anthropic: AsyncAnthropic,
    base_url: str,
    agent_id: str,
    conv_id: str,
) -> None:
    ws_base = base_url.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_base}/conversations/{conv_id}/stream/ws?agent_id={agent_id}"
    sse_url = f"{base_url}/conversations/{conv_id}/stream"

    history: list[dict] = []
    open_messages: dict[str, dict] = {}

    logger.info("starting task for conversation %s", conv_id)
    try:
        async with ws_connect(ws_url) as ws:
            async with aconnect_sse(
                http, "GET", sse_url, headers={"X-Agent-Id": agent_id}
            ) as es:
                async for sse in es.aiter_sse():
                    rec = json.loads(sse.data)
                    rtype = rec.get("type")
                    sender = rec.get("agent_id")
                    now = time.monotonic()
                    _gc_stale_messages(open_messages, now)

                    if sender == agent_id:
                        # Resolution: skip our own records — we don't react to ourselves.
                        continue

                    if rtype == "message_start":
                        msg_id = rec["message_id"]
                        open_messages[msg_id] = {
                            "agent_id": sender,
                            "tokens": [],
                            "last_active": now,
                        }
                    elif rtype == "token":
                        msg_id = rec["message_id"]
                        info = open_messages.get(msg_id)
                        if info is not None:
                            info["tokens"].append(rec.get("content", ""))
                            info["last_active"] = now
                    elif rtype == "message_abandoned":
                        open_messages.pop(rec["message_id"], None)
                    elif rtype == "message_end":
                        info = open_messages.pop(rec["message_id"], None)
                        if info is None:
                            continue
                        full_text = "".join(info["tokens"])
                        history.append({"role": "user", "content": full_text})
                        try:
                            assistant_text = await _stream_response_via_ws(
                                ws, anthropic, history
                            )
                            history.append(
                                {"role": "assistant", "content": assistant_text}
                            )
                        except Exception:
                            # Per Resolution 25 / plan 03: the only relevant abandon
                            # reason here is "disconnect". The server will append
                            # message_abandoned automatically when we drop the WS.
                            logger.exception(
                                "error streaming Claude response on conv %s; dropping ws",
                                conv_id,
                            )
                            return
    except ConnectionClosed as e:
        logger.info(
            "ws closed for conv %s (code=%s reason=%s)", conv_id, e.code, e.reason
        )
    except Exception:
        logger.exception("conversation task for %s failed", conv_id)
    finally:
        logger.info("ending task for conversation %s", conv_id)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    base_url = config.DEMO_AGENT_BASE_URL.rstrip("/")
    if not config.ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY is required")
        sys.exit(1)
    if not config.DEMO_AGENT_REGISTRATION_TOKEN:
        logger.error("DEMO_AGENT_REGISTRATION_TOKEN is required")
        sys.exit(1)

    async with httpx.AsyncClient(timeout=30.0) as http:
        agent_id = await ensure_agent_id(
            http,
            base_url,
            config.DEMO_AGENT_ID_FILE,
            config.DEMO_AGENT_REGISTRATION_TOKEN,
        )
        logger.info("==== DEMO AGENT ID: %s ====", agent_id)

        anthropic = AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY)
        active: dict[str, asyncio.Task] = {}

        try:
            while True:
                try:
                    conv_ids = await list_conversation_ids(http, base_url, agent_id)
                except Exception:
                    logger.exception("failed to list conversations")
                    conv_ids = []

                for cid in conv_ids:
                    existing = active.get(cid)
                    if existing is None or existing.done():
                        active[cid] = asyncio.create_task(
                            conversation_task(http, anthropic, base_url, agent_id, cid)
                        )

                for cid in list(active.keys()):
                    t = active[cid]
                    if t.done():
                        try:
                            t.result()
                        except Exception:
                            pass
                        del active[cid]

                await asyncio.sleep(POLL_INTERVAL_SECONDS)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("shutting down")
        finally:
            for t in active.values():
                t.cancel()
            for t in active.values():
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


if __name__ == "__main__":
    asyncio.run(main())

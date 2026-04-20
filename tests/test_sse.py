import asyncio
import json

from httpx_sse import aconnect_sse

from tests.conftest import register_test_agent


async def _read_n_events(client, url, headers, n, timeout=5.0):
    events: list = []

    async def _read():
        async with aconnect_sse(client, "GET", url, headers=headers) as es:
            async for sse in es.aiter_sse():
                events.append(sse)
                if len(events) >= n:
                    return

    await asyncio.wait_for(_read(), timeout=timeout)
    return events


async def _send_discrete(client, conv_id, agent_id, text):
    r = await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": agent_id},
        json={"text": text},
    )
    assert r.status_code == 201, r.text
    return r.json()


async def test_sse_receives_historical(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    await _send_discrete(live_client, conv_id, a, "one")
    await _send_discrete(live_client, conv_id, a, "two")
    await _send_discrete(live_client, conv_id, a, "three")

    events = await _read_n_events(
        live_client,
        f"/conversations/{conv_id}/stream?from=0",
        {"X-Agent-Id": b},
        n=9,
    )
    assert len(events) == 9
    types = [json.loads(e.data)["type"] for e in events]
    assert types == [
        "message_start", "token", "message_end",
        "message_start", "token", "message_end",
        "message_start", "token", "message_end",
    ]
    contents = [
        json.loads(e.data)["content"]
        for e in events
        if json.loads(e.data)["type"] == "token"
    ]
    assert contents == ["one", "two", "three"]


async def test_sse_receives_live(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    events: list = []

    async def reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream?from=0",
            headers={"X-Agent-Id": b},
        ) as es:
            async for sse in es.aiter_sse():
                events.append(sse)
                if len(events) >= 3:
                    return

    async def writer():
        await asyncio.sleep(0.2)
        await _send_discrete(live_client, conv_id, a, "hello live")

    await asyncio.wait_for(asyncio.gather(reader(), writer()), timeout=5.0)
    assert len(events) == 3
    types = [json.loads(e.data)["type"] for e in events]
    assert types == ["message_start", "token", "message_end"]
    token = json.loads(events[1].data)
    assert token["content"] == "hello live"


async def test_sse_from_zero(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    await _send_discrete(live_client, conv_id, a, "msg-1")

    first = await _read_n_events(
        live_client,
        f"/conversations/{conv_id}/stream?from=0",
        {"X-Agent-Id": b},
        n=3,
    )
    assert len(first) == 3

    # Reconnect with from=0 — should re-read everything.
    second = await _read_n_events(
        live_client,
        f"/conversations/{conv_id}/stream?from=0",
        {"X-Agent-Id": b},
        n=3,
    )
    assert len(second) == 3
    assert [e.id for e in first] == [e.id for e in second]


async def test_sse_from_latest(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    await _send_discrete(live_client, conv_id, a, "history-1")
    await _send_discrete(live_client, conv_id, a, "history-2")

    events: list = []

    async def reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream?from=latest",
            headers={"X-Agent-Id": b},
        ) as es:
            async for sse in es.aiter_sse():
                events.append(sse)
                if len(events) >= 3:
                    return

    async def writer():
        await asyncio.sleep(0.3)
        await _send_discrete(live_client, conv_id, a, "new-msg")

    await asyncio.wait_for(asyncio.gather(reader(), writer()), timeout=5.0)

    assert len(events) == 3
    tokens = [
        json.loads(e.data)["content"]
        for e in events
        if json.loads(e.data)["type"] == "token"
    ]
    assert tokens == ["new-msg"]


async def test_sse_invalid_from_value(live_client):
    a = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await live_client.get(
        f"/conversations/{conv_id}/stream?from=notanumber",
        headers={"X-Agent-Id": a},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_frame"


async def test_sse_read_position_persists(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    await _send_discrete(live_client, conv_id, a, "first-msg")

    first = await _read_n_events(
        live_client,
        f"/conversations/{conv_id}/stream?from=0",
        {"X-Agent-Id": b},
        n=3,
    )
    assert len(first) == 3

    # Give the finally-cleanup a moment to flush the read position update.
    await asyncio.sleep(0.5)

    # Reconnect without from=; since read position was updated on clean close,
    # no records should be re-delivered. Wait briefly and confirm no events.
    received: list = []

    async def reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream",
            headers={"X-Agent-Id": b},
        ) as es:
            async for sse in es.aiter_sse():
                received.append(sse)
                return

    try:
        await asyncio.wait_for(reader(), timeout=1.0)
    except asyncio.TimeoutError:
        pass

    assert received == []


async def test_sse_non_member(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await live_client.get(
        f"/conversations/{conv_id}/stream",
        headers={"X-Agent-Id": b},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "not_a_member"


async def test_sse_transitions_historical_to_live(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    await _send_discrete(live_client, conv_id, a, "hist-1")
    await _send_discrete(live_client, conv_id, a, "hist-2")

    events: list = []

    async def reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream?from=0",
            headers={"X-Agent-Id": b},
        ) as es:
            async for sse in es.aiter_sse():
                events.append(sse)
                if len(events) >= 9:
                    return

    async def writer():
        # Wait for the historical batch to have been consumed before sending more.
        while len(events) < 6:
            await asyncio.sleep(0.05)
        await _send_discrete(live_client, conv_id, a, "live-1")

    await asyncio.wait_for(asyncio.gather(reader(), writer()), timeout=5.0)

    assert len(events) == 9
    tokens = [
        json.loads(e.data)["content"]
        for e in events
        if json.loads(e.data)["type"] == "token"
    ]
    assert tokens == ["hist-1", "hist-2", "live-1"]

import asyncio
import json

import pytest
from httpx_sse import aconnect_sse
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from agentmail import streams
from tests.conftest import register_test_agent


def _ws_url(server_url: str, conv_id: str) -> str:
    base = server_url.replace("http://", "ws://")
    return f"{base}/conversations/{conv_id}/stream/ws"


async def test_create_conversation(client):
    agent_id = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": agent_id})
    assert r.status_code == 201
    body = r.json()
    assert body["conversation_id"].startswith("conv_")
    assert body["members"] == [agent_id]


async def test_list_conversations_empty(client):
    agent_id = await register_test_agent(client)
    r = await client.get("/conversations", headers={"X-Agent-Id": agent_id})
    assert r.status_code == 200
    assert r.json() == {"conversations": []}


async def test_list_conversations_with_conversations(client):
    agent_id = await register_test_agent(client)
    r1 = await client.post("/conversations", headers={"X-Agent-Id": agent_id})
    r2 = await client.post("/conversations", headers={"X-Agent-Id": agent_id})
    conv1 = r1.json()["conversation_id"]
    conv2 = r2.json()["conversation_id"]

    r = await client.get("/conversations", headers={"X-Agent-Id": agent_id})
    assert r.status_code == 200
    convs = r.json()["conversations"]
    assert {c["conversation_id"] for c in convs} == {conv1, conv2}
    for c in convs:
        assert c["members"] == [agent_id]
        assert c["created_at"]


async def test_invite_agent(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    assert r.status_code == 200
    assert set(r.json()["members"]) == {a, b}


async def test_invite_nonexistent_agent(client):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": "agent_00000000000000000000000000000000"},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "agent_not_found"


async def test_invite_already_member(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    await client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    r = await client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    assert r.status_code == 200
    assert set(r.json()["members"]) == {a, b}


async def test_leave_conversation(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )

    r = await client.delete(
        f"/conversations/{conv_id}/members/me",
        headers={"X-Agent-Id": b},
    )
    assert r.status_code == 204

    r = await client.get(f"/conversations/{conv_id}", headers={"X-Agent-Id": a})
    assert r.status_code == 200
    assert r.json()["members"] == [a]


async def test_leave_when_last_member(client):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.delete(
        f"/conversations/{conv_id}/members/me",
        headers={"X-Agent-Id": a},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "last_member_cannot_leave"


async def test_non_member_cannot_read(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.get(f"/conversations/{conv_id}", headers={"X-Agent-Id": b})
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "not_a_member"


async def test_missing_agent_id_header(client):
    r = await client.post("/conversations")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "missing_agent_id"


async def test_nonexistent_agent_id_header(client):
    r = await client.post(
        "/conversations",
        headers={"X-Agent-Id": "agent_00000000000000000000000000000000"},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "agent_not_found"


async def test_get_nonexistent_conversation(client):
    a = await register_test_agent(client)
    r = await client.get(
        "/conversations/conv_00000000000000000000000000000000",
        headers={"X-Agent-Id": a},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "conversation_not_found"


async def test_invite_to_nonexistent_conversation(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post(
        "/conversations/conv_00000000000000000000000000000000/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "conversation_not_found"


async def test_create_conversation_creates_s2_stream(client, app):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    seq = await streams.append_record(app.state.s2, conv_id, {"probe": True})
    assert isinstance(seq, int)


async def test_leave_when_not_a_member(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.delete(
        f"/conversations/{conv_id}/members/me",
        headers={"X-Agent-Id": b},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "not_a_member"


async def _two_agents_conv_live(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    r2 = await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    assert r2.status_code == 200
    return a, b, conv_id


async def test_leave_closes_own_writer_ws(live_client, server_url):
    a, b, conv_id = await _two_agents_conv_live(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", b)]) as ws:
        await asyncio.sleep(0.1)
        r = await live_client.delete(
            f"/conversations/{conv_id}/members/me",
            headers={"X-Agent-Id": b},
        )
        assert r.status_code == 204
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1000
        assert exc.value.reason == "leave"


async def test_leave_closes_own_reader_sse(live_client, server_url):
    a, b, conv_id = await _two_agents_conv_live(live_client)
    stream_ended = asyncio.Event()

    async def reader():
        try:
            async with aconnect_sse(
                live_client,
                "GET",
                f"/conversations/{conv_id}/stream?from=0",
                headers={"X-Agent-Id": b},
            ) as es:
                async for _sse in es.aiter_sse():
                    pass
        finally:
            stream_ended.set()

    reader_task = asyncio.create_task(reader())
    await asyncio.sleep(0.3)
    r = await live_client.delete(
        f"/conversations/{conv_id}/members/me",
        headers={"X-Agent-Id": b},
    )
    assert r.status_code == 204
    await asyncio.wait_for(stream_ended.wait(), timeout=3.0)
    await reader_task


async def test_leave_does_not_close_other_members_connections(
    live_client, server_url
):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    c = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    for m in [b, c]:
        r2 = await live_client.post(
            f"/conversations/{conv_id}/members",
            headers={"X-Agent-Id": a},
            json={"agent_id": m},
        )
        assert r2.status_code == 200

    url = _ws_url(server_url, conv_id)
    a_done = asyncio.Event()
    b_events: list = []
    c_events: list = []

    async def a_reader():
        try:
            async with aconnect_sse(
                live_client,
                "GET",
                f"/conversations/{conv_id}/stream?from=0",
                headers={"X-Agent-Id": a},
            ) as es:
                async for _sse in es.aiter_sse():
                    pass
        finally:
            a_done.set()

    async def b_reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream?from=0",
            headers={"X-Agent-Id": b},
        ) as es:
            async for sse in es.aiter_sse():
                b_events.append(sse)

    async def c_reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream?from=0",
            headers={"X-Agent-Id": c},
        ) as es:
            async for sse in es.aiter_sse():
                c_events.append(sse)

    a_task = asyncio.create_task(a_reader())
    b_task = asyncio.create_task(b_reader())
    c_task = asyncio.create_task(c_reader())

    try:
        async with ws_connect(
            url, additional_headers=[("X-Agent-Id", b)]
        ) as b_ws:
            await asyncio.sleep(0.3)

            r = await live_client.delete(
                f"/conversations/{conv_id}/members/me",
                headers={"X-Agent-Id": a},
            )
            assert r.status_code == 204

            # A's own reader should end
            await asyncio.wait_for(a_done.wait(), timeout=3.0)

            # B's writer ws should still function
            await b_ws.send(json.dumps({"type": "message_start"}))
            ack1 = json.loads(await b_ws.recv())
            assert ack1["type"] == "message_started"
            await b_ws.send(
                json.dumps({"type": "token", "content": "still-here"})
            )
            ack2 = json.loads(await b_ws.recv())
            assert ack2["type"] == "token_ack"
            await b_ws.send(json.dumps({"type": "message_end"}))
            ack3 = json.loads(await b_ws.recv())
            assert ack3["type"] == "message_ended"

            # Both B and C's readers should have received the records
            await asyncio.sleep(0.3)
            assert not b_task.done()
            assert not c_task.done()
            assert len(b_events) >= 3
            assert len(c_events) >= 3
    finally:
        for t in (a_task, b_task, c_task):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


async def test_leave_midmessage_emits_abandoned_reason_leave(
    live_client, live_app, server_url
):
    a, b, conv_id = await _two_agents_conv_live(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", b)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        started = json.loads(await ws.recv())
        msg_id = started["message_id"]
        await ws.send(json.dumps({"type": "token", "content": "oh no"}))
        await ws.recv()
        r = await live_client.delete(
            f"/conversations/{conv_id}/members/me",
            headers={"X-Agent-Id": b},
        )
        assert r.status_code == 204
        with pytest.raises(ConnectionClosed):
            await ws.recv()

    async def _find_abandoned():
        async for _seq, rec in streams.read_session(
            live_app.state.s2, conv_id, 0
        ):
            if rec["type"] == "message_abandoned":
                return rec
        return None

    found = await asyncio.wait_for(_find_abandoned(), timeout=5.0)
    assert found is not None
    assert found["reason"] == "leave"
    assert found["message_id"] == msg_id
    assert found["agent_id"] == b

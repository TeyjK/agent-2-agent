import asyncio
import json

import pytest
from httpx_sse import aconnect_sse
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from agentmail import config, streams
from tests.conftest import register_test_agent


def _ws_url(server_url: str, conv_id: str) -> str:
    base = server_url.replace("http://", "ws://").replace("https://", "wss://")
    return f"{base}/conversations/{conv_id}/stream/ws"


async def _solo_conv(live_client):
    a = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    assert r.status_code == 201, r.text
    return a, r.json()["conversation_id"]


async def _two_agents_conv(live_client):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    r2 = await live_client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    assert r2.status_code == 200, r2.text
    return a, b, conv_id


async def _recv_json(ws):
    raw = await ws.recv()
    return json.loads(raw)


async def test_ws_connect_and_send_full_message(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        ack1 = await _recv_json(ws)
        assert ack1["type"] == "message_started"
        assert ack1["message_id"].startswith("msg_")
        assert isinstance(ack1["sequence"], int)

        await ws.send(json.dumps({"type": "token", "content": "hello"}))
        ack2 = await _recv_json(ws)
        assert ack2["type"] == "token_ack"
        assert ack2["sequence"] == ack1["sequence"] + 1

        await ws.send(json.dumps({"type": "message_end"}))
        ack3 = await _recv_json(ws)
        assert ack3["type"] == "message_ended"
        assert ack3["message_id"] == ack1["message_id"]
        assert ack3["sequence"] == ack2["sequence"] + 1


async def test_ws_server_assigns_sequence_in_message(
    live_client, live_app, server_url
):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        await _recv_json(ws)
        for t in ["a", "b", "c"]:
            await ws.send(json.dumps({"type": "token", "content": t}))
            await _recv_json(ws)
        await ws.send(json.dumps({"type": "message_end"}))
        await _recv_json(ws)

    async def _collect():
        seen = []
        async for _seq, rec in streams.read_session(live_app.state.s2, conv_id, 0):
            seen.append(rec)
            if rec["type"] == "message_end":
                return seen
        return seen

    records_out = await asyncio.wait_for(_collect(), timeout=5.0)
    tokens = [r for r in records_out if r["type"] == "token"]
    assert [r["sequence_in_message"] for r in tokens] == [0, 1, 2]
    assert [r["content"] for r in tokens] == ["a", "b", "c"]


async def test_ws_token_before_message_start_closed(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "token", "content": "x"}))
        err = await _recv_json(ws)
        assert err["error"]["code"] == "protocol_error"
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1002


async def test_ws_message_start_while_in_message_closed(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        await _recv_json(ws)
        await ws.send(json.dumps({"type": "message_start"}))
        err = await _recv_json(ws)
        assert err["error"]["code"] == "protocol_error"
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1002


async def test_ws_oversize_token_rejected(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    big = "x" * (config.MAX_TOKEN_LENGTH + 1)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        await _recv_json(ws)
        await ws.send(json.dumps({"type": "token", "content": big}))
        err = await _recv_json(ws)
        assert err["error"]["code"] == "invalid_frame"
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1009


async def test_ws_malformed_json_closed(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send("not-json")
        err = await _recv_json(ws)
        assert err["error"]["code"] == "invalid_frame"
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1002


async def test_ws_unknown_frame_type_closed(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "bogus"}))
        err = await _recv_json(ws)
        assert err["error"]["code"] == "invalid_frame"
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1002


async def test_ws_double_connection_rejected(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws1:
        await asyncio.sleep(0.1)
        async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws2:
            with pytest.raises(ConnectionClosed) as exc:
                await ws2.recv()
            assert exc.value.code == 1008
            assert exc.value.reason == "already_connected"
        # ws1 should still be usable
        await ws1.send(json.dumps({"type": "message_start"}))
        ack = await _recv_json(ws1)
        assert ack["type"] == "message_started"


async def test_ws_disconnect_midmessage_emits_abandoned(
    live_client, live_app, server_url
):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        started = await _recv_json(ws)
        msg_id = started["message_id"]
        for t in ["p", "q"]:
            await ws.send(json.dumps({"type": "token", "content": t}))
            await _recv_json(ws)
        # client closes without sending message_end

    async def _find_abandoned():
        async for _seq, rec in streams.read_session(live_app.state.s2, conv_id, 0):
            if rec["type"] == "message_abandoned":
                return rec
        return None

    found = await asyncio.wait_for(_find_abandoned(), timeout=5.0)
    assert found is not None
    assert found["reason"] == "disconnect"
    assert found["message_id"] == msg_id
    assert found["agent_id"] == a


async def test_ws_disconnect_between_messages_no_abandoned(
    live_client, live_app, server_url
):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        await _recv_json(ws)
        await ws.send(json.dumps({"type": "token", "content": "done"}))
        await _recv_json(ws)
        await ws.send(json.dumps({"type": "message_end"}))
        await _recv_json(ws)
        # clean disconnect after message_end

    # Let the server finalize the disconnect.
    await asyncio.sleep(0.5)
    tail = await streams.get_stream_tail(live_app.state.s2, conv_id)
    assert tail == 3  # message_start, token, message_end — no abandoned


async def test_ws_missing_agent_id_closed(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url) as ws:
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1008
        assert exc.value.reason == "missing_agent_id"


async def test_ws_agent_not_found_closed(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    url = _ws_url(server_url, conv_id)
    bogus = "agent_00000000000000000000000000000000"
    async with ws_connect(url, additional_headers=[("X-Agent-Id", bogus)]) as ws:
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1008
        assert exc.value.reason == "agent_not_found"


async def test_ws_non_member_closed(live_client, server_url):
    a = await register_test_agent(live_client)
    b = await register_test_agent(live_client)
    r = await live_client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    url = _ws_url(server_url, conv_id)
    async with ws_connect(url, additional_headers=[("X-Agent-Id", b)]) as ws:
        with pytest.raises(ConnectionClosed) as exc:
            await ws.recv()
        assert exc.value.code == 1008
        assert exc.value.reason == "not_a_member"


async def test_ws_query_param_auth(live_client, server_url):
    a, conv_id = await _solo_conv(live_client)
    base = server_url.replace("http://", "ws://")
    url = f"{base}/conversations/{conv_id}/stream/ws?agent_id={a}"
    async with ws_connect(url) as ws:
        await ws.send(json.dumps({"type": "message_start"}))
        ack = await _recv_json(ws)
        assert ack["type"] == "message_started"


async def test_ws_streaming_visible_to_reader(live_client, server_url):
    a, b, conv_id = await _two_agents_conv(live_client)
    url = _ws_url(server_url, conv_id)

    events: list = []
    reader_ready = asyncio.Event()

    async def reader():
        async with aconnect_sse(
            live_client,
            "GET",
            f"/conversations/{conv_id}/stream?from=0",
            headers={"X-Agent-Id": b},
        ) as es:
            reader_ready.set()
            async for sse in es.aiter_sse():
                events.append(sse)
                if len(events) >= 5:
                    return

    async def writer():
        await reader_ready.wait()
        await asyncio.sleep(0.1)
        async with ws_connect(url, additional_headers=[("X-Agent-Id", a)]) as ws:
            await ws.send(json.dumps({"type": "message_start"}))
            await _recv_json(ws)
            for t in ["foo", "bar", "baz"]:
                await ws.send(json.dumps({"type": "token", "content": t}))
                await _recv_json(ws)
            await ws.send(json.dumps({"type": "message_end"}))
            await _recv_json(ws)

    await asyncio.wait_for(asyncio.gather(reader(), writer()), timeout=8.0)
    assert len(events) == 5
    types = [json.loads(e.data)["type"] for e in events]
    assert types == ["message_start", "token", "token", "token", "message_end"]
    contents = [
        json.loads(e.data)["content"]
        for e in events
        if json.loads(e.data)["type"] == "token"
    ]
    assert contents == ["foo", "bar", "baz"]

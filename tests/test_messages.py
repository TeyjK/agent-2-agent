from agentmail import streams
from tests.conftest import register_test_agent


async def _collect(app, conv_id, n):
    out = []
    async for seq, data in streams.read_session(app.state.s2, conv_id, 0):
        out.append((seq, data))
        if len(out) == n:
            break
    return out


async def test_send_discrete_message_happy_path(client, app):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": a},
        json={"text": "Hello"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["message_id"].startswith("msg_")
    assert isinstance(body["start_sequence"], int)
    assert isinstance(body["end_sequence"], int)
    assert body["end_sequence"] == body["start_sequence"] + 2

    got = await _collect(app, conv_id, 3)
    types = [d["type"] for _, d in got]
    assert types == ["message_start", "token", "message_end"]

    _, token = got[1]
    assert token["content"] == "Hello"
    assert token["sequence_in_message"] == 0


async def test_discrete_message_all_three_records_have_same_message_id(client, app):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    r = await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": a},
        json={"text": "hi"},
    )
    mid = r.json()["message_id"]

    got = await _collect(app, conv_id, 3)
    assert {d["message_id"] for _, d in got} == {mid}


async def test_discrete_message_sequence_numbers_are_consecutive(client, app):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    r = await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": a},
        json={"text": "hi"},
    )
    body = r.json()

    got = await _collect(app, conv_id, 3)
    seqs = [s for s, _ in got]
    assert seqs[0] == body["start_sequence"]
    assert seqs[1] == body["start_sequence"] + 1
    assert seqs[2] == body["end_sequence"]


async def test_discrete_message_visible_to_other_member(client, app):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await client.post(
        f"/conversations/{conv_id}/members",
        headers={"X-Agent-Id": a},
        json={"agent_id": b},
    )
    await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": a},
        json={"text": "hello B"},
    )

    got = await _collect(app, conv_id, 3)
    token = got[1][1]
    assert token["content"] == "hello B"
    assert token["agent_id"] == a


async def test_discrete_message_attribution(client, app):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]
    await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": a},
        json={"text": "x"},
    )
    got = await _collect(app, conv_id, 3)
    for _, d in got:
        assert d["agent_id"] == a


async def test_non_member_cannot_send_discrete_message(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": b},
        json={"text": "hi"},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "not_a_member"


async def test_send_discrete_message_to_nonexistent_conversation(client):
    a = await register_test_agent(client)
    r = await client.post(
        "/conversations/conv_00000000000000000000000000000000/messages",
        headers={"X-Agent-Id": a},
        json={"text": "hi"},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "conversation_not_found"


async def test_oversize_discrete_message_rejected(client):
    from agentmail import config

    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    text = "x" * (config.MAX_DISCRETE_MESSAGE_LENGTH + 1)
    r = await client.post(
        f"/conversations/{conv_id}/messages",
        headers={"X-Agent-Id": a},
        json={"text": text},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_frame"


async def test_missing_agent_id_header_on_discrete_message(client):
    a = await register_test_agent(client)
    r = await client.post("/conversations", headers={"X-Agent-Id": a})
    conv_id = r.json()["conversation_id"]

    r = await client.post(
        f"/conversations/{conv_id}/messages",
        json={"text": "hi"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "missing_agent_id"

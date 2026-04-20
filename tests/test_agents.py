import re


async def test_register_agent_returns_201(client):
    r = await client.post("/agents")
    assert r.status_code == 201


async def test_register_agent_response_has_agent_id(client):
    r = await client.post("/agents")
    body = r.json()
    assert "agent_id" in body
    assert isinstance(body["agent_id"], str)


async def test_register_agent_id_has_correct_format(client):
    r = await client.post("/agents")
    agent_id = r.json()["agent_id"]
    assert re.match(r"^agent_[0-9a-f]{32}$", agent_id) is not None


async def test_register_agent_multiple_times_returns_distinct_ids(client):
    ids = set()
    for _ in range(5):
        r = await client.post("/agents")
        assert r.status_code == 201
        ids.add(r.json()["agent_id"])
    assert len(ids) == 5

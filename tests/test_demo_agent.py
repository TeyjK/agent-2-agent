from tests.conftest import register_test_agent


async def test_put_demo_agent_with_valid_token(client):
    agent_id = await register_test_agent(client)
    r = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "test-token"},
        json={"agent_id": agent_id},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"agent_id": agent_id}


async def test_put_demo_agent_missing_token_returns_401(client):
    agent_id = await register_test_agent(client)
    r = await client.put("/demo-agent", json={"agent_id": agent_id})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_registration_token"


async def test_put_demo_agent_wrong_token_returns_401(client):
    agent_id = await register_test_agent(client)
    r = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "not-the-real-token"},
        json={"agent_id": agent_id},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_registration_token"


async def test_put_demo_agent_nonexistent_agent_returns_404(client):
    r = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "test-token"},
        json={"agent_id": "agent_00000000000000000000000000000000"},
    )
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "agent_not_found"


async def test_get_demo_agent_before_registration_returns_404(client):
    r = await client.get("/demo-agent")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "demo_agent_not_registered"


async def test_get_demo_agent_after_registration(client):
    agent_id = await register_test_agent(client)
    r = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "test-token"},
        json={"agent_id": agent_id},
    )
    assert r.status_code == 200, r.text

    g = await client.get("/demo-agent")
    assert g.status_code == 200
    assert g.json() == {"agent_id": agent_id}


async def test_put_demo_agent_is_idempotent(client):
    a = await register_test_agent(client)
    b = await register_test_agent(client)

    r1 = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "test-token"},
        json={"agent_id": a},
    )
    assert r1.status_code == 200, r1.text

    r2 = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "test-token"},
        json={"agent_id": b},
    )
    assert r2.status_code == 200, r2.text

    g = await client.get("/demo-agent")
    assert g.status_code == 200
    assert g.json() == {"agent_id": b}


async def test_get_demo_agent_no_auth_required(client):
    agent_id = await register_test_agent(client)
    r = await client.put(
        "/demo-agent",
        headers={"X-Registration-Token": "test-token"},
        json={"agent_id": agent_id},
    )
    assert r.status_code == 200, r.text

    # Plain GET without any header succeeds.
    g = await client.get("/demo-agent")
    assert g.status_code == 200
    assert g.json() == {"agent_id": agent_id}

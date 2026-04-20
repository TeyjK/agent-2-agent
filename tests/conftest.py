import asyncio
import importlib
import socket
import subprocess
import time
from collections.abc import AsyncIterator
from urllib.parse import urlparse

import httpx
import pytest
import pytest_asyncio
import uvicorn

S2_LITE_CONTAINER = "agentmail-s2-lite-test"
S2_LITE_IMAGE = "ghcr.io/s2-streamstore/s2"
S2_LITE_URL = "http://localhost:8080"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_s2: test runs against real S2 (requires credentials), skipped by default",
    )
    config.addinivalue_line(
        "markers",
        "real_anthropic: test runs against real Anthropic API (requires ANTHROPIC_API_KEY), skipped by default",
    )


def pytest_collection_modifyitems(config, items):
    # Only run gated tests when their marker is explicitly requested via -m.
    selected_markers = config.getoption("-m") or ""
    skip_real_anthropic = pytest.mark.skip(
        reason="needs `-m real_anthropic` to run (hits real Anthropic API)"
    )
    skip_real_s2 = pytest.mark.skip(
        reason="needs `-m real_s2` to run (hits real S2)"
    )
    for item in items:
        if "real_anthropic" in item.keywords and "real_anthropic" not in selected_markers:
            item.add_marker(skip_real_anthropic)
        if "real_s2" in item.keywords and "real_s2" not in selected_markers:
            item.add_marker(skip_real_s2)


def _docker_available() -> bool:
    try:
        r = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=10
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _container_running(name: str) -> bool:
    try:
        r = subprocess.run(
            [
                "docker", "ps",
                "--filter", f"name=^{name}$",
                "--format", "{{.Names}}",
            ],
            capture_output=True, text=True, timeout=10,
        )
        return name in r.stdout.strip().splitlines()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _wait_for_s2_lite(url: str, timeout: float = 30.0) -> bool:
    # S2 Lite speaks HTTP/2 (gRPC) only, so an HTTP/1.1 probe gets reset.
    # A successful TCP connect is sufficient to confirm the port is listening.
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 80
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.3)
    return False


@pytest.fixture(scope="session")
def s2_lite():
    if not _docker_available():
        pytest.fail(
            "Docker is required to run tests that use S2 Lite. "
            "Install Docker (https://docs.docker.com/get-docker/) and try again."
        )
    if not _container_running(S2_LITE_CONTAINER):
        r = subprocess.run(
            [
                "docker", "run", "-d", "--rm",
                "-p", "8080:80",
                "--name", S2_LITE_CONTAINER,
                S2_LITE_IMAGE, "lite",
            ],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            pytest.fail(f"Failed to start S2 Lite container: {r.stderr.strip()}")
    if not _wait_for_s2_lite(S2_LITE_URL):
        pytest.fail(
            f"S2 Lite did not become reachable at {S2_LITE_URL} within 30 seconds"
        )
    yield S2_LITE_URL


@pytest.fixture
def env_vars(s2_lite, monkeypatch):
    monkeypatch.setenv("S2_ACCOUNT_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("S2_BASIN_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("S2_ACCESS_TOKEN", "ignored")
    monkeypatch.setenv("S2_BASIN", "agentmail-test")
    monkeypatch.setenv("DATABASE_PATH", ":memory:")
    monkeypatch.setenv("DEMO_AGENT_REGISTRATION_TOKEN", "test-token")


@pytest_asyncio.fixture
async def app(env_vars):
    import agentmail.config as config_mod
    import agentmail.main as main_mod

    importlib.reload(config_mod)
    importlib.reload(main_mod)

    async with main_mod.lifespan(main_mod.app):
        yield main_mod.app


@pytest_asyncio.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as c:
        yield c


@pytest_asyncio.fixture
async def live_app(env_vars):
    # A separate fixture that does NOT enter lifespan itself — uvicorn will.
    # Used for tests that need true response streaming (SSE, WebSockets),
    # which `httpx.ASGITransport` does not support (it buffers responses).
    import agentmail.config as config_mod
    import agentmail.main as main_mod

    importlib.reload(config_mod)
    importlib.reload(main_mod)
    return main_mod.app


@pytest_asyncio.fixture
async def server_url(live_app) -> AsyncIterator[str]:
    config = uvicorn.Config(
        live_app, host="127.0.0.1", port=0, log_level="warning", lifespan="on"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    try:
        deadline = time.monotonic() + 10.0
        while not server.started and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        if not server.started:
            raise RuntimeError("uvicorn server did not start in time")
        port = server.servers[0].sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@pytest_asyncio.fixture
async def live_client(server_url) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=server_url) as c:
        yield c


async def register_test_agent(client: httpx.AsyncClient) -> str:
    r = await client.post("/agents")
    assert r.status_code == 201, r.text
    return r.json()["agent_id"]

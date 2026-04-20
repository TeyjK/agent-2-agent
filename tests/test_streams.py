import asyncio
import uuid

import pytest_asyncio

from agentmail import streams


def _unique_stream() -> str:
    return f"conv_{uuid.uuid4().hex}"


@pytest_asyncio.fixture
async def s2_client(env_vars):
    client = await streams.create_s2_client()
    try:
        yield client
    finally:
        await client.close()


async def test_create_stream(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)


async def test_create_stream_is_idempotent(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)
    await streams.create_stream(s2_client, name)


async def test_append_single_record(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)
    seq = await streams.append_record(s2_client, name, {"hello": "world"})
    assert isinstance(seq, int)
    assert seq >= 0


async def test_append_batch_records(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)
    seqs = await streams.append_records_batch(
        s2_client, name, [{"i": 0}, {"i": 1}, {"i": 2}]
    )
    assert len(seqs) == 3
    assert seqs[1] == seqs[0] + 1
    assert seqs[2] == seqs[0] + 2


async def test_read_session_from_zero(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)
    payloads = [{"i": 0}, {"i": 1}, {"i": 2}]
    await streams.append_records_batch(s2_client, name, payloads)

    collected = []
    async for seq, data in streams.read_session(s2_client, name, 0):
        collected.append((seq, data))
        if len(collected) == 3:
            break

    assert [d for _, d in collected] == payloads
    seqs = [s for s, _ in collected]
    assert seqs == sorted(seqs)


async def test_read_session_from_midpoint(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)
    payloads = [{"i": i} for i in range(5)]
    seqs = await streams.append_records_batch(s2_client, name, payloads)

    collected = []
    async for seq, data in streams.read_session(s2_client, name, seqs[2]):
        collected.append((seq, data))
        if len(collected) == 3:
            break

    assert [d for _, d in collected] == payloads[2:]


async def test_read_session_transitions_to_live(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)

    collected: list[tuple[int, dict]] = []

    async def reader():
        async for seq, data in streams.read_session(s2_client, name, 0):
            collected.append((seq, data))
            if len(collected) == 2:
                return

    async def writer():
        await asyncio.sleep(0.2)
        await streams.append_records_batch(
            s2_client, name, [{"live": 1}, {"live": 2}]
        )

    await asyncio.wait_for(
        asyncio.gather(reader(), writer()),
        timeout=5.0,
    )
    assert [d for _, d in collected] == [{"live": 1}, {"live": 2}]


async def test_get_stream_tail(s2_client):
    name = _unique_stream()
    await streams.create_stream(s2_client, name)
    seqs = await streams.append_records_batch(
        s2_client, name, [{"i": 0}, {"i": 1}, {"i": 2}]
    )
    tail = await streams.get_stream_tail(s2_client, name)
    assert tail == seqs[-1] + 1

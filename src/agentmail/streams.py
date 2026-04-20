import json
import logging
from collections.abc import AsyncIterator

from s2_sdk import (
    AppendInput,
    Endpoints,
    Record,
    S2,
    S2ServerError,
    SeqNum,
)

from . import config

logger = logging.getLogger(__name__)


def _build_endpoints() -> Endpoints:
    if config.S2_ACCOUNT_ENDPOINT and config.S2_BASIN_ENDPOINT:
        return Endpoints(
            account=config.S2_ACCOUNT_ENDPOINT,
            basin=config.S2_BASIN_ENDPOINT,
        )
    return Endpoints.default()


async def create_s2_client() -> S2:
    client = S2(access_token=config.S2_ACCESS_TOKEN, endpoints=_build_endpoints())
    try:
        await client.create_basin(config.S2_BASIN)
    except S2ServerError as e:
        if "already exists" not in str(e):
            raise
    return client


async def create_stream(client: S2, conv_id: str) -> None:
    basin = client.basin(config.S2_BASIN)
    try:
        await basin.create_stream(conv_id)
    except S2ServerError as e:
        if "already exists" in str(e):
            logger.info("stream %s already exists, continuing", conv_id)
            return
        raise


async def append_record(client: S2, conv_id: str, record: dict) -> int:
    payload = json.dumps(record).encode("utf-8")
    stream = client.basin(config.S2_BASIN).stream(conv_id)
    ack = await stream.append(AppendInput(records=[Record(body=payload)]))
    return ack.start.seq_num


async def append_records_batch(
    client: S2, conv_id: str, records: list[dict]
) -> list[int]:
    serialized = [json.dumps(r).encode("utf-8") for r in records]
    stream = client.basin(config.S2_BASIN).stream(conv_id)
    ack = await stream.append(
        AppendInput(records=[Record(body=b) for b in serialized])
    )
    start = ack.start.seq_num
    return [start + i for i in range(len(records))]


async def read_session(
    client: S2, conv_id: str, start_seq: int
) -> AsyncIterator[tuple[int, dict]]:
    stream = client.basin(config.S2_BASIN).stream(conv_id)
    async for batch in stream.read_session(start=SeqNum(value=start_seq)):
        for record in batch.records:
            yield record.seq_num, json.loads(record.body)


async def get_stream_tail(client: S2, conv_id: str) -> int:
    stream = client.basin(config.S2_BASIN).stream(conv_id)
    tail = await stream.check_tail()
    return tail.seq_num

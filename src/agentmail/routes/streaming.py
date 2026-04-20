import asyncio
import json
import logging
import time

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from sse_starlette.sse import EventSourceResponse

from .. import config, db, records, streams
from ..connections import ActiveReader, ActiveWriter
from ..deps import require_conversation_membership

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/conversations")


async def _resolve_start_sequence(
    request: Request,
    agent_id: str,
    conversation_id: str,
    from_: str | None,
) -> int:
    if from_ is None:
        return await db.get_read_position(
            request.app.state.db, agent_id, conversation_id
        )
    if from_ == "latest":
        return await streams.get_stream_tail(request.app.state.s2, conversation_id)
    try:
        value = int(from_)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_frame",
                "message": f"invalid from= value: {from_!r}",
            },
        )
    if value < 0:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_frame",
                "message": f"from= must be non-negative: {from_!r}",
            },
        )
    return value


@router.get("/{conversation_id}/stream")
async def sse_stream(
    request: Request,
    from_: str | None = Query(default=None, alias="from"),
    membership: tuple[str, str] = Depends(require_conversation_membership),
) -> EventSourceResponse:
    agent_id, conversation_id = membership
    start_seq = await _resolve_start_sequence(
        request, agent_id, conversation_id, from_
    )

    conn = request.app.state.db
    s2 = request.app.state.s2
    registry = request.app.state.connections

    reader = ActiveReader(agent_id=agent_id, conversation_id=conversation_id)
    registry.register_reader(reader)

    async def event_generator():
        next_seq_to_read: int | None = None
        records_since_update = 0
        last_update_time = time.monotonic()
        read_iter = streams.read_session(s2, conversation_id, start_seq).__aiter__()
        try:
            while True:
                next_task = asyncio.ensure_future(read_iter.__anext__())
                close_task = asyncio.ensure_future(reader.close_event.wait())
                try:
                    done, pending = await asyncio.wait(
                        {next_task, close_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except asyncio.CancelledError:
                    next_task.cancel()
                    close_task.cancel()
                    raise

                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, StopAsyncIteration, Exception):
                        pass

                if close_task in done:
                    break
                try:
                    seq, record = next_task.result()
                except StopAsyncIteration:
                    break

                yield {"id": str(seq), "data": json.dumps(record)}
                next_seq_to_read = seq + 1
                records_since_update += 1
                now = time.monotonic()
                if (
                    records_since_update
                    >= config.READ_POSITION_UPDATE_EVERY_N_RECORDS
                    or now - last_update_time
                    >= config.READ_POSITION_UPDATE_EVERY_N_SECONDS
                ):
                    await db.set_read_position(
                        conn, agent_id, conversation_id, next_seq_to_read
                    )
                    records_since_update = 0
                    last_update_time = now
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "error in SSE stream for %s/%s", agent_id, conversation_id
            )
            yield {
                "event": "error",
                "data": json.dumps(
                    {"code": "internal_error", "message": "stream error"}
                ),
            }
        finally:
            if next_seq_to_read is not None:
                try:
                    await db.set_read_position(
                        conn, agent_id, conversation_id, next_seq_to_read
                    )
                except Exception:
                    logger.exception(
                        "failed to update read position on close for %s/%s",
                        agent_id,
                        conversation_id,
                    )
            try:
                await read_iter.aclose()
            except Exception:
                pass
            registry.unregister_reader(reader)

    return EventSourceResponse(event_generator(), ping=15)


async def _ws_send_error(ws: WebSocket, code: str, message: str) -> None:
    try:
        await ws.send_text(
            json.dumps({"error": {"code": code, "message": message}})
        )
    except Exception:
        pass


async def _ws_close(ws: WebSocket, code: int, reason: str) -> None:
    try:
        await ws.close(code=code, reason=reason)
    except Exception:
        pass


@router.websocket("/{conversation_id}/stream/ws")
async def ws_stream(
    websocket: WebSocket,
    conversation_id: str,
    agent_id: str | None = Query(default=None),
) -> None:
    await websocket.accept()

    header_agent = websocket.headers.get("x-agent-id")
    effective = agent_id or header_agent
    if not effective:
        await _ws_close(websocket, 1008, "missing_agent_id")
        return

    conn = websocket.app.state.db
    s2 = websocket.app.state.s2
    registry = websocket.app.state.connections

    if await db.get_agent(conn, effective) is None:
        await _ws_close(websocket, 1008, "agent_not_found")
        return
    if await db.get_conversation(conn, conversation_id) is None:
        await _ws_close(websocket, 1008, "conversation_not_found")
        return
    if not await db.check_membership(conn, effective, conversation_id):
        await _ws_close(websocket, 1008, "not_a_member")
        return

    writer = ActiveWriter(
        websocket=websocket,
        agent_id=effective,
        conversation_id=conversation_id,
    )
    if not registry.register_writer(writer):
        await _ws_close(websocket, 1008, "already_connected")
        return

    try:
        while True:
            try:
                frame_text = await websocket.receive_text()
            except WebSocketDisconnect:
                break

            try:
                frame = json.loads(frame_text)
            except json.JSONDecodeError:
                await _ws_send_error(websocket, "invalid_frame", "malformed JSON")
                await _ws_close(websocket, 1002, "invalid_frame")
                return

            if not isinstance(frame, dict) or "type" not in frame:
                await _ws_send_error(
                    websocket, "invalid_frame", "missing 'type' field"
                )
                await _ws_close(websocket, 1002, "invalid_frame")
                return

            ftype = frame.get("type")

            if ftype == "message_start":
                if writer.current_message_id is not None:
                    await _ws_send_error(
                        websocket,
                        "protocol_error",
                        "message_start while already in message",
                    )
                    await _ws_close(websocket, 1002, "protocol_error")
                    return
                msg_id = records.generate_message_id()
                writer.current_message_id = msg_id
                writer.sequence_in_message = 0
                record = records.build_message_start(effective, msg_id)
                seq = await streams.append_record(s2, conversation_id, record)
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "message_started",
                            "message_id": msg_id,
                            "sequence": seq,
                        }
                    )
                )

            elif ftype == "token":
                if writer.current_message_id is None:
                    await _ws_send_error(
                        websocket, "protocol_error", "token outside message"
                    )
                    await _ws_close(websocket, 1002, "protocol_error")
                    return
                content = frame.get("content")
                if not isinstance(content, str):
                    await _ws_send_error(
                        websocket,
                        "invalid_frame",
                        "token missing 'content' string",
                    )
                    await _ws_close(websocket, 1002, "invalid_frame")
                    return
                if len(content) > config.MAX_TOKEN_LENGTH:
                    await _ws_send_error(
                        websocket,
                        "invalid_frame",
                        f"token exceeds max length of {config.MAX_TOKEN_LENGTH}",
                    )
                    await _ws_close(websocket, 1009, "invalid_frame")
                    return
                record = records.build_token(
                    effective,
                    writer.current_message_id,
                    content,
                    writer.sequence_in_message,
                )
                seq = await streams.append_record(s2, conversation_id, record)
                writer.sequence_in_message += 1
                await websocket.send_text(
                    json.dumps({"type": "token_ack", "sequence": seq})
                )

            elif ftype == "message_end":
                if writer.current_message_id is None:
                    await _ws_send_error(
                        websocket,
                        "protocol_error",
                        "message_end outside message",
                    )
                    await _ws_close(websocket, 1002, "protocol_error")
                    return
                msg_id = writer.current_message_id
                record = records.build_message_end(effective, msg_id)
                seq = await streams.append_record(s2, conversation_id, record)
                writer.current_message_id = None
                writer.sequence_in_message = 0
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "message_ended",
                            "message_id": msg_id,
                            "sequence": seq,
                        }
                    )
                )

            else:
                await _ws_send_error(
                    websocket,
                    "invalid_frame",
                    f"unknown frame type: {ftype!r}",
                )
                await _ws_close(websocket, 1002, "invalid_frame")
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "error in WS writer for %s/%s", effective, conversation_id
        )
        await _ws_close(websocket, 1011, "internal_error")
    finally:
        if writer.current_message_id is not None:
            try:
                abandoned = records.build_message_abandoned(
                    effective, writer.current_message_id, "disconnect"
                )
                await streams.append_record(s2, conversation_id, abandoned)
            except Exception:
                logger.exception(
                    "failed to append message_abandoned for %s/%s",
                    effective,
                    conversation_id,
                )
            writer.current_message_id = None
        registry.unregister_writer(effective, conversation_id)

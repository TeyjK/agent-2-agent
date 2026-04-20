import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from .. import db, records, streams
from ..deps import require_agent_id, require_conversation_membership

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/conversations")


class InviteBody(BaseModel):
    agent_id: str


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_conversation(
    request: Request,
    agent_id: str = Depends(require_agent_id),
) -> dict:
    conn = request.app.state.db
    conv_id = f"conv_{secrets.token_hex(16)}"
    await db.create_conversation(conn, conv_id)
    await db.add_member(conn, agent_id, conv_id)
    try:
        await streams.create_stream(request.app.state.s2, conv_id)
    except Exception:
        logger.exception("failed to create S2 stream for %s", conv_id)
    members = await db.list_members_of_conversation(conn, conv_id)
    return {"conversation_id": conv_id, "members": members}


@router.get("")
async def list_conversations(
    request: Request,
    agent_id: str = Depends(require_agent_id),
) -> dict:
    conn = request.app.state.db
    convs = await db.list_conversations_for_agent(conn, agent_id)
    out = []
    for c in convs:
        members = await db.list_members_of_conversation(conn, c["id"])
        out.append(
            {
                "conversation_id": c["id"],
                "members": members,
                "created_at": c["created_at"],
            }
        )
    return {"conversations": out}


@router.get("/{conversation_id}")
async def get_conversation(
    request: Request,
    membership: tuple[str, str] = Depends(require_conversation_membership),
) -> dict:
    _, conversation_id = membership
    conn = request.app.state.db
    conv = await db.get_conversation(conn, conversation_id)
    members = await db.list_members_of_conversation(conn, conversation_id)
    return {
        "conversation_id": conv["id"],
        "members": members,
        "created_at": conv["created_at"],
    }


@router.post("/{conversation_id}/members")
async def invite_member(
    request: Request,
    body: InviteBody,
    membership: tuple[str, str] = Depends(require_conversation_membership),
) -> dict:
    _, conversation_id = membership
    conn = request.app.state.db
    invitee = await db.get_agent(conn, body.agent_id)
    if invitee is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "agent_not_found",
                "message": f"No agent with ID {body.agent_id}",
            },
        )
    await db.add_member(conn, body.agent_id, conversation_id)
    members = await db.list_members_of_conversation(conn, conversation_id)
    return {"conversation_id": conversation_id, "members": members}


@router.delete(
    "/{conversation_id}/members/me",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def leave_conversation(
    request: Request,
    membership: tuple[str, str] = Depends(require_conversation_membership),
) -> Response:
    agent_id, conversation_id = membership
    conn = request.app.state.db
    if await db.count_members(conn, conversation_id) <= 1:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "last_member_cannot_leave",
                "message": "Cannot leave a conversation as its last member",
            },
        )
    await db.remove_member(conn, agent_id, conversation_id)

    registry = request.app.state.connections
    writer, readers = registry.close_agent_connections_in_conversation(
        agent_id, conversation_id
    )
    if writer is not None:
        if writer.current_message_id is not None:
            try:
                abandoned = records.build_message_abandoned(
                    agent_id, writer.current_message_id, "leave"
                )
                await streams.append_record(
                    request.app.state.s2, conversation_id, abandoned
                )
            except Exception:
                logger.exception(
                    "failed to append leave-abandoned for %s/%s",
                    agent_id,
                    conversation_id,
                )
            writer.current_message_id = None
        try:
            await writer.websocket.close(code=1000, reason="leave")
        except Exception:
            logger.exception(
                "failed to close writer ws on leave for %s/%s",
                agent_id,
                conversation_id,
            )
    for reader in readers:
        reader.close_event.set()

    return Response(status_code=status.HTTP_204_NO_CONTENT)

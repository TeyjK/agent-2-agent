from fastapi import Depends, Header, HTTPException, Request

from . import db


async def require_agent_id(
    request: Request,
    x_agent_id: str | None = Header(default=None, alias="X-Agent-Id"),
) -> str:
    if not x_agent_id:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "missing_agent_id",
                "message": "X-Agent-Id header is required",
            },
        )
    agent = await db.get_agent(request.app.state.db, x_agent_id)
    if agent is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "agent_not_found",
                "message": f"No agent with ID {x_agent_id}",
            },
        )
    return x_agent_id


async def require_conversation_membership(
    conversation_id: str,
    request: Request,
    agent_id: str = Depends(require_agent_id),
) -> tuple[str, str]:
    conn = request.app.state.db
    conv = await db.get_conversation(conn, conversation_id)
    if conv is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "conversation_not_found",
                "message": f"No conversation with ID {conversation_id}",
            },
        )
    if not await db.check_membership(conn, agent_id, conversation_id):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "not_a_member",
                "message": "Agent is not a member of this conversation",
            },
        )
    return agent_id, conversation_id

import secrets

from fastapi import APIRouter, Request, status

from .. import db

router = APIRouter()


@router.post("/agents", status_code=status.HTTP_201_CREATED)
async def register_agent(request: Request) -> dict[str, str]:
    agent_id = f"agent_{secrets.token_hex(16)}"
    await db.create_agent(request.app.state.db, agent_id)
    return {"agent_id": agent_id}

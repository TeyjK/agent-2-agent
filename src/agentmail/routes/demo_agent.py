from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel

from .. import config, db

router = APIRouter()


class RegisterDemoAgentBody(BaseModel):
    agent_id: str


@router.put("/demo-agent")
async def register_demo_agent(
    request: Request,
    body: RegisterDemoAgentBody,
    x_registration_token: str | None = Header(default=None),
) -> dict[str, str]:
    expected = config.DEMO_AGENT_REGISTRATION_TOKEN
    if not expected or x_registration_token != expected:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "invalid_registration_token",
                "message": "X-Registration-Token header missing or invalid",
            },
        )

    conn = request.app.state.db
    if await db.get_agent(conn, body.agent_id) is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "agent_not_found",
                "message": f"No agent with ID {body.agent_id}",
            },
        )

    await db.set_demo_agent_id(conn, body.agent_id)
    return {"agent_id": body.agent_id}


@router.get("/demo-agent")
async def get_demo_agent(request: Request) -> dict[str, str]:
    conn = request.app.state.db
    agent_id = await db.get_demo_agent_id(conn)
    if agent_id is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "demo_agent_not_registered",
                "message": "No demo agent has been registered",
            },
        )
    return {"agent_id": agent_id}

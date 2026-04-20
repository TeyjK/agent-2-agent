from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import config
from .connections import ConnectionRegistry
from .db import close_db, init_db
from .routes import agents as agents_routes
from .routes import conversations as conversations_routes
from .routes import demo_agent as demo_agent_routes
from .routes import messages as messages_routes
from .routes import streaming as streaming_routes
from .streams import create_s2_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(config.DATABASE_PATH)
    app.state.s2 = await create_s2_client()
    app.state.connections = ConnectionRegistry()
    yield
    await close_db(app.state.db)
    await app.state.s2.close()


app = FastAPI(lifespan=lifespan)
app.include_router(agents_routes.router)
app.include_router(conversations_routes.router)
app.include_router(messages_routes.router)
app.include_router(streaming_routes.router)
app.include_router(demo_agent_routes.router)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, dict) and "code" in detail and "message" in detail:
        body = {"error": detail}
    else:
        body = {
            "error": {
                "code": "internal_error",
                "message": str(detail) if detail is not None else "Unknown error",
            }
        }
    return JSONResponse(status_code=exc.status_code, content=body)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}

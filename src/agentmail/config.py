import os

from dotenv import load_dotenv

load_dotenv()


# ---- Service env vars ----
DATABASE_PATH: str = os.environ.get("DATABASE_PATH", "/data/agentmail.db")
S2_ACCESS_TOKEN: str = os.environ.get("S2_ACCESS_TOKEN", "")
S2_BASIN: str = os.environ.get("S2_BASIN", "agentmail")
S2_ACCOUNT_ENDPOINT: str | None = os.environ.get("S2_ACCOUNT_ENDPOINT") or None
S2_BASIN_ENDPOINT: str | None = os.environ.get("S2_BASIN_ENDPOINT") or None
PORT: int = int(os.environ.get("PORT", "8080"))
DEMO_AGENT_REGISTRATION_TOKEN: str = os.environ.get("DEMO_AGENT_REGISTRATION_TOKEN", "")

# ---- Demo agent env vars ----
ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")
DEMO_AGENT_BASE_URL: str = os.environ.get("DEMO_AGENT_BASE_URL", "http://localhost:8080")
DEMO_AGENT_ID_FILE: str = os.environ.get("DEMO_AGENT_ID_FILE", "/data/demo_agent_id.txt")


# ---- System limits (Resolution 15 + Resolution 27) ----
MAX_TOKEN_LENGTH: int = 10_000
MAX_TOKENS_PER_MESSAGE: int = 10_000
MAX_DISCRETE_MESSAGE_LENGTH: int = 100_000
MAX_CONCURRENT_CONNECTIONS_PER_AGENT: int = 50
MAX_MEMBERS_PER_CONVERSATION: int = 1_000
WEBSOCKET_IDLE_TIMEOUT_SECONDS: int = 300

# Read-position update cadence on SSE connections (Resolution 6).
READ_POSITION_UPDATE_EVERY_N_RECORDS: int = 10
READ_POSITION_UPDATE_EVERY_N_SECONDS: float = 5.0

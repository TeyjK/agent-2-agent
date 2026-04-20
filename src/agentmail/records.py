import json
import secrets
from datetime import datetime, timezone
from typing import Literal

AbandonReason = Literal["disconnect", "leave"]


def generate_message_id() -> str:
    return f"msg_{secrets.token_hex(16)}"


def now_iso() -> str:
    now = datetime.now(timezone.utc)
    ms = now.microsecond // 1000
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def build_message_start(agent_id: str, message_id: str) -> dict:
    return {
        "type": "message_start",
        "message_id": message_id,
        "agent_id": agent_id,
        "timestamp": now_iso(),
    }


def build_token(
    agent_id: str, message_id: str, content: str, sequence_in_message: int
) -> dict:
    return {
        "type": "token",
        "message_id": message_id,
        "agent_id": agent_id,
        "timestamp": now_iso(),
        "content": content,
        "sequence_in_message": sequence_in_message,
    }


def build_message_end(agent_id: str, message_id: str) -> dict:
    return {
        "type": "message_end",
        "message_id": message_id,
        "agent_id": agent_id,
        "timestamp": now_iso(),
    }


def build_message_abandoned(
    agent_id: str, message_id: str, reason: AbandonReason
) -> dict:
    if reason not in ("disconnect", "leave"):
        raise ValueError(f"invalid abandon reason: {reason!r}")
    return {
        "type": "message_abandoned",
        "message_id": message_id,
        "agent_id": agent_id,
        "timestamp": now_iso(),
        "reason": reason,
    }


def record_to_bytes(record: dict) -> bytes:
    return json.dumps(record).encode("utf-8")


def record_from_bytes(data: bytes) -> dict:
    return json.loads(data)

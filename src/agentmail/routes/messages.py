from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from .. import config, records, streams
from ..deps import require_conversation_membership

router = APIRouter(prefix="/conversations")


class SendMessageBody(BaseModel):
    text: str


@router.post(
    "/{conversation_id}/messages",
    status_code=status.HTTP_201_CREATED,
)
async def send_discrete_message(
    request: Request,
    body: SendMessageBody,
    membership: tuple[str, str] = Depends(require_conversation_membership),
) -> dict:
    agent_id, conversation_id = membership

    if len(body.text) > config.MAX_DISCRETE_MESSAGE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_frame",
                "message": (
                    f"text exceeds maximum length of "
                    f"{config.MAX_DISCRETE_MESSAGE_LENGTH} characters"
                ),
            },
        )

    message_id = records.generate_message_id()
    batch = [
        records.build_message_start(agent_id, message_id),
        records.build_token(agent_id, message_id, body.text, 0),
        records.build_message_end(agent_id, message_id),
    ]
    seq_nums = await streams.append_records_batch(
        request.app.state.s2, conversation_id, batch
    )
    return {
        "message_id": message_id,
        "start_sequence": seq_nums[0],
        "end_sequence": seq_nums[2],
    }

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ActiveWriter:
    websocket: Any
    agent_id: str
    conversation_id: str
    current_message_id: str | None = None
    sequence_in_message: int = 0


@dataclass
class ActiveReader:
    agent_id: str
    conversation_id: str
    close_event: asyncio.Event = field(default_factory=asyncio.Event)


class ConnectionRegistry:
    def __init__(self) -> None:
        self.writers: dict[tuple[str, str], ActiveWriter] = {}
        self.readers: dict[tuple[str, str], list[ActiveReader]] = {}

    def register_writer(self, writer: ActiveWriter) -> bool:
        key = (writer.agent_id, writer.conversation_id)
        if key in self.writers:
            return False
        self.writers[key] = writer
        return True

    def unregister_writer(self, agent_id: str, conversation_id: str) -> None:
        self.writers.pop((agent_id, conversation_id), None)

    def get_writer(
        self, agent_id: str, conversation_id: str
    ) -> ActiveWriter | None:
        return self.writers.get((agent_id, conversation_id))

    def register_reader(self, reader: ActiveReader) -> None:
        key = (reader.agent_id, reader.conversation_id)
        self.readers.setdefault(key, []).append(reader)

    def unregister_reader(self, reader: ActiveReader) -> None:
        key = (reader.agent_id, reader.conversation_id)
        lst = self.readers.get(key)
        if not lst:
            return
        self.readers[key] = [r for r in lst if r is not reader]
        if not self.readers[key]:
            del self.readers[key]

    def get_readers_for(
        self, agent_id: str, conversation_id: str
    ) -> list[ActiveReader]:
        return list(self.readers.get((agent_id, conversation_id), []))

    def close_agent_connections_in_conversation(
        self, agent_id: str, conversation_id: str
    ) -> tuple[ActiveWriter | None, list[ActiveReader]]:
        key = (agent_id, conversation_id)
        writer = self.writers.pop(key, None)
        readers = self.readers.pop(key, [])
        return writer, readers

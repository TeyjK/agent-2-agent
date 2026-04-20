import aiosqlite

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS agents (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS conversations (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memberships (
        agent_id TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        joined_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        PRIMARY KEY (agent_id, conversation_id),
        FOREIGN KEY (agent_id) REFERENCES agents(id),
        FOREIGN KEY (conversation_id) REFERENCES conversations(id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_memberships_agent ON memberships(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_memberships_conversation ON memberships(conversation_id)",
    """
    CREATE TABLE IF NOT EXISTS read_positions (
        agent_id TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        sequence_number INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        PRIMARY KEY (agent_id, conversation_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS demo_agent_registration (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        agent_id TEXT NOT NULL,
        registered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    """,
)


async def init_db(path: str) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(path)
    conn.row_factory = aiosqlite.Row

    if path != ":memory:":
        await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA synchronous=NORMAL")

    for statement in SCHEMA_STATEMENTS:
        await conn.execute(statement)
    await conn.commit()
    return conn


async def close_db(conn: aiosqlite.Connection) -> None:
    await conn.close()


# ---- Agents ----

async def create_agent(conn: aiosqlite.Connection, agent_id: str) -> None:
    await conn.execute("INSERT INTO agents (id) VALUES (?)", (agent_id,))
    await conn.commit()


async def get_agent(conn: aiosqlite.Connection, agent_id: str) -> dict | None:
    async with conn.execute(
        "SELECT id, created_at FROM agents WHERE id = ?", (agent_id,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


# ---- Conversations ----

async def create_conversation(conn: aiosqlite.Connection, conv_id: str) -> None:
    await conn.execute("INSERT INTO conversations (id) VALUES (?)", (conv_id,))
    await conn.commit()


async def get_conversation(conn: aiosqlite.Connection, conv_id: str) -> dict | None:
    async with conn.execute(
        "SELECT id, created_at FROM conversations WHERE id = ?", (conv_id,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def list_conversations_for_agent(
    conn: aiosqlite.Connection, agent_id: str
) -> list[dict]:
    async with conn.execute(
        """
        SELECT c.id, c.created_at
        FROM conversations c
        JOIN memberships m ON m.conversation_id = c.id
        WHERE m.agent_id = ?
        ORDER BY c.created_at ASC
        """,
        (agent_id,),
    ) as cur:
        rows = await cur.fetchall()
    return [dict(row) for row in rows]


# ---- Memberships ----

async def add_member(
    conn: aiosqlite.Connection, agent_id: str, conv_id: str
) -> None:
    await conn.execute(
        "INSERT OR IGNORE INTO memberships (agent_id, conversation_id) VALUES (?, ?)",
        (agent_id, conv_id),
    )
    await conn.commit()


async def remove_member(
    conn: aiosqlite.Connection, agent_id: str, conv_id: str
) -> None:
    await conn.execute(
        "DELETE FROM memberships WHERE agent_id = ? AND conversation_id = ?",
        (agent_id, conv_id),
    )
    await conn.commit()


async def check_membership(
    conn: aiosqlite.Connection, agent_id: str, conv_id: str
) -> bool:
    async with conn.execute(
        "SELECT 1 FROM memberships WHERE agent_id = ? AND conversation_id = ?",
        (agent_id, conv_id),
    ) as cur:
        row = await cur.fetchone()
    return row is not None


async def list_members_of_conversation(
    conn: aiosqlite.Connection, conv_id: str
) -> list[str]:
    async with conn.execute(
        "SELECT agent_id FROM memberships WHERE conversation_id = ? ORDER BY joined_at ASC",
        (conv_id,),
    ) as cur:
        rows = await cur.fetchall()
    return [row["agent_id"] for row in rows]


async def count_members(conn: aiosqlite.Connection, conv_id: str) -> int:
    async with conn.execute(
        "SELECT COUNT(*) AS n FROM memberships WHERE conversation_id = ?",
        (conv_id,),
    ) as cur:
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


# ---- Read positions ----

async def get_read_position(
    conn: aiosqlite.Connection, agent_id: str, conv_id: str
) -> int:
    async with conn.execute(
        "SELECT sequence_number FROM read_positions WHERE agent_id = ? AND conversation_id = ?",
        (agent_id, conv_id),
    ) as cur:
        row = await cur.fetchone()
    return int(row["sequence_number"]) if row else 0


async def set_read_position(
    conn: aiosqlite.Connection,
    agent_id: str,
    conv_id: str,
    sequence_number: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO read_positions (agent_id, conversation_id, sequence_number)
        VALUES (?, ?, ?)
        ON CONFLICT(agent_id, conversation_id) DO UPDATE SET
            sequence_number = excluded.sequence_number,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        """,
        (agent_id, conv_id, sequence_number),
    )
    await conn.commit()


# ---- Demo agent registration ----

async def set_demo_agent_id(conn: aiosqlite.Connection, agent_id: str) -> None:
    await conn.execute(
        """
        INSERT INTO demo_agent_registration (id, agent_id) VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET
            agent_id = excluded.agent_id,
            registered_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        """,
        (agent_id,),
    )
    await conn.commit()


async def get_demo_agent_id(conn: aiosqlite.Connection) -> str | None:
    async with conn.execute(
        "SELECT agent_id FROM demo_agent_registration WHERE id = 1"
    ) as cur:
        row = await cur.fetchone()
    return row["agent_id"] if row else None

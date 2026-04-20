import aiosqlite
import pytest
import pytest_asyncio

from agentmail.db import (
    add_member,
    check_membership,
    count_members,
    close_db,
    create_agent,
    create_conversation,
    get_agent,
    get_conversation,
    get_demo_agent_id,
    get_read_position,
    init_db,
    list_conversations_for_agent,
    list_members_of_conversation,
    remove_member,
    set_demo_agent_id,
    set_read_position,
)


@pytest_asyncio.fixture
async def db():
    conn = await init_db(":memory:")
    try:
        yield conn
    finally:
        await close_db(conn)


# ---- init_db ----

async def test_init_db_creates_all_tables(db):
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ) as cur:
        rows = await cur.fetchall()
    names = {row["name"] for row in rows}
    assert {
        "agents",
        "conversations",
        "memberships",
        "read_positions",
        "demo_agent_registration",
    }.issubset(names)


async def test_init_db_creates_membership_indexes(db):
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ) as cur:
        rows = await cur.fetchall()
    names = {row["name"] for row in rows}
    assert "idx_memberships_agent" in names
    assert "idx_memberships_conversation" in names


async def test_init_db_enables_foreign_keys(db):
    async with db.execute("PRAGMA foreign_keys") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_init_db_is_idempotent(db):
    conn2 = await init_db(":memory:")
    await close_db(conn2)
    # Running init_db on the same already-initialized connection also works
    from agentmail.db import SCHEMA_STATEMENTS

    for statement in SCHEMA_STATEMENTS:
        await db.execute(statement)
    await db.commit()


# ---- Agents ----

async def test_create_and_get_agent(db):
    await create_agent(db, "agent_aaa")
    got = await get_agent(db, "agent_aaa")
    assert got is not None
    assert got["id"] == "agent_aaa"
    assert got["created_at"]


async def test_get_agent_missing_returns_none(db):
    assert await get_agent(db, "agent_nope") is None


async def test_duplicate_agent_raises(db):
    await create_agent(db, "agent_dup")
    with pytest.raises(aiosqlite.IntegrityError):
        await create_agent(db, "agent_dup")


# ---- Conversations ----

async def test_create_and_get_conversation(db):
    await create_conversation(db, "conv_aaa")
    got = await get_conversation(db, "conv_aaa")
    assert got is not None
    assert got["id"] == "conv_aaa"
    assert got["created_at"]


async def test_get_conversation_missing_returns_none(db):
    assert await get_conversation(db, "conv_nope") is None


async def test_duplicate_conversation_raises(db):
    await create_conversation(db, "conv_dup")
    with pytest.raises(aiosqlite.IntegrityError):
        await create_conversation(db, "conv_dup")


# ---- Memberships ----

async def test_add_and_check_membership(db):
    await create_agent(db, "agent_a")
    await create_conversation(db, "conv_a")
    assert await check_membership(db, "agent_a", "conv_a") is False
    await add_member(db, "agent_a", "conv_a")
    assert await check_membership(db, "agent_a", "conv_a") is True


async def test_add_member_is_idempotent(db):
    await create_agent(db, "agent_a")
    await create_conversation(db, "conv_a")
    await add_member(db, "agent_a", "conv_a")
    await add_member(db, "agent_a", "conv_a")
    assert await count_members(db, "conv_a") == 1


async def test_remove_member(db):
    await create_agent(db, "agent_a")
    await create_conversation(db, "conv_a")
    await add_member(db, "agent_a", "conv_a")
    await remove_member(db, "agent_a", "conv_a")
    assert await check_membership(db, "agent_a", "conv_a") is False


async def test_remove_non_member_is_noop(db):
    await create_agent(db, "agent_a")
    await create_conversation(db, "conv_a")
    await remove_member(db, "agent_a", "conv_a")
    assert await check_membership(db, "agent_a", "conv_a") is False


async def test_membership_fk_enforced_on_agent(db):
    await create_conversation(db, "conv_a")
    with pytest.raises(aiosqlite.IntegrityError):
        await add_member(db, "agent_missing", "conv_a")


async def test_membership_fk_enforced_on_conversation(db):
    await create_agent(db, "agent_a")
    with pytest.raises(aiosqlite.IntegrityError):
        await add_member(db, "agent_a", "conv_missing")


async def test_list_members_of_conversation(db):
    await create_agent(db, "agent_a")
    await create_agent(db, "agent_b")
    await create_agent(db, "agent_c")
    await create_conversation(db, "conv_a")
    await add_member(db, "agent_a", "conv_a")
    await add_member(db, "agent_b", "conv_a")
    members = await list_members_of_conversation(db, "conv_a")
    assert set(members) == {"agent_a", "agent_b"}
    assert len(members) == 2


async def test_count_members(db):
    await create_agent(db, "agent_a")
    await create_agent(db, "agent_b")
    await create_conversation(db, "conv_a")
    assert await count_members(db, "conv_a") == 0
    await add_member(db, "agent_a", "conv_a")
    assert await count_members(db, "conv_a") == 1
    await add_member(db, "agent_b", "conv_a")
    assert await count_members(db, "conv_a") == 2


async def test_list_conversations_for_agent(db):
    await create_agent(db, "agent_a")
    await create_agent(db, "agent_b")
    await create_conversation(db, "conv_1")
    await create_conversation(db, "conv_2")
    await create_conversation(db, "conv_3")
    await add_member(db, "agent_a", "conv_1")
    await add_member(db, "agent_a", "conv_2")
    await add_member(db, "agent_b", "conv_3")

    a_convs = await list_conversations_for_agent(db, "agent_a")
    assert {c["id"] for c in a_convs} == {"conv_1", "conv_2"}

    b_convs = await list_conversations_for_agent(db, "agent_b")
    assert {c["id"] for c in b_convs} == {"conv_3"}


# ---- Read positions ----

async def test_get_read_position_returns_zero_when_missing(db):
    assert await get_read_position(db, "agent_a", "conv_a") == 0


async def test_set_then_get_read_position(db):
    await set_read_position(db, "agent_a", "conv_a", 42)
    assert await get_read_position(db, "agent_a", "conv_a") == 42


async def test_set_read_position_upserts(db):
    await set_read_position(db, "agent_a", "conv_a", 5)
    await set_read_position(db, "agent_a", "conv_a", 99)
    assert await get_read_position(db, "agent_a", "conv_a") == 99
    async with db.execute(
        "SELECT COUNT(*) AS n FROM read_positions WHERE agent_id = ? AND conversation_id = ?",
        ("agent_a", "conv_a"),
    ) as cur:
        row = await cur.fetchone()
    assert row["n"] == 1


async def test_read_positions_are_per_pair(db):
    await set_read_position(db, "agent_a", "conv_1", 10)
    await set_read_position(db, "agent_a", "conv_2", 20)
    await set_read_position(db, "agent_b", "conv_1", 30)
    assert await get_read_position(db, "agent_a", "conv_1") == 10
    assert await get_read_position(db, "agent_a", "conv_2") == 20
    assert await get_read_position(db, "agent_b", "conv_1") == 30
    assert await get_read_position(db, "agent_b", "conv_2") == 0


# ---- Demo agent registration ----

async def test_get_demo_agent_id_returns_none_when_missing(db):
    assert await get_demo_agent_id(db) is None


async def test_set_then_get_demo_agent_id(db):
    await set_demo_agent_id(db, "agent_demo_1")
    assert await get_demo_agent_id(db) == "agent_demo_1"


async def test_set_demo_agent_id_upserts_singleton(db):
    await set_demo_agent_id(db, "agent_demo_1")
    await set_demo_agent_id(db, "agent_demo_2")
    assert await get_demo_agent_id(db) == "agent_demo_2"
    async with db.execute("SELECT COUNT(*) AS n FROM demo_agent_registration") as cur:
        row = await cur.fetchone()
    assert row["n"] == 1

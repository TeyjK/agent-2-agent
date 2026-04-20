import re

import pytest

from agentmail import records


def test_message_id_format():
    mid = records.generate_message_id()
    assert re.match(r"^msg_[0-9a-f]{32}$", mid) is not None


def test_now_iso_format():
    ts = records.now_iso()
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", ts) is not None


def test_build_message_start():
    r = records.build_message_start("agent_A", "msg_M")
    assert r["type"] == "message_start"
    assert r["message_id"] == "msg_M"
    assert r["agent_id"] == "agent_A"
    assert "timestamp" in r


def test_build_token():
    r = records.build_token("agent_A", "msg_M", "hi", 3)
    assert r["type"] == "token"
    assert r["message_id"] == "msg_M"
    assert r["agent_id"] == "agent_A"
    assert r["content"] == "hi"
    assert r["sequence_in_message"] == 3
    assert "timestamp" in r


def test_build_message_end():
    r = records.build_message_end("agent_A", "msg_M")
    assert r["type"] == "message_end"
    assert r["message_id"] == "msg_M"
    assert r["agent_id"] == "agent_A"
    assert "timestamp" in r


def test_build_message_abandoned_disconnect():
    r = records.build_message_abandoned("agent_A", "msg_M", "disconnect")
    assert r["type"] == "message_abandoned"
    assert r["reason"] == "disconnect"


def test_build_message_abandoned_leave():
    r = records.build_message_abandoned("agent_A", "msg_M", "leave")
    assert r["reason"] == "leave"


def test_build_message_abandoned_invalid_reason():
    with pytest.raises(ValueError):
        records.build_message_abandoned("agent_A", "msg_M", "bogus")


def test_record_json_roundtrip():
    originals = [
        records.build_message_start("agent_A", "msg_M"),
        records.build_token("agent_A", "msg_M", "hello", 0),
        records.build_message_end("agent_A", "msg_M"),
        records.build_message_abandoned("agent_A", "msg_M", "leave"),
    ]
    for r in originals:
        assert records.record_from_bytes(records.record_to_bytes(r)) == r

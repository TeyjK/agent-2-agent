from agentmail.connections import ActiveReader, ActiveWriter, ConnectionRegistry


def test_register_writer_returns_true_then_false_on_duplicate():
    reg = ConnectionRegistry()
    w = ActiveWriter(websocket=object(), agent_id="a", conversation_id="c")
    assert reg.register_writer(w) is True
    w2 = ActiveWriter(websocket=object(), agent_id="a", conversation_id="c")
    assert reg.register_writer(w2) is False


def test_get_writer_returns_registered_writer():
    reg = ConnectionRegistry()
    w = ActiveWriter(websocket=object(), agent_id="a", conversation_id="c")
    reg.register_writer(w)
    assert reg.get_writer("a", "c") is w
    assert reg.get_writer("a", "other") is None


def test_unregister_writer_clears_it():
    reg = ConnectionRegistry()
    w = ActiveWriter(websocket=object(), agent_id="a", conversation_id="c")
    reg.register_writer(w)
    reg.unregister_writer("a", "c")
    assert reg.get_writer("a", "c") is None
    # re-registering after unregister should succeed
    w2 = ActiveWriter(websocket=object(), agent_id="a", conversation_id="c")
    assert reg.register_writer(w2) is True


def test_register_reader_allows_multiple_per_key():
    reg = ConnectionRegistry()
    r1 = ActiveReader(agent_id="a", conversation_id="c")
    r2 = ActiveReader(agent_id="a", conversation_id="c")
    reg.register_reader(r1)
    reg.register_reader(r2)
    readers = reg.get_readers_for("a", "c")
    assert len(readers) == 2
    assert r1 in readers and r2 in readers


def test_unregister_reader_removes_specific_reader():
    reg = ConnectionRegistry()
    r1 = ActiveReader(agent_id="a", conversation_id="c")
    r2 = ActiveReader(agent_id="a", conversation_id="c")
    reg.register_reader(r1)
    reg.register_reader(r2)
    reg.unregister_reader(r1)
    readers = reg.get_readers_for("a", "c")
    assert readers == [r2]
    reg.unregister_reader(r2)
    assert reg.get_readers_for("a", "c") == []


def test_close_agent_connections_returns_and_unregisters():
    reg = ConnectionRegistry()
    w = ActiveWriter(websocket=object(), agent_id="a", conversation_id="c")
    r1 = ActiveReader(agent_id="a", conversation_id="c")
    r2 = ActiveReader(agent_id="a", conversation_id="c")
    # unrelated entries that should be untouched
    other_w = ActiveWriter(websocket=object(), agent_id="a", conversation_id="other")
    other_r = ActiveReader(agent_id="b", conversation_id="c")
    reg.register_writer(w)
    reg.register_reader(r1)
    reg.register_reader(r2)
    reg.register_writer(other_w)
    reg.register_reader(other_r)

    closed_w, closed_rs = reg.close_agent_connections_in_conversation("a", "c")
    assert closed_w is w
    assert len(closed_rs) == 2
    assert r1 in closed_rs and r2 in closed_rs

    assert reg.get_writer("a", "c") is None
    assert reg.get_readers_for("a", "c") == []
    assert reg.get_writer("a", "other") is other_w
    assert reg.get_readers_for("b", "c") == [other_r]

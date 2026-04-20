# Testing Plan

## Principles

- **Integration tests are the main source of confidence.** The service is a stateful, stream-based system; unit tests on individual functions don't catch the bugs that matter.
- **Tests use S2 Lite (the official in-memory S2 emulator) and real SQLite.** No mocks for external dependencies. The real S2 SDK is pointed at a local S2 Lite instance via endpoint override — same SDK, same behaviors, no network, no flakiness, no credentials needed for routine test runs.
- **Test agents are scripted, not LLM-powered.** Tests must be fast, deterministic, and cheap. Hardcoded text is used everywhere.
- **Each test is independent.** Fresh database, fresh S2 streams per test.

## Test Infrastructure

- `pytest` + `pytest-asyncio` as the framework
- `httpx.AsyncClient` for HTTP requests
- `websockets` library for WebSocket client tests
- `httpx-sse` for SSE consumption tests

**S2 Lite setup.** S2 Lite is an MIT-licensed, officially-maintained in-memory implementation of the S2 API, embedded in the S2 CLI (`s2 lite` subcommand) and distributed as a Docker image (`ghcr.io/s2-streamstore/s2`). The test suite launches it as a Docker container once per test session:

```bash
docker run --rm -d -p 8080:80 --name s2-lite-test ghcr.io/s2-streamstore/s2 lite
```

The test `conftest.py` starts this container as a session-scoped fixture (or assumes it's already running), points the S2 client at `http://localhost:8080` via endpoint override, and uses an arbitrary access token (`S2_ACCESS_TOKEN=ignored`). The `s2-sdk` Python package supports endpoint overrides for exactly this purpose (pass `Endpoints(account="http://localhost:8080", basin="http://localhost:8080")` to the `S2` constructor).

**Why S2 Lite instead of real S2 for tests:** tests run without network, without credentials, without hitting S2's production infrastructure, and without risk of polluting real basins. Since S2 Lite is the same code the S2 team runs internally (just with an in-memory backend instead of object storage), behavior is identical — sequence assignment, read sessions, tailing semantics, everything. This is strictly better than a hand-rolled mock.

**Small end-to-end smoke suite against real S2.** In addition to the S2 Lite-based test suite, we maintain a small set (5-10) of end-to-end tests that run against real S2 using the `agentmail-test` basin. These run manually before deployment, not on every test invocation. They catch any discrepancy between S2 Lite's behavior and real S2. A pytest marker (`@pytest.mark.real_s2`) gates them so they're skipped by default.

**Database.** Each test uses an in-memory SQLite database (`DATABASE_PATH=:memory:`). No file cleanup needed.

**Per-test isolation.** Each test function generates a unique stream name prefix (e.g., `test_{uuid}_conv_...`) so streams don't collide across tests. At end of test, delete created streams via `basin.delete_stream(...)`.

## Test Scenarios

### Basic Flow
1. `test_register_agent` — POST /agents, assert response has agent_id
2. `test_create_conversation` — register agent, create conversation, assert creator is a member
3. `test_list_conversations_empty` — new agent, GET /conversations, assert empty list
4. `test_list_conversations_with_conversations` — agent creates two conversations, GET returns both
5. `test_invite_agent` — create conversation, invite another agent, assert both are members
6. `test_invite_nonexistent_agent` — assert 404 agent_not_found
7. `test_invite_already_member` — invite same agent twice, assert no-op (returns 200 with same member list)
8. `test_leave_conversation` — agent joins and leaves, assert removed from members
9. `test_leave_when_last_member` — single-member conversation, attempt leave, assert 409 last_member_cannot_leave
10. `test_non_member_cannot_read` — agent tries to read from conversation it's not in, assert 403 not_a_member
11. `test_non_member_cannot_write` — agent tries to send message to conversation it's not in, assert 403 not_a_member
12. `test_missing_agent_id_header` — make a request that requires the header without it, assert 400 missing_agent_id
13. `test_nonexistent_agent_id_header` — make a request with a fake agent ID, assert 404 agent_not_found

### Discrete Messages
14. `test_send_discrete_message` — send a message via POST, verify all three records (start, token, end) appear in the stream when read back
15. `test_discrete_message_visible_to_other_member` — agent A sends message, agent B reads history and sees it
16. `test_discrete_message_attribution` — verify all three records have agent_id matching the sender

### Streaming Writes
17. `test_websocket_streaming_write` — open WebSocket, send start + multiple tokens + end, verify all records in S2 with correct message_id grouping
18. `test_websocket_token_before_start` — send a token frame without start_message first, assert error frame `no_open_message` and connection closed
19. `test_websocket_double_start` — send start_message twice without end, assert error frame `message_already_open`
20. `test_websocket_end_without_start` — send end_message without start, assert error frame `no_open_message`
21. `test_websocket_invalid_frame` — send malformed JSON or missing required field, assert error frame `invalid_frame`
22. `test_websocket_non_member` — non-member tries to connect, assert error frame and close
23. `test_websocket_acks_come_in_order` — send frames, verify ack_start → ack_token → ack_end come back in order with monotonically increasing sequence numbers
24. `test_websocket_oversize_token_rejected` — send a token with content exceeding MAX_TOKEN_LENGTH, assert `invalid_frame` error
25. `test_websocket_already_connected` — agent opens two WebSockets to the same conversation, assert second gets `already_connected` error and closes
26. `test_websocket_sequence_in_message_serialization` — stream many small tokens rapidly, verify `sequence_in_message` is 0, 1, 2, ... in order and no reordering occurred (tests per-connection serialization)

### Streaming Reads
27. `test_sse_receives_historical` — agent A sends 3 messages, agent B opens SSE, receives all historical records in order
28. `test_sse_receives_live` — agent B opens SSE on empty conversation, agent A sends a message, B receives records in real time
29. `test_sse_from_zero` — agent B has read 5 records, reconnects with `?from=0`, receives everything from the beginning
30. `test_sse_from_latest` — conversation has 10 records, agent opens SSE with `?from=latest`, receives nothing initially, then receives only new records appended after connection
31. `test_sse_read_position_persistence` — agent B reads records and disconnects cleanly, reconnects without `from=`, server resumes from the last saved position
32. `test_sse_read_position_batched_updates` — agent receives 5 records then crashes (connection abruptly closes), reconnects, verify a few most-recent records are re-delivered (expected behavior under batched position updates)
33. `test_sse_read_position_updates_on_clean_close` — agent receives 20 records and calls close, reconnects without from, verify no records are re-delivered (position was updated on clean close)
34. `test_sse_transition_historical_to_live` — agent B connects while historical records exist, consumes history, verify it then receives new live records without a gap
35. `test_sse_non_member` — non-member attempts to connect, assert 403 or error event

### Concurrent Writes
36. `test_concurrent_writers_basic` — two agents open WebSockets simultaneously, both stream messages, verify all tokens appear in the stream with correct message_id attribution
37. `test_concurrent_interleaving` — two agents write simultaneously, verify that when grouped by message_id, each message's tokens are in correct order (sequence_in_message increments correctly per message) despite interleaving at the stream level
38. `test_concurrent_readers` — one writer, multiple readers, verify all readers receive every record

### Offline / Reconnect
39. `test_offline_agent_catches_up` — agent B disconnects, agent A sends 10 messages, agent B reconnects and receives all 10 via replay
40. `test_reconnect_after_partial_read` — agent B reads half the messages, disconnects cleanly, reconnects, only receives the unread half
41. `test_reconnect_during_concurrent_write` — agent B reconnects while agent A is mid-stream, verify no tokens are lost (re-delivery of recent records is acceptable; deduplication by sequence is the client's responsibility)

### Crash / Disconnect Mid-Message
42. `test_client_crash_mid_message_appends_abandoned` — agent opens WebSocket, sends start + tokens, abruptly closes, verify `message_abandoned` record appears with `reason: "disconnect"`
43. `test_client_crash_preserves_partial_tokens` — same as above but verify the partial tokens remain in the stream (not deleted)
44. `test_client_crash_abandoned_visible_to_readers` — while agent A is mid-message, agent B is tailing; A crashes; B receives the `message_abandoned` record

### Server-Crash Mid-Message (Known Limitation)
45. `test_server_crash_orphaned_start_no_end` — simulate server crash by killing the WebSocket handler task while a message is open, restart server, verify the stream contains `message_start` + tokens with no `message_end` or `message_abandoned`. This test documents the known limitation; the expected behavior is "the orphan exists" — clients must use the 60s timeout heuristic to detect this.

### Group Conversations
46. `test_five_agents_in_conversation` — 5 agents, all invited to one conversation, one sends a message, all receive it
47. `test_three_concurrent_writers_in_group` — 5 agents in conversation, 3 write simultaneously, all 5 receive every record in the same order
48. `test_invite_mid_conversation` — 2 agents chat for a while, invite agent C; C opens SSE from 0, receives full history

### Leave During Active Streams
49. `test_leave_terminates_own_websocket` — agent opens WebSocket, starts a message, calls DELETE /members/me; verify WebSocket is closed and `message_abandoned` (`reason: "leave"`) appears
50. `test_leave_terminates_own_sse` — agent opens SSE, calls DELETE /members/me from another connection; verify SSE connection is closed
51. `test_leave_does_not_affect_others` — agent A is mid-stream in conversation X; agent B leaves the same conversation; verify A's WebSocket is untouched and A's message completes normally
52. `test_rejoin_sees_own_history` — agent leaves, is re-invited, reads from sequence 0, sees its own prior messages attributed to it

### Demo Agent Discovery
53. `test_put_demo_agent_with_token` — PUT /demo-agent with correct `X-Registration-Token`, assert 200 and agent ID stored
54. `test_put_demo_agent_wrong_token` — PUT /demo-agent with wrong token, assert 403
55. `test_get_demo_agent_before_registered` — GET /demo-agent before any PUT, assert 404
56. `test_get_demo_agent_after_registered` — PUT then GET, assert 200 with the agent ID

### Attribution
57. `test_message_attribution_in_records` — every record (start, token, end, abandoned) has correct agent_id
58. `test_self_messages_delivered_to_sender` — agent A writes a message and has an SSE connection open; verify A receives its own tokens

### Message Structure
59. `test_message_ids_unique` — send many messages, verify all message_ids are unique
60. `test_sequence_in_message_monotonic` — within a single message, tokens have sequence_in_message 0, 1, 2, ... in order
61. `test_stream_sequence_monotonic` — across a conversation, stream sequence numbers are monotonically increasing

## Unit Tests (lightweight)

- `test_agent_id_format` — generated IDs match `agent_[0-9a-f]{32}` pattern
- `test_record_json_roundtrip` — serialize and deserialize each record type, assert equality
- `test_config_loads_from_env` — set env vars, load config, assert values match

## Running Tests

```bash
# Set test env vars
export S2_ACCESS_TOKEN=...
export S2_BASIN=agentmail-test
export DATABASE_PATH=:memory:
export ANTHROPIC_API_KEY=not-used-by-tests

# Run all tests
pytest

# Run specific test
pytest tests/test_streaming.py::test_websocket_streaming_write -v
```

## What We DON'T Test

- Claude API integration — the demo agent is tested manually via the live service
- Load/performance testing — out of scope
- Network failure scenarios beyond clean disconnects — out of scope for this implementation (noted in no-restrictions design)

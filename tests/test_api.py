import hashlib

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

GAP = 300  # seconds


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=GAP)
    with TestClient(app) as c:
        yield c


def submit(client, device_id, event_id, occurred_at):
    return client.post(
        "/events",
        json={"device_id": device_id, "event_id": event_id, "occurred_at": occurred_at},
    )


def expected_session_id(device_id, members):
    """Reference implementation of the documented session-id scheme."""
    h = hashlib.sha256()
    h.update(device_id.encode())
    for occurred_at, event_id in members:
        h.update(b"\x00")
        h.update(occurred_at.encode())
        h.update(b"\x1f")
        h.update(event_id.encode())
    return h.hexdigest()


def test_out_of_order_arrival_same_result(client):
    """Arrival order must not affect grouping or session ids."""
    ordered = [
        ("e1", "2026-09-20T10:00:00Z"),
        ("e2", "2026-09-20T10:02:00Z"),
        ("e3", "2026-09-20T10:20:00Z"),  # gap > 300s -> new session
        ("e4", "2026-09-20T10:21:00Z"),
    ]
    for event_id, ts in ordered:
        assert submit(client, "dev-a", f"a-{event_id}", ts).status_code == 201
    sessions_ordered = client.get("/devices/dev-a/sessions").json()

    shuffled = [ordered[2], ordered[0], ordered[3], ordered[1]]
    for event_id, ts in shuffled:
        assert submit(client, "dev-b", f"b-{event_id}", ts).status_code == 201
    sessions_shuffled = client.get("/devices/dev-b/sessions").json()

    # Different devices -> different ids, but structure must be identical.
    assert [s["event_count"] for s in sessions_ordered] == [2, 2]
    assert [s["event_count"] for s in sessions_shuffled] == [2, 2]
    assert [
        [m["event_id"][2:] for m in s["members"]] for s in sessions_ordered
    ] == [[m["event_id"][2:] for m in s["members"]] for s in sessions_shuffled]


def test_late_event_bridges_two_sessions(client):
    """A late event landing in the gap merges both groups transitively."""
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    submit(client, "dev-1", "e2", "2026-09-20T10:01:40Z")
    submit(client, "dev-1", "e3", "2026-09-20T10:08:20Z")  # 400s gap -> new session
    submit(client, "dev-1", "e4", "2026-09-20T10:10:00Z")

    sessions = client.get("/devices/dev-1/sessions").json()
    assert len(sessions) == 2
    first_session_id = sessions[0]["session_id"]

    # Late event at 10:05:00: 200s after e2 and 200s before e3 -> bridge.
    resp = submit(client, "dev-1", "e5", "2026-09-20T10:05:00Z")
    assert resp.status_code == 201
    merged = resp.json()["session"]
    assert merged["event_count"] == 5
    assert [m["event_id"] for m in merged["members"]] == ["e1", "e2", "e5", "e3", "e4"]
    assert merged["session_id"] != first_session_id

    sessions = client.get("/devices/dev-1/sessions").json()
    assert len(sessions) == 1
    assert sessions[0]["session_id"] == merged["session_id"]


def test_idempotent_replay(client):
    resp1 = submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    assert resp1.status_code == 201
    assert resp1.json()["replay"] is False

    resp2 = submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    assert resp2.status_code == 200
    assert resp2.json()["replay"] is True
    assert resp2.json()["session"] == resp1.json()["session"]

    sessions = client.get("/devices/dev-1/sessions").json()
    assert len(sessions) == 1
    assert sessions[0]["event_count"] == 1


def test_conflicting_event_id_returns_409(client):
    assert submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z").status_code == 201

    # Same id, different occurred_at.
    resp = submit(client, "dev-1", "e1", "2026-09-20T10:00:01Z")
    assert resp.status_code == 409

    # Same id, different device.
    resp = submit(client, "dev-2", "e1", "2026-09-20T10:00:00Z")
    assert resp.status_code == 409

    # Original event untouched.
    sessions = client.get("/devices/dev-1/sessions").json()
    assert sessions[0]["event_count"] == 1


def test_equivalent_timestamps_are_not_conflicts(client):
    """Same instant in a different offset is an idempotent replay, not a conflict."""
    assert submit(client, "dev-1", "e1", "2026-09-20T12:00:00+02:00").status_code == 201
    resp = submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    assert resp.status_code == 200
    assert resp.json()["replay"] is True


def test_session_id_matches_documented_scheme(client):
    submit(client, "dev-x", "b", "2026-09-20T10:01:00Z")
    submit(client, "dev-x", "a", "2026-09-20T10:00:00Z")

    session = client.get("/devices/dev-x/sessions").json()[0]
    # Members sorted by (occurred_at, event_id).
    members = [
        ("2026-09-20T10:00:00+00:00", "a"),
        ("2026-09-20T10:01:00+00:00", "b"),
    ]
    assert session["session_id"] == expected_session_id("dev-x", members)


def test_gap_threshold_configurable(tmp_path):
    app = create_app(database_path=str(tmp_path / "tight.db"), gap_seconds=60)
    with TestClient(app) as c:
        submit(c, "dev-1", "e1", "2026-09-20T10:00:00Z")
        submit(c, "dev-1", "e2", "2026-09-20T10:01:00Z")  # exactly 60s -> same session
        submit(c, "dev-1", "e3", "2026-09-20T10:02:01Z")  # 61s gap -> new session
        sessions = c.get("/devices/dev-1/sessions").json()
        assert [s["event_count"] for s in sessions] == [2, 1]


def test_get_session_by_id_and_404(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    session = client.get("/devices/dev-1/sessions").json()[0]

    resp = client.get(f"/devices/dev-1/sessions/{session['session_id']}")
    assert resp.status_code == 200
    assert resp.json() == session

    assert client.get("/devices/dev-1/sessions/0" * 64).status_code == 404
    assert client.get("/devices/unknown/sessions").json() == []


def test_devices_are_isolated(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    submit(client, "dev-2", "e2", "2026-09-20T10:00:10Z")
    s1 = client.get("/devices/dev-1/sessions").json()
    s2 = client.get("/devices/dev-2/sessions").json()
    assert len(s1) == len(s2) == 1
    assert s1[0]["session_id"] != s2[0]["session_id"]

"""Collection integrity: source streams, sequence ledgers and checkpoints.

Covers stream registration (idempotent), out-of-order/backfill event
ingestion through the existing ingestion transaction, the slot/event-id
binding rules, the integrity status with compressed missing ranges,
gap-gated SHA-256 checkpoints with idempotent replay, independence from
event retention (ledger/checkpoints survive event deletion), and legacy
database migration.
"""

import hashlib
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.ingestion import canonical_json
from app.streams import CHECKPOINT_DIGEST_PREFIX, checkpoint_checksum


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=300)
    with TestClient(app) as c:
        yield c


def _event(source_seq, event_id, ts="2026-09-20T10:00:00Z", **extra):
    body = {
        "device_id": "dev-1",
        "source_seq": source_seq,
        "event_id": event_id,
        "occurred_at": ts,
    }
    body.update(extra)
    return body


def _register(client, device_id="dev-1", stream_id="main"):
    return client.put(f"/devices/{device_id}/streams/{stream_id}")


def _put_event(client, body, device_id="dev-1", stream_id="main"):
    return client.put(
        f"/devices/{device_id}/streams/{stream_id}/events", json=body
    )


# ------------------------------------------------------------- registration


def test_register_stream_and_idempotent_replay(client):
    resp = _register(client)
    assert resp.status_code == 201
    first = resp.json()
    assert first["stream_id"] == "main"
    assert first["event_count"] == 0
    assert first["highest_seen"] == 0
    assert first["contiguous_through"] == 0
    assert first["missing_ranges"] == []
    created_at = first["created_at"]

    replay = _register(client)
    assert replay.status_code == 200
    assert replay.json()["created_at"] == created_at


def test_list_streams_isolated_per_device(client):
    _register(client, "dev-1", "a")
    _register(client, "dev-2", "b")
    a = client.get("/devices/dev-1/streams").json()
    b = client.get("/devices/dev-2/streams").json()
    assert [s["stream_id"] for s in a] == ["a"]
    assert [s["stream_id"] for s in b] == ["b"]


def test_status_unknown_stream_is_404(client):
    assert client.get("/devices/dev-1/streams/nope").status_code == 404
    assert client.get("/devices/dev-1/streams/nope/status").status_code == 404


# ------------------------------------------------------------- ingestion


def test_event_requires_registered_stream(client):
    resp = _put_event(client, _event(1, "e1"))
    assert resp.status_code == 404


def test_out_of_order_backfill_and_status(client):
    _register(client)
    # Arrive 2, then 5, then backfill 1,3,4.
    for seq, eid in ((2, "e2"), (5, "e5"), (1, "e1"), (3, "e3"), (4, "e4")):
        resp = _put_event(
            client, _event(seq, eid, f"2026-09-20T10:0{seq}:00Z")
        )
        assert resp.status_code == 201, resp.text

    status = client.get("/devices/dev-1/streams/main").json()
    assert status["highest_seen"] == 5
    assert status["contiguous_through"] == 5
    assert status["missing_ranges"] == []
    assert status["event_count"] == 5


def test_status_reports_compressed_missing_ranges(client):
    _register(client)
    # Slots: 1, 4, 5, 8 -> runs of gaps [2,3] and [6,7].
    for seq in (1, 4, 5, 8):
        assert (
            _put_event(
                client, _event(seq, f"e{seq}", f"2026-09-20T10:0{seq}:00Z")
            ).status_code
            == 201
        )
    status = client.get("/devices/dev-1/streams/main").json()
    assert status["highest_seen"] == 8
    assert status["contiguous_through"] == 1
    assert status["missing_ranges"] == [[2, 3], [6, 7]]
    assert status["event_count"] == 4


def test_contiguous_through_zero_when_slot_one_missing(client):
    _register(client)
    assert _put_event(client, _event(3, "e3")).status_code == 201
    status = client.get("/devices/dev-1/streams/main").json()
    assert status["highest_seen"] == 3
    assert status["contiguous_through"] == 0
    assert status["missing_ranges"] == [[1, 2]]


def test_identical_replay_is_idempotent_200(client):
    _register(client)
    body = _event(1, "e1")
    assert _put_event(client, body).status_code == 201
    replay = _put_event(client, body)
    assert replay.status_code == 200
    assert replay.json()["replay"] is True
    assert client.get("/devices/dev-1/streams/main").json()["event_count"] == 1


def test_source_seq_conflict_is_409(client):
    _register(client)
    assert _put_event(client, _event(1, "e1")).status_code == 201
    resp = _put_event(client, _event(1, "other"))
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "source_seq_conflict"
    # The ledger keeps the original binding.
    status = client.get("/devices/dev-1/streams/main").json()
    assert status["event_count"] == 1


def test_event_id_conflict_is_409(client):
    _register(client)
    assert _put_event(client, _event(1, "e1")).status_code == 201
    resp = _put_event(client, _event(2, "e1"))
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "event_id_conflict"
    status = client.get("/devices/dev-1/streams/main").json()
    assert status["event_count"] == 1
    assert status["highest_seen"] == 1


def test_replay_with_different_content_is_409(client):
    _register(client)
    assert _put_event(client, _event(1, "e1")).status_code == 201
    resp = _put_event(
        client, _event(1, "e1", "2026-09-20T10:00:01Z")
    )
    assert resp.status_code == 409
    assert client.get("/devices/dev-1/streams/main").json()["event_count"] == 1


def test_same_event_id_allowed_on_different_stream(client):
    _register(client, "dev-1", "a")
    _register(client, "dev-1", "b")
    body = _event(1, "shared")
    assert _put_event(client, body, stream_id="a").status_code == 201
    assert _put_event(client, body, stream_id="b").status_code == 201


def test_stream_ingest_can_bind_event_first_seen_via_post_events(client):
    _register(client)
    plain = {
        "device_id": "dev-1",
        "event_id": "legacy",
        "occurred_at": "2026-09-20T10:00:00Z",
    }
    assert client.post("/events", json=plain).status_code == 201
    # The event already exists globally; the stream binding is still new.
    resp = _put_event(client, {**plain, "source_seq": 1})
    assert resp.status_code == 201
    assert resp.json()["replay"] is False


def test_device_id_path_body_mismatch_is_422(client):
    _register(client)
    body = _event(1, "e1")
    body["device_id"] = "dev-2"
    assert _put_event(client, body).status_code == 422


def test_source_seq_must_start_from_one(client):
    _register(client)
    assert _put_event(client, _event(0, "e0")).status_code == 422
    assert _put_event(client, _event(-5, "en")).status_code == 422


def test_contract_failure_rolls_back_ledger(client):
    client.put(
        "/schemas/tele/1.0",
        json={
            "schema": {
                "type": "object",
                "properties": {"v": {"type": "integer"}},
                "required": ["v"],
            }
        },
    )
    _register(client)
    resp = _put_event(
        client,
        _event(
            1,
            "e1",
            event_type="tele",
            schema_version="1.0",
            payload={"v": "not-an-int"},
        ),
    )
    assert resp.status_code == 422
    status = client.get("/devices/dev-1/streams/main").json()
    assert status["event_count"] == 0
    assert status["highest_seen"] == 0


def test_state_machine_rejection_rolls_back_ledger(client):
    _register(client)
    client.put(
        "/devices/dev-1/state-machine/versions/1",
        json={
            "version": 1,
            "initial_state": "idle",
            "states": ["idle", "run"],
            "transitions": [
                {"from_state": "idle", "event_type": "go", "to_state": "run"}
            ],
        },
    )
    client.post("/devices/dev-1/state-machine/enable", json={"version": 1})
    # A no-payload event never matches a transition -> 409 and no binding.
    resp = _put_event(client, _event(1, "e1"))
    assert resp.status_code == 409
    assert client.get("/devices/dev-1/streams/main").json()["event_count"] == 0


# ------------------------------------------------------------- checkpoints


def test_checkpoint_requires_gap_free_prefix(client):
    _register(client)
    for seq in (1, 3):
        assert (
            _put_event(
                client, _event(seq, f"e{seq}", f"2026-09-20T10:0{seq}:00Z")
            ).status_code
            == 201
        )
    resp = client.put("/devices/dev-1/streams/main/checkpoints/3")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["reason"] == "checkpoint_gap"
    assert detail["missing_ranges"] == [[2, 2]]
    assert detail["contiguous_through"] == 1
    # Nothing was stored.
    assert client.get("/devices/dev-1/streams/main/checkpoints").json() == []


def test_checkpoint_beyond_high_water_is_409(client):
    _register(client)
    assert _put_event(client, _event(1, "e1")).status_code == 201
    resp = client.put("/devices/dev-1/streams/main/checkpoints/2")
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "checkpoint_gap"


def test_checkpoint_sha256_over_ordered_ledger(client):
    _register(client)
    # Backfill out of order; checksum must follow source_seq order.
    pairs = [(2, "e2"), (1, "e1"), (3, "e3")]
    for seq, eid in pairs:
        assert (
            _put_event(
                client, _event(seq, eid, f"2026-09-20T10:0{seq}:00Z")
            ).status_code
            == 201
        )
    resp = client.put("/devices/dev-1/streams/main/checkpoints/3")
    assert resp.status_code == 201
    body = resp.json()
    assert body["through_seq"] == 3
    assert body["event_count"] == 3
    expected = checkpoint_checksum(
        "dev-1", "main", [(1, "e1"), (2, "e2"), (3, "e3")]
    )
    assert body["checksum_sha256"] == expected


def test_checkpoint_algorithm_matches_documented_material():
    material = canonical_json(
        {
            "device_id": "dev-1",
            "stream_id": "main",
            "entries": [[1, "e1"], [2, "e2"]],
        }
    )
    expected = hashlib.sha256(
        b"device-event-stream-checkpoint:1\n" + material.encode("utf-8")
    ).hexdigest()
    assert (
        checkpoint_checksum("dev-1", "main", [(1, "e1"), (2, "e2")])
        == expected
    )
    assert CHECKPOINT_DIGEST_PREFIX == b"device-event-stream-checkpoint:1\n"


def test_checkpoint_repeated_creation_replays_original(client):
    _register(client)
    assert _put_event(client, _event(1, "e1")).status_code == 201
    first = client.put("/devices/dev-1/streams/main/checkpoints/1")
    assert first.status_code == 201
    replay = client.put("/devices/dev-1/streams/main/checkpoints/1")
    assert replay.status_code == 200
    assert replay.json() == first.json()

    fetched = client.get("/devices/dev-1/streams/main/checkpoints/1")
    assert fetched.status_code == 200
    assert fetched.json() == first.json()


def test_checkpoints_ordered_and_unknown_404(client):
    _register(client)
    for seq in (1, 2):
        assert (
            _put_event(
                client, _event(seq, f"e{seq}", f"2026-09-20T10:0{seq}:00Z")
            ).status_code
            == 201
        )
    client.put("/devices/dev-1/streams/main/checkpoints/1")
    client.put("/devices/dev-1/streams/main/checkpoints/2")
    saved = client.get("/devices/dev-1/streams/main/checkpoints").json()
    assert [c["through_seq"] for c in saved] == [1, 2]
    assert (
        client.get("/devices/dev-1/streams/main/checkpoints/9").status_code
        == 404
    )
    assert client.put("/devices/dev-1/streams/nope/checkpoints/1").status_code == 404
    assert (
        client.get("/devices/dev-1/streams/nope/checkpoints").status_code == 404
    )


def test_checkpoint_through_seq_must_be_positive(client):
    _register(client)
    # Path validation rejects non-positive sequences before any logic.
    assert (
        client.put("/devices/dev-1/streams/main/checkpoints/0").status_code
        == 422
    )


# ------------------------------------------------- retention independence


def _cleanup_device(client, device_id, before="2027-01-01T00:00:00Z"):
    plan = client.post(
        "/retention/cleanup-plans",
        json={
            "device_id": device_id,
            "before": before,
            "protected_windows": [],
        },
    ).json()
    return client.post(
        f"/retention/cleanup-plans/{plan['plan_id']}/confirm"
    )


def test_ledger_and_checkpoints_survive_event_cleanup(client):
    _register(client)
    for seq in (1, 2, 3):
        assert (
            _put_event(
                client, _event(seq, f"e{seq}", f"2026-09-20T10:0{seq}:00Z")
            ).status_code
            == 201
        )
    saved = client.put("/devices/dev-1/streams/main/checkpoints/3")
    assert saved.status_code == 201
    checksum = saved.json()["checksum_sha256"]

    assert _cleanup_device(client, "dev-1").status_code == 200

    # The events are gone; the ledger and checkpoint remain.
    status = client.get("/devices/dev-1/streams/main").json()
    assert status["event_count"] == 3
    assert status["highest_seen"] == 3
    assert status["contiguous_through"] == 3
    replayed = client.put("/devices/dev-1/streams/main/checkpoints/3")
    assert replayed.status_code == 200
    assert replayed.json()["checksum_sha256"] == checksum


def test_new_checkpoint_computable_from_ledger_after_cleanup(client):
    _register(client)
    for seq in (1, 2, 3):
        assert (
            _put_event(
                client, _event(seq, f"e{seq}", f"2026-09-20T10:0{seq}:00Z")
            ).status_code
            == 201
        )
    assert _cleanup_device(client, "dev-1").status_code == 200
    resp = client.put("/devices/dev-1/streams/main/checkpoints/2")
    assert resp.status_code == 201
    assert resp.json()["checksum_sha256"] == checkpoint_checksum(
        "dev-1", "main", [(1, "e1"), (2, "e2")]
    )


def test_cleanup_does_not_loosen_slot_binding(client):
    _register(client)
    assert _put_event(client, _event(1, "e1")).status_code == 201
    assert _cleanup_device(client, "dev-1").status_code == 200
    # Even with the event deleted, slot 1 is still bound to e1: another
    # event_id cannot take it, and replaying e1 is still idempotent.
    conflict = _put_event(client, _event(1, "e2"))
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["reason"] == "source_seq_conflict"
    replay = _put_event(client, _event(1, "e1"))
    assert replay.status_code == 200
    assert replay.json()["replay"] is True


# ------------------------------------------------------------- persistence


def test_ledger_and_checkpoints_persist_across_restart(tmp_path):
    db_path = str(tmp_path / "restart.db")
    with TestClient(create_app(database_path=db_path)) as c:
        _register(c)
        assert _put_event(c, _event(2, "e2")).status_code == 201
        assert _put_event(c, _event(1, "e1")).status_code == 201
        assert c.put("/devices/dev-1/streams/main/checkpoints/2").status_code == 201

    with TestClient(create_app(database_path=db_path)) as c:
        status = c.get("/devices/dev-1/streams/main").json()
        assert status["highest_seen"] == 2
        assert status["contiguous_through"] == 2
        assert (
            c.put("/devices/dev-1/streams/main/checkpoints/2").status_code == 200
        )


def test_legacy_database_gains_collection_tables(tmp_path):
    """A pre-release database (original events table) migrates on startup."""
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );
            CREATE INDEX idx_events_device_time
                ON events (device_id, occurred_at, event_id);
            CREATE INDEX idx_events_time_id ON events (occurred_at, event_id);
            """
        )
        conn.execute(
            "INSERT INTO events VALUES ('old', 'dev-1', '2026-09-20T09:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    with TestClient(create_app(database_path=db_path)) as c:
        assert _register(c).status_code == 201
        assert (
            _put_event(
                c, _event(1, "new", "2026-09-20T10:00:00Z")
            ).status_code
            == 201
        )
        status = c.get("/devices/dev-1/streams/main").json()
        assert status["contiguous_through"] == 1
        # The legacy event and the new event coexist; POST /events is intact.
        found = c.post("/events/search", json={"device_id": "dev-1"}).json()
        assert [e["event_id"] for e in found["events"]] == ["old", "new"]

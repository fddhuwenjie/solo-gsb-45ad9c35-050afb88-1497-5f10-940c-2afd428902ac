"""Session evolution snapshot tests.

Covers:
* snapshot creation freezes the arrival high-water and the ordered
  sessions inside one write transaction,
* idempotent replay of a repeated id and 409 on cross-device id reuse,
* late events and retention cleanup never rewriting a stored snapshot,
* full member copies (payloads included) surviving event deletion,
* the old/new bipartite-graph diff: unchanged, changed, merged, split,
* created, removed and many-to-many reorganized components, stable
  ordering and added/removed member lists,
* refusing to compare snapshots built with different gap configurations,
* legacy databases gaining the snapshot tables automatically.
"""

import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.snapshots import (
    SnapshotMember,
    SnapshotSession,
    SessionSnapshot,
    diff_snapshots,
)

GAP = 300  # seconds


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=GAP)
    with TestClient(app) as c:
        yield c


def submit(client, device_id, event_id, occurred_at, **extra):
    return client.post(
        "/events",
        json={
            "device_id": device_id,
            "event_id": event_id,
            "occurred_at": occurred_at,
            **extra,
        },
    )


def _changes(client, device_id, old_id, new_id):
    resp = client.post(
        f"/devices/{device_id}/session-snapshots/diff",
        json={"old_snapshot_id": old_id, "new_snapshot_id": new_id},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["changes"]


def _by_change(changes):
    return {entry["change"]: entry for entry in changes}


# ------------------------------------------------------------- creation/freeze


def test_snapshot_freezes_sessions_and_high_water(client):
    for event_id, ts in [
        ("e1", "2026-09-20T10:00:00Z"),
        ("e2", "2026-09-20T10:02:00Z"),
        ("e3", "2026-09-20T10:20:00Z"),  # 18 min gap -> second session
        ("e4", "2026-09-20T10:21:00Z"),
    ]:
        assert submit(client, "dev-1", event_id, ts).status_code == 201

    resp = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "snap-1"}
    )
    assert resp.status_code == 201, resp.text
    snapshot = resp.json()
    assert snapshot["snapshot_id"] == "snap-1"
    assert snapshot["device_id"] == "dev-1"
    assert snapshot["gap_seconds"] == GAP
    assert snapshot["event_high_water"] == 4
    assert snapshot["session_count"] == 2
    assert [s["event_count"] for s in snapshot["sessions"]] == [2, 2]
    assert [m["event_id"] for m in snapshot["sessions"][0]["members"]] == [
        "e1",
        "e2",
    ]
    assert [m["event_id"] for m in snapshot["sessions"][1]["members"]] == [
        "e3",
        "e4",
    ]
    # Sessions in the snapshot are stored in snapshot-time order, and the
    # deterministic session ids match the live grouping.
    live = client.get("/devices/dev-1/sessions").json()
    assert [s["session_id"] for s in snapshot["sessions"]] == [
        s["session_id"] for s in live
    ]


def test_snapshot_freezes_full_member_payloads(client):
    schema = {
        "type": "object",
        "required": ["v"],
        "properties": {"v": {"type": "number"}},
    }
    put = client.put(
        "/schemas/t/1",
        json={"schema": schema},
    )
    assert put.status_code in (200, 201), put.text
    submit(
        client,
        "dev-1",
        "e1",
        "2026-09-20T10:00:00Z",
        event_type="t",
        schema_version="1",
        payload={"v": 7},
    )
    snap = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "s"}
    ).json()
    member = snap["sessions"][0]["members"][0]
    assert member["event_type"] == "t"
    assert member["schema_version"] == "1"
    assert member["payload"] == {"v": 7}


def test_empty_device_snapshot(client):
    resp = client.post(
        "/devices/dev-x/session-snapshots", json={"snapshot_id": "empty"}
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["session_count"] == 0
    assert body["sessions"] == []
    assert body["event_high_water"] == 0


def test_repeated_snapshot_id_replays_original_unchanged(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    submit(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    first = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "snap-1"}
    )
    assert first.status_code == 201
    original = first.json()

    # A late event bridges nothing here but adds a second session; the
    # replay must still return the original frozen document.
    submit(client, "dev-1", "e3", "2026-09-20T11:00:00Z")

    second = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "snap-1"}
    )
    assert second.status_code == 200
    replayed = second.json()
    assert replayed == original
    assert replayed["event_high_water"] == 2
    assert replayed["session_count"] == 1


def test_snapshot_id_reuse_on_another_device_conflicts(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    first = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "shared"}
    )
    assert first.status_code == 201

    conflict = client.post(
        "/devices/dev-2/session-snapshots", json={"snapshot_id": "shared"}
    )
    assert conflict.status_code == 409
    detail = conflict.json()["detail"]
    assert detail["reason"] == "snapshot_id_device_conflict"
    assert detail["bound_device_id"] == "dev-1"
    assert detail["conflicting_device_id"] == "dev-2"

    # Nothing was stored for dev-2; the original is still the original.
    assert (
        client.get("/devices/dev-2/session-snapshots/shared").status_code == 404
    )
    replay = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "shared"}
    )
    assert replay.status_code == 200
    assert replay.json()["created_at"] == first.json()["created_at"]


def test_invalid_snapshot_id_shape(client):
    resp = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "a/b"}
    )
    assert resp.status_code == 422
    resp = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": ""}
    )
    assert resp.status_code == 422


# ------------------------------------------------------------- listing/fetch


def test_list_and_get_snapshots(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "b"})
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "a"})

    listing = client.get("/devices/dev-1/session-snapshots")
    assert listing.status_code == 200
    rows = listing.json()
    # Other devices never appear.
    assert [row["snapshot_id"] for row in rows] == ["b", "a"]
    summary = rows[0]
    assert summary["event_high_water"] == 1
    assert summary["gap_seconds"] == GAP
    assert summary["sessions"][0]["event_count"] == 1
    assert summary["sessions"][0]["session_id"]

    full = client.get("/devices/dev-1/session-snapshots/b")
    assert full.status_code == 200
    assert full.json()["sessions"][0]["members"][0]["event_id"] == "e1"

    assert client.get("/devices/dev-1/session-snapshots/nope").status_code == 404
    # An id owned by another device is 404 here, never readable cross-device.
    assert client.get("/devices/dev-2/session-snapshots/b").status_code == 404


# ------------------------------------------------- immutability under change


def test_late_event_does_not_rewrite_old_snapshot(client):
    for event_id, ts in [
        ("e1", "2026-09-20T10:00:00Z"),
        ("e2", "2026-09-20T10:08:00Z"),  # 480s apart -> two sessions
    ]:
        submit(client, "dev-1", event_id, ts)
    first = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "before"}
    ).json()
    assert first["session_count"] == 2

    # Late bridging event merges the live sessions into one.
    submit(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    assert len(client.get("/devices/dev-1/sessions").json()) == 1

    stored = client.get("/devices/dev-1/session-snapshots/before").json()
    assert stored["session_count"] == 2
    assert stored["event_high_water"] == first["event_high_water"]
    assert [s["session_id"] for s in stored["sessions"]] == [
        s["session_id"] for s in first["sessions"]
    ]


def test_retention_cleanup_does_not_rewrite_old_snapshot(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    submit(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    before = client.post(
        "/devices/dev-1/session-snapshots", json={"snapshot_id": "before"}
    ).json()

    plan = client.post(
        "/retention/cleanup-plans",
        json={
            "device_id": "dev-1",
            "before": "2026-09-21T00:00:00Z",
            "protected_windows": [],
        },
    )
    assert plan.status_code == 201, plan.text
    plan_id = plan.json()["plan_id"]
    confirmed = client.post(f"/retention/cleanup-plans/{plan_id}/confirm")
    assert confirmed.status_code == 200, confirmed.text

    # Live view is now empty; the frozen copy still returns both members.
    assert client.get("/devices/dev-1/sessions").json() == []
    stored = client.get("/devices/dev-1/session-snapshots/before")
    assert stored.status_code == 200
    body = stored.json()
    assert [m["event_id"] for m in body["sessions"][0]["members"]] == [
        "e1",
        "e2",
    ]
    assert body["created_at"] == before["created_at"]


# --------------------------------------------------------------------- diffs


def test_diff_unchanged_changed_and_created(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    submit(client, "dev-1", "e2", "2026-09-20T10:01:00Z")
    submit(client, "dev-1", "e3", "2026-09-20T10:20:00Z")
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s1"})

    # One later member joins the second session; a later event opens a third.
    submit(client, "dev-1", "e4", "2026-09-20T10:21:00Z")
    submit(client, "dev-1", "e5", "2026-09-20T11:00:00Z")
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s2"})

    changes = _changes(client, "dev-1", "s1", "s2")
    by = _by_change(changes)
    assert set(by) == {"unchanged", "changed", "created"}
    assert by["changed"]["added_member_event_ids"] == ["e4"]
    assert by["changed"]["removed_member_event_ids"] == []
    assert by["created"]["old_session_ids"] == []
    assert by["created"]["added_member_event_ids"] == ["e5"]
    assert by["unchanged"]["added_member_event_ids"] == []
    assert by["unchanged"]["removed_member_event_ids"] == []

    # Every entry exposes both sides' session ids (the empty side is []).
    for entry in changes:
        assert entry["old_session_ids"] or entry["new_session_ids"]


def test_diff_merged_via_late_bridge(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    submit(client, "dev-1", "e2", "2026-09-20T10:08:00Z")  # 480s gap
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s1"})

    submit(client, "dev-1", "e3", "2026-09-20T10:04:00Z")  # bridges both
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s2"})

    by = _by_change(_changes(client, "dev-1", "s1", "s2"))
    assert list(by) == ["merged"]
    entry = by["merged"]
    assert len(entry["old_session_ids"]) == 2
    assert len(entry["new_session_ids"]) == 1
    assert entry["added_member_event_ids"] == ["e3"]
    assert entry["removed_member_event_ids"] == []


def test_diff_split_and_removed_after_event_deletion(tmp_path, client):
    db_path = str(tmp_path / "test.db")
    # Five events chained by 240s gaps -> a single session.
    for i in range(1, 6):
        submit(
            client,
            "dev-1",
            f"e{i}",
            f"2026-09-20T10:{(i - 1) * 4:02d}:00Z",
        )
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s1"})

    # Out-of-band deletion (what a retention confirm performs) of the
    # middle event e3 (10:08) breaks the chain into [e1,e2] / [e4,e5]:
    # adjacent gaps were 240s, the e2(10:04) -> e4(10:12) gap is 480s.
    # e3 itself is gone from the new side.
    raw = sqlite3.connect(db_path)
    try:
        raw.execute("DELETE FROM events WHERE event_id = 'e3'")
        raw.commit()
    finally:
        raw.close()

    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s2"})
    by = _by_change(_changes(client, "dev-1", "s1", "s2"))
    assert list(by) == ["split"]
    entry = by["split"]
    assert len(entry["old_session_ids"]) == 1
    assert len(entry["new_session_ids"]) == 2
    assert entry["added_member_event_ids"] == []
    assert entry["removed_member_event_ids"] == ["e3"]

    # Removing every event makes the old single session a removal.
    raw = sqlite3.connect(db_path)
    try:
        raw.execute("DELETE FROM events")
        raw.commit()
    finally:
        raw.close()
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s3"})
    by = _by_change(_changes(client, "dev-1", "s1", "s3"))
    assert list(by) == ["removed"]
    assert by["removed"]["removed_member_event_ids"] == [
        "e1",
        "e2",
        "e3",
        "e4",
        "e5",
    ]


def test_diff_is_stably_sorted_and_uses_frozen_copies(client):
    # Three independent sessions; two disappear, one stays, one appears.
    submit(client, "dev-1", "e1", "2026-09-20T09:00:00Z")
    submit(client, "dev-1", "e2", "2026-09-20T10:00:00Z")
    submit(client, "dev-1", "e3", "2026-09-20T11:00:00Z")
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s1"})

    s2_before_late = _changes(client, "dev-1", "s1", "s1")
    assert [entry["change"] for entry in s2_before_late] == [
        "unchanged",
        "unchanged",
        "unchanged",
    ]

    submit(client, "dev-1", "e4", "2026-09-20T12:00:00Z")
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s2"})
    # Repeated diffs of the same frozen pair are byte-identical regardless
    # of what live sessions look like meanwhile.
    first = _changes(client, "dev-1", "s1", "s2")
    second = _changes(client, "dev-1", "s1", "s2")
    assert first == second
    # Components come in started_at order; the created session is last.
    assert [entry["change"] for entry in first] == [
        "unchanged",
        "unchanged",
        "unchanged",
        "created",
    ]
    # Self-comparison is always all-unchanged.
    assert all(
        entry["change"] == "unchanged"
        for entry in _changes(client, "dev-1", "s2", "s2")
    )


def test_diff_unknown_snapshot_and_cross_device_ids_404(client):
    submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    client.post("/devices/dev-1/session-snapshots", json={"snapshot_id": "s1"})
    client.post("/devices/dev-2/session-snapshots", json={"snapshot_id": "s2"})

    resp = client.post(
        "/devices/dev-1/session-snapshots/diff",
        json={"old_snapshot_id": "nope", "new_snapshot_id": "s1"},
    )
    assert resp.status_code == 404
    resp = client.post(
        "/devices/dev-1/session-snapshots/diff",
        json={"old_snapshot_id": "s1", "new_snapshot_id": "s2"},
    )
    assert resp.status_code == 404


def test_diff_rejects_different_gap_configuration(tmp_path):
    db_path = str(tmp_path / "test.db")
    app = create_app(database_path=db_path, gap_seconds=300)
    with TestClient(app) as client:
        submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
        first = client.post(
            "/devices/dev-1/session-snapshots", json={"snapshot_id": "s300"}
        )
        assert first.status_code == 201

    app2 = create_app(database_path=db_path, gap_seconds=60)
    with TestClient(app2) as client2:
        second = client2.post(
            "/devices/dev-1/session-snapshots", json={"snapshot_id": "s60"}
        )
        assert second.status_code == 201
        assert second.json()["gap_seconds"] == 60

        resp = client2.post(
            "/devices/dev-1/session-snapshots/diff",
            json={"old_snapshot_id": "s300", "new_snapshot_id": "s60"},
        )
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["reason"] == "gap_configuration_mismatch"
        assert detail["old_gap_seconds"] == 300
        assert detail["new_gap_seconds"] == 60


# ----------------------------------------------------- pure helper: reorganized


def _frozen_session(session_id_, member_ids):
    members = tuple(
        SnapshotMember(
            event_id=event_id,
            occurred_at=datetime(2026, 9, 20, 10, 0, i, tzinfo=timezone.utc),
        )
        for i, event_id in enumerate(member_ids)
    )
    return SnapshotSession(
        session_id=session_id_,
        started_at=members[0].occurred_at,
        ended_at=members[-1].occurred_at,
        members=members,
    )


def _frozen_snapshot(snapshot_id, sessions):
    return SessionSnapshot(
        snapshot_id=snapshot_id,
        device_id="dev-1",
        event_high_water=0,
        gap_seconds=300.0,
        created_at=datetime(2026, 9, 20, tzinfo=timezone.utc),
        sessions=tuple(sessions),
    )


def test_diff_many_to_many_component_is_reorganized():
    old = _frozen_snapshot(
        "old",
        [
            _frozen_session("P", ["p1", "p2"]),
            _frozen_session("Q", ["q1", "q2"]),
        ],
    )
    new = _frozen_snapshot(
        "new",
        [
            _frozen_session("U", ["p1", "q1"]),
            _frozen_session("V", ["p2", "q2"]),
        ],
    )
    changes = diff_snapshots(old, new)
    assert len(changes) == 1
    assert changes[0]["change"] == "reorganized"
    assert sorted(changes[0]["old_session_ids"]) == ["P", "Q"]
    assert sorted(changes[0]["new_session_ids"]) == ["U", "V"]


# ------------------------------------------------------------- old DB upgrade


LEGACY_SCHEMA = """
CREATE TABLE events (
    event_id    TEXT PRIMARY KEY,
    device_id   TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX idx_events_device_time
    ON events (device_id, occurred_at, event_id);
CREATE INDEX idx_events_time_id
    ON events (occurred_at, event_id);
"""


def test_legacy_database_gains_snapshot_tables(tmp_path):
    db_path = tmp_path / "legacy.db"
    raw = sqlite3.connect(str(db_path))
    try:
        raw.executescript(LEGACY_SCHEMA)
        raw.execute(
            "INSERT INTO events (event_id, device_id, occurred_at) "
            "VALUES ('e1', 'dev-1', '2026-09-20T10:00:00+00:00')"
        )
        raw.commit()
    finally:
        raw.close()

    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as client:
        resp = client.post(
            "/devices/dev-1/session-snapshots", json={"snapshot_id": "s1"}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["session_count"] == 1
        assert body["sessions"][0]["members"][0]["event_id"] == "e1"

        listing = client.get("/devices/dev-1/session-snapshots")
        assert [row["snapshot_id"] for row in listing.json()] == ["s1"]

    raw = sqlite3.connect(str(db_path))
    try:
        names = {
            row[0]
            for row in raw.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "session_snapshots" in names
        assert "session_snapshot_sessions" in names
    finally:
        raw.close()

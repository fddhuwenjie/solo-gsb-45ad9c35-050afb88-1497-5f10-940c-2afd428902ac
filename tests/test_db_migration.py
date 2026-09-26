"""Migration tests: legacy SQLite files gain the persistent ``seq`` column.

Covers:
* databases created by the initial release (events without the later
  payload columns),
* databases from the payload-contract release (those columns present),
* seq values preserving insertion order and the AUTOINCREMENT counter
  surviving an app restart,
* idempotency of the migration,
* search snapshots behaving correctly on migrated data.
"""

import sqlite3

from fastapi.testclient import TestClient

from app.main import create_app

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

LEGACY_SCHEMA_WITH_COLUMNS = """
CREATE TABLE events (
    event_id    TEXT PRIMARY KEY,
    device_id   TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    event_type    TEXT,
    schema_version TEXT,
    payload       TEXT
);
CREATE INDEX idx_events_device_time
    ON events (device_id, occurred_at, event_id);
CREATE INDEX idx_events_time_id
    ON events (occurred_at, event_id);
"""


def _create_legacy_db(path, schema, rows):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(schema)
        for row in rows:
            if len(row) == 3:
                conn.execute(
                    "INSERT INTO events (event_id, device_id, occurred_at) "
                    "VALUES (?, ?, ?)",
                    row,
                )
            else:
                conn.execute(
                    "INSERT INTO events (event_id, device_id, occurred_at, "
                    "event_type, schema_version, payload) VALUES (?, ?, ?, ?, ?, ?)",
                    row,
                )
        conn.commit()
    finally:
        conn.close()


def _raw_columns(path):
    conn = sqlite3.connect(path)
    try:
        return [row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()]
    finally:
        conn.close()


def test_initial_release_db_migrates_and_preserves_rows(tmp_path):
    db_path = tmp_path / "legacy.db"
    rows = [
        ("e1", "dev-1", "2026-09-20T10:00:00+00:00"),
        ("e2", "dev-1", "2026-09-20T10:01:00+00:00"),
        ("e0", "dev-2", "2026-09-09T08:00:00+00:00"),
    ]
    _create_legacy_db(str(db_path), LEGACY_SCHEMA, rows)
    assert "seq" not in _raw_columns(str(db_path))

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        # Historical rows are readable and correctly ordered.
        resp = client.post("/events/search", json={"limit": 10})
        assert resp.status_code == 200, resp.text
        assert [e["event_id"] for e in resp.json()["events"]] == [
            "e0",
            "e1",
            "e2",
        ]

        # New rows keep inserting after the migrated high-water.
        post = client.post(
            "/events",
            json={
                "device_id": "dev-1",
                "event_id": "e3",
                "occurred_at": "2026-09-20T10:02:00Z",
            },
        )
        assert post.status_code in (200, 201), post.text

    # seq preserved insertion order; the counter persisted beyond the max row.
    conn = sqlite3.connect(str(db_path))
    try:
        info = conn.execute("PRAGMA table_info(events)").fetchall()
        assert info[0][1] == "seq"
        # The column backing the monotonic sequence is the rowid alias with
        # AUTOINCREMENT semantics.
        assert info[0][2].upper() == "INTEGER" and info[0][5] == 1
        pairs = conn.execute(
            "SELECT seq, event_id FROM events ORDER BY seq"
        ).fetchall()
        assert pairs == [(1, "e1"), (2, "e2"), (3, "e0"), (4, "e3")]
        counter = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'events'"
        ).fetchone()
        assert counter == (4,)
        indexes = sorted(
            row[1]
            for row in conn.execute("PRAGMA index_list(events)").fetchall()
        )
        assert "idx_events_device_time" in indexes
        assert "idx_events_time_id" in indexes
        # event_id still enforces uniqueness.
        assert any(
            row[2] == 1 and "sqlite_autoindex_events" in row[1]
            for row in conn.execute("PRAGMA index_list(events)").fetchall()
        )
    finally:
        conn.close()


def test_migration_idempotent_across_restarts(tmp_path):
    db_path = tmp_path / "legacy.db"
    _create_legacy_db(
        str(db_path),
        LEGACY_SCHEMA,
        [("e1", "dev-1", "2026-09-20T10:00:00+00:00")],
    )

    for _ in range(2):
        app = create_app(database_path=str(db_path), gap_seconds=300)
        with TestClient(app):
            pass

    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'events'"
        ).fetchone() == (1,)
    finally:
        conn.close()


def test_payload_column_release_db_migrates_and_keeps_payloads(tmp_path):
    db_path = tmp_path / "legacy2.db"
    _create_legacy_db(
        str(db_path),
        LEGACY_SCHEMA_WITH_COLUMNS,
        [
            (
                "p1",
                "dev-1",
                "2026-09-20T10:00:00+00:00",
                "telemetry",
                "1",
                '{"v": 7}',
            ),
            ("plain", "dev-1", "2026-09-20T10:01:00+00:00", None, None, None),
        ],
    )

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        resp = client.post(
            "/events/search",
            json={
                "conditions": [
                    {"pointer": "/v", "operator": "eq", "value": 7}
                ]
            },
        )
        assert resp.status_code == 200, resp.text
        assert [e["event_id"] for e in resp.json()["events"]] == ["p1"]

    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute(
            "SELECT payload FROM events WHERE event_id = 'p1'"
        ).fetchone()[0] == '{"v": 7}'
    finally:
        conn.close()


def test_empty_legacy_table_migrates_counter_from_one(tmp_path):
    db_path = tmp_path / "empty.db"
    _create_legacy_db(str(db_path), LEGACY_SCHEMA, [])

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        post = client.post(
            "/events",
            json={
                "device_id": "dev-1",
                "event_id": "first",
                "occurred_at": "2026-09-20T10:00:00Z",
            },
        )
        assert post.status_code == 201, post.text

    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("SELECT seq, event_id FROM events").fetchall() == [
            (1, "first")
        ]
    finally:
        conn.close()


def test_snapshot_on_migrated_data_survives_delete_max_then_insert(tmp_path):
    db_path = tmp_path / "legacy3.db"
    rows = [
        (f"e{i}", "dev-1", f"2026-09-20T10:00:0{i}+00:00") for i in range(4)
    ]
    _create_legacy_db(str(db_path), LEGACY_SCHEMA, rows)

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        first = client.post("/events/search", json={"limit": 2})
        assert [e["event_id"] for e in first.json()["events"]] == ["e0", "e1"]
        cursor = first.json()["next_cursor"]
        assert cursor is not None and cursor.startswith("evc2:")

        # Delete the row holding the maximum sequence (e3 -> seq 4) out of
        # band, then insert an event sorting after the cursor anchor.
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("DELETE FROM events WHERE event_id = 'e3'")
            conn.commit()
        finally:
            conn.close()

        post = client.post(
            "/events",
            json={
                "device_id": "dev-1",
                "event_id": "zz",
                "occurred_at": "2026-09-20T12:00:00Z",
            },
        )
        assert post.status_code == 201, post.text

        second = client.post(
            "/events/search", json={"limit": 10, "cursor": cursor}
        )
        assert second.status_code == 200, second.text
        assert [e["event_id"] for e in second.json()["events"]] == ["e2"]
        assert second.json()["next_cursor"] is None

        conn = sqlite3.connect(str(db_path))
        try:
            assert conn.execute(
                "SELECT seq FROM events WHERE event_id = 'zz'"
            ).fetchone() == (5,)
        finally:
            conn.close()

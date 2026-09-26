"""Migration tests for the alert-rules/alarms tables.

An existing database created before the alerting release gains the new
tables idempotently on startup; pre-existing events and the new feature
both keep working afterwards, and restarting never duplicates tables or
loses alarm/progress rows.
"""

import os
import sqlite3

from fastapi.testclient import TestClient

from app.main import create_app


def _old_db(path):
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            CREATE TABLE events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                device_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                event_type TEXT,
                schema_version TEXT,
                payload TEXT
            );
            CREATE INDEX idx_events_device_time
                ON events (device_id, occurred_at, event_id);
            CREATE INDEX idx_events_time_id
                ON events (occurred_at, event_id);
            CREATE TABLE schemas (
                event_type TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                body TEXT NOT NULL,
                supersedes TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (event_type, schema_version)
            );
            CREATE TABLE cleanup_plans (
                plan_id TEXT PRIMARY KEY, device_id TEXT NOT NULL,
                before_at TEXT NOT NULL, protected_windows TEXT NOT NULL,
                plan_digest TEXT NOT NULL, status TEXT NOT NULL,
                candidates TEXT NOT NULL, event_count INTEGER NOT NULL,
                created_at TEXT NOT NULL, confirmed_at TEXT
            );
            CREATE TABLE device_commands (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL, command_id TEXT NOT NULL,
                payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_token TEXT, leased_at TEXT, expires_at TEXT,
                not_before TEXT, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, acked_at TEXT,
                UNIQUE (device_id, command_id)
            );
            INSERT INTO events VALUES
                (1,'e1','dev-1','2026-09-20T10:00:00+00:00','temp','1.0','{"v":50}'),
                (2,'e2','dev-1','2026-09-20T10:02:00+00:00','temp','1.0','{"v":50}');
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_alert_tables_added_to_legacy_database(tmp_path):
    db_path = tmp_path / "legacy.db"
    _old_db(db_path)

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        client.put(
            "/schemas/temp/1.0",
            json={"schema": {"type": "object",
                             "properties": {"v": {"type": "number"}}}},
        )
        # Existing events survived the migration.
        resp = client.post("/events/search", json={"limit": 10})
        assert [e["event_id"] for e in resp.json()["events"]] == ["e1", "e2"]

        # A rule created after the data existed must not backfill: only
        # later arrivals can trigger it.
        client.put(
            "/devices/dev-1/alert-rules/hot/versions/1",
            json={"version": 1, "event_type": "temp",
                  "conditions": [{"pointer": "/v", "operator": "gte",
                                  "value": 40}],
                  "window_seconds": 600, "consecutive": 2,
                  "cooldown_seconds": 0},
        )
        assert client.get("/devices/dev-1/alarms").json() == []
        client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "e3",
                  "occurred_at": "2026-09-20T10:04:00Z",
                  "event_type": "temp", "schema_version": "1.0",
                  "payload": {"v": 50}},
        )
        assert client.get("/devices/dev-1/alarms").json() == []
        client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "e4",
                  "occurred_at": "2026-09-20T10:06:00Z",
                  "event_type": "temp", "schema_version": "1.0",
                  "payload": {"v": 50}},
        )
        fired = client.get("/devices/dev-1/alarms").json()
        assert [a["trigger_event_id"] for a in fired] == ["e4"]

    conn = sqlite3.connect(str(db_path))
    try:
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"alarm_rules", "alarms", "alarm_rule_progress"} <= names
    finally:
        conn.close()


def test_alert_migration_idempotent_across_restarts(tmp_path):
    db_path = tmp_path / "legacy.db"
    _old_db(db_path)

    for _ in range(3):
        app = create_app(database_path=str(db_path), gap_seconds=300)
        with TestClient(app):
            pass

    conn = sqlite3.connect(str(db_path))
    try:
        # Existing rows untouched.
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM alarm_rules").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM alarms").fetchone()[0] == 0
    finally:
        conn.close()


def test_alarm_state_survives_restart(tmp_path):
    db_path = tmp_path / "state.db"
    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        client.put(
            "/schemas/temp/1.0",
            json={"schema": {"type": "object",
                             "properties": {"v": {"type": "number"}}}},
        )
        client.put(
            "/devices/dev-1/alert-rules/hot/versions/1",
            json={"version": 1, "event_type": "temp",
                  "conditions": [{"pointer": "/v", "operator": "gte",
                                  "value": 40}],
                  "window_seconds": 600, "consecutive": 2,
                  "cooldown_seconds": 3600},
        )
        for eid, ts in [("e1", "2026-09-20T10:00:00Z"),
                        ("e2", "2026-09-20T10:02:00Z")]:
            client.post(
                "/events",
                json={"device_id": "dev-1", "event_id": eid,
                      "occurred_at": ts, "event_type": "temp",
                      "schema_version": "1.0", "payload": {"v": 50}},
            )
        before = client.get("/devices/dev-1/alarms").json()
        assert len(before) == 1

    # Restart the service on the same database file.
    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        after = client.get("/devices/dev-1/alarms").json()
        assert after == before
        # A late non-matching event after restart still invalidates.
        client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "late",
                  "occurred_at": "2026-09-20T10:01:00Z",
                  "event_type": "temp", "schema_version": "1.0",
                  "payload": {"v": 0}},
        )
        alarms = client.get("/devices/dev-1/alarms").json()
        assert alarms[0]["status"] == "invalidated"


def _pre_eras_rules_db(path):
    """A database shaped like the pre-eras release: rules lack era columns.

    Contains two versions of one rule and four events arranged so that the
    era frontiers are reconstructible purely from ``created_seq``:

    * seq 1..3 arrived while v1 was the latest (frontier 10:04/e3);
    * v2 was created with created_seq=3, so v1's closed era must end at the
      frontier (10:04, e3) and v2's open era starts there;
    * seq 4 arrived after v2 creation.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            CREATE TABLE events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                device_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                event_type TEXT, schema_version TEXT, payload TEXT
            );
            CREATE INDEX idx_events_device_time
                ON events (device_id, occurred_at, event_id);
            CREATE INDEX idx_events_time_id ON events (occurred_at, event_id);
            CREATE TABLE schemas (
                event_type TEXT NOT NULL, schema_version TEXT NOT NULL,
                body TEXT NOT NULL, supersedes TEXT, created_at TEXT NOT NULL,
                PRIMARY KEY (event_type, schema_version)
            );
            CREATE TABLE cleanup_plans (
                plan_id TEXT PRIMARY KEY, device_id TEXT NOT NULL,
                before_at TEXT NOT NULL, protected_windows TEXT NOT NULL,
                plan_digest TEXT NOT NULL, status TEXT NOT NULL,
                candidates TEXT NOT NULL, event_count INTEGER NOT NULL,
                created_at TEXT NOT NULL, confirmed_at TEXT
            );
            CREATE TABLE device_commands (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL, command_id TEXT NOT NULL,
                payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_token TEXT, leased_at TEXT, expires_at TEXT,
                not_before TEXT, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, acked_at TEXT,
                UNIQUE (device_id, command_id)
            );
            CREATE TABLE alarm_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL, rule_id TEXT NOT NULL,
                version INTEGER NOT NULL, event_type TEXT NOT NULL,
                conditions_json TEXT NOT NULL, window_seconds REAL NOT NULL,
                consecutive INTEGER NOT NULL, cooldown_seconds REAL NOT NULL,
                created_seq INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE (device_id, rule_id, version)
            );
            CREATE TABLE alarms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alarm_id TEXT NOT NULL UNIQUE, device_id TEXT NOT NULL,
                rule_id TEXT NOT NULL, rule_version_id INTEGER NOT NULL,
                rule_version INTEGER NOT NULL, trigger_key TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'active',
                fired_at TEXT NOT NULL, first_match_at TEXT NOT NULL,
                last_match_at TEXT NOT NULL, cooldown_until TEXT NOT NULL,
                trigger_event_id TEXT NOT NULL, member_event_ids TEXT NOT NULL,
                seq_start INTEGER NOT NULL, seq_end INTEGER NOT NULL,
                acked_at TEXT, acked_seq INTEGER, invalidated_at TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                UNIQUE (rule_version_id, trigger_key)
            );
            CREATE INDEX idx_alarms_device_fired ON alarms (device_id, fired_at);
            CREATE INDEX idx_alarms_rule_version ON alarms (rule_version_id);
            CREATE TABLE alarm_rule_progress (
                rule_version_id INTEGER PRIMARY KEY, window_seconds REAL NOT NULL,
                streak_event_ids TEXT NOT NULL, streak_times TEXT NOT NULL,
                gate_state TEXT, gate_fired_at TEXT,
                gate_cooldown_until TEXT, gate_acked_seq INTEGER
            );
            INSERT INTO events VALUES
                (1,'e1','dev-1','2026-09-20T10:00:00+00:00','temp','1.0','{"v":50}'),
                (2,'e2','dev-1','2026-09-20T10:02:00+00:00','temp','1.0','{"v":50}'),
                (3,'e3','dev-1','2026-09-20T10:04:00+00:00','temp','1.0','{"v":50}'),
                (4,'e4','dev-1','2026-09-20T11:00:00+00:00','temp','1.0','{"v":50}');
            INSERT INTO alarm_rules VALUES
                (1,'dev-1','hot',1,'temp','[]',600,3,0,0,'2026-09-20T00:00:00+00:00'),
                (2,'dev-1','hot',2,'temp','[]',600,1,0,3,'2026-09-20T01:00:00+00:00');
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_era_columns_migrate_and_late_event_replays_frozen_version(tmp_path):
    db_path = tmp_path / "eras.db"
    _pre_eras_rules_db(db_path)

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        client.put(
            "/schemas/temp/1.0",
            json={"schema": {"type": "object",
                             "properties": {"v": {"type": "number"}}}},
        )
        # The migration reconstructed the era frontiers from created_seq:
        # v1 closed at (10:04, e3), v2 open above it.
        rows = client.get("/devices/dev-1/alert-rules").json()
        by_version = {r["version"]: r for r in rows}
        assert by_version[1]["active"] is False
        assert by_version[2]["active"] is True

    conn = sqlite3.connect(str(db_path))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(alarm_rules)")}
        assert {
            "era_start_occurred_at", "era_start_event_id",
            "era_end_occurred_at", "era_end_event_id",
        } <= cols
        v1 = conn.execute(
            "SELECT era_end_occurred_at, era_end_event_id FROM alarm_rules "
            "WHERE version = 1"
        ).fetchone()
        v2 = conn.execute(
            "SELECT era_start_occurred_at, era_start_event_id, "
            "era_end_occurred_at FROM alarm_rules WHERE version = 2"
        ).fetchone()
        assert v1 == ("2026-09-20T10:04:00+00:00", "e3")
        assert v2[0] == "2026-09-20T10:04:00+00:00"
        assert v2[1] == "e3"
        assert v2[2] is None  # latest version: open-ended era
    finally:
        conn.close()

    # After migration a late arrival sorting into v1's closed era replays
    # the frozen version: e1,e2,late are 3 consecutive hits within the
    # 600s window (10:04's e3 keeps the streak but the alarm already fires
    # at "late" first... verify via the final deterministic alarm set).
    with TestClient(app) as client:
        client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "late",
                  "occurred_at": "2026-09-20T10:01:00Z",
                  "event_type": "temp", "schema_version": "1.0",
                  "payload": {"v": 50}},
        )
        fired = client.get("/devices/dev-1/alarms").json()
        by_version = {(a["rule_version"], a["trigger_event_id"])
                      for a in fired}
        # v1's streak closes on e2 with members [e1, late, e2]; e2..e4 then
        # cannot fire again because the threshold run was consumed.
        v1_alarms = [a for a in fired if a["rule_version"] == 1]
        assert len(v1_alarms) == 1
        assert v1_alarms[0]["trigger_event_id"] == "e2"
        assert v1_alarms[0]["member_event_ids"] == ["e1", "late", "e2"]
        # e4 arrived in v2's open era: consecutive=1 on the migrated v2.
        assert (2, "e4") in by_version


def test_cross_era_alarm_state_survives_restart(tmp_path):
    """Late-era replay results and progress persist across a restart."""
    db_path = tmp_path / "cross.db"
    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        client.put(
            "/schemas/temp/1.0",
            json={"schema": {"type": "object",
                             "properties": {"v": {"type": "number"}}}},
        )
        client.put(
            "/devices/dev-1/alert-rules/hot/versions/1",
            json={"version": 1, "event_type": "temp",
                  "conditions": [{"pointer": "/v", "operator": "gte",
                                  "value": 40}],
                  "window_seconds": 600, "consecutive": 3,
                  "cooldown_seconds": 0},
        )
        for eid, ts in [("e1", "2026-09-20T10:00:00Z"),
                        ("e2", "2026-09-20T10:02:00Z")]:
            client.post(
                "/events",
                json={"device_id": "dev-1", "event_id": eid,
                      "occurred_at": ts, "event_type": "temp",
                      "schema_version": "1.0", "payload": {"v": 50}},
            )
        client.put(
            "/devices/dev-1/alert-rules/hot/versions/2",
            json={"version": 2, "event_type": "temp",
                  "conditions": [{"pointer": "/v", "operator": "gte",
                                  "value": 99}],
                  "window_seconds": 60, "consecutive": 1,
                  "cooldown_seconds": 0},
        )
        client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "late",
                  "occurred_at": "2026-09-20T09:59:00Z",
                  "event_type": "temp", "schema_version": "1.0",
                  "payload": {"v": 50}},
        )
        before = client.get("/devices/dev-1/alarms").json()
        assert {(a["rule_version"], a["trigger_event_id"]) for a in before} \
            == {(1, "e2")}

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        after = client.get("/devices/dev-1/alarms").json()
        # The persisted firing (id, trigger, members, times) survives the
        # restart; only mutable bookkeeping may be refreshed.
        assert _alarm_identity(after) == _alarm_identity(before)
        # Replaying the late event after restart stays idempotent.
        client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "late",
                  "occurred_at": "2026-09-20T09:59:00Z",
                  "event_type": "temp", "schema_version": "1.0",
                  "payload": {"v": 50}},
        )
        replayed = client.get("/devices/dev-1/alarms").json()
        assert _alarm_identity(replayed) == _alarm_identity(before)
        assert len(replayed) == len(before)


def _alarm_identity(alarms):
    """Structural fields of an alarm list (order-insensitive)."""
    return sorted(
        (
            a["alarm_id"], a["rule_version"], a["trigger_event_id"],
            tuple(a["member_event_ids"]), a["fired_at"],
            a["first_match_at"], a["status"], a["acked_at"],
            a["invalidated_at"],
        )
        for a in alarms
    )

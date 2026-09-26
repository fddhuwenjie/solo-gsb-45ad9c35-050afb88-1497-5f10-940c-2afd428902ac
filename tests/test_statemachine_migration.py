"""Migration tests for the state-machine tables.

A database created before the state-machine release gains the new tables
(``state_machine_versions``, ``device_state_machines`` and
``state_transitions_log``) idempotently on startup; pre-existing events keep
working, the feature is usable afterwards, and restarting never duplicates
tables or loses the projected current state / log.
"""

import sqlite3

from fastapi.testclient import TestClient

from app.main import create_app


def _old_db(path):
    """Create a database with every table that existed before this feature."""
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
            CREATE TABLE alarm_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL, rule_id TEXT NOT NULL,
                version INTEGER NOT NULL, event_type TEXT NOT NULL,
                conditions_json TEXT NOT NULL, window_seconds REAL NOT NULL,
                consecutive INTEGER NOT NULL, cooldown_seconds REAL NOT NULL,
                created_seq INTEGER NOT NULL DEFAULT 0,
                era_start_occurred_at TEXT, era_start_event_id TEXT,
                era_end_occurred_at TEXT, era_end_event_id TEXT,
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
            CREATE TABLE alarm_rule_progress (
                rule_version_id INTEGER PRIMARY KEY, window_seconds REAL NOT NULL,
                streak_event_ids TEXT NOT NULL, streak_times TEXT NOT NULL,
                gate_state TEXT, gate_fired_at TEXT,
                gate_cooldown_until TEXT, gate_acked_seq INTEGER
            );
            INSERT INTO events VALUES
                (1,'old-1','dev-1','2026-09-20T10:00:00+00:00',
                 'command','1.0','{"action":"unlock"}');
            """
        )
        conn.commit()
    finally:
        conn.close()


def _register_and_enable(client):
    client.put(
        "/schemas/command/1.0",
        json={
            "schema": {
                "type": "object",
                "required": ["action"],
                "properties": {"action": {"type": "string"}},
            }
        },
    )
    client.put(
        "/devices/dev-1/state-machine/versions/1",
        json={
            "version": 1,
            "initial_state": "locked",
            "states": ["locked", "unlocked"],
            "transitions": [
                {"from_state": "locked", "event_type": "command",
                 "to_state": "unlocked",
                 "conditions": [{"pointer": "/action", "operator": "eq",
                                 "value": "unlock"}]},
                {"from_state": "unlocked", "event_type": "command",
                 "to_state": "locked",
                 "conditions": [{"pointer": "/action", "operator": "eq",
                                 "value": "lock"}]},
            ],
        },
    )
    client.post("/devices/dev-1/state-machine/enable", json={"version": 1})


def test_state_machine_tables_added_to_legacy_database(tmp_path):
    db_path = tmp_path / "legacy.db"
    _old_db(db_path)

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        # Historical events survived the migration.
        resp = client.post("/events/search", json={"device_id": "dev-1"})
        assert [e["event_id"] for e in resp.json()["events"]] == ["old-1"]

        _register_and_enable(client)

        # The pre-enable event is NOT projected (high-water pinned at seq 1).
        status = client.get("/devices/dev-1/state-machine").json()
        assert status["current_state"] == "locked"
        assert status["last_event_seq"] == 1
        assert client.get(
            "/devices/dev-1/state-machine/transitions"
        ).json()["transitions"] == []

        resp = client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "new-1",
                  "occurred_at": "2026-09-22T10:00:00Z",
                  "event_type": "command", "schema_version": "1.0",
                  "payload": {"action": "unlock"}},
        )
        assert resp.status_code == 201
        assert resp.json()["state_transition"]["to_state"] == "unlocked"

    conn = sqlite3.connect(str(db_path))
    try:
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {
            "state_machine_versions",
            "device_state_machines",
            "state_transitions_log",
        } <= names
        # Existing data untouched.
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2
    finally:
        conn.close()


def test_state_machine_migration_idempotent_across_restarts(tmp_path):
    db_path = tmp_path / "legacy.db"
    _old_db(db_path)

    for _ in range(3):
        app = create_app(database_path=str(db_path), gap_seconds=300)
        with TestClient(app):
            pass

    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM state_machine_versions"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM state_transitions_log"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_projected_state_and_log_recover_after_restart(tmp_path):
    """The persisted current state and log survive a restart unchanged."""
    db_path = tmp_path / "state.db"

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        _register_and_enable(client)
        for eid, action, ts in [
            ("e1", "unlock", "2026-09-22T10:00:00Z"),
            ("e2", "lock", "2026-09-22T10:01:00Z"),
        ]:
            client.post(
                "/events",
                json={"device_id": "dev-1", "event_id": eid,
                      "occurred_at": ts, "event_type": "command",
                      "schema_version": "1.0", "payload": {"action": action}},
            )
        before = client.get(
            "/devices/dev-1/state-machine/transitions?limit=500"
        ).json()
        assert [t["event_id"] for t in before["transitions"]] == ["e1", "e2"]

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        status = client.get("/devices/dev-1/state-machine").json()
        assert status["current_state"] == "locked"
        assert status["last_event_seq"] == 2
        after = client.get(
            "/devices/dev-1/state-machine/transitions?limit=500"
        ).json()
        assert after == before

        # Replaying events after restart neither reprojects nor appends.
        replay = client.post(
            "/events",
            json={"device_id": "dev-1", "event_id": "e1",
                  "occurred_at": "2026-09-22T10:00:00Z",
                  "event_type": "command", "schema_version": "1.0",
                  "payload": {"action": "unlock"}},
        )
        assert replay.status_code == 200
        assert replay.json()["replay"] is True
        log = client.get(
            "/devices/dev-1/state-machine/transitions?limit=500"
        ).json()
        assert [t["event_id"] for t in log["transitions"]] == ["e1", "e2"]

"""Regression tests for the POST /events ingestion refactor.

``EventIngestionService`` is exercised directly (no HTTP layer) to prove
it is independently testable and that the caller-owned transaction rolls
every step back together; the ``create_app``-level tests pin that two app
instances never share schema caches or session-gap configuration.
"""

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app import ingestion as ingestion_module
from app.contracts import ContractError
from app.ingestion import (
    EventIngestionService,
    IngestionConflict,
    SessionView,
)
from app.main import create_app
from app.sessions import Event, from_storage, group_sessions, session_id
from app.statemachines import (
    build_definition,
    enable_machine,
    get_machine,
    get_version,
    insert_version,
)

COMMAND_SCHEMA = {
    "type": "object",
    "required": ["action"],
    "properties": {"action": {"type": "string"}},
    "additionalProperties": False,
}

TURNSTILE_TRANSITIONS = [
    {
        "from_state": "locked",
        "event_type": "command",
        "to_state": "unlocked",
        "conditions": [{"pointer": "/action", "operator": "eq", "value": "unlock"}],
    },
    {
        "from_state": "unlocked",
        "event_type": "command",
        "to_state": "locked",
        "conditions": [{"pointer": "/action", "operator": "eq", "value": "lock"}],
    },
]

TURNSTILE_DEFINITION = {
    "version": 1,
    "initial_state": "locked",
    "states": ["locked", "unlocked"],
    "transitions": TURNSTILE_TRANSITIONS,
}


def make_session_builder(gap_seconds):
    """A session builder equivalent to the one create_app injects."""

    def build(conn, device_id):
        rows = conn.execute(
            "SELECT event_id, occurred_at, event_type, schema_version, payload "
            "FROM events WHERE device_id = ? ORDER BY occurred_at, event_id",
            (device_id,),
        ).fetchall()
        events = [
            Event(
                device_id=device_id,
                event_id=row["event_id"],
                occurred_at=from_storage(row["occurred_at"]),
                event_type=row["event_type"],
                schema_version=row["schema_version"],
                payload=(
                    json.loads(row["payload"]) if row["payload"] is not None else None
                ),
            )
            for row in rows
        ]
        return [
            SessionView(
                session_id=session_id(device_id, members),
                device_id=device_id,
                started_at=members[0].occurred_at,
                ended_at=members[-1].occurred_at,
                members=tuple(members),
            )
            for members in group_sessions(events, gap_seconds)
        ]

    return build


def static_resolver(document):
    """Contract resolver stub always returning the same document (or None)."""

    def resolve(conn, event_type, version):
        return document

    return resolve


def forbidden_resolver(conn, event_type, version):
    raise AssertionError("payload-less ingestion must not resolve contracts")


def run_ingest(conn, service, **kwargs):
    """Mirror the thin route: caller-owned BEGIN IMMEDIATE around the service."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        outcome = service.ingest(conn, **kwargs)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return outcome


def event_count(conn):
    return conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]


@pytest.fixture()
def conn(tmp_path):
    db_path = str(tmp_path / "ingest.db")
    db_module.init_db(db_path)
    connection = db_module.connect(db_path)
    try:
        yield connection
    finally:
        connection.close()


def enable_turnstile(conn, device_id="dev-1"):
    definition = build_definition("locked", ["locked", "unlocked"], TURNSTILE_TRANSITIONS)
    now = datetime.now(timezone.utc)
    conn.execute("BEGIN IMMEDIATE")
    insert_version(conn, device_id, 1, definition, created_seq=0, created_at=now)
    enable_machine(conn, device_id, get_version(conn, device_id, 1), now)
    conn.commit()


# ------------------------------------------------------- service level (no HTTP)


def test_service_ingest_replay_and_conflict_without_http(conn):
    service = EventIngestionService(
        contract_resolver=forbidden_resolver,
        session_builder=make_session_builder(300),
    )
    occurred = datetime(2026, 9, 20, 10, 0, 0)  # naive: treated as UTC
    outcome = run_ingest(
        conn, service, device_id="dev-1", event_id="e1", occurred_at=occurred
    )
    assert outcome.replay is False
    assert outcome.state_transition is None
    assert outcome.event.occurred_at.tzinfo is timezone.utc
    assert outcome.session.event_count == 1
    assert [m.event_id for m in outcome.session.members] == ["e1"]

    replay = run_ingest(
        conn, service, device_id="dev-1", event_id="e1", occurred_at=occurred
    )
    assert replay.replay is True
    assert replay.session == outcome.session
    assert event_count(conn) == 1

    # Same event_id with different content -> conflict, nothing changes.
    with pytest.raises(IngestionConflict) as excinfo:
        run_ingest(
            conn,
            service,
            device_id="dev-1",
            event_id="e1",
            occurred_at=datetime(2026, 9, 20, 11, 0, 0),
        )
    assert "already exists with different" in excinfo.value.detail
    assert event_count(conn) == 1


def test_service_validates_contract_triplet_and_known_contract(conn):
    service = EventIngestionService(
        contract_resolver=static_resolver(None),  # no contract registered
        session_builder=make_session_builder(300),
    )
    occurred = datetime(2026, 9, 20, 10, 0, 0)

    # Partial triplet: event_type without schema_version/payload.
    with pytest.raises(ContractError) as excinfo:
        run_ingest(
            conn,
            service,
            device_id="d",
            event_id="e1",
            occurred_at=occurred,
            event_type="telemetry",
        )
    assert excinfo.value.errors == [
        {"path": "/schema_version", "error": "field is required for a payload event"},
        {"path": "/payload", "error": "field is required for a payload event"},
    ]

    # Full triplet but the resolver does not know the contract.
    with pytest.raises(ContractError) as excinfo:
        run_ingest(
            conn,
            service,
            device_id="d",
            event_id="e2",
            occurred_at=occurred,
            event_type="telemetry",
            schema_version="1.0",
            payload={"v": 1},
            payload_provided=True,
        )
    assert excinfo.value.message == "unknown event contract"
    assert event_count(conn) == 0


def test_service_payload_must_satisfy_the_contract(conn):
    service = EventIngestionService(
        contract_resolver=static_resolver(COMMAND_SCHEMA),
        session_builder=make_session_builder(300),
    )
    with pytest.raises(ContractError) as excinfo:
        run_ingest(
            conn,
            service,
            device_id="d",
            event_id="e1",
            occurred_at=datetime(2026, 9, 20, 10, 0, 0),
            event_type="command",
            schema_version="1.0",
            payload={"action": 42},  # not a string
            payload_provided=True,
        )
    assert excinfo.value.message == "payload does not satisfy the registered schema"
    assert event_count(conn) == 0


def test_service_leaves_transaction_control_to_the_caller(conn):
    """The service must not commit: a caller rollback undoes its writes."""
    service = EventIngestionService(
        contract_resolver=forbidden_resolver,
        session_builder=make_session_builder(300),
    )
    conn.execute("BEGIN IMMEDIATE")
    service.ingest(
        conn,
        device_id="d",
        event_id="e1",
        occurred_at=datetime(2026, 9, 20, 10, 0, 0),
    )
    conn.rollback()  # a no-op had the service committed on its own
    assert event_count(conn) == 0


def test_service_rolls_back_insert_and_projection_when_alerts_fail(
    conn, monkeypatch
):
    """A failure in the alert recomputation undoes the whole ingestion."""
    enable_turnstile(conn)
    service = EventIngestionService(
        contract_resolver=static_resolver(COMMAND_SCHEMA),
        session_builder=make_session_builder(300),
    )
    failing = {"on": True}

    def maybe_failing_reconcile(conn, device_id, now):
        if failing["on"]:
            raise RuntimeError("alert recomputation failed")

    monkeypatch.setattr(
        ingestion_module, "reconcile_device_alarms", maybe_failing_reconcile
    )

    conn.execute("BEGIN IMMEDIATE")
    with pytest.raises(RuntimeError):
        service.ingest(
            conn,
            device_id="dev-1",
            event_id="e1",
            occurred_at=datetime(2026, 9, 22, 10, 0, 0),
            event_type="command",
            schema_version="1.0",
            payload={"action": "unlock"},
            payload_provided=True,
        )
    conn.rollback()

    # Event row, transition log row and the state advance all vanished.
    assert event_count(conn) == 0
    assert (
        conn.execute("SELECT COUNT(*) AS c FROM state_transitions_log")
        .fetchone()["c"]
        == 0
    )
    machine = get_machine(conn, "dev-1")
    assert machine["current_state"] == "locked"
    assert machine["last_event_seq"] == 0

    # With the failure gone, the same ingest commits and projects.
    failing["on"] = False
    outcome = run_ingest(
        conn,
        service,
        device_id="dev-1",
        event_id="e1",
        occurred_at=datetime(2026, 9, 22, 10, 0, 0),
        event_type="command",
        schema_version="1.0",
        payload={"action": "unlock"},
        payload_provided=True,
    )
    assert outcome.replay is False
    assert outcome.state_transition["from_state"] == "locked"
    assert outcome.state_transition["to_state"] == "unlocked"
    assert get_machine(conn, "dev-1")["current_state"] == "unlocked"


def test_service_illegal_event_conflict_carries_structured_detail(conn):
    enable_turnstile(conn)
    service = EventIngestionService(
        contract_resolver=static_resolver(COMMAND_SCHEMA),
        session_builder=make_session_builder(300),
    )
    with pytest.raises(IngestionConflict) as excinfo:
        run_ingest(
            conn,
            service,
            device_id="dev-1",
            event_id="e1",
            occurred_at=datetime(2026, 9, 22, 10, 0, 0),
            event_type="command",
            schema_version="1.0",
            payload={"action": "lock"},  # no transition from "locked"
            payload_provided=True,
        )
    detail = excinfo.value.detail
    assert detail["reason"] == "no_matching_transition"
    assert detail["current_state"] == "locked"
    assert detail["machine_version"] == 1
    assert event_count(conn) == 0


# ------------------------------------------------------------- route level


def test_route_rolls_back_everything_when_alert_recompute_fails(
    tmp_path, monkeypatch
):
    """A 500 deep inside the write transaction leaves no partial state."""
    app = create_app(database_path=str(tmp_path / "route.db"), gap_seconds=300)
    with TestClient(app, raise_server_exceptions=False) as client:
        assert (
            client.put("/schemas/command/1.0", json={"schema": COMMAND_SCHEMA})
            .status_code
            == 201
        )
        assert (
            client.put(
                "/devices/dev-1/state-machine/versions/1",
                json=TURNSTILE_DEFINITION,
            ).status_code
            == 201
        )
        assert (
            client.post(
                "/devices/dev-1/state-machine/enable", json={"version": 1}
            ).status_code
            == 201
        )

        failing = {"on": True}

        def maybe_failing_reconcile(conn, device_id, now):
            if failing["on"]:
                raise RuntimeError("alert recomputation failed")

        monkeypatch.setattr(
            ingestion_module, "reconcile_device_alarms", maybe_failing_reconcile
        )
        event = {
            "device_id": "dev-1",
            "event_id": "e1",
            "occurred_at": "2026-09-22T10:00:00Z",
            "event_type": "command",
            "schema_version": "1.0",
            "payload": {"action": "unlock"},
        }
        resp = client.post("/events", json=event)
        assert resp.status_code == 500

        # The whole transaction rolled back: no event, no log row, the
        # machine never left its initial state.
        assert client.get("/devices/dev-1/sessions").json() == []
        status = client.get("/devices/dev-1/state-machine").json()
        assert status["current_state"] == "locked"
        assert status["last_event_seq"] == 0
        log = client.get("/devices/dev-1/state-machine/transitions").json()
        assert log["transitions"] == []

        # Once the failure is gone the identical request succeeds and the
        # response keeps the original session/transition semantics.
        failing["on"] = False
        resp = client.post("/events", json=event)
        assert resp.status_code == 201
        body = resp.json()
        assert body["replay"] is False
        assert body["state_transition"]["from_state"] == "locked"
        assert body["state_transition"]["to_state"] == "unlocked"
        assert [m["event_id"] for m in body["session"]["members"]] == ["e1"]
        assert client.get("/devices/dev-1/state-machine").json()[
            "current_state"
        ] == "unlocked"


# ------------------------------------------- create_app instance isolation


def test_create_app_instances_do_not_share_schema_cache(tmp_path):
    """Each app validates against its own contracts, cache included."""
    schema_a = {
        "type": "object",
        "properties": {"v": {"type": "number", "minimum": 0}},
        "additionalProperties": False,
    }
    schema_b = {
        "type": "object",
        "properties": {"v": {"type": "number", "minimum": 100}},
        "additionalProperties": False,
    }
    app_a = create_app(database_path=str(tmp_path / "a.db"), gap_seconds=300)
    app_b = create_app(database_path=str(tmp_path / "b.db"), gap_seconds=300)

    def event(event_id, value):
        return {
            "device_id": "dev-1",
            "event_id": event_id,
            "occurred_at": "2026-09-20T10:00:00Z",
            "event_type": "telemetry",
            "schema_version": "1.0",
            "payload": {"v": value},
        }

    with TestClient(app_a) as ca, TestClient(app_b) as cb:
        assert ca.put("/schemas/telemetry/1.0", json={"schema": schema_a}).status_code == 201

        # app_b has no such contract; app_a's registration/cache must not leak.
        resp = cb.post("/events", json=event("b-e0", 50))
        assert resp.status_code == 422
        assert resp.json()["detail"]["message"] == "unknown event contract"

        # The same (event_type, version) key with a *different* body on app_b.
        assert cb.put("/schemas/telemetry/1.0", json={"schema": schema_b}).status_code == 201

        # Warm app_a's cache: v=50 satisfies schema_a (minimum 0).
        assert ca.post("/events", json=event("a-e1", 50)).status_code == 201
        # app_b must still validate against its own body (minimum 100): a
        # shared cache would wrongly accept v=50 here.
        resp = cb.post("/events", json=event("b-e1", 50))
        assert resp.status_code == 422
        assert (
            resp.json()["detail"]["message"]
            == "payload does not satisfy the registered schema"
        )
        assert cb.post("/events", json=event("b-e2", 150)).status_code == 201
        # And app_a still applies its own body after app_b's traffic: a
        # cache poisoned by schema_b would reject v=50 here.
        assert ca.post("/events", json=event("a-e2", 50)).status_code == 201


def test_create_app_instances_do_not_share_gap_config(tmp_path):
    """The session-gap threshold is per app instance, never global."""
    app_a = create_app(database_path=str(tmp_path / "a.db"), gap_seconds=60)
    app_b = create_app(database_path=str(tmp_path / "b.db"), gap_seconds=3600)

    def submit_pair(client):
        first = client.post(
            "/events",
            json={
                "device_id": "dev-1",
                "event_id": "e1",
                "occurred_at": "2026-09-20T10:00:00Z",
            },
        )
        assert first.status_code == 201
        second = client.post(
            "/events",
            json={
                "device_id": "dev-1",
                "event_id": "e2",
                "occurred_at": "2026-09-20T10:02:00Z",  # 120s later
            },
        )
        assert second.status_code == 201
        return second

    with TestClient(app_a) as ca, TestClient(app_b) as cb:
        # 120s exceeds the 60s gap: e2 opens its own session.
        assert submit_pair(ca).json()["session"]["event_count"] == 1
        assert len(ca.get("/devices/dev-1/sessions").json()) == 2
        # 120s is within the 3600s gap: both events share one session.
        assert submit_pair(cb).json()["session"]["event_count"] == 2
        assert len(cb.get("/devices/dev-1/sessions").json()) == 1

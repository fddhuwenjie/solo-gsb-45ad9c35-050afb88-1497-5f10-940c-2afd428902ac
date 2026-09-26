"""Event payload contracts: registration, version chains and validation."""

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

GAP = 300  # seconds

NESTED_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["reading", "tags"],
    "properties": {
        "reading": {
            "type": "object",
            "required": ["value", "unit"],
            "properties": {
                "value": {"type": "number", "minimum": 0},
                "unit": {"type": "string", "enum": ["c", "f"]},
                "meta": {
                    "type": "object",
                    "properties": {"source": {"type": "string", "minLength": 1}},
                },
            },
        },
        "tags": {"type": "array", "items": {"type": "string", "minLength": 1}},
    },
}

ARRAY_SCHEMA = {
    "type": "array",
    "minItems": 1,
    "items": {
        "type": "object",
        "required": ["id"],
        "properties": {"id": {"type": "integer", "minimum": 1}},
    },
}


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=GAP)
    with TestClient(app) as c:
        yield c


def register(client, event_type, version, schema, supersedes=None):
    body = {"schema": schema}
    if supersedes is not None:
        body["supersedes"] = supersedes
    return client.put(f"/schemas/{event_type}/{version}", json=body)


def submit_payload(client, event_id, event_type, version, payload, ts="2026-09-20T10:00:00Z", device="dev-1"):
    return client.post(
        "/events",
        json={
            "device_id": device,
            "event_id": event_id,
            "occurred_at": ts,
            "event_type": event_type,
            "schema_version": version,
            "payload": payload,
        },
    )


# ------------------------------------------------------------ registration

def test_register_and_get_contract(client):
    resp = register(client, "telemetry", "1.0", NESTED_SCHEMA)
    assert resp.status_code == 201
    body = resp.json()
    assert body["event_type"] == "telemetry"
    assert body["schema_version"] == "1.0"
    assert body["supersedes"] is None
    assert body["schema"] == NESTED_SCHEMA
    assert "created_at" in body

    fetched = client.get("/schemas/telemetry/1.0")
    assert fetched.status_code == 200
    assert fetched.json()["schema"] == NESTED_SCHEMA


def test_get_unknown_contract_is_404(client):
    resp = client.get("/schemas/nope/9.9")
    assert resp.status_code == 404


def test_identical_put_is_idempotent_replay(client):
    first = register(client, "telemetry", "1.0", NESTED_SCHEMA)
    assert first.status_code == 201
    second = register(client, "telemetry", "1.0", NESTED_SCHEMA)
    assert second.status_code == 200
    assert second.json()["created_at"] == first.json()["created_at"]


def test_contract_is_immutable_rewrite_rejected(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    changed = dict(NESTED_SCHEMA)
    changed["description"] = "mutated"
    resp = register(client, "telemetry", "1.0", changed)
    assert resp.status_code == 409

    # Original document survives.
    assert client.get("/schemas/telemetry/1.0").json()["schema"] == NESTED_SCHEMA


def test_rewrite_of_supersedes_pointer_is_rejected(client):
    register(client, "telemetry", "1.0", {"type": "object"})
    resp = register(client, "telemetry", "1.0", {"type": "object"}, supersedes="0.9")
    assert resp.status_code == 409


def test_supersedes_requires_existing_predecessor(client):
    resp = register(client, "telemetry", "2.0", {"type": "object"}, supersedes="1.0")
    assert resp.status_code == 422
    errors = resp.json()["detail"]["errors"]
    assert errors == [
        {
            "path": "/supersedes",
            "error": "version '1.0' is not registered for this event_type",
        }
    ]


def test_version_chain_is_scoped_per_event_type(client):
    # 1.0 exists for telemetry but not for diagnostics.
    register(client, "telemetry", "1.0", {"type": "object"})
    resp = register(client, "diagnostics", "2.0", {"type": "object"}, supersedes="1.0")
    assert resp.status_code == 422


def test_supersedes_self_reference_is_rejected(client):
    resp = register(client, "telemetry", "1.0", {"type": "object"}, supersedes="1.0")
    assert resp.status_code == 422
    assert "cycle" in resp.json()["detail"]["errors"][0]["error"]


def test_chained_supersedes_walks_to_root(client):
    register(client, "telemetry", "1.0", {"type": "object"})
    assert register(
        client, "telemetry", "2.0", {"type": "object"}, supersedes="1.0"
    ).status_code == 201
    resp = register(
        client, "telemetry", "3.0", {"type": "object"}, supersedes="2.0"
    )
    assert resp.status_code == 201
    assert resp.json()["supersedes"] == "2.0"
    assert client.get("/schemas/telemetry/3.0").json()["supersedes"] == "2.0"


def test_missing_grandpredecessor_anywhere_in_chain_rejected(client):
    # Build 1.0 then 3.0 -> 2.0 where 2.0 is missing: rejected even though
    # the direct predecessor string is non-empty.
    register(client, "telemetry", "1.0", {"type": "object"})
    # Register 2.0 for a *different* event type; it must not be visible here.
    register(client, "other", "2.0", {"type": "object"})
    resp = register(
        client, "telemetry", "3.0", {"type": "object"}, supersedes="2.0"
    )
    assert resp.status_code == 422
    assert "'2.0'" in resp.json()["detail"]["errors"][0]["error"]


def test_external_refs_are_rejected_with_pointer(client):
    cases = [
        {"$ref": "https://example.com/common.json#/x"},
        {"$ref": "file:///etc/schemas/x.json"},
        {"properties": {"a": {"$dynamicRef": "https://example.com/dyn"}}, "type": "object"},
        {"items": [{"$ref": "other.json"}], "type": "array"},
    ]
    for index, document in enumerate(cases):
        resp = register(client, f"bad{index}", "1", document)
        assert resp.status_code == 422
        errors = resp.json()["detail"]["errors"]
        assert all(e["path"] for e in errors)
        assert "external" in errors[0]["error"] or "'#'" in errors[0]["error"]


def test_local_definition_ref_is_allowed(client):
    document = {
        "type": "object",
        "required": ["name"],
        "properties": {"name": {"$ref": "#/$defs/nonEmpty"}},
        "$defs": {"nonEmpty": {"type": "string", "minLength": 1}},
    }
    assert register(client, "local", "1.0", document).status_code == 201


def test_invalid_meta_schema_rejected(client):
    resp = register(client, "bad", "1.0", {"type": "not-a-type"})
    assert resp.status_code == 422
    assert resp.json()["detail"]["errors"][0]["path"] == "/type"


def test_schema_root_must_be_object(client):
    resp = client.put("/schemas/bad/1.0", json={"schema": ["nope"]})
    assert resp.status_code == 422


# ------------------------------------------------------------ event validation

def test_valid_payload_event_persists_and_echoes(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    payload = {
        "reading": {"value": 21.5, "unit": "c", "meta": {"source": "sensor-a"}},
        "tags": ["warm", "lab"],
    }
    resp = submit_payload(client, "e1", "telemetry", "1.0", payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["event"]["event_type"] == "telemetry"
    assert body["event"]["schema_version"] == "1.0"
    assert body["event"]["payload"] == payload

    # Reloaded from SQLite on session read.
    members = client.get("/devices/dev-1/sessions").json()[0]["members"]
    assert members[0]["payload"] == payload
    assert members[0]["event_type"] == "telemetry"


def test_nested_object_error_has_json_pointer(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    payload = {"reading": {"value": -4, "unit": "kelvin"}, "tags": []}
    resp = submit_payload(client, "e1", "telemetry", "1.0", payload)
    assert resp.status_code == 422
    paths = {e["path"] for e in resp.json()["detail"]["errors"]}
    assert "/reading/value" in paths
    assert "/reading/unit" in paths


def test_array_index_and_deeply_nested_pointers(client):
    register(client, "batch", "1.0", ARRAY_SCHEMA)
    resp = submit_payload(
        client, "e1", "batch", "1.0", [{"id": 1}, {"id": 0}]
    )
    assert resp.status_code == 422
    errors = resp.json()["detail"]["errors"]
    assert errors[0]["path"] == "/1/id"
    assert "minimum" in errors[0]["error"]


def test_missing_required_field_pointer_is_object_path(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    resp = submit_payload(client, "e1", "telemetry", "1.0", {})
    assert resp.status_code == 422
    paths = {e["path"] for e in resp.json()["detail"]["errors"]}
    assert paths == {"/reading", "/tags"}


def test_root_payload_wrong_type_pointer_is_empty(client):
    register(client, "batch", "1.0", ARRAY_SCHEMA)
    resp = submit_payload(client, "e1", "batch", "1.0", {"not": "an array"})
    assert resp.status_code == 422
    assert resp.json()["detail"]["errors"][0]["path"] == ""


def test_unknown_version_is_422_and_nothing_persisted(client):
    resp = submit_payload(client, "e1", "telemetry", "9.9", {"x": 1})
    assert resp.status_code == 422
    assert resp.json()["detail"]["errors"][0]["path"] == "/schema_version"
    assert client.get("/devices/dev-1/sessions").json() == []


def test_validation_failure_happens_before_insert(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    resp = submit_payload(client, "e1", "telemetry", "1.0", {"bad": True})
    assert resp.status_code == 422
    assert client.get("/devices/dev-1/sessions").json() == []


def test_partial_contract_triplet_is_422(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    base = {
        "device_id": "dev-1",
        "event_id": "e1",
        "occurred_at": "2026-09-20T10:00:00Z",
    }

    only_type = client.post("/events", json={**base, "event_type": "telemetry"})
    assert only_type.status_code == 422
    paths = {e["path"] for e in only_type.json()["detail"]["errors"]}
    assert paths == {"/schema_version", "/payload"}
    assert client.get("/devices/dev-1/sessions").json() == []

    type_and_version = client.post(
        "/events",
        json={**base, "event_type": "telemetry", "schema_version": "1.0"},
    )
    assert type_and_version.status_code == 422
    assert type_and_version.json()["detail"]["errors"][0]["path"] == "/payload"


def test_payload_replay_is_idempotent(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    payload = {"reading": {"value": 1, "unit": "c"}, "tags": ["x"]}
    first = submit_payload(client, "e1", "telemetry", "1.0", payload)
    second = submit_payload(client, "e1", "telemetry", "1.0", payload)
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["replay"] is True
    assert second.json()["session"] == first.json()["session"]


def test_payload_replay_key_order_does_not_matter(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    first = submit_payload(
        client,
        "e1",
        "telemetry",
        "1.0",
        {"tags": ["x"], "reading": {"unit": "c", "value": 1}},
    )
    second = submit_payload(
        client,
        "e1",
        "telemetry",
        "1.0",
        {"reading": {"value": 1, "unit": "c"}, "tags": ["x"]},
    )
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["replay"] is True


def test_payload_difference_is_409(client):
    register(client, "telemetry", "1.0", NESTED_SCHEMA)
    submit_payload(
        client, "e1", "telemetry", "1.0",
        {"reading": {"value": 1, "unit": "c"}, "tags": ["x"]},
    )
    resp = submit_payload(
        client, "e1", "telemetry", "1.0",
        {"reading": {"value": 2, "unit": "c"}, "tags": ["x"]},
    )
    assert resp.status_code == 409


def test_schema_version_difference_is_409(client):
    register(client, "telemetry", "1.0", {"type": "object"})
    register(
        client,
        "telemetry",
        "2.0",
        {"type": "object", "properties": {"v": {"type": "number"}}},
        supersedes="1.0",
    )
    submit_payload(client, "e1", "telemetry", "1.0", {})
    resp = submit_payload(client, "e1", "telemetry", "2.0", {"v": 1})
    assert resp.status_code == 409


def test_event_type_difference_is_409(client):
    register(client, "telemetry", "1.0", {"type": "object"})
    register(client, "diagnostics", "1.0", {"type": "object"})
    submit_payload(client, "e1", "telemetry", "1.0", {})
    resp = submit_payload(client, "e1", "diagnostics", "1.0", {})
    assert resp.status_code == 409


def test_legacy_and_payload_events_merge_together(client):
    """Payload-less requests stay compatible and share the same merge result."""
    register(client, "telemetry", "1.0", {"type": "object"})

    legacy = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "old",
            "occurred_at": "2026-09-20T10:00:00Z",
        },
    )
    assert legacy.status_code == 201
    assert legacy.json()["event"]["event_type"] is None
    assert legacy.json()["event"]["payload"] is None

    loaded = submit_payload(
        client, "new", "telemetry", "1.0", {}, ts="2026-09-20T10:01:00Z"
    )
    assert loaded.status_code == 201
    session = loaded.json()["session"]
    assert session["event_count"] == 2
    assert [m["event_id"] for m in session["members"]] == ["old", "new"]
    assert session["members"][0]["payload"] is None
    assert session["members"][1]["payload"] == {}


def test_legacy_replay_conflict_logic_unchanged(client):
    client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "e1",
            "occurred_at": "2026-09-20T10:00:00Z",
        },
    )
    # Legacy replay.
    replay = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "e1",
            "occurred_at": "2026-09-20T10:00:00Z",
        },
    )
    assert replay.status_code == 200
    assert replay.json()["replay"] is True

    # Same id reused as payload event -> conflict.
    register(client, "telemetry", "1.0", {"type": "object"})
    resp = submit_payload(client, "e1", "telemetry", "1.0", {})
    assert resp.status_code == 409


def test_contracts_and_events_persist_across_app_restart(tmp_path):
    db_path = str(tmp_path / "persist.db")
    with TestClient(create_app(database_path=db_path, gap_seconds=GAP)) as c:
        register(c, "telemetry", "1.0", NESTED_SCHEMA)
        submit_payload(
            c, "e1", "telemetry", "1.0",
            {"reading": {"value": 1, "unit": "c"}, "tags": ["a"]},
        )

    with TestClient(create_app(database_path=db_path, gap_seconds=GAP)) as c:
        schema = c.get("/schemas/telemetry/1.0")
        assert schema.status_code == 200
        assert schema.json()["schema"] == NESTED_SCHEMA
        members = c.get("/devices/dev-1/sessions").json()[0]["members"]
        assert members[0]["payload"]["reading"]["value"] == 1


def test_payload_event_participates_in_bridge_merge(client):
    register(client, "telemetry", "1.0", {"type": "object"})
    submit_payload(client, "a", "telemetry", "1.0", {}, ts="2026-09-20T10:00:00Z")
    submit_payload(client, "b", "telemetry", "1.0", {}, ts="2026-09-20T10:08:00Z")
    assert len(client.get("/devices/dev-1/sessions").json()) == 2

    resp = submit_payload(client, "c", "telemetry", "1.0", {}, ts="2026-09-20T10:04:00Z")
    assert resp.json()["session"]["event_count"] == 3
    assert [m["event_id"] for m in resp.json()["session"]["members"]] == ["a", "c", "b"]


# ------------------------------------------------------------ chain unit test

def test_validate_chain_link_detects_preexisting_cycle():
    """Defensive: even if stored data already contains a cycle, linking into
    it (and walking it) must fail instead of looping forever."""
    from app.contracts import MISSING, ContractError, validate_chain_link

    # Stored chain: v1 -> v2 -> v1 (cycle, normally impossible via the API).
    store = {"v1": "v2", "v2": "v1"}

    def get_supersedes(version):
        return store.get(version, MISSING)

    with pytest.raises(ContractError) as exc:
        validate_chain_link("v3", "v2", get_supersedes)
    assert "cycle" in exc.value.errors[0]["error"]


def test_legacy_database_without_payload_columns_is_migrated(tmp_path):
    """A DB created before payload contracts keep working after startup."""
    import sqlite3

    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE events (
            event_id    TEXT PRIMARY KEY,
            device_id   TEXT NOT NULL,
            occurred_at TEXT NOT NULL
        );
        INSERT INTO events VALUES
            ('legacy-1', 'dev-1', '2026-09-20T10:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()

    with TestClient(create_app(database_path=db_path, gap_seconds=GAP)) as c:
        resp = c.post(
            "/events",
            json={
                "device_id": "dev-1",
                "event_id": "legacy-2",
                "occurred_at": "2026-09-20T10:01:00Z",
            },
        )
        assert resp.status_code == 201
        sessions = c.get("/devices/dev-1/sessions").json()
        assert sessions[0]["event_count"] == 2

        register(c, "t", "1", {"type": "object"})
        resp = submit_payload(
            c, "p-1", "t", "1", {}, ts="2026-09-20T10:02:00Z"
        )
        assert resp.status_code == 201
        assert len(c.get("/devices/dev-1/sessions").json()[0]["members"]) == 3


def test_explicit_null_scalar_fields_are_422(client):
    """event_type/schema_version sent as JSON null count as missing."""
    base = {
        "device_id": "dev-1",
        "event_id": "e1",
        "occurred_at": "2026-09-20T10:00:00Z",
    }
    resp = client.post(
        "/events",
        json={
            **base,
            "event_type": None,
            "schema_version": "1.0",
            "payload": {},
        },
    )
    assert resp.status_code == 422
    assert resp.json()["detail"]["errors"][0]["path"] == "/event_type"
    assert client.get("/devices/dev-1/sessions").json() == []


def test_null_payload_validates_against_null_contract(client):
    """An explicit payload of null is a real value when the schema allows it."""
    register(client, "ping", "1.0", {"type": "null"})
    resp = submit_payload(client, "e1", "ping", "1.0", None)
    assert resp.status_code == 201
    assert resp.json()["event"]["payload"] is None

    replay = submit_payload(client, "e1", "ping", "1.0", None)
    assert replay.status_code == 200
    assert replay.json()["replay"] is True

    members = client.get("/devices/dev-1/sessions").json()[0]["members"]
    assert members[0]["payload"] is None
    assert members[0]["event_type"] == "ping"

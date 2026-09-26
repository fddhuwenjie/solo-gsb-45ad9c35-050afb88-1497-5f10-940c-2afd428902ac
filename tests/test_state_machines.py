"""Tests for the per-device state-machine event projection API.

Covers immutable version registration, enabling, projection in arrival
order, illegal/ambiguous events (409, nothing persisted), duplicate
event_id idempotency, the persisted current state, cursor-paged transition
logs, concurrency and restart/replay behavior.
"""

import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def turnstile_definition(version=1):
    return {
        "version": version,
        "initial_state": "locked",
        "states": ["locked", "unlocked"],
        "transitions": [
            {
                "from_state": "locked",
                "event_type": "command",
                "to_state": "unlocked",
                "conditions": [
                    {"pointer": "/action", "operator": "eq", "value": "unlock"}
                ],
            },
            {
                "from_state": "unlocked",
                "event_type": "command",
                "to_state": "locked",
                "conditions": [
                    {"pointer": "/action", "operator": "eq", "value": "lock"}
                ],
            },
        ],
    }


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=300)
    with TestClient(app) as c:
        yield c


def register_turnstile(client, device="dev-1", version=1):
    return client.put(
        f"/devices/{device}/state-machine/versions/{version}",
        json=turnstile_definition(version),
    )


def enable(client, device="dev-1", version=1):
    return client.post(
        f"/devices/{device}/state-machine/enable", json={"version": version}
    )


def post_event(client, action, event_id, device="dev-1", ts="2026-09-22T10:00:00Z"):
    return client.post(
        "/events",
        json={
            "device_id": device,
            "event_id": event_id,
            "occurred_at": ts,
            "event_type": "command",
            "schema_version": "1.0",
            "payload": {"action": action},
        },
    )


@pytest.fixture(autouse=True)
def _command_schema(client):
    # Payload events must satisfy a registered contract even for projection.
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


# ----------------------------------------------------------- registration


def test_register_first_version_and_get(client):
    resp = register_turnstile(client)
    assert resp.status_code == 201
    body = resp.json()
    assert body["version"] == 1
    assert body["initial_state"] == "locked"
    assert body["states"] == ["locked", "unlocked"]
    assert len(body["transitions"]) == 2
    assert body["created_seq"] == 0

    fetched = client.get("/devices/dev-1/state-machine/versions/1")
    assert fetched.status_code == 200
    assert fetched.json() == body

    listing = client.get("/devices/dev-1/state-machine/versions")
    assert [v["version"] for v in listing.json()] == [1]


def test_get_unknown_version_404(client):
    assert client.get("/devices/dev-1/state-machine/versions/9").status_code == 404


def test_identical_put_is_idempotent_replay(client):
    first = register_turnstile(client)
    assert first.status_code == 201
    second = register_turnstile(client)
    assert second.status_code == 200
    assert second.json() == first.json()


def test_version_numbering_is_append_only(client):
    assert register_turnstile(client, version=1).status_code == 201
    # Skipping version 2 is a 409.
    resp = client.put(
        "/devices/dev-1/state-machine/versions/3",
        json=turnstile_definition(3),
    )
    assert resp.status_code == 409
    # Registering the next consecutive version works.
    assert register_turnstile(client, version=2).status_code == 201
    # Re-registering an existing version with a *different* body is 409.
    changed = turnstile_definition(1)
    changed["initial_state"] = "unlocked"
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=changed)
    assert resp.status_code == 409
    # The immutable version 1 is unchanged.
    v1 = client.get("/devices/dev-1/state-machine/versions/1").json()
    assert v1["initial_state"] == "locked"


def test_path_version_must_match_body(client):
    body = turnstile_definition(2)  # body says 2, path says 1
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=body)
    assert resp.status_code == 422
    assert resp.json()["detail"]["errors"][0]["path"] == "/version"


def test_invalid_definitions_are_422(client):
    base = turnstile_definition()

    bad_initial = {**base, "initial_state": "missing"}
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=bad_initial)
    assert resp.status_code == 422
    paths = {e["path"] for e in resp.json()["detail"]["errors"]}
    assert "/initial_state" in paths

    dup_states = {**base, "states": ["locked", "locked"]}
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=dup_states)
    assert resp.status_code == 422
    assert any(e["path"] == "/states" for e in resp.json()["detail"]["errors"])

    bad_target = {
        **base,
        "transitions": [
            {**base["transitions"][0], "to_state": "nowhere"},
            *base["transitions"][1:],
        ],
    }
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=bad_target)
    assert resp.status_code == 422
    assert any(
        e["path"] == "/transitions/0/to_state"
        for e in resp.json()["detail"]["errors"]
    )

    bad_pointer = {
        **base,
        "transitions": [
            {
                **base["transitions"][0],
                "conditions": [
                    {"pointer": "action", "operator": "eq", "value": "unlock"}
                ],
            },
            *base["transitions"][1:],
        ],
    }
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=bad_pointer)
    assert resp.status_code == 422  # pointer must start with '/'

    duplicate = {
        **base,
        "transitions": [base["transitions"][0], base["transitions"][0]],
    }
    resp = client.put("/devices/dev-1/state-machine/versions/1", json=duplicate)
    assert resp.status_code == 422
    assert any(
        "duplicate transition" in e["error"] for e in resp.json()["detail"]["errors"]
    )


# --------------------------------------------------------------- enabling


def test_enable_starts_at_initial_state_and_pins_high_water(client):
    # An event written BEFORE enabling must never be projected.
    register_turnstile(client)
    assert post_event(client, "unlock", "pre-1").status_code == 201

    resp = enable(client)
    assert resp.status_code == 201
    body = resp.json()
    assert body["version"] == 1
    assert body["current_state"] == "locked"
    assert body["last_event_seq"] == 1  # pinned to the pre-existing event

    status = client.get("/devices/dev-1/state-machine")
    assert status.json()["current_state"] == "locked"
    # No log rows for the pre-enable event.
    page = client.get("/devices/dev-1/state-machine/transitions")
    assert page.json()["transitions"] == []


def test_enable_unknown_version_404(client):
    register_turnstile(client)
    resp = client.post(
        "/devices/dev-1/state-machine/enable", json={"version": 7}
    )
    assert resp.status_code == 404


def test_enabled_version_is_immutable(client):
    register_turnstile(client, version=1)
    register_turnstile(client, version=2)
    assert enable(client, version=1).status_code == 201

    # Same version replays 200.
    again = enable(client, version=1)
    assert again.status_code == 200

    # Switching to another version is rejected.
    resp = enable(client, version=2)
    assert resp.status_code == 409
    assert client.get("/devices/dev-1/state-machine").json()["version"] == 1


def test_status_404_without_enabling(client):
    register_turnstile(client)
    assert client.get("/devices/dev-1/state-machine").status_code == 404
    assert client.get(
        "/devices/dev-1/state-machine/transitions"
    ).status_code == 404


# ------------------------------------------------------------- projection


def test_projection_executes_unique_transitions_in_arrival_order(client):
    register_turnstile(client)
    enable(client)

    resp = post_event(client, "unlock", "e1", ts="2026-09-22T10:00:00Z")
    assert resp.status_code == 201
    transition = resp.json()["state_transition"]
    assert transition["from_state"] == "locked"
    assert transition["to_state"] == "unlocked"
    assert transition["event_id"] == "e1"
    assert transition["transition_index"] == 0

    assert client.get("/devices/dev-1/state-machine").json()["current_state"] == \
        "unlocked"

    resp = post_event(client, "lock", "e2", ts="2026-09-22T10:01:00Z")
    assert resp.json()["state_transition"]["to_state"] == "locked"
    assert client.get("/devices/dev-1/state-machine").json()["current_state"] == \
        "locked"


def test_illegal_event_is_409_and_nothing_is_persisted(client):
    register_turnstile(client)
    enable(client)

    # From locked, a "lock" command has no matching transition.
    resp = post_event(client, "lock", "bad-1")
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "no_matching_transition"

    # The event was not stored.
    search = client.post("/events/search", json={"device_id": "dev-1"})
    assert search.json()["events"] == []
    # State and log untouched.
    status = client.get("/devices/dev-1/state-machine").json()
    assert status["current_state"] == "locked"
    assert client.get(
        "/devices/dev-1/state-machine/transitions"
    ).json()["transitions"] == []

    # A legal event afterwards still works from the unchanged state.
    assert post_event(client, "unlock", "e1").status_code == 201


def test_ambiguous_event_is_409_and_nothing_is_persisted(client):
    # Two transitions from idle both match the same payload: one matches the
    # event type alone (no conditions), the other matches the type plus a
    # condition that holds. Neither definition is an exact duplicate (so
    # registration succeeds), yet at runtime both match.
    ambiguous = {
        "version": 1,
        "initial_state": "idle",
        "states": ["idle", "a", "b"],
        "transitions": [
            {"from_state": "idle", "event_type": "command", "to_state": "a",
             "conditions": []},
            {"from_state": "idle", "event_type": "command", "to_state": "b",
             "conditions": [{"pointer": "/action", "operator": "eq",
                             "value": "go"}]},
        ],
    }
    resp = client.put(
        "/devices/dev-1/state-machine/versions/1", json=ambiguous
    )
    assert resp.status_code == 201
    enable(client)

    resp = post_event(client, "go", "amb-1")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["reason"] == "ambiguous_transition"
    assert detail["matching_transition_indices"] == [0, 1]

    # Nothing persisted: no event, no state change, no log row.
    assert client.post(
        "/events/search", json={"device_id": "dev-1"}
    ).json()["events"] == []
    assert client.get("/devices/dev-1/state-machine").json()["current_state"] \
        == "idle"
    assert client.get(
        "/devices/dev-1/state-machine/transitions"
    ).json()["transitions"] == []

    # A different action matches only the unconditional transition -> 201.
    resp = post_event(client, "other", "amb-2")
    assert resp.status_code == 201
    assert resp.json()["state_transition"]["to_state"] == "a"


def test_payloadless_event_rejected_while_machine_enabled(client):
    register_turnstile(client)
    enable(client)
    resp = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "plain-1",
            "occurred_at": "2026-09-22T10:00:00Z",
        },
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "no_matching_transition"


def test_wrong_event_type_does_not_match(client):
    register_turnstile(client)
    enable(client)
    resp = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "other-1",
            "occurred_at": "2026-09-22T10:00:00Z",
            "event_type": "telemetry",
            "schema_version": "1.0",
            "payload": {"action": "unlock"},
        },
    )
    # Contract validation happens first: telemetry/1.0 is unknown -> 422.
    assert resp.status_code == 422

    # Register a permissive telemetry contract, then the type mismatch
    # itself is an illegal transition (409, not stored).
    client.put(
        "/schemas/telemetry/1.0",
        json={"schema": {"type": "object"}},
    )
    resp = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "other-2",
            "occurred_at": "2026-09-22T10:01:00Z",
            "event_type": "telemetry",
            "schema_version": "1.0",
            "payload": {"action": "unlock"},
        },
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "no_matching_transition"


def test_duplicate_event_id_is_idempotent_and_does_not_reproject(client):
    register_turnstile(client)
    enable(client)

    first = post_event(client, "unlock", "e1")
    assert first.status_code == 201
    assert first.json()["state_transition"]["to_state"] == "unlocked"

    # Identical replay: 200, same transition, state/log not advanced.
    replay = post_event(client, "unlock", "e1")
    assert replay.status_code == 200
    assert replay.json()["replay"] is True
    assert replay.json()["state_transition"] == first.json()["state_transition"]

    log = client.get("/devices/dev-1/state-machine/transitions").json()
    assert [t["event_id"] for t in log["transitions"]] == ["e1"]
    assert client.get("/devices/dev-1/state-machine").json()["current_state"] \
        == "unlocked"

    # Even though a replay from the current state would be illegal, the
    # replay is still 200 (idempotency precedes projection); then a real
    # "unlock" from unlocked is illegal (409).
    assert post_event(client, "unlock", "e2").status_code == 409


def test_duplicate_event_id_with_different_content_still_409(client):
    register_turnstile(client)
    enable(client)
    assert post_event(client, "unlock", "e1").status_code == 201

    resp = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "e1",
            "occurred_at": "2026-09-22T10:00:01Z",  # different timestamp
            "event_type": "command",
            "schema_version": "1.0",
            "payload": {"action": "unlock"},
        },
    )
    assert resp.status_code == 409
    assert "already exists" in resp.json()["detail"]


def test_devices_without_enabled_machine_are_unaffected(client):
    register_turnstile(client, device="dev-1")
    enable(client, device="dev-1")
    # dev-2 never enabled a machine: arbitrary legal events still ingest.
    resp = client.post(
        "/events",
        json={
            "device_id": "dev-2",
            "event_id": "x1",
            "occurred_at": "2026-09-22T10:00:00Z",
            "event_type": "command",
            "schema_version": "1.0",
            "payload": {"action": "anything"},
        },
    )
    assert resp.status_code == 201
    assert resp.json()["state_transition"] is None


# ------------------------------------------------------------ log paging


def test_transition_log_keyset_pagination(client):
    register_turnstile(client)
    enable(client)
    # locked -> unlocked -> locked -> unlocked: 3 projected events.
    post_event(client, "unlock", "e1", ts="2026-09-22T10:00:00Z")
    post_event(client, "lock", "e2", ts="2026-09-22T10:01:00Z")
    post_event(client, "unlock", "e3", ts="2026-09-22T10:02:00Z")

    first = client.get(
        "/devices/dev-1/state-machine/transitions?limit=2"
    ).json()
    assert [t["event_id"] for t in first["transitions"]] == ["e1", "e2"]
    assert first["limit"] == 2
    assert first["next_cursor"] is not None
    # Illegal events do not consume a log slot.
    assert post_event(client, "unlock", "bad").status_code == 409

    second = client.get(
        "/devices/dev-1/state-machine/transitions",
        params={"limit": 2, "cursor": first["next_cursor"]},
    ).json()
    assert [t["event_id"] for t in second["transitions"]] == ["e3"]
    assert second["next_cursor"] is None
    assert second["limit"] == 2  # cursor carries the original limit


def test_log_cursor_is_opaque_and_validated(client):
    register_turnstile(client)
    enable(client)
    post_event(client, "unlock", "e1", ts="2026-09-22T10:00:00Z")
    post_event(client, "lock", "e2", ts="2026-09-22T10:01:00Z")

    resp = client.get(
        "/devices/dev-1/state-machine/transitions",
        params={"cursor": "smc1:not-a-token"},
    )
    assert resp.status_code == 422
    resp = client.get(
        "/devices/dev-1/state-machine/transitions",
        params={"cursor": "evc1:abc"},
    )
    assert resp.status_code == 422
    # A tampered cursor with a valid prefix still fails the signature.
    page = client.get(
        "/devices/dev-1/state-machine/transitions?limit=1"
    ).json()
    cursor = page["next_cursor"]
    # Flip a character in the payload portion (before the '.' signature).
    payload, sig = cursor[len("smc1:"):].rsplit(".", 1)
    flipped_char = "A" if payload[0] != "A" else "B"
    tampered = "smc1:" + flipped_char + payload[1:] + "." + sig
    resp = client.get(
        "/devices/dev-1/state-machine/transitions",
        params={"cursor": tampered},
    )
    assert resp.status_code == 422


def test_arrival_order_follows_seq_not_event_time(client):
    # State projection is in ARRIVAL order: a legal chain arriving with
    # out-of-order occurred_at still projects by seq.
    register_turnstile(client)
    enable(client)
    post_event(client, "unlock", "late-1", ts="2026-09-22T11:00:00Z")
    post_event(client, "lock", "early-1", ts="2026-09-22T09:00:00Z")
    log = client.get("/devices/dev-1/state-machine/transitions").json()
    assert [t["event_id"] for t in log["transitions"]] == ["late-1", "early-1"]
    assert client.get("/devices/dev-1/state-machine").json()["current_state"] \
        == "locked"


# --------------------------------------------------------------- restart


def test_state_and_log_survive_restart(tmp_path):
    db_path = tmp_path / "restart.db"
    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as first:
        first.put(
            "/schemas/command/1.0",
            json={
                "schema": {
                    "type": "object",
                    "required": ["action"],
                    "properties": {"action": {"type": "string"}},
                }
            },
        )
        register_turnstile(first)
        enable(first)
        post_event(first, "unlock", "e1", ts="2026-09-22T10:00:00Z")
        post_event(first, "lock", "e2", ts="2026-09-22T10:01:00Z")
        before = first.get(
            "/devices/dev-1/state-machine/transitions"
        ).json()
        assert before["transitions"][-1]["to_state"] == "locked"

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as second:
        status = second.get("/devices/dev-1/state-machine").json()
        assert status["current_state"] == "locked"
        assert status["version"] == 1
        after = second.get("/devices/dev-1/state-machine/transitions").json()
        assert [t["event_id"] for t in after["transitions"]] == ["e1", "e2"]

        # Projection continues from the persisted state.
        resp = post_event(second, "unlock", "e3",
                          ts="2026-09-22T10:02:00Z")
        assert resp.status_code == 201
        assert resp.json()["state_transition"]["from_state"] == "locked"

        # Replaying an old event after restart stays idempotent.
        replay = post_event(second, "lock", "e2",
                            ts="2026-09-22T10:01:00Z")
        assert replay.status_code == 200
        log = second.get("/devices/dev-1/state-machine/transitions").json()
        assert [t["event_id"] for t in log["transitions"]] == [
            "e1", "e2", "e3"
        ]


# ------------------------------------------------------------ concurrency


def test_concurrent_events_serialize_in_arrival_order(tmp_path):
    """Concurrent legal events: one machine, all writers serialize.

    All 8 threads start from ``locked`` sending "unlock": exactly one
    request can win (201); every loser observes the advanced ``unlocked``
    state inside the same write lock and is rejected as illegal (409) --
    state and log therefore stay consistent (single log row).
    """
    db_path = tmp_path / "conc.db"
    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as setup:
        setup.put(
            "/schemas/command/1.0",
            json={
                "schema": {
                    "type": "object",
                    "required": ["action"],
                    "properties": {"action": {"type": "string"}},
                }
            },
        )
        register_turnstile(setup)
        enable(setup)

    barrier = threading.Barrier(8)
    codes: list[int] = []
    lock = threading.Lock()

    def worker(index):
        with TestClient(app) as local:
            barrier.wait()
            resp = post_event(
                local, "unlock", f"c{index}",
                ts=f"2026-09-22T10:00:{index:02d}Z",
            )
            with lock:
                codes.append(resp.status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with TestClient(app) as check:
        assert codes.count(201) == 1
        assert codes.count(409) == 7
        assert all(code in (201, 409) for code in codes)
        status = check.get("/devices/dev-1/state-machine").json()
        assert status["current_state"] == "unlocked"
        entries = check.get(
            "/devices/dev-1/state-machine/transitions?limit=500"
        ).json()["transitions"]
        assert len(entries) == 1
        assert entries[0]["to_state"] == "unlocked"


def test_concurrent_projection_against_single_machine(tmp_path):
    """8 threads fire alternating commands at one enabled machine.

    Every accepted event must fire exactly one transition, the final state
    must be derivable from the log, and the log order must strictly follow
    events.seq; illegal-looking losers are simply rejected (409) without
    corrupting the state.
    """
    db_path = tmp_path / "conc2.db"
    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as setup:
        setup.put(
            "/schemas/command/1.0",
            json={
                "schema": {
                    "type": "object",
                    "required": ["action"],
                    "properties": {"action": {"type": "string"}},
                }
            },
        )
        register_turnstile(setup)
        enable(setup)

    # From "locked" only unlock can succeed; after it, only lock. Fire a
    # mix concurrently: the number of 201s equals the log length and the
    # log reconstructs a valid alternating walk.
    barrier = threading.Barrier(8)
    codes: list[int] = []
    codes_lock = threading.Lock()

    def worker(index):
        action = "unlock" if index % 2 == 0 else "lock"
        with TestClient(app) as local:
            barrier.wait()
            resp = post_event(
                local, action, f"p{index}",
                ts=f"2026-09-22T10:00:{index:02d}Z",
            )
            with codes_lock:
                codes.append(resp.status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with TestClient(app) as check:
        log = check.get("/devices/dev-1/state-machine/transitions?limit=500")
        entries = log.json()["transitions"]
        # Exactly one event can win from locked; the log is a valid walk.
        state = "locked"
        for entry in entries:
            assert entry["from_state"] == state
            state = entry["to_state"]
        assert check.get("/devices/dev-1/state-machine").json()[
            "current_state"
        ] == state
        # Log seqs are strictly increasing and unique.
        seqs = [e["event_seq"] for e in entries]
        assert seqs == sorted(set(seqs))
        assert codes.count(201) == len(entries)
        assert all(code in (201, 409) for code in codes)


def test_concurrent_duplicate_submission_single_winner(client):
    """The same event_id submitted concurrently: one 201, rest 200 replays."""
    register_turnstile(client)
    enable(client)
    barrier = threading.Barrier(6)
    codes: list[int] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        resp = post_event(client, "unlock", "dup")
        with lock:
            codes.append(resp.status_code)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(codes).count(201) == 1
    assert sorted(codes).count(200) == 5
    log = client.get("/devices/dev-1/state-machine/transitions").json()
    assert [t["event_id"] for t in log["transitions"]] == ["dup"]

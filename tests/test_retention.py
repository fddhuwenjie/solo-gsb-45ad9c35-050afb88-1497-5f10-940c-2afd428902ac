"""Tests for the confirmable retention-cleanup flow."""

import asyncio
import hashlib
import sqlite3
import threading
import time
from datetime import datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.main import create_app, canonical_json
from app.retention import plan_digest


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=300)
    with TestClient(app) as c:
        yield c


def seed(client, device_id, events):
    for event_id, ts in events:
        resp = client.post(
            "/events",
            json={
                "device_id": device_id,
                "event_id": event_id,
                "occurred_at": ts,
            },
        )
        assert resp.status_code in (200, 201), resp.text


def make_plan(client, device_id, before, protected_windows=None):
    return client.post(
        "/retention/cleanup-plans",
        json={
            "device_id": device_id,
            "before": before,
            "protected_windows": protected_windows or [],
        },
    )


# ---------------------------------------------------------------- selection

def test_plan_selects_events_before_cutoff_only(client):
    events = [
        ("e1", "2026-09-01T08:00:00Z"),
        ("e2", "2026-09-01T09:00:00Z"),
        ("e3", "2026-09-01T10:00:00Z"),  # exactly at cutoff -> retained
        ("e4", "2026-09-01T11:00:00Z"),
    ]
    seed(client, "dev-1", events)

    resp = make_plan(client, "dev-1", "2026-09-01T10:00:00Z")
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "pending"
    assert body["count"] == 2
    assert [c["event_id"] for c in body["candidates"]] == ["e1", "e2"]
    assert body["confirmed_at"] is None


def test_plan_sorts_candidates_by_occurred_at_then_id(client):
    # Out-of-order insertion and equal timestamps must not change the order.
    seed(
        client,
        "dev-1",
        [
            ("b", "2026-09-01T09:00:00Z"),
            ("z", "2026-09-01T08:00:00Z"),
            ("a", "2026-09-01T08:00:00Z"),
        ],
    )
    body = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    assert [(c["occurred_at"], c["event_id"]) for c in body["candidates"]] == [
        ("2026-09-01T08:00:00Z", "a"),
        ("2026-09-01T08:00:00Z", "z"),
        ("2026-09-01T09:00:00Z", "b"),
    ]


def test_protected_windows_are_half_open(client):
    seed(
        client,
        "dev-1",
        [
            ("at-start", "2026-09-01T08:00:00Z"),  # == start: protected
            ("inside", "2026-09-01T08:30:00Z"),
            ("at-end", "2026-09-01T09:00:00Z"),    # == end: deletable
        ],
    )
    body = make_plan(
        client,
        "dev-1",
        "2026-09-10T00:00:00Z",
        [
            {
                "start": "2026-09-01T08:00:00Z",
                "end": "2026-09-01T09:00:00Z",
            }
        ],
    ).json()
    assert [c["event_id"] for c in body["candidates"]] == ["at-end"]


def test_overlapping_protected_windows_are_unioned(client):
    seed(
        client,
        "dev-1",
        [
            ("e1", "2026-09-01T08:00:00Z"),
            ("e2", "2026-09-01T08:30:00Z"),
            ("e3", "2026-09-01T09:30:00Z"),
        ],
    )
    body = make_plan(
        client,
        "dev-1",
        "2026-09-10T00:00:00Z",
        [
            {"start": "2026-09-01T07:30:00Z", "end": "2026-09-01T08:45:00Z"},
            {"start": "2026-09-01T08:30:00Z", "end": "2026-09-01T10:00:00Z"},
        ],
    ).json()
    assert body["count"] == 0
    assert body["candidates"] == []


def test_plan_is_scoped_to_device(client):
    seed(client, "dev-1", [("e1", "2026-09-01T08:00:00Z")])
    seed(client, "dev-2", [("e2", "2026-09-01T08:00:00Z")])
    body = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    assert [c["event_id"] for c in body["candidates"]] == ["e1"]


def test_empty_protected_window_is_422(client):
    seed(client, "dev-1", [("e1", "2026-09-01T08:00:00Z")])
    resp = make_plan(
        client,
        "dev-1",
        "2026-09-10T00:00:00Z",
        [
            {"start": "2026-09-01T09:00:00Z", "end": "2026-09-01T09:00:00Z"},
        ],
    )
    assert resp.status_code == 422


def test_plan_digest_binds_criteria_and_ids(client):
    seed(
        client,
        "dev-1",
        [
            ("e1", "2026-09-01T08:00:00Z"),
            ("e2", "2026-09-01T09:00:00Z"),
        ],
    )
    cutoff = datetime.fromisoformat("2026-09-01T12:00:00+00:00")
    windows = [
        (
            datetime.fromisoformat("2026-09-01T10:00:00+00:00"),
            datetime.fromisoformat("2026-09-01T11:00:00+00:00"),
        )
    ]
    body = make_plan(
        client,
        "dev-1",
        "2026-09-01T12:00:00Z",
        [
            {"start": "2026-09-01T10:00:00Z", "end": "2026-09-01T11:00:00Z"},
        ],
    ).json()
    expected = plan_digest("dev-1", cutoff, windows, ["e1", "e2"])
    assert body["plan_digest"] == expected

    # A plan for another device with identical timestamps shares the digest
    # computation but yields a different digest because device_id is bound.
    seed(
        client,
        "dev-2",
        [
            ("d2-e1", "2026-09-01T08:00:00Z"),
            ("d2-e2", "2026-09-01T09:00:00Z"),
        ],
    )
    other_device = make_plan(
        client,
        "dev-2",
        "2026-09-01T12:00:00Z",
        [
            {"start": "2026-09-01T10:00:00Z", "end": "2026-09-01T11:00:00Z"},
        ],
    ).json()
    assert other_device["plan_digest"] == plan_digest(
        "dev-2", cutoff, windows, ["d2-e1", "d2-e2"]
    )
    assert other_device["plan_digest"] != body["plan_digest"]


def test_digest_changes_with_criteria_and_candidate_set(client):
    seed(
        client,
        "dev-1",
        [
            ("e1", "2026-09-01T08:00:00Z"),
            ("e2", "2026-09-01T09:00:00Z"),
        ],
    )
    base = make_plan(client, "dev-1", "2026-09-01T12:00:00Z").json()
    other_cutoff = make_plan(client, "dev-1", "2026-09-01T08:30:00Z").json()
    protected = make_plan(
        client,
        "dev-1",
        "2026-09-01T12:00:00Z",
        [{"start": "2026-09-01T08:00:00Z", "end": "2026-09-01T09:00:00Z"}],
    ).json()
    digests = {base["plan_digest"], other_cutoff["plan_digest"], protected["plan_digest"]}
    assert len(digests) == 3
    assert other_cutoff["count"] == 1
    assert [c["event_id"] for c in other_cutoff["candidates"]] == ["e1"]
    assert protected["count"] == 1
    assert [c["event_id"] for c in protected["candidates"]] == ["e2"]


# ------------------------------------------------------------- confirmation

def test_confirm_deletes_exact_batch(client):
    seed(
        client,
        "dev-1",
        [
            ("old", "2026-09-01T08:00:00Z"),
            ("new", "2026-09-20T08:00:00Z"),
        ],
    )
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()

    resp = client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    assert resp.status_code == 200
    confirmed = resp.json()
    assert confirmed["status"] == "confirmed"
    assert confirmed["confirmed_at"] is not None
    assert [c["event_id"] for c in confirmed["candidates"]] == ["old"]

    sessions = client.get("/devices/dev-1/sessions").json()
    assert len(sessions) == 1
    assert [m["event_id"] for m in sessions[0]["members"]] == ["new"]


def test_confirm_is_idempotent_same_result(client):
    seed(
        client,
        "dev-1",
        [
            ("old", "2026-09-01T08:00:00Z"),
            ("new", "2026-09-20T08:00:00Z"),
        ],
    )
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()

    first = client.post(
        f"/retention/cleanup-plans/{plan['plan_id']}/confirm"
    ).json()
    second = client.post(
        f"/retention/cleanup-plans/{plan['plan_id']}/confirm"
    )
    assert second.status_code == 200
    assert second.json() == first

    # Only the old event is gone.
    sessions = client.get("/devices/dev-1/sessions").json()
    assert [m["event_id"] for m in sessions[0]["members"]] == ["new"]


def test_confirm_unknown_plan_is_404(client):
    resp = client.post("/retention/cleanup-plans/deadbeef/confirm")
    assert resp.status_code == 404
    assert client.get("/retention/cleanup-plans/deadbeef").status_code == 404


def test_stale_plan_is_invalidated_in_same_transaction_and_changes_nothing(client):
    seed(
        client,
        "dev-1",
        [
            ("e1", "2026-09-01T08:00:00Z"),
            ("e2", "2026-09-01T09:00:00Z"),
        ],
    )
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    assert plan["count"] == 2

    # A new old event arrives after planning: the candidate set grows.
    seed(client, "dev-1", [("e3", "2026-09-01T07:00:00Z")])

    resp = client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["planned_event_ids"] == ["e1", "e2"]
    assert detail["current_event_ids"] == ["e3", "e1", "e2"]
    assert detail["planned_digest"] != detail["current_digest"]

    # The 409 is persisted in the same write transaction: the plan is now
    # terminally 'invalidated', not merely left pending.
    fetched = client.get(f"/retention/cleanup-plans/{plan['plan_id']}").json()
    assert fetched["status"] == "invalidated"
    sessions = client.get("/devices/dev-1/sessions").json()
    assert sum(s["event_count"] for s in sessions) == 3


def test_invalidated_plan_never_deletes_even_when_candidates_recover(client):
    """Drift permanently invalidates the plan: recovery of the original
    candidate set must not let a later confirmation delete events."""
    seed(
        client,
        "dev-1",
        [
            ("e1", "2026-09-01T08:00:00Z"),
            ("e2", "2026-09-01T09:00:00Z"),
        ],
    )
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    plan_id = plan["plan_id"]

    # Drift by an external deletion: first confirmation invalidates.
    import sqlite3

    db_path = client.app.state.db_path
    ext = sqlite3.connect(db_path)
    ext.execute("DELETE FROM events WHERE event_id = 'e1'")
    ext.commit()
    ext.close()

    assert client.post(f"/retention/cleanup-plans/{plan_id}/confirm").status_code == 409
    assert client.get(f"/retention/cleanup-plans/{plan_id}").json()["status"] == "invalidated"

    # The candidate set "recovers" to the planned snapshot (e1 reinserted
    # via an out-of-band writer). Confirming must still fail and delete
    # nothing -- invalidated is terminal.
    ext = sqlite3.connect(db_path)
    ext.execute(
        "INSERT INTO events (event_id, device_id, occurred_at, "
        "event_type, schema_version, payload) VALUES (?, ?, ?, NULL, NULL, NULL)",
        ("e1", "dev-1", "2026-09-01T08:00:00+00:00"),
    )
    ext.commit()
    ext.close()

    again = client.post(f"/retention/cleanup-plans/{plan_id}/confirm")
    assert again.status_code == 409
    detail = again.json()["detail"]
    assert "invalidated" in detail["message"]
    # The current set now equals the original snapshot again.
    assert detail["current_event_ids"] == ["e1", "e2"]
    assert detail["planned_event_ids"] == ["e1", "e2"]

    assert client.get(f"/retention/cleanup-plans/{plan_id}").json()["status"] == "invalidated"
    sessions = client.get("/devices/dev-1/sessions").json()
    assert [m["event_id"] for s in sessions for m in s["members"]] == ["e1", "e2"]


def test_plan_then_external_deletion_is_409(client, tmp_path):
    seed(client, "dev-1", [("e1", "2026-09-01T08:00:00Z"),
                           ("e2", "2026-09-01T09:00:00Z")])
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()

    # Simulate out-of-band deletion of e1 through the database directly.
    import sqlite3

    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute("DELETE FROM events WHERE event_id = 'e1'")
    conn.commit()
    conn.close()

    resp = client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    assert resp.status_code == 409
    # e2 survived the aborted confirmation.
    sessions = client.get("/devices/dev-1/sessions").json()
    assert [m["event_id"] for m in sessions[0]["members"]] == ["e2"]


def test_confirm_empty_candidate_plan(client):
    seed(client, "dev-1", [("new", "2026-09-20T08:00:00Z")])
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    assert plan["count"] == 0
    resp = client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    assert resp.status_code == 200
    assert resp.json()["status"] == "confirmed"
    assert resp.json()["count"] == 0
    # Replay still fine.
    again = client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    assert again.status_code == 200
    sessions = client.get("/devices/dev-1/sessions").json()
    assert sessions[0]["event_count"] == 1


def test_creation_does_not_delete_anything(client):
    seed(client, "dev-1", [("e1", "2026-09-01T08:00:00Z")])
    make_plan(client, "dev-1", "2026-09-10T00:00:00Z")
    sessions = client.get("/devices/dev-1/sessions").json()
    assert sessions[0]["event_count"] == 1


def test_timezone_offsets_are_normalized(client):
    seed(client, "dev-1", [("e1", "2026-09-01T10:00:00+02:00")])  # 08:00Z
    body = make_plan(client, "dev-1", "2026-09-01T10:00:00+02:00").json()
    # Cutoff equals the event instant -> exclusive -> nothing selected.
    assert body["count"] == 0
    body = make_plan(client, "dev-1", "2026-09-01T12:00:00+02:00").json()
    assert [c["event_id"] for c in body["candidates"]] == ["e1"]


def test_sessions_recomputed_from_remaining_events(client):
    seed(
        client,
        "dev-1",
        [
            ("e1", "2026-09-01T08:00:00Z"),
            ("e2", "2026-09-01T08:01:00Z"),
            ("e3", "2026-09-20T08:00:00Z"),
        ],
    )
    assert len(client.get("/devices/dev-1/sessions").json()) == 2
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    sessions = client.get("/devices/dev-1/sessions").json()
    assert len(sessions) == 1
    assert [m["event_id"] for m in sessions[0]["members"]] == ["e3"]


def test_get_plan_returns_stored_snapshot(client):
    seed(client, "dev-1", [("e1", "2026-09-01T08:00:00Z")])
    created = make_plan(
        client,
        "dev-1",
        "2026-09-10T00:00:00Z",
        [{"start": "2026-09-02T00:00:00Z", "end": "2026-09-03T00:00:00Z"}],
    ).json()
    fetched = client.get(f"/retention/cleanup-plans/{created['plan_id']}").json()
    assert fetched == created


def test_digest_algorithm_reference(client):
    """The digest is a SHA-256 over a documented canonical document."""
    seed(
        client,
        "dev-1",
        [("e1", "2026-09-01T08:00:00Z"), ("e2", "2026-09-01T09:00:00Z")],
    )
    body = make_plan(
        client,
        "dev-1",
        "2026-09-01T10:00:00Z",
        [{"start": "2026-09-01T05:00:00Z", "end": "2026-09-01T06:00:00Z"}],
    ).json()
    document = {
        "device_id": "dev-1",
        "before": "2026-09-01T10:00:00+00:00",
        "protected_windows": [["2026-09-01T05:00:00+00:00", "2026-09-01T06:00:00+00:00"]],
        "event_ids": ["e1", "e2"],
    }
    material = canonical_json(document)
    expected = hashlib.sha256(
        b"device-event-retention-plan:1\n" + material.encode("utf-8")
    ).hexdigest()
    assert body["plan_digest"] == expected


# ------------------------------------------------------------- concurrency
#
# FastAPI runs sync endpoints on an anyio worker thread pool, so "two
# clients hitting confirm at once" really means different threads and
# different sqlite connections. The tests below drive the exact same app
# instance over the ASGI transport with real concurrent requests (the
# TestClient fixture already ran lifespan and created the DB file).


class WinnerGate:
    """Hold the write lock for a moment on the first gated selection.

    The delayed call runs *inside* ``BEGIN IMMEDIATE`` (candidate
    selection happens after the lock is taken), which guarantees a
    second concurrent confirmation has already read the pending plan and
    is blocked on the write lock when the first request commits.
    """

    def __init__(self, delay: float = 0.4):
        self.delay = delay
        self.armed = False
        self.fired = threading.Event()
        self._lock = threading.Lock()

    def arm(self) -> None:
        self.armed = True
        self.fired.clear()

    def install(self, monkeypatch) -> None:
        gate = self
        original_select = main_module.select_candidates

        def gated_select(events, before, windows):
            if gate.armed and gate.fired.is_set() is False:
                with gate._lock:
                    if gate.armed and not gate.fired.is_set():
                        gate.fired.set()
                        time.sleep(gate.delay)
            return original_select(events, before, windows)

        monkeypatch.setattr(main_module, "select_candidates", gated_select)


def _asgi_post(app, path):
    async def request():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            return await http.post(path)

    return request


def test_concurrent_confirmations_both_replay_first_result(client, monkeypatch):
    """Two simultaneous confirms of a still-valid plan are idempotent.

    The request that loses the write-lock race must re-read the plan and
    return the first confirmation's 200 result instead of recomputing
    against the post-deletion state and reporting 409.
    """
    seed(
        client,
        "dev-1",
        [
            ("old", "2026-09-01T08:00:00Z"),
            ("new", "2026-09-20T08:00:00Z"),
        ],
    )
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    url = f"/retention/cleanup-plans/{plan['plan_id']}/confirm"

    gate = WinnerGate()
    gate.install(monkeypatch)
    gate.arm()
    request = _asgi_post(client.app, url)

    async def both():
        return await asyncio.gather(request(), request())

    responses = asyncio.run(both())
    assert gate.fired.is_set()

    statuses = sorted(resp.status_code for resp in responses)
    assert statuses == [200, 200], [(r.status_code, r.text) for r in responses]
    assert responses[0].json() == responses[1].json()
    body = responses[0].json()
    assert body["status"] == "confirmed"
    assert body["confirmed_at"] is not None
    assert [c["event_id"] for c in body["candidates"]] == ["old"]

    # Exactly one batch deleted, once.
    sessions = client.get("/devices/dev-1/sessions").json()
    assert [m["event_id"] for s in sessions for m in s["members"]] == ["new"]

    stored = sqlite3.connect(client.app.state.db_path)
    try:
        row = stored.execute(
            "SELECT status, event_count FROM cleanup_plans WHERE plan_id = ?",
            (plan["plan_id"],),
        ).fetchone()
        assert tuple(row) == ("confirmed", 1)
    finally:
        stored.close()


def test_concurrent_confirmation_with_drift_invalidates_without_deleting(
    client, monkeypatch
):
    """A drifted plan confirmed by two racing requests is invalidated once.

    Setup: an older event landed after planning, so the candidate set has
    already drifted when both confirmations start. The winner observes it
    under the write lock and invalidates the plan; a third connection then
    removes the extra event so the candidate set "recovers" to the planned
    snapshot while the loser is still waiting for the lock. The loser must
    honour the persisted terminal state and return 409 -- it must never
    delete the original batch.
    """
    seed(
        client,
        "dev-1",
        [("e1", "2026-09-01T08:00:00Z"), ("e2", "2026-09-01T09:00:00Z")],
    )
    plan = make_plan(client, "dev-1", "2026-09-10T00:00:00Z").json()
    plan_id = plan["plan_id"]
    url = f"/retention/cleanup-plans/{plan_id}/confirm"
    db_path = client.app.state.db_path

    # Drift is committed before the confirmations, like an event arriving
    # between planning and confirmation.
    drift_conn = sqlite3.connect(db_path)
    drift_conn.execute(
        "INSERT INTO events (event_id, device_id, occurred_at, "
        "event_type, schema_version, payload) "
        "VALUES ('e0', 'dev-1', '2026-09-01T07:00:00+00:00', NULL, NULL, NULL)"
    )
    drift_conn.commit()
    drift_conn.close()

    gate = WinnerGate()
    gate.install(monkeypatch)
    gate.arm()

    def recover_when_invalidated():
        # The winner fires the gate while holding the write lock and
        # recomputing against the drifted set. Wait for it to persist
        # 'invalidated', then make the candidate set match the original
        # plan again (simulating recovery / out-of-band undo).
        assert gate.fired.wait(2.0)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            watcher = sqlite3.connect(db_path, timeout=10)
            try:
                status = watcher.execute(
                    "SELECT status FROM cleanup_plans WHERE plan_id = ?",
                    (plan_id,),
                ).fetchone()[0]
            finally:
                watcher.close()
            if status == "invalidated":
                break
            time.sleep(0.01)
        else:
            raise AssertionError("plan was never invalidated")

        recovery = sqlite3.connect(db_path, timeout=10)
        try:
            recovery.execute("DELETE FROM events WHERE event_id = 'e0'")
            recovery.commit()
        finally:
            recovery.close()

    request = _asgi_post(client.app, url)

    async def all_three():
        return await asyncio.gather(
            request(), request(), asyncio.to_thread(recover_when_invalidated)
        )

    responses = asyncio.run(all_three())
    for resp in responses[:2]:
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert detail["planned_event_ids"] == ["e1", "e2"]

    stored = sqlite3.connect(db_path)
    try:
        status = stored.execute(
            "SELECT status FROM cleanup_plans WHERE plan_id = ?", (plan_id,)
        ).fetchone()[0]
        assert status == "invalidated"
        # The core safety property: the planned batch survives even though
        # the candidate set recovered to the planned snapshot.
        remaining = [
            row[0]
            for row in stored.execute(
                "SELECT event_id FROM events WHERE device_id = 'dev-1' "
                "ORDER BY event_id"
            ).fetchall()
        ]
        assert remaining == ["e1", "e2"]
    finally:
        stored.close()


def test_concurrent_requests_work_across_threadpool_threads(client):
    """Regression for SQLite thread-affinity errors.

    Sync dependencies and endpoints are dispatched on different anyio
    worker threads; a burst of concurrent reads and writes must all
    succeed instead of raising "SQLite objects created in a thread can
    only be used in that same thread".
    """

    async def burst():
        transport = httpx.ASGITransport(app=client.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            async def write(index):
                return await http.post(
                    "/events",
                    json={
                        "device_id": "dev-concurrent",
                        "event_id": f"evt-{index}",
                        "occurred_at": f"2026-09-01T00:{index:02d}:00Z",
                    },
                )

            async def read():
                return await http.get("/devices/dev-concurrent/sessions")

            writes = [write(i) for i in range(12)]
            reads = [read() for _ in range(12)]
            return await asyncio.gather(*writes, *reads)

    responses = asyncio.run(burst())
    write_responses = responses[:12]
    read_responses = responses[12:]
    assert all(resp.status_code == 201 for resp in write_responses), [
        (r.status_code, r.text) for r in write_responses
    ]
    assert all(resp.status_code == 200 for resp in read_responses), [
        (r.status_code, r.text) for r in read_responses
    ]

    sessions = client.get("/devices/dev-concurrent/sessions").json()
    assert sum(s["event_count"] for s in sessions) == 12

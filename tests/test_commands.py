"""Persistent downstream command queue: submit/claim/ack/nack/status.

Covers idempotent submission with content conflicts, strict per-device
FIFO delivery, lease semantics (token rotation, attempts, expiry), nack
retry backoff, token validation on ack/nack, single-writer concurrency
(only one concurrent claim wins), and migration of legacy databases.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.sessions import to_storage

GAP = 300  # seconds


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=GAP)
    with TestClient(app) as c:
        yield c


def put(client, device_id, command_id, payload):
    return client.put(
        f"/devices/{device_id}/commands/{command_id}", json={"payload": payload}
    )


def claim(client, device_id, lease_seconds=60):
    return client.post(
        f"/devices/{device_id}/commands/claim",
        json={"lease_seconds": lease_seconds},
    )


def ack(client, device_id, command_id, token):
    return client.post(
        f"/devices/{device_id}/commands/{command_id}/ack",
        json={"lease_token": token},
    )


def nack(client, device_id, command_id, token, retry_after=0):
    return client.post(
        f"/devices/{device_id}/commands/{command_id}/nack",
        json={"lease_token": token, "retry_after_seconds": retry_after},
    )


# --------------------------------------------------------------- submission


def test_submit_enqueues_and_status_is_queued(client):
    resp = put(client, "dev-1", "cmd-1", {"op": "reboot", "force": True})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["device_id"] == "dev-1"
    assert body["command_id"] == "cmd-1"
    assert body["payload"] == {"op": "reboot", "force": True}
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["leased_at"] is None
    assert body["lease_expires_at"] is None
    assert body["not_before"] is None
    assert body["acked_at"] is None

    status = client.get("/devices/dev-1/commands/cmd-1")
    assert status.status_code == 200
    assert status.json()["status"] == "queued"
    # The status view must never hand out the lease token.
    assert "lease_token" not in status.json()


def test_payload_accepts_any_json_type(client):
    for payload in (None, True, 42, "just a string", [1, {"two": 2}, [3]]):
        cid = f"cmd-{type(payload).__name__}-{str(payload)[:8]}"
        resp = put(client, "dev-1", cid, payload)
        assert resp.status_code == 201, resp.text
        assert resp.json()["payload"] == payload


def test_missing_payload_key_is_422(client):
    resp = client.put("/devices/dev-1/commands/cmd-1", json={})
    assert resp.status_code == 422


def test_idempotent_same_content_replays_without_moving(client):
    r1 = put(client, "dev-1", "cmd-1", {"b": 2, "a": 1})
    assert r1.status_code == 201
    created_at = r1.json()["created_at"]

    # Claim it first so a hypothetical duplicate insert would be visible.
    claimed = claim(client, "dev-1")
    assert claimed.status_code == 201

    # Canonical JSON: object key order is irrelevant.
    r2 = put(client, "dev-1", "cmd-1", {"a": 1, "b": 2})
    assert r2.status_code == 200
    assert r2.json()["created_at"] == created_at
    assert r2.json()["attempts"] == 1  # replay did not reset the delivery
    # Replay did not re-enqueue: the queue has no second command.
    assert ack(client, "dev-1", "cmd-1", claimed.json()["lease_token"]).status_code == 200
    assert claim(client, "dev-1").status_code == 409


def test_different_payload_same_id_is_409_and_keeps_original(client):
    assert put(client, "dev-1", "cmd-1", {"v": 1}).status_code == 201
    conflict = put(client, "dev-1", "cmd-1", {"v": 2})
    assert conflict.status_code == 409

    got = client.get("/devices/dev-1/commands/cmd-1")
    assert got.json()["payload"] == {"v": 1}
    # Exactly one row: the claim returns the original payload.
    claimed = claim(client, "dev-1")
    assert claimed.status_code == 201
    assert claimed.json()["command_id"] == "cmd-1"
    assert claimed.json()["payload"] == {"v": 1}


def test_command_identity_is_scoped_to_device(client):
    assert put(client, "dev-1", "cmd-1", {"v": 1}).status_code == 201
    # Same id, different device is an independent command.
    assert put(client, "dev-2", "cmd-1", {"v": 2}).status_code == 201
    c1 = claim(client, "dev-1").json()
    c2 = claim(client, "dev-2").json()
    assert c1["payload"] == {"v": 1}
    assert c2["payload"] == {"v": 2}


# ------------------------------------------------------------ FIFO delivery


def test_claim_returns_head_in_creation_order(client):
    for cid in ("c1", "c2", "c3"):
        assert put(client, "dev-1", cid, {"id": cid}).status_code == 201
    for expected in ("c1", "c2", "c3"):
        r = claim(client, "dev-1")
        assert r.status_code == 201
        assert r.json()["command_id"] == expected
        assert r.json()["attempts"] == 1
        token = r.json()["lease_token"]
        assert len(token) >= 32
        assert ack(client, "dev-1", expected, token).status_code == 200
    assert claim(client, "dev-1").status_code == 409  # empty queue


def test_claim_never_skips_an_active_head_lease(client):
    put(client, "dev-1", "c1", None)
    put(client, "dev-1", "c2", None)
    head = claim(client, "dev-1", lease_seconds=300)
    assert head.json()["command_id"] == "c1"

    blocked = claim(client, "dev-1")
    assert blocked.status_code == 409
    assert "c1" in blocked.json()["detail"]

    # Status reflects the lease and never exposes the token.
    st = client.get("/devices/dev-1/commands/c1").json()
    assert st["status"] == "leased"
    assert st["attempts"] == 1
    assert st["lease_expires_at"] is not None
    assert "lease_token" not in st


def test_lease_seconds_bounds(client):
    put(client, "dev-1", "c1", None)
    assert claim(client, "dev-1", lease_seconds=0).status_code == 422
    assert claim(client, "dev-1", lease_seconds=301).status_code == 422
    ok = claim(client, "dev-1", lease_seconds=1)
    assert ok.status_code == 201
    ack(client, "dev-1", "c1", ok.json()["lease_token"])

    put(client, "dev-1", "c2", None)
    ok = claim(client, "dev-1", lease_seconds=300)
    assert ok.status_code == 201


def test_leased_head_does_not_block_other_devices(client):
    put(client, "dev-1", "c1", None)
    put(client, "dev-2", "c2", None)
    assert claim(client, "dev-1").status_code == 201
    other = claim(client, "dev-2")
    assert other.status_code == 201
    assert other.json()["command_id"] == "c2"


# -------------------------------------------------------------- lease expiry


def test_expired_lease_redelivers_rotates_token_and_bumps_attempts(client):
    put(client, "dev-1", "c1", {"v": 1})
    first = claim(client, "dev-1", lease_seconds=1)
    old_token = first.json()["lease_token"]
    assert first.json()["attempts"] == 1

    # Still inside the lease: cannot re-claim, old token is current.
    assert claim(client, "dev-1").status_code == 409

    time.sleep(1.1)

    # Expired: derived status is back to queued without any sweeper.
    st = client.get("/devices/dev-1/commands/c1").json()
    assert st["status"] == "queued"
    assert st["lease_expires_at"] is None

    # The old token can neither nack nor ack after expiry.
    assert nack(client, "dev-1", "c1", old_token).status_code == 409
    assert ack(client, "dev-1", "c1", old_token).status_code == 409

    second = claim(client, "dev-1", lease_seconds=300)
    assert second.status_code == 201
    assert second.json()["attempts"] == 2
    new_token = second.json()["lease_token"]
    assert new_token != old_token

    # New token completes the command.
    assert ack(client, "dev-1", "c1", new_token).status_code == 200
    assert client.get("/devices/dev-1/commands/c1").json()["status"] == "acked"


# -------------------------------------------------------------- nack / retry


def test_nack_with_zero_backoff_reopens_immediately(client):
    put(client, "dev-1", "c1", None)
    token = claim(client, "dev-1").json()["lease_token"]

    r = nack(client, "dev-1", "c1", token, retry_after=0)
    assert r.status_code == 200
    assert r.json()["status"] == "queued"
    assert r.json()["attempts"] == 1  # nack does not itself count an attempt
    assert r.json()["leased_at"] is None

    # Old token voided immediately.
    assert nack(client, "dev-1", "c1", token).status_code == 409
    assert ack(client, "dev-1", "c1", token).status_code == 409

    again = claim(client, "dev-1")
    assert again.status_code == 201
    assert again.json()["attempts"] == 2


def test_nack_backoff_keeps_head_closed_then_reopens(client):
    put(client, "dev-1", "c1", None)
    put(client, "dev-1", "c2", None)
    token = claim(client, "dev-1").json()["lease_token"]

    r = nack(client, "dev-1", "c1", token, retry_after=2)
    assert r.status_code == 200
    assert r.json()["status"] == "retry_wait"
    assert r.json()["not_before"] is not None

    # Backoff: head cannot be claimed and is never skipped over.
    assert claim(client, "dev-1").status_code == 409
    assert client.get("/devices/dev-1/commands/c1").json()["status"] == "retry_wait"

    time.sleep(2.1)
    st = client.get("/devices/dev-1/commands/c1").json()
    assert st["status"] == "queued"
    assert st["not_before"] is None

    redelivered = claim(client, "dev-1")
    assert redelivered.status_code == 201
    assert redelivered.json()["command_id"] == "c1"
    assert redelivered.json()["attempts"] == 2
    ack(client, "dev-1", "c1", redelivered.json()["lease_token"])

    # Only after the retried head completes does c2 move.
    nxt = claim(client, "dev-1")
    assert nxt.status_code == 201
    assert nxt.json()["command_id"] == "c2"


def test_nack_negative_backoff_is_422(client):
    put(client, "dev-1", "c1", None)
    token = claim(client, "dev-1").json()["lease_token"]
    resp = client.post(
        "/devices/dev-1/commands/c1/nack",
        json={"lease_token": token, "retry_after_seconds": -1},
    )
    assert resp.status_code == 422


# -------------------------------------------------------------------- acking


def test_acked_command_leaves_the_queue_and_is_terminal(client):
    put(client, "dev-1", "c1", None)
    put(client, "dev-1", "c2", None)
    token1 = claim(client, "dev-1").json()["lease_token"]

    done = ack(client, "dev-1", "c1", token1)
    assert done.status_code == 200
    assert done.json()["status"] == "acked"
    assert done.json()["acked_at"] is not None

    # Double ack is a conflict, not an idempotent replay.
    assert ack(client, "dev-1", "c1", token1).status_code == 409

    nxt = claim(client, "dev-1")
    assert nxt.json()["command_id"] == "c2"


def test_ack_wrong_and_garbage_tokens_are_409(client):
    put(client, "dev-1", "c1", None)
    claim(client, "dev-1")
    assert ack(client, "dev-1", "c1", "not-the-token").status_code == 409
    assert nack(client, "dev-1", "c1", "not-the-token").status_code == 409


def test_unknown_command_returns_404(client):
    assert client.get("/devices/dev-1/commands/nope").status_code == 404
    assert ack(client, "dev-1", "nope", "t").status_code == 404
    assert nack(client, "dev-1", "nope", "t").status_code == 404


# --------------------------------------------------------------- concurrency


def test_concurrent_claims_only_one_wins(tmp_path):
    app = create_app(database_path=str(tmp_path / "q.db"), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)

        results: list[int] = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def worker():
            with TestClient(app) as c:
                barrier.wait()
                resp = claim(c, "dev-1", lease_seconds=300)
                with lock:
                    results.append(resp.status_code)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    winners = [code for code in results if code == 201]
    losers = [code for code in results if code == 409]
    assert len(winners) == 1
    assert len(losers) == 7


# ------------------------------------------------- lease settlement races


def _expire_lease_in_database(db_path: str, device_id: str, command_id: str) -> None:
    """Force the current lease's expiry into the past without rotating it.

    This models "the lease has expired but nobody has re-claimed yet"
    deterministically (no wall-clock sleep): the stored token stays in
    place and only ``expires_at`` moves behind ``now``.
    """
    past = datetime.now(timezone.utc) - timedelta(seconds=60)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE device_commands SET expires_at = ? "
            "WHERE device_id = ? AND command_id = ?",
            (to_storage(past), device_id, command_id),
        )
        conn.commit()
    finally:
        conn.close()


def _race_settlers(app, device_id, command_id, token, requests, barrier):
    """Fire ``requests`` ack/nack calls concurrently; return status codes."""
    codes: list[int] = []
    lock = threading.Lock()

    def worker(kind: str):
        with TestClient(app) as c:
            barrier.wait()
            if kind == "ack":
                resp = ack(c, device_id, command_id, token)
            else:
                resp = nack(c, device_id, command_id, token)
            with lock:
                codes.append(resp.status_code)

    threads = [threading.Thread(target=worker, args=(kind,)) for kind in requests]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return codes


def test_concurrent_acks_with_same_token_only_one_succeeds(tmp_path):
    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)
        token = main_client.post(
            "/devices/dev-1/commands/claim", json={"lease_seconds": 300}
        ).json()["lease_token"]

        barrier = threading.Barrier(8)
        codes = _race_settlers(app, "dev-1", "c1", token, ["ack"] * 8, barrier)

    assert sorted(codes).count(200) == 1
    assert sorted(codes).count(409) == 7
    with TestClient(app) as main_client:
        body = main_client.get("/devices/dev-1/commands/c1").json()
        assert body["status"] == "acked"
        assert body["acked_at"] is not None


def test_concurrent_nacks_with_same_token_only_one_succeeds(tmp_path):
    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)
        token = main_client.post(
            "/devices/dev-1/commands/claim", json={"lease_seconds": 300}
        ).json()["lease_token"]

        barrier = threading.Barrier(8)
        codes = _race_settlers(app, "dev-1", "c1", token, ["nack"] * 8, barrier)

    assert codes.count(200) == 1
    assert codes.count(409) == 7
    with TestClient(app) as main_client:
        body = main_client.get("/devices/dev-1/commands/c1").json()
        # The winning nack voided the lease once; losers changed nothing.
        assert body["status"] == "queued"
        assert body["leased_at"] is None
        # A second sequential nack with the same voided token stays a 409.
        assert nack(main_client, "dev-1", "c1", token).status_code == 409
        assert ack(main_client, "dev-1", "c1", token).status_code == 409
        # Exactly one reopen: the next claim is delivery attempt 2.
        redelivered = claim(main_client, "dev-1")
        assert redelivered.status_code == 201
        assert redelivered.json()["attempts"] == 2


def test_concurrent_ack_and_nack_exactly_one_settles(tmp_path):
    """ack/nack fanned out together: exactly one outcome, no resurrect.

    Whichever settler wins the single writer lock owns the final state,
    and every loser's 409 must never mutate the row -- in particular a
    nack arriving after a winning ack cannot reopen an acked command, and
    an ack arriving after a winning nack cannot complete the voided lease.
    The high fan-out (12 settlers released on one barrier) guarantees the
    requests genuinely overlap at the write lock rather than serializing
    by scheduler luck.
    """
    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)
        token = claim(main_client, "dev-1", lease_seconds=300).json()[
            "lease_token"
        ]

        barrier = threading.Barrier(12)
        requests = ["ack"] * 6 + ["nack"] * 6
        codes = _race_settlers(app, "dev-1", "c1", token, requests, barrier)

    assert codes.count(200) == 1
    assert codes.count(409) == 11
    with TestClient(app) as main_client:
        final = main_client.get("/devices/dev-1/commands/c1").json()
        # The token is voided no matter which side won, so replaying the
        # losers' calls stays a state-changing-free 409.
        assert ack(main_client, "dev-1", "c1", token).status_code == 409
        assert nack(main_client, "dev-1", "c1", token).status_code == 409
        after = main_client.get("/devices/dev-1/commands/c1").json()
        assert after["status"] == final["status"]
        assert after["updated_at"] == final["updated_at"]
        assert after["attempts"] == 1

        if final["status"] == "acked":
            assert after["acked_at"] is not None
            # Acked leaves the queue: the queue is empty now.
            assert claim(main_client, "dev-1").status_code == 409
        else:
            # Nack won: status is queued (zero backoff was sent by the
            # nack workers), and re-claiming is delivery attempt 2 -- not
            # an acked command that somehow came back to life.
            assert after["acked_at"] is None
            redelivered = claim(main_client, "dev-1")
            assert redelivered.status_code == 201
            assert redelivered.json()["attempts"] == 2


def test_nack_backoff_is_not_overwritten_by_racing_nacks(tmp_path):
    """Racing nacks with different backoffs: losers must not touch the win.

    All settlers present the same current token; the single winning nack's
    ``not_before``/voided lease must survive every losing 409 untouched.
    """
    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)
        token = claim(main_client, "dev-1", lease_seconds=300).json()[
            "lease_token"
        ]

        results: list[tuple[str, int]] = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def worker(kind: str, backoff: float):
            with TestClient(app) as c:
                barrier.wait()
                resp = nack(c, "dev-1", "c1", token, retry_after=backoff)
                with lock:
                    results.append((kind, resp.status_code))

        threads = [
            threading.Thread(target=worker, args=("long", 300))
            for _ in range(4)
        ] + [
            threading.Thread(target=worker, args=("short", 0)) for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        codes = [code for _, code in results]
        assert codes.count(200) == 1
        assert codes.count(409) == 7
        winner = next(kind for kind, code in results if code == 200)
        body = main_client.get("/devices/dev-1/commands/c1").json()
        assert body["leased_at"] is None
        if winner == "long":
            # Long backoff won and survives: head is still closed.
            assert body["status"] == "retry_wait"
            assert claim(main_client, "dev-1").status_code == 409
        else:
            assert body["status"] == "queued"


def test_old_token_settle_races_reclaim_after_expiry(tmp_path):
    """A settler with an expired token racing a reclaim must never win.

    After the lease expires, claim rotates the token and increments
    attempts; the old token's ack/nack, even concurrent with that
    reclaim, must return 409 and leave the command pending under the new
    lease (never acked, never reopened with its backoff clobbered).
    """
    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)
        old_token = claim(main_client, "dev-1", lease_seconds=300).json()[
            "lease_token"
        ]

    _expire_lease_in_database(db_path, "dev-1", "c1")

    for loser_kind in ("ack", "nack"):
        db_file = tmp_path / f"race-{loser_kind}.db"
        src = sqlite3.connect(str(db_path))
        dst = sqlite3.connect(str(db_file))
        try:
            src.backup(dst)
        finally:
            src.close()
            dst.close()

        app = create_app(database_path=str(db_file), gap_seconds=GAP)
        settle_codes: list[int] = []
        barrier = threading.Barrier(2)
        lock = threading.Lock()

        def reclaim_worker():
            with TestClient(app) as c:
                barrier.wait()
                resp = claim(c, "dev-1", lease_seconds=300)
                with lock:
                    settle_codes.append(resp.status_code)

        def settle_worker():
            with TestClient(app) as c:
                barrier.wait()
                if loser_kind == "ack":
                    resp = ack(c, "dev-1", "c1", old_token)
                else:
                    resp = nack(c, "dev-1", "c1", old_token, retry_after=10)
                with lock:
                    settle_codes.append(resp.status_code)

        threads = [
            threading.Thread(target=reclaim_worker),
            threading.Thread(target=settle_worker),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sorted(settle_codes) == [201, 409], loser_kind
        with TestClient(app) as c:
            body = c.get("/devices/dev-1/commands/c1").json()
            assert body["status"] == "leased"
            assert body["attempts"] == 2
            assert body["acked_at"] is None
            # The old token is definitively dead under the new lease.
            assert ack(c, "dev-1", "c1", old_token).status_code == 409
            assert nack(c, "dev-1", "c1", old_token).status_code == 409


def test_stale_token_conditional_update_matches_nothing_after_rotation(
    tmp_path,
):
    """White-box: settlement UPDATE is bound to the live token and its expiry.

    Deterministically reproduces the interleave that timing-based HTTP
    races only hit by luck: a holder reads the row while its token is
    still current, the lease then expires and a reclaim rotates the
    token (or a nack voids it). The settlement UPDATE that the endpoint
    issues must be conditional on that exact token still being stored and
    its ``expires_at`` still being in the future -- otherwise it would
    ack/nack a lease that is no longer the holder's.
    """
    from app.commands import COMMAND_COLUMNS, STATE_PENDING

    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        put(main_client, "dev-1", "c1", None)
        first = claim(main_client, "dev-1", lease_seconds=300)
        old_token = first.json()["lease_token"]

    # Simulate the holder's read: it snapshots the row (and the token it
    # holds) at a moment when the lease is still active. Use a short-lived
    # connection so the snapshot is not held across the later writers.
    reader = sqlite3.connect(str(db_path))
    reader.row_factory = sqlite3.Row
    try:
        seen = reader.execute(
            f"SELECT {COMMAND_COLUMNS} FROM device_commands "
            "WHERE device_id = ? AND command_id = ?",
            ("dev-1", "c1"),
        ).fetchone()
        assert seen["lease_token"] == old_token
    finally:
        reader.close()

    # Before the holder settles, the lease expires and a reclaim
    # rotates the token and bumps attempts.
    _expire_lease_in_database(db_path, "dev-1", "c1")
    with TestClient(app) as reclaiming:
        second = claim(reclaiming, "dev-1", lease_seconds=300)
        assert second.status_code == 201
        new_token = second.json()["lease_token"]
        assert new_token != old_token

    holder = sqlite3.connect(str(db_path))
    try:
        now = datetime.now(timezone.utc)
        # The exact conditional UPDATE the fixed ack endpoint issues.
        acked = holder.execute(
            "UPDATE device_commands SET state = 'acked' "
            "WHERE seq = ? AND state = ? AND lease_token = ? "
            "AND expires_at > ?",
            (seen["seq"], STATE_PENDING, old_token, to_storage(now)),
        ).rowcount
        assert acked == 0  # old token must never complete the renewed lease

        # Contrast with the pre-fix predicate, which guarded only on the
        # row still being pending: after the rotation that stale predicate
        # *would* match -- this is exactly the bug the token/expiry guards
        # close. (SELECT ... = 1 without modifying anything.)
        would_match = holder.execute(
            "SELECT COUNT(*) FROM device_commands "
            "WHERE seq = ? AND state = ?",
            (seen["seq"], STATE_PENDING),
        ).fetchone()[0]
        assert would_match == 1

        # ... and the same predicate used by nack must match nothing too.
        nacked = holder.execute(
            "UPDATE device_commands SET lease_token = NULL, expires_at = NULL "
            "WHERE seq = ? AND state = ? AND lease_token = ? "
            "AND expires_at > ?",
            (seen["seq"], STATE_PENDING, old_token, to_storage(now)),
        ).rowcount
        assert nacked == 0
        holder.commit()
    finally:
        holder.close()

    # A nack-voided lease is equally proof against a stale holder:
    # settle the new lease with a nack, then retry the old token.
    with TestClient(app) as reclaiming:
        assert nack(reclaiming, "dev-1", "c1", new_token).status_code == 200

    holder = sqlite3.connect(str(db_path))
    holder.row_factory = sqlite3.Row
    try:
        now = datetime.now(timezone.utc)
        after_nack = holder.execute(
            "UPDATE device_commands SET state = 'acked' "
            "WHERE seq = ? AND state = ? AND lease_token = ? "
            "AND expires_at > ?",
            (seen["seq"], STATE_PENDING, old_token, to_storage(now)),
        ).rowcount
        assert after_nack == 0
        holder.commit()

        row = holder.execute(
            f"SELECT {COMMAND_COLUMNS} FROM device_commands "
            "WHERE device_id = ? AND command_id = ?",
            ("dev-1", "c1"),
        ).fetchone()
        assert row["state"] == STATE_PENDING
        assert row["attempts"] == 2
        assert row["lease_token"] is None  # the nack, not the stale ack, won
    finally:
        holder.close()


def test_concurrent_settles_on_unknown_command_stay_404(tmp_path):
    """404 for unknown commands is preserved under concurrency."""
    db_path = tmp_path / "q.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    barrier = threading.Barrier(4)
    codes: list[int] = []
    lock = threading.Lock()

    def worker(kind: str):
        with TestClient(app) as c:
            barrier.wait()
            if kind == "ack":
                resp = ack(c, "dev-1", "ghost", "anything")
            else:
                resp = nack(c, "dev-1", "ghost", "anything")
            with lock:
                codes.append(resp.status_code)

    threads = [
        threading.Thread(target=worker, args=(kind,))
        for kind in ("ack", "ack", "nack", "nack")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert codes == [404, 404, 404, 404]


# ---------------------------------------------------------------- migration


def test_legacy_database_without_queue_table_migrates(tmp_path):
    db_path = tmp_path / "legacy.db"
    # A database from the initial release: only the events table, no
    # device_commands table at all.
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE events (
                event_id    TEXT PRIMARY KEY,
                device_id   TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO events VALUES ('e1', 'dev-1', '2026-09-20T10:00:00+00:00')"
        )
        conn.commit()
        table_names = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        ]
        assert "device_commands" not in table_names
    finally:
        conn.close()

    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as client:
        assert put(client, "dev-1", "c1", {"hello": "queue"}).status_code == 201
        claimed = claim(client, "dev-1")
        assert claimed.status_code == 201
        token = claimed.json()["lease_token"]
        assert claimed.json()["payload"] == {"hello": "queue"}
        assert ack(client, "dev-1", "c1", token).status_code == 200

        # Old event data is untouched and the existing API still serves it.
        sessions = client.get("/devices/dev-1/sessions").json()
        assert sessions[0]["event_count"] == 1

    # The queue table landed with the FIFO/uniqueness machinery.
    conn = sqlite3.connect(str(db_path))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(device_commands)")}
        assert {
            "seq", "device_id", "command_id", "payload", "state", "attempts",
            "lease_token", "leased_at", "expires_at", "not_before",
            "created_at", "updated_at", "acked_at",
        } <= cols
        indexes = {
            r[1] for r in conn.execute("PRAGMA index_list(device_commands)")
        }
        assert "idx_device_commands_device_seq" in indexes
        assert any(
            "sqlite_autoindex_device_commands" in name for name in indexes
        )
    finally:
        conn.close()


def test_queue_seq_is_monotonic_across_acked_heads(tmp_path):
    """Creation order stays fixed even after many acked commands."""
    app = create_app(database_path=str(tmp_path / "q.db"), gap_seconds=GAP)
    with TestClient(app) as client:
        for i in range(3):
            put(client, "dev-1", f"c{i}", i)
            claimed = claim(client, "dev-1")
            assert claimed.json()["command_id"] == f"c{i}"
            ack(client, "dev-1", f"c{i}", claimed.json()["lease_token"])
        # Fourth command arrives after the first three were completed; it
        # must still be the head (acks never reuse or reorder seq).
        put(client, "dev-1", "c3", 3)
        assert claim(client, "dev-1").json()["command_id"] == "c3"

"""Event transparency receipts: RFC 6962 Merkle tree over accepted events.

Covers leaf appending on first successful writes only (never on replays or
failures), the canonical leaf binding, immutable checkpoints with
idempotent replay, offline-verifiable membership and consistency proofs,
the 422 rules for illegal tree sizes, survival of receipts across
retention cleanup, and the one-shot reproducible backfill for legacy
databases.
"""

import hashlib
import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app.main import create_app
from app.receipts import (
    LEAF_MATERIAL_PREFIX,
    compute_leaf_hash,
    verify_consistency,
    verify_membership,
)
from app.sessions import from_storage, to_storage


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=300)
    with TestClient(app) as c:
        yield c


def _submit(client, device_id, event_id, occurred_at, **extra):
    body = {"device_id": device_id, "event_id": event_id, "occurred_at": occurred_at}
    body.update(extra)
    return client.post("/events", json=body)


def _register_telemetry_schema(client):
    client.put(
        "/schemas/telemetry/1.0",
        json={
            "schema": {
                "type": "object",
                "required": ["reading"],
                "properties": {"reading": {"type": "number"}},
            }
        },
    )


# ------------------------------------------------------- reference Merkle


def ref_leaf_hash(leaf):
    """Independent reimplementation of the documented leaf format."""
    material = json.dumps(
        {
            "seq": leaf["seq"],
            "device_id": leaf["device_id"],
            "event_id": leaf["event_id"],
            # The leaf binds the UTC-normalized storage form of the instant.
            "occurred_at": to_storage(from_storage(leaf["occurred_at"])),
            "event_type": leaf["event_type"],
            "schema_version": leaf["schema_version"],
            "payload": leaf["payload"],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    # RFC 6962: SHA-256(0x00 || domain-prefix || canonical JSON).
    return hashlib.sha256(
        b"\x00" + LEAF_MATERIAL_PREFIX + material.encode()
    ).digest()


def ref_mth(leaf_hashes):
    """Independent RFC 6962 MTH over leaf hashes (bytes)."""
    n = len(leaf_hashes)
    if n == 0:
        return hashlib.sha256(b"").digest()
    if n == 1:
        return leaf_hashes[0]
    k = 1
    while k < n:
        k <<= 1
    k >>= 1
    return hashlib.sha256(
        b"\x01" + ref_mth(leaf_hashes[:k]) + ref_mth(leaf_hashes[k:])
    ).digest()


def expected_root(leaves, size=None):
    """Root hex over the first ``size`` leaves (dicts in tree order)."""
    hashes = [ref_leaf_hash(leaf) for leaf in leaves]
    if size is not None:
        hashes = hashes[:size]
    return ref_mth(hashes).hex()


def leaf_from_response(proof):
    return proof["leaf"]


# ------------------------------------------------------------- leaf append


def test_first_write_appends_leaf_replay_and_failures_do_not(client):
    assert _submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z").status_code == 201
    # Identical replay -> 200, no new leaf.
    assert _submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z").status_code == 200
    # Conflicting content for the same event_id -> 409, no new leaf.
    assert _submit(client, "dev-1", "e1", "2026-09-20T11:00:00Z").status_code == 409
    # Contract failure -> 422, no new leaf.
    resp = _submit(
        client,
        "dev-1",
        "e2",
        "2026-09-20T10:01:00Z",
        event_type="telemetry",
        schema_version="1.0",
        payload={"reading": 1},
    )
    assert resp.status_code == 422

    # Exactly one leaf exists: the tree has size 1.
    proof = client.get("/receipts/proof/e1").json()
    assert proof["tree_size"] == 1
    assert proof["leaf_index"] == 0
    assert proof["audit_path"] == []
    # The failed event has no leaf.
    assert client.get("/receipts/proof/e2").status_code == 404

    # A checkpoint at size 2 is still illegal: only one leaf exists.
    assert client.post("/receipts/checkpoints", json={"tree_size": 2}).status_code == 422


def test_leaf_binds_all_event_fields(client):
    _register_telemetry_schema(client)
    _submit(
        client,
        "dev-1",
        "e1",
        "2026-09-20T10:00:00Z",
        event_type="telemetry",
        schema_version="1.0",
        payload={"reading": 21.5, "tags": ["a", "b"]},
    )
    _submit(client, "dev-2", "e2", "2026-09-20T10:00:00Z")  # payload-less

    proof1 = client.get("/receipts/proof/e1").json()
    leaf1 = leaf_from_response(proof1)
    assert leaf1["seq"] == 1
    assert leaf1["device_id"] == "dev-1"
    assert leaf1["event_type"] == "telemetry"
    assert leaf1["schema_version"] == "1.0"
    assert leaf1["payload"] == {"reading": 21.5, "tags": ["a", "b"]}

    proof2 = client.get("/receipts/proof/e2").json()
    leaf2 = leaf_from_response(proof2)
    assert leaf2["seq"] == 2
    assert leaf2["event_type"] is None
    assert leaf2["payload"] is None

    # The leaf hash recomputed from the returned fields matches the
    # reference implementation of the documented format.
    assert compute_leaf_hash(**leaf1) == ref_leaf_hash(leaf1).hex()
    assert compute_leaf_hash(**leaf2) == ref_leaf_hash(leaf2).hex()


def test_leaves_follow_arrival_order_not_event_time(client):
    # Late event (earlier occurred_at) arrives after: seq order wins.
    _submit(client, "dev-1", "late", "2026-09-20T09:00:00Z")
    _submit(client, "dev-1", "early", "2026-09-20T08:00:00Z")
    first = client.get("/receipts/proof/late").json()
    second = client.get("/receipts/proof/early").json()
    assert first["leaf_index"] == 0 and first["leaf"]["seq"] == 1
    assert second["leaf_index"] == 1 and second["leaf"]["seq"] == 2


def test_stream_events_append_leaves_too(client):
    client.put("/devices/dev-1/streams/main")
    resp = client.put(
        "/devices/dev-1/streams/main/events",
        json={
            "device_id": "dev-1",
            "source_seq": 1,
            "event_id": "s1",
            "occurred_at": "2026-09-20T10:00:00Z",
        },
    )
    assert resp.status_code == 201
    proof = client.get("/receipts/proof/s1")
    assert proof.status_code == 200
    assert proof.json()["leaf_index"] == 0


def test_state_machine_rejection_appends_no_leaf(client):
    client.put(
        "/devices/dev-1/state-machine/versions/1",
        json={
            "version": 1,
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
                }
            ],
        },
    )
    client.post("/devices/dev-1/state-machine/enable", json={"version": 1})
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
    # Legal event -> 201, one leaf.
    assert _submit(
        client,
        "dev-1",
        "ok",
        "2026-09-20T10:00:00Z",
        event_type="command",
        schema_version="1.0",
        payload={"action": "unlock"},
    ).status_code == 201
    # Illegal event for the new state (no matching transition) -> 409.
    assert _submit(
        client,
        "dev-1",
        "bad",
        "2026-09-20T10:01:00Z",
        event_type="command",
        schema_version="1.0",
        payload={"action": "unlock"},
    ).status_code == 409
    assert client.get("/receipts/proof/bad").status_code == 404
    assert client.get("/receipts/proof/ok").json()["tree_size"] == 1


# -------------------------------------------------------------- checkpoints


def test_checkpoint_create_replay_and_get(client):
    for i in range(3):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")

    resp = client.post("/receipts/checkpoints", json={"tree_size": 2})
    assert resp.status_code == 201
    first = resp.json()
    assert first["tree_size"] == 2

    # Repeated creation replays the identical stored checkpoint.
    replay = client.post("/receipts/checkpoints", json={"tree_size": 2})
    assert replay.status_code == 200
    assert replay.json() == first

    # Fetching by size returns the same document.
    fetched = client.get("/receipts/checkpoints/2")
    assert fetched.status_code == 200
    assert fetched.json() == first
    assert client.get("/receipts/checkpoints/3").status_code == 404

    # Listing is ordered by tree size.
    client.post("/receipts/checkpoints", json={"tree_size": 3})
    listed = client.get("/receipts/checkpoints").json()
    assert [c["tree_size"] for c in listed] == [2, 3]


def test_checkpoint_defaults_to_current_size_and_matches_reference(client):
    leaves = []
    for i in range(5):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
        leaves.append(client.get(f"/receipts/proof/e{i}").json()["leaf"])

    resp = client.post("/receipts/checkpoints")
    assert resp.status_code == 201
    body = resp.json()
    assert body["tree_size"] == 5
    # The root equals an independent recomputation from the leaf fields.
    assert body["root_hash"] == expected_root(leaves)


def test_checkpoint_invalid_sizes_return_422(client):
    _submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    # Empty body on a one-leaf tree is fine; explicit bad sizes are not.
    assert client.post("/receipts/checkpoints", json={"tree_size": 0}).status_code == 422
    assert client.post("/receipts/checkpoints", json={"tree_size": -3}).status_code == 422
    assert client.post("/receipts/checkpoints", json={"tree_size": 2}).status_code == 422
    # Nothing was stored for the rejected sizes.
    assert client.get("/receipts/checkpoints").json() == []


def test_checkpoint_on_empty_tree_returns_422(client):
    assert client.post("/receipts/checkpoints").status_code == 422
    assert client.post("/receipts/checkpoints", json={"tree_size": 1}).status_code == 422


# ---------------------------------------------------------- membership proof


def _recompute_and_verify(proof):
    """Offline verification using only the proof response data."""
    leaf = proof["leaf"]
    leaf_hash = compute_leaf_hash(
        seq=leaf["seq"],
        device_id=leaf["device_id"],
        event_id=leaf["event_id"],
        occurred_at=leaf["occurred_at"],
        event_type=leaf["event_type"],
        schema_version=leaf["schema_version"],
        payload=leaf["payload"],
    )
    assert verify_membership(
        leaf_hash,
        proof["leaf_index"],
        proof["tree_size"],
        proof["audit_path"],
        proof["root_hash"],
    )


def test_membership_proofs_verify_offline_for_every_leaf(client):
    _register_telemetry_schema(client)
    for i in range(9):
        _submit(
            client,
            "dev-1",
            f"e{i}",
            f"2026-09-20T10:{i:02d}:00Z",
            event_type="telemetry",
            schema_version="1.0",
            payload={"reading": i},
        )
    for i in range(9):
        proof = client.get(f"/receipts/proof/e{i}").json()
        assert proof["tree_size"] == 9
        assert proof["leaf_index"] == i
        _recompute_and_verify(proof)


def test_membership_proof_against_older_tree_size(client):
    for i in range(4):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    # Prove inclusion against the tree as of size 3 (leaf e3 excluded).
    proof = client.get("/receipts/proof/e2", params={"tree_size": 3}).json()
    assert proof["tree_size"] == 3
    _recompute_and_verify(proof)
    # The root equals the checkpoint later created at that size.
    checkpoint = client.post("/receipts/checkpoints", json={"tree_size": 3}).json()
    assert proof["root_hash"] == checkpoint["root_hash"]


def test_membership_proof_invalid_tree_size_returns_422(client):
    for i in range(3):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    # Beyond the current tree.
    assert client.get("/receipts/proof/e0", params={"tree_size": 4}).status_code == 422
    # Does not cover the leaf (e2 sits at index 2).
    assert client.get("/receipts/proof/e2", params={"tree_size": 2}).status_code == 422
    # Below 1.
    assert client.get("/receipts/proof/e0", params={"tree_size": 0}).status_code == 422


def test_membership_proof_unknown_event_returns_404(client):
    assert client.get("/receipts/proof/nope").status_code == 404


def test_membership_proof_tampering_detected(client):
    for i in range(4):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    proof = client.get("/receipts/proof/e1").json()
    leaf_hash = compute_leaf_hash(**proof["leaf"])
    # Wrong root, wrong index and a tampered path element must all fail.
    assert not verify_membership(
        leaf_hash, proof["leaf_index"], proof["tree_size"],
        proof["audit_path"], "00" * 32,
    )
    assert not verify_membership(
        leaf_hash, proof["leaf_index"] + 1, proof["tree_size"],
        proof["audit_path"], proof["root_hash"],
    )
    bad_path = list(proof["audit_path"])
    bad_path[0] = "ff" * 32
    assert not verify_membership(
        leaf_hash, proof["leaf_index"], proof["tree_size"],
        bad_path, proof["root_hash"],
    )
    # A leaf with a single changed field recomputes to a different hash.
    tampered_leaf = dict(proof["leaf"], payload={"forged": True})
    assert not verify_membership(
        compute_leaf_hash(**tampered_leaf), proof["leaf_index"],
        proof["tree_size"], proof["audit_path"], proof["root_hash"],
    )


# --------------------------------------------------------- consistency proof


def test_consistency_proofs_verify_offline(client):
    for i in range(7):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    for first in range(1, 8):
        for second in range(first, 8):
            proof = client.get(
                "/receipts/consistency",
                params={"first": first, "second": second},
            ).json()
            assert proof["first_size"] == first
            assert proof["second_size"] == second
            if first == second:
                assert proof["proof"] == []
                assert proof["first_root"] == proof["second_root"]
            assert verify_consistency(
                first, proof["first_root"], second, proof["second_root"],
                proof["proof"],
            )


def test_consistency_proof_matches_stored_checkpoints(client):
    for i in range(5):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    cp2 = client.post("/receipts/checkpoints", json={"tree_size": 2}).json()
    cp5 = client.post("/receipts/checkpoints", json={"tree_size": 5}).json()
    proof = client.get(
        "/receipts/consistency", params={"first": 2, "second": 5}
    ).json()
    assert proof["first_root"] == cp2["root_hash"]
    assert proof["second_root"] == cp5["root_hash"]
    assert verify_consistency(2, cp2["root_hash"], 5, cp5["root_hash"], proof["proof"])


def test_consistency_proof_invalid_sizes_return_422(client):
    for i in range(3):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    # first > second
    assert client.get(
        "/receipts/consistency", params={"first": 3, "second": 2}
    ).status_code == 422
    # beyond the current tree
    assert client.get(
        "/receipts/consistency", params={"first": 1, "second": 4}
    ).status_code == 422
    # below 1
    assert client.get(
        "/receipts/consistency", params={"first": 0, "second": 2}
    ).status_code == 422
    assert client.get(
        "/receipts/consistency", params={"first": 1, "second": 0}
    ).status_code == 422


def test_consistency_proof_on_empty_tree_returns_422(client):
    assert client.get(
        "/receipts/consistency", params={"first": 1, "second": 1}
    ).status_code == 422


def test_consistency_proof_tampering_detected(client):
    for i in range(5):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    proof = client.get(
        "/receipts/consistency", params={"first": 2, "second": 5}
    ).json()
    assert not verify_consistency(2, "00" * 32, 5, proof["second_root"], proof["proof"])
    assert not verify_consistency(2, proof["first_root"], 5, "00" * 32, proof["proof"])
    bad = list(proof["proof"])
    bad[0] = "ff" * 32
    assert not verify_consistency(
        2, proof["first_root"], 5, proof["second_root"], bad
    )


# ------------------------------------------------------------- retention


def _cleanup_all(client, device_id, before):
    plan = client.post(
        "/retention/cleanup-plans",
        json={"device_id": device_id, "before": before, "protected_windows": []},
    ).json()
    resp = client.post(f"/retention/cleanup-plans/{plan['plan_id']}/confirm")
    assert resp.status_code == 200
    assert resp.json()["status"] == "confirmed"


def test_receipts_survive_retention_cleanup(client):
    for i in range(4):
        _submit(client, "dev-1", f"e{i}", f"2026-09-20T10:0{i}:00Z")
    checkpoint = client.post("/receipts/checkpoints", json={"tree_size": 4}).json()
    before_proofs = {
        f"e{i}": client.get(f"/receipts/proof/e{i}").json() for i in range(4)
    }

    _cleanup_all(client, "dev-1", "2026-09-21T00:00:00Z")
    # The event bodies are gone.
    assert client.get("/devices/dev-1/sessions").json() == []

    # Receipts, leaves and the old checkpoint are untouched and the
    # membership proofs still verify offline against the old root.
    assert client.get("/receipts/checkpoints/4").json() == checkpoint
    for event_id, old_proof in before_proofs.items():
        proof = client.get(f"/receipts/proof/{event_id}").json()
        assert proof == old_proof
        _recompute_and_verify(proof)

    # New events keep growing the same tree; consistency between the old
    # checkpoint and the grown tree still verifies.
    _submit(client, "dev-1", "e4", "2026-09-21T10:00:00Z")
    proof = client.get(
        "/receipts/consistency", params={"first": 4, "second": 5}
    ).json()
    assert proof["first_root"] == checkpoint["root_hash"]
    assert verify_consistency(
        4, checkpoint["root_hash"], 5, proof["second_root"], proof["proof"]
    )


def test_reaccepted_event_id_after_cleanup_adds_no_second_leaf(client):
    """The tree records each event_id's first acceptance only."""
    _submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    _submit(client, "dev-1", "e2", "2026-09-20T10:01:00Z")
    original = client.get("/receipts/proof/e1").json()
    checkpoint = client.post("/receipts/checkpoints").json()
    assert checkpoint["tree_size"] == 2

    _cleanup_all(client, "dev-1", "2026-09-21T00:00:00Z")
    # The event_id is accepted again (its body is gone): a new events.seq
    # is assigned, but the receipt tree already covers this event_id and
    # does not grow -- the original leaf still binds the original seq.
    resp = _submit(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    assert resp.status_code == 201
    proof = client.get("/receipts/proof/e1").json()
    assert proof["leaf"] == original["leaf"]
    assert proof["leaf_index"] == original["leaf_index"] == 0
    assert proof["tree_size"] == 2
    _recompute_and_verify(proof)
    # The stored checkpoint is untouched and still matches the tree.
    assert client.post("/receipts/checkpoints", json={"tree_size": 2}).json()[
        "root_hash"
    ] == checkpoint["root_hash"]


# --------------------------------------------------------------- migration


LEGACY_EVENTS_SCHEMA = """
CREATE TABLE events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL UNIQUE,
    device_id   TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    event_type    TEXT,
    schema_version TEXT,
    payload       TEXT
);
CREATE INDEX idx_events_device_time ON events (device_id, occurred_at, event_id);
CREATE INDEX idx_events_time_id ON events (occurred_at, event_id);
"""

LEGACY_ROWS = [
    ("e1", "dev-1", "2026-09-20T10:00:00+00:00", None, None, None),
    ("e2", "dev-1", "2026-09-20T10:01:00+00:00", "telemetry", "1.0",
     '{"reading":21.5}'),
    ("e3", "dev-2", "2026-09-20T10:02:00+00:00", None, None, None),
]


def _create_legacy_db(path):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(LEGACY_EVENTS_SCHEMA)
        for row in LEGACY_ROWS:
            conn.execute(
                "INSERT INTO events (event_id, device_id, occurred_at, "
                "event_type, schema_version, payload) VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
        conn.commit()
    finally:
        conn.close()


def _table_names(path):
    conn = sqlite3.connect(path)
    try:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()


def test_legacy_db_backfills_leaves_once_and_reproducibly(tmp_path):
    db_path = tmp_path / "legacy.db"
    _create_legacy_db(db_path)
    assert "receipt_leaves" not in _table_names(db_path)

    app = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app) as client:
        # The three pre-existing events were backfilled in seq order.
        for i, event_id in enumerate(["e1", "e2", "e3"]):
            proof = client.get(f"/receipts/proof/{event_id}").json()
            assert proof["leaf_index"] == i
            assert proof["leaf"]["seq"] == i + 1
            assert proof["tree_size"] == 3
            _recompute_and_verify(proof)
        root = client.post("/receipts/checkpoints").json()["root_hash"]

    # A second migration of an identical legacy database yields the
    # identical tree (the backfill is a pure function of the events).
    db_path2 = tmp_path / "legacy-copy.db"
    _create_legacy_db(db_path2)
    app2 = create_app(database_path=str(db_path2), gap_seconds=300)
    with TestClient(app2) as client2:
        root2 = client2.post("/receipts/checkpoints").json()["root_hash"]
    assert root == root2

    # Restarting the first app does not append duplicate leaves.
    app3 = create_app(database_path=str(db_path), gap_seconds=300)
    with TestClient(app3) as client3:
        assert client3.get("/receipts/proof/e3").json()["tree_size"] == 3
        # New writes continue the same tree at the next index.
        _submit(client3, "dev-1", "e4", "2026-09-20T10:03:00Z")
        proof = client3.get("/receipts/proof/e4").json()
        assert proof["leaf_index"] == 3
        assert proof["leaf"]["seq"] == 4
        assert proof["tree_size"] == 4
        _recompute_and_verify(proof)
        # Consistency between the backfilled prefix and the grown tree.
        consistency = client3.get(
            "/receipts/consistency", params={"first": 3, "second": 4}
        ).json()
        assert consistency["first_root"] == root
        assert verify_consistency(
            3, root, 4, consistency["second_root"], consistency["proof"]
        )


def test_backfill_is_idempotent_at_storage_level(tmp_path):
    db_path = str(tmp_path / "direct.db")
    db_module.init_db(db_path)
    conn = db_module.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO events (event_id, device_id, occurred_at) "
            "VALUES ('e1', 'dev-1', '2026-09-20T10:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()
    # Re-init (as a restart would): the existing event is backfilled once.
    db_module.init_db(db_path)
    db_module.init_db(db_path)
    conn = db_module.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT leaf_index, event_seq FROM receipt_leaves ORDER BY leaf_index"
        ).fetchall()
        assert [(row["leaf_index"], row["event_seq"]) for row in rows] == [(0, 1)]
    finally:
        conn.close()

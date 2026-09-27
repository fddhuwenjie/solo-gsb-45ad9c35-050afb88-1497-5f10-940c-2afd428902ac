"""Transparent event receipts: RFC 6962-style Merkle log over events.

Covers:
* one leaf per first-time write inside the same BEGIN IMMEDIATE transaction
  (idempotent replays and failed requests append nothing),
* RFC 6962 SHA-256 leaf/node prefix rules and the canonical leaf binding
  (seq, device_id, event_id, occurred_at, event_type, schema_version,
  payload),
* immutable checkpoint creation with idempotent replay and 422 for illegal
  tree sizes,
* membership proofs by event_id and consistency proofs between checkpoints,
  recomputed *offline* solely from the returned data,
* retention cleanup deleting event bodies while leaves/proofs/checkpoints
  stay verifiable,
* one-time reproducible backfill when migrating an old database.
"""

import hashlib
import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

LEAF_PREFIX = b"\x00"
NODE_PREFIX = b"\x01"


# ------------------------------------------------- independent RFC 6962 verifier
# This is a deliberately independent, byte-level reimplementation used to
# verify the returned proofs without importing the production tree helpers:
# a proof must be recomputable using only its own payload.


def _node(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(NODE_PREFIX + left + right).digest()


def _split(size: int) -> int:
    # largest power of two strictly smaller than size (size >= 2)
    return 1 << ((size - 1).bit_length() - 1)


def _verify_inclusion(index0: int, size: int, leaf: bytes,
                      path: list[bytes], root: bytes) -> bool:
    """RFC 6962 §2.1.1/§2.1.2 verifier over an inclusion proof.

    Independent of the production verifier: it rebuilds the leaf's own
    subtree first (post-order) and consumes each appended sibling on the
    way up, matching the recursive PATH ordering. Path length varies for
    non-power-of-two sizes (e.g. the last leaf of a 3-leaf tree has one
    sibling, not two).
    """
    nodes = list(path)
    cursor = 0

    def take():
        nonlocal cursor
        value = nodes[cursor]
        cursor += 1
        return value

    def walk(index: int, width: int) -> bytes:
        if width == 1:
            return leaf  # index is subtree-relative on the way down
        k = _split(width)
        if index < k:
            inner = walk(index, k)
            return _node(inner, take())  # right subtree is the sibling
        inner = walk(index - k, width - k)
        return _node(take(), inner)      # left subtree is the sibling

    try:
        rebuilt = walk(index0, size)
    except (IndexError, AssertionError):
        return False
    return cursor == len(nodes) and rebuilt == root


def _verify_consistency(old: int, new: int, old_root: bytes, new_root: bytes,
                        path: list[bytes]) -> bool:
    """Verify a consistency proof using only the path and trusted old root."""
    if old == new:
        return not path and old_root == new_root
    cursor = 0

    def take():
        nonlocal cursor
        node = path[cursor]
        cursor += 1
        return node

    def walk(m: int, n: int, top: bool):
        if m == n:
            raise AssertionError("unreachable")
        k = _split(n)
        if m == k:
            left = old_root if top else take()
            right = take()
            return left, _node(left, right)
        if m < k:
            old_prefix, left_root = walk(m, k, False)
            right = take()
            return old_prefix, _node(left_root, right)
        old_suffix, right_root = walk(m - k, n - k, False)
        left = take()
        return _node(left, old_suffix), _node(left, right_root)

    try:
        rebuilt_old, rebuilt_new = walk(old, new, True)
    except IndexError:
        return False
    return (
        cursor == len(path)
        and rebuilt_old == old_root
        and rebuilt_new == new_root
    )


# ------------------------------------------------------------------- fixtures


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=300)
    with TestClient(app) as c:
        yield c


def _post_event(client, event_id, *, device_id="dev-1",
                ts="2026-09-20T10:00:00Z", payload_event=None):
    body = {"device_id": device_id, "event_id": event_id, "occurred_at": ts}
    if payload_event is not None:
        body.update(payload_event)
    return client.post("/events", json=body)


def _checkpoint(client, size):
    return client.put(f"/receipts/checkpoints/{size}")


def _hexlist(values):
    return [bytes.fromhex(v) for v in values]


def _expected_leaf(data: dict) -> bytes:
    material = json.dumps(
        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(LEAF_PREFIX + material).digest()


# ------------------------------------------------------------ leaf append rules


def test_empty_tree_state(client):
    resp = client.get("/receipts/tree")
    assert resp.status_code == 200
    body = resp.json()
    assert body["tree_size"] == 0
    assert body["root_hash"] is None
    assert body["checkpoint_count"] == 0
    assert client.get("/receipts/checkpoints").json() == []


def test_one_leaf_per_first_time_write(client):
    r1 = _post_event(client, "e1")
    assert r1.status_code == 201
    tree = client.get("/receipts/tree").json()
    assert tree["tree_size"] == 1
    assert tree["root_hash"]

    # Idempotent replay must not append a leaf.
    r2 = _post_event(client, "e1")
    assert r2.status_code == 200
    assert r2.json()["replay"] is True
    tree = client.get("/receipts/tree").json()
    assert tree["tree_size"] == 1


def test_failed_request_appends_no_leaf(client):
    # Register a contract so an invalid payload yields 422 (no event, no leaf).
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["reading"],
        "properties": {"reading": {"type": "integer"}},
    }
    resp = client.put(
        "/schemas/telemetry/1.0", json={"schema": schema}
    )
    assert resp.status_code in (200, 201)

    bad = client.post(
        "/events",
        json={
            "device_id": "dev-1",
            "event_id": "bad",
            "occurred_at": "2026-09-20T10:00:00Z",
            "event_type": "telemetry",
            "schema_version": "1.0",
            "payload": {"reading": "not-an-int"},
        },
    )
    assert bad.status_code == 422
    assert client.get("/receipts/tree").json()["tree_size"] == 0

    # A conflicting event_id (409) also leaves no leaf.
    _post_event(client, "dup")
    conflict = _post_event(client, "dup", ts="2026-09-20T10:00:01Z")
    assert conflict.status_code == 409
    assert client.get("/receipts/tree").json()["tree_size"] == 1


def test_leaf_binds_canonical_full_event(client):
    payload_event = {
        "event_type": "telemetry",
        "schema_version": "1.0",
        "payload": {"reading": 21, "tags": ["a", "b"]},
    }
    # register the schema
    client.put(
        "/schemas/telemetry/1.0",
        json={
            "schema": {
                "type": "object",
                "properties": {"reading": {"type": "number"},
                               "tags": {"type": "array"}},
            }
        },
    )
    assert _post_event(client, "e1", payload_event=payload_event).status_code == 201
    proof = client.get("/events/e1/receipt").json()
    data = proof["leaf_data"]
    assert data == {
        "seq": 1,
        "device_id": "dev-1",
        "event_id": "e1",
        "occurred_at": "2026-09-20T10:00:00+00:00",
        "event_type": "telemetry",
        "schema_version": "1.0",
        "payload": {"reading": 21, "tags": ["a", "b"]},
    }
    # Leaf hash follows the 0x00 prefix rule over canonical JSON.
    assert bytes.fromhex(proof["leaf_hash"]) == _expected_leaf(data)
    assert proof["event_seq"] == 1
    assert proof["leaf_index"] == 1

    # Key-order variation on payload does not change the canonical leaf.
    client.post(
        "/events",
        json={
            "device_id": "dev-2",
            "event_id": "e2",
            "occurred_at": "2026-09-20T10:02:00Z",
            "event_type": "telemetry",
            "schema_version": "1.0",
            "payload": {"tags": ["a", "b"], "reading": 21},
        },
    )
    data2 = client.get("/events/e2/receipt").json()["leaf_data"]
    assert data2["payload"] == {"reading": 21, "tags": ["a", "b"]}


def test_leaf_indexes_are_dense_in_arrival_order(client):
    for i in range(1, 6):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:0{i}:00Z")
    tree = client.get("/receipts/tree").json()
    assert tree["tree_size"] == 5
    for i in range(1, 6):
        proof = client.get(f"/events/e{i}/receipt").json()
        assert proof["leaf_index"] == i
        assert proof["event_seq"] == i


# ------------------------------------------------------------------ checkpoints


def test_create_immutable_checkpoint_and_replay(client):
    for i in range(1, 4):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:0{i}:00Z")
    r1 = _checkpoint(client, 3)
    assert r1.status_code == 201
    first = r1.json()
    assert first["tree_size"] == 3
    assert len(first["root_hash"]) == 64

    r2 = _checkpoint(client, 3)
    assert r2.status_code == 200
    second = r2.json()
    assert second["root_hash"] == first["root_hash"]
    assert second["created_at"] == first["created_at"]

    listing = client.get("/receipts/checkpoints").json()
    assert [c["tree_size"] for c in listing] == [3]
    single = client.get("/receipts/checkpoints/3")
    assert single.json()["root_hash"] == first["root_hash"]


def test_checkpoint_illegal_tree_size_returns_422(client):
    _post_event(client, "e1")
    # zero and future sizes are 422
    assert client.put("/receipts/checkpoints/0").status_code == 422
    resp = client.put("/receipts/checkpoints/5")
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["reason"] == "invalid_tree_size"
    assert detail["tree_size"] == 5
    assert detail["current_tree_size"] == 1
    # nothing was stored
    assert client.get("/receipts/checkpoints").json() == []


def test_get_unknown_checkpoint_404(client):
    assert client.get("/receipts/checkpoints/7").status_code == 404


# --------------------------------------------------------- inclusion: offline


def test_inclusion_proof_verifies_offline_for_all_leaves(client):
    n = 7
    for i in range(1, n + 1):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:{i:02d}:00Z")
    cp = _checkpoint(client, n).json()
    root = bytes.fromhex(cp["root_hash"])
    for i in range(1, n + 1):
        proof = client.get(f"/events/e{i}/receipt?tree_size={n}").json()
        assert proof["tree_size"] == n
        assert proof["checkpoint"]["root_hash"] == cp["root_hash"]
        leaf = _expected_leaf(proof["leaf_data"])
        assert bytes.fromhex(proof["leaf_hash"]) == leaf
        assert _verify_inclusion(
            proof["leaf_index"] - 1, n, leaf, _hexlist(proof["audit_path"]), root
        )
        # Tampering any node must fail.
        if proof["audit_path"]:
            tampered = list(proof["audit_path"])
            tampered[0] = "00" * 32
            assert not _verify_inclusion(
                proof["leaf_index"] - 1, n, leaf, _hexlist(tampered), root
            )
        # A different leaf cannot be proven at this index.
        other = client.get(f"/events/e{1 if i != 1 else 2}/receipt").json()
        other_leaf = _expected_leaf(other["leaf_data"])
        assert not _verify_inclusion(
            proof["leaf_index"] - 1, n, other_leaf,
            _hexlist(proof["audit_path"]), root
        )


def test_inclusion_proof_defaults_to_current_size_and_works_at_one(client):
    _post_event(client, "only")
    proof = client.get("/events/only/receipt").json()
    assert proof["tree_size"] == 1
    assert proof["audit_path"] == []
    leaf = _expected_leaf(proof["leaf_data"])
    assert _verify_inclusion(0, 1, leaf, [], bytes.fromhex(proof["root_hash"]))
    assert proof["checkpoint"] is None


def test_inclusion_at_prior_size_and_422_for_predating_leaf(client):
    for i in range(1, 4):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:0{i}:00Z")
    # e3 (leaf 3) cannot be proven at size 2.
    resp = client.get("/events/e3/receipt?tree_size=2")
    assert resp.status_code == 422
    assert resp.json()["detail"]["reason"] == "leaf_not_in_tree"
    # beyond current size is 422
    assert client.get("/events/e1/receipt?tree_size=9").status_code == 422
    assert client.get("/events/e1/receipt?tree_size=0").status_code == 422
    # valid earlier size
    proof = client.get("/events/e1/receipt?tree_size=2").json()
    assert proof["tree_size"] == 2
    cp2 = _checkpoint(client, 2).json()
    assert _verify_inclusion(
        0, 2, _expected_leaf(proof["leaf_data"]),
        _hexlist(proof["audit_path"]), bytes.fromhex(cp2["root_hash"])
    )


def test_inclusion_unknown_event_404(client):
    assert client.get("/events/ghost/receipt").status_code == 404


# -------------------------------------------------------- consistency: offline


def test_consistency_proof_verifies_offline(client):
    for i in range(1, 8):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:{i:02d}:00Z")
    roots = {}
    for size in range(1, 8):
        roots[size] = _checkpoint(client, size).json()["root_hash"]
    for first in range(1, 8):
        for second in range(first, 8):
            resp = client.get(
                f"/receipts/consistency/{first}/{second}"
            )
            assert resp.status_code == 200, (first, second, resp.text)
            body = resp.json()
            assert body["first_root_hash"] == roots[first]
            assert body["second_root_hash"] == roots[second]
            assert _verify_consistency(
                first, second,
                bytes.fromhex(roots[first]), bytes.fromhex(roots[second]),
                _hexlist(body["consistency_path"]),
            )
            if first == second:
                assert body["consistency_path"] == []
            # A forged trusted root must fail.
            if first != second:
                assert not _verify_consistency(
                    first, second, b"\x00" * 32,
                    bytes.fromhex(roots[second]),
                    _hexlist(body["consistency_path"]),
                )


def test_consistency_requires_existing_checkpoints_and_order(client):
    for i in range(1, 4):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:0{i}:00Z")
    _checkpoint(client, 1)
    _checkpoint(client, 3)
    # size 2 checkpoint does not exist -> 404
    assert client.get("/receipts/consistency/1/2").status_code == 404
    assert client.get("/receipts/consistency/2/3").status_code == 404
    # reversed order: path ge=1; 3 > 1 -> 422
    resp = client.get("/receipts/consistency/3/1")
    assert resp.status_code == 422
    assert resp.json()["detail"]["reason"] == "invalid_tree_size"
    # equal valid sizes is fine
    assert client.get("/receipts/consistency/1/1").status_code == 200
    # unknown (beyond tree) checkpoint -> 404
    assert client.get("/receipts/consistency/1/99").status_code == 404


# -------------------------------------------------------------- retention survival


def test_retention_cleanup_keeps_leaves_proofs_and_checkpoints(client):
    # 4 events, then a cleanup deleting the first two.
    for i in range(1, 5):
        _post_event(client, f"e{i}", ts=f"2026-09-20T10:0{i}:00Z")
    cp3 = _checkpoint(client, 3).json()
    root3 = bytes.fromhex(cp3["root_hash"])

    plan = client.post(
        "/retention/cleanup-plans",
        json={
            "device_id": "dev-1",
            "before": "2026-09-20T10:03:00Z",
            "protected_windows": [],
        },
    ).json()
    assert plan["count"] == 2
    confirmed = client.post(
        f"/retention/cleanup-plans/{plan['plan_id']}/confirm"
    )
    assert confirmed.status_code == 200

    # Event bodies are gone, but the leaf/membership proof remains and the
    # old checkpoint root still verifies the deleted event offline.
    remaining = {
        e["event_id"]
        for e in client.post("/events/search", json={}).json()["events"]
    }
    assert remaining == {"e3", "e4"}
    proof = client.get("/events/e1/receipt?tree_size=3").json()
    assert _verify_inclusion(
        proof["leaf_index"] - 1, 3, _expected_leaf(proof["leaf_data"]),
        _hexlist(proof["audit_path"]), root3
    )
    # The old checkpoint row itself is untouched.
    got = client.get("/receipts/checkpoints/3").json()
    assert got["root_hash"] == cp3["root_hash"]
    assert got["created_at"] == cp3["created_at"]

    # A new event extends the log; consistency from the pre-cleanup
    # checkpoint to a new one still verifies.
    _post_event(client, "e5", ts="2026-09-20T11:00:00Z")
    cp5 = _checkpoint(client, 5).json()
    cons = client.get("/receipts/consistency/3/5").json()
    assert _verify_consistency(
        3, 5, root3, bytes.fromhex(cp5["root_hash"]),
        _hexlist(cons["consistency_path"])
    )


# ------------------------------------------------------------- migration backfill


def test_backfill_from_legacy_db_is_complete_and_reproducible(tmp_path):
    schema = """
    CREATE TABLE events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        device_id TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        event_type TEXT, schema_version TEXT, payload TEXT
    );
    CREATE INDEX idx_events_device_time ON events (device_id, occurred_at, event_id);
    CREATE INDEX idx_events_time_id ON events (occurred_at, event_id);
    """

    def build(path):
        conn = sqlite3.connect(path)
        conn.executescript(schema)
        ts = "2026-09-20T10:00:00+00:00"
        for eid in ("a", "b", "c"):
            conn.execute(
                "INSERT INTO events (event_id,device_id,occurred_at,"
                "event_type,schema_version,payload) VALUES (?,?,?,?,?,?)",
                (eid, "d1", ts, None, None, None),
            )
        conn.commit()
        conn.close()

    p1 = tmp_path / "a.db"
    p2 = tmp_path / "b.db"
    build(str(p1))
    build(str(p2))

    def root_of(path):
        app = create_app(database_path=str(path))
        with TestClient(app) as c:
            return c.get("/receipts/tree").json()

    t1 = root_of(p1)
    t2 = root_of(p2)
    assert t1["tree_size"] == t2["tree_size"] == 3
    # Same columns/order => byte-identical root regardless of which DB file.
    assert t1["root_hash"] == t2["root_hash"]

    # Leaves can be proven immediately after migration.
    app = create_app(database_path=str(p1))
    with TestClient(app) as c:
        proof = c.get("/events/a/receipt").json()
        assert proof["leaf_index"] == 1
        cp = c.put("/receipts/checkpoints/3").json()
        assert _verify_inclusion(
            proof["leaf_index"] - 1, 3, _expected_leaf(proof["leaf_data"]),
            _hexlist(proof["audit_path"]), bytes.fromhex(cp["root_hash"])
        )
        # A post-migration write continues the dense leaf numbering.
        r = c.post(
            "/events",
            json={"device_id": "d1", "event_id": "d",
                  "occurred_at": "2026-09-20T10:05:00Z"},
        )
        assert r.status_code == 201
        assert c.get("/events/d/receipt").json()["leaf_index"] == 4
        assert c.get("/receipts/tree").json()["tree_size"] == 4


def test_backfill_handles_pre_module_deleted_event_gap(tmp_path):
    """An old DB that deleted a high event before receipts leaves a seq gap;
    backfill still produces a dense, complete Merkle log."""
    path = tmp_path / "gapped.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE, device_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            event_type TEXT, schema_version TEXT, payload TEXT);
        """
    )
    ts = "2026-09-20T10:00:00+00:00"
    for eid in ("a", "b", "gone", "c"):
        conn.execute(
            "INSERT INTO events (event_id,device_id,occurred_at,"
            "event_type,schema_version,payload) VALUES (?,?,?,?,?,?)",
            (eid, "d1", ts, None, None, None),
        )
    conn.execute("DELETE FROM events WHERE event_id='gone'")  # frees no seq
    conn.commit()
    conn.close()

    app = create_app(database_path=str(path))
    with TestClient(app) as c:
        tree = c.get("/receipts/tree").json()
        assert tree["tree_size"] == 3
        # leaf positions dense; event_seq retains the original gapped values
        c_idx = c.get("/events/c/receipt").json()
        assert c_idx["leaf_index"] == 3
        assert c_idx["event_seq"] == 4
        cp = c.put("/receipts/checkpoints/3").json()
        proof = c.get("/events/c/receipt").json()
        assert _verify_inclusion(
            2, 3, _expected_leaf(proof["leaf_data"]),
            _hexlist(proof["audit_path"]), bytes.fromhex(cp["root_hash"])
        )


def test_init_db_backfill_is_idempotent(tmp_path):
    path = tmp_path / "idemp.db"
    app = create_app(database_path=str(path))
    with TestClient(app) as c:
        _post_event(c, "x")
        assert c.get("/receipts/tree").json()["tree_size"] == 1
    # Restarting must not duplicate or re-backfill leaves.
    app2 = create_app(database_path=str(path))
    with TestClient(app2) as c:
        assert c.get("/receipts/tree").json()["tree_size"] == 1
        _post_event(c, "y", ts="2026-09-20T10:01:00Z")
        assert c.get("/events/y/receipt").json()["leaf_index"] == 2


def test_stream_events_also_grow_the_receipt_log(client):
    client.put("/devices/dev-1/streams/main")
    body = {
        "device_id": "dev-1",
        "source_seq": 1,
        "event_id": "s1",
        "occurred_at": "2026-09-20T10:00:00Z",
    }
    r = client.put("/devices/dev-1/streams/main/events", json=body)
    assert r.status_code == 201
    assert client.get("/receipts/tree").json()["tree_size"] == 1
    # replay adds no leaf
    assert client.put(
        "/devices/dev-1/streams/main/events", json=body
    ).status_code == 200
    assert client.get("/receipts/tree").json()["tree_size"] == 1

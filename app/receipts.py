"""Event transparency receipts: an append-only RFC 6962 Merkle tree.

Every event that is *successfully written for the first time* appends one
Merkle leaf -- inside the same ``BEGIN IMMEDIATE`` transaction as the event
insert -- ordered by the persistent arrival sequence ``events.seq``. The
canonical leaf material binds ``seq``, ``device_id``, ``event_id``,
``occurred_at``, ``event_type``, ``schema_version`` and ``payload``, so the
leaf hash commits to the exact stored event. Idempotent replays and failed
requests append nothing (a failure rolls the whole transaction back).

Hashing follows RFC 6962 (section 2.1): a leaf is
``SHA-256(0x00 || material)`` and an interior node is
``SHA-256(0x01 || left || right)``; the tree of ``n`` leaves splits at
``k``, the largest power of two strictly smaller than ``n``. On top of the
tree the module provides:

* immutable checkpoints: ``(tree_size, root_hash)`` pairs, stored once and
  replayed verbatim on repeat creation;
* membership (inclusion) proofs by ``event_id``;
* consistency proofs between two tree sizes.

Both proof kinds are verifiable *offline* from the response data alone:
:func:`compute_leaf_hash`, :func:`verify_membership` and
:func:`verify_consistency` are the exact reference procedures a client can
re-run without any server state.

The leaves and checkpoints live in their own tables with no foreign key to
``events``: retention cleanup deletes only event bodies, while every
receipt, leaf and old checkpoint remains verifiable. Databases created
before this module are backfilled once at startup in ``events.seq`` order
(:func:`backfill_leaves`); the result depends only on the stored events and
is therefore reproducible.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from .ingestion import canonical_json
from .sessions import from_storage, to_storage

# Full column lists; kept here so every SELECT reads a stable shape.
LEAF_COLUMNS = (
    "leaf_index, event_seq, event_id, device_id, occurred_at, event_type, "
    "schema_version, payload, leaf_hash, created_at"
)
CHECKPOINT_COLUMNS = "tree_size, root_hash, created_at"

# Domain-separation prefix for the canonical leaf material; the version
# suffix is part of the wire algorithm documented in the README.
LEAF_MATERIAL_PREFIX = b"device-event-receipt-leaf:1\n"


class ReceiptError(Exception):
    """An invalid tree size was requested (HTTP 422 semantics).

    ``message`` is the exact response detail message that the route
    re-raises as ``422`` after rolling back the transaction.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class MembershipProof:
    """Everything needed to verify one leaf's inclusion offline."""

    leaf: sqlite3.Row
    tree_size: int
    root_hash: str
    audit_path: tuple[str, ...]


@dataclass(frozen=True)
class ConsistencyProof:
    """Everything needed to verify append-only growth between two sizes."""

    first_size: int
    first_root: str
    second_size: int
    second_root: str
    proof: tuple[str, ...]


# ------------------------------------------------------ RFC 6962 primitives


def hash_leaf_material(material: bytes) -> bytes:
    """RFC 6962 leaf hash: ``SHA-256(0x00 || material)``."""
    return hashlib.sha256(b"\x00" + material).digest()


def hash_node(left: bytes, right: bytes) -> bytes:
    """RFC 6962 interior node hash: ``SHA-256(0x01 || left || right)``."""
    return hashlib.sha256(b"\x01" + left + right).digest()


def _split(n: int) -> int:
    """Largest power of two strictly smaller than ``n`` (``n >= 2``)."""
    k = 1
    while k < n:
        k <<= 1
    return k >> 1


def leaf_material(
    *,
    event_seq: int,
    device_id: str,
    event_id: str,
    occurred_at: str,
    event_type: str | None,
    schema_version: str | None,
    payload: str | None,
) -> bytes:
    """Canonical leaf material binding every receipt field.

    ``occurred_at`` is the stored UTC ISO-8601 string and ``payload`` the
    stored canonical JSON text (or ``None`` for a payload-less event); the
    material is the versioned domain prefix followed by canonical JSON, so
    byte-identical events always hash to the same leaf.
    """
    material = canonical_json(
        {
            "seq": int(event_seq),
            "device_id": device_id,
            "event_id": event_id,
            "occurred_at": occurred_at,
            "event_type": event_type,
            "schema_version": schema_version,
            "payload": json.loads(payload) if payload is not None else None,
        }
    )
    return LEAF_MATERIAL_PREFIX + material.encode("utf-8")


def _mth(hashes: Sequence[bytes]) -> bytes:
    """Merkle Tree Hash of already-hashed leaves (RFC 6962 section 2.1)."""
    n = len(hashes)
    if n == 0:
        # The empty tree hashes to SHA-256 of the empty input.
        return hashlib.sha256(b"").digest()
    if n == 1:
        return hashes[0]
    k = _split(n)
    return hash_node(_mth(hashes[:k]), _mth(hashes[k:]))


def _audit_path(hashes: Sequence[bytes], m: int) -> list[bytes]:
    """Sibling hashes proving inclusion of leaf ``m`` (0-based)."""
    n = len(hashes)
    if n == 1:
        return []
    k = _split(n)
    if m < k:
        return _audit_path(hashes[:k], m) + [_mth(hashes[k:])]
    return _audit_path(hashes[k:], m - k) + [_mth(hashes[:k])]


def _subproof(hashes: Sequence[bytes], m: int, n: int, complete: bool) -> list[bytes]:
    """RFC 6962 SUBPROOF(m, D[n], complete); ``hashes`` covers D[0:n]."""
    if m == n:
        return [] if complete else [_mth(hashes[:m])]
    k = _split(n)
    if m <= k:
        return _subproof(hashes[:k], m, k, complete) + [_mth(hashes[k:])]
    return _subproof(hashes[k:], m - k, n - k, False) + [_mth(hashes[:k])]


# ------------------------------------------------------- offline verifiers


def compute_leaf_hash(
    *,
    seq: int,
    device_id: str,
    event_id: str,
    occurred_at: Any,
    event_type: str | None,
    schema_version: str | None,
    payload: Any,
) -> str:
    """Recompute a leaf hash (hex) from the receipt fields alone.

    ``occurred_at`` accepts a ``datetime`` or any ISO-8601 string (the
    API returns ``Z``-suffixed UTC); it is normalized to the stored UTC
    ISO-8601 form before hashing, so equivalent spellings of the same
    instant hash identically. ``payload`` is the parsed JSON value
    exactly as returned by the API (``None`` for a payload-less event).
    This is the client-side half of the leaf format: prefix + canonical
    JSON, then the RFC 6962 leaf hash.
    """
    if isinstance(occurred_at, datetime):
        occurred_text = to_storage(occurred_at)
    else:
        occurred_text = to_storage(from_storage(str(occurred_at)))
    material = canonical_json(
        {
            "seq": int(seq),
            "device_id": device_id,
            "event_id": event_id,
            "occurred_at": occurred_text,
            "event_type": event_type,
            "schema_version": schema_version,
            "payload": payload,
        }
    )
    return hash_leaf_material(LEAF_MATERIAL_PREFIX + material.encode("utf-8")).hex()


def verify_membership(
    leaf_hash: str,
    leaf_index: int,
    tree_size: int,
    audit_path: Sequence[str],
    root_hash: str,
) -> bool:
    """Verify an inclusion proof, RFC 6962 section 2.1.1.

    Folds the leaf hash with the audit path, choosing the hash order from
    the leaf index and tree size; succeeds iff the fold reproduces
    ``root_hash``. Pure function of the returned proof data.
    """
    if leaf_index < 0 or tree_size < 1 or leaf_index >= tree_size:
        return False
    fn = leaf_index
    sn = tree_size - 1
    try:
        r = bytes.fromhex(leaf_hash)
        path = [bytes.fromhex(p) for p in audit_path]
        expected = bytes.fromhex(root_hash)
    except ValueError:
        return False
    for p in path:
        if (fn & 1) or fn == sn:
            r = hash_node(p, r)
        else:
            r = hash_node(r, p)
        fn >>= 1
        sn >>= 1
    return r == expected


def _proof_layout(m: int, n: int) -> list[tuple[int, int]]:
    """``(start, size)`` of every subtree hash in PROOF(m, D[n]).

    The layout depends only on the two tree sizes, so a verifier can
    reconstruct exactly which subtree each returned proof hash covers.
    """
    layout: list[tuple[int, int]] = []

    def walk(m: int, n: int, complete: bool, base: int) -> None:
        if m == n:
            if not complete:
                layout.append((base, m))
            return
        k = _split(n)
        if m <= k:
            walk(m, k, complete, base)
            layout.append((base + k, n - k))
        else:
            walk(m - k, n - k, False, base + k)
            layout.append((base, k))

    walk(m, n, True, 0)
    return layout


def _assemble_root(size: int, available: dict[tuple[int, int], bytes]) -> bytes | None:
    """Recompute MTH(D[0:size]) from known subtree hashes, or ``None``.

    Mirrors the MTH definition recursively; any subtree hash not present
    in ``available`` is recomputed from its children, and a missing leaf
    makes the whole assembly fail (``None``).
    """
    memo: dict[tuple[int, int], bytes | None] = {}

    def rec(start: int, length: int) -> bytes | None:
        key = (start, length)
        if key in available:
            return available[key]
        if key in memo:
            return memo[key]
        if length == 1:
            memo[key] = None
            return None
        k = _split(length)
        left = rec(start, k)
        right = rec(start + k, length - k)
        memo[key] = hash_node(left, right) if left is not None and right is not None else None
        return memo[key]

    return rec(0, size)


def verify_consistency(
    first_size: int,
    first_root: str,
    second_size: int,
    second_root: str,
    proof: Sequence[str],
) -> bool:
    """Verify a consistency proof between two tree sizes.

    The proof layout (which subtree each hash covers) is recomputed from
    the two sizes alone; both roots are then reassembled from the proof
    hashes (plus ``first_root`` itself when the first tree is a perfect
    power of two, in which case the proof contains nothing for it) and
    must match the returned roots. Pure function of the response data.
    """
    if first_size < 1 or second_size < 1 or first_size > second_size:
        return False
    if first_size == second_size:
        return len(proof) == 0 and first_root == second_root
    layout = _proof_layout(first_size, second_size)
    if len(layout) != len(proof):
        return False
    try:
        available = {
            key: bytes.fromhex(value) for key, value in zip(layout, proof)
        }
        expected_first = bytes.fromhex(first_root)
        expected_second = bytes.fromhex(second_root)
    except ValueError:
        return False
    if first_size & (first_size - 1) == 0:
        # A perfect first tree contributes no proof hash of its own; its
        # root is the only subtree hash the second tree's assembly needs.
        available[(0, first_size)] = expected_first
    return (
        _assemble_root(first_size, available) == expected_first
        and _assemble_root(second_size, available) == expected_second
    )


# ------------------------------------------------------------------ SQL: read


def leaf_count(conn: sqlite3.Connection) -> int:
    """Current number of leaves == the current Merkle tree size."""
    return int(
        conn.execute("SELECT COUNT(*) AS c FROM receipt_leaves").fetchone()["c"]
    )


def get_leaf_by_event(conn: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {LEAF_COLUMNS} FROM receipt_leaves WHERE event_id = ?",
        (event_id,),
    ).fetchone()


def get_checkpoint(conn: sqlite3.Connection, tree_size: int) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {CHECKPOINT_COLUMNS} FROM receipt_checkpoints "
        "WHERE tree_size = ?",
        (tree_size,),
    ).fetchone()


def list_checkpoints(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            f"SELECT {CHECKPOINT_COLUMNS} FROM receipt_checkpoints "
            "ORDER BY tree_size"
        ).fetchall()
    )


def _leaf_hashes(conn: sqlite3.Connection, tree_size: int) -> list[bytes]:
    """Stored leaf hashes for indices ``0..tree_size-1``, in tree order."""
    rows = conn.execute(
        "SELECT leaf_hash FROM receipt_leaves WHERE leaf_index < ? "
        "ORDER BY leaf_index",
        (tree_size,),
    ).fetchall()
    return [bytes.fromhex(row["leaf_hash"]) for row in rows]


def tree_root_hash(conn: sqlite3.Connection, tree_size: int) -> str:
    """Recompute the Merkle root of the first ``tree_size`` leaves (hex)."""
    return _mth(_leaf_hashes(conn, tree_size)).hex()


# ----------------------------------------------------------------- SQL: write


def append_leaf(
    conn: sqlite3.Connection,
    *,
    event_seq: int,
    device_id: str,
    event_id: str,
    occurred_at: str,
    event_type: str | None,
    schema_version: str | None,
    payload: str | None,
    now: datetime | None = None,
) -> int:
    """Append the Merkle leaf for one freshly inserted event.

    Must run inside the caller's ``BEGIN IMMEDIATE`` transaction (the same
    one that inserted the event), so the leaf and the event commit or roll
    back together. ``leaf_index`` is the next free index; because the
    single-writer lock serializes appends, indices are contiguous and
    ``event_seq`` values arrive strictly increasing. Returns the index.

    The tree records each ``event_id``'s *first* acceptance: retention
    cleanup deletes only the event body, never the leaf, so an event_id
    that is accepted again after its body was cleaned up is a replay of
    history and does not grow the tree -- the existing leaf (binding the
    original ``events.seq``) is returned instead.
    """
    existing = conn.execute(
        "SELECT leaf_index FROM receipt_leaves WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if existing is not None:
        return int(existing["leaf_index"])
    row = conn.execute(
        "SELECT COALESCE(MAX(leaf_index), -1) AS top_index FROM receipt_leaves"
    ).fetchone()
    leaf_index = int(row["top_index"]) + 1
    material = leaf_material(
        event_seq=event_seq,
        device_id=device_id,
        event_id=event_id,
        occurred_at=occurred_at,
        event_type=event_type,
        schema_version=schema_version,
        payload=payload,
    )
    if now is None:
        now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO receipt_leaves (leaf_index, event_seq, event_id, "
        "device_id, occurred_at, event_type, schema_version, payload, "
        "leaf_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            leaf_index,
            event_seq,
            event_id,
            device_id,
            occurred_at,
            event_type,
            schema_version,
            payload,
            hash_leaf_material(material).hex(),
            to_storage(now),
        ),
    )
    return leaf_index


def backfill_leaves(conn: sqlite3.Connection) -> int:
    """Append leaves for events that predate the receipts module.

    Idempotent one-shot migration: every stored event whose ``event_id``
    has no leaf yet receives one, in ``events.seq`` order, so re-running
    is a no-op and the resulting tree depends only on the stored events
    (reproducible across databases with identical content). The anti-join
    (rather than a high-water comparison) also covers an event_id that
    was re-accepted after retention cleanup removed its first body: its
    original leaf is retained and no second leaf is ever appended.
    Returns the number of leaves appended.
    """
    events = conn.execute(
        "SELECT e.seq, e.device_id, e.event_id, e.occurred_at, e.event_type, "
        "e.schema_version, e.payload FROM events AS e "
        "LEFT JOIN receipt_leaves AS l ON l.event_id = e.event_id "
        "WHERE l.event_id IS NULL ORDER BY e.seq"
    ).fetchall()
    if not events:
        return 0
    row = conn.execute(
        "SELECT COALESCE(MAX(leaf_index), -1) AS top_index FROM receipt_leaves"
    ).fetchone()
    next_index = int(row["top_index"]) + 1
    now = datetime.now(timezone.utc)
    for offset, event in enumerate(events):
        material = leaf_material(
            event_seq=int(event["seq"]),
            device_id=event["device_id"],
            event_id=event["event_id"],
            occurred_at=event["occurred_at"],
            event_type=event["event_type"],
            schema_version=event["schema_version"],
            payload=event["payload"],
        )
        conn.execute(
            "INSERT INTO receipt_leaves (leaf_index, event_seq, event_id, "
            "device_id, occurred_at, event_type, schema_version, payload, "
            "leaf_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                next_index + offset,
                int(event["seq"]),
                event["event_id"],
                event["device_id"],
                event["occurred_at"],
                event["event_type"],
                event["schema_version"],
                event["payload"],
                hash_leaf_material(material).hex(),
                to_storage(now),
            ),
        )
    return len(events)


# --------------------------------------------------------- proofs/checkpoints


def save_checkpoint(
    conn: sqlite3.Connection,
    *,
    tree_size: int | None = None,
    now: datetime | None = None,
) -> tuple[sqlite3.Row, bool]:
    """Save an immutable ``(tree_size, root_hash)`` checkpoint.

    Returns ``(row, created)``: a repeated request for an existing size
    replays the originally stored row (``created`` is False) without
    recomputation -- the tree is append-only, so the stored root is
    necessarily identical. ``tree_size`` defaults to the current tree
    size. Sizes below 1 or beyond the current tree size raise
    :class:`ReceiptError` (mapped to 422) and store nothing.
    """
    current = leaf_count(conn)
    if tree_size is None:
        tree_size = current
    if tree_size < 1 or tree_size > current:
        raise ReceiptError(
            f"invalid tree_size {tree_size}: the receipt tree currently "
            f"holds {current} leaf(s); a checkpoint requires "
            "1 <= tree_size <= current size"
        )
    existing = get_checkpoint(conn, tree_size)
    if existing is not None:
        return existing, False
    root_hash = tree_root_hash(conn, tree_size)
    if now is None:
        now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO receipt_checkpoints (tree_size, root_hash, created_at) "
        "VALUES (?, ?, ?)",
        (tree_size, root_hash, to_storage(now)),
    )
    saved = get_checkpoint(conn, tree_size)
    assert saved is not None
    return saved, True


def membership_proof(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    tree_size: int | None = None,
) -> MembershipProof | None:
    """Build the inclusion proof for one event's leaf.

    Returns ``None`` when the event has no receipt leaf (the route maps
    this to 404). The proof targets the current tree size unless an
    explicit ``tree_size`` is given; a size that is out of range or does
    not cover the leaf raises :class:`ReceiptError` (mapped to 422).
    """
    leaf = get_leaf_by_event(conn, event_id)
    if leaf is None:
        return None
    current = leaf_count(conn)
    size = current if tree_size is None else tree_size
    leaf_index = int(leaf["leaf_index"])
    if size < 1 or size > current or size <= leaf_index:
        raise ReceiptError(
            f"invalid tree_size {size}: the receipt tree currently holds "
            f"{current} leaf(s) and the leaf of event {event_id!r} is at "
            f"index {leaf_index}; a proof requires "
            "leaf_index < tree_size <= current size"
        )
    hashes = _leaf_hashes(conn, size)
    path = _audit_path(hashes, leaf_index)
    return MembershipProof(
        leaf=leaf,
        tree_size=size,
        root_hash=_mth(hashes).hex(),
        audit_path=tuple(h.hex() for h in path),
    )


def consistency_proof(
    conn: sqlite3.Connection,
    *,
    first_size: int,
    second_size: int,
) -> ConsistencyProof:
    """Build the RFC 6962 consistency proof between two tree sizes.

    Both sizes must satisfy ``1 <= first_size <= second_size <= current
    size``; anything else raises :class:`ReceiptError` (mapped to 422).
    The returned roots are recomputed from the leaves and therefore match
    any checkpoint stored for the same sizes.
    """
    current = leaf_count(conn)
    if first_size < 1 or second_size < 1 or first_size > second_size:
        raise ReceiptError(
            f"invalid tree sizes: consistency requires "
            f"1 <= first_size <= second_size, got first={first_size} "
            f"second={second_size}"
        )
    if second_size > current:
        raise ReceiptError(
            f"invalid tree sizes: the receipt tree currently holds "
            f"{current} leaf(s), so second_size {second_size} is beyond "
            "the current tree"
        )
    hashes = _leaf_hashes(conn, second_size)
    proof = _subproof(hashes, first_size, second_size, True)
    return ConsistencyProof(
        first_size=first_size,
        first_root=_mth(hashes[:first_size]).hex(),
        second_size=second_size,
        second_root=_mth(hashes).hex(),
        proof=tuple(h.hex() for h in proof),
    )

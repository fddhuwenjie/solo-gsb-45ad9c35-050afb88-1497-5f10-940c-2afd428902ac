"""Transparent event receipts: RFC 6962-style Merkle log over stored events.

Every event that is written for the *first* time gains one Merkle leaf in
the same ``BEGIN IMMEDIATE`` transaction that inserts the ``events`` row,
positioned at the event's persistent, never-reused ``events.seq``. A leaf
binds the full event identity (``seq``, ``device_id``, ``event_id``,
``occurred_at``, ``event_type``, ``schema_version`` and ``payload``) into
one canonical document; leaf and node hashing follow RFC 6962's domain
separation (a ``0x00`` byte before leaf input, ``0x01`` before paired node
hashes). Idempotent replays and failed requests append nothing, so the
append-only leaf sequence is exactly ``1..tree_size`` with no gaps.

The leaf table deliberately has no foreign key to ``events`` and stores a
copy of the canonical leaf document itself: retention cleanup deletes only
the event bodies, while receipts, leaves, membership proofs and old
checkpoints all remain verifiable offline.

The pure tree helpers below (root, inclusion path/proof verification,
consistency path/proof verification) hold no state; the SQL helpers take
an open connection and leave transaction ownership to the caller, matching
the other storage-backed modules.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Sequence

from .sessions import to_storage

# RFC 6962 §2.1 domain-separation prefixes.
LEAF_HASH_PREFIX = b"\x00"
NODE_HASH_PREFIX = b"\x01"

LEAF_COLUMNS = "seq, event_seq, event_id, leaf_data, leaf_hash, created_at"
CHECKPOINT_COLUMNS = "tree_size, root_hash, created_at"


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical JSON (sorted keys, no whitespace, UTF-8, non-ASCII kept)."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


# ------------------------------------------------------------- leaf binding


def leaf_document(
    *,
    seq: int,
    device_id: str,
    event_id: str,
    occurred_at: str,
    event_type: str | None,
    schema_version: str | None,
    payload: Any,
) -> bytes:
    """Return the canonical bytes bound by one Merkle leaf.

    The contract-triplet fields are present unconditionally (explicit JSON
    ``null`` for payload-less events), so the distinction between a
    payload-less event and one carrying null values is part of the hash.
    ``occurred_at`` is the normalized UTC ISO-8601 string used in the
    ``events`` row, so the same physical event always hashes identically.
    """
    return canonical_json_bytes(
        {
            "seq": int(seq),
            "device_id": device_id,
            "event_id": event_id,
            "occurred_at": occurred_at,
            "event_type": event_type,
            "schema_version": schema_version,
            "payload": payload,
        }
    )


def leaf_hash(leaf_data: bytes) -> bytes:
    """RFC 6962 leaf hash: SHA-256(0x00 || leaf input)."""
    return hashlib.sha256(LEAF_HASH_PREFIX + leaf_data).digest()


def node_hash(left: bytes, right: bytes) -> bytes:
    """RFC 6962 node hash: SHA-256(0x01 || left || right)."""
    return hashlib.sha256(NODE_HASH_PREFIX + left + right).digest()


# ------------------------------------------------------------- pure tree math


def _largest_pow2_less(size: int) -> int:
    """Largest power of two strictly smaller than ``size`` (size >= 2).

    The RFC 6962 split point: highest bit of ``size - 1``.
    """
    return 1 << ((size - 1).bit_length() - 1)


def root_from_hashes(hashes: Sequence[bytes]) -> bytes:
    """Merkle tree hash over one or more leaf hashes (RFC 6962 §2.1.1)."""
    if not hashes:
        raise ValueError("cannot hash an empty tree")
    # Stack of (node hash, subtree width); each new leaf merges equal-width
    # neighbors. Remaining entries have strictly descending power-of-two
    # widths and fold from the right: H(outer_left, accumulator).
    stack: list[tuple[bytes, int]] = []
    for value in hashes:
        node, width = value, 1
        while stack and stack[-1][1] == width:
            sibling, sibling_width = stack.pop()
            node = node_hash(sibling, node)
            width += sibling_width
        stack.append((node, width))
    result, _ = stack[-1]
    for value, _ in reversed(stack[:-1]):
        result = node_hash(value, result)
    return result


def inclusion_path(leaves: Sequence[bytes], leaf_index: int) -> list[bytes]:
    """RFC 6962 inclusion audit path (without the leaf itself).

    Mirrors the recursive ``PATH(m, D[n])`` definition: at each split the
    sibling subtree root is appended, so the list runs from the sibling at
    the leaf's own parent outwards to the top-level sibling. Its length
    varies for non-power-of-two tree sizes (e.g. the last leaf of a
    3-leaf tree has one sibling, not two).
    """
    size = len(leaves)
    if not (0 <= leaf_index < size):
        raise ValueError("leaf index out of range")

    def gen(index: int, width: int, offset: int) -> list[bytes]:
        if width == 1:
            return []
        split = _largest_pow2_less(width)
        if index < split:
            # Inner audit path inside the perfect left subtree, followed by
            # the right subtree root (the outer sibling).
            return gen(index, split, offset) + [
                root_from_hashes(leaves[offset + split : offset + width])
            ]
        # Inner audit path inside the (possibly imperfect) right subtree,
        # followed by the perfect left subtree root.
        return gen(index - split, width - split, offset + split) + [
            root_from_hashes(leaves[offset : offset + split])
        ]

    return gen(leaf_index, size, 0)


def rebuild_root_from_inclusion(
    leaf_index: int,
    tree_size: int,
    leaf: bytes,
    path: Sequence[bytes],
) -> bytes:
    """Recompute the tree root from an inclusion proof.

    Mirrors the recursive ``PATH`` construction, consuming the inner-first
    audit path: the innermost subtree is rebuilt first and its appended
    outer sibling consumed on the way out. Raises ``ValueError`` when the
    index/size pair is invalid or the path has the wrong length (callers
    treat that as a failed verification rather than an internal error).
    """
    if tree_size < 1 or not (0 <= leaf_index < tree_size):
        raise ValueError("invalid leaf index / tree size")
    nodes = list(path)
    cursor = 0

    def take() -> bytes:
        nonlocal cursor
        if cursor >= len(nodes):
            raise ValueError("inclusion proof is too short")
        value = nodes[cursor]
        cursor += 1
        return value

    def rebuild(index: int, width: int) -> bytes:
        if width == 1:
            if index != 0:
                raise ValueError("invalid leaf index within subtree")
            return leaf
        split = _largest_pow2_less(width)
        if index < split:
            inner = rebuild(index, split)
            return node_hash(inner, take())  # sibling is the right subtree
        inner = rebuild(index - split, width - split)
        return node_hash(take(), inner)      # sibling is the left subtree

    root = rebuild(leaf_index, tree_size)
    if cursor != len(nodes):
        raise ValueError("inclusion proof is too long")
    return root


def verify_inclusion(
    leaf_index: int,
    tree_size: int,
    leaf: bytes,
    path: Sequence[bytes],
    root: bytes,
) -> bool:
    """True iff ``path`` proves ``leaf`` at ``leaf_index`` under ``root``."""
    try:
        return rebuild_root_from_inclusion(
            leaf_index, tree_size, leaf, path
        ) == root
    except ValueError:
        return False


def consistency_path(
    leaves: Sequence[bytes], old_size: int, new_size: int | None = None
) -> list[bytes]:
    """Append-only consistency proof nodes between two tree sizes.

    Node order is bottom-up (the boundary leaf/subtree first, wider shared
    subtrees last) and follows RFC 6962's recursive consistency split,
    except that at a *nested* boundary aligned exactly with the split the
    boundary subtree root is emitted too (the holder only knows the
    top-level old root, not intermediate subtree roots). At the top level
    the old root itself is never emitted: the verifier supplies it, so
    ``old_size`` being a power of two yields only the new-side nodes.
    """
    size = new_size if new_size is not None else len(leaves)
    if not (1 <= old_size <= size):
        raise ValueError("invalid consistency sizes")

    def gen(old: int, width: int, offset: int, top: bool) -> list[bytes]:
        if old == width:
            return []
        split = _largest_pow2_less(width)
        if old == split:
            right = root_from_hashes(leaves[offset + split : offset + width])
            if top:
                return [right]
            left = root_from_hashes(leaves[offset : offset + old])
            return [left, right]
        if old < split:
            return gen(old, split, offset, False) + [
                root_from_hashes(leaves[offset + split : offset + width])
            ]
        return gen(old - split, width - split, offset + split, False) + [
            root_from_hashes(leaves[offset : offset + split])
        ]
    return gen(old_size, size, 0, True)


def rebuild_consistency(
    old_size: int,
    new_size: int,
    old_root: bytes,
    path: Sequence[bytes],
) -> tuple[bytes, bytes]:
    """Recompute ``(old root, new root)`` from a consistency proof.

    ``old_root`` is the checkpoint root the verifier already trusts; it is
    injected at the top boundary. Raises ``ValueError`` on malformed input.
    """
    if not (1 <= old_size <= new_size):
        raise ValueError("invalid consistency sizes")
    # The generator lists inner nodes first and appends outer siblings
    # afterwards (post-order), so the rebuild consumes it left-to-right.
    cursor = 0

    def take() -> bytes:
        nonlocal cursor
        if cursor >= len(path):
            raise ValueError("consistency proof is too short")
        node = path[cursor]
        cursor += 1
        return node

    def rebuild(old: int, width: int, top: bool) -> tuple[bytes, bytes]:
        """Rebuild (root of the old prefix, root of this interval) bottom-up.

        The generator emits inner nodes first; this mirrors it: complete the
        inner subtree, then consume the outer sibling that was appended.
        """
        if old == width:
            return b"", b""  # only reached for the trivial m == n case
        split = _largest_pow2_less(width)
        if old == split:
            # Flat sibling pair ``[left, right]`` at a nested boundary; at
            # the top level the left one is the trusted old root and only
            # the right node is present in the proof.
            left = old_root if top else take()
            right = take()
            return left, node_hash(left, right)
        if old < split:
            # The old prefix lives wholly in the perfect left subtree:
            # the inner call yields both its old-prefix root and the full
            # left subtree root; the appended outer node is the right root.
            old_prefix, left_root = rebuild(old, split, False)
            right = take()
            return old_prefix, node_hash(left_root, right)
        # The old boundary is inside the right subtree: the inner call
        # yields the old root's right suffix and the right subtree root;
        # the appended outer node is the perfect left subtree root.
        old_suffix, right_root = rebuild(old - split, width - split, False)
        left = take()
        return node_hash(left, old_suffix), node_hash(left, right_root)

    if old_size == new_size:
        if cursor != len(path):
            raise ValueError("consistency proof must be empty for equal sizes")
        return old_root, old_root
    rebuilt_old, rebuilt_new = rebuild(old_size, new_size, True)
    if cursor != len(path):
        raise ValueError("consistency proof is too long")
    return rebuilt_old, rebuilt_new


def verify_consistency(
    old_size: int,
    new_size: int,
    old_root: bytes,
    new_root: bytes,
    path: Sequence[bytes],
) -> bool:
    """True iff ``path`` binds ``new_root`` as an append-only extension."""
    try:
        rebuilt_old, rebuilt_new = rebuild_consistency(
            old_size, new_size, old_root, path
        )
    except ValueError:
        return False
    if old_size == new_size:
        return old_root == new_root
    return rebuilt_old == old_root and rebuilt_new == new_root


# ------------------------------------------------------------------- SQL API


def append_event_leaf(
    conn: sqlite3.Connection,
    *,
    event_seq: int,
    device_id: str,
    event_id: str,
    occurred_at: str,
    event_type: str | None,
    schema_version: str | None,
    payload: Any,
    created_at: datetime,
) -> tuple[int, str]:
    """Append the leaf for one newly inserted event.

    Called inside the event-write transaction immediately after the
    ``events`` insert, so the leaf commits and rolls back together with the
    event row. The dense Merkle position is assigned as the table's
    INTEGER PRIMARY KEY (one per previously stored leaf); ``event_seq``
    records the bound, never-reused ``events.seq``. Returns
    ``(leaf_seq, leaf_hash_hex)``.
    """
    data = leaf_document(
        seq=int(event_seq),
        device_id=device_id,
        event_id=event_id,
        occurred_at=occurred_at,
        event_type=event_type,
        schema_version=schema_version,
        payload=payload,
    )
    digest_hex = leaf_hash(data).hex()
    cur = conn.execute(
        "INSERT INTO event_receipt_leaves "
        "(event_seq, event_id, leaf_data, leaf_hash, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (int(event_seq), event_id, data.decode("utf-8"), digest_hex,
         to_storage(created_at)),
    )
    return int(cur.lastrowid), digest_hex


def current_tree_size(conn: sqlite3.Connection) -> int:
    """Number of leaves; the leaf positions are the gap-free range 1..size."""
    row = conn.execute("SELECT COUNT(*) AS n FROM event_receipt_leaves").fetchone()
    return int(row["n"])


def leaf_hashes_through(conn: sqlite3.Connection, tree_size: int) -> list[bytes]:
    return [
        bytes.fromhex(row["leaf_hash"])
        for row in conn.execute(
            "SELECT leaf_hash FROM event_receipt_leaves WHERE seq <= ? "
            "ORDER BY seq",
            (tree_size,),
        ).fetchall()
    ]


def get_leaf_by_event_id(
    conn: sqlite3.Connection, event_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {LEAF_COLUMNS} FROM event_receipt_leaves WHERE event_id = ?",
        (event_id,),
    ).fetchone()


def get_checkpoint(
    conn: sqlite3.Connection, tree_size: int
) -> sqlite3.Row | None:
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


def insert_checkpoint(
    conn: sqlite3.Connection,
    *,
    tree_size: int,
    root_hash: str,
    created_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO receipt_checkpoints (tree_size, root_hash, created_at) "
        "VALUES (?, ?, ?)",
        (tree_size, root_hash, to_storage(created_at)),
    )


def backfill_leaves(conn: sqlite3.Connection) -> int:
    """One-time backfill of leaves for events written before this module.

    Reads every surviving ``events`` row in ``seq`` order and inserts the
    deterministic leaf derived from the stored, normalized columns, so the
    result is reproducible byte for byte (and identical to leaves appended
    online). Leaves are numbered densely by arrival order: an old database
    that already deleted some events before the upgrade leaves gaps in
    ``events.seq`` but not in the Merkle positions; each leaf still records
    the original ``event_seq`` it binds. The caller runs this inside the
    idempotent startup migration; it is a no-op whenever any leaf exists.
    """
    if current_tree_size(conn):
        return 0
    rows = conn.execute(
        "SELECT seq, event_id, device_id, occurred_at, event_type, "
        "schema_version, payload FROM events ORDER BY seq"
    ).fetchall()
    # Deterministic marker for migrated rows; online leaves use the real
    # acceptance instant. The leaf hash never covers created_at anyway.
    created_at = "1970-01-01T00:00:00+00:00"
    for row in rows:
        payload = json.loads(row["payload"]) if row["payload"] is not None else None
        data = leaf_document(
            seq=int(row["seq"]),
            device_id=row["device_id"],
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            event_type=row["event_type"],
            schema_version=row["schema_version"],
            payload=payload,
        )
        conn.execute(
            "INSERT INTO event_receipt_leaves "
            "(event_seq, event_id, leaf_data, leaf_hash, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                int(row["seq"]),
                row["event_id"],
                data.decode("utf-8"),
                leaf_hash(data).hex(),
                created_at,
            ),
        )
    return len(rows)


# --------------------------------------------------------- proof orchestration


class InvalidTreeSize(Exception):
    """A supplied tree size is invalid (HTTP 422 semantics).

    Legal sizes are integers from 1 through the current tree size for
    inclusion/consistency inputs; checkpoint creation additionally accepts
    exactly the current size.
    """

    def __init__(self, message: str, *, reason: str, tree_size: int | None = None,
                 current_size: int | None = None):
        super().__init__(message)
        self.message = message
        self.reason = reason
        self.tree_size = tree_size
        self.current_size = current_size

    def detail(self) -> dict[str, Any]:
        body: dict[str, Any] = {"message": self.message, "reason": self.reason}
        if self.tree_size is not None:
            body["tree_size"] = self.tree_size
        if self.current_size is not None:
            body["current_tree_size"] = self.current_size
        return body


class UnknownReceipt(Exception):
    """No leaf/checkpoint exists for the requested identifier (HTTP 404)."""


def tree_root_at(conn: sqlite3.Connection, tree_size: int) -> bytes:
    hashes = leaf_hashes_through(conn, tree_size)
    if len(hashes) != tree_size:
        raise RuntimeError(
            f"receipt log is inconsistent: expected {tree_size} leaves, "
            f"found {len(hashes)}"
        )
    return root_from_hashes(hashes)


def create_checkpoint(
    conn: sqlite3.Connection,
    tree_size: int,
    *,
    now: datetime | None = None,
) -> tuple[sqlite3.Row, bool]:
    """Freeze an immutable checkpoint at ``tree_size``.

    Returns ``(row, created)``; a repeated request for the same size
    replays the stored row (same root and ``created_at``) with
    ``created=False``. A size outside ``1..current_tree_size`` raises
    :class:`InvalidTreeSize` (mapped to 422) and writes nothing.
    """
    current = current_tree_size(conn)
    if not isinstance(tree_size, int) or tree_size < 1 or tree_size > current:
        raise InvalidTreeSize(
            f"tree_size must be between 1 and the current tree size "
            f"({current}); got {tree_size}",
            reason="invalid_tree_size",
            tree_size=tree_size,
            current_size=current,
        )
    existing = get_checkpoint(conn, tree_size)
    if existing is not None:
        return existing, False
    root_hex = tree_root_at(conn, tree_size).hex()
    if now is None:
        now = datetime.now(timezone.utc)
    insert_checkpoint(
        conn, tree_size=tree_size, root_hash=root_hex, created_at=now
    )
    saved = get_checkpoint(conn, tree_size)
    assert saved is not None
    return saved, True


def inclusion_proof(
    conn: sqlite3.Connection,
    event_id: str,
    tree_size: int | None = None,
) -> dict[str, Any]:
    """Build a membership proof for ``event_id`` verifiable offline.

    The response carries everything an offline verifier needs: the dense
    ``leaf_index``, the ``tree_size``, the canonical ``leaf_data`` (and its
    hex ``leaf_hash``), the RFC 6962 audit path (bottom-up hex hashes) and
    the recomputed hex root -- plus the bound ``event_seq`` and, when a
    checkpoint exists at that size, its frozen root and creation time.

    Raises :class:`UnknownReceipt` when the event has no leaf, and
    :class:`InvalidTreeSize` for a non-positive or too-large size
    (including a size predating this leaf).
    """
    leaf_row = get_leaf_by_event_id(conn, event_id)
    if leaf_row is None:
        raise UnknownReceipt(f"no receipt leaf exists for event_id {event_id!r}")
    current = current_tree_size(conn)
    if tree_size is None:
        tree_size = current
    if (
        not isinstance(tree_size, int)
        or tree_size < 1
        or tree_size > current
    ):
        raise InvalidTreeSize(
            f"tree_size must be between 1 and the current tree size "
            f"({current}); got {tree_size}",
            reason="invalid_tree_size",
            tree_size=tree_size,
            current_size=current,
        )
    leaf_index = int(leaf_row["seq"])
    if leaf_index > tree_size:
        raise InvalidTreeSize(
            f"event_id {event_id!r} is leaf {leaf_index}, which is not part "
            f"of tree size {tree_size}",
            reason="leaf_not_in_tree",
            tree_size=tree_size,
            current_size=current,
        )
    hashes = leaf_hashes_through(conn, tree_size)
    path = inclusion_path(hashes, leaf_index - 1)
    root = tree_root_at(conn, tree_size)
    leaf = bytes.fromhex(leaf_row["leaf_hash"])
    assert verify_inclusion(leaf_index - 1, tree_size, leaf, path, root)
    checkpoint = get_checkpoint(conn, tree_size)
    return {
        "event_id": event_id,
        "event_seq": int(leaf_row["event_seq"]),
        "leaf_index": leaf_index,
        "tree_size": tree_size,
        "leaf_data": json.loads(leaf_row["leaf_data"]),
        "leaf_hash": leaf_row["leaf_hash"],
        "audit_path": [node.hex() for node in path],
        "root_hash": root.hex(),
        "checkpoint": (
            None
            if checkpoint is None
            else {
                "tree_size": tree_size,
                "root_hash": checkpoint["root_hash"],
                "created_at": checkpoint["created_at"],
            }
        ),
    }


def consistency_proof(
    conn: sqlite3.Connection, first_size: int, second_size: int
) -> dict[str, Any]:
    """Build a consistency proof between two frozen checkpoints.

    Both sizes must reference existing immutable checkpoints (otherwise
    :class:`UnknownReceipt`, mapped to 404); ``first_size`` must be <=
    ``second_size`` and both within the current tree (otherwise
    :class:`InvalidTreeSize`, 422). The returned proof is self-contained:
    both checkpoint roots/timestamps, the RFC 6962 consistency nodes
    (bottom-up hex hashes) and both sizes -- enough to recompute both
    roots offline from the trusted first checkpoint root.
    """
    first = get_checkpoint(conn, first_size)
    second = get_checkpoint(conn, second_size)
    if first is None or second is None:
        missing = first_size if first is None else second_size
        raise UnknownReceipt(
            f"no receipt checkpoint exists at tree size {missing}"
        )
    current = current_tree_size(conn)
    if not (
        isinstance(first_size, int)
        and isinstance(second_size, int)
        and 1 <= first_size <= second_size <= current
    ):
        raise InvalidTreeSize(
            f"checkpoint sizes must satisfy 1 <= {first_size} <= "
            f"{second_size} <= current tree size ({current})",
            reason="invalid_tree_size",
            current_size=current,
        )
    hashes = leaf_hashes_through(conn, second_size)
    path = consistency_path(hashes, first_size, second_size)
    first_root = bytes.fromhex(first["root_hash"])
    second_root = tree_root_at(conn, second_size)
    assert verify_consistency(
        first_size, second_size, first_root, second_root, path
    )
    return {
        "first_tree_size": first_size,
        "second_tree_size": second_size,
        "first_root_hash": first["root_hash"],
        "second_root_hash": second["root_hash"],
        "first_checkpoint_created_at": first["created_at"],
        "second_checkpoint_created_at": second["created_at"],
        "consistency_path": [node.hex() for node in path],
    }

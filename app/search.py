"""Payload-conditioned event search: evaluation, fingerprints and cursors.

The search endpoint selects events by scalar columns (device_id,
event_type, schema_version, a half-open occurred_at interval) and by up to
five AND conditions located inside the canonicalized payload with
RFC 6901 JSON Pointers.

Results are ordered by (occurred_at, event_id) and paged with opaque,
signed keyset cursors. The first page (a request without a cursor) locks
the then-current maximum ``seq`` -- the persistent AUTOINCREMENT column
that is never reused, even after the maximum row is deleted -- as a stable
high-water mark carried in every subsequent cursor: rows inserted
afterwards never enter the result set, while rows deleted in the meantime
are simply absent.

Everything in this module is pure apart from the query helpers taking an
open sqlite connection; callers own the transactions.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
from datetime import datetime
from typing import Any, Literal, NamedTuple

from .sessions import normalize_ts, to_storage

Operator = Literal["exists", "eq", "lt", "lte", "gt", "gte"]
OPERATORS: frozenset[str] = frozenset({"exists", "eq", "lt", "lte", "gt", "gte"})
MAX_CONDITIONS = 5
DEFAULT_LIMIT = 50
MAX_LIMIT = 500

# v1 cursors pinned the implicit rowid, which SQLite can reuse after the
# maximum row is deleted: a row inserted afterwards (possibly sorting before
# the cursor key) could leak into later pages. v2 pins ``seq`` instead.
# The token prefix is bumped so old cursors are rejected with a clear 422.
CURSOR_PREFIX = "evc2:"
LEGACY_CURSOR_PREFIX = "evc1:"
CURSOR_VERSION = 2
_MISSING = object()


class SearchError(Exception):
    """A client-correctable search failure (reported as 422)."""


class Condition(NamedTuple):
    pointer: str
    operator: Operator
    value: Any  # raw JSON comparison value (boolean for ``exists``)


class SearchQuery(NamedTuple):
    device_id: str | None
    event_type: str | None
    schema_version: str | None
    occurred_start: datetime | None  # inclusive
    occurred_end: datetime | None  # exclusive
    conditions: tuple[Condition, ...]


# ------------------------------------------------------------- JSON Pointer

def parse_pointer(pointer: str) -> list[str]:
    """Parse an RFC 6901 JSON Pointer into its reference tokens.

    Only the absolute-pointer form (empty string or a string starting with
    ``/``) is accepted; relative or otherwise malformed pointers raise
    :class:`SearchError`.
    """
    if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
        raise SearchError(
            f"invalid JSON Pointer {pointer!r}: must be empty or start with '/'"
        )
    tokens: list[str] = []
    # Skip the leading slash; split on the remaining ones.
    for raw in pointer.split("/")[1:]:
        token = raw.replace("~1", "/").replace("~0", "~")
        tokens.append(token)
    return tokens


def _array_index(token: str, length: int) -> int | None:
    """Decode an array reference token per RFC 6901, else None.

    ``-`` never indexes an existing element; ``00`` and leading-zero
    indices are invalid; ``0`` addresses the first element.
    """
    if token == "-" or not token or not token.isdigit():
        return None
    if len(token) > 1 and token[0] == "0":
        return None
    index = int(token)
    return index if 0 <= index < length else None


def resolve_pointer(document: Any, tokens: list[str]) -> Any:
    """Resolve ``tokens`` against ``document``; return the internal MISSING sentinel.

    A traversal that steps through a non-container, past an array bound, or
    through an absent object key yields :data:`_MISSING` -- distinct from an
    explicit JSON ``null``, which resolves to Python ``None``.
    """
    current: Any = document
    for token in tokens:
        if isinstance(current, dict):
            if token not in current:
                return _MISSING
            current = current[token]
        elif isinstance(current, list):
            index = _array_index(token, len(current))
            if index is None:
                return _MISSING
            current = current[index]
        else:
            return _MISSING
    return current


# ---------------------------------------------------------- value semantics

def json_number(value: Any) -> bool:
    """True for JSON numbers. Booleans are not numbers in JSON."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def json_equal(left: Any, right: Any) -> bool:
    """Strict JSON-type equality.

    Types never compare equal across the JSON type system (``1`` !=
    ``"1"``, ``null`` != anything but ``null``); integers and floats are
    compared numerically (``1`` == ``1.0``); objects compare independent of
    key order and arrays element-wise.
    """
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if json_number(left) and json_number(right):
        return left == right
    if isinstance(left, str) or isinstance(right, str):
        return isinstance(left, str) and isinstance(right, str) and left == right
    if isinstance(left, list) or isinstance(right, list):
        return (
            isinstance(left, list)
            and isinstance(right, list)
            and len(left) == len(right)
            and all(json_equal(a, b) for a, b in zip(left, right))
        )
    if isinstance(left, dict) or isinstance(right, dict):
        if not (isinstance(left, dict) and isinstance(right, dict)):
            return False
        if left.keys() != right.keys():
            return False
        return all(json_equal(left[key], right[key]) for key in left)
    return False


def evaluate_condition(value: Any, condition: Condition) -> bool:
    """Evaluate one condition against a resolved payload location.

    ``value`` is the internal MISSING sentinel for absent locations. An
    explicit JSON ``null`` is a present value: it matches ``exists: true``
    and ``eq: null`` but never the ordering operators.
    """
    present = value is not _MISSING
    if condition.operator == "exists":
        # ``exists`` is the only operator that distinguishes an absent
        # location from an explicit JSON null: both are "present" here,
        # the absence of the key is what it tests for.
        return present if condition.value else not present
    if not present:
        return False
    if condition.operator == "eq":
        return json_equal(value, condition.value)
    # Ordering operators are strictly numeric on both sides: strings,
    # booleans, null, objects and arrays never participate.
    if not json_number(value) or not json_number(condition.value):
        return False
    if condition.operator == "lt":
        return value < condition.value
    if condition.operator == "lte":
        return value <= condition.value
    if condition.operator == "gt":
        return value > condition.value
    if condition.operator == "gte":
        return value >= condition.value
    raise SearchError(f"unsupported operator {condition.operator!r}")  # pragma: no cover


def row_matches(payload_text: str | None, conditions: tuple[Condition, ...]) -> bool:
    """Evaluate all AND conditions against an event row's stored payload.

    A NULL payload column (a legacy/no-payload event) has no document at
    all: every pointer, including the root pointer, resolves as missing.
    Thus ``exists: false`` matches such events while ``exists: true`` and
    every other operator reject them.
    """
    if not conditions:
        return True
    document = json.loads(payload_text) if payload_text is not None else None
    for condition in conditions:
        tokens = parse_pointer(condition.pointer)
        if payload_text is None:
            value: Any = _MISSING
        else:
            value = resolve_pointer(document, tokens)
        if not evaluate_condition(value, condition):
            return False
    return True


# ------------------------------------------------------- query fingerprint

def canonical_json(value: Any) -> str:
    """Stable serialization: byte-identical JSON values compare equal."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def query_fingerprint(query: SearchQuery) -> str:
    """Deterministic digest binding a cursor to its normalized query.

    Conditions (order-sensitive as submitted, but pointers/values are
    already canonical) and the half-open interval are fully covered; the
    page size is deliberately excluded so changing ``limit`` while walking
    a cursor is not a binding violation.
    """
    document = {
        "device_id": query.device_id,
        "event_type": query.event_type,
        "schema_version": query.schema_version,
        "occurred_start": (
            to_storage(normalize_ts(query.occurred_start))
            if query.occurred_start is not None
            else None
        ),
        "occurred_end": (
            to_storage(normalize_ts(query.occurred_end))
            if query.occurred_end is not None
            else None
        ),
        "conditions": [
            {"pointer": c.pointer, "operator": c.operator, "value": c.value}
            for c in query.conditions
        ],
    }
    material = canonical_json(document)
    return hashlib.sha256(
        b"device-event-search-query:1\n" + material.encode("utf-8")
    ).hexdigest()


# ----------------------------------------------------------------- cursors

class Cursor(NamedTuple):
    version: int  # cursor format version (CURSOR_VERSION); embedded explicitly
    fingerprint: str
    high_water: int  # maximum visible seq, fixed at the first page
    occurred_at: str  # last returned key: occurred_at (UTC ISO-8601)
    event_id: str  # last returned key: event_id


# Opaque cursors are integrity-protected with a server-side HMAC key and
# additionally bound to the normalized query fingerprint, so a cursor can
# neither be tampered with nor replayed against different conditions.
_CURSOR_KEY = b"device-event-search-cursor-hmac-key-v1"


def _sign(body: str) -> str:
    return hmac.new(_CURSOR_KEY, body.encode("utf-8"), hashlib.sha256).hexdigest()


def encode_cursor(cursor: Cursor) -> str:
    body = canonical_json(
        {
            "v": cursor.version,
            "f": cursor.fingerprint,
            "w": cursor.high_water,
            "t": cursor.occurred_at,
            "e": cursor.event_id,
        }
    )
    payload = base64.urlsafe_b64encode(body.encode("utf-8")).decode("ascii")
    return CURSOR_PREFIX + payload + "." + _sign(body)[:32]


def decode_cursor(raw: str) -> Cursor:
    """Decode and verify an opaque cursor; raise SearchError (-> 422).

    Cursors produced before the persistent-sequence snapshot (the ``evc1``
    rowid-based format) are rejected outright: their high-water mark can
    have been invalidated by rowid reuse, so their pages must be restarted.
    """
    if not isinstance(raw, str) or not raw.startswith(CURSOR_PREFIX):
        if isinstance(raw, str) and raw.startswith(LEGACY_CURSOR_PREFIX):
            raise SearchError(
                "cursor is from an incompatible older version: restart the "
                "search without a cursor"
            )
        raise SearchError("invalid cursor: unrecognized format")
    packed = raw[len(CURSOR_PREFIX):]
    if packed.count(".") != 1:
        raise SearchError("invalid cursor: malformed token")
    payload, signature = packed.rsplit(".", 1)
    try:
        body = base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise SearchError("invalid cursor: malformed token") from exc
    if not hmac.compare_digest(signature, _sign(body)[:32]):
        raise SearchError("invalid cursor: signature mismatch")
    try:
        data = json.loads(body)
        version = int(data["v"])
        if version != CURSOR_VERSION:
            raise SearchError(
                "cursor is from an incompatible older version: restart the "
                "search without a cursor"
            )
        return Cursor(
            version=version,
            fingerprint=str(data["f"]),
            high_water=int(data["w"]),
            occurred_at=str(data["t"]),
            event_id=str(data["e"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SearchError("invalid cursor: malformed token") from exc


# ------------------------------------------------------------- SQL scanning

def current_high_water(conn: sqlite3.Connection) -> int:
    """Maximum ``seq`` currently visible; 0 for an empty table.

    Captured by the first page of a search and then carried in the cursor
    so rows inserted later can never leak into subsequent pages. ``seq``
    is an AUTOINCREMENT value: deleting the current maximum does not free
    the value for reuse, so the mark stays valid for the cursor's lifetime.
    """
    row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM events").fetchone()
    return int(row["m"])


def _window_clauses(
    query: SearchQuery, high_water: int, after: tuple[str, str] | None
) -> tuple[str, list[Any]]:
    """Scalar predicates for one scan batch: high-water mark, column filters,
    the half-open occurred_at interval and the moving keyset window."""
    clauses: list[str] = ["seq <= ?"]
    params: list[Any] = [high_water]
    if query.device_id is not None:
        clauses.append("device_id = ?")
        params.append(query.device_id)
    if query.event_type is not None:
        clauses.append("event_type IS ?")
        params.append(query.event_type)
    if query.schema_version is not None:
        clauses.append("schema_version IS ?")
        params.append(query.schema_version)
    if query.occurred_start is not None:
        clauses.append("occurred_at >= ?")
        params.append(to_storage(normalize_ts(query.occurred_start)))
    if query.occurred_end is not None:
        clauses.append("occurred_at < ?")
        params.append(to_storage(normalize_ts(query.occurred_end)))
    if after is not None:
        # Strict keyset continuation after (occurred_at, event_id).
        clauses.append("(occurred_at > ? OR (occurred_at = ? AND event_id > ?))")
        params.extend((after[0], after[0], after[1]))
    return " AND ".join(clauses), params


_SELECT_COLUMNS = (
    "seq, event_id, device_id, occurred_at, event_type, "
    "schema_version, payload"
)
# A single batch stays small; non-matching payload rows are skipped and the
# scan resumes with a moved keyset window until a full page accumulates.
_BATCH_SIZE = 1000


def scan_page(
    conn: sqlite3.Connection,
    query: SearchQuery,
    high_water: int,
    after: tuple[str, str] | None,
    limit: int,
) -> tuple[list[sqlite3.Row], bool]:
    """Collect up to ``limit`` matching rows in sorted order.

    The scan gathers ``limit + 1`` matches internally: the extra match is
    discarded and only used to decide whether another (non-empty) page
    exists, so callers never observe empty intermediate pages even when
    large stretches of the scalar window fail the payload conditions.
    Returns ``(rows, has_next_page)``.
    """
    gathered: list[sqlite3.Row] = []
    while len(gathered) <= limit:
        where, params = _window_clauses(query, high_water, after)
        rows = conn.execute(
            f"SELECT {_SELECT_COLUMNS} FROM events WHERE {where} "
            "ORDER BY occurred_at, event_id, seq LIMIT ?",
            [*params, _BATCH_SIZE],
        ).fetchall()
        if not rows:
            break
        for row in rows:
            after = (row["occurred_at"], row["event_id"])
            if row_matches(row["payload"], query.conditions):
                gathered.append(row)
                if len(gathered) > limit:
                    break
        if len(rows) < _BATCH_SIZE:
            # The last row of this batch is also the last row of the scalar
            # window: no further batch (and hence no further match) exists.
            break

    has_next_page = len(gathered) > limit
    return gathered[:limit], has_next_page

"""Session evolution snapshots: frozen session sets and their diffs.

A *session snapshot* freezes one device's sessions exactly as they are at
one arrival high-water (``events.seq``): the snapshot stores, inside one
write transaction, the high-water itself and every session derived from
the events with ``seq <= high_water`` -- each session with its ordered
members serialized in full (the same fields the session API returns).

The frozen copy is deliberately independent of the live ``events`` rows:
a late event that later bridges two sessions, and a retention cleanup
that deletes events, both mutate only the *live* grouping. A stored
snapshot -- its sessions, member set and session ids -- is never
rewritten. Because each member is copied (instead of referenced by
``event_id``), snapshots also stay readable after cleanup removed the
underlying event rows.

Each snapshot also pins the ``gap_seconds`` configuration it was built
with. Session boundaries depend on the gap, so two snapshots built with
different gaps are never compared: the diff endpoint rejects them with
``409``.

Snapshot ids are client-chosen and globally unique: re-POSTing the same
id for the same device replays the original snapshot idempotently, while
reusing an id on another device is a ``409`` conflict.

The diff between two snapshots of one device is built from the bipartite
graph whose left vertices are the old sessions, right vertices the new
sessions, and whose edges connect sessions sharing at least one
``event_id``. Every connected component is classified as ``unchanged``
(one old, one new, identical members), ``changed`` (one old, one new,
different members), ``merged`` (several old, one new), ``split`` (one
old, several new), ``reorganized`` (several old *and* several new),
``removed`` (old only, no shared events) or ``created`` (new only).

Apart from the small SQL helpers taking an open connection, the pure
diff helper holds no state; callers (the FastAPI endpoints) own the
transactions, exactly as for the other storage-backed modules.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

from .ingestion import canonical_json
from .sessions import (
    Event,
    from_storage,
    group_sessions,
    session_id,
    to_storage,
)

# Full column lists; kept here so every SELECT reads a stable shape.
SNAPSHOT_COLUMNS = (
    "snapshot_id, device_id, event_high_water, gap_seconds, session_count, "
    "created_at"
)
SNAPSHOT_SESSION_COLUMNS = (
    "snapshot_id, session_index, session_id, started_at, ended_at, members_json"
)


class SnapshotConflict(Exception):
    """A snapshot constraint was violated (HTTP 409 semantics).

    ``detail`` is the exact response detail payload -- a plain string or a
    structured object -- that the route re-raises as ``409`` after rolling
    back the transaction.
    """

    def __init__(self, detail: Any):
        super().__init__(detail if isinstance(detail, str) else detail.get("message"))
        self.detail = detail


@dataclass(frozen=True)
class SnapshotMember:
    """One frozen session member, copied from the live event row."""

    event_id: str
    occurred_at: datetime
    event_type: str | None = None
    schema_version: str | None = None
    payload: object | None = None


@dataclass(frozen=True)
class SnapshotSession:
    """One frozen session: deterministic id plus its ordered member copy."""

    session_id: str
    started_at: datetime
    ended_at: datetime
    members: tuple[SnapshotMember, ...]

    @property
    def event_count(self) -> int:
        return len(self.members)

    @property
    def event_ids(self) -> frozenset[str]:
        return frozenset(member.event_id for member in self.members)


@dataclass(frozen=True)
class SessionSnapshot:
    """One full snapshot: high-water, gap configuration and its sessions."""

    snapshot_id: str
    device_id: str
    event_high_water: int
    gap_seconds: float
    created_at: datetime
    sessions: tuple[SnapshotSession, ...]

    @property
    def session_count(self) -> int:
        return len(self.sessions)


# ------------------------------------------------------------- serialization


def _member_to_storage(member: Event) -> dict[str, Any]:
    return {
        "event_id": member.event_id,
        "occurred_at": to_storage(member.occurred_at),
        "event_type": member.event_type,
        "schema_version": member.schema_version,
        "payload": member.payload,
    }


def _member_from_storage(raw: dict[str, Any]) -> SnapshotMember:
    return SnapshotMember(
        event_id=raw["event_id"],
        occurred_at=from_storage(raw["occurred_at"]),
        event_type=raw.get("event_type"),
        schema_version=raw.get("schema_version"),
        payload=raw.get("payload"),
    )


def _session_to_storage(members: Sequence[Event]) -> dict[str, Any]:
    """Canonical JSON document of one frozen session's ordered members."""
    return {
        "session_id": session_id(members[0].device_id, members),
        "started_at": to_storage(members[0].occurred_at),
        "ended_at": to_storage(members[-1].occurred_at),
        "members": [_member_to_storage(member) for member in members],
    }


# --------------------------------------------------------------- SQL helpers


def get_snapshot_row(
    conn: sqlite3.Connection, snapshot_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {SNAPSHOT_COLUMNS} FROM session_snapshots "
        "WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()


def list_snapshot_rows(
    conn: sqlite3.Connection, device_id: str
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            f"SELECT {SNAPSHOT_COLUMNS} FROM session_snapshots "
            "WHERE device_id = ? ORDER BY created_at, snapshot_id",
            (device_id,),
        ).fetchall()
    )


def _snapshot_session_rows(
    conn: sqlite3.Connection, snapshot_id: str
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            f"SELECT {SNAPSHOT_SESSION_COLUMNS} FROM session_snapshot_sessions "
            "WHERE snapshot_id = ? ORDER BY session_index",
            (snapshot_id,),
        ).fetchall()
    )


def load_snapshot(
    conn: sqlite3.Connection, snapshot_id: str
) -> SessionSnapshot | None:
    """Load one full snapshot (header plus frozen ordered sessions)."""
    row = get_snapshot_row(conn, snapshot_id)
    if row is None:
        return None
    return _snapshot_from_row(conn, row)


def _snapshot_from_row(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> SessionSnapshot:
    device_id = row["device_id"]
    sessions: list[SnapshotSession] = []
    for session_row in _snapshot_session_rows(conn, row["snapshot_id"]):
        document = json.loads(session_row["members_json"])
        members = tuple(
            _member_from_storage(member) for member in document["members"]
        )
        sessions.append(
            SnapshotSession(
                session_id=session_row["session_id"],
                started_at=from_storage(session_row["started_at"]),
                ended_at=from_storage(session_row["ended_at"]),
                members=members,
            )
        )
    return SessionSnapshot(
        snapshot_id=row["snapshot_id"],
        device_id=device_id,
        event_high_water=int(row["event_high_water"]),
        gap_seconds=float(row["gap_seconds"]),
        created_at=from_storage(row["created_at"]),
        sessions=tuple(sessions),
    )


def _events_at_or_below(
    conn: sqlite3.Connection, device_id: str, high_water: int
) -> list[Event]:
    """Device events visible at the frozen high-water, in session order.

    Only ``events`` rows with ``seq <= high_water`` are read: an event
    inserted by a concurrent transaction (impossible while the write lock
    is held) or later (a late arrival) is outside the frozen window.
    """
    rows = conn.execute(
        "SELECT event_id, occurred_at, event_type, schema_version, payload "
        "FROM events WHERE device_id = ? AND seq <= ? "
        "ORDER BY occurred_at, event_id",
        (device_id, high_water),
    ).fetchall()
    return [
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


def device_event_high_water(conn: sqlite3.Connection, device_id: str) -> int:
    """Maximum arrival ``seq`` of the device's events; 0 when there are none."""
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE device_id = ?",
        (device_id,),
    ).fetchone()
    return int(row["m"])


def create_snapshot(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
    device_id: str,
    gap_seconds: float,
    created_at: datetime,
) -> tuple[SessionSnapshot, bool]:
    """Freeze the device's current sessions under one new high-water.

    Must run inside the caller's open ``BEGIN IMMEDIATE`` transaction: the
    high-water read, the event-window read and the inserts share the same
    write lock, so a concurrent writer can never interleave between the
    frozen mark and the frozen session rows.

    Returns ``(snapshot, created)``. ``created`` is False for a repeated
    POST of the same id on the same device, in which case the originally
    stored snapshot is returned untouched. A same id on another device
    raises :class:`SnapshotConflict`; nothing is written.
    """
    existing = get_snapshot_row(conn, snapshot_id)
    if existing is not None:
        if existing["device_id"] != device_id:
            raise SnapshotConflict(
                {
                    "message": (
                        f"snapshot_id {snapshot_id!r} already belongs to "
                        f"device {existing['device_id']!r}; snapshot ids are "
                        "global and cannot be reused across devices"
                    ),
                    "reason": "snapshot_id_device_conflict",
                    "snapshot_id": snapshot_id,
                    "bound_device_id": existing["device_id"],
                    "conflicting_device_id": device_id,
                }
            )
        loaded = load_snapshot(conn, snapshot_id)
        assert loaded is not None
        return loaded, False

    # Freeze the monotonic arrival sequence first; every session below is
    # derived strictly from this prefix.
    high_water = device_event_high_water(conn, device_id)
    events = _events_at_or_below(conn, device_id, high_water)
    grouped = group_sessions(events, gap_seconds)

    conn.execute(
        "INSERT INTO session_snapshots (snapshot_id, device_id, "
        "event_high_water, gap_seconds, session_count, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            snapshot_id,
            device_id,
            high_water,
            float(gap_seconds),
            len(grouped),
            to_storage(created_at),
        ),
    )
    for index, members in enumerate(grouped):
        document = _session_to_storage(members)
        conn.execute(
            "INSERT INTO session_snapshot_sessions (snapshot_id, "
            "session_index, session_id, started_at, ended_at, members_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                snapshot_id,
                index,
                document["session_id"],
                document["started_at"],
                document["ended_at"],
                canonical_json(document),
            ),
        )
    snapshot = load_snapshot(conn, snapshot_id)
    assert snapshot is not None
    return snapshot, True


# ------------------------------------------------------------- session diff

CHANGE_UNCHANGED = "unchanged"
CHANGE_CHANGED = "changed"
CHANGE_MERGED = "merged"
CHANGE_SPLIT = "split"
CHANGE_REORGANIZED = "reorganized"
CHANGE_CREATED = "created"
CHANGE_REMOVED = "removed"

# Vertex-key prefixes keep old/new session ids apart inside the union-find
# even though both spaces hash the same device id.
_OLD_PREFIX = "o:"
_NEW_PREFIX = "n:"


def _member_order_key(member: SnapshotMember) -> tuple[str, str]:
    return to_storage(member.occurred_at), member.event_id


def _classify_component(
    old_sessions: Sequence[SnapshotSession],
    new_sessions: Sequence[SnapshotSession],
) -> str:
    old_count = len(old_sessions)
    new_count = len(new_sessions)
    if old_count == 1 and new_count == 1:
        return (
            CHANGE_UNCHANGED
            if old_sessions[0].event_ids == new_sessions[0].event_ids
            else CHANGE_CHANGED
        )
    if old_count == 0 and new_count >= 1:
        return CHANGE_CREATED
    if new_count == 0 and old_count >= 1:
        return CHANGE_REMOVED
    if old_count == 1 and new_count > 1:
        return CHANGE_SPLIT
    if old_count > 1 and new_count == 1:
        return CHANGE_MERGED
    # Many old and many new share events transitively: neither a plain
    # merge nor a plain split -- the component is a many-to-many reorg.
    return CHANGE_REORGANIZED


def _component_sort_key(
    old_sessions: Sequence[SnapshotSession],
    new_sessions: Sequence[SnapshotSession],
) -> tuple[Any, ...]:
    """Deterministic component ordering, independent of component type."""
    starts = sorted(
        [to_storage(s.started_at) for s in old_sessions]
        + [to_storage(s.started_at) for s in new_sessions]
    )
    ids = sorted(
        [s.session_id for s in old_sessions]
        + [s.session_id for s in new_sessions]
    )
    return (starts[0] if starts else "", ids[0] if ids else "")


def diff_snapshots(
    old: SessionSnapshot, new: SessionSnapshot
) -> list[dict[str, Any]]:
    """Compare two frozen snapshots of the same device.

    The comparison is built only from the stored, frozen session copies
    (live rows are never read), so late arrivals and cleanup cannot change
    the outcome of an earlier comparison. The caller is responsible for
    checking that both snapshots belong to one device and were frozen with
    the same ``gap_seconds``.

    Returns one entry per connected component of the old/new session
    bipartite graph, sorted deterministically. Each entry carries the
    component ``change``, the old/new session ids (empty list for a
    created/removed side) and the members added to / removed from the
    component's sessions. Member id lists are sorted by the same
    ``(occurred_at, event_id)`` order as sessions.
    """
    # Build the bipartite graph on shared event_ids. Event ids are globally
    # unique and immutable, so an event links at most one old and one new
    # session at each side.
    old_by_event: dict[str, SnapshotSession] = {}
    new_by_event: dict[str, SnapshotSession] = {}
    for session in old.sessions:
        for member in session.members:
            old_by_event[member.event_id] = session
    for session in new.sessions:
        for member in session.members:
            new_by_event[member.event_id] = session

    parent: dict[str, str] = {}

    def find(key: str) -> str:
        root = key
        while parent[root] != root:
            root = parent[root]
        while parent[key] != root:
            parent[key], key = root, parent[key]
        return root

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            # Union by the stable session-id order so component identity
            # never depends on edge insertion order.
            if left_root < right_root:
                parent[right_root] = left_root
            else:
                parent[left_root] = right_root

    for session in old.sessions:
        parent[_OLD_PREFIX + session.session_id] = _OLD_PREFIX + session.session_id
    for session in new.sessions:
        parent[_NEW_PREFIX + session.session_id] = _NEW_PREFIX + session.session_id

    shared_event_ids = sorted(set(old_by_event) & set(new_by_event))
    for event_id in shared_event_ids:
        union(
            _OLD_PREFIX + old_by_event[event_id].session_id,
            _NEW_PREFIX + new_by_event[event_id].session_id,
        )

    components: dict[str, dict[str, dict[str, SnapshotSession]]] = {}
    for session in old.sessions:
        root = find(_OLD_PREFIX + session.session_id)
        components.setdefault(root, {"old": {}, "new": {}})["old"][
            session.session_id
        ] = session
    for session in new.sessions:
        root = find(_NEW_PREFIX + session.session_id)
        components.setdefault(root, {"old": {}, "new": {}})["new"][
            session.session_id
        ] = session

    entries: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for sides in components.values():
        old_sessions = sorted(
            sides["old"].values(), key=lambda s: (to_storage(s.started_at), s.session_id)
        )
        new_sessions = sorted(
            sides["new"].values(), key=lambda s: (to_storage(s.started_at), s.session_id)
        )
        change = _classify_component(old_sessions, new_sessions)

        old_member_ids = {
            member.event_id
            for session in old_sessions
            for member in session.members
        }
        new_member_ids = {
            member.event_id
            for session in new_sessions
            for member in session.members
        }
        added_members = sorted(
            (
                member
                for session in new_sessions
                for member in session.members
                if member.event_id not in old_member_ids
            ),
            key=_member_order_key,
        )
        removed_members = sorted(
            (
                member
                for session in old_sessions
                for member in session.members
                if member.event_id not in new_member_ids
            ),
            key=_member_order_key,
        )
        entries.append(
            (
                _component_sort_key(old_sessions, new_sessions),
                {
                    "change": change,
                    "old_session_ids": [s.session_id for s in old_sessions],
                    "new_session_ids": [s.session_id for s in new_sessions],
                    "added_member_event_ids": [m.event_id for m in added_members],
                    "removed_member_event_ids": [m.event_id for m in removed_members],
                },
            )
        )

    entries.sort(key=lambda item: item[0])
    return [entry for _, entry in entries]

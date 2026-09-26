"""Pure session-grouping logic.

Sessions are always derived from the full, sorted event set of a device,
so the arrival order of events can never influence the grouping result.
A late event simply takes its sorted position and may bridge two groups
that were previously separate.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence


@dataclass(frozen=True)
class Event:
    device_id: str
    event_id: str
    occurred_at: datetime  # always timezone-aware, normalized to UTC
    event_type: str | None = None
    schema_version: str | None = None
    payload: object | None = None  # parsed JSON, absent for legacy events
    seq: int | None = None  # persistent arrival sequence; used by alert rules


def normalize_ts(dt: datetime) -> datetime:
    """Normalize a timestamp to aware UTC. Naive input is assumed to be UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_storage(dt: datetime) -> str:
    """Serialize as UTC ISO-8601. Lexicographic order == chronological order."""
    return normalize_ts(dt).isoformat()


def from_storage(raw: str) -> datetime:
    return normalize_ts(datetime.fromisoformat(raw))


def group_sessions(events: Sequence[Event], gap_seconds: float) -> list[list[Event]]:
    """Group events into sessions.

    `events` must be sorted by (occurred_at, event_id). Two adjacent events
    belong to the same session iff their gap is <= `gap_seconds`; grouping is
    transitive across a chain of such events.
    """
    sessions: list[list[Event]] = []
    current: list[Event] = []
    previous: Event | None = None
    for event in events:
        if previous is not None:
            gap = (event.occurred_at - previous.occurred_at).total_seconds()
            if gap > gap_seconds:
                sessions.append(current)
                current = []
        current.append(event)
        previous = event
    if current:
        sessions.append(current)
    return sessions


def session_id(device_id: str, members: Sequence[Event]) -> str:
    """Deterministic session identifier.

    SHA-256 over the device id and the member list sorted by
    (occurred_at, event_id). Members must already be sorted; the caller
    guarantees this by construction.
    """
    digest = hashlib.sha256()
    digest.update(device_id.encode("utf-8"))
    for member in members:
        digest.update(b"\x00")
        digest.update(member.occurred_at.isoformat().encode("utf-8"))
        digest.update(b"\x1f")
        digest.update(member.event_id.encode("utf-8"))
    return digest.hexdigest()

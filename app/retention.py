"""Pure retention-cleanup logic: candidate selection and plan digest.

Cleanup is a two-phase, confirmable process. Plan creation snapshots the
events that satisfy the retention selection (one device, strictly before a
cutoff, outside every protected half-open ``[start, end)`` window) but
deletes nothing. Confirmation recomputes the selection inside a write
transaction and only deletes when it still matches the snapshot exactly.

Everything in this module is pure: callers persist the snapshots and own
the transactions.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime

from .sessions import Event, normalize_ts, to_storage


def normalize_windows(
    windows: Sequence[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    """Normalize windows to aware UTC and return them sorted.

    Window order never matters semantically (an event is protected when it
    falls in *any* window), so sorting gives the plan digest a canonical
    form independent of request ordering.
    """
    normalized = sorted(
        (normalize_ts(start), normalize_ts(end)) for start, end in windows
    )
    for start, end in normalized:
        if start >= end:
            raise ValueError(
                f"protected window {to_storage(start)!r}..{to_storage(end)!r} "
                "is empty: start must be strictly before end"
            )
    return normalized


def is_protected(occurred_at: datetime, windows: Sequence[tuple[datetime, datetime]]) -> bool:
    """True when the instant lies in any half-open ``[start, end)`` window."""
    return any(start <= occurred_at < end for start, end in windows)


def select_candidates(
    events: Sequence[Event],
    before: datetime,
    windows: Sequence[tuple[datetime, datetime]],
) -> list[Event]:
    """Return cleanup candidates among already-sorted device events.

    An event is a candidate when ``occurred_at < before`` (the cutoff is
    exclusive) and it is not covered by a protected window. Input events
    are sorted by (occurred_at, event_id) and the result preserves that
    order.
    """
    cutoff = normalize_ts(before)
    return [
        event
        for event in events
        if event.occurred_at < cutoff and not is_protected(event.occurred_at, windows)
    ]


def windows_to_storage(
    windows: Sequence[tuple[datetime, datetime]],
) -> list[list[str]]:
    return [[to_storage(start), to_storage(end)] for start, end in windows]


def plan_digest(
    device_id: str,
    before: datetime,
    windows: Sequence[tuple[datetime, datetime]],
    event_ids: Sequence[str],
) -> str:
    """SHA-256 binding the selection criteria to the ordered candidate ids.

    Candidates are ordered by (occurred_at, event_id); the digest therefore
    also covers count and order, so a different candidate set can never
    reproduce the same plan digest.
    """
    document = {
        "device_id": device_id,
        "before": to_storage(before),
        "protected_windows": windows_to_storage(windows),
        "event_ids": list(event_ids),
    }
    material = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(
        b"device-event-retention-plan:1\n" + material.encode("utf-8")
    ).hexdigest()

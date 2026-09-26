"""Versioned, immutable device alert rules and their fired alarm instances.

Rules are created *per device* under a ``rule_id`` and are append-only:
every version is an immutable document (matched ``event_type`` plus up to
five AND payload conditions located with RFC 6901 JSON Pointers) together
with a sliding time window, a required number of consecutive matches and a
cooldown duration. Only the latest version of a rule is *active*, but every
version -- including superseded ones -- keeps evaluating the slice of
events that belongs to its event-time **era**, so a late event sorting into
an old era is replayed against the version that owned that era instead of
being silently swallowed by the newest version.

Eras partition event time ``(occurred_at, event_id)`` (a total order):

* when version *v* (v >= 2) is created, the device's current event-time
  frontier ``F_v = max key present at creation`` pins its *open* lower
  bound -- it never sees keys ``<= F_v`` (no backfill of the already known
  event set);
* when the successor *v+1* is created, ``F_{v+1}`` simultaneously closes
  version *v*: its frozen slice covers ``key <= F_{v+1}``;
* ``created_seq`` still pins the arrival high-water at creation: events
  already present can never feed a version, even when an even later
  arrival sorts into its era by event time. This keeps the no-backfill
  guarantee while making the final state independent of arrival order.

Firing semantics are evaluated in **event time** (``occurred_at``), never
in wall-clock time, because events may arrive out of order:

* events are processed in ``(occurred_at, event_id)`` order;
* a non-matching event resets the consecutive-hit streak, and matches
  older than ``window_seconds`` before the current event fall out of the
  sliding window;
* once ``consecutive`` matches sit inside the window an alarm *fires*;
* while the previous firing is inside its cooldown (or has been
  acknowledged and the event did not arrive after ``acked_seq``), further
  firings are suppressed -- at most one open alarm exists at a time;
* acknowledging an alarm persists an ``acked_seq`` arrival high-water
  mark, so the next eligible firing (a later arrival) opens a new alarm;
  once the cooldown elapses in event time the alarm reopens on its own.

Every event write recomputes **every version** of every rule of that
device (latest and frozen alike) *inside the same write transaction* from
the ordered event set and the persisted era frontiers, so the result never
depends on arrival order: an event arriving late after a newer version was
created takes its era membership from ``occurred_at`` and closes the streak
of the version that owned that era. The deterministic ``trigger_key``
binds an alarm to the exact triggering event, making the reconciliation
idempotent: replaying the same ``event_id`` and re-running the evaluation
produce no duplicate alarms. Firings that disappear after a recomputation
(a late event invalidated the streak) move to the terminal
``invalidated`` state unless they were already acknowledged; acknowledged
history is always preserved.

Apart from the small SQL helpers taking an open connection, the
evaluation is pure: callers (the FastAPI endpoints) own the transactions.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, NamedTuple, Sequence

from .sessions import Event, from_storage, to_storage
from .search import (
    Condition,
    _MISSING,
    evaluate_condition,
    parse_pointer,
    resolve_pointer,
)

# Stored lifecycle states of an alarm instance. ``active`` is the only
# non-terminal state; the externally visible status additionally derives
# ``expired`` (an unacked alarm whose cooldown has elapsed in wall time).
STATE_ACTIVE = "active"
STATE_ACKED = "acked"  # terminal: acknowledged by an operator
STATE_INVALIDATED = "invalidated"  # terminal: recomputation removed the firing

# Derived statuses reported by the API.
STATUS_ACTIVE = "active"
STATUS_EXPIRED = "expired"  # cooldown elapsed in wall time, not yet acked
STATUS_ACKED = "acked"
STATUS_INVALIDATED = "invalidated"

ALARM_RULE_COLUMNS = (
    "id, device_id, rule_id, version, event_type, conditions_json, "
    "window_seconds, consecutive, cooldown_seconds, created_seq, "
    "era_start_occurred_at, era_start_event_id, "
    "era_end_occurred_at, era_end_event_id, created_at"
)
ALARM_COLUMNS = (
    "id, alarm_id, device_id, rule_id, rule_version_id, rule_version, "
    "trigger_key, state, fired_at, first_match_at, last_match_at, "
    "cooldown_until, trigger_event_id, member_event_ids, seq_start, "
    "seq_end, acked_at, acked_seq, invalidated_at, created_at, updated_at"
)


# --------------------------------------------------------------------- model


class StoredRule(NamedTuple):
    id: int
    device_id: str
    rule_id: str
    version: int
    event_type: str
    conditions: tuple[Condition, ...]
    window_seconds: float
    consecutive: int
    cooldown_seconds: float
    created_seq: int
    era_start: tuple[datetime, str] | None
    era_end: tuple[datetime, str] | None
    created_at: datetime


@dataclass
class ComputedAlarm:
    """One firing produced by the pure evaluation."""

    trigger_key: str
    fired_at: datetime  # occurred_at of the triggering (last) event
    first_match_at: datetime
    last_match_at: datetime  # == fired_at
    cooldown_until: datetime
    trigger_event_id: str
    member_event_ids: tuple[str, ...]
    seq_start: int
    seq_end: int


@dataclass
class GateState:
    """Suppression boundary inherited from the previous evaluation pass.

    Exactly one of the two boundaries is live at a time: an unacked firing
    suppresses in event time until ``cooldown_until``; an acknowledged
    firing suppresses arrivals up to and including ``acked_seq`` and then
    reopens regardless of the remaining cooldown.
    """

    state: str  # STATE_ACTIVE or STATE_ACKED
    fired_at: datetime | None
    cooldown_until: datetime | None
    acked_seq: int | None


@dataclass
class ComputedProgress:
    """Persisted consecutive-hit progress after the evaluated event slice."""

    streak_event_ids: tuple[str, ...]
    streak_times: tuple[str, ...]
    gate: GateState | None


def _frontier_from_row(
    occurred_col: str, event_col: str, row: sqlite3.Row
) -> tuple[datetime, str] | None:
    """The ``(occurred_at, event_id)`` frontier columns of a rule row.

    ``None`` when the bound is unbounded on that side (version 1 has no
    lower era bound; the latest version has no upper era bound).
    """
    raw = row[occurred_col]
    if raw is None:
        return None
    return (from_storage(raw), row[event_col])


def rule_from_row(row: sqlite3.Row) -> StoredRule:
    return StoredRule(
        id=row["id"],
        device_id=row["device_id"],
        rule_id=row["rule_id"],
        version=row["version"],
        event_type=row["event_type"],
        conditions=tuple(
            Condition(
                pointer=item["pointer"],
                operator=item["operator"],
                value=item["value"],
            )
            for item in json.loads(row["conditions_json"])
        ),
        window_seconds=float(row["window_seconds"]),
        consecutive=int(row["consecutive"]),
        cooldown_seconds=float(row["cooldown_seconds"]),
        created_seq=int(row["created_seq"]),
        era_start=_frontier_from_row(
            "era_start_occurred_at", "era_start_event_id", row
        ),
        era_end=_frontier_from_row(
            "era_end_occurred_at", "era_end_event_id", row
        ),
        created_at=from_storage(row["created_at"]),
    )


def conditions_to_storage(
    conditions: Sequence[Condition],
) -> list[dict[str, Any]]:
    return [
        {"pointer": c.pointer, "operator": c.operator, "value": c.value}
        for c in conditions
    ]


def new_alarm_id() -> str:
    return uuid.uuid4().hex


# --------------------------------------------------------------- evaluation


def _event_matches(event: Event, rule: StoredRule) -> bool:
    """Match one event against a rule version.

    The event type must be exactly equal and every AND payload condition
    must hold. Legacy/no-payload events (``payload is None``) cannot match
    a payload rule: every pointer, including the root, resolves as missing.
    """
    if event.event_type != rule.event_type:
        return False
    if not rule.conditions:
        return event.payload is not None
    if event.payload is None:
        return False
    for condition in rule.conditions:
        tokens = parse_pointer(condition.pointer)
        value: Any = (
            resolve_pointer(event.payload, tokens)
            if event.payload is not None
            else _MISSING
        )
        if not evaluate_condition(value, condition):
            return False
    return True


def _trigger_key(rule_id: str, version: int, trigger_event_id: str) -> str:
    """Deterministic identity of a firing: rule version + triggering event.

    Because the trigger event is a function of the ordered, slice-limited
    event set alone, re-running the evaluation (or replaying the same
    event_id) yields the same key and can never create a second alarm
    instance.
    """
    return hashlib.sha256(
        b"device-alarm-trigger:1\n"
        + rule_id.encode("utf-8")
        + b"\x00"
        + str(version).encode("ascii")
        + b"\x00"
        + trigger_event_id.encode("utf-8")
    ).hexdigest()


def evaluate_rule(
    rule: StoredRule,
    events: Sequence[Event],
    seed_acked_seq: int | None,
    *,
    honor_cooldown: bool = True,
) -> tuple[list[ComputedAlarm], ComputedProgress]:
    """Evaluate one rule version against its ordered, era-limited events.

    ``events`` must already be restricted to the version's slice: arrivals
    later than ``created_seq`` whose ``(occurred_at, event_id)`` key lies
    inside the version's event-time era, sorted by
    ``(occurred_at, event_id)``. The reconciler performs that filtering.
    ``seed_acked_seq`` is the arrival high-water mark left by the most
    recent acknowledgement (``None`` when the rule was never acked): every
    firing whose triggering event arrived no later than it stays
    suppressed, so acknowledging a firing can never be undone by a later
    full recomputation. An *unacked* cooldown gate needs no seeding -- a
    from-scratch recomputation regenerates it deterministically as it
    walks the events (when ``honor_cooldown`` is set), which is also why
    invalidating a firing drops its cooldown cleanly.

    ``honor_cooldown=False`` produces the *structural* firing set: every
    run that reaches the threshold, ignoring the event-time cooldown. The
    reconciler uses it to tell a gate-suppressed (still valid) alarm from
    one removed outright by a late event rewrite.

    Returns the new firings of this pass in event-time order and the streak
    / gate progress to persist.
    """
    window = timedelta(seconds=rule.window_seconds)
    needed = rule.consecutive
    # Sliding window of the current streak: (occurred_at, event_id, seq).
    streak: deque[tuple[datetime, str, int]] = deque()
    fired: list[ComputedAlarm] = []
    gate_cooldown_until: datetime | None = None
    gate_acked_seq: int | None = seed_acked_seq

    def is_suppressed(occurred: datetime, event_seq: int) -> bool:
        # The structural pass (honor_cooldown=False) ignores every gate:
        # cooldown and the acked high-water suppress firings but do not
        # change what the ordered event set structurally supports.
        if not honor_cooldown:
            return False
        if gate_cooldown_until is not None and occurred < gate_cooldown_until:
            return True
        if gate_acked_seq is not None and event_seq <= gate_acked_seq:
            return True
        return False

    for event in events:
        assert event.seq is not None
        if _event_matches(event, rule):
            # Drop streak entries outside the sliding window ending here.
            while streak and event.occurred_at - streak[0][0] > window:
                streak.popleft()
            streak.append((event.occurred_at, event.event_id, event.seq))
            if len(streak) >= needed:
                first_at, _first_id, first_seq = streak[0]
                cooldown_until = event.occurred_at + timedelta(
                    seconds=rule.cooldown_seconds
                )
                if not is_suppressed(event.occurred_at, event.seq):
                    member_ids = tuple(item[1] for item in streak)
                    fired.append(
                        ComputedAlarm(
                            trigger_key=_trigger_key(
                                rule.rule_id, rule.version, event.event_id
                            ),
                            fired_at=event.occurred_at,
                            first_match_at=first_at,
                            last_match_at=event.occurred_at,
                            cooldown_until=cooldown_until,
                            trigger_event_id=event.event_id,
                            member_event_ids=member_ids,
                            seq_start=first_seq,
                            seq_end=event.seq,
                        )
                    )
                    gate_cooldown_until = cooldown_until
                    gate_acked_seq = None
                # The threshold streak is consumed either way: only one
                # alarm can come out of one consecutive run.
                streak.clear()
        else:
            # Consecutive hits: any non-matching event resets the run.
            streak.clear()

    if gate_cooldown_until is not None:
        gate = GateState(
            state=STATE_ACTIVE,
            fired_at=gate_cooldown_until
            - timedelta(seconds=rule.cooldown_seconds),
            cooldown_until=gate_cooldown_until,
            acked_seq=None,
        )
    elif gate_acked_seq is not None:
        # No new firing overtook the acknowledged boundary; retain it.
        # The gate is purely seq-based, so the acked firing's own event
        # time is irrelevant for suppression (fired_at stays NULL).
        gate = GateState(
            state=STATE_ACKED,
            fired_at=None,
            cooldown_until=None,
            acked_seq=gate_acked_seq,
        )
    else:
        gate = None

    progress = ComputedProgress(
        streak_event_ids=tuple(item[1] for item in streak),
        streak_times=tuple(to_storage(item[0]) for item in streak),
        gate=gate,
    )
    return fired, progress


# ------------------------------------------------------------- persistence


def get_latest_version(
    conn: sqlite3.Connection, device_id: str, rule_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {ALARM_RULE_COLUMNS} FROM alarm_rules "
        "WHERE device_id = ? AND rule_id = ? "
        "ORDER BY version DESC LIMIT 1",
        (device_id, rule_id),
    ).fetchone()


def get_rule_version(
    conn: sqlite3.Connection, device_id: str, rule_id: str, version: int
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {ALARM_RULE_COLUMNS} FROM alarm_rules "
        "WHERE device_id = ? AND rule_id = ? AND version = ?",
        (device_id, rule_id, version),
    ).fetchone()


def list_rules(
    conn: sqlite3.Connection, device_id: str | None = None
) -> list[sqlite3.Row]:
    if device_id is None:
        return conn.execute(
            f"SELECT {ALARM_RULE_COLUMNS} FROM alarm_rules ORDER BY id"
        ).fetchall()
    return conn.execute(
        f"SELECT {ALARM_RULE_COLUMNS} FROM alarm_rules "
        "WHERE device_id = ? ORDER BY rule_id, version",
        (device_id,),
    ).fetchall()


def get_device_rule_versions(
    conn: sqlite3.Connection, device_id: str
) -> list[StoredRule]:
    """Every registered rule version of a device, oldest first.

    Reconciliation replays *all* versions -- frozen ones included --
    because a late event can sort by ``(occurred_at, event_id)`` into a
    superseded version's era. The final row of each rule chain is the
    currently active version.
    """
    rows = conn.execute(
        f"SELECT {ALARM_RULE_COLUMNS} FROM alarm_rules "
        "WHERE device_id = ? ORDER BY rule_id, version",
        (device_id,),
    ).fetchall()
    return [rule_from_row(row) for row in rows]


def get_rule_alarms(
    conn: sqlite3.Connection, rule_version_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        f"SELECT {ALARM_COLUMNS} FROM alarms "
        "WHERE rule_version_id = ? ORDER BY fired_at, id",
        (rule_version_id,),
    ).fetchall()


def get_alarm(
    conn: sqlite3.Connection, device_id: str, alarm_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {ALARM_COLUMNS} FROM alarms "
        "WHERE device_id = ? AND alarm_id = ?",
        (device_id, alarm_id),
    ).fetchone()


def list_device_alarms(
    conn: sqlite3.Connection, device_id: str
) -> list[sqlite3.Row]:
    return conn.execute(
        f"SELECT {ALARM_COLUMNS} FROM alarms WHERE device_id = ? "
        "ORDER BY fired_at DESC, id DESC",
        (device_id,),
    ).fetchall()


# --------------------------------------------------------- reconciliation


def current_event_frontier(
    conn: sqlite3.Connection, device_id: str
) -> tuple[str, str] | None:
    """The device's current max event-time key as storage strings.

    ``None`` when the device has no events. Returning the already-normalized
    storage strings lets callers persist the frontier verbatim into
    ``alarm_rules`` (UTC ISO-8601 strings sort lexicographically).
    """
    row = conn.execute(
        "SELECT occurred_at, event_id FROM events WHERE device_id = ? "
        "ORDER BY occurred_at DESC, event_id DESC LIMIT 1",
        (device_id,),
    ).fetchone()
    return None if row is None else (row["occurred_at"], row["event_id"])


def _load_device_events(
    conn: sqlite3.Connection, device_id: str
) -> list[Event]:
    """All events of a device ordered by event time ``(occurred_at, event_id)``."""
    rows = conn.execute(
        "SELECT seq, event_id, device_id, occurred_at, event_type, "
        "schema_version, payload FROM events WHERE device_id = ? "
        "ORDER BY occurred_at, event_id",
        (device_id,),
    ).fetchall()
    return [
        Event(
            device_id=row["device_id"],
            event_id=row["event_id"],
            occurred_at=from_storage(row["occurred_at"]),
            event_type=row["event_type"],
            schema_version=row["schema_version"],
            payload=json.loads(row["payload"]) if row["payload"] is not None else None,
            seq=row["seq"],
        )
        for row in rows
    ]


def _events_for_version(
    rule: StoredRule, events: Sequence[Event], *, frozen: bool
) -> list[Event]:
    """Restrict an ordered device event set to one version's era slice.

    ``frozen`` marks a version that has a successor and therefore no longer
    owns open-ended event time. An event belongs to the version when:

    * it arrived after the version was created (``seq > created_seq``) --
      events already present at creation never backfill, regardless of
      where their ``occurred_at`` sorts;
    * its key is strictly above the version's lower era frontier pinned at
      the version's own creation (version 1 has no lower bound; a NULL
      lower bound means negative infinity);
    * for a frozen version its key is at or below the successor frontier
      recorded when that successor was created. A NULL upper bound on a
      frozen version means the successor was created while no events
      existed yet: its closed era is empty (upper bound negative infinity).
      The latest version has no upper bound instead.

    Successive frontiers are the max key present at each creation and are
    therefore non-decreasing, so with the creation-seq guard every event
    belongs to at most one version.
    """
    result: list[Event] = []
    for event in events:
        assert event.seq is not None
        if event.seq <= rule.created_seq:
            continue
        key = (event.occurred_at, event.event_id)
        if rule.era_start is not None and key <= rule.era_start:
            continue
        if frozen and rule.era_end is None:
            # Superseded before the device ever saw an event: nothing can
            # sort into its closed era.
            return []
        if rule.era_end is not None and key > rule.era_end:
            continue
        result.append(event)
    return result


def reconcile_device_alarms(
    conn: sqlite3.Connection,
    device_id: str,
    now: datetime,
) -> None:
    """Recompute every rule version of one device from the ordered events.

    Must run inside the caller's open write transaction (the same
    transaction that inserted the triggering event). All versions are
    replayed, not only the active one: an out-of-order arrival whose
    ``occurred_at`` falls inside a superseded version's closed era rewrites
    that version's firings and progress. For each version it:

    * evaluates the version's era slice seeded with the persisted
      acknowledgement high-water;
    * upserts alarm instances keyed by the deterministic trigger_key
      (replays and re-runs therefore never duplicate);
    * moves previously active firings that vanished from the recomputation
      to the terminal ``invalidated`` state (acked history is preserved);
    * persists the open streak and gate as rule-hit progress.
    """
    rules = get_device_rule_versions(conn, device_id)
    if not rules:
        return
    # The last version of every rule_id chain is still active (open-ended
    # era); every earlier version is frozen at its successor's frontier.
    latest_versions = {
        (rule.device_id, rule.rule_id): rule.version for rule in rules
    }
    # Load the ordered event set once; each version then keeps only its
    # era slice. A late arrival is visible to every version, but the era
    # bounds plus the creation-seq guard assign it to exactly one of them.
    events = _load_device_events(conn, device_id)
    for rule in rules:
        frozen = (
            latest_versions[(rule.device_id, rule.rule_id)] != rule.version
        )
        version_events = _events_for_version(rule, events, frozen=frozen)
        stored_alarms = get_rule_alarms(conn, rule.id)
        existing = {row["trigger_key"]: row for row in stored_alarms}

        # The only suppression boundary carried across recomputations is
        # the acknowledgement high-water of the most recently acked alarm.
        # Unacked cooldowns are regenerated by the from-scratch walk below,
        # so an invalidated firing cannot leave a stale cooldown behind.
        acked_marker = max(
            (row for row in stored_alarms if row["state"] == STATE_ACKED),
            key=lambda row: (
                int(row["acked_seq"]) if row["acked_seq"] is not None else -1
            ),
            default=None,
        )
        seed_acked_seq = (
            int(acked_marker["acked_seq"]) if acked_marker is not None else None
        )
        fired, progress = evaluate_rule(rule, version_events, seed_acked_seq)
        # An ungated pass answers a different question: which firings the
        # ordered era slice structurally supports right now (every run
        # reaching the threshold, cooldown and acked high-water ignored).
        # A live alarm absent from both passes was removed by a late event
        # rewrite (invalidate it); one absent only from the gated pass is
        # merely suppressed by a gate and is kept.
        structural, _ = evaluate_rule(
            rule, version_events, None, honor_cooldown=False
        )
        computed_keys = {item.trigger_key for item in fired}
        structural_keys = {item.trigger_key for item in structural}

        for key, row in existing.items():
            if (
                key not in computed_keys
                and key not in structural_keys
                and row["state"] == STATE_ACTIVE
            ):
                conn.execute(
                    "UPDATE alarms SET state = ?, invalidated_at = ?, "
                    "updated_at = ? WHERE id = ? AND state = 'active'",
                    (
                        STATE_INVALIDATED,
                        to_storage(now),
                        to_storage(now),
                        row["id"],
                    ),
                )

        for item in fired:
            stored = existing.get(item.trigger_key)
            if stored is None:
                conn.execute(
                    "INSERT INTO alarms (alarm_id, device_id, rule_id, "
                    "rule_version_id, rule_version, trigger_key, state, "
                    "fired_at, first_match_at, last_match_at, cooldown_until, "
                    "trigger_event_id, member_event_ids, seq_start, seq_end, "
                    "acked_at, acked_seq, invalidated_at, created_at, "
                    "updated_at) VALUES (?, ?, ?, ?, ?, ?, 'active', "
                    "?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)",
                    (
                        new_alarm_id(),
                        device_id,
                        rule.rule_id,
                        rule.id,
                        rule.version,
                        item.trigger_key,
                        to_storage(item.fired_at),
                        to_storage(item.first_match_at),
                        to_storage(item.last_match_at),
                        to_storage(item.cooldown_until),
                        item.trigger_event_id,
                        json.dumps(list(item.member_event_ids)),
                        item.seq_start,
                        item.seq_end,
                        to_storage(now),
                        to_storage(now),
                    ),
                )
            else:
                # The same firing recomputed identically; refresh the
                # mutable bookkeeping but never resurrect a terminal row.
                conn.execute(
                    "UPDATE alarms SET fired_at = ?, first_match_at = ?, "
                    "last_match_at = ?, cooldown_until = ?, "
                    "trigger_event_id = ?, member_event_ids = ?, "
                    "seq_start = ?, seq_end = ?, updated_at = ? "
                    "WHERE id = ? AND state = 'active'",
                    (
                        to_storage(item.fired_at),
                        to_storage(item.first_match_at),
                        to_storage(item.last_match_at),
                        to_storage(item.cooldown_until),
                        item.trigger_event_id,
                        json.dumps(list(item.member_event_ids)),
                        item.seq_start,
                        item.seq_end,
                        to_storage(now),
                        stored["id"],
                    ),
                )

        gate = progress.gate
        conn.execute(
            "INSERT INTO alarm_rule_progress (rule_version_id, window_seconds, "
            "streak_event_ids, streak_times, gate_state, gate_fired_at, "
            "gate_cooldown_until, gate_acked_seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(rule_version_id) DO UPDATE SET "
            "window_seconds = excluded.window_seconds, "
            "streak_event_ids = excluded.streak_event_ids, "
            "streak_times = excluded.streak_times, "
            "gate_state = excluded.gate_state, "
            "gate_fired_at = excluded.gate_fired_at, "
            "gate_cooldown_until = excluded.gate_cooldown_until, "
            "gate_acked_seq = excluded.gate_acked_seq",
            (
                rule.id,
                rule.window_seconds,
                json.dumps(list(progress.streak_event_ids)),
                json.dumps(list(progress.streak_times)),
                gate.state if gate is not None else None,
                (
                    to_storage(gate.fired_at)
                    if gate is not None and gate.fired_at is not None
                    else None
                ),
                (
                    to_storage(gate.cooldown_until)
                    if gate is not None and gate.cooldown_until is not None
                    else None
                ),
                gate.acked_seq if gate is not None else None,
            ),
        )


def current_high_water_seq(conn: sqlite3.Connection) -> int:
    """Maximum arrival sequence; 0 when the events table is empty."""
    row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM events").fetchone()
    return int(row["m"])


def acknowledge_alarm(
    conn: sqlite3.Connection, row: sqlite3.Row, now: datetime
) -> int:
    """Conditionally acknowledge one alarm; returns rows updated (0 or 1).

    Switches the persisted suppression gate of the rule version from the
    firing's event-time cooldown to an ``acked_seq`` arrival high-water
    mark (the current maximum event seq). Events already present can never
    retrigger the rule; a later arrival can open a new alarm regardless of
    the cooldown remaining in event time.
    """
    acked_seq = current_high_water_seq(conn)
    updated = conn.execute(
        "UPDATE alarms SET state = 'acked', acked_at = ?, acked_seq = ?, "
        "updated_at = ? WHERE id = ? AND state = 'active'",
        (to_storage(now), acked_seq, to_storage(now), row["id"]),
    ).rowcount
    if updated == 1:
        conn.execute(
            "UPDATE alarm_rule_progress SET gate_state = 'acked', "
            "gate_fired_at = ?, gate_cooldown_until = NULL, "
            "gate_acked_seq = ? WHERE rule_version_id = "
            "(SELECT id FROM alarm_rules WHERE device_id = ? AND rule_id = ? "
            "AND version = ?)",
            (
                row["fired_at"],
                acked_seq,
                row["device_id"],
                row["rule_id"],
                row["rule_version"],
            ),
        )
    return updated


def derive_alarm_status(state: str, cooldown_until: datetime, now: datetime) -> str:
    """Externally visible status from the stored state and wall time.

    An unacked alarm inside its event-time cooldown is ``active``; once the
    cooldown instant has passed it is reported as ``expired`` (the rule may
    fire again) until acknowledged. Terminal states pass through.
    """
    if state == STATE_ACTIVE:
        return STATUS_ACTIVE if cooldown_until > now else STATUS_EXPIRED
    return state

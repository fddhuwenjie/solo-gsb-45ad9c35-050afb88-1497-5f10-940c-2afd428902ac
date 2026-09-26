"""Per-device state machines: immutable versions, event projection and logs.

A client registers an *immutable* state-machine version for a device: an
initial state, a state set and a list of transitions. Each transition fires
when the machine is in its ``from_state``, the event's ``event_type``
matches and every AND condition (RFC 6901 JSON Pointer + the same
exists/eq/lt/lte/gt/gte semantics as ``POST /events/search``) holds against
the event payload.

After a device *enables* one registered version, every subsequent event
write is projected **in arrival order** (the monotonic, never-reused
``events.seq``) inside the same ``BEGIN IMMEDIATE`` transaction that inserts
the event:

* exactly one transition of the enabled version may match from the current
  state -- the unique match moves the machine to its ``to_state`` and an
  append-only transition log row is written;
* an event for which zero transitions match (an illegal event for the
  current state) is rejected with ``409`` and **nothing is persisted** --
  not the event, not a log row;
* an event matching more than one transition is ambiguous and rejected with
  ``409`` in the same way;
* an event without an ``event_type``/payload can never match (transition
  conditions are evaluated against the payload document), so it is rejected
  while a machine is enabled;
* a duplicate ``event_id`` is an idempotent replay of the existing event
  semantics: the stored event is compared in full and an identical replay
  returns ``200`` and the previously projected transition (if any) without
  touching current state or the log.

The current state and the projection high-water mark live in
``device_state_machines``; the transition log in ``state_transitions_log``.
Both survive restarts, and the log is keyset-paged in arrival order with
opaque HMAC-signed cursors.

Versions are append-only and immutable per device: the first version is 1
and every later one is exactly the current latest plus one. A version
definition can never be modified; a byte-identical re-registration is an
idempotent replay.

Apart from the small SQL helpers taking an open connection, this module is
pure: callers (the FastAPI endpoints) own the transactions.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, NamedTuple, Sequence

from .sessions import Event, from_storage, to_storage
from .search import (
    Condition,
    evaluate_condition,
    parse_pointer,
    resolve_pointer,
)


class StateMachineError(Exception):
    """A client-correctable state-machine definition failure (reported as 422)."""

    def __init__(self, message: str, errors: list[dict[str, str]] | None = None):
        super().__init__(message)
        self.message = message
        self.errors = errors or []


# Full column lists; kept here so every SELECT reads a stable shape.
VERSION_COLUMNS = (
    "id, device_id, version, initial_state, states_json, "
    "definition_json, created_seq, created_at"
)
MACHINE_COLUMNS = (
    "device_id, version_id, version, current_state, last_event_seq, "
    "enabled_at, updated_at"
)
LOG_COLUMNS = (
    "id, device_id, version_id, version, event_seq, event_id, event_type, "
    "occurred_at, from_state, to_state, transition_index, projected_at"
)


@dataclass(frozen=True)
class Transition:
    from_state: str
    event_type: str
    to_state: str
    conditions: tuple[Condition, ...] = ()


@dataclass(frozen=True)
class MachineDefinition:
    initial_state: str
    states: tuple[str, ...]
    transitions: tuple[Transition, ...]


class VersionInfo(NamedTuple):
    id: int
    device_id: str
    version: int
    definition: MachineDefinition
    created_seq: int
    created_at: datetime


# ------------------------------------------------------------- parsing / validation


def build_definition(
    initial_state: str,
    states: Sequence[str],
    transitions: Sequence[dict[str, Any]],
) -> MachineDefinition:
    """Validate a client definition and build the immutable model.

    Raises :class:`StateMachineError` with JSON-Pointer-ish ``errors``
    payloads (mirroring the contract/search endpoints) when:

    * the state set is empty or contains duplicates;
    * ``initial_state`` is not one of the states;
    * a transition references an unknown source/target state;
    * two transitions are exact duplicates (same from/event/conditions);
    * a condition carries an invalid pointer/operator/value.
    """
    errors: list[dict[str, str]] = []
    if not states:
        errors.append({"path": "/states", "error": "states must not be empty"})
    if len(set(states)) != len(states):
        errors.append({"path": "/states", "error": "states must be unique"})
    state_set = set(states)
    if initial_state not in state_set:
        errors.append(
            {"path": "/initial_state", "error": "initial_state must be one of states"}
        )

    def condition_key(condition: Condition) -> str:
        return json.dumps(
            {"pointer": condition.pointer, "operator": condition.operator,
             "value": condition.value},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    parsed_transitions: list[Transition] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for index, raw in enumerate(transitions):
        prefix = f"/transitions/{index}"
        from_state = raw.get("from_state")
        event_type = raw.get("event_type")
        to_state = raw.get("to_state")
        if not isinstance(from_state, str) or from_state not in state_set:
            errors.append({"path": f"{prefix}/from_state", "error": "unknown state"})
        if not isinstance(to_state, str) or to_state not in state_set:
            errors.append({"path": f"{prefix}/to_state", "error": "unknown state"})
        if not isinstance(event_type, str) or not event_type:
            errors.append(
                {"path": f"{prefix}/event_type", "error": "event_type is required"}
            )
        conditions: list[Condition] = []
        for c_index, condition in enumerate(raw.get("conditions", [])):
            cprefix = f"{prefix}/conditions/{c_index}"
            try:
                pointer = condition["pointer"]
                operator = condition["operator"]
                if "value" not in condition:
                    raise StateMachineError("value is required")
                value = condition["value"]
                parse_pointer(pointer)
                if operator == "exists":
                    if not isinstance(value, bool):
                        raise StateMachineError(
                            "exists requires a boolean value (true/false)"
                        )
                elif operator in {"lt", "lte", "gt", "gte"}:
                    if not (
                        isinstance(value, (int, float))
                        and not isinstance(value, bool)
                    ):
                        raise StateMachineError(f"{operator} requires a JSON number")
                elif operator != "eq":
                    raise StateMachineError(f"unsupported operator {operator!r}")
            except (KeyError, TypeError, StateMachineError) as exc:
                errors.append({"path": cprefix, "error": str(exc)})
                continue
            conditions.append(
                Condition(pointer=pointer, operator=operator, value=value)
            )
        if isinstance(from_state, str) and isinstance(event_type, str) and event_type:
            key = (
                from_state,
                event_type,
                json.dumps(
                    sorted((condition_key(c) for c in conditions)),
                    separators=(",", ":"),
                ),
            )
            if key in seen_keys:
                errors.append(
                    {
                        "path": prefix,
                        "error": (
                            "duplicate transition: identical from_state, event_type "
                            "and conditions already defined"
                        ),
                    }
                )
            seen_keys.add(key)
        parsed_transitions.append(
            Transition(
                from_state=from_state if isinstance(from_state, str) else "",
                event_type=event_type if isinstance(event_type, str) else "",
                to_state=to_state if isinstance(to_state, str) else "",
                conditions=tuple(conditions),
            )
        )

    if errors:
        raise StateMachineError("invalid state machine definition", errors)

    return MachineDefinition(
        initial_state=initial_state,
        states=tuple(states),
        transitions=tuple(parsed_transitions),
    )


def definition_to_document(definition: MachineDefinition) -> dict[str, Any]:
    """Plain JSON document persisted as the immutable version body."""
    return {
        "initial_state": definition.initial_state,
        "states": list(definition.states),
        "transitions": [
            {
                "from_state": t.from_state,
                "event_type": t.event_type,
                "to_state": t.to_state,
                "conditions": [
                    {"pointer": c.pointer, "operator": c.operator, "value": c.value}
                    for c in t.conditions
                ],
            }
            for t in definition.transitions
        ],
    }


def definition_from_document(document: dict[str, Any]) -> MachineDefinition:
    return build_definition(
        initial_state=document["initial_state"],
        states=document["states"],
        transitions=document["transitions"],
    )


def version_from_row(row: sqlite3.Row) -> VersionInfo:
    definition = definition_from_document(json.loads(row["definition_json"]))
    return VersionInfo(
        id=row["id"],
        device_id=row["device_id"],
        version=row["version"],
        definition=definition,
        created_seq=int(row["created_seq"]),
        created_at=from_storage(row["created_at"]),
    )


# ------------------------------------------------------------- matching


def event_matches_transition(event: Event, transition: Transition) -> bool:
    """Match one event against one transition.

    The event type must be exactly equal; payload-less events (legacy
    events without the event_type/payload triplet) can never match. Every
    AND condition must hold against the payload; a transition without
    conditions matches every payload event of its event_type.
    """
    if event.event_type != transition.event_type or event.payload is None:
        return False
    for condition in transition.conditions:
        tokens = parse_pointer(condition.pointer)
        value = resolve_pointer(event.payload, tokens)
        if not evaluate_condition(value, condition):
            return False
    return True


def match_transitions(
    current_state: str,
    event: Event,
    definition: MachineDefinition,
) -> list[tuple[int, Transition]]:
    """All transitions applicable to ``event`` from ``current_state``.

    Returns the definition-order indices together with the matching
    transitions. Projection requires exactly one match: zero means the
    event is illegal for the current state, more than one is ambiguous.
    """
    return [
        (index, transition)
        for index, transition in enumerate(definition.transitions)
        if transition.from_state == current_state
        and event_matches_transition(event, transition)
    ]


# ------------------------------------------------------------- SQL helpers


def get_latest_version(
    conn: sqlite3.Connection, device_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {VERSION_COLUMNS} FROM state_machine_versions "
        "WHERE device_id = ? ORDER BY version DESC LIMIT 1",
        (device_id,),
    ).fetchone()


def get_version(
    conn: sqlite3.Connection, device_id: str, version: int
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {VERSION_COLUMNS} FROM state_machine_versions "
        "WHERE device_id = ? AND version = ?",
        (device_id, version),
    ).fetchone()


def list_versions(conn: sqlite3.Connection, device_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        f"SELECT {VERSION_COLUMNS} FROM state_machine_versions "
        "WHERE device_id = ? ORDER BY version",
        (device_id,),
    ).fetchall()


def get_machine(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {MACHINE_COLUMNS} FROM device_state_machines WHERE device_id = ?",
        (device_id,),
    ).fetchone()


def get_enabled_version(
    conn: sqlite3.Connection, machine_row: sqlite3.Row
) -> sqlite3.Row:
    return conn.execute(
        f"SELECT {VERSION_COLUMNS} FROM state_machine_versions WHERE id = ?",
        (machine_row["version_id"],),
    ).fetchone()


def get_log_by_event(
    conn: sqlite3.Connection, device_id: str, event_id: str
) -> sqlite3.Row | None:
    """The projection log row of one event, if the event fired a transition."""
    return conn.execute(
        f"SELECT {LOG_COLUMNS} FROM state_transitions_log "
        "WHERE device_id = ? AND event_id = ?",
        (device_id, event_id),
    ).fetchone()


def log_entry_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Serialize one projection log row (API shape, UTC datetimes)."""
    return {
        "event_seq": row["event_seq"],
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "occurred_at": from_storage(row["occurred_at"]),
        "from_state": row["from_state"],
        "to_state": row["to_state"],
        "transition_index": row["transition_index"],
        "projected_at": from_storage(row["projected_at"]),
    }


def insert_version(
    conn: sqlite3.Connection,
    device_id: str,
    version: int,
    definition: MachineDefinition,
    created_seq: int,
    created_at: datetime,
) -> int:
    document = definition_to_document(definition)
    canonical = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    cur = conn.execute(
        "INSERT INTO state_machine_versions (device_id, version, initial_state, "
        "states_json, definition_json, created_seq, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            device_id,
            version,
            definition.initial_state,
            json.dumps(list(definition.states)),
            canonical,
            created_seq,
            to_storage(created_at),
        ),
    )
    return int(cur.lastrowid)


def device_event_high_water(conn: sqlite3.Connection, device_id: str) -> int:
    """Maximum arrival seq of the device's events; 0 when there are none."""
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE device_id = ?",
        (device_id,),
    ).fetchone()
    return int(row["m"])


def enable_machine(
    conn: sqlite3.Connection,
    device_id: str,
    version_row: sqlite3.Row,
    enabled_at: datetime,
) -> None:
    """Persist the enabled version; the current state starts at initial_state.

    Called only after the caller verified the device has no enabled machine
    under a different version. A fresh enable pins the high-water
    (``last_event_seq``) to the device's current maximum event seq so only
    events that arrive afterwards are projected -- the historical event
    set is never backfilled.
    """
    info = version_from_row(version_row)
    conn.execute(
        "INSERT INTO device_state_machines (device_id, version_id, version, "
        "current_state, last_event_seq, enabled_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            device_id,
            version_row["id"],
            version_row["version"],
            info.definition.initial_state,
            device_event_high_water(conn, device_id),
            to_storage(enabled_at),
            to_storage(enabled_at),
        ),
    )


def append_transition(
    conn: sqlite3.Connection,
    machine_row: sqlite3.Row,
    version: VersionInfo,
    *,
    event_seq: int,
    event_id: str,
    event_type: str,
    occurred_at: datetime,
    from_state: str,
    to_state: str,
    transition_index: int,
    projected_at: datetime,
) -> int:
    """Advance the machine's current state and append one projection log row.

    The UPDATE is guarded by ``last_event_seq < ?``: arrival ordering is
    enforced by the single write lock, but the predicate makes a stale
    projection (an event whose seq was already passed) affect zero rows
    instead of rewinding state. The log's UNIQUE (device_id, event_id)
    additionally guarantees a replay can never insert a second row.
    """
    updated = conn.execute(
        "UPDATE device_state_machines SET current_state = ?, "
        "last_event_seq = ?, updated_at = ? "
        "WHERE device_id = ? AND last_event_seq < ?",
        (
            to_state,
            event_seq,
            to_storage(projected_at),
            machine_row["device_id"],
            event_seq,
        ),
    ).rowcount
    if updated != 1:  # pragma: no cover - defensive: caller holds the write lock
        raise RuntimeError(
            f"state machine of device {machine_row['device_id']!r}: arrival "
            "high-water moved past the projected event"
        )
    cur = conn.execute(
        "INSERT INTO state_transitions_log (device_id, version_id, version, "
        "event_seq, event_id, event_type, occurred_at, from_state, to_state, "
        "transition_index, projected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            machine_row["device_id"],
            version.id,
            version.version,
            event_seq,
            event_id,
            event_type,
            to_storage(occurred_at),
            from_state,
            to_state,
            transition_index,
            to_storage(projected_at),
        ),
    )
    return int(cur.lastrowid)


# ----------------------------------------------------------------- cursors


class LogCursor(NamedTuple):
    version: int
    after_id: int  # keyset: strictly greater log ids (arrival order)
    limit: int


CURSOR_PREFIX = "smc1:"
CURSOR_VERSION = 1
# Server-side integrity key, mirroring the search cursor approach: cursors
# are opaque and must not be tampered with.
_CURSOR_KEY = b"device-state-machine-log-cursor-hmac-key-v1"


def _sign(body: str) -> str:
    return hmac.new(_CURSOR_KEY, body.encode("utf-8"), hashlib.sha256).hexdigest()


def encode_log_cursor(cursor: LogCursor) -> str:
    body = json.dumps(
        {"v": cursor.version, "a": cursor.after_id, "l": cursor.limit},
        separators=(",", ":"),
        sort_keys=True,
    )
    payload = base64.urlsafe_b64encode(body.encode("utf-8")).decode("ascii")
    return CURSOR_PREFIX + payload + "." + _sign(body)[:32]


def decode_log_cursor(raw: str) -> LogCursor:
    """Decode/verify an opaque log cursor; raise StateMachineError (-> 422)."""
    if not isinstance(raw, str) or not raw.startswith(CURSOR_PREFIX):
        raise StateMachineError("invalid cursor: unrecognized format")
    packed = raw[len(CURSOR_PREFIX):]
    if packed.count(".") != 1:
        raise StateMachineError("invalid cursor: malformed token")
    payload, signature = packed.rsplit(".", 1)
    try:
        body = base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise StateMachineError("invalid cursor: malformed token") from exc
    if not hmac.compare_digest(signature, _sign(body)[:32]):
        raise StateMachineError("invalid cursor: signature mismatch")
    try:
        data = json.loads(body)
        version = int(data["v"])
        if version != CURSOR_VERSION:
            raise StateMachineError(
                "cursor is from an incompatible version: restart without a cursor"
            )
        return LogCursor(
            version=version, after_id=int(data["a"]), limit=int(data["l"])
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StateMachineError("invalid cursor: malformed token") from exc


def scan_log_page(
    conn: sqlite3.Connection,
    device_id: str,
    after_id: int,
    limit: int,
) -> tuple[list[sqlite3.Row], bool]:
    """Collect up to ``limit`` log rows past the cursor, in arrival order.

    The log ``id`` is an AUTOINCREMENT key assigned in projection order, so
    an ``id > after_id`` keyset walk is exactly the arrival sequence. One
    extra row decides whether another page exists.
    """
    rows = conn.execute(
        f"SELECT {LOG_COLUMNS} FROM state_transitions_log "
        "WHERE device_id = ? AND id > ? ORDER BY id LIMIT ?",
        (device_id, after_id, limit + 1),
    ).fetchall()
    has_next = len(rows) > limit
    return rows[:limit], has_next

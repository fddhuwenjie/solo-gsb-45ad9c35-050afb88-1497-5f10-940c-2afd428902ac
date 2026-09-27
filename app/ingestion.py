"""Ingestion orchestration for the ``POST /events`` write path.

:class:`EventIngestionService` sequences one event write end to end:
contract-triplet validation, contract resolution and payload validation,
the idempotency decision, the event insert, the state-machine projection
and the alert-rule recomputation, plus selection of the session the event
lands in. The service is transport-agnostic and holds no application
state: the SQLite connection is passed per call and its two collaborators
-- the contract resolver and the session builder -- are injected
callables, so the service never owns a module-level connection or an
application-global cache (the parsed-schema cache stays with the app
factory that supplies the resolver).

The *caller* owns the transaction: the route opens ``BEGIN IMMEDIATE``,
calls :meth:`EventIngestionService.ingest`, and commits on success or
rolls back on any error, so the idempotency decision, the insert, the
projection and the alert recomputation are always committed or rolled
back together. Domain outcomes are reported as
:class:`~app.contracts.ContractError` (the route maps it to 422) and
:class:`IngestionConflict` (mapped to 409); unexpected exceptions
propagate so the caller rolls back as well.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .alerts import reconcile_device_alarms
from .contracts import ContractError, check_payload
from .receipts import append_event_leaf
from .sessions import Event, normalize_ts, to_storage
from .statemachines import (
    LOG_COLUMNS,
    append_transition,
    get_enabled_version,
    get_log_by_event,
    get_machine,
    log_entry_to_dict,
    match_transitions,
    version_from_row,
)

CONTRACT_FIELDS = ("event_type", "schema_version", "payload")


def canonical_json(value: Any) -> str:
    """Stable serialization: byte-identical JSON values compare equal."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class IngestionConflict(Exception):
    """The event cannot be accepted (HTTP 409 semantics); nothing persists.

    ``detail`` is the exact response detail payload -- a plain string or a
    structured object -- that the route re-raises as a ``409`` after
    rolling back the transaction.
    """

    def __init__(self, detail: Any):
        super().__init__(detail if isinstance(detail, str) else detail.get("message"))
        self.detail = detail


@dataclass(frozen=True)
class SessionView:
    """One merged session as the ingestion pipeline sees it.

    Framework-free counterpart of the API's ``SessionOut``; the route maps
    it to the response model. ``members`` are sorted by
    ``(occurred_at, event_id)`` -- the same order the API returns.
    """

    session_id: str
    device_id: str
    started_at: datetime
    ended_at: datetime
    members: tuple[Event, ...]

    @property
    def event_count(self) -> int:
        return len(self.members)


@dataclass(frozen=True)
class IngestionOutcome:
    """Result of one accepted ingestion, ready to map onto the response."""

    replay: bool
    event: Event
    session: SessionView
    state_transition: dict[str, Any] | None


# Resolve the registered contract document for (event_type, version), or
# None when no such contract exists. The app factory injects a resolver
# backed by *its own* parsed-schema cache; the service only sees the
# callable and never touches the cache itself.
ContractResolver = Callable[[sqlite3.Connection, str, str], dict[str, Any] | None]
# Build every session of one device from its stored events, in session
# order. Injected bound to the app instance's configured gap seconds.
SessionBuilder = Callable[[sqlite3.Connection, str], list[SessionView]]


class EventIngestionService:
    """Orchestrates one ``POST /events`` write inside the caller's transaction.

    Collaborators arrive through explicit parameters only: the SQLite
    connection is handed to :meth:`ingest` per call, while the contract
    resolver and the session builder are constructor-injected. The service
    therefore holds no module-level connection and no application-global
    cache, and instances can be tested without any FastAPI machinery.
    """

    def __init__(
        self,
        *,
        contract_resolver: ContractResolver,
        session_builder: SessionBuilder,
    ) -> None:
        self._contract_resolver = contract_resolver
        self._session_builder = session_builder

    def ingest(
        self,
        conn: sqlite3.Connection,
        *,
        device_id: str,
        event_id: str,
        occurred_at: datetime,
        event_type: str | None = None,
        schema_version: str | None = None,
        payload: Any = None,
        payload_provided: bool = False,
        now: datetime | None = None,
    ) -> IngestionOutcome:
        """Run one event ingestion inside the caller's open write transaction.

        The caller must have begun a ``BEGIN IMMEDIATE`` transaction on
        ``conn`` and commits (or rolls back) afterwards: every write below
        -- the event insert, the state-machine projection and the alert
        recomputation -- is covered by that single transaction, so any
        failure rolls all of them back together. The response session is
        computed from the same transactional snapshot, so it is exactly
        the session the event lands in at commit time.

        Raises:
            ContractError: the contract triplet is incomplete, the
                contract is unknown, or the payload fails validation
                (mapped to 422 by the route).
            IngestionConflict: the ``event_id`` exists with different
                content, or the event is illegal/ambiguous for the
                device's enabled state machine (mapped to 409); nothing
                is persisted.
        """
        occurred = normalize_ts(occurred_at)
        stored = to_storage(occurred)
        if now is None:
            now = datetime.now(timezone.utc)

        # The contract triplet is all-or-nothing; point at the missing fields.
        # Explicit nulls for the scalar fields count as missing; payload null
        # itself is a legitimate value (a schema may allow {"type": "null"}).
        provided = {
            "event_type": event_type is not None,
            "schema_version": schema_version is not None,
            "payload": payload_provided,
        }
        if any(provided.values()) and not all(provided.values()):
            missing = [f"/{f}" for f, present in provided.items() if not present]
            raise ContractError(
                "event_type, schema_version and payload must be provided together",
                [
                    {"path": path, "error": "field is required for a payload event"}
                    for path in missing
                ],
            )

        stored_payload: str | None = None
        if event_type is not None:
            # Validate strictly BEFORE any insert or session computation.
            document = self._contract_resolver(conn, event_type, schema_version)
            if document is None:
                raise ContractError(
                    "unknown event contract",
                    [
                        {
                            "path": "/schema_version",
                            "error": (
                                f"schema {event_type!r}/"
                                f"{schema_version!r} is not registered"
                            ),
                        }
                    ],
                )
            check_payload(document, payload)
            stored_payload = canonical_json(payload)

        replay = False
        projected_log_row: sqlite3.Row | None = None
        existing = conn.execute(
            "SELECT seq, device_id, occurred_at, event_type, "
            "schema_version, payload FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            # Idempotency is decided before projection: an identical
            # replay of a known event_id returns 200 even when the event
            # would be illegal against the *current* machine state; the
            # stored transition (if any) is returned untouched.
            if (
                existing["device_id"] != device_id
                or existing["occurred_at"] != stored
                or (existing["event_type"] or None) != event_type
                or (existing["schema_version"] or None) != schema_version
                or (existing["payload"] or None) != stored_payload
            ):
                raise IngestionConflict(
                    f"event_id {event_id!r} already exists with different "
                    "device_id, occurred_at, event_type, schema_version or "
                    "payload"
                )
            replay = True
            projected_log_row = get_log_by_event(conn, device_id, event_id)
        else:
            # Projection is decided BEFORE the insert: an illegal (no
            # matching transition) or ambiguous (more than one match)
            # event raises 409 here and leaves no rows behind.
            machine_row = get_machine(conn, device_id)
            projection: tuple[Any, int, Any, str] | None = None
            if machine_row is not None:
                version_row = get_enabled_version(conn, machine_row)
                assert version_row is not None  # FK-backed enabled version
                info = version_from_row(version_row)
                incoming = Event(
                    device_id=device_id,
                    event_id=event_id,
                    occurred_at=occurred,
                    event_type=event_type,
                    schema_version=schema_version,
                    payload=payload,
                )
                current_state = machine_row["current_state"]
                matches = match_transitions(current_state, incoming, info.definition)
                if not matches:
                    raise IngestionConflict(
                        {
                            "message": (
                                "event is illegal for the device's current "
                                "state machine state: no transition matches "
                                "this event from the current state; the "
                                "event was not stored"
                            ),
                            "reason": "no_matching_transition",
                            "current_state": current_state,
                            "machine_version": machine_row["version"],
                        }
                    )
                if len(matches) > 1:
                    raise IngestionConflict(
                        {
                            "message": (
                                "event is ambiguous for the device's "
                                "current state machine state: more than "
                                "one transition matches; the event was "
                                "not stored"
                            ),
                            "reason": "ambiguous_transition",
                            "current_state": current_state,
                            "machine_version": machine_row["version"],
                            "matching_transition_indices": [
                                index for index, _ in matches
                            ],
                        }
                    )
                index, transition = matches[0]
                projection = (
                    info,
                    index,
                    transition,
                    current_state,
                )

            try:
                inserted = conn.execute(
                    "INSERT INTO events (event_id, device_id, occurred_at, "
                    "event_type, schema_version, payload) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event_id,
                        device_id,
                        stored,
                        event_type,
                        schema_version,
                        stored_payload,
                    ),
                )
            except sqlite3.IntegrityError:
                # A concurrent writer won the global event_id race after
                # our pre-check under the same write lock cannot happen
                # (the lock is held), but stay defensive: report it as
                # an ordinary id conflict.
                raise IngestionConflict(
                    f"event_id {event_id!r} already exists"
                ) from None

            event_seq = int(inserted.lastrowid)
            # Transparent receipt: exactly one Merkle leaf per first-time
            # write, appended in this same transaction (so a later failure
            # -- e.g. an alert-reconciliation error -- rolls the leaf back
            # too). The leaf binds the stored, normalized columns; replays
            # and failed requests never reach this branch.
            append_event_leaf(
                conn,
                event_seq=event_seq,
                device_id=device_id,
                event_id=event_id,
                occurred_at=stored,
                event_type=event_type,
                schema_version=schema_version,
                payload=(
                    json.loads(stored_payload) if stored_payload is not None else None
                ),
                created_at=now,
            )

            if projection is not None:
                info, index, transition, from_state = projection
                log_id = append_transition(
                    conn,
                    machine_row,
                    info,
                    event_seq=event_seq,
                    event_id=event_id,
                    event_type=event_type,
                    occurred_at=occurred,
                    from_state=from_state,
                    to_state=transition.to_state,
                    transition_index=index,
                    projected_at=now,
                )
                projected_log_row = conn.execute(
                    f"SELECT {LOG_COLUMNS} FROM state_transitions_log "
                    "WHERE id = ?",
                    (log_id,),
                ).fetchone()

        # Recompute every active alert rule for the device in event-time
        # order. Replays and genuinely new events run the same
        # deterministic reconciliation: identical trigger keys make it a
        # no-op for replays, so a replayed event_id never double-fires.
        reconcile_device_alarms(conn, device_id, now)

        session = next(
            view
            for view in self._session_builder(conn, device_id)
            if any(member.event_id == event_id for member in view.members)
        )
        return IngestionOutcome(
            replay=replay,
            event=Event(
                device_id=device_id,
                event_id=event_id,
                occurred_at=occurred,
                event_type=event_type,
                schema_version=schema_version,
                payload=payload,
            ),
            session=session,
            state_transition=(
                log_entry_to_dict(projected_log_row)
                if projected_log_row is not None
                else None
            ),
        )

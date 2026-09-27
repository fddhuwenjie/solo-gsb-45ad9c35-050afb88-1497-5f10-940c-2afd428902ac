"""FastAPI application: out-of-order device event session merging."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
import hmac
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Any, Iterator

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, model_validator

from . import db as db_module
from .clockcal import (
    CURSOR_VERSION as TIMELINE_CURSOR_VERSION,
    DEFAULT_DRIFT_PPM,
    DEFAULT_EXTRAPOLATION_SECONDS,
    REASON_ANCHOR_CONTENT_CONFLICT,
    ClockCalibrationError,
    TimelineCursor,
    decode_timeline_cursor,
    encode_timeline_cursor,
    frozen_anchor_high_waters,
    get_anchor,
    insert_anchor,
    load_device_anchors,
    normalize_device_time,
    timeline_fingerprint,
    validate_and_plan,
)
from .alerts import (
    STATUS_ACTIVE,
    STATUS_ACKED as ALARM_STATUS_ACKED,
    STATUS_EXPIRED,
    STATUS_INVALIDATED,
    acknowledge_alarm,
    conditions_to_storage,
    current_event_frontier,
    current_high_water_seq,
    derive_alarm_status,
    get_alarm,
    get_latest_version,
    get_rule_version,
    list_device_alarms,
    list_rules,
)
from .commands import (
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
    STATE_ACKED,
    STATE_PENDING,
    STATUS_LEASED,
    STATUS_RETRY_WAIT,
    derive_status,
    get_command,
    head_pending_command,
)
from .contracts import (
    MISSING,
    ContractError,
    check_meta,
    validate_chain_link,
)
from .ingestion import (
    EventIngestionService,
    IngestionConflict,
    SessionView,
    canonical_json,
)
from .receipts import (
    ReceiptError,
    consistency_proof as build_consistency_proof,
    get_checkpoint as get_receipt_checkpoint,
    list_checkpoints as list_receipt_checkpoints,
    membership_proof as build_membership_proof,
    save_checkpoint as save_receipt_checkpoint,
)
from .receipts import append_leaf as append_receipt_leaf
from .retention import (
    normalize_windows,
    plan_digest,
    select_candidates,
    windows_to_storage,
)
from .search import (
    MAX_CONDITIONS,
    MAX_LIMIT,
    DEFAULT_LIMIT,
    OPERATORS,
    CURSOR_VERSION,
    Condition,
    Cursor,
    SearchError,
    SearchQuery,
    current_high_water,
    decode_cursor,
    encode_cursor,
    parse_pointer,
    query_fingerprint,
    scan_page,
)
from .sessions import (
    Event,
    from_storage,
    group_sessions,
    normalize_ts,
    session_id,
    to_storage,
)
from .snapshots import (
    SnapshotConflict,
    SessionSnapshot,
    SnapshotSession,
    create_snapshot,
    diff_snapshots,
    list_snapshot_rows,
    load_snapshot,
)
from .streams import (
    IntegrityConflict as StreamIntegrityConflict,
    UnknownStream,
    get_checkpoint as get_stream_checkpoint,
    get_stream,
    integrity_status,
    list_checkpoints as list_stream_checkpoints,
    list_streams,
    register_stream,
    save_checkpoint,
    stream_source_seqs,
    submit_stream_event,
)
from .statemachines import (
    LogCursor,
    StateMachineError,
    build_definition,
    decode_log_cursor,
    definition_to_document,
    device_event_high_water,
    enable_machine,
    encode_log_cursor,
    get_machine,
    get_version as get_machine_version,
    get_latest_version as get_latest_machine_version,
    insert_version,
    list_versions as list_machine_versions,
    log_entry_to_dict,
    scan_log_page,
    version_from_row,
)

DEFAULT_GAP_SECONDS = 300.0
# Name kept stable across starlette versions (the re-exported constant is
# deprecated in favour of HTTP_422_UNPROCESSABLE_CONTENT).
HTTP_422_UNPROCESSABLE_ENTITY = 422


# ---------------------------------------------------------------- schemas

class EventIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=256)
    event_id: str = Field(min_length=1, max_length=256)
    occurred_at: datetime = Field(
        description="ISO-8601 timestamp; naive values are treated as UTC"
    )
    event_type: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        description="Required together with schema_version and payload.",
    )
    schema_version: str | None = Field(
        default=None, min_length=1, max_length=256
    )
    payload: Any = Field(
        default=None,
        description="Arbitrary JSON validated against the registered contract.",
    )


class EventOut(BaseModel):
    device_id: str
    event_id: str
    occurred_at: datetime
    event_type: str | None = None
    schema_version: str | None = None
    payload: Any = None


class SessionOut(BaseModel):
    session_id: str
    device_id: str
    started_at: datetime
    ended_at: datetime
    event_count: int
    members: list[EventOut]


class EventAccepted(BaseModel):
    replay: bool = Field(
        description="true when the event was an idempotent replay of an existing one"
    )
    event: EventOut
    session: SessionOut
    state_transition: dict[str, Any] | None = Field(
        default=None,
        description=(
            "The transition this event fired on the device's enabled state "
            "machine; null when no machine is enabled, the event predates "
            "enabling, or the device/machine projection has no log row"
        ),
    )


# ------------------------------------------------- session evolution snapshots


class SnapshotCreateIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    snapshot_id: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[^/]+$",
        description=(
            "Client-chosen, globally unique id; re-POSTing it for the same "
            "device replays the original snapshot, reusing it on another "
            "device returns 409"
        ),
    )


class SnapshotSessionRef(BaseModel):
    session_id: str
    started_at: datetime
    ended_at: datetime
    event_count: int


class SessionSnapshotOut(BaseModel):
    snapshot_id: str
    device_id: str
    event_high_water: int = Field(
        description=(
            "Frozen arrival high-water (events.seq): only events with "
            "seq <= this value contributed to the snapshot"
        )
    )
    gap_seconds: float = Field(
        description="Session gap configuration frozen with this snapshot"
    )
    session_count: int
    sessions: list[SessionOut] = Field(
        description="Frozen sessions in snapshot-time (started_at) order"
    )
    created_at: datetime


class SessionSnapshotSummaryOut(BaseModel):
    snapshot_id: str
    device_id: str
    event_high_water: int
    gap_seconds: float
    session_count: int
    sessions: list[SnapshotSessionRef] = Field(
        description="Session ids and bounds, in snapshot-time order"
    )
    created_at: datetime


class SnapshotDiffIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    old_snapshot_id: str = Field(min_length=1, max_length=256)
    new_snapshot_id: str = Field(min_length=1, max_length=256)


class SnapshotDiffEntryOut(BaseModel):
    change: str = Field(
        description=(
            "unchanged | changed | merged | split | reorganized | created | "
            "removed; reorganized marks a many-to-many connected component"
        )
    )
    old_session_ids: list[str]
    new_session_ids: list[str]
    added_member_event_ids: list[str] = Field(
        description="New-side members absent from the old side, sorted by (occurred_at, event_id)"
    )
    removed_member_event_ids: list[str] = Field(
        description="Old-side members absent from the new side, sorted by (occurred_at, event_id)"
    )


class SnapshotDiffOut(BaseModel):
    device_id: str
    old_snapshot_id: str
    new_snapshot_id: str
    gap_seconds: float
    changes: list[SnapshotDiffEntryOut] = Field(
        description="One entry per bipartite-graph connected component, stably sorted"
    )


# ---------------------------------------------------- collection integrity


class StreamRegistrationIn(BaseModel):
    model_config = ConfigDict(extra="ignore")


class StreamOut(BaseModel):
    device_id: str
    stream_id: str
    created_at: datetime
    event_count: int = Field(
        description="Number of accepted (source_seq -> event_id) ledger bindings"
    )
    highest_seen: int = Field(
        description="Greatest accepted source_seq; 0 while the ledger is empty"
    )
    contiguous_through: int = Field(
        description=(
            "Greatest n such that every slot 1..n is present; 0 when slot 1 "
            "is missing"
        )
    )
    missing_ranges: list[list[int]] = Field(
        description=(
            "Compressed gaps inside 1..highest_seen as inclusive "
            "[start, end] pairs; [] when the ledger is gap-free"
        )
    )


class StreamEventIn(EventIn):
    source_seq: int = Field(
        ge=1,
        description=(
            "Device-assigned sequence within the stream, numbered from 1; "
            "out-of-order and late backfill are allowed"
        ),
    )


class StreamEventAccepted(BaseModel):
    replay: bool = Field(
        description=(
            "true when the (stream, source_seq) binding already existed: the "
            "whole submission was an idempotent replay"
        )
    )
    device_id: str
    stream_id: str
    source_seq: int
    event: EventOut
    session: SessionOut
    state_transition: dict[str, Any] | None = None


class CheckpointIn(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CheckpointOut(BaseModel):
    device_id: str
    stream_id: str
    through_seq: int = Field(description="Inclusive, gap-free prefix 1..through_seq")
    checksum_sha256: str = Field(
        description=(
            "SHA-256 over the stream coordinates and the ordered "
            "(source_seq, event_id) pairs of the ledger prefix"
        )
    )
    event_count: int
    created_at: datetime


# ---------------------------------------------------- transparency receipts


class ReceiptCheckpointIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tree_size: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Tree size to checkpoint; defaults to the current size. Must "
            "not exceed the current number of receipt leaves"
        ),
    )


class ReceiptCheckpointOut(BaseModel):
    tree_size: int = Field(description="Number of leaves the checkpoint covers")
    root_hash: str = Field(
        description=(
            "RFC 6962 Merkle root over the first tree_size receipt "
            "leaves, SHA-256 hex"
        )
    )
    created_at: datetime


class ReceiptLeafOut(BaseModel):
    seq: int = Field(description="Persistent arrival sequence (events.seq)")
    device_id: str
    event_id: str
    occurred_at: datetime
    event_type: str | None = None
    schema_version: str | None = None
    payload: Any = Field(
        default=None,
        description="Parsed JSON payload; null for payload-less events",
    )


class MembershipProofOut(BaseModel):
    event_id: str
    leaf_index: int = Field(description="0-based Merkle position of the leaf")
    tree_size: int = Field(description="Tree size this proof verifies against")
    root_hash: str = Field(
        description="Expected Merkle root at tree_size; matches any checkpoint of that size"
    )
    leaf: ReceiptLeafOut = Field(
        description=(
            "Every field needed to recompute the leaf hash offline: "
            "prefix 'device-event-receipt-leaf:1\\n' + canonical JSON, "
            "then SHA-256(0x00 || material)"
        )
    )
    audit_path: list[str] = Field(
        description="RFC 6962 sibling hashes, SHA-256 hex, in fold order"
    )


class ConsistencyProofOut(BaseModel):
    first_size: int
    first_root: str = Field(description="Merkle root of the tree at first_size")
    second_size: int
    second_root: str = Field(description="Merkle root of the tree at second_size")
    proof: list[str] = Field(
        description=(
            "RFC 6962 consistency proof hashes (SHA-256 hex); which "
            "subtree each hash covers is derived from the two sizes alone"
        )
    )


class SchemaIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_: dict[str, Any] = Field(
        alias="schema", description="Self-contained JSON Schema (Draft 2020-12)"
    )
    supersedes: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        description="Version of the predecessor contract for this event_type.",
    )


class SchemaOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_type: str
    schema_version: str
    schema_: dict[str, Any] = Field(alias="schema")
    supersedes: str | None
    created_at: datetime


class TimeWindow(BaseModel):
    start: datetime = Field(
        description="Window start (inclusive); naive values are treated as UTC"
    )
    end: datetime = Field(
        description="Window end (exclusive); naive values are treated as UTC"
    )


class CleanupPlanIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=256)
    before: datetime = Field(
        description="Exclusive cutoff: only events with occurred_at < before qualify"
    )
    protected_windows: list[TimeWindow] = Field(
        default_factory=list,
        description="Half-open [start, end) ranges that must never be deleted",
    )

    @model_validator(mode="after")
    def validate_windows(self) -> CleanupPlanIn:
        for window in self.protected_windows:
            if normalize_ts(window.start) >= normalize_ts(window.end):
                raise ValueError(
                    "protected window is empty: start must be strictly before end"
                )
        return self


class CleanupPlanOut(BaseModel):
    plan_id: str
    device_id: str
    before: datetime
    protected_windows: list[TimeWindow]
    plan_digest: str = Field(
        description="SHA-256 over the selection criteria and ordered candidate event_ids"
    )
    status: str = Field(
        description=(
            "pending before confirmation; confirmed after the candidates "
            "matched and the batch was deleted; invalidated (terminal) once "
            "drift was detected, in which case the plan can never delete"
        )
    )
    candidates: list[EventOut] = Field(
        description="Events selected for deletion, sorted by (occurred_at, event_id)"
    )
    count: int
    created_at: datetime
    confirmed_at: datetime | None = None


# ----------------------------------------------------------- event search

class OccurredAtRange(BaseModel):
    start: datetime | None = Field(
        default=None,
        description="Half-open interval start (inclusive); naive values are UTC",
    )
    end: datetime | None = Field(
        default=None,
        description="Half-open interval end (exclusive); naive values are UTC",
    )

    @model_validator(mode="after")
    def validate_range(self) -> OccurredAtRange:
        if (
            self.start is not None
            and self.end is not None
            and normalize_ts(self.start) >= normalize_ts(self.end)
        ):
            raise ValueError(
                "occurred_at interval is empty: start must be strictly before end"
            )
        return self


class PayloadCondition(BaseModel):
    pointer: str = Field(
        description="RFC 6901 JSON Pointer locating the field inside the event payload"
    )
    operator: str = Field(
        description="exists | eq | lt | lte | gt | gte; conditions combine with AND"
    )
    value: Any = Field(
        description=(
            "JSON value compared against. Required for every operator; "
            "exists takes a boolean, ordering operators take a JSON number."
        )
    )

    @model_validator(mode="after")
    def validate_condition(self) -> PayloadCondition:
        if self.operator not in OPERATORS:
            raise ValueError(
                f"unsupported operator {self.operator!r}: expected one of "
                + ", ".join(sorted(OPERATORS))
            )
        # The value key itself must be present; an explicit null is a legal
        # eq target distinct from a missing location.
        if "value" not in self.model_fields_set:
            raise ValueError("value is required")
        if self.operator == "exists" and not isinstance(self.value, bool):
            raise ValueError("exists requires a boolean value (true/false)")
        if self.operator in {"lt", "lte", "gt", "gte"} and not (
            isinstance(self.value, (int, float))
            and not isinstance(self.value, bool)
        ):
            raise ValueError(f"{self.operator} requires a JSON number value")
        try:
            parse_pointer(self.pointer)
        except SearchError as exc:
            raise ValueError(str(exc)) from exc
        return self


class SearchEventsIn(BaseModel):
    device_id: str | None = Field(default=None, min_length=1, max_length=256)
    event_type: str | None = Field(default=None, min_length=1, max_length=256)
    schema_version: str | None = Field(default=None, min_length=1, max_length=256)
    occurred_at: OccurredAtRange | None = None
    conditions: list[PayloadCondition] = Field(
        default_factory=list,
        description="At most 5 AND conditions located by RFC 6901 JSON Pointers",
    )
    limit: int = Field(
        default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT,
        description=f"Page size (1..{MAX_LIMIT}, default {DEFAULT_LIMIT})",
    )
    cursor: str | None = Field(
        default=None,
        max_length=2048,
        description="Opaque next_cursor from the previous page of the same query",
    )

    @model_validator(mode="after")
    def validate_conditions(self) -> SearchEventsIn:
        if len(self.conditions) > MAX_CONDITIONS:
            raise ValueError(
                f"at most {MAX_CONDITIONS} payload conditions are supported "
                f"(got {len(self.conditions)})"
            )
        return self


class SearchEventsOut(BaseModel):
    events: list[EventOut]
    next_cursor: str | None = Field(
        description="Present while another page may exist; null on the final page"
    )
    limit: int


# ------------------------------------------------------------- alert rules


class AlertRuleIn(BaseModel):
    version: int = Field(
        ge=1,
        description=(
            "Strictly increasing, immutable version number; the first "
            "version is 1, each superseding version must be exactly one "
            "higher than the current latest"
        ),
    )
    event_type: str = Field(
        min_length=1,
        max_length=256,
        description="Only events with this exact event_type can match",
    )
    conditions: list[PayloadCondition] = Field(
        default_factory=list,
        max_length=MAX_CONDITIONS,
        description=(
            "Up to 5 AND conditions on payload JSON-Pointer locations; "
            "an empty list matches every payload event of event_type"
        ),
    )
    window_seconds: float = Field(
        gt=0,
        le=365 * 24 * 3600.0,
        description="Sliding window in which the consecutive hits must occur",
    )
    consecutive: int = Field(
        ge=1,
        le=10_000,
        description="Number of consecutive matching events that fires an alarm",
    )
    cooldown_seconds: float = Field(
        ge=0.0,
        le=365 * 24 * 3600.0,
        description=(
            "After an alarm fires, suppress further firings for this many "
            "seconds of event time (or until the alarm is acknowledged)"
        ),
    )


class AlertRuleOut(BaseModel):
    device_id: str
    rule_id: str
    version: int
    event_type: str
    conditions: list[PayloadCondition]
    window_seconds: float
    consecutive: int
    cooldown_seconds: float
    active: bool = Field(
        description="true when this is the highest (current) version of the rule"
    )
    created_seq: int = Field(
        description=(
            "Arrival high-water mark at creation; events already present "
            "then never feed this version, while event-time eras decide "
            "which later arrivals (including late ones) belong to it"
        )
    )
    created_at: datetime


class AlarmOut(BaseModel):
    alarm_id: str
    device_id: str
    rule_id: str
    rule_version: int
    status: str = Field(
        description=(
            "active (inside cooldown, unacked) | expired (cooldown elapsed, "
            "unacked) | acked (terminal) | invalidated (terminal: a "
            "recomputation after out-of-order arrival removed the firing)"
        )
    )
    fired_at: datetime = Field(
        description="occurred_at of the triggering event (event time)"
    )
    first_match_at: datetime
    last_match_at: datetime
    cooldown_until: datetime = Field(
        description="Event-time instant at which the rule may fire again"
    )
    trigger_event_id: str
    member_event_ids: list[str] = Field(
        description="Consecutive matching events backing the firing, in event-time order"
    )
    acked_at: datetime | None = None
    invalidated_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class AlarmAckIn(BaseModel):
    expected_status: str = Field(
        description=(
            "Required only for an optimistic state transition; allowed "
            "values: active | expired | acked. Acknowledging an alarm "
            "already in the expected status replays 200 idempotently"
        )
    )
    expected_rule_version: int = Field(
        ge=1,
        description="The alarm must currently belong to this exact rule version",
    )

    @model_validator(mode="after")
    def validate_expected_status(self) -> AlarmAckIn:
        allowed = {STATUS_ACTIVE, STATUS_EXPIRED, ALARM_STATUS_ACKED}
        if self.expected_status not in allowed:
            raise ValueError(
                "expected_status must be one of: " + ", ".join(sorted(allowed))
            )
        return self


# -------------------------------------------------------- state machines


class TransitionIn(BaseModel):
    from_state: str = Field(
        min_length=1,
        max_length=256,
        description="Source state; the transition is only considered while in it",
    )
    event_type: str = Field(
        min_length=1,
        max_length=256,
        description="Only events with this exact event_type can match",
    )
    to_state: str = Field(
        min_length=1,
        max_length=256,
        description="Target state reached when this transition uniquely matches"
    )
    conditions: list[PayloadCondition] = Field(
        default_factory=list,
        max_length=MAX_CONDITIONS,
        description=(
            "Up to 5 AND payload conditions (RFC 6901 JSON Pointer); "
            "an empty list matches every payload event of event_type"
        ),
    )


class StateMachineVersionIn(BaseModel):
    version: int = Field(
        ge=1,
        description=(
            "Immutable, append-only version number; the first version is 1, "
            "every later version is exactly the current latest plus one"
        ),
    )
    initial_state: str = Field(min_length=1, max_length=256)
    states: list[str] = Field(
        min_length=1,
        description="Non-empty set of state names; duplicates are rejected",
    )
    transitions: list[TransitionIn] = Field(
        default_factory=list,
        description=(
            "Transitions matched while the machine is enabled; an event must "
            "match exactly one of them (zero -> illegal, several -> ambiguous)"
        ),
    )


class StateMachineVersionOut(BaseModel):
    device_id: str
    version: int
    initial_state: str
    states: list[str]
    transitions: list[TransitionIn]
    created_seq: int = Field(
        description="Arrival high-water (events.seq) at registration"
    )
    created_at: datetime


class StateMachineEnableIn(BaseModel):
    version: int = Field(
        ge=1, description="Registered version to enable; it can never be switched"
    )


class StateMachineStatusOut(BaseModel):
    device_id: str
    version: int = Field(description="The one enabled, immutable version")
    current_state: str
    last_event_seq: int = Field(
        description="Arrival seq of the most recently projected event; 0 before the first"
    )
    enabled_at: datetime
    updated_at: datetime


class TransitionLogEntryOut(BaseModel):
    event_seq: int
    event_id: str
    event_type: str
    occurred_at: datetime
    from_state: str
    to_state: str
    transition_index: int = Field(
        description="Index of the fired transition in the version definition"
    )
    projected_at: datetime


class TransitionLogPageOut(BaseModel):
    device_id: str
    version: int
    transitions: list[TransitionLogEntryOut]
    next_cursor: str | None = Field(
        description="Present while another page may exist; null on the final page"
    )
    limit: int


# -------------------------------------------------------- command queue

# command_id shares the device_id shape: non-empty, bounded text. The
# pattern excludes empty segments so "a//b" cannot masquerade as two ids.
CommandId = Annotated[str, Path(min_length=1, max_length=256, pattern=r"^[^/]+$")]

# Checkpoint prefixes are positive source sequences (the ledger starts at 1).
CheckpointThroughSeq = Annotated[int, Path(ge=1)]


class CommandSubmitIn(BaseModel):
    payload: Any = Field(
        description=(
            "Arbitrary JSON (object, array, string, number, boolean or "
            "null); compared canonically for idempotency/conflicts"
        )
    )

    @model_validator(mode="after")
    def require_payload(self) -> CommandSubmitIn:
        # The key must be present: a command without a payload is not
        # valid; an explicit JSON null is, however, a legitimate payload.
        if "payload" not in self.model_fields_set:
            raise ValueError("payload is required")
        return self


class CommandOut(BaseModel):
    device_id: str
    command_id: str
    payload: Any
    status: str = Field(
        description=(
            "queued (claimable) | leased (active lease) | retry_wait "
            "(nacked backoff) | acked (terminal)"
        )
    )
    attempts: int = Field(description="How many times the command has been claimed")
    created_at: datetime
    updated_at: datetime
    leased_at: datetime | None = None
    lease_expires_at: datetime | None = None
    not_before: datetime | None = Field(
        default=None, description="Retry backoff ends at this instant"
    )
    acked_at: datetime | None = None


class ClaimIn(BaseModel):
    lease_seconds: float = Field(
        ge=MIN_LEASE_SECONDS,
        le=MAX_LEASE_SECONDS,
        description=f"Lease duration in seconds ({MIN_LEASE_SECONDS:g}..{MAX_LEASE_SECONDS:g})",
    )


class ClaimOut(BaseModel):
    device_id: str
    command_id: str
    payload: Any
    attempts: int = Field(
        description="Delivery attempt number; 1 on the first claim, increments on reclaim"
    )
    lease_token: str = Field(
        description="Opaque, unpredictable token required by ack/nack until it expires"
    )
    leased_at: datetime
    lease_expires_at: datetime
    created_at: datetime
    updated_at: datetime


class AckIn(BaseModel):
    lease_token: str = Field(min_length=1, max_length=256)


class NackIn(BaseModel):
    lease_token: str = Field(min_length=1, max_length=256)
    retry_after_seconds: float = Field(
        default=0.0,
        ge=0.0,
        description=(
            "Backoff before the command becomes claimable again; 0 "
            "(default) reopens it immediately"
        ),
    )


# --------------------------------------------------------- clock calibration


class ClockAnchorIn(BaseModel):
    anchor_id: str = Field(
        min_length=1,
        max_length=256,
        description="Client-chosen id; (device_id, anchor_id) is idempotent",
    )
    observed_seq: int = Field(
        ge=0,
        description=(
            "Device's monotonic observation counter; must strictly exceed "
            "every previously accepted anchor of the device"
        ),
    )
    device_time: datetime = Field(
        description="Device clock reading; naive values are treated as UTC"
    )
    reference_time: datetime = Field(
        description="Reference clock reading at the same observation (UTC)"
    )


class ClockAnchorOut(BaseModel):
    device_id: str
    anchor_id: str
    observed_seq: int
    device_time: datetime
    reference_time: datetime
    epoch: int = Field(description="Clock epoch; increments on every device_time reset")
    drift_ppm: float | None = Field(
        description="Measured rate mismatch vs the previous in-epoch anchor; null at epoch start"
    )
    created_at: datetime


class ClockNormalizeIn(BaseModel):
    device_time: datetime = Field(
        description="Device clock reading to normalize; naive values are treated as UTC"
    )
    observed_seq: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Optional observation counter: selects the epoch whose last "
            "anchor is the greatest one not exceeding it"
        ),
    )
    epoch: int | None = Field(
        default=None, ge=1, description="Explicit epoch to normalize within"
    )

    @model_validator(mode="after")
    def disallow_both(self) -> ClockNormalizeIn:
        if self.observed_seq is not None and self.epoch is not None:
            raise ValueError("provide at most one of observed_seq and epoch")
        return self


class UncertaintyInterval(BaseModel):
    lower: datetime
    upper: datetime


class ClockNormalizedOut(BaseModel):
    device_id: str
    device_time: datetime
    corrected_at: datetime = Field(description="Reference-time mapping of device_time")
    epoch: int
    method: str = Field(
        description=(
            "How corrected_at was derived: anchor (exact anchor hit) | "
            "interpolate (linear interpolation of adjacent in-epoch anchors) "
            "| extrapolate (linear extension within the configured range)"
        )
    )
    uncertainty_seconds: float = Field(
        description="Symmetric half-width of the absolute uncertainty interval"
    )
    uncertainty: UncertaintyInterval
    bracket_anchor_ids: list[str | None] = Field(
        description="[left, right] bracketing anchors; one side is null when extrapolating"
    )


class ClockTimelineIn(BaseModel):
    device_ids: list[str] | None = Field(
        default=None,
        description="Restrict the timeline to these devices; null/absent means every device",
    )
    limit: int = Field(
        default=50, ge=1, le=500, description="Page size (1..500, default 50)"
    )
    cursor: str | None = Field(
        default=None,
        max_length=16384,
        description="Opaque next_cursor from the previous page of the same walk",
    )

    @model_validator(mode="after")
    def validate_device_ids(self) -> ClockTimelineIn:
        if self.device_ids is not None:
            if not self.device_ids:
                raise ValueError("device_ids must contain at least one id when present")
            if any(not isinstance(d, str) or not (1 <= len(d) <= 256) for d in self.device_ids):
                raise ValueError("every device id must be a non-empty string of at most 256 chars")
            if len(set(self.device_ids)) != len(self.device_ids):
                raise ValueError("device_ids must not contain duplicates")
        return self


class ClockTimelineEventOut(BaseModel):
    corrected_at: datetime
    epoch: int
    method: str
    uncertainty_seconds: float
    device_id: str
    event_id: str
    occurred_at: datetime
    event_type: str | None = None
    schema_version: str | None = None
    payload: Any = None


class ClockTimelineOut(BaseModel):
    events: list[ClockTimelineEventOut]
    next_cursor: str | None = Field(
        description="Present while another page may exist; null on the final page"
    )
    limit: int
    frozen_anchor_high_waters: dict[str, int] = Field(
        description=(
            "Per-device anchor seq frozen at the first page; anchors written "
            "afterwards never affect this walk"
        )
    )


# ---------------------------------------------------------------- app factory

def create_app(
    database_path: str | None = None,
    gap_seconds: float | None = None,
    drift_ppm: float | None = None,
    max_extrapolation_seconds: float | None = None,
) -> FastAPI:
    db_path = database_path or os.environ.get("DATABASE_PATH", "./data/events.db")
    gap = float(
        gap_seconds
        if gap_seconds is not None
        else os.environ.get("SESSION_GAP_SECONDS", DEFAULT_GAP_SECONDS)
    )
    drift_limit = float(
        drift_ppm
        if drift_ppm is not None
        else os.environ.get("CLOCK_DRIFT_PPM", DEFAULT_DRIFT_PPM)
    )
    extrapolation_limit = float(
        max_extrapolation_seconds
        if max_extrapolation_seconds is not None
        else os.environ.get(
            "CLOCK_MAX_EXTRAPOLATION_SECONDS", DEFAULT_EXTRAPOLATION_SECONDS
        )
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        db_module.init_db(db_path)
        yield

    app = FastAPI(title="Device Event Session Merger", version="1.12.0", lifespan=lifespan)
    app.state.db_path = db_path
    app.state.gap_seconds = gap
    app.state.drift_ppm = drift_limit
    app.state.max_extrapolation_seconds = extrapolation_limit
    # Contracts are immutable, so parsed documents can be cached indefinitely.
    app.state.schema_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def get_conn() -> Iterator[sqlite3.Connection]:
        conn = db_module.connect(app.state.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def unprocessable(exc: ContractError) -> HTTPException:
        return HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": exc.message, "errors": exc.errors},
        )

    def get_contract(
        conn: sqlite3.Connection, event_type: str, version: str
    ) -> dict[str, Any] | None:
        key = (event_type, version)
        cached = app.state.schema_cache.get(key)
        if cached is not None:
            return cached
        row = conn.execute(
            "SELECT body FROM schemas WHERE event_type = ? AND schema_version = ?",
            key,
        ).fetchone()
        if row is None:
            return None
        document = json.loads(row["body"])
        app.state.schema_cache[key] = document
        return document

    def load_device_events(conn: sqlite3.Connection, device_id: str) -> list[Event]:
        rows = conn.execute(
            "SELECT event_id, occurred_at, event_type, schema_version, payload "
            "FROM events WHERE device_id = ? ORDER BY occurred_at, event_id",
            (device_id,),
        ).fetchall()
        return [
            Event(
                device_id=device_id,
                event_id=row["event_id"],
                occurred_at=from_storage(row["occurred_at"]),
                event_type=row["event_type"],
                schema_version=row["schema_version"],
                payload=json.loads(row["payload"]) if row["payload"] is not None else None,
            )
            for row in rows
        ]

    def build_session_view(device_id: str, members: list[Event]) -> SessionView:
        return SessionView(
            session_id=session_id(device_id, members),
            device_id=device_id,
            started_at=members[0].occurred_at,
            ended_at=members[-1].occurred_at,
            members=tuple(members),
        )

    def session_views_for_device(
        conn: sqlite3.Connection, device_id: str
    ) -> list[SessionView]:
        events = load_device_events(conn, device_id)
        return [
            build_session_view(device_id, members)
            for members in group_sessions(events, app.state.gap_seconds)
        ]

    def session_out_from_view(view: SessionView) -> SessionOut:
        return SessionOut(
            session_id=view.session_id,
            device_id=view.device_id,
            started_at=view.started_at,
            ended_at=view.ended_at,
            event_count=view.event_count,
            members=[
                EventOut(
                    device_id=view.device_id,
                    event_id=m.event_id,
                    occurred_at=m.occurred_at,
                    event_type=m.event_type,
                    schema_version=m.schema_version,
                    payload=m.payload,
                )
                for m in view.members
            ],
        )

    def sessions_for_device(conn: sqlite3.Connection, device_id: str) -> list[SessionOut]:
        return [
            session_out_from_view(view)
            for view in session_views_for_device(conn, device_id)
        ]

    def candidate_to_out(device_id: str, candidate: dict[str, Any]) -> EventOut:
        return EventOut(
            device_id=device_id,
            event_id=candidate["event_id"],
            occurred_at=candidate["occurred_at"],
            event_type=candidate.get("event_type"),
            schema_version=candidate.get("schema_version"),
            payload=candidate.get("payload"),
        )

    def event_to_candidate(event: Event) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "occurred_at": event.occurred_at,
            "event_type": event.event_type,
            "schema_version": event.schema_version,
            "payload": event.payload,
        }

    def serialize_candidates(candidates: list[dict[str, Any]]) -> str:
        """Canonical JSON snapshot; datetimes use normalized UTC ISO-8601."""
        stored = [
            {**candidate, "occurred_at": to_storage(candidate["occurred_at"])}
            for candidate in candidates
        ]
        return canonical_json(stored)

    def deserialize_candidates(raw: str) -> list[dict[str, Any]]:
        return [
            {**candidate, "occurred_at": from_storage(candidate["occurred_at"])}
            for candidate in json.loads(raw)
        ]

    def plan_out_from_row(row: sqlite3.Row) -> CleanupPlanOut:
        device_id = row["device_id"]
        candidates = deserialize_candidates(row["candidates"])
        windows = json.loads(row["protected_windows"])
        return CleanupPlanOut(
            plan_id=row["plan_id"],
            device_id=device_id,
            before=from_storage(row["before_at"]),
            protected_windows=[
                {"start": from_storage(start), "end": from_storage(end)}
                for start, end in windows
            ],
            plan_digest=row["plan_digest"],
            status=row["status"],
            candidates=[candidate_to_out(device_id, c) for c in candidates],
            count=row["event_count"],
            created_at=from_storage(row["created_at"]),
            confirmed_at=(
                from_storage(row["confirmed_at"])
                if row["confirmed_at"] is not None
                else None
            ),
        )

    def get_plan_row(
        conn: sqlite3.Connection, plan_id: str
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT plan_id, device_id, before_at, protected_windows, "
            "plan_digest, status, candidates, event_count, created_at, "
            "confirmed_at FROM cleanup_plans WHERE plan_id = ?",
            (plan_id,),
        ).fetchone()

    # The ingestion service orchestrates the POST /events write path. Its
    # collaborators are this instance's own closures: the contract resolver
    # is backed by *this* app's schema cache, the session builder by
    # *this* app's gap configuration, and the receipt leaf appender extends
    # *this* app's transparency tree, so two create_app instances never
    # share caches or settings. The SQLite connection is passed per call
    # by the route, which also owns the write transaction.
    ingestion_service = EventIngestionService(
        contract_resolver=get_contract,
        session_builder=session_views_for_device,
        leaf_appender=append_receipt_leaf,
    )

    # ------------------------------------------------------------ endpoints

    @app.put(
        "/schemas/{event_type}/{version}",
        response_model=SchemaOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {"description": "contract already exists with a different body"},
            422: {"description": "invalid schema, missing predecessor or cycle"},
        },
    )
    def register_schema(
        event_type: str,
        version: str,
        body: SchemaIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SchemaOut:
        canonical = canonical_json(body.schema_)
        supersedes = body.supersedes

        existing = conn.execute(
            "SELECT body, supersedes, created_at FROM schemas "
            "WHERE event_type = ? AND schema_version = ?",
            (event_type, version),
        ).fetchone()
        if existing is not None:
            if (
                existing["body"] == canonical
                and (existing["supersedes"] or None) == supersedes
            ):
                # Immutable contract: an identical PUT replays without side effects.
                response.status_code = status.HTTP_200_OK
                return SchemaOut(
                    event_type=event_type,
                    schema_version=version,
                    schema=json.loads(existing["body"]),
                    supersedes=existing["supersedes"],
                    created_at=from_storage(existing["created_at"]),
                )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"schema {event_type!r}/{version!r} is immutable and "
                    "already registered with a different definition"
                ),
            )

        try:
            check_meta(body.schema_)
        except ContractError as exc:
            raise unprocessable(exc) from exc

        def get_supersedes(candidate: str) -> str | None | object:
            row = conn.execute(
                "SELECT supersedes FROM schemas "
                "WHERE event_type = ? AND schema_version = ?",
                (event_type, candidate),
            ).fetchone()
            return MISSING if row is None else row["supersedes"]

        try:
            validate_chain_link(version, supersedes, get_supersedes)
        except ContractError as exc:
            raise unprocessable(exc) from exc

        created_at = datetime.now(timezone.utc)
        try:
            with conn:
                conn.execute(
                    "INSERT INTO schemas "
                    "(event_type, schema_version, body, supersedes, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        event_type,
                        version,
                        canonical,
                        supersedes,
                        to_storage(created_at),
                    ),
                )
        except sqlite3.IntegrityError:
            # Lost a race with a concurrent registration of the same version.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"schema {event_type!r}/{version!r} is immutable and "
                    "already registered"
                ),
            ) from None
        app.state.schema_cache[(event_type, version)] = body.schema_
        return SchemaOut(
            event_type=event_type,
            schema_version=version,
            schema=body.schema_,
            supersedes=supersedes,
            created_at=created_at,
        )

    @app.get(
        "/schemas/{event_type}/{version}",
        response_model=SchemaOut,
        responses={404: {"description": "contract not registered"}},
    )
    def get_schema(
        event_type: str,
        version: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SchemaOut:
        row = conn.execute(
            "SELECT body, supersedes, created_at FROM schemas "
            "WHERE event_type = ? AND schema_version = ?",
            (event_type, version),
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"schema {event_type!r}/{version!r} is not registered",
            )
        return SchemaOut(
            event_type=event_type,
            schema_version=version,
            schema=json.loads(row["body"]),
            supersedes=row["supersedes"],
            created_at=from_storage(row["created_at"]),
        )

    @app.post(
        "/events",
        response_model=EventAccepted,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {"description": "event_id already exists with different content"},
            422: {"description": "unknown contract or payload validation failure"},
        },
    )
    def submit_event(
        payload: EventIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> EventAccepted:
        # The insert, the state-machine projection and the alert evaluation
        # share one write transaction: rule-hit progress, current state, the
        # transition log and the triggering event are committed atomically.
        # BEGIN IMMEDIATE also serializes concurrent writers on the single
        # writer lock, so two concurrent event writes can never interleave
        # their projections -- transitions execute strictly in arrival
        # order (events.seq). The ingestion service performs every step
        # inside this transaction; any failure rolls all of them back.
        conn.execute("BEGIN IMMEDIATE")
        try:
            outcome = ingestion_service.ingest(
                conn,
                device_id=payload.device_id,
                event_id=payload.event_id,
                occurred_at=payload.occurred_at,
                event_type=payload.event_type,
                schema_version=payload.schema_version,
                payload=payload.payload,
                payload_provided="payload" in payload.model_fields_set,
            )
            conn.commit()
        except ContractError as exc:
            conn.rollback()
            raise unprocessable(exc) from exc
        except IngestionConflict as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=exc.detail
            ) from exc
        except Exception:
            conn.rollback()
            raise

        if outcome.replay:
            # Identical replay of an already-stored event.
            response.status_code = status.HTTP_200_OK
        return EventAccepted(
            replay=outcome.replay,
            event=EventOut(
                device_id=outcome.event.device_id,
                event_id=outcome.event.event_id,
                occurred_at=outcome.event.occurred_at,
                event_type=outcome.event.event_type,
                schema_version=outcome.event.schema_version,
                payload=outcome.event.payload,
            ),
            session=session_out_from_view(outcome.session),
            state_transition=outcome.state_transition,
        )

    @app.get("/devices/{device_id}/sessions", response_model=list[SessionOut])
    def list_sessions(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[SessionOut]:
        return sessions_for_device(conn, device_id)

    @app.get("/devices/{device_id}/sessions/{sid}", response_model=SessionOut)
    def get_session(
        device_id: str,
        sid: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SessionOut:
        for session in sessions_for_device(conn, device_id):
            if session.session_id == sid:
                return session
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"session {sid!r} not found for device {device_id!r}",
        )

    # -------------------------------------- session evolution snapshots

    def frozen_session_out(
        device_id: str, session: SnapshotSession
    ) -> SessionOut:
        return SessionOut(
            session_id=session.session_id,
            device_id=device_id,
            started_at=session.started_at,
            ended_at=session.ended_at,
            event_count=session.event_count,
            members=[
                EventOut(
                    device_id=device_id,
                    event_id=member.event_id,
                    occurred_at=member.occurred_at,
                    event_type=member.event_type,
                    schema_version=member.schema_version,
                    payload=member.payload,
                )
                for member in session.members
            ],
        )

    def snapshot_out_from_snapshot(
        snapshot: SessionSnapshot,
    ) -> SessionSnapshotOut:
        return SessionSnapshotOut(
            snapshot_id=snapshot.snapshot_id,
            device_id=snapshot.device_id,
            event_high_water=snapshot.event_high_water,
            gap_seconds=snapshot.gap_seconds,
            session_count=snapshot.session_count,
            sessions=[
                frozen_session_out(snapshot.device_id, session)
                for session in snapshot.sessions
            ],
            created_at=snapshot.created_at,
        )

    def snapshot_summary_out_from_row(
        conn: sqlite3.Connection, row: sqlite3.Row
    ) -> SessionSnapshotSummaryOut:
        session_rows = conn.execute(
            "SELECT session_id, started_at, ended_at, members_json "
            "FROM session_snapshot_sessions WHERE snapshot_id = ? "
            "ORDER BY session_index",
            (row["snapshot_id"],),
        ).fetchall()
        return SessionSnapshotSummaryOut(
            snapshot_id=row["snapshot_id"],
            device_id=row["device_id"],
            event_high_water=row["event_high_water"],
            gap_seconds=row["gap_seconds"],
            session_count=row["session_count"],
            sessions=[
                SnapshotSessionRef(
                    session_id=session_row["session_id"],
                    started_at=from_storage(session_row["started_at"]),
                    ended_at=from_storage(session_row["ended_at"]),
                    event_count=len(
                        json.loads(session_row["members_json"])["members"]
                    ),
                )
                for session_row in session_rows
            ],
            created_at=from_storage(row["created_at"]),
        )

    @app.post(
        "/devices/{device_id}/session-snapshots",
        response_model=SessionSnapshotOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"description": "the snapshot_id already existed for this device (idempotent replay)"},
            409: {"description": "snapshot_id is already bound to another device"},
        },
    )
    def create_session_snapshot(
        device_id: str,
        body: SnapshotCreateIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SessionSnapshotOut:
        """Freeze the device's current sessions as one immutable snapshot.

        Inside one ``BEGIN IMMEDIATE`` write transaction the endpoint first
        pins the device's current monotonic arrival high-water
        (``events.seq``) and then derives the sessions strictly from the
        events with ``seq <=`` that mark using the server's configured
        ``SESSION_GAP_SECONDS``. Sessions and their ordered members are
        copied into dedicated snapshot tables, so later late events and
        retention cleanup never rewrite the snapshot.

        A repeated POST with the same ``snapshot_id`` for the same device
        is an idempotent replay (``200``) of the originally stored
        snapshot -- same high-water, sessions and ``created_at`` -- even
        when the live sessions have since evolved. Reusing a
        ``snapshot_id`` on a different device returns ``409`` and stores
        nothing.
        """
        created_at = datetime.now(timezone.utc)
        conn.execute("BEGIN IMMEDIATE")
        try:
            snapshot, created = create_snapshot(
                conn,
                snapshot_id=body.snapshot_id,
                device_id=device_id,
                gap_seconds=app.state.gap_seconds,
                created_at=created_at,
            )
            conn.commit()
        except SnapshotConflict as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=exc.detail
            ) from exc
        except Exception:
            conn.rollback()
            raise
        if not created:
            response.status_code = status.HTTP_200_OK
        return snapshot_out_from_snapshot(snapshot)

    @app.get(
        "/devices/{device_id}/session-snapshots",
        response_model=list[SessionSnapshotSummaryOut],
    )
    def list_session_snapshots(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[SessionSnapshotSummaryOut]:
        """List the device's snapshots oldest first."""
        rows = list_snapshot_rows(conn, device_id)
        return [snapshot_summary_out_from_row(conn, row) for row in rows]

    @app.get(
        "/devices/{device_id}/session-snapshots/{snapshot_id}",
        response_model=SessionSnapshotOut,
        responses={404: {"description": "unknown snapshot for the device"}},
    )
    def get_session_snapshot(
        device_id: str,
        snapshot_id: CommandId,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SessionSnapshotOut:
        """Fetch one full frozen snapshot (sessions with their members)."""
        snapshot = load_snapshot(conn, snapshot_id)
        if snapshot is None or snapshot.device_id != device_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"snapshot {snapshot_id!r} is not known for device "
                    f"{device_id!r}"
                ),
            )
        return snapshot_out_from_snapshot(snapshot)

    @app.post(
        "/devices/{device_id}/session-snapshots/diff",
        response_model=SnapshotDiffOut,
        responses={
            404: {"description": "one of the snapshot ids is unknown for the device"},
            409: {"description": "the snapshots were frozen with different gap configurations"},
        },
    )
    def diff_session_snapshots(
        device_id: str,
        body: SnapshotDiffIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SnapshotDiffOut:
        """Compare two frozen snapshots of the device, old -> new.

        The bipartite graph links an old session to a new session when
        they share an ``event_id``; each connected component is reported
        once as ``unchanged`` (1:1, same members), ``changed`` (1:1,
        different members), ``merged`` (n old -> 1 new), ``split``
        (1 old -> n new), ``reorganized`` (many-to-many), ``created``
        (new side only) or ``removed`` (old side only). Entries carry
        both sides' session ids and the members added/removed, all sorted
        stably. The comparison reads only the frozen copies, so late
        arrivals and cleanup cannot change the result; snapshots frozen
        with different ``gap_seconds`` are rejected with ``409``.
        """
        old_snapshot = load_snapshot(conn, body.old_snapshot_id)
        if old_snapshot is None or old_snapshot.device_id != device_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"snapshot {body.old_snapshot_id!r} is not known for "
                    f"device {device_id!r}"
                ),
            )
        new_snapshot = load_snapshot(conn, body.new_snapshot_id)
        if new_snapshot is None or new_snapshot.device_id != device_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"snapshot {body.new_snapshot_id!r} is not known for "
                    f"device {device_id!r}"
                ),
            )
        if old_snapshot.gap_seconds != new_snapshot.gap_seconds:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": (
                        "snapshots were frozen with different gap "
                        "configurations and cannot be compared"
                    ),
                    "reason": "gap_configuration_mismatch",
                    "old_snapshot_id": old_snapshot.snapshot_id,
                    "old_gap_seconds": old_snapshot.gap_seconds,
                    "new_snapshot_id": new_snapshot.snapshot_id,
                    "new_gap_seconds": new_snapshot.gap_seconds,
                },
            )
        changes = diff_snapshots(old_snapshot, new_snapshot)
        return SnapshotDiffOut(
            device_id=device_id,
            old_snapshot_id=old_snapshot.snapshot_id,
            new_snapshot_id=new_snapshot.snapshot_id,
            gap_seconds=old_snapshot.gap_seconds,
            changes=changes,
        )

    # ------------------------------------------------ collection integrity

    def stream_status_out_from_row(
        conn: sqlite3.Connection, row: sqlite3.Row
    ) -> StreamOut:
        seqs = stream_source_seqs(conn, row["device_id"], row["stream_id"])
        status = integrity_status(seqs)
        return StreamOut(
            device_id=row["device_id"],
            stream_id=row["stream_id"],
            created_at=from_storage(row["created_at"]),
            event_count=status.event_count,
            highest_seen=status.highest_seen,
            contiguous_through=status.contiguous_through,
            missing_ranges=[list(gap) for gap in status.missing_ranges],
        )

    def checkpoint_out_from_row(row: sqlite3.Row) -> CheckpointOut:
        return CheckpointOut(
            device_id=row["device_id"],
            stream_id=row["stream_id"],
            through_seq=row["through_seq"],
            checksum_sha256=row["checksum_sha256"],
            event_count=row["event_count"],
            created_at=from_storage(row["created_at"]),
        )

    @app.put(
        "/devices/{device_id}/streams/{stream_id}",
        response_model=StreamOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"description": "the stream was already registered (idempotent replay)"},
        },
    )
    def register_collection_stream(
        device_id: str,
        stream_id: CommandId,
        response: Response,
        body: StreamRegistrationIn | None = None,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StreamOut:
        """Register (or idempotently re-register) a device's collection stream.

        A stream is the device's own ordered ingestion channel, addressed
        by ``(device_id, stream_id)``. The device assigns ``source_seq``
        values numbered from 1 on its events; the stream itself has no
        mutable definition, so a repeat PUT replays ``200`` with the
        original ``created_at`` and the current integrity status.
        """
        created_at = datetime.now(timezone.utc)
        conn.execute("BEGIN IMMEDIATE")
        try:
            created = register_stream(conn, device_id, stream_id, created_at)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        row = get_stream(conn, device_id, stream_id)
        assert row is not None
        if not created:
            response.status_code = status.HTTP_200_OK
        return stream_status_out_from_row(conn, row)

    @app.get(
        "/devices/{device_id}/streams",
        response_model=list[StreamOut],
    )
    def list_collection_streams(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[StreamOut]:
        """List the device's registered streams with their integrity status."""
        return [
            stream_status_out_from_row(conn, row)
            for row in list_streams(conn, device_id)
        ]

    @app.get(
        "/devices/{device_id}/streams/{stream_id}",
        response_model=StreamOut,
        responses={404: {"description": "stream is not registered for the device"}},
    )
    @app.get(
        "/devices/{device_id}/streams/{stream_id}/status",
        response_model=StreamOut,
        responses={404: {"description": "stream is not registered for the device"}},
    )
    def get_collection_stream_status(
        device_id: str,
        stream_id: CommandId,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StreamOut:
        """Integrity status of one stream.

        Returns ``event_count``, ``highest_seen`` (greatest accepted
        source_seq), ``contiguous_through`` (largest gap-free prefix from
        slot 1) and ``missing_ranges`` (the gaps inside
        1..highest_seen compressed into inclusive ``[start, end]``
        pairs).
        """
        row = get_stream(conn, device_id, stream_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"stream {stream_id!r} is not registered for device "
                    f"{device_id!r}"
                ),
            )
        return stream_status_out_from_row(conn, row)

    @app.put(
        "/devices/{device_id}/streams/{stream_id}/events",
        response_model=StreamEventAccepted,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"description": "idempotent replay of an existing slot binding"},
            404: {"description": "stream is not registered for the device"},
            409: {
                "description": (
                    "source_seq is already bound to another event_id, the "
                    "event_id is already bound to another source_seq, or the "
                    "event content/state-machine rules conflict"
                )
            },
            422: {"description": "unknown contract, payload validation failure"},
        },
    )
    def submit_stream_event_endpoint(
        device_id: str,
        stream_id: CommandId,
        payload: StreamEventIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StreamEventAccepted:
        """Ingest one event into a registered stream at its ``source_seq``.

        The event fields are exactly those of ``POST /events`` and the
        event is stored through the same ingestion pipeline; the ledger
        decision, the event insert, state-machine projection and alert
        recomputation all share one ``BEGIN IMMEDIATE`` transaction.
        Out-of-order and late backfill are allowed. ``(device_id,
        stream_id, source_seq)`` binds exactly one ``event_id`` and, within
        a stream, an ``event_id`` occupies exactly one slot. Re-submitting
        the identical slot is an idempotent replay (``200``); binding a
        different ``event_id`` to an occupied slot, or an ``event_id``
        already bound elsewhere in the stream, returns ``409`` and stores
        nothing. The original ``POST /events`` path is unchanged.
        """
        if payload.device_id != device_id:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "device_id in the path and in the body must match",
                    "errors": [
                        {
                            "path": "/device_id",
                            "error": f"{device_id} != {payload.device_id}",
                        }
                    ],
                },
            )
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = submit_stream_event(
                conn,
                ingestion=ingestion_service,
                device_id=device_id,
                stream_id=stream_id,
                source_seq=payload.source_seq,
                event_id=payload.event_id,
                occurred_at=payload.occurred_at,
                event_type=payload.event_type,
                schema_version=payload.schema_version,
                payload=payload.payload,
                payload_provided="payload" in payload.model_fields_set,
            )
            conn.commit()
        except UnknownStream as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
            ) from exc
        except StreamIntegrityConflict as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=exc.detail
            ) from exc
        except ContractError as exc:
            conn.rollback()
            raise unprocessable(exc) from exc
        except IngestionConflict as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=exc.detail
            ) from exc
        except Exception:
            conn.rollback()
            raise

        if result.replay:
            response.status_code = status.HTTP_200_OK
        outcome = result.ingestion
        return StreamEventAccepted(
            replay=result.replay,
            device_id=device_id,
            stream_id=stream_id,
            source_seq=payload.source_seq,
            event=EventOut(
                device_id=outcome.event.device_id,
                event_id=outcome.event.event_id,
                occurred_at=outcome.event.occurred_at,
                event_type=outcome.event.event_type,
                schema_version=outcome.event.schema_version,
                payload=outcome.event.payload,
            ),
            session=session_out_from_view(outcome.session),
            state_transition=outcome.state_transition,
        )

    @app.put(
        "/devices/{device_id}/streams/{stream_id}/checkpoints/{through_seq}",
        response_model=CheckpointOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"description": "repeated PUT replays the stored checkpoint"},
            404: {"description": "stream is not registered for the device"},
            409: {
                "description": (
                    "the ledger has gaps inside 1..through_seq (or "
                    "through_seq exceeds the current high-water)"
                )
            },
        },
    )
    def create_stream_checkpoint(
        device_id: str,
        stream_id: CommandId,
        through_seq: CheckpointThroughSeq,
        response: Response,
        body: CheckpointIn | None = None,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CheckpointOut:
        """Save a SHA-256 checkpoint over the gap-free prefix 1..through_seq.

        Only succeeds when every slot from 1 through ``through_seq`` is
        present; otherwise the ledger gaps are reported (compressed) and
        ``409`` returned with nothing stored. The checksum is computed
        over the ordered ``(source_seq, event_id)`` pairs of the ledger
        prefix (and the stream coordinates), so it remains available after
        the referenced events themselves have been deleted by retention
        cleanup -- the ledger and checkpoints are retained. Repeating the
        PUT replays the originally stored checksum and ``created_at``.
        """
        conn.execute("BEGIN IMMEDIATE")
        try:
            row, created = save_checkpoint(
                conn,
                device_id=device_id,
                stream_id=stream_id,
                through_seq=through_seq,
            )
            conn.commit()
        except UnknownStream as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
            ) from exc
        except StreamIntegrityConflict as exc:
            conn.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=exc.detail
            ) from exc
        except Exception:
            conn.rollback()
            raise
        if not created:
            response.status_code = status.HTTP_200_OK
        return checkpoint_out_from_row(row)

    @app.get(
        "/devices/{device_id}/streams/{stream_id}/checkpoints",
        response_model=list[CheckpointOut],
    )
    def list_stream_checkpoints_endpoint(
        device_id: str,
        stream_id: CommandId,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[CheckpointOut]:
        """List the stream's saved checkpoints ordered by through_seq."""
        if get_stream(conn, device_id, stream_id) is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"stream {stream_id!r} is not registered for device "
                    f"{device_id!r}"
                ),
            )
        return [
            checkpoint_out_from_row(row)
            for row in list_stream_checkpoints(conn, device_id, stream_id)
        ]

    @app.get(
        "/devices/{device_id}/streams/{stream_id}/checkpoints/{through_seq}",
        response_model=CheckpointOut,
        responses={404: {"description": "unknown stream or checkpoint"}},
    )
    def get_stream_checkpoint_endpoint(
        device_id: str,
        stream_id: CommandId,
        through_seq: CheckpointThroughSeq,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CheckpointOut:
        row = get_stream_checkpoint(conn, device_id, stream_id, through_seq)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"no checkpoint through {through_seq} for stream "
                    f"{stream_id!r} of device {device_id!r}"
                ),
            )
        return checkpoint_out_from_row(row)

    # ------------------------------------------------ transparency receipts

    def receipt_checkpoint_out_from_row(row: sqlite3.Row) -> ReceiptCheckpointOut:
        return ReceiptCheckpointOut(
            tree_size=row["tree_size"],
            root_hash=row["root_hash"],
            created_at=from_storage(row["created_at"]),
        )

    @app.post(
        "/receipts/checkpoints",
        response_model=ReceiptCheckpointOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"description": "a checkpoint already exists at this tree size (idempotent replay)"},
            422: {"description": "invalid tree size (below 1 or beyond the current tree)"},
        },
    )
    def create_receipt_checkpoint(
        response: Response,
        body: ReceiptCheckpointIn | None = None,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ReceiptCheckpointOut:
        """Freeze an immutable ``(tree_size, root_hash)`` checkpoint.

        The checkpoint covers the first ``tree_size`` receipt leaves (the
        current tree size when omitted). The Merkle root is recomputed
        from the retained leaves and stored once: the tree is append-only,
        so a repeated POST for the same size replays ``200`` with the
        originally stored root and ``created_at``. Sizes below 1 or beyond
        the current tree size return ``422`` and store nothing.
        """
        conn.execute("BEGIN IMMEDIATE")
        try:
            row, created = save_receipt_checkpoint(
                conn, tree_size=body.tree_size if body is not None else None
            )
            conn.commit()
        except ReceiptError as exc:
            conn.rollback()
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": exc.message},
            ) from exc
        except Exception:
            conn.rollback()
            raise
        if not created:
            response.status_code = status.HTTP_200_OK
        return receipt_checkpoint_out_from_row(row)

    @app.get(
        "/receipts/checkpoints",
        response_model=list[ReceiptCheckpointOut],
    )
    def list_receipt_checkpoints_endpoint(
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[ReceiptCheckpointOut]:
        """List every stored receipt checkpoint, ordered by tree size."""
        return [
            receipt_checkpoint_out_from_row(row)
            for row in list_receipt_checkpoints(conn)
        ]

    @app.get(
        "/receipts/checkpoints/{tree_size}",
        response_model=ReceiptCheckpointOut,
        responses={404: {"description": "no checkpoint stored at this tree size"}},
    )
    def get_receipt_checkpoint_endpoint(
        tree_size: Annotated[int, Path(ge=1)],
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ReceiptCheckpointOut:
        row = get_receipt_checkpoint(conn, tree_size)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"no receipt checkpoint stored at tree size {tree_size}",
            )
        return receipt_checkpoint_out_from_row(row)

    @app.get(
        "/receipts/proof/{event_id}",
        response_model=MembershipProofOut,
        responses={
            404: {"description": "the event has no receipt leaf"},
            422: {"description": "invalid tree size for the proof"},
        },
    )
    def get_membership_proof(
        event_id: str,
        tree_size: int | None = Query(
            default=None,
            ge=1,
            description=(
                "Tree size to prove against; defaults to the current size. "
                "Must exceed the leaf's index and not exceed the current size"
            ),
        ),
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> MembershipProofOut:
        """Inclusion proof for one event's receipt leaf, by ``event_id``.

        The response carries the full leaf (every field needed to recompute
        the RFC 6962 leaf hash offline), its 0-based ``leaf_index``, the
        ``tree_size`` and expected ``root_hash`` it verifies against, and
        the ``audit_path`` of sibling hashes. Verification needs nothing
        but this response. The leaf is read from the receipt tree, so the
        proof stays available and verifiable even after retention cleanup
        has deleted the event body.
        """
        try:
            proof = build_membership_proof(conn, event_id, tree_size=tree_size)
        except ReceiptError as exc:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": exc.message},
            ) from exc
        if proof is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"event {event_id!r} has no transparency receipt leaf",
            )
        leaf = proof.leaf
        return MembershipProofOut(
            event_id=event_id,
            leaf_index=leaf["leaf_index"],
            tree_size=proof.tree_size,
            root_hash=proof.root_hash,
            leaf=ReceiptLeafOut(
                seq=leaf["event_seq"],
                device_id=leaf["device_id"],
                event_id=leaf["event_id"],
                occurred_at=from_storage(leaf["occurred_at"]),
                event_type=leaf["event_type"],
                schema_version=leaf["schema_version"],
                payload=(
                    json.loads(leaf["payload"])
                    if leaf["payload"] is not None
                    else None
                ),
            ),
            audit_path=list(proof.audit_path),
        )

    @app.get(
        "/receipts/consistency",
        response_model=ConsistencyProofOut,
        responses={
            422: {
                "description": (
                    "invalid tree sizes (first > second, or beyond the "
                    "current tree)"
                )
            }
        },
    )
    def get_consistency_proof(
        first: int = Query(ge=1, description="Earlier tree size"),
        second: int = Query(ge=1, description="Later tree size"),
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ConsistencyProofOut:
        """RFC 6962 consistency proof between two tree sizes.

        Proves the tree grew append-only: every leaf covered by the
        ``first`` size is unchanged inside the ``second`` size. Both roots
        are recomputed from the retained leaves and therefore match the
        checkpoints stored for those sizes. Requires
        ``1 <= first <= second <= current size``; anything else returns
        ``422``. Verification needs only the returned data.
        """
        try:
            proof = build_consistency_proof(
                conn, first_size=first, second_size=second
            )
        except ReceiptError as exc:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": exc.message},
            ) from exc
        return ConsistencyProofOut(
            first_size=proof.first_size,
            first_root=proof.first_root,
            second_size=proof.second_size,
            second_root=proof.second_root,
            proof=list(proof.proof),
        )

    # ------------------------------------------------------- command queue

    def command_out_from_row(row: sqlite3.Row, now: datetime) -> CommandOut:
        lease_expires = (
            from_storage(row["expires_at"]) if row["expires_at"] is not None else None
        )
        not_before = (
            from_storage(row["not_before"])
            if row["not_before"] is not None
            else None
        )
        # An expired lease is transparently reported as queued: the stored
        # token is stale and the next claim rotates it and bumps attempts.
        delivery_status = derive_status(
            row["state"], row["lease_token"], lease_expires, not_before, now
        )
        return CommandOut(
            device_id=row["device_id"],
            command_id=row["command_id"],
            payload=json.loads(row["payload"]),
            status=delivery_status,
            attempts=row["attempts"],
            created_at=from_storage(row["created_at"]),
            updated_at=from_storage(row["updated_at"]),
            leased_at=(
                from_storage(row["leased_at"])
                if row["leased_at"] is not None
                else None
            ),
            lease_expires_at=(
                lease_expires if delivery_status == STATUS_LEASED else None
            ),
            not_before=(
                not_before if delivery_status == STATUS_RETRY_WAIT else None
            ),
            acked_at=(
                from_storage(row["acked_at"]) if row["acked_at"] is not None else None
            ),
        )

    @app.put(
        "/devices/{device_id}/commands/{command_id}",
        response_model=CommandOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {"description": "command_id exists for the device with a different payload"},
            422: {"description": "request body lacks a payload"},
        },
    )
    def submit_command(
        device_id: str,
        command_id: CommandId,
        body: CommandSubmitIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CommandOut:
        """Enqueue a downstream command (create), or replay it idempotently.

        Identity is ``(device_id, command_id)``: the same id on a different
        device is a different command. The arbitrary JSON payload is
        canonicalized, so two submissions with byte-identical JSON values
        (object key order irrelevant) are idempotent replays and return
        ``200`` without re-enqueuing or moving the command; a different
        payload under the same identity returns ``409`` and never overwrites
        the stored command.
        """
        canonical = canonical_json(body.payload)
        now = datetime.now(timezone.utc)
        replay = False
        try:
            with conn:
                conn.execute(
                    "INSERT INTO device_commands "
                    "(device_id, command_id, payload, state, attempts, "
                    "lease_token, leased_at, expires_at, not_before, "
                    "created_at, updated_at, acked_at) "
                    "VALUES (?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, ?, NULL)",
                    (
                        device_id,
                        command_id,
                        canonical,
                        STATE_PENDING,
                        to_storage(now),
                        to_storage(now),
                    ),
                )
        except sqlite3.IntegrityError:
            row = get_command(conn, device_id, command_id)
            # UNIQUE(device_id, command_id) is the only constraint that can
            # fire here, so the row must exist (unless a concurrent
            # transaction deleted it, which this API never does).
            assert row is not None
            if row["payload"] != canonical:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"command_id {command_id!r} is already queued for "
                        f"device {device_id!r} with a different payload"
                    ),
                )
            replay = True
            response.status_code = status.HTTP_200_OK
            row = get_command(conn, device_id, command_id)
            assert row is not None
            return command_out_from_row(row, datetime.now(timezone.utc))
        row = get_command(conn, device_id, command_id)
        assert row is not None
        return command_out_from_row(row, now)

    @app.post(
        "/devices/{device_id}/commands/claim",
        response_model=ClaimOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {
                "description": (
                    "empty queue, or the head command is still inside its "
                    "lease or its nack retry backoff; the head is never skipped"
                )
            }
        },
    )
    def claim_command(
        device_id: str,
        body: ClaimIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ClaimOut:
        """Claim the head command of a device under a fresh lease.

        Strictly FIFO: only the oldest pending command (by ``seq``) can be
        claimed. While the head holds an unexpired lease or is inside a
        nack-imposed ``retry_after`` backoff, the call fails with ``409``
        even if later commands are queued -- it never jumps over the head.
        A claim whose lease has since expired is a redelivery: a new
        unpredictable ``lease_token`` is issued, ``attempts`` is
        incremented, and the previous token can no longer ack/nack (``409``).

        The decision and the update happen in one ``BEGIN IMMEDIATE``
        transaction, so concurrent claims serialize on the single writer
        lock and at most one caller ever wins the head.
        """
        now = datetime.now(timezone.utc)
        conn.execute("BEGIN IMMEDIATE")
        head: sqlite3.Row | None = None
        try:
            head = head_pending_command(conn, device_id)
            if head is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"device {device_id!r} has no claimable command",
                )
            expires = (
                from_storage(head["expires_at"])
                if head["expires_at"] is not None
                else None
            )
            not_before = (
                from_storage(head["not_before"])
                if head["not_before"] is not None
                else None
            )
            current = derive_status(
                head["state"], head["lease_token"], expires, not_before, now
            )
            if current == STATUS_LEASED:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"head command {head['command_id']!r} is leased "
                        f"until {to_storage(expires)}; wait for expiry or ack/nack"
                    ),
                )
            if current == STATUS_RETRY_WAIT:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"head command {head['command_id']!r} is in retry "
                        f"backoff until {to_storage(not_before)}"
                    ),
                )

            # Claimable (queued, possibly an expired previous lease or an
            # elapsed backoff): rotate the token and count the delivery.
            token = secrets.token_urlsafe(32)
            leased_at = now
            new_expires = datetime.fromtimestamp(
                now.timestamp() + float(body.lease_seconds), tz=timezone.utc
            )
            attempts = head["attempts"] + 1
            conn.execute(
                "UPDATE device_commands SET lease_token = ?, leased_at = ?, "
                "expires_at = ?, not_before = NULL, attempts = ?, "
                "updated_at = ? WHERE seq = ?",
                (
                    token,
                    to_storage(leased_at),
                    to_storage(new_expires),
                    attempts,
                    to_storage(now),
                    head["seq"],
                ),
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise

        row = get_command(conn, device_id, head["command_id"])
        assert row is not None
        return ClaimOut(
            device_id=row["device_id"],
            command_id=row["command_id"],
            payload=json.loads(row["payload"]),
            attempts=row["attempts"],
            lease_token=row["lease_token"],
            leased_at=from_storage(row["leased_at"]),
            lease_expires_at=from_storage(row["expires_at"]),
            created_at=from_storage(row["created_at"]),
            updated_at=from_storage(row["updated_at"]),
        )

    def resolve_leased_command(
        conn: sqlite3.Connection,
        device_id: str,
        command_id: str,
        token: str,
        now: datetime,
    ) -> sqlite3.Row:
        """Look up a command for ack/nack and validate its current token.

        Must be called inside the endpoint's open ``BEGIN IMMEDIATE``
        transaction, so the row read here and the settlement UPDATE run
        under one write lock: a concurrent ack/nack/claim is either fully
        visible to this read or fully blocked behind it -- there is no
        window in which two settlers observe the same current token.

        Raises ``404`` when the command is unknown for the device and
        ``409`` when it is already completed, the token is wrong, or the
        token was current but has expired (the command is redeliverable,
        so the holder must re-claim). Token comparison is constant time.
        """
        row = get_command(conn, device_id, command_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"command {command_id!r} is not queued for device "
                    f"{device_id!r}"
                ),
            )
        stored_token = row["lease_token"]
        expires = (
            from_storage(row["expires_at"]) if row["expires_at"] is not None else None
        )
        if row["state"] == STATE_ACKED:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"command {command_id!r} is already acked",
            )
        if stored_token is None or not hmac.compare_digest(stored_token, token):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="lease_token is not the current token for this command",
            )
        if expires is None or expires <= now:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "lease_token has expired; re-claim the command to receive "
                    "a fresh token (the old one is invalid)"
                ),
            )
        return row

    @app.post(
        "/devices/{device_id}/commands/{command_id}/ack",
        response_model=CommandOut,
        responses={
            404: {"description": "unknown command for the device"},
            409: {"description": "already acked, or the token is wrong/expired"},
        },
    )
    def ack_command(
        device_id: str,
        command_id: CommandId,
        body: AckIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CommandOut:
        """Complete a command with its current, unexpired lease token.

        The 404 lookup, the token/expiry checks and the terminal UPDATE
        all happen in one ``BEGIN IMMEDIATE`` transaction. The UPDATE is
        itself conditional on the *same* token still being current and
        unexpired, so two concurrent acks (or an ack racing a nack or a
        lease-expiry reclaim) serialize on the write lock and exactly one
        request can ever settle the lease: losers see the post-settlement
        row and get ``409`` without changing anything.
        """
        now = datetime.now(timezone.utc)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = resolve_leased_command(
                conn, device_id, command_id, body.lease_token, now
            )
            # The current token is retained on the terminal row so a later
            # replay is reported as "already acked" (409) rather than
            # confused with a never-claimed command. The token and expiry
            # predicates repeat the validation atomically: if the lease was
            # just settled or rotated under another transaction, the row no
            # longer matches and rowcount is 0.
            settled = conn.execute(
                "UPDATE device_commands SET state = ?, updated_at = ?, "
                "acked_at = ? WHERE seq = ? AND state = ? AND lease_token = ? "
                "AND expires_at > ?",
                (
                    STATE_ACKED,
                    to_storage(now),
                    to_storage(now),
                    row["seq"],
                    STATE_PENDING,
                    body.lease_token,
                    to_storage(now),
                ),
            ).rowcount
            if settled != 1:
                # Lost the settlement race (concurrent ack/nack, or a
                # reclaim rotated the lease between the read and the write).
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        "lease settlement lost a concurrent race: the token "
                        "is no longer the current, unexpired token"
                    ),
                )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise

        row = get_command(conn, device_id, command_id)
        assert row is not None
        return command_out_from_row(row, datetime.now(timezone.utc))

    @app.post(
        "/devices/{device_id}/commands/{command_id}/nack",
        response_model=CommandOut,
        responses={
            404: {"description": "unknown command for the device"},
            409: {"description": "already acked, or the token is wrong/expired"},
            422: {"description": "negative retry_after_seconds"},
        },
    )
    def nack_command(
        device_id: str,
        command_id: CommandId,
        body: NackIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CommandOut:
        """Reject a delivery and schedule a retry.

        The current token is voided immediately (it can no longer ack or
        nack), the lease cleared, and the command reopens after
        ``retry_after_seconds`` (``0`` reopens it immediately). It remains
        the FIFO head throughout the backoff: no later command can be
        delivered until this one is claimed again (``attempts`` then
        increments) and completed.

        Like ack, the lookup, token/expiry validation and voiding UPDATE
        run in one ``BEGIN IMMEDIATE`` transaction, and the UPDATE only
        fires while the presented token is still the current, unexpired
        one. A nack that loses a race with a concurrent ack/nack or with a
        lease-expiry reclaim affects no rows and returns ``409``; in
        particular an ack that settled first is never turned back into a
        pending retry, and a first nack's backoff is never overwritten.
        """
        now = datetime.now(timezone.utc)
        not_before = datetime.fromtimestamp(
            now.timestamp() + float(body.retry_after_seconds), tz=timezone.utc
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = resolve_leased_command(
                conn, device_id, command_id, body.lease_token, now
            )
            settled = conn.execute(
                "UPDATE device_commands SET lease_token = NULL, "
                "leased_at = NULL, expires_at = NULL, not_before = ?, "
                "updated_at = ? WHERE seq = ? AND state = ? "
                "AND lease_token = ? AND expires_at > ?",
                (
                    to_storage(not_before),
                    to_storage(now),
                    row["seq"],
                    STATE_PENDING,
                    body.lease_token,
                    to_storage(now),
                ),
            ).rowcount
            if settled != 1:
                # The lease was settled (acked/nacked) or rotated by a
                # reclaim between the locked read and the write.
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        "lease settlement lost a concurrent race: the token "
                        "is no longer the current, unexpired token"
                    ),
                )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise

        row = get_command(conn, device_id, command_id)
        assert row is not None
        return command_out_from_row(row, datetime.now(timezone.utc))

    @app.get(
        "/devices/{device_id}/commands/{command_id}",
        response_model=CommandOut,
        responses={404: {"description": "unknown command for the device"}},
    )
    def get_command_status(
        device_id: str,
        command_id: CommandId,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CommandOut:
        """Inspect the current delivery status of a single command.

        Never reveals the lease token. Lease expiry and retry backoff are
        evaluated against the current time, so ``leased`` transitions to
        ``queued`` (and ``retry_wait`` to ``queued``) automatically.
        """
        row = get_command(conn, device_id, command_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"command {command_id!r} is not queued for device "
                    f"{device_id!r}"
                ),
            )
        return command_out_from_row(row, datetime.now(timezone.utc))

    # ----------------------------------------------------- retention cleanup

    @app.post(
        "/retention/cleanup-plans",
        response_model=CleanupPlanOut,
        status_code=status.HTTP_201_CREATED,
        responses={422: {"description": "empty protected window (start >= end)"}},
    )
    def create_cleanup_plan(
        body: CleanupPlanIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CleanupPlanOut:
        """Dry-run retention selection: no event is deleted.

        Selects events of ``device_id`` with ``occurred_at < before`` that
        fall outside every protected ``[start, end)`` window, returns them
        sorted by (occurred_at, event_id) together with a plan digest that
        binds the selection criteria to the candidate event ids.
        """
        before = normalize_ts(body.before)
        windows = normalize_windows((w.start, w.end) for w in body.protected_windows)

        plan_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc)
        # Snapshot inside an immediate transaction so a concurrent writer
        # cannot change the selected rows between selection and persistence.
        conn.execute("BEGIN IMMEDIATE")
        try:
            events = load_device_events(conn, body.device_id)
            candidates = select_candidates(events, before, windows)
            event_ids = [c.event_id for c in candidates]
            digest = plan_digest(body.device_id, before, windows, event_ids)
            snapshot = serialize_candidates(
                [event_to_candidate(candidate) for candidate in candidates]
            )
            conn.execute(
                "INSERT INTO cleanup_plans (plan_id, device_id, before_at, "
                "protected_windows, plan_digest, status, candidates, "
                "event_count, created_at, confirmed_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, NULL)",
                (
                    plan_id,
                    body.device_id,
                    to_storage(before),
                    canonical_json(windows_to_storage(windows)),
                    digest,
                    snapshot,
                    len(candidates),
                    to_storage(created_at),
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        row = get_plan_row(conn, plan_id)
        assert row is not None  # just inserted
        return plan_out_from_row(row)

    @app.get(
        "/retention/cleanup-plans/{plan_id}",
        response_model=CleanupPlanOut,
        responses={404: {"description": "unknown plan id"}},
    )
    def get_cleanup_plan(
        plan_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CleanupPlanOut:
        row = get_plan_row(conn, plan_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"cleanup plan {plan_id!r} is not known",
            )
        return plan_out_from_row(row)

    @app.post(
        "/retention/cleanup-plans/{plan_id}/confirm",
        response_model=CleanupPlanOut,
        responses={
            404: {"description": "unknown plan id"},
            409: {"description": "plan is invalidated: recomputed candidates differ"},
        },
    )
    def confirm_cleanup_plan(
        plan_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> CleanupPlanOut:
        """Apply a pending plan after recomputing the selection.

        The plan row is read again *after* acquiring the single write lock,
        so racing confirmations are serialized: the request that wins the
        lock recomputes candidates and either deletes the exact batch or
        invalidates the plan; every later request observes the resulting
        terminal state and simply replays it -- a confirmation that lost
        the race to an identical request is never reported as a conflict.

        When the recomputed candidate ids or digest differ from the plan,
        the plan is durably marked ``invalidated`` in that same write
        transaction and ``409`` is returned. Invalidated is terminal: no
        later confirmation of that plan may ever delete events, even if
        the candidate set later happens to look like the snapshot again.
        """
        # Cheap pre-lock lookup just to return 404 for unknown plans; the
        # authoritative read happens inside the write transaction below.
        if get_plan_row(conn, plan_id) is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"cleanup plan {plan_id!r} is not known",
            )

        # Acquire the single writer lock before any decision so that two
        # concurrent confirmations cannot both see a pending plan.
        conn.execute("BEGIN IMMEDIATE")
        row = get_plan_row(conn, plan_id)
        assert row is not None  # existence was checked before locking

        # Re-read under the lock: a racing request may already have reached
        # a terminal state. Confirmed plans replay the identical first
        # result without deleting again.
        if row["status"] == "confirmed":
            conn.rollback()  # read-only replay; release the write lock
            return plan_out_from_row(row)

        device_id = row["device_id"]
        before = from_storage(row["before_at"])
        windows = [
            (from_storage(start), from_storage(end))
            for start, end in json.loads(row["protected_windows"])
        ]
        stored_ids = [c["event_id"] for c in json.loads(row["candidates"])]
        stored_digest = row["plan_digest"]

        stale_detail: dict[str, Any] | None = None
        try:
            if row["status"] == "invalidated":
                # Drift was already detected by an earlier confirmation;
                # this state is terminal, so recompute only for reporting
                # and never delete.
                current = select_candidates(
                    load_device_events(conn, device_id), before, windows
                )
                current_ids = [event.event_id for event in current]
                current_digest = plan_digest(
                    device_id, before, windows, current_ids
                )
                stale_detail = {
                    "message": (
                        "cleanup plan was previously invalidated because the "
                        "candidate set drifted; it can no longer be confirmed"
                    ),
                    "plan_id": plan_id,
                    "planned_event_ids": stored_ids,
                    "current_event_ids": current_ids,
                    "planned_digest": stored_digest,
                    "current_digest": current_digest,
                }
            else:
                current = select_candidates(
                    load_device_events(conn, device_id), before, windows
                )
                current_ids = [event.event_id for event in current]
                current_digest = plan_digest(
                    device_id, before, windows, current_ids
                )

                if current_ids != stored_ids or current_digest != stored_digest:
                    stale_detail = {
                        "message": (
                            "cleanup plan is stale: the recomputed candidate "
                            "set no longer matches the plan; the plan has been "
                            "invalidated and can never delete events"
                        ),
                        "plan_id": plan_id,
                        "planned_event_ids": stored_ids,
                        "current_event_ids": current_ids,
                        "planned_digest": stored_digest,
                        "current_digest": current_digest,
                    }
                else:
                    deleted = 0
                    if stored_ids:
                        deleted = conn.execute(
                            "DELETE FROM events WHERE device_id = ? AND event_id IN "
                            "({})".format(",".join("?" for _ in stored_ids)),
                            (device_id, *stored_ids),
                        ).rowcount
                    if deleted != len(stored_ids):
                        # Candidates matched just above in this same
                        # transaction, so rows cannot have vanished; treat
                        # anything else as an internal consistency failure.
                        raise RuntimeError(
                            f"cleanup plan {plan_id!r}: deleted {deleted} of "
                            f"{len(stored_ids)} matched candidate rows"
                        )
                    conn.execute(
                        "UPDATE cleanup_plans SET status = 'confirmed', "
                        "confirmed_at = ? WHERE plan_id = ?",
                        (to_storage(datetime.now(timezone.utc)), plan_id),
                    )

            if stale_detail is not None:
                # First observation of drift (or a replay of an already
                # invalidated plan): persist the terminal status in this
                # same write transaction so no concurrent or later
                # confirmation can delete the snapshot's events.
                conn.execute(
                    "UPDATE cleanup_plans SET status = 'invalidated' "
                    "WHERE plan_id = ? AND status != 'invalidated'",
                    (plan_id,),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        if stale_detail is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=stale_detail
            )

        row = get_plan_row(conn, plan_id)
        assert row is not None  # just confirmed
        return plan_out_from_row(row)

    # ------------------------------------------------------------ search

    def search_row_to_out(row: sqlite3.Row) -> EventOut:
        return EventOut(
            device_id=row["device_id"],
            event_id=row["event_id"],
            occurred_at=from_storage(row["occurred_at"]),
            event_type=row["event_type"],
            schema_version=row["schema_version"],
            payload=(
                json.loads(row["payload"]) if row["payload"] is not None else None
            ),
        )

    @app.post(
        "/events/search",
        response_model=SearchEventsOut,
        responses={
            422: {
                "description": (
                    "malformed or outdated cursor, cursor bound to different "
                    "conditions, or invalid search parameters"
                )
            }
        },
    )
    def search_events(
        body: SearchEventsIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> SearchEventsOut:
        """Search events by scalar columns and payload conditions.

        Results are sorted by ``(occurred_at, event_id)`` and keyset-paged
        with opaque cursors. The first page pins the maximum persistent
        ``seq`` visible at that moment into its cursor, freezing the result
        window: later inserts never appear (``seq`` is AUTOINCREMENT and is
        never reused, even when the current maximum row is deleted), while
        rows deleted meanwhile are simply absent. A cursor is
        cryptographically bound to the normalized query -- resubmitting it
        with changed conditions or an invalid token yields ``422``; cursors
        from the previous (rowid-based) format are rejected with ``422`` and
        the search must be restarted without a cursor.
        """
        interval = body.occurred_at or OccurredAtRange()
        query = SearchQuery(
            device_id=body.device_id,
            event_type=body.event_type,
            schema_version=body.schema_version,
            occurred_start=(
                normalize_ts(interval.start) if interval.start is not None else None
            ),
            occurred_end=(
                normalize_ts(interval.end) if interval.end is not None else None
            ),
            conditions=tuple(
                Condition(
                    pointer=c.pointer,
                    operator=c.operator,  # type: ignore[arg-type]
                    value=c.value,
                )
                for c in body.conditions
            ),
        )
        fingerprint = query_fingerprint(query)

        if body.cursor is not None:
            try:
                page_cursor = decode_cursor(body.cursor)
            except SearchError as exc:
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={"message": str(exc)},
                ) from exc
            if not hmac.compare_digest(page_cursor.fingerprint, fingerprint):
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "message": (
                            "cursor is bound to a different normalized query: "
                            "restart the search without a cursor"
                        )
                    },
                )
            high_water = page_cursor.high_water
            after: tuple[str, str] | None = (
                page_cursor.occurred_at,
                page_cursor.event_id,
            )
        else:
            high_water = current_high_water(conn)
            after = None

        rows, has_next_page = scan_page(
            conn, query, high_water, after, body.limit
        )

        next_cursor: str | None = None
        if has_next_page:
            last = rows[-1]
            next_cursor = encode_cursor(
                Cursor(
                    version=CURSOR_VERSION,
                    fingerprint=fingerprint,
                    high_water=high_water,
                    occurred_at=last["occurred_at"],
                    event_id=last["event_id"],
                )
            )
        return SearchEventsOut(
            events=[search_row_to_out(row) for row in rows],
            next_cursor=next_cursor,
            limit=body.limit,
        )

    # ------------------------------------------------------------ alerts

    def rule_out_from_row(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        active: bool | None = None,
    ) -> AlertRuleOut:
        if active is None:
            latest = get_latest_version(conn, row["device_id"], row["rule_id"])
            active = latest is not None and latest["version"] == row["version"]
        return AlertRuleOut(
            device_id=row["device_id"],
            rule_id=row["rule_id"],
            version=row["version"],
            event_type=row["event_type"],
            conditions=[
                PayloadCondition(
                    pointer=item["pointer"],
                    operator=item["operator"],
                    value=item["value"],
                )
                for item in json.loads(row["conditions_json"])
            ],
            window_seconds=row["window_seconds"],
            consecutive=row["consecutive"],
            cooldown_seconds=row["cooldown_seconds"],
            active=active,
            created_seq=row["created_seq"],
            created_at=from_storage(row["created_at"]),
        )

    def alarm_out_from_row(row: sqlite3.Row, now: datetime) -> AlarmOut:
        return AlarmOut(
            alarm_id=row["alarm_id"],
            device_id=row["device_id"],
            rule_id=row["rule_id"],
            rule_version=row["rule_version"],
            status=derive_alarm_status(
                row["state"], from_storage(row["cooldown_until"]), now
            ),
            fired_at=from_storage(row["fired_at"]),
            first_match_at=from_storage(row["first_match_at"]),
            last_match_at=from_storage(row["last_match_at"]),
            cooldown_until=from_storage(row["cooldown_until"]),
            trigger_event_id=row["trigger_event_id"],
            member_event_ids=json.loads(row["member_event_ids"]),
            acked_at=(
                from_storage(row["acked_at"]) if row["acked_at"] is not None else None
            ),
            invalidated_at=(
                from_storage(row["invalidated_at"])
                if row["invalidated_at"] is not None
                else None
            ),
            created_at=from_storage(row["created_at"]),
            updated_at=from_storage(row["updated_at"]),
        )

    @app.put(
        "/devices/{device_id}/alert-rules/{rule_id}/versions/{version}",
        response_model=AlertRuleOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {
                "description": (
                    "version is not exactly one greater than the current latest"
                )
            },
            422: {"description": "invalid rule definition or conditions"},
        },
    )
    def create_alert_rule_version(
        device_id: str,
        rule_id: CommandId,
        version: int,
        body: AlertRuleIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> AlertRuleOut:
        """Register an immutable alert rule version for a device.

        Versions are append-only and numbered from 1 with no gaps: the
        first PUT must use version 1 and every later PUT must be exactly
        one greater than the current highest version. A registered version
        can never be modified. Versions partition event time into eras: at
        creation the device's current max (occurred_at, event_id) becomes
        the new version's exclusive lower frontier (it never backfills the
        known event set), and when a successor is added the same frontier
        closes this version's era. A late event arriving afterwards sorts
        into one version's era purely by event time, so the final result
        is independent of arrival order.
        """
        if body.version != version:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "version in the path and in the body must match",
                    "errors": [
                        {"path": "/version", "error": f"{version} != {body.version}"}
                    ],
                },
            )

        canonical_conditions = canonical_json(
            conditions_to_storage(
                tuple(
                    Condition(
                        pointer=c.pointer, operator=c.operator, value=c.value
                    )
                    for c in body.conditions
                )
            )
        )
        created_at = datetime.now(timezone.utc)
        # The version check, created_seq read, frontier pinning and insert
        # all happen under the single write lock, so two concurrent PUTs of
        # the same next version serialize: one wins, the other re-reads the
        # latest version inside the transaction and gets 409 instead of
        # racing on the UNIQUE constraint. Any event inserted afterwards
        # takes a strictly larger seq than created_seq.
        conn.execute("BEGIN IMMEDIATE")
        try:
            latest = get_latest_version(conn, device_id, rule_id)
            expected = 1 if latest is None else latest["version"] + 1
            if version != expected:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"rule {rule_id!r} versions are immutable and "
                        f"append-only: expected version {expected}, got {version}"
                    ),
                )
            created_seq = current_high_water_seq(conn)
            # Event-time era frontiers. The new version opens its era just
            # above the device's current max (occurred_at, event_id): it
            # never backfills the already known event set even by event
            # time, while a future late arrival sorting into the old era is
            # replayed against the predecessor instead. The same frontier
            # closes the predecessor version's era. Both stay NULL when no
            # event exists yet (version 1 has no lower bound regardless).
            frontier = current_event_frontier(conn, device_id)
            start_occurred, start_event_id = (
                frontier if frontier is not None else (None, None)
            )
            cur = conn.execute(
                "INSERT INTO alarm_rules (device_id, rule_id, version, "
                "event_type, conditions_json, window_seconds, consecutive, "
                "cooldown_seconds, created_seq, era_start_occurred_at, "
                "era_start_event_id, era_end_occurred_at, era_end_event_id, "
                "created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)",
                (
                    device_id,
                    rule_id,
                    version,
                    body.event_type,
                    canonical_conditions,
                    body.window_seconds,
                    body.consecutive,
                    body.cooldown_seconds,
                    created_seq,
                    start_occurred,
                    start_event_id,
                    to_storage(created_at),
                ),
            )
            rule_id_pk = cur.lastrowid
            if latest is not None and frontier is not None:
                # Close the predecessor era at the same frontier. The new
                # version starts strictly above it, so the eras partition
                # event time without overlap; late events with keys inside
                # the closed era keep replaying the frozen predecessor.
                conn.execute(
                    "UPDATE alarm_rules SET era_end_occurred_at = ?, "
                    "era_end_event_id = ? WHERE id = ?",
                    (start_occurred, start_event_id, latest["id"]),
                )
            # A new version starts with empty progress and no gate. The
            # predecessor keeps its own progress and stays reconciled
            # whenever a late event lands inside its closed era.
            conn.execute(
                "INSERT INTO alarm_rule_progress (rule_version_id, "
                "window_seconds, streak_event_ids, streak_times, gate_state, "
                "gate_fired_at, gate_cooldown_until, gate_acked_seq) "
                "VALUES (?, ?, '[]', '[]', NULL, NULL, NULL, NULL)",
                (rule_id_pk, body.window_seconds),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        row = get_rule_version(conn, device_id, rule_id, version)
        assert row is not None
        return rule_out_from_row(conn, row, active=True)

    @app.get(
        "/devices/{device_id}/alert-rules/{rule_id}/versions/{version}",
        response_model=AlertRuleOut,
        responses={404: {"description": "unknown rule version"}},
    )
    def get_alert_rule_version(
        device_id: str,
        rule_id: CommandId,
        version: int,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> AlertRuleOut:
        row = get_rule_version(conn, device_id, rule_id, version)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"alert rule {rule_id!r} version {version} is not "
                    f"registered for device {device_id!r}"
                ),
            )
        return rule_out_from_row(conn, row)

    @app.get(
        "/devices/{device_id}/alert-rules",
        response_model=list[AlertRuleOut],
    )
    def list_alert_rules(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[AlertRuleOut]:
        """List all registered rule versions of a device."""
        latest_versions = {
            row["rule_id"]: row["version"]
            for row in conn.execute(
                "SELECT rule_id, MAX(version) AS version FROM alarm_rules "
                "WHERE device_id = ? GROUP BY rule_id",
                (device_id,),
            ).fetchall()
        }
        return [
            rule_out_from_row(
                conn,
                row,
                active=latest_versions.get(row["rule_id"]) == row["version"],
            )
            for row in list_rules(conn, device_id)
        ]

    @app.get(
        "/devices/{device_id}/alarms",
        response_model=list[AlarmOut],
    )
    def list_alarms(
        device_id: str,
        status_filter: str | None = Query(default=None, alias="status"),
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[AlarmOut]:
        """List alarm instances of a device, newest firing first.

        ``status`` is an optional query filter: ``active``, ``expired``,
        ``acked`` or ``invalidated``. ``active``/``expired`` are derived
        against the current wall time from the stored event-time cooldown.
        """
        allowed = {
            STATUS_ACTIVE,
            STATUS_EXPIRED,
            ALARM_STATUS_ACKED,
            STATUS_INVALIDATED,
        }
        if status_filter is not None and status_filter not in allowed:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "unsupported status filter",
                    "errors": [
                        {"path": "/status", "error": "expected one of "
                         + ", ".join(sorted(allowed))}
                    ],
                },
            )
        now = datetime.now(timezone.utc)
        out = [alarm_out_from_row(row, now) for row in list_device_alarms(conn, device_id)]
        if status_filter is not None:
            out = [alarm for alarm in out if alarm.status == status_filter]
        return out

    @app.post(
        "/devices/{device_id}/alarms/{alarm_id}/ack",
        response_model=AlarmOut,
        responses={
            404: {"description": "unknown alarm for the device"},
            409: {
                "description": (
                    "alarm no longer satisfies the precondition (different "
                    "current status/version, or it was invalidated); "
                    "concurrent acknowledgements: exactly one transitions it"
                )
            },
            422: {"description": "invalid expected_status value"},
        },
    )
    def acknowledge_alarm_endpoint(
        device_id: str,
        alarm_id: str,
        body: AlarmAckIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> AlarmOut:
        """Acknowledge an alarm with an optimistic status/version precondition.

        Both ``expected_status`` and ``expected_rule_version`` must match
        the alarm's *current* values:

        * ``active`` / ``expired`` -> the alarm is acknowledged (terminal);
        * ``acked`` -> an idempotent replay of a previous acknowledgement
          returns ``200`` with the same stored ``acked_at``;
        * any mismatch (the cooldown-derived status changed, the rule
          version differs, or the alarm was invalidated by an
          out-of-order recomputation) returns ``409`` and changes nothing.

        Acknowledging pins the current arrival high-water: only events that
        arrive *after* the acknowledgement can fire the rule again,
        regardless of the cooldown remaining in event time. The lookup,
        precondition check and conditional UPDATE run in one
        ``BEGIN IMMEDIATE`` transaction, so concurrent acks serialize and
        at most one transitions the row.
        """
        row = get_alarm(conn, device_id, alarm_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"alarm {alarm_id!r} is not known for device {device_id!r}",
            )

        now = datetime.now(timezone.utc)
        current_status = derive_alarm_status(
            row["state"], from_storage(row["cooldown_until"]), now
        )
        version_mismatch = row["rule_version"] != body.expected_rule_version
        status_mismatch = current_status != body.expected_status

        if row["state"] == "invalidated":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "alarm was invalidated by an out-of-order event "
                    "recomputation and can no longer be acknowledged"
                ),
            )
        if version_mismatch or status_mismatch:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": (
                        "alarm no longer matches the acknowledgement precondition"
                    ),
                    "current_status": current_status,
                    "current_rule_version": row["rule_version"],
                },
            )

        if row["state"] == "acked":
            # Idempotent replay of the same acknowledged terminal state.
            return alarm_out_from_row(row, now)

        conn.execute("BEGIN IMMEDIATE")
        try:
            updated = acknowledge_alarm(conn, row, now)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        if updated != 1:
            # A concurrent ack (or an out-of-order reconciliation that
            # invalidated the firing) changed the row first.
            latest_row = get_alarm(conn, device_id, alarm_id)
            assert latest_row is not None
            if latest_row["state"] == "invalidated":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        "alarm was invalidated by a concurrent out-of-order "
                        "event recomputation and can no longer be acknowledged"
                    ),
                )
            # Lost the ack race; surface the current state as a conflict so
            # the caller can re-check its precondition.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "alarm was acknowledged by a concurrent request",
                    "current_status": "acked",
                    "current_rule_version": latest_row["rule_version"],
                },
            )

        latest_row = get_alarm(conn, device_id, alarm_id)
        assert latest_row is not None
        return alarm_out_from_row(latest_row, datetime.now(timezone.utc))

    # ------------------------------------------------------- state machines

    def machine_version_out_from_row(row: sqlite3.Row) -> StateMachineVersionOut:
        info = version_from_row(row)
        document = definition_to_document(info.definition)
        return StateMachineVersionOut(
            device_id=row["device_id"],
            version=row["version"],
            initial_state=document["initial_state"],
            states=document["states"],
            transitions=document["transitions"],
            created_seq=row["created_seq"],
            created_at=from_storage(row["created_at"]),
        )

    def machine_status_out_from_row(row: sqlite3.Row) -> StateMachineStatusOut:
        return StateMachineStatusOut(
            device_id=row["device_id"],
            version=row["version"],
            current_state=row["current_state"],
            last_event_seq=row["last_event_seq"],
            enabled_at=from_storage(row["enabled_at"]),
            updated_at=from_storage(row["updated_at"]),
        )

    @app.put(
        "/devices/{device_id}/state-machine/versions/{version}",
        response_model=StateMachineVersionOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {
                "description": (
                    "version is not exactly one greater than the current "
                    "latest, or the immutable definition already differs"
                )
            },
            422: {"description": "invalid state machine definition or conditions"},
        },
    )
    def create_state_machine_version(
        device_id: str,
        version: int,
        body: StateMachineVersionIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StateMachineVersionOut:
        """Register an immutable state-machine version for a device.

        Versions are append-only and numbered from 1 with no gaps: the first
        PUT must use version 1 and every later PUT must be exactly one
        greater than the current highest version. A registered version can
        never be modified; an identical PUT replays ``200`` without side
        effects.
        """
        if body.version != version:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "version in the path and in the body must match",
                    "errors": [
                        {"path": "/version", "error": f"{version} != {body.version}"}
                    ],
                },
            )
        try:
            definition = build_definition(
                body.initial_state,
                body.states,
                [
                    {
                        "from_state": t.from_state,
                        "event_type": t.event_type,
                        "to_state": t.to_state,
                        "conditions": [
                            {"pointer": c.pointer, "operator": c.operator,
                             "value": c.value}
                            for c in t.conditions
                        ],
                    }
                    for t in body.transitions
                ],
            )
        except StateMachineError as exc:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": exc.message, "errors": exc.errors},
            ) from exc

        canonical = canonical_json(definition_to_document(definition))
        created_at = datetime.now(timezone.utc)
        conn.execute("BEGIN IMMEDIATE")
        try:
            latest = get_latest_machine_version(conn, device_id)
            expected = 1 if latest is None else latest["version"] + 1
            if version != expected:
                existing = get_machine_version(conn, device_id, version)
                if existing is not None and existing["definition_json"] == canonical:
                    response.status_code = status.HTTP_200_OK
                    conn.rollback()  # read-only replay; release the write lock
                    return machine_version_out_from_row(existing)
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"state machine versions for device {device_id!r} are "
                        f"immutable and append-only: expected version "
                        f"{expected}, got {version}"
                    ),
                )
            created_seq = device_event_high_water(conn, device_id)
            insert_version(
                conn, device_id, version, definition, created_seq, created_at
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise

        row = get_machine_version(conn, device_id, version)
        assert row is not None
        return machine_version_out_from_row(row)

    @app.get(
        "/devices/{device_id}/state-machine/versions/{version}",
        response_model=StateMachineVersionOut,
        responses={404: {"description": "unknown state machine version"}},
    )
    def get_state_machine_version(
        device_id: str,
        version: int,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StateMachineVersionOut:
        row = get_machine_version(conn, device_id, version)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"state machine version {version} is not registered for "
                    f"device {device_id!r}"
                ),
            )
        return machine_version_out_from_row(row)

    @app.get(
        "/devices/{device_id}/state-machine/versions",
        response_model=list[StateMachineVersionOut],
    )
    def list_state_machine_versions(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[StateMachineVersionOut]:
        return [
            machine_version_out_from_row(row)
            for row in list_machine_versions(conn, device_id)
        ]

    @app.post(
        "/devices/{device_id}/state-machine/enable",
        response_model=StateMachineStatusOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            404: {"description": "unknown state machine version for the device"},
            409: {
                "description": (
                    "a different version is already enabled; the enabled "
                    "version is immutable"
                )
            },
        },
    )
    def enable_state_machine(
        device_id: str,
        body: StateMachineEnableIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StateMachineStatusOut:
        """Enable one registered, immutable version for the device.

        The current state starts at the version's ``initial_state`` and the
        arrival high-water is pinned to the device's current MAX(seq): only
        events written *afterwards* are projected, strictly in arrival
        order. The enabled version can never be switched: enabling the same
        version again replays ``200``; enabling a different version returns
        ``409``.
        """
        enabled_at = datetime.now(timezone.utc)
        conn.execute("BEGIN IMMEDIATE")
        try:
            version_row = get_machine_version(conn, device_id, body.version)
            if version_row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        f"state machine version {body.version} is not "
                        f"registered for device {device_id!r}"
                    ),
                )
            machine_row = get_machine(conn, device_id)
            if machine_row is not None:
                if machine_row["version"] == body.version:
                    response.status_code = status.HTTP_200_OK
                    conn.rollback()  # read-only replay; release the write lock
                    return machine_status_out_from_row(machine_row)
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"device {device_id!r} has state machine version "
                        f"{machine_row['version']} enabled; the enabled "
                        "version is immutable and cannot be switched"
                    ),
                )
            enable_machine(conn, device_id, version_row, enabled_at)
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise

        row = get_machine(conn, device_id)
        assert row is not None
        return machine_status_out_from_row(row)

    @app.get(
        "/devices/{device_id}/state-machine",
        response_model=StateMachineStatusOut,
        responses={404: {"description": "device has no enabled state machine"}},
    )
    def get_state_machine_status(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> StateMachineStatusOut:
        """Current projected state of the enabled machine.

        Returns the enabled version, the current state, the last projected
        event's arrival seq and the enable/update timestamps. The current
        state is persisted on every projection, so it survives restarts.
        """
        row = get_machine(conn, device_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"device {device_id!r} has no enabled state machine"
                ),
            )
        return machine_status_out_from_row(row)

    @app.get(
        "/devices/{device_id}/state-machine/transitions",
        response_model=TransitionLogPageOut,
        responses={
            404: {"description": "device has no enabled state machine"},
            422: {"description": "malformed or incompatible cursor"},
        },
    )
    def list_state_transitions(
        device_id: str,
        limit: int = Query(default=50, ge=1, le=500),
        cursor: str | None = Query(default=None, max_length=2048),
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> TransitionLogPageOut:
        """Page through the append-only transition log in arrival order.

        Keyset pagination by the log's AUTOINCREMENT id (projection order,
        identical to arrival order). The opaque HMAC-signed cursor carries
        its own limit; changing ``limit`` on a cursor walk keeps the
        cursor-embedded page size (matching the search endpoint behavior).
        """
        machine_row = get_machine(conn, device_id)
        if machine_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"device {device_id!r} has no enabled state machine",
            )
        if cursor is not None:
            try:
                page_cursor = decode_log_cursor(cursor)
            except StateMachineError as exc:
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={"message": exc.message},
                ) from exc
            after_id = page_cursor.after_id
            page_limit = page_cursor.limit
        else:
            after_id = 0
            page_limit = limit

        rows, has_next = scan_log_page(conn, device_id, after_id, page_limit)
        next_cursor: str | None = None
        if has_next:
            next_cursor = encode_log_cursor(
                LogCursor(version=1, after_id=rows[-1]["id"], limit=page_limit)
            )
        return TransitionLogPageOut(
            device_id=device_id,
            version=machine_row["version"],
            transitions=[log_entry_to_dict(row) for row in rows],
            next_cursor=next_cursor,
            limit=page_limit,
        )

    # ------------------------------------------------------- clock calibration

    def clock_conflict(exc: ClockCalibrationError) -> HTTPException:
        detail: dict[str, Any] = {"message": exc.message, "reason": exc.reason}
        detail.update(exc.extra)
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)

    @app.put(
        "/devices/{device_id}/clock/anchors/{anchor_id}",
        response_model=ClockAnchorOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            409: {
                "description": (
                    "anchor_id exists with different content, observed_seq "
                    "does not strictly increase, reference_time does not "
                    "strictly increase, or in-epoch drift exceeds drift_ppm; "
                    "nothing is stored"
                )
            }
        },
    )
    def submit_clock_anchor(
        device_id: str,
        anchor_id: CommandId,
        body: ClockAnchorIn,
        response: Response,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ClockAnchorOut:
        """Submit (or idempotently replay) one clock calibration anchor.

        Anchors validate strictly in observation order: ``observed_seq`` and
        ``reference_time`` must each strictly exceed the device's latest
        accepted anchor. A non-advancing ``device_time`` (equal to or before
        the previous reading) is treated as a clock reset and opens a new
        epoch; otherwise the measured rate mismatch (ppm) against the
        previous anchor must stay within ``CLOCK_DRIFT_PPM``. Any violation
        returns ``409`` and leaves no row behind. Re-PUTting the exact same
        anchor content is a ``200`` idempotent replay.
        """
        if body.anchor_id != anchor_id:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "anchor_id in the path and in the body must match",
                    "errors": [
                        {"path": "/anchor_id", "error": f"{anchor_id} != {body.anchor_id}"}
                    ],
                },
            )
        device_time = normalize_ts(body.device_time)
        reference_time = normalize_ts(body.reference_time)

        created_at = datetime.now(timezone.utc)
        # The replay/conflict check, the chain validation and the insert run
        # under the single write lock, so two concurrent anchors of one
        # device serialize and the monotonicity checks see a stable chain.
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = get_anchor(conn, device_id, anchor_id)
            if existing is not None:
                same_content = (
                    existing["observed_seq"] == body.observed_seq
                    and existing["device_time"] == to_storage(device_time)
                    and existing["reference_time"] == to_storage(reference_time)
                )
                if not same_content:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "message": (
                                f"anchor_id {anchor_id!r} already exists for "
                                f"device {device_id!r} with different content"
                            ),
                            "reason": REASON_ANCHOR_CONTENT_CONFLICT,
                        },
                    )
                response.status_code = status.HTTP_200_OK
                conn.rollback()  # read-only replay; release the write lock
                replay_row = conn.execute(
                    "SELECT created_at FROM clock_anchors "
                    "WHERE device_id = ? AND anchor_id = ?",
                    (device_id, anchor_id),
                ).fetchone()
                return ClockAnchorOut(
                    device_id=existing["device_id"],
                    anchor_id=existing["anchor_id"],
                    observed_seq=existing["observed_seq"],
                    device_time=from_storage(existing["device_time"]),
                    reference_time=from_storage(existing["reference_time"]),
                    epoch=existing["epoch"],
                    drift_ppm=existing["drift_ppm"],
                    created_at=from_storage(replay_row["created_at"]),
                )

            anchors = load_device_anchors(conn, device_id)
            try:
                epoch, measured_drift = validate_and_plan(
                    anchors,
                    anchor_id=anchor_id,
                    observed_seq=body.observed_seq,
                    device_time=device_time,
                    reference_time=reference_time,
                    drift_ppm_limit=app.state.drift_ppm,
                )
            except ClockCalibrationError as exc:
                raise clock_conflict(exc) from exc
            insert_anchor(
                conn,
                device_id=device_id,
                anchor_id=anchor_id,
                observed_seq=body.observed_seq,
                device_time=device_time,
                reference_time=reference_time,
                epoch=epoch,
                drift_ppm=measured_drift,
                created_at=created_at,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise

        row = conn.execute(
            "SELECT device_id, anchor_id, observed_seq, device_time, "
            "reference_time, epoch, drift_ppm, created_at FROM clock_anchors "
            "WHERE device_id = ? AND anchor_id = ?",
            (device_id, anchor_id),
        ).fetchone()
        assert row is not None
        return ClockAnchorOut(
            device_id=row["device_id"],
            anchor_id=row["anchor_id"],
            observed_seq=row["observed_seq"],
            device_time=from_storage(row["device_time"]),
            reference_time=from_storage(row["reference_time"]),
            epoch=row["epoch"],
            drift_ppm=row["drift_ppm"],
            created_at=from_storage(row["created_at"]),
        )

    @app.get(
        "/devices/{device_id}/clock/anchors",
        response_model=list[ClockAnchorOut],
    )
    def list_clock_anchors(
        device_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> list[ClockAnchorOut]:
        """List a device's calibration anchors in observation order."""
        rows = conn.execute(
            "SELECT seq, device_id, anchor_id, observed_seq, device_time, "
            "reference_time, epoch, drift_ppm, created_at FROM clock_anchors "
            "WHERE device_id = ? ORDER BY observed_seq",
            (device_id,),
        ).fetchall()
        return [
            ClockAnchorOut(
                device_id=row["device_id"],
                anchor_id=row["anchor_id"],
                observed_seq=row["observed_seq"],
                device_time=from_storage(row["device_time"]),
                reference_time=from_storage(row["reference_time"]),
                epoch=row["epoch"],
                drift_ppm=row["drift_ppm"],
                created_at=from_storage(row["created_at"]),
            )
            for row in rows
        ]

    @app.post(
        "/devices/{device_id}/clock/normalize",
        response_model=ClockNormalizedOut,
        responses={
            409: {
                "description": (
                    "no anchors / unknown epoch, observed_seq precedes the "
                    "first anchor, or device_time is beyond the configured "
                    "extrapolation range"
                )
            }
        },
    )
    def normalize_clock_event(
        device_id: str,
        body: ClockNormalizeIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ClockNormalizedOut:
        """Normalize one device clock reading to reference time.

        Uses the device's latest epoch by default; pass ``observed_seq`` to
        select the epoch active at that observation (the epoch whose last
        anchor is the greatest one not exceeding it) or an explicit
        ``epoch``. The mapping is exact at an anchor (``anchor``), a linear
        interpolation between adjacent in-epoch anchors
        (``interpolate``), or a linear extension outside the support
        (``extrapolate``) only while within
        ``CLOCK_MAX_EXTRAPOLATION_SECONDS``. The response carries the epoch,
        the method, the bracketing anchor ids and an absolute uncertainty
        interval.
        """
        anchors = load_device_anchors(conn, device_id)
        try:
            result = normalize_device_time(
                anchors,
                body.device_time,
                drift_ppm_limit=app.state.drift_ppm,
                max_extrapolation_seconds=app.state.max_extrapolation_seconds,
                observed_seq=body.observed_seq,
                epoch=body.epoch,
            )
        except ClockCalibrationError as exc:
            raise clock_conflict(exc) from exc
        return ClockNormalizedOut(
            device_id=device_id,
            device_time=normalize_ts(body.device_time),
            corrected_at=result.corrected_at,
            epoch=result.epoch,
            method=result.method,
            uncertainty_seconds=result.uncertainty_seconds,
            uncertainty=UncertaintyInterval(lower=result.lower, upper=result.upper),
            bracket_anchor_ids=list(result.bracket_anchor_ids),
        )

    # Columns read for each timeline row; payload text is canonicalized at
    # insert time and parsed only for the output document.
    _TIMELINE_COLUMNS = (
        "seq, device_id, event_id, occurred_at, event_type, schema_version, payload"
    )
    _TIMELINE_BATCH = 1000

    @app.post(
        "/clock/timeline",
        response_model=ClockTimelineOut,
        responses={
            422: {"description": "malformed cursor, or cursor bound to a different device filter"}
        },
    )
    def clock_timeline(
        body: ClockTimelineIn,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> ClockTimelineOut:
        """Cross-device event timeline ordered by corrected reference time.

        Every stored event's ``occurred_at`` is treated as a device clock
        reading and normalized with that device's latest anchor chain.
        Events of devices without anchors (or outside the configured
        extrapolation range) are absent. The first page freezes two marks
        into its cursor: the global events high-water (``events.seq``) and
        each device's anchor high-water (``clock_anchors.seq``). Neither
        events nor anchors written afterwards can enter the walk, so later
        pages -- sorted by
        ``(corrected_at, device_id, event_id)`` keyset -- neither repeat nor
        skip rows even when a newly added anchor would have remapped an
        earlier event. A cursor is cryptographically bound to the device
        filter; changing it or tampering with the token yields ``422``.
        """
        device_filter = tuple(sorted(set(body.device_ids))) if body.device_ids else None
        fingerprint = timeline_fingerprint(device_filter)

        if body.cursor is not None:
            try:
                page_cursor = decode_timeline_cursor(body.cursor)
            except ValueError as exc:
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={"message": str(exc)},
                ) from exc
            if not hmac.compare_digest(page_cursor.fingerprint, fingerprint):
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "message": (
                            "cursor is bound to a different device filter: "
                            "restart the timeline without a cursor"
                        )
                    },
                )
            waters = page_cursor.high_waters
            event_high_water = page_cursor.events_high_water
            after: tuple[str, str, str] | None = (
                page_cursor.after_corrected,
                page_cursor.after_device,
                page_cursor.after_event,
            )
            page_limit = page_cursor.limit
        else:
            if device_filter is not None:
                device_scope = list(device_filter)
            else:
                device_scope = [
                    row["device_id"]
                    for row in conn.execute(
                        "SELECT DISTINCT device_id FROM clock_anchors"
                    ).fetchall()
                ]
            waters = frozen_anchor_high_waters(conn, device_scope)
            event_high_water = int(
                conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS m FROM events"
                ).fetchone()["m"]
            )
            after = None
            page_limit = body.limit

        # Frozen per-device anchor chains. Devices whose frozen mark is 0
        # (no anchor visible at the first page) contribute nothing.
        chains: dict[str, list] = {}
        for device_id, hi in waters.items():
            if hi > 0:
                chain = load_device_anchors(conn, device_id, hi)
                if chain:
                    chains[device_id] = chain

        # Normalize the whole frozen event window once (batched scans keep
        # memory bounded), then sort the normalized keys and slice the
        # keyset page. The window is frozen on both axes: events.seq pins
        # the row set and the per-device clock_anchors.seq pins the
        # calibration, so newly written events or anchors can neither move
        # an already-emitted key nor insert before it.
        entries: list[tuple[str, str, str, sqlite3.Row, Any]] = []
        clauses = ["seq <= ?"]
        params: list[Any] = [event_high_water]
        if device_filter is not None:
            clauses.append(
                "device_id IN ({})".format(",".join("?" for _ in device_filter))
            )
            params.extend(device_filter)
        where_sql = " AND ".join(clauses)
        scan_after = 0
        while True:
            rows = conn.execute(
                f"SELECT {_TIMELINE_COLUMNS} FROM events WHERE {where_sql} "
                "AND seq > ? ORDER BY seq LIMIT ?",
                [*params, scan_after, _TIMELINE_BATCH],
            ).fetchall()
            if not rows:
                break
            for row in rows:
                scan_after = int(row["seq"])
                chain = chains.get(row["device_id"])
                if not chain:
                    continue
                occurred = from_storage(row["occurred_at"])
                try:
                    normalized = normalize_device_time(
                        chain,
                        occurred,
                        drift_ppm_limit=app.state.drift_ppm,
                        max_extrapolation_seconds=app.state.max_extrapolation_seconds,
                    )
                except ClockCalibrationError:
                    # Unmappable (beyond the extrapolation range): absent
                    # from the timeline rather than failing the whole page.
                    continue
                entries.append(
                    (
                        to_storage(normalized.corrected_at),
                        row["device_id"],
                        row["event_id"],
                        row,
                        normalized,
                    )
                )
            if len(rows) < _TIMELINE_BATCH:
                break

        entries.sort(key=lambda item: item[:3])
        start = 0
        if after is not None:
            # Strict keyset continuation after (corrected_at, device_id, event_id).
            start = next(
                (i for i, item in enumerate(entries) if item[:3] > after),
                len(entries),
            )
        page = entries[start : start + page_limit]
        has_next = start + page_limit < len(entries)

        next_cursor: str | None = None
        if has_next:
            last_corrected, last_device, last_event, _, _ = page[-1]
            next_cursor = encode_timeline_cursor(
                TimelineCursor(
                    version=TIMELINE_CURSOR_VERSION,
                    fingerprint=fingerprint,
                    high_waters=dict(waters),
                    events_high_water=event_high_water,
                    after_corrected=last_corrected,
                    after_device=last_device,
                    after_event=last_event,
                    limit=page_limit,
                )
            )

        return ClockTimelineOut(
            events=[
                ClockTimelineEventOut(
                    corrected_at=normalized.corrected_at,
                    epoch=normalized.epoch,
                    method=normalized.method,
                    uncertainty_seconds=normalized.uncertainty_seconds,
                    device_id=row["device_id"],
                    event_id=row["event_id"],
                    occurred_at=from_storage(row["occurred_at"]),
                    event_type=row["event_type"],
                    schema_version=row["schema_version"],
                    payload=(
                        json.loads(row["payload"])
                        if row["payload"] is not None
                        else None
                    ),
                )
                for _, _, _, row, normalized in page
            ],
            next_cursor=next_cursor,
            limit=page_limit,
            frozen_anchor_high_waters={
                device_id: hi for device_id, hi in waters.items() if hi > 0
            },
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()

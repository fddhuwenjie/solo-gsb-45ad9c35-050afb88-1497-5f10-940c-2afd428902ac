"""Collection integrity: per-device source streams, sequence ledgers, checkpoints.

A *collection stream* is a device's own ordered, gap-detectable ingestion
channel, registered independently of events. Events of a stream are
submitted with the device-assigned ``source_seq`` (numbered from 1);
out-of-order and late backfill are both allowed. A ledger table binds each
``(device_id, stream_id, source_seq)`` to exactly one ``event_id`` (and,
within a stream, each ``event_id`` to exactly one ``source_seq``); the
actual event is stored through the *existing* ingestion pipeline inside
the caller's transaction, so state-machine projection and alert
reconciliation behave exactly as they do for ``POST /events``.

The ledger and the checkpoints are deliberately independent of the
``events`` rows: retention cleanup may delete events, but the sequence
ledger (what arrived in which slot) and every saved checkpoint survive,
so integrity can still be reported and a checkpoint digest can still be
recomputed after the referenced event payloads are gone.

Apart from the small SQL helpers taking an open connection, the pure
helpers (ledger compression, checksum) hold no state; callers (the
FastAPI endpoints) own the transactions, exactly as for the other
storage-backed modules.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from .ingestion import EventIngestionService, IngestionOutcome, canonical_json
from .sessions import to_storage

# Full column lists; kept here so every SELECT reads a stable shape.
STREAM_COLUMNS = "device_id, stream_id, created_at"
STREAM_EVENT_COLUMNS = "device_id, stream_id, source_seq, event_id, created_at"
CHECKPOINT_COLUMNS = (
    "device_id, stream_id, through_seq, checksum_sha256, event_count, created_at"
)

# Domain-separation prefix for the checkpoint material; the version suffix
# is part of the wire algorithm documented in the README.
CHECKPOINT_DIGEST_PREFIX = b"device-event-stream-checkpoint:1\n"


class UnknownStream(Exception):
    """The ``(device_id, stream_id)`` pair is not registered (HTTP 404)."""

    def __init__(self, device_id: str, stream_id: str):
        super().__init__(
            f"stream {stream_id!r} is not registered for device {device_id!r}"
        )
        self.device_id = device_id
        self.stream_id = stream_id


class IntegrityConflict(Exception):
    """A ledger/checkpoint constraint was violated (HTTP 409 semantics).

    ``detail`` is the exact response detail payload -- a plain string or a
    structured object -- that the route re-raises as ``409`` after rolling
    back the transaction.
    """

    def __init__(self, detail: Any):
        super().__init__(detail if isinstance(detail, str) else detail.get("message"))
        self.detail = detail


@dataclass(frozen=True)
class IntegrityStatus:
    """Compressed integrity view of one stream's sequence ledger.

    ``highest_seen`` is the greatest accepted ``source_seq`` (0 while the
    ledger is empty); ``contiguous_through`` is the greatest n such that
    every slot 1..n is occupied (0 when slot 1 is missing);
    ``missing_ranges`` compresses the gaps inside 1..highest_seen into
    inclusive ``[start, end]`` pairs.
    """

    event_count: int
    highest_seen: int
    contiguous_through: int
    missing_ranges: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class StreamEventResult:
    """Outcome of one stream-event submission, ready to map onto a response."""

    ingestion: IngestionOutcome
    # True when the (stream, source_seq) -> event_id binding already existed:
    # the whole submission was an idempotent replay (HTTP 200).
    replay: bool


# ------------------------------------------------------------- pure helpers


def compress_runs(values: Sequence[int]) -> list[tuple[int, int]]:
    """Compress sorted integers into inclusive ``[start, end]`` runs."""
    runs: list[tuple[int, int]] = []
    for value in values:
        if runs and value == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], value)
        else:
            runs.append((value, value))
    return runs


def integrity_status(source_seqs: Sequence[int]) -> IntegrityStatus:
    """Compute highest/contiguous/missing from a set of accepted seqs."""
    ordered = sorted(source_seqs)
    if not ordered:
        return IntegrityStatus(0, 0, 0, ())
    highest = ordered[-1]
    present = set(ordered)
    contiguous = 0
    for candidate in range(1, highest + 1):
        if candidate not in present:
            break
        contiguous = candidate
    missing = [candidate for candidate in range(1, highest + 1)
               if candidate not in present]
    return IntegrityStatus(
        event_count=len(present),
        highest_seen=highest,
        contiguous_through=contiguous,
        missing_ranges=tuple(compress_runs(missing)),
    )


def checkpoint_checksum(
    device_id: str,
    stream_id: str,
    entries: Sequence[tuple[int, str]],
) -> str:
    """SHA-256 over the ordered ``(source_seq, event_id)`` ledger prefix.

    ``entries`` must be ordered by ``source_seq``. The material is
    canonical JSON (key order irrelevant, UTF-8) under a versioned domain
    prefix; the stream coordinates bind the digest to one stream, so two
    streams over identical event sequences still produce distinct
    checksums.
    """
    material = canonical_json(
        {
            "device_id": device_id,
            "stream_id": stream_id,
            "entries": [[int(seq), event_id] for seq, event_id in entries],
        }
    )
    return hashlib.sha256(
        CHECKPOINT_DIGEST_PREFIX + material.encode("utf-8")
    ).hexdigest()


# ----------------------------------------------------------------- SQL: streams


def get_stream(
    conn: sqlite3.Connection, device_id: str, stream_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {STREAM_COLUMNS} FROM collection_streams "
        "WHERE device_id = ? AND stream_id = ?",
        (device_id, stream_id),
    ).fetchone()


def list_streams(
    conn: sqlite3.Connection, device_id: str
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            f"SELECT {STREAM_COLUMNS} FROM collection_streams "
            "WHERE device_id = ? ORDER BY stream_id",
            (device_id,),
        ).fetchall()
    )


def register_stream(
    conn: sqlite3.Connection,
    device_id: str,
    stream_id: str,
    created_at: datetime,
) -> bool:
    """Insert the stream unless it already exists. Returns True when created.

    The caller owns the (immediate) write transaction; a re-PUT of an
    existing stream matches ``OR IGNORE`` and reports ``False`` without
    touching ``created_at``.
    """
    cur = conn.execute(
        "INSERT OR IGNORE INTO collection_streams "
        "(device_id, stream_id, created_at) VALUES (?, ?, ?)",
        (device_id, stream_id, to_storage(created_at)),
    )
    return cur.rowcount == 1


# ------------------------------------------------------------- SQL: ledger


def get_ledger_entry(
    conn: sqlite3.Connection, device_id: str, stream_id: str, source_seq: int
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {STREAM_EVENT_COLUMNS} FROM collection_stream_events "
        "WHERE device_id = ? AND stream_id = ? AND source_seq = ?",
        (device_id, stream_id, source_seq),
    ).fetchone()


def get_ledger_entry_for_event(
    conn: sqlite3.Connection, device_id: str, stream_id: str, event_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {STREAM_EVENT_COLUMNS} FROM collection_stream_events "
        "WHERE device_id = ? AND stream_id = ? AND event_id = ?",
        (device_id, stream_id, event_id),
    ).fetchone()


def stream_source_seqs(
    conn: sqlite3.Connection, device_id: str, stream_id: str
) -> list[int]:
    return [
        int(row["source_seq"])
        for row in conn.execute(
            "SELECT source_seq FROM collection_stream_events "
            "WHERE device_id = ? AND stream_id = ? ORDER BY source_seq",
            (device_id, stream_id),
        ).fetchall()
    ]


def ledger_entries_through(
    conn: sqlite3.Connection, device_id: str, stream_id: str, through_seq: int
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            f"SELECT {STREAM_EVENT_COLUMNS} FROM collection_stream_events "
            "WHERE device_id = ? AND stream_id = ? AND source_seq <= ? "
            "ORDER BY source_seq",
            (device_id, stream_id, through_seq),
        ).fetchall()
    )


def insert_ledger_entry(
    conn: sqlite3.Connection,
    *,
    device_id: str,
    stream_id: str,
    source_seq: int,
    event_id: str,
    created_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO collection_stream_events "
        "(device_id, stream_id, source_seq, event_id, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (device_id, stream_id, source_seq, event_id, to_storage(created_at)),
    )


# -------------------------------------------------------- SQL: checkpoints


def get_checkpoint(
    conn: sqlite3.Connection, device_id: str, stream_id: str, through_seq: int
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {CHECKPOINT_COLUMNS} FROM collection_stream_checkpoints "
        "WHERE device_id = ? AND stream_id = ? AND through_seq = ?",
        (device_id, stream_id, through_seq),
    ).fetchone()


def list_checkpoints(
    conn: sqlite3.Connection, device_id: str, stream_id: str
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            f"SELECT {CHECKPOINT_COLUMNS} FROM collection_stream_checkpoints "
            "WHERE device_id = ? AND stream_id = ? ORDER BY through_seq",
            (device_id, stream_id),
        ).fetchall()
    )


def insert_checkpoint(
    conn: sqlite3.Connection,
    *,
    device_id: str,
    stream_id: str,
    through_seq: int,
    checksum_sha256: str,
    event_count: int,
    created_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO collection_stream_checkpoints "
        "(device_id, stream_id, through_seq, checksum_sha256, event_count, "
        "created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            device_id,
            stream_id,
            through_seq,
            checksum_sha256,
            event_count,
            to_storage(created_at),
        ),
    )


# ------------------------------------------------------- write orchestration


def submit_stream_event(
    conn: sqlite3.Connection,
    *,
    ingestion: EventIngestionService,
    device_id: str,
    stream_id: str,
    source_seq: int,
    event_id: str,
    occurred_at: datetime,
    event_type: str | None = None,
    schema_version: str | None = None,
    payload: Any = None,
    payload_provided: bool = False,
    now: datetime | None = None,
) -> StreamEventResult:
    """Bind one ``source_seq`` slot and ingest its event in the caller's tx.

    The caller must have begun a ``BEGIN IMMEDIATE`` transaction and commits
    (or rolls back) afterwards: the ledger decision, the event insert, the
    state-machine projection and the alert recomputation all commit or
    roll back together.

    Ledger semantics:

    * the slot already holds this same ``event_id``: idempotent replay --
      the existing event is replayed through the normal ingestion path
      (which itself compares the full event content, so a different body
      still yields the usual event-content ``409``);
    * the slot holds a different ``event_id``: ``409``
      (``source_seq_conflict``), nothing is written;
    * the ``event_id`` is already bound to another slot of the same
      stream: ``409`` (``event_id_conflict``), nothing is written;
    * otherwise the event is ingested (newly inserted or, when it was
      first seen on another stream / via ``POST /events``, replayed) and
      the fresh slot binding is stored in the same transaction.

    Raises :class:`UnknownStream` (404), :class:`IntegrityConflict`
    (409), and the ingestion service's own ``ContractError`` (422) /
    ``IngestionConflict`` (409).
    """
    if get_stream(conn, device_id, stream_id) is None:
        raise UnknownStream(device_id, stream_id)
    if now is None:
        now = datetime.now(timezone.utc)

    slot = get_ledger_entry(conn, device_id, stream_id, source_seq)
    if slot is not None:
        if slot["event_id"] != event_id:
            raise IntegrityConflict(
                {
                    "message": (
                        f"source_seq {source_seq} of stream {stream_id!r} is "
                        f"already bound to event_id {slot['event_id']!r}; a "
                        "slot can be bound to exactly one event_id"
                    ),
                    "reason": "source_seq_conflict",
                    "source_seq": source_seq,
                    "bound_event_id": slot["event_id"],
                    "conflicting_event_id": event_id,
                }
            )
        # Identical slot binding: replay the whole request through the
        # ingestion pipeline, so the stored full-content comparison and
        # the response view stay identical to POST /events replay.
        outcome = ingestion.ingest(
            conn,
            device_id=device_id,
            event_id=event_id,
            occurred_at=occurred_at,
            event_type=event_type,
            schema_version=schema_version,
            payload=payload,
            payload_provided=payload_provided,
            now=now,
        )
        return StreamEventResult(ingestion=outcome, replay=True)

    other_slot = get_ledger_entry_for_event(
        conn, device_id, stream_id, event_id
    )
    if other_slot is not None:
        raise IntegrityConflict(
            {
                "message": (
                    f"event_id {event_id!r} is already bound to source_seq "
                    f"{other_slot['source_seq']} in stream {stream_id!r}; an "
                    "event_id can occupy exactly one slot of a stream"
                ),
                "reason": "event_id_conflict",
                "source_seq": source_seq,
                "bound_source_seq": other_slot["source_seq"],
                "event_id": event_id,
            }
        )

    outcome = ingestion.ingest(
        conn,
        device_id=device_id,
        event_id=event_id,
        occurred_at=occurred_at,
        event_type=event_type,
        schema_version=schema_version,
        payload=payload,
        payload_provided=payload_provided,
        now=now,
    )
    insert_ledger_entry(
        conn,
        device_id=device_id,
        stream_id=stream_id,
        source_seq=source_seq,
        event_id=event_id,
        created_at=now,
    )
    return StreamEventResult(ingestion=outcome, replay=False)


def save_checkpoint(
    conn: sqlite3.Connection,
    *,
    device_id: str,
    stream_id: str,
    through_seq: int,
    now: datetime | None = None,
) -> tuple[sqlite3.Row, bool]:
    """Save a checkpoint over the gap-free ``1..through_seq`` prefix.

    Returns ``(row, created)``: ``created`` is False for a repeated PUT,
    which replays the originally stored result (same checksum and same
    ``created_at``) without recomputation. When any slot in
    ``1..through_seq`` is missing -- including ``through_seq`` beyond the
    current high-water -- raises :class:`IntegrityConflict`
    (``checkpoint_gap``) and stores nothing. The checksum is the SHA-256
    over the ordered ``(source_seq, event_id)`` pairs and depends only on
    the ledger, which survives event cleanup.
    """
    if get_stream(conn, device_id, stream_id) is None:
        raise UnknownStream(device_id, stream_id)

    existing = get_checkpoint(conn, device_id, stream_id, through_seq)
    if existing is not None:
        return existing, False

    status = integrity_status(stream_source_seqs(conn, device_id, stream_id))
    if status.contiguous_through < through_seq:
        gap_ranges = [
            [start, min(end, through_seq)]
            for start, end in status.missing_ranges
            if start <= through_seq
        ]
        raise IntegrityConflict(
            {
                "message": (
                    f"cannot save a checkpoint through {through_seq}: the "
                    "ledger has gaps inside 1.."
                    f"{through_seq} (contiguous only through "
                    f"{status.contiguous_through})"
                ),
                "reason": "checkpoint_gap",
                "through_seq": through_seq,
                "highest_seen": status.highest_seen,
                "contiguous_through": status.contiguous_through,
                "missing_ranges": gap_ranges,
            }
        )

    rows = ledger_entries_through(conn, device_id, stream_id, through_seq)
    # The contiguity check above guarantees exactly through_seq rows with
    # source_seq 1..through_seq; stay defensive against ledger corruption.
    if len(rows) != through_seq or [int(r["source_seq"]) for r in rows] != list(
        range(1, through_seq + 1)
    ):
        raise RuntimeError(
            f"stream {stream_id!r} ledger is inconsistent with its "
            f"contiguous prefix 1..{through_seq}"
        )
    checksum = checkpoint_checksum(
        device_id,
        stream_id,
        [(int(row["source_seq"]), row["event_id"]) for row in rows],
    )
    if now is None:
        now = datetime.now(timezone.utc)
    insert_checkpoint(
        conn,
        device_id=device_id,
        stream_id=stream_id,
        through_seq=through_seq,
        checksum_sha256=checksum,
        event_count=through_seq,
        created_at=now,
    )
    saved = get_checkpoint(conn, device_id, stream_id, through_seq)
    assert saved is not None
    return saved, True

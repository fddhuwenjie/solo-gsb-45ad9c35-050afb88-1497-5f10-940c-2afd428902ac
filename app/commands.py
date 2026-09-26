"""Persistent per-device downstream command queue.

Every device owns a strict FIFO queue: commands are delivered strictly in
creation order (the global, never-reused ``seq`` column). Claiming the
head command grants an unpredictable ``lease_token`` valid for
``lease_seconds`` (1..300); only the *current*, unexpired token can ack
(complete) or nack (release with a retry backoff) the command.

A claim never skips the head: while the head holds an active lease or a
nack-imposed retry backoff, every claim fails with 409 even though later
commands already sit behind it. Once the lease expires the head may be
claimed again -- the token rotates, ``attempts`` increments and the old
token can never ack or nack anymore (409).

Storage keeps only the minimal state: a terminal flag (``state``), the
current lease and the retry backoff instant. The user-facing delivery
status is *derived* from those columns against the current time in
:func:`derive_status`, so lease expiry needs no background sweeper.

Apart from the small SQL helpers taking an open connection, this module
is pure: callers (the FastAPI endpoints) own the transactions. Every
state-changing lease operation -- claim, ack and nack alike -- runs
inside ``BEGIN IMMEDIATE`` so concurrent operations on one command
serialize on SQLite's single writer lock. Ack/nack additionally make
their UPDATE conditional on the presented ``lease_token`` still being
current and unexpired (and check the affected row count), so two
settlers presenting the same token can never both succeed: the loser
of the write-lock race sees the settled row and gets ``409``.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

# Stored lifecycle states. Only completion is persisted as a state; lease
# and retry situations are derived from the timestamps below.
STATE_PENDING = "pending"  # not completed yet; delivery status is derived
STATE_ACKED = "acked"  # terminal: delivered and acknowledged

# Derived delivery statuses reported by the API.
STATUS_QUEUED = "queued"  # available for the next claim
STATUS_LEASED = "leased"  # head held by an active, unexpired lease
STATUS_RETRY_WAIT = "retry_wait"  # nacked; closed until not_before elapses
STATUS_ACKED = "acked"  # terminal

# Lease duration window accepted by POST .../commands/claim.
MIN_LEASE_SECONDS = 1.0
MAX_LEASE_SECONDS = 300.0

# Full column list of a command row; kept here so every SELECT reads the
# same shape regardless of the SQLite version's table order.
COMMAND_COLUMNS = (
    "seq, device_id, command_id, payload, state, attempts, "
    "lease_token, leased_at, expires_at, not_before, created_at, "
    "updated_at, acked_at"
)


def derive_status(
    state: str,
    lease_token: str | None,
    expires_at: datetime | None,
    not_before: datetime | None,
    now: datetime,
) -> str:
    """Derive the externally visible delivery status from stored columns.

    ``expires_at`` / ``not_before`` are already parsed aware-UTC datetimes
    (or ``None``). An expired lease is not ``leased`` -- the command is
    back to ``queued`` and may be claimed again, which rotates the token.
    A nacked command is ``retry_wait`` only until ``not_before``; once the
    backoff elapses it is ``queued`` again.
    """
    if state == STATE_ACKED:
        return STATUS_ACKED
    if lease_token is not None and expires_at is not None and expires_at > now:
        return STATUS_LEASED
    if not_before is not None and not_before > now:
        return STATUS_RETRY_WAIT
    return STATUS_QUEUED


def get_command(
    conn: sqlite3.Connection, device_id: str, command_id: str
) -> sqlite3.Row | None:
    """Fetch one command by its per-device identity, or ``None``."""
    return conn.execute(
        f"SELECT {COMMAND_COLUMNS} FROM device_commands "
        "WHERE device_id = ? AND command_id = ?",
        (device_id, command_id),
    ).fetchone()


def head_pending_command(
    conn: sqlite3.Connection, device_id: str
) -> sqlite3.Row | None:
    """Select the oldest not-yet-acked command of a device.

    This *is* the queue head: acked commands leave the delivery queue, and
    ordering by the monotonic ``seq`` realizes strict per-device creation
    order. The caller is responsible for deciding whether the head is
    currently claimable (active lease / retry backoff block it).
    """
    return conn.execute(
        f"SELECT {COMMAND_COLUMNS} FROM device_commands "
        "WHERE device_id = ? AND state = ? "
        "ORDER BY seq LIMIT 1",
        (device_id, STATE_PENDING),
    ).fetchone()

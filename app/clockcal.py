"""Device clock calibration: anchors, epochs, normalization and timeline paging.

A device occasionally reports a *calibration anchor* pairing its own clock
reading (``device_time``) with the reference clock at the same observation
(``reference_time``). Anchors of one device are validated strictly in
observation order (``observed_seq``):

* ``anchor_id`` is idempotent: re-PUTting the exact same anchor content is a
  no-op replay; the same id with different content returns 409;
* ``observed_seq`` must be strictly greater than every accepted anchor;
* ``reference_time`` must strictly increase (including across epochs);
* a non-advancing ``device_time`` (equal or backwards) marks a clock reset:
  a new *epoch* opens, and no drift check or mapping line ever spans two
  epochs;
* inside an epoch the measured rate mismatch (drift ppm) against the
  previous anchor must not exceed the configured ``drift_ppm`` limit,
  otherwise the anchor is rejected with 409 and never stored.

A device time is normalized to reference time with the anchors of one
epoch:

* at an exact anchor position the mapping is exact (``anchor``);
* between two adjacent anchors it is the linear interpolation of the pair
  (``interpolate``);
* outside the bracketed range it is extrapolated with the nearest pair's
  slope (the single anchor's slope for a one-anchor epoch, i.e. clock rate
  1:1), but only while within the configured extrapolation budget;
* every result carries an absolute uncertainty interval whose half-width
  grows with the distance from the calibration support.

The cross-device timeline normalizes stored events onto one corrected time
axis. Its first page freezes, for every device, the anchor high-water
(MAX of the never-reused ``clock_anchors.seq``) into an HMAC-signed cursor:
anchors written afterwards change live normalization but can never remap
the rows of an ongoing page walk, so pagination can neither repeat nor
skip.

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
from datetime import datetime, timezone
from typing import Any, NamedTuple

from .search import canonical_json
from .sessions import from_storage, normalize_ts, to_storage

# Reasons carried by 409 responses; clients can branch on these.
REASON_SEQ_CONFLICT = "observed_seq_conflict"
REASON_REFERENCE_NOT_MONOTONIC = "reference_time_not_monotonic"
REASON_DRIFT_EXCEEDED = "drift_ppm_exceeded"
REASON_ANCHOR_CONTENT_CONFLICT = "anchor_content_conflict"
REASON_NO_ANCHORS = "no_calibration_anchors"
REASON_UNCALIBRATED_SEQUENCE = "observed_seq_before_first_anchor"
REASON_EXTRAPOLATION_LIMIT = "outside_extrapolation_range"

DEFAULT_DRIFT_PPM = 50_000.0  # 5% rate mismatch
DEFAULT_EXTRAPOLATION_SECONDS = 3600.0

# Opaque timeline cursors, integrity-protected with a server-side HMAC key.
# The per-device anchor high-water map is carried inside the cursor, so the
# freeze is immune to later anchors without a server-side snapshot table.
CURSOR_PREFIX = "ctl1:"
CURSOR_VERSION = 1
_CURSOR_KEY = b"device-clock-timeline-cursor-hmac-key-v1"


class ClockCalibrationError(Exception):
    """A client-correctable calibration failure (reported as 409)."""

    def __init__(self, reason: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.extra = extra


@dataclass(frozen=True)
class Anchor:
    device_id: str
    anchor_id: str
    observed_seq: int
    device_time: datetime
    reference_time: datetime
    epoch: int
    drift_ppm: float | None  # None for the epoch-opening anchor
    seq: int | None = None  # persistent arrival sequence of the anchor row

    @property
    def device_seconds(self) -> float:
        return self.device_time.timestamp()

    @property
    def reference_seconds(self) -> float:
        return self.reference_time.timestamp()


@dataclass(frozen=True)
class Normalization:
    corrected_at: datetime
    epoch: int
    method: str  # 'anchor' | 'interpolate' | 'extrapolate'
    uncertainty_seconds: float  # symmetric half-width
    lower: datetime
    upper: datetime
    bracket_anchor_ids: tuple[str | None, str | None]


# ------------------------------------------------------------ SQL helpers

ANCHOR_COLUMNS = (
    "seq, device_id, anchor_id, observed_seq, device_time, "
    "reference_time, epoch, drift_ppm"
)


def anchor_from_row(row: sqlite3.Row) -> Anchor:
    return Anchor(
        device_id=row["device_id"],
        anchor_id=row["anchor_id"],
        observed_seq=int(row["observed_seq"]),
        device_time=from_storage(row["device_time"]),
        reference_time=from_storage(row["reference_time"]),
        epoch=int(row["epoch"]),
        drift_ppm=row["drift_ppm"],
        seq=int(row["seq"]),
    )


def get_anchor(
    conn: sqlite3.Connection, device_id: str, anchor_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {ANCHOR_COLUMNS} FROM clock_anchors "
        "WHERE device_id = ? AND anchor_id = ?",
        (device_id, anchor_id),
    ).fetchone()


def latest_anchor(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row | None:
    """The most recently observed anchor (maximum observed_seq) of a device."""
    return conn.execute(
        f"SELECT {ANCHOR_COLUMNS} FROM clock_anchors WHERE device_id = ? "
        "ORDER BY observed_seq DESC LIMIT 1",
        (device_id,),
    ).fetchone()


def load_device_anchors(
    conn: sqlite3.Connection, device_id: str, seq_high_water: int | None = None
) -> list[Anchor]:
    """All anchors of a device in observation order.

    When ``seq_high_water`` is given, only anchors with
    ``seq <= seq_high_water`` are read -- the frozen view used while walking
    a timeline cursor.
    """
    if seq_high_water is None:
        where, params = "device_id = ?", (device_id,)
    else:
        where, params = "device_id = ? AND seq <= ?", (device_id, seq_high_water)
    rows = conn.execute(
        f"SELECT {ANCHOR_COLUMNS} FROM clock_anchors WHERE {where} "
        "ORDER BY observed_seq",
        params,
    ).fetchall()
    return [anchor_from_row(row) for row in rows]


def frozen_anchor_high_waters(
    conn: sqlite3.Connection, device_ids: list[str] | tuple[str, ...]
) -> dict[str, int]:
    """Per-device MAX(anchor seq) at the first page of a timeline walk."""
    if not device_ids:
        return {}
    placeholders = ",".join("?" for _ in device_ids)
    rows = conn.execute(
        "SELECT device_id, COALESCE(MAX(seq), 0) AS hi FROM clock_anchors "
        f"WHERE device_id IN ({placeholders}) GROUP BY device_id",
        tuple(device_ids),
    ).fetchall()
    return {row["device_id"]: int(row["hi"]) for row in rows}


def insert_anchor(
    conn: sqlite3.Connection,
    *,
    device_id: str,
    anchor_id: str,
    observed_seq: int,
    device_time: datetime,
    reference_time: datetime,
    epoch: int,
    drift_ppm: float | None,
    created_at: datetime,
) -> int:
    cur = conn.execute(
        "INSERT INTO clock_anchors (device_id, anchor_id, observed_seq, "
        "device_time, reference_time, epoch, drift_ppm, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            device_id,
            anchor_id,
            observed_seq,
            to_storage(device_time),
            to_storage(reference_time),
            epoch,
            drift_ppm,
            to_storage(created_at),
        ),
    )
    return int(cur.lastrowid)


# ------------------------------------------------------------ acceptance

def validate_and_plan(
    anchors: list[Anchor],
    *,
    anchor_id: str,
    observed_seq: int,
    device_time: datetime,
    reference_time: datetime,
    drift_ppm_limit: float,
) -> tuple[int, float | None]:
    """Decide epoch and measured drift for a candidate anchor.

    ``anchors`` is the device's accepted anchor chain in observation order.
    Returns ``(epoch, drift_ppm)`` where drift is None for an epoch-opening
    anchor. Raises :class:`ClockCalibrationError` (-> 409, nothing stored)
    when observation order, reference-time monotonicity or the drift budget
    is violated.
    """
    if anchors:
        last = anchors[-1]
        if observed_seq <= last.observed_seq:
            raise ClockCalibrationError(
                REASON_SEQ_CONFLICT,
                "observed_seq must be strictly greater than the device's "
                f"latest accepted anchor {last.observed_seq}",
                latest_observed_seq=last.observed_seq,
            )
        if reference_time <= last.reference_time:
            raise ClockCalibrationError(
                REASON_REFERENCE_NOT_MONOTONIC,
                "reference_time must be strictly greater than the latest "
                "accepted anchor's reference_time",
                latest_reference_time=to_storage(last.reference_time),
            )
        if device_time <= last.device_time:
            # Clock reset (a non-advancing device clock): open a new epoch.
            # No drift comparison spans the reset.
            return last.epoch + 1, None
        device_span = (device_time - last.device_time).total_seconds()
        reference_span = (reference_time - last.reference_time).total_seconds()
        # Measured rate mismatch relative to true elapsed time.
        drift = abs(reference_span - device_span) / reference_span * 1_000_000.0
        if drift > drift_ppm_limit:
            raise ClockCalibrationError(
                REASON_DRIFT_EXCEEDED,
                f"measured drift {drift:.3f} ppm exceeds the configured "
                f"limit of {drift_ppm_limit:g} ppm; anchor rejected",
                measured_drift_ppm=round(drift, 6),
                drift_ppm_limit=drift_ppm_limit,
                previous_anchor_id=last.anchor_id,
            )
        return last.epoch, drift
    return 1, None


# ------------------------------------------------------------ calibration

def _linear_correct(
    anchor: Anchor,
    slope: float,
    t: float,
) -> float:
    """Reference time on the line through ``anchor`` with device/reference slope."""
    return anchor.reference_seconds + (t - anchor.device_seconds) * slope


def _slope(left: Anchor, right: Anchor | None = None) -> float:
    if right is None:
        # A single anchor calibrates the offset only; assume rate 1:1.
        return 1.0
    device_span = right.device_seconds - left.device_seconds
    reference_span = right.reference_seconds - left.reference_seconds
    return reference_span / device_span


def _uncertainty_interpolation(
    t: float, left: Anchor, right: Anchor, drift_ppm_limit: float
) -> float:
    """Half-width inside a bracket: the allowed drift rate, accumulated from
    whichever bracket edge is farther away."""
    budget = drift_ppm_limit / 1_000_000.0
    return budget * max(t - left.device_seconds, right.device_seconds - t)


def _uncertainty_extrapolation(
    *,
    chain: list[Anchor],
    t: float,
    slope: float,
    drift_ppm_limit: float,
) -> float:
    """Half-width outside the support: budget drift plus the pair's residual.

    The support edge is the nearest end anchor; the measured slope residual
    (zero by construction for a one-anchor epoch) additionally displaces the
    extension of the fitted line.
    """
    budget = drift_ppm_limit / 1_000_000.0
    measured = abs(slope - 1.0) if len(chain) >= 2 else 0.0
    if t < chain[0].device_seconds:
        edge = chain[0].device_seconds
    else:
        edge = chain[-1].device_seconds
    return (budget + measured) * abs(t - edge)


def normalize_device_time(
    anchors: list[Anchor],
    device_time: datetime,
    *,
    drift_ppm_limit: float,
    max_extrapolation_seconds: float,
    observed_seq: int | None = None,
    epoch: int | None = None,
) -> Normalization:
    """Map one device clock reading onto reference time.

    The usable epoch is the explicitly requested one, else the epoch whose
    last anchor has the greatest ``observed_seq`` not exceeding the event's
    ``observed_seq``, else the device's latest epoch. Raises
    :class:`ClockCalibrationError` (-> 409) when no calibration exists, the
    sequence predates every anchor, the epoch is unknown, or the reading lies
    outside the calibration support by more than the extrapolation budget.
    """
    if not anchors:
        raise ClockCalibrationError(
            REASON_NO_ANCHORS,
            "device has no calibration anchors; submit one before normalizing",
        )

    epochs: dict[int, list[Anchor]] = {}
    for anchor in anchors:
        epochs.setdefault(anchor.epoch, []).append(anchor)

    if epoch is not None:
        if epoch not in epochs:
            raise ClockCalibrationError(
                REASON_NO_ANCHORS,
                f"device has no anchors in epoch {epoch}",
            )
        chosen = epoch
    elif observed_seq is not None:
        eligible = [
            chain[-1]
            for chain in epochs.values()
            if chain[-1].observed_seq <= observed_seq
        ]
        if not eligible:
            raise ClockCalibrationError(
                REASON_UNCALIBRATED_SEQUENCE,
                "observed_seq precedes every calibration anchor of the device",
                first_observed_seq=anchors[0].observed_seq,
            )
        chosen = max(eligible, key=lambda a: a.observed_seq).epoch
    else:
        chosen = max(epochs)

    chain = epochs[chosen]  # already in observation order
    t = normalize_ts(device_time).timestamp()

    exact = next(
        (a for a in chain if a.device_seconds == t),
        None,
    )
    if exact is not None:
        corrected = exact.reference_time
        return Normalization(
            corrected_at=corrected,
            epoch=chosen,
            method="anchor",
            uncertainty_seconds=0.0,
            lower=corrected,
            upper=corrected,
            bracket_anchor_ids=(exact.anchor_id, exact.anchor_id),
        )

    # Locate the adjacent in-epoch anchors.
    left: Anchor | None = None
    right: Anchor | None = None
    for anchor in chain:
        if anchor.device_seconds <= t:
            left = anchor
        if anchor.device_seconds > t and right is None:
            right = anchor
            break

    if left is not None and right is not None:
        # Between two anchors: linear interpolation of the adjacent pair.
        method = "interpolate"
        slope = _slope(left, right)
        bracket = (left.anchor_id, right.anchor_id)
        half = _uncertainty_interpolation(t, left, right, drift_ppm_limit)
    elif left is not None:
        # Past the support: extend with the last pair's slope (rate 1:1 for
        # a one-anchor epoch), only within the configured range.
        beyond = t - left.device_seconds
        if beyond > max_extrapolation_seconds:
            raise ClockCalibrationError(
                REASON_EXTRAPOLATION_LIMIT,
                "device_time is past the last anchor by "
                f"{beyond:.3f}s, beyond the configured extrapolation "
                f"range of {max_extrapolation_seconds:g}s",
                edge_anchor_id=left.anchor_id,
                beyond_seconds=round(beyond, 6),
                max_extrapolation_seconds=max_extrapolation_seconds,
            )
        method = "extrapolate"
        slope = _slope(chain[-2], left) if len(chain) >= 2 else 1.0
        bracket = (left.anchor_id, None)
        half = _uncertainty_extrapolation(
            chain=chain, t=t, slope=slope, drift_ppm_limit=drift_ppm_limit
        )
    else:
        # Before the support: symmetric treatment at the first anchor.
        assert right is not None
        beyond = right.device_seconds - t
        if beyond > max_extrapolation_seconds:
            raise ClockCalibrationError(
                REASON_EXTRAPOLATION_LIMIT,
                "device_time is before the first anchor by "
                f"{beyond:.3f}s, beyond the configured extrapolation "
                f"range of {max_extrapolation_seconds:g}s",
                edge_anchor_id=right.anchor_id,
                beyond_seconds=round(beyond, 6),
                max_extrapolation_seconds=max_extrapolation_seconds,
            )
        method = "extrapolate"
        slope = _slope(chain[0], chain[1] if len(chain) >= 2 else None)
        bracket = (None, right.anchor_id)
        half = _uncertainty_extrapolation(
            chain=chain, t=t, slope=slope, drift_ppm_limit=drift_ppm_limit
        )

    base = left if left is not None else right
    corrected_seconds = _linear_correct(base, slope, t)
    corrected = datetime.fromtimestamp(corrected_seconds, tz=timezone.utc)
    return Normalization(
        corrected_at=corrected,
        epoch=chosen,
        method=method,
        uncertainty_seconds=max(half, 0.0),
        lower=datetime.fromtimestamp(corrected_seconds - half, tz=timezone.utc),
        upper=datetime.fromtimestamp(corrected_seconds + half, tz=timezone.utc),
        bracket_anchor_ids=bracket,
    )


# ------------------------------------------------------------ timeline cursor

class TimelineCursor(NamedTuple):
    version: int
    fingerprint: str
    # Frozen per-device anchor arrival high-water (clock_anchors.seq).
    high_waters: dict[str, int]
    # Frozen global arrival high-water of the events table (events.seq).
    events_high_water: int
    after_corrected: str  # last returned key: corrected_at (UTC ISO-8601)
    after_device: str  # last returned key: device_id
    after_event: str  # last returned key: event_id
    limit: int


def _sign(body: str) -> str:
    return hmac.new(_CURSOR_KEY, body.encode("utf-8"), hashlib.sha256).hexdigest()


def encode_timeline_cursor(cursor: TimelineCursor) -> str:
    body = canonical_json(
        {
            "v": cursor.version,
            "f": cursor.fingerprint,
            "w": cursor.high_waters,
            "s": cursor.events_high_water,
            "t": cursor.after_corrected,
            "d": cursor.after_device,
            "e": cursor.after_event,
            "l": cursor.limit,
        }
    )
    payload = base64.urlsafe_b64encode(body.encode("utf-8")).decode("ascii")
    return CURSOR_PREFIX + payload + "." + _sign(body)[:32]


def decode_timeline_cursor(raw: str) -> TimelineCursor:
    """Decode and verify an opaque timeline cursor; raise ValueError (-> 422)."""
    if not isinstance(raw, str) or not raw.startswith(CURSOR_PREFIX):
        raise ValueError("invalid cursor: unrecognized format")
    packed = raw[len(CURSOR_PREFIX):]
    if packed.count(".") != 1:
        raise ValueError("invalid cursor: malformed token")
    payload, signature = packed.rsplit(".", 1)
    try:
        body = base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("invalid cursor: malformed token") from exc
    if not hmac.compare_digest(signature, _sign(body)[:32]):
        raise ValueError("invalid cursor: signature mismatch")
    try:
        data = json.loads(body)
        version = int(data["v"])
        if version != CURSOR_VERSION:
            raise ValueError(
                "cursor is from an incompatible older version: restart the "
                "timeline without a cursor"
            )
        waters = {str(device): int(hi) for device, hi in data["w"].items()}
        return TimelineCursor(
            version=version,
            fingerprint=str(data["f"]),
            high_waters=waters,
            events_high_water=int(data["s"]),
            after_corrected=str(data["t"]),
            after_device=str(data["d"]),
            after_event=str(data["e"]),
            limit=int(data["l"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid cursor: malformed token") from exc


def timeline_fingerprint(device_ids: tuple[str, ...] | None) -> str:
    """Digest binding a cursor walk to its (normalized) device filter.

    The filter is the only timeline query parameter; page size is embedded
    separately so changing ``limit`` mid-walk stays valid (the cursor's
    embedded limit wins, matching the event search convention).
    """
    document = {"device_ids": sorted(device_ids) if device_ids is not None else None}
    material = canonical_json(document)
    return hashlib.sha256(
        b"device-clock-timeline-query:1\n" + material.encode("utf-8")
    ).hexdigest()

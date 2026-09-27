"""SQLite storage layer."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from . import receipts

# The ``events`` table carries ``seq``: an INTEGER PRIMARY KEY
# AUTOINCREMENT. Unlike the implicit rowid, its value is never reused after
# the current maximum row is deleted (the high-water mark survives in
# ``sqlite_sequence``), which is what the search snapshot cursor relies on.
SCHEMA = """
CREATE TABLE IF NOT EXISTS schemas (
    event_type    TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    body          TEXT NOT NULL,  -- canonicalized JSON Schema document
    supersedes    TEXT,           -- version of the predecessor within this event_type
    created_at    TEXT NOT NULL,  -- UTC ISO-8601
    PRIMARY KEY (event_type, schema_version)
);
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,  -- persistent, never reused
    event_id    TEXT NOT NULL UNIQUE,
    device_id   TEXT NOT NULL,
    occurred_at TEXT NOT NULL,  -- UTC ISO-8601, lexicographically sortable
    event_type    TEXT,
    schema_version TEXT,
    payload       TEXT  -- canonicalized JSON when the event carries a payload
);
CREATE INDEX IF NOT EXISTS idx_events_device_time
    ON events (device_id, occurred_at, event_id);
-- Keyset order used by POST /events/search, including cross-device scans.
CREATE INDEX IF NOT EXISTS idx_events_time_id
    ON events (occurred_at, event_id);
CREATE TABLE IF NOT EXISTS cleanup_plans (
    plan_id     TEXT PRIMARY KEY,
    device_id   TEXT NOT NULL,
    before_at   TEXT NOT NULL,  -- exclusive cutoff, UTC ISO-8601
    protected_windows TEXT NOT NULL,  -- canonical JSON: [[start, end], ...]
    plan_digest TEXT NOT NULL,  -- digest of selection criteria + candidate ids
    status      TEXT NOT NULL,  -- 'pending', 'confirmed' or 'invalidated' (terminal)
    candidates  TEXT NOT NULL,  -- canonical JSON snapshot of selected events
    event_count INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    confirmed_at TEXT
);
-- Persistent per-device downstream command queue. ``seq`` is the same
-- never-reused AUTOINCREMENT sequence used by ``events``: it fixes strict
-- per-device creation order (ORDER BY seq) independently of later deletes.
-- ``command_id`` is unique only together with ``device_id``, and that
-- uniqueness backs both the idempotent replay (same payload -> 200) and
-- the 409 conflict (different payload) decisions. Lease columns are
-- rotated in place on every claim; delivery status is derived from them
-- at read time, so lease expiry needs no background sweeper.
CREATE TABLE IF NOT EXISTS device_commands (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- persistent, never reused
    device_id    TEXT NOT NULL,
    command_id   TEXT NOT NULL,
    payload      TEXT NOT NULL,  -- canonicalized arbitrary JSON
    state        TEXT NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending', 'acked')),  -- 'acked' is terminal
    attempts     INTEGER NOT NULL DEFAULT 0
                 CHECK (attempts >= 0),  -- delivery count; increments per claim
    lease_token  TEXT,             -- random token of the current lease, if any
    leased_at    TEXT,             -- when the current lease began (UTC ISO-8601)
    expires_at   TEXT,             -- when the current lease expires
    not_before   TEXT,             -- retry backoff: closed until this instant
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    acked_at     TEXT,
    UNIQUE (device_id, command_id)
);
CREATE INDEX IF NOT EXISTS idx_device_commands_device_seq
    ON device_commands (device_id, seq);
-- Per-device, append-only, immutable alert rule versions. A rule is
-- addressed by (device_id, rule_id) and versioned with strictly increasing
-- integer versions; only the highest version of each rule is active.
-- Versions partition event *time* into eras: at version creation the
-- device's current event-time frontier (max (occurred_at, event_id)) is
-- pinned as ``era_start_*`` (the version never sees older keys), and when a
-- successor version is created that frontier becomes this version's
-- ``era_end_*``. ``created_seq`` additionally keeps the no-backfill
-- guarantee: events already present at creation can never feed the version,
-- even when a later arrival sorts into its era by event time.
CREATE TABLE IF NOT EXISTS alarm_rules (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id        TEXT NOT NULL,
    rule_id          TEXT NOT NULL,
    version          INTEGER NOT NULL CHECK (version >= 1),
    event_type       TEXT NOT NULL,
    conditions_json  TEXT NOT NULL,  -- canonical JSON: [{pointer, operator, value}]
    window_seconds   REAL NOT NULL CHECK (window_seconds > 0),
    consecutive      INTEGER NOT NULL CHECK (consecutive >= 1),
    cooldown_seconds REAL NOT NULL CHECK (cooldown_seconds >= 0),
    created_seq      INTEGER NOT NULL DEFAULT 0,
    era_start_occurred_at TEXT,  -- exclusive lower event-time frontier; NULL for v1
    era_start_event_id    TEXT,
    era_end_occurred_at   TEXT,  -- inclusive upper frontier once superseded; NULL while latest
    era_end_event_id      TEXT,
    created_at       TEXT NOT NULL,
    UNIQUE (device_id, rule_id, version)
);
-- Fired alarm instances. ``trigger_key`` deterministically binds an alarm
-- to (rule version, triggering event_id), so recomputation over the
-- ordered, era-partitioned event set and event_id replay can never create
-- duplicates (including a late event replayed into a frozen version).
-- ``state``: active (not yet acked; cooldown may already have elapsed),
-- acked (terminal), invalidated (terminal: a recomputation -- e.g. a late
-- event -- removed this firing). acked rows keep acked_seq: the arrival
-- high-water that must be exceeded before the rule may fire again.
CREATE TABLE IF NOT EXISTS alarms (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    alarm_id         TEXT NOT NULL UNIQUE,
    device_id        TEXT NOT NULL,
    rule_id          TEXT NOT NULL,
    rule_version_id  INTEGER NOT NULL REFERENCES alarm_rules(id),
    rule_version     INTEGER NOT NULL,
    trigger_key      TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'active'
                     CHECK (state IN ('active', 'acked', 'invalidated')),
    fired_at         TEXT NOT NULL,  -- occurred_at of the triggering event
    first_match_at   TEXT NOT NULL,  -- occurred_at of the first streak event
    last_match_at    TEXT NOT NULL,
    cooldown_until   TEXT NOT NULL,  -- event-time reopening instant
    trigger_event_id TEXT NOT NULL,
    member_event_ids TEXT NOT NULL,  -- JSON array of streak event ids
    seq_start        INTEGER NOT NULL,
    seq_end          INTEGER NOT NULL,
    acked_at         TEXT,
    acked_seq        INTEGER,
    invalidated_at   TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    UNIQUE (rule_version_id, trigger_key)
);
CREATE INDEX IF NOT EXISTS idx_alarms_device_fired
    ON alarms (device_id, fired_at);
CREATE INDEX IF NOT EXISTS idx_alarms_rule_version
    ON alarms (rule_version_id);
-- Persisted rule-hit progress: the open sliding-window streak (survives
-- restarts even while it has not reached the firing threshold) and the
-- suppression gate after the last evaluated event slice.
CREATE TABLE IF NOT EXISTS alarm_rule_progress (
    rule_version_id   INTEGER PRIMARY KEY REFERENCES alarm_rules(id),
    window_seconds    REAL NOT NULL,
    streak_event_ids  TEXT NOT NULL,  -- JSON array of event ids
    streak_times      TEXT NOT NULL,  -- JSON array of occurred_at strings
    gate_state        TEXT,           -- 'active' | 'acked'
    gate_fired_at     TEXT,
    gate_cooldown_until TEXT,
    gate_acked_seq    INTEGER
);
-- Per-device, append-only, immutable state-machine versions. Each device
-- owns one state machine with strictly increasing integer versions (the
-- first is 1); the whole definition (initial state, state set,
-- transitions) is stored canonically in ``definition_json`` so an
-- identical PUT can be detected as an idempotent replay. ``created_seq``
-- keeps the arrival high-water at registration, matching the alert-rule
-- convention.
CREATE TABLE IF NOT EXISTS state_machine_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    version         INTEGER NOT NULL CHECK (version >= 1),
    initial_state   TEXT NOT NULL,
    states_json     TEXT NOT NULL,  -- JSON array of state names
    definition_json TEXT NOT NULL,  -- canonical JSON of the full definition
    created_seq     INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    UNIQUE (device_id, version)
);
-- The one enabled version of a device plus the projected current state and
-- the arrival high-water: only events with events.seq > last_event_seq are
-- projected, strictly in arrival order. Enabling pins last_event_seq to
-- the device's current MAX(events.seq), so the historical event set is
-- never backfilled.
CREATE TABLE IF NOT EXISTS device_state_machines (
    device_id      TEXT PRIMARY KEY,
    version_id     INTEGER NOT NULL REFERENCES state_machine_versions(id),
    version        INTEGER NOT NULL,
    current_state  TEXT NOT NULL,
    last_event_seq INTEGER NOT NULL,
    enabled_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
-- Append-only projection log: one row per event that fired exactly one
-- transition, in arrival order (``id`` AUTOINCREMENT; ``event_seq`` mirrors
-- events.seq). UNIQUE (device_id, event_id) makes replaying an event_id
-- idempotent and prevents duplicate log rows under any retry.
CREATE TABLE IF NOT EXISTS state_transitions_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id        TEXT NOT NULL,
    version_id       INTEGER NOT NULL REFERENCES state_machine_versions(id),
    version          INTEGER NOT NULL,
    event_seq        INTEGER NOT NULL,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    occurred_at      TEXT NOT NULL,
    from_state       TEXT NOT NULL,
    to_state         TEXT NOT NULL,
    transition_index INTEGER NOT NULL,
    projected_at     TEXT NOT NULL,
    UNIQUE (device_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_state_log_device_id
    ON state_transitions_log (device_id, id);
-- Per-device clock calibration anchors. Each anchor pairs a device clock
-- reading (device_time) with the reference clock at the same observation.
-- ``observed_seq`` is the device's monotonic observation counter: a new
-- anchor must carry a strictly larger value than every accepted anchor, so
-- anchors are validated in observation order independently of HTTP retries.
-- A device_time that does not advance (equal to, or falls back past, the
-- previous anchor) marks a clock reset: ``epoch`` increments and no drift
-- comparison or interpolation line ever spans two epochs. ``reference_time``
-- is true time and must strictly increase across every anchor, including
-- epoch starts. ``drift_ppm`` records the measured rate mismatch against the
-- previous anchor inside the same epoch (NULL for the epoch-opening anchor);
-- anchors above the configured drift limit are rejected with 409 and never
-- inserted. ``seq`` is the same persistent, never-reused AUTOINCREMENT
-- sequence used by ``events``: the cross-device timeline freezes per-device
-- MAX(seq) into its first cursor so anchors written afterwards can never
-- remap (and thereby reorder) rows of an ongoing page walk.
CREATE TABLE IF NOT EXISTS clock_anchors (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,  -- persistent, never reused
    device_id      TEXT NOT NULL,
    anchor_id      TEXT NOT NULL,
    observed_seq   INTEGER NOT NULL CHECK (observed_seq >= 0),
    device_time    TEXT NOT NULL,  -- device clock reading, normalized UTC ISO-8601
    reference_time TEXT NOT NULL,  -- reference clock reading, normalized UTC ISO-8601
    epoch          INTEGER NOT NULL CHECK (epoch >= 1),
    drift_ppm      REAL,           -- measured ppm vs previous in-epoch anchor; NULL at epoch start
    created_at     TEXT NOT NULL,
    UNIQUE (device_id, anchor_id)
);
-- Observation-order scan used by anchor validation and calibration builds.
CREATE INDEX IF NOT EXISTS idx_clock_anchors_device_obs
    ON clock_anchors (device_id, observed_seq);
-- Per-device frozen-high-water lookup (MAX(seq)) for timeline cursors.
CREATE INDEX IF NOT EXISTS idx_clock_anchors_device_seq
    ON clock_anchors (device_id, seq);
-- Collection integrity: registered per-device source streams. A stream is
-- the device's own ordered ingestion channel; registration is idempotent
-- and carries no mutable definition (the identity is the definition).
CREATE TABLE IF NOT EXISTS collection_streams (
    device_id  TEXT NOT NULL,
    stream_id  TEXT NOT NULL,
    created_at TEXT NOT NULL,  -- UTC ISO-8601
    PRIMARY KEY (device_id, stream_id)
);
-- The sequence ledger: one row per accepted source slot. source_seq is the
-- device-assigned sequence (numbered from 1) and may arrive out of order;
-- it is NOT the arrival seq. Both uniqueness rules are enforced here:
-- * PRIMARY KEY (device_id, stream_id, source_seq): a slot binds exactly
--   one event_id,
-- * UNIQUE (device_id, stream_id, event_id): within a stream an event_id
--   occupies exactly one slot (the same event_id may still be ingested on
--   another stream, since the key includes stream_id).
-- The ledger deliberately has no REFERENCES events(event_id): retention
-- cleanup may delete event rows, while the ledger (and every checkpoint
-- computed from it) must survive.
CREATE TABLE IF NOT EXISTS collection_stream_events (
    device_id  TEXT NOT NULL,
    stream_id  TEXT NOT NULL,
    source_seq INTEGER NOT NULL CHECK (source_seq >= 1),
    event_id   TEXT NOT NULL,
    created_at TEXT NOT NULL,  -- acceptance (arrival) time, UTC ISO-8601
    PRIMARY KEY (device_id, stream_id, source_seq),
    UNIQUE (device_id, stream_id, event_id),
    FOREIGN KEY (device_id, stream_id)
        REFERENCES collection_streams (device_id, stream_id)
);
-- Saved integrity checkpoints: one per gap-free prefix 1..through_seq. The
-- SHA-256 is computed over the ordered (source_seq, event_id) pairs of the
-- ledger only, so it remains verifiable after the referenced events have
-- been deleted by retention cleanup. A repeated PUT replays the stored row
-- (the (device_id, stream_id, through_seq) primary key makes it idempotent).
CREATE TABLE IF NOT EXISTS collection_stream_checkpoints (
    device_id       TEXT NOT NULL,
    stream_id       TEXT NOT NULL,
    through_seq     INTEGER NOT NULL CHECK (through_seq >= 1),
    checksum_sha256 TEXT NOT NULL,
    event_count     INTEGER NOT NULL CHECK (event_count >= 1),
    created_at      TEXT NOT NULL,
    PRIMARY KEY (device_id, stream_id, through_seq),
    FOREIGN KEY (device_id, stream_id)
        REFERENCES collection_streams (device_id, stream_id)
);
-- Session evolution snapshots. A snapshot freezes one device's sessions at
-- one arrival high-water: ``event_high_water`` is the monotonic
-- ``events.seq`` mark captured inside the creating write transaction, and
-- the sessions are derived only from rows with seq <= that mark. Each
-- snapshot additionally pins the ``gap_seconds`` configuration used to
-- build it: two snapshots built with different gaps are never compared.
-- ``snapshot_id`` is client-chosen and GLOBALLY unique (cross-device reuse
-- is a 409, same-device re-POST replays the original snapshot), so it is
-- the primary key without device_id.
CREATE TABLE IF NOT EXISTS session_snapshots (
    snapshot_id       TEXT PRIMARY KEY,
    device_id         TEXT NOT NULL,
    event_high_water  INTEGER NOT NULL CHECK (event_high_water >= 0),
    gap_seconds       REAL NOT NULL CHECK (gap_seconds >= 0),
    session_count     INTEGER NOT NULL CHECK (session_count >= 0),
    created_at        TEXT NOT NULL  -- UTC ISO-8601
);
-- Per-device, chronologically ordered snapshot listing.
CREATE INDEX IF NOT EXISTS idx_session_snapshots_device
    ON session_snapshots (device_id, created_at, snapshot_id);
-- The frozen, ordered sessions of one snapshot. Members are COPIED in full
-- (canonical JSON: the session_id plus every member's event_id,
-- occurred_at, event_type, schema_version and payload), so a late event or
-- a retention cleanup can never rewrite an old snapshot and snapshots stay
-- readable after the referenced event rows have been deleted. The table
-- deliberately has no REFERENCES events(event_id). ``session_index`` keeps
-- the snapshot-time (started_at, event_id) session order stable.
CREATE TABLE IF NOT EXISTS session_snapshot_sessions (
    snapshot_id   TEXT NOT NULL,
    session_index INTEGER NOT NULL CHECK (session_index >= 0),
    session_id    TEXT NOT NULL,
    started_at    TEXT NOT NULL,  -- UTC ISO-8601
    ended_at      TEXT NOT NULL,
    members_json  TEXT NOT NULL,  -- canonical JSON copy of the ordered members
    PRIMARY KEY (snapshot_id, session_index),
    FOREIGN KEY (snapshot_id) REFERENCES session_snapshots (snapshot_id)
);
CREATE INDEX IF NOT EXISTS idx_session_snapshot_sessions_id
    ON session_snapshot_sessions (snapshot_id, session_id);
-- Event transparency receipts: an append-only RFC 6962 Merkle tree over
-- every event ever accepted, in arrival order. ``leaf_index`` is the
-- 0-based Merkle position (contiguous, assigned under the single writer
-- lock); ``event_seq`` mirrors the event's persistent ``events.seq`` at
-- insert time, so leaf order is exactly arrival order. The leaf copies
-- every field its hash commits to (seq, device_id, event_id, occurred_at,
-- event_type, schema_version, payload) and deliberately has no REFERENCES
-- events(event_id): retention cleanup deletes only event bodies, while
-- the receipt leaves -- and therefore every membership proof and
-- checkpoint -- remain verifiable afterwards.
CREATE TABLE IF NOT EXISTS receipt_leaves (
    leaf_index  INTEGER PRIMARY KEY CHECK (leaf_index >= 0),
    event_seq   INTEGER NOT NULL UNIQUE,  -- events.seq at insert time
    event_id    TEXT NOT NULL UNIQUE,
    device_id   TEXT NOT NULL,
    occurred_at TEXT NOT NULL,  -- UTC ISO-8601, same storage form as events
    event_type  TEXT,
    schema_version TEXT,
    payload     TEXT,           -- canonicalized JSON, NULL for payload-less events
    leaf_hash   TEXT NOT NULL,  -- RFC 6962 leaf hash, SHA-256 hex
    created_at  TEXT NOT NULL
);
-- Immutable transparency checkpoints: one (tree_size, root_hash) pair per
-- tree size, computed once and replayed verbatim on repeat creation. The
-- tree is append-only, so a size's root never changes; old checkpoints
-- stay verifiable against the retained leaves forever.
CREATE TABLE IF NOT EXISTS receipt_checkpoints (
    tree_size  INTEGER PRIMARY KEY CHECK (tree_size >= 1),
    root_hash  TEXT NOT NULL,  -- Merkle root over leaves 0..tree_size-1, hex
    created_at TEXT NOT NULL
);
"""

# Columns added after the initial release; applied idempotently to existing DBs.
EVENT_ADDED_COLUMNS = {
    "event_type": "TEXT",
    "schema_version": "TEXT",
    "payload": "TEXT",
}

# Columns added to ``alarm_rules`` when event-time eras replaced the
# seq-only version slicing. Applied idempotently to existing databases.
ALARM_RULE_ADDED_COLUMNS = {
    "era_start_occurred_at": "TEXT",
    "era_start_event_id": "TEXT",
    "era_end_occurred_at": "TEXT",
    "era_end_event_id": "TEXT",
}

# Backfill of the era frontiers for databases created before event-time
# eras. At every successor creation the predecessor's closed era ends at
# the device event-time frontier visible at that moment; the successor's
# open era starts at the same frontier. ``created_seq`` (the arrival
# high-water at creation) pins exactly which rows were visible then, so the
# frontier can be reconstructed deterministically from the stored events.
_ALARM_ERA_BACKFILL = """
UPDATE alarm_rules AS r
SET era_end_occurred_at = (
        SELECT e.occurred_at FROM events AS e
        WHERE e.device_id = r.device_id AND e.seq <=
              (SELECT s.created_seq FROM alarm_rules AS s
               WHERE s.device_id = r.device_id AND s.rule_id = r.rule_id
                 AND s.version = r.version + 1)
        ORDER BY e.occurred_at DESC, e.event_id DESC LIMIT 1
    ),
    era_end_event_id = (
        SELECT e.event_id FROM events AS e
        WHERE e.device_id = r.device_id AND e.seq <=
              (SELECT s.created_seq FROM alarm_rules AS s
               WHERE s.device_id = r.device_id AND s.rule_id = r.rule_id
                 AND s.version = r.version + 1)
        ORDER BY e.occurred_at DESC, e.event_id DESC LIMIT 1
    )
WHERE EXISTS (
    SELECT 1 FROM alarm_rules AS s
    WHERE s.device_id = r.device_id AND s.rule_id = r.rule_id
      AND s.version = r.version + 1
);
UPDATE alarm_rules AS r
SET era_start_occurred_at = (
        SELECT e.occurred_at FROM events AS e
        WHERE e.device_id = r.device_id AND e.seq <= r.created_seq
        ORDER BY e.occurred_at DESC, e.event_id DESC LIMIT 1
    ),
    era_start_event_id = (
        SELECT e.event_id FROM events AS e
        WHERE e.device_id = r.device_id AND e.seq <= r.created_seq
        ORDER BY e.occurred_at DESC, e.event_id DESC LIMIT 1
    )
WHERE r.version > 1;
"""

# Shape of the rebuilt events table during the seq-column migration. Kept in
# sync with the canonical DDL in SCHEMA: ``event_id`` keeps its NOT NULL
# UNIQUE constraint (semantically identical to the original PRIMARY KEY),
# and every later-added column is carried over.
_EVENTS_MIGRATION_DDL = (
    "CREATE TABLE events_new (\n"
    "    seq INTEGER PRIMARY KEY AUTOINCREMENT,\n"
    "    event_id TEXT NOT NULL UNIQUE,\n"
    "    device_id TEXT NOT NULL,\n"
    "    occurred_at TEXT NOT NULL,\n"
    "    event_type TEXT,\n"
    "    schema_version TEXT,\n"
    "    payload TEXT\n"
    ")"
)
_EVENTS_MIGRATION_COLUMNS = (
    "event_id, device_id, occurred_at, event_type, schema_version, payload"
)


def connect(db_path: str) -> sqlite3.Connection:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    # FastAPI executes sync dependencies and endpoints on an anyio thread
    # pool: a request's connection is created in one pooled worker thread
    # (the dependency generator's __enter__) and may be used/closed on
    # another (the endpoint body, then __exit__). SQLite's default
    # thread-affinity check therefore raises ProgrammingError under
    # concurrency. ``check_same_thread=False`` relaxes that check; this is
    # safe because every connection is private to exactly one request and
    # is never shared between threads simultaneously.
    conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL permits concurrent readers while one writer holds the write lock.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # Honor the connect timeout: have blocked writers wait (and retry on
    # SQLITE_BUSY) for up to ``busy_timeout`` ms instead of failing fast.
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _migrate_events_to_seq(conn: sqlite3.Connection) -> None:
    """Rebuild the legacy ``events`` table with the persistent ``seq`` column.

    Databases created before this release store ``events`` without ``seq``;
    the search snapshot used the implicit rowid, which SQLite is free to
    reuse once the maximum rowid row is deleted. Any later-added column is
    attached to the legacy table first, then the table is rebuilt inside a
    single write transaction: historical rows receive ``seq`` equal to
    their former rowid (insertion order), both indexes are recreated, and
    AUTOINCREMENT takes over afterwards -- SQLite seeds the counter in
    ``sqlite_sequence`` from the explicitly inserted values, so no value is
    ever reused. The caller guarantees the ``events`` table lacks ``seq``.
    """
    for column, decl in EVENT_ADDED_COLUMNS.items():
        present = {
            row["name"] for row in conn.execute("PRAGMA table_info(events)")
        }
        if column not in present:
            conn.execute(f"ALTER TABLE events ADD COLUMN {column} {decl}")
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(_EVENTS_MIGRATION_DDL)
        conn.execute(
            "INSERT INTO events_new (seq, "
            + _EVENTS_MIGRATION_COLUMNS
            + ") SELECT rowid, "
            + _EVENTS_MIGRATION_COLUMNS
            + " FROM events ORDER BY rowid"
        )
        conn.execute("DROP TABLE events")
        conn.execute("ALTER TABLE events_new RENAME TO events")
        # Recreate the indexes dropped with the old table.
        conn.execute(
            "CREATE INDEX idx_events_device_time "
            "ON events (device_id, occurred_at, event_id)"
        )
        conn.execute(
            "CREATE INDEX idx_events_time_id ON events (occurred_at, event_id)"
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        existing = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if "seq" not in existing:
            # Legacy database: rebuild the table so the never-reused
            # AUTOINCREMENT sequence backs the search high-water mark.
            _migrate_events_to_seq(conn)
        else:
            for column, decl in EVENT_ADDED_COLUMNS.items():
                if column not in existing:
                    conn.execute(f"ALTER TABLE events ADD COLUMN {column} {decl}")
        _migrate_alarm_rule_eras(conn)
        # One-shot, idempotent backfill of the transparency receipt tree:
        # events stored before this module existed receive their Merkle
        # leaves in events.seq order. The result depends only on the
        # stored events, so migrating identical legacy databases yields
        # identical trees; on databases that are already current this is
        # a no-op.
        receipts.backfill_leaves(conn)


def _migrate_alarm_rule_eras(conn: sqlite3.Connection) -> None:
    """Add event-time era columns to a pre-eras ``alarm_rules`` table.

    Fresh databases already have every column (``CREATE TABLE IF NOT
    EXISTS``), in which case this is a no-op. For an older database the
    four columns are added in place and the era frontiers of every version
    are reconstructed from ``created_seq`` and the surviving event rows in
    one idempotent transaction.
    """
    columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(alarm_rules)")
    }
    missing = [
        (column, decl)
        for column, decl in ALARM_RULE_ADDED_COLUMNS.items()
        if column not in columns
    ]
    if not missing:
        return
    # A single script: ``executescript`` commits any pending transaction
    # first and commits the whole batch atomically at the end (SQLite DDL
    # is transactional), so the columns and the reconstructed frontiers
    # appear together.
    alters = "".join(
        f"ALTER TABLE alarm_rules ADD COLUMN {column} {decl};"
        for column, decl in missing
    )
    conn.executescript(alters + _ALARM_ERA_BACKFILL)

"""Versioned alert rules: rule lifecycle, event-time window/cooldown
evaluation, out-of-order (late event) recomputation, idempotent replay,
acknowledgement preconditions and single-writer concurrency.
"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

GAP = 300  # seconds

SCHEMA_BODY = {"schema": {"type": "object",
                          "properties": {"v": {"type": "number"}}}}


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "alerts.db"), gap_seconds=GAP)
    with TestClient(app) as c:
        c.put("/schemas/temp/1.0", json=SCHEMA_BODY)
        yield c


def rule(client, device, rule_id, version, *, event_type="temp",
         conditions=None, window=600, consecutive=3, cooldown=1000):
    return client.put(
        f"/devices/{device}/alert-rules/{rule_id}/versions/{version}",
        json={
            "version": version,
            "event_type": event_type,
            "conditions": conditions if conditions is not None else [
                {"pointer": "/v", "operator": "gte", "value": 40}
            ],
            "window_seconds": window,
            "consecutive": consecutive,
            "cooldown_seconds": cooldown,
        },
    )


def event(client, device, eid, ts, v=50, *, event_type="temp", version="1.0"):
    return client.post(
        "/events",
        json={
            "device_id": device,
            "event_id": eid,
            "occurred_at": ts,
            "event_type": event_type,
            "schema_version": version,
            "payload": {"v": v},
        },
    )


def alarms(client, device, **params):
    return client.get(f"/devices/{device}/alarms", params=params).json()


# --------------------------------------------------------------- rule CRUD


def test_create_rule_version_and_get(client):
    resp = rule(client, "dev-1", "hot", 1)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["version"] == 1
    assert body["active"] is True
    assert body["created_seq"] == 0
    assert body["conditions"][0]["pointer"] == "/v"

    got = client.get("/devices/dev-1/alert-rules/hot/versions/1")
    assert got.status_code == 200
    assert got.json() == body


def test_first_version_must_be_one(client):
    resp = rule(client, "dev-1", "hot", 2)
    assert resp.status_code == 409


def test_versions_are_append_only_and_immutable(client):
    assert rule(client, "dev-1", "hot", 1).status_code == 201
    # Cannot re-register the same version (even with a changed document).
    resp = rule(client, "dev-1", "hot", 1, consecutive=5)
    assert resp.status_code == 409
    # Cannot skip versions.
    assert rule(client, "dev-1", "hot", 3).status_code == 409
    # The exact next version succeeds and becomes active.
    assert rule(client, "dev-1", "hot", 2, consecutive=5).status_code == 201
    versions = client.get("/devices/dev-1/alert-rules").json()
    assert [(v["version"], v["active"]) for v in versions] == [(1, False), (2, True)]
    assert client.get("/devices/dev-1/alert-rules/hot/versions/9").status_code == 404


def test_path_and_body_version_must_match(client):
    resp = client.put(
        "/devices/dev-1/alert-rules/hot/versions/2",
        json={"version": 1, "event_type": "temp", "conditions": [],
              "window_seconds": 60, "consecutive": 1, "cooldown_seconds": 0},
    )
    assert resp.status_code == 422


def test_rule_validation(client):
    # window must be positive
    resp = rule(client, "dev-1", "hot", 1, window=0)
    assert resp.status_code == 422
    # consecutive at least one
    resp = rule(client, "dev-1", "hot", 1, consecutive=0)
    assert resp.status_code == 422
    # cooldown may be zero but not negative
    resp = rule(client, "dev-1", "hot", 1, cooldown=-1)
    assert resp.status_code == 422
    # malformed pointer
    resp = rule(client, "dev-1", "hot", 1,
                conditions=[{"pointer": "oops", "operator": "exists",
                             "value": True}])
    assert resp.status_code == 422
    # gte requires a numeric value
    resp = rule(client, "dev-1", "hot", 1,
                conditions=[{"pointer": "/v", "operator": "gte", "value": "x"}])
    assert resp.status_code == 422


def test_rule_is_scoped_per_device(client):
    assert rule(client, "dev-1", "hot", 1).status_code == 201
    # Same rule_id on another device starts again at version 1.
    assert rule(client, "dev-2", "hot", 1).status_code == 201


# --------------------------------------------------------------- firing


def test_fires_after_consecutive_hits_in_window(client):
    rule(client, "dev-1", "hot", 1)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    fired = alarms(client, "dev-1")
    assert len(fired) == 1
    alarm = fired[0]
    assert alarm["trigger_event_id"] == "e3"
    assert alarm["member_event_ids"] == ["e1", "e2", "e3"]
    assert alarm["fired_at"].endswith("10:04:00Z")
    assert alarm["first_match_at"].endswith("10:00:00Z")
    assert alarm["last_match_at"] == alarm["fired_at"]
    assert alarm["rule_version"] == 1
    assert alarm["cooldown_until"].endswith("10:20:40Z")


def test_out_of_order_arrival_fires_in_logical_order(client):
    rule(client, "dev-1", "hot", 1)
    # Arrive reversed: e3, e2, e1.
    event(client, "dev-1", "e3", "2026-09-20T10:03:00Z")
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e1", "2026-09-20T10:01:00Z")
    fired = alarms(client, "dev-1")
    assert len(fired) == 1
    assert fired[0]["trigger_event_id"] == "e3"
    assert fired[0]["member_event_ids"] == ["e1", "e2", "e3"]


def test_non_matching_event_resets_consecutive_streak(client):
    rule(client, "dev-1", "hot", 1)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    # A cold event after the first hot hit resets the run.
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z", v=1)
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    event(client, "dev-1", "e4", "2026-09-20T10:06:00Z")
    # Only two consecutive hot hits (e3, e4): nothing yet.
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e5", "2026-09-20T10:08:00Z")
    fired = alarms(client, "dev-1")
    assert [a["trigger_event_id"] for a in fired] == ["e5"]
    assert fired[0]["member_event_ids"] == ["e3", "e4", "e5"]


def test_streak_must_fit_in_sliding_window(client):
    # 2 hits within 300s, cooldown 0.
    rule(client, "dev-1", "hot", 1, window=300, consecutive=2, cooldown=0)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    # 301s later: e1 falls out of the window ending at e2.
    event(client, "dev-1", "e2", "2026-09-20T10:05:01Z")
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e3", "2026-09-20T10:06:00Z")
    # e2..e3 are 59s apart -> fit.
    fired = alarms(client, "dev-1")
    assert [a["trigger_event_id"] for a in fired] == ["e3"]


def test_payload_and_event_type_matching(client):
    # No payload conditions: any payload event of the type matches.
    client.put("/schemas/ping/1.0", json=SCHEMA_BODY)
    rule(client, "dev-1", "any", 1, event_type="ping",
         conditions=[], window=600, consecutive=1, cooldown=0)
    # Legacy no-payload event never matches, even with the same device.
    client.post("/events", json={"device_id": "dev-1", "event_id": "p0",
                                 "occurred_at": "2026-09-20T10:00:00Z"})
    # A different event_type does not match.
    event(client, "dev-1", "t1", "2026-09-20T10:01:00Z",
          event_type="temp")
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "g1", "2026-09-20T10:02:00Z",
          event_type="ping")
    fired = alarms(client, "dev-1")
    assert [a["rule_id"] for a in fired] == ["any"]
    assert fired[0]["trigger_event_id"] == "g1"


def test_eq_condition_matches_scalar(client):
    client.put("/schemas/code/1.0", json={
        "schema": {"type": "object",
                   "properties": {"code": {"type": "string"}}}})
    rule(client, "dev-1", "code", 1,
         event_type="code",
         conditions=[{"pointer": "/code", "operator": "eq", "value": "E5"}],
         window=600, consecutive=1, cooldown=0)

    def coded(eid, minute, code):
        return client.post("/events", json={
            "device_id": "dev-1", "event_id": eid,
            "occurred_at": f"2026-09-20T10:{minute:02d}:00Z",
            "event_type": "code", "schema_version": "1.0",
            "payload": {"code": code}})

    coded("c1", 0, "E4")
    assert alarms(client, "dev-1") == []
    coded("c2", 1, "E5")
    fired = alarms(client, "dev-1")
    assert [a["rule_id"] for a in fired] == ["code"]
    assert fired[0]["trigger_event_id"] == "c2"


# ------------------------------------------------- cooldown & at-most-one


def test_cooldown_keeps_a_single_open_alarm(client):
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=600)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:05:00Z")  # inside cooldown
    event(client, "dev-1", "e3", "2026-09-20T10:09:00Z")  # still inside
    assert [a["trigger_event_id"] for a in alarms(client, "dev-1")] == ["e1"]
    # 10:00 + 10min cooldown; an event at exactly 10:10:00 reopens.
    event(client, "dev-1", "e4", "2026-09-20T10:10:00Z")
    assert [a["trigger_event_id"] for a in alarms(client, "dev-1")] == \
        ["e4", "e1"]


def test_replayed_event_id_never_double_fires(client):
    rule(client, "dev-1", "hot", 1)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    assert len(alarms(client, "dev-1")) == 1
    # Identical replays of every contributing event, in any order.
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    assert len(alarms(client, "dev-1")) == 1


# --------------------------------------------------- late events invalidate


def test_late_non_matching_event_invalidates_open_alarm(client):
    rule(client, "dev-1", "hot", 1)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    alarm = alarms(client, "dev-1")[0]
    assert alarm["status"] in {"active", "expired"}

    # A cold event arriving late, sorting between e1 and e2, breaks the run.
    event(client, "dev-1", "late", "2026-09-20T10:01:30Z", v=0)
    refreshed = alarms(client, "dev-1")[0]
    assert refreshed["alarm_id"] == alarm["alarm_id"]
    assert refreshed["status"] == "invalidated"
    assert refreshed["invalidated_at"] is not None

    # Only two consecutive hot events remain (e2, e3): no new firing.
    event(client, "dev-1", "e4", "2026-09-20T10:06:00Z")
    live = alarms(client, "dev-1", status="invalidated")
    assert len(live) == 1


def test_late_event_can_move_the_trigger_but_not_duplicate(client):
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=0)
    event(client, "dev-1", "a", "2026-09-20T10:00:00Z")
    first_id = alarms(client, "dev-1")[0]["alarm_id"]
    # Late arrival sorting earlier than "a" is the true first match in
    # event time; consecutive=1 fires per matching event with no cooldown.
    event(client, "dev-1", "late", "2026-09-20T09:00:00Z")
    all_alarms = alarms(client, "dev-1")
    assert {a["alarm_id"] for a in all_alarms} >= {first_id}
    assert len(all_alarms) == 2  # "late" and "a" are distinct trigger events


# --------------------------------------------------------------- versions


def test_late_event_replays_frozen_version_era(client):
    """The headline fix: a late event sorts into a superseded version's era.

    v1 (3 consecutive hot hits) exists when e1/e2 arrive; v2 is then
    created (single v>=99 hit). A later-arriving event with an old
    occurred_at (09:59, inside v1's closed era) must be replayed against
    v1 and close its streak, instead of being swallowed by the active v2.
    """
    rule(client, "dev-1", "hot", 1, consecutive=3, cooldown=0)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z", v=50)
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z", v=50)
    assert alarms(client, "dev-1") == []  # v1 streak of 2, not yet firing
    # v2 requires v>=99 and a single hit.
    rule(client, "dev-1", "hot", 2,
         conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
         window=60, consecutive=1, cooldown=0)

    # The late event arrives *after* v2 creation but sorts (by event time)
    # before both existing events: it belongs to v1's closed era.
    event(client, "dev-1", "old", "2026-09-20T09:59:00Z", v=50)
    by_version = {(a["rule_version"], a["trigger_event_id"])
                  for a in alarms(client, "dev-1")}
    assert by_version == {(1, "e2")}
    v1_alarm = next(a for a in alarms(client, "dev-1")
                    if a["rule_version"] == 1)
    # Members are the streak in event-time order.
    assert v1_alarm["member_event_ids"] == ["old", "e1", "e2"]

    # A new arrival in v2's era matches v2 only (v>=99); v50 does not.
    event(client, "dev-1", "newcold", "2026-09-20T11:00:00Z", v=50)
    by_version = {(a["rule_version"], a["trigger_event_id"])
                  for a in alarms(client, "dev-1")}
    assert by_version == {(1, "e2")}
    event(client, "dev-1", "n1", "2026-09-20T11:01:00Z", v=100)
    by_version = {(a["rule_version"], a["trigger_event_id"])
                  for a in alarms(client, "dev-1")}
    assert by_version == {(1, "e2"), (2, "n1")}


def test_version_partition_independent_of_arrival_order(client):
    """Same final ordered event set + version timeline => same alarms.

    Compare two interleavings on separate devices:

    * world A: the era-filling third hit is already present when v2 is
      created, so v1 fires before the version switch;
    * world B: that same event only arrives (late) after v2 was created.
    """
    def world(device, late):
        p = device[-1]  # event_id is globally unique: prefix per device
        rule(client, device, "hot", 1, consecutive=3, cooldown=0)
        event(client, device, f"{p}e1", "2026-09-20T10:00:00Z", v=50)
        event(client, device, f"{p}e2", "2026-09-20T10:02:00Z", v=50)
        if not late:
            event(client, device, f"{p}e0", "2026-09-20T09:59:00Z", v=50)
        rule(client, device, "hot", 2,
             conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
             window=60, consecutive=1, cooldown=0)
        if late:
            event(client, device, f"{p}e0", "2026-09-20T09:59:00Z", v=50)
        event(client, device, f"{p}n1", "2026-09-20T11:00:00Z", v=100)
        # Normalize away the device-specific id prefix so the two worlds
        # can be compared structurally (version, trigger and streak shape).
        return {
            (a["rule_version"], a["trigger_event_id"].lstrip("ab"),
             tuple(mid.lstrip("ab") for mid in a["member_event_ids"]))
            for a in alarms(client, device)
        }

    world_a = world("dev-a", late=False)
    world_b = world("dev-b", late=True)
    assert world_a == world_b
    assert world_a == {
        (1, "e2", ("e0", "e1", "e2")),
        (2, "n1", ("n1",)),
    }


def test_late_event_in_frozen_era_can_invalidate(client):
    """A late non-matching event in a closed era invalidates v1's alarm."""
    rule(client, "dev-1", "hot", 1, consecutive=3, cooldown=0)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z", v=50)
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z", v=50)
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z", v=50)
    assert [a["trigger_event_id"] for a in alarms(client, "dev-1")] == ["e3"]
    rule(client, "dev-1", "hot", 2,
         conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
         window=60, consecutive=1, cooldown=0)
    # Late cold event sorts inside the frozen v1 run and breaks it.
    event(client, "dev-1", "break", "2026-09-20T10:01:00Z", v=0)
    statuses = {(a["rule_version"], a["trigger_event_id"]): a["status"]
                for a in alarms(client, "dev-1")}
    assert statuses[(1, "e3")] == "invalidated"


def test_frozen_version_acked_alarm_survives_late_replay(client):
    """Acknowledged history of a superseded version is never rewritten."""
    rule(client, "dev-1", "hot", 1, consecutive=3, cooldown=0)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z", v=50)
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z", v=50)
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z", v=50)
    alarm = alarms(client, "dev-1")[0]
    client.post(f"/devices/dev-1/alarms/{alarm['alarm_id']}/ack",
                json={"expected_status": alarm["status"],
                      "expected_rule_version": 1})
    rule(client, "dev-1", "hot", 2,
         conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
         window=60, consecutive=1, cooldown=0)
    # Even a structurally destroying late event keeps the acked v1 alarm.
    event(client, "dev-1", "break", "2026-09-20T10:01:00Z", v=0)
    statuses = {(a["rule_version"], a["trigger_event_id"]): a["status"]
                for a in alarms(client, "dev-1")}
    assert statuses[(1, "e3")] == "acked"


def test_late_event_on_era_boundary_tie_breaks_by_event_id(client):
    """Boundary keys use the same total order as (occurred_at, event_id)."""
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=0)
    # Frontier at v2 creation is (10:00, "f1").
    event(client, "dev-1", "f1", "2026-09-20T10:00:00Z", v=50)
    rule(client, "dev-1", "hot", 2,
         conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
         window=60, consecutive=1, cooldown=0)
    # Same occurred_at, smaller event_id: strictly below the frontier -> v1.
    event(client, "dev-1", "f0", "2026-09-20T10:00:00Z", v=50)
    # Same occurred_at, larger event_id: strictly above the frontier -> v2;
    # v=50 fails the v2 condition so nothing fires for it.
    event(client, "dev-1", "f2", "2026-09-20T10:00:00Z", v=50)
    by_version = {(a["rule_version"], a["trigger_event_id"])
                  for a in alarms(client, "dev-1")}
    assert by_version == {(1, "f1"), (1, "f0")}


def test_empty_eras_when_versions_precede_events(client):
    """Versions created before any event: only the latest open era fires."""
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=0)
    # v2 immediately, with no events in between: v1's era is empty forever.
    rule(client, "dev-1", "hot", 2,
         conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
         window=60, consecutive=1, cooldown=0)
    # Even an old-timestamped event arriving now cannot fill v1's era.
    event(client, "dev-1", "old", "2026-09-20T09:59:00Z", v=50)
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "hot1", "2026-09-20T10:00:00Z", v=100)
    assert {(a["rule_version"], a["trigger_event_id"])
            for a in alarms(client, "dev-1")} == {(2, "hot1")}


def test_replayed_late_event_is_idempotent(client):
    rule(client, "dev-1", "hot", 1, consecutive=3, cooldown=0)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z", v=50)
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z", v=50)
    rule(client, "dev-1", "hot", 2,
         conditions=[{"pointer": "/v", "operator": "gte", "value": 99}],
         window=60, consecutive=1, cooldown=0)
    event(client, "dev-1", "old", "2026-09-20T09:59:00Z", v=50)
    assert len(alarms(client, "dev-1")) == 1
    # Replays of every involved event (including the late one and v2-era
    # events) never duplicate alarms across versions.
    event(client, "dev-1", "old", "2026-09-20T09:59:00Z", v=50)
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z", v=50)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z", v=50)
    assert len(alarms(client, "dev-1")) == 1


def test_three_versions_late_events_hit_each_era(client):
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=0)
    event(client, "dev-1", "a1", "2026-09-20T10:00:00Z", v=50)
    rule(client, "dev-1", "hot", 2, consecutive=1, cooldown=0)
    event(client, "dev-1", "b1", "2026-09-20T11:00:00Z", v=50)
    rule(client, "dev-1", "hot", 3, consecutive=1, cooldown=0)
    # Late arrivals after v3: each sorts into its proper era.
    event(client, "dev-1", "a0", "2026-09-20T09:00:00Z", v=50)  # v1 era
    event(client, "dev-1", "b0", "2026-09-20T10:30:00Z", v=50)  # v2 era
    event(client, "dev-1", "c1", "2026-09-20T12:00:00Z", v=50)  # v3 era
    by_version = {(a["rule_version"], a["trigger_event_id"])
                  for a in alarms(client, "dev-1")}
    assert by_version == {
        (1, "a1"), (1, "a0"),
        (2, "b1"), (2, "b0"),
        (3, "c1"),
    }


def test_rule_created_after_events_does_not_backfill(client):
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    # Rule created afterwards: the two earlier arrivals must not be counted.
    rule(client, "dev-1", "hot", 1, consecutive=2)
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    # Still only one post-creation hit.
    assert alarms(client, "dev-1") == []
    event(client, "dev-1", "e4", "2026-09-20T10:06:00Z")
    assert [a["trigger_event_id"] for a in alarms(client, "dev-1")] == ["e4"]


# ----------------------------------------------------------- acknowledge


def test_ack_requires_matching_status_and_version(client, tmp_path):
    rule(client, "dev-1", "hot", 1)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    alarm = alarms(client, "dev-1")[0]
    aid = alarm["alarm_id"]

    # Wrong rule version.
    resp = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                       json={"expected_status": alarm["status"],
                             "expected_rule_version": 2})
    assert resp.status_code == 409
    # Wrong expected status.
    other = "active" if alarm["status"] == "expired" else "expired"
    resp = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                       json={"expected_status": other,
                             "expected_rule_version": 1})
    assert resp.status_code == 409
    # Bad status value -> 422.
    resp = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                       json={"expected_status": "bogus",
                             "expected_rule_version": 1})
    assert resp.status_code == 422
    # Matching precondition succeeds.
    resp = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                       json={"expected_status": alarm["status"],
                             "expected_rule_version": 1})
    assert resp.status_code == 200
    assert resp.json()["status"] == "acked"
    assert resp.json()["acked_at"] is not None


def test_ack_is_idempotent_and_pins_the_high_water(client):
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=3600)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    alarm = alarms(client, "dev-1")[0]
    aid = alarm["alarm_id"]
    first = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                        json={"expected_status": alarm["status"],
                              "expected_rule_version": 1})
    assert first.status_code == 200
    acked_at = first.json()["acked_at"]

    # Replay with expected_status=acked returns the identical terminal row.
    replay = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                         json={"expected_status": "acked",
                               "expected_rule_version": 1})
    assert replay.status_code == 200
    assert replay.json()["acked_at"] == acked_at

    # A later arrival (after the ack) is allowed to fire even though its
    # event time is still deep inside the 1h cooldown: acknowledgement
    # reopens the rule for arrivals beyond the pinned high-water.
    event(client, "dev-1", "e2", "2026-09-20T10:01:00Z")
    fired = [a for a in alarms(client, "dev-1") if a["status"] != "acked"]
    assert [a["trigger_event_id"] for a in fired] == ["e2"]


def test_events_present_at_ack_cannot_retrigger(client):
    rule(client, "dev-1", "hot", 1, consecutive=1, cooldown=3600)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    alarm = alarms(client, "dev-1")[0]
    aid = alarm["alarm_id"]
    # A second event inside the cooldown is suppressed (no second alarm).
    event(client, "dev-1", "e2", "2026-09-20T10:05:00Z")
    assert [a["trigger_event_id"] for a in alarms(client, "dev-1")] == ["e1"]
    # Acknowledge inside the cooldown: the pinned high-water now covers e2.
    client.post(
        f"/devices/dev-1/alarms/{aid}/ack",
        json={"expected_status": alarm["status"], "expected_rule_version": 1},
    )
    # Replaying e2 (which re-runs the whole reconciliation) must neither
    # create a new alarm nor resurrect/duplicate the acked one.
    event(client, "dev-1", "e2", "2026-09-20T10:05:00Z")
    assert len(alarms(client, "dev-1")) == 1
    # More suppressed-range replays, out of order, stay a no-op.
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    assert len(alarms(client, "dev-1")) == 1
    # A later arrival after the ack fires even at event time inside cooldown.
    event(client, "dev-1", "e3", "2026-09-20T10:06:00Z")
    live = [a for a in alarms(client, "dev-1") if a["status"] != "acked"]
    assert [a["trigger_event_id"] for a in live] == ["e3"]


def test_late_event_after_ack_still_invalidates_structurally_gone_alarm(client):
    # Two independent open alarms (cooldown=0): e1 is acked; afterwards a
    # late non-matching event rewrites history so that a *different* open
    # alarm (m3) loses its streak. The acked high-water must not shield m3
    # from structural invalidation.
    rule(client, "dev-1", "hot", 1, consecutive=3, cooldown=0)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    m3 = alarms(client, "dev-1")[0]
    client.post(
        f"/devices/dev-1/alarms/{m3['alarm_id']}/ack",
        json={"expected_status": m3["status"], "expected_rule_version": 1},
    )
    # New run after the ack -> m6 is a fresh, separate open alarm.
    event(client, "dev-1", "m4", "2026-09-20T11:00:00Z")
    event(client, "dev-1", "m5", "2026-09-20T11:02:00Z")
    event(client, "dev-1", "m6", "2026-09-20T11:04:00Z")
    open_alarms = [a for a in alarms(client, "dev-1") if a["status"] != "acked"]
    assert [a["trigger_event_id"] for a in open_alarms] == ["m6"]

    # A late cold event sorting into the m4..m6 run destroys that streak.
    event(client, "dev-1", "break", "2026-09-20T11:01:00Z", v=0)
    statuses = {a["trigger_event_id"]: a["status"]
                for a in alarms(client, "dev-1")}
    assert statuses["m6"] == "invalidated"
    # The acknowledged alarm stays acked (terminal history retained).
    assert statuses["e3"] == "acked"


def test_invalidated_alarm_can_not_be_acknowledged(client):
    rule(client, "dev-1", "hot", 1)
    event(client, "dev-1", "e1", "2026-09-20T10:00:00Z")
    event(client, "dev-1", "e2", "2026-09-20T10:02:00Z")
    event(client, "dev-1", "e3", "2026-09-20T10:04:00Z")
    aid = alarms(client, "dev-1")[0]["alarm_id"]
    event(client, "dev-1", "late", "2026-09-20T10:01:30Z", v=0)
    resp = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                       json={"expected_status": "invalidated",
                             "expected_rule_version": 1})
    # invalidated is not an ackable expected_status (422 from validation);
    # presenting a stale expectation yields 409.
    assert resp.status_code == 422
    resp = client.post(f"/devices/dev-1/alarms/{aid}/ack",
                       json={"expected_status": "active",
                             "expected_rule_version": 1})
    assert resp.status_code == 409


def test_unknown_alarm_ack_is_404(client):
    resp = client.post("/devices/dev-1/alarms/nope/ack",
                       json={"expected_status": "active",
                             "expected_rule_version": 1})
    assert resp.status_code == 404


# ------------------------------------------------------------ concurrency


def test_concurrent_out_of_order_writes_remain_duplicate_free(tmp_path):
    db_path = tmp_path / "ooo.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        main_client.put("/schemas/temp/1.0", json=SCHEMA_BODY)
        rule(main_client, "dev-1", "hot", 1, consecutive=2, cooldown=0)

        barrier = threading.Barrier(6)

        def worker(i):
            with TestClient(app) as c:
                barrier.wait()
                # Six threads, two devices, event times deliberately
                # shuffled relative to thread/arrival order.
                device = "dev-1" if i % 2 == 0 else "dev-2"
                eid = f"d{i}"
                minute = (i * 7) % 30
                event(c, device, eid, f"2026-09-20T12:{minute:02d}:00Z")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with TestClient(app) as check:
            # Every trigger_key unique: repeated reconciliations under
            # serialized writes never produced duplicate alarm rows.
            conn = __import__("sqlite3").connect(str(db_path))
            try:
                total = conn.execute("SELECT COUNT(*) FROM alarms").fetchone()[0]
                distinct = conn.execute(
                    "SELECT COUNT(DISTINCT trigger_key) FROM alarms"
                ).fetchone()[0]
                assert total == distinct
            finally:
                conn.close()


def test_concurrent_rule_version_creation_only_one_wins(tmp_path):
    app = create_app(database_path=str(tmp_path / "rules.db"), gap_seconds=GAP)
    codes: list[int] = []
    barrier = threading.Barrier(6)
    lock = threading.Lock()

    def worker():
        with TestClient(app) as c:
            barrier.wait()
            resp = rule(c, "dev-1", "hot", 1, consecutive=1, cooldown=0)
            with lock:
                codes.append(resp.status_code)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert codes.count(201) == 1
    assert codes.count(409) == 5
    with TestClient(app) as c:
        versions = c.get("/devices/dev-1/alert-rules").json()
        assert [v["version"] for v in versions] == [1]


def test_concurrent_event_writes_produce_exactly_one_alarm(tmp_path):
    db_path = tmp_path / "conc.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        main_client.put("/schemas/temp/1.0", json=SCHEMA_BODY)
        rule(main_client, "dev-1", "hot", 1, consecutive=1, cooldown=0)

        results: list[int] = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def worker(i):
            with TestClient(app) as c:
                barrier.wait()
                resp = event(c, "dev-1", f"p{i}",
                             f"2026-09-20T11:00:{i:02d}Z")
                with lock:
                    results.append(resp.status_code)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results == [201] * 8
        with TestClient(app) as check:
            fired = alarms(check, "dev-1")
            # 8 distinct trigger events, consecutive=1, cooldown=0 ->
            # exactly 8 alarm rows, never duplicated by the race.
            assert len(fired) == 8
            assert len({a["alarm_id"] for a in fired}) == 8
            assert sorted(a["trigger_event_id"] for a in fired) == \
                [f"p{i}" for i in range(8)]


def test_concurrent_cross_era_writes_are_deterministic(tmp_path):
    """Concurrent late events across a frozen and an open era stay correct.

    v1 and v2 are created before the racing writes (v1 era ends at a 10:00
    anchor). Eight concurrent writers insert events whose event times are
    shuffled across both eras (and a few non-matching ones): the final
    alarm set must be deterministic, duplicate-free and partitioned across
    versions purely by event time.
    """
    db_path = tmp_path / "eras.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        main_client.put("/schemas/temp/1.0", json=SCHEMA_BODY)
        rule(main_client, "dev-1", "hot", 1, consecutive=1, cooldown=0)
        # Anchor pins the v1/v2 frontier at (10:00, anchor).
        event(main_client, "dev-1", "anchor",
              "2026-09-20T10:00:00Z", v=50)
        rule(main_client, "dev-1", "hot", 2,
             conditions=[{"pointer": "/v", "operator": "gte", "value": 40}],
             window=600, consecutive=1, cooldown=0)

        barrier = threading.Barrier(8)

        def worker(i):
            with TestClient(app) as c:
                # i even  -> v1 era (before 10:00); i odd -> v2 era (after)
                minute = 9 if i % 2 == 0 else 11
                value = 0 if i == 3 else 50  # one v2-era non-matching event
                barrier.wait()
                event(c, "dev-1", f"w{i}",
                      f"2026-09-20T{minute:02d}:{i:02d}:00Z", v=value)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with TestClient(app) as check:
            fired = alarms(check, "dev-1")
            by_version = {}
            for a in fired:
                by_version.setdefault(a["rule_version"], set()).add(
                    a["trigger_event_id"])
            # Even i (0,2,4,6) sort into v1's closed era; odd matching ones
            # (1,5,7) into v2; i=3 is v=0 and never matches.
            assert by_version.get(1) == {"anchor", "w0", "w2", "w4", "w6"}
            assert by_version.get(2) == {"w1", "w5", "w7"}

            conn = __import__("sqlite3").connect(str(db_path))
            try:
                total = conn.execute("SELECT COUNT(*) FROM alarms").fetchone()[0]
                distinct = conn.execute(
                    "SELECT COUNT(DISTINCT trigger_key) FROM alarms"
                ).fetchone()[0]
                assert total == distinct
            finally:
                conn.close()


def test_concurrent_acks_only_one_transitions(tmp_path):
    db_path = tmp_path / "ack.db"
    app = create_app(database_path=str(db_path), gap_seconds=GAP)
    with TestClient(app) as main_client:
        main_client.put("/schemas/temp/1.0", json=SCHEMA_BODY)
        rule(main_client, "dev-1", "hot", 1, consecutive=1, cooldown=10)
        event(main_client, "dev-1", "e1", "2026-09-20T10:00:00Z")
        expected = alarms(main_client, "dev-1")[0]["status"]
        aid = alarms(main_client, "dev-1")[0]["alarm_id"]

        codes: list[int] = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def worker():
            with TestClient(app) as c:
                barrier.wait()
                resp = c.post(f"/devices/dev-1/alarms/{aid}/ack",
                              json={"expected_status": expected,
                                    "expected_rule_version": 1})
                with lock:
                    codes.append(resp.status_code)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    # Cooldown is only 10s of event time but status is wall-time derived;
    # all racers present the same optimistic precondition.
    assert codes.count(200) == 1
    assert codes.count(409) == 7

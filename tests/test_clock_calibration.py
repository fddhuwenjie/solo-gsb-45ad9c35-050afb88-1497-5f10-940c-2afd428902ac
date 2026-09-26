"""Tests for the device clock calibration module: anchors, epochs,
normalization and the frozen cross-device timeline."""

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

DRIFT_PPM = 50_000.0
EXTRAP = 3600.0


@pytest.fixture()
def client(tmp_path):
    app = create_app(
        database_path=str(tmp_path / "test.db"),
        drift_ppm=DRIFT_PPM,
        max_extrapolation_seconds=EXTRAP,
    )
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def loose_client(tmp_path):
    """Client with a very permissive drift budget (200%)."""
    app = create_app(
        database_path=str(tmp_path / "loose.db"),
        drift_ppm=2_000_000.0,
        max_extrapolation_seconds=EXTRAP,
    )
    with TestClient(app) as c:
        yield c


def put_anchor(client, device, anchor, seq, device_time, reference_time):
    return client.put(
        f"/devices/{device}/clock/anchors/{anchor}",
        json={
            "anchor_id": anchor,
            "observed_seq": seq,
            "device_time": device_time,
            "reference_time": reference_time,
        },
    )


def test_first_anchor_opens_epoch_1_and_is_idempotent(client):
    resp = put_anchor(
        client, "dev-1", "a1", 1,
        "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z",
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["epoch"] == 1
    assert body["drift_ppm"] is None

    # Exact replay -> 200 with no changes.
    replay = put_anchor(
        client, "dev-1", "a1", 1,
        "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z",
    )
    assert replay.status_code == 200
    assert replay.json() == body

    # Same id, different content -> 409 and no new row.
    conflict = put_anchor(
        client, "dev-1", "a1", 1,
        "2026-01-01T00:00:01Z", "2026-01-01T00:00:00Z",
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["reason"] == "anchor_content_conflict"
    anchors = client.get("/devices/dev-1/clock/anchors").json()
    assert len(anchors) == 1


def test_observed_seq_must_strictly_increase(client):
    put_anchor(client, "dev-1", "a1", 10, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    resp = put_anchor(
        client, "dev-1", "a2", 10,
        "2026-01-01T00:01:00Z", "2026-01-01T00:01:00Z",
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "observed_seq_conflict"
    assert len(client.get("/devices/dev-1/clock/anchors").json()) == 1


def test_reference_time_must_strictly_increase(client):
    put_anchor(client, "dev-1", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    resp = put_anchor(
        client, "dev-1", "a2", 2,
        "2026-01-01T00:01:00Z", "2026-01-01T00:01:00Z",
    )
    assert resp.status_code == 201
    resp = put_anchor(
        client, "dev-1", "a3", 3,
        "2026-01-01T00:02:00Z", "2026-01-01T00:00:30Z",
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "reference_time_not_monotonic"


def test_drift_over_limit_rejected_and_not_stored(client):
    put_anchor(client, "dev-1", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    # Device advances 100s, reference 200s -> 100% = 1,000,000 ppm > 50,000.
    resp = put_anchor(
        client, "dev-1", "a2", 2,
        "2026-01-01T00:01:40Z", "2026-01-01T00:03:20Z",
    )
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["reason"] == "drift_ppm_exceeded"
    assert detail["measured_drift_ppm"] > DRIFT_PPM
    assert len(client.get("/devices/dev-1/clock/anchors").json()) == 1

    # A within-budget anchor then succeeds.
    resp = put_anchor(
        client, "dev-1", "a2", 2,
        "2026-01-01T00:01:40Z", "2026-01-01T00:01:41Z",
    )
    assert resp.status_code == 201
    assert resp.json()["drift_ppm"] == pytest.approx(9900.99, abs=1.0)


def test_device_time_rollback_opens_new_epoch_without_drift_check(client):
    put_anchor(client, "dev-1", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    # An enormous jump back in device time would fail any drift test, but a
    # reset opens a fresh epoch instead.
    resp = put_anchor(
        client, "dev-1", "a2", 2,
        "1970-01-01T00:00:00Z", "2026-01-01T00:01:00Z",
    )
    assert resp.status_code == 201
    assert resp.json()["epoch"] == 2
    assert resp.json()["drift_ppm"] is None

    # Equal device_time also opens an epoch.
    resp = put_anchor(
        client, "dev-1", "a3", 3,
        "1970-01-01T00:00:00Z", "2026-01-01T00:02:00Z",
    )
    assert resp.status_code == 201
    assert resp.json()["epoch"] == 3

    epochs = {a["anchor_id"]: a["epoch"] for a in
              client.get("/devices/dev-1/clock/anchors").json()}
    assert epochs == {"a1": 1, "a2": 2, "a3": 3}


def test_normalize_methods_and_uncertainty(client):
    # Fast clock: device 100s == reference 101s.
    put_anchor(client, "dev-1", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    put_anchor(client, "dev-1", "a2", 2, "2026-01-01T00:01:40Z", "2026-01-01T00:01:41Z")

    def norm(device_time, **kw):
        return client.post(
            "/devices/dev-1/clock/normalize",
            json={"device_time": device_time, **kw},
        )

    resp = norm("2026-01-01T00:00:00Z")
    assert resp.status_code == 200
    body = resp.json()
    assert body["method"] == "anchor"
    assert body["corrected_at"] == "2026-01-01T00:00:00+00:00"
    assert body["uncertainty_seconds"] == 0.0
    assert body["uncertainty"]["lower"] == body["uncertainty"]["upper"]
    assert body["bracket_anchor_ids"] == ["a1", "a1"]

    # Halfway in device time -> 50.5s reference (slope 101/100).
    body = norm("2026-01-01T00:00:50Z").json()
    assert body["method"] == "interpolate"
    assert body["corrected_at"] == "2026-01-01T00:00:50.500000+00:00"
    assert body["bracket_anchor_ids"] == ["a1", "a2"]
    assert body["uncertainty_seconds"] == pytest.approx(2.5, abs=1e-9)

    # 60s beyond the last anchor: reference 101 + 1.01*60 = 161.6s.
    body = norm("2026-01-01T00:02:40Z").json()
    assert body["method"] == "extrapolate"
    assert body["corrected_at"] == "2026-01-01T00:02:41.600000+00:00"
    assert body["bracket_anchor_ids"] == ["a2", None]

    # Beyond the configured range -> 409.
    resp = norm("2026-01-01T02:00:00Z")
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "outside_extrapolation_range"


def test_normalize_without_anchors_is_409(client):
    resp = client.post(
        "/devices/dev-x/clock/normalize",
        json={"device_time": "2026-01-01T00:00:00Z"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "no_calibration_anchors"


def test_normalize_epoch_selection(client):
    put_anchor(client, "dev-1", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    put_anchor(client, "dev-1", "a2", 2, "1970-01-01T00:00:05Z", "2026-01-01T00:01:00Z")

    def norm(device_time, **kw):
        return client.post(
            "/devices/dev-1/clock/normalize",
            json={"device_time": device_time, **kw},
        ).json()

    # observed_seq chooses the epoch whose last anchor it has seen.
    assert norm("1970-01-01T00:00:00Z", observed_seq=2)["epoch"] == 2
    assert norm("2026-01-01T00:00:00Z", observed_seq=1)["epoch"] == 1
    assert norm("1970-01-01T00:00:00Z")["epoch"] == 2  # default: latest
    assert norm("1970-01-01T00:00:00Z", epoch=1)["epoch"] == 1

    resp = client.post(
        "/devices/dev-1/clock/normalize",
        json={"device_time": "2026-01-01T00:00:00Z", "observed_seq": 0},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "observed_seq_before_first_anchor"

    resp = client.post(
        "/devices/dev-1/clock/normalize",
        json={"device_time": "2026-01-01T00:00:00Z", "epoch": 9},
    )
    assert resp.status_code == 409

    # Both selectors at once -> 422.
    resp = client.post(
        "/devices/dev-1/clock/normalize",
        json={
            "device_time": "2026-01-01T00:00:00Z",
            "observed_seq": 2,
            "epoch": 2,
        },
    )
    assert resp.status_code == 422


def test_timeline_orders_by_corrected_time_across_devices(client):
    # dev-a runs 1s fast per 60s device time; dev-b is exact.
    put_anchor(client, "dev-a", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    put_anchor(client, "dev-a", "a2", 2, "2026-01-01T00:01:00Z", "2026-01-01T00:01:01Z")
    put_anchor(client, "dev-b", "b1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")

    # Device-time ordering would put a1 first; corrected ordering interleaves.
    submit = [
        ("dev-a", "ea", "2026-01-01T00:00:30Z"),  # corrected 30.5
        ("dev-b", "eb", "2026-01-01T00:00:31Z"),  # corrected 31.0
        ("dev-a", "ea2", "2026-01-01T00:00:40Z"),  # corrected 40.6667
    ]
    for device, event_id, ts in submit:
        resp = client.post(
            "/events",
            json={"device_id": device, "event_id": event_id, "occurred_at": ts},
        )
        assert resp.status_code == 201

    resp = client.post("/clock/timeline", json={"limit": 50})
    assert resp.status_code == 200
    body = resp.json()
    assert body["next_cursor"] is None
    assert [e["event_id"] for e in body["events"]] == ["ea", "eb", "ea2"]
    assert body["events"][0]["corrected_at"] == "2026-01-01T00:00:30.500000+00:00"
    assert body["frozen_anchor_high_waters"]

    # Devices without anchors never appear.
    client.post(
        "/events",
        json={"device_id": "dev-c", "event_id": "ec",
              "occurred_at": "2026-01-01T00:00:00Z"},
    )
    body = client.post("/clock/timeline", json={}).json()
    assert {e["device_id"] for e in body["events"]} == {"dev-a", "dev-b"}

    # Device filter and its fingerprint.
    body = client.post("/clock/timeline", json={"device_ids": ["dev-b"]}).json()
    assert [e["event_id"] for e in body["events"]] == ["eb"]
    # No cursor on the final page, so page with limit=1 to obtain one, then
    # replaying it against a different filter is a 422.
    first = client.post("/clock/timeline", json={"limit": 1}).json()
    resp = client.post(
        "/clock/timeline",
        json={"device_ids": ["dev-b"], "cursor": first["next_cursor"]},
    )
    assert resp.status_code == 422


def _walk_timeline(client, payload):
    """POST /clock/timeline with a payload, following every next_cursor."""
    seen = []
    body = payload
    frozen = None
    while True:
        page = client.post("/clock/timeline", json=body).json()
        if frozen is None:
            frozen = page["frozen_anchor_high_waters"]
        seen.extend(page["events"])
        if page["next_cursor"] is None:
            return seen, frozen
        body = {"cursor": page["next_cursor"]}


def test_timeline_pagination_freezes_anchor_high_water(loose_client):
    client = loose_client
    put_anchor(client, "dev-a", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")

    events = [(f"e{i:02d}", f"2026-01-01T00:00:{i:02d}Z") for i in range(1, 7)]
    for event_id, ts in events:
        client.post(
            "/events",
            json={"device_id": "dev-a", "event_id": event_id, "occurred_at": ts},
        )

    # Walk page 1 (limit 2), then add a new anchor that remaps future device
    # time substantially; the ongoing walk must keep the frozen mapping.
    first = client.post("/clock/timeline", json={"limit": 2}).json()
    assert [e["event_id"] for e in first["events"]] == ["e01", "e02"]
    frozen = first["frozen_anchor_high_waters"]
    assert frozen == {"dev-a": 1}
    cursor = first["next_cursor"]
    assert cursor is not None

    # New anchor: device clock now 100s behind reference at seq 2, plus a
    # new event that would sort into the middle of the range; neither may
    # affect the already-started (frozen) page walk.
    resp = put_anchor(
        client, "dev-a", "a2", 2,
        "2026-01-01T00:01:00Z", "2026-01-01T00:02:40Z",
    )
    assert resp.status_code == 201
    client.post(
        "/events",
        json={"device_id": "dev-a", "event_id": "late",
              "occurred_at": "2026-01-01T00:00:03Z"},
    )

    # Continue the frozen walk: no repeats, no skips, old mapping retained.
    rest = []
    while cursor:
        page = client.post("/clock/timeline", json={"cursor": cursor}).json()
        rest.extend(page["events"])
        cursor = page["next_cursor"]
    walked = first["events"] + rest
    ids = [e["event_id"] for e in walked]
    assert ids == [f"e{i:02d}" for i in range(1, 7)]
    assert "late" not in ids
    # Frozen one-anchor epoch (slope 1): corrected == occurred for all rows.
    for event in walked:
        assert event["corrected_at"][:19] == event["occurred_at"][:19]
        assert event["epoch"] == 1

    # A fresh walk sees the new anchor: historical events are remapped and
    # re-sorted using the two-anchor interpolation, and the late event is in.
    fresh, fresh_frozen = _walk_timeline(client, {"limit": 2})
    assert fresh_frozen == {"dev-a": 2}
    # Slope now (160-0)/(60-0) = 8/3: e03 at device t=3s -> reference 8s.
    e03 = next(e for e in fresh if e["event_id"] == "e03")
    assert e03["corrected_at"] == "2026-01-01T00:00:08+00:00"
    assert "late" in [e["event_id"] for e in fresh]


def test_timeline_tie_break_and_total_order(client):
    put_anchor(client, "dev-a", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    put_anchor(client, "dev-b", "b1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    # Same corrected_at on two devices: (device_id, event_id) breaks the tie
    # deterministically and keyset paging neither repeats nor skips it.
    for device, event_id in [("dev-b", "x"), ("dev-a", "x")]:
        client.post(
            "/events",
            json={"device_id": device, "event_id": event_id,
                  "occurred_at": "2026-01-01T00:00:05Z"},
        )
    seen, _ = _walk_timeline(client, {"limit": 1})
    assert [(e["device_id"], e["event_id"]) for e in seen] == [
        ("dev-a", "x"),
        ("dev-b", "x"),
    ]


def test_timeline_cursor_rejects_garbage(client):
    resp = client.post("/clock/timeline", json={"cursor": "nope"})
    assert resp.status_code == 422
    resp = client.post("/clock/timeline", json={"cursor": "ctl1:AAAA.aaaa"})
    assert resp.status_code == 422


def test_anchor_path_body_mismatch_is_422(client):
    resp = client.put(
        "/devices/dev-1/clock/anchors/a1",
        json={
            "anchor_id": "other",
            "observed_seq": 1,
            "device_time": "2026-01-01T00:00:00Z",
            "reference_time": "2026-01-01T00:00:00Z",
        },
    )
    assert resp.status_code == 422


def test_devices_share_anchor_id_namespace_independently(client):
    r1 = put_anchor(client, "dev-1", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    r2 = put_anchor(client, "dev-2", "a1", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    assert r1.status_code == r2.status_code == 201

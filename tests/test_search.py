"""Tests for POST /events/search: payload conditions, paging and snapshot."""

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.search import SearchQuery, query_fingerprint


@pytest.fixture()
def client(tmp_path):
    app = create_app(database_path=str(tmp_path / "test.db"), gap_seconds=300)
    with TestClient(app) as c:
        yield c


def register_permissive_schema(client, event_type="telemetry", version="1"):
    resp = client.put(
        f"/schemas/{event_type}/{version}",
        json={"schema": {}},  # empty schema: any JSON instance, including null
    )
    assert resp.status_code in (200, 201), resp.text


def submit_payload(
    client,
    event_id,
    occurred_at,
    payload=None,
    device_id="dev-1",
    event_type="telemetry",
    version="1",
):
    resp = client.post(
        "/events",
        json={
            "device_id": device_id,
            "event_id": event_id,
            "occurred_at": occurred_at,
            "event_type": event_type,
            "schema_version": version,
            "payload": payload,
        },
    )
    assert resp.status_code in (200, 201), resp.text
    return resp


def submit_plain(client, event_id, occurred_at, device_id="dev-1"):
    resp = client.post(
        "/events",
        json={
            "device_id": device_id,
            "event_id": event_id,
            "occurred_at": occurred_at,
        },
    )
    assert resp.status_code in (200, 201), resp.text
    return resp


def search(client, **body):
    return client.post("/events/search", json=body)


def ids(resp):
    return [e["event_id"] for e in resp.json()["events"]]


# ------------------------------------------------------------- scalar filters

def test_no_filters_returns_all_sorted(client):
    register_permissive_schema(client)
    submit_payload(client, "b", "2026-09-20T10:02:00Z", {"v": 2})
    submit_payload(client, "a", "2026-09-20T10:02:00Z", {"v": 1})
    submit_payload(client, "c", "2026-09-20T10:01:00Z", {"v": 0})

    resp = search(client)
    assert resp.status_code == 200
    assert ids(resp) == ["c", "a", "b"]  # occurred_at, then event_id
    assert resp.json()["next_cursor"] is None


def test_scalar_filters_device_type_version(client):
    register_permissive_schema(client)
    submit_payload(client, "a", "2026-09-20T10:00:00Z", {"v": 1}, device_id="d1")
    submit_payload(client, "b", "2026-09-20T10:01:00Z", {"v": 2}, device_id="d2")
    submit_plain(client, "p", "2026-09-20T10:02:00Z", device_id="d1")
    register_permissive_schema(client, event_type="other", version="9")
    submit_payload(
        client, "o", "2026-09-20T10:03:00Z", {"v": 3},
        device_id="d1", event_type="other", version="9",
    )

    assert ids(search(client, device_id="d1")) == ["a", "p", "o"]
    assert ids(search(client, device_id="d1", event_type="telemetry")) == ["a"]
    assert ids(search(client, schema_version="1")) == ["a", "b"]
    # No-payload events have NULL type/version and never match scalars there.
    assert ids(search(client, event_type="telemetry")) == ["a", "b"]


def test_occurred_at_half_open_interval(client):
    register_permissive_schema(client)
    for eid, ts in [
        ("a", "2026-09-20T10:00:00Z"),
        ("b", "2026-09-20T10:05:00Z"),
        ("c", "2026-09-20T10:10:00Z"),
    ]:
        submit_payload(client, eid, ts, {"v": 1})

    resp = search(
        client,
        occurred_at={
            "start": "2026-09-20T10:00:00Z",  # inclusive
            "end": "2026-09-20T10:10:00Z",    # exclusive
        },
    )
    assert ids(resp) == ["a", "b"]

    # Timezone normalization: the same instant in another offset.
    resp = search(
        client,
        occurred_at={
            "start": "2026-09-20T12:00:00+02:00",
            "end": "2026-09-20T12:10:00+02:00",
        },
    )
    assert ids(resp) == ["a", "b"]


def test_empty_interval_is_422(client):
    resp = search(
        client,
        occurred_at={
            "start": "2026-09-20T10:00:00Z",
            "end": "2026-09-20T10:00:00Z",
        },
    )
    assert resp.status_code == 422
    resp = search(
        client,
        occurred_at={
            "start": "2026-09-20T10:01:00Z",
            "end": "2026-09-20T10:00:00Z",
        },
    )
    assert resp.status_code == 422


# ------------------------------------------------------------ JSON semantics

def test_exists_distinguishes_missing_from_null(client):
    register_permissive_schema(client)
    submit_payload(client, "missing", "2026-09-20T10:00:00Z", {"other": 1})
    submit_payload(client, "null", "2026-09-20T10:01:00Z", {"x": None})
    submit_payload(client, "value", "2026-09-20T10:02:00Z", {"x": 0})
    submit_plain(client, "nopayload", "2026-09-20T10:03:00Z")

    resp = search(client, conditions=[{"pointer": "/x", "operator": "exists", "value": True}])
    assert ids(resp) == ["null", "value"]

    resp = search(client, conditions=[{"pointer": "/x", "operator": "exists", "value": False}])
    assert ids(resp) == ["missing", "nopayload"]


def test_eq_is_strict_across_json_types(client):
    register_permissive_schema(client)
    submit_payload(client, "num", "2026-09-20T10:00:00Z", {"x": 1})
    submit_payload(client, "numfloat", "2026-09-20T10:01:00Z", {"x": 1.0})
    submit_payload(client, "str", "2026-09-20T10:02:00Z", {"x": "1"})
    submit_payload(client, "bool", "2026-09-20T10:03:00Z", {"x": True})
    submit_payload(client, "nullfield", "2026-09-20T10:04:00Z", {"x": None})
    submit_payload(client, "missing", "2026-09-20T10:05:00Z", {})

    assert ids(search(client, conditions=[{"pointer": "/x", "operator": "eq", "value": 1}])) == [
        "num",
        "numfloat",
    ]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "eq", "value": "1"}])
    ) == ["str"]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "eq", "value": True}])
    ) == ["bool"]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "eq", "value": None}])
    ) == ["nullfield"]


def test_ordering_operators_numeric_only(client):
    register_permissive_schema(client)
    submit_payload(client, "ten", "2026-09-20T10:00:00Z", {"x": 10})
    submit_payload(client, "str", "2026-09-20T10:01:00Z", {"x": "10"})
    submit_payload(client, "bool", "2026-09-20T10:02:00Z", {"x": True})
    submit_payload(client, "nullfield", "2026-09-20T10:03:00Z", {"x": None})
    submit_payload(client, "missing", "2026-09-20T10:04:00Z", {})

    # Threshold inside the domain: only the numeric 10 participates;
    # strings, booleans, null and missing locations are never compared.
    expected = {
        "lt": ["ten"],
        "lte": ["ten"],
        "gt": [],
        "gte": [],
    }
    for op in ("lt", "lte", "gt", "gte"):
        resp = search(
            client, conditions=[{"pointer": "/x", "operator": op, "value": 100}]
        )
        assert ids(resp) == expected[op]

    # A smaller threshold selects the numeric row for the upper operators too.
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "gt", "value": 5}])
    ) == ["ten"]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "gte", "value": 10}])
    ) == ["ten"]


def test_ordering_boundaries(client):
    register_permissive_schema(client)
    for eid, ts, v in [
        ("a", "2026-09-20T10:00:00Z", 1),
        ("b", "2026-09-20T10:01:00Z", 5),
        ("c", "2026-09-20T10:02:00Z", 10),
    ]:
        submit_payload(client, eid, ts, {"x": v})

    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "lt", "value": 5}])
    ) == ["a"]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "lte", "value": 5}])
    ) == ["a", "b"]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "gt", "value": 5}])
    ) == ["c"]
    assert ids(
        search(client, conditions=[{"pointer": "/x", "operator": "gte", "value": 5}])
    ) == ["b", "c"]


def test_nested_pointer_and_escapes(client):
    register_permissive_schema(client)
    submit_payload(
        client,
        "deep",
        "2026-09-20T10:00:00Z",
        {"reading": {"value": 42}, "a/b": {"~x": 7}, "tags": [10, 20]},
    )
    submit_payload(client, "other", "2026-09-20T10:01:00Z", {"reading": {}})

    assert ids(
        search(
            client,
            conditions=[{"pointer": "/reading/value", "operator": "gte", "value": 42}],
        )
    ) == ["deep"]
    assert ids(
        search(
            client,
            conditions=[{"pointer": "/a~1b/~0x", "operator": "eq", "value": 7}],
        )
    ) == ["deep"]
    assert ids(
        search(client, conditions=[{"pointer": "/tags/1", "operator": "eq", "value": 20}])
    ) == ["deep"]
    # "-" addresses no existing element.
    resp = search(
        client, conditions=[{"pointer": "/tags/-", "operator": "exists", "value": True}]
    )
    assert ids(resp) == []
    # Leading-zero array indices are invalid per RFC 6901.
    resp = search(
        client, conditions=[{"pointer": "/tags/01", "operator": "exists", "value": True}]
    )
    assert ids(resp) == []


def test_root_pointer(client):
    register_permissive_schema(client)
    submit_payload(client, "n", "2026-09-20T10:00:00Z", None)
    submit_payload(client, "obj", "2026-09-20T10:01:00Z", {"x": 1})
    submit_plain(client, "plain", "2026-09-20T10:02:00Z")

    assert ids(
        search(client, conditions=[{"pointer": "", "operator": "eq", "value": None}])
    ) == ["n"]
    assert ids(
        search(client, conditions=[{"pointer": "", "operator": "exists", "value": False}])
    ) == ["plain"]
    assert ids(
        search(client, conditions=[{"pointer": "", "operator": "exists", "value": True}])
    ) == ["n", "obj"]


def test_conditions_combine_as_and(client):
    register_permissive_schema(client)
    submit_payload(client, "both", "2026-09-20T10:00:00Z", {"a": 1, "b": 2})
    submit_payload(client, "one", "2026-09-20T10:01:00Z", {"a": 1, "b": 99})
    submit_payload(client, "none", "2026-09-20T10:02:00Z", {"a": 9, "b": 9})

    resp = search(
        client,
        conditions=[
            {"pointer": "/a", "operator": "eq", "value": 1},
            {"pointer": "/b", "operator": "lt", "value": 50},
        ],
    )
    assert ids(resp) == ["both"]


# ------------------------------------------------------------ request errors

def test_more_than_five_conditions_is_422(client):
    resp = search(
        client,
        conditions=[
            {"pointer": f"/f{i}", "operator": "exists", "value": True}
            for i in range(6)
        ],
    )
    assert resp.status_code == 422


@pytest.mark.parametrize(
    "condition",
    [
        {"pointer": "x", "operator": "exists", "value": True},  # no leading slash
        {"pointer": "/x", "operator": "near", "value": 1},
        {"pointer": "/x", "operator": "exists", "value": "yes"},
        {"pointer": "/x", "operator": "lt", "value": "1"},
        {"pointer": "/x", "operator": "gt", "value": True},
    ],
)
def test_invalid_conditions_are_422(client, condition):
    resp = search(client, conditions=[condition])
    assert resp.status_code == 422


def test_missing_value_key_is_422_even_for_null(client):
    # /value omitted -> 422; explicit null is accepted.
    resp = client.post(
        "/events/search",
        json={"conditions": [{"pointer": "/x", "operator": "eq"}]},
    )
    assert resp.status_code == 422
    resp = search(
        client, conditions=[{"pointer": "/x", "operator": "eq", "value": None}]
    )
    assert resp.status_code == 200


def test_limit_validation(client):
    assert search(client, limit=0).status_code == 422
    assert search(client, limit=501).status_code == 422


# --------------------------------------------------------------- pagination

def walk_all(client, page_size, **body):
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        req = {**body, "limit": page_size}
        if cursor is not None:
            req["cursor"] = cursor
        resp = search(client, **req)
        assert resp.status_code == 200, resp.text
        batch = ids(resp)
        seen.extend(batch)
        pages += 1
        cursor = resp.json()["next_cursor"]
        if cursor is None:
            break
        assert pages < 100
    return seen, pages


def test_keyset_pagination_walks_everything_in_order(client):
    register_permissive_schema(client)
    expected = [
        (f"e{i:03d}", f"2026-09-20T10:{i // 60:02d}:{i % 60:02d}Z", i)
        for i in range(25)
    ]
    # Insert shuffled.
    for eid, ts, v in expected[::7] + expected[1::7] + expected[2::7] + expected[3::7] + expected[4::7] + expected[5::7] + expected[6::7]:
        submit_payload(client, eid, ts, {"v": v})

    for page_size in (1, 2, 10, 25, 100):
        seen, pages = walk_all(client, page_size)
        assert seen == [e[0] for e in expected]
        assert len(seen) == 25
        assert pages == max(1, -(-25 // page_size))


def test_pagination_with_nonmatching_rows_between(client):
    register_permissive_schema(client)
    # Mix of matching and non-matching payloads; pages must not skip matches.
    for i in range(20):
        submit_payload(
            client,
            f"e{i:02d}",
            f"2026-09-20T10:00:{i:02d}Z",
            {"v": i if i % 2 == 0 else f"s{i}"},
        )
    seen, pages = walk_all(
        client,
        5,
        conditions=[{"pointer": "/v", "operator": "gte", "value": 0}],
    )
    assert seen == [f"e{i:02d}" for i in range(0, 20, 2)]
    assert pages == 2


def test_timestamps_tied_order_by_event_id_across_pages(client):
    register_permissive_schema(client)
    for eid in ["z", "a", "m", "b", "y"]:
        submit_payload(client, eid, "2026-09-20T10:00:00Z", {"v": 1})
    seen, _ = walk_all(client, 2)
    assert seen == ["a", "b", "m", "y", "z"]


def test_first_page_pins_snapshot_new_rows_never_mix_in(client):
    register_permissive_schema(client)
    for i in range(3):
        submit_payload(
            client, f"e{i}", f"2026-09-20T10:00:0{i}Z", {"v": i}
        )

    first = search(client, limit=2)
    assert ids(first) == ["e0", "e1"]
    cursor = first.json()["next_cursor"]
    assert cursor is not None

    # Rows inserted after the first page: one before the window, one after.
    submit_payload(client, "late-early", "2026-09-20T09:00:00Z", {"v": 99})
    submit_payload(client, "late-late", "2026-09-20T11:00:00Z", {"v": 99})

    second = search(client, limit=2, cursor=cursor)
    assert ids(second) == ["e2"]  # neither late row leaks in
    assert second.json()["next_cursor"] is None

    # A brand-new first page (no cursor) observes the current full set.
    fresh = search(client)
    assert set(ids(fresh)) == {"e0", "e1", "e2", "late-early", "late-late"}


def test_deleted_rows_simply_absent_while_walk_continues(client):
    register_permissive_schema(client)
    for i in range(6):
        submit_payload(client, f"e{i}", f"2026-09-20T10:00:0{i}Z", {"v": i})

    first = search(client, limit=2)
    cursor = first.json()["next_cursor"]
    assert ids(first) == ["e0", "e1"]

    # Delete a row that belongs to a future page directly out of band.
    db = client.app.state.db_path
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM events WHERE event_id = 'e3'")

    second = search(client, limit=10, cursor=cursor)
    assert ids(second) == ["e2", "e4", "e5"]
    assert second.json()["next_cursor"] is None

    # Deleting the cursor's own anchor row is harmless too.
    first = search(client, limit=2)
    cursor = first.json()["next_cursor"]
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM events WHERE event_id = 'e1'")
    second = search(client, limit=10, cursor=cursor)
    assert ids(second) == ["e2", "e4", "e5"]


def test_deleting_max_seq_then_inserting_never_leaks_into_pages(client):
    """Regression for rowid reuse: the pinned maximum may be deleted, but a
    later insert must not inherit its position under the cursor.

    Old behaviour pinned ``MAX(rowid)``: deleting the top row let the next
    insert reuse that rowid, so an event sorting after the cursor key would
    show up on a later page of the snapshot. The persistent AUTOINCREMENT
    ``seq`` never reuses the value, so the new event stays above the mark.
    """
    register_permissive_schema(client)
    for i in range(4):
        submit_payload(client, f"e{i}", f"2026-09-20T10:00:0{i}Z", {"v": i})

    db = client.app.state.db_path

    def seq_of(event_id):
        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT seq FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return None if row is None else row[0]

    assert seq_of("e3") == 4  # current maximum sequence value

    first = search(client, limit=2)
    assert ids(first) == ["e0", "e1"]
    cursor = first.json()["next_cursor"]
    assert cursor is not None

    # Cleanup confirmation deletes the event holding the pinned maximum seq.
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM events WHERE event_id = 'e3'")
        conn.commit()

    # A new event sorts well AFTER the cursor's keyset anchor (e1). Under
    # rowid reuse it would inherit rowid 4 <= high-water and leak onto page 2.
    submit_payload(client, "zz", "2026-09-20T12:00:00Z", {"v": 99})

    # seq is not reused: zz takes 5 even though 4 was the deleted maximum.
    assert seq_of("e3") is None
    assert seq_of("zz") == 5

    second = search(client, limit=10, cursor=cursor)
    assert ids(second) == ["e2"]  # zz must not appear
    assert second.json()["next_cursor"] is None

    # A fresh first page sees the post-cleanup state, zz included.
    fresh = search(client)
    assert set(ids(fresh)) == {"e0", "e1", "e2", "zz"}


def test_seq_is_unique_and_strictly_increasing_across_concurrent_writes(client):
    """Concurrent inserts must receive unique, strictly increasing seq values."""
    import asyncio

    import httpx

    register_permissive_schema(client)

    async def burst():
        transport = httpx.ASGITransport(app=client.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            async def write(index):
                # Scatter timestamps on both sides of any snapshot point.
                hour = 8 + (index % 6)
                return await http.post(
                    "/events",
                    json={
                        "device_id": "dev-c",
                        "event_id": f"c{index:03d}",
                        "occurred_at": f"2026-09-20T{hour:02d}:{index % 60:02d}:00Z",
                    },
                )

            return await asyncio.gather(*(write(i) for i in range(30)))

    responses = asyncio.run(burst())
    assert all(r.status_code in (200, 201) for r in responses), [
        (r.status_code, r.text) for r in responses if r.status_code not in (200, 201)
    ]

    db = client.app.state.db_path
    with sqlite3.connect(db) as conn:
        seqs = [row[0] for row in conn.execute("SELECT seq FROM events").fetchall()]
        counter = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'events'"
        ).fetchone()[0]

    assert len(seqs) == 30
    assert len(set(seqs)) == 30  # unique
    assert sorted(seqs) == list(range(1, 31))  # strictly increasing from 1
    assert counter == 30  # durable high-water, not derived from live rows


def test_snapshot_excludes_concurrent_late_writes_anywhere_in_order(client):
    """Writes landing while pages are walked never enter the snapshot, even
    when their sort position falls between existing pages."""
    import asyncio

    import httpx

    register_permissive_schema(client)
    # 5 in-window rows, inserted out of order.
    for eid, ts in [
        ("a0", "2026-09-20T10:00:00Z"),
        ("a2", "2026-09-20T10:00:02Z"),
        ("a4", "2026-09-20T10:00:04Z"),
        ("a1", "2026-09-20T10:00:01Z"),
        ("a3", "2026-09-20T10:00:03Z"),
    ]:
        submit_payload(client, eid, ts, {"v": 1})

    first = search(client, limit=2)
    assert ids(first) == ["a0", "a1"]
    cursor = first.json()["next_cursor"]

    async def late_writes():
        transport = httpx.ASGITransport(app=client.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            async def write(eid, minute, second):
                return await http.post(
                    "/events",
                    json={
                        "device_id": "dev-1",
                        "event_id": eid,
                        "occurred_at": (
                            f"2026-09-20T10:{minute:02d}:{second:02d}Z"
                        ),
                        "event_type": "telemetry",
                        "schema_version": "1",
                        "payload": {"v": 1},
                    },
                )

            # Positions before the anchor, between future pages and after.
            return await asyncio.gather(
                write("late-before", 0, 0),  # tie ordering varies; excluded either way
                write("late-mid", 0, 2),
                write("late-after", 5, 0),
            )

    responses = asyncio.run(late_writes())
    assert all(r.status_code in (200, 201) for r in responses), [
        r.text for r in responses
    ]

    seen = ids(first)
    while cursor is not None:
        page = search(client, limit=2, cursor=cursor)
        assert page.status_code == 200, page.text
        seen.extend(ids(page))
        cursor = page.json()["next_cursor"]

    assert seen == ["a0", "a1", "a2", "a3", "a4"]
    assert not any(eid.startswith("late-") for eid in seen)


# ----------------------------------------------------------------- cursors

def test_cursor_rejects_changed_conditions(client):
    register_permissive_schema(client)
    for i in range(4):
        submit_payload(client, f"e{i}", f"2026-09-20T10:00:0{i}Z", {"v": i})

    cursor = search(client, device_id="dev-1", limit=2).json()["next_cursor"]

    # Different device filter -> 422.
    resp = search(client, device_id="dev-2", limit=2, cursor=cursor)
    assert resp.status_code == 422
    # Different condition -> 422.
    resp = search(
        client,
        limit=2,
        cursor=cursor,
        conditions=[{"pointer": "/v", "operator": "gte", "value": 0}],
    )
    assert resp.status_code == 422
    # Different time window -> 422.
    resp = search(
        client,
        limit=2,
        cursor=cursor,
        occurred_at={"start": "2026-09-20T10:00:02Z", "end": None},
    )
    assert resp.status_code == 422
    # Changing only limit is allowed and the walk completes.
    resp = search(client, device_id="dev-1", limit=3, cursor=cursor)
    assert resp.status_code == 200
    assert ids(resp) == ["e2", "e3"]


def test_cursor_bound_to_canonical_equivalent_query(client):
    register_permissive_schema(client)
    for i in range(3):
        submit_payload(client, f"e{i}", f"2026-09-20T10:00:0{i}Z", {"v": i})

    cursor = search(
        client,
        occurred_at={
            "start": "2026-09-20T12:00:00+02:00",
            "end": "2026-09-20T12:00:03+02:00",
        },
        limit=2,
    ).json()["next_cursor"]

    # Same instants expressed in Z -> still the same normalized query.
    resp = search(
        client,
        occurred_at={
            "start": "2026-09-20T10:00:00Z",
            "end": "2026-09-20T10:00:03Z",
        },
        limit=2,
        cursor=cursor,
    )
    assert resp.status_code == 200
    assert ids(resp) == ["e2"]


@pytest.mark.parametrize(
    "bad",
    ["", "garbage", "evc1:garbage", "evc1:.abcdef0123456789", "evc1:eyJhIjoxfQ.deadbeefdeadbeefdeadbeefdeadbeef"],
)
def test_malformed_cursor_is_422(client, bad):
    resp = search(client, cursor=bad)
    assert resp.status_code == 422


def test_tampered_cursor_signature_is_422(client):
    register_permissive_schema(client)
    submit_payload(client, "e0", "2026-09-20T10:00:00Z", {"v": 0})
    submit_payload(client, "e1", "2026-09-20T10:00:01Z", {"v": 1})
    cursor = search(client, limit=1).json()["next_cursor"]
    assert cursor.startswith("evc2:")
    prefix, packed = cursor.split("evc2:", 1)
    payload, sig = packed.rsplit(".", 1)
    tampered = "evc2:" + payload + "." + ("f" * len(sig))
    assert search(client, limit=1, cursor=tampered).status_code == 422
    # Flip a payload byte as well.
    flipped = payload[:-2] + ("aa" if payload[-2:] != "aa" else "bb")
    assert search(client, limit=1, cursor="evc2:" + flipped + "." + sig).status_code == 422


def test_v1_rowid_cursor_is_rejected_with_clear_422(client):
    """A genuinely signed legacy (evc1) cursor must be rejected, not decoded."""
    from app import search as search_module
    from app.search import Cursor, encode_cursor

    register_permissive_schema(client)
    for i in range(3):
        submit_payload(client, f"e{i}", f"2026-09-20T10:00:0{i}Z", {"v": i})

    query = SearchQuery(
        device_id=None,
        event_type=None,
        schema_version=None,
        occurred_start=None,
        occurred_end=None,
        conditions=(),
    )
    fingerprint = query_fingerprint(query)
    # Forge a well-formed v1 token with the real HMAC key, as the old code
    # would have produced it.
    old_cursor = Cursor(
        version=1,
        fingerprint=fingerprint,
        high_water=2,
        occurred_at="2026-09-20T10:00:00+00:00",
        event_id="e0",
    )
    original_prefix = search_module.CURSOR_PREFIX
    original_version = search_module.CURSOR_VERSION
    try:
        search_module.CURSOR_PREFIX = search_module.LEGACY_CURSOR_PREFIX
        search_module.CURSOR_VERSION = 1
        legacy_token = encode_cursor(old_cursor)
    finally:
        search_module.CURSOR_PREFIX = original_prefix
        search_module.CURSOR_VERSION = original_version
    assert legacy_token.startswith("evc1:")

    resp = search(client, cursor=legacy_token)
    assert resp.status_code == 422
    message = resp.json()["detail"]["message"]
    assert "older version" in message or "incompatible" in message

    # A fresh first page, of course, still works and emits v2 cursors.
    fresh = search(client, limit=2)
    assert fresh.status_code == 200
    assert fresh.json()["next_cursor"].startswith("evc2:")


def test_empty_result_first_page_has_no_cursor(client):
    resp = search(client, device_id="nobody")
    assert resp.status_code == 200
    assert resp.json() == {"events": [], "next_cursor": None, "limit": 50}

"""Spec 1 acceptance tests. All offline, against responses saved in tests/fixtures/."""
import datetime as dt
import json

import pandas as pd
import pytest
import requests

from pipeline import _common
from pipeline import pull_eia_storage as ps

PULLED_AT = dt.datetime(2026, 11, 14, 15, 0, tzinfo=dt.timezone.utc)


# ---------- API history ----------

def test_api_sample_parses_to_long_format(fixture_json):
    rows = fixture_json("eia_stor_wkly_data_sample.json")["response"]["data"]
    df = ps.parse_api(rows, PULLED_AT, ps.API_URL)
    assert list(df.columns) == ["series_id", "period", "value", "units", "pulled_at", "source_url"]
    assert set(df.series_id) == {"NW2_EPG0_SWO_R48_BCF", "NW2_EPG0_SSO_R33_BCF"}
    assert all(isinstance(p, dt.date) and not isinstance(p, dt.datetime) for p in df.period)
    assert df.value.dtype == float
    assert (df.pulled_at == pd.Timestamp(PULLED_AT)).all()


def test_api_sample_passes_checks_for_its_series(fixture_json):
    rows = fixture_json("eia_stor_wkly_data_sample.json")["response"]["data"]
    df = ps.parse_api(rows, PULLED_AT, ps.API_URL)
    assert ps.check_storage(df, expected={"NW2_EPG0_SWO_R48_BCF", "NW2_EPG0_SSO_R33_BCF"}) == []


def test_api_sample_spot_value(fixture_json):
    """Hand-copied from the fixture: Lower 48 on 2026-11-06 was 3862 Bcf."""
    rows = fixture_json("eia_stor_wkly_data_sample.json")["response"]["data"]
    df = ps.parse_api(rows, PULLED_AT, ps.API_URL)
    v = df[(df.series_id == "NW2_EPG0_SWO_R48_BCF") & (df.period == dt.date(2026, 11, 6))].value
    assert v.tolist() == [3862.0]


def test_facet_listing_matches_spec_series(fixture_json):
    facets = fixture_json("eia_stor_wkly_facet_series.json")["response"]["facets"]
    assert {f["id"] for f in facets} == set(ps.SERIES)


def _page(rows, total):
    class R:
        def json(self):
            return {"response": {"total": str(total), "data": rows}}
    return R()


def test_paging_collects_every_row():
    pages = [_page([{"n": i} for i in range(5000)], 7000), _page([{"n": i} for i in range(2000)], 7000)]
    calls = []

    def fake_get(url, params=None):
        calls.append(dict(params))
        return pages[len(calls) - 1]

    rows = ps.fetch_api_rows(dt.date(2010, 1, 1), None, "KEY", get=fake_get)
    assert len(rows) == 7000
    assert [c["offset"] for c in calls] == [0, 5000]


def test_paging_raises_on_short_return():
    def fake_get(url, params=None):
        return _page([{"n": 1}] if params["offset"] == 0 else [], 10)

    with pytest.raises(_common.PullError, match="expected 10 rows, got 1"):
        ps.fetch_api_rows(dt.date(2010, 1, 1), None, "KEY", get=fake_get)


def test_request_lists_every_series_once():
    seen = {}

    def fake_get(url, params=None):
        seen["params"] = params
        return _page([], 0)

    ps.fetch_api_rows(dt.date(2010, 1, 1), dt.date(2010, 2, 1), "KEY", get=fake_get)
    facets = [v for k, v in seen["params"].items() if k.startswith("facets")]
    assert sorted(facets[0]) == sorted(ps.SERIES)


# ---------- Release-day JSON ----------

def test_release_ids_map_to_api_ids():
    assert ps.release_id_to_api("png.nw2_epg0_swo_r48_bcf.w") == "NW2_EPG0_SWO_R48_BCF"
    with pytest.raises(_common.PullError):
        ps.release_id_to_api("something_else")


def test_release_parses_levels_and_published_changes(fixture_json):
    df = ps.parse_release(fixture_json("eia_wngsr_release.json"), PULLED_AT)
    levels = df[~df.series_id.str.contains(":")]
    assert set(levels.series_id) == set(ps.SERIES)
    assert set(levels.period) == {dt.date(2026, 11, 6), dt.date(2026, 10, 30)}
    published = df[df.series_id.str.contains(":")]
    assert set(published.period) == {dt.date(2026, 11, 6)}
    assert {s.split(":")[1] for s in published.series_id} == {"year_ago", "net_change", "implied_flow"}
    l48 = published.set_index("series_id").value
    assert l48["NW2_EPG0_SWO_R48_BCF:year_ago"] == 3954
    # Hand-copied from the fixture (matches the EIA report for the week ending Nov 6).
    assert l48["NW2_EPG0_SWO_R48_BCF:net_change"] == 61
    assert l48["NW2_EPG0_SWO_R48_BCF:implied_flow"] == 61


def test_release_regional_sums_within_rounding(fixture_json):
    df = ps.parse_release(fixture_json("eia_wngsr_release.json"), PULLED_AT)
    assert ps.check_storage(df, check_gaps=False) == []


def test_release_decodes_byte_order_mark(fixture_json):
    body = "﻿" + json.dumps(fixture_json("eia_wngsr_release.json"))

    class R:
        content = body.encode("utf-8")

    doc = ps.fetch_release(get=lambda url, params=None: R())
    assert doc["current_week"] == "2026-11-06"


def test_poll_waits_for_the_new_week(fixture_json, raw_dir):
    new = fixture_json("eia_wngsr_release.json")
    old = dict(new, current_week="2026-10-30")
    responses = iter([old, old, new])
    sleeps = []
    df = ps.poll_release(previous_week=dt.date(2026, 10, 30), fetch=lambda: next(responses),
                         sleep=sleeps.append, interval=20, max_minutes=5)
    assert sleeps == [20, 20]
    assert (df.period.max()) == dt.date(2026, 11, 6)
    assert len(list(raw_dir.glob("eia_storage_release/*/*.parquet"))) == 1


def test_poll_gives_up_loudly(fixture_json, raw_dir):
    old = dict(fixture_json("eia_wngsr_release.json"), current_week="2026-10-30")
    with pytest.raises(_common.PullError, match="did not advance"):
        ps.poll_release(previous_week=dt.date(2026, 10, 30), fetch=lambda: old,
                        sleep=lambda s: None, interval=20, max_minutes=1)
    assert not raw_dir.exists()


# ---------- Checks catch bad data ----------

def _frame(rows):
    return pd.DataFrame(rows, columns=["series_id", "period", "value"])


def test_check_flags_non_friday_duplicates_and_gaps():
    s = "NW2_EPG0_SWO_R48_BCF"
    df = _frame([(s, dt.date(2026, 10, 23), 1.0), (s, dt.date(2026, 10, 23), 1.0),
                 (s, dt.date(2026, 11, 6), 1.0), (s, dt.date(2026, 11, 7), 1.0)])
    problems = " | ".join(ps.check_storage(df, expected={s}))
    assert "duplicate" in problems and "not a Friday" in problems and "gap" in problems


def test_check_flags_missing_series_and_bad_sums(fixture_json):
    df = ps.parse_release(fixture_json("eia_wngsr_release.json"), PULLED_AT)
    bumped = df.copy()
    bumped.loc[bumped.series_id == "NW2_EPG0_SWO_R31_BCF", "value"] += 10
    assert any("regions sum" in p for p in ps.check_storage(bumped, check_gaps=False))
    missing = df[df.series_id != "NW2_EPG0_SWO_R35_BCF"]
    assert any("missing series" in p for p in ps.check_storage(missing, check_gaps=False))


# ---------- Plumbing ----------

def test_raw_files_are_never_overwritten(raw_dir):
    df = _frame([("x", dt.date(2026, 11, 6), 1.0)])
    p = _common.write_raw(df, "eia_storage", "storage_api", PULLED_AT)
    assert p.exists()
    with pytest.raises(_common.PullError, match="already exists"):
        _common.write_raw(df, "eia_storage", "storage_api", PULLED_AT)


def test_http_errors_never_show_the_key(monkeypatch):
    monkeypatch.setattr(_common.time, "sleep", lambda s: None)

    class S:
        def get(self, url, params=None, timeout=None):
            raise requests.ConnectionError(f"failed {url}?api_key=SECRET123")

    with pytest.raises(_common.PullError) as e:
        _common.http_get("https://api.eia.gov/v2/x", params={"api_key": "SECRET123"}, session=S())
    assert "SECRET123" not in str(e.value)

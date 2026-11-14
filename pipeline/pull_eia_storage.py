"""Spec 1: EIA weekly working gas in storage, Lower 48 and regions.

Two sources:
  * EIA API v2 for history and routine pulls (`pull`).
  * The release-day JSON on ir.eia.gov for the Thursday 10:30 ET print (`poll_release`).

Usage:
  python -m pipeline.pull_eia_storage                  # backfill on first run, else last 8 weeks
  python -m pipeline.pull_eia_storage --start 2024-01-01
  python -m pipeline.pull_eia_storage --release        # poll the release JSON until the week advances
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from urllib.parse import urlencode

import pandas as pd

from pipeline import _common
from pipeline._common import PullError, env, http_get, now_utc, write_raw

SOURCE = "eia_storage"
RELEASE_SOURCE = "eia_storage_release"
API_URL = "https://api.eia.gov/v2/natural-gas/stor/wkly/data/"
RELEASE_URL = "https://ir.eia.gov/ngs/wngsr.json"

SERIES = {
    "NW2_EPG0_SWO_R48_BCF": "Lower 48 total",
    "NW2_EPG0_SWO_R31_BCF": "East",
    "NW2_EPG0_SWO_R32_BCF": "Midwest",
    "NW2_EPG0_SWO_R33_BCF": "South Central",
    "NW2_EPG0_SSO_R33_BCF": "South Central salt",
    "NW2_EPG0_SNO_R33_BCF": "South Central nonsalt",
    "NW2_EPG0_SWO_R34_BCF": "Mountain",
    "NW2_EPG0_SWO_R35_BCF": "Pacific",
}
L48, SC, SALT, NONSALT = "NW2_EPG0_SWO_R48_BCF", "NW2_EPG0_SWO_R33_BCF", "NW2_EPG0_SSO_R33_BCF", "NW2_EPG0_SNO_R33_BCF"
REGIONS = ["NW2_EPG0_SWO_R31_BCF", "NW2_EPG0_SWO_R32_BCF", SC, "NW2_EPG0_SWO_R34_BCF", "NW2_EPG0_SWO_R35_BCF"]
PUBLISHED_FIELDS = ("net_change", "implied_flow")

HISTORY_START = dt.date(2010, 1, 1)
LOOKBACK = dt.timedelta(weeks=8)
PAGE = 5000
COLUMNS = ["series_id", "period", "value", "units", "pulled_at", "source_url"]


# ---------- API ----------

def fetch_api_rows(start: dt.date, end: dt.date | None, key: str, get=http_get) -> list[dict]:
    """Every row for the eight series between start and end, paging past the 5,000-row cap."""
    params = {
        "api_key": key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": list(SERIES),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "start": start.isoformat(),
        "length": PAGE,
        "offset": 0,
    }
    if end:
        params["end"] = end.isoformat()
    rows: list[dict] = []
    total = None
    while True:
        response = get(API_URL, params=dict(params)).json()["response"]
        total = int(response["total"])
        batch = response["data"]
        rows.extend(batch)
        params["offset"] += len(batch)
        if not batch or params["offset"] >= total:
            break
    if len(rows) != total:
        raise PullError(f"EIA storage API: expected {total} rows, got {len(rows)}")
    return rows


def parse_api(rows: list[dict], pulled_at: dt.datetime, source_url: str) -> pd.DataFrame:
    df = pd.DataFrame({
        "series_id": [r["series"] for r in rows],
        "period": [dt.date.fromisoformat(r["period"]) for r in rows],
        "value": pd.to_numeric(pd.Series([r["value"] for r in rows], dtype=object), errors="raise").astype(float),
        "units": [r["units"] for r in rows],
    })
    df["pulled_at"] = pd.Timestamp(pulled_at)
    df["source_url"] = source_url
    return df[COLUMNS]


def pull(start: dt.date | None = None, end: dt.date | None = None, write: bool = True) -> pd.DataFrame:
    """Pull from the API. First run backfills from 2010; later runs re-pull the last eight weeks."""
    pulled_at = now_utc()
    if start is None:
        start = HISTORY_START if not _common.raw_files(SOURCE) else pulled_at.date() - LOOKBACK
    rows = fetch_api_rows(start, end, env("EIA_API_KEY"))
    query = {"frequency": "weekly", "start": start.isoformat(), **({"end": end.isoformat()} if end else {})}
    df = parse_api(rows, pulled_at, f"{API_URL}?{urlencode(query)}")
    problems = check_storage(df)
    if problems:
        raise PullError("EIA storage pull failed its checks; nothing written:\n  " + "\n  ".join(problems))
    if write:
        path = write_raw(df, SOURCE, "storage_api", pulled_at)
        print(f"wrote {len(df):,} rows to {path}")
    return df


# ---------- Release-day JSON ----------

def release_id_to_api(release_id: str) -> str:
    """'png.nw2_epg0_swo_r48_bcf.w' -> 'NW2_EPG0_SWO_R48_BCF'."""
    s = release_id.lower()
    if not (s.startswith("png.") and s.endswith(".w")):
        raise PullError(f"unexpected release series id {release_id!r}")
    return s[len("png."):-len(".w")].upper()


def fetch_release(get=http_get) -> dict:
    # The file starts with a UTF-8 byte-order mark, which json.loads rejects.
    return json.loads(get(RELEASE_URL).content.decode("utf-8-sig"))


def parse_release(doc: dict, pulled_at: dt.datetime) -> pd.DataFrame:
    """Levels for the current and prior week, plus EIA's published year-ago level, net change
    and implied flow.

    The published fields get series ids '<api id>:year_ago', '<api id>:net_change' and
    '<api id>:implied_flow', all on the current week.
    They differ when EIA reclassifies gas, and the model has to know which one it predicts.
    """
    week = dt.date.fromisoformat(doc["current_week"])
    # EIA interpolates the year-ago figure to the report's calendar date, so its date is
    # not a storage week. It is kept as '<api id>:year_ago' on the current week instead.
    year_ago = dt.date.fromisoformat(doc["year_ago"]) if doc.get("year_ago") else None
    rows = []
    for s in doc["series"]:
        sid = release_id_to_api(s["series_id"])
        units = s.get("unitsshort", "bcf").upper()
        for period, value in s["data"]:
            d = dt.date.fromisoformat(period)
            if d == year_ago:
                rows.append((f"{sid}:year_ago", week, float(value), units))
            else:
                rows.append((sid, d, float(value), units))
        calc = s.get("calculated", {})
        for field in PUBLISHED_FIELDS:
            if calc.get(field) is not None:
                rows.append((f"{sid}:{field}", week, float(calc[field]), units))
    df = pd.DataFrame(rows, columns=["series_id", "period", "value", "units"])
    df["pulled_at"] = pd.Timestamp(pulled_at)
    df["source_url"] = RELEASE_URL
    return df[COLUMNS]


def last_release_week() -> dt.date | None:
    files = _common.raw_files(RELEASE_SOURCE)
    if not files:
        return None
    df = pd.read_parquet(files[-1])
    return max(df.period)


def poll_release(previous_week: dt.date | None = None, fetch=fetch_release, sleep=time.sleep,
                 interval: int = 20, max_minutes: int = 5) -> pd.DataFrame:
    """Poll the release JSON until its week advances past the last one stored, then write it."""
    previous_week = previous_week or last_release_week()
    deadline = time.monotonic() + max_minutes * 60
    while True:
        doc = fetch()
        week = dt.date.fromisoformat(doc["current_week"])
        if previous_week is None or week > previous_week:
            pulled_at = now_utc()
            df = parse_release(doc, pulled_at)
            problems = check_storage(df, check_gaps=False)
            if problems:
                raise PullError("release JSON failed its checks; nothing written:\n  " + "\n  ".join(problems))
            path = write_raw(df, RELEASE_SOURCE, "release", pulled_at)
            print(f"week ending {week}: wrote {len(df)} rows to {path}")
            return df
        if time.monotonic() + interval > deadline:
            raise PullError(f"release JSON did not advance past {previous_week} within {max_minutes} minutes")
        sleep(interval)


# ---------- Checks ----------

def check_storage(df: pd.DataFrame, expected: set[str] | None = None, check_gaps: bool = True) -> list[str]:
    """Spec 1 acceptance checks. Returns a list of problems; empty means clean."""
    expected = set(SERIES) if expected is None else expected
    levels = df[~df.series_id.str.contains(":")]
    problems = []
    missing = expected - set(levels.series_id)
    if missing:
        problems.append(f"missing series: {sorted(missing)}")
    dupes = levels.duplicated(["series_id", "period"])
    if dupes.any():
        problems.append(f"{dupes.sum()} duplicate (series_id, period) rows")
    periods = pd.to_datetime(levels.period)
    not_friday = sorted({d.date() for d in periods[periods.dt.day_of_week != 4]})
    if not_friday:
        problems.append(f"periods not a Friday: {not_friday[:5]}")
    if check_gaps:
        for sid, g in levels.drop_duplicates(["series_id", "period"]).groupby("series_id"):
            steps = pd.to_datetime(g.period).sort_values().diff().dropna()
            bad = steps[steps != pd.Timedelta(days=7)]
            if len(bad):
                problems.append(f"{sid}: {len(bad)} gap(s) between consecutive weeks")
    wide = levels.drop_duplicates(["series_id", "period"]).pivot(index="period", columns="series_id", values="value")
    if {SALT, NONSALT, SC} <= set(wide.columns):
        off = (wide[SALT] + wide[NONSALT] - wide[SC]).abs() > 2
        if off.any():
            problems.append(f"salt + nonsalt differs from South Central by more than 2 Bcf on {off.sum()} week(s)")
    if set(REGIONS + [L48]) <= set(wide.columns):
        off = (wide[REGIONS].sum(axis=1) - wide[L48]).abs() > 3
        if off.any():
            problems.append(f"five regions sum differs from Lower 48 by more than 3 Bcf on {off.sum()} week(s)")
    return problems


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--release", action="store_true", help="poll the release-day JSON")
    p.add_argument("--start", type=dt.date.fromisoformat)
    p.add_argument("--end", type=dt.date.fromisoformat)
    args = p.parse_args()
    if args.release:
        poll_release()
    else:
        pull(args.start, args.end)


if __name__ == "__main__":
    main()

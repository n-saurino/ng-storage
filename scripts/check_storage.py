"""Print stored EIA storage data for checking by hand against EIA's pages.

Run after a backfill or a Thursday release:
    python scripts/check_storage.py
    python scripts/check_storage.py --weeks 2014-03-28 2018-01-05

Compare section 1 with the report page  https://ir.eia.gov/ngs/ngs.html
and section 3 with the history table    https://www.eia.gov/dnav/ng/ng_stor_wkly_s1_w.htm
"""
from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import _common  # noqa: E402
from pipeline.pull_eia_storage import L48, RELEASE_SOURCE, SERIES, SOURCE  # noqa: E402

DEFAULT_WEEKS = ["2014-03-28", "2018-01-05", "2025-11-07"]
ORDER = list(SERIES.values())


def latest_file(source: str) -> pd.DataFrame:
    files = _common.raw_files(source)
    if not files:
        sys.exit(f"no raw files for {source}; run the puller first")
    print(f"  ({files[-1].relative_to(_common.ROOT)})")
    return pd.read_parquet(files[-1])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--weeks", nargs="+", default=DEFAULT_WEEKS, help="week-ending Fridays to show from the API backfill")
    args = p.parse_args()

    print("Reading latest files:")
    api = latest_file(SOURCE)
    rel = latest_file(RELEASE_SOURCE)

    base = rel.series_id.str.split(":").str[0]
    rel = rel.assign(region=base.map(SERIES), field=rel.series_id.str.split(":").str[1].fillna("level"))
    week = max(rel.loc[rel.field == "level", "period"])
    table = (rel[rel.period == week]
             .pivot(index="region", columns="field", values="value")
             .reindex(ORDER)[["level", "net_change", "implied_flow", "year_ago"]])
    print(f"\n1. RELEASE, week ending {week}  (check against the report page)")
    print(table.to_string(float_format=lambda v: f"{v:,.0f}"))

    prev = week - dt.timedelta(days=7)
    rel_prev = rel[(rel.field == "level") & (rel.period == prev)].set_index("series_id").value
    api_prev = api[api.period == prev].set_index("series_id").value
    diff = (rel_prev - api_prev.reindex(rel_prev.index)).dropna()
    print(f"\n2. CROSS-CHECK, week ending {prev}: release vs API backfill")
    if api_prev.empty:
        print("   API backfill has no data for that week yet.")
    elif (diff != 0).any():
        print("   DIFFERENT (EIA revised, or a pull is wrong):")
        print(diff[diff != 0].rename(index=SERIES).to_string())
    else:
        print(f"   all {len(diff)} series match")

    l48 = api[api.series_id == L48].set_index("period").value.sort_index()
    print("\n3. API BACKFILL, Lower 48  (check against the history table)")
    for w in args.weeks:
        d = dt.date.fromisoformat(w)
        if d not in l48.index:
            print(f"   {d}: not in the backfill (is it a Friday?)")
            continue
        i = l48.index.get_loc(d)
        change = f"{l48.iloc[i] - l48.iloc[i - 1]:+,.0f}" if i > 0 else "n/a"
        print(f"   {d}: {l48.iloc[i]:,.0f} Bcf, weekly change {change}")


if __name__ == "__main__":
    main()

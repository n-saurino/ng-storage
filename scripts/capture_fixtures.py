"""Capture one real response from each data source into tests/fixtures/.

Run from the repo root:
    python3 scripts/capture_fixtures.py            # EIA and NOAA
    python3 scripts/capture_fixtures.py databento  # Databento only (costs a few cents)
Standard library only, so it works before the virtualenv exists.
API keys are read from .env and scrubbed from every saved file.
"""
import base64
import json
import sys
import urllib.error
import urllib.parse
import os
import pathlib
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
FIX = ROOT / "tests" / "fixtures"


def load_key(name):
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found in .env")


KEY = load_key("EIA_API_KEY")
DB_KEY = load_key("DATABENTO_API_KEY")
DB = "https://hist.databento.com/v0"
DB_BASE = {"dataset": "GLBX.MDP3", "symbols": "NG.FUT", "stype_in": "parent"}

# Spec 2b: Databento. Each entry: (filename, method, params). Cost quotes are free;
# the two samples are a couple of days each and cost cents.
DATABENTO_TARGETS = [
    ("databento_cost_statistics_full.json", "metadata.get_cost",
     {**DB_BASE, "schema": "statistics", "start": "2010-06-06", "end": "2026-11-14"}),
    ("databento_cost_definition_full.json", "metadata.get_cost",
     {**DB_BASE, "schema": "definition", "start": "2010-06-06", "end": "2026-11-14"}),
    ("databento_statistics_sample.csv", "timeseries.get_range",
     {**DB_BASE, "schema": "statistics", "start": "2026-11-11", "end": "2026-11-13",
      "encoding": "csv", "pretty_px": "true", "pretty_ts": "true", "map_symbols": "true"}),
    ("databento_definition_sample.csv", "timeseries.get_range",
     {**DB_BASE, "schema": "definition", "start": "2026-11-12", "end": "2026-11-13",
      "encoding": "csv", "pretty_px": "true", "pretty_ts": "true", "map_symbols": "true"}),
]
API = "https://api.eia.gov/v2"
CPC = "https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted"

TARGETS = {
    # Spec 1: storage
    "eia_wngsr_release.json": "https://ir.eia.gov/ngs/wngsr.json",
    "eia_stor_wkly_facet_series.json": f"{API}/natural-gas/stor/wkly/facet/series?api_key={KEY}",
    "eia_stor_wkly_data_sample.json": (
        f"{API}/natural-gas/stor/wkly/data/?api_key={KEY}&frequency=weekly&data[0]=value"
        "&facets[series][]=NW2_EPG0_SWO_R48_BCF&facets[series][]=NW2_EPG0_SSO_R33_BCF"
        "&sort[0][column]=period&sort[0][direction]=desc&length=10"
    ),
    # Spec 2: prices (unconfirmed route and ids)
    "eia_pri_fut_facet_series.json": f"{API}/natural-gas/pri/fut/facet/series?api_key={KEY}",
    "eia_pri_fut_data_sample.json": (
        f"{API}/natural-gas/pri/fut/data/?api_key={KEY}&frequency=daily&data[0]=value"
        "&sort[0][column]=period&sort[0][direction]=desc&length=20"
    ),
    "eia_pri_fut_RNGC1_latest.json": (
        f"{API}/natural-gas/pri/fut/data/?api_key={KEY}&frequency=daily&data[0]=value"
        "&facets[series][]=RNGC1&sort[0][column]=period&sort[0][direction]=desc&length=10"
    ),
    # Spec 3: degree days (observed path unconfirmed)
    "cpc_daily_data_2026_index.html": f"{CPC}/daily_data/2026/",
    "cpc_weighted_index.html": f"{CPC}/",
    "cpc_daily_data_index.html": f"{CPC}/daily_data/",
    "cpc_forecast_2026_11_13_index.html": f"{CPC}/daily_forecasts_7day/2026/11/13/",
    "cpc_forecast_UtilityGas.Heating.txt": f"{CPC}/daily_forecasts_7day/2026/11/13/UtilityGas.Heating.txt",
}


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ng-storage fixture capture"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8-sig", errors="replace")


def scrub(text):
    return text.replace(KEY, "REDACTED").replace(DB_KEY, "REDACTED")


def db_fetch(method, params):
    auth = base64.b64encode(f"{DB_KEY}:".encode()).decode()
    req = urllib.request.Request(
        f"{DB}/{method}",
        data=urllib.parse.urlencode(params).encode(),
        headers={"Authorization": f"Basic {auth}", "User-Agent": "ng-storage fixture capture"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read().decode("utf-8-sig", errors="replace")
    except urllib.error.HTTPError as e:  # Databento explains errors in the body
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode(errors='replace')[:500]}")


def capture_databento():
    for name, method, params in DATABENTO_TARGETS:
        try:
            status, body = db_fetch(method, params)
            body = scrub(body)
            (FIX / name).write_text(body)
            preview = body.strip() if method == "metadata.get_cost" else f"{body.count(chr(10)):,} lines"
            print(f"OK   {status}  {name}  ({preview})")
        except Exception as e:
            print(f"FAIL       {name}  {type(e).__name__}: {scrub(str(e))}")


def main():
    FIX.mkdir(parents=True, exist_ok=True)
    if sys.argv[1:] == ["databento"]:
        capture_databento()
        check_leaks()
        return
    for name, url in TARGETS.items():
        try:
            status, body = fetch(url)
            body = scrub(body)
            if name.endswith(".json"):
                body = json.dumps(json.loads(body), indent=2)
            (FIX / name).write_text(body)
            print(f"OK   {status}  {name}  ({len(body):,} chars)")
        except Exception as e:  # report and keep going; a failure is information
            print(f"FAIL       {name}  {type(e).__name__}: {scrub(str(e))}")
    check_leaks()


def check_leaks():
    leaked = [p.name for p in FIX.iterdir()
              if p.is_file() and any(k in p.read_text(errors="ignore") for k in (KEY, DB_KEY))]
    print("Key check:", "LEAK in " + ", ".join(leaked) if leaked else "no key in any fixture")


if __name__ == "__main__":
    main()

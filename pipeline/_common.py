"""Shared plumbing for the pullers: secrets, HTTP with retries, append-only raw writes."""
from __future__ import annotations

import datetime as dt
import os
import pathlib
import re
import time

import pandas as pd
import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
TIMEOUT = 30
RETRIES = 3

_KEY_PATTERN = re.compile(r"(api_key=)[^&\s'\"]+")


class PullError(RuntimeError):
    """A pull failed or returned data that fails its checks. Never swallowed."""


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def env(name: str) -> str:
    """Read a secret from the environment, falling back to the repo's .env file."""
    if os.environ.get(name):
        return os.environ[name]
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip()
    raise PullError(f"{name} is not set in the environment or .env")


def redact(text: str, *secrets: str) -> str:
    text = _KEY_PATTERN.sub(r"\1REDACTED", text)
    for s in secrets:
        if s:
            text = text.replace(s, "REDACTED")
    return text


def http_get(url: str, params: dict | None = None, session=None) -> requests.Response:
    """GET with a 30-second timeout and three retries with exponential backoff.

    Client errors (4xx other than 429) fail at once; they will not fix themselves.
    """
    client = session or requests
    secrets = [str(v) for k, v in (params or {}).items() if "key" in k.lower()]
    last: Exception | None = None
    for attempt in range(RETRIES + 1):
        try:
            r = client.get(url, params=params, timeout=TIMEOUT)
            if 400 <= r.status_code < 500 and r.status_code != 429:
                raise PullError(f"HTTP {r.status_code} from {url}: {r.text[:300]}")
            r.raise_for_status()
            return r
        except PullError as e:
            raise PullError(redact(str(e), *secrets)) from None
        except requests.RequestException as e:
            last = e
            if attempt < RETRIES:
                time.sleep(2 ** attempt)
    raise PullError(redact(f"GET {url} failed after {RETRIES + 1} attempts: {last}", *secrets))


def write_raw(df: pd.DataFrame, source: str, name: str, pulled_at: dt.datetime) -> pathlib.Path:
    """Write one pull to data/raw/<source>/<pull_date>/<name>_<HHMMSS>.parquet. Never overwrites."""
    path = RAW / source / pulled_at.date().isoformat() / f"{name}_{pulled_at:%H%M%S}.parquet"
    if path.exists():
        raise PullError(f"{path} already exists; raw files are append-only")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def raw_files(source: str) -> list[pathlib.Path]:
    return sorted((RAW / source).glob("*/*.parquet"))

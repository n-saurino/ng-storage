"""Live checks against EIA. Skipped unless RUN_LIVE=1. Writes nothing."""
import os

import pytest

from pipeline import pull_eia_storage as ps

pytestmark = [pytest.mark.live,
              pytest.mark.skipif(os.environ.get("RUN_LIVE") != "1", reason="set RUN_LIVE=1")]


def test_live_full_history_passes_checks():
    df = ps.pull(start=ps.HISTORY_START, write=False)
    assert ps.check_storage(df) == []
    assert df.period.min().year == 2010


def test_live_release_parses_and_passes_checks():
    df = ps.parse_release(ps.fetch_release(), ps.now_utc())
    assert ps.check_storage(df, check_gaps=False) == []

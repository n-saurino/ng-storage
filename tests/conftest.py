import json
import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def fixture_json():
    def load(name):
        return json.loads((FIXTURES / name).read_text(encoding="utf-8-sig"))
    return load


@pytest.fixture
def raw_dir(tmp_path, monkeypatch):
    """Point every raw-file write and read at a temporary folder."""
    from pipeline import _common
    monkeypatch.setattr(_common, "RAW", tmp_path / "raw")
    return tmp_path / "raw"

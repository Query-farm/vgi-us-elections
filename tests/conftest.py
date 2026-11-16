"""Shared fixtures: the offline mock of every upstream, and per-test options pointing at it."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from us_elections_worker.common import Options, options_from

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures"


@pytest.fixture(scope="session")
def mock_api() -> Iterator[str]:
    """Start test/mock_api/server.py on a free port and return its base URL."""
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_api" / "server.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture
def opts(mock_api: str, tmp_path: Path) -> Options:
    """Options aimed at the mock server with a fresh cache directory."""
    return options_from(
        {
            "CACHE_DIR": str(tmp_path / "cache"),
            "DATAVERSE_URL": f"{mock_api}/dataverse",
            "LEWIS_BASE_URL": f"{mock_api}/lewis",
            "CENSUS_CD_BASE_URL": f"{mock_api}/census",
            "FEC_BASE_URL": f"{mock_api}/fec/v1",
        }
    )

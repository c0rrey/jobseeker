"""
Tests for pipeline/src/enrichment/*.

Covers active enrichment modules using mocked HTTP responses so that no
real network calls are made during the test suite. Each module's enrich()
function is tested for:

- Success path: HTTP 200 with valid payload -> True, DB row updated
- HTTP error: enrich() returns False and logs a warning
- Network error: enrich() returns False and logs a warning
- Empty API response: enrich() returns False and logs a warning
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.src.database import init_db
from pipeline.src.enrichment.levelsfy import enrich as levelsfy_enrich


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return an open SQLite connection with the V2 schema and a test company row."""
    path = tmp_path / "test.db"
    init_db(path)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("INSERT INTO companies (name) VALUES ('Acme Corp');")
    conn.commit()
    return conn


def _get_company(conn: sqlite3.Connection, name: str) -> sqlite3.Row | None:
    """Fetch a company row by name."""
    return conn.execute(
        "SELECT * FROM companies WHERE name = ?", (name,)
    ).fetchone()


# ---------------------------------------------------------------------------
# Helpers: mock response factories
# ---------------------------------------------------------------------------


def _mock_response(
    json_data: Any = None,
    status_code: int = 200,
    raise_for_status: bool = False,
) -> MagicMock:
    """Build a mock requests.Response object.

    Args:
        json_data: Data returned by response.json().
        status_code: HTTP status code.
        raise_for_status: If True, raise_for_status() raises HTTPError.

    Returns:
        Configured MagicMock.
    """
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data or {}
    if raise_for_status:
        http_error = requests.exceptions.HTTPError(response=mock)
        mock.raise_for_status.side_effect = http_error
    else:
        mock.raise_for_status.return_value = None
    return mock


# ===========================================================================
# Levels.fyi
# ===========================================================================


class TestLevelsfyEnrich:
    """Tests for pipeline.src.enrichment.levelsfy.enrich."""

    _VALID_RESPONSE = {
        "medianTotalComp": 250000,
        "medianBaseSalary": 180000,
        "sampleSize": 42,
        "levels": [
            {"level": "L4", "medianTotalComp": 220000},
            {"level": "L5", "medianTotalComp": 310000},
        ],
    }

    def test_success(self, db_conn: sqlite3.Connection) -> None:
        """enrich() returns True and writes comp data to crunchbase_data."""
        mock_resp = _mock_response(json_data=self._VALID_RESPONSE)

        with patch("pipeline.src.enrichment.levelsfy.requests.get", return_value=mock_resp):
            result = levelsfy_enrich(1, "Acme Corp", db_conn)

        assert result is True
        row = _get_company(db_conn, "Acme Corp")
        assert row is not None
        stored = json.loads(row["crunchbase_data"])
        levelsfy_data = stored["levelsfy"]
        assert levelsfy_data["source"] == "levels.fyi"
        assert levelsfy_data["median_total_comp"] == 250000
        assert levelsfy_data["sample_size"] == 42
        assert len(levelsfy_data["levels"]) == 2

    def test_http_error_returns_false(self, db_conn: sqlite3.Connection) -> None:
        """enrich() returns False on HTTP error."""
        mock_resp = _mock_response(status_code=404, raise_for_status=True)

        with patch("pipeline.src.enrichment.levelsfy.requests.get", return_value=mock_resp):
            result = levelsfy_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_network_error_returns_false(self, db_conn: sqlite3.Connection) -> None:
        """enrich() returns False on network error."""
        with patch(
            "pipeline.src.enrichment.levelsfy.requests.get",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            result = levelsfy_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_empty_response_returns_false(self, db_conn: sqlite3.Connection) -> None:
        """enrich() returns False when the API returns an empty body."""
        mock_resp = _mock_response(json_data={})

        with patch("pipeline.src.enrichment.levelsfy.requests.get", return_value=mock_resp):
            result = levelsfy_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_http_error_logs_warning(
        self, db_conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
    ) -> None:
        """enrich() emits a logging.WARNING on HTTP failure."""
        import logging

        mock_resp = _mock_response(status_code=503, raise_for_status=True)

        with patch("pipeline.src.enrichment.levelsfy.requests.get", return_value=mock_resp):
            with caplog.at_level(logging.WARNING, logger="pipeline.src.enrichment.levelsfy"):
                levelsfy_enrich(1, "Acme Corp", db_conn)

        assert len(caplog.records) > 0



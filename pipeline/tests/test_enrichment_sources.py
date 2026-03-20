"""
Tests for pipeline/src/enrichment/*.

Covers all four enrichment modules using mocked HTTP responses so that no
real network calls are made during the test suite. Each module's enrich()
function is tested for:

- Success path: HTTP 200 with valid payload -> True, DB row updated
- Missing API key: enrich() returns False and logs a warning (crunchbase,
  glassdoor, stackshare)
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
from pipeline.src.enrichment.crunchbase import enrich as crunchbase_enrich
from pipeline.src.enrichment.glassdoor import enrich as glassdoor_enrich
from pipeline.src.enrichment.levelsfy import enrich as levelsfy_enrich
from pipeline.src.enrichment.stackshare import enrich as stackshare_enrich


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
# Crunchbase
# ===========================================================================


class TestCrunchbaseEnrich:
    """Tests for pipeline.src.enrichment.crunchbase.enrich."""

    _VALID_RESPONSE = {
        "properties": {
            "short_description": "Cloud platform company",
            "num_employees_enum": "c_01001_05000",
            "funding_total": {"value": 50_000_000, "currency": "USD"},
            "last_funding_type": "series_b",
            "categories": {
                "entities": [
                    {"properties": {"name": "Cloud Computing"}}
                ]
            },
        }
    }

    def test_success(self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch) -> None:
        """enrich() returns True and writes size_range, industry, funding_stage."""
        monkeypatch.setenv("CRUNCHBASE_API_KEY", "test-key")
        mock_resp = _mock_response(json_data=self._VALID_RESPONSE)

        with patch("pipeline.src.enrichment.crunchbase.requests.get", return_value=mock_resp):
            result = crunchbase_enrich(1, "Acme Corp", db_conn)

        assert result is True
        row = _get_company(db_conn, "Acme Corp")
        assert row is not None
        assert row["size_range"] == "1001-5000"
        assert row["industry"] == "Cloud Computing"
        assert row["funding_stage"] == "series_b"
        crunchbase_data = json.loads(row["crunchbase_data"])
        assert crunchbase_data["last_funding_type"] == "series_b"

    def test_missing_api_key_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when CRUNCHBASE_API_KEY is not set."""
        monkeypatch.delenv("CRUNCHBASE_API_KEY", raising=False)
        result = crunchbase_enrich(1, "Acme Corp", db_conn)
        assert result is False

    def test_http_error_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False and logs warning on HTTP error."""
        monkeypatch.setenv("CRUNCHBASE_API_KEY", "test-key")
        mock_resp = _mock_response(status_code=429, raise_for_status=True)

        with patch("pipeline.src.enrichment.crunchbase.requests.get", return_value=mock_resp):
            result = crunchbase_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_network_error_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False and logs warning on network error."""
        monkeypatch.setenv("CRUNCHBASE_API_KEY", "test-key")

        with patch(
            "pipeline.src.enrichment.crunchbase.requests.get",
            side_effect=requests.exceptions.ConnectionError("timeout"),
        ):
            result = crunchbase_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_empty_properties_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when the API returns empty properties."""
        monkeypatch.setenv("CRUNCHBASE_API_KEY", "test-key")
        mock_resp = _mock_response(json_data={"properties": {}})

        with patch("pipeline.src.enrichment.crunchbase.requests.get", return_value=mock_resp):
            result = crunchbase_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_missing_api_key_logs_warning(
        self,
        db_conn: sqlite3.Connection,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """enrich() emits a logging.WARNING when the API key is absent."""
        monkeypatch.delenv("CRUNCHBASE_API_KEY", raising=False)
        import logging

        with caplog.at_level(logging.WARNING, logger="pipeline.src.enrichment.crunchbase"):
            crunchbase_enrich(1, "Acme Corp", db_conn)

        assert any("CRUNCHBASE_API_KEY" in record.message for record in caplog.records)


# ===========================================================================
# Glassdoor
# ===========================================================================


class TestGlassdoorEnrich:
    """Tests for pipeline.src.enrichment.glassdoor.enrich."""

    _VALID_RESPONSE = {
        "response": {
            "employers": [
                {
                    "name": "Acme Corp",
                    "overallRating": "4.2",
                    "featuredReviewUrl": "https://www.glassdoor.com/Reviews/Acme-Reviews.htm",
                    "reviewsUrl": "https://www.glassdoor.com/Reviews/Acme.htm",
                }
            ]
        }
    }

    def test_success(self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch) -> None:
        """enrich() returns True and writes glassdoor_rating and glassdoor_url."""
        monkeypatch.setenv("GLASSDOOR_PARTNER_ID", "pid-123")
        monkeypatch.setenv("GLASSDOOR_API_KEY", "gd-key")
        mock_resp = _mock_response(json_data=self._VALID_RESPONSE)

        with patch("pipeline.src.enrichment.glassdoor.requests.get", return_value=mock_resp):
            result = glassdoor_enrich(1, "Acme Corp", db_conn)

        assert result is True
        row = _get_company(db_conn, "Acme Corp")
        assert row is not None
        assert row["glassdoor_rating"] == pytest.approx(4.2)
        assert "glassdoor.com" in row["glassdoor_url"]

    def test_missing_credentials_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when partner ID or API key is absent."""
        monkeypatch.delenv("GLASSDOOR_PARTNER_ID", raising=False)
        monkeypatch.delenv("GLASSDOOR_API_KEY", raising=False)
        result = glassdoor_enrich(1, "Acme Corp", db_conn)
        assert result is False

    def test_http_error_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False on HTTP error."""
        monkeypatch.setenv("GLASSDOOR_PARTNER_ID", "pid-123")
        monkeypatch.setenv("GLASSDOOR_API_KEY", "gd-key")
        mock_resp = _mock_response(status_code=403, raise_for_status=True)

        with patch("pipeline.src.enrichment.glassdoor.requests.get", return_value=mock_resp):
            result = glassdoor_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_network_error_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False on network error."""
        monkeypatch.setenv("GLASSDOOR_PARTNER_ID", "pid-123")
        monkeypatch.setenv("GLASSDOOR_API_KEY", "gd-key")

        with patch(
            "pipeline.src.enrichment.glassdoor.requests.get",
            side_effect=requests.exceptions.Timeout("timed out"),
        ):
            result = glassdoor_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_empty_employers_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when no employers are returned."""
        monkeypatch.setenv("GLASSDOOR_PARTNER_ID", "pid-123")
        monkeypatch.setenv("GLASSDOOR_API_KEY", "gd-key")
        mock_resp = _mock_response(json_data={"response": {"employers": []}})

        with patch("pipeline.src.enrichment.glassdoor.requests.get", return_value=mock_resp):
            result = glassdoor_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_missing_credentials_logs_warning(
        self,
        db_conn: sqlite3.Connection,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """enrich() emits a logging.WARNING when credentials are absent."""
        monkeypatch.delenv("GLASSDOOR_PARTNER_ID", raising=False)
        monkeypatch.delenv("GLASSDOOR_API_KEY", raising=False)
        import logging

        with caplog.at_level(logging.WARNING, logger="pipeline.src.enrichment.glassdoor"):
            glassdoor_enrich(1, "Acme Corp", db_conn)

        assert len(caplog.records) > 0


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


# ===========================================================================
# StackShare
# ===========================================================================


class TestStackshareEnrich:
    """Tests for pipeline.src.enrichment.stackshare.enrich."""

    _VALID_RESPONSE = {
        "data": {
            "company": {
                "name": "Acme Corp",
                "stackItems": [
                    {
                        "tool": {
                            "name": "Python",
                            "category": {"name": "Languages & Frameworks"},
                        }
                    },
                    {
                        "tool": {
                            "name": "PostgreSQL",
                            "category": {"name": "Databases"},
                        }
                    },
                ],
            }
        }
    }

    def test_success(self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch) -> None:
        """enrich() returns True and writes tech_stack as a JSON array."""
        monkeypatch.setenv("STACKSHARE_API_KEY", "ss-key")
        mock_resp = _mock_response(json_data=self._VALID_RESPONSE)

        with patch(
            "pipeline.src.enrichment.stackshare.requests.post", return_value=mock_resp
        ):
            result = stackshare_enrich(1, "Acme Corp", db_conn)

        assert result is True
        row = _get_company(db_conn, "Acme Corp")
        assert row is not None
        tools = json.loads(row["tech_stack"])
        assert len(tools) == 2
        tool_names = {t["name"] for t in tools}
        assert "Python" in tool_names
        assert "PostgreSQL" in tool_names

    def test_missing_api_key_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when STACKSHARE_API_KEY is not set."""
        monkeypatch.delenv("STACKSHARE_API_KEY", raising=False)
        result = stackshare_enrich(1, "Acme Corp", db_conn)
        assert result is False

    def test_http_error_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False on HTTP error."""
        monkeypatch.setenv("STACKSHARE_API_KEY", "ss-key")
        mock_resp = _mock_response(status_code=401, raise_for_status=True)

        with patch(
            "pipeline.src.enrichment.stackshare.requests.post", return_value=mock_resp
        ):
            result = stackshare_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_network_error_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False on network error."""
        monkeypatch.setenv("STACKSHARE_API_KEY", "ss-key")

        with patch(
            "pipeline.src.enrichment.stackshare.requests.post",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            result = stackshare_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_company_not_found_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when company is not found on StackShare."""
        monkeypatch.setenv("STACKSHARE_API_KEY", "ss-key")
        mock_resp = _mock_response(json_data={"data": {"company": None}})

        with patch(
            "pipeline.src.enrichment.stackshare.requests.post", return_value=mock_resp
        ):
            result = stackshare_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_graphql_errors_returns_false(
        self, db_conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enrich() returns False when GraphQL errors are present."""
        monkeypatch.setenv("STACKSHARE_API_KEY", "ss-key")
        mock_resp = _mock_response(
            json_data={"errors": [{"message": "Unauthorized"}], "data": None}
        )

        with patch(
            "pipeline.src.enrichment.stackshare.requests.post", return_value=mock_resp
        ):
            result = stackshare_enrich(1, "Acme Corp", db_conn)

        assert result is False

    def test_missing_api_key_logs_warning(
        self,
        db_conn: sqlite3.Connection,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """enrich() emits a logging.WARNING when the API key is absent."""
        monkeypatch.delenv("STACKSHARE_API_KEY", raising=False)
        import logging

        with caplog.at_level(logging.WARNING, logger="pipeline.src.enrichment.stackshare"):
            stackshare_enrich(1, "Acme Corp", db_conn)

        assert any("STACKSHARE_API_KEY" in record.message for record in caplog.records)

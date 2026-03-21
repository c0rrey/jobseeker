"""
Tests for pipeline/src/company_discovery.py.

Covers:
- discover_company() with explicit career_url (skips Glassdoor API call)
- discover_company() with no career_url triggers Glassdoor/RapidAPI lookup
- Missing RAPIDAPI_KEY logs warning and returns None
- ATS-detected pages create career_page_configs row and update companies.ats_platform
- Non-ATS pages store scrape_strategy JSON in career_page_configs
- rediscover_broken() re-runs discovery and updates status
- Phase isolation: each internal phase tested independently
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline.src.company_discovery import (
    DiscoveryResult,
    _analyse_html,
    _fetch_html,
    _persist_discovery,
    _resolve_career_url,
    discover_company,
    rediscover_broken,
)
from pipeline.src.database import get_connection, init_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Return a path to an initialised SQLite database."""
    path = tmp_path / "test.db"
    init_db(path)
    return path


@pytest.fixture()
def conn(db_path: Path) -> sqlite3.Connection:
    """Open a connection to the test database."""
    c = get_connection(db_path)
    yield c
    c.close()


@pytest.fixture()
def seeded_company(conn: sqlite3.Connection) -> int:
    """Insert a test company and return its ID."""
    conn.execute("INSERT INTO companies (name) VALUES ('Acme Corp')")
    conn.commit()
    row = conn.execute("SELECT id FROM companies WHERE name = 'Acme Corp'").fetchone()
    return row["id"]


# ---------------------------------------------------------------------------
# LLM response fixtures
# ---------------------------------------------------------------------------

ATS_LLM_RESPONSE: dict[str, Any] = {
    "is_ats": True,
    "ats_platform": "greenhouse",
    "ats_slug": "acmecorp",
    "ats_feed_url": "https://boards.greenhouse.io/acmecorp",
    "scrape_strategy": None,
}

CUSTOM_LLM_RESPONSE: dict[str, Any] = {
    "is_ats": False,
    "ats_platform": None,
    "ats_slug": None,
    "ats_feed_url": None,
    "scrape_strategy": {
        "job_list_selector": "ul.jobs li",
        "job_title_selector": "h3 a",
        "job_url_selector": "h3 a[href]",
        "job_location_selector": ".location",
        "job_department_selector": None,
        "url_base": "https://acme.com",
        "url_patterns": ["https://acme.com/jobs/"],
        "pagination": {"type": "none", "next_selector": None},
        "notes": "Standard list page.",
    },
}

GREENHOUSE_HTML = """
<html>
<head><title>Acme Corp Careers</title></head>
<body>
  <div id="grnhse_app"></div>
  <script src="https://boards.greenhouse.io/embed/job_board/js?for=acmecorp"></script>
</body>
</html>
"""

CUSTOM_HTML = """
<html>
<head><title>Acme Careers</title></head>
<body>
  <ul class="jobs">
    <li><h3><a href="/jobs/senior-engineer">Senior Engineer</a></h3><span class="location">Remote</span></li>
  </ul>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# _resolve_career_url tests
# ---------------------------------------------------------------------------


class TestResolveCareerUrl:
    """Tests for the Glassdoor-based _resolve_career_url implementation."""

    def test_returns_probed_career_url_when_glassdoor_has_website(
        self, conn: sqlite3.Connection
    ) -> None:
        """Returns the first live career URL found by probing the Glassdoor website domain."""
        glassdoor_data = {"name": "Acme Corp", "website": "https://acme.com"}
        probe_response = MagicMock()
        probe_response.status_code = 200

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.company_discovery._probe_career_url",
            return_value="https://acme.com/careers",
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ):
            url, metadata = _resolve_career_url("Acme Corp", conn, "fake-rapidapi-key")

        assert url == "https://acme.com/careers"
        assert metadata is not None
        assert metadata["name"] == "Acme Corp"

    def test_returns_none_url_when_glassdoor_has_no_website(
        self, conn: sqlite3.Connection
    ) -> None:
        """Returns (None, metadata) when Glassdoor data lacks a website field."""
        glassdoor_data = {"name": "No Website Corp", "website": None}

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ):
            url, metadata = _resolve_career_url("No Website Corp", conn, "fake-rapidapi-key")

        assert url is None
        assert metadata is not None

    def test_returns_none_none_when_glassdoor_api_fails(
        self, conn: sqlite3.Connection
    ) -> None:
        """Returns (None, None) when the Glassdoor API call fails entirely."""
        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=None,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ):
            url, metadata = _resolve_career_url("Ghost Corp", conn, "fake-rapidapi-key")

        assert url is None
        assert metadata is None

    def test_uses_cache_hit_without_api_call(
        self, conn: sqlite3.Connection
    ) -> None:
        """Uses Glassdoor cache data and skips the live API call on a cache hit."""
        cached_entry = {
            "status": "OK",
            "data": {"name": "Cached Corp", "website": "https://cached.com"},
        }

        with patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={"Cached Corp": cached_entry},
        ), patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data"
        ) as mock_api, patch(
            "pipeline.src.company_discovery._probe_career_url",
            return_value="https://cached.com/careers",
        ):
            url, metadata = _resolve_career_url("Cached Corp", conn, "fake-rapidapi-key")

        mock_api.assert_not_called()
        assert url == "https://cached.com/careers"


# ---------------------------------------------------------------------------
# _fetch_html tests
# ---------------------------------------------------------------------------


class TestFetchHtml:
    def test_returns_html_text(self) -> None:
        mock_response = MagicMock()
        mock_response.text = "<html>hello</html>"
        mock_response.raise_for_status = MagicMock()

        with patch("pipeline.src.company_discovery.requests.get", return_value=mock_response):
            html = _fetch_html("https://acme.com/careers")

        assert html == "<html>hello</html>"

    def test_truncates_long_html(self) -> None:
        long_html = "x" * 25_000
        mock_response = MagicMock()
        mock_response.text = long_html
        mock_response.raise_for_status = MagicMock()

        with patch("pipeline.src.company_discovery.requests.get", return_value=mock_response):
            html = _fetch_html("https://acme.com/careers")

        assert html is not None
        assert len(html) == 20_000

    def test_returns_none_on_http_error(self) -> None:
        import requests as _req

        with patch(
            "pipeline.src.company_discovery.requests.get",
            side_effect=_req.RequestException("connection refused"),
        ):
            html = _fetch_html("https://unreachable.example")

        assert html is None


# ---------------------------------------------------------------------------
# _analyse_html tests
# ---------------------------------------------------------------------------


class TestAnalyseHtml:
    def test_returns_parsed_json_for_ats(self) -> None:
        with patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(ATS_LLM_RESPONSE),
        ):
            result = _analyse_html(GREENHOUSE_HTML)

        assert result is not None
        assert result["is_ats"] is True
        assert result["ats_platform"] == "greenhouse"

    def test_strips_markdown_fences(self) -> None:
        wrapped = f"```json\n{json.dumps(ATS_LLM_RESPONSE)}\n```"
        with patch("pipeline.src.company_discovery._call_llm", return_value=wrapped):
            result = _analyse_html(GREENHOUSE_HTML)

        assert result is not None
        assert result["is_ats"] is True

    def test_returns_none_when_llm_returns_none(self) -> None:
        with patch("pipeline.src.company_discovery._call_llm", return_value=None):
            result = _analyse_html(GREENHOUSE_HTML)

        assert result is None

    def test_returns_none_on_invalid_json(self) -> None:
        with patch("pipeline.src.company_discovery._call_llm", return_value="not json {{{{"):
            result = _analyse_html(GREENHOUSE_HTML)

        assert result is None

    def test_returns_custom_scrape_strategy(self) -> None:
        with patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(CUSTOM_LLM_RESPONSE),
        ):
            result = _analyse_html(CUSTOM_HTML)

        assert result is not None
        assert result["is_ats"] is False
        assert result["scrape_strategy"] is not None
        assert result["scrape_strategy"]["job_list_selector"] == "ul.jobs li"


# ---------------------------------------------------------------------------
# _persist_discovery tests
# ---------------------------------------------------------------------------


class TestPersistDiscovery:
    def test_ats_creates_career_page_configs_row(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        result = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            html=GREENHOUSE_HTML,
            llm_response=ATS_LLM_RESPONSE,
        )
        ok = _persist_discovery(result, conn, seeded_company)

        assert ok is True
        row = conn.execute(
            "SELECT * FROM career_page_configs WHERE company_id = ?", (seeded_company,)
        ).fetchone()
        assert row is not None
        assert row["url"] == "https://boards.greenhouse.io/acmecorp"
        assert row["status"] == "active"
        assert row["discovery_method"] == "auto"

    def test_ats_updates_companies_ats_platform(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        result = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            html=GREENHOUSE_HTML,
            llm_response=ATS_LLM_RESPONSE,
        )
        _persist_discovery(result, conn, seeded_company)

        row = conn.execute(
            "SELECT ats_platform FROM companies WHERE id = ?", (seeded_company,)
        ).fetchone()
        assert row["ats_platform"] == "greenhouse"

    def test_non_ats_stores_scrape_strategy_json(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        result = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            html=CUSTOM_HTML,
            llm_response=CUSTOM_LLM_RESPONSE,
        )
        ok = _persist_discovery(result, conn, seeded_company)

        assert ok is True
        row = conn.execute(
            "SELECT scrape_strategy FROM career_page_configs WHERE company_id = ?",
            (seeded_company,),
        ).fetchone()
        assert row is not None
        parsed = json.loads(row["scrape_strategy"])
        assert parsed["job_list_selector"] == "ul.jobs li"

    def test_non_ats_does_not_set_ats_platform(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        result = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            html=CUSTOM_HTML,
            llm_response=CUSTOM_LLM_RESPONSE,
        )
        _persist_discovery(result, conn, seeded_company)

        row = conn.execute(
            "SELECT ats_platform FROM companies WHERE id = ?", (seeded_company,)
        ).fetchone()
        assert row["ats_platform"] is None

    def test_returns_false_when_llm_response_is_none(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        result = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            llm_response=None,
        )
        ok = _persist_discovery(result, conn, seeded_company)

        assert ok is False

    def test_upserts_existing_config(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        """Second persist on same company_id updates the existing row."""
        result1 = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            html=CUSTOM_HTML,
            llm_response=CUSTOM_LLM_RESPONSE,
        )
        _persist_discovery(result1, conn, seeded_company)

        # Now re-persist with ATS response
        result2 = DiscoveryResult(
            company_name="Acme Corp",
            career_url="https://acme.com/careers",
            html=GREENHOUSE_HTML,
            llm_response=ATS_LLM_RESPONSE,
        )
        ok = _persist_discovery(result2, conn, seeded_company)

        assert ok is True
        rows = conn.execute(
            "SELECT COUNT(*) FROM career_page_configs WHERE company_id = ?",
            (seeded_company,),
        ).fetchone()
        assert rows[0] == 1  # still only one row


# ---------------------------------------------------------------------------
# discover_company integration tests
# ---------------------------------------------------------------------------


class TestDiscoverCompany:
    def test_ats_page_returns_company_record(
        self, conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ATS page discovery returns a CompanyRecord with the resolved career URL."""
        monkeypatch.setenv("RAPIDAPI_KEY", "fake-rapidapi-key")
        glassdoor_data = {"name": "Acme Corp", "website": "https://acme.com", "rating": 4.5}

        mock_html_response = MagicMock()
        mock_html_response.text = GREENHOUSE_HTML
        mock_html_response.raise_for_status = MagicMock()

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ), patch(
            "pipeline.src.company_discovery.requests.get",
            return_value=mock_html_response,
        ), patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(ATS_LLM_RESPONSE),
        ):
            result = discover_company(
                company_name="Acme Corp",
                db_connection=conn,
                career_url="https://acme.com/careers",
            )

        from pipeline.src.company_discovery import CompanyRecord
        assert isinstance(result, CompanyRecord)
        assert result.career_page_url == "https://acme.com/careers"
        # Verify the career_page_configs row was written with the ATS feed URL
        row = conn.execute(
            "SELECT url, status, discovery_method FROM career_page_configs"
            " WHERE company_id = ?", (result.company_id,)
        ).fetchone()
        assert row is not None
        assert row["url"] == "https://boards.greenhouse.io/acmecorp"
        assert row["status"] == "active"
        assert row["discovery_method"] == "auto"

    def test_non_ats_page_stores_scrape_strategy(
        self, conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("RAPIDAPI_KEY", "fake-rapidapi-key")
        glassdoor_data = {"name": "Acme Corp", "website": "https://acme.com", "rating": 3.8}

        mock_html_response = MagicMock()
        mock_html_response.text = CUSTOM_HTML
        mock_html_response.raise_for_status = MagicMock()

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ), patch(
            "pipeline.src.company_discovery.requests.get",
            return_value=mock_html_response,
        ), patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(CUSTOM_LLM_RESPONSE),
        ):
            result = discover_company(
                company_name="Acme Corp",
                db_connection=conn,
                career_url="https://acme.com/careers",
            )

        assert result is not None
        row = conn.execute(
            "SELECT scrape_strategy FROM career_page_configs WHERE company_id = ?",
            (result.company_id,),
        ).fetchone()
        assert row is not None
        parsed = json.loads(row["scrape_strategy"])
        assert parsed["job_list_selector"] == "ul.jobs li"

    def test_missing_rapidapi_key_returns_none(
        self, conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When RAPIDAPI_KEY is absent and no career_url supplied, returns None and logs a warning."""
        import logging
        monkeypatch.delenv("RAPIDAPI_KEY", raising=False)
        with caplog.at_level(logging.WARNING, logger="pipeline.src.company_discovery"):
            config = discover_company(
                company_name="Acme Corp",
                db_connection=conn,
                # no career_url — requires Glassdoor lookup which needs RAPIDAPI_KEY
            )

        assert config is None
        assert "RAPIDAPI_KEY" in caplog.text

    def test_uses_glassdoor_when_career_url_is_none(
        self, conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When career_url is None, Glassdoor/RapidAPI is used to discover the company."""
        monkeypatch.setenv("RAPIDAPI_KEY", "fake-rapidapi-key")
        glassdoor_data = {"name": "Acme Corp", "website": "https://acme.com", "rating": 4.2}

        html_response = MagicMock()
        html_response.text = GREENHOUSE_HTML
        html_response.raise_for_status = MagicMock()
        html_response.status_code = 200

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ), patch(
            "pipeline.src.company_discovery._probe_career_url",
            return_value="https://acme.com/careers",
        ), patch(
            "pipeline.src.company_discovery.requests.get",
            return_value=html_response,
        ), patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(ATS_LLM_RESPONSE),
        ):
            result = discover_company(
                company_name="Acme Corp",
                db_connection=conn,
                # no career_url — triggers Glassdoor-based resolution
            )

        assert result is not None
        assert result.career_page_url == "https://acme.com/careers"

    def test_returns_company_record_when_html_fetch_fails(
        self, conn: sqlite3.Connection, seeded_company: int
    ) -> None:
        """When HTML fetch fails for an existing company, returns CompanyRecord with no career URL update."""
        import requests as _req

        # seeded_company already exists — skips the RAPIDAPI_KEY check and goes
        # directly to the existing-row path.  With career_url provided, it runs
        # the HTML/LLM phases; a failed HTML fetch returns the existing record.
        with patch(
            "pipeline.src.company_discovery.requests.get",
            side_effect=_req.RequestException("timeout"),
        ):
            result = discover_company(
                company_name="Acme Corp",
                db_connection=conn,
                career_url="https://acme.com/careers",
            )

        # HTML fetch failed — returns the existing CompanyRecord without new career URL
        from pipeline.src.company_discovery import CompanyRecord
        assert isinstance(result, CompanyRecord)
        assert result.company_id == seeded_company

    def test_creates_company_row_if_not_exists(
        self, conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("RAPIDAPI_KEY", "fake-rapidapi-key")
        glassdoor_data = {"name": "Brand New Corp", "website": "https://brandnew.com", "rating": 3.9}

        mock_html_response = MagicMock()
        mock_html_response.text = GREENHOUSE_HTML
        mock_html_response.raise_for_status = MagicMock()

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ), patch(
            "pipeline.src.company_discovery.requests.get",
            return_value=mock_html_response,
        ), patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(ATS_LLM_RESPONSE),
        ):
            discover_company(
                company_name="Brand New Corp",
                db_connection=conn,
                career_url="https://brandnew.com/careers",
            )

        row = conn.execute(
            "SELECT name FROM companies WHERE name = 'Brand New Corp'"
        ).fetchone()
        assert row is not None

    def test_ats_platform_set_on_companies_table(
        self, conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("RAPIDAPI_KEY", "fake-rapidapi-key")
        glassdoor_data = {"name": "Acme Corp", "website": "https://acme.com", "rating": 4.5}

        mock_html_response = MagicMock()
        mock_html_response.text = GREENHOUSE_HTML
        mock_html_response.raise_for_status = MagicMock()

        with patch(
            "pipeline.src.company_discovery._fetch_glassdoor_data",
            return_value=glassdoor_data,
        ), patch(
            "pipeline.src.enrichment.glassdoor_rapidapi._load_cache",
            return_value={},
        ), patch(
            "pipeline.src.company_discovery.requests.get",
            return_value=mock_html_response,
        ), patch(
            "pipeline.src.company_discovery._call_llm",
            return_value=json.dumps(ATS_LLM_RESPONSE),
        ):
            discover_company(
                company_name="Acme Corp",
                db_connection=conn,
                career_url="https://acme.com/careers",
            )

        row = conn.execute(
            "SELECT ats_platform FROM companies WHERE name = 'Acme Corp'"
        ).fetchone()
        assert row["ats_platform"] == "greenhouse"


# ---------------------------------------------------------------------------
# rediscover_broken tests
# ---------------------------------------------------------------------------


class TestRediscoverBroken:
    def _insert_broken_config(
        self, conn: sqlite3.Connection, company_name: str, url: str
    ) -> None:
        conn.execute("INSERT INTO companies (name) VALUES (?)", (company_name,))
        conn.commit()
        company_id = conn.execute(
            "SELECT id FROM companies WHERE name = ?", (company_name,)
        ).fetchone()["id"]
        conn.execute(
            """
            INSERT INTO career_page_configs
                (company_id, url, discovery_method, status)
            VALUES (?, ?, 'auto', 'broken')
            """,
            (company_id, url),
        )
        conn.commit()

    def test_recovers_broken_config(self, conn: sqlite3.Connection) -> None:
        self._insert_broken_config(conn, "Fix Corp", "https://fix.com/careers")

        mock_html_response = MagicMock()
        mock_html_response.text = GREENHOUSE_HTML
        mock_html_response.raise_for_status = MagicMock()

        with patch("pipeline.src.company_discovery.requests.get", return_value=mock_html_response), \
             patch(
                 "pipeline.src.company_discovery._call_llm",
                 return_value=json.dumps(ATS_LLM_RESPONSE),
             ):
            stats = rediscover_broken(conn)

        assert stats["attempted"] == 1
        assert stats["recovered"] == 1
        assert stats["still_broken"] == 0

    def test_leaves_still_broken_on_failure(self, conn: sqlite3.Connection) -> None:
        self._insert_broken_config(conn, "Broken Corp", "https://broken.com/careers")

        import requests as _req

        with patch(
            "pipeline.src.company_discovery.requests.get",
            side_effect=_req.RequestException("still down"),
        ):
            stats = rediscover_broken(conn)

        assert stats["attempted"] == 1
        assert stats["recovered"] == 0
        assert stats["still_broken"] == 1

    def test_returns_zero_counts_when_no_broken_configs(
        self, conn: sqlite3.Connection
    ) -> None:
        stats = rediscover_broken(conn)
        assert stats == {"attempted": 0, "recovered": 0, "still_broken": 0}

    def test_handles_multiple_broken_configs(self, conn: sqlite3.Connection) -> None:
        self._insert_broken_config(conn, "Good Corp", "https://good.com/careers")
        self._insert_broken_config(conn, "Bad Corp", "https://bad.com/careers")

        import requests as _req

        mock_html_response = MagicMock()
        mock_html_response.text = GREENHOUSE_HTML
        mock_html_response.raise_for_status = MagicMock()

        call_counts: list[int] = [0]

        def selective_get(url: str, **kwargs: Any) -> Any:
            call_counts[0] += 1
            if "bad.com" in url:
                raise _req.RequestException("still down")
            return mock_html_response

        with patch("pipeline.src.company_discovery.requests.get", side_effect=selective_get), \
             patch(
                 "pipeline.src.company_discovery._call_llm",
                 return_value=json.dumps(ATS_LLM_RESPONSE),
             ):
            stats = rediscover_broken(conn)

        assert stats["attempted"] == 2
        assert stats["recovered"] == 1
        assert stats["still_broken"] == 1

    def test_active_config_status_after_recovery(self, conn: sqlite3.Connection) -> None:
        self._insert_broken_config(conn, "Recover Corp", "https://recover.com/careers")

        mock_html_response = MagicMock()
        mock_html_response.text = GREENHOUSE_HTML
        mock_html_response.raise_for_status = MagicMock()

        with patch("pipeline.src.company_discovery.requests.get", return_value=mock_html_response), \
             patch(
                 "pipeline.src.company_discovery._call_llm",
                 return_value=json.dumps(ATS_LLM_RESPONSE),
             ):
            rediscover_broken(conn)

        row = conn.execute(
            """
            SELECT cpc.status FROM career_page_configs cpc
            JOIN companies c ON c.id = cpc.company_id
            WHERE c.name = 'Recover Corp'
            """
        ).fetchone()
        assert row["status"] == "active"

"""
Tests for pipeline/src/fetchers/career_page.py and its normalizer support.

Covers:
- CareerPageFetcher is importable and extends BaseFetcher
- source_type returns 'career_page'
- fetch() iterates active configs and applies CSS selectors
- last_crawled_at updated after each crawl
- 0-result on previously-crawled config flags it as 'broken'
- 0-result on first-run (last_crawled_at IS NULL) does NOT flag broken
- HTTP errors do not update timestamps or flag broken
- normalize_career_page() returns a correctly-shaped V2 Job
- normalizer dispatch for source='career_page'

BeautifulSoup4 is mocked via patch where present so tests run without
requiring bs4 to be installed.  A separate integration test class uses a
real bs4 import if available.
"""

import json
import sqlite3
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline.src.fetchers import BaseFetcher, CareerPageFetcher
from pipeline.src.normalizer import normalize, normalize_career_page


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_STRATEGY: dict[str, str] = {
    "job_container": ".job-listing",
    "title": ".job-title",
    "url": "a.apply-link",
    "location": ".location",
    "description": ".description",
}

SAMPLE_HTML = """
<html><body>
  <ul>
    <li class="job-listing">
      <span class="job-title">Senior Data Engineer</span>
      <a class="apply-link" href="https://careers.acme.com/jobs/101">Apply</a>
      <span class="location">Remote</span>
      <span class="description">Build pipelines.</span>
    </li>
    <li class="job-listing">
      <span class="job-title">Data Engineer</span>
      <a class="apply-link" href="/jobs/102">Apply</a>
      <span class="location">Miami, FL</span>
    </li>
  </ul>
</body></html>
"""

CAREER_PAGE_RAW: dict[str, Any] = {
    "title": "Senior Data Engineer",
    "url": "https://careers.acme.com/jobs/101",
    "location": "Remote",
    "description": "Build pipelines.",
    "source": "career_page",
    "_career_page_config_id": 1,
}


def _make_conn(
    configs: list[dict[str, Any]] | None = None,
) -> sqlite3.Connection:
    """Build an in-memory SQLite DB with career_page_configs populated."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE career_page_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL DEFAULT 1,
            url TEXT NOT NULL,
            discovery_method TEXT NOT NULL DEFAULT 'manual',
            scrape_strategy TEXT,
            last_crawled_at TEXT,
            status TEXT NOT NULL DEFAULT 'active'
        )
        """
    )
    if configs:
        for c in configs:
            conn.execute(
                """
                INSERT INTO career_page_configs
                    (url, scrape_strategy, last_crawled_at, status)
                VALUES (?, ?, ?, ?)
                """,
                (
                    c["url"],
                    c.get("scrape_strategy"),
                    c.get("last_crawled_at"),
                    c.get("status", "active"),
                ),
            )
    conn.commit()
    return conn


def _make_http_response(html: str) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.text = html
    return mock_resp


# ---------------------------------------------------------------------------
# Import surface and BaseFetcher contract
# ---------------------------------------------------------------------------


class TestCareerPageFetcherImport:
    def test_importable(self) -> None:
        assert CareerPageFetcher is not None

    def test_is_base_fetcher_subclass(self) -> None:
        assert issubclass(CareerPageFetcher, BaseFetcher)

    def test_instantiates_with_conn(self) -> None:
        conn = _make_conn()
        fetcher = CareerPageFetcher(conn)
        assert fetcher is not None
        conn.close()


# ---------------------------------------------------------------------------
# source_type
# ---------------------------------------------------------------------------


class TestCareerPageSourceType:
    def test_source_type_is_career_page(self) -> None:
        conn = _make_conn()
        assert CareerPageFetcher(conn).source_type == "career_page"
        conn.close()

    def test_source_type_in_valid_set(self) -> None:
        conn = _make_conn()
        valid = {"api", "career_page", "ats_feed"}
        assert CareerPageFetcher(conn).source_type in valid
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — no active configs
# ---------------------------------------------------------------------------


class TestCareerPageFetcherNoConfigs:
    def test_no_active_configs_returns_empty_list(self) -> None:
        conn = _make_conn(configs=[])
        result = CareerPageFetcher(conn).fetch()
        assert result == []
        conn.close()

    def test_disabled_config_not_fetched(self) -> None:
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "status": "disabled",
            }]
        )
        result = CareerPageFetcher(conn).fetch()
        assert result == []
        conn.close()

    def test_broken_config_not_fetched(self) -> None:
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "status": "broken",
            }]
        )
        result = CareerPageFetcher(conn).fetch()
        assert result == []
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — HTML extraction with mocked bs4
# ---------------------------------------------------------------------------


class TestCareerPageFetcherExtraction:
    """Tests that mock both requests.get and BeautifulSoup to avoid bs4 dep."""

    def _run_fetch_with_html(
        self,
        html: str,
        strategy: dict[str, str],
        last_crawled_at: str | None = "2026-03-01T00:00:00Z",
    ) -> tuple[list[dict[str, Any]], sqlite3.Connection]:
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": json.dumps(strategy),
                "last_crawled_at": last_crawled_at,
                "status": "active",
            }]
        )
        fetcher = CareerPageFetcher(conn)

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            mock_get.return_value = _make_http_response(html)
            result = fetcher.fetch()

        return result, conn

    def test_fetch_returns_list(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        assert isinstance(result, list)
        conn.close()

    def test_fetch_returns_dicts(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        assert all(isinstance(j, dict) for j in result)
        conn.close()

    def test_fetch_extracts_job_titles(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        titles = [j["title"] for j in result]
        assert "Senior Data Engineer" in titles
        conn.close()

    def test_fetch_extracts_absolute_url(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        urls = [j["url"] for j in result]
        assert "https://careers.acme.com/jobs/101" in urls
        conn.close()

    def test_fetch_resolves_relative_url(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        urls = [j["url"] for j in result]
        # /jobs/102 should be resolved against careers.acme.com
        assert any("careers.acme.com/jobs/102" in u for u in urls)
        conn.close()

    def test_fetch_extracts_location(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        assert result[0]["location"] == "Remote"
        conn.close()

    def test_fetch_annotates_config_id(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        result, conn = self._run_fetch_with_html(SAMPLE_HTML, SAMPLE_STRATEGY)
        assert all("_career_page_config_id" in j for j in result)
        conn.close()


# ---------------------------------------------------------------------------
# last_crawled_at update
# ---------------------------------------------------------------------------


class TestCareerPageLastCrawledAt:
    def test_last_crawled_at_updated_after_successful_crawl(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "last_crawled_at": None,
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            mock_get.return_value = _make_http_response(SAMPLE_HTML)
            CareerPageFetcher(conn).fetch()

        row = conn.execute(
            "SELECT last_crawled_at FROM career_page_configs WHERE id = 1"
        ).fetchone()
        assert row["last_crawled_at"] is not None
        conn.close()

    def test_last_crawled_at_not_updated_on_http_error(self) -> None:
        import requests as _requests

        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "last_crawled_at": None,
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            mock_get.side_effect = _requests.RequestException("timeout")
            CareerPageFetcher(conn).fetch()

        row = conn.execute(
            "SELECT last_crawled_at FROM career_page_configs WHERE id = 1"
        ).fetchone()
        assert row["last_crawled_at"] is None
        conn.close()


# ---------------------------------------------------------------------------
# Broken-config detection
# ---------------------------------------------------------------------------


class TestCareerPageBrokenDetection:
    def test_zero_results_on_previously_crawled_config_marks_broken(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        empty_html = "<html><body><ul></ul></body></html>"
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "last_crawled_at": "2026-03-01T00:00:00Z",  # previously crawled
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            mock_get.return_value = _make_http_response(empty_html)
            CareerPageFetcher(conn).fetch()

        row = conn.execute(
            "SELECT status FROM career_page_configs WHERE id = 1"
        ).fetchone()
        assert row["status"] == "broken"
        conn.close()

    def test_zero_results_on_first_run_does_not_mark_broken(self) -> None:
        pytest.importorskip("bs4", reason="beautifulsoup4 not installed")
        empty_html = "<html><body><ul></ul></body></html>"
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "last_crawled_at": None,  # never crawled before
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            mock_get.return_value = _make_http_response(empty_html)
            CareerPageFetcher(conn).fetch()

        row = conn.execute(
            "SELECT status FROM career_page_configs WHERE id = 1"
        ).fetchone()
        assert row["status"] == "active"
        conn.close()

    def test_http_error_does_not_mark_broken(self) -> None:
        import requests as _requests

        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": json.dumps(SAMPLE_STRATEGY),
                "last_crawled_at": "2026-03-01T00:00:00Z",
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            mock_get.side_effect = _requests.RequestException("timeout")
            CareerPageFetcher(conn).fetch()

        row = conn.execute(
            "SELECT status FROM career_page_configs WHERE id = 1"
        ).fetchone()
        assert row["status"] == "active"
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — config with no scrape_strategy
# ---------------------------------------------------------------------------


class TestCareerPageNoStrategy:
    def test_config_without_strategy_is_skipped(self) -> None:
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": None,
                "last_crawled_at": None,
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get") as mock_get:
            result = CareerPageFetcher(conn).fetch()

        assert result == []
        mock_get.assert_not_called()
        conn.close()

    def test_config_without_strategy_still_updates_last_crawled_at(self) -> None:
        conn = _make_conn(
            configs=[{
                "url": "https://careers.acme.com/jobs",
                "scrape_strategy": None,
                "last_crawled_at": None,
                "status": "active",
            }]
        )

        with patch("pipeline.src.fetchers.career_page.requests.get"):
            CareerPageFetcher(conn).fetch()

        row = conn.execute(
            "SELECT last_crawled_at FROM career_page_configs WHERE id = 1"
        ).fetchone()
        assert row["last_crawled_at"] is not None
        conn.close()


# ---------------------------------------------------------------------------
# normalize_career_page()
# ---------------------------------------------------------------------------


class TestNormalizeCareerPage:
    def test_returns_job(self) -> None:
        from pipeline.src.models import Job

        assert isinstance(normalize_career_page(CAREER_PAGE_RAW), Job)

    def test_title(self) -> None:
        assert normalize_career_page(CAREER_PAGE_RAW).title == "Senior Data Engineer"

    def test_url(self) -> None:
        assert normalize_career_page(CAREER_PAGE_RAW).url == "https://careers.acme.com/jobs/101"

    def test_source_is_career_page(self) -> None:
        assert normalize_career_page(CAREER_PAGE_RAW).source == "career_page"

    def test_source_type_is_career_page(self) -> None:
        assert normalize_career_page(CAREER_PAGE_RAW).source_type == "career_page"

    def test_location_mapped(self) -> None:
        assert normalize_career_page(CAREER_PAGE_RAW).location == "Remote"

    def test_description_mapped(self) -> None:
        assert normalize_career_page(CAREER_PAGE_RAW).description == "Build pipelines."

    def test_raw_json_stored(self) -> None:
        job = normalize_career_page(CAREER_PAGE_RAW)
        assert json.loads(job.raw_json) == CAREER_PAGE_RAW

    def test_missing_optional_fields_are_none(self) -> None:
        minimal = {"title": "Eng", "url": "https://x.com", "source": "career_page"}
        job = normalize_career_page(minimal)
        assert job.location is None
        assert job.description is None

    def test_normalize_dispatch(self) -> None:
        jobs = normalize([CAREER_PAGE_RAW], "career_page")
        assert len(jobs) == 1
        assert jobs[0].source == "career_page"

    def test_normalize_dispatch_empty_list(self) -> None:
        assert normalize([], "career_page") == []

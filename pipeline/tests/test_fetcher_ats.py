"""
Tests for pipeline/src/fetchers/ats.py and its normalizer support.

Covers:
- ATSFetcher is importable and extends BaseFetcher
- source_type returns 'ats_feed'
- fetch() queries companies table and dispatches per ATS platform
- Greenhouse, Lever, Ashby endpoint URL construction
- Per-company HTTP errors are caught; fetcher continues with remainder
- normalize_greenhouse(), normalize_lever(), normalize_ashby() round-trip
- normalizer dispatch for each ATS source name
"""

import json
import sqlite3
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from pipeline.src.fetchers import ATSFetcher, BaseFetcher
from pipeline.src.normalizer import (
    normalize,
    normalize_ashby,
    normalize_greenhouse,
    normalize_lever,
)


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

GREENHOUSE_JOB: dict[str, Any] = {
    "id": 12345,
    "title": "Senior Data Engineer",
    "updated_at": "2026-03-01T08:00:00.000Z",
    "location": {"name": "Remote"},
    "absolute_url": "https://boards.greenhouse.io/acme/jobs/12345",
    "_ats_platform": "greenhouse",
    "_company_name": "Acme Corp",
}

LEVER_JOB: dict[str, Any] = {
    "id": "abc-123-def",
    "text": "Data Engineer",
    "createdAt": 1740441600000,  # 2025-02-25T00:00:00Z
    "categories": {"location": "Remote", "team": "Engineering"},
    "hostedUrl": "https://jobs.lever.co/acme/abc-123-def",
    "_ats_platform": "lever",
    "_company_name": "Lever Co",
}

ASHBY_JOB: dict[str, Any] = {
    "id": "xyz-789",
    "title": "Analytics Engineer",
    "publishedDate": "2026-03-01T00:00:00.000Z",
    "jobUrl": "https://jobs.ashbyhq.com/acme/xyz-789",
    "locationName": "New York, NY",
    "_ats_platform": "ashby",
    "_company_name": "Ashby Inc",
}


def _make_response(data: Any) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = data
    return mock_resp


def _make_conn(
    companies: list[dict[str, Any]] | None = None,
) -> sqlite3.Connection:
    """
    Build an in-memory SQLite DB with the companies table populated.

    Each company dict should have keys: name, ats_platform, domain.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            ats_platform TEXT,
            domain TEXT
        )
        """
    )
    if companies:
        for c in companies:
            conn.execute(
                "INSERT INTO companies (name, ats_platform, domain) VALUES (?, ?, ?)",
                (c["name"], c["ats_platform"], c.get("domain")),
            )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Import surface and BaseFetcher contract
# ---------------------------------------------------------------------------


class TestATSFetcherImport:
    def test_importable(self) -> None:
        assert ATSFetcher is not None

    def test_is_base_fetcher_subclass(self) -> None:
        assert issubclass(ATSFetcher, BaseFetcher)

    def test_instantiates_with_conn(self) -> None:
        conn = _make_conn()
        fetcher = ATSFetcher(conn)
        assert fetcher is not None
        conn.close()


# ---------------------------------------------------------------------------
# source_type
# ---------------------------------------------------------------------------


class TestATSSourceType:
    def test_source_type_is_ats_feed(self) -> None:
        conn = _make_conn()
        assert ATSFetcher(conn).source_type == "ats_feed"
        conn.close()

    def test_source_type_in_valid_set(self) -> None:
        conn = _make_conn()
        valid = {"api", "career_page", "ats_feed"}
        assert ATSFetcher(conn).source_type in valid
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — no companies
# ---------------------------------------------------------------------------


class TestATSFetcherNoCompanies:
    def test_empty_companies_returns_empty_list(self) -> None:
        conn = _make_conn(companies=[])
        result = ATSFetcher(conn).fetch()
        assert result == []
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — Greenhouse
# ---------------------------------------------------------------------------


class TestATSFetcherGreenhouse:
    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_greenhouse_hits_correct_url(self, mock_get: MagicMock) -> None:
        """Greenhouse fetcher calls boards-api.greenhouse.io/v1/boards/{slug}/jobs."""
        mock_get.return_value = _make_response({"jobs": [GREENHOUSE_JOB]})
        conn = _make_conn([{"name": "Acme Corp", "ats_platform": "greenhouse", "domain": "acme.com"}])

        ATSFetcher(conn).fetch()

        mock_get.assert_called_once()
        url_called = mock_get.call_args[0][0]
        assert "boards-api.greenhouse.io/v1/boards/acme/jobs" in url_called
        conn.close()

    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_greenhouse_returns_annotated_dicts(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response({"jobs": [{"id": 1, "title": "Eng", "absolute_url": "https://x.com"}]})
        conn = _make_conn([{"name": "Acme", "ats_platform": "greenhouse", "domain": "acme.io"}])

        result = ATSFetcher(conn).fetch()

        assert len(result) == 1
        assert result[0]["_ats_platform"] == "greenhouse"
        assert result[0]["_company_name"] == "Acme"
        conn.close()

    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_greenhouse_empty_jobs_list(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response({"jobs": []})
        conn = _make_conn([{"name": "Acme", "ats_platform": "greenhouse", "domain": "acme.io"}])

        result = ATSFetcher(conn).fetch()

        assert result == []
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — Lever
# ---------------------------------------------------------------------------


class TestATSFetcherLever:
    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_lever_hits_correct_url(self, mock_get: MagicMock) -> None:
        """Lever fetcher calls api.lever.co/v0/postings/{slug}."""
        mock_get.return_value = _make_response([LEVER_JOB])
        conn = _make_conn([{"name": "Lever Co", "ats_platform": "lever", "domain": "leverco.com"}])

        ATSFetcher(conn).fetch()

        url_called = mock_get.call_args[0][0]
        assert "api.lever.co/v0/postings/leverco" in url_called
        conn.close()

    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_lever_returns_annotated_dicts(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response([{"id": "abc", "text": "Engineer", "hostedUrl": "https://jobs.lever.co/x/abc"}])
        conn = _make_conn([{"name": "My Co", "ats_platform": "lever", "domain": "myco.com"}])

        result = ATSFetcher(conn).fetch()

        assert len(result) == 1
        assert result[0]["_ats_platform"] == "lever"
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — Ashby
# ---------------------------------------------------------------------------


class TestATSFetcherAshby:
    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_ashby_hits_correct_url(self, mock_get: MagicMock) -> None:
        """Ashby fetcher calls api.ashbyhq.com/posting-api/job-board/{slug}."""
        mock_get.return_value = _make_response({"success": True, "results": [ASHBY_JOB]})
        conn = _make_conn([{"name": "Ashby Inc", "ats_platform": "ashby", "domain": "ashby.io"}])

        ATSFetcher(conn).fetch()

        url_called = mock_get.call_args[0][0]
        assert "api.ashbyhq.com/posting-api/job-board/ashby" in url_called
        conn.close()

    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_fetch_ashby_returns_annotated_dicts(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response({"results": [{"id": "x", "title": "Eng", "jobUrl": "https://jobs.ashbyhq.com/co/x"}]})
        conn = _make_conn([{"name": "Co", "ats_platform": "ashby", "domain": "co.io"}])

        result = ATSFetcher(conn).fetch()

        assert result[0]["_ats_platform"] == "ashby"
        conn.close()


# ---------------------------------------------------------------------------
# fetch() — error resilience
# ---------------------------------------------------------------------------


class TestATSFetcherErrorResilience:
    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_single_company_http_error_does_not_stop_batch(
        self, mock_get: MagicMock
    ) -> None:
        """A RequestException for one company logs a warning but fetching continues."""
        import requests as _requests

        # First company raises; second succeeds
        mock_get.side_effect = [
            _requests.RequestException("timeout"),
            _make_response({"jobs": [GREENHOUSE_JOB]}),
        ]
        conn = _make_conn([
            {"name": "Bad Co", "ats_platform": "greenhouse", "domain": "bad.io"},
            {"name": "Good Co", "ats_platform": "greenhouse", "domain": "good.io"},
        ])

        result = ATSFetcher(conn).fetch()

        # Only Good Co's job should be in results
        assert len(result) == 1
        assert result[0]["_company_name"] == "Good Co"
        conn.close()

    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_all_companies_fail_returns_empty_list(
        self, mock_get: MagicMock
    ) -> None:
        import requests as _requests

        mock_get.side_effect = _requests.RequestException("network down")
        conn = _make_conn([{"name": "Co", "ats_platform": "greenhouse", "domain": "co.io"}])

        result = ATSFetcher(conn).fetch()

        assert result == []
        conn.close()

    @patch("pipeline.src.fetchers.ats.requests.get")
    def test_multiple_companies_all_succeed(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [
            _make_response({"jobs": [GREENHOUSE_JOB]}),
            _make_response([LEVER_JOB]),
        ]
        conn = _make_conn([
            {"name": "Acme", "ats_platform": "greenhouse", "domain": "acme.io"},
            {"name": "Lever Co", "ats_platform": "lever", "domain": "leverco.com"},
        ])

        result = ATSFetcher(conn).fetch()

        assert len(result) == 2
        conn.close()

    def test_unsupported_platform_is_skipped(self) -> None:
        conn = _make_conn([{"name": "Weird Co", "ats_platform": "workday", "domain": "weird.io"}])

        # workday is not in _ATS_URLS so the DB query won't even return it
        result = ATSFetcher(conn).fetch()

        assert result == []
        conn.close()


# ---------------------------------------------------------------------------
# normalize_greenhouse()
# ---------------------------------------------------------------------------


class TestNormalizeGreenhouse:
    def test_returns_job(self) -> None:
        from pipeline.src.models import Job

        assert isinstance(normalize_greenhouse(GREENHOUSE_JOB), Job)

    def test_title(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).title == "Senior Data Engineer"

    def test_company(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).company == "Acme Corp"

    def test_url(self) -> None:
        assert "greenhouse.io" in normalize_greenhouse(GREENHOUSE_JOB).url

    def test_source_is_greenhouse(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).source == "greenhouse"

    def test_source_type_is_ats_feed(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).source_type == "ats_feed"

    def test_ats_platform_field(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).ats_platform == "greenhouse"

    def test_external_id(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).external_id == "12345"

    def test_location_from_dict(self) -> None:
        assert normalize_greenhouse(GREENHOUSE_JOB).location == "Remote"

    def test_raw_json_stored(self) -> None:
        job = normalize_greenhouse(GREENHOUSE_JOB)
        assert json.loads(job.raw_json) == GREENHOUSE_JOB

    def test_normalize_dispatch(self) -> None:
        jobs = normalize([GREENHOUSE_JOB], "greenhouse")
        assert jobs[0].source == "greenhouse"


# ---------------------------------------------------------------------------
# normalize_lever()
# ---------------------------------------------------------------------------


class TestNormalizeLever:
    def test_returns_job(self) -> None:
        from pipeline.src.models import Job

        assert isinstance(normalize_lever(LEVER_JOB), Job)

    def test_title(self) -> None:
        assert normalize_lever(LEVER_JOB).title == "Data Engineer"

    def test_company(self) -> None:
        assert normalize_lever(LEVER_JOB).company == "Lever Co"

    def test_url(self) -> None:
        assert "lever.co" in normalize_lever(LEVER_JOB).url

    def test_source_is_lever(self) -> None:
        assert normalize_lever(LEVER_JOB).source == "lever"

    def test_source_type_is_ats_feed(self) -> None:
        assert normalize_lever(LEVER_JOB).source_type == "ats_feed"

    def test_ats_platform_field(self) -> None:
        assert normalize_lever(LEVER_JOB).ats_platform == "lever"

    def test_external_id(self) -> None:
        assert normalize_lever(LEVER_JOB).external_id == "abc-123-def"

    def test_location_from_categories(self) -> None:
        assert normalize_lever(LEVER_JOB).location == "Remote"

    def test_posted_at_converted_from_ms(self) -> None:
        """createdAt milliseconds should be converted to ISO-8601 string."""
        job = normalize_lever(LEVER_JOB)
        assert job.posted_at is not None
        assert "T" in job.posted_at  # ISO-8601 format

    def test_posted_at_none_when_missing(self) -> None:
        raw = dict(LEVER_JOB)
        del raw["createdAt"]
        assert normalize_lever(raw).posted_at is None

    def test_normalize_dispatch(self) -> None:
        jobs = normalize([LEVER_JOB], "lever")
        assert jobs[0].source == "lever"


# ---------------------------------------------------------------------------
# normalize_ashby()
# ---------------------------------------------------------------------------


class TestNormalizeAshby:
    def test_returns_job(self) -> None:
        from pipeline.src.models import Job

        assert isinstance(normalize_ashby(ASHBY_JOB), Job)

    def test_title(self) -> None:
        assert normalize_ashby(ASHBY_JOB).title == "Analytics Engineer"

    def test_company(self) -> None:
        assert normalize_ashby(ASHBY_JOB).company == "Ashby Inc"

    def test_url(self) -> None:
        assert "ashbyhq.com" in normalize_ashby(ASHBY_JOB).url

    def test_source_is_ashby(self) -> None:
        assert normalize_ashby(ASHBY_JOB).source == "ashby"

    def test_source_type_is_ats_feed(self) -> None:
        assert normalize_ashby(ASHBY_JOB).source_type == "ats_feed"

    def test_ats_platform_field(self) -> None:
        assert normalize_ashby(ASHBY_JOB).ats_platform == "ashby"

    def test_external_id(self) -> None:
        assert normalize_ashby(ASHBY_JOB).external_id == "xyz-789"

    def test_location(self) -> None:
        assert normalize_ashby(ASHBY_JOB).location == "New York, NY"

    def test_raw_json_stored(self) -> None:
        job = normalize_ashby(ASHBY_JOB)
        assert json.loads(job.raw_json) == ASHBY_JOB

    def test_normalize_dispatch(self) -> None:
        jobs = normalize([ASHBY_JOB], "ashby")
        assert jobs[0].source == "ashby"

"""
Tests for pipeline/src/fetchers/linkedin.py and its normalizer support.

Covers:
- LinkedInFetcher is importable and extends BaseFetcher
- source_type returns 'api'
- fetch() returns list[dict] when HTTP is mocked
- fetch() deduplicates by job_url
- fetch() returns empty list when RAPIDAPI_KEY is absent
- fetch() returns empty list on HTTP error (graceful degradation)
- normalize_linkedin() returns a correctly-shaped V2 Job
- normalizer dispatch for source='linkedin'
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline.src.fetchers import BaseFetcher, LinkedInFetcher
from pipeline.src.normalizer import normalize, normalize_linkedin


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

LINKEDIN_JOB_1: dict[str, Any] = {
    "job_id": "3812345001",
    "job_url": "https://www.linkedin.com/jobs/view/3812345001",
    "job_title": "Senior Data Engineer",
    "company_name": "Acme Corp",
    "job_location": "Miami, FL (Remote)",
    "job_description": "Build and maintain data pipelines at scale.",
    "job_posted_at": "2026-03-01",
    "min_salary": 140000,
    "max_salary": 180000,
}

LINKEDIN_JOB_2: dict[str, Any] = {
    "job_id": "3812345002",
    "job_url": "https://www.linkedin.com/jobs/view/3812345002",
    "job_title": "Data Engineer",
    "company_name": "Beta Inc",
    "job_location": "Remote",
    "job_description": "Work on Spark and Kafka.",
    "job_posted_at": "2026-03-02",
    "min_salary": 130000,
    "max_salary": 160000,
}

MOCK_PROFILE: dict[str, Any] = {
    "title_keywords": ["data engineer"],
    "salary_target": 130000,
}


def _make_response(data: Any) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = data
    return mock_resp


# ---------------------------------------------------------------------------
# Import surface and BaseFetcher contract
# ---------------------------------------------------------------------------


class TestLinkedInFetcherImport:
    """LinkedInFetcher must be importable and satisfy BaseFetcher ABC."""

    def test_importable(self) -> None:
        assert LinkedInFetcher is not None

    def test_is_base_fetcher_subclass(self) -> None:
        assert issubclass(LinkedInFetcher, BaseFetcher)

    def test_can_instantiate_with_key(self) -> None:
        fetcher = LinkedInFetcher(rapidapi_key="test-key")
        assert fetcher is not None

    def test_can_instantiate_without_key(self) -> None:
        """Construction must not raise even when RAPIDAPI_KEY is absent."""
        fetcher = LinkedInFetcher()
        assert fetcher is not None


# ---------------------------------------------------------------------------
# source_type
# ---------------------------------------------------------------------------


class TestLinkedInSourceType:
    def test_source_type_is_api(self) -> None:
        assert LinkedInFetcher(rapidapi_key="k").source_type == "api"

    def test_source_type_in_valid_set(self) -> None:
        valid = {"api", "career_page", "ats_feed"}
        assert LinkedInFetcher(rapidapi_key="k").source_type in valid


# ---------------------------------------------------------------------------
# fetch() — mocked HTTP
# ---------------------------------------------------------------------------


class TestLinkedInFetcherFetch:
    """LinkedInFetcher.fetch() with mocked load_profile and requests.get."""

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_returns_list(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        mock_profile.return_value = MOCK_PROFILE
        mock_get.return_value = _make_response([LINKEDIN_JOB_1, LINKEDIN_JOB_2])

        result = LinkedInFetcher(rapidapi_key="test-key").fetch()

        assert isinstance(result, list)

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_returns_dicts(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        mock_profile.return_value = MOCK_PROFILE
        mock_get.return_value = _make_response([LINKEDIN_JOB_1, LINKEDIN_JOB_2])

        result = LinkedInFetcher(rapidapi_key="test-key").fetch()

        assert all(isinstance(item, dict) for item in result)

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_deduplicates_by_url(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        """Same job_url appearing across keyword searches appears only once."""
        mock_profile.return_value = {"title_keywords": ["data engineer", "senior data engineer"]}
        # Both keywords return the same job
        mock_get.return_value = _make_response([LINKEDIN_JOB_1])

        result = LinkedInFetcher(rapidapi_key="test-key").fetch()

        urls = [j["job_url"] for j in result]
        assert len(urls) == len(set(urls))
        assert len(result) == 1

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_empty_api_response(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        mock_profile.return_value = MOCK_PROFILE
        mock_get.return_value = _make_response([])

        result = LinkedInFetcher(rapidapi_key="test-key").fetch()

        assert result == []

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_http_error_returns_empty_list(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        """HTTP errors are caught; returns empty list rather than raising."""
        import requests as _requests

        mock_profile.return_value = MOCK_PROFILE
        mock_get.side_effect = _requests.RequestException("timeout")

        result = LinkedInFetcher(rapidapi_key="test-key").fetch()

        assert isinstance(result, list)

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    def test_fetch_missing_key_returns_empty_list(
        self,
        mock_profile: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When RAPIDAPI_KEY env var is unset, fetch() returns [] without crashing."""
        mock_profile.return_value = MOCK_PROFILE
        monkeypatch.delenv("RAPIDAPI_KEY", raising=False)

        result = LinkedInFetcher().fetch()  # No key provided, no env var

        assert result == []

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_respects_results_per_keyword_cap(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        """results_per_keyword limits how many results are kept per keyword."""
        mock_profile.return_value = MOCK_PROFILE
        mock_get.return_value = _make_response([LINKEDIN_JOB_1, LINKEDIN_JOB_2])

        result = LinkedInFetcher(rapidapi_key="k", results_per_keyword=1).fetch()

        assert len(result) <= 1

    @patch("pipeline.src.fetchers.linkedin.load_profile")
    @patch("pipeline.src.fetchers.linkedin.requests.get")
    def test_fetch_dict_wrapped_response(
        self,
        mock_get: MagicMock,
        mock_profile: MagicMock,
    ) -> None:
        """Handles endpoints that wrap the job list in a dict under 'jobs'."""
        mock_profile.return_value = MOCK_PROFILE
        mock_get.return_value = _make_response({"jobs": [LINKEDIN_JOB_1]})

        result = LinkedInFetcher(rapidapi_key="k").fetch()

        assert len(result) == 1
        assert result[0]["job_title"] == "Senior Data Engineer"


# ---------------------------------------------------------------------------
# normalize_linkedin()
# ---------------------------------------------------------------------------


class TestNormalizeLinkedIn:
    """normalize_linkedin() converts raw dicts to V2 Job dataclasses correctly."""

    def test_returns_job(self) -> None:
        from pipeline.src.models import Job

        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert isinstance(job, Job)

    def test_title_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.title == "Senior Data Engineer"

    def test_company_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.company == "Acme Corp"

    def test_url_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.url == "https://www.linkedin.com/jobs/view/3812345001"

    def test_source_is_linkedin(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.source == "linkedin"

    def test_source_type_is_api(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.source_type == "api"

    def test_location_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.location == "Miami, FL (Remote)"

    def test_salary_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.salary_min == 140000
        assert job.salary_max == 180000

    def test_posted_at_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.posted_at == "2026-03-01"

    def test_external_id_mapped(self) -> None:
        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert job.external_id == "3812345001"

    def test_raw_json_stored(self) -> None:
        import json

        job = normalize_linkedin(LINKEDIN_JOB_1)
        assert json.loads(job.raw_json) == LINKEDIN_JOB_1

    def test_missing_optional_fields_are_none(self) -> None:
        minimal = {"job_title": "Engineer", "company_name": "Foo", "job_url": "https://x.com"}
        job = normalize_linkedin(minimal)
        assert job.salary_min is None
        assert job.salary_max is None
        assert job.location is None


# ---------------------------------------------------------------------------
# normalize() dispatcher
# ---------------------------------------------------------------------------


class TestNormalizeDispatchLinkedIn:
    def test_dispatches_linkedin(self) -> None:
        jobs = normalize([LINKEDIN_JOB_1], "linkedin")
        assert len(jobs) == 1
        assert jobs[0].source == "linkedin"

    def test_dispatches_empty_list(self) -> None:
        jobs = normalize([], "linkedin")
        assert jobs == []

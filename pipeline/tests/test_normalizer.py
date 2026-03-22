"""
Tests for pipeline/src/normalizer.py.

Covers edge cases for normalize_adzuna, normalize_linkedin, normalize_greenhouse,
and the normalize() dispatcher with all supported sources.

All normalizer functions are pure transformations (no I/O), so tests call them
directly with crafted raw dicts and assert on the resulting Job fields.
"""

from __future__ import annotations

import json

import pytest

from pipeline.src.normalizer import (
    normalize,
    normalize_adzuna,
    normalize_ashby,
    normalize_career_page,
    normalize_greenhouse,
    normalize_lever,
    normalize_linkedin,
    normalize_mock,
    normalize_remoteok,
)


# ---------------------------------------------------------------------------
# normalize_adzuna
# ---------------------------------------------------------------------------


class TestNormalizeAdzuna:
    """Edge-case tests for normalize_adzuna."""

    def test_full_payload_maps_all_fields(self) -> None:
        """All standard Adzuna fields map to the correct Job attributes."""
        raw = {
            "id": "12345",
            "title": "Senior Data Engineer",
            "company": {"display_name": "Example Corp"},
            "location": {"display_name": "San Francisco, CA"},
            "description": "Build pipelines.",
            "redirect_url": "https://adzuna.com/jobs/12345",
            "salary_min": 100_000.0,
            "salary_max": 150_000.0,
            "created": "2024-01-15T12:00:00Z",
        }
        job = normalize_adzuna(raw)

        assert job.title == "Senior Data Engineer"
        assert job.company == "Example Corp"
        assert job.url == "https://adzuna.com/jobs/12345"
        assert job.description == "Build pipelines."
        assert job.source == "adzuna"
        assert job.source_type == "api"
        assert job.location == "San Francisco, CA"
        assert job.salary_min == 100_000.0
        assert job.salary_max == 150_000.0
        assert job.posted_at == "2024-01-15T12:00:00Z"

    def test_missing_company_field_returns_unknown(self) -> None:
        """When 'company' key is absent, company defaults to 'Unknown'."""
        raw = {
            "title": "Data Engineer",
            "redirect_url": "https://adzuna.com/jobs/1",
            "description": "desc",
        }
        job = normalize_adzuna(raw)
        assert job.company == "Unknown"

    def test_none_company_field_returns_unknown(self) -> None:
        """When 'company' is explicitly None, company defaults to 'Unknown'."""
        raw = {
            "title": "Data Engineer",
            "company": None,
            "redirect_url": "https://adzuna.com/jobs/2",
        }
        job = normalize_adzuna(raw)
        assert job.company == "Unknown"

    def test_missing_location_field_is_none(self) -> None:
        """When 'location' is absent, job.location is None."""
        raw = {
            "title": "Engineer",
            "company": {"display_name": "Corp"},
            "redirect_url": "https://adzuna.com/jobs/3",
        }
        job = normalize_adzuna(raw)
        assert job.location is None

    def test_none_location_field_is_none(self) -> None:
        """When 'location' is explicitly None, job.location is None."""
        raw = {
            "title": "Engineer",
            "company": {"display_name": "Corp"},
            "location": None,
            "redirect_url": "https://adzuna.com/jobs/4",
        }
        job = normalize_adzuna(raw)
        assert job.location is None

    def test_missing_salary_fields_are_none(self) -> None:
        """When salary_min/salary_max are absent, both are None."""
        raw = {"title": "Engineer", "redirect_url": "https://adzuna.com/jobs/5"}
        job = normalize_adzuna(raw)
        assert job.salary_min is None
        assert job.salary_max is None

    def test_empty_dict_returns_default_values(self) -> None:
        """An empty raw dict produces a Job with empty strings and None optionals."""
        job = normalize_adzuna({})
        assert job.title == ""
        assert job.company == "Unknown"
        assert job.url == ""
        assert job.source == "adzuna"
        assert job.source_type == "api"

    def test_raw_json_is_serialised_input(self) -> None:
        """raw_json on the resulting Job is the JSON-serialised input dict."""
        raw = {"title": "Test", "redirect_url": "https://adzuna.com/jobs/6"}
        job = normalize_adzuna(raw)
        assert json.loads(job.raw_json) == raw  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# normalize_linkedin
# ---------------------------------------------------------------------------


class TestNormalizeLinkedin:
    """Edge-case tests for normalize_linkedin."""

    def test_full_payload_maps_all_fields(self) -> None:
        """All standard LinkedIn RapidAPI fields map to the correct Job attributes."""
        raw = {
            "job_id": "3812345678",
            "job_url": "https://linkedin.com/jobs/view/3812345678",
            "job_title": "Senior Data Engineer",
            "company_name": "Acme Corp",
            "job_location": "Miami, FL (Remote)",
            "job_description": "We are looking for…",
            "job_posted_at": "2026-03-01",
            "min_salary": 140_000,
            "max_salary": 180_000,
        }
        job = normalize_linkedin(raw)

        assert job.title == "Senior Data Engineer"
        assert job.company == "Acme Corp"
        assert job.url == "https://linkedin.com/jobs/view/3812345678"
        assert job.description == "We are looking for…"
        assert job.source == "linkedin"
        assert job.source_type == "api"
        assert job.location == "Miami, FL (Remote)"
        assert job.salary_min == 140_000
        assert job.salary_max == 180_000
        assert job.posted_at == "2026-03-01"
        assert job.external_id == "3812345678"

    def test_missing_company_name_returns_unknown(self) -> None:
        """When 'company_name' is absent, company defaults to 'Unknown'."""
        raw = {"job_title": "Engineer", "job_url": "https://linkedin.com/jobs/1"}
        job = normalize_linkedin(raw)
        assert job.company == "Unknown"

    def test_missing_salary_fields_are_none(self) -> None:
        """When min_salary/max_salary are absent, both are None."""
        raw = {"job_title": "Engineer", "job_url": "https://linkedin.com/jobs/2"}
        job = normalize_linkedin(raw)
        assert job.salary_min is None
        assert job.salary_max is None

    def test_missing_description_is_none(self) -> None:
        """When 'job_description' is absent, description is None."""
        raw = {"job_title": "Engineer", "job_url": "https://linkedin.com/jobs/3"}
        job = normalize_linkedin(raw)
        assert job.description is None

    def test_empty_dict_returns_default_values(self) -> None:
        """An empty raw dict produces a Job with empty strings and None optionals."""
        job = normalize_linkedin({})
        assert job.title == ""
        assert job.company == "Unknown"
        assert job.url == ""
        assert job.source == "linkedin"
        assert job.source_type == "api"


# ---------------------------------------------------------------------------
# normalize_greenhouse
# ---------------------------------------------------------------------------


class TestNormalizeGreenhouse:
    """Edge-case tests for normalize_greenhouse."""

    def test_full_payload_maps_all_fields(self) -> None:
        """All standard Greenhouse fields map to the correct Job attributes."""
        raw = {
            "id": 99001,
            "title": "Data Engineer",
            "updated_at": "2026-03-01T08:00:00.000Z",
            "location": {"name": "Remote"},
            "absolute_url": "https://boards.greenhouse.io/acme/jobs/99001",
            "_ats_platform": "greenhouse",
            "_company_name": "Acme Corp",
        }
        job = normalize_greenhouse(raw)

        assert job.title == "Data Engineer"
        assert job.company == "Acme Corp"
        assert job.url == "https://boards.greenhouse.io/acme/jobs/99001"
        assert job.description is None  # Greenhouse list endpoint omits description
        assert job.source == "greenhouse"
        assert job.source_type == "ats_feed"
        assert job.location == "Remote"
        assert job.ats_platform == "greenhouse"
        assert job.external_id == "99001"
        assert job.posted_at == "2026-03-01T08:00:00.000Z"

    def test_missing_company_name_returns_unknown(self) -> None:
        """When '_company_name' is absent, company defaults to 'Unknown'."""
        raw = {
            "id": 1,
            "title": "Engineer",
            "absolute_url": "https://boards.greenhouse.io/co/jobs/1",
        }
        job = normalize_greenhouse(raw)
        assert job.company == "Unknown"

    def test_missing_location_is_none(self) -> None:
        """When 'location' key is absent, job.location is None."""
        raw = {
            "id": 2,
            "title": "Engineer",
            "absolute_url": "https://boards.greenhouse.io/co/jobs/2",
            "_company_name": "Corp",
        }
        job = normalize_greenhouse(raw)
        assert job.location is None

    def test_location_as_string_not_dict(self) -> None:
        """When 'location' is a plain string (not dict), it is used directly."""
        raw = {
            "id": 3,
            "title": "Engineer",
            "absolute_url": "https://boards.greenhouse.io/co/jobs/3",
            "_company_name": "Corp",
            "location": "New York",
        }
        job = normalize_greenhouse(raw)
        assert job.location == "New York"

    def test_none_id_produces_none_external_id(self) -> None:
        """When 'id' is None, external_id is None."""
        raw = {
            "id": None,
            "title": "Engineer",
            "absolute_url": "https://boards.greenhouse.io/co/jobs/0",
            "_company_name": "Corp",
        }
        job = normalize_greenhouse(raw)
        assert job.external_id is None

    def test_integer_id_is_converted_to_string_external_id(self) -> None:
        """Integer 'id' is converted to string for external_id."""
        raw = {
            "id": 42,
            "title": "Engineer",
            "absolute_url": "https://boards.greenhouse.io/co/jobs/42",
            "_company_name": "Corp",
        }
        job = normalize_greenhouse(raw)
        assert job.external_id == "42"

    def test_empty_dict_returns_default_values(self) -> None:
        """An empty raw dict produces a Job with safe default values."""
        job = normalize_greenhouse({})
        assert job.title == ""
        assert job.company == "Unknown"
        assert job.url == ""
        assert job.source == "greenhouse"
        assert job.source_type == "ats_feed"
        assert job.external_id is None


# ---------------------------------------------------------------------------
# Additional normalizers (smoke tests)
# ---------------------------------------------------------------------------


class TestNormalizeMock:
    def test_maps_required_fields(self) -> None:
        """normalize_mock correctly maps all required fields."""
        raw = {
            "title": "Job",
            "company": "Corp",
            "url": "https://example.com",
            "description": "Desc",
        }
        job = normalize_mock(raw)
        assert job.title == "Job"
        assert job.company == "Corp"
        assert job.source == "mock"
        assert job.source_type == "mock"

    def test_optional_location_is_mapped(self) -> None:
        raw = {
            "title": "Job",
            "company": "Corp",
            "url": "https://example.com",
            "description": "Desc",
            "location": "Remote",
        }
        job = normalize_mock(raw)
        assert job.location == "Remote"


class TestNormalizeRemoteok:
    def test_zero_salary_becomes_none(self) -> None:
        """salary_min/salary_max of 0 are normalised to None."""
        raw = {
            "position": "Engineer",
            "company": "Corp",
            "apply_url": "https://remoteok.com/1",
            "salary_min": 0,
            "salary_max": 0,
        }
        job = normalize_remoteok(raw)
        assert job.salary_min is None
        assert job.salary_max is None

    def test_apply_url_preferred_over_url(self) -> None:
        """apply_url takes priority over url when both are present."""
        raw = {
            "position": "Engineer",
            "company": "Corp",
            "apply_url": "https://apply.example.com",
            "url": "https://remoteok.com/fallback",
        }
        job = normalize_remoteok(raw)
        assert job.url == "https://apply.example.com"

    def test_fallback_to_url_when_apply_url_missing(self) -> None:
        """Falls back to 'url' when 'apply_url' is absent."""
        raw = {
            "position": "Engineer",
            "company": "Corp",
            "url": "https://remoteok.com/2",
        }
        job = normalize_remoteok(raw)
        assert job.url == "https://remoteok.com/2"


class TestNormalizeLever:
    def test_timestamp_ms_is_converted_to_iso(self) -> None:
        """Lever's createdAt (ms timestamp) is converted to ISO-8601 string."""
        raw = {
            "id": "abc-123",
            "text": "Engineer",
            "createdAt": 1_709_289_600_000,
            "categories": {"location": "Remote"},
            "hostedUrl": "https://jobs.lever.co/acme/abc-123",
            "_company_name": "Acme",
        }
        job = normalize_lever(raw)
        assert job.posted_at is not None
        assert job.posted_at.startswith("2024-03-")

    def test_missing_created_at_is_none(self) -> None:
        """When 'createdAt' is absent, posted_at is None."""
        raw = {
            "id": "xyz",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/acme/xyz",
            "_company_name": "Acme",
        }
        job = normalize_lever(raw)
        assert job.posted_at is None

    def test_categories_not_dict_does_not_raise(self) -> None:
        """When 'categories' is not a dict, location falls back to None."""
        raw = {
            "id": "xyz2",
            "text": "Engineer",
            "categories": "Remote",  # malformed — should be dict
            "hostedUrl": "https://jobs.lever.co/acme/xyz2",
            "_company_name": "Acme",
        }
        job = normalize_lever(raw)
        assert job.location is None


class TestNormalizeAshby:
    def test_maps_standard_fields(self) -> None:
        """normalize_ashby maps all standard Ashby fields correctly."""
        raw = {
            "id": "ashby-001",
            "title": "Data Engineer",
            "publishedDate": "2026-03-01T00:00:00.000Z",
            "jobUrl": "https://jobs.ashbyhq.com/acme/ashby-001",
            "locationName": "Remote",
            "_ats_platform": "ashby",
            "_company_name": "Acme Corp",
        }
        job = normalize_ashby(raw)
        assert job.title == "Data Engineer"
        assert job.company == "Acme Corp"
        assert job.url == "https://jobs.ashbyhq.com/acme/ashby-001"
        assert job.source == "ashby"
        assert job.source_type == "ats_feed"
        assert job.ats_platform == "ashby"
        assert job.external_id == "ashby-001"
        assert job.location == "Remote"


class TestNormalizeCareerPage:
    def test_maps_standard_fields(self) -> None:
        """normalize_career_page maps all standard fields correctly."""
        raw = {
            "title": "Data Engineer",
            "url": "https://careers.acme.com/jobs/123",
            "location": "Remote",
            "description": "Build data pipelines",
            "_company_name": "Acme Corp",
            "_career_page_config_id": 7,
        }
        job = normalize_career_page(raw)
        assert job.title == "Data Engineer"
        assert job.company == "Acme Corp"
        assert job.url == "https://careers.acme.com/jobs/123"
        assert job.source == "career_page"
        assert job.source_type == "career_page"


# ---------------------------------------------------------------------------
# normalize() dispatcher
# ---------------------------------------------------------------------------


class TestNormalizeDispatcher:
    """Tests for the normalize() list dispatcher."""

    def test_dispatches_to_adzuna(self) -> None:
        raw_list = [{"title": "A", "redirect_url": "https://adzuna.com/1"}]
        jobs = normalize(raw_list, "adzuna")
        assert len(jobs) == 1
        assert jobs[0].source == "adzuna"

    def test_dispatches_to_linkedin(self) -> None:
        raw_list = [{"job_title": "A", "job_url": "https://linkedin.com/1"}]
        jobs = normalize(raw_list, "linkedin")
        assert len(jobs) == 1
        assert jobs[0].source == "linkedin"

    def test_dispatches_to_greenhouse(self) -> None:
        raw_list = [
            {
                "id": 1,
                "title": "A",
                "absolute_url": "https://boards.greenhouse.io/co/jobs/1",
                "_company_name": "Corp",
            }
        ]
        jobs = normalize(raw_list, "greenhouse")
        assert len(jobs) == 1
        assert jobs[0].source == "greenhouse"

    def test_dispatches_to_mock(self) -> None:
        raw_list = [
            {
                "title": "A",
                "company": "B",
                "url": "https://example.com",
                "description": "D",
            }
        ]
        jobs = normalize(raw_list, "mock")
        assert len(jobs) == 1
        assert jobs[0].source == "mock"

    def test_dispatches_to_remoteok(self) -> None:
        raw_list = [{"position": "A", "company": "B", "url": "https://remoteok.com/1"}]
        jobs = normalize(raw_list, "remoteok")
        assert len(jobs) == 1
        assert jobs[0].source == "remoteok"

    def test_dispatches_to_lever(self) -> None:
        raw_list = [
            {
                "id": "abc",
                "text": "A",
                "hostedUrl": "https://jobs.lever.co/co/abc",
                "_company_name": "Corp",
            }
        ]
        jobs = normalize(raw_list, "lever")
        assert len(jobs) == 1
        assert jobs[0].source == "lever"

    def test_dispatches_to_ashby(self) -> None:
        raw_list = [
            {
                "id": "ash-1",
                "title": "A",
                "jobUrl": "https://jobs.ashbyhq.com/co/ash-1",
                "_company_name": "Corp",
            }
        ]
        jobs = normalize(raw_list, "ashby")
        assert len(jobs) == 1
        assert jobs[0].source == "ashby"

    def test_dispatches_to_career_page(self) -> None:
        raw_list = [
            {
                "title": "A",
                "url": "https://careers.corp.com/1",
                "_company_name": "Corp",
            }
        ]
        jobs = normalize(raw_list, "career_page")
        assert len(jobs) == 1
        assert jobs[0].source == "career_page"

    def test_unknown_source_raises_value_error(self) -> None:
        """normalize() raises ValueError for an unrecognised source string."""
        with pytest.raises(ValueError, match="Unknown source"):
            normalize([{"title": "A"}], "unknown_source")

    def test_empty_list_returns_empty_list(self) -> None:
        """normalize() with an empty raw_list returns an empty list."""
        assert normalize([], "adzuna") == []

"""
Tests for pipeline/src/fetchers/adzuna.py and remoteok.py.

Covers:
- Import surface (all three classes importable from pipeline.src.fetchers)
- source_type property on BaseFetcher, AdzunaFetcher, RemoteOKFetcher
- AdzunaFetcher.fetch() returns list of dicts (mocked HTTP)
- RemoteOKFetcher.fetch() returns list of dicts (mocked HTTP)
- BaseFetcher is abstract (cannot be instantiated directly)
- Correct import path: pipeline.config.settings (not config.settings)
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline.src.fetchers import AdzunaFetcher, BaseFetcher, RemoteOKFetcher


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ADZUNA_PAGE_RESPONSE: dict[str, Any] = {
    "results": [
        {
            "id": "az-001",
            "title": "Senior Data Engineer",
            "company": {"display_name": "Acme Corp"},
            "location": {"display_name": "Miami, FL"},
            "description": "Build data pipelines.",
            "redirect_url": "https://adzuna.com/jobs/az-001",
            "salary_min": 140000,
            "salary_max": 170000,
            "created": "2026-03-01T08:00:00Z",
        },
        {
            "id": "az-002",
            "title": "Data Engineer",
            "company": {"display_name": "Beta Inc"},
            "location": {"display_name": "Remote"},
            "description": "Work on Spark pipelines.",
            "redirect_url": "https://adzuna.com/jobs/az-002",
            "salary_min": 130000,
            "salary_max": 160000,
            "created": "2026-03-02T09:00:00Z",
        },
    ]
}

# RemoteOK returns an array: [metadata_obj, job1, job2, ...]
REMOTEOK_RESPONSE: list[Any] = [
    {"legal": "RemoteOK.com"},  # metadata row — should be skipped
    {
        "slug": "data-engineer-acme",
        "id": "ro-001",
        "epoch": 1740000000,
        "date": "2026-03-01T10:00:00Z",
        "company": "Acme Corp",
        "position": "Data Engineer",
        "tags": ["python", "spark", "data-engineer"],
        "description": "<p>Build pipelines.</p>",
        "location": "Remote",
        "apply_url": "https://remoteok.com/apply/ro-001",
        "url": "https://remoteok.com/ro-001",
        "salary_min": 130000,
        "salary_max": 160000,
    },
    {
        "slug": "senior-data-engineer-beta",
        "id": "ro-002",
        "epoch": 1740010000,
        "date": "2026-03-02T11:00:00Z",
        "company": "Beta Inc",
        "position": "Senior Data Engineer",
        "tags": ["python", "data-engineer", "remote"],
        "description": "<p>Senior role.</p>",
        "location": "Worldwide",
        "apply_url": "https://remoteok.com/apply/ro-002",
        "url": "https://remoteok.com/ro-002",
        "salary_min": 150000,
        "salary_max": 180000,
    },
]

MOCK_PROFILE: dict[str, Any] = {
    "title_keywords": ["data engineer"],
    "salary_target": 130000,
    "locations": ["Florida", "Remote"],
}


# ---------------------------------------------------------------------------
# Import surface
# ---------------------------------------------------------------------------


class TestImports:
    """All three fetcher classes must be importable from pipeline.src.fetchers."""

    def test_base_fetcher_importable(self) -> None:
        assert BaseFetcher is not None

    def test_adzuna_fetcher_importable(self) -> None:
        assert AdzunaFetcher is not None

    def test_remoteok_fetcher_importable(self) -> None:
        assert RemoteOKFetcher is not None


# ---------------------------------------------------------------------------
# BaseFetcher — abstract enforcement
# ---------------------------------------------------------------------------


class TestBaseFetcherAbstract:
    """BaseFetcher cannot be instantiated without implementing abstract members."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseFetcher()  # type: ignore[abstract]

    def test_subclass_without_source_type_is_abstract(self) -> None:
        """A subclass that omits source_type cannot be instantiated."""

        class IncompleteFetcher(BaseFetcher):
            def fetch(self) -> list[dict[str, Any]]:
                return []

        with pytest.raises(TypeError):
            IncompleteFetcher()  # type: ignore[abstract]

    def test_subclass_without_fetch_is_abstract(self) -> None:
        """A subclass that omits fetch() cannot be instantiated."""

        class IncompleteFetcher(BaseFetcher):
            @property
            def source_type(self) -> str:
                return "api"

        with pytest.raises(TypeError):
            IncompleteFetcher()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# source_type property
# ---------------------------------------------------------------------------


class TestSourceType:
    """Both concrete fetchers must report source_type == 'api'."""

    def test_adzuna_source_type_is_api(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ADZUNA_APP_ID", "test-id")
        monkeypatch.setenv("ADZUNA_APP_KEY", "test-key")
        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key")
        assert fetcher.source_type == "api"

    def test_remoteok_source_type_is_api(self) -> None:
        fetcher = RemoteOKFetcher()
        assert fetcher.source_type == "api"

    def test_source_type_is_valid_value(self) -> None:
        """source_type must be one of the allowed values."""
        valid = {"api", "career_page", "ats_feed"}
        assert AdzunaFetcher(app_id="x", app_key="y").source_type in valid
        assert RemoteOKFetcher().source_type in valid


# ---------------------------------------------------------------------------
# AdzunaFetcher.fetch() — mocked HTTP
# ---------------------------------------------------------------------------


class TestAdzunaFetcherFetch:
    """AdzunaFetcher.fetch() returns a list of dicts when HTTP is mocked."""

    def _mock_response(self, data: dict[str, Any]) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = data
        return mock_resp

    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_returns_list(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        mock_load_profile.return_value = MOCK_PROFILE
        # First page returns 2 results (< results_per_page=50), no second page
        mock_get.return_value = self._mock_response(ADZUNA_PAGE_RESPONSE)

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        assert isinstance(result, list)

    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_returns_dicts(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response(ADZUNA_PAGE_RESPONSE)

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        assert all(isinstance(item, dict) for item in result)

    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_deduplicates_by_url(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """Jobs with the same redirect_url appear only once."""
        mock_load_profile.return_value = MOCK_PROFILE
        # Return the same 2-result page for every call
        mock_get.return_value = self._mock_response(ADZUNA_PAGE_RESPONSE)

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        # Each keyword × location pair may hit the same URLs — verify no dupes
        urls = [job.get("redirect_url") for job in result]
        assert len(urls) == len(set(u for u in urls if u))

    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_empty_results(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """Fetch handles empty API response gracefully."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response({"results": []})

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        assert result == []

    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_http_error_is_caught(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """RequestException on a page is caught and fetch continues."""
        import requests as _requests

        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.side_effect = _requests.RequestException("connection refused")

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        # Should return empty list rather than raising
        assert isinstance(result, list)

    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_respects_results_per_page_cap(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """results_per_page is capped at 50 per Adzuna API limit."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response({"results": []})

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", results_per_page=100)

        assert fetcher.results_per_page == 50

    @patch("pipeline.src.fetchers.adzuna.time.sleep")
    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_null_results_value_returns_empty_list(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """API response with results=null returns [] without raising TypeError."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response({"results": None})

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        assert result == []

    @patch("pipeline.src.fetchers.adzuna.time.sleep")
    @patch("pipeline.src.fetchers.adzuna.load_profile")
    @patch("pipeline.src.fetchers.adzuna.requests.get")
    def test_fetch_skips_non_dict_elements_in_results(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """Non-dict elements (e.g. null) in results array are skipped without AttributeError."""
        valid_job = {
            "id": "az-003",
            "title": "Data Engineer",
            "redirect_url": "https://adzuna.com/jobs/az-003",
        }
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response({"results": [None, valid_job]})

        fetcher = AdzunaFetcher(app_id="test-id", app_key="test-key", max_pages=1)
        result = fetcher.fetch()

        assert len(result) == 1
        assert result[0]["redirect_url"] == "https://adzuna.com/jobs/az-003"


# ---------------------------------------------------------------------------
# RemoteOKFetcher.fetch() — mocked HTTP
# ---------------------------------------------------------------------------


class TestRemoteOKFetcherFetch:
    """RemoteOKFetcher.fetch() returns a list of dicts when HTTP is mocked."""

    def _mock_response(self, data: list[Any]) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = data
        return mock_resp

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_returns_list(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response(REMOTEOK_RESPONSE)

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        assert isinstance(result, list)

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_returns_dicts(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response(REMOTEOK_RESPONSE)

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        assert all(isinstance(item, dict) for item in result)

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_skips_metadata_row(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """The first item (metadata) must not appear in results."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response(REMOTEOK_RESPONSE)

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        for job in result:
            assert "legal" not in job

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_filters_by_keyword(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """Jobs matching 'data engineer' keyword are returned."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response(REMOTEOK_RESPONSE)

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        # Both jobs in the fixture match 'data engineer' via tags
        assert len(result) == 2

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_empty_response(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """Handles empty API response gracefully."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response([])

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        assert result == []

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_metadata_only_response(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """Response with only the metadata row returns empty list."""
        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.return_value = self._mock_response([{"legal": "RemoteOK.com"}])

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        assert result == []

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_http_error_returns_empty_list(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """RequestException is caught; returns empty list instead of raising."""
        import requests as _requests

        mock_load_profile.return_value = MOCK_PROFILE
        mock_get.side_effect = _requests.RequestException("timeout")

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        assert result == []

    @patch("pipeline.src.fetchers.remoteok.load_profile")
    @patch("pipeline.src.fetchers.remoteok.requests.get")
    def test_fetch_no_keywords_returns_all(
        self,
        mock_get: MagicMock,
        mock_load_profile: MagicMock,
    ) -> None:
        """When profile has no keywords, all jobs are returned unfiltered."""
        mock_load_profile.return_value = {"title_keywords": [], "salary_target": 100000}
        mock_get.return_value = self._mock_response(REMOTEOK_RESPONSE)

        fetcher = RemoteOKFetcher()
        result = fetcher.fetch()

        # Both non-metadata rows should be returned
        assert len(result) == 2

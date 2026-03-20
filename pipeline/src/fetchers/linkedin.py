"""
LinkedIn job fetcher via RapidAPI.

Uses the LinkedIn Jobs Search API available on RapidAPI to search for
job postings matching profile.yaml title_keywords.

Requires the RAPIDAPI_KEY environment variable to be set.
When the key is absent, fetch() logs a warning and returns an empty list
rather than raising an exception, so the pipeline can degrade gracefully.

RapidAPI LinkedIn Jobs endpoint used:
  GET https://linkedin-jobs-search.p.rapidapi.com/
  Query params: query, location, page, date_posted
"""

import logging
from typing import Any, Optional

import requests

from pipeline.config.settings import get_rapidapi_key, load_profile

from .base import BaseFetcher

logger = logging.getLogger(__name__)

_RAPIDAPI_HOST = "linkedin-jobs-search.p.rapidapi.com"
_RAPIDAPI_BASE_URL = f"https://{_RAPIDAPI_HOST}/"


class LinkedInFetcher(BaseFetcher):
    """Fetches jobs from LinkedIn via the RapidAPI LinkedIn Jobs Search API."""

    @property
    def source_type(self) -> str:
        """Return 'api' — LinkedIn data is fetched via a REST API."""
        return "api"

    def __init__(
        self,
        rapidapi_key: Optional[str] = None,
        location: str = "United States",
        date_posted: str = "past_week",
        results_per_keyword: int = 10,
    ) -> None:
        """
        Initialize the LinkedIn fetcher.

        Args:
            rapidapi_key: RapidAPI key. If None, loaded from RAPIDAPI_KEY env var.
                          A missing key is deferred to fetch() so the fetcher can
                          be constructed for testing without env vars set.
            location: Location filter string passed to the RapidAPI endpoint.
            date_posted: Recency filter. Typical values: 'past_24_hours',
                         'past_week', 'past_month', 'any_time'.
            results_per_keyword: Maximum results to request per keyword search.
        """
        self._rapidapi_key = rapidapi_key
        self.location = location
        self.date_posted = date_posted
        self.results_per_keyword = results_per_keyword

    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch LinkedIn jobs for all title_keywords in profile.yaml.

        Iterates profile.yaml title_keywords, issues one RapidAPI request per
        keyword, and deduplicates results by job URL (job_url field).

        Returns:
            List of raw job dicts from the RapidAPI LinkedIn response.
            Returns an empty list when RAPIDAPI_KEY is not set.
        """
        api_key = self._resolve_api_key()
        if api_key is None:
            return []

        profile = load_profile()
        keywords: list[str] = profile.get("title_keywords", ["data engineer"])

        all_jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        logger.info(
            "Fetching LinkedIn jobs for %d keyword(s): %s",
            len(keywords),
            ", ".join(keywords),
        )

        for keyword in keywords:
            jobs = self._fetch_keyword(api_key, keyword)
            for job in jobs:
                url = job.get("job_url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_jobs.append(job)

        logger.info("LinkedIn: %d unique jobs fetched", len(all_jobs))
        return all_jobs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_api_key(self) -> Optional[str]:
        """
        Resolve the RapidAPI key, logging a warning if absent.

        Returns:
            The key string, or None if not configured.
        """
        if self._rapidapi_key:
            return self._rapidapi_key
        try:
            return get_rapidapi_key()
        except ValueError:
            logger.warning(
                "RAPIDAPI_KEY is not set. LinkedInFetcher will return an empty list. "
                "Set the RAPIDAPI_KEY environment variable to enable LinkedIn fetching."
            )
            return None

    def _fetch_keyword(
        self, api_key: str, keyword: str
    ) -> list[dict[str, Any]]:
        """
        Fetch jobs for a single keyword from the RapidAPI LinkedIn endpoint.

        Args:
            api_key: RapidAPI authentication key.
            keyword: Job title keyword to search.

        Returns:
            List of raw job dicts, or an empty list on HTTP error.
        """
        headers = {
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": _RAPIDAPI_HOST,
        }
        params = {
            "query": keyword,
            "location": self.location,
            "date_posted": self.date_posted,
            "page": "0",
        }

        try:
            response = requests.get(
                _RAPIDAPI_BASE_URL,
                headers=headers,
                params=params,
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            # RapidAPI LinkedIn endpoint returns a list of job objects directly
            if isinstance(data, list):
                return data[: self.results_per_keyword]
            # Some endpoint versions wrap in a dict
            if isinstance(data, dict):
                return data.get("jobs", data.get("data", []))[: self.results_per_keyword]
            return []
        except requests.RequestException as exc:
            logger.warning(
                "LinkedIn RapidAPI request failed for keyword '%s': %s",
                keyword,
                exc,
            )
            return []

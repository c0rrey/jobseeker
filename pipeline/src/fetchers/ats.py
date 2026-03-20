"""
ATS feed fetcher for Greenhouse, Lever, and Ashby.

Queries the companies table for rows with a non-null ats_platform and
fetches structured job JSON from each ATS's public API. The ATS slug
used in URL construction is derived from the company domain
(e.g. "acme.com" -> "acme") when an explicit slug is not stored.

Supported platforms and their public endpoints:
  - Greenhouse: https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
  - Lever:      https://api.lever.co/v0/postings/{slug}
  - Ashby:      https://api.ashbyhq.com/posting-api/job-board/{slug}

Each raw job dict is annotated with '_ats_platform' and '_company_name'
so the normalizer can reconstruct source metadata without a second DB hit.
"""

import logging
import sqlite3
from typing import Any

import requests

from .base import BaseFetcher

logger = logging.getLogger(__name__)

# ATS endpoint templates keyed by ats_platform value stored in companies table
_ATS_URLS: dict[str, str] = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
    "lever": "https://api.lever.co/v0/postings/{slug}",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{slug}",
}


def _derive_slug(company_name: str, domain: str | None) -> str:
    """
    Derive an ATS company slug from domain or company name.

    Most ATS platforms use a lowercase slug that matches the company's primary
    domain without the TLD, or the company name lowercased with spaces replaced.

    Args:
        company_name: Human-readable company name from the companies table.
        domain: Optional domain field (e.g. "stripe.com").

    Returns:
        Best-guess slug string.
    """
    if domain:
        # "stripe.com" -> "stripe", "my-company.io" -> "my-company"
        base = domain.split(".")[0]
        return base.lower()
    # Fallback: lowercase company name, replace spaces/special chars with hyphens
    slug = company_name.lower().strip()
    slug = "".join(c if c.isalnum() or c == "-" else "-" for c in slug)
    # Collapse consecutive hyphens
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


class ATSFetcher(BaseFetcher):
    """
    Fetches structured job feeds from Greenhouse, Lever, and Ashby ATS platforms.

    Accepts a live SQLite connection to query the companies table.  The fetcher
    is intentionally fault-tolerant: a failure for any single company is logged
    and skipped so the rest of the batch can still be processed.
    """

    @property
    def source_type(self) -> str:
        """Return 'ats_feed' — jobs come from structured ATS JSON APIs."""
        return "ats_feed"

    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        Initialize the ATS fetcher.

        Args:
            conn: An open SQLite connection (see database.get_connection).
                  The fetcher does NOT close this connection; the caller owns it.
        """
        self._conn = conn

    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch jobs for all companies with a known ATS platform.

        Queries the companies table for rows where ats_platform is one of
        'greenhouse', 'lever', or 'ashby', then hits each platform's public
        job-board API.  Errors on individual companies are logged and skipped.

        Returns:
            Flat list of raw job dicts, each annotated with '_ats_platform'
            and '_company_name' for downstream normalization.
        """
        companies = self._query_ats_companies()
        if not companies:
            logger.info("ATSFetcher: no companies with ATS platform configured")
            return []

        all_jobs: list[dict[str, Any]] = []

        for company in companies:
            name: str = company["name"]
            platform: str = company["ats_platform"]
            domain: str | None = company["domain"]

            if platform not in _ATS_URLS:
                logger.warning(
                    "ATSFetcher: unsupported ATS platform '%s' for company '%s', skipping",
                    platform,
                    name,
                )
                continue

            slug = _derive_slug(name, domain)
            jobs = self._fetch_company(name, platform, slug)
            all_jobs.extend(jobs)

        logger.info("ATSFetcher: %d total jobs fetched across all companies", len(all_jobs))
        return all_jobs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _query_ats_companies(self) -> list[sqlite3.Row]:
        """
        Return all companies rows where ats_platform is not null and is a
        supported platform.

        Returns:
            List of sqlite3.Row objects with at least name, ats_platform, domain.
        """
        placeholders = ",".join("?" for _ in _ATS_URLS)
        sql = f"""
            SELECT name, ats_platform, domain
            FROM companies
            WHERE ats_platform IN ({placeholders})
        """
        try:
            return self._conn.execute(sql, list(_ATS_URLS.keys())).fetchall()
        except sqlite3.Error as exc:
            logger.error("ATSFetcher: DB query failed: %s", exc)
            return []

    def _fetch_company(
        self, company_name: str, platform: str, slug: str
    ) -> list[dict[str, Any]]:
        """
        Fetch jobs for a single company from its ATS JSON feed.

        Args:
            company_name: Human-readable name for logging.
            platform: ATS platform key ('greenhouse', 'lever', 'ashby').
            slug: URL slug for the company on the ATS platform.

        Returns:
            List of annotated raw job dicts, or empty list on error.
        """
        url = _ATS_URLS[platform].format(slug=slug)
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            logger.warning(
                "ATSFetcher: request failed for %s (%s) at %s: %s",
                company_name,
                platform,
                url,
                exc,
            )
            return []
        except ValueError as exc:
            logger.warning(
                "ATSFetcher: JSON decode error for %s (%s): %s",
                company_name,
                platform,
                exc,
            )
            return []

        raw_jobs = self._extract_jobs(platform, data)

        # Annotate each job with ATS context for the normalizer.
        # Build new dicts rather than mutating the original response objects
        # so that callers sharing response fixtures (e.g. in tests) are not
        # affected by in-place modification.
        raw_jobs = [
            {**job, "_ats_platform": platform, "_company_name": company_name}
            for job in raw_jobs
        ]

        logger.debug(
            "ATSFetcher: %d jobs from %s (%s)", len(raw_jobs), company_name, platform
        )
        return raw_jobs

    def _extract_jobs(self, platform: str, data: Any) -> list[dict[str, Any]]:
        """
        Extract the list of job objects from the ATS-specific response structure.

        Args:
            platform: ATS platform key.
            data: Parsed JSON response body.

        Returns:
            List of job dicts in the platform's native format.
        """
        if platform == "greenhouse":
            # {"jobs": [...], "meta": {...}}
            if isinstance(data, dict):
                return data.get("jobs", [])
        elif platform == "lever":
            # Returns a JSON array directly: [{...}, ...]
            if isinstance(data, list):
                return data
        elif platform == "ashby":
            # {"success": true, "results": [...]}
            if isinstance(data, dict):
                return data.get("results", [])
        return []

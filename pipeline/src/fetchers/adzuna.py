"""
Adzuna API fetcher.

Fetches job listings from the free Adzuna REST API.
Requires an API key from https://developer.adzuna.com/

The API returns a different structure than our Job schema;
the normalizer handles the conversion.
"""

import logging
import time
from typing import Any, Optional

import requests

from pipeline.config.settings import get_adzuna_credentials, load_profile

from .base import BaseFetcher

logger = logging.getLogger(__name__)

# Mapping of US state abbreviations to full state names used in Adzuna location queries.
# Covers all 50 states so that any preferred_locations entry like "City, ST" can produce
# both a short-form query ("Tampa, FL") and a long-form query ("Tampa, Florida").
_STATE_ABBREV_TO_FULL: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}


def _build_location_tuples(preferred_locations: list[str]) -> list[tuple[str, str]]:
    """Build Adzuna (name, query_string) tuples from profile preferred_locations.

    Each non-'Remote' entry in ``preferred_locations`` is expected to be in
    ``"City, ST"`` format (e.g. ``"Tampa, FL"``).  Two search tuples are
    produced per entry — one using the state abbreviation and one using the
    full state name — to maximise Adzuna result coverage:

    ``"Tampa, FL"``  →  ``[("Tampa, FL", "Tampa, FL"),
                           ("Tampa, Florida", "Tampa, Florida")]``

    Entries equal to ``"Remote"`` (case-insensitive) are skipped because
    Adzuna does not support "Remote" as a geographic ``where`` parameter.

    Entries that cannot be parsed into ``"City, ST"`` form (no comma, or
    unrecognised state abbreviation) are included as-is with a single tuple
    and a debug-level log message.

    Args:
        preferred_locations: List of location strings from profile.yaml.

    Returns:
        List of ``(name, query_string)`` tuples ready for the fetch loop.
    """
    tuples: list[tuple[str, str]] = []
    for entry in preferred_locations:
        if entry.strip().lower() == "remote":
            continue
        # Split on the last comma to handle multi-word city names like
        # "St. Petersburg, FL"
        if "," in entry:
            last_comma = entry.rfind(",")
            city = entry[:last_comma].strip()
            state_abbrev = entry[last_comma + 1:].strip()
            full_state = _STATE_ABBREV_TO_FULL.get(state_abbrev.upper())
            if full_state:
                short_query = f"{city}, {state_abbrev}"
                long_query = f"{city}, {full_state}"
                tuples.append((short_query, short_query))
                tuples.append((long_query, long_query))
            else:
                # Unrecognised abbreviation — include as-is
                logger.debug(
                    "Unknown state abbreviation '%s' in preferred_locations entry '%s'; using as-is",
                    state_abbrev,
                    entry,
                )
                tuples.append((entry, entry))
        else:
            # No comma — include as-is (e.g. a country name)
            logger.debug(
                "No comma found in preferred_locations entry '%s'; using as-is",
                entry,
            )
            tuples.append((entry, entry))
    return tuples


class AdzunaFetcher(BaseFetcher):
    """Fetches jobs from the Adzuna API."""

    @property
    def source_type(self) -> str:
        """Return 'api' — Adzuna is fetched via REST API."""
        return "api"

    def __init__(
        self,
        app_id: Optional[str] = None,
        app_key: Optional[str] = None,
        country: str = "us",
        results_per_page: int = 50,
        max_pages: int = 20,
        auto_increase_pages: bool = True,
    ):
        """
        Initialize Adzuna fetcher.
        
        Args:
            app_id: Adzuna app ID (loads from env if not provided)
            app_key: Adzuna app key (loads from env if not provided)
            country: Country code (us, uk, etc.)
            results_per_page: Results per page (max 50)
            max_pages: Maximum pages to fetch per keyword per location
            auto_increase_pages: If True, logs a warning recommending the caller
                re-run with ``max_pages=10`` when pagination limits are detected.
                Does not automatically increase page count.
        """
        if app_id is None or app_key is None:
            app_id, app_key = get_adzuna_credentials()
        
        self.app_id = app_id
        self.app_key = app_key
        self.country = country
        self.results_per_page = min(results_per_page, 50)  # API max is 50
        self.max_pages = max_pages
        self.auto_increase_pages = auto_increase_pages
        self.base_url = f"https://api.adzuna.com/v1/api/jobs/{country}/search"

    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch jobs from Adzuna API.

        Performs targeted searches for each keyword and location in profile.yaml.
        Search locations are derived from ``preferred_locations``; entries equal
        to ``"Remote"`` are skipped (Adzuna uses geographic ``where`` params).
        When ``preferred_locations`` is empty, returns an empty list immediately.

        ``max_days_old`` is read from ``max_job_age_days`` in the profile
        (default 60 if absent).

        Returns:
            List of raw job dicts from Adzuna
        """
        profile = load_profile()

        # Use NO salary filter at API level to get maximum results
        # We'll filter to the target salary locally for more control and better coverage
        api_salary_min = None  # No API filter - cast widest net
        target_salary = profile.get("salary_target", 130000)

        # Get keywords from profile
        keywords = profile.get("title_keywords", ["data engineer"])

        # Read max_job_age_days from profile; fall back to 60 if absent
        max_days_old: int = profile.get("max_job_age_days", 60)

        # Build location search tuples from profile, skipping 'Remote'
        preferred_locations: list[str] = profile.get("preferred_locations", [])
        locations = _build_location_tuples(preferred_locations)

        if not locations:
            logger.warning(
                "Adzuna fetcher: no search locations derived from preferred_locations %s — "
                "skipping fetch",
                preferred_locations,
            )
            return []

        all_jobs = []
        seen_urls = set()

        logger.info(
            "Fetching jobs from Adzuna (no API salary filter, will filter to $%s locally)...",
            f"{target_salary:,}",
        )
        logger.info("Searching for %d keywords: %s", len(keywords), ", ".join(keywords))
        logger.info(
            "Searching %d location queries from preferred_locations",
            len(locations),
        )

        # Track if we're hitting pagination limits
        need_more_pages = False

        for keyword_idx, keyword in enumerate(keywords, 1):
            logger.info("Keyword %d/%d: '%s'", keyword_idx, len(keywords), keyword)

            for location_name, location_query in locations:
                jobs_from_location = []
                
                for page in range(1, self.max_pages + 1):
                    try:
                        jobs = self._fetch_page(
                            page,
                            keyword,
                            location_query,
                            api_salary_min,
                            max_days_old,
                        )
                        
                        # Deduplicate by URL
                        new_jobs = 0
                        for job in jobs:
                            if not isinstance(job, dict):
                                continue
                            url = job.get("redirect_url", "")
                            if url and url not in seen_urls:
                                seen_urls.add(url)
                                all_jobs.append(job)
                                jobs_from_location.append(job)
                                new_jobs += 1
                        
                        time.sleep(1.5)  # Rate limiting
                        
                        # If we got a full page, we might be hitting the limit
                        if len(jobs) >= self.results_per_page:
                            need_more_pages = True
                        
                        # If we got less than expected, no need to fetch next page
                        if len(jobs) < self.results_per_page:
                            break
                            
                    except requests.RequestException as e:
                        logger.warning("Error on page %d: %s", page, str(e)[:80])
                        continue
                
                if jobs_from_location:
                    logger.info("%s: %d jobs", location_name, len(jobs_from_location))

        logger.info("Total: %d unique jobs fetched", len(all_jobs))
        
        # Auto-increase pagination if we're hitting limits frequently
        if need_more_pages and self.auto_increase_pages and self.max_pages < 10:
            logger.warning("Hit pagination limit on some searches.")
            logger.warning("Consider running again with max_pages=10 for better coverage.")
            logger.warning("Example: AdzunaFetcher(max_pages=10)")
        
        return all_jobs

    def _fetch_page(
        self,
        page: int,
        what: str,
        where: str,
        salary_min: Optional[int] = None,
        max_days_old: int = 60,
    ) -> list[dict[str, Any]]:
        """
        Fetch a single page of results.

        Args:
            page: Page number (1-indexed)
            what: Search query (job titles/keywords)
            where: Location query
            salary_min: Minimum salary filter
            max_days_old: Maximum age of postings in days (default 60)

        Returns:
            List of job dicts from this page
        """
        # Adzuna API requires page number in URL path, not query params
        url = f"{self.base_url}/{page}"

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": self.results_per_page,
            "what": what,
        }

        # Only add where if it's not empty
        if where:
            params["where"] = where

        # Add salary filter if provided
        if salary_min:
            params["salary_min"] = salary_min

        # Filter to recent postings server-side to maximise coverage
        # within our page budget; value comes from profile.yaml max_job_age_days
        params["max_days_old"] = max_days_old
        
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, dict):
                return []
            return data.get("results") or []
            
        except requests.exceptions.HTTPError as e:
            # Try to get more details from the response
            try:
                error_data = e.response.json()
                raise requests.RequestException(f"API Error: {error_data}") from e
            except (ValueError, AttributeError):
                raise e

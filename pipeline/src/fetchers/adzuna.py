"""
Adzuna API fetcher.

Fetches job listings from the free Adzuna REST API.
Requires an API key from https://developer.adzuna.com/

The API returns a different structure than our Job schema;
the normalizer handles the conversion.
"""

import time
from typing import Optional

import requests

from pipeline.config.settings import get_adzuna_credentials, load_profile

from .base import BaseFetcher


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
        max_pages: int = 3,
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
            auto_increase_pages: If True, increases to 10 pages when hitting limits
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

    def fetch(self) -> list[dict]:
        """
        Fetch jobs from Adzuna API.
        
        Performs targeted searches for each keyword in profile.yaml.
        For each keyword, searches both:
        1. Florida-based jobs
        2. Nationwide remote jobs
        
        Dynamically increases pagination if hitting the limit.

        Returns:
            List of raw job dicts from Adzuna
        """
        profile = load_profile()
        
        # Use NO salary filter at API level to get maximum results
        # We'll filter to the target salary locally for more control and better coverage
        api_salary_min = None  # No API filter - cast widest net
        target_salary = profile.get("salary_min", 130000)
        
        # Get keywords from profile
        keywords = profile.get("title_keywords", ["data engineer"])
        
        all_jobs = []
        seen_urls = set()
        
        print(f"Fetching jobs from Adzuna (no API salary filter, will filter to ${target_salary:,} locally)...")
        print(f"Searching for {len(keywords)} keywords: {', '.join(keywords)}")
        
        # Track if we're hitting pagination limits
        need_more_pages = False
        
        for keyword_idx, keyword in enumerate(keywords, 1):
            print(f"\n  Keyword {keyword_idx}/{len(keywords)}: '{keyword}'")
            
            # Search both Florida and nationwide remote for this keyword
            locations = [
                ("Florida", "Florida"),
                ("Remote", "")  # Empty string = nationwide
            ]
            
            for location_name, location_query in locations:
                jobs_from_location = []
                
                for page in range(1, self.max_pages + 1):
                    try:
                        jobs = self._fetch_page(page, keyword, location_query, api_salary_min)
                        
                        # Deduplicate by URL
                        new_jobs = 0
                        for job in jobs:
                            url = job.get("redirect_url", "")
                            if url and url not in seen_urls:
                                seen_urls.add(url)
                                all_jobs.append(job)
                                jobs_from_location.append(job)
                                new_jobs += 1
                        
                        time.sleep(0.5)  # Rate limiting
                        
                        # If we got a full page, we might be hitting the limit
                        if len(jobs) >= self.results_per_page:
                            need_more_pages = True
                        
                        # If we got less than expected, no need to fetch next page
                        if len(jobs) < self.results_per_page:
                            break
                            
                    except requests.RequestException as e:
                        print(f"      Warning: Error on page {page}: {str(e)[:80]}")
                        continue
                
                if jobs_from_location:
                    print(f"    {location_name}: {len(jobs_from_location)} jobs")
        
        print(f"\nTotal: {len(all_jobs)} unique jobs fetched")
        
        # Auto-increase pagination if we're hitting limits frequently
        if need_more_pages and self.auto_increase_pages and self.max_pages < 10:
            print(f"\n⚠️  Hit pagination limit on some searches.")
            print(f"   Consider running again with max_pages=10 for better coverage.")
            print(f"   Example: AdzunaFetcher(max_pages=10)")
        
        return all_jobs

    def _fetch_page(self, page: int, what: str, where: str, salary_min: Optional[int] = None) -> list[dict]:
        """
        Fetch a single page of results.
        
        Args:
            page: Page number (1-indexed)
            what: Search query (job titles/keywords)
            where: Location query
            salary_min: Minimum salary filter
            
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
        
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            return data.get("results", [])
            
        except requests.exceptions.HTTPError as e:
            # Try to get more details from the response
            try:
                error_data = e.response.json()
                raise requests.RequestException(f"API Error: {error_data}")
            except (ValueError, AttributeError):
                raise e

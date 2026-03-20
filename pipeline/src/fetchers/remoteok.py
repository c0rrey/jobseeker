"""
RemoteOK API fetcher.

Fetches remote job listings from the RemoteOK public JSON API.
No API key required - completely free and open.

API: https://remoteok.com/api
"""

import logging
import time
from typing import Optional

import requests

from pipeline.config.settings import load_profile

from .base import BaseFetcher

logger = logging.getLogger(__name__)


class RemoteOKFetcher(BaseFetcher):
    """Fetches jobs from the RemoteOK API."""

    @property
    def source_type(self) -> str:
        """Return 'api' — RemoteOK is fetched via public JSON API."""
        return "api"

    def __init__(self):
        """Initialize RemoteOK fetcher."""
        self.base_url = "https://remoteok.com/api"

    def fetch(self) -> list[dict]:
        """
        Fetch jobs from RemoteOK API.
        
        RemoteOK returns all jobs in a single request (typically 100+ jobs).
        We filter them locally based on profile preferences.

        Returns:
            List of raw job dicts from RemoteOK
        """
        profile = load_profile()
        
        logger.info("Fetching remote jobs from RemoteOK...")
        
        try:
            # RemoteOK API returns all jobs in one call
            response = requests.get(
                self.base_url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; JobDigestBot/1.0; +mailto:jobs@example.com)'
                },
                timeout=15
            )
            response.raise_for_status()
            
            data = response.json()
            
            # First item is metadata, skip it
            jobs = data[1:] if len(data) > 1 else []
            
            logger.info(f"Retrieved {len(jobs)} jobs from RemoteOK")
            
            # Filter by relevant keywords locally
            # RemoteOK doesn't support server-side filtering beyond tags
            keywords = profile.get("title_keywords", [])
            filtered_jobs = self._filter_by_keywords(jobs, keywords)
            
            logger.info(f"{len(filtered_jobs)} jobs match keywords: {', '.join(keywords[:3])}")
            
            return filtered_jobs
            
        except requests.RequestException as e:
            logger.error(f"Error fetching from RemoteOK: {e}")
            return []

    def _filter_by_keywords(self, jobs: list[dict], keywords: list[str]) -> list[dict]:
        """
        Filter jobs by keywords in title, tags, or description.
        
        Args:
            jobs: List of jobs from RemoteOK
            keywords: List of keywords from profile
            
        Returns:
            Filtered list of jobs
        """
        if not keywords:
            return jobs
        
        filtered = []
        for job in jobs:
            position = job.get("position", "").lower()
            tags = [tag.lower() for tag in job.get("tags", [])]
            description = job.get("description", "").lower()
            
            # Check if any keyword matches
            for keyword in keywords:
                keyword_lower = keyword.lower()
                if (keyword_lower in position or 
                    keyword_lower in description or
                    any(keyword_lower in tag for tag in tags)):
                    filtered.append(job)
                    break
        
        return filtered

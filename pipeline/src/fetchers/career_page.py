"""
Career page crawler using stored scrape_strategy CSS selectors.

Reads active career_page_configs from the database, fetches each career
page URL with requests, and applies the stored scrape_strategy (a JSON
object containing CSS selector rules) to extract job listings.

Broken-config detection:
  When a config has previously been crawled (last_crawled_at IS NOT NULL)
  and the current crawl returns 0 results, the config's status is updated
  to 'broken' in the database.  First-time crawls (last_crawled_at IS NULL)
  that return 0 results are not flagged as broken because the page may
  simply have no openings right now.

The scrape_strategy JSON structure expected by this fetcher:
  {
    "job_container": ".jobs-list li",   // CSS selector for each job element
    "title":         ".job-title",      // child selector for job title
    "url":           "a",               // child selector for apply link (href used)
    "location":      ".location",       // (optional) child selector for location
    "description":   ".description"     // (optional) child selector for description
  }
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

import requests

from .base import BaseFetcher

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (compatible; jseeker-crawler/2.0; +mailto:jobs@example.com)"
)


class CareerPageFetcher(BaseFetcher):
    """
    Crawls company career pages and extracts job listings via CSS selectors.

    Accepts a live SQLite connection used to read career_page_configs and to
    update last_crawled_at / status after each crawl attempt.  The caller
    owns the connection and is responsible for closing it.
    """

    @property
    def source_type(self) -> str:
        """Return 'career_page' — jobs come from direct HTML crawling."""
        return "career_page"

    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        Initialize the career page fetcher.

        Args:
            conn: Open SQLite connection (see database.get_connection).
                  The fetcher does NOT close this connection.
        """
        self._conn = conn

    def fetch(self) -> list[dict[str, Any]]:
        """
        Crawl all active career_page_configs and return extracted job dicts.

        For each active config:
        1. Fetch the HTML at config.url
        2. Apply CSS selectors from config.scrape_strategy
        3. Update last_crawled_at to now
        4. Flag the config as 'broken' if 0 results on a previously-crawled config

        Returns:
            Flat list of raw job dicts, each containing at minimum 'title', 'url',
            and '_career_page_config_id' for normalizer context.
        """
        configs = self._query_active_configs()
        if not configs:
            logger.info("CareerPageFetcher: no active career_page_configs found")
            return []

        all_jobs: list[dict[str, Any]] = []

        for config in configs:
            config_id: int = config["id"]
            url: str = config["url"]
            last_crawled_at: Optional[str] = config["last_crawled_at"]
            strategy_json: Optional[str] = config["scrape_strategy"]
            raw_company_name: Optional[str] = config["company_name"]

            if raw_company_name is None:
                logger.warning(
                    "CareerPageFetcher: config %d has no matching company row "
                    "(orphaned company_id) — crawling with empty company name",
                    config_id,
                )
            company_name: str = raw_company_name or ""

            if not strategy_json:
                logger.warning(
                    "CareerPageFetcher: config %d has no scrape_strategy, skipping",
                    config_id,
                )
                self._update_last_crawled(config_id)
                continue

            try:
                strategy: dict[str, str] = json.loads(strategy_json)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "CareerPageFetcher: invalid scrape_strategy JSON for config %d: %s",
                    config_id,
                    exc,
                )
                self._update_last_crawled(config_id)
                continue

            html = self._fetch_html(url)
            if html is None:
                # HTTP error — do not update timestamps or flag broken;
                # transient network errors should not flag a page as broken.
                continue

            jobs = self._extract_jobs(html, strategy, config_id, url)

            # Annotate each job with company name for the normalizer
            jobs = [{**job, "_company_name": company_name} for job in jobs]

            # Update timestamp regardless of result count
            self._update_last_crawled(config_id)

            # Broken-config detection: only flag if this config has been crawled before
            if len(jobs) == 0 and last_crawled_at is not None:
                logger.warning(
                    "CareerPageFetcher: 0 results for config %d (previously crawled "
                    "at %s) — marking as broken",
                    config_id,
                    last_crawled_at,
                )
                self._mark_broken(config_id)
            else:
                all_jobs.extend(jobs)

        logger.info(
            "CareerPageFetcher: %d total jobs extracted across all configs",
            len(all_jobs),
        )
        return all_jobs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _query_active_configs(self) -> list[sqlite3.Row]:
        """
        Return all career_page_configs with status='active', joined with company name.

        Returns:
            List of sqlite3.Row objects including company_name.
        """
        sql = """
            SELECT cpc.id, cpc.url, cpc.scrape_strategy, cpc.last_crawled_at,
                   c.name AS company_name
            FROM career_page_configs cpc
            LEFT JOIN companies c ON c.id = cpc.company_id
            WHERE cpc.status = 'active'
        """
        try:
            return self._conn.execute(sql).fetchall()
        except sqlite3.Error as exc:
            logger.error("CareerPageFetcher: DB query failed: %s", exc)
            return []

    def _fetch_html(self, url: str) -> Optional[str]:
        """
        Fetch HTML content of the given URL.

        Args:
            url: The career page URL to fetch.

        Returns:
            HTML string, or None on HTTP/network error.
        """
        try:
            response = requests.get(
                url,
                headers={"User-Agent": _USER_AGENT},
                timeout=20,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            logger.warning(
                "CareerPageFetcher: failed to fetch '%s': %s", url, exc
            )
            return None

    def _extract_jobs(
        self,
        html: str,
        strategy: dict[str, str],
        config_id: int,
        base_url: str,
    ) -> list[dict[str, Any]]:
        """
        Apply CSS selector strategy to extract job listings from HTML.

        Uses BeautifulSoup when available (preferred).  Falls back to an
        empty list with a warning when bs4 is not installed, to avoid a
        hard crash in environments without the optional dependency.

        Args:
            html: Raw HTML string of the career page.
            strategy: Dict of CSS selectors parsed from scrape_strategy JSON.
            config_id: Config ID used for annotating results.
            base_url: Page URL, used to resolve relative job links.

        Returns:
            List of extracted job dicts.
        """
        try:
            from bs4 import BeautifulSoup  # type: ignore[import-untyped]
        except ImportError:
            logger.warning(
                "CareerPageFetcher: beautifulsoup4 is not installed. "
                "Install it with: pip install beautifulsoup4"
            )
            return []

        container_selector = strategy.get("job_container", "")
        if not container_selector:
            logger.warning(
                "CareerPageFetcher: config %d scrape_strategy missing "
                "'job_container' key",
                config_id,
            )
            return []

        soup = BeautifulSoup(html, "html.parser")
        containers = soup.select(container_selector)

        jobs: list[dict[str, Any]] = []
        for elem in containers:
            job = self._parse_job_element(elem, strategy, config_id, base_url)
            if job:
                jobs.append(job)

        return jobs

    def _parse_job_element(
        self,
        elem: Any,
        strategy: dict[str, str],
        config_id: int,
        base_url: str,
    ) -> Optional[dict[str, Any]]:
        """
        Extract a single job dict from one matched HTML element.

        Args:
            elem: A BeautifulSoup tag matching the job_container selector.
            strategy: CSS selector strategy dict.
            config_id: Config row ID for annotation.
            base_url: Career page URL for resolving relative hrefs.

        Returns:
            Job dict, or None if the minimum required fields are missing.
        """
        title = self._select_text(elem, strategy.get("title", ""))
        url = self._select_href(elem, strategy.get("url", ""), base_url)

        if not title and not url:
            return None

        job: dict[str, Any] = {
            "title": title or "",
            "url": url or "",
            "_career_page_config_id": config_id,
            "source": "career_page",
        }

        location_sel = strategy.get("location", "")
        if location_sel:
            job["location"] = self._select_text(elem, location_sel)

        description_sel = strategy.get("description", "")
        if description_sel:
            job["description"] = self._select_text(elem, description_sel)

        return job

    @staticmethod
    def _select_text(elem: Any, selector: str) -> str:
        """
        Find first element matching selector and return its stripped text.

        Returns empty string when selector is empty or no element is found.
        """
        if not selector:
            return ""
        found = elem.select_one(selector)
        return found.get_text(strip=True) if found else ""

    @staticmethod
    def _select_href(elem: Any, selector: str, base_url: str) -> str:
        """
        Find first element matching selector and return its href attribute.

        Handles absolute and root-relative URLs.  Returns empty string when
        not found.
        """
        if not selector:
            return ""
        found = elem.select_one(selector)
        if not found:
            return ""
        href: str = found.get("href", "")
        if not href:
            return ""
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            # Derive origin from base_url
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{href}"
        return href

    def _update_last_crawled(self, config_id: int) -> None:
        """
        Set last_crawled_at to the current UTC timestamp for a config row.

        Args:
            config_id: Primary key of the career_page_configs row.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            self._conn.execute(
                "UPDATE career_page_configs SET last_crawled_at = ? WHERE id = ?",
                (now, config_id),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            logger.error(
                "CareerPageFetcher: failed to update last_crawled_at for config %d: %s",
                config_id,
                exc,
            )

    def _mark_broken(self, config_id: int) -> None:
        """
        Update config status to 'broken' in the database.

        Args:
            config_id: Primary key of the career_page_configs row.
        """
        try:
            self._conn.execute(
                "UPDATE career_page_configs SET status = 'broken' WHERE id = ?",
                (config_id,),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            logger.error(
                "CareerPageFetcher: failed to mark config %d as broken: %s",
                config_id,
                exc,
            )

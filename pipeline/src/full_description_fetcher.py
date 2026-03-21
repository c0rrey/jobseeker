"""
Fetch full job descriptions from job posting URLs.

Handles HTML parsing and text extraction for various job boards. Ported from
V1.5 (~/projects/jobseeker-v1.5/src/full_description_fetcher.py) and adapted
to V2 conventions: uses the standard ``logging`` module and Google-style
docstrings.
"""

from __future__ import annotations

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class FullDescriptionFetcher:
    """Fetch and parse full job descriptions from URLs.

    Attributes:
        rate_limit: Seconds to wait between HTTP requests.
        last_request_time: Monotonic timestamp of the last completed request.
        session: Shared ``requests.Session`` with a browser-like User-Agent.
    """

    def __init__(self, rate_limit_seconds: float = 1.0) -> None:
        """Initialize the fetcher.

        Args:
            rate_limit_seconds: Minimum seconds between outbound requests.
                Defaults to 1.0.
        """
        self.rate_limit: float = rate_limit_seconds
        self.last_request_time: float = 0.0
        self.session: requests.Session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36"
                )
            }
        )

    def _rate_limit_wait(self) -> None:
        """Enforce rate limiting between requests.

        Sleeps for the remainder of the rate-limit window if the previous
        request completed fewer than ``self.rate_limit`` seconds ago.
        """
        elapsed = time.monotonic() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.monotonic()

    def _clean_text(self, text: str) -> str:
        """Normalise whitespace in extracted text.

        Args:
            text: Raw text extracted from HTML.

        Returns:
            Text with collapsed whitespace and trimmed edges.
        """
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        return text.strip()

    def _extract_adzuna_description(self, soup: BeautifulSoup) -> str | None:
        """Extract a job description from an Adzuna job page.

        Tries multiple selectors in order of specificity, falling back to the
        full ``<main>`` or ``<body>`` content with boilerplate stripped.

        Args:
            soup: Parsed HTML of the Adzuna job page.

        Returns:
            Cleaned description text, or ``None`` if nothing was found.
        """
        desc_div = soup.find("div", class_="job-description")
        if desc_div:
            return self._clean_text(desc_div.get_text())

        article = soup.find("article")
        if article:
            return self._clean_text(article.get_text())

        desc = soup.find(attrs={"itemprop": "description"})
        if desc:
            return self._clean_text(desc.get_text())

        main_content = soup.find("main") or soup.find("body")
        if main_content:
            for element in main_content(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            return self._clean_text(main_content.get_text())

        return None

    def _extract_remoteok_description(self, soup: BeautifulSoup) -> str | None:
        """Extract a job description from a RemoteOK job page.

        Args:
            soup: Parsed HTML of the RemoteOK job page.

        Returns:
            Cleaned description text, or ``None`` if nothing was found.
        """
        desc = soup.find("div", class_="description")
        if desc:
            return self._clean_text(desc.get_text())

        main = soup.find("main")
        if main:
            for element in main(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            return self._clean_text(main.get_text())

        return None

    def _extract_generic_description(
        self, soup: BeautifulSoup, url: str
    ) -> str | None:
        """Extract a job description from an unrecognised job board.

        Iterates a prioritised list of common CSS selectors and returns the
        first match whose cleaned text exceeds 200 characters.

        Args:
            soup: Parsed HTML of the job page.
            url: Original URL (reserved for future source-specific heuristics).

        Returns:
            Cleaned description text, or ``None`` if nothing was found.
        """
        selectors: list[tuple[str, dict]] = [
            ("div", {"class": re.compile(r"job[-_]description", re.I)}),
            ("div", {"class": re.compile(r"description", re.I)}),
            ("section", {"class": re.compile(r"job[-_]details", re.I)}),
            ("article", {}),
            ("main", {}),
        ]

        for tag, attrs in selectors:
            element = soup.find(tag, attrs)
            if element:
                for unwanted in element(
                    ["script", "style", "nav", "header", "footer", "aside"]
                ):
                    unwanted.decompose()

                text = self._clean_text(element.get_text())
                if len(text) > 200:
                    return text

        return None

    def fetch_full_description(self, url: str, source: str) -> str | None:
        """Fetch and parse the full job description at *url*.

        Enforces rate limiting before each request. Network errors and HTML
        parse failures are caught and logged; ``None`` is returned so callers
        can treat a failed fetch as a no-op.

        Args:
            url: Direct URL to the job posting page.
            source: Source identifier (e.g. ``"adzuna"``, ``"remoteok"``).
                Used to select the appropriate extraction strategy.

        Returns:
            Full description text (at least 100 characters), or ``None`` if
            the fetch failed or yielded insufficient content.
        """
        if not url:
            logger.warning("Skipping fetch: url is None or empty")
            return None

        try:
            self._rate_limit_wait()

            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            if "adzuna" in url.lower() or source == "adzuna":
                description = self._extract_adzuna_description(soup)
            elif "remoteok" in url.lower() or source == "remoteok":
                description = self._extract_remoteok_description(soup)
            else:
                description = self._extract_generic_description(soup, url)

            if description and len(description) > 100:
                return description

            logger.warning("Insufficient content extracted from %s", url)
            return None

        except requests.RequestException as exc:
            logger.warning("Failed to fetch %s: %s", url, exc)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error parsing %s: %s", url, exc)
            return None

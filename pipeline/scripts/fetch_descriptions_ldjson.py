"""
Fetch full job descriptions from Adzuna /details/ pages using LD+JSON extraction.

Handles jobs where the standard HTML extraction failed (typically /land/ad/ redirect
URLs that return 403). Constructs the /details/{adzuna_id} URL from the raw_json
stored at fetch time, then extracts the description from the application/ld+json
<script> tag embedded in the page.

Usage::

    python -m pipeline.scripts.fetch_descriptions_ldjson [--db PATH] [--rate-limit SECS]
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time

import requests
from bs4 import BeautifulSoup

from pipeline.config.settings import get_db_path
from pipeline.src.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_DETAILS_URL = "https://www.adzuna.com/details/{adzuna_id}"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _extract_ldjson_description(html: str) -> str | None:
    """Extract job description from LD+JSON script tag.

    Args:
        html: Raw HTML content of the Adzuna details page.

    Returns:
        Description text if found, None otherwise.
    """
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        text = script.string or ""
        if not text.strip().startswith("{"):
            continue
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            continue
        desc = data.get("description")
        if desc and isinstance(desc, str) and len(desc) > 100:
            return desc.strip()
    return None


def run(db_path: str, rate_limit: float = 1.0) -> int:
    """Fetch descriptions for jobs missing full_description using LD+JSON extraction.

    Targets Pass 2 scored jobs that still have NULL full_description.
    Constructs /details/{id} URLs from the Adzuna job ID stored in raw_json.

    Args:
        db_path: Path to the SQLite database.
        rate_limit: Seconds between requests.

    Returns:
        Exit code: 0 on success, 1 on fatal error.
    """
    try:
        conn = get_connection(db_path)
    except Exception as exc:
        logger.error("Cannot open database %s: %s", db_path, exc)
        return 1

    try:
        rows = conn.execute(
            """
            SELECT j.id, j.raw_json
            FROM jobs j
            INNER JOIN score_dimensions sd ON sd.job_id = j.id AND sd.pass = 1 AND sd.overall > 0
            WHERE j.full_description IS NULL
              AND j.source = 'adzuna'
            ORDER BY j.id
            """
        ).fetchall()

        total = len(rows)
        successful = 0
        failed = 0

        logger.info("LD+JSON fetcher: %d jobs to process", total)

        if total == 0:
            logger.info("Nothing to fetch.")
            return 0

        session = requests.Session()
        session.headers.update(_HEADERS)

        for row in rows:
            db_id = row[0]
            raw_json_str = row[1]

            # Extract Adzuna job ID from raw_json
            adzuna_id = None
            if raw_json_str:
                try:
                    raw = json.loads(raw_json_str)
                    adzuna_id = raw.get("id")
                except (json.JSONDecodeError, TypeError):
                    pass

            if not adzuna_id:
                logger.warning("  job id=%d: no Adzuna ID in raw_json, skipping", db_id)
                failed += 1
                continue

            url = _DETAILS_URL.format(adzuna_id=adzuna_id)
            logger.info("  job id=%d: fetching %s", db_id, url)

            try:
                time.sleep(rate_limit)
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
            except requests.RequestException as exc:
                logger.warning("  job id=%d: HTTP error: %s", db_id, exc)
                failed += 1
                continue

            desc = _extract_ldjson_description(resp.text)
            if desc:
                conn.execute(
                    "UPDATE jobs SET full_description = ? WHERE id = ?",
                    (desc, db_id),
                )
                conn.commit()
                successful += 1
                logger.info("  job id=%d: saved %d chars", db_id, len(desc))
            else:
                failed += 1
                logger.warning("  job id=%d: no LD+JSON description found", db_id)

        logger.info(
            "LD+JSON fetcher complete: total=%d successful=%d failed=%d",
            total, successful, failed,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch descriptions via LD+JSON extraction")
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--rate-limit", type=float, default=0.5, help="Seconds between requests")
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    exit(run(db_path, args.rate_limit))

"""
Standalone script: fetch full job descriptions for Pass 1 survivors.

Queries the database for jobs that passed Pass 1 scoring (score_dimensions
pass=1, overall > 0) and whose full_description is still NULL, then fetches
the full HTML description from each job's URL and writes it back.

Usage::

    python -m pipeline.scripts.fetch_descriptions [--db PATH] [--rate-limit SECS]

Environment variables:
    DB_PATH: Override the default database path (same as pipeline CLI).

Exit codes:
    0: Success (even if some individual fetches failed).
    1: Fatal error (e.g. database not accessible).
"""

from __future__ import annotations

import argparse
import logging
import sys

from pipeline.config.settings import get_db_path
from pipeline.src.database import get_connection
from pipeline.src.full_description_fetcher import FullDescriptionFetcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Pass constant matching pipeline/src/scorer.py PASS_1
_PASS_1: int = 1


def _get_pass1_survivors_without_description(
    conn, *, limit: int | None = None
) -> list[dict]:
    """Return Pass 1 survivor jobs that lack a full description.

    A job qualifies when:
    - It has a Pass 1 score_dimensions row with ``overall > 0``.
    - Its ``full_description`` column is NULL (not yet fetched or previously
      failed).

    Args:
        conn: Open SQLite connection (Row factory expected).
        limit: Optional cap on the number of rows returned.

    Returns:
        List of dicts with keys: ``id``, ``url``, ``source``.
    """
    sql = """
        SELECT
            j.id,
            j.url,
            j.source
        FROM jobs j
        INNER JOIN score_dimensions sd
            ON sd.job_id = j.id
            AND sd.pass = :pass1
            AND sd.overall > 0
        WHERE j.full_description IS NULL
        ORDER BY sd.overall DESC, j.id
    """
    if limit is not None and limit > 0:
        sql += f" LIMIT {int(limit)}"
    cursor = conn.execute(sql, {"pass1": _PASS_1})
    return [dict(row) for row in cursor.fetchall()]


def _save_description(conn, db_id: int, text: str) -> None:
    """Persist a fetched description to the database.

    Args:
        conn: Open SQLite connection.
        db_id: Primary key of the jobs row to update.
        text: Full description text to write.
    """
    conn.execute(
        "UPDATE jobs SET full_description = ? WHERE id = ?",
        (text, db_id),
    )
    conn.commit()


def run(db_path: str, rate_limit: float = 1.0, limit: int | None = None) -> int:
    """Fetch full descriptions for all eligible Pass 1 survivor jobs.

    Args:
        db_path: Filesystem path to the SQLite database.
        rate_limit: Minimum seconds between HTTP requests.
        limit: Optional cap on jobs processed in this run.

    Returns:
        Exit code: 0 on success, 1 on fatal error.
    """
    try:
        conn = get_connection(db_path)
    except Exception as exc:
        logger.error("Cannot open database %s: %s", db_path, exc)
        return 1

    try:
        try:
            jobs = _get_pass1_survivors_without_description(conn, limit=limit)
        except Exception as exc:
            logger.error("Failed to query Pass 1 survivors: %s", exc)
            return 1

        total = len(jobs)
        successful = 0
        failed = 0

        logger.info(
            "fetch_descriptions: total=%d to fetch, rate_limit=%.1fs",
            total,
            rate_limit,
        )

        if total == 0:
            logger.info("Nothing to fetch — all Pass 1 survivors already have descriptions.")
            return 0

        fetcher = FullDescriptionFetcher(rate_limit_seconds=rate_limit)

        for job in jobs:
            db_id: int = job["id"]
            url: str = job["url"]
            source: str = job["source"]

            logger.info("Fetching job id=%d url=%s", db_id, url)
            text = fetcher.fetch_full_description(url=url, source=source)

            if text:
                try:
                    _save_description(conn, db_id, text)
                    successful += 1
                    logger.info("  Saved %d chars for job id=%d", len(text), db_id)
                except Exception as exc:
                    logger.warning("  DB write failed for job id=%d: %s", db_id, exc)
                    failed += 1
            else:
                failed += 1
                logger.warning("  No description retrieved for job id=%d", db_id)

        logger.info(
            "fetch_descriptions complete: total=%d successful=%d failed=%d",
            total,
            successful,
            failed,
        )

        return 0
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch full job descriptions for Pass 1 survivors."
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help="Path to SQLite database. Defaults to DB_PATH env var or data/jobs.db.",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.0,
        metavar="SECS",
        help="Minimum seconds between HTTP requests (default: 1.0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Cap the number of jobs fetched in this run.",
    )
    return parser


def main() -> None:
    """Entry point for the fetch_descriptions script."""
    parser = _build_parser()
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    sys.exit(run(db_path=db_path, rate_limit=args.rate_limit, limit=args.limit))


if __name__ == "__main__":
    main()

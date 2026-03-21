"""
Standalone script: auto-discover companies from Pass 1 survivors.

Queries the database for jobs that passed Pass 1 scoring (score_dimensions
pass=1, overall > 0), identifies distinct company names not yet present in the
companies table, calls discover_company() for each new name, and triggers
run_enrichment() once all discoveries are complete.

The script is idempotent: companies already in the companies table are excluded
by a LEFT JOIN, so re-running with no new Pass 1 survivors produces zero new
rows.

Usage::

    python -m pipeline.scripts.discover_companies [--db PATH]

    # or directly:
    python pipeline/scripts/discover_companies.py --db data/jseeker.db

Environment variables:
    DB_PATH: Override the default database path (same as pipeline CLI).

Exit codes:
    0: Success (even if some individual discoveries or enrichments failed).
    1: Fatal error (e.g. database not accessible, query failure).
"""

from __future__ import annotations

import argparse
import logging
import sys

from pipeline.config.settings import get_db_path
from pipeline.src.company_discovery import discover_company
from pipeline.src.database import get_connection
from pipeline.src.enrichment.orchestrator import run_enrichment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Pass constant matching pipeline/src/scorer.py PASS_1
_PASS_1: int = 1

_QUERY_NEW_COMPANIES = """
    SELECT DISTINCT j.company
    FROM jobs j
    INNER JOIN score_dimensions sd
        ON sd.job_id = j.id
        AND sd.pass = :pass1
        AND sd.overall > 0
    LEFT JOIN companies c
        ON c.name = j.company
    WHERE c.id IS NULL
    ORDER BY j.company
"""

_QUERY_EXISTING_COUNT = """
    SELECT COUNT(DISTINCT j.company) AS cnt
    FROM jobs j
    INNER JOIN score_dimensions sd
        ON sd.job_id = j.id
        AND sd.pass = :pass1
        AND sd.overall > 0
    INNER JOIN companies c
        ON c.name = j.company
"""


def _get_new_survivor_companies(conn) -> list[str]:
    """Return distinct company names from Pass 1 survivors not yet in companies table.

    A company qualifies when:
    - At least one of its jobs has a Pass 1 score_dimensions row with overall > 0.
    - The company name has no matching row in the companies table.

    Args:
        conn: Open SQLite connection (Row factory expected).

    Returns:
        Sorted list of company name strings to discover.
    """
    cursor = conn.execute(_QUERY_NEW_COMPANIES, {"pass1": _PASS_1})
    return [row["company"] for row in cursor.fetchall()]


def _get_existing_survivor_company_count(conn) -> int:
    """Count distinct company names from Pass 1 survivors already in companies table.

    Args:
        conn: Open SQLite connection (Row factory expected).

    Returns:
        Integer count of survivor companies already present.
    """
    row = conn.execute(_QUERY_EXISTING_COUNT, {"pass1": _PASS_1}).fetchone()
    return row["cnt"] if row else 0


def run(db_path: str) -> int:
    """Discover companies from Pass 1 survivors and trigger enrichment.

    Queries the database for Pass 1 survivor jobs (pass=1, overall > 0),
    collects distinct company names not yet in the companies table, calls
    discover_company() for each, then calls run_enrichment() once to enrich
    all newly created company rows.

    Args:
        db_path: Filesystem path to the SQLite database.

    Returns:
        Exit code: 0 on success, 1 on fatal error.
    """
    try:
        conn = get_connection(db_path)
    except Exception as exc:
        logger.error("Cannot open database %s: %s", db_path, exc)
        return 1

    try:
        new_companies = _get_new_survivor_companies(conn)
        already_existing_count = _get_existing_survivor_company_count(conn)
    except Exception as exc:
        logger.error("Failed to query Pass 1 survivors: %s", exc)
        conn.close()
        return 1

    logger.info(
        "discover_companies: new=%d to discover, already_existing=%d",
        len(new_companies),
        already_existing_count,
    )

    if not new_companies:
        logger.info(
            "Nothing to discover — %d Pass 1 survivor %s already in companies table.",
            already_existing_count,
            "company" if already_existing_count == 1 else "companies",
        )
        conn.close()
        return 0

    discovered = 0
    failed = 0

    for company_name in new_companies:
        logger.info("Discovering company: '%s'", company_name)
        try:
            result = discover_company(company_name, conn)
        except Exception as exc:
            logger.warning("  Unexpected error discovering '%s': %s", company_name, exc)
            failed += 1
            continue

        if result is not None:
            discovered += 1
            logger.info("  Discovered '%s' (company_id=%s)", company_name, result.company_id)
        else:
            failed += 1
            logger.warning("  Discovery returned None for '%s' — skipping.", company_name)

    logger.info(
        "discover_companies: discovery complete — discovered=%d failed=%d",
        discovered,
        failed,
    )

    if discovered > 0:
        logger.info("Running enrichment for newly discovered companies...")
        try:
            enrichment_summary = run_enrichment(conn)
            logger.info(
                "Enrichment complete: companies_processed=%d sources_succeeded=%s sources_failed=%s",
                enrichment_summary["companies_processed"],
                enrichment_summary["sources_succeeded"],
                enrichment_summary["sources_failed"],
            )
        except Exception as exc:
            logger.error("Enrichment run failed: %s", exc)
            # Non-fatal: discovery succeeded, enrichment can be retried.

    logger.info(
        "discover_companies summary: new_discovered=%d already_existing=%d discovery_failed=%d",
        discovered,
        already_existing_count,
        failed,
    )

    conn.close()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-discover companies from Pass 1 survivors and trigger enrichment."
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help="Path to SQLite database. Defaults to DB_PATH env var or data/jobs.db.",
    )
    return parser


def main() -> None:
    """Entry point for the discover_companies script."""
    parser = _build_parser()
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    sys.exit(run(db_path=db_path))


if __name__ == "__main__":
    main()

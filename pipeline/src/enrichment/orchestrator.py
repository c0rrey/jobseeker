"""
Enrichment orchestrator module.

Coordinates two enrichment sources (Glassdoor via RapidAPI and levels.fyi)
for every company in the database that has no enrichment data or whose
enrichment is stale (older than 30 days).

Each source is called independently. A failure in one source is logged and
counted but does not prevent the remaining sources from running for the same
company. After all sources have been attempted, ``enriched_at`` is set to the
current UTC timestamp regardless of partial failures.

Includes per-source rate-limit constants and simple exponential backoff so
that transient API errors are retried before being recorded as failures.

Public API:
    run_enrichment(db_connection) -> EnrichmentSummary
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone
from typing import Callable, TypedDict

import pipeline.src.enrichment.glassdoor_rapidapi as _glassdoor_mod
import pipeline.src.enrichment.levelsfy as _levelsfy_mod

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Staleness threshold
# ---------------------------------------------------------------------------

_STALE_DAYS: int = 30

# ---------------------------------------------------------------------------
# Per-source rate-limit constants (seconds between successive calls to the
# same source, applied as the base delay for exponential backoff retries).
# ---------------------------------------------------------------------------

_RATE_LIMIT_GLASSDOOR: float = 0.5
_RATE_LIMIT_LEVELSFY: float = 0.25

# Maximum number of retry attempts per source per company (not counting the
# initial attempt).
_MAX_RETRIES: int = 2

# Upper bound for the computed backoff delay (seconds).
_MAX_BACKOFF: float = 8.0

# ---------------------------------------------------------------------------
# Source dispatch table
# ---------------------------------------------------------------------------

#: Each entry is (source_name, module_object, base_rate_limit_seconds).
#:
#: Callables are accessed via ``module.enrich`` at runtime rather than stored
#: as direct references so that test patches on the module object take effect
#: without needing to patch the ``_SOURCES`` list itself.
_SOURCES: list[tuple[str, object, float]] = [
    ("glassdoor", _glassdoor_mod, _RATE_LIMIT_GLASSDOOR),
    # levels.fyi disabled — /api/company endpoint returns 404 as of 2026-03.
    # ("levelsfy", _levelsfy_mod, _RATE_LIMIT_LEVELSFY),
]


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


class EnrichmentSummary(TypedDict):
    """Summary dict returned by :func:`run_enrichment`.

    Attributes:
        companies_processed: Total number of companies that were processed
            (had enrichment attempted).
        sources_succeeded: Map of source name to number of companies for
            which that source returned True.
        sources_failed: Map of source name to number of companies for which
            that source returned False (including all retries exhausted).
    """

    companies_processed: int
    sources_succeeded: dict[str, int]
    sources_failed: dict[str, int]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _query_companies_needing_enrichment(
    conn: sqlite3.Connection,
) -> list[tuple[int, str]]:
    """Return (id, name) pairs for companies that need enrichment.

    A company needs enrichment when:
    - ``enriched_at`` IS NULL, or
    - ``enriched_at`` is older than :data:`_STALE_DAYS` days.

    Args:
        conn: Open SQLite connection to the V2 pipeline database.

    Returns:
        List of ``(company_id, company_name)`` tuples in ascending ``id`` order.
    """
    rows = conn.execute(
        """
        SELECT id, name
        FROM   companies
        WHERE  enriched_at IS NULL
               OR enriched_at < datetime('now', :threshold)
        ORDER  BY id ASC
        """,
        {"threshold": f"-{_STALE_DAYS} days"},
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def _call_with_backoff(
    source_name: str,
    enrich_fn: Callable[[int, str, sqlite3.Connection], bool],
    company_id: int,
    company_name: str,
    conn: sqlite3.Connection,
    base_delay: float,
) -> bool:
    """Call an enrich function with exponential backoff on failure.

    Attempts the call up to ``1 + _MAX_RETRIES`` times.  On each failure the
    delay doubles (starting from ``base_delay``), capped at
    :data:`_MAX_BACKOFF`.  If the function raises an uncaught exception it is
    treated as a failure and logged; ``False`` is returned.

    Args:
        source_name: Human-readable source label used in log messages.
        enrich_fn: The enrichment callable to invoke.
        company_id: Primary key of the company row passed to ``enrich_fn``.
        company_name: Company display name passed to ``enrich_fn``.
        conn: Open SQLite connection passed to ``enrich_fn``.
        base_delay: Initial sleep duration (seconds) before the first retry.

    Returns:
        True if any attempt returned True; False if all attempts failed.
    """
    for attempt in range(1 + _MAX_RETRIES):
        try:
            result = enrich_fn(company_id, company_name, conn)
        except Exception:
            logger.exception(
                "Unexpected exception in %s enrichment for '%s' (attempt %d/%d).",
                source_name,
                company_name,
                attempt + 1,
                1 + _MAX_RETRIES,
            )
            result = False

        if result:
            return True

        # Only sleep before a retry, not after the final attempt.
        if attempt < _MAX_RETRIES:
            sleep_secs = min(base_delay * (2 ** attempt), _MAX_BACKOFF)
            logger.debug(
                "%s enrichment returned False for '%s' (attempt %d/%d). "
                "Retrying in %.1fs.",
                source_name,
                company_name,
                attempt + 1,
                1 + _MAX_RETRIES,
                sleep_secs,
            )
            time.sleep(sleep_secs)

    logger.warning(
        "%s enrichment failed for '%s' after %d attempt(s).",
        source_name,
        company_name,
        1 + _MAX_RETRIES,
    )
    return False


def _update_enriched_at(conn: sqlite3.Connection, company_id: int) -> None:
    """Stamp ``enriched_at`` with the current UTC time for the given company.

    Args:
        conn: Open SQLite connection to the V2 pipeline database.
        company_id: The primary key of the company row to update.
    """
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "UPDATE companies SET enriched_at = ? WHERE id = ?",
        (now, company_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_enrichment(db_connection: sqlite3.Connection) -> EnrichmentSummary:
    """Run all enrichment sources for every company needing enrichment.

    Queries the database for companies where ``enriched_at`` is NULL or older
    than :data:`_STALE_DAYS` days. For each such company, calls Glassdoor
    (via RapidAPI) and levels.fyi enrichment functions independently with
    exponential backoff. A failure in any single source is logged and counted
    but does not prevent the remaining sources from running.

    After all sources have been attempted for a company, the ``enriched_at``
    timestamp is updated to the current UTC time regardless of which sources
    succeeded or failed.

    Args:
        db_connection: Open SQLite connection to the V2 pipeline database.
            The caller is responsible for opening and closing this connection.

    Returns:
        An :class:`EnrichmentSummary` dict with three keys:

        - ``companies_processed`` (int): number of companies that had
          enrichment attempted.
        - ``sources_succeeded`` (dict[str, int]): per-source success counts.
        - ``sources_failed`` (dict[str, int]): per-source failure counts.

    Example::

        conn = get_connection("data/jobs.db")
        summary = run_enrichment(conn)
        print(summary["companies_processed"])   # e.g. 12
        print(summary["sources_succeeded"])     # {"glassdoor": 10, "levelsfy": 11}
        print(summary["sources_failed"])        # {"glassdoor": 2, "levelsfy": 1}
    """
    source_names = [name for name, _, _ in _SOURCES]
    succeeded: dict[str, int] = {name: 0 for name in source_names}
    failed: dict[str, int] = {name: 0 for name in source_names}

    companies = _query_companies_needing_enrichment(db_connection)
    logger.info(
        "Enrichment run started: %d %s need enrichment.",
        len(companies),
        "company" if len(companies) == 1 else "companies",
    )

    for company_id, company_name in companies:
        logger.debug("Enriching company: '%s' (id=%d).", company_name, company_id)

        for source_name, module, rate_limit in _SOURCES:
            # Access enrich via the module object at call time so that
            # test patches on the module attribute take effect.
            enrich_fn: Callable[[int, str, sqlite3.Connection], bool] = module.enrich  # type: ignore[attr-defined]
            ok = _call_with_backoff(
                source_name,
                enrich_fn,
                company_id,
                company_name,
                db_connection,
                base_delay=rate_limit,
            )
            if ok:
                succeeded[source_name] += 1
            else:
                failed[source_name] += 1

        _update_enriched_at(db_connection, company_id)

    summary: EnrichmentSummary = {
        "companies_processed": len(companies),
        "sources_succeeded": succeeded,
        "sources_failed": failed,
    }
    logger.info(
        "Enrichment run complete. Processed %d %s. Succeeded: %s. Failed: %s.",
        summary["companies_processed"],
        "company" if summary["companies_processed"] == 1 else "companies",
        summary["sources_succeeded"],
        summary["sources_failed"],
    )
    return summary

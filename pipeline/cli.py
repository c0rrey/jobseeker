"""
CLI entry point for the jseeker V2 pipeline.

Provides four mutually exclusive stage flags:

  --fetch      Run all API fetchers (Adzuna, RemoteOK, LinkedIn), ATS feed
               fetcher, and career page crawler, then deduplicate and insert
               into the database.
  --enrich     Run the enrichment orchestrator on companies needing enrichment.
  --prefilter  Run the deterministic pre-filter on unfiltered jobs.
  --all        Run fetch, enrich, and prefilter in sequence.

Note: Scoring and other LLM-based stages are handled by Claude Code subagents
and are not exposed through this CLI.

Usage::

    python pipeline/cli.py --help
    python pipeline/cli.py --fetch
    python pipeline/cli.py --enrich
    python pipeline/cli.py --prefilter
    python pipeline/cli.py --all
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from pipeline.config.settings import get_db_path
from pipeline.src.database import get_connection, init_db
from pipeline.src.deduplicator import deduplicate_and_insert
from pipeline.src.enrichment.orchestrator import run_enrichment
from pipeline.src.fetchers import (
    AdzunaFetcher,
    ATSFetcher,
    CareerPageFetcher,
    LinkedInFetcher,
    RemoteOKFetcher,
)
from pipeline.src.filter import run_prefilter as _filter_run_prefilter
from pipeline.src.normalizer import (
    normalize_adzuna,
    normalize_ashby,
    normalize_career_page,
    normalize_greenhouse,
    normalize_lever,
    normalize_linkedin,
    normalize_remoteok,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Normalizer dispatch table keyed by source name embedded in raw dicts
# ---------------------------------------------------------------------------

_ATS_NORMALIZERS = {
    "greenhouse": normalize_greenhouse,
    "lever": normalize_lever,
    "ashby": normalize_ashby,
}


# ---------------------------------------------------------------------------
# Stage handlers
# ---------------------------------------------------------------------------


def run_fetch(db_path: str) -> dict[str, Any]:
    """Execute all fetch sources, deduplicate, and insert into the database.

    Runs the following fetchers in order:
    1. AdzunaFetcher — REST API
    2. RemoteOKFetcher — REST API
    3. LinkedInFetcher — REST API via RapidAPI
    4. ATSFetcher — Greenhouse / Lever / Ashby ATS feeds
    5. CareerPageFetcher — CSS-selector-based HTML crawler

    Each fetcher's raw output is normalized to the common Job schema before
    being passed to the deduplicator.

    Args:
        db_path: Filesystem path of the SQLite database.

    Returns:
        A dict with keys ``"fetched"`` (total raw records) and ``"inserted"``
        and ``"updated"`` (deduplicator counts).
    """
    conn = get_connection(db_path)
    try:
        all_job_pairs: list[Any] = []

        # --- API fetchers ---------------------------------------------------
        try:
            adzuna_raw = AdzunaFetcher().fetch()
            logger.info("Adzuna: fetched %d raw jobs", len(adzuna_raw))
            all_job_pairs.extend(
                (normalize_adzuna(r), r) for r in adzuna_raw
            )
        except Exception:
            logger.exception("Adzuna fetch failed")

        try:
            remoteok_raw = RemoteOKFetcher().fetch()
            logger.info("RemoteOK: fetched %d raw jobs", len(remoteok_raw))
            all_job_pairs.extend(
                (normalize_remoteok(r), r) for r in remoteok_raw
            )
        except Exception:
            logger.exception("RemoteOK fetch failed")

        try:
            linkedin_raw = LinkedInFetcher().fetch()
            logger.info("LinkedIn: fetched %d raw jobs", len(linkedin_raw))
            all_job_pairs.extend(
                (normalize_linkedin(r), r) for r in linkedin_raw
            )
        except Exception:
            logger.exception("LinkedIn fetch failed")

        # --- ATS feed fetcher -----------------------------------------------
        try:
            ats_raw = ATSFetcher(conn).fetch()
            logger.info("ATS feeds: fetched %d raw jobs", len(ats_raw))
            for r in ats_raw:
                platform = r.get("_ats_platform", "")
                norm_fn = _ATS_NORMALIZERS.get(platform)
                if norm_fn:
                    all_job_pairs.append((norm_fn(r), r))
                else:
                    logger.warning("ATS: unknown platform '%s', skipping", platform)
        except Exception:
            logger.exception("ATS fetch failed")

        # --- Career page crawler --------------------------------------------
        try:
            career_raw = CareerPageFetcher(conn).fetch()
            logger.info("Career pages: fetched %d raw jobs", len(career_raw))
            all_job_pairs.extend(
                (normalize_career_page(r), r) for r in career_raw
            )
        except Exception:
            logger.exception("Career page fetch failed")

        # --- Dedup and insert -----------------------------------------------
        jobs = [job for job, _ in all_job_pairs]
        inserted, updated = deduplicate_and_insert(jobs, conn)
        conn.commit()

        return {
            "fetched": len(all_job_pairs),
            "inserted": inserted,
            "updated": updated,
        }
    finally:
        conn.close()


def run_enrich(db_path: str) -> dict[str, Any]:
    """Run the enrichment orchestrator on companies needing enrichment.

    Args:
        db_path: Filesystem path of the SQLite database.

    Returns:
        The :class:`~pipeline.src.enrichment.orchestrator.EnrichmentSummary`
        dict with keys ``companies_processed``, ``sources_succeeded``, and
        ``sources_failed``.
    """
    conn = get_connection(db_path)
    try:
        summary = run_enrichment(conn)
        return dict(summary)
    finally:
        conn.close()


def run_prefilter(db_path: str) -> dict[str, int]:
    """Run the deterministic pre-filter on unfiltered jobs.

    Args:
        db_path: Filesystem path of the SQLite database.

    Returns:
        A dict with keys ``"examined"``, ``"filtered"``, and ``"passed"``.
    """
    conn = get_connection(db_path)
    try:
        return _filter_run_prefilter(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Summary printers
# ---------------------------------------------------------------------------


def _print_fetch_summary(summary: dict[str, Any]) -> None:
    """Print a human-readable fetch stage summary to stdout."""
    print(
        f"Fetch complete. "
        f"Fetched {summary['fetched']} jobs, "
        f"{summary['inserted']} new, "
        f"{summary['updated']} updated."
    )


def _print_enrich_summary(summary: dict[str, Any]) -> None:
    """Print a human-readable enrichment stage summary to stdout."""
    processed = summary.get("companies_processed", 0)
    succeeded = summary.get("sources_succeeded", {})
    failed = summary.get("sources_failed", {})
    succeeded_total = sum(succeeded.values())
    failed_total = sum(failed.values())
    print(
        f"Enrich complete. "
        f"Enriched {processed} companies "
        f"({succeeded_total} source calls succeeded, "
        f"{failed_total} failed)."
    )


def _print_prefilter_summary(summary: dict[str, int]) -> None:
    """Print a human-readable pre-filter stage summary to stdout."""
    print(
        f"Prefilter complete. "
        f"Examined {summary.get('examined', 0)} jobs, "
        f"{summary.get('filtered', 0)} filtered out, "
        f"{summary.get('passed', 0)} passed."
    )


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Construct and return the CLI argument parser.

    Returns:
        Configured :class:`argparse.ArgumentParser` instance.
    """
    parser = argparse.ArgumentParser(
        prog="python pipeline/cli.py",
        description=(
            "jseeker V2 pipeline runner. "
            "Choose one stage to execute. "
            "Note: LLM-based scoring stages run via Claude Code subagents, "
            "not this CLI."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python pipeline/cli.py --fetch\n"
            "  python pipeline/cli.py --enrich\n"
            "  python pipeline/cli.py --prefilter\n"
            "  python pipeline/cli.py --all\n"
        ),
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--fetch",
        action="store_true",
        help=(
            "Run all API fetchers (Adzuna, RemoteOK, LinkedIn), ATS feed "
            "fetcher, and career page crawler, then dedup and insert."
        ),
    )
    group.add_argument(
        "--enrich",
        action="store_true",
        help="Run the enrichment orchestrator on companies needing enrichment.",
    )
    group.add_argument(
        "--prefilter",
        action="store_true",
        help="Run the deterministic pre-filter on unfiltered jobs.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Run fetch, enrich, and prefilter in sequence.",
    )

    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help=(
            "Path to the SQLite database file. "
            "Defaults to $DB_PATH or data/jobs.db relative to the project root."
        ),
    )

    return parser


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and dispatch to the appropriate pipeline stage.

    Args:
        argv: Argument list to parse. Defaults to ``sys.argv[1:]`` when
            ``None``.

    Returns:
        Exit code — 0 on success, 1 on error.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # No stage selected — print help and exit cleanly.
    if not any([args.fetch, args.enrich, args.prefilter, args.all]):
        parser.print_help()
        return 0

    db_path = args.db if args.db is not None else get_db_path()

    try:
        # Initialise the database (idempotent — creates tables if not exists).
        logger.info("Initialising database at %s", db_path)
        init_db(db_path)

        if args.fetch:
            summary = run_fetch(db_path)
            _print_fetch_summary(summary)

        elif args.enrich:
            summary = run_enrich(db_path)
            _print_enrich_summary(summary)

        elif args.prefilter:
            summary = run_prefilter(db_path)
            _print_prefilter_summary(summary)

        elif args.all:
            logger.info("Running all stages: fetch -> enrich -> prefilter")

            fetch_summary = run_fetch(db_path)
            _print_fetch_summary(fetch_summary)

            enrich_summary = run_enrich(db_path)
            _print_enrich_summary(enrich_summary)

            prefilter_summary = run_prefilter(db_path)
            _print_prefilter_summary(prefilter_summary)

    except Exception:
        logger.exception("Pipeline stage failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

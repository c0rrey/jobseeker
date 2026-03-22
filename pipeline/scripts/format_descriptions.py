"""
Standalone script: format job descriptions for Pass 2 survivors.

Queries the database for jobs that passed Pass 2 scoring (score_dimensions
pass=2, overall > 0) and whose formatted_description is still NULL, sends
each to an LLM for markdown formatting, and writes the result back.

Usage::

    python -m pipeline.scripts.format_descriptions [--db PATH]

Environment variables:
    DB_PATH: Override the default database path (same as pipeline CLI).

Exit codes:
    0: Success (even if some individual formatting calls failed).
    1: Fatal error (e.g. database not accessible).
"""

from __future__ import annotations

import argparse
import logging
import sys

from pipeline.config.settings import get_db_path
from pipeline.src.description_formatter import format_descriptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run(db_path: str) -> dict[str, int]:
    """Format descriptions for all eligible Pass 2 survivor jobs.

    Args:
        db_path: Filesystem path to the SQLite database.

    Returns:
        A dict with keys ``"examined"``, ``"formatted"``, and ``"skipped"``
        (all ``int``).  When no eligible jobs exist all three values are 0.

    Raises:
        FileNotFoundError: If *db_path* does not exist.
        sqlite3.Error: If a database operation fails unexpectedly.
    """
    return format_descriptions(db_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Format job descriptions for Pass 2 survivors."
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help="Path to SQLite database. Defaults to DB_PATH env var or data/jobs.db.",
    )
    return parser


def main() -> None:
    """Entry point for the format_descriptions script."""
    parser = _build_parser()
    args = parser.parse_args()

    db_path = args.db or get_db_path()
    try:
        summary = run(db_path=db_path)
        logger.info(
            "format_descriptions complete: examined=%d formatted=%d skipped=%d",
            summary["examined"],
            summary["formatted"],
            summary["skipped"],
        )
    except Exception:
        logger.exception("format_descriptions failed")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

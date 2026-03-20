"""
Deduplicator — database-backed deduplication for the V2 pipeline.

Primary dedup uses the UNIQUE constraint on ``jobs.url``; an existing URL
triggers a ``last_seen_at`` update instead of a re-insert.

Secondary (fuzzy) dedup uses ``dedup_hash``, computed as
SHA256(lowercase(title) + lowercase(company)) with whitespace collapsed.
Jobs that share a hash but differ by URL are treated as cross-source
duplicates: a WARNING is logged but the row is still inserted.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Sequence

from .models import Job

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Hash helpers
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Return *text* lowercased with all whitespace runs collapsed to a single space."""
    return re.sub(r"\s+", " ", text.lower()).strip()


def compute_dedup_hash(title: str, company: str) -> str:
    """Compute the canonical dedup hash for a job.

    The hash is SHA256 of ``normalize(title) + normalize(company)``.

    Args:
        title: Job title string (raw, un-normalized).
        company: Company name string (raw, un-normalized).

    Returns:
        64-character lowercase hex digest.
    """
    content = _normalize(title) + _normalize(company)
    return hashlib.sha256(content.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 500


def _fetch_existing_urls(conn: sqlite3.Connection, urls: list[str]) -> set[str]:
    """Return the subset of *urls* that already exist in the ``jobs`` table.

    Queries in chunks of :data:`_CHUNK_SIZE` to stay well under SQLite's
    ``SQLITE_MAX_VARIABLE_NUMBER`` limit (default 999 on many builds).
    """
    existing: set[str] = set()
    for i in range(0, len(urls), _CHUNK_SIZE):
        chunk = urls[i : i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT url FROM jobs WHERE url IN ({placeholders});",
            chunk,
        ).fetchall()
        existing.update(row[0] for row in rows)
    return existing


def _fetch_existing_hashes(conn: sqlite3.Connection, hashes: list[str]) -> dict[str, str]:
    """Return a mapping of ``dedup_hash → url`` for hashes already in the DB.

    Queries in chunks of :data:`_CHUNK_SIZE` to stay well under SQLite's
    ``SQLITE_MAX_VARIABLE_NUMBER`` limit (default 999 on many builds).
    """
    existing: dict[str, str] = {}
    for i in range(0, len(hashes), _CHUNK_SIZE):
        chunk = hashes[i : i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT dedup_hash, url FROM jobs WHERE dedup_hash IN ({placeholders});",
            chunk,
        ).fetchall()
        existing.update({row[0]: row[1] for row in rows})
    return existing


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def deduplicate_and_insert(
    jobs: Sequence[Job],
    conn: sqlite3.Connection,
) -> tuple[int, int]:
    """Deduplicate a batch of jobs against the DB and persist them.

    For each job in *jobs*:

    - If the URL already exists in ``jobs``, update ``last_seen_at`` to now.
    - If the URL is new but the ``dedup_hash`` matches an existing row (same
      title+company, different source URL), log a WARNING and still insert.
    - If the URL is genuinely new, insert the full row.

    ``dedup_hash`` is always computed here; any value set on the incoming
    :class:`~pipeline.src.models.Job` instance is overwritten.

    Args:
        jobs: Sequence of :class:`~pipeline.src.models.Job` dataclass instances.
        conn: An open :class:`sqlite3.Connection` to the V2 database.  The
            connection must already have the ``jobs`` table created (e.g. via
            :func:`~pipeline.src.database.init_db`).

    Returns:
        A ``(inserted, updated)`` tuple with counts of rows inserted and rows
        whose ``last_seen_at`` was updated.

    Raises:
        sqlite3.Error: Propagated on unexpected database errors.
    """
    if not jobs:
        return 0, 0

    now: str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Compute dedup_hash for every incoming job.
    hashed: list[tuple[Job, str]] = [
        (job, compute_dedup_hash(job.title, job.company)) for job in jobs
    ]

    # ------------------------------------------------------------------
    # Phase 1: split incoming jobs into "existing URL" vs "new URL".
    # ------------------------------------------------------------------
    incoming_urls = [job.url for job, _ in hashed]

    # Fetch only the rows whose URL appears in the incoming batch — O(batch).
    # Chunked to avoid SQLite SQLITE_MAX_VARIABLE_NUMBER on large batches.
    existing_urls: set[str] = _fetch_existing_urls(conn, incoming_urls)

    new_jobs: list[tuple[Job, str]] = []
    update_urls: list[tuple[str, str]] = []  # (now, url)

    for job, dhash in hashed:
        if job.url in existing_urls:
            update_urls.append((now, job.url))
        else:
            new_jobs.append((job, dhash))

    # ------------------------------------------------------------------
    # Phase 2: update last_seen_at for existing URLs.
    # ------------------------------------------------------------------
    if update_urls:
        conn.executemany(
            "UPDATE jobs SET last_seen_at = ? WHERE url = ?;",
            update_urls,
        )
        logger.debug("Updated last_seen_at for %d existing job(s).", len(update_urls))

    # ------------------------------------------------------------------
    # Phase 3: cross-source fuzzy check for new jobs.
    # ------------------------------------------------------------------
    if new_jobs:
        new_hashes = [dhash for _, dhash in new_jobs]
        # Chunked to avoid SQLite SQLITE_MAX_VARIABLE_NUMBER on large batches.
        existing_hash_map: dict[str, str] = _fetch_existing_hashes(conn, new_hashes)

        for job, dhash in new_jobs:
            if dhash in existing_hash_map:
                logger.warning(
                    "Cross-source duplicate detected: job '%s' at '%s' shares "
                    "dedup_hash with existing job at '%s'. Inserting anyway.",
                    job.title,
                    job.url,
                    existing_hash_map[dhash],
                )

    # ------------------------------------------------------------------
    # Phase 4: bulk-insert genuinely new jobs.
    # ------------------------------------------------------------------
    insert_rows = [
        (
            job.source,
            job.source_type,
            job.external_id,
            job.url,
            job.title,
            job.company,
            job.company_id,
            job.location,
            job.description,
            job.salary_min,
            job.salary_max,
            job.posted_at,
            now,   # fetched_at
            now,   # last_seen_at
            job.ats_platform,
            job.raw_json,
            dhash,
        )
        for job, dhash in new_jobs
    ]

    if insert_rows:
        conn.executemany(
            """
            INSERT INTO jobs (
                source, source_type, external_id, url, title, company,
                company_id, location, description, salary_min, salary_max,
                posted_at, fetched_at, last_seen_at, ats_platform,
                raw_json, dedup_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            insert_rows,
        )
        logger.debug("Inserted %d new job(s).", len(insert_rows))

    conn.commit()

    return len(insert_rows), len(update_urls)

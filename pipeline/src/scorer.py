"""
Pass 1 fast-filter scoring orchestration for the Jobseeker V2 pipeline.

Provides the data layer for the fast-filter scoring pass:

- ``get_unscored_jobs``: jobs with no score_dimensions row at all.
- ``get_stale_scored_jobs``: jobs whose Pass 1 profile_hash is out of date.
- ``split_into_batches``: partition a job list into fixed-size chunks.
- ``write_pass1_results``: upsert Pass 1 scores into score_dimensions.
- ``compute_profile_hash``: deterministic SHA-256 of profile YAML + snapshot.

The actual LLM subagent calls (fast_filter.md) are the caller's responsibility.
This module handles only DB I/O and data shaping so that the core logic is
testable with an in-memory SQLite connection and no real LLM calls.

Schema reference (database.py L83-98):
    score_dimensions(
        id, job_id, pass, role_fit, skills_gap, culture_signals,
        growth_potential, comp_alignment, overall, reasoning,
        scored_at, profile_hash, UNIQUE(job_id, pass)
    )
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE: int = 40
PASS_1: int = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def compute_profile_hash(profile_yaml: str, snapshot_yaml: str = "") -> str:
    """Return a stable SHA-256 hex digest of the combined profile content.

    The hash is used to detect when a job's Pass 1 score was produced against
    an older profile snapshot and should be re-computed.

    Args:
        profile_yaml: Raw string contents of ``pipeline/config/profile.yaml``.
        snapshot_yaml: Optional latest profile_snapshots row (profile_yaml
            column). Defaults to empty string when no snapshot exists yet.

    Returns:
        64-character lowercase hex digest.
    """
    combined = profile_yaml + "\n---\n" + snapshot_yaml
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------


def get_unscored_jobs(db_connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return jobs that have no Pass 1 score_dimensions row.

    Uses a LEFT JOIN so that only jobs with no matching score_dimensions row
    (sd.id IS NULL) are returned.  The query is restricted to pass=1 so that
    a job that has only a Pass 2 row is correctly identified as having no
    Pass 1 score.

    Args:
        db_connection: Open SQLite connection (WAL mode recommended).

    Returns:
        List of dicts with keys: id, title, company, location, description.
        Empty list when all jobs have been scored.
    """
    cursor = db_connection.execute(
        """
        SELECT
            j.id,
            j.title,
            j.company,
            j.location,
            j.description
        FROM jobs j
        LEFT JOIN score_dimensions sd
            ON sd.job_id = j.id AND sd.pass = :pass
        WHERE sd.id IS NULL
        ORDER BY j.id
        """,
        {"pass": PASS_1},
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_stale_scored_jobs(
    db_connection: sqlite3.Connection, current_profile_hash: str
) -> list[dict[str, Any]]:
    """Return jobs whose Pass 1 score was computed with an outdated profile hash.

    A job is stale when its score_dimensions row for pass=1 has a
    ``profile_hash`` that differs from ``current_profile_hash``.  This
    triggers re-scoring so that the candidate profile changes are reflected.

    Args:
        db_connection: Open SQLite connection.
        current_profile_hash: SHA-256 hex digest of the current profile state,
            as returned by :func:`compute_profile_hash`.

    Returns:
        List of dicts with keys: id, title, company, location, description.
        Empty list when no stale rows exist.
    """
    cursor = db_connection.execute(
        """
        SELECT
            j.id,
            j.title,
            j.company,
            j.location,
            j.description
        FROM jobs j
        INNER JOIN score_dimensions sd
            ON sd.job_id = j.id AND sd.pass = :pass
        WHERE sd.profile_hash != :current_hash
           OR sd.profile_hash IS NULL
        ORDER BY j.id
        """,
        {"pass": PASS_1, "current_hash": current_profile_hash},
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Batch utility
# ---------------------------------------------------------------------------


def split_into_batches(
    jobs: list[dict[str, Any]], batch_size: int = BATCH_SIZE
) -> list[list[dict[str, Any]]]:
    """Partition a flat list of job dicts into fixed-size sublists.

    When the pool size is less than or equal to ``batch_size``, a single-
    element list containing the full pool is returned.  Empty input returns an
    empty list.

    Args:
        jobs: Flat list of job dicts (from ``get_unscored_jobs`` or
            ``get_stale_scored_jobs``).
        batch_size: Maximum number of jobs per batch.  Defaults to 40.

    Returns:
        List of sublists, each containing at most ``batch_size`` job dicts.
    """
    if not jobs:
        return []
    return [jobs[i : i + batch_size] for i in range(0, len(jobs), batch_size)]


# ---------------------------------------------------------------------------
# Write function
# ---------------------------------------------------------------------------


def write_pass1_results(
    db_connection: sqlite3.Connection,
    results: list[dict[str, Any]],
    profile_hash: str = "",
) -> int:
    """Upsert Pass 1 scoring results into the score_dimensions table.

    Each element of ``results`` is expected to contain at minimum:
        - ``job_id`` (int): database primary key of the job
        - ``verdict`` (str): ``"yes"`` or ``"no"``
        - ``confidence`` (int): 0–100

    Mapping rules:
        - ``verdict == "no"``  → ``overall = 0``
        - ``verdict == "yes"`` → ``overall = confidence``

    The INSERT OR REPLACE strategy removes the old row (if any) and inserts a
    fresh one, satisfying the UNIQUE(job_id, pass) constraint while always
    recording the current ``scored_at`` and ``profile_hash``.

    Args:
        db_connection: Open SQLite connection.  The caller is responsible for
            committing or rolling back the enclosing transaction.
        results: List of result dicts from the fast-filter subagent.
        profile_hash: SHA-256 digest of the profile used during scoring.
            Stored in the ``profile_hash`` column so stale detection works on
            future runs.

    Returns:
        Number of rows written (inserted or replaced).

    Raises:
        sqlite3.IntegrityError: If a ``job_id`` does not reference a valid
            row in the ``jobs`` table (foreign key violation).
    """
    if not results:
        return 0

    rows_written = 0
    for result in results:
        job_id: int = result["job_id"]
        verdict: str = str(result.get("verdict", "no")).lower()
        confidence: int = int(result.get("confidence", 0))
        reasoning: str | None = result.get("reasoning")

        overall: int = 0 if verdict == "no" else confidence

        db_connection.execute(
            """
            INSERT OR REPLACE INTO score_dimensions
                (job_id, pass, overall, reasoning, profile_hash, scored_at)
            VALUES
                (:job_id, :pass, :overall, :reasoning, :profile_hash,
                 datetime('now'))
            """,
            {
                "job_id": job_id,
                "pass": PASS_1,
                "overall": overall,
                "reasoning": reasoning,
                "profile_hash": profile_hash or None,
            },
        )
        rows_written += 1

    return rows_written

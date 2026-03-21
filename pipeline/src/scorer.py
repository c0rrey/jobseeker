"""
Scoring orchestration for the Jobseeker V2 pipeline (Pass 1 and Pass 2).

Provides the data layer for both scoring passes:

Pass 1 (fast filter):
- ``get_unscored_jobs``: jobs with no score_dimensions row at all.
- ``get_stale_scored_jobs``: jobs whose Pass 1 profile_hash is out of date.
- ``split_into_batches``: partition a job list into fixed-size chunks.
- ``write_pass1_results``: upsert Pass 1 scores into score_dimensions.
- ``compute_profile_hash``: deterministic SHA-256 of profile YAML + snapshot.

Pass 2 (deep analysis):
- ``get_pass1_survivors``: jobs that passed Pass 1 (overall > 0), including
  those with a stale Pass 2 profile_hash that need re-scoring.
- ``write_pass2_results``: upsert all 5 dimension scores into score_dimensions
  with pass=2, using a single transaction per call to avoid write contention.

The actual LLM subagent calls (fast_filter.md, deep_scorer.md) are the
caller's responsibility.  This module handles only DB I/O and data shaping so
that the core logic is testable with an in-memory SQLite connection and no real
LLM calls.

Schema reference (database.py: _CREATE_SCORE_DIMENSIONS):
    score_dimensions(
        id, job_id, pass, role_fit, skills_match, culture_signals,
        growth_potential, comp_alignment, overall, reasoning,
        scored_at, profile_hash, UNIQUE(job_id, pass)
    )
"""

from __future__ import annotations

import hashlib
import logging
import math
import sqlite3
from typing import Any

from pipeline.src.duplicate_detector import propagate_scores

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE: int = 40
PASS_REJECTED: int = 0  # sentinel: job was filtered out / not scored
PASS_1: int = 1
PASS_2: int = 2

# Maximum description length (chars) sent to the LLM for each pass.
# Pass 1 (fast_filter) batches are large so we keep payloads small.
# Pass 2 (deep_scorer) processes fewer jobs so a longer budget is acceptable.
FAST_FILTER_DESC_CHARS: int = 4000
DEEP_SCORER_DESC_CHARS: int = 8000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_int(value: Any, default: int = 0) -> int:
    """Convert *value* to int, returning *default* on TypeError or ValueError."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


# SQL fragment applied to all scorer queries to exclude non-representative
# duplicates.  Jobs with dup_group_id IS NULL are ungrouped (unique) and are
# always included.  Within a duplicate group, only the job flagged
# is_representative = 1 is included so only one copy is sent to the LLM.
_DUP_FILTER = "AND (j.dup_group_id IS NULL OR j.is_representative = 1)"


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------


def get_unscored_jobs(db_connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return jobs that have no Pass 1 score_dimensions row.

    Uses a LEFT JOIN so that only jobs with no matching score_dimensions row
    (sd.id IS NULL) are returned.  The query is restricted to pass=1 so that
    a job that has only a Pass 2 row is correctly identified as having no
    Pass 1 score.

    Non-representative duplicates are excluded via :data:`_DUP_FILTER`:
    jobs with ``dup_group_id IS NOT NULL AND is_representative = 0`` are
    skipped.  Ungrouped jobs (``dup_group_id IS NULL``) are always returned.

    The ``description`` value in each returned dict is resolved via COALESCE:
    ``full_description`` is preferred when non-NULL, falling back to the
    truncated ``description`` column.  The value is capped at
    :data:`FAST_FILTER_DESC_CHARS` characters to avoid token bloat in the
    fast-filter LLM batch.

    Args:
        db_connection: Open SQLite connection (WAL mode recommended).

    Returns:
        List of dicts with keys: id, title, company, location, description.
        At most one job per duplicate group (the representative).
        Empty list when all jobs have been scored.
    """
    cursor = db_connection.execute(
        f"""
        SELECT
            j.id,
            j.title,
            j.company,
            j.location,
            SUBSTR(COALESCE(j.full_description, j.description), 1,
                   {FAST_FILTER_DESC_CHARS}) AS description
        FROM jobs j
        LEFT JOIN score_dimensions sd
            ON sd.job_id = j.id AND sd.pass = :pass
        WHERE sd.id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM score_dimensions sd0
            WHERE sd0.job_id = j.id AND sd0.pass = {PASS_REJECTED}
          )
          {_DUP_FILTER}
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

    Non-representative duplicates are excluded via :data:`_DUP_FILTER` to
    prevent stale rescoring from overwriting propagated scores on duplicate
    copies.  Ungrouped jobs (``dup_group_id IS NULL``) are always returned.

    The ``description`` value in each returned dict is resolved via COALESCE:
    ``full_description`` is preferred when non-NULL, falling back to the
    truncated ``description`` column.  The value is capped at
    :data:`FAST_FILTER_DESC_CHARS` characters to avoid token bloat in the
    fast-filter LLM batch.

    Args:
        db_connection: Open SQLite connection.
        current_profile_hash: SHA-256 hex digest of the current profile state,
            as returned by :func:`compute_profile_hash`.

    Returns:
        List of dicts with keys: id, title, company, location, description.
        At most one job per duplicate group (the representative).
        Empty list when no stale rows exist.
    """
    cursor = db_connection.execute(
        f"""
        SELECT
            j.id,
            j.title,
            j.company,
            j.location,
            SUBSTR(COALESCE(j.full_description, j.description), 1,
                   {FAST_FILTER_DESC_CHARS}) AS description
        FROM jobs j
        INNER JOIN score_dimensions sd
            ON sd.job_id = j.id AND sd.pass = :pass
        WHERE (sd.profile_hash != :current_hash
           OR sd.profile_hash IS NULL)
          {_DUP_FILTER}
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
    jobs: list[dict[str, Any]],
    n_batches: int | None = None,
    *,
    batch_size: int = BATCH_SIZE,
) -> list[list[dict[str, Any]]]:
    """Partition a flat list of job dicts into sublists.

    Supports two calling conventions:

    - **Pass 1 style** — fixed chunk size: ``split_into_batches(jobs)`` or
      ``split_into_batches(jobs, batch_size=40)``.  Splits the list into
      chunks of at most ``batch_size`` items.
    - **Pass 2 style** — fixed batch count: ``split_into_batches(jobs, 4)``.
      Splits the list into at most ``n_batches`` roughly equal sublists.
      When ``len(jobs) < n_batches``, fewer batches are returned (one per job).

    Empty input always returns an empty list.

    Args:
        jobs: Flat list of job dicts (from ``get_unscored_jobs``,
            ``get_stale_scored_jobs``, or ``get_pass1_survivors``).
        n_batches: When provided (and > 0), divide ``jobs`` into this many
            roughly equal batches rather than using a fixed chunk size.
            Positional argument; pass as the second positional arg for Pass 2
            orchestration (e.g. ``split_into_batches(jobs, 4)``).
        batch_size: Maximum number of jobs per batch when ``n_batches`` is
            *not* supplied.  Keyword-only.  Defaults to :data:`BATCH_SIZE`.

    Returns:
        List of sublists.  When ``n_batches`` is given, at most ``n_batches``
        sublists are returned, each as equal in length as possible.  When
        ``batch_size`` is used, each sublist has at most ``batch_size`` items.
    """
    if not jobs:
        return []

    if n_batches is not None and n_batches > 0:
        size = math.ceil(len(jobs) / n_batches)
        return [jobs[i : i + size] for i in range(0, len(jobs), size)]

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
        - ``reasoning`` (str | None): LLM explanation text; ``None`` when absent

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
        try:
            if "job_id" not in result:
                logger.warning("Skipping Pass 1 result missing 'job_id': %r", result)
                continue
            job_id: int = result["job_id"]
            verdict: str = str(result.get("verdict", "no")).lower()
            confidence: int = _safe_int(result.get("confidence", 0))
            reasoning: str | None = result.get("reasoning")

            overall: int = confidence if verdict == "yes" else 0

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
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Skipping malformed Pass 1 result job_id=%r: %s",
                result.get("job_id"),
                exc,
            )

    propagated = propagate_scores(db_connection, PASS_1)
    logger.debug("write_pass1_results: propagated %d Pass 1 scores to group members", propagated)

    return rows_written


# ---------------------------------------------------------------------------
# Pass 2 query functions
# ---------------------------------------------------------------------------


def get_pass1_survivors(
    db_connection: sqlite3.Connection, current_profile_hash: str = ""
) -> list[dict[str, Any]]:
    """Return jobs that passed Pass 1 and need Pass 2 deep scoring.

    A job qualifies when:
    - It has a Pass 1 row with ``overall > 0`` (fast-filter verdict = YES), AND
    - It either has no Pass 2 row yet, OR its Pass 2 ``profile_hash`` does not
      match ``current_profile_hash`` (stale re-scoring).

    This mirrors the staleness logic used by :func:`get_stale_scored_jobs` for
    Pass 1, applied to Pass 2 rows.

    Non-representative duplicates are excluded via :data:`_DUP_FILTER` so
    only the representative from each group advances to deep scoring.
    Ungrouped jobs (``dup_group_id IS NULL``) are always returned.

    When ``current_profile_hash`` is empty (first run before any profile
    snapshot exists), all Pass 1 survivors with no Pass 2 row are returned.

    Args:
        db_connection: Open SQLite connection (WAL mode recommended).
        current_profile_hash: SHA-256 hex digest of the current combined
            profile state, as returned by :func:`compute_profile_hash`.
            Pass an empty string if no profile snapshot exists yet.

    Returns:
        List of dicts with keys: id, title, company, location, description,
        salary_min, salary_max, salary_currency, company_id.  The extended
        set of keys (compared to Pass 1 queries) allows the deep-scorer
        prompt to access compensation data and company enrichment via the
        company_id FK.

        The ``description`` value is resolved via COALESCE: ``full_description``
        is preferred when non-NULL, falling back to the truncated ``description``
        column.  The value is capped at :data:`DEEP_SCORER_DESC_CHARS`
        characters; this is larger than the Pass 1 cap because the deep scorer
        processes far fewer jobs per batch.

        At most one job per duplicate group (the representative).
        Empty list when no jobs require Pass 2 scoring.
    """
    cursor = db_connection.execute(
        f"""
        SELECT
            j.id,
            j.title,
            j.company,
            j.location,
            SUBSTR(COALESCE(j.full_description, j.description), 1,
                   {DEEP_SCORER_DESC_CHARS}) AS description,
            j.salary_min,
            j.salary_max,
            j.salary_currency,
            j.company_id
        FROM jobs j
        INNER JOIN score_dimensions sd1
            ON sd1.job_id = j.id AND sd1.pass = :pass1 AND sd1.overall > {PASS_REJECTED}
        LEFT JOIN score_dimensions sd2
            ON sd2.job_id = j.id AND sd2.pass = :pass2
        WHERE (
            sd2.id IS NULL
            OR sd2.profile_hash IS NULL
            OR sd2.profile_hash != :current_hash
        )
          {_DUP_FILTER}
        ORDER BY sd1.overall DESC, j.id
        """,
        {"pass1": PASS_1, "pass2": PASS_2, "current_hash": current_profile_hash},
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Pass 2 write function
# ---------------------------------------------------------------------------


def write_pass2_results(
    db_connection: sqlite3.Connection,
    results: list[dict[str, Any]],
    profile_hash: str = "",
) -> int:
    """Upsert Pass 2 deep-scoring results into the score_dimensions table.

    All rows from a single subagent batch are written in one transaction block.
    The caller is responsible for committing after this function returns, which
    prevents partial writes and reduces write contention when multiple subagent
    batches run in parallel (each batch calls this function independently).

    Each element of ``results`` must contain:
        - ``job_id`` (int): database primary key of the job
        - ``role_fit`` (int): 0-100 role alignment score
        - ``skills_match`` (int): 0-100 skills match score
        - ``culture_signals`` (int): 0-100 culture fit score
        - ``growth_potential`` (int): 0-100 growth opportunity score
        - ``comp_alignment`` (int): 0-100 compensation alignment score
        - ``overall`` (int): 0-100 weighted composite score
        - ``reasoning`` (str | None): JSON-serialised per-dimension explanations

    The overall score is expected to be pre-computed by the LLM using the
    canonical weighting (role_fit 30%, skills_match 25%, culture_signals 15%,
    growth_potential 15%, comp_alignment 15%).

    The INSERT OR REPLACE strategy satisfies the UNIQUE(job_id, pass) constraint
    while always recording the current ``scored_at`` and ``profile_hash``.

    Args:
        db_connection: Open SQLite connection.  The caller must commit or roll
            back after this function returns.
        results: List of result dicts from the deep-scorer subagent.
        profile_hash: SHA-256 digest of the profile used during scoring.
            Stored in ``profile_hash`` so staleness detection works on future
            runs.  Pass an empty string if no profile snapshot exists yet.

    Returns:
        Number of rows written (inserted or replaced).

    Raises:
        sqlite3.IntegrityError: If a ``job_id`` does not reference a valid row
            in the ``jobs`` table (foreign key violation).
    """
    if not results:
        return 0

    rows_written = 0
    for result in results:
        try:
            if "job_id" not in result:
                logger.warning("Skipping Pass 2 result missing 'job_id': %r", result)
                continue
            job_id: int = result["job_id"]
            role_fit: int = _safe_int(result.get("role_fit", 0))
            skills_match: int = _safe_int(result.get("skills_match", 0))
            culture_signals: int = _safe_int(result.get("culture_signals", 0))
            growth_potential: int = _safe_int(result.get("growth_potential", 0))
            comp_alignment: int = _safe_int(result.get("comp_alignment", 0))
            overall: int = _safe_int(result.get("overall", 0))
            reasoning: str | None = result.get("reasoning")

            db_connection.execute(
                """
                INSERT OR REPLACE INTO score_dimensions
                    (job_id, pass, role_fit, skills_match, culture_signals,
                     growth_potential, comp_alignment, overall, reasoning,
                     profile_hash, scored_at)
                VALUES
                    (:job_id, :pass, :role_fit, :skills_match, :culture_signals,
                     :growth_potential, :comp_alignment, :overall, :reasoning,
                     :profile_hash, datetime('now'))
                """,
                {
                    "job_id": job_id,
                    "pass": PASS_2,
                    "role_fit": role_fit,
                    "skills_match": skills_match,
                    "culture_signals": culture_signals,
                    "growth_potential": growth_potential,
                    "comp_alignment": comp_alignment,
                    "overall": overall,
                    "reasoning": reasoning,
                    "profile_hash": profile_hash or None,
                },
            )
            rows_written += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Skipping malformed Pass 2 result job_id=%r: %s",
                result.get("job_id"),
                exc,
            )

    propagated = propagate_scores(db_connection, PASS_2)
    logger.debug("write_pass2_results: propagated %d Pass 2 scores to group members", propagated)

    return rows_written

"""
Content-based duplicate detection engine for job postings.

Groups jobs from the same company whose titles are sufficiently similar
(>= _TITLE_SIMILARITY_THRESHOLD) and whose descriptions are sufficiently
similar (>= _SIMILARITY_THRESHOLD) using difflib.SequenceMatcher.  Within
each group the job with the lowest integer ID is marked as the representative.

Algorithm
---------
1. Fetch all qualifying jobs (non-NULL description, len >= 20 chars) ordered
   by id ASC so that the representative selection is deterministic.
2. Partition jobs by normalised company name (case-insensitive strip).
3. Within each partition check pairwise title similarity first (cheap, short
   strings); skip pairs below _TITLE_SIMILARITY_THRESHOLD.  For surviving
   pairs run SequenceMatcher on the description text.
   Union-Find is used to build transitive groups so that A~B, B~C → one group
   rather than two overlapping groups.
4. Discard singleton non-groups (no detected duplicates).
5. Persist: clear all existing dup_group_id / is_representative data, insert
   rows into job_duplicate_groups, then UPDATE jobs in bulk.

Scale note
----------
The pairwise step is O(N^2) within each company partition.  At expected
pipeline scale (< 500 jobs per company, typically < 50) the inner loop
completes in well under 1 second.  For very large corpora (> 5 000 jobs per
company) pre-filtering by dedup_hash or shingling would be warranted — that
is a future optimisation, not in scope here.

Public API
----------
    detect_duplicates(conn) -> DetectionSummary
    propagate_scores(conn, pass_number) -> int
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SIMILARITY_THRESHOLD: float = 0.70
_TITLE_SIMILARITY_THRESHOLD: float = 0.50

# Descriptions shorter than this are excluded from grouping to avoid
# false-positive matches on stub / placeholder text.
_MIN_DESCRIPTION_LEN: int = 20

# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _Job:
    """Lightweight representation of a qualifying job row."""

    id: int
    company_key: str  # normalised: lower().strip()
    title: str  # normalised: lower().strip()
    description: str


@dataclass
class DetectionSummary:
    """Summary returned by detect_duplicates().

    Attributes:
        groups_created: Number of new duplicate groups inserted.
        jobs_grouped: Total number of jobs assigned to a group.
        representatives_set: Number of jobs marked is_representative=1.
    """

    groups_created: int = 0
    jobs_grouped: int = 0
    representatives_set: int = 0


# ---------------------------------------------------------------------------
# Union-Find (path-compressed, union-by-rank)
# ---------------------------------------------------------------------------


class _UnionFind:
    """Union-Find data structure for grouping integer job IDs.

    Node IDs must be ``int`` values (job primary keys).  The internal
    parent and rank dictionaries are typed ``dict[int, int]``.
    """

    def __init__(self) -> None:
        self._parent: dict[int, int] = {}
        self._rank: dict[int, int] = {}

    def add(self, x: int) -> None:
        """Register a new element.  No-op if already present."""
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0

    def find(self, x: int) -> int:
        """Return the root representative of x's component (path-compressed)."""
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: int, y: int) -> None:
        """Merge the components containing x and y."""
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def groups(self) -> Iterator[frozenset[int]]:
        """Yield each component as a frozenset of element IDs."""
        buckets: dict[int, set[int]] = defaultdict(set)
        for x in self._parent:
            buckets[self.find(x)].add(x)
        for members in buckets.values():
            yield frozenset(members)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def _title_similarity(a: str, b: str) -> float:
    """Return SequenceMatcher ratio between two normalised title strings.

    Args:
        a: First title (already lowercased/stripped by _Job).
        b: Second title (already lowercased/stripped by _Job).

    Returns:
        A float in [0.0, 1.0] representing similarity.
    """
    return SequenceMatcher(None, a, b).ratio()


def _description_similarity(a: str, b: str) -> float:
    """Return SequenceMatcher ratio between two description strings.

    Args:
        a: First description text.
        b: Second description text.

    Returns:
        A float in [0.0, 1.0] representing similarity.
    """
    return SequenceMatcher(None, a, b).ratio()


def _fetch_qualifying_jobs(conn: sqlite3.Connection) -> list[_Job]:
    """Fetch jobs eligible for duplicate detection.

    Excludes rows where description IS NULL or shorter than
    _MIN_DESCRIPTION_LEN characters.  Also excludes rows where the normalised
    company name is an empty string, which would otherwise cause unrelated jobs
    to be false-positive grouped under the same empty company_key.  Rows are
    ordered by id ASC to ensure the lowest-ID representative is deterministic.

    Args:
        conn: An open SQLite connection.

    Returns:
        A list of _Job objects ordered by id ASC.
    """
    query = """
        SELECT id, company, title, description
        FROM jobs
        WHERE description IS NOT NULL
          AND length(description) >= :min_len
        ORDER BY id ASC
    """
    rows = conn.execute(query, {"min_len": _MIN_DESCRIPTION_LEN}).fetchall()
    return [
        _Job(
            id=row[0],
            company_key=company_key,
            title=(row[2] or "").strip().lower(),
            description=row[3],
        )
        for row in rows
        if (company_key := row[1].strip().lower())
    ]


def _build_groups(jobs: list[_Job]) -> list[frozenset[int]]:
    """Identify duplicate groups within the provided job list.

    Jobs are partitioned by company_key first (O(N) pass), then pairwise
    SequenceMatcher ratios are computed within each partition.  Union-Find
    merges transitively similar jobs into one group.  Singleton sets (no
    detected duplicate) are discarded.

    Args:
        jobs: Qualifying jobs fetched from the database.

    Returns:
        A list of frozensets, each containing the IDs of jobs that form a
        duplicate group.  Only groups with two or more members are returned.
    """
    # Partition by company.
    by_company: dict[str, list[_Job]] = defaultdict(list)
    for job in jobs:
        by_company[job.company_key].append(job)

    all_groups: list[frozenset[int]] = []

    for company_key, company_jobs in by_company.items():
        if len(company_jobs) < 2:
            # Cannot have duplicates with only one job for this company.
            continue

        uf = _UnionFind()
        for j in company_jobs:
            uf.add(j.id)

        for i, job_a in enumerate(company_jobs):
            for job_b in company_jobs[i + 1 :]:
                if _title_similarity(job_a.title, job_b.title) < _TITLE_SIMILARITY_THRESHOLD:
                    continue
                similarity = _description_similarity(
                    job_a.description, job_b.description
                )
                if similarity >= _SIMILARITY_THRESHOLD:
                    uf.union(job_a.id, job_b.id)

        # Collect multi-member groups (discard singletons).
        for group in uf.groups():
            if len(group) >= 2:
                all_groups.append(group)

    return all_groups


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def _clear_existing_groups(conn: sqlite3.Connection) -> None:
    """Reset all dup_group_id and is_representative values on jobs.

    Also deletes all rows from job_duplicate_groups.  This enables a clean
    rebuild on each detect_duplicates() call.

    Args:
        conn: An open SQLite connection (within a transaction).
    """
    conn.execute("UPDATE jobs SET dup_group_id = NULL, is_representative = 0")
    conn.execute("DELETE FROM job_duplicate_groups")


def _persist_groups(
    conn: sqlite3.Connection,
    groups: list[frozenset[int]],
) -> tuple[int, int, int]:
    """Insert groups into job_duplicate_groups and update jobs.

    For each group:
    - Insert a row into job_duplicate_groups to obtain a new group id.
    - Mark all member jobs with dup_group_id = <new_group_id>.
    - Mark the member with the lowest job id as is_representative = 1.

    Args:
        conn: An open SQLite connection (within a transaction).
        groups: List of frozensets of job IDs representing duplicate groups.

    Returns:
        Tuple of (groups_created, jobs_grouped, representatives_set).
    """
    groups_created = 0
    jobs_grouped = 0
    representatives_set = 0

    for member_ids in groups:
        cursor = conn.execute(
            "INSERT INTO job_duplicate_groups DEFAULT VALUES"
        )
        group_id = cursor.lastrowid
        groups_created += 1

        # Assign all members to the group.
        sorted_ids = sorted(member_ids)
        placeholders = ",".join("?" * len(sorted_ids))
        conn.execute(
            f"UPDATE jobs SET dup_group_id = ? WHERE id IN ({placeholders})",
            [group_id, *sorted_ids],
        )
        jobs_grouped += len(sorted_ids)

        # The lowest id is the representative.
        representative_id = sorted_ids[0]
        conn.execute(
            "UPDATE jobs SET is_representative = 1 WHERE id = ?",
            (representative_id,),
        )
        representatives_set += 1

    return groups_created, jobs_grouped, representatives_set


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_duplicates(conn: sqlite3.Connection) -> DetectionSummary:
    """Detect and persist content-based duplicate job groups.

    This is a full rebuild: all existing dup_group_id and is_representative
    values are cleared before new groups are computed.  The operation runs
    inside a single transaction; if any step fails the database is rolled back
    to the pre-call state.

    Jobs are grouped when they share the same company (case-insensitive),
    their titles have a SequenceMatcher ratio >= _TITLE_SIMILARITY_THRESHOLD
    (0.50), and their description texts have a SequenceMatcher ratio >=
    _SIMILARITY_THRESHOLD (0.70).  NULL descriptions and descriptions shorter
    than 20 characters are excluded.

    Args:
        conn: An open SQLite connection with foreign_keys = ON.  The caller
            is responsible for opening and closing the connection.

    Returns:
        A DetectionSummary describing what was created / updated.

    Raises:
        sqlite3.Error: If any database operation fails.  The transaction is
            rolled back automatically by the context manager.
    """
    logger.info("detect_duplicates: starting duplicate detection run")

    qualifying_jobs = _fetch_qualifying_jobs(conn)
    logger.debug(
        "detect_duplicates: %d qualifying jobs fetched", len(qualifying_jobs)
    )

    groups = _build_groups(qualifying_jobs)
    logger.debug("detect_duplicates: %d duplicate groups identified", len(groups))

    with conn:
        _clear_existing_groups(conn)
        groups_created, jobs_grouped, representatives_set = _persist_groups(
            conn, groups
        )

    summary = DetectionSummary(
        groups_created=groups_created,
        jobs_grouped=jobs_grouped,
        representatives_set=representatives_set,
    )
    logger.info(
        "detect_duplicates: complete — groups=%d jobs_grouped=%d representatives=%d",
        summary.groups_created,
        summary.jobs_grouped,
        summary.representatives_set,
    )
    return summary


def propagate_scores(conn: sqlite3.Connection, pass_number: int) -> int:
    """Copy scores from each duplicate group's representative to all non-representative members.

    For every duplicate group that has a scored representative (i.e. a
    ``score_dimensions`` row with the given ``pass_number``), this function
    writes an equivalent row for every non-representative member of the same
    group.  Rows are written with ``INSERT OR REPLACE`` so the operation is
    fully idempotent — reruns overwrite previously propagated rows without
    raising a ``UNIQUE`` constraint error.

    The ``reasoning`` column for propagated rows is set to:
    ``"Score propagated from representative job_id=<rep_id>"``
    so that downstream consumers can distinguish propagated scores from
    independently scored ones.

    The ``scored_at`` timestamp is set to ``datetime('now')`` at write time.
    The ``profile_hash`` from the representative row is preserved so that
    staleness detection in ``scorer.get_stale_scored_jobs`` and
    ``scorer.get_pass1_survivors`` treats propagated rows consistently.

    Known limitation: if a representative job is later deleted, the reasoning
    field of already-propagated rows will contain a dangling reference.  No FK
    constraint enforces this — it is an observable but harmless inconsistency.

    Args:
        conn: An open SQLite connection.  The caller is responsible for
            opening and closing the connection.  This function commits its
            own transaction internally.
        pass_number: The scoring pass to propagate (1 or 2).  Must match a
            value stored in the ``pass`` column of ``score_dimensions``.

    Returns:
        The number of ``score_dimensions`` rows written (inserted or replaced)
        for non-representative group members.  Returns 0 when no duplicate
        groups exist or no representatives have been scored for this pass.

    Raises:
        sqlite3.Error: If any database operation fails.  The transaction is
            rolled back automatically.
    """
    # Fetch all representative scores for this pass together with the full
    # list of non-representative members in the same group.
    #
    # The query joins:
    #   score_dimensions (rep's row)  →  jobs (rep)  →  jobs (non-reps in group)
    #
    # Only groups where the representative already has a score_dimensions row
    # for `pass_number` are included.  Non-representatives that already have a
    # row will be overwritten (INSERT OR REPLACE handles this).
    # Column positions (0-based) in the SELECT below — documented here so the
    # index-based access further down is auditable without re-reading the SQL.
    # 0: member_job_id   1: pass   2: role_fit   3: skills_match
    # 4: culture_signals  5: growth_potential  6: comp_alignment
    # 7: overall  8: profile_hash  9: representative_id
    query = """
        SELECT
            non_rep.id          AS member_job_id,
            rep_sd.pass         AS pass,
            rep_sd.role_fit     AS role_fit,
            rep_sd.skills_match AS skills_match,
            rep_sd.culture_signals   AS culture_signals,
            rep_sd.growth_potential  AS growth_potential,
            rep_sd.comp_alignment    AS comp_alignment,
            rep_sd.overall      AS overall,
            rep_sd.profile_hash AS profile_hash,
            rep_job.id          AS representative_id
        FROM jobs AS rep_job
        INNER JOIN score_dimensions AS rep_sd
            ON rep_sd.job_id = rep_job.id
           AND rep_sd.pass = :pass_number
        INNER JOIN jobs AS non_rep
            ON non_rep.dup_group_id = rep_job.dup_group_id
           AND non_rep.is_representative = 0
        WHERE rep_job.is_representative = 1
          AND rep_job.dup_group_id IS NOT NULL
        ORDER BY rep_job.id, non_rep.id
    """
    rows = conn.execute(query, {"pass_number": pass_number}).fetchall()

    if not rows:
        logger.debug(
            "propagate_scores: pass=%d — no representative scores found, nothing to propagate",
            pass_number,
        )
        return 0

    # Build parameter dicts for executemany.  Column access uses both index
    # and key syntax so the function works regardless of whether the caller
    # has set conn.row_factory = sqlite3.Row or left it as the default tuple.
    params: list[dict[str, object]] = []
    for row in rows:
        # row[9] = representative_id (works for tuple rows and sqlite3.Row)
        representative_id = row[9]
        reasoning = (
            f"Score propagated from representative job_id={representative_id}"
        )
        params.append(
            {
                "job_id": row[0],    # member_job_id
                "pass": row[1],      # pass
                "role_fit": row[2],  # role_fit
                "skills_match": row[3],       # skills_match
                "culture_signals": row[4],    # culture_signals
                "growth_potential": row[5],   # growth_potential
                "comp_alignment": row[6],     # comp_alignment
                "overall": row[7],            # overall
                "reasoning": reasoning,
                "profile_hash": row[8],       # profile_hash
            }
        )

    with conn:
        conn.executemany(
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
            params,
        )

    count = len(params)
    logger.info(
        "propagate_scores: pass=%d — propagated %d score rows to non-representative group members",
        pass_number,
        count,
    )
    return count

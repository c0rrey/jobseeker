"""
Content-based duplicate detection engine for job postings.

Groups jobs from the same company whose descriptions are sufficiently similar
(>= _SIMILARITY_THRESHOLD) using difflib.SequenceMatcher.  Within each group
the job with the lowest integer ID is marked as the representative.

Algorithm
---------
1. Fetch all qualifying jobs (non-NULL description, len >= 20 chars) ordered
   by id ASC so that the representative selection is deterministic.
2. Partition jobs by normalised company name (case-insensitive strip).
3. Within each partition run pairwise SequenceMatcher on the description text.
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
    """Union-Find data structure for grouping job IDs.

    Supports arbitrary hashable elements as node IDs.
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
    _MIN_DESCRIPTION_LEN characters.  Rows are ordered by id ASC to ensure
    the lowest-ID representative is deterministic.

    Args:
        conn: An open SQLite connection.

    Returns:
        A list of _Job objects ordered by id ASC.
    """
    query = """
        SELECT id, company, description
        FROM jobs
        WHERE description IS NOT NULL
          AND length(description) >= :min_len
        ORDER BY id ASC
    """
    rows = conn.execute(query, {"min_len": _MIN_DESCRIPTION_LEN}).fetchall()
    return [
        _Job(
            id=row[0],
            company_key=row[1].strip().lower(),
            description=row[2],
        )
        for row in rows
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

    Jobs are grouped when they share the same company (case-insensitive) and
    their description texts have a SequenceMatcher ratio >= _SIMILARITY_THRESHOLD
    (0.70).  NULL descriptions and descriptions shorter than 20 characters are
    excluded.

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

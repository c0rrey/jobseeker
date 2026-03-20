"""
Tests for pipeline/src/scorer.py.

Covers:
- get_unscored_jobs: returns only jobs with no Pass 1 score_dimensions row.
- get_stale_scored_jobs: returns jobs whose Pass 1 profile_hash differs from
  the current hash, or is NULL.
- split_into_batches: correct partitioning for various pool sizes.
- write_pass1_results: upsert behavior, overall mapping (yes/no), profile_hash
  persistence, empty input no-op, return value.
- compute_profile_hash: determinism, sensitivity to input changes.

All tests use an in-memory SQLite connection initialised via init_db() from
pipeline.src.database.  No LLM calls are made.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from pipeline.src.database import get_connection, init_db
from pipeline.src.scorer import (
    BATCH_SIZE,
    PASS_1,
    compute_profile_hash,
    get_stale_scored_jobs,
    get_unscored_jobs,
    split_into_batches,
    write_pass1_results,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return an open SQLite connection to a freshly initialised database."""
    db_path = tmp_path / "test_scorer.db"
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


def _insert_job(
    conn: sqlite3.Connection,
    *,
    title: str = "Data Engineer",
    company: str = "Acme Corp",
    url: str | None = None,
    description: str = "We need someone good with SQL.",
    location: str | None = "Remote",
) -> int:
    """Insert a minimal job row and return its id."""
    static_counter = getattr(_insert_job, "_counter", 0) + 1
    _insert_job._counter = static_counter  # type: ignore[attr-defined]
    unique_url = url or f"https://example.com/job/{static_counter}"
    conn.execute(
        """
        INSERT INTO jobs (source, source_type, url, title, company, description, location)
        VALUES ('test', 'api', ?, ?, ?, ?, ?)
        """,
        (unique_url, title, company, description, location),
    )
    conn.commit()
    row = conn.execute("SELECT last_insert_rowid();").fetchone()
    return row[0]


def _insert_score(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    pass_num: int = PASS_1,
    overall: int = 75,
    profile_hash: str | None = None,
) -> None:
    """Insert a score_dimensions row for the given job."""
    conn.execute(
        """
        INSERT INTO score_dimensions (job_id, pass, overall, profile_hash)
        VALUES (?, ?, ?, ?)
        """,
        (job_id, pass_num, overall, profile_hash),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# compute_profile_hash
# ---------------------------------------------------------------------------


class TestComputeProfileHash:
    def test_returns_64_char_hex(self) -> None:
        h = compute_profile_hash("profile: data")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self) -> None:
        h1 = compute_profile_hash("same", "same snapshot")
        h2 = compute_profile_hash("same", "same snapshot")
        assert h1 == h2

    def test_different_profile_yields_different_hash(self) -> None:
        h1 = compute_profile_hash("profile A")
        h2 = compute_profile_hash("profile B")
        assert h1 != h2

    def test_different_snapshot_yields_different_hash(self) -> None:
        h1 = compute_profile_hash("profile", "snapshot v1")
        h2 = compute_profile_hash("profile", "snapshot v2")
        assert h1 != h2

    def test_empty_inputs_are_valid(self) -> None:
        h = compute_profile_hash("", "")
        assert len(h) == 64


# ---------------------------------------------------------------------------
# get_unscored_jobs
# ---------------------------------------------------------------------------


class TestGetUnscoredJobs:
    def test_returns_empty_when_no_jobs(self, db_conn: sqlite3.Connection) -> None:
        result = get_unscored_jobs(db_conn)
        assert result == []

    def test_returns_job_with_no_score(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn, title="Analytics Engineer")
        result = get_unscored_jobs(db_conn)
        assert len(result) == 1
        assert result[0]["id"] == job_id
        assert result[0]["title"] == "Analytics Engineer"

    def test_excludes_job_with_pass1_score(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=1)
        result = get_unscored_jobs(db_conn)
        assert result == []

    def test_includes_job_with_only_pass2_score(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A job scored at pass=2 but not pass=1 is still unscored for pass 1."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=2)
        result = get_unscored_jobs(db_conn)
        assert len(result) == 1
        assert result[0]["id"] == job_id

    def test_returns_multiple_unscored_jobs(self, db_conn: sqlite3.Connection) -> None:
        ids = [_insert_job(db_conn, url=f"https://example.com/j{i}") for i in range(3)]
        result = get_unscored_jobs(db_conn)
        returned_ids = {r["id"] for r in result}
        assert returned_ids == set(ids)

    def test_mixed_scored_and_unscored(self, db_conn: sqlite3.Connection) -> None:
        scored_id = _insert_job(db_conn, url="https://example.com/scored")
        unscored_id = _insert_job(db_conn, url="https://example.com/unscored")
        _insert_score(db_conn, scored_id, pass_num=1)
        result = get_unscored_jobs(db_conn)
        returned_ids = {r["id"] for r in result}
        assert returned_ids == {unscored_id}

    def test_result_has_expected_keys(self, db_conn: sqlite3.Connection) -> None:
        _insert_job(db_conn)
        result = get_unscored_jobs(db_conn)
        assert set(result[0].keys()) == {"id", "title", "company", "location", "description"}


# ---------------------------------------------------------------------------
# get_stale_scored_jobs
# ---------------------------------------------------------------------------


class TestGetStaleScoredJobs:
    def test_returns_empty_when_no_jobs(self, db_conn: sqlite3.Connection) -> None:
        result = get_stale_scored_jobs(db_conn, "newhash")
        assert result == []

    def test_returns_empty_when_hash_matches(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, profile_hash="abc123")
        result = get_stale_scored_jobs(db_conn, "abc123")
        assert result == []

    def test_returns_job_with_different_hash(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, profile_hash="oldhash")
        result = get_stale_scored_jobs(db_conn, "newhash")
        assert len(result) == 1
        assert result[0]["id"] == job_id

    def test_returns_job_with_null_hash(self, db_conn: sqlite3.Connection) -> None:
        """NULL profile_hash means the row predates hash tracking — always stale."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, profile_hash=None)
        result = get_stale_scored_jobs(db_conn, "anyhash")
        assert len(result) == 1
        assert result[0]["id"] == job_id

    def test_excludes_unscored_jobs(self, db_conn: sqlite3.Connection) -> None:
        """Jobs with no score_dimensions row at all are NOT stale — they're unscored."""
        _insert_job(db_conn)
        result = get_stale_scored_jobs(db_conn, "somehash")
        assert result == []

    def test_only_considers_pass1_rows(self, db_conn: sqlite3.Connection) -> None:
        """Pass 2 rows with old hashes do not trigger stale pass-1 re-scoring."""
        job_id = _insert_job(db_conn)
        # Insert pass=1 with current hash AND pass=2 with old hash
        _insert_score(db_conn, job_id, pass_num=1, profile_hash="current")
        _insert_score(db_conn, job_id, pass_num=2, profile_hash="old")
        result = get_stale_scored_jobs(db_conn, "current")
        assert result == []

    def test_result_has_expected_keys(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, profile_hash="old")
        result = get_stale_scored_jobs(db_conn, "new")
        assert set(result[0].keys()) == {"id", "title", "company", "location", "description"}


# ---------------------------------------------------------------------------
# split_into_batches
# ---------------------------------------------------------------------------


class TestSplitIntoBatches:
    def _make_jobs(self, n: int) -> list[dict[str, Any]]:
        return [{"id": i, "title": f"Job {i}"} for i in range(n)]

    def test_empty_input_returns_empty_list(self) -> None:
        assert split_into_batches([]) == []

    def test_single_job_returns_one_batch(self) -> None:
        jobs = self._make_jobs(1)
        batches = split_into_batches(jobs)
        assert len(batches) == 1
        assert batches[0] == jobs

    def test_exactly_batch_size_is_one_batch(self) -> None:
        jobs = self._make_jobs(BATCH_SIZE)
        batches = split_into_batches(jobs)
        assert len(batches) == 1
        assert len(batches[0]) == BATCH_SIZE

    def test_batch_size_plus_one_yields_two_batches(self) -> None:
        jobs = self._make_jobs(BATCH_SIZE + 1)
        batches = split_into_batches(jobs)
        assert len(batches) == 2
        assert len(batches[0]) == BATCH_SIZE
        assert len(batches[1]) == 1

    def test_three_full_batches(self) -> None:
        jobs = self._make_jobs(BATCH_SIZE * 3)
        batches = split_into_batches(jobs)
        assert len(batches) == 3
        assert all(len(b) == BATCH_SIZE for b in batches)

    def test_custom_batch_size(self) -> None:
        jobs = self._make_jobs(10)
        batches = split_into_batches(jobs, batch_size=3)
        assert len(batches) == 4  # 3+3+3+1
        assert len(batches[-1]) == 1

    def test_no_jobs_lost(self) -> None:
        jobs = self._make_jobs(95)
        batches = split_into_batches(jobs)
        total = sum(len(b) for b in batches)
        assert total == 95

    def test_original_order_preserved(self) -> None:
        jobs = self._make_jobs(50)
        batches = split_into_batches(jobs)
        flattened = [j for batch in batches for j in batch]
        assert flattened == jobs


# ---------------------------------------------------------------------------
# write_pass1_results
# ---------------------------------------------------------------------------


class TestWritePass1Results:
    def test_empty_results_returns_zero(self, db_conn: sqlite3.Connection) -> None:
        count = write_pass1_results(db_conn, [])
        assert count == 0

    def test_no_row_is_no_row_written_for_empty(
        self, db_conn: sqlite3.Connection
    ) -> None:
        write_pass1_results(db_conn, [])
        rows = db_conn.execute("SELECT COUNT(*) FROM score_dimensions").fetchone()[0]
        assert rows == 0

    def test_yes_verdict_uses_confidence_as_overall(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn)
        write_pass1_results(
            db_conn,
            [{"job_id": job_id, "verdict": "yes", "confidence": 85, "reasoning": "Good fit."}],
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchone()
        assert row["overall"] == 85

    def test_no_verdict_sets_overall_to_zero(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn)
        write_pass1_results(
            db_conn,
            [{"job_id": job_id, "verdict": "no", "confidence": 0, "reasoning": "Junior role."}],
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchone()
        assert row["overall"] == 0

    def test_writes_pass_equals_1(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        write_pass1_results(
            db_conn, [{"job_id": job_id, "verdict": "yes", "confidence": 70}]
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT pass FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["pass"] == 1

    def test_profile_hash_stored(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        write_pass1_results(
            db_conn,
            [{"job_id": job_id, "verdict": "yes", "confidence": 60}],
            profile_hash="abc123",
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT profile_hash FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["profile_hash"] == "abc123"

    def test_upsert_replaces_existing_row(self, db_conn: sqlite3.Connection) -> None:
        """Re-running Pass 1 should replace the existing score_dimensions row."""
        job_id = _insert_job(db_conn)
        # First write
        write_pass1_results(
            db_conn, [{"job_id": job_id, "verdict": "yes", "confidence": 50}]
        )
        db_conn.commit()
        # Second write with updated score
        write_pass1_results(
            db_conn, [{"job_id": job_id, "verdict": "yes", "confidence": 90}]
        )
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchall()
        assert len(rows) == 1, "Expected exactly one row after upsert"
        assert rows[0]["overall"] == 90

    def test_returns_count_of_rows_written(self, db_conn: sqlite3.Connection) -> None:
        ids = [
            _insert_job(db_conn, url=f"https://example.com/wr{i}") for i in range(5)
        ]
        results = [
            {"job_id": jid, "verdict": "yes", "confidence": 70} for jid in ids
        ]
        count = write_pass1_results(db_conn, results)
        assert count == 5

    def test_reasoning_stored(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        write_pass1_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "verdict": "yes",
                    "confidence": 80,
                    "reasoning": "Strong analytics background alignment.",
                }
            ],
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT reasoning FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["reasoning"] == "Strong analytics background alignment."

    def test_verdict_case_insensitive(self, db_conn: sqlite3.Connection) -> None:
        """Uppercase YES should still map to overall = confidence."""
        job_id = _insert_job(db_conn)
        write_pass1_results(
            db_conn, [{"job_id": job_id, "verdict": "YES", "confidence": 77}]
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["overall"] == 77

    def test_multiple_results_all_written(self, db_conn: sqlite3.Connection) -> None:
        job_ids = [
            _insert_job(db_conn, url=f"https://example.com/multi{i}") for i in range(3)
        ]
        results = [
            {"job_id": job_ids[0], "verdict": "yes", "confidence": 90},
            {"job_id": job_ids[1], "verdict": "no", "confidence": 0},
            {"job_id": job_ids[2], "verdict": "yes", "confidence": 55},
        ]
        write_pass1_results(db_conn, results)
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT job_id, overall FROM score_dimensions WHERE pass = 1 ORDER BY job_id"
        ).fetchall()
        assert len(rows) == 3
        by_id = {r["job_id"]: r["overall"] for r in rows}
        assert by_id[job_ids[0]] == 90
        assert by_id[job_ids[1]] == 0
        assert by_id[job_ids[2]] == 55

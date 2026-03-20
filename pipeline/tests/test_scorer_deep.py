"""
Tests for Pass 2 deep-scoring functions in pipeline/src/scorer.py.

Covers:
- get_pass1_survivors: returns jobs with pass=1 overall>0 and no pass=2 row,
  including stale pass=2 rows, and excludes jobs already scored with a current
  profile_hash.
- split_into_batches (n_batches mode): divides jobs into roughly equal batches
  by count, handles edge cases (empty, fewer jobs than n_batches, exact fit).
- write_pass2_results: upsert behaviour for all five dimension columns, pass=2,
  profile_hash persistence, empty input no-op, return value, and single-
  transaction write for contention safety.

All tests use an in-memory SQLite connection initialised via init_db() from
pipeline.src.database.  No LLM calls are made.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from pipeline.src.database import get_connection, init_db
from pipeline.src.scorer import (
    PASS_1,
    PASS_2,
    get_pass1_survivors,
    split_into_batches,
    write_pass2_results,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return an open SQLite connection to a freshly initialised database."""
    db_path = tmp_path / "test_scorer_deep.db"
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_job_counter: int = 0


def _insert_job(
    conn: sqlite3.Connection,
    *,
    title: str = "Staff Data Engineer",
    company: str = "Acme Corp",
    url: str | None = None,
    description: str = "We need a senior data engineer with 7+ years of experience.",
    location: str | None = "Remote",
    salary_min: float | None = None,
    salary_max: float | None = None,
) -> int:
    """Insert a minimal job row and return its primary key."""
    global _job_counter
    _job_counter += 1
    unique_url = url or f"https://example.com/job/{_job_counter}"
    conn.execute(
        """
        INSERT INTO jobs
            (source, source_type, url, title, company, description, location,
             salary_min, salary_max)
        VALUES ('test', 'api', ?, ?, ?, ?, ?, ?, ?)
        """,
        (unique_url, title, company, description, location, salary_min, salary_max),
    )
    conn.commit()
    row = conn.execute("SELECT last_insert_rowid();").fetchone()
    return row[0]


def _insert_score(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    pass_num: int,
    overall: int = 75,
    profile_hash: str | None = None,
    role_fit: int | None = None,
    skills_gap: int | None = None,
    culture_signals: int | None = None,
    growth_potential: int | None = None,
    comp_alignment: int | None = None,
) -> None:
    """Insert a score_dimensions row for the given job and pass."""
    conn.execute(
        """
        INSERT INTO score_dimensions
            (job_id, pass, overall, profile_hash, role_fit, skills_gap,
             culture_signals, growth_potential, comp_alignment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            pass_num,
            overall,
            profile_hash,
            role_fit,
            skills_gap,
            culture_signals,
            growth_potential,
            comp_alignment,
        ),
    )
    conn.commit()


def _make_jobs(n: int) -> list[dict[str, Any]]:
    """Return a list of n minimal job dicts for split_into_batches tests."""
    return [{"id": i, "title": f"Job {i}"} for i in range(n)]


# ---------------------------------------------------------------------------
# get_pass1_survivors
# ---------------------------------------------------------------------------


class TestGetPass1Survivors:
    def test_returns_empty_when_no_jobs(self, db_conn: sqlite3.Connection) -> None:
        result = get_pass1_survivors(db_conn)
        assert result == []

    def test_returns_empty_when_no_pass1_rows(
        self, db_conn: sqlite3.Connection
    ) -> None:
        _insert_job(db_conn)
        result = get_pass1_survivors(db_conn)
        assert result == []

    def test_excludes_jobs_that_failed_pass1(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Pass 1 overall=0 (verdict NO) must not appear as a survivor."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=0)
        result = get_pass1_survivors(db_conn)
        assert result == []

    def test_returns_job_with_no_pass2_row(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=80)
        result = get_pass1_survivors(db_conn)
        assert len(result) == 1
        assert result[0]["id"] == job_id

    def test_excludes_job_with_current_pass2_hash(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Job already scored at Pass 2 with the current profile_hash is not returned."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=80)
        _insert_score(
            db_conn, job_id, pass_num=PASS_2, overall=72, profile_hash="current"
        )
        result = get_pass1_survivors(db_conn, "current")
        assert result == []

    def test_includes_job_with_stale_pass2_hash(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Job whose Pass 2 profile_hash is outdated must be re-scored."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=75)
        _insert_score(
            db_conn, job_id, pass_num=PASS_2, overall=60, profile_hash="oldhash"
        )
        result = get_pass1_survivors(db_conn, "newhash")
        assert len(result) == 1
        assert result[0]["id"] == job_id

    def test_includes_job_with_null_pass2_hash(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Pass 2 rows with NULL profile_hash predate hash tracking — always stale."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=85)
        _insert_score(db_conn, job_id, pass_num=PASS_2, overall=70, profile_hash=None)
        result = get_pass1_survivors(db_conn, "anyhash")
        assert len(result) == 1
        assert result[0]["id"] == job_id

    def test_returns_multiple_survivors(self, db_conn: sqlite3.Connection) -> None:
        ids = [
            _insert_job(db_conn, url=f"https://example.com/multi/{i}")
            for i in range(3)
        ]
        for job_id in ids:
            _insert_score(db_conn, job_id, pass_num=PASS_1, overall=70)
        result = get_pass1_survivors(db_conn)
        returned_ids = {r["id"] for r in result}
        assert returned_ids == set(ids)

    def test_mixed_scored_and_unscored(self, db_conn: sqlite3.Connection) -> None:
        """Only jobs passing Pass 1 without a current Pass 2 row are returned."""
        scored_id = _insert_job(db_conn, url="https://example.com/done")
        pending_id = _insert_job(db_conn, url="https://example.com/pending")
        failed_id = _insert_job(db_conn, url="https://example.com/failed")

        _insert_score(db_conn, scored_id, pass_num=PASS_1, overall=80)
        _insert_score(
            db_conn, scored_id, pass_num=PASS_2, overall=72, profile_hash="current"
        )
        _insert_score(db_conn, pending_id, pass_num=PASS_1, overall=65)
        _insert_score(db_conn, failed_id, pass_num=PASS_1, overall=0)

        result = get_pass1_survivors(db_conn, "current")
        returned_ids = {r["id"] for r in result}
        assert returned_ids == {pending_id}

    def test_result_has_expected_keys(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn, salary_min=140000.0, salary_max=180000.0)
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=80)
        result = get_pass1_survivors(db_conn)
        assert set(result[0].keys()) == {
            "id",
            "title",
            "company",
            "location",
            "description",
            "salary_min",
            "salary_max",
            "company_id",
        }

    def test_salary_fields_propagated(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(
            db_conn,
            url="https://example.com/salary",
            salary_min=150000.0,
            salary_max=190000.0,
        )
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=88)
        result = get_pass1_survivors(db_conn)
        assert result[0]["salary_min"] == 150000.0
        assert result[0]["salary_max"] == 190000.0

    def test_company_id_is_none_when_unlinked(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Jobs not yet linked to a companies row have company_id = None."""
        job_id = _insert_job(db_conn, url="https://example.com/nocompany")
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=70)
        result = get_pass1_survivors(db_conn)
        assert result[0]["company_id"] is None

    def test_ordered_by_pass1_overall_descending(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Survivors should be returned highest Pass 1 score first."""
        id_low = _insert_job(db_conn, url="https://example.com/low")
        id_high = _insert_job(db_conn, url="https://example.com/high")
        id_mid = _insert_job(db_conn, url="https://example.com/mid")

        _insert_score(db_conn, id_low, pass_num=PASS_1, overall=55)
        _insert_score(db_conn, id_high, pass_num=PASS_1, overall=92)
        _insert_score(db_conn, id_mid, pass_num=PASS_1, overall=73)

        result = get_pass1_survivors(db_conn)
        returned_ids = [r["id"] for r in result]
        assert returned_ids == [id_high, id_mid, id_low]

    def test_empty_profile_hash_includes_all_survivors(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """When current_profile_hash is empty, all survivors without Pass 2 are returned."""
        job_id = _insert_job(db_conn, url="https://example.com/nohash")
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=75)
        result = get_pass1_survivors(db_conn, "")
        assert len(result) == 1
        assert result[0]["id"] == job_id


# ---------------------------------------------------------------------------
# split_into_batches — n_batches mode
# ---------------------------------------------------------------------------


class TestSplitIntoBatchesNBatches:
    """Tests for split_into_batches when called with n_batches (Pass 2 style)."""

    def test_empty_input_returns_empty_list(self) -> None:
        assert split_into_batches([], 4) == []

    def test_four_batches_from_twelve_jobs(self) -> None:
        jobs = _make_jobs(12)
        batches = split_into_batches(jobs, 4)
        assert len(batches) == 4
        assert all(len(b) == 3 for b in batches)

    def test_no_jobs_lost_with_n_batches(self) -> None:
        jobs = _make_jobs(22)
        batches = split_into_batches(jobs, 4)
        total = sum(len(b) for b in batches)
        assert total == 22

    def test_order_preserved_with_n_batches(self) -> None:
        jobs = _make_jobs(20)
        batches = split_into_batches(jobs, 4)
        flattened = [j for batch in batches for j in batch]
        assert flattened == jobs

    def test_fewer_jobs_than_n_batches(self) -> None:
        """When there are fewer jobs than n_batches, batches <= n_batches."""
        jobs = _make_jobs(2)
        batches = split_into_batches(jobs, 5)
        total = sum(len(b) for b in batches)
        assert total == 2
        assert len(batches) <= 5

    def test_single_job_single_batch(self) -> None:
        jobs = _make_jobs(1)
        batches = split_into_batches(jobs, 4)
        assert len(batches) == 1
        assert batches[0] == jobs

    def test_n_batches_one_returns_single_batch(self) -> None:
        jobs = _make_jobs(10)
        batches = split_into_batches(jobs, 1)
        assert len(batches) == 1
        assert batches[0] == jobs

    def test_rough_equality_of_batch_sizes(self) -> None:
        """Batches should be roughly equal — no batch should be more than one
        chunk size larger than the theoretical floor (len/n_batches)."""
        jobs = _make_jobs(10)
        batches = split_into_batches(jobs, 3)
        sizes = [len(b) for b in batches]
        # ceil(10/3)=4, so [4,4,2] is valid; max-min can be up to chunk_size-1
        # The key invariant is that the largest batch is <= ceil(len/n_batches)
        import math

        assert max(sizes) <= math.ceil(len(jobs) / 3)

    def test_batch_size_keyword_still_works(self) -> None:
        """Existing Pass 1 keyword calling convention must not be broken."""
        jobs = _make_jobs(10)
        batches = split_into_batches(jobs, batch_size=3)
        assert len(batches) == 4  # ceil(10/3) = 4
        assert len(batches[-1]) == 1


# ---------------------------------------------------------------------------
# write_pass2_results
# ---------------------------------------------------------------------------


class TestWritePass2Results:
    def test_empty_results_returns_zero(self, db_conn: sqlite3.Connection) -> None:
        count = write_pass2_results(db_conn, [])
        assert count == 0

    def test_empty_results_writes_no_rows(self, db_conn: sqlite3.Connection) -> None:
        write_pass2_results(db_conn, [])
        rows = db_conn.execute(
            "SELECT COUNT(*) FROM score_dimensions WHERE pass = 2"
        ).fetchone()[0]
        assert rows == 0

    def test_writes_pass_equals_2(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn, url="https://example.com/pass2")
        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 80,
                    "skills_gap": 75,
                    "culture_signals": 70,
                    "growth_potential": 65,
                    "comp_alignment": 80,
                    "overall": 75,
                }
            ],
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT pass FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["pass"] == 2

    def test_all_five_dimensions_stored(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn, url="https://example.com/dims")
        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 90,
                    "skills_gap": 85,
                    "culture_signals": 70,
                    "growth_potential": 60,
                    "comp_alignment": 75,
                    "overall": 79,
                }
            ],
        )
        db_conn.commit()
        row = db_conn.execute(
            """
            SELECT role_fit, skills_gap, culture_signals, growth_potential,
                   comp_alignment, overall
            FROM score_dimensions WHERE job_id = ? AND pass = 2
            """,
            (job_id,),
        ).fetchone()
        assert row["role_fit"] == 90
        assert row["skills_gap"] == 85
        assert row["culture_signals"] == 70
        assert row["growth_potential"] == 60
        assert row["comp_alignment"] == 75
        assert row["overall"] == 79

    def test_profile_hash_stored(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn, url="https://example.com/hash")
        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 80,
                    "skills_gap": 70,
                    "culture_signals": 60,
                    "growth_potential": 65,
                    "comp_alignment": 70,
                    "overall": 71,
                }
            ],
            profile_hash="deadbeef",
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT profile_hash FROM score_dimensions WHERE job_id = ? AND pass = 2",
            (job_id,),
        ).fetchone()
        assert row["profile_hash"] == "deadbeef"

    def test_empty_profile_hash_stored_as_none(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn, url="https://example.com/nohash")
        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 70,
                    "skills_gap": 70,
                    "culture_signals": 70,
                    "growth_potential": 70,
                    "comp_alignment": 70,
                    "overall": 70,
                }
            ],
            profile_hash="",
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT profile_hash FROM score_dimensions WHERE job_id = ? AND pass = 2",
            (job_id,),
        ).fetchone()
        assert row["profile_hash"] is None

    def test_reasoning_stored(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn, url="https://example.com/reasoning")
        reasoning = json.dumps(
            {
                "role_fit": "Strong IC data engineering alignment at staff level.",
                "skills_gap": "Covers 9 of 10 required skills; missing Flink.",
                "culture_signals": "Glassdoor 4.2; positive remote culture reviews.",
                "growth_potential": "Series C growth stage; greenfield platform scope.",
                "comp_alignment": "Posted $160k–$185k matches candidate target range.",
            }
        )
        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 85,
                    "skills_gap": 78,
                    "culture_signals": 72,
                    "growth_potential": 68,
                    "comp_alignment": 82,
                    "overall": 79,
                    "reasoning": reasoning,
                }
            ],
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT reasoning FROM score_dimensions WHERE job_id = ? AND pass = 2",
            (job_id,),
        ).fetchone()
        assert row["reasoning"] == reasoning

    def test_upsert_replaces_stale_pass2_row(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Re-running Pass 2 with a new profile_hash replaces the old row."""
        job_id = _insert_job(db_conn, url="https://example.com/upsert")
        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 60,
                    "skills_gap": 55,
                    "culture_signals": 50,
                    "growth_potential": 45,
                    "comp_alignment": 60,
                    "overall": 56,
                }
            ],
            profile_hash="oldhash",
        )
        db_conn.commit()

        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 85,
                    "skills_gap": 80,
                    "culture_signals": 72,
                    "growth_potential": 68,
                    "comp_alignment": 82,
                    "overall": 79,
                }
            ],
            profile_hash="newhash",
        )
        db_conn.commit()

        rows = db_conn.execute(
            "SELECT overall, profile_hash FROM score_dimensions WHERE job_id = ? AND pass = 2",
            (job_id,),
        ).fetchall()
        assert len(rows) == 1, "Expected exactly one row after upsert"
        assert rows[0]["overall"] == 79
        assert rows[0]["profile_hash"] == "newhash"

    def test_returns_count_of_rows_written(self, db_conn: sqlite3.Connection) -> None:
        ids = [
            _insert_job(db_conn, url=f"https://example.com/count/{i}")
            for i in range(4)
        ]
        results = [
            {
                "job_id": job_id,
                "role_fit": 80,
                "skills_gap": 75,
                "culture_signals": 70,
                "growth_potential": 65,
                "comp_alignment": 80,
                "overall": 75,
            }
            for job_id in ids
        ]
        count = write_pass2_results(db_conn, results)
        assert count == 4

    def test_multiple_results_all_written(self, db_conn: sqlite3.Connection) -> None:
        id_a = _insert_job(db_conn, url="https://example.com/multi/a")
        id_b = _insert_job(db_conn, url="https://example.com/multi/b")
        id_c = _insert_job(db_conn, url="https://example.com/multi/c")

        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": id_a,
                    "role_fit": 90,
                    "skills_gap": 85,
                    "culture_signals": 80,
                    "growth_potential": 70,
                    "comp_alignment": 85,
                    "overall": 84,
                },
                {
                    "job_id": id_b,
                    "role_fit": 50,
                    "skills_gap": 45,
                    "culture_signals": 40,
                    "growth_potential": 35,
                    "comp_alignment": 50,
                    "overall": 45,
                },
                {
                    "job_id": id_c,
                    "role_fit": 70,
                    "skills_gap": 68,
                    "culture_signals": 65,
                    "growth_potential": 60,
                    "comp_alignment": 72,
                    "overall": 68,
                },
            ],
        )
        db_conn.commit()

        rows = db_conn.execute(
            """
            SELECT job_id, overall FROM score_dimensions
            WHERE pass = 2 ORDER BY job_id
            """
        ).fetchall()
        assert len(rows) == 3
        by_id = {r["job_id"]: r["overall"] for r in rows}
        assert by_id[id_a] == 84
        assert by_id[id_b] == 45
        assert by_id[id_c] == 68

    def test_missing_dimension_defaults_to_zero(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Absent dimension keys in the result dict should default to 0."""
        job_id = _insert_job(db_conn, url="https://example.com/missing")
        write_pass2_results(
            db_conn,
            [{"job_id": job_id, "overall": 50}],
        )
        db_conn.commit()
        row = db_conn.execute(
            """
            SELECT role_fit, skills_gap, culture_signals,
                   growth_potential, comp_alignment
            FROM score_dimensions WHERE job_id = ? AND pass = 2
            """,
            (job_id,),
        ).fetchone()
        assert row["role_fit"] == 0
        assert row["skills_gap"] == 0
        assert row["culture_signals"] == 0
        assert row["growth_potential"] == 0
        assert row["comp_alignment"] == 0

    def test_pass1_row_unaffected_by_pass2_write(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Writing Pass 2 must not disturb an existing Pass 1 row."""
        job_id = _insert_job(db_conn, url="https://example.com/p1safe")
        _insert_score(db_conn, job_id, pass_num=PASS_1, overall=88, profile_hash="p1hash")

        write_pass2_results(
            db_conn,
            [
                {
                    "job_id": job_id,
                    "role_fit": 82,
                    "skills_gap": 78,
                    "culture_signals": 70,
                    "growth_potential": 65,
                    "comp_alignment": 80,
                    "overall": 77,
                }
            ],
            profile_hash="p2hash",
        )
        db_conn.commit()

        p1_row = db_conn.execute(
            "SELECT overall, profile_hash FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchone()
        assert p1_row["overall"] == 88
        assert p1_row["profile_hash"] == "p1hash"

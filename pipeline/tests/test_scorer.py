"""
Tests for pipeline/src/scorer.py.

Covers:
- get_unscored_jobs: returns only jobs with no Pass 1 score_dimensions row.
- get_stale_scored_jobs: returns jobs whose Pass 1 profile_hash differs from
  the current hash, or is NULL.
- split_into_batches: correct partitioning for various pool sizes.
- write_pass1_results: writes a JSON file with the correct envelope structure.
- upsert_pass1_results_from_files: upsert behavior, overall mapping (yes/no),
  profile_hash persistence, empty directory no-op, return value.
- compute_profile_hash: determinism, sensitivity to input changes.
- Duplicate-aware query filtering: get_unscored_jobs, get_stale_scored_jobs,
  and get_pass1_survivors must return at most one job per duplicate group
  (the representative); non-representatives are excluded.

All tests use an in-memory SQLite connection initialised via init_db() from
pipeline.src.database.  No LLM calls are made.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from pipeline.src.database import get_connection, init_db
from pipeline.src.duplicate_detector import detect_duplicates
from pipeline.src.scorer import (
    BATCH_SIZE,
    PASS_1,
    PASS_2,
    compute_profile_hash,
    get_pass1_survivors,
    get_stale_scored_jobs,
    get_unscored_jobs,
    split_into_batches,
    upsert_pass1_results_from_files,
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


_insert_job_counter = itertools.count(start=1)


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
    unique_url = url or f"https://example.com/job/{next(_insert_job_counter)}"
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
# write_pass1_results (file-based output)
# ---------------------------------------------------------------------------


class TestWritePass1Results:
    """write_pass1_results writes a JSON envelope to a file; no DB involved."""

    def test_writes_json_file(self, tmp_path: Path) -> None:
        output = tmp_path / "batch_0.json"
        results = [{"job_id": 1, "verdict": "yes", "confidence": 85}]
        write_pass1_results(results, output, profile_hash="abc123")
        assert output.exists()

    def test_json_envelope_structure(self, tmp_path: Path) -> None:
        output = tmp_path / "batch_0.json"
        results = [{"job_id": 1, "verdict": "yes", "confidence": 85, "reasoning": "Good."}]
        write_pass1_results(results, output, profile_hash="abc123")
        envelope = json.loads(output.read_text())
        assert envelope["profile_hash"] == "abc123"
        assert envelope["results"] == results

    def test_empty_results_writes_empty_envelope(self, tmp_path: Path) -> None:
        output = tmp_path / "batch_empty.json"
        write_pass1_results([], output, profile_hash="")
        envelope = json.loads(output.read_text())
        assert envelope["results"] == []

    def test_profile_hash_preserved_in_file(self, tmp_path: Path) -> None:
        output = tmp_path / "batch_0.json"
        write_pass1_results([{"job_id": 5, "verdict": "no", "confidence": 0}], output, profile_hash="deadbeef")
        envelope = json.loads(output.read_text())
        assert envelope["profile_hash"] == "deadbeef"

    def test_multiple_results_all_in_file(self, tmp_path: Path) -> None:
        results = [
            {"job_id": 1, "verdict": "yes", "confidence": 90},
            {"job_id": 2, "verdict": "no", "confidence": 0},
            {"job_id": 3, "verdict": "yes", "confidence": 55},
        ]
        output = tmp_path / "batch_0.json"
        write_pass1_results(results, output)
        envelope = json.loads(output.read_text())
        assert len(envelope["results"]) == 3


# ---------------------------------------------------------------------------
# upsert_pass1_results_from_files (sequential DB upsert)
# ---------------------------------------------------------------------------


class TestUpsertPass1ResultsFromFiles:
    """upsert_pass1_results_from_files reads JSON files and writes to score_dimensions."""

    def _make_result_file(
        self,
        results_dir: Path,
        filename: str,
        results: list[dict[str, Any]],
        profile_hash: str = "",
    ) -> None:
        """Helper: write a JSON envelope into results_dir/filename."""
        file_path = results_dir / filename
        file_path.write_text(
            json.dumps({"profile_hash": profile_hash, "results": results}),
            encoding="utf-8",
        )

    def test_empty_directory_returns_zero(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        count = upsert_pass1_results_from_files(db_conn, results_dir)
        assert count == 0

    def test_missing_directory_returns_zero(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        missing = tmp_path / "nonexistent"
        count = upsert_pass1_results_from_files(db_conn, missing)
        assert count == 0

    def test_no_row_written_for_empty_directory(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        upsert_pass1_results_from_files(db_conn, results_dir)
        rows = db_conn.execute("SELECT COUNT(*) FROM score_dimensions").fetchone()[0]
        assert rows == 0

    def test_yes_verdict_uses_confidence_as_overall(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "yes", "confidence": 85, "reasoning": "Good fit."}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchone()
        assert row["overall"] == 85

    def test_no_verdict_sets_overall_to_zero(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "no", "confidence": 0, "reasoning": "Junior role."}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchone()
        assert row["overall"] == 0

    def test_writes_pass_equals_1(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "yes", "confidence": 70}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT pass FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["pass"] == 1

    def test_profile_hash_stored(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "yes", "confidence": 60}],
            profile_hash="abc123",
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT profile_hash FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["profile_hash"] == "abc123"

    def test_upsert_replaces_existing_row(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Re-running upsert should replace the existing score_dimensions row."""
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "yes", "confidence": 50}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        # Overwrite file with updated score and re-run.
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "yes", "confidence": 90}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (job_id,),
        ).fetchall()
        assert len(rows) == 1, "Expected exactly one row after upsert"
        assert rows[0]["overall"] == 90

    def test_returns_count_of_rows_written(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        ids = [_insert_job(db_conn, url=f"https://example.com/wr{i}") for i in range(5)]
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": jid, "verdict": "yes", "confidence": 70} for jid in ids],
        )
        count = upsert_pass1_results_from_files(db_conn, results_dir)
        assert count == 5

    def test_reasoning_stored(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "yes", "confidence": 80, "reasoning": "Strong analytics background alignment."}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT reasoning FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["reasoning"] == "Strong analytics background alignment."

    def test_verdict_case_insensitive(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Uppercase YES should still map to overall = confidence."""
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_id = _insert_job(db_conn)
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_id, "verdict": "YES", "confidence": 77}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT overall FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert row["overall"] == 77

    def test_multiple_files_all_processed(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        results_dir = tmp_path / "pass1_results"
        results_dir.mkdir()
        job_ids = [
            _insert_job(db_conn, url=f"https://example.com/multi{i}") for i in range(3)
        ]
        self._make_result_file(
            results_dir,
            "batch_0.json",
            [{"job_id": job_ids[0], "verdict": "yes", "confidence": 90}],
        )
        self._make_result_file(
            results_dir,
            "batch_1.json",
            [{"job_id": job_ids[1], "verdict": "no", "confidence": 0}],
        )
        self._make_result_file(
            results_dir,
            "batch_2.json",
            [{"job_id": job_ids[2], "verdict": "yes", "confidence": 55}],
        )
        upsert_pass1_results_from_files(db_conn, results_dir)
        db_conn.commit()
        rows = db_conn.execute(
            "SELECT job_id, overall FROM score_dimensions WHERE pass = 1 ORDER BY job_id"
        ).fetchall()
        assert len(rows) == 3
        by_id = {r["job_id"]: r["overall"] for r in rows}
        assert by_id[job_ids[0]] == 90
        assert by_id[job_ids[1]] == 0
        assert by_id[job_ids[2]] == 55


# ---------------------------------------------------------------------------
# Duplicate-aware query filtering helpers
# ---------------------------------------------------------------------------

# A sufficiently long description so that duplicate detection qualifies these jobs.
_DEDUP_DESC = (
    "Senior Data Engineer position. Build reliable data pipelines with Python "
    "and dbt. Work with Spark, Snowflake, and BigQuery. Strong SQL required. "
    "5+ years experience. Remote-friendly team with great benefits."
)


def _make_duplicate_group(
    conn: sqlite3.Connection,
    company: str = "DupFilterCo",
    n_members: int = 3,
    url_prefix: str = "dup",
) -> tuple[int, list[int]]:
    """Insert n_members jobs with identical descriptions for the same company,
    call detect_duplicates(), and return (representative_id, [non_rep_ids]).

    The lowest-id job becomes the representative.
    """
    ids: list[int] = []
    static = getattr(_make_duplicate_group, "_counter", 0)
    for i in range(n_members):
        static += 1
        url = f"https://example.com/{url_prefix}/{static}"
        conn.execute(
            """
            INSERT INTO jobs (source, source_type, url, title, company, description)
            VALUES ('test', 'api', ?, 'Data Engineer', ?, ?)
            """,
            (url, company, _DEDUP_DESC),
        )
        conn.commit()
        ids.append(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    _make_duplicate_group._counter = static  # type: ignore[attr-defined]

    detect_duplicates(conn)

    representative_id = min(ids)
    non_rep_ids = [jid for jid in ids if jid != representative_id]
    return representative_id, non_rep_ids


# ---------------------------------------------------------------------------
# get_unscored_jobs — duplicate-aware filtering
# ---------------------------------------------------------------------------


class TestGetUnscoredJobsDuplicateAware:
    """get_unscored_jobs must return at most one job per duplicate group."""

    def test_returns_only_representative_for_group(
        self, db_conn: sqlite3.Connection
    ) -> None:
        rep_id, non_rep_ids = _make_duplicate_group(
            db_conn, company="UnscoredDupCo", url_prefix="usd"
        )
        result = get_unscored_jobs(db_conn)
        returned_ids = {r["id"] for r in result}

        assert rep_id in returned_ids
        for nrid in non_rep_ids:
            assert nrid not in returned_ids

    def test_at_most_one_per_group(self, db_conn: sqlite3.Connection) -> None:
        _make_duplicate_group(db_conn, company="OncePerGroup", url_prefix="opg")
        result = get_unscored_jobs(db_conn)
        assert len(result) <= 1

    def test_ungrouped_job_always_returned(self, db_conn: sqlite3.Connection) -> None:
        """A job with dup_group_id IS NULL (no group) must always be included."""
        ungrouped_id = _insert_job(
            db_conn, url="https://example.com/ungrouped1", company="SoloFirm"
        )
        result = get_unscored_jobs(db_conn)
        returned_ids = {r["id"] for r in result}
        assert ungrouped_id in returned_ids

    def test_both_ungrouped_and_representative_returned(
        self, db_conn: sqlite3.Connection
    ) -> None:
        ungrouped_id = _insert_job(
            db_conn, url="https://example.com/ungrouped2", company="IndieInc"
        )
        rep_id, _ = _make_duplicate_group(
            db_conn, company="MixedCo", url_prefix="mix"
        )
        result = get_unscored_jobs(db_conn)
        returned_ids = {r["id"] for r in result}

        assert ungrouped_id in returned_ids
        assert rep_id in returned_ids


# ---------------------------------------------------------------------------
# get_stale_scored_jobs — duplicate-aware filtering
# ---------------------------------------------------------------------------


class TestGetStaleScoredJobsDuplicateAware:
    """get_stale_scored_jobs must return at most one job per duplicate group."""

    def test_excludes_non_representative_stale_rows(
        self, db_conn: sqlite3.Connection
    ) -> None:
        rep_id, non_rep_ids = _make_duplicate_group(
            db_conn, company="StaleDupCo", url_prefix="stale"
        )
        # Score both representative and one non-representative with an old hash.
        old_hash = "oldhash_dedup"
        _insert_score(db_conn, rep_id, pass_num=1, profile_hash=old_hash)
        _insert_score(db_conn, non_rep_ids[0], pass_num=1, profile_hash=old_hash)

        result = get_stale_scored_jobs(db_conn, "newhash_dedup")
        returned_ids = {r["id"] for r in result}

        assert rep_id in returned_ids
        assert non_rep_ids[0] not in returned_ids

    def test_ungrouped_stale_job_returned(self, db_conn: sqlite3.Connection) -> None:
        ungrouped_id = _insert_job(
            db_conn, url="https://example.com/stale_ungrouped", company="StaleAlone"
        )
        _insert_score(db_conn, ungrouped_id, pass_num=1, profile_hash="oldone")

        result = get_stale_scored_jobs(db_conn, "newone")
        returned_ids = {r["id"] for r in result}
        assert ungrouped_id in returned_ids


# ---------------------------------------------------------------------------
# get_pass1_survivors — duplicate-aware filtering
# ---------------------------------------------------------------------------


class TestGetPass1SurvivorsDuplicateAware:
    """get_pass1_survivors must return at most one job per duplicate group."""

    def test_returns_only_representative_from_group(
        self, db_conn: sqlite3.Connection
    ) -> None:
        rep_id, non_rep_ids = _make_duplicate_group(
            db_conn, company="SurvivorCo", url_prefix="surv"
        )
        # Score representative with Pass 1 overall > 0 (YES verdict).
        _insert_score(db_conn, rep_id, pass_num=PASS_1, overall=75)
        # Also score non-representative with Pass 1 to simulate propagated score.
        _insert_score(db_conn, non_rep_ids[0], pass_num=PASS_1, overall=75)

        result = get_pass1_survivors(db_conn, "currenthash")
        returned_ids = {r["id"] for r in result}

        assert rep_id in returned_ids
        for nrid in non_rep_ids:
            assert nrid not in returned_ids

    def test_ungrouped_pass1_survivor_returned(
        self, db_conn: sqlite3.Connection
    ) -> None:
        ungrouped_id = _insert_job(
            db_conn, url="https://example.com/surv_ungrouped", company="SurvAlone"
        )
        _insert_score(db_conn, ungrouped_id, pass_num=PASS_1, overall=80)

        result = get_pass1_survivors(db_conn, "anyhash")
        returned_ids = {r["id"] for r in result}
        assert ungrouped_id in returned_ids

    def test_rejected_representative_not_returned(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A representative with overall = 0 (rejected) must not appear in survivors."""
        rep_id, _ = _make_duplicate_group(
            db_conn, company="RejectedGroup", url_prefix="rej"
        )
        _insert_score(db_conn, rep_id, pass_num=PASS_1, overall=0)

        result = get_pass1_survivors(db_conn, "hash")
        returned_ids = {r["id"] for r in result}
        assert rep_id not in returned_ids

    def test_result_has_extended_keys_for_pass2(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Pass 2 survivors query must expose salary and company_id fields."""
        ungrouped_id = _insert_job(
            db_conn, url="https://example.com/keys_check", company="KeysCo"
        )
        _insert_score(db_conn, ungrouped_id, pass_num=PASS_1, overall=70)

        result = get_pass1_survivors(db_conn, "")
        assert len(result) == 1
        expected_keys = {
            "id", "title", "company", "location", "description",
            "salary_min", "salary_max", "salary_currency", "company_id",
        }
        assert expected_keys.issubset(set(result[0].keys()))

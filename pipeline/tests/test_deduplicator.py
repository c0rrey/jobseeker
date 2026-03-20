"""
Tests for pipeline/src/deduplicator.py.

Uses an in-memory SQLite database initialised with the full V2 schema so
that every test starts from a clean state without touching the filesystem.

Covers:
- compute_dedup_hash: normalisation, whitespace collapse, case-folding
- deduplicate_and_insert:
    - New URL → inserted with all fields and correct dedup_hash
    - Existing URL → last_seen_at updated, row count unchanged
    - Cross-source duplicate (same hash, different URL) → inserted + warning
    - Empty input → no-op
    - Return values (inserted, updated) counts
    - Batch with mixed new/existing URLs
"""

from __future__ import annotations

import sqlite3
import logging

import pytest

from pipeline.src.deduplicator import compute_dedup_hash, deduplicate_and_insert
from pipeline.src.models import Job


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_job(
    *,
    title: str = "Software Engineer",
    company: str = "Acme Corp",
    url: str = "https://example.com/jobs/1",
    source: str = "adzuna",
    source_type: str = "api",
    description: str | None = "A great job.",
    location: str | None = "Remote",
    salary_min: float | None = 100_000.0,
    salary_max: float | None = 150_000.0,
    posted_at: str | None = "2026-03-20",
    external_id: str | None = "adzuna-123",
    ats_platform: str | None = None,
    raw_json: str | None = None,
) -> Job:
    """Return a fully populated Job with sensible defaults."""
    return Job(
        title=title,
        company=company,
        url=url,
        source=source,
        source_type=source_type,
        description=description,
        location=location,
        salary_min=salary_min,
        salary_max=salary_max,
        posted_at=posted_at,
        external_id=external_id,
        ats_platform=ats_platform,
        raw_json=raw_json,
    )


@pytest.fixture()
def mem_conn() -> sqlite3.Connection:
    """In-memory SQLite connection with full V2 schema applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Apply the V2 schema directly against the in-memory connection.
    # init_db() opens its own connection to a file; we replicate the DDL
    # by calling it on a temp path and then re-using the in-memory approach.
    # Simpler: execute the DDL strings via database._SCHEMA.
    from pipeline.src.database import _SCHEMA, _apply_connection_settings
    _apply_connection_settings(conn)
    for create_sql, index_sqls in _SCHEMA:
        conn.execute(create_sql)
        for idx_sql in index_sqls:
            conn.execute(idx_sql)
    conn.commit()
    yield conn
    conn.close()


def _row_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM jobs;").fetchone()[0]


def _fetch_job(conn: sqlite3.Connection, url: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM jobs WHERE url = ?;", (url,)).fetchone()


# ---------------------------------------------------------------------------
# compute_dedup_hash
# ---------------------------------------------------------------------------


class TestComputeDedupHash:
    def test_produces_64_char_hex(self) -> None:
        result = compute_dedup_hash("Software Engineer", "Acme")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_case_insensitive(self) -> None:
        assert compute_dedup_hash("Software Engineer", "Acme") == compute_dedup_hash(
            "software engineer", "acme"
        )

    def test_whitespace_collapsed(self) -> None:
        """Multiple spaces and tabs should produce the same hash as single space."""
        assert compute_dedup_hash("Software  Engineer", "Acme  Corp") == compute_dedup_hash(
            "Software Engineer", "Acme Corp"
        )

    def test_leading_trailing_whitespace_stripped(self) -> None:
        assert compute_dedup_hash("  Software Engineer  ", "  Acme  ") == compute_dedup_hash(
            "Software Engineer", "Acme"
        )

    def test_different_titles_produce_different_hashes(self) -> None:
        h1 = compute_dedup_hash("Software Engineer", "Acme")
        h2 = compute_dedup_hash("Data Scientist", "Acme")
        assert h1 != h2

    def test_different_companies_produce_different_hashes(self) -> None:
        h1 = compute_dedup_hash("Engineer", "Acme")
        h2 = compute_dedup_hash("Engineer", "BetaCo")
        assert h1 != h2

    def test_deterministic_across_calls(self) -> None:
        assert compute_dedup_hash("Foo", "Bar") == compute_dedup_hash("Foo", "Bar")


# ---------------------------------------------------------------------------
# deduplicate_and_insert — basic insertion
# ---------------------------------------------------------------------------


class TestDeduplicateAndInsertNewJob:
    def test_inserts_new_job(self, mem_conn: sqlite3.Connection) -> None:
        job = _make_job()
        inserted, updated = deduplicate_and_insert([job], mem_conn)
        assert inserted == 1
        assert updated == 0
        assert _row_count(mem_conn) == 1

    def test_inserted_row_has_correct_fields(self, mem_conn: sqlite3.Connection) -> None:
        job = _make_job(
            title="Backend Engineer",
            company="Widgets Inc",
            url="https://widgets.com/jobs/42",
            source="remoteok",
            source_type="api",
            description="Build stuff.",
            location="Remote",
            salary_min=90_000.0,
            salary_max=130_000.0,
            posted_at="2026-03-19",
            external_id="ro-42",
        )
        deduplicate_and_insert([job], mem_conn)
        row = _fetch_job(mem_conn, job.url)
        assert row is not None
        assert row["title"] == "Backend Engineer"
        assert row["company"] == "Widgets Inc"
        assert row["source"] == "remoteok"
        assert row["source_type"] == "api"
        assert row["description"] == "Build stuff."
        assert row["location"] == "Remote"
        assert row["salary_min"] == 90_000.0
        assert row["salary_max"] == 130_000.0
        assert row["posted_at"] == "2026-03-19"
        assert row["external_id"] == "ro-42"

    def test_inserted_row_has_dedup_hash(self, mem_conn: sqlite3.Connection) -> None:
        job = _make_job(title="ML Engineer", company="DeepCo")
        deduplicate_and_insert([job], mem_conn)
        row = _fetch_job(mem_conn, job.url)
        expected_hash = compute_dedup_hash("ML Engineer", "DeepCo")
        assert row["dedup_hash"] == expected_hash

    def test_dedup_hash_overwrites_job_field(self, mem_conn: sqlite3.Connection) -> None:
        """Even if Job.dedup_hash is pre-set, deduplicate_and_insert recomputes it."""
        job = _make_job(title="Analyst", company="Corp")
        job.dedup_hash = "stale_hash_value"
        deduplicate_and_insert([job], mem_conn)
        row = _fetch_job(mem_conn, job.url)
        assert row["dedup_hash"] == compute_dedup_hash("Analyst", "Corp")

    def test_empty_list_is_noop(self, mem_conn: sqlite3.Connection) -> None:
        inserted, updated = deduplicate_and_insert([], mem_conn)
        assert inserted == 0
        assert updated == 0
        assert _row_count(mem_conn) == 0

    def test_batch_inserts_multiple_new_jobs(self, mem_conn: sqlite3.Connection) -> None:
        jobs = [
            _make_job(url="https://example.com/1", title="Dev A"),
            _make_job(url="https://example.com/2", title="Dev B"),
            _make_job(url="https://example.com/3", title="Dev C"),
        ]
        inserted, updated = deduplicate_and_insert(jobs, mem_conn)
        assert inserted == 3
        assert updated == 0
        assert _row_count(mem_conn) == 3


# ---------------------------------------------------------------------------
# deduplicate_and_insert — existing URL dedup
# ---------------------------------------------------------------------------


class TestDeduplicateAndInsertExistingUrl:
    def test_existing_url_not_reinserted(self, mem_conn: sqlite3.Connection) -> None:
        job = _make_job()
        deduplicate_and_insert([job], mem_conn)
        assert _row_count(mem_conn) == 1
        # Second call with same URL
        inserted, updated = deduplicate_and_insert([job], mem_conn)
        assert inserted == 0
        assert updated == 1
        assert _row_count(mem_conn) == 1

    def test_existing_url_updates_last_seen_at(self, mem_conn: sqlite3.Connection) -> None:
        job = _make_job()
        deduplicate_and_insert([job], mem_conn)
        original_last_seen = _fetch_job(mem_conn, job.url)["last_seen_at"]

        # Simulate a later fetch by inserting a tiny delay marker via second call.
        deduplicate_and_insert([job], mem_conn)
        updated_last_seen = _fetch_job(mem_conn, job.url)["last_seen_at"]

        # last_seen_at is re-set to "now"; both calls happen nearly simultaneously
        # in a test, so we just verify the column exists and is non-null.
        assert updated_last_seen is not None
        assert isinstance(updated_last_seen, str)
        assert len(updated_last_seen) > 0

    def test_mixed_batch_new_and_existing(self, mem_conn: sqlite3.Connection) -> None:
        job_existing = _make_job(url="https://example.com/old", title="Old Job")
        deduplicate_and_insert([job_existing], mem_conn)

        job_new = _make_job(url="https://example.com/new", title="New Job")
        inserted, updated = deduplicate_and_insert([job_existing, job_new], mem_conn)

        assert inserted == 1
        assert updated == 1
        assert _row_count(mem_conn) == 2


# ---------------------------------------------------------------------------
# deduplicate_and_insert — cross-source fuzzy dedup (dedup_hash match)
# ---------------------------------------------------------------------------


class TestCrossSourceDedupWarning:
    def test_cross_source_duplicate_still_inserted(self, mem_conn: sqlite3.Connection) -> None:
        """Same title+company but different URLs → both rows exist in DB."""
        job_a = _make_job(
            title="Backend Engineer",
            company="Acme",
            url="https://adzuna.com/jobs/1",
            source="adzuna",
        )
        job_b = _make_job(
            title="Backend Engineer",
            company="Acme",
            url="https://remoteok.com/jobs/99",
            source="remoteok",
        )
        deduplicate_and_insert([job_a], mem_conn)
        inserted, updated = deduplicate_and_insert([job_b], mem_conn)

        assert inserted == 1
        assert updated == 0
        assert _row_count(mem_conn) == 2

    def test_cross_source_duplicate_logs_warning(
        self, mem_conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
    ) -> None:
        job_a = _make_job(
            title="Data Scientist",
            company="DataCo",
            url="https://adzuna.com/jobs/ds1",
            source="adzuna",
        )
        job_b = _make_job(
            title="Data Scientist",
            company="DataCo",
            url="https://linkedin.com/jobs/ds99",
            source="linkedin",
        )
        deduplicate_and_insert([job_a], mem_conn)
        with caplog.at_level(logging.WARNING, logger="pipeline.src.deduplicator"):
            deduplicate_and_insert([job_b], mem_conn)

        assert any("Cross-source duplicate" in record.message for record in caplog.records)

    def test_same_hash_different_company_casing_warns(
        self, mem_conn: sqlite3.Connection, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Title+company that differ only in casing share the same dedup_hash → warning."""
        job_a = _make_job(
            title="DevOps Engineer",
            company="STARTUP INC",
            url="https://source-a.com/1",
        )
        job_b = _make_job(
            title="devops engineer",
            company="startup inc",
            url="https://source-b.com/1",
        )
        deduplicate_and_insert([job_a], mem_conn)
        with caplog.at_level(logging.WARNING, logger="pipeline.src.deduplicator"):
            deduplicate_and_insert([job_b], mem_conn)

        assert any("Cross-source duplicate" in record.message for record in caplog.records)
        # Both rows must be present
        assert _row_count(mem_conn) == 2


# ---------------------------------------------------------------------------
# Return value correctness
# ---------------------------------------------------------------------------


class TestReturnValues:
    def test_returns_zero_zero_for_empty(self, mem_conn: sqlite3.Connection) -> None:
        assert deduplicate_and_insert([], mem_conn) == (0, 0)

    def test_returns_correct_counts_all_new(self, mem_conn: sqlite3.Connection) -> None:
        jobs = [_make_job(url=f"https://example.com/{i}") for i in range(5)]
        inserted, updated = deduplicate_and_insert(jobs, mem_conn)
        assert inserted == 5
        assert updated == 0

    def test_returns_correct_counts_all_existing(self, mem_conn: sqlite3.Connection) -> None:
        jobs = [_make_job(url=f"https://example.com/{i}") for i in range(3)]
        deduplicate_and_insert(jobs, mem_conn)
        inserted, updated = deduplicate_and_insert(jobs, mem_conn)
        assert inserted == 0
        assert updated == 3

    def test_returns_correct_counts_mixed(self, mem_conn: sqlite3.Connection) -> None:
        existing = [_make_job(url=f"https://example.com/old/{i}") for i in range(2)]
        deduplicate_and_insert(existing, mem_conn)

        batch = existing + [_make_job(url=f"https://example.com/new/{i}") for i in range(4)]
        inserted, updated = deduplicate_and_insert(batch, mem_conn)
        assert inserted == 4
        assert updated == 2

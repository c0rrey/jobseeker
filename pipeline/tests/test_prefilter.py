"""
Tests for run_prefilter() in pipeline/src/filter.py.

Uses an in-memory SQLite database seeded with the V2 schema so no filesystem
I/O is required.  profile.yaml and red_flags.yaml are patched via monkeypatch
to give each test deterministic settings independent of the developer's local
profile.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Generator

import pytest

from pipeline.src.database import _apply_connection_settings
from pipeline.src.filter import run_prefilter


# ---------------------------------------------------------------------------
# Minimal V2 DDL (only the two tables run_prefilter touches)
# ---------------------------------------------------------------------------

_CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    external_id TEXT,
    url TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    company_id INTEGER,
    location TEXT,
    description TEXT,
    salary_min REAL,
    salary_max REAL,
    posted_at TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    ats_platform TEXT,
    raw_json TEXT,
    dedup_hash TEXT
);
"""

_CREATE_SCORE_DIMENSIONS = """
CREATE TABLE IF NOT EXISTS score_dimensions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    pass INTEGER NOT NULL,
    role_fit INTEGER,
    skills_match INTEGER,
    culture_signals INTEGER,
    growth_potential INTEGER,
    comp_alignment INTEGER,
    overall INTEGER NOT NULL,
    reasoning TEXT,
    scored_at TEXT NOT NULL DEFAULT (datetime('now')),
    profile_hash TEXT,
    UNIQUE(job_id, pass)
);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iso_now() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_days_ago(days: int) -> str:
    """Return an ISO-8601 UTC timestamp for ``days`` ago."""
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db() -> Generator[sqlite3.Connection, None, None]:
    """In-memory SQLite connection with the V2 jobs + score_dimensions schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _apply_connection_settings(conn)
    conn.execute(_CREATE_JOBS)
    conn.execute(_CREATE_SCORE_DIMENSIONS)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def _patch_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch load_profile() to return deterministic settings for all tests."""
    profile = {
        "salary_min": 100_000,
        "max_job_age_days": 30,
        "title_keywords": ["data engineer"],
    }
    monkeypatch.setattr(
        "pipeline.src.filter.load_profile",
        lambda: profile,
    )


@pytest.fixture(autouse=True)
def _patch_red_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch load_red_flags() to return deterministic rules for all tests."""
    red_flags = {
        "phrases": ["equity only"],
        "keywords": ["pyramid scheme"],
    }
    monkeypatch.setattr(
        "pipeline.src.filter.load_red_flags",
        lambda: red_flags,
    )


# ---------------------------------------------------------------------------
# Utility: insert a job row and return its id
# ---------------------------------------------------------------------------


def insert_job(
    conn: sqlite3.Connection,
    *,
    title: str = "Senior Data Engineer",
    company: str = "Acme Corp",
    url: str | None = None,
    description: str = "A great job.",
    salary_min: float | None = 110_000,
    salary_max: float | None = 150_000,
    posted_at: str | None = None,
) -> int:
    """Insert a single job row and return its auto-assigned ``id``."""
    if url is None:
        # Make URLs unique by embedding company + title to avoid UNIQUE violations
        url = f"https://example.com/{company.replace(' ', '-')}/{title.replace(' ', '-')}"
    if posted_at is None:
        posted_at = _iso_now()

    conn.execute(
        """
        INSERT INTO jobs (source, source_type, url, title, company,
                          description, salary_min, salary_max, posted_at)
        VALUES ('test', 'api', ?, ?, ?, ?, ?, ?, ?)
        """,
        (url, title, company, description, salary_min, salary_max, posted_at),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid();").fetchone()[0]


def get_sentinel(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row | None:
    """Return the pre-filter sentinel score_dimensions row for a job, or None."""
    return conn.execute(
        "SELECT * FROM score_dimensions WHERE job_id = ? AND pass = 0",
        (job_id,),
    ).fetchone()


# ---------------------------------------------------------------------------
# Basic contract
# ---------------------------------------------------------------------------


class TestRunPrefilterContract:
    """run_prefilter() returns the expected summary dict."""

    def test_returns_dict_with_required_keys(self, db: sqlite3.Connection) -> None:
        result = run_prefilter(db)
        assert set(result.keys()) == {"examined", "filtered", "passed"}

    def test_empty_db_returns_zeros(self, db: sqlite3.Connection) -> None:
        result = run_prefilter(db)
        assert result == {"examined": 0, "filtered": 0, "passed": 0}

    def test_counts_are_consistent(self, db: sqlite3.Connection) -> None:
        insert_job(db, title="Senior Data Engineer")
        insert_job(db, title="Data Engineer Intern", company="Corp B", salary_min=0, salary_max=0)
        result = run_prefilter(db)
        assert result["examined"] == result["filtered"] + result["passed"]


# ---------------------------------------------------------------------------
# Red flag filter
# ---------------------------------------------------------------------------


class TestRedFlagFilter:
    def test_keyword_in_description_is_filtered(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, description="Join our pyramid scheme today!")
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["overall"] == -1
        assert sentinel["reasoning"] == "red_flag"

    def test_phrase_in_title_is_filtered(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, title="Data Engineer — equity only", description="Some desc.")
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["reasoning"] == "red_flag"

    def test_clean_job_not_filtered_for_red_flags(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, description="Build great data pipelines.")
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None


# ---------------------------------------------------------------------------
# Salary filter
# ---------------------------------------------------------------------------


class TestSalaryFilter:
    def test_no_salary_is_filtered(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, salary_min=None, salary_max=None)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["reasoning"] == "salary"

    def test_salary_zero_is_filtered(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, salary_min=0, salary_max=0)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["reasoning"] == "salary"

    def test_salary_below_min_is_filtered(self, db: sqlite3.Connection) -> None:
        # profile salary_min is 100_000; 50k max should fail
        job_id = insert_job(db, salary_min=40_000, salary_max=50_000)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["reasoning"] == "salary"

    def test_salary_at_min_passes(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, salary_min=100_000, salary_max=100_000)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None

    def test_salary_above_min_passes(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, salary_min=110_000, salary_max=150_000)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None

    def test_only_max_salary_at_min_passes(self, db: sqlite3.Connection) -> None:
        """A job with only salary_max set still passes if max >= salary_min."""
        job_id = insert_job(db, salary_min=None, salary_max=120_000)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None


# ---------------------------------------------------------------------------
# Intern filter
# ---------------------------------------------------------------------------


class TestInternFilter:
    @pytest.mark.parametrize(
        "title",
        [
            "Data Engineering Intern",
            "Analytics Internship",
            "Data Engineer Co-Op",
            "Data Engineer Coop",
            "INTERN Data Analyst",
        ],
    )
    def test_intern_titles_are_filtered(self, db: sqlite3.Connection, title: str) -> None:
        job_id = insert_job(db, title=title, url=f"https://example.com/{title.replace(' ', '-')}")
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None, f"Expected sentinel for title: {title!r}"
        assert sentinel["reasoning"] == "intern"

    def test_non_intern_title_not_filtered_for_intern(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(db, title="Senior Data Engineer")
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None


# ---------------------------------------------------------------------------
# Age filter
# ---------------------------------------------------------------------------


class TestAgeFilter:
    def test_too_old_job_is_filtered(self, db: sqlite3.Connection) -> None:
        # profile max_job_age_days is 30; posting 60 days ago should be filtered
        posted_at = _iso_days_ago(60)
        job_id = insert_job(db, posted_at=posted_at)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["reasoning"] == "too_old"

    def test_recent_job_passes_age_check(self, db: sqlite3.Connection) -> None:
        posted_at = _iso_days_ago(5)
        job_id = insert_job(db, posted_at=posted_at)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None

    def test_job_without_posted_at_passes(self, db: sqlite3.Connection) -> None:
        """Jobs with no posted_at date are treated as recent and kept."""
        job_id = insert_job(db, posted_at=None)
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None


# ---------------------------------------------------------------------------
# Already-scored jobs are skipped
# ---------------------------------------------------------------------------


class TestAlreadyScoredJobsSkipped:
    def test_job_with_existing_score_dimensions_is_skipped(
        self, db: sqlite3.Connection
    ) -> None:
        """A job that already has a score_dimensions row must not be re-examined."""
        job_id = insert_job(db)
        # Insert a real scoring row (pass=1) to simulate an already-scored job.
        db.execute(
            "INSERT INTO score_dimensions (job_id, pass, overall) VALUES (?, 1, 85)",
            (job_id,),
        )
        db.commit()

        result = run_prefilter(db)
        assert result["examined"] == 0

    def test_job_with_sentinel_already_not_re_inserted(
        self, db: sqlite3.Connection
    ) -> None:
        """Calling run_prefilter twice on the same job must not duplicate the sentinel."""
        posted_at = _iso_days_ago(60)
        job_id = insert_job(db, posted_at=posted_at)

        run_prefilter(db)  # first pass — inserts sentinel
        sentinel_count_first = db.execute(
            "SELECT COUNT(*) FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()[0]

        run_prefilter(db)  # second pass — job already has sentinel, should be skipped
        sentinel_count_second = db.execute(
            "SELECT COUNT(*) FROM score_dimensions WHERE job_id = ?", (job_id,)
        ).fetchone()[0]

        assert sentinel_count_first == 1
        assert sentinel_count_second == 1


# ---------------------------------------------------------------------------
# Filter priority: red_flag beats salary
# ---------------------------------------------------------------------------


class TestFilterPriority:
    def test_red_flag_takes_priority_over_salary(self, db: sqlite3.Connection) -> None:
        """A job with both a red flag and no salary should be marked red_flag, not salary."""
        job_id = insert_job(
            db,
            description="pyramid scheme opportunity",
            salary_min=None,
            salary_max=None,
        )
        run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is not None
        assert sentinel["reasoning"] == "red_flag"


# ---------------------------------------------------------------------------
# Pass-through: a clean job gets no sentinel
# ---------------------------------------------------------------------------


class TestPassThroughJob:
    def test_clean_job_has_no_sentinel(self, db: sqlite3.Connection) -> None:
        job_id = insert_job(
            db,
            title="Senior Data Engineer",
            description="Build amazing data pipelines.",
            salary_min=120_000,
            salary_max=180_000,
            posted_at=_iso_days_ago(7),
        )
        result = run_prefilter(db)
        sentinel = get_sentinel(db, job_id)
        assert sentinel is None
        assert result["passed"] == 1
        assert result["filtered"] == 0

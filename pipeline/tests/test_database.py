"""
Tests for pipeline/src/database.py.

Covers:
- Schema creation: all 9 tables exist after init_db()
- All required indices exist after init_db()
- Specific constraints: UNIQUE on jobs.url, UNIQUE(job_id, pass) on
  score_dimensions, CHECK on feedback.signal
- Connection settings: WAL mode and busy_timeout >= 5000ms
- Idempotency: calling init_db() twice does not error
- get_connection() applies WAL and busy_timeout on a pre-existing database
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline.src.database import (
    EXPECTED_INDICES,
    EXPECTED_TABLES,
    get_connection,
    init_db,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Return a path inside a temporary directory and call init_db()."""
    path = tmp_path / "test.db"
    init_db(path)
    return path


# ---------------------------------------------------------------------------
# Schema: tables
# ---------------------------------------------------------------------------


class TestInitDb:
    def test_creates_file(self, tmp_path: Path) -> None:
        path = tmp_path / "new.db"
        assert not path.exists()
        init_db(path)
        assert path.exists()

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "deep" / "test.db"
        init_db(path)
        assert path.exists()

    def test_creates_all_expected_tables(self, db_path: Path) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            ).fetchall()
        table_names = {row[0] for row in rows}
        assert table_names == EXPECTED_TABLES, (
            f"Expected {sorted(EXPECTED_TABLES)}, got {sorted(table_names)}"
        )

    def test_is_idempotent(self, db_path: Path) -> None:
        """Calling init_db a second time on an existing database should not raise."""
        init_db(db_path)  # second call

    def test_accepts_string_path(self, tmp_path: Path) -> None:
        path = tmp_path / "str.db"
        init_db(str(path))  # str, not Path
        assert path.exists()


# ---------------------------------------------------------------------------
# Schema: individual tables
# ---------------------------------------------------------------------------


class TestTableSchemas:
    def _column_names(self, conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
        return {row[1] for row in rows}

    def test_jobs_table_has_url_unique_constraint(self, db_path: Path) -> None:
        """Inserting two rows with the same url must raise IntegrityError."""
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO jobs (source, source_type, url, title, company) "
                "VALUES ('test', 'api', 'https://example.com/job/1', 'Dev', 'Acme');"
            )
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO jobs (source, source_type, url, title, company) "
                    "VALUES ('test2', 'api', 'https://example.com/job/1', 'Dev2', 'Acme2');"
                )

    def test_score_dimensions_unique_job_id_pass(self, db_path: Path) -> None:
        """Inserting two score_dimensions rows with the same (job_id, pass) must raise."""
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO jobs (source, source_type, url, title, company) "
                "VALUES ('test', 'api', 'https://example.com/job/2', 'Dev', 'Acme');"
            )
            job_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
            conn.execute(
                "INSERT INTO score_dimensions (job_id, pass, overall) VALUES (?, 1, 80);",
                (job_id,),
            )
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO score_dimensions (job_id, pass, overall) VALUES (?, 1, 90);",
                    (job_id,),
                )

    def test_feedback_signal_check_constraint_valid_values(self, db_path: Path) -> None:
        """thumbs_up and thumbs_down are valid signal values."""
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO jobs (source, source_type, url, title, company) "
                "VALUES ('test', 'api', 'https://example.com/job/3', 'Dev', 'Acme');"
            )
            job_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
            # Both values must be accepted without error.
            conn.execute(
                "INSERT INTO feedback (job_id, signal) VALUES (?, 'thumbs_up');",
                (job_id,),
            )
            conn.execute(
                "INSERT INTO feedback (job_id, signal) VALUES (?, 'thumbs_down');",
                (job_id,),
            )

    def test_feedback_signal_check_constraint_invalid_value(self, db_path: Path) -> None:
        """A signal value other than thumbs_up/thumbs_down must raise IntegrityError."""
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO jobs (source, source_type, url, title, company) "
                "VALUES ('test', 'api', 'https://example.com/job/4', 'Dev', 'Acme');"
            )
            job_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO feedback (job_id, signal) VALUES (?, 'meh');",
                    (job_id,),
                )

    def test_career_page_configs_status_check_constraint_valid(self, db_path: Path) -> None:
        """active, broken, and disabled are valid status values for career_page_configs."""
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO companies (name) VALUES ('TestCo');"
            )
            company_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
            for status in ("active", "broken", "disabled"):
                conn.execute(
                    "INSERT INTO career_page_configs (company_id, url, discovery_method, status) "
                    "VALUES (?, 'https://testco.com/jobs', 'manual', ?);",
                    (company_id, status),
                )

    def test_career_page_configs_status_check_constraint_invalid(self, db_path: Path) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO companies (name) VALUES ('TestCo2');")
            company_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO career_page_configs (company_id, url, discovery_method, status) "
                    "VALUES (?, 'https://testco.com/jobs', 'manual', 'unknown');",
                    (company_id,),
                )

    def test_profile_suggestions_status_check_constraint_invalid(self, db_path: Path) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO profile_suggestions "
                    "(suggestion_type, description, reasoning, suggested_change, status) "
                    "VALUES ('add_skill', 'desc', 'reason', '{}', 'maybe');"
                )


# ---------------------------------------------------------------------------
# Schema: indices
# ---------------------------------------------------------------------------


class TestIndices:
    def _index_names(self, conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%';"
        ).fetchall()
        return {row[0] for row in rows}

    def test_all_required_indices_exist(self, db_path: Path) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            actual = self._index_names(conn)
        missing = EXPECTED_INDICES - actual
        assert not missing, f"Missing indices: {sorted(missing)}"

    @pytest.mark.parametrize("index_name", sorted(EXPECTED_INDICES))
    def test_individual_index_exists(self, db_path: Path, index_name: str) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            actual = self._index_names(conn)
        assert index_name in actual, f"Index '{index_name}' not found in {sorted(actual)}"


# ---------------------------------------------------------------------------
# Connection settings: WAL and busy_timeout
# ---------------------------------------------------------------------------


class TestGetConnection:
    def test_returns_connection(self, db_path: Path) -> None:
        conn = get_connection(db_path)
        try:
            assert isinstance(conn, sqlite3.Connection)
        finally:
            conn.close()

    def test_journal_mode_is_wal(self, db_path: Path) -> None:
        conn = get_connection(db_path)
        try:
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            assert mode == "wal", f"Expected 'wal', got '{mode}'"
        finally:
            conn.close()

    def test_busy_timeout_at_least_5000(self, db_path: Path) -> None:
        conn = get_connection(db_path)
        try:
            timeout = conn.execute("PRAGMA busy_timeout;").fetchone()[0]
            assert timeout >= 5000, f"Expected >= 5000 ms, got {timeout}"
        finally:
            conn.close()

    def test_context_manager_usage(self, db_path: Path) -> None:
        """get_connection result works as a context manager."""
        with get_connection(db_path) as conn:
            result = conn.execute("SELECT 1;").fetchone()
            assert result[0] == 1

    def test_row_factory_set(self, db_path: Path) -> None:
        """get_connection sets row_factory to sqlite3.Row."""
        with get_connection(db_path) as conn:
            assert conn.row_factory is sqlite3.Row

    def test_accepts_string_path(self, db_path: Path) -> None:
        conn = get_connection(str(db_path))
        try:
            assert isinstance(conn, sqlite3.Connection)
        finally:
            conn.close()

    def test_wal_on_fresh_connection_without_init_db(self, tmp_path: Path) -> None:
        """WAL is applied even when get_connection opens a brand-new file (no prior init_db)."""
        path = tmp_path / "fresh.db"
        conn = get_connection(path)
        try:
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            assert mode == "wal"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Migration: formatted_description column
# ---------------------------------------------------------------------------


class TestFormattedDescriptionMigration:
    """Verify formatted_description column exists after init_db() and that
    migration is idempotent (AC1, AC2, AC3 from seek-163)."""

    def _jobs_column_names(self, db_path: Path) -> set[str]:
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute("PRAGMA table_info(jobs);").fetchall()
        return {row[1] for row in rows}

    def test_fresh_db_has_formatted_description(self, tmp_path: Path) -> None:
        """After init_db() on a fresh database, formatted_description column exists."""
        path = tmp_path / "fresh.db"
        init_db(path)
        cols = self._jobs_column_names(path)
        assert "formatted_description" in cols, (
            f"formatted_description not found in jobs columns: {sorted(cols)}"
        )

    def test_existing_db_without_column_gets_migrated(self, tmp_path: Path) -> None:
        """init_db() adds formatted_description to an existing database that lacks it.

        Simulates a real legacy database by first running init_db() to build the
        full current schema, then directly adding a column-free copy of the jobs
        table is not possible without DDL rewrite.  Instead, we verify the migration
        code path by constructing the pre-migration state with raw SQLite: drop the
        jobs table and recreate it without formatted_description, then call init_db().
        """
        path = tmp_path / "legacy.db"
        # Step 1 — let init_db create every supporting table + index exactly as
        # production would.  This avoids hand-rolling each supporting table.
        init_db(path)

        # Step 2 — drop and recreate jobs without formatted_description to simulate
        # the pre-migration state.  SQLite does not support DROP COLUMN in older
        # versions, so we recreate the table.
        with sqlite3.connect(str(path)) as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = OFF;
                DROP TABLE jobs;
                CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    company TEXT NOT NULL,
                    company_id INTEGER REFERENCES companies(id),
                    location TEXT,
                    description TEXT,
                    salary_min REAL,
                    salary_max REAL,
                    salary_currency TEXT,
                    posted_at TEXT,
                    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
                    ats_platform TEXT,
                    raw_json TEXT,
                    dedup_hash TEXT,
                    full_description TEXT,
                    dup_group_id INTEGER REFERENCES job_duplicate_groups(id),
                    is_representative INTEGER NOT NULL DEFAULT 0
                );
                PRAGMA foreign_keys = ON;
                """
            )

        # Verify column is absent before migration.
        cols_before = self._jobs_column_names(path)
        assert "formatted_description" not in cols_before

        init_db(path)

        cols_after = self._jobs_column_names(path)
        assert "formatted_description" in cols_after, (
            f"formatted_description not added by migration: {sorted(cols_after)}"
        )

    def test_migration_is_idempotent(self, tmp_path: Path) -> None:
        """Calling init_db() twice on the same database does not raise."""
        path = tmp_path / "idempotent.db"
        init_db(path)
        init_db(path)  # Must not raise

    def test_formatted_description_column_type_is_text(self, tmp_path: Path) -> None:
        """The formatted_description column must have type TEXT."""
        path = tmp_path / "type_check.db"
        init_db(path)
        with sqlite3.connect(str(path)) as conn:
            rows = conn.execute("PRAGMA table_info(jobs);").fetchall()
        col_types = {row[1]: row[2] for row in rows}
        assert col_types.get("formatted_description") == "TEXT", (
            f"Expected TEXT, got {col_types.get('formatted_description')!r}"
        )


# ---------------------------------------------------------------------------
# init_db WAL check
# ---------------------------------------------------------------------------


class TestInitDbWal:
    def test_init_db_connection_uses_wal(self, tmp_path: Path) -> None:
        """After init_db, a plain sqlite3.connect on the file reports WAL mode."""
        path = tmp_path / "wal_check.db"
        init_db(path)
        with sqlite3.connect(str(path)) as conn:
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        assert mode == "wal", f"Expected 'wal', got '{mode}'"

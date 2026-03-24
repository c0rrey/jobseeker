"""
Tests for pipeline/scripts/fetch_descriptions.py.

Covers:
- run() returns a dict with the correct keys ("total", "successful", "failed").
- Zero-jobs early-return path returns {"total": 0, "successful": 0, "failed": 0}.
- run() correctly tallies successful and failed fetches.
- run() raises RuntimeError when the database cannot be opened.
- main() calls sys.exit(0) on success and sys.exit(1) on fatal DB error.
- _get_pass1_survivors_without_description filters by fetched_at when since given.
- Backward compatibility: omitting since returns all eligible jobs.
- since + limit composition: limit caps results after since filter applies.
- main() with invalid --since raises SystemExit with code 2.
- run() with since logs both total eligible and scoped counts.
- run() without since does NOT log 'after --since filter'.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.scripts.fetch_descriptions import (
    _get_pass1_survivors_without_description,
    main,
    run,
)
from pipeline.src.database import init_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SUMMARY_KEYS = {"total", "successful", "failed"}


def _make_job(job_id: int = 1) -> dict:
    return {"id": job_id, "url": f"https://example.com/job/{job_id}", "source": "adzuna"}


# ---------------------------------------------------------------------------
# run() — return-dict contract
# ---------------------------------------------------------------------------


class TestRunReturnsDictWithCorrectKeys:
    """AC-1: run() returns a dict with keys 'total', 'successful', and 'failed'."""

    def test_returns_dict_type(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=MagicMock(),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[],
            ),
        ):
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert isinstance(result, dict)

    def test_has_required_keys(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=MagicMock(),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[],
            ),
        ):
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert set(result.keys()) == _SUMMARY_KEYS

    def test_values_are_ints(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=MagicMock(),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[],
            ),
        ):
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert all(isinstance(result[k], int) for k in _SUMMARY_KEYS)


# ---------------------------------------------------------------------------
# run() — zero-jobs early-return (AC-2)
# ---------------------------------------------------------------------------


class TestRunZeroJobsEarlyReturn:
    """AC-2: When no Pass 1 survivors lack a full_description, run() returns zeros."""

    def test_all_zeros_when_no_jobs(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=MagicMock(),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[],
            ),
        ):
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert result == {"total": 0, "successful": 0, "failed": 0}


# ---------------------------------------------------------------------------
# run() — tally correctness
# ---------------------------------------------------------------------------


class TestRunTally:
    """run() correctly counts successful and failed fetches."""

    def test_one_successful_fetch(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        job = _make_job(1)

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[job],
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.FullDescriptionFetcher"
            ) as mock_fetcher_cls,
            patch(
                "pipeline.scripts.fetch_descriptions._save_description"
            ),
        ):
            mock_fetcher_cls.return_value.fetch_full_description.return_value = "x" * 200
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert result == {"total": 1, "successful": 1, "failed": 0}

    def test_one_failed_fetch_returns_none(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        job = _make_job(2)

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[job],
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.FullDescriptionFetcher"
            ) as mock_fetcher_cls,
        ):
            mock_fetcher_cls.return_value.fetch_full_description.return_value = None
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert result == {"total": 1, "successful": 0, "failed": 1}

    def test_mixed_success_and_failure(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        jobs = [_make_job(1), _make_job(2), _make_job(3)]

        def fake_fetch(url: str, source: str) -> str | None:
            # job 1 succeeds, job 2 fails, job 3 succeeds
            if "job/2" in url:
                return None
            return "y" * 200

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=jobs,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.FullDescriptionFetcher"
            ) as mock_fetcher_cls,
            patch(
                "pipeline.scripts.fetch_descriptions._save_description"
            ),
        ):
            mock_fetcher_cls.return_value.fetch_full_description.side_effect = fake_fetch
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert result == {"total": 3, "successful": 2, "failed": 1}

    def test_fetch_exception_increments_failed_and_continues(self, tmp_path: Path) -> None:
        """AC: network exception from fetch_full_description increments failed, doesn't abort."""
        mock_conn = MagicMock()
        jobs = [_make_job(1), _make_job(2), _make_job(3)]

        def fake_fetch(url: str, source: str) -> str:
            if "job/2" in url:
                raise OSError("connection reset")
            return "x" * 200

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=jobs,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.FullDescriptionFetcher"
            ) as mock_fetcher_cls,
            patch(
                "pipeline.scripts.fetch_descriptions._save_description"
            ),
        ):
            mock_fetcher_cls.return_value.fetch_full_description.side_effect = fake_fetch
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert result == {"total": 3, "successful": 2, "failed": 1}

    def test_db_write_failure_counts_as_failed(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        job = _make_job(1)

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[job],
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.FullDescriptionFetcher"
            ) as mock_fetcher_cls,
            patch(
                "pipeline.scripts.fetch_descriptions._save_description",
                side_effect=RuntimeError("disk full"),
            ),
        ):
            mock_fetcher_cls.return_value.fetch_full_description.return_value = "z" * 200
            result = run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert result == {"total": 1, "successful": 0, "failed": 1}


# ---------------------------------------------------------------------------
# run() — fatal error handling
# ---------------------------------------------------------------------------


class TestRunFatalErrors:
    """run() raises RuntimeError on fatal database errors."""

    def test_raises_on_db_open_failure(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                side_effect=FileNotFoundError("no db"),
            ),
            pytest.raises(RuntimeError, match="Cannot open database"),
        ):
            run(db_path=str(tmp_path / "missing.db"), rate_limit=0.0)

    def test_raises_on_query_failure(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                side_effect=Exception("syntax error"),
            ),
            pytest.raises(RuntimeError, match="Failed to query Pass 1 survivors"),
        ):
            run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)


# ---------------------------------------------------------------------------
# main() — sys.exit codes (AC-3 / AC-5c)
# ---------------------------------------------------------------------------


class TestMainExitCodes:
    """main() calls sys.exit(0) on success and sys.exit(1) on fatal DB error."""

    def test_exits_0_on_success(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_db_path",
                return_value=str(tmp_path / "jobs.db"),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.run",
                return_value={"total": 0, "successful": 0, "failed": 0},
            ),
            patch("sys.argv", ["fetch_descriptions"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 0

    def test_exits_1_on_fatal_error(self, tmp_path: Path) -> None:
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_db_path",
                return_value=str(tmp_path / "missing.db"),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.run",
                side_effect=RuntimeError("fatal"),
            ),
            patch("sys.argv", ["fetch_descriptions"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Helpers for in-memory DB tests
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path) -> tuple[Path, sqlite3.Connection]:
    """Return (db_path, open connection) for a freshly initialised test database."""
    db_path = tmp_path / "test_fetch_desc.db"
    init_db(db_path)
    from pipeline.src.database import get_connection as _gc

    conn = _gc(db_path)
    return db_path, conn


def _insert_job_with_fetched_at(
    conn: sqlite3.Connection,
    job_id_hint: int,
    fetched_at: str,
    *,
    full_description: str | None = None,
) -> int:
    """Insert a minimal job row with an explicit fetched_at and return its id.

    Args:
        conn: Open SQLite connection.
        job_id_hint: Used to form a unique URL.
        fetched_at: ISO 8601 space-separated datetime string stored verbatim.
        full_description: If provided, the full_description column is set.

    Returns:
        The new row's primary key.
    """
    conn.execute(
        """
        INSERT INTO jobs
            (source, source_type, url, title, company, description, fetched_at,
             full_description)
        VALUES ('test', 'api', ?, 'Engineer', 'Acme', 'desc', ?, ?)
        """,
        (
            f"https://example.com/job/{job_id_hint}",
            fetched_at,
            full_description,
        ),
    )
    conn.commit()
    row = conn.execute("SELECT last_insert_rowid();").fetchone()
    return row[0]


def _insert_pass1_score(conn: sqlite3.Connection, job_id: int, overall: int = 75) -> None:
    """Insert a Pass 1 score_dimensions row for the given job_id."""
    conn.execute(
        """
        INSERT INTO score_dimensions (job_id, pass, overall)
        VALUES (?, 1, ?)
        """,
        (job_id, overall),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# _get_pass1_survivors_without_description — since filtering (real SQLite)
# ---------------------------------------------------------------------------


class TestGetPass1SurvivorsWithoutDescriptionSinceFilter:
    """AC-2/AC-3: since filters jobs by fetched_at; omitting since returns all eligible."""

    def test_since_excludes_older_jobs(self, tmp_path: Path) -> None:
        """Jobs fetched before the since timestamp are excluded."""
        _db_path, conn = _make_db(tmp_path)
        try:
            old_id = _insert_job_with_fetched_at(conn, 1, "2026-03-23 09:00:00")
            new_id = _insert_job_with_fetched_at(conn, 2, "2026-03-23 11:00:00")
            _insert_pass1_score(conn, old_id)
            _insert_pass1_score(conn, new_id)

            result = _get_pass1_survivors_without_description(
                conn, since="2026-03-23 10:00:00"
            )
        finally:
            conn.close()

        returned_ids = {r["id"] for r in result}
        assert new_id in returned_ids
        assert old_id not in returned_ids

    def test_since_includes_jobs_at_exact_boundary(self, tmp_path: Path) -> None:
        """A job fetched exactly at the since timestamp is included (>= boundary)."""
        _db_path, conn = _make_db(tmp_path)
        try:
            boundary_id = _insert_job_with_fetched_at(conn, 3, "2026-03-23 10:00:00")
            _insert_pass1_score(conn, boundary_id)

            result = _get_pass1_survivors_without_description(
                conn, since="2026-03-23 10:00:00"
            )
        finally:
            conn.close()

        assert any(r["id"] == boundary_id for r in result)

    def test_without_since_returns_all_eligible(self, tmp_path: Path) -> None:
        """AC-3: Omitting since returns every Pass 1 survivor without a description."""
        _db_path, conn = _make_db(tmp_path)
        try:
            ids = []
            for i, ts in enumerate(
                ["2026-03-20 08:00:00", "2026-03-21 12:00:00", "2026-03-23 15:00:00"],
                start=10,
            ):
                jid = _insert_job_with_fetched_at(conn, i, ts)
                _insert_pass1_score(conn, jid)
                ids.append(jid)

            result = _get_pass1_survivors_without_description(conn)
        finally:
            conn.close()

        returned_ids = {r["id"] for r in result}
        for jid in ids:
            assert jid in returned_ids

    def test_jobs_with_full_description_excluded(self, tmp_path: Path) -> None:
        """Jobs that already have a full_description are not returned."""
        _db_path, conn = _make_db(tmp_path)
        try:
            has_desc_id = _insert_job_with_fetched_at(
                conn, 20, "2026-03-23 11:00:00", full_description="already fetched"
            )
            no_desc_id = _insert_job_with_fetched_at(conn, 21, "2026-03-23 11:30:00")
            _insert_pass1_score(conn, has_desc_id)
            _insert_pass1_score(conn, no_desc_id)

            result = _get_pass1_survivors_without_description(conn)
        finally:
            conn.close()

        returned_ids = {r["id"] for r in result}
        assert has_desc_id not in returned_ids
        assert no_desc_id in returned_ids


# ---------------------------------------------------------------------------
# _get_pass1_survivors_without_description — since + limit composition (AC-4)
# ---------------------------------------------------------------------------


class TestGetPass1SurvivorsWithoutDescriptionSinceLimit:
    """AC-4: since + limit compose correctly — limit caps the since-filtered set."""

    def test_since_and_limit_compose(self, tmp_path: Path) -> None:
        """5 eligible jobs after since; limit=3 returns exactly 3."""
        _db_path, conn = _make_db(tmp_path)
        try:
            # 1 job before the since threshold (should be excluded)
            before_id = _insert_job_with_fetched_at(conn, 100, "2026-03-23 09:00:00")
            _insert_pass1_score(conn, before_id)

            # 5 eligible jobs after the since threshold
            after_ids = []
            for i in range(5):
                jid = _insert_job_with_fetched_at(conn, 101 + i, f"2026-03-23 10:{i:02d}:00")
                _insert_pass1_score(conn, jid)
                after_ids.append(jid)

            result = _get_pass1_survivors_without_description(
                conn, since="2026-03-23 10:00:00", limit=3
            )
        finally:
            conn.close()

        assert len(result) == 3
        # All returned jobs must be from the after-threshold set
        returned_ids = {r["id"] for r in result}
        assert returned_ids.issubset(set(after_ids))

    def test_limit_zero_returns_empty(self, tmp_path: Path) -> None:
        """limit=0 short-circuits and returns an empty list regardless of since."""
        _db_path, conn = _make_db(tmp_path)
        try:
            jid = _insert_job_with_fetched_at(conn, 200, "2026-03-23 11:00:00")
            _insert_pass1_score(conn, jid)

            result = _get_pass1_survivors_without_description(
                conn, since="2026-03-23 10:00:00", limit=0
            )
        finally:
            conn.close()

        assert result == []


# ---------------------------------------------------------------------------
# main() — invalid --since raises SystemExit 2 (AC-5)
# ---------------------------------------------------------------------------


class TestMainSinceValidation:
    """AC-5: main() with an invalid --since value exits with code 2."""

    def test_invalid_since_exits_with_code_2(self) -> None:
        """argparse rejects non-ISO-8601 values for --since with exit code 2."""
        with (
            patch("sys.argv", ["fetch_descriptions", "--since", "not-a-date"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_valid_t_separated_since_is_accepted(self, tmp_path: Path) -> None:
        """T-separator form (2026-03-23T10:00:00) is valid and normalized to space."""
        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_db_path",
                return_value=str(tmp_path / "jobs.db"),
            ),
            patch(
                "pipeline.scripts.fetch_descriptions.run",
                return_value={"total": 0, "successful": 0, "failed": 0},
            ) as mock_run,
            patch("sys.argv", ["fetch_descriptions", "--since", "2026-03-23T10:00:00"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 0
        # T separator must be normalized to a space before being passed to run()
        called_since = mock_run.call_args.kwargs.get("since") or mock_run.call_args[1].get(
            "since"
        )
        assert called_since == "2026-03-23 10:00:00"


# ---------------------------------------------------------------------------
# run() — backlog logging (AC-6 / AC-7)
# ---------------------------------------------------------------------------


class TestRunBacklogLogging:
    """AC-6/AC-7: run() logs backlog counts when since is set; not when omitted."""

    def test_with_since_logs_total_and_scoped_counts(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """AC-6: log contains both total eligible count and scoped count when since given."""
        import logging

        mock_conn = MagicMock()

        def fake_get_survivors(conn, *, limit=None, since=None):
            # With since: 2 scoped jobs; without since (total query): 5 total
            if since is not None:
                return [{"id": 1, "url": "https://example.com/1", "source": "test"}] * 2
            return [{"id": i, "url": f"https://example.com/{i}", "source": "test"} for i in range(5)]

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                side_effect=fake_get_survivors,
            ),
            patch("pipeline.scripts.fetch_descriptions.FullDescriptionFetcher") as mock_fetcher_cls,
            patch("pipeline.scripts.fetch_descriptions._save_description"),
            caplog.at_level(logging.INFO, logger="pipeline.scripts.fetch_descriptions"),
        ):
            mock_fetcher_cls.return_value.fetch_full_description.return_value = "text"
            run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0, since="2026-03-23 10:00:00")

        log_text = "\n".join(caplog.messages)
        assert "5" in log_text  # total eligible
        assert "after --since filter" in log_text

    def test_without_since_does_not_log_since_filter_message(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """AC-7: log does NOT contain 'after --since filter' when since is omitted."""
        import logging

        mock_conn = MagicMock()

        with (
            patch(
                "pipeline.scripts.fetch_descriptions.get_connection",
                return_value=mock_conn,
            ),
            patch(
                "pipeline.scripts.fetch_descriptions._get_pass1_survivors_without_description",
                return_value=[],
            ),
            caplog.at_level(logging.INFO, logger="pipeline.scripts.fetch_descriptions"),
        ):
            run(db_path=str(tmp_path / "jobs.db"), rate_limit=0.0)

        assert "after --since filter" not in "\n".join(caplog.messages)

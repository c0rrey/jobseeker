"""
Tests for pipeline/scripts/fetch_descriptions.py.

Covers:
- run() returns a dict with the correct keys ("total", "successful", "failed").
- Zero-jobs early-return path returns {"total": 0, "successful": 0, "failed": 0}.
- run() correctly tallies successful and failed fetches.
- run() raises RuntimeError when the database cannot be opened.
- main() calls sys.exit(0) on success and sys.exit(1) on fatal DB error.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.scripts.fetch_descriptions import main, run


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

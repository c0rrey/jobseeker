"""
Tests for pipeline/cli.py.

All external dependencies (fetchers, enrichment, prefilter, database I/O) are
mocked so that no network calls or real disk writes occur during the test run.
Tests verify:

- --help output contains all expected flags
- No-argument invocation prints help and returns exit code 0
- --fetch calls all five fetchers, normalizes, deduplicates, and prints summary
- --enrich calls run_enrichment and prints summary
- --prefilter calls run_prefilter and prints summary
- --all runs all three stages in sequence and prints all three summaries
- init_db is called before any stage runs
- Mutually exclusive flag group prevents combining --fetch and --enrich
- Stage exception is caught and returns exit code 1
- --db flag overrides the default database path
- detect_duplicates is called during --fetch after deduplicate_and_insert
- run_fetch summary dict includes dedup fields (dup_groups, dup_jobs, dup_representatives)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline.cli import (
    _build_parser,
    _print_discover_summary,
    _print_enrich_summary,
    _print_fetch_descriptions_summary,
    _print_fetch_summary,
    _print_prefilter_summary,
    main,
    run_discover,
    run_enrich,
    run_fetch,
    run_fetch_descriptions,
    run_prefilter,
)
from pipeline.src.duplicate_detector import DetectionSummary

# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_PATCH_INIT_DB = "pipeline.cli.init_db"
_PATCH_GET_CONNECTION = "pipeline.cli.get_connection"
_PATCH_GET_DB_PATH = "pipeline.cli.get_db_path"

_PATCH_ADZUNA = "pipeline.cli.AdzunaFetcher"
_PATCH_REMOTEOK = "pipeline.cli.RemoteOKFetcher"
_PATCH_LINKEDIN = "pipeline.cli.LinkedInFetcher"
_PATCH_ATS = "pipeline.cli.ATSFetcher"
_PATCH_CAREER = "pipeline.cli.CareerPageFetcher"

_PATCH_DEDUP = "pipeline.cli.deduplicate_and_insert"
_PATCH_DETECT_DUPLICATES = "pipeline.cli.detect_duplicates"
_PATCH_RUN_ENRICHMENT = "pipeline.cli.run_enrichment"
_PATCH_RUN_PREFILTER = "pipeline.cli._filter_run_prefilter"

_PATCH_DISCOVER_COMPANY = "pipeline.cli.discover_company"
_PATCH_GET_NEW_SURVIVORS = "pipeline.cli._get_new_survivor_companies"
_PATCH_GET_EXISTING_SURVIVORS = "pipeline.cli._get_existing_survivor_company_count"

_PATCH_FETCH_DESCRIPTIONS_RUN = "pipeline.cli._fetch_descriptions_run"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_db_path(tmp_path: Path) -> str:
    """Return a temporary database path string."""
    return str(tmp_path / "test.db")


@pytest.fixture()
def mock_conn() -> MagicMock:
    """Return a mock SQLite connection."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


# ---------------------------------------------------------------------------
# Parser construction tests
# ---------------------------------------------------------------------------


class TestBuildParser:
    """Tests for _build_parser()."""

    def test_returns_argumentparser(self) -> None:
        parser = _build_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_fetch_flag_present(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--fetch"])
        assert args.fetch is True

    def test_enrich_flag_present(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--enrich"])
        assert args.enrich is True

    def test_prefilter_flag_present(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--prefilter"])
        assert args.prefilter is True

    def test_all_flag_present(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--all"])
        assert args.all is True

    def test_db_flag_default_is_none(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--fetch"])
        assert args.db is None

    def test_db_flag_accepts_custom_path(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--fetch", "--db", "/tmp/custom.db"])
        assert args.db == "/tmp/custom.db"

    def test_mutually_exclusive_fetch_and_enrich(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--fetch", "--enrich"])

    def test_mutually_exclusive_fetch_and_prefilter(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--fetch", "--prefilter"])

    def test_mutually_exclusive_enrich_and_all(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--enrich", "--all"])


# ---------------------------------------------------------------------------
# Help / no-argument behaviour
# ---------------------------------------------------------------------------


class TestNoArguments:
    """CLI with no arguments prints help and exits with code 0."""

    def test_no_args_returns_zero(self, capsys: pytest.CaptureFixture) -> None:
        result = main([])
        assert result == 0

    def test_no_args_prints_usage(self, capsys: pytest.CaptureFixture) -> None:
        main([])
        captured = capsys.readouterr()
        assert "--fetch" in captured.out
        assert "--enrich" in captured.out
        assert "--prefilter" in captured.out
        assert "--all" in captured.out

    def test_help_flag_mentions_llm_note(self, capsys: pytest.CaptureFixture) -> None:
        """Help text should clarify that LLM stages run via Claude Code."""
        with pytest.raises(SystemExit):
            main(["--help"])
        captured = capsys.readouterr()
        # The description or epilog should mention LLM / subagent
        assert "LLM" in captured.out or "subagent" in captured.out


# ---------------------------------------------------------------------------
# init_db is called before any stage
# ---------------------------------------------------------------------------


class TestInitDbCalled:
    """Verify init_db is invoked before the stage runs."""

    def test_init_db_called_on_fetch(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        with (
            patch(_PATCH_INIT_DB) as mock_init,
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(0, 0)),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            result = main(["--fetch"])

        mock_init.assert_called_once_with(fake_db_path)
        assert result == 0

    def test_init_db_called_on_enrich(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        enrich_summary = {
            "companies_processed": 0,
            "sources_succeeded": {},
            "sources_failed": {},
        }
        with (
            patch(_PATCH_INIT_DB) as mock_init,
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary),
        ):
            result = main(["--enrich"])

        mock_init.assert_called_once_with(fake_db_path)
        assert result == 0

    def test_init_db_called_on_prefilter(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        prefilter_summary = {"examined": 0, "filtered": 0, "passed": 0}
        with (
            patch(_PATCH_INIT_DB) as mock_init,
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_PREFILTER, return_value=prefilter_summary),
        ):
            result = main(["--prefilter"])

        mock_init.assert_called_once_with(fake_db_path)
        assert result == 0


# ---------------------------------------------------------------------------
# --fetch stage
# ---------------------------------------------------------------------------


def _configure_fetcher_mocks(
    mock_adzuna_cls: MagicMock,
    mock_remoteok_cls: MagicMock,
    mock_linkedin_cls: MagicMock,
    mock_ats_cls: MagicMock,
    mock_career_cls: MagicMock,
    adzuna_jobs: list[Any] | None = None,
    remoteok_jobs: list[Any] | None = None,
    linkedin_jobs: list[Any] | None = None,
    ats_jobs: list[Any] | None = None,
    career_jobs: list[Any] | None = None,
) -> None:
    """Configure fetcher class mocks with sensible defaults."""
    mock_adzuna_cls.return_value.fetch.return_value = adzuna_jobs or []
    mock_remoteok_cls.return_value.fetch.return_value = remoteok_jobs or []
    mock_linkedin_cls.return_value.fetch.return_value = linkedin_jobs or []
    mock_ats_cls.return_value.fetch.return_value = ats_jobs or []
    mock_career_cls.return_value.fetch.return_value = career_jobs or []


class TestFetchStage:
    """Tests for the --fetch stage end-to-end."""

    def test_all_five_fetchers_called(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(3, 1)),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            result = main(["--fetch"])

        mock_adzuna_cls.return_value.fetch.assert_called_once()
        mock_remoteok_cls.return_value.fetch.assert_called_once()
        mock_linkedin_cls.return_value.fetch.assert_called_once()
        mock_ats_cls.return_value.fetch.assert_called_once()
        mock_career_cls.return_value.fetch.assert_called_once()
        assert result == 0

    def test_dedup_called_after_fetchers(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(0, 0)) as mock_dedup,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            main(["--fetch"])

        mock_dedup.assert_called_once()

    def test_fetch_summary_printed(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(5, 2)),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            main(["--fetch"])

        captured = capsys.readouterr()
        assert "Fetch complete" in captured.out
        assert "5 new" in captured.out
        assert "2 updated" in captured.out

    def test_fetcher_exception_is_tolerated(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """A failing fetcher should be caught; stage completes without error."""
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(0, 0)),
        ):
            # Adzuna raises; others succeed
            mock_adzuna_cls.return_value.fetch.side_effect = RuntimeError("API down")
            mock_remoteok_cls.return_value.fetch.return_value = []
            mock_linkedin_cls.return_value.fetch.return_value = []
            mock_ats_cls.return_value.fetch.return_value = []
            mock_career_cls.return_value.fetch.return_value = []
            result = main(["--fetch"])

        assert result == 0

    def test_ats_jobs_normalized_by_platform(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """ATS raw dicts with _ats_platform are dispatched to the right normalizer."""
        ats_jobs = [
            {"_ats_platform": "greenhouse", "_company_name": "Acme",
             "id": 1, "title": "Eng", "updated_at": None, "absolute_url": "https://gh.io/1",
             "location": {"name": "Remote"}},
            {"_ats_platform": "lever", "_company_name": "Beta",
             "id": "abc", "text": "Dev", "createdAt": None, "hostedUrl": "https://jobs.lever.co/beta/abc",
             "categories": {}},
            {"_ats_platform": "ashby", "_company_name": "Gamma",
             "id": "xyz", "title": "SWE", "publishedDate": None, "jobUrl": "https://jobs.ashbyhq.com/gamma/xyz",
             "locationName": None},
        ]
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(3, 0)) as mock_dedup,
        ):
            mock_adzuna_cls.return_value.fetch.return_value = []
            mock_remoteok_cls.return_value.fetch.return_value = []
            mock_linkedin_cls.return_value.fetch.return_value = []
            mock_ats_cls.return_value.fetch.return_value = ats_jobs
            mock_career_cls.return_value.fetch.return_value = []
            main(["--fetch"])

        # deduplicate_and_insert should have received 3 Job objects
        actual_jobs = mock_dedup.call_args[0][0]
        assert len(actual_jobs) == 3

    def test_unknown_ats_platform_skipped(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """ATS dicts with unknown _ats_platform are skipped (not passed to dedup)."""
        ats_jobs = [
            {"_ats_platform": "unknown_ats", "_company_name": "Acme",
             "title": "Eng", "url": "https://example.com/job/1"},
        ]
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(0, 0)) as mock_dedup,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
                ats_jobs=ats_jobs,
            )
            main(["--fetch"])

        actual_jobs = mock_dedup.call_args[0][0]
        assert len(actual_jobs) == 0


# ---------------------------------------------------------------------------
# --enrich stage
# ---------------------------------------------------------------------------


class TestEnrichStage:
    """Tests for the --enrich stage end-to-end."""

    def _enrich_summary(
        self,
        companies_processed: int = 3,
        succeeded: dict[str, int] | None = None,
        failed: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        return {
            "companies_processed": companies_processed,
            "sources_succeeded": succeeded or {"glassdoor": 3, "levelsfy": 2},
            "sources_failed": failed or {"glassdoor": 1},
        }

    def test_run_enrichment_called(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        summary = self._enrich_summary()
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_ENRICHMENT, return_value=summary) as mock_enrich,
        ):
            result = main(["--enrich"])

        mock_enrich.assert_called_once_with(mock_conn)
        assert result == 0

    def test_enrich_summary_printed(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        summary = self._enrich_summary(
            companies_processed=5,
            succeeded={"glassdoor": 5, "levelsfy": 4},
            failed={"glassdoor": 1},
        )
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_ENRICHMENT, return_value=summary),
        ):
            main(["--enrich"])

        captured = capsys.readouterr()
        assert "Enrich complete" in captured.out
        assert "5 companies" in captured.out


# ---------------------------------------------------------------------------
# --prefilter stage
# ---------------------------------------------------------------------------


class TestPrefilterStage:
    """Tests for the --prefilter stage end-to-end."""

    def test_run_prefilter_called(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        pf_summary = {"examined": 10, "filtered": 3, "passed": 7}
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary) as mock_pf,
        ):
            result = main(["--prefilter"])

        mock_pf.assert_called_once_with(mock_conn)
        assert result == 0

    def test_prefilter_summary_printed(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        pf_summary = {"examined": 20, "filtered": 8, "passed": 12}
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary),
        ):
            main(["--prefilter"])

        captured = capsys.readouterr()
        assert "Prefilter complete" in captured.out
        assert "20 jobs" in captured.out
        assert "8 filtered" in captured.out
        assert "12 passed" in captured.out


# ---------------------------------------------------------------------------
# --all stage
# ---------------------------------------------------------------------------


class TestAllStage:
    """Tests for the --all stage running fetch + prefilter + enrich in sequence."""

    def test_all_three_stages_run(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        enrich_summary = {
            "companies_processed": 2,
            "sources_succeeded": {"levelsfy": 2},
            "sources_failed": {},
        }
        pf_summary = {"examined": 5, "filtered": 1, "passed": 4}

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(4, 1)),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary) as mock_enrich,
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary) as mock_pf,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            result = main(["--all"])

        mock_enrich.assert_called_once()
        mock_pf.assert_called_once()
        assert result == 0

    def test_all_prints_three_summaries(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        enrich_summary = {
            "companies_processed": 1,
            "sources_succeeded": {},
            "sources_failed": {},
        }
        pf_summary = {"examined": 3, "filtered": 0, "passed": 3}

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(2, 0)),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary),
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            main(["--all"])

        captured = capsys.readouterr()
        assert "Fetch complete" in captured.out
        assert "Enrich complete" in captured.out
        assert "Prefilter complete" in captured.out

    def test_all_init_db_called_once(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """Even with --all, init_db should be called exactly once."""
        enrich_summary = {
            "companies_processed": 0,
            "sources_succeeded": {},
            "sources_failed": {},
        }
        pf_summary = {"examined": 0, "filtered": 0, "passed": 0}

        with (
            patch(_PATCH_INIT_DB) as mock_init,
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(0, 0)),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary),
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            main(["--all"])

        mock_init.assert_called_once()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Stage exceptions are caught and the CLI returns exit code 1."""

    def test_enrich_exception_returns_exit_code_1(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_ENRICHMENT, side_effect=RuntimeError("DB error")),
        ):
            result = main(["--enrich"])

        assert result == 1

    def test_prefilter_exception_returns_exit_code_1(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_RUN_PREFILTER, side_effect=RuntimeError("filter error")),
        ):
            result = main(["--prefilter"])

        assert result == 1


# ---------------------------------------------------------------------------
# --db flag overrides default path
# ---------------------------------------------------------------------------


class TestDbFlag:
    """The --db flag passes the custom path to init_db and get_connection."""

    def test_db_flag_overrides_default(
        self, tmp_path: Path, mock_conn: MagicMock
    ) -> None:
        custom_path = str(tmp_path / "custom.db")
        pf_summary = {"examined": 0, "filtered": 0, "passed": 0}

        with (
            patch(_PATCH_INIT_DB) as mock_init,
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH) as mock_get_db,
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary),
        ):
            main(["--prefilter", "--db", custom_path])

        # get_db_path should NOT have been called when --db is provided
        mock_get_db.assert_not_called()
        mock_init.assert_called_once_with(custom_path)


# ---------------------------------------------------------------------------
# Summary printer unit tests
# ---------------------------------------------------------------------------


class TestSummaryPrinters:
    """Unit tests for the summary formatting functions."""

    def test_print_fetch_summary(self, capsys: pytest.CaptureFixture) -> None:
        _print_fetch_summary({"fetched": 100, "inserted": 40, "updated": 60})
        out = capsys.readouterr().out
        assert "100" in out
        assert "40 new" in out
        assert "60 updated" in out

    def test_print_enrich_summary(self, capsys: pytest.CaptureFixture) -> None:
        _print_enrich_summary({
            "companies_processed": 7,
            "sources_succeeded": {"glassdoor": 6, "levelsfy": 5},
            "sources_failed": {"glassdoor": 2},
        })
        out = capsys.readouterr().out
        assert "7 companies" in out
        assert "11 source calls succeeded" in out
        assert "2 failed" in out

    def test_print_prefilter_summary(self, capsys: pytest.CaptureFixture) -> None:
        _print_prefilter_summary({"examined": 50, "filtered": 15, "passed": 35})
        out = capsys.readouterr().out
        assert "50 jobs" in out
        assert "15 filtered" in out
        assert "35 passed" in out

    def test_print_fetch_summary_zero_counts(self, capsys: pytest.CaptureFixture) -> None:
        _print_fetch_summary({"fetched": 0, "inserted": 0, "updated": 0})
        out = capsys.readouterr().out
        assert "Fetch complete" in out

    def test_print_enrich_summary_empty_sources(self, capsys: pytest.CaptureFixture) -> None:
        _print_enrich_summary({
            "companies_processed": 0,
            "sources_succeeded": {},
            "sources_failed": {},
        })
        out = capsys.readouterr().out
        assert "Enrich complete" in out
        assert "0 companies" in out


# ---------------------------------------------------------------------------
# detect_duplicates integration within --fetch
# ---------------------------------------------------------------------------


class TestFetchStageDetectDuplicates:
    """Verify detect_duplicates() is called during --fetch and its results are surfaced."""

    def test_detect_duplicates_called_after_dedup_insert(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """detect_duplicates must be invoked once during --fetch."""
        dup_summary = DetectionSummary(
            groups_created=2, jobs_grouped=6, representatives_set=2
        )
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(4, 0)),
            patch(_PATCH_DETECT_DUPLICATES, return_value=dup_summary) as mock_detect,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            result = main(["--fetch"])

        mock_detect.assert_called_once()
        assert result == 0

    def test_fetch_summary_includes_dup_fields(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """run_fetch summary dict must expose dup_groups, dup_jobs, dup_representatives."""
        dup_summary = DetectionSummary(
            groups_created=3, jobs_grouped=9, representatives_set=3
        )
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(9, 0)),
            patch(_PATCH_DETECT_DUPLICATES, return_value=dup_summary),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            summary = run_fetch(fake_db_path)

        assert summary["dup_groups"] == 3
        assert summary["dup_jobs"] == 9
        assert summary["dup_representatives"] == 3

    def test_detect_duplicates_called_with_open_connection(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """detect_duplicates must receive the same connection used for dedup_and_insert."""
        dup_summary = DetectionSummary(
            groups_created=0, jobs_grouped=0, representatives_set=0
        )
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(0, 0)),
            patch(_PATCH_DETECT_DUPLICATES, return_value=dup_summary) as mock_detect,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            run_fetch(fake_db_path)

        # detect_duplicates should have been called with the mock connection.
        call_args = mock_detect.call_args
        assert call_args is not None
        assert call_args[0][0] is mock_conn

    def test_fetch_zero_dup_groups_still_returns_valid_summary(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """A run with no duplicate groups must produce zeros for dup fields."""
        dup_summary = DetectionSummary(
            groups_created=0, jobs_grouped=0, representatives_set=0
        )
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(2, 0)),
            patch(_PATCH_DETECT_DUPLICATES, return_value=dup_summary),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            summary = run_fetch(fake_db_path)

        assert summary["dup_groups"] == 0
        assert summary["dup_jobs"] == 0
        assert summary["dup_representatives"] == 0


# ---------------------------------------------------------------------------
# --discover stage
# ---------------------------------------------------------------------------


def _discover_summary(
    new_discovered: int = 2,
    already_existing: int = 1,
    discovery_failed: int = 0,
    enrichment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical discover summary dict for test use."""
    return {
        "new_discovered": new_discovered,
        "already_existing": already_existing,
        "discovery_failed": discovery_failed,
        "enrichment": enrichment,
    }


class TestDiscoverStage:
    """Tests for the --discover CLI stage end-to-end."""

    def test_discover_flag_present_in_parser(self) -> None:
        """--discover flag must be present in the argument parser."""
        parser = _build_parser()
        args = parser.parse_args(["--discover"])
        assert args.discover is True

    def test_run_discover_called_on_discover_flag(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """main(['--discover']) must invoke run_discover and return exit code 0."""
        summary = _discover_summary()
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch("pipeline.cli.run_discover", return_value=summary) as mock_run_discover,
        ):
            result = main(["--discover"])

        mock_run_discover.assert_called_once()
        assert result == 0

    def test_discover_summary_printed(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        """The discover summary must be printed to stdout."""
        summary = _discover_summary(new_discovered=3, already_existing=2, discovery_failed=1)
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch("pipeline.cli.run_discover", return_value=summary),
        ):
            main(["--discover"])

        captured = capsys.readouterr()
        assert "Discover complete" in captured.out
        assert "3" in captured.out
        assert "2" in captured.out
        assert "1" in captured.out

    def test_run_discover_return_struct_has_required_keys(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """run_discover() return dict must contain new_discovered, already_existing,
        discovery_failed, and enrichment keys."""
        from pipeline.src.company_discovery import CompanyRecord

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_GET_NEW_SURVIVORS, return_value=["Acme Corp", "Beta Corp"]),
            patch(_PATCH_GET_EXISTING_SURVIVORS, return_value=1),
            patch(
                _PATCH_DISCOVER_COMPANY,
                return_value=CompanyRecord(company_id=1, career_page_url="https://acme.com/careers"),
            ),
            patch(_PATCH_RUN_ENRICHMENT, return_value={
                "companies_processed": 2,
                "sources_succeeded": {},
                "sources_failed": {},
            }),
        ):
            summary = run_discover(fake_db_path)

        assert "new_discovered" in summary
        assert "already_existing" in summary
        assert "discovery_failed" in summary
        assert "enrichment" in summary
        assert isinstance(summary["new_discovered"], int)
        assert isinstance(summary["already_existing"], int)
        assert isinstance(summary["discovery_failed"], int)

    def test_run_discover_counts_new_and_already_existing(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """run_discover returns correct counts when some companies are new and some exist."""
        from pipeline.src.company_discovery import CompanyRecord

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_GET_NEW_SURVIVORS, return_value=["New Corp A", "New Corp B"]),
            patch(_PATCH_GET_EXISTING_SURVIVORS, return_value=3),
            patch(
                _PATCH_DISCOVER_COMPANY,
                return_value=CompanyRecord(company_id=10, career_page_url="https://x.com/careers"),
            ),
            patch(_PATCH_RUN_ENRICHMENT, return_value={
                "companies_processed": 2,
                "sources_succeeded": {},
                "sources_failed": {},
            }),
        ):
            summary = run_discover(fake_db_path)

        assert summary["new_discovered"] == 2
        assert summary["already_existing"] == 3
        assert summary["discovery_failed"] == 0

    def test_run_discover_counts_failed_when_discover_company_returns_none(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """When discover_company returns None, the company is counted as failed."""
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_GET_NEW_SURVIVORS, return_value=["Ghost Corp"]),
            patch(_PATCH_GET_EXISTING_SURVIVORS, return_value=0),
            patch(_PATCH_DISCOVER_COMPANY, return_value=None),
        ):
            summary = run_discover(fake_db_path)

        assert summary["new_discovered"] == 0
        assert summary["discovery_failed"] == 1
        assert summary["enrichment"] is None

    def test_run_discover_skips_enrichment_when_no_new_companies(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """When no new companies are discovered, enrichment is skipped (enrichment=None)."""
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_GET_NEW_SURVIVORS, return_value=[]),
            patch(_PATCH_GET_EXISTING_SURVIVORS, return_value=5),
            patch(_PATCH_RUN_ENRICHMENT) as mock_enrich,
        ):
            summary = run_discover(fake_db_path)

        mock_enrich.assert_not_called()
        assert summary["new_discovered"] == 0
        assert summary["already_existing"] == 5
        assert summary["enrichment"] is None

    def test_run_discover_runs_enrichment_when_companies_discovered(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """When at least one company is discovered, run_enrichment is called."""
        from pipeline.src.company_discovery import CompanyRecord

        enrich_result = {
            "companies_processed": 1,
            "sources_succeeded": {"glassdoor": 1},
            "sources_failed": {},
        }

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_GET_NEW_SURVIVORS, return_value=["Fresh Corp"]),
            patch(_PATCH_GET_EXISTING_SURVIVORS, return_value=0),
            patch(
                _PATCH_DISCOVER_COMPANY,
                return_value=CompanyRecord(company_id=7, career_page_url="https://fresh.com/careers"),
            ),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_result) as mock_enrich,
        ):
            summary = run_discover(fake_db_path)

        mock_enrich.assert_called_once()
        assert summary["enrichment"] is not None
        assert summary["enrichment"]["companies_processed"] == 1

    def test_run_discover_handles_discover_company_exception_as_failed(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """An exception from discover_company is caught; the company is counted as failed."""
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_GET_NEW_SURVIVORS, return_value=["Exploding Corp"]),
            patch(_PATCH_GET_EXISTING_SURVIVORS, return_value=0),
            patch(_PATCH_DISCOVER_COMPANY, side_effect=RuntimeError("unexpected")),
        ):
            summary = run_discover(fake_db_path)

        assert summary["discovery_failed"] == 1
        assert summary["new_discovered"] == 0


# ---------------------------------------------------------------------------
# _print_discover_summary unit tests
# ---------------------------------------------------------------------------


class TestPrintDiscoverSummary:
    """Unit tests for the _print_discover_summary formatter."""

    def test_prints_discover_complete(self, capsys: pytest.CaptureFixture) -> None:
        _print_discover_summary(_discover_summary(new_discovered=2, already_existing=1))
        assert "Discover complete" in capsys.readouterr().out

    def test_prints_new_discovered_count(self, capsys: pytest.CaptureFixture) -> None:
        _print_discover_summary(_discover_summary(new_discovered=4))
        assert "4" in capsys.readouterr().out

    def test_prints_enrichment_skipped_when_no_new_companies(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        _print_discover_summary(_discover_summary(new_discovered=0, enrichment=None))
        out = capsys.readouterr().out
        assert "Enrichment skipped" in out or "no new" in out

    def test_prints_enrichment_summary_when_present(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        enrichment = {
            "companies_processed": 3,
            "sources_succeeded": {"glassdoor": 3},
            "sources_failed": {},
        }
        _print_discover_summary(_discover_summary(new_discovered=3, enrichment=enrichment))
        out = capsys.readouterr().out
        assert "3" in out


# ---------------------------------------------------------------------------
# --fetch-descriptions parser / mutual exclusion
# ---------------------------------------------------------------------------


class TestFetchDescriptionsParser:
    """Parser-level tests for --fetch-descriptions."""

    def test_fetch_descriptions_flag_present(self) -> None:
        """--fetch-descriptions must be recognised by the parser."""
        parser = _build_parser()
        args = parser.parse_args(["--fetch-descriptions"])
        assert args.fetch_descriptions is True

    def test_fetch_descriptions_mutually_exclusive_with_fetch(self) -> None:
        """--fetch and --fetch-descriptions cannot be combined (exit code 2)."""
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--fetch", "--fetch-descriptions"])
        assert exc_info.value.code == 2

    def test_fetch_descriptions_mutually_exclusive_with_enrich(self) -> None:
        """--fetch-descriptions and --enrich cannot be combined."""
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--fetch-descriptions", "--enrich"])
        assert exc_info.value.code == 2

    def test_fetch_descriptions_mutually_exclusive_with_prefilter(self) -> None:
        """--fetch-descriptions and --prefilter cannot be combined."""
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--fetch-descriptions", "--prefilter"])
        assert exc_info.value.code == 2

    def test_fetch_descriptions_mutually_exclusive_with_all(self) -> None:
        """--fetch-descriptions and --all cannot be combined."""
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--fetch-descriptions", "--all"])
        assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# _print_fetch_descriptions_summary unit tests
# ---------------------------------------------------------------------------


class TestPrintFetchDescriptionsSummary:
    """Unit tests for _print_fetch_descriptions_summary."""

    def test_nothing_to_fetch_when_total_zero(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        """When total is 0, prints 'Nothing to fetch.'."""
        _print_fetch_descriptions_summary({"total": 0, "successful": 0, "failed": 0})
        out = capsys.readouterr().out
        assert "Nothing to fetch" in out

    def test_prints_complete_message_when_total_nonzero(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        """When total > 0, prints 'Fetch-descriptions complete' with counts."""
        _print_fetch_descriptions_summary({"total": 10, "successful": 8, "failed": 2})
        out = capsys.readouterr().out
        assert "Fetch-descriptions complete" in out
        assert "10" in out
        assert "8" in out
        assert "2" in out

    def test_complete_message_shows_all_three_counts(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        """Summary line must surface total, successful, and failed values."""
        _print_fetch_descriptions_summary({"total": 5, "successful": 5, "failed": 0})
        out = capsys.readouterr().out
        assert "5" in out
        assert "successful" in out
        assert "failed" in out


# ---------------------------------------------------------------------------
# --fetch-descriptions standalone stage
# ---------------------------------------------------------------------------


class TestFetchDescriptionsStage:
    """End-to-end tests for the standalone --fetch-descriptions stage."""

    def test_run_fetch_descriptions_called(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """main(['--fetch-descriptions']) must invoke run_fetch_descriptions."""
        fd_summary = {"total": 3, "successful": 3, "failed": 0}
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_FETCH_DESCRIPTIONS_RUN, return_value=fd_summary) as mock_run,
        ):
            result = main(["--fetch-descriptions"])

        mock_run.assert_called_once_with(fake_db_path)
        assert result == 0

    def test_fetch_descriptions_summary_printed(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        """A non-empty run prints 'Fetch-descriptions complete' with counts."""
        fd_summary = {"total": 7, "successful": 6, "failed": 1}
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_FETCH_DESCRIPTIONS_RUN, return_value=fd_summary),
        ):
            main(["--fetch-descriptions"])

        out = capsys.readouterr().out
        assert "Fetch-descriptions complete" in out
        assert "7" in out
        assert "6" in out
        assert "1" in out

    def test_nothing_to_fetch_prints_nothing_to_fetch(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        """When run() returns total=0, prints 'Nothing to fetch.' and exits 0."""
        fd_summary = {"total": 0, "successful": 0, "failed": 0}
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_FETCH_DESCRIPTIONS_RUN, return_value=fd_summary),
        ):
            result = main(["--fetch-descriptions"])

        out = capsys.readouterr().out
        assert "Nothing to fetch" in out
        assert result == 0

    def test_fetch_descriptions_exception_returns_exit_code_1(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """An exception from run_fetch_descriptions propagates and returns exit code 1."""
        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_FETCH_DESCRIPTIONS_RUN, side_effect=RuntimeError("DB error")),
        ):
            result = main(["--fetch-descriptions"])

        assert result == 1

    def test_run_fetch_descriptions_wrapper_delegates_to_run(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """run_fetch_descriptions() must call the underlying _fetch_descriptions_run."""
        fd_summary = {"total": 2, "successful": 2, "failed": 0}
        with patch(_PATCH_FETCH_DESCRIPTIONS_RUN, return_value=fd_summary) as mock_run:
            result = run_fetch_descriptions(fake_db_path)

        mock_run.assert_called_once_with(fake_db_path)
        assert result == fd_summary


# ---------------------------------------------------------------------------
# --all stage: fetch-descriptions integration
# ---------------------------------------------------------------------------


class TestAllStageFetchDescriptions:
    """Tests that verify fetch-descriptions is wired into --all correctly."""

    def test_all_four_stages_run(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """--all must run fetch, fetch-descriptions, prefilter, and enrich."""
        fd_summary = {"total": 3, "successful": 3, "failed": 0}
        enrich_summary = {
            "companies_processed": 1,
            "sources_succeeded": {},
            "sources_failed": {},
        }
        pf_summary = {"examined": 5, "filtered": 1, "passed": 4}

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(4, 1)),
            patch(_PATCH_FETCH_DESCRIPTIONS_RUN, return_value=fd_summary) as mock_fd,
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary) as mock_enrich,
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary) as mock_pf,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            result = main(["--all"])

        mock_fd.assert_called_once_with(fake_db_path)
        mock_pf.assert_called_once()
        mock_enrich.assert_called_once()
        assert result == 0

    def test_all_prints_four_summaries(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        """--all must print summaries for all four stages."""
        fd_summary = {"total": 4, "successful": 4, "failed": 0}
        enrich_summary = {
            "companies_processed": 2,
            "sources_succeeded": {},
            "sources_failed": {},
        }
        pf_summary = {"examined": 6, "filtered": 2, "passed": 4}

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(3, 0)),
            patch(_PATCH_FETCH_DESCRIPTIONS_RUN, return_value=fd_summary),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary),
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            main(["--all"])

        out = capsys.readouterr().out
        assert "Fetch complete" in out
        assert "Fetch-descriptions complete" in out
        assert "Prefilter complete" in out
        assert "Enrich complete" in out

    def test_fetch_descriptions_failure_in_all_is_non_fatal(
        self, fake_db_path: str, mock_conn: MagicMock
    ) -> None:
        """If fetch-descriptions raises during --all, prefilter and enrich still run."""
        enrich_summary = {
            "companies_processed": 1,
            "sources_succeeded": {},
            "sources_failed": {},
        }
        pf_summary = {"examined": 3, "filtered": 0, "passed": 3}

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(2, 0)),
            patch(
                _PATCH_FETCH_DESCRIPTIONS_RUN,
                side_effect=RuntimeError("fetch-descriptions crashed"),
            ),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary) as mock_enrich,
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary) as mock_pf,
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            result = main(["--all"])

        # Pipeline continues and exits successfully despite fetch-descriptions failure.
        mock_pf.assert_called_once()
        mock_enrich.assert_called_once()
        assert result == 0

    def test_fetch_descriptions_failure_in_all_does_not_print_fd_summary(
        self, fake_db_path: str, mock_conn: MagicMock, capsys: pytest.CaptureFixture
    ) -> None:
        """When fetch-descriptions fails in --all, its summary is NOT printed."""
        pf_summary = {"examined": 2, "filtered": 0, "passed": 2}
        enrich_summary = {
            "companies_processed": 0,
            "sources_succeeded": {},
            "sources_failed": {},
        }

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_CONNECTION, return_value=mock_conn),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_ADZUNA) as mock_adzuna_cls,
            patch(_PATCH_REMOTEOK) as mock_remoteok_cls,
            patch(_PATCH_LINKEDIN) as mock_linkedin_cls,
            patch(_PATCH_ATS) as mock_ats_cls,
            patch(_PATCH_CAREER) as mock_career_cls,
            patch(_PATCH_DEDUP, return_value=(1, 0)),
            patch(
                _PATCH_FETCH_DESCRIPTIONS_RUN,
                side_effect=RuntimeError("network failure"),
            ),
            patch(_PATCH_RUN_ENRICHMENT, return_value=enrich_summary),
            patch(_PATCH_RUN_PREFILTER, return_value=pf_summary),
        ):
            _configure_fetcher_mocks(
                mock_adzuna_cls, mock_remoteok_cls, mock_linkedin_cls,
                mock_ats_cls, mock_career_cls,
            )
            main(["--all"])

        out = capsys.readouterr().out
        # Fetch-descriptions complete should NOT appear since it threw an exception.
        assert "Fetch-descriptions complete" not in out
        # But the other stages' summaries must still appear.
        assert "Fetch complete" in out
        assert "Prefilter complete" in out
        assert "Enrich complete" in out

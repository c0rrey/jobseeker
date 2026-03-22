"""
Tests for pipeline/scripts/format_descriptions.py and CLI integration.

Covers:
- run() returns a dict with the correct keys ("examined", "formatted", "skipped").
- run() delegates to format_descriptions() from description_formatter.
- run() with an empty database (no eligible jobs) returns zeroes.
- run() with jobs that have NULL formatted_description (eligible).
- run() with jobs that already have formatted_description (should be skipped).
- run() when the LLM callable raises an exception (per-job error isolation).
- The script's main() entry point with mocked run().
- The CLI --format-descriptions flag is recognized and dispatches correctly.
- The CLI summary printer output format.
- Edge case: db_path doesn't exist raises FileNotFoundError.
- CLI --format-descriptions is mutually exclusive with other stage flags.
"""

from __future__ import annotations

import itertools
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.cli import (
    _build_parser,
    _print_format_descriptions_summary,
    main,
    run_format_descriptions,
)
from pipeline.scripts.format_descriptions import main as script_main
from pipeline.scripts.format_descriptions import run
from pipeline.src.database import get_connection, init_db


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_PATCH_INIT_DB = "pipeline.cli.init_db"
_PATCH_GET_DB_PATH = "pipeline.cli.get_db_path"
_PATCH_FORMAT_DESCRIPTIONS_RUN = "pipeline.cli._format_descriptions_run"

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

_counter = itertools.count(start=5_000)


def _unique_url() -> str:
    return f"https://example.com/job/{next(_counter)}"


def _insert_job(
    conn: sqlite3.Connection,
    *,
    title: str = "Data Engineer",
    company: str = "Acme Corp",
    description: str | None = "We need solid SQL skills.",
    full_description: str | None = None,
    formatted_description: str | None = None,
    url: str | None = None,
) -> int:
    """Insert a minimal jobs row and return the new id."""
    job_url = url or _unique_url()
    conn.execute(
        """
        INSERT INTO jobs
            (source, source_type, url, title, company,
             description, full_description, formatted_description)
        VALUES ('test', 'api', ?, ?, ?, ?, ?, ?)
        """,
        (job_url, title, company, description, full_description, formatted_description),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid();").fetchone()[0]


def _insert_pass2_score(
    conn: sqlite3.Connection, job_id: int, *, overall: int = 75
) -> None:
    """Insert a Pass 2 score_dimensions row for *job_id*."""
    conn.execute(
        """
        INSERT INTO score_dimensions (job_id, pass, overall)
        VALUES (?, 2, ?)
        """,
        (job_id, overall),
    )
    conn.commit()


def _stub_llm(prompt: str) -> str:  # noqa: ARG001
    """Stub LLM callable that returns a fixed formatted string."""
    return "## Formatted\n\nThis is the formatted description."


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Return path to a freshly initialised SQLite database."""
    path = tmp_path / "test_fmt_script.db"
    init_db(path)
    return path


@pytest.fixture()
def conn(db_path: Path) -> sqlite3.Connection:
    """Return an open connection to the test database."""
    c = get_connection(db_path)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# run() — return-dict contract
# ---------------------------------------------------------------------------


class TestRunReturnsDictWithCorrectKeys:
    """run() returns a dict with keys 'examined', 'formatted', and 'skipped'."""

    def test_returns_dict_type(self, db_path: Path) -> None:
        result = run(db_path=str(db_path))
        assert isinstance(result, dict)

    def test_has_required_keys(self, db_path: Path) -> None:
        result = run(db_path=str(db_path))
        assert set(result.keys()) == {"examined", "formatted", "skipped"}

    def test_values_are_ints(self, db_path: Path) -> None:
        result = run(db_path=str(db_path))
        assert all(isinstance(v, int) for v in result.values())


# ---------------------------------------------------------------------------
# run() — empty database returns zeroes
# ---------------------------------------------------------------------------


class TestRunEmptyDatabase:
    """When no Pass 2 survivors need formatting, run() returns all zeros."""

    def test_all_zeros_when_no_jobs(self, db_path: Path) -> None:
        result = run(db_path=str(db_path))
        assert result == {"examined": 0, "formatted": 0, "skipped": 0}


# ---------------------------------------------------------------------------
# run() — eligible jobs (NULL formatted_description)
# ---------------------------------------------------------------------------


class TestRunEligibleJobs:
    """run() formats Pass 2 survivors with NULL formatted_description."""

    def test_formats_eligible_job(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(conn, description="Raw text.")
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        # Use format_descriptions directly with a stub LLM to avoid real API calls.
        from pipeline.src.description_formatter import format_descriptions

        result = format_descriptions(db_path, llm_callable=_stub_llm)

        assert result["examined"] == 1
        assert result["formatted"] == 1
        assert result["skipped"] == 0

        # Verify DB was updated
        verify_conn = get_connection(db_path)
        try:
            row = verify_conn.execute(
                "SELECT formatted_description FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            assert row["formatted_description"] is not None
        finally:
            verify_conn.close()

    def test_run_delegates_to_format_descriptions(self) -> None:
        """run() delegates directly to format_descriptions()."""
        expected = {"examined": 5, "formatted": 4, "skipped": 1}
        with patch(
            "pipeline.scripts.format_descriptions.format_descriptions",
            return_value=expected,
        ) as mock_fd:
            result = run(db_path="/tmp/test.db")

        mock_fd.assert_called_once_with("/tmp/test.db")
        assert result == expected


# ---------------------------------------------------------------------------
# run() — already-formatted jobs are skipped
# ---------------------------------------------------------------------------


class TestRunAlreadyFormatted:
    """Jobs with non-NULL formatted_description are not re-processed."""

    def test_skips_already_formatted(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(
            conn,
            description="Raw text.",
            formatted_description="## Already done.",
        )
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        result = run(db_path=str(db_path))

        assert result == {"examined": 0, "formatted": 0, "skipped": 0}


# ---------------------------------------------------------------------------
# run() — LLM error isolation
# ---------------------------------------------------------------------------


class TestRunLLMErrorIsolation:
    """LLM errors on individual jobs are caught; other jobs still proceed."""

    def test_llm_error_counted_as_skipped(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(conn, description="Some text.")
        _insert_pass2_score(conn, job_id, overall=70)
        conn.close()

        def failing_llm(prompt: str) -> str:  # noqa: ARG001
            raise RuntimeError("API timeout")

        from pipeline.src.description_formatter import format_descriptions

        result = format_descriptions(db_path, llm_callable=failing_llm)

        assert result["examined"] == 1
        assert result["formatted"] == 0
        assert result["skipped"] == 1

    def test_error_on_one_does_not_abort_others(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        job_a = _insert_job(conn, title="Job A", description="Text A.")
        _insert_pass2_score(conn, job_a, overall=70)
        job_b = _insert_job(conn, title="Job B", description="Text B.")
        _insert_pass2_score(conn, job_b, overall=72)
        conn.close()

        calls: list[str] = []

        def sometimes_failing_llm(prompt: str) -> str:
            calls.append(prompt)
            if len(calls) == 1:
                raise RuntimeError("first call fails")
            return "## Formatted B"

        from pipeline.src.description_formatter import format_descriptions

        result = format_descriptions(db_path, llm_callable=sometimes_failing_llm)

        assert result["examined"] == 2
        assert result["formatted"] == 1
        assert result["skipped"] == 1


# ---------------------------------------------------------------------------
# run() — db_path doesn't exist
# ---------------------------------------------------------------------------


class TestRunMissingDatabase:
    """run() raises FileNotFoundError when db_path does not exist."""

    def test_raises_file_not_found(self, tmp_path: Path) -> None:
        missing = str(tmp_path / "no_such_file.db")
        with pytest.raises(FileNotFoundError):
            run(db_path=missing)


# ---------------------------------------------------------------------------
# script main() entry point
# ---------------------------------------------------------------------------


class TestScriptMain:
    """Tests for the script's main() entry point."""

    def test_exits_0_on_success(self) -> None:
        with (
            patch(
                "pipeline.scripts.format_descriptions.get_db_path",
                return_value="/tmp/test.db",
            ),
            patch(
                "pipeline.scripts.format_descriptions.run",
                return_value={"examined": 0, "formatted": 0, "skipped": 0},
            ),
            patch("sys.argv", ["format_descriptions"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            script_main()

        assert exc_info.value.code == 0

    def test_exits_1_on_fatal_error(self) -> None:
        with (
            patch(
                "pipeline.scripts.format_descriptions.get_db_path",
                return_value="/tmp/missing.db",
            ),
            patch(
                "pipeline.scripts.format_descriptions.run",
                side_effect=FileNotFoundError("no db"),
            ),
            patch("sys.argv", ["format_descriptions"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            script_main()

        assert exc_info.value.code == 1

    def test_db_flag_passed_to_run(self) -> None:
        with (
            patch(
                "pipeline.scripts.format_descriptions.run",
                return_value={"examined": 0, "formatted": 0, "skipped": 0},
            ) as mock_run,
            patch("sys.argv", ["format_descriptions", "--db", "/custom/path.db"]),
            pytest.raises(SystemExit),
        ):
            script_main()

        mock_run.assert_called_once_with(db_path="/custom/path.db")


# ---------------------------------------------------------------------------
# CLI --format-descriptions flag
# ---------------------------------------------------------------------------


class TestCLIFormatDescriptionsFlag:
    """The --format-descriptions flag is recognized and dispatches correctly."""

    def test_flag_present_in_parser(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--format-descriptions"])
        assert args.format_descriptions is True

    def test_mutually_exclusive_with_fetch(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--format-descriptions", "--fetch"])

    def test_mutually_exclusive_with_enrich(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--format-descriptions", "--enrich"])

    def test_dispatches_to_run_format_descriptions(
        self, tmp_path: Path
    ) -> None:
        fake_db_path = str(tmp_path / "test.db")
        fmt_summary = {"examined": 5, "formatted": 4, "skipped": 1}

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(
                _PATCH_FORMAT_DESCRIPTIONS_RUN, return_value=fmt_summary
            ) as mock_fmt,
        ):
            result = main(["--format-descriptions"])

        mock_fmt.assert_called_once_with(fake_db_path)
        assert result == 0

    def test_exception_returns_exit_code_1(self, tmp_path: Path) -> None:
        fake_db_path = str(tmp_path / "test.db")

        with (
            patch(_PATCH_INIT_DB),
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(
                _PATCH_FORMAT_DESCRIPTIONS_RUN,
                side_effect=FileNotFoundError("no db"),
            ),
        ):
            result = main(["--format-descriptions"])

        assert result == 1

    def test_init_db_called(self, tmp_path: Path) -> None:
        fake_db_path = str(tmp_path / "test.db")
        fmt_summary = {"examined": 0, "formatted": 0, "skipped": 0}

        with (
            patch(_PATCH_INIT_DB) as mock_init,
            patch(_PATCH_GET_DB_PATH, return_value=fake_db_path),
            patch(_PATCH_FORMAT_DESCRIPTIONS_RUN, return_value=fmt_summary),
        ):
            main(["--format-descriptions"])

        mock_init.assert_called_once_with(fake_db_path)

    def test_no_args_help_mentions_format_descriptions(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        main([])
        captured = capsys.readouterr()
        assert "--format-descriptions" in captured.out


# ---------------------------------------------------------------------------
# CLI run_format_descriptions stage handler
# ---------------------------------------------------------------------------


class TestRunFormatDescriptionsHandler:
    """The run_format_descriptions stage handler delegates correctly."""

    def test_delegates_to_script_run(self) -> None:
        expected = {"examined": 3, "formatted": 2, "skipped": 1}
        with patch(_PATCH_FORMAT_DESCRIPTIONS_RUN, return_value=expected) as mock_run:
            result = run_format_descriptions("/tmp/test.db")

        mock_run.assert_called_once_with("/tmp/test.db")
        assert result == expected


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------


class TestPrintFormatDescriptionsSummary:
    """Tests for _print_format_descriptions_summary output."""

    def test_prints_counts(self, capsys: pytest.CaptureFixture) -> None:
        _print_format_descriptions_summary(
            {"examined": 10, "formatted": 8, "skipped": 2}
        )
        out = capsys.readouterr().out
        assert "Format-descriptions complete" in out
        assert "10" in out
        assert "8" in out
        assert "2" in out

    def test_prints_nothing_to_format_when_zero(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        _print_format_descriptions_summary(
            {"examined": 0, "formatted": 0, "skipped": 0}
        )
        out = capsys.readouterr().out
        assert "Nothing to format" in out

    def test_output_includes_field_labels(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        _print_format_descriptions_summary(
            {"examined": 5, "formatted": 3, "skipped": 2}
        )
        out = capsys.readouterr().out
        assert "Examined" in out
        assert "formatted" in out
        assert "skipped" in out

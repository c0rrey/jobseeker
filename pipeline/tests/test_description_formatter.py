"""
Tests for pipeline/src/description_formatter.py.

Covers:
- Only Pass 2 survivors (pass=2, overall > 0) with NULL formatted_description
  are targeted by the query.
- Jobs where both full_description and description are NULL are skipped (not
  sent to the LLM).
- formatted_description is written to the DB after a successful LLM call.
- The original full_description and description columns are not modified.
- Jobs that already have a non-NULL formatted_description are not re-formatted.
- Pass 1-only jobs (no Pass 2 row) are not targeted.
- Pass 2 jobs with overall = 0 (rejected) are not targeted.
- LLM errors on one job are caught and logged; other jobs still proceed.
- format_descriptions raises FileNotFoundError for a missing database path.
- The prompt rendered to the LLM callable contains the expected placeholders
  (job_id, title, company, raw_description content).
- Truncation: raw descriptions longer than MAX_RAW_CHARS are truncated before
  the LLM callable receives the prompt.
- Return value reports correct examined / formatted / skipped counts.

All tests use an in-memory-backed temporary SQLite database initialised via
init_db().  No real LLM calls are made; a stub callable is injected via the
llm_callable parameter.
"""

from __future__ import annotations

import itertools
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.src.database import get_connection, init_db
from pipeline.src.description_formatter import (
    MAX_RAW_CHARS,
    _get_pass2_survivors_needing_format,
    _render_prompt,
    _load_prompt_template,
    format_descriptions,
)


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

_counter = itertools.count(start=1_000)


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


def _insert_pass1_score(
    conn: sqlite3.Connection, job_id: int, *, overall: int = 60
) -> None:
    """Insert a Pass 1 score_dimensions row for *job_id*."""
    conn.execute(
        """
        INSERT INTO score_dimensions (job_id, pass, overall)
        VALUES (?, 1, ?)
        """,
        (job_id, overall),
    )
    conn.commit()


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Return path to a freshly initialised SQLite database."""
    path = tmp_path / "test_fmt.db"
    init_db(path)
    return path


@pytest.fixture()
def conn(db_path: Path) -> sqlite3.Connection:
    """Return an open connection to the test database."""
    c = get_connection(db_path)
    yield c
    c.close()


def _stub_llm(prompt: str) -> str:  # noqa: ARG001
    """Stub LLM callable that returns a fixed formatted string."""
    return "## Formatted\n\nThis is the formatted description."


# ---------------------------------------------------------------------------
# Tests for _get_pass2_survivors_needing_format (query layer)
# ---------------------------------------------------------------------------


class TestGetPass2SurvivorsNeedingFormat:
    """Unit tests for the SQL query helper."""

    def test_returns_pass2_survivor_with_null_formatted(self, conn: sqlite3.Connection) -> None:
        """A Pass 2 job with NULL formatted_description is returned."""
        job_id = _insert_job(conn, description="Raw description text.")
        _insert_pass2_score(conn, job_id, overall=80)

        rows = _get_pass2_survivors_needing_format(conn)

        assert len(rows) == 1
        assert rows[0]["id"] == job_id

    def test_excludes_job_with_existing_formatted_description(
        self, conn: sqlite3.Connection
    ) -> None:
        """A Pass 2 job that already has formatted_description is excluded."""
        job_id = _insert_job(
            conn,
            description="Raw text.",
            formatted_description="## Already formatted\n\nDone.",
        )
        _insert_pass2_score(conn, job_id, overall=80)

        rows = _get_pass2_survivors_needing_format(conn)

        assert rows == []

    def test_excludes_pass1_only_job(self, conn: sqlite3.Connection) -> None:
        """A job with only a Pass 1 row (no Pass 2) is not returned."""
        job_id = _insert_job(conn, description="Some text.")
        _insert_pass1_score(conn, job_id, overall=65)

        rows = _get_pass2_survivors_needing_format(conn)

        assert rows == []

    def test_excludes_pass2_rejected_job(self, conn: sqlite3.Connection) -> None:
        """A Pass 2 row with overall=0 (rejected) is not returned."""
        job_id = _insert_job(conn, description="Some text.")
        _insert_pass2_score(conn, job_id, overall=0)

        rows = _get_pass2_survivors_needing_format(conn)

        assert rows == []

    def test_excludes_job_with_both_descriptions_null(
        self, conn: sqlite3.Connection
    ) -> None:
        """A Pass 2 job where both description and full_description are NULL is excluded."""
        conn.execute(
            """
            INSERT INTO jobs
                (source, source_type, url, title, company,
                 description, full_description, formatted_description)
            VALUES ('test', 'api', ?, 'Engineer', 'Corp', NULL, NULL, NULL)
            """,
            (_unique_url(),),
        )
        conn.commit()
        job_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
        _insert_pass2_score(conn, job_id, overall=70)

        rows = _get_pass2_survivors_needing_format(conn)

        assert rows == []

    def test_prefers_full_description_over_description(
        self, conn: sqlite3.Connection
    ) -> None:
        """raw_description uses full_description when non-NULL."""
        job_id = _insert_job(
            conn,
            description="Short fallback.",
            full_description="Long full description text.",
        )
        _insert_pass2_score(conn, job_id, overall=80)

        rows = _get_pass2_survivors_needing_format(conn)

        assert len(rows) == 1
        assert rows[0]["raw_description"] == "Long full description text."

    def test_falls_back_to_description_when_full_description_null(
        self, conn: sqlite3.Connection
    ) -> None:
        """raw_description falls back to description when full_description is NULL."""
        job_id = _insert_job(
            conn,
            description="Fallback text.",
            full_description=None,
        )
        _insert_pass2_score(conn, job_id, overall=80)

        rows = _get_pass2_survivors_needing_format(conn)

        assert len(rows) == 1
        assert rows[0]["raw_description"] == "Fallback text."

    def test_truncates_raw_description_to_max_chars(
        self, conn: sqlite3.Connection
    ) -> None:
        """raw_description is truncated to MAX_RAW_CHARS characters."""
        long_text = "x" * (MAX_RAW_CHARS + 500)
        job_id = _insert_job(conn, full_description=long_text)
        _insert_pass2_score(conn, job_id, overall=80)

        rows = _get_pass2_survivors_needing_format(conn)

        assert len(rows) == 1
        assert len(rows[0]["raw_description"]) == MAX_RAW_CHARS

    def test_returns_multiple_qualifying_jobs(self, conn: sqlite3.Connection) -> None:
        """All qualifying jobs are returned when multiple exist."""
        job_ids = []
        for i in range(3):
            jid = _insert_job(conn, title=f"Role {i}", description="Text.")
            _insert_pass2_score(conn, jid, overall=70 + i)
            job_ids.append(jid)

        rows = _get_pass2_survivors_needing_format(conn)

        returned_ids = {r["id"] for r in rows}
        assert returned_ids == set(job_ids)


# ---------------------------------------------------------------------------
# Tests for _render_prompt
# ---------------------------------------------------------------------------


class TestRenderPrompt:
    """Unit tests for the prompt rendering helper."""

    def test_substitutes_all_placeholders(self) -> None:
        """All {{ variable }} placeholders are replaced."""
        template = "ID={{ job_id }} T={{ title }} C={{ company }} D={{ raw_description }}"
        result = _render_prompt(
            template,
            job_id=42,
            title="Data Engineer",
            company="Acme",
            raw_description="Build pipelines.",
        )
        assert result == "ID=42 T=Data Engineer C=Acme D=Build pipelines."

    def test_handles_none_title_and_company(self) -> None:
        """None values for title/company become empty strings."""
        template = "T={{ title }} C={{ company }}"
        result = _render_prompt(
            template,
            job_id=1,
            title=None,  # type: ignore[arg-type]
            company=None,  # type: ignore[arg-type]
            raw_description="text",
        )
        assert result == "T= C="

    def test_job_id_is_string_in_output(self) -> None:
        """job_id integer is converted to string in the rendered prompt."""
        template = "{{ job_id }}"
        result = _render_prompt(
            template,
            job_id=99,
            title="T",
            company="C",
            raw_description="D",
        )
        assert result == "99"


# ---------------------------------------------------------------------------
# Tests for _load_prompt_template
# ---------------------------------------------------------------------------


class TestLoadPromptTemplate:
    """Unit tests for the prompt template loader."""

    def test_loads_template_with_expected_placeholders(self) -> None:
        """The loaded template contains all required {{ variable }} placeholders."""
        template = _load_prompt_template()
        assert "{{ job_id }}" in template
        assert "{{ title }}" in template
        assert "{{ company }}" in template
        assert "{{ raw_description }}" in template

    def test_template_instructs_markdown_output(self) -> None:
        """The template instructs the LLM to produce markdown."""
        template = _load_prompt_template()
        assert "markdown" in template.lower()


# ---------------------------------------------------------------------------
# Tests for format_descriptions (integration via stub LLM)
# ---------------------------------------------------------------------------


class TestFormatDescriptions:
    """Integration tests for the public format_descriptions function."""

    def test_formats_qualifying_job_and_writes_to_db(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """formatted_description is written for a qualifying Pass 2 survivor."""
        job_id = _insert_job(conn, description="Raw job text.")
        _insert_pass2_score(conn, job_id, overall=75)
        conn.close()

        stats = format_descriptions(db_path, llm_callable=_stub_llm)

        # Re-open to verify DB state
        verify_conn = get_connection(db_path)
        try:
            row = verify_conn.execute(
                "SELECT formatted_description FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            assert row["formatted_description"] == "## Formatted\n\nThis is the formatted description."
        finally:
            verify_conn.close()

        assert stats["examined"] == 1
        assert stats["formatted"] == 1
        assert stats["skipped"] == 0

    def test_does_not_modify_original_description_columns(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """full_description and description are not altered by the formatter."""
        original_description = "Original description text."
        original_full = "Original full description text, much longer."
        job_id = _insert_job(
            conn,
            description=original_description,
            full_description=original_full,
        )
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        format_descriptions(db_path, llm_callable=_stub_llm)

        verify_conn = get_connection(db_path)
        try:
            row = verify_conn.execute(
                "SELECT description, full_description FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            assert row["description"] == original_description
            assert row["full_description"] == original_full
        finally:
            verify_conn.close()

    def test_skips_jobs_with_both_descriptions_null(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """Jobs where both full_description and description are NULL are skipped."""
        conn.execute(
            """
            INSERT INTO jobs
                (source, source_type, url, title, company,
                 description, full_description)
            VALUES ('test', 'api', ?, 'Engineer', 'Corp', NULL, NULL)
            """,
            (_unique_url(),),
        )
        conn.commit()
        job_id = conn.execute("SELECT last_insert_rowid();").fetchone()[0]
        _insert_pass2_score(conn, job_id, overall=70)
        conn.close()

        llm_mock = MagicMock(return_value="## Formatted")
        stats = format_descriptions(db_path, llm_callable=llm_mock)

        # LLM must not have been called since there is nothing to format
        llm_mock.assert_not_called()
        assert stats["examined"] == 0
        assert stats["formatted"] == 0

    def test_skips_already_formatted_jobs(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """Jobs that already have formatted_description are not re-processed."""
        existing_formatted = "## Existing\n\nAlready done."
        job_id = _insert_job(
            conn,
            description="Raw text.",
            formatted_description=existing_formatted,
        )
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        llm_mock = MagicMock(return_value="## New")
        stats = format_descriptions(db_path, llm_callable=llm_mock)

        llm_mock.assert_not_called()
        assert stats["examined"] == 0

    def test_skips_pass1_only_jobs(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """Jobs with only a Pass 1 score (no Pass 2) are not targeted."""
        job_id = _insert_job(conn, description="Some text.")
        _insert_pass1_score(conn, job_id, overall=65)
        conn.close()

        llm_mock = MagicMock(return_value="## Formatted")
        stats = format_descriptions(db_path, llm_callable=llm_mock)

        llm_mock.assert_not_called()
        assert stats["examined"] == 0

    def test_skips_pass2_rejected_jobs(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """Pass 2 jobs with overall=0 are not targeted."""
        job_id = _insert_job(conn, description="Some text.")
        _insert_pass2_score(conn, job_id, overall=0)
        conn.close()

        llm_mock = MagicMock(return_value="## Formatted")
        stats = format_descriptions(db_path, llm_callable=llm_mock)

        llm_mock.assert_not_called()
        assert stats["examined"] == 0

    def test_llm_error_is_caught_and_job_is_skipped(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """An LLM error on one job is caught; that job is counted as skipped."""
        job_id = _insert_job(conn, description="Some text.")
        _insert_pass2_score(conn, job_id, overall=70)
        conn.close()

        def failing_llm(prompt: str) -> str:  # noqa: ARG001
            raise RuntimeError("API timeout")

        stats = format_descriptions(db_path, llm_callable=failing_llm)

        assert stats["examined"] == 1
        assert stats["formatted"] == 0
        assert stats["skipped"] == 1

        # formatted_description should remain NULL
        verify_conn = get_connection(db_path)
        try:
            row = verify_conn.execute(
                "SELECT formatted_description FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            assert row["formatted_description"] is None
        finally:
            verify_conn.close()

    def test_llm_error_on_one_job_does_not_abort_remaining(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """An LLM error on job A does not prevent job B from being formatted."""
        job_a = _insert_job(conn, title="Job A", description="Text A.")
        _insert_pass2_score(conn, job_a, overall=70)
        job_b = _insert_job(conn, title="Job B", description="Text B.")
        _insert_pass2_score(conn, job_b, overall=72)
        conn.close()

        calls = []

        def sometimes_failing_llm(prompt: str) -> str:
            calls.append(prompt)
            if len(calls) == 1:
                raise RuntimeError("first call fails")
            return "## Formatted B"

        stats = format_descriptions(db_path, llm_callable=sometimes_failing_llm)

        assert stats["examined"] == 2
        assert stats["formatted"] == 1
        assert stats["skipped"] == 1

        verify_conn = get_connection(db_path)
        try:
            row_b = verify_conn.execute(
                "SELECT formatted_description FROM jobs WHERE id = ?", (job_b,)
            ).fetchone()
            assert row_b["formatted_description"] == "## Formatted B"
        finally:
            verify_conn.close()

    def test_raises_file_not_found_for_missing_db(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when db_path does not exist."""
        missing = tmp_path / "no_such_file.db"
        with pytest.raises(FileNotFoundError):
            format_descriptions(missing, llm_callable=_stub_llm)

    def test_prompt_sent_to_llm_contains_raw_description(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """The prompt passed to llm_callable contains the job's raw description."""
        raw_text = "Unique sentinel text: abc123xyz."
        job_id = _insert_job(conn, description=raw_text)
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        captured_prompts: list[str] = []

        def capturing_llm(prompt: str) -> str:
            captured_prompts.append(prompt)
            return "## Done"

        format_descriptions(db_path, llm_callable=capturing_llm)

        assert len(captured_prompts) == 1
        assert raw_text in captured_prompts[0]

    def test_prompt_sent_to_llm_contains_title_and_company(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """The prompt passed to llm_callable contains the job title and company."""
        job_id = _insert_job(
            conn, title="Staff Data Engineer", company="WidgetCorp", description="Text."
        )
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        captured_prompts: list[str] = []

        def capturing_llm(prompt: str) -> str:
            captured_prompts.append(prompt)
            return "## Done"

        format_descriptions(db_path, llm_callable=capturing_llm)

        assert "Staff Data Engineer" in captured_prompts[0]
        assert "WidgetCorp" in captured_prompts[0]

    def test_returns_correct_counts_for_mixed_jobs(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """Examined/formatted/skipped counts are accurate for a mixed set."""
        # 2 qualifying, 1 already formatted, 1 pass-1 only
        jid1 = _insert_job(conn, title="Job 1", description="Text 1.")
        _insert_pass2_score(conn, jid1, overall=70)

        jid2 = _insert_job(conn, title="Job 2", description="Text 2.")
        _insert_pass2_score(conn, jid2, overall=75)

        jid3 = _insert_job(
            conn, title="Job 3", description="Text 3.", formatted_description="## Done"
        )
        _insert_pass2_score(conn, jid3, overall=80)

        jid4 = _insert_job(conn, title="Job 4", description="Text 4.")
        _insert_pass1_score(conn, jid4, overall=55)

        conn.close()

        stats = format_descriptions(db_path, llm_callable=_stub_llm)

        assert stats["examined"] == 2
        assert stats["formatted"] == 2
        assert stats["skipped"] == 0

    def test_full_description_preferred_over_description_in_prompt(
        self, db_path: Path, conn: sqlite3.Connection
    ) -> None:
        """full_description text appears in the LLM prompt, not description text."""
        job_id = _insert_job(
            conn,
            description="Short fallback.",
            full_description="Long full description sentinel: zzz999.",
        )
        _insert_pass2_score(conn, job_id, overall=80)
        conn.close()

        captured_prompts: list[str] = []

        def capturing_llm(prompt: str) -> str:
            captured_prompts.append(prompt)
            return "## Done"

        format_descriptions(db_path, llm_callable=capturing_llm)

        assert "zzz999" in captured_prompts[0]
        assert "Short fallback" not in captured_prompts[0]

    def test_returns_zero_counts_when_no_jobs_qualify(
        self, db_path: Path
    ) -> None:
        """Returns all-zero stats when the database has no qualifying jobs."""
        stats = format_descriptions(db_path, llm_callable=_stub_llm)

        assert stats == {"examined": 0, "formatted": 0, "skipped": 0}

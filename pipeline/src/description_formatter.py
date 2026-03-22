"""
Description formatter for the Jobseeker V2 pipeline.

Queries Pass 2 survivor jobs whose ``formatted_description`` column is NULL,
sends each job's raw text (``full_description`` preferred, ``description`` as
fallback) to an LLM with a formatting prompt, and writes the returned clean
markdown back to the ``formatted_description`` column of the corresponding
``jobs`` row.

The module is designed to be called by a subagent (no CLI flag required).
Database writes are sequential (one UPDATE per job) so there is no concurrent
write contention.

Usage example::

    from pipeline.src.description_formatter import format_descriptions
    stats = format_descriptions("data/jobs.db")

LLM contract
------------
The LLM callable receives a single string (the rendered prompt) and must
return a single string (the formatted markdown).  The default implementation
calls the Anthropic Messages API directly using ``anthropic.Anthropic()``.
Pass a custom callable via *llm_callable* to substitute a stub in tests.

Input truncation
----------------
Raw descriptions are truncated to :data:`MAX_RAW_CHARS` characters before
being sent to the LLM.  This guards against context-limit errors on unusually
long descriptions (e.g. full HTML dumps).

Schema reference (database.py: _CREATE_JOBS):
    jobs(id, ..., description TEXT, full_description TEXT,
         formatted_description TEXT, ...)
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum characters of raw description text sent to the LLM per job.
# Keeps the prompt well within typical 200 k-token context windows while
# still covering the vast majority of real job descriptions.
MAX_RAW_CHARS: int = 32_000

# Path to the Jinja-style prompt template, relative to the project root.
_PROMPT_PATH: Path = Path(__file__).parent.parent / "prompts" / "format_description.md"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_prompt_template() -> str:
    """Read and return the format_description.md prompt template.

    Returns:
        Raw template string with ``{{ variable }}`` placeholders.

    Raises:
        FileNotFoundError: If the prompt file does not exist at the expected path.
    """
    return _PROMPT_PATH.read_text(encoding="utf-8")


def _render_prompt(
    template: str,
    *,
    job_id: int,
    title: str,
    company: str,
    raw_description: str,
) -> str:
    """Substitute template placeholders with job-specific values.

    Uses simple string replacement on Jinja-style ``{{ variable }}``
    placeholders.  No Jinja2 dependency is required.

    Args:
        template: Raw prompt template string from ``format_description.md``.
        job_id: Database primary key of the job being formatted.
        title: Job title.
        company: Company name.
        raw_description: Raw description text (pre-truncated to
            :data:`MAX_RAW_CHARS` characters by the caller).

    Returns:
        Fully rendered prompt string ready to send to the LLM.
    """
    return (
        template.replace("{{ job_id }}", str(job_id))
        .replace("{{ title }}", title or "")
        .replace("{{ company }}", company or "")
        .replace("{{ raw_description }}", raw_description)
    )


def _default_llm_callable(prompt: str) -> str:
    """Call the Anthropic API and return the text response.

    This is the production LLM backend.  It is only imported and invoked when
    no custom *llm_callable* is passed to :func:`format_descriptions`.

    Args:
        prompt: Fully rendered prompt string.

    Returns:
        LLM response text (formatted markdown).

    Raises:
        anthropic.APIError: On API-level errors (rate limits, auth failures,
            etc.).
    """
    import anthropic  # deferred import so tests never need the SDK

    client = anthropic.Anthropic()
    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def _get_pass2_survivors_needing_format(
    conn: sqlite3.Connection,
) -> list[dict]:
    """Query Pass 2 survivor jobs with NULL formatted_description.

    A job qualifies when:
    - It has a Pass 2 row in ``score_dimensions`` (pass=2, overall > 0), AND
    - Its ``formatted_description`` column in ``jobs`` is NULL, AND
    - At least one of ``full_description`` or ``description`` is non-NULL
      (so there is something to format).

    Args:
        conn: Open SQLite connection.

    Returns:
        List of dicts with keys: id, title, company, raw_description.
        ``raw_description`` is COALESCE(full_description, description),
        truncated to :data:`MAX_RAW_CHARS` characters.
        Empty list when no qualifying jobs exist.
    """
    cursor = conn.execute(
        f"""
        SELECT
            j.id,
            j.title,
            j.company,
            SUBSTR(COALESCE(j.full_description, j.description), 1,
                   {MAX_RAW_CHARS}) AS raw_description
        FROM jobs j
        INNER JOIN score_dimensions sd
            ON sd.job_id = j.id
            AND sd.pass = 2
            AND sd.overall > 0
        WHERE j.formatted_description IS NULL
          AND COALESCE(j.full_description, j.description) IS NOT NULL
        ORDER BY j.id
        """
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def _write_formatted_description(
    conn: sqlite3.Connection, job_id: int, formatted: str
) -> None:
    """Write a formatted description to the jobs table.

    Updates only ``formatted_description``; ``full_description`` and
    ``description`` are never touched.

    Args:
        conn: Open SQLite connection.
        job_id: Primary key of the job to update.
        formatted: Formatted markdown string returned by the LLM.
    """
    conn.execute(
        "UPDATE jobs SET formatted_description = ? WHERE id = ?",
        (formatted, job_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def format_descriptions(
    db_path: str | Path,
    llm_callable: Callable[[str], str] | None = None,
) -> dict[str, int]:
    """Format job descriptions for Pass 2 survivors and persist results.

    Queries all Pass 2 survivor jobs (score_dimensions.pass=2, overall > 0)
    whose ``formatted_description`` is NULL, sends each to the LLM for
    markdown formatting, and writes the result to the ``formatted_description``
    column.  Jobs where both ``full_description`` and ``description`` are NULL
    are skipped (nothing to format).

    Only ``formatted_description`` is written; ``full_description`` and
    ``description`` columns are never modified.

    Args:
        db_path: Filesystem path to the SQLite database.  The file must already
            exist (as returned by :func:`pipeline.src.database.init_db`).
        llm_callable: Optional callable ``(prompt: str) -> str`` that performs
            the LLM call.  Defaults to :func:`_default_llm_callable` which
            uses the Anthropic API.  Pass a stub in tests to avoid real API
            calls.

    Returns:
        Dict with keys:
            - ``"examined"``: number of qualifying jobs found.
            - ``"formatted"``: number of jobs successfully formatted and saved.
            - ``"skipped"``: number of jobs skipped due to LLM errors.

    Raises:
        FileNotFoundError: If *db_path* does not exist.
        sqlite3.Error: If a database operation fails unexpectedly.
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Database file not found: {path!r}. "
            "Run init_db() first or check the path."
        )

    if llm_callable is None:
        llm_callable = _default_llm_callable

    template = _load_prompt_template()

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")

    try:
        jobs = _get_pass2_survivors_needing_format(conn)
        examined = len(jobs)
        formatted_count = 0
        skipped_count = 0

        logger.info(
            "format_descriptions: %d Pass 2 survivor(s) need formatting", examined
        )

        for job in jobs:
            job_id: int = job["id"]
            title: str = job["title"] or ""
            company: str = job["company"] or ""
            raw_description: str = job["raw_description"] or ""

            prompt = _render_prompt(
                template,
                job_id=job_id,
                title=title,
                company=company,
                raw_description=raw_description,
            )

            try:
                formatted = llm_callable(prompt)
                _write_formatted_description(conn, job_id, formatted)
                formatted_count += 1
                logger.info(
                    "format_descriptions: formatted job_id=%d (%s — %s)",
                    job_id,
                    company,
                    title,
                )
            except Exception as exc:  # noqa: BLE001
                skipped_count += 1
                logger.warning(
                    "format_descriptions: skipping job_id=%d due to LLM error: %s",
                    job_id,
                    exc,
                )

        logger.info(
            "format_descriptions: done — examined=%d formatted=%d skipped=%d",
            examined,
            formatted_count,
            skipped_count,
        )
        return {
            "examined": examined,
            "formatted": formatted_count,
            "skipped": skipped_count,
        }
    finally:
        conn.close()

"""
Profile evolution analysis for the Jobseeker V2 pipeline.

Determines when there are enough new feedback signals to warrant re-analyzing
the candidate profile and generating improvement suggestions.

Public API
----------
should_run_evolution(db_connection)
    Returns True when 20+ feedback rows exist that were created after the most
    recent profile_snapshots row.  When profile_snapshots is empty, counts ALL
    feedback rows.

get_feedback_with_scores(db_connection)
    Joins feedback, score_dimensions (pass=2 preferred, else pass=1), and jobs
    to build full context rows for the evolution subagent.

apply_approved_suggestions(db_connection, profile_yaml_path)
    Reads profile_suggestions where status='approved', applies each
    suggested_change JSON diff to the YAML file at *profile_yaml_path*, and
    marks the suggestion processed.

Schema gap note
---------------
The profile_suggestions CHECK constraint only permits 'pending', 'approved',
and 'rejected'.  There is no 'applied' or 'conflict' state in the DB schema
(database.py L155).  Until a migration adds those states, this module uses
'rejected' as a proxy for both "successfully applied" (cannot be re-processed)
and "conflict" (application failed).  A note stored in the JSON of the row's
reasoning column distinguishes the two cases.  This is documented as an
adjacent issue for seek-15; do NOT fix it here.

PITFALL: The feedback table allows multiple signals per job (no UNIQUE
constraint on job_id).  The evolution subagent prompt notes this so the LLM
does not double-count or miscalculate signal strength.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Union

import yaml


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EVOLUTION_THRESHOLD: int = 20

# Status values available in the DB CHECK constraint.
_STATUS_APPROVED: str = "approved"
_STATUS_REJECTED: str = "rejected"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def should_run_evolution(db_connection: sqlite3.Connection) -> bool:
    """Return True when enough new feedback exists to warrant an evolution run.

    Compares the count of feedback rows created after the most recent
    ``profile_snapshots.created_at`` timestamp against the threshold of
    :data:`EVOLUTION_THRESHOLD` (20).

    When ``profile_snapshots`` is empty (bootstrap / first run), ALL feedback
    rows are counted — so any pipeline run with 20+ total feedback signals
    will trigger evolution.

    Args:
        db_connection: Open SQLite connection to the pipeline database.
            Both ``profile_snapshots`` and ``feedback`` tables must exist.

    Returns:
        ``True`` if the count of qualifying feedback rows is >= 20;
        ``False`` otherwise.
    """
    # Find the most recent snapshot timestamp; None when the table is empty.
    row = db_connection.execute(
        "SELECT MAX(created_at) FROM profile_snapshots"
    ).fetchone()
    latest_snapshot_ts: str | None = row[0] if row else None

    if latest_snapshot_ts is None:
        # Bootstrap: count all feedback rows regardless of timestamp.
        count_row = db_connection.execute(
            "SELECT COUNT(*) FROM feedback"
        ).fetchone()
    else:
        # Only count feedback that arrived after the last snapshot.
        count_row = db_connection.execute(
            "SELECT COUNT(*) FROM feedback WHERE created_at > ?",
            (latest_snapshot_ts,),
        ).fetchone()

    feedback_count: int = count_row[0] if count_row else 0
    return feedback_count >= EVOLUTION_THRESHOLD


def get_feedback_with_scores(
    db_connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Return feedback rows joined with scores and job metadata.

    Each row provides the evolution subagent with the full context it needs to
    identify patterns: which roles were liked/disliked, their scores, and key
    job attributes.

    The join prefers a Pass 2 (deep analysis) score_dimensions row when
    available, falling back to a Pass 1 (fast filter) row.  Jobs with no
    score at all are still included; their score fields will be ``None``.

    Note: Multiple feedback rows can exist per job (the feedback table has no
    UNIQUE constraint on job_id).  The returned list may contain more rows
    than unique jobs.  The subagent prompt documents this behavior.

    Args:
        db_connection: Open SQLite connection to the pipeline database.

    Returns:
        List of dicts with keys:
            feedback_id, job_id, signal, note, feedback_created_at,
            title, company, location,
            overall, role_fit, skills_gap, culture_signals,
            growth_potential, comp_alignment, score_pass, reasoning.
        Empty list when no feedback exists.
    """
    # Prefer pass=2 over pass=1; use MAX(pass) to pick the higher pass when
    # both exist.  COALESCE is not needed because MAX ignores NULLs naturally.
    sql = """
        SELECT
            f.id            AS feedback_id,
            f.job_id        AS job_id,
            f.signal        AS signal,
            f.note          AS note,
            f.created_at    AS feedback_created_at,
            j.title         AS title,
            j.company       AS company,
            j.location      AS location,
            sd.overall      AS overall,
            sd.role_fit     AS role_fit,
            sd.skills_gap   AS skills_gap,
            sd.culture_signals AS culture_signals,
            sd.growth_potential AS growth_potential,
            sd.comp_alignment AS comp_alignment,
            sd.pass         AS score_pass,
            sd.reasoning    AS reasoning
        FROM feedback f
        JOIN jobs j ON j.id = f.job_id
        LEFT JOIN score_dimensions sd
            ON sd.job_id = f.job_id
            AND sd.pass = (
                SELECT MAX(sd2.pass)
                FROM score_dimensions sd2
                WHERE sd2.job_id = f.job_id
            )
        ORDER BY f.created_at DESC
    """
    cursor = db_connection.execute(sql)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def apply_approved_suggestions(
    db_connection: sqlite3.Connection,
    profile_yaml_path: Union[str, Path],
) -> tuple[int, int]:
    """Apply approved profile suggestions to the YAML file on disk.

    Reads all ``profile_suggestions`` rows with ``status='approved'``, parses
    each row's ``suggested_change`` JSON, and applies the specified diff to the
    profile YAML file.  Each application is atomic: the updated YAML is written
    to a temporary file and then renamed over the original.

    After processing each suggestion:
    - On success: status is updated to ``'rejected'`` (the only DB-available
      terminal state; see "Schema gap note" in the module docstring) with
      ``resolved_at`` set to the current timestamp.
    - On failure (key not found, JSON decode error, etc.): the suggestion is
      also marked ``'rejected'`` and the failure reason is appended to its
      ``reasoning`` column with a ``[CONFLICT]`` prefix.  Processing continues
      with the next suggestion.

    The YAML write uses an atomic temp-file-then-rename sequence to prevent
    partial writes on crash.

    Args:
        db_connection: Open SQLite connection with ``profile_suggestions`` table.
        profile_yaml_path: Path to ``profile.yaml`` that will be modified.

    Returns:
        Tuple of ``(applied_count, conflict_count)``.

    Raises:
        FileNotFoundError: If *profile_yaml_path* does not exist.
        yaml.YAMLError: If the profile YAML cannot be parsed on the first read.
    """
    yaml_path = Path(profile_yaml_path)

    # Load current profile YAML once (raise early if file is missing/malformed).
    with yaml_path.open(encoding="utf-8") as fh:
        profile: dict[str, Any] = yaml.safe_load(fh) or {}

    # Fetch all approved suggestions ordered by creation date (FIFO).
    cursor = db_connection.execute(
        """
        SELECT id, suggestion_type, suggested_change, reasoning
        FROM profile_suggestions
        WHERE status = 'approved'
        ORDER BY created_at ASC
        """,
    )
    suggestions = cursor.fetchall()

    applied_count = 0
    conflict_count = 0

    for suggestion in suggestions:
        suggestion_id: int = suggestion["id"]
        suggestion_type: str = suggestion["suggestion_type"]
        suggested_change_raw: str = suggestion["suggested_change"]
        original_reasoning: str = suggestion["reasoning"]

        conflict_reason: str | None = None

        try:
            change: dict[str, Any] = json.loads(suggested_change_raw)
            _apply_change(profile, suggestion_type, change)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            conflict_reason = f"[CONFLICT] Failed to apply suggestion: {exc}"

        if conflict_reason is None:
            # Write updated YAML atomically.
            _write_yaml_atomic(yaml_path, profile)
            _mark_suggestion(
                db_connection,
                suggestion_id,
                status=_STATUS_REJECTED,
                reasoning=original_reasoning,
            )
            applied_count += 1
        else:
            # Mark as conflict (using rejected — schema limitation).
            _mark_suggestion(
                db_connection,
                suggestion_id,
                status=_STATUS_REJECTED,
                reasoning=f"{original_reasoning}\n{conflict_reason}",
            )
            conflict_count += 1

    db_connection.commit()
    return applied_count, conflict_count


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_change(
    profile: dict[str, Any],
    suggestion_type: str,
    change: dict[str, Any],
) -> None:
    """Mutate *profile* in place according to *suggestion_type* and *change*.

    Supported suggestion types and their expected *change* fields:

    - ``add_skill``: ``{"skill": str}`` — appends to ``profile["skills"]``.
    - ``remove_skill``: ``{"skill": str}`` — removes from ``profile["skills"]``.
    - ``adjust_weight``: ``{"key": str, "value": ...}`` — sets a top-level key.
    - ``add_keyword``: ``{"list": str, "keyword": str}`` — appends a keyword to
      the named top-level list (e.g., ``"title_keywords"``).
    - ``remove_keyword``: ``{"list": str, "keyword": str}`` — removes from list.
    - ``set_field``: ``{"key": str, "value": ...}`` — generic top-level field set.
    - ``update_freeform``: ``{"value": str}`` — replaces ``freeform_preferences``.

    Args:
        profile: Mutable profile dict (modified in place).
        suggestion_type: One of the type strings listed above.
        change: JSON-decoded dict describing the specific mutation.

    Raises:
        KeyError: If a required key is missing from *change* or *profile*.
        ValueError: If the suggestion_type is unrecognized.
    """
    if suggestion_type == "add_skill":
        skill: str = change["skill"]
        skills: list[str] = profile.setdefault("skills", [])
        if skill not in skills:
            skills.append(skill)

    elif suggestion_type == "remove_skill":
        skill = change["skill"]
        skills = profile.get("skills", [])
        if skill in skills:
            skills.remove(skill)
        else:
            raise KeyError(f"Skill '{skill}' not found in profile skills list")

    elif suggestion_type == "adjust_weight":
        key: str = change["key"]
        value = change["value"]
        if key not in profile:
            raise KeyError(f"Key '{key}' not found in profile")
        profile[key] = value

    elif suggestion_type == "add_keyword":
        list_name: str = change["list"]
        keyword: str = change["keyword"]
        target_list: list = profile.setdefault(list_name, [])
        if keyword not in target_list:
            target_list.append(keyword)

    elif suggestion_type == "remove_keyword":
        list_name = change["list"]
        keyword = change["keyword"]
        target_list = profile.get(list_name, [])
        if keyword in target_list:
            target_list.remove(keyword)
        else:
            raise KeyError(
                f"Keyword '{keyword}' not found in profile['{list_name}']"
            )

    elif suggestion_type == "set_field":
        key = change["key"]
        value = change["value"]
        profile[key] = value

    elif suggestion_type == "update_freeform":
        profile["freeform_preferences"] = change["value"]

    else:
        raise ValueError(f"Unrecognized suggestion_type: '{suggestion_type}'")


def _write_yaml_atomic(yaml_path: Path, profile: dict[str, Any]) -> None:
    """Write *profile* as YAML to *yaml_path* via an atomic temp-file rename.

    Creates a temporary file in the same directory as *yaml_path*, writes the
    YAML content, flushes and closes, then renames the temp file over the
    target.  On POSIX systems, ``os.replace`` is atomic.

    Args:
        yaml_path: Destination path for the YAML file.
        profile: Dict to serialise.
    """
    dir_path = yaml_path.parent
    # NamedTemporaryFile in the same directory ensures rename stays on same fs.
    fd, tmp_name = tempfile.mkstemp(dir=dir_path, suffix=".yaml.tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            yaml.dump(
                profile,
                fh,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        os.replace(tmp_name, yaml_path)
    except Exception:
        # Clean up the temp file if rename failed.
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _mark_suggestion(
    db_connection: sqlite3.Connection,
    suggestion_id: int,
    *,
    status: str,
    reasoning: str,
) -> None:
    """Update a profile_suggestions row after processing.

    Sets ``status``, ``resolved_at`` (to current DB time), and ``reasoning``
    (which may carry a ``[CONFLICT]`` annotation).

    Args:
        db_connection: Open SQLite connection.
        suggestion_id: Primary key of the suggestion row to update.
        status: New status value ('rejected' — schema limit; see module note).
        reasoning: Updated reasoning text (may include conflict annotation).
    """
    db_connection.execute(
        """
        UPDATE profile_suggestions
        SET status = ?,
            resolved_at = datetime('now'),
            reasoning = ?
        WHERE id = ?
        """,
        (status, reasoning, suggestion_id),
    )

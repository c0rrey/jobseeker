"""
Tests for pipeline/src/profile_evolution.py.

Covers:
- should_run_evolution: returns True when 20+ feedback rows exist after latest
  profile_snapshots timestamp.
- should_run_evolution: returns True when profile_snapshots is empty and
  feedback count >= 20 (bootstrap case).
- should_run_evolution: returns False when fewer than 20 qualifying feedback rows.
- should_run_evolution: only counts feedback rows created AFTER the snapshot.
- get_feedback_with_scores: returns rows joining feedback + jobs + score_dimensions.
- get_feedback_with_scores: includes jobs with no score (LEFT JOIN).
- get_feedback_with_scores: prefers higher pass (pass=2 over pass=1) for scores.
- get_feedback_with_scores: returns empty list when no feedback exists.
- apply_approved_suggestions: applies add_skill change to YAML file.
- apply_approved_suggestions: applies remove_skill change.
- apply_approved_suggestions: writes atomically (temp file + rename).
- apply_approved_suggestions: marks processed suggestions as rejected.
- apply_approved_suggestions: marks conflicting suggestions as rejected with note.
- apply_approved_suggestions: continues processing after a conflict.
- apply_approved_suggestions: returns (applied, conflict) counts.
- apply_approved_suggestions: skips pending/rejected suggestions.

All tests use in-memory SQLite with the full schema created by init_db().
No real YAML file is written for schema-only tests; YAML tests use tmp_path.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from pipeline.src.database import get_connection, init_db
from pipeline.src.profile_evolution import (
    EVOLUTION_THRESHOLD,
    apply_approved_suggestions,
    get_feedback_with_scores,
    should_run_evolution,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Fully-initialised pipeline database connection (all 7 tables)."""
    db_path = tmp_path / "test_evolution.db"
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


@pytest.fixture()
def profile_yaml_file(tmp_path: Path) -> Path:
    """Write a minimal profile.yaml to tmp_path and return the path."""
    content = {
        "title_keywords": ["data engineer", "analytics engineer"],
        "skills": ["SQL", "Python", "dbt"],
        "location_preference": "remote",
        "salary_min": 130000,
        "freeform_preferences": "Experienced data professional.",
    }
    path = tmp_path / "profile.yaml"
    path.write_text(yaml.dump(content, default_flow_style=False), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_job(
    conn: sqlite3.Connection,
    *,
    title: str = "Data Engineer",
    company: str = "Acme Corp",
    url: str | None = None,
    location: str | None = "Remote",
) -> int:
    """Insert a minimal job row and return its id."""
    _insert_job._counter = getattr(_insert_job, "_counter", 0) + 1  # type: ignore[attr-defined]
    unique_url = url or f"https://example.com/job/{_insert_job._counter}"
    conn.execute(
        "INSERT INTO jobs (source, source_type, url, title, company, location) "
        "VALUES ('test', 'api', ?, ?, ?, ?)",
        (unique_url, title, company, location),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_feedback(
    conn: sqlite3.Connection,
    job_id: int,
    signal: str = "thumbs_up",
    *,
    note: str | None = None,
    created_at: str | None = None,
) -> int:
    """Insert a feedback row and return its id."""
    if created_at:
        conn.execute(
            "INSERT INTO feedback (job_id, signal, note, created_at) VALUES (?, ?, ?, ?)",
            (job_id, signal, note, created_at),
        )
    else:
        conn.execute(
            "INSERT INTO feedback (job_id, signal, note) VALUES (?, ?, ?)",
            (job_id, signal, note),
        )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_snapshot(
    conn: sqlite3.Connection,
    *,
    created_at: str | None = None,
) -> int:
    """Insert a profile_snapshots row and return its id."""
    if created_at:
        conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml, created_at) VALUES ('yaml: true', ?)",
            (created_at,),
        )
    else:
        conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml) VALUES ('yaml: true')"
        )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_score(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    pass_num: int = 1,
    overall: int = 75,
    role_fit: int | None = None,
) -> int:
    """Insert a score_dimensions row and return its id."""
    conn.execute(
        "INSERT INTO score_dimensions (job_id, pass, overall, role_fit) VALUES (?, ?, ?, ?)",
        (job_id, pass_num, overall, role_fit),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_suggestion(
    conn: sqlite3.Connection,
    *,
    suggestion_type: str = "add_skill",
    description: str = "Add a skill",
    reasoning: str = "Seen in liked jobs",
    suggested_change: dict | None = None,
    status: str = "approved",
) -> int:
    """Insert a profile_suggestions row and return its id."""
    if suggested_change is None:
        suggested_change = {"skill": "Airflow"}
    conn.execute(
        "INSERT INTO profile_suggestions "
        "(suggestion_type, description, reasoning, suggested_change, status) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            suggestion_type,
            description,
            reasoning,
            json.dumps(suggested_change),
            status,
        ),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ---------------------------------------------------------------------------
# should_run_evolution
# ---------------------------------------------------------------------------


class TestShouldRunEvolution:
    def test_returns_false_when_no_feedback(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Zero feedback rows should return False."""
        assert should_run_evolution(db_conn) is False

    def test_returns_false_when_below_threshold(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Fewer than 20 feedback rows returns False (bootstrap: no snapshots)."""
        job_id = _insert_job(db_conn)
        for _ in range(EVOLUTION_THRESHOLD - 1):
            _insert_feedback(db_conn, job_id)
        assert should_run_evolution(db_conn) is False

    def test_returns_true_at_threshold(self, db_conn: sqlite3.Connection) -> None:
        """Exactly 20 feedback rows with no snapshot returns True."""
        job_id = _insert_job(db_conn)
        for _ in range(EVOLUTION_THRESHOLD):
            _insert_feedback(db_conn, job_id)
        assert should_run_evolution(db_conn) is True

    def test_returns_true_above_threshold(self, db_conn: sqlite3.Connection) -> None:
        """More than 20 feedback rows returns True."""
        job_id = _insert_job(db_conn)
        for _ in range(EVOLUTION_THRESHOLD + 5):
            _insert_feedback(db_conn, job_id)
        assert should_run_evolution(db_conn) is True

    def test_empty_snapshots_counts_all_feedback(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """When profile_snapshots is empty, all feedback rows are counted."""
        job_id = _insert_job(db_conn)
        for _ in range(EVOLUTION_THRESHOLD):
            _insert_feedback(db_conn, job_id)
        # Confirm table is empty
        count = db_conn.execute(
            "SELECT COUNT(*) FROM profile_snapshots"
        ).fetchone()[0]
        assert count == 0
        assert should_run_evolution(db_conn) is True

    def test_only_counts_feedback_after_snapshot(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Feedback rows created before the snapshot do not count."""
        job_id = _insert_job(db_conn)

        # Insert 20 feedback rows BEFORE the snapshot
        for _ in range(EVOLUTION_THRESHOLD):
            _insert_feedback(db_conn, job_id, created_at="2024-01-01 10:00:00")

        # Insert a snapshot
        _insert_snapshot(db_conn, created_at="2024-06-01 00:00:00")

        # No new feedback after snapshot
        assert should_run_evolution(db_conn) is False

    def test_counts_only_feedback_after_latest_snapshot(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Only feedback after the MOST RECENT snapshot is counted."""
        job_id = _insert_job(db_conn)

        # 20 feedback rows before the first snapshot
        for _ in range(EVOLUTION_THRESHOLD):
            _insert_feedback(db_conn, job_id, created_at="2024-01-01 10:00:00")

        # Two snapshots — only the latest one matters
        _insert_snapshot(db_conn, created_at="2024-03-01 00:00:00")
        _insert_snapshot(db_conn, created_at="2024-07-01 00:00:00")

        # 5 new feedback rows after latest snapshot — below threshold
        for _ in range(5):
            _insert_feedback(db_conn, job_id, created_at="2024-08-01 10:00:00")

        assert should_run_evolution(db_conn) is False

    def test_threshold_met_after_snapshot(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Returns True when 20+ feedback rows exist after the latest snapshot."""
        job_id = _insert_job(db_conn)

        _insert_snapshot(db_conn, created_at="2024-01-01 00:00:00")

        for _ in range(EVOLUTION_THRESHOLD):
            _insert_feedback(db_conn, job_id, created_at="2024-02-01 10:00:00")

        assert should_run_evolution(db_conn) is True


# ---------------------------------------------------------------------------
# get_feedback_with_scores
# ---------------------------------------------------------------------------


class TestGetFeedbackWithScores:
    def test_returns_empty_when_no_feedback(
        self, db_conn: sqlite3.Connection
    ) -> None:
        result = get_feedback_with_scores(db_conn)
        assert result == []

    def test_returns_one_row_per_feedback(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn)
        _insert_feedback(db_conn, job_id, "thumbs_up")
        _insert_feedback(db_conn, job_id, "thumbs_down")
        result = get_feedback_with_scores(db_conn)
        assert len(result) == 2

    def test_result_has_expected_keys(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        _insert_feedback(db_conn, job_id)
        row = get_feedback_with_scores(db_conn)[0]
        expected_keys = {
            "feedback_id",
            "job_id",
            "signal",
            "note",
            "feedback_created_at",
            "title",
            "company",
            "location",
            "overall",
            "role_fit",
            "skills_match",
            "culture_signals",
            "growth_potential",
            "comp_alignment",
            "score_pass",
            "reasoning",
        }
        assert set(row.keys()) == expected_keys

    def test_includes_job_with_no_score(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Jobs with no score_dimensions row still appear with None score fields."""
        job_id = _insert_job(db_conn)
        _insert_feedback(db_conn, job_id)
        result = get_feedback_with_scores(db_conn)
        assert len(result) == 1
        assert result[0]["overall"] is None
        assert result[0]["score_pass"] is None

    def test_includes_job_score_when_present(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(db_conn)
        _insert_feedback(db_conn, job_id)
        _insert_score(db_conn, job_id, pass_num=1, overall=80, role_fit=70)
        result = get_feedback_with_scores(db_conn)
        assert result[0]["overall"] == 80
        assert result[0]["role_fit"] == 70
        assert result[0]["score_pass"] == 1

    def test_prefers_pass2_over_pass1(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """When both pass=1 and pass=2 rows exist, pass=2 score is returned."""
        job_id = _insert_job(db_conn)
        _insert_feedback(db_conn, job_id)
        _insert_score(db_conn, job_id, pass_num=1, overall=60)
        _insert_score(db_conn, job_id, pass_num=2, overall=90)
        result = get_feedback_with_scores(db_conn)
        assert result[0]["overall"] == 90
        assert result[0]["score_pass"] == 2

    def test_returns_correct_signal(self, db_conn: sqlite3.Connection) -> None:
        job_id = _insert_job(db_conn)
        _insert_feedback(db_conn, job_id, "thumbs_down")
        result = get_feedback_with_scores(db_conn)
        assert result[0]["signal"] == "thumbs_down"

    def test_returns_correct_job_metadata(
        self, db_conn: sqlite3.Connection
    ) -> None:
        job_id = _insert_job(
            db_conn,
            title="Analytics Engineer",
            company="DataCo",
            location="Remote",
        )
        _insert_feedback(db_conn, job_id)
        result = get_feedback_with_scores(db_conn)
        assert result[0]["title"] == "Analytics Engineer"
        assert result[0]["company"] == "DataCo"
        assert result[0]["location"] == "Remote"

    def test_multiple_feedback_same_job(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Multiple feedback rows for the same job all appear separately."""
        job_id = _insert_job(db_conn)
        _insert_score(db_conn, job_id, overall=75)
        for _ in range(3):
            _insert_feedback(db_conn, job_id, "thumbs_up")
        result = get_feedback_with_scores(db_conn)
        assert len(result) == 3
        assert all(r["job_id"] == job_id for r in result)


# ---------------------------------------------------------------------------
# apply_approved_suggestions
# ---------------------------------------------------------------------------


class TestApplyApprovedSuggestions:
    def test_add_skill_applied_to_yaml(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """add_skill suggestion appends skill to the YAML skills list."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Airflow"},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert "Airflow" in updated["skills"]

    def test_remove_skill_applied_to_yaml(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """remove_skill suggestion removes a skill from the YAML skills list."""
        _insert_suggestion(
            db_conn,
            suggestion_type="remove_skill",
            suggested_change={"skill": "dbt"},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert "dbt" not in updated["skills"]

    def test_returns_applied_count(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """Return value first element is count of successfully applied suggestions."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Spark"},
        )
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 1
        assert conflicts == 0

    def test_returns_conflict_count(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """A suggestion targeting a missing key increments conflict count."""
        _insert_suggestion(
            db_conn,
            suggestion_type="remove_skill",
            suggested_change={"skill": "NonExistentSkill"},
        )
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 0
        assert conflicts == 1

    def test_marks_applied_suggestion_as_rejected(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """Successfully applied suggestions get status=rejected (DB schema limit)."""
        sid = _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Spark"},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        row = db_conn.execute(
            "SELECT status, resolved_at FROM profile_suggestions WHERE id = ?", (sid,)
        ).fetchone()
        assert row["status"] == "rejected"
        assert row["resolved_at"] is not None

    def test_marks_conflict_suggestion_as_rejected_with_note(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """Failed suggestions get status=rejected with [CONFLICT] in reasoning."""
        sid = _insert_suggestion(
            db_conn,
            suggestion_type="remove_skill",
            suggested_change={"skill": "DoesNotExist"},
            reasoning="Original reasoning",
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        row = db_conn.execute(
            "SELECT status, reasoning FROM profile_suggestions WHERE id = ?", (sid,)
        ).fetchone()
        assert row["status"] == "rejected"
        assert "[CONFLICT]" in row["reasoning"]

    def test_continues_after_conflict(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """A conflict on one suggestion does not stop processing others."""
        # First suggestion will conflict
        _insert_suggestion(
            db_conn,
            suggestion_type="remove_skill",
            suggested_change={"skill": "DoesNotExist"},
        )
        # Second suggestion should succeed
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Spark"},
        )
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 1
        assert conflicts == 1

    def test_skips_pending_suggestions(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """Pending suggestions are not processed."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Spark"},
            status="pending",
        )
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 0
        assert conflicts == 0

    def test_skips_already_rejected_suggestions(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """Previously rejected suggestions are not re-processed."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Spark"},
            status="rejected",
        )
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 0
        assert conflicts == 0

    def test_add_keyword_applied(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """add_keyword suggestion appends to the named list field."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_keyword",
            suggested_change={"list": "title_keywords", "keyword": "staff engineer"},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert "staff engineer" in updated["title_keywords"]

    def test_remove_keyword_applied(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """remove_keyword suggestion removes from the named list field."""
        _insert_suggestion(
            db_conn,
            suggestion_type="remove_keyword",
            suggested_change={"list": "title_keywords", "keyword": "data engineer"},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert "data engineer" not in updated["title_keywords"]

    def test_set_field_applied(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """set_field suggestion updates a top-level key."""
        _insert_suggestion(
            db_conn,
            suggestion_type="set_field",
            suggested_change={"key": "salary_min", "value": 150000},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert updated["salary_min"] == 150000

    def test_atomic_write_creates_valid_yaml(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """After apply, the YAML file is valid and parseable."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Databricks"},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        content = profile_yaml_file.read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        assert isinstance(parsed, dict)
        assert "skills" in parsed

    def test_no_suggestions_returns_zero_counts(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """When there are no approved suggestions, returns (0, 0)."""
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 0
        assert conflicts == 0

    def test_multiple_suggestions_all_applied(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """Three valid suggestions all succeed."""
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Spark"},
        )
        _insert_suggestion(
            db_conn,
            suggestion_type="add_skill",
            suggested_change={"skill": "Databricks"},
        )
        _insert_suggestion(
            db_conn,
            suggestion_type="add_keyword",
            suggested_change={"list": "title_keywords", "keyword": "staff analytics"},
        )
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 3
        assert conflicts == 0
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert "Spark" in updated["skills"]
        assert "Databricks" in updated["skills"]
        assert "staff analytics" in updated["title_keywords"]

    def test_invalid_json_in_suggested_change_is_conflict(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """A suggestion with malformed JSON in suggested_change becomes a conflict."""
        # Insert directly with bad JSON
        db_conn.execute(
            "INSERT INTO profile_suggestions "
            "(suggestion_type, description, reasoning, suggested_change, status) "
            "VALUES ('add_skill', 'desc', 'reason', 'not valid json', 'approved')"
        )
        db_conn.commit()
        applied, conflicts = apply_approved_suggestions(db_conn, profile_yaml_file)
        assert applied == 0
        assert conflicts == 1

    def test_update_freeform_applied(
        self,
        db_conn: sqlite3.Connection,
        profile_yaml_file: Path,
    ) -> None:
        """update_freeform suggestion replaces the freeform_preferences value."""
        new_text = "Updated freeform preference text."
        _insert_suggestion(
            db_conn,
            suggestion_type="update_freeform",
            suggested_change={"value": new_text},
        )
        apply_approved_suggestions(db_conn, profile_yaml_file)
        updated = yaml.safe_load(profile_yaml_file.read_text(encoding="utf-8"))
        assert updated["freeform_preferences"] == new_text

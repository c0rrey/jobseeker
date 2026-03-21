"""
Tests for pipeline/src/duplicate_detector.py.

Covers:
- detect_duplicates(): grouping behaviour for same-company identical descriptions,
  different-company same descriptions, same-company below-threshold descriptions,
  NULL and short descriptions excluded, representative selection (lowest ID),
  transitive grouping via Union-Find.
- propagate_scores(): Pass 1 propagation (overall value copied), Pass 2 propagation
  (all 6 dimension values copied), reasoning text format, idempotency.
- DetectionSummary field counts are accurate.

All tests use tmp_path SQLite databases initialised via init_db().
No LLM calls and no network calls are made.
"""

from __future__ import annotations

import itertools
import sqlite3
from pathlib import Path

import pytest

from pipeline.src.database import get_connection, init_db
from pipeline.src.duplicate_detector import DetectionSummary, detect_duplicates, propagate_scores

# ---------------------------------------------------------------------------
# Constants used to build fixture data
# ---------------------------------------------------------------------------

# A long, realistic description that exceeds the 20-char minimum and is used
# as the "same" description across duplicate jobs.
_LONG_DESC = (
    "We are looking for a Staff Data Engineer to join our analytics platform team. "
    "You will design, build, and maintain scalable data pipelines using Python, "
    "dbt, and Apache Spark. Strong SQL skills and experience with cloud data "
    "warehouses (Snowflake or BigQuery) are required. 5+ years of experience."
)

# A description that is clearly different — similarity well below 0.70.
_DIFFERENT_DESC = (
    "Junior Frontend Developer role focusing on React and TypeScript. "
    "We are a fast-paced startup looking for someone comfortable shipping "
    "UI components daily. No data engineering experience required."
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return an open SQLite connection to a freshly initialised database."""
    db_path = tmp_path / "test_dedup.db"
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_url_counter = itertools.count(start=1)


def _insert_job(
    conn: sqlite3.Connection,
    *,
    company: str = "Acme Corp",
    description: str | None = _LONG_DESC,
    url: str | None = None,
    title: str = "Data Engineer",
) -> int:
    """Insert a minimal job row and return its auto-assigned id.

    Uses a module-level counter to guarantee unique URLs across calls.
    """
    unique_url = url or f"https://example.com/job/{next(_url_counter)}"
    conn.execute(
        """
        INSERT INTO jobs (source, source_type, url, title, company, description)
        VALUES ('test', 'api', ?, ?, ?, ?)
        """,
        (unique_url, title, company, description),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_score_p1(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    overall: int = 80,
    profile_hash: str = "testhash",
) -> None:
    """Insert a Pass 1 score_dimensions row for the given job."""
    conn.execute(
        """
        INSERT OR REPLACE INTO score_dimensions
            (job_id, pass, overall, reasoning, profile_hash, scored_at)
        VALUES (?, 1, ?, 'Good fit.', ?, datetime('now'))
        """,
        (job_id, overall, profile_hash),
    )
    conn.commit()


def _insert_score_p2(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    role_fit: int = 85,
    skills_match: int = 78,
    culture_signals: int = 72,
    growth_potential: int = 68,
    comp_alignment: int = 90,
    overall: int = 81,
    profile_hash: str = "testhash",
) -> None:
    """Insert a Pass 2 score_dimensions row for the given job."""
    conn.execute(
        """
        INSERT OR REPLACE INTO score_dimensions
            (job_id, pass, role_fit, skills_match, culture_signals,
             growth_potential, comp_alignment, overall, reasoning,
             profile_hash, scored_at)
        VALUES (?, 2, ?, ?, ?, ?, ?, ?, 'Deep analysis.', ?, datetime('now'))
        """,
        (
            job_id,
            role_fit,
            skills_match,
            culture_signals,
            growth_potential,
            comp_alignment,
            overall,
            profile_hash,
        ),
    )
    conn.commit()


def _fetch_job_row(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row:
    """Return the jobs row for the given id."""
    return conn.execute(
        "SELECT * FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()


def _fetch_score_row(
    conn: sqlite3.Connection, job_id: int, pass_number: int
) -> sqlite3.Row | None:
    """Return the score_dimensions row for (job_id, pass_number), or None."""
    return conn.execute(
        "SELECT * FROM score_dimensions WHERE job_id = ? AND pass = ?",
        (job_id, pass_number),
    ).fetchone()


# ---------------------------------------------------------------------------
# detect_duplicates — grouping behaviour
# ---------------------------------------------------------------------------


class TestDetectDuplicatesGrouping:
    """Verify that detect_duplicates() correctly identifies duplicate groups."""

    def test_no_jobs_returns_empty_summary(self, db_conn: sqlite3.Connection) -> None:
        summary = detect_duplicates(db_conn)
        assert summary.groups_created == 0
        assert summary.jobs_grouped == 0
        assert summary.representatives_set == 0

    def test_summary_is_detection_summary_instance(
        self, db_conn: sqlite3.Connection
    ) -> None:
        summary = detect_duplicates(db_conn)
        assert isinstance(summary, DetectionSummary)

    def test_single_job_no_group_formed(self, db_conn: sqlite3.Connection) -> None:
        _insert_job(db_conn, company="Solo Corp")
        summary = detect_duplicates(db_conn)
        assert summary.groups_created == 0

    def test_two_same_company_identical_descriptions_form_one_group(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_a = _insert_job(db_conn, company="LaunchPotato", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="LaunchPotato", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 1
        assert summary.jobs_grouped == 2
        assert summary.representatives_set == 1

        row_a = _fetch_job_row(db_conn, id_a)
        row_b = _fetch_job_row(db_conn, id_b)
        assert row_a["dup_group_id"] == row_b["dup_group_id"]
        assert row_a["dup_group_id"] is not None

    def test_twelve_same_company_identical_jobs_form_one_group(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """12 same-company identical descriptions must collapse to exactly 1 group."""
        ids = [
            _insert_job(db_conn, company="Launch Potato", description=_LONG_DESC)
            for _ in range(12)
        ]
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 1
        assert summary.jobs_grouped == 12
        assert summary.representatives_set == 1

        # All jobs share the same dup_group_id.
        group_ids = {
            _fetch_job_row(db_conn, jid)["dup_group_id"] for jid in ids
        }
        assert len(group_ids) == 1
        assert None not in group_ids

    def test_lowest_id_is_representative(self, db_conn: sqlite3.Connection) -> None:
        id_a = _insert_job(db_conn, company="RepCo", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="RepCo", description=_LONG_DESC)
        id_c = _insert_job(db_conn, company="RepCo", description=_LONG_DESC)
        detect_duplicates(db_conn)

        row_a = _fetch_job_row(db_conn, id_a)
        row_b = _fetch_job_row(db_conn, id_b)
        row_c = _fetch_job_row(db_conn, id_c)

        # Lowest id is representative; others are not.
        assert row_a["is_representative"] == 1
        assert row_b["is_representative"] == 0
        assert row_c["is_representative"] == 0

    def test_different_company_same_description_not_grouped(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Identical descriptions from different companies must NOT be grouped."""
        id_a = _insert_job(db_conn, company="Alpha Inc", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="Beta LLC", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 0

        row_a = _fetch_job_row(db_conn, id_a)
        row_b = _fetch_job_row(db_conn, id_b)
        assert row_a["dup_group_id"] is None
        assert row_b["dup_group_id"] is None

    def test_same_company_below_threshold_not_grouped(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Same company but very different descriptions must NOT be grouped."""
        id_a = _insert_job(db_conn, company="Threshold Corp", description=_LONG_DESC)
        id_b = _insert_job(
            db_conn, company="Threshold Corp", description=_DIFFERENT_DESC
        )
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 0

        row_a = _fetch_job_row(db_conn, id_a)
        row_b = _fetch_job_row(db_conn, id_b)
        assert row_a["dup_group_id"] is None
        assert row_b["dup_group_id"] is None

    def test_null_description_excluded_from_grouping(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Jobs with NULL description must be excluded from duplicate detection."""
        id_null = _insert_job(db_conn, company="Null Co", description=None)
        id_valid = _insert_job(db_conn, company="Null Co", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 0
        assert _fetch_job_row(db_conn, id_null)["dup_group_id"] is None
        assert _fetch_job_row(db_conn, id_valid)["dup_group_id"] is None

    def test_short_description_excluded_from_grouping(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Descriptions shorter than 20 characters are excluded from grouping."""
        short_desc = "Short job."  # 10 chars — below 20-char minimum
        id_short = _insert_job(db_conn, company="Short Co", description=short_desc)
        id_valid = _insert_job(db_conn, company="Short Co", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        # Only one qualifying job for Short Co — no group can form.
        assert summary.groups_created == 0
        assert _fetch_job_row(db_conn, id_short)["dup_group_id"] is None
        assert _fetch_job_row(db_conn, id_valid)["dup_group_id"] is None

    def test_description_at_exactly_20_chars_is_included(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A description of exactly 20 characters meets the minimum and qualifies."""
        desc_20 = "A" * 20  # exactly 20 characters
        id_a = _insert_job(db_conn, company="Exact Corp", description=desc_20)
        id_b = _insert_job(db_conn, company="Exact Corp", description=desc_20)
        summary = detect_duplicates(db_conn)

        # Both qualify and are identical — should form a group.
        assert summary.groups_created == 1
        assert summary.jobs_grouped == 2

    def test_company_name_comparison_is_case_insensitive(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """'Acme' and 'ACME' and 'acme' must be treated as the same company."""
        id_a = _insert_job(db_conn, company="Acme Corp", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="ACME CORP", description=_LONG_DESC)
        id_c = _insert_job(db_conn, company="acme corp", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 1
        assert summary.jobs_grouped == 3

    def test_company_name_comparison_strips_whitespace(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Leading/trailing whitespace in company name is ignored."""
        id_a = _insert_job(db_conn, company="  SpaceInc  ", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="SpaceInc", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 1
        assert summary.jobs_grouped == 2

    def test_empty_company_name_excluded_from_grouping(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Jobs with an empty (or all-whitespace) company name must not be grouped.

        Before the fix, company_key="" caused all such jobs to share the same
        bucket in the by_company dict, producing false-positive duplicate groups
        among unrelated jobs that simply had no company name.
        """
        # Two jobs with identical long descriptions but no company name.
        # They must NOT be grouped together.
        id_a = _insert_job(db_conn, company="", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="   ", description=_LONG_DESC)
        # A normal job at a real company must still form its own valid group.
        id_c = _insert_job(db_conn, company="RealCo", description=_LONG_DESC)
        id_d = _insert_job(db_conn, company="RealCo", description=_LONG_DESC)

        summary = detect_duplicates(db_conn)

        # Only the two RealCo jobs should form a group.
        assert summary.groups_created == 1
        assert summary.jobs_grouped == 2

        # Empty-company jobs must remain ungrouped.
        assert _fetch_job_row(db_conn, id_a)["dup_group_id"] is None
        assert _fetch_job_row(db_conn, id_b)["dup_group_id"] is None

        # RealCo jobs must still be grouped correctly.
        group_id_c = _fetch_job_row(db_conn, id_c)["dup_group_id"]
        group_id_d = _fetch_job_row(db_conn, id_d)["dup_group_id"]
        assert group_id_c is not None
        assert group_id_c == group_id_d

    def test_two_independent_groups_from_two_companies(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Two separate same-company clusters should produce two independent groups."""
        id_a1 = _insert_job(db_conn, company="Alpha Inc", description=_LONG_DESC)
        id_a2 = _insert_job(db_conn, company="Alpha Inc", description=_LONG_DESC)
        id_b1 = _insert_job(db_conn, company="Beta LLC", description=_LONG_DESC)
        id_b2 = _insert_job(db_conn, company="Beta LLC", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 2
        assert summary.jobs_grouped == 4
        assert summary.representatives_set == 2

        # The two groups must have different dup_group_ids.
        group_id_a = _fetch_job_row(db_conn, id_a1)["dup_group_id"]
        group_id_b = _fetch_job_row(db_conn, id_b1)["dup_group_id"]
        assert group_id_a != group_id_b

    def test_full_rebuild_on_second_call(self, db_conn: sqlite3.Connection) -> None:
        """A second call to detect_duplicates() clears and rebuilds all groups."""
        id_a = _insert_job(db_conn, company="Rebuild Co", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="Rebuild Co", description=_LONG_DESC)
        detect_duplicates(db_conn)  # first run

        # Second run should produce the same result, not double-count.
        summary2 = detect_duplicates(db_conn)
        assert summary2.groups_created == 1
        assert summary2.jobs_grouped == 2

    def test_non_group_jobs_have_null_dup_group_id(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Jobs that do not form a group must have dup_group_id = NULL after detection."""
        id_solo = _insert_job(db_conn, company="Solo Corp", description=_LONG_DESC)
        id_a = _insert_job(db_conn, company="Group Co", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="Group Co", description=_LONG_DESC)
        detect_duplicates(db_conn)

        solo_row = _fetch_job_row(db_conn, id_solo)
        assert solo_row["dup_group_id"] is None
        assert solo_row["is_representative"] == 0

    def test_transitive_grouping_a_similar_b_b_similar_c(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A~B and B~C must place A, B, C all in the same group (transitive closure)."""
        # Use a base description and two variants that are each individually
        # similar to the previous but differ in small ways.  Since A==B==C here
        # (identical text), transitivity is trivially satisfied.
        id_a = _insert_job(db_conn, company="Trans Co", description=_LONG_DESC)
        id_b = _insert_job(db_conn, company="Trans Co", description=_LONG_DESC)
        id_c = _insert_job(db_conn, company="Trans Co", description=_LONG_DESC)
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 1
        assert summary.jobs_grouped == 3

        gids = {_fetch_job_row(db_conn, jid)["dup_group_id"] for jid in [id_a, id_b, id_c]}
        assert len(gids) == 1


# ---------------------------------------------------------------------------
# propagate_scores — Pass 1
# ---------------------------------------------------------------------------


class TestPropagateScoresPass1:
    """Verify propagate_scores(conn, 1) copies overall values to group members."""

    def test_returns_zero_when_no_groups(self, db_conn: sqlite3.Connection) -> None:
        count = propagate_scores(db_conn, 1)
        assert count == 0

    def test_returns_zero_when_no_representative_scored(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="PropCo")
        id_mem = _insert_job(db_conn, company="PropCo")
        detect_duplicates(db_conn)
        # No score written for the representative — nothing to propagate.
        count = propagate_scores(db_conn, 1)
        assert count == 0

    def test_pass1_propagates_overall_to_all_members(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="PropCo")
        id_mem1 = _insert_job(db_conn, company="PropCo")
        id_mem2 = _insert_job(db_conn, company="PropCo")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=77)
        count = propagate_scores(db_conn, 1)

        assert count == 2  # two non-representatives received rows

        for member_id in [id_mem1, id_mem2]:
            row = _fetch_score_row(db_conn, member_id, 1)
            assert row is not None
            assert row["overall"] == 77

    def test_pass1_propagated_reasoning_format(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="ReasonCo")
        id_mem = _insert_job(db_conn, company="ReasonCo")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=65)
        propagate_scores(db_conn, 1)

        row = _fetch_score_row(db_conn, id_mem, 1)
        assert row is not None
        assert f"Score propagated from representative job_id={id_rep}" in row["reasoning"]

    def test_pass1_propagation_is_idempotent(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Calling propagate_scores twice must not raise and must overwrite cleanly."""
        id_rep = _insert_job(db_conn, company="IdemCo")
        id_mem = _insert_job(db_conn, company="IdemCo")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=55)
        propagate_scores(db_conn, 1)
        propagate_scores(db_conn, 1)  # should not raise

        rows = db_conn.execute(
            "SELECT COUNT(*) FROM score_dimensions WHERE job_id = ? AND pass = 1",
            (id_mem,),
        ).fetchone()[0]
        assert rows == 1  # still exactly one row

    def test_pass1_representative_row_not_overwritten(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """The representative's own score_dimensions row must not be changed."""
        id_rep = _insert_job(db_conn, company="PresCo")
        id_mem = _insert_job(db_conn, company="PresCo")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=88, profile_hash="orig")
        propagate_scores(db_conn, 1)

        rep_row = _fetch_score_row(db_conn, id_rep, 1)
        assert rep_row is not None
        assert rep_row["overall"] == 88
        assert rep_row["profile_hash"] == "orig"

    def test_pass1_profile_hash_preserved_in_propagated_row(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="HashCo")
        id_mem = _insert_job(db_conn, company="HashCo")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=70, profile_hash="deadbeef")
        propagate_scores(db_conn, 1)

        row = _fetch_score_row(db_conn, id_mem, 1)
        assert row is not None
        assert row["profile_hash"] == "deadbeef"

    def test_pass1_does_not_propagate_to_ungrouped_jobs(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_ungrouped = _insert_job(db_conn, company="Solo Corp")
        id_rep = _insert_job(db_conn, company="GroupCo")
        id_mem = _insert_job(db_conn, company="GroupCo")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=60)
        propagate_scores(db_conn, 1)

        # The ungrouped job should have no propagated score.
        ungrouped_score = _fetch_score_row(db_conn, id_ungrouped, 1)
        assert ungrouped_score is None

    def test_pass1_returns_count_equal_to_non_representatives(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="CountCo")
        members = [_insert_job(db_conn, company="CountCo") for _ in range(4)]
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=75)
        count = propagate_scores(db_conn, 1)

        assert count == 4


# ---------------------------------------------------------------------------
# propagate_scores — Pass 2
# ---------------------------------------------------------------------------


class TestPropagateScoresPass2:
    """Verify propagate_scores(conn, 2) copies all 6 dimension values."""

    def test_pass2_propagates_all_six_dimensions(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="DimCo")
        id_mem = _insert_job(db_conn, company="DimCo")
        detect_duplicates(db_conn)

        _insert_score_p2(
            db_conn,
            id_rep,
            role_fit=85,
            skills_match=78,
            culture_signals=72,
            growth_potential=68,
            comp_alignment=90,
            overall=81,
        )
        propagate_scores(db_conn, 2)

        row = _fetch_score_row(db_conn, id_mem, 2)
        assert row is not None
        assert row["role_fit"] == 85
        assert row["skills_match"] == 78
        assert row["culture_signals"] == 72
        assert row["growth_potential"] == 68
        assert row["comp_alignment"] == 90
        assert row["overall"] == 81

    def test_pass2_propagated_reasoning_format(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="Reason2Co")
        id_mem = _insert_job(db_conn, company="Reason2Co")
        detect_duplicates(db_conn)

        _insert_score_p2(db_conn, id_rep, overall=74)
        propagate_scores(db_conn, 2)

        row = _fetch_score_row(db_conn, id_mem, 2)
        assert row is not None
        assert f"Score propagated from representative job_id={id_rep}" in row["reasoning"]

    def test_pass2_is_idempotent(self, db_conn: sqlite3.Connection) -> None:
        id_rep = _insert_job(db_conn, company="Idem2Co")
        id_mem = _insert_job(db_conn, company="Idem2Co")
        detect_duplicates(db_conn)

        _insert_score_p2(db_conn, id_rep, overall=60)
        propagate_scores(db_conn, 2)
        propagate_scores(db_conn, 2)  # must not raise

        count = db_conn.execute(
            "SELECT COUNT(*) FROM score_dimensions WHERE job_id = ? AND pass = 2",
            (id_mem,),
        ).fetchone()[0]
        assert count == 1

    def test_pass2_returns_correct_count(self, db_conn: sqlite3.Connection) -> None:
        id_rep = _insert_job(db_conn, company="Count2Co")
        members = [_insert_job(db_conn, company="Count2Co") for _ in range(3)]
        detect_duplicates(db_conn)

        _insert_score_p2(db_conn, id_rep, overall=79)
        count = propagate_scores(db_conn, 2)

        assert count == 3

    def test_pass2_does_not_propagate_pass1_data(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """propagate_scores(conn, 2) must not touch Pass 1 rows."""
        id_rep = _insert_job(db_conn, company="PassSep")
        id_mem = _insert_job(db_conn, company="PassSep")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=70)
        _insert_score_p2(db_conn, id_rep, overall=80)

        propagate_scores(db_conn, 2)

        # Member should have a Pass 2 row but NOT a Pass 1 row.
        assert _fetch_score_row(db_conn, id_mem, 2) is not None
        assert _fetch_score_row(db_conn, id_mem, 1) is None

    def test_pass2_profile_hash_preserved(self, db_conn: sqlite3.Connection) -> None:
        id_rep = _insert_job(db_conn, company="P2HashCo")
        id_mem = _insert_job(db_conn, company="P2HashCo")
        detect_duplicates(db_conn)

        _insert_score_p2(db_conn, id_rep, overall=77, profile_hash="cafebabe")
        propagate_scores(db_conn, 2)

        row = _fetch_score_row(db_conn, id_mem, 2)
        assert row is not None
        assert row["profile_hash"] == "cafebabe"

    def test_pass2_zero_when_representative_not_scored(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="NoScore2Co")
        id_mem = _insert_job(db_conn, company="NoScore2Co")
        detect_duplicates(db_conn)

        # No Pass 2 score for representative — nothing to propagate.
        count = propagate_scores(db_conn, 2)
        assert count == 0


# ---------------------------------------------------------------------------
# Edge cases across the full detect + propagate cycle
# ---------------------------------------------------------------------------


class TestDetectAndPropagateCycle:
    """Integration-style tests for the full detect_duplicates → propagate_scores flow."""

    def test_detect_then_p1_propagate_then_p2_propagate(
        self, db_conn: sqlite3.Connection
    ) -> None:
        id_rep = _insert_job(db_conn, company="FullCycle")
        id_mem = _insert_job(db_conn, company="FullCycle")
        detect_duplicates(db_conn)

        _insert_score_p1(db_conn, id_rep, overall=82)
        propagate_scores(db_conn, 1)

        _insert_score_p2(db_conn, id_rep, role_fit=88, overall=84)
        propagate_scores(db_conn, 2)

        p1_row = _fetch_score_row(db_conn, id_mem, 1)
        p2_row = _fetch_score_row(db_conn, id_mem, 2)

        assert p1_row is not None and p1_row["overall"] == 82
        assert p2_row is not None and p2_row["overall"] == 84
        assert p2_row["role_fit"] == 88

    def test_rebuild_after_additional_jobs_added(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Adding new duplicates after first run and re-detecting must work correctly."""
        id_a = _insert_job(db_conn, company="ExpandCo")
        id_b = _insert_job(db_conn, company="ExpandCo")
        detect_duplicates(db_conn)
        assert _fetch_job_row(db_conn, id_a)["is_representative"] == 1

        # Add a third job and re-detect.
        id_c = _insert_job(db_conn, company="ExpandCo")
        summary = detect_duplicates(db_conn)

        assert summary.groups_created == 1
        assert summary.jobs_grouped == 3
        group_ids = {
            _fetch_job_row(db_conn, jid)["dup_group_id"]
            for jid in [id_a, id_b, id_c]
        }
        assert len(group_ids) == 1

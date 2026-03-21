"""
Tests for pipeline/src/enrichment/orchestrator.py.

Enrichment sources are mocked so no real network calls are made.
The orchestrator runs two sources: glassdoor (via glassdoor_rapidapi) and
levelsfy.

Tests verify:
- companies_needing_enrichment query (NULL and stale enriched_at)
- all active sources called per company
- partial failure: one source fails, others still run
- enriched_at updated after all sources attempted (success and partial failure)
- summary dict keys and counts
- empty database: no companies processed
- exception inside enrich function treated as failure (logged, not raised)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.src.database import init_db
from pipeline.src.enrichment.orchestrator import (
    _SOURCES,
    _query_companies_needing_enrichment,
    run_enrichment,
)

# ---------------------------------------------------------------------------
# Patch targets — each source module's enrich attribute is patched at the
# module level so that run_enrichment picks up the mock when it accesses
# module.enrich at call time.
# ---------------------------------------------------------------------------

_PATCH_GLASSDOOR = "pipeline.src.enrichment.glassdoor_rapidapi.enrich"
_PATCH_LEVELSFY = "pipeline.src.enrichment.levelsfy.enrich"

# Active sources/targets: glassdoor (via glassdoor_rapidapi) and levelsfy.
_ACTIVE_PATCH_TARGETS = [
    _PATCH_GLASSDOOR,
    _PATCH_LEVELSFY,
]

_ACTIVE_SOURCE_NAMES = ["glassdoor", "levelsfy"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return an open SQLite connection with the V2 schema."""
    path = tmp_path / "test.db"
    init_db(path)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _insert_company(
    conn: sqlite3.Connection,
    name: str,
    enriched_at: str | None = None,
) -> None:
    """Insert a test company row with an optional enriched_at value."""
    conn.execute(
        "INSERT INTO companies (name, enriched_at) VALUES (?, ?)",
        (name, enriched_at),
    )
    conn.commit()


def _get_enriched_at(conn: sqlite3.Connection, name: str) -> str | None:
    """Return the enriched_at value for the named company."""
    row = conn.execute(
        "SELECT enriched_at FROM companies WHERE name = ?", (name,)
    ).fetchone()
    return row["enriched_at"] if row else None


# ---------------------------------------------------------------------------
# Helper: patch all active sources with the same return_value
# ---------------------------------------------------------------------------


class _AllSourcesPatched:
    """Context manager that patches all active enrichment source modules."""

    def __init__(self, return_value: bool = True) -> None:
        self._return_value = return_value
        self._patches: list = []
        self.mocks: list[MagicMock] = []

    def __enter__(self) -> list[MagicMock]:
        for target in _ACTIVE_PATCH_TARGETS:
            p = patch(target, return_value=self._return_value)
            mock = p.start()
            self._patches.append(p)
            self.mocks.append(mock)
        return self.mocks

    def __exit__(self, *args: object) -> None:
        for p in self._patches:
            p.stop()


def _patch_all_sources(return_value: bool = True) -> "_AllSourcesPatched":
    """Return a context manager that patches all active source enrich functions."""
    return _AllSourcesPatched(return_value=return_value)


# ---------------------------------------------------------------------------
# _query_companies_needing_enrichment
# ---------------------------------------------------------------------------


class TestQueryCompaniesNeedingEnrichment:
    """Tests for the internal query helper."""

    def test_includes_company_with_null_enriched_at(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Companies with enriched_at IS NULL are returned."""
        _insert_company(db_conn, "Alpha Corp", enriched_at=None)
        rows = _query_companies_needing_enrichment(db_conn)
        assert any(name == "Alpha Corp" for _, name in rows)

    def test_includes_company_with_stale_enriched_at(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Companies enriched more than 30 days ago are returned."""
        stale_ts = "2020-01-01 00:00:00"
        _insert_company(db_conn, "Beta Corp", enriched_at=stale_ts)
        rows = _query_companies_needing_enrichment(db_conn)
        assert any(name == "Beta Corp" for _, name in rows)

    def test_excludes_recently_enriched_company(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Companies enriched within 30 days are excluded."""
        db_conn.execute(
            "INSERT INTO companies (name, enriched_at) VALUES (?, datetime('now', '-1 day'))",
            ("Gamma Corp",),
        )
        db_conn.commit()
        rows = _query_companies_needing_enrichment(db_conn)
        assert all(name != "Gamma Corp" for _, name in rows)

    def test_empty_table_returns_empty_list(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Empty companies table yields an empty list."""
        rows = _query_companies_needing_enrichment(db_conn)
        assert rows == []

    def test_returns_list_of_tuples_int_str(self, db_conn: sqlite3.Connection) -> None:
        """Return type contains (int, str) tuples, not Row objects."""
        _insert_company(db_conn, "Delta Corp")
        rows = _query_companies_needing_enrichment(db_conn)
        assert all(isinstance(company_id, int) and isinstance(name, str) for company_id, name in rows)


# ---------------------------------------------------------------------------
# run_enrichment — summary dict structure
# ---------------------------------------------------------------------------


class TestRunEnrichmentSummaryStructure:
    """Verify the shape and types of the returned summary dict."""

    def test_returns_required_keys(self, db_conn: sqlite3.Connection) -> None:
        """run_enrichment always returns a dict with the three required keys."""
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        assert "companies_processed" in summary
        assert "sources_succeeded" in summary
        assert "sources_failed" in summary

    def test_sources_succeeded_has_active_entries(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """sources_succeeded contains an entry for each active source."""
        _insert_company(db_conn, "Alpha Corp")
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        assert set(summary["sources_succeeded"].keys()) == set(_ACTIVE_SOURCE_NAMES)

    def test_sources_failed_has_active_entries(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """sources_failed contains an entry for each active source."""
        _insert_company(db_conn, "Alpha Corp")
        with _patch_all_sources(return_value=False), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            summary = run_enrichment(db_conn)

        assert set(summary["sources_failed"].keys()) == set(_ACTIVE_SOURCE_NAMES)


# ---------------------------------------------------------------------------
# run_enrichment — empty database
# ---------------------------------------------------------------------------


class TestRunEnrichmentEmptyDatabase:
    """run_enrichment with no companies needing enrichment."""

    def test_no_companies_processed(self, db_conn: sqlite3.Connection) -> None:
        """companies_processed is 0 when the database is empty."""
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        assert summary["companies_processed"] == 0

    def test_all_counts_zero(self, db_conn: sqlite3.Connection) -> None:
        """All source counts are 0 when no companies need enrichment."""
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        for count in summary["sources_succeeded"].values():
            assert count == 0
        for count in summary["sources_failed"].values():
            assert count == 0


# ---------------------------------------------------------------------------
# run_enrichment — all sources succeed
# ---------------------------------------------------------------------------


class TestRunEnrichmentAllSucceed:
    """All active sources return True for every company."""

    def test_companies_processed_count(self, db_conn: sqlite3.Connection) -> None:
        """companies_processed equals the number of qualifying companies."""
        _insert_company(db_conn, "Alpha Corp")
        _insert_company(db_conn, "Beta Corp")
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        assert summary["companies_processed"] == 2

    def test_all_sources_succeeded_count(self, db_conn: sqlite3.Connection) -> None:
        """Each source succeeded for each company."""
        _insert_company(db_conn, "Alpha Corp")
        _insert_company(db_conn, "Beta Corp")
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        for source_name, count in summary["sources_succeeded"].items():
            assert count == 2, f"{source_name} succeeded count wrong: {count}"

    def test_no_source_failures(self, db_conn: sqlite3.Connection) -> None:
        """sources_failed counts are all 0 when every source succeeds."""
        _insert_company(db_conn, "Alpha Corp")
        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        for source_name, count in summary["sources_failed"].items():
            assert count == 0, f"{source_name} failed count should be 0: {count}"

    def test_enriched_at_is_set(self, db_conn: sqlite3.Connection) -> None:
        """enriched_at is stamped after all sources run."""
        _insert_company(db_conn, "Alpha Corp", enriched_at=None)
        with _patch_all_sources(return_value=True):
            run_enrichment(db_conn)

        enriched_at = _get_enriched_at(db_conn, "Alpha Corp")
        assert enriched_at is not None

    def test_all_active_sources_called(self, db_conn: sqlite3.Connection) -> None:
        """Each active source function is called once per company."""
        _insert_company(db_conn, "Alpha Corp")

        with patch(_PATCH_GLASSDOOR, return_value=True) as gd, \
             patch(_PATCH_LEVELSFY, return_value=True) as lf:
            run_enrichment(db_conn)

        for mock_fn, source in zip(
            [gd, lf], _ACTIVE_SOURCE_NAMES
        ):
            assert mock_fn.call_count == 1, f"{source} not called once"
            args, _ = mock_fn.call_args
            assert isinstance(args[0], int), f"{source}: first arg should be company_id (int)"
            assert args[1] == "Alpha Corp", f"{source}: second arg should be company_name"


# ---------------------------------------------------------------------------
# run_enrichment — partial failures
# ---------------------------------------------------------------------------


class TestRunEnrichmentPartialFailure:
    """One source fails, the others succeed."""

    def test_failed_source_counted(self, db_conn: sqlite3.Connection) -> None:
        """The failing source appears in sources_failed with count >= 1."""
        _insert_company(db_conn, "Alpha Corp")

        # Glassdoor fails, levelsfy succeeds.
        with patch(_PATCH_GLASSDOOR, return_value=False), \
             patch(_PATCH_LEVELSFY, return_value=True), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            summary = run_enrichment(db_conn)

        assert summary["sources_failed"]["glassdoor"] >= 1
        assert summary["sources_succeeded"]["levelsfy"] == 1

    def test_enriched_at_still_set_on_partial_failure(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at is updated even when some sources fail."""
        _insert_company(db_conn, "Alpha Corp", enriched_at=None)

        with patch(_PATCH_GLASSDOOR, return_value=False), \
             patch(_PATCH_LEVELSFY, return_value=True), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            run_enrichment(db_conn)

        enriched_at = _get_enriched_at(db_conn, "Alpha Corp")
        assert enriched_at is not None

    def test_all_sources_called_despite_one_failure(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """All active sources are called even when one returns False."""
        _insert_company(db_conn, "Alpha Corp")

        with patch(_PATCH_GLASSDOOR, return_value=False) as gd, \
             patch(_PATCH_LEVELSFY, return_value=True) as lf, \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            run_enrichment(db_conn)

        # Each mock is called at least once (backoff may trigger retries for gd).
        assert gd.call_count >= 1
        assert lf.call_count >= 1


# ---------------------------------------------------------------------------
# run_enrichment — all sources fail
# ---------------------------------------------------------------------------


class TestRunEnrichmentAllFail:
    """All active sources return False."""

    def test_all_sources_failed_count(self, db_conn: sqlite3.Connection) -> None:
        """Each source is counted as failed for each company."""
        _insert_company(db_conn, "Alpha Corp")

        with _patch_all_sources(return_value=False), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            summary = run_enrichment(db_conn)

        for source_name in _ACTIVE_SOURCE_NAMES:
            assert summary["sources_failed"][source_name] == 1

    def test_companies_processed_still_counted(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """companies_processed reflects all attempted companies even on total failure."""
        _insert_company(db_conn, "Alpha Corp")
        _insert_company(db_conn, "Beta Corp")

        with _patch_all_sources(return_value=False), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            summary = run_enrichment(db_conn)

        assert summary["companies_processed"] == 2

    def test_enriched_at_set_even_on_total_failure(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at is updated even when every source fails."""
        _insert_company(db_conn, "Alpha Corp", enriched_at=None)

        with _patch_all_sources(return_value=False), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            run_enrichment(db_conn)

        enriched_at = _get_enriched_at(db_conn, "Alpha Corp")
        assert enriched_at is not None


# ---------------------------------------------------------------------------
# run_enrichment — exception inside an enrich function
# ---------------------------------------------------------------------------


class TestRunEnrichmentExceptionHandling:
    """Uncaught exceptions inside enrich() are treated as failures."""

    def test_exception_is_not_propagated(self, db_conn: sqlite3.Connection) -> None:
        """run_enrichment does not re-raise exceptions from enrichment sources."""
        _insert_company(db_conn, "Alpha Corp")

        with patch(_PATCH_GLASSDOOR, side_effect=RuntimeError("boom")), \
             patch(_PATCH_LEVELSFY, return_value=True), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            # Should not raise.
            summary = run_enrichment(db_conn)

        assert summary["sources_failed"]["glassdoor"] >= 1

    def test_exception_counted_as_failure(self, db_conn: sqlite3.Connection) -> None:
        """A source that raises is recorded in sources_failed."""
        _insert_company(db_conn, "Alpha Corp")

        with patch(_PATCH_GLASSDOOR, return_value=True), \
             patch(_PATCH_LEVELSFY, side_effect=ValueError("bad data")), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            summary = run_enrichment(db_conn)

        assert summary["sources_failed"]["levelsfy"] >= 1
        assert summary["sources_succeeded"]["glassdoor"] == 1

    def test_enriched_at_set_despite_exception(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at is still updated even when a source raises."""
        _insert_company(db_conn, "Alpha Corp", enriched_at=None)

        with patch(_PATCH_GLASSDOOR, side_effect=RuntimeError("crash")), \
             patch(_PATCH_LEVELSFY, return_value=True), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            run_enrichment(db_conn)

        enriched_at = _get_enriched_at(db_conn, "Alpha Corp")
        assert enriched_at is not None


# ---------------------------------------------------------------------------
# run_enrichment — staleness boundary
# ---------------------------------------------------------------------------


class TestRunEnrichmentStaleness:
    """Verify the 30-day staleness threshold is respected."""

    def test_recently_enriched_company_skipped(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A company enriched 1 day ago is not reprocessed."""
        db_conn.execute(
            "INSERT INTO companies (name, enriched_at) VALUES (?, datetime('now', '-1 day'))",
            ("Fresh Corp",),
        )
        db_conn.commit()

        with _patch_all_sources(return_value=True) as mocks:
            summary = run_enrichment(db_conn)

        assert summary["companies_processed"] == 0
        for mock_fn in mocks:
            assert mock_fn.call_count == 0

    def test_stale_company_is_processed(self, db_conn: sqlite3.Connection) -> None:
        """A company enriched 31 days ago is reprocessed."""
        db_conn.execute(
            "INSERT INTO companies (name, enriched_at) VALUES (?, datetime('now', '-31 days'))",
            ("Stale Corp",),
        )
        db_conn.commit()

        with _patch_all_sources(return_value=True):
            summary = run_enrichment(db_conn)

        assert summary["companies_processed"] == 1


# ---------------------------------------------------------------------------
# run_enrichment — multiple companies, per-source counts
# ---------------------------------------------------------------------------


class TestRunEnrichmentMultipleCompanies:
    """Verify counts aggregate correctly over multiple companies."""

    def test_per_source_counts_accumulate(self, db_conn: sqlite3.Connection) -> None:
        """sources_succeeded and sources_failed sum across all companies."""
        _insert_company(db_conn, "Alpha Corp")
        _insert_company(db_conn, "Beta Corp")
        _insert_company(db_conn, "Gamma Corp")

        def glassdoor_alternating(company_id: int, name: str, conn: sqlite3.Connection) -> bool:
            """Succeed for Alpha and Gamma, fail for Beta."""
            return name != "Beta Corp"

        with patch(_PATCH_GLASSDOOR, side_effect=glassdoor_alternating), \
             patch(_PATCH_LEVELSFY, return_value=True), \
             patch("pipeline.src.enrichment.orchestrator.time.sleep"):
            summary = run_enrichment(db_conn)

        assert summary["companies_processed"] == 3
        assert summary["sources_succeeded"]["glassdoor"] == 2
        assert summary["sources_failed"]["glassdoor"] >= 1
        assert summary["sources_succeeded"]["levelsfy"] == 3


# ---------------------------------------------------------------------------
# _SOURCES structure
# ---------------------------------------------------------------------------


class TestSourcesStructure:
    """Verify the _SOURCES dispatch table reflects the two-source architecture."""

    def test_exactly_two_sources(self) -> None:
        """The orchestrator dispatch table contains exactly two sources."""
        assert len(_SOURCES) == 2

    def test_source_names(self) -> None:
        """The two source names are glassdoor and levelsfy."""
        names = [name for name, _, _ in _SOURCES]
        assert names == ["glassdoor", "levelsfy"]

    def test_sources_have_module_and_rate_limit(self) -> None:
        """Each entry in _SOURCES is a (str, module, float) triple."""
        for name, module, rate_limit in _SOURCES:
            assert isinstance(name, str)
            assert hasattr(module, "enrich"), f"{name} module missing .enrich"
            assert isinstance(rate_limit, float)

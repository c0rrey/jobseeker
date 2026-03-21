"""
Tests for pipeline/src/enrichment/glassdoor_rapidapi.py.

All HTTP calls are mocked via unittest.mock.patch; no real network requests
are made.  The glassdoor_rapidapi module-level ``_RESPONSE_CACHE`` is reset
between tests that exercise the cache path.

Tests verify:
- successful enrichment writes glassdoor_rating and crunchbase_data['glassdoor']
- zero-rating / zero-review response returns False (no DB write)
- API HTTP error returns False
- missing RAPIDAPI_KEY env var returns False
- budget exhaustion at 90 calls skips API and returns False
- budget counter increments on each live API call
- budget counter resets when the calendar month changes
- companies with non-stale enriched_at do not trigger API calls
- cache hit bypasses the API and does not count against the budget
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.src.database import init_db
import pipeline.src.enrichment.glassdoor_rapidapi as gd_mod
from pipeline.src.enrichment.glassdoor_rapidapi import (
    _MONTHLY_BUDGET,
    _check_and_increment_budget,
    _ensure_usage_table,
    _is_fresh,
    enrich,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return an open SQLite connection with the full V2 schema."""
    path = tmp_path / "test.db"
    init_db(path)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def reset_response_cache() -> None:
    """Reset the module-level response cache before every test.

    This prevents a cache loaded by one test from polluting the next.
    """
    gd_mod._RESPONSE_CACHE = None
    yield
    gd_mod._RESPONSE_CACHE = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_company(
    conn: sqlite3.Connection,
    name: str,
    enriched_at: str | None = None,
) -> int:
    """Insert a test company row and return its auto-assigned id."""
    cur = conn.execute(
        "INSERT INTO companies (name, enriched_at) VALUES (?, ?)",
        (name, enriched_at),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def _get_company_row(conn: sqlite3.Connection, company_id: int) -> sqlite3.Row:
    """Fetch the companies row for the given id."""
    row = conn.execute(
        "SELECT * FROM companies WHERE id = ?", (company_id,)
    ).fetchone()
    assert row is not None, f"company id={company_id} not found"
    return row


def _get_usage_row(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """Return the single glassdoor_api_usage row, or None if absent."""
    return conn.execute(
        "SELECT month, count FROM glassdoor_api_usage WHERE id = 1"
    ).fetchone()


def _seed_budget(
    conn: sqlite3.Connection, month: str, count: int
) -> None:
    """Insert or replace the glassdoor_api_usage row with given values."""
    _ensure_usage_table(conn)
    conn.execute(
        """
        INSERT INTO glassdoor_api_usage (id, month, count)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET month = excluded.month, count = excluded.count
        """,
        (month, count),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Minimal valid API response payload
# ---------------------------------------------------------------------------

_GOOD_DATA: dict = {
    "rating": 4.2,
    "culture_and_values_rating": 4.0,
    "work_life_balance_rating": 3.8,
    "compensation_and_benefits_rating": 4.5,
    "career_opportunities_rating": 4.1,
    "senior_management_rating": 3.9,
    "diversity_and_inclusion_rating": 4.3,
    "ceo_rating": 85.0,
    "revenue": "$1B to $5B",
    "company_type": "Public",
    "company_description": "A good company.",
    "review_count": 1000,
    "year_founded": 2005,
    "recommend_to_friend_rating": 70.0,
    "business_outlook_rating": 65.0,
    "company_link": "https://www.glassdoor.com/overview/acme",
}

_GOOD_PAYLOAD: dict = {"data": _GOOD_DATA}


# ---------------------------------------------------------------------------
# _is_fresh
# ---------------------------------------------------------------------------


class TestIsFresh:
    """Tests for the _is_fresh staleness-check helper."""

    def test_returns_false_when_enriched_at_is_null(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at IS NULL -> not fresh -> returns False."""
        company_id = _insert_company(db_conn, "Acme", enriched_at=None)
        assert _is_fresh(db_conn, company_id) is False

    def test_returns_false_when_company_missing(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Non-existent company id -> returns False."""
        assert _is_fresh(db_conn, 99999) is False

    def test_returns_true_when_enriched_at_is_recent(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at within 30 days -> fresh -> returns True."""
        recent = datetime.now(timezone.utc).isoformat()
        company_id = _insert_company(db_conn, "FreshCo", enriched_at=recent)
        assert _is_fresh(db_conn, company_id) is True

    def test_returns_false_when_enriched_at_is_old(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at more than 30 days ago -> stale -> returns False."""
        old_ts = "2020-01-01T00:00:00+00:00"
        company_id = _insert_company(db_conn, "StaleCo", enriched_at=old_ts)
        assert _is_fresh(db_conn, company_id) is False


# ---------------------------------------------------------------------------
# Budget tracker: _check_and_increment_budget
# ---------------------------------------------------------------------------


class TestBudgetTracker:
    """Tests for the monthly API budget counter."""

    def test_first_call_returns_true_and_seeds_row(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """First ever call creates the usage row with count = 1."""
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        result = _check_and_increment_budget(db_conn, "Acme")

        assert result is True
        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["month"] == current_month
        assert row["count"] == 1

    def test_counter_increments_on_each_call(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Budget counter increases by 1 per allowed call."""
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, 5)

        _check_and_increment_budget(db_conn, "Acme")

        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["count"] == 6

    def test_budget_exhausted_returns_false(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Returns False and does not increment when count >= 90."""
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, _MONTHLY_BUDGET)

        result = _check_and_increment_budget(db_conn, "Acme")

        assert result is False
        row = _get_usage_row(db_conn)
        assert row is not None
        # Count must not have changed.
        assert row["count"] == _MONTHLY_BUDGET

    def test_budget_exhaustion_at_exactly_90(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """The 90th stored count triggers exhaustion on the next call."""
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        # Seed at 90 — already exhausted.
        _seed_budget(db_conn, current_month, 90)
        result = _check_and_increment_budget(db_conn, "Acme")
        assert result is False

    def test_month_change_resets_counter(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Stored month mismatch resets count to 1 and returns True."""
        _seed_budget(db_conn, "2023-01", _MONTHLY_BUDGET)  # old month, exhausted

        result = _check_and_increment_budget(db_conn, "Acme")

        assert result is True
        row = _get_usage_row(db_conn)
        assert row is not None
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        assert row["month"] == current_month
        assert row["count"] == 1

    def test_counter_at_one_below_budget_still_allowed(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Count at 89 (one below budget) is still allowed."""
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, _MONTHLY_BUDGET - 1)

        result = _check_and_increment_budget(db_conn, "Acme")

        assert result is True
        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["count"] == _MONTHLY_BUDGET


# ---------------------------------------------------------------------------
# enrich(): missing API key
# ---------------------------------------------------------------------------


class TestEnrichMissingApiKey:
    """enrich() returns False immediately when RAPIDAPI_KEY is absent."""

    def test_returns_false_without_api_key(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Missing RAPIDAPI_KEY causes early return False."""
        company_id = _insert_company(db_conn, "Acme")
        with patch.dict("os.environ", {}, clear=True):
            # Ensure RAPIDAPI_KEY is not set.
            import os
            os.environ.pop("RAPIDAPI_KEY", None)
            result = enrich(company_id, "Acme", db_conn)
        assert result is False

    def test_no_api_call_made_when_key_missing(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """No HTTP request is made when RAPIDAPI_KEY is absent."""
        company_id = _insert_company(db_conn, "Acme")
        with patch("requests.get") as mock_get, \
             patch.dict("os.environ", {}, clear=True):
            import os
            os.environ.pop("RAPIDAPI_KEY", None)
            enrich(company_id, "Acme", db_conn)
        mock_get.assert_not_called()

    def test_no_db_write_when_key_missing(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """glassdoor_rating remains NULL when key is absent."""
        company_id = _insert_company(db_conn, "Acme")
        with patch.dict("os.environ", {}, clear=True):
            import os
            os.environ.pop("RAPIDAPI_KEY", None)
            enrich(company_id, "Acme", db_conn)
        row = _get_company_row(db_conn, company_id)
        assert row["glassdoor_rating"] is None


# ---------------------------------------------------------------------------
# enrich(): fresh company skipped
# ---------------------------------------------------------------------------


class TestEnrichFreshCompanySkipped:
    """Companies with non-stale enriched_at do not trigger API calls."""

    def test_returns_false_for_fresh_company(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enrich() returns False without calling the API for fresh records."""
        recent = datetime.now(timezone.utc).isoformat()
        company_id = _insert_company(db_conn, "FreshCo", enriched_at=recent)

        with patch("requests.get") as mock_get, \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "FreshCo", db_conn)

        assert result is False
        mock_get.assert_not_called()

    def test_no_budget_consumed_for_fresh_company(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Budget counter is not touched when the company is fresh."""
        recent = datetime.now(timezone.utc).isoformat()
        company_id = _insert_company(db_conn, "FreshCo", enriched_at=recent)
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, 10)

        with patch("requests.get"), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "FreshCo", db_conn)

        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["count"] == 10  # unchanged


# ---------------------------------------------------------------------------
# enrich(): successful enrichment
# ---------------------------------------------------------------------------


class TestEnrichSuccess:
    """Successful API response writes to the database."""

    def test_returns_true_on_success(self, db_conn: sqlite3.Connection) -> None:
        """enrich() returns True when API returns valid data with a rating."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        assert result is True

    def test_glassdoor_rating_written_to_db(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """glassdoor_rating column is updated with the overall rating."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        assert row["glassdoor_rating"] == pytest.approx(4.2)

    def test_crunchbase_data_has_glassdoor_key(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """crunchbase_data JSON blob contains a 'glassdoor' key."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        blob = json.loads(row["crunchbase_data"])
        assert "glassdoor" in blob

    def test_glassdoor_blob_contains_sub_ratings(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """The glassdoor sub-dict includes expected sub-rating fields."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        glassdoor_sub = json.loads(row["crunchbase_data"])["glassdoor"]
        assert "culture_and_values_rating" in glassdoor_sub
        assert "work_life_balance_rating" in glassdoor_sub
        assert "compensation_and_benefits_rating" in glassdoor_sub

    def test_enriched_at_is_updated_on_success(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enriched_at is written when enrichment succeeds."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        assert row["enriched_at"] is not None

    def test_existing_crunchbase_keys_preserved(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Glassdoor enrichment merges into existing crunchbase_data, not replaces."""
        existing_blob = json.dumps({"levelsfy": {"p50_total_comp": 200000}})
        company_id = _insert_company(db_conn, "Acme")
        db_conn.execute(
            "UPDATE companies SET crunchbase_data = ? WHERE id = ?",
            (existing_blob, company_id),
        )
        db_conn.commit()

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        blob = json.loads(row["crunchbase_data"])
        assert "levelsfy" in blob, "existing levelsfy key should be preserved"
        assert "glassdoor" in blob

    def test_budget_incremented_on_live_api_call(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Each successful live API call increments the budget counter."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["count"] == 1


# ---------------------------------------------------------------------------
# enrich(): zero-rating / zero-review response
# ---------------------------------------------------------------------------


class TestEnrichZeroRating:
    """Zero-rating AND zero-review response returns False (no DB write)."""

    def test_returns_false_for_zero_rating_zero_reviews(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Zero rating + zero reviews -> returns False."""
        company_id = _insert_company(db_conn, "Acme")

        zero_payload = {"data": {"rating": 0, "review_count": 0}}

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = zero_payload

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        assert result is False

    def test_no_db_write_for_zero_rating(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """glassdoor_rating remains NULL when zero-rating response received."""
        company_id = _insert_company(db_conn, "Acme")

        zero_payload = {"data": {"rating": 0, "review_count": 0}}

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = zero_payload

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        assert row["glassdoor_rating"] is None

    def test_none_rating_with_nonzero_reviews_succeeds(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """None rating but non-zero reviews does not trigger the zero-data guard."""
        company_id = _insert_company(db_conn, "Acme")

        payload = {"data": {"rating": None, "review_count": 500}}

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = payload

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        # review_count > 0, so not treated as zero data
        assert result is True


# ---------------------------------------------------------------------------
# enrich(): API HTTP error
# ---------------------------------------------------------------------------


class TestEnrichApiHttpError:
    """HTTP errors from the RapidAPI endpoint return False."""

    def test_returns_false_on_http_error(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """An HTTPError from requests.get.raise_for_status() returns False."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        http_error = requests.exceptions.HTTPError(response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 429

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        assert result is False

    def test_no_db_write_on_http_error(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """glassdoor_rating remains NULL after an HTTP error."""
        company_id = _insert_company(db_conn, "Acme")

        mock_resp = MagicMock()
        http_error = requests.exceptions.HTTPError(response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 500

        with patch("requests.get", return_value=mock_resp), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_company_row(db_conn, company_id)
        assert row["glassdoor_rating"] is None

    def test_returns_false_on_connection_error(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A ConnectionError from requests.get also returns False."""
        company_id = _insert_company(db_conn, "Acme")

        with patch(
            "requests.get",
            side_effect=requests.exceptions.ConnectionError("timeout"),
        ), patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        assert result is False


# ---------------------------------------------------------------------------
# enrich(): budget exhaustion
# ---------------------------------------------------------------------------


class TestEnrichBudgetExhaustion:
    """When the monthly budget is exhausted, enrich() skips the API."""

    def test_returns_false_when_budget_exhausted(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """enrich() returns False when the 90-call limit has been reached."""
        company_id = _insert_company(db_conn, "Acme")
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, _MONTHLY_BUDGET)

        with patch("requests.get") as mock_get, \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        assert result is False
        mock_get.assert_not_called()

    def test_no_api_call_when_budget_exhausted(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """No HTTP request is made when the budget is exhausted."""
        company_id = _insert_company(db_conn, "Acme")
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, _MONTHLY_BUDGET)

        with patch("requests.get") as mock_get, \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        mock_get.assert_not_called()

    def test_budget_counter_not_incremented_when_exhausted(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """The counter stays at 90 after a rejected call."""
        company_id = _insert_company(db_conn, "Acme")
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, _MONTHLY_BUDGET)

        with patch("requests.get"), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["count"] == _MONTHLY_BUDGET


# ---------------------------------------------------------------------------
# enrich(): cache hit — bypasses API and budget
# ---------------------------------------------------------------------------


class TestEnrichCacheHit:
    """Pre-fetched cache hits skip the live API call."""

    def test_cache_hit_does_not_call_api(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """A cache entry with status='OK' skips requests.get entirely."""
        company_id = _insert_company(db_conn, "Acme")

        cached = {"Acme": {"status": "OK", "data": _GOOD_DATA}}
        gd_mod._RESPONSE_CACHE = cached

        with patch("requests.get") as mock_get, \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        mock_get.assert_not_called()
        assert result is True

    def test_cache_hit_does_not_consume_budget(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Cache hits do not increment the budget counter."""
        company_id = _insert_company(db_conn, "Acme")
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        _seed_budget(db_conn, current_month, 5)

        gd_mod._RESPONSE_CACHE = {"Acme": {"status": "OK", "data": _GOOD_DATA}}

        with patch("requests.get"), \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            enrich(company_id, "Acme", db_conn)

        row = _get_usage_row(db_conn)
        assert row is not None
        assert row["count"] == 5  # unchanged

    def test_cache_miss_falls_through_to_api(
        self, db_conn: sqlite3.Connection
    ) -> None:
        """Empty cache results in a live API call."""
        company_id = _insert_company(db_conn, "Acme")
        gd_mod._RESPONSE_CACHE = {}  # explicit empty cache

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = _GOOD_PAYLOAD

        with patch("requests.get", return_value=mock_resp) as mock_get, \
             patch.dict("os.environ", {"RAPIDAPI_KEY": "test-key"}):
            result = enrich(company_id, "Acme", db_conn)

        mock_get.assert_called_once()
        assert result is True

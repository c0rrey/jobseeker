"""
Glassdoor RapidAPI enrichment module.

Queries the RapidAPI 'Real-Time Glassdoor Data' endpoint
(real-time-glassdoor-data.p.rapidapi.com) by company name and writes the
overall Glassdoor rating plus seven sub-ratings and company metadata to
``companies.glassdoor_rating`` and ``companies.crunchbase_data`` (merged
under the ``'glassdoor'`` key).

Authentication uses the ``RAPIDAPI_KEY`` environment variable.  When the
variable is absent the module logs a warning and returns False immediately.

A lightweight monthly budget tracker persists request counts in the
``glassdoor_api_usage`` table of the pipeline database.  Calls are capped
at 90 per calendar month; the counter resets automatically when the
calendar month changes.

Companies whose ``enriched_at`` timestamp is less than 30 days old are
skipped to avoid redundant API calls.

A pre-fetched cache file (``data/glassdoor_cache.json``) is consulted
before making any live API call; hits never count against the budget.

Returns True on success, False on any failure.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://real-time-glassdoor-data.p.rapidapi.com/company-overview"
_TIMEOUT = 10  # seconds, per project convention
_MONTHLY_BUDGET = 9000
_STALE_DAYS = 30

# Resolve the cache file relative to this module's location:
# pipeline/src/enrichment/ -> pipeline/ -> project root -> data/
_CACHE_PATH: Path = (
    Path(__file__).resolve().parent.parent.parent.parent / "data" / "glassdoor_cache.json"
)

# Module-level cache loaded once per process lifetime.
_RESPONSE_CACHE: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _load_cache() -> dict[str, Any]:
    """Load the pre-fetched Glassdoor response cache from disk.

    Returns:
        Dict mapping company name to cached API response dict.  Returns an
        empty dict if the file is missing or malformed.
    """
    global _RESPONSE_CACHE  # noqa: PLW0603
    if _RESPONSE_CACHE is not None:
        return _RESPONSE_CACHE

    if not _CACHE_PATH.exists():
        logger.debug(
            "Glassdoor cache file not found at '%s'; all requests will hit the API.",
            _CACHE_PATH,
        )
        _RESPONSE_CACHE = {}
        return _RESPONSE_CACHE

    try:
        with _CACHE_PATH.open(encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, dict):
            logger.warning(
                "Glassdoor cache at '%s' is not a JSON object; ignoring.", _CACHE_PATH
            )
            _RESPONSE_CACHE = {}
        else:
            _RESPONSE_CACHE = raw
            logger.debug(
                "Glassdoor cache loaded: %d %s.",
                len(_RESPONSE_CACHE),
                "entry" if len(_RESPONSE_CACHE) == 1 else "entries",
            )
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load Glassdoor cache: %s", exc)
        _RESPONSE_CACHE = {}

    return _RESPONSE_CACHE


# ---------------------------------------------------------------------------
# Staleness check
# ---------------------------------------------------------------------------


def _is_fresh(conn: sqlite3.Connection, company_id: int) -> bool:
    """Return True if the company's enriched_at is less than 30 days old.

    Args:
        conn: Open SQLite connection to the V2 pipeline database.
        company_id: Primary key of the company row.

    Returns:
        True when enriched_at is present and less than _STALE_DAYS days ago.
    """
    row = conn.execute(
        "SELECT enriched_at FROM companies WHERE id = ?", (company_id,)
    ).fetchone()
    if row is None or row[0] is None:
        return False

    enriched_at_str: str = row[0]
    try:
        enriched_at = datetime.fromisoformat(enriched_at_str)
        # Ensure timezone-aware for comparison.
        if enriched_at.tzinfo is None:
            enriched_at = enriched_at.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - enriched_at
        return age.days < _STALE_DAYS
    except (ValueError, TypeError):
        return False


# ---------------------------------------------------------------------------
# Budget tracker
# ---------------------------------------------------------------------------


def _ensure_usage_table(conn: sqlite3.Connection) -> None:
    """Create glassdoor_api_usage if it doesn't already exist.

    This is a safety net; under normal operation init_db() creates the table
    before any enrich() calls.

    Args:
        conn: Open SQLite connection to the V2 pipeline database.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS glassdoor_api_usage (
            id      INTEGER PRIMARY KEY CHECK (id = 1),
            month   TEXT    NOT NULL,
            count   INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()


def _check_and_increment_budget(
    conn: sqlite3.Connection, company_name: str
) -> bool:
    """Return True and increment the counter if budget allows; False if exhausted.

    Resets the counter when the current calendar month differs from the
    stored month.  Uses a single-row table with a CHECK constraint on id = 1
    to enforce exactly one global counter.

    Args:
        conn: Open SQLite connection to the V2 pipeline database.
        company_name: Used only in the exhaustion warning message.

    Returns:
        True when the call is allowed and the counter has been incremented.
        False when the monthly budget is already exhausted.
    """
    _ensure_usage_table(conn)

    current_month = datetime.now(timezone.utc).strftime("%Y-%m")

    row = conn.execute(
        "SELECT month, count FROM glassdoor_api_usage WHERE id = 1"
    ).fetchone()

    if row is None:
        # First-ever call — insert the seed row.
        conn.execute(
            "INSERT INTO glassdoor_api_usage (id, month, count) VALUES (1, ?, 1)",
            (current_month,),
        )
        conn.commit()
        return True

    stored_month: str = row[0]
    stored_count: int = row[1]

    if stored_month != current_month:
        # New calendar month — reset counter.
        conn.execute(
            "UPDATE glassdoor_api_usage SET month = ?, count = 1 WHERE id = 1",
            (current_month,),
        )
        conn.commit()
        return True

    if stored_count >= _MONTHLY_BUDGET:
        logger.warning(
            "Glassdoor API monthly budget exhausted (%d/%d used). "
            "Skipping API call for '%s'.",
            stored_count,
            _MONTHLY_BUDGET,
            company_name,
        )
        return False

    conn.execute(
        "UPDATE glassdoor_api_usage SET count = count + 1 WHERE id = 1"
    )
    conn.commit()
    return True


# ---------------------------------------------------------------------------
# API interaction
# ---------------------------------------------------------------------------


def _fetch_glassdoor_rapidapi(
    company_name: str, api_key: str
) -> dict[str, Any] | None:
    """Query the RapidAPI Glassdoor company-overview endpoint.

    Args:
        company_name: Company name to search for.
        api_key: RapidAPI key (value of RAPIDAPI_KEY env var).

    Returns:
        The ``data`` sub-dict from the API response on success, or None on
        any HTTP or network failure.
    """
    headers = {
        "x-rapidapi-host": "real-time-glassdoor-data.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }
    params = {"company_id": company_name, "domain": "www.glassdoor.com"}

    try:
        response = requests.get(
            _BASE_URL, headers=headers, params=params, timeout=_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data")
        if data is None:
            logger.warning(
                "Glassdoor RapidAPI enrichment unavailable for '%s': "
                "response missing 'data' field.",
                company_name,
            )
            return None
        return data
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning(
            "Glassdoor RapidAPI enrichment unavailable for '%s': HTTP %s",
            company_name,
            status,
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            "Glassdoor RapidAPI enrichment unavailable for '%s': %s",
            company_name,
            exc,
        )
        return None


def _get_company_data(
    company_name: str, api_key: str, conn: sqlite3.Connection
) -> dict[str, Any] | None:
    """Return Glassdoor data dict, consulting cache before hitting the live API.

    Cache hits bypass both the API call and the budget counter.

    Args:
        company_name: Company name to look up.
        api_key: RapidAPI key used only when the cache misses.
        conn: Open DB connection for budget tracking (live calls only).

    Returns:
        The Glassdoor data dict on success, None on budget exhaustion or API
        failure.
    """
    cache = _load_cache()
    cached_entry = cache.get(company_name)
    if cached_entry is not None:
        status = cached_entry.get("status", "")
        if status == "OK":
            logger.debug(
                "Glassdoor cache hit for '%s'; skipping API call.", company_name
            )
            return cached_entry.get("data")
        # Non-OK cached entry — treat as no data available.
        logger.debug(
            "Glassdoor cache entry for '%s' has status '%s'; treating as miss.",
            company_name,
            status,
        )

    # Live API call — check budget first.
    if not _check_and_increment_budget(conn, company_name):
        return None

    return _fetch_glassdoor_rapidapi(company_name, api_key)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_company_data(data: dict[str, Any]) -> dict[str, Any]:
    """Extract rating, URL, and metadata from the RapidAPI Glassdoor data dict.

    Args:
        data: The ``data`` sub-dict from the API (or cache) response.

    Returns:
        Dict with two top-level keys:

        - ``glassdoor_rating`` (float | None): overall rating.
        - ``glassdoor_blob`` (dict): sub-ratings and metadata for the
          ``crunchbase_data['glassdoor']`` JSON blob.
    """

    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _safe_str(value: Any) -> str | None:
        if value is None or str(value).strip() == "":
            return None
        return str(value)

    overall = _safe_float(data.get("rating"))

    glassdoor_blob: dict[str, Any] = {
        # Sub-ratings
        "culture_and_values_rating": _safe_float(
            data.get("culture_and_values_rating")
        ),
        "work_life_balance_rating": _safe_float(
            data.get("work_life_balance_rating")
        ),
        "compensation_and_benefits_rating": _safe_float(
            data.get("compensation_and_benefits_rating")
        ),
        "career_opportunities_rating": _safe_float(
            data.get("career_opportunities_rating")
        ),
        "senior_management_rating": _safe_float(
            data.get("senior_management_rating")
        ),
        "diversity_and_inclusion_rating": _safe_float(
            data.get("diversity_and_inclusion_rating")
        ),
        "ceo_rating": _safe_float(data.get("ceo_rating")),
        # Metadata
        "revenue": _safe_str(data.get("revenue")),
        "company_type": _safe_str(data.get("company_type")),
        "company_description": _safe_str(data.get("company_description")),
        "review_count": data.get("review_count"),
        "year_founded": data.get("year_founded"),
        "recommend_to_friend_rating": _safe_float(
            data.get("recommend_to_friend_rating")
        ),
        "business_outlook_rating": _safe_float(data.get("business_outlook_rating")),
        # Convenience: Glassdoor link
        "company_link": _safe_str(data.get("company_link")),
    }

    # First-class columns the deep scorer reads directly.
    size_raw = _safe_str(data.get("company_size"))
    industry_raw = _safe_str(data.get("industry"))
    website_raw = _safe_str(data.get("website"))
    glassdoor_url = _safe_str(data.get("company_link"))
    hq_location = _safe_str(data.get("headquarters_location"))

    # Store extras in the blob too for traceability.
    glassdoor_blob["company_size_category"] = _safe_str(
        data.get("company_size_category")
    )
    glassdoor_blob["headquarters_location"] = hq_location

    return {
        "glassdoor_rating": overall,
        "glassdoor_url": glassdoor_url,
        "size_range": size_raw,
        "industry": industry_raw,
        "website": website_raw,
        "glassdoor_blob": glassdoor_blob,
    }


# ---------------------------------------------------------------------------
# Database write
# ---------------------------------------------------------------------------


def _update_company(
    conn: sqlite3.Connection,
    company_id: int,
    parsed: dict[str, Any],
) -> None:
    """Merge Glassdoor enrichment data into the companies row.

    Reads the existing ``crunchbase_data`` blob, merges the ``'glassdoor'``
    key, then writes back.  Existing keys (e.g. ``'levelsfy'``) are
    preserved.  Also writes ``size_range``, ``industry``, ``glassdoor_url``,
    and ``domain`` when the API provides them.

    Args:
        conn: Open SQLite connection to the V2 pipeline database.
        company_id: Primary key used to locate the row.
        parsed: The dict returned by ``_parse_company_data``.
    """
    glassdoor_blob = parsed["glassdoor_blob"]

    existing_row = conn.execute(
        "SELECT crunchbase_data FROM companies WHERE id = ?", (company_id,)
    ).fetchone()

    existing_data: dict[str, Any] = {}
    if existing_row and existing_row[0]:
        try:
            existing = json.loads(existing_row[0])
            existing_data = existing if isinstance(existing, dict) else {}
        except (json.JSONDecodeError, TypeError):
            existing_data = {}

    merged = {**existing_data, "glassdoor": glassdoor_blob}

    # Derive domain from website URL for the companies.domain column.
    website = parsed.get("website")
    domain = None
    if website:
        from urllib.parse import urlparse

        netloc = urlparse(website).netloc
        if netloc:
            domain = netloc.removeprefix("www.")

    conn.execute(
        """
        UPDATE companies
        SET    glassdoor_rating = ?,
               glassdoor_url    = ?,
               crunchbase_data  = ?,
               size_range       = COALESCE(?, size_range),
               industry         = COALESCE(?, industry),
               domain           = COALESCE(?, domain)
        WHERE  id = ?
        """,  # noqa: S608
        (
            parsed.get("glassdoor_rating"),
            parsed.get("glassdoor_url"),
            json.dumps(merged),
            parsed.get("size_range"),
            parsed.get("industry"),
            domain,
            company_id,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich(
    company_id: int, company_name: str, db_connection: sqlite3.Connection
) -> bool:
    """Enrich a company record with Glassdoor ratings and metadata.

    Checks whether the company's enrichment data is still fresh (< 30 days
    old) and skips the API call if so.  Otherwise consults the pre-fetched
    cache file and then, on a miss, calls the RapidAPI Glassdoor endpoint.
    Budget is capped at 90 requests per calendar month; calls beyond this
    limit are skipped and logged.

    Sub-ratings and metadata are merged into ``companies.crunchbase_data``
    under the ``'glassdoor'`` key, preserving other enrichment keys.

    Args:
        company_id: Primary key of the company row in the companies table.
        company_name: The company display name to look up on Glassdoor.
        db_connection: Open SQLite connection to the V2 pipeline database.

    Returns:
        True if enrichment succeeded and the row was updated with a non-null
        rating; False if the API was unavailable, the company was not found,
        the data had zero rating and zero reviews, or any error occurred.
    """
    api_key = os.environ.get("RAPIDAPI_KEY", "")
    if not api_key:
        logger.warning(
            "Glassdoor RapidAPI enrichment unavailable for '%s': "
            "RAPIDAPI_KEY environment variable is not set.",
            company_name,
        )
        return False

    if _is_fresh(db_connection, company_id):
        logger.debug(
            "Glassdoor RapidAPI enrichment skipped for '%s' (enriched_at < %d days old).",
            company_name,
            _STALE_DAYS,
        )
        return False

    data = _get_company_data(company_name, api_key, db_connection)
    if data is None:
        return False

    try:
        parsed = _parse_company_data(data)
        glassdoor_rating: float | None = parsed["glassdoor_rating"]
        review_count = parsed["glassdoor_blob"].get("review_count") or 0

        # Reject zero-rating / zero-review responses (e.g. Good Inside).
        if (glassdoor_rating is None or glassdoor_rating == 0) and review_count == 0:
            logger.warning(
                "Glassdoor RapidAPI enrichment for '%s': no meaningful data available "
                "(rating=%s, review_count=%s).",
                company_name,
                glassdoor_rating,
                review_count,
            )
            return False

        _update_company(db_connection, company_id, parsed)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Glassdoor RapidAPI enrichment failed for '%s': %s", company_name, exc
        )
        return False

    return True

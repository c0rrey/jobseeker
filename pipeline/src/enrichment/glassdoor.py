"""
Glassdoor enrichment module.

Queries the unofficial Glassdoor employer search endpoint to fetch a
company's overall rating and its Glassdoor profile URL, then writes
those values to ``companies.glassdoor_rating`` and
``companies.glassdoor_url``.

Authentication uses the ``GLASSDOOR_PARTNER_ID`` and
``GLASSDOOR_API_KEY`` environment variables (Glassdoor Partner API).
When either variable is absent the module logs a warning and returns
False immediately.

Returns True on success, False on any failure.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.glassdoor.com/api/api.htm"
_TIMEOUT = 10  # seconds


def _fetch_glassdoor(
    company_name: str, partner_id: str, api_key: str
) -> dict[str, Any] | None:
    """Fetch employer data from the Glassdoor employer search API.

    Args:
        company_name: Company name to search for.
        partner_id: Glassdoor partner ID.
        api_key: Glassdoor API key.

    Returns:
        The first employer dict from the API response, or None on failure.
    """
    params = {
        "v": "1",
        "format": "json",
        "t.p": partner_id,
        "t.k": api_key,
        "action": "employers",
        "q": company_name,
        "ps": 1,
        "userip": "0.0.0.0",
        "useragent": "jseeker-enrichment/1.0",
    }

    try:
        response = requests.get(_BASE_URL, params=params, timeout=_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        employers = (
            data.get("response", {}).get("employers", [])
        )
        if not employers:
            logger.warning(
                "Glassdoor enrichment unavailable for '%s': no employer results returned.",
                company_name,
            )
            return None
        return employers[0]
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning(
            "Glassdoor enrichment unavailable for '%s': HTTP %s", company_name, status
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            "Glassdoor enrichment unavailable for '%s': %s", company_name, exc
        )
        return None


def _parse_employer(employer: dict[str, Any]) -> dict[str, Any]:
    """Extract glassdoor_rating and glassdoor_url from an employer record.

    Args:
        employer: A single employer dict from the Glassdoor API response.

    Returns:
        Dict with keys: glassdoor_rating (float | None), glassdoor_url (str).
    """
    rating_raw = employer.get("overallRating")
    try:
        glassdoor_rating: float | None = float(rating_raw) if rating_raw else None
    except (TypeError, ValueError):
        glassdoor_rating = None

    glassdoor_url: str = employer.get("featuredReviewUrl") or employer.get("reviewsUrl", "")

    return {
        "glassdoor_rating": glassdoor_rating,
        "glassdoor_url": glassdoor_url,
    }


def _update_company(
    conn: sqlite3.Connection,
    company_id: int,
    fields: dict[str, Any],
) -> None:
    """Write enrichment fields to the companies table row for company_id.

    Args:
        conn: Open SQLite connection with the V2 schema.
        company_id: Primary key used to locate the row via the ``id`` column.
        fields: Column-value pairs to update.
    """
    fields["enriched_at"] = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{col} = ?" for col in fields)
    values = list(fields.values()) + [company_id]
    conn.execute(
        f"UPDATE companies SET {set_clause} WHERE id = ?",  # noqa: S608
        values,
    )
    conn.commit()


def enrich(company_id: int, company_name: str, db_connection: sqlite3.Connection) -> bool:
    """Enrich a company record with Glassdoor rating and URL.

    Queries the Glassdoor Partner API for ``company_name`` and writes
    ``glassdoor_rating`` and ``glassdoor_url`` to the companies row identified
    by ``company_id``.

    Args:
        company_id: Primary key of the company row in the companies table.
        company_name: The company display name to look up on Glassdoor.
        db_connection: Open SQLite connection to the V2 pipeline database.

    Returns:
        True if enrichment succeeded and the row was updated; False if the
        API was unavailable, the company was not found, or any error occurred.
    """
    partner_id = os.environ.get("GLASSDOOR_PARTNER_ID", "")
    api_key = os.environ.get("GLASSDOOR_API_KEY", "")

    if not partner_id or not api_key:
        logger.warning(
            "Glassdoor enrichment unavailable for '%s': "
            "GLASSDOOR_PARTNER_ID and/or GLASSDOOR_API_KEY environment variables are not set.",
            company_name,
        )
        return False

    employer = _fetch_glassdoor(company_name, partner_id, api_key)
    if employer is None:
        return False

    try:
        fields = _parse_employer(employer)
        _update_company(db_connection, company_id, fields)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Glassdoor enrichment failed for '%s': %s", company_name, exc
        )
        return False

    return True

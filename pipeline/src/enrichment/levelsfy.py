"""
Levels.fyi enrichment module.

Fetches compensation data for a company from the unofficial levels.fyi
public API and merges the result into the ``companies.crunchbase_data``
JSON column under the ``"levelsfy"`` key, preserving any existing
Crunchbase signals written by the crunchbase enrichment module.

The levels.fyi endpoint is unauthenticated and public, but rate-limited.
No API key is required. If the request fails the module logs a warning and
returns False immediately without raising.

Returns True on success, False on any failure.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Unofficial public endpoint used by levels.fyi's own web app.
_BASE_URL = "https://www.levels.fyi/api/company"
_TIMEOUT = 10  # seconds


def _fetch_levelsfy(company_name: str) -> dict[str, Any] | None:
    """Fetch compensation summary data from levels.fyi for a company.

    Args:
        company_name: Company name to query.

    Returns:
        Parsed JSON data dict on success, None on any HTTP or network error.
    """
    params = {"company": company_name}

    try:
        response = requests.get(_BASE_URL, params=params, timeout=_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if not data:
            logger.warning(
                "Levels.fyi enrichment unavailable for '%s': empty response.",
                company_name,
            )
            return None
        return data
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning(
            "Levels.fyi enrichment unavailable for '%s': HTTP %s", company_name, status
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            "Levels.fyi enrichment unavailable for '%s': %s", company_name, exc
        )
        return None


def _parse_comp_data(raw: dict[str, Any], company_name: str) -> dict[str, Any]:
    """Build a standardised comp summary from the levels.fyi response.

    Args:
        raw: Raw JSON response from the levels.fyi API.
        company_name: Included in the stored blob for traceability.

    Returns:
        Dict suitable for storage under the ``"levelsfy"`` key of the
        ``companies.crunchbase_data`` JSON column.
    """
    return {
        "source": "levels.fyi",
        "company": company_name,
        "levels": raw.get("levels", []),
        "median_total_comp": raw.get("medianTotalComp"),
        "median_base_salary": raw.get("medianBaseSalary"),
        "sample_size": raw.get("sampleSize"),
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
    """Enrich a company record with levels.fyi compensation data.

    Queries levels.fyi for ``company_name`` and merges the compensation
    summary into the ``companies.crunchbase_data`` JSON column under the
    ``"levelsfy"`` key, preserving any existing Crunchbase signals.

    Args:
        company_id: Primary key of the company row in the companies table.
        company_name: The company display name to look up on levels.fyi.
        db_connection: Open SQLite connection to the V2 pipeline database.

    Returns:
        True if enrichment succeeded and the row was updated; False if the
        source was unavailable, the company was not found, or any error
        occurred.
    """
    raw = _fetch_levelsfy(company_name)
    if raw is None:
        return False

    try:
        comp_summary = _parse_comp_data(raw, company_name)

        # Read existing crunchbase_data so we can merge rather than overwrite.
        existing_row = db_connection.execute(
            "SELECT crunchbase_data FROM companies WHERE id = ?", (company_id,)
        ).fetchone()
        existing_data: dict[str, Any] = {}
        if existing_row and existing_row[0]:
            try:
                parsed = json.loads(existing_row[0])
                existing_data = parsed if isinstance(parsed, dict) else {}
            except (json.JSONDecodeError, TypeError):
                existing_data = {}

        # Merge levels.fyi data under "levelsfy" key, preserving crunchbase signals.
        merged = {**existing_data, "levelsfy": comp_summary}
        fields = {"crunchbase_data": json.dumps(merged)}
        _update_company(db_connection, company_id, fields)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Levels.fyi enrichment failed for '%s': %s", company_name, exc
        )
        return False

    return True

"""
Crunchbase enrichment module.

Queries the Crunchbase Basic API (free tier) for company metadata and writes
size_range, industry, funding_stage, and crunchbase_data to the companies
table. Returns True on success, False on any failure.

Crunchbase Basic API endpoint:
    GET https://api.crunchbase.com/api/v4/entities/organizations/{permalink}
    ?user_key={key}&field_ids=short_description,num_employees_enum,
    funding_total,last_funding_type,categories

The API key is read from the CRUNCHBASE_API_KEY environment variable. If the
variable is not set the module logs a warning and returns False immediately.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.crunchbase.com/api/v4/entities/organizations"
_TIMEOUT = 10  # seconds
_FIELD_IDS = "short_description,num_employees_enum,funding_total,last_funding_type,categories"


# ---------------------------------------------------------------------------
# Employee count enum -> human-readable range mapping
# ---------------------------------------------------------------------------

_EMPLOYEE_ENUM_MAP: dict[str, str] = {
    "c_00001_00010": "1-10",
    "c_00011_00050": "11-50",
    "c_00051_00100": "51-100",
    "c_00101_00250": "101-250",
    "c_00251_00500": "251-500",
    "c_00501_01000": "501-1000",
    "c_01001_05000": "1001-5000",
    "c_05001_10000": "5001-10000",
    "c_10001_max": "10001+",
}


def _company_name_to_permalink(company_name: str) -> str:
    """Convert a company display name to a best-guess Crunchbase permalink slug.

    Args:
        company_name: The company's display name (e.g. "Acme Corp").

    Returns:
        A lowercase, hyphen-separated slug (e.g. "acme-corp").
    """
    return company_name.lower().strip().replace(" ", "-").replace(",", "").replace(".", "")


def _fetch_crunchbase(company_name: str, api_key: str) -> dict[str, Any] | None:
    """Fetch raw organization data from the Crunchbase API.

    Args:
        company_name: The company display name used to derive the permalink.
        api_key: Crunchbase API key.

    Returns:
        Parsed JSON ``properties`` dict on success, None on any HTTP or
        network error.
    """
    permalink = _company_name_to_permalink(company_name)
    url = f"{_BASE_URL}/{permalink}"
    params = {"user_key": api_key, "field_ids": _FIELD_IDS}

    try:
        response = requests.get(url, params=params, timeout=_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("properties", {})
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning(
            "Crunchbase enrichment unavailable for '%s': HTTP %s", company_name, status
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            "Crunchbase enrichment unavailable for '%s': %s", company_name, exc
        )
        return None


def _parse_properties(props: dict[str, Any]) -> dict[str, Any]:
    """Extract enrichment fields from Crunchbase organization properties.

    Args:
        props: The ``properties`` dict from the Crunchbase API response.

    Returns:
        Dict with keys: size_range, industry, funding_stage, crunchbase_data.
    """
    size_enum = props.get("num_employees_enum", "")
    size_range = _EMPLOYEE_ENUM_MAP.get(size_enum, "")

    # Categories is a list of category objects; take the first category name.
    categories = (props.get("categories") or {}).get("entities", [])
    industry = categories[0].get("properties", {}).get("name", "") if categories else ""

    funding_stage = props.get("last_funding_type", "") or ""

    crunchbase_data = json.dumps(
        {
            "short_description": props.get("short_description", ""),
            "funding_total": props.get("funding_total", {}),
            "last_funding_type": funding_stage,
            "num_employees_enum": size_enum,
        }
    )

    return {
        "size_range": size_range,
        "industry": industry,
        "funding_stage": funding_stage,
        "crunchbase_data": crunchbase_data,
    }


def _update_company(
    conn: sqlite3.Connection,
    company_id: int,
    fields: dict[str, Any],
) -> None:
    """Write enrichment fields into the companies table row for company_id.

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
    """Enrich a company record with Crunchbase data.

    Queries the Crunchbase Basic API for ``company_name`` and writes
    ``size_range``, ``industry``, ``funding_stage``, and ``crunchbase_data``
    to the companies table row identified by ``company_id``.

    Args:
        company_id: Primary key of the company row in the companies table.
        company_name: The company display name to look up on Crunchbase.
        db_connection: Open SQLite connection to the V2 pipeline database.

    Returns:
        True if enrichment succeeded and the row was updated; False if the
        API was unavailable, the company was not found, or any error occurred.
    """
    api_key = os.environ.get("CRUNCHBASE_API_KEY", "")
    if not api_key:
        logger.warning(
            "Crunchbase enrichment unavailable for '%s': "
            "CRUNCHBASE_API_KEY environment variable is not set.",
            company_name,
        )
        return False

    props = _fetch_crunchbase(company_name, api_key)
    if props is None:
        return False

    if not props:
        logger.warning(
            "Crunchbase enrichment unavailable for '%s': empty response properties.",
            company_name,
        )
        return False

    try:
        fields = _parse_properties(props)
        _update_company(db_connection, company_id, fields)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Crunchbase enrichment failed for '%s': %s", company_name, exc
        )
        return False

    return True

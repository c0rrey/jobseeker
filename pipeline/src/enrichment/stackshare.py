"""
StackShare enrichment module.

Queries the StackShare GraphQL API to fetch the technology stack for a
company and writes the result as a JSON array to ``companies.tech_stack``.

Authentication uses the ``STACKSHARE_API_KEY`` environment variable. When
the variable is absent the module logs a warning and returns False
immediately.

Returns True on success, False on any failure.
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

_GRAPHQL_URL = "https://api.stackshare.io/graphql"
_TIMEOUT = 10  # seconds

_STACK_QUERY = """
query GetCompanyStack($slug: String!) {
  company(slug: $slug) {
    name
    stackItems {
      tool {
        name
        category {
          name
        }
      }
    }
  }
}
"""


def _company_name_to_slug(company_name: str) -> str:
    """Convert a company display name to a best-guess StackShare slug.

    Args:
        company_name: The company's display name (e.g. "Acme Corp").

    Returns:
        Lowercase, hyphen-separated slug (e.g. "acme-corp").
    """
    return company_name.lower().strip().replace(" ", "-").replace(",", "").replace(".", "")


def _fetch_stackshare(company_name: str, api_key: str) -> list[dict[str, Any]] | None:
    """Fetch the tech stack for a company from the StackShare GraphQL API.

    Args:
        company_name: Company display name used to derive the StackShare slug.
        api_key: StackShare API key passed as Authorization bearer token.

    Returns:
        List of tool dicts on success, None on any HTTP or network error.
    """
    slug = _company_name_to_slug(company_name)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "query": _STACK_QUERY,
        "variables": {"slug": slug},
    }

    try:
        response = requests.post(
            _GRAPHQL_URL, json=payload, headers=headers, timeout=_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        errors = data.get("errors")
        if errors:
            logger.warning(
                "StackShare enrichment unavailable for '%s': GraphQL errors: %s",
                company_name,
                errors,
            )
            return None

        company_data = data.get("data", {}).get("company")
        if not company_data:
            logger.warning(
                "StackShare enrichment unavailable for '%s': company not found on StackShare.",
                company_name,
            )
            return None

        stack_items = company_data.get("stackItems", [])
        return stack_items

    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning(
            "StackShare enrichment unavailable for '%s': HTTP %s", company_name, status
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            "StackShare enrichment unavailable for '%s': %s", company_name, exc
        )
        return None


def _parse_stack_items(stack_items: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Convert raw StackShare stackItems into a clean list of tool records.

    Args:
        stack_items: List of stackItem dicts from the StackShare GraphQL response.

    Returns:
        List of dicts with ``name`` and ``category`` keys.
    """
    result: list[dict[str, str]] = []
    for item in stack_items:
        tool = item.get("tool", {})
        tool_name = tool.get("name", "")
        category = tool.get("category", {}) or {}
        category_name = category.get("name", "")
        if tool_name:
            result.append({"name": tool_name, "category": category_name})
    return result


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
    """Enrich a company record with tech stack data from StackShare.

    Queries the StackShare GraphQL API for ``company_name`` and serialises the
    tool list as a JSON array into ``companies.tech_stack``.

    Args:
        company_id: Primary key of the company row in the companies table.
        company_name: The company display name to look up on StackShare.
        db_connection: Open SQLite connection to the V2 pipeline database.

    Returns:
        True if enrichment succeeded and the row was updated; False if the
        API was unavailable, the company was not found, or any error occurred.
    """
    api_key = os.environ.get("STACKSHARE_API_KEY", "")
    if not api_key:
        logger.warning(
            "StackShare enrichment unavailable for '%s': "
            "STACKSHARE_API_KEY environment variable is not set.",
            company_name,
        )
        return False

    stack_items = _fetch_stackshare(company_name, api_key)
    if stack_items is None:
        return False

    try:
        tools = _parse_stack_items(stack_items)
        fields = {"tech_stack": json.dumps(tools)}
        _update_company(db_connection, company_id, fields)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "StackShare enrichment failed for '%s': %s", company_name, exc
        )
        return False

    return True

"""
Company discovery module for the Jobseeker V2 pipeline.

Provides two public functions:

    discover_company(company_name, db_connection, career_url=None)
        Finds a company's career page, detects whether it uses a known ATS
        platform (Greenhouse, Lever, Workday, Ashby, etc.), and persists the
        result to ``career_page_configs``.  Also updates ``companies.ats_platform``
        when an ATS is detected.

        If ``career_url`` is None, SerpAPI is used to search for the career page.
        If the SERPAPI_KEY environment variable is absent, a warning is logged
        and None is returned without raising.

    rediscover_broken(db_connection)
        Fetches all ``career_page_configs`` rows where ``status='broken'`` and
        re-runs ``discover_company()`` for each.  Updates ``status`` to
        ``'active'`` on success; leaves it as ``'broken'`` on second failure.

Intermediate results are captured in the ``DiscoveryResult`` dataclass, which
is passed between the URL-resolution, HTML-fetch, and LLM-analysis phases.
This makes each phase independently testable.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

import requests

from pipeline.config.settings import get_serpapi_key

logger = logging.getLogger(__name__)

# SerpAPI endpoint
_SERPAPI_URL = "https://serpapi.com/search"
# Request timeout in seconds
_HTTP_TIMEOUT = 15
# Max HTML bytes sent to the LLM (first 20 000 characters of response text)
_HTML_TRUNCATE = 20_000


# ---------------------------------------------------------------------------
# Intermediate result container
# ---------------------------------------------------------------------------


@dataclass
class DiscoveryResult:
    """Intermediate container passed between discovery phases.

    Attributes:
        company_name: Display name of the company being discovered.
        career_url: Resolved URL of the career page (may be None before URL
            resolution succeeds).
        html: Raw HTML of the career page (None before the fetch phase).
        llm_response: Parsed JSON dict returned by the LLM analysis phase
            (None before analysis runs).
        error: Human-readable error message if any phase fails.
    """

    company_name: str
    career_url: Optional[str] = None
    html: Optional[str] = None
    llm_response: Optional[dict[str, Any]] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Phase 1 — URL resolution
# ---------------------------------------------------------------------------


def _resolve_career_url(company_name: str) -> Optional[str]:
    """Search SerpAPI for the company's career page URL.

    Args:
        company_name: Display name of the company (e.g. "Acme Corp").

    Returns:
        The first organic result URL, or None if the search fails or returns
        no results.
    """
    try:
        api_key = get_serpapi_key()
    except ValueError:
        logger.warning(
            "SERPAPI_KEY is not set; skipping career page search for '%s'.",
            company_name,
        )
        return None

    query = f"{company_name} careers jobs"
    params: dict[str, str] = {
        "q": query,
        "api_key": api_key,
        "engine": "google",
        "num": "5",
    }

    try:
        response = requests.get(_SERPAPI_URL, params=params, timeout=_HTTP_TIMEOUT)
        response.raise_for_status()
        data: dict[str, Any] = response.json()
    except requests.RequestException as exc:
        logger.warning(
            "SerpAPI request failed for '%s': %s", company_name, exc
        )
        return None

    organic_results: list[dict[str, Any]] = data.get("organic_results", [])
    if not organic_results:
        logger.warning("SerpAPI returned no results for '%s'.", company_name)
        return None

    url: str = organic_results[0].get("link", "")
    if not url:
        logger.warning(
            "SerpAPI first result for '%s' has no 'link' field.", company_name
        )
        return None

    logger.info("Resolved career URL for '%s': %s", company_name, url)
    return url


# ---------------------------------------------------------------------------
# Phase 2 — HTML fetch
# ---------------------------------------------------------------------------


def _fetch_html(url: str) -> Optional[str]:
    """Fetch the HTML content of a URL, truncated to _HTML_TRUNCATE characters.

    Args:
        url: The URL to fetch.

    Returns:
        Truncated text content, or None on request failure.
    """
    try:
        response = requests.get(
            url,
            timeout=_HTTP_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; jseeker-bot/2.0)"},
        )
        response.raise_for_status()
        return response.text[:_HTML_TRUNCATE]
    except requests.RequestException as exc:
        logger.warning("Failed to fetch HTML from '%s': %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Phase 3 — LLM analysis (prompt invocation stub)
# ---------------------------------------------------------------------------


def _load_prompt_template() -> str:
    """Load the company_discovery.md prompt template.

    Returns:
        The raw template string with {{ html }} placeholder.

    Raises:
        FileNotFoundError: If the prompt file does not exist.
    """
    from pathlib import Path

    prompt_path = Path(__file__).resolve().parent.parent / "prompts" / "company_discovery.md"
    return prompt_path.read_text(encoding="utf-8")


def _analyse_html(html: str) -> Optional[dict[str, Any]]:
    """Invoke the LLM to detect ATS and generate scrape strategy.

    This function renders the company_discovery.md template with the provided
    HTML, then calls the Claude API. In the current implementation the API call
    is delegated to ``_call_llm``, which is mocked in tests.

    Args:
        html: Career page HTML (pre-truncated to _HTML_TRUNCATE chars).

    Returns:
        Parsed JSON dict from the LLM, or None if the call fails or the
        response cannot be parsed as JSON.
    """
    try:
        template = _load_prompt_template()
    except FileNotFoundError:
        logger.error("company_discovery.md prompt template not found.")
        return None

    prompt = template.replace("{{ html }}", html)
    raw_response = _call_llm(prompt)
    if raw_response is None:
        return None

    # Strip markdown code fences if the LLM wraps output
    cleaned = raw_response.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        # Remove opening fence (```json or ```)
        lines = lines[1:]
        # Remove closing fence
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        logger.warning("LLM response was not valid JSON: %s", exc)
        return None


def _call_llm(prompt: str) -> Optional[str]:
    """Send the rendered prompt to the Claude API and return the text response.

    This thin wrapper is kept separate so tests can mock it without patching
    the entire ``_analyse_html`` function.

    Args:
        prompt: Fully rendered prompt string.

    Returns:
        Raw text from the model, or None on error.
    """
    try:
        import anthropic  # type: ignore[import]
    except ImportError:
        logger.error(
            "anthropic package is not installed; cannot invoke LLM. "
            "Install it with: pip install anthropic"
        )
        return None

    try:
        client = anthropic.Anthropic()
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as exc:  # noqa: BLE001
        logger.warning("LLM call failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Phase 4 — DB persistence
# ---------------------------------------------------------------------------


def _persist_discovery(
    result: DiscoveryResult,
    db_connection: sqlite3.Connection,
    company_id: int,
) -> bool:
    """Write discovery results to career_page_configs and update companies.

    For ATS-detected pages:
        - Inserts (or replaces) a career_page_configs row using the ATS feed URL.
        - Updates companies.ats_platform with the detected platform.

    For non-ATS pages:
        - Inserts (or replaces) a career_page_configs row with scrape_strategy JSON.

    Args:
        result: Completed DiscoveryResult containing llm_response.
        db_connection: Open SQLite connection.
        company_id: Primary key of the company row in the companies table.

    Returns:
        True when persistence succeeds, False on error.
    """
    if result.llm_response is None or result.career_url is None:
        logger.warning(
            "Cannot persist discovery for '%s': llm_response or career_url is None.",
            result.company_name,
        )
        return False

    llm = result.llm_response
    is_ats: bool = bool(llm.get("is_ats", False))

    if is_ats:
        ats_platform: Optional[str] = llm.get("ats_platform")
        ats_feed_url: Optional[str] = llm.get("ats_feed_url") or result.career_url
        config_url = ats_feed_url or result.career_url
        scrape_strategy_json: Optional[str] = None
    else:
        ats_platform = None
        config_url = result.career_url
        raw_strategy = llm.get("scrape_strategy")
        scrape_strategy_json = json.dumps(raw_strategy) if raw_strategy else None

    try:
        # Upsert career_page_configs: if a row already exists for this company,
        # update it; otherwise insert.
        existing = db_connection.execute(
            "SELECT id FROM career_page_configs WHERE company_id = ?",
            (company_id,),
        ).fetchone()

        if existing:
            db_connection.execute(
                """
                UPDATE career_page_configs
                SET url = ?,
                    discovery_method = 'auto',
                    scrape_strategy = ?,
                    status = 'active'
                WHERE company_id = ?
                """,
                (config_url, scrape_strategy_json, company_id),
            )
        else:
            db_connection.execute(
                """
                INSERT INTO career_page_configs
                    (company_id, url, discovery_method, scrape_strategy, status)
                VALUES (?, ?, 'auto', ?, 'active')
                """,
                (company_id, config_url, scrape_strategy_json),
            )

        if is_ats and ats_platform:
            db_connection.execute(
                "UPDATE companies SET ats_platform = ?, career_page_url = ? WHERE id = ?",
                (ats_platform, result.career_url, company_id),
            )
        else:
            db_connection.execute(
                "UPDATE companies SET career_page_url = ? WHERE id = ?",
                (result.career_url, company_id),
            )

        db_connection.commit()
        logger.info(
            "Persisted discovery for company_id=%d (%s): is_ats=%s, platform=%s",
            company_id,
            result.company_name,
            is_ats,
            ats_platform,
        )
        return True

    except sqlite3.Error as exc:
        logger.error(
            "DB error persisting discovery for '%s': %s", result.company_name, exc
        )
        db_connection.rollback()
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def discover_company(
    company_name: str,
    db_connection: sqlite3.Connection,
    career_url: Optional[str] = None,
) -> Optional[CareerPageConfig]:
    """Discover and persist career page information for a company.

    Phases:
        1. URL resolution — use SerpAPI if ``career_url`` is None.
        2. HTML fetch — download the career page.
        3. LLM analysis — detect ATS platform or generate scrape strategy.
        4. DB persistence — upsert career_page_configs; update companies.

    Args:
        company_name: Display name of the company (e.g. "Acme Corp").
        db_connection: Open SQLite connection with WAL mode (from
            ``database.get_connection``).  Must have the companies and
            career_page_configs tables initialised.
        career_url: Optional known URL of the career page.  When None, SerpAPI
            is used to find it.  If SERPAPI_KEY is absent, this function logs a
            warning and returns None.

    Returns:
        A :class:`CareerPageConfig` if discovery and persistence succeed,
        None otherwise.
    """
    from pipeline.src.models import CareerPageConfig

    result = DiscoveryResult(company_name=company_name, career_url=career_url)

    # --- Phase 1: URL resolution ---
    if result.career_url is None:
        resolved = _resolve_career_url(company_name)
        if resolved is None:
            # Already logged inside _resolve_career_url
            return None
        result.career_url = resolved

    # --- Phase 2: HTML fetch ---
    html = _fetch_html(result.career_url)
    if html is None:
        logger.warning(
            "Could not fetch HTML for '%s' at %s.", company_name, result.career_url
        )
        return None
    result.html = html

    # --- Phase 3: LLM analysis ---
    llm_response = _analyse_html(html)
    if llm_response is None:
        logger.warning(
            "LLM analysis returned no result for '%s'.", company_name
        )
        return None
    result.llm_response = llm_response

    # --- Resolve or create company row ---
    company_row = db_connection.execute(
        "SELECT id FROM companies WHERE name = ?", (company_name,)
    ).fetchone()

    if company_row is None:
        db_connection.execute(
            "INSERT INTO companies (name) VALUES (?)", (company_name,)
        )
        db_connection.commit()
        company_row = db_connection.execute(
            "SELECT id FROM companies WHERE name = ?", (company_name,)
        ).fetchone()

    company_id: int = company_row["id"] if hasattr(company_row, "keys") else company_row[0]

    # --- Phase 4: DB persistence ---
    ok = _persist_discovery(result, db_connection, company_id)
    if not ok:
        return None

    # Build and return the CareerPageConfig dataclass from what we persisted
    llm = result.llm_response
    is_ats = bool(llm.get("is_ats", False))
    if is_ats:
        config_url = llm.get("ats_feed_url") or result.career_url
        scrape_strategy = None
    else:
        config_url = result.career_url
        raw_strategy = llm.get("scrape_strategy")
        scrape_strategy = json.dumps(raw_strategy) if raw_strategy else None

    # Fetch the persisted row to get the id
    config_row = db_connection.execute(
        "SELECT id FROM career_page_configs WHERE company_id = ?",
        (company_id,),
    ).fetchone()
    config_id = (
        config_row["id"] if hasattr(config_row, "keys") else config_row[0]
    ) if config_row else None

    return CareerPageConfig(
        company_id=company_id,
        url=config_url or result.career_url,  # type: ignore[arg-type]
        discovery_method="auto",
        scrape_strategy=scrape_strategy,
        status="active",
        id=config_id,
    )


def rediscover_broken(db_connection: sqlite3.Connection) -> dict[str, int]:
    """Re-run discovery for all career_page_configs rows with status='broken'.

    For each broken config, calls :func:`discover_company` with the config's
    existing URL (bypassing SerpAPI).  On success, the config status becomes
    'active' (persisted by :func:`discover_company`).  On failure the config
    remains 'broken'.

    Args:
        db_connection: Open SQLite connection.

    Returns:
        A dict with keys ``attempted``, ``recovered``, ``still_broken``
        counting the results.
    """
    broken_rows = db_connection.execute(
        """
        SELECT cpc.id, cpc.company_id, cpc.url, c.name AS company_name
        FROM career_page_configs cpc
        JOIN companies c ON c.id = cpc.company_id
        WHERE cpc.status = 'broken'
        """
    ).fetchall()

    stats: dict[str, int] = {
        "attempted": len(broken_rows),
        "recovered": 0,
        "still_broken": 0,
    }

    for row in broken_rows:
        if hasattr(row, "keys"):
            company_name: str = row["company_name"]
            career_url: str = row["url"]
        else:
            company_name = row[3]
            career_url = row[2]

        logger.info(
            "Re-running discovery for broken config: company='%s', url='%s'",
            company_name,
            career_url,
        )

        config = discover_company(
            company_name=company_name,
            db_connection=db_connection,
            career_url=career_url,
        )

        if config is not None:
            stats["recovered"] += 1
            logger.info("Recovered broken config for '%s'.", company_name)
        else:
            stats["still_broken"] += 1
            logger.warning("Discovery still failing for '%s'; config remains broken.", company_name)

    logger.info(
        "rediscover_broken: attempted=%d, recovered=%d, still_broken=%d",
        stats["attempted"],
        stats["recovered"],
        stats["still_broken"],
    )
    return stats

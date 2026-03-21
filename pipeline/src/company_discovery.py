"""
Company discovery module for the Jobseeker V2 pipeline.

Provides two public functions:

    discover_company(company_name, db_connection, career_url=None)
        Looks up the company on Glassdoor via the RapidAPI endpoint,
        derives a career page URL by probing ``/careers`` and ``/jobs`` on
        the company's website domain, and persists the result to the
        ``companies`` table (with Glassdoor metadata) and optionally to
        ``career_page_configs``.

        When ``career_url`` is supplied the Glassdoor lookup is still
        performed for metadata, but the supplied URL is used directly for
        career page discovery.

        If ``RAPIDAPI_KEY`` is absent a warning is logged and ``None`` is
        returned without raising.

        Returns a :class:`CompanyRecord` on success (even when career URL
        probing fails), ``None`` only when the company cannot be looked up
        at all (budget exhausted or API error with no existing row).

    rediscover_broken(db_connection)
        Fetches all ``career_page_configs`` rows where ``status='broken'``
        and re-runs ``discover_company()`` for each.  Updates ``status`` to
        ``'active'`` on success; leaves it as ``'broken'`` on second
        failure.

Intermediate results are captured in the ``DiscoveryResult`` dataclass,
which is passed between the URL-resolution, HTML-fetch, and LLM-analysis
phases.  This makes each phase independently testable.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Glassdoor RapidAPI endpoint (same as enrichment module)
_GLASSDOOR_BASE_URL = "https://real-time-glassdoor-data.p.rapidapi.com/company-overview"
# Request timeout in seconds
_HTTP_TIMEOUT = 15
# Shorter timeout for career-URL probing (HEAD requests)
_PROBE_TIMEOUT = 8
# Max HTML bytes sent to the LLM (first 20 000 characters of response text)
_HTML_TRUNCATE = 20_000
# Candidate path suffixes to probe for career pages, in priority order
_CAREER_PATHS = ["/careers", "/jobs"]


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


@dataclass
class CompanyRecord:
    """Lightweight result returned from :func:`discover_company`.

    Provides a stable interface for callers that only need the company's
    primary key and whether a career page URL was found.

    Attributes:
        company_id: Primary key of the row in ``companies``.
        career_page_url: Probed career page URL, or ``None`` when probing
            failed.
    """

    company_id: int
    career_page_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Phase 1 — Glassdoor lookup and career URL resolution
# ---------------------------------------------------------------------------


def _extract_domain(website_url: str) -> Optional[str]:
    """Extract the scheme+host from a website URL.

    Args:
        website_url: A URL string such as ``"https://acme.com"`` or
            ``"acme.com"`` (without scheme).

    Returns:
        Normalised ``"scheme://host"`` string, or ``None`` if parsing
        fails or the host is empty.
    """
    url = website_url.strip()
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"
    except ValueError:
        return None


def _probe_career_url(domain: str) -> Optional[str]:
    """Probe ``/careers`` and ``/jobs`` paths on *domain* and return the first live URL.

    Uses HTTP HEAD with a short timeout; falls back to GET if the server
    returns a ``405 Method Not Allowed``.

    Args:
        domain: Normalised ``"scheme://host"`` string (no trailing slash).

    Returns:
        The first URL path that returns a 2xx or 3xx response, or ``None``
        if all candidates fail.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; jseeker-bot/2.0)"}
    for path in _CAREER_PATHS:
        url = domain + path
        try:
            resp = requests.head(
                url, timeout=_PROBE_TIMEOUT, headers=headers, allow_redirects=True
            )
            if resp.status_code == 405:
                # Server does not accept HEAD — try GET
                resp = requests.get(
                    url,
                    timeout=_PROBE_TIMEOUT,
                    headers=headers,
                    allow_redirects=True,
                    stream=True,
                )
                resp.close()
            if resp.status_code < 400:
                logger.info("Career URL probe succeeded for %s: %s", domain, url)
                return url
        except requests.RequestException as exc:
            logger.debug("Probe failed for %s: %s", url, exc)
    logger.info("Career URL probe found no live path on domain %s.", domain)
    return None


def _fetch_glassdoor_data(
    company_name: str,
    api_key: str,
    db_connection: sqlite3.Connection,
) -> Optional[dict[str, Any]]:
    """Query Glassdoor via RapidAPI for company-overview data.

    Checks the monthly budget table before making any live API call.
    Budget tracking reuses the ``glassdoor_api_usage`` table maintained
    by :mod:`pipeline.src.enrichment.glassdoor_rapidapi`.

    Args:
        company_name: Company display name (used as the ``company_id``
            query parameter after special-character normalisation).
        api_key: Value of the ``RAPIDAPI_KEY`` environment variable.
        db_connection: Open SQLite connection (for budget tracking).

    Returns:
        The raw ``data`` sub-dict from the API response on success, or
        ``None`` on budget exhaustion or any HTTP/network failure.
    """
    # Reuse the budget tracker from the enrichment module.
    from pipeline.src.enrichment.glassdoor_rapidapi import _check_and_increment_budget

    if not _check_and_increment_budget(db_connection, company_name):
        return None

    headers = {
        "x-rapidapi-host": "real-time-glassdoor-data.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }
    # Normalise the company name: strip characters that confuse the API.
    safe_name = company_name.strip()
    params: dict[str, str] = {
        "company_id": safe_name,
        "domain": "www.glassdoor.com",
    }

    try:
        response = requests.get(
            _GLASSDOOR_BASE_URL,
            headers=headers,
            params=params,
            timeout=_HTTP_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data")
        if data is None:
            logger.warning(
                "Glassdoor API returned no 'data' for '%s'.", company_name
            )
            return None
        return data
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning(
            "Glassdoor API HTTP error for '%s': %s", company_name, status
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            "Glassdoor API request failed for '%s': %s", company_name, exc
        )
        return None


def _parse_glassdoor_metadata(data: dict[str, Any]) -> dict[str, Any]:
    """Extract company metadata fields from a Glassdoor API data dict.

    Args:
        data: The ``data`` sub-dict from the API response.

    Returns:
        Dict with keys usable directly as ``companies`` column values:
        ``name``, ``glassdoor_rating``, ``glassdoor_url``, ``industry``,
        ``size_range``, and ``website`` (the domain source for probing).
    """

    def _safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _safe_str(value: Any) -> Optional[str]:
        v = str(value).strip() if value is not None else ""
        return v if v else None

    return {
        "name": _safe_str(data.get("name")) or _safe_str(data.get("employer_name")),
        "glassdoor_rating": _safe_float(data.get("rating")),
        "glassdoor_url": _safe_str(data.get("company_link")),
        "industry": _safe_str(data.get("industry")),
        "size_range": _safe_str(data.get("size")),
        "website": _safe_str(data.get("website")),
        "review_count": data.get("review_count") or 0,
    }


def _resolve_career_url(
    company_name: str,
    db_connection: sqlite3.Connection,
    api_key: str,
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    """Look up the company on Glassdoor and probe for a career URL.

    Uses the Glassdoor RapidAPI endpoint exclusively via
    :func:`_fetch_glassdoor_data`.

    Steps:
        1. Check the pre-fetched Glassdoor cache (no API call required).
        2. Call the RapidAPI Glassdoor company-overview endpoint (budget
           permitting) if cache misses.
        3. Extract the ``website`` field from the Glassdoor response.
        4. Probe ``/careers`` and ``/jobs`` on the derived domain.

    Args:
        company_name: Display name of the company.
        db_connection: Open SQLite connection (for budget tracking).
        api_key: Value of the ``RAPIDAPI_KEY`` environment variable.

    Returns:
        A two-tuple ``(career_url, metadata)`` where:

        - ``career_url`` is the probed URL string, or ``None`` when
          probing fails or the Glassdoor response has no ``website``
          field.
        - ``metadata`` is the parsed Glassdoor metadata dict (see
          :func:`_parse_glassdoor_metadata`), or ``None`` when the API
          call fails entirely (budget exhausted, network error).

        Both elements may be ``None`` independently of each other.
    """
    # Consult the pre-fetched cache first (no budget cost).
    from pipeline.src.enrichment.glassdoor_rapidapi import _load_cache

    cache = _load_cache()
    cached_entry = cache.get(company_name)
    raw_data: Optional[dict[str, Any]] = None

    if cached_entry is not None and cached_entry.get("status") == "OK":
        logger.debug("Glassdoor cache hit for '%s'.", company_name)
        raw_data = cached_entry.get("data")
    else:
        raw_data = _fetch_glassdoor_data(company_name, api_key, db_connection)

    if raw_data is None:
        logger.warning(
            "Glassdoor lookup returned no data for '%s'; "
            "cannot derive career URL.",
            company_name,
        )
        return None, None

    metadata = _parse_glassdoor_metadata(raw_data)

    website = metadata.get("website")
    if not website:
        logger.info(
            "Glassdoor data for '%s' has no 'website' field; cannot probe career URL.",
            company_name,
        )
        return None, metadata

    domain = _extract_domain(website)
    if domain is None:
        logger.warning(
            "Could not parse domain from website '%s' for '%s'.",
            website,
            company_name,
        )
        return None, metadata

    career_url = _probe_career_url(domain)
    return career_url, metadata


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
        if not message.content:
            logger.warning("LLM returned empty content list")
            return None
        return message.content[0].text
    except Exception as exc:  # noqa: BLE001
        logger.warning("LLM call failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Phase 4 — DB persistence
# ---------------------------------------------------------------------------


def _upsert_company_row(
    db_connection: sqlite3.Connection,
    company_name: str,
    metadata: Optional[dict[str, Any]],
    career_url: Optional[str],
) -> int:
    """Create or update the company row with Glassdoor metadata.

    When a row for *company_name* already exists it is updated in-place
    with any non-null metadata values.  When no row exists a new one is
    inserted.

    Zero-rating companies (metadata present but ``glassdoor_rating`` is
    ``None`` or ``0`` and ``review_count`` is ``0``) still get a row;
    enrichment fields are left ``NULL``.

    Args:
        db_connection: Open SQLite connection.
        company_name: Display name of the company.
        metadata: Parsed Glassdoor metadata dict from
            :func:`_parse_glassdoor_metadata`, or ``None`` when no data
            was returned.
        career_url: Probed career page URL, or ``None``.

    Returns:
        Primary key of the (created or existing) company row.
    """
    existing = db_connection.execute(
        "SELECT id FROM companies WHERE name = ?", (company_name,)
    ).fetchone()

    if metadata is not None:
        rating = metadata.get("glassdoor_rating")
        review_count = metadata.get("review_count") or 0
        is_zero_rating = (rating is None or rating == 0) and review_count == 0

        if is_zero_rating:
            # Zero-rating: create row with name only; leave enrichment fields NULL.
            logger.info(
                "Glassdoor data for '%s' has zero rating and zero reviews; "
                "creating company row with name only.",
                company_name,
            )
            effective_metadata: Optional[dict[str, Any]] = None
        else:
            effective_metadata = metadata
    else:
        effective_metadata = None

    if existing is not None:
        company_id: int = (
            existing["id"] if hasattr(existing, "keys") else existing[0]
        )
        if effective_metadata is not None:
            db_connection.execute(
                """
                UPDATE companies
                SET glassdoor_rating = COALESCE(?, glassdoor_rating),
                    glassdoor_url    = COALESCE(?, glassdoor_url),
                    industry         = COALESCE(?, industry),
                    size_range       = COALESCE(?, size_range),
                    career_page_url  = COALESCE(?, career_page_url)
                WHERE id = ?
                """,
                (
                    effective_metadata.get("glassdoor_rating"),
                    effective_metadata.get("glassdoor_url"),
                    effective_metadata.get("industry"),
                    effective_metadata.get("size_range"),
                    career_url,
                    company_id,
                ),
            )
        elif career_url is not None:
            db_connection.execute(
                "UPDATE companies SET career_page_url = COALESCE(?, career_page_url) WHERE id = ?",
                (career_url, company_id),
            )
        db_connection.commit()
        return company_id

    # Insert a new row.
    if effective_metadata is not None:
        db_connection.execute(
            """
            INSERT INTO companies
                (name, glassdoor_rating, glassdoor_url, industry, size_range, career_page_url)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                company_name,
                effective_metadata.get("glassdoor_rating"),
                effective_metadata.get("glassdoor_url"),
                effective_metadata.get("industry"),
                effective_metadata.get("size_range"),
                career_url,
            ),
        )
    else:
        db_connection.execute(
            "INSERT INTO companies (name, career_page_url) VALUES (?, ?)",
            (company_name, career_url),
        )
    db_connection.commit()

    new_row = db_connection.execute(
        "SELECT id FROM companies WHERE name = ?", (company_name,)
    ).fetchone()
    return new_row["id"] if hasattr(new_row, "keys") else new_row[0]


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
) -> Optional[CompanyRecord]:
    """Discover and persist career page information for a company.

    Phases:
        1. Glassdoor lookup — query the RapidAPI Glassdoor endpoint to get
           company metadata and derive a career URL by probing the company
           website domain.  If ``career_url`` is already supplied, it is
           used directly without probing.
        2. Company row upsert — create or update the company row with
           Glassdoor metadata (name, glassdoor_rating, industry,
           size_range, glassdoor_url).  The row is created even when career
           URL probing fails (``career_page_url = NULL``).
        3. HTML fetch — download the career page (skipped when no career
           URL is available).
        4. LLM analysis — detect ATS platform or generate scrape strategy
           (skipped when HTML fetch is skipped).
        5. DB persistence — upsert career_page_configs; update
           companies.ats_platform (skipped when LLM analysis is skipped).

    Args:
        company_name: Display name of the company (e.g. ``"Acme Corp"``).
        db_connection: Open SQLite connection with WAL mode (from
            ``database.get_connection``).  Must have the ``companies`` and
            ``career_page_configs`` tables initialised.
        career_url: Optional known URL of the career page.  When ``None``,
            the Glassdoor API is used to find the company website and
            ``/careers`` / ``/jobs`` paths are probed.

    Returns:
        A :class:`CompanyRecord` whose ``company_id`` is always set when
        the company row was created or already existed.
        ``career_page_url`` is ``None`` when probing failed.

        Returns ``None`` only when:
        - ``RAPIDAPI_KEY`` is not set, AND the company does not already
          exist in the companies table.
        - The Glassdoor API budget is exhausted AND the company does not
          already exist.
        - An unexpected error prevents company row creation.
    """
    # --- Check if company row already exists ---
    existing_row = db_connection.execute(
        "SELECT id, career_page_url FROM companies WHERE name = ?",
        (company_name,),
    ).fetchone()

    if existing_row is not None:
        if hasattr(existing_row, "keys"):
            existing_id: int = existing_row["id"]
            existing_career_url: Optional[str] = existing_row["career_page_url"]
        else:
            existing_id = existing_row[0]
            existing_career_url = existing_row[1]

        if career_url is not None:
            # Caller supplied a URL (e.g. rediscover_broken) — skip the API
            # call (company already has metadata) but re-run HTML fetch, LLM,
            # and career_page_configs persistence with the supplied URL.
            logger.info(
                "discover_company: '%s' already exists (company_id=%d); "
                "skipping API, re-running HTML/LLM phases with supplied URL.",
                company_name,
                existing_id,
            )
            result = DiscoveryResult(
                company_name=company_name, career_url=career_url
            )
            html = _fetch_html(career_url)
            if html is None:
                return CompanyRecord(
                    company_id=existing_id, career_page_url=existing_career_url
                )
            result.html = html
            llm_response = _analyse_html(html)
            if llm_response is None:
                return CompanyRecord(
                    company_id=existing_id, career_page_url=existing_career_url
                )
            result.llm_response = llm_response
            _persist_discovery(result, db_connection, existing_id)
            return CompanyRecord(
                company_id=existing_id, career_page_url=career_url
            )
        else:
            # Fresh discovery: company row already exists — no API call needed.
            logger.info(
                "discover_company: '%s' already exists (company_id=%d); skipping API.",
                company_name,
                existing_id,
            )
            return CompanyRecord(
                company_id=existing_id,
                career_page_url=existing_career_url,
            )

    # --- Phase 1: Glassdoor lookup and career URL resolution ---
    api_key = os.environ.get("RAPIDAPI_KEY", "")
    if not api_key:
        logger.warning(
            "RAPIDAPI_KEY is not set; cannot discover '%s' via Glassdoor.",
            company_name,
        )
        return None

    if career_url is not None:
        # Caller supplied a URL (company is new) — do Glassdoor lookup for
        # metadata, but use the supplied URL rather than probing.
        from pipeline.src.enrichment.glassdoor_rapidapi import _load_cache

        cache = _load_cache()
        cached_entry = cache.get(company_name)
        if cached_entry is not None and cached_entry.get("status") == "OK":
            raw_data: Optional[dict[str, Any]] = cached_entry.get("data")
        else:
            raw_data = _fetch_glassdoor_data(company_name, api_key, db_connection)

        metadata: Optional[dict[str, Any]] = (
            _parse_glassdoor_metadata(raw_data) if raw_data is not None else None
        )
        resolved_url = career_url
    else:
        resolved_url, metadata = _resolve_career_url(
            company_name, db_connection, api_key
        )
        # metadata being None means a hard API failure (not zero-rating).
        if metadata is None:
            logger.warning(
                "Glassdoor lookup failed entirely for '%s'; no company row created.",
                company_name,
            )
            return None

    # --- Phase 2: Upsert company row with Glassdoor metadata ---
    try:
        company_id = _upsert_company_row(
            db_connection, company_name, metadata, resolved_url
        )
    except sqlite3.Error as exc:
        logger.error(
            "Failed to upsert company row for '%s': %s", company_name, exc
        )
        return None

    logger.info(
        "discover_company: company row upserted for '%s' (company_id=%d, career_url=%s).",
        company_name,
        company_id,
        resolved_url,
    )

    # When there is no career URL we cannot run phases 3-5; return early.
    if resolved_url is None:
        return CompanyRecord(company_id=company_id, career_page_url=None)

    result = DiscoveryResult(company_name=company_name, career_url=resolved_url)

    # --- Phase 3: HTML fetch ---
    html = _fetch_html(result.career_url)  # type: ignore[arg-type]
    if html is None:
        logger.warning(
            "Could not fetch HTML for '%s' at %s; career_page_configs not created.",
            company_name,
            result.career_url,
        )
        return CompanyRecord(company_id=company_id, career_page_url=resolved_url)
    result.html = html

    # --- Phase 4: LLM analysis ---
    llm_response = _analyse_html(html)
    if llm_response is None:
        logger.warning(
            "LLM analysis returned no result for '%s'; career_page_configs not created.",
            company_name,
        )
        return CompanyRecord(company_id=company_id, career_page_url=resolved_url)
    result.llm_response = llm_response

    # --- Phase 5: DB persistence (career_page_configs) ---
    _persist_discovery(result, db_connection, company_id)

    return CompanyRecord(company_id=company_id, career_page_url=resolved_url)


def rediscover_broken(db_connection: sqlite3.Connection) -> dict[str, int]:
    """Re-run discovery for all career_page_configs rows with status='broken'.

    For each broken config, calls :func:`discover_company` with the config's
    existing URL (bypassing Glassdoor probing).  On success, the config status
    becomes 'active' (persisted by :func:`discover_company`).  On failure the
    config remains 'broken'.

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
            existing_url: str = row["url"]
        else:
            company_name = row[3]
            existing_url = row[2]

        logger.info(
            "Re-running discovery for broken config: company='%s', url='%s'",
            company_name,
            existing_url,
        )

        record = discover_company(
            company_name=company_name,
            db_connection=db_connection,
            career_url=existing_url,
        )

        if record is not None and record.career_page_url is not None:
            stats["recovered"] += 1
            logger.info("Recovered broken config for '%s'.", company_name)
        else:
            stats["still_broken"] += 1
            logger.warning(
                "Discovery still failing for '%s'; config remains broken.",
                company_name,
            )

    logger.info(
        "rediscover_broken: attempted=%d, recovered=%d, still_broken=%d",
        stats["attempted"],
        stats["recovered"],
        stats["still_broken"],
    )
    return stats

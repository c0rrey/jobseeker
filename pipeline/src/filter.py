"""
Filter — remove jobs that don't meet requirements.

``filter_jobs`` applies deterministic filters to an in-memory list of
:class:`~pipeline.src.models.Job` objects; ``run_prefilter`` performs the
same filtering against unscored rows in the SQLite database, marking
rejected jobs with a sentinel row so downstream scoring stages skip them.
"""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from pipeline.config.settings import load_profile, load_red_flags

from .models import Job

# ---------------------------------------------------------------------------
# Static reference data for location filtering
# ---------------------------------------------------------------------------

# Maps US state two-letter abbreviations to lowercase full names.
# Only the states likely to appear in preferred_locations need to be here;
# extend as new target metros are added.
_STATE_ABBR_TO_NAME: dict[str, str] = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas",
    "CA": "california", "CO": "colorado", "CT": "connecticut", "DE": "delaware",
    "FL": "florida", "GA": "georgia", "HI": "hawaii", "ID": "idaho",
    "IL": "illinois", "IN": "indiana", "IA": "iowa", "KS": "kansas",
    "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland",
    "MA": "massachusetts", "MI": "michigan", "MN": "minnesota", "MS": "mississippi",
    "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new hampshire", "NJ": "new jersey", "NM": "new mexico", "NY": "new york",
    "NC": "north carolina", "ND": "north dakota", "OH": "ohio", "OK": "oklahoma",
    "OR": "oregon", "PA": "pennsylvania", "RI": "rhode island", "SC": "south carolina",
    "SD": "south dakota", "TN": "tennessee", "TX": "texas", "UT": "utah",
    "VT": "vermont", "VA": "virginia", "WA": "washington", "WV": "west virginia",
    "WI": "wisconsin", "WY": "wyoming", "DC": "district of columbia",
}

# Maps lowercase city names to a list of lowercase county-name synonyms.
# Adzuna often returns locations like "Tampa, Hillsborough County" instead of
# "Tampa, FL".  When a preferred_locations entry includes a city, we also
# accept any job whose location contains a known county for that city.
# Add entries here when targeting new metros that use county-style locations.
_CITY_COUNTY_SYNONYMS: dict[str, list[str]] = {
    "tampa": ["hillsborough"],
    "orlando": ["orange", "osceola", "seminole"],
    "miami": ["miami-dade"],
    "fort lauderdale": ["broward"],
    "jacksonville": ["duval"],
    "st. petersburg": ["pinellas"],
    "clearwater": ["pinellas"],
    "sarasota": ["sarasota"],
    "naples": ["collier"],
    "fort myers": ["lee"],
    "seattle": ["king county"],
    "bellevue": ["king county"],
    "redmond": ["king county"],
    "denver": ["denver"],
    "austin": ["travis"],
    "portland": ["multnomah"],
    "chicago": ["cook"],
    "new york": ["new york", "brooklyn", "queens", "bronx", "staten island"],
    "los angeles": ["los angeles"],
    "san francisco": ["san francisco"],
    "san jose": ["santa clara"],
}

logger = logging.getLogger(__name__)


def meets_salary_requirement(job: Job, min_salary: int) -> bool:
    """
    Return True if the job meets the minimum salary requirement.
    
    For jobs with a salary range, we check against the maximum salary
    (the upper end of what you could potentially negotiate).
    
    Accepts Adzuna salary estimates (jobs with only min OR max salary).
    
    Args:
        job: The job to check.
        min_salary: Minimum required salary.
        
    Returns:
        True if job meets salary requirement.
    """
    # Reject jobs with absolutely no salary information
    if (not job.salary_min or job.salary_min == 0) and (not job.salary_max or job.salary_max == 0):
        return False
    
    # Check maximum salary first (best case scenario or estimated salary)
    if job.salary_max and job.salary_max > 0 and job.salary_max >= min_salary:
        return True
    
    # If no max (or max is 0), check minimum
    if job.salary_min and job.salary_min > 0 and job.salary_min >= min_salary:
        return True
    
    return False


def matches_title_keywords(job: Job, title_keywords: list[str]) -> bool:
    """
    Return True if the job title contains any of the target keywords.
    
    This ensures we only get jobs with relevant titles, not just jobs where
    keywords appear somewhere in the description.
    
    Args:
        job: The job to check.
        title_keywords: List of keywords from profile (e.g., "data engineer", "product analyst")
        
    Returns:
        True if job title matches any keyword.
    """
    if not job.title or not title_keywords:
        return False
    
    title_lower = job.title.lower()
    
    # Check if any keyword appears in the title
    # For multi-word keywords, check if all words appear (not necessarily adjacent)
    for keyword in title_keywords:
        keyword_words = keyword.lower().split()
        if all(word in title_lower for word in keyword_words):
            return True
    
    return False


def is_intern_role(job: Job) -> bool:
    """
    Return True if the job appears to be an intern/internship role.
    
    Args:
        job: The job to check.
        
    Returns:
        True if this is an intern role (should be filtered out).
    """
    if not job.title:
        return False
    
    title_lower = job.title.lower()
    intern_keywords = ["intern", "internship", "co-op", "coop"]
    
    return any(keyword in title_lower for keyword in intern_keywords)


def is_non_ic_role(job: Job) -> bool:
    """Return True if the job is a management/leadership role, not IC."""
    if not job.title:
        return False

    title_lower = job.title.lower()
    non_ic_keywords = [
        "manager", "director", "vice president", "vp ", "vp,",
        "head of", "chief", "svp", "evp",
    ]

    return any(keyword in title_lower for keyword in non_ic_keywords)


def is_too_old(job: Job, max_age_days: int = 90) -> bool:
    """
    Return True if the job posting is older than max_age_days.
    
    Args:
        job: The job to check.
        max_age_days: Maximum age in days (from profile).
        
    Returns:
        True if job is too old (should be filtered out).
    """
    if not job.posted_at:
        # If no date, assume it's recent and keep it
        return False
    
    try:
        # Parse ISO format: "2026-01-05T10:41:12Z" or date-only "2026-03-01"
        posted_date = datetime.fromisoformat(job.posted_at.replace("Z", "+00:00"))
        if posted_date.tzinfo is None:
            posted_date = posted_date.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - posted_date).days
        return age_days > max_age_days
    except (ValueError, AttributeError):
        # If we can't parse the date, keep the job
        return False


def has_red_flags(job: Job, red_flags: dict | None = None) -> bool:
    """
    Return True if the job contains red-flag keywords or phrases.

    Args:
        job: The job to check.
        red_flags: Optional dict from load_red_flags(). Loaded if not provided.
    """
    if red_flags is None:
        red_flags = load_red_flags()

    text = f"{job.title or ''} {job.description or ''}".lower()
    phrases = red_flags.get("phrases", [])
    keywords = red_flags.get("keywords", [])

    for phrase in phrases:
        if phrase.lower() in text:
            return True
    for keyword in keywords:
        if keyword.lower() in text:
            return True
    return False


def is_allowed_location(job: Job, preferred_locations: list[str] | None = None) -> bool:
    """Return True if the job is in an allowed location.

    Derives the allowlist dynamically from ``preferred_locations``, which
    should come from the ``preferred_locations`` key in ``profile.yaml``.
    If ``preferred_locations`` is ``None`` or empty, all locations are
    accepted (fail-open behaviour for callers that haven't loaded the
    profile yet).

    Permanent pass-through rules (always True, regardless of
    ``preferred_locations``):

    - Job has no location field (null/empty).
    - Location contains a remote-indicating keyword ("remote", "work from
      home", "wfh", "telecommute", "anywhere").
    - Location normalises to one of "us", "usa", "united states".

    For each ``"City, ST"`` entry in ``preferred_locations`` the function
    accepts a job whose location contains:

    - The city name (case-insensitive substring),
    - The state abbreviation (as " ST", ",ST", or at the end of the
      string), or
    - The full state name, or
    - A known county synonym for that city (see ``_CITY_COUNTY_SYNONYMS``).

    Entries whose value is a remote-indicating keyword are skipped (the
    universal remote check above already handles them).

    Args:
        job: The job to check.
        preferred_locations: List of location strings from profile (e.g.
            ``["Tampa, FL", "Seattle, WA", "Remote"]``).  ``None`` means
            all locations are allowed.

    Returns:
        True if location is allowed, False otherwise.
    """
    if not job.location:
        # No location specified — could be remote, so allow it.
        return True

    location_lower = job.location.lower()

    # Universal: remote-indicating keywords are always allowed.
    remote_keywords = ["remote", "work from home", "wfh", "telecommute", "anywhere"]
    if any(keyword in location_lower for keyword in remote_keywords):
        return True

    # Universal: bare US / USA / United States indicates nationwide posting.
    if location_lower.strip() in ["us", "usa", "united states"]:
        return True

    # If no preferred_locations configured, fail-open — allow everything.
    if not preferred_locations:
        return True

    for entry in preferred_locations:
        entry_lower = entry.lower().strip()

        # Skip remote-sentinel entries; the universal check above covers them.
        if any(kw in entry_lower for kw in remote_keywords):
            continue

        # Split on last comma to handle "St. Petersburg, FL" or "New York, NY".
        if "," in entry_lower:
            city_lower = entry_lower.rsplit(",", 1)[0].strip()
            state_abbr = entry_lower.rsplit(",", 1)[1].strip().upper()
        else:
            # Entry has no comma — treat the whole thing as a city name.
            city_lower = entry_lower
            state_abbr = ""

        state_full = _STATE_ABBR_TO_NAME.get(state_abbr, "")

        # 1. City name match (substring).
        if city_lower and city_lower in location_lower:
            return True

        # 2. State abbreviation match — must appear as a token, not a substring
        #    (avoids "wa" matching "iowa", etc.).
        if state_abbr:
            abbr_lower = state_abbr.lower()
            if (
                location_lower.endswith(f" {abbr_lower}")
                or location_lower.endswith(f",{abbr_lower}")
                or f" {abbr_lower}," in location_lower
                or f",{abbr_lower}," in location_lower
                or f" {abbr_lower} " in location_lower
            ):
                return True

        # 3. Full state name match.
        if state_full and state_full in location_lower:
            return True

        # 4. County synonyms for this city.
        for county in _CITY_COUNTY_SYNONYMS.get(city_lower, []):
            if county in location_lower:
                return True

    return False


def filter_jobs(jobs: list[Job]) -> list[Job]:
    """
    Remove jobs that don't meet requirements.
    
    Applies filters in order:
    1. Title keywords (must match target roles)
    2. Salary minimum (from profile)
    3. Intern roles (remove)
    4. Job age (remove old postings)
    5. Red flags (remove problematic keywords)
    6. Location (keep only remote or Florida)
    
    Args:
        jobs: List of jobs to filter.
        
    Returns:
        Filtered list of jobs.
    """
    profile = load_profile()
    red_flags = load_red_flags()
    max_age_days = profile.get("max_job_age_days", 90)
    # Fallback chain: salary_min (hard floor) -> salary_target -> 100000.
    # salary_min is the actual rejection threshold; salary_target is a softer
    # preference used by LLM scoring.  Existing sentinel rows in score_dimensions
    # are NOT retroactively updated when this value changes.
    min_salary = profile.get("salary_min") or profile.get("salary_target", 100000)
    title_keywords = profile.get("title_keywords", [])
    preferred_locations: list[str] = profile.get("preferred_locations", [])

    initial_count = len(jobs)

    # Filter 1: Title keywords (must match target roles)
    jobs = [j for j in jobs if matches_title_keywords(j, title_keywords)]
    logger.info("After title keyword filter: %s/%s jobs", len(jobs), initial_count)

    # Filter 2: Salary minimum
    jobs = [j for j in jobs if meets_salary_requirement(j, min_salary)]
    logger.info("After salary filter (>=$%s): %s/%s jobs", f"{min_salary:,}", len(jobs), initial_count)

    # Filter 3: Remove intern roles
    jobs = [j for j in jobs if not is_intern_role(j)]
    logger.info("After intern filter: %s/%s jobs", len(jobs), initial_count)

    # Filter 4: Remove old postings
    jobs = [j for j in jobs if not is_too_old(j, max_age_days)]
    logger.info("After age filter (%s days): %s/%s jobs", max_age_days, len(jobs), initial_count)

    # Filter 5: Remove red flags
    jobs = [j for j in jobs if not has_red_flags(j, red_flags)]
    logger.info("After red flag filter: %s/%s jobs", len(jobs), initial_count)

    # Filter 6: Location filtering — derived from preferred_locations in profile
    jobs = [j for j in jobs if is_allowed_location(j, preferred_locations)]
    logger.info("After location filter: %s/%s jobs", len(jobs), initial_count)

    return jobs


def _row_to_job(row: sqlite3.Row) -> Job:
    """Convert a sqlite3.Row from the jobs table into a Job dataclass.

    Args:
        row: A row from ``SELECT * FROM jobs``.

    Returns:
        A Job dataclass populated from the row fields.
    """
    return Job(
        title=row["title"],
        company=row["company"],
        url=row["url"],
        source=row["source"],
        source_type=row["source_type"],
        description=row["description"],
        location=row["location"],
        salary_min=row["salary_min"],
        salary_max=row["salary_max"],
        posted_at=row["posted_at"],
        db_id=row["id"],
        company_id=row["company_id"],
        ats_platform=row["ats_platform"],
        dedup_hash=row["dedup_hash"],
        external_id=row["external_id"],
        raw_json=row["raw_json"],
    )


def run_prefilter(db_connection: sqlite3.Connection) -> dict[str, int]:
    """Read unscored jobs from the database and mark pre-filtered jobs.

    Queries jobs that have no entry in ``score_dimensions`` (i.e. have not
    been processed by any pipeline stage yet) and applies four deterministic
    filters in order:

    1. Red flag keyword/phrase check (``has_red_flags``)
    2. Salary minimum check (``meets_salary_requirement``)
    3. Intern/internship/co-op title check (``is_intern_role``)
    4. Posting age check (``is_too_old``)

    Jobs that fail any filter are marked by inserting a sentinel row into
    ``score_dimensions`` with ``pass=0`` and ``overall=-1``.  Downstream
    stages skip jobs where such a sentinel exists.

    Jobs that pass all filters are left untouched so the scoring stages
    process them normally.

    Args:
        db_connection: An open SQLite connection with ``row_factory`` set to
            ``sqlite3.Row`` (as returned by
            ``pipeline.src.database.get_connection``).

    Returns:
        A dict with keys ``"examined"``, ``"filtered"``, and ``"passed"``
        reporting how many jobs were processed and what the outcome was.
    """
    profile = load_profile()
    red_flags = load_red_flags()
    max_age_days: int = profile.get("max_job_age_days", 90)
    # Fallback chain: salary_min (hard floor) -> salary_target -> 100000.
    # salary_min is the actual rejection threshold; salary_target is a softer
    # preference used by LLM scoring.  Existing sentinel rows in score_dimensions
    # are NOT retroactively updated when this value changes.
    min_salary: int = profile.get("salary_min") or profile.get("salary_target", 100000)
    preferred_locations: list[str] = profile.get("preferred_locations", [])

    # Fetch jobs with no score_dimensions entry of any pass.
    unscored_rows = db_connection.execute(
        """
        SELECT j.*
        FROM jobs j
        LEFT JOIN score_dimensions sd ON sd.job_id = j.id
        WHERE sd.job_id IS NULL
        """
    ).fetchall()

    examined = len(unscored_rows)
    filtered = 0
    passed = 0

    for row in unscored_rows:
        job = _row_to_job(row)

        reason: Optional[str] = None
        if has_red_flags(job, red_flags):
            reason = "red_flag"
        elif not meets_salary_requirement(job, min_salary):
            reason = "salary"
        elif is_intern_role(job):
            reason = "intern"
        elif is_non_ic_role(job):
            reason = "non_ic"
        elif is_too_old(job, max_age_days):
            reason = "too_old"
        elif not is_allowed_location(job, preferred_locations):
            reason = "location"

        if reason is not None:
            db_connection.execute(
                """
                INSERT OR IGNORE INTO score_dimensions (job_id, pass, overall, reasoning)
                VALUES (?, 0, -1, ?)
                """,
                (job.db_id, reason),
            )
            filtered += 1
        else:
            passed += 1

    db_connection.commit()

    return {"examined": examined, "filtered": filtered, "passed": passed}

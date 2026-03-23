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


def is_allowed_location(job: Job) -> bool:
    """
    Return True if the job is in an allowed location.

    Allowed locations:
    - Remote (anywhere in the US)
    - Any location in Florida (including by county name)
    - Seattle metro area / Washington state (King County)

    The LLM scorer will consider location preference (Tampa/Orlando/Seattle) as part of the match score.

    Args:
        job: The job to check.

    Returns:
        True if location is allowed, False otherwise.
    """
    if not job.location:
        # No location specified - could be remote, so allow it
        return True

    location_lower = job.location.lower()

    # Check for remote
    remote_keywords = ["remote", "work from home", "wfh", "telecommute", "anywhere"]
    if any(keyword in location_lower for keyword in remote_keywords):
        return True

    # Check if location is just "US" or "USA" (indicates remote/nationwide)
    if location_lower.strip() in ["us", "usa", "united states"]:
        return True

    # Check for Florida - state name or abbreviation
    florida_keywords = ["florida", " fl ", " fl,", ",fl"]
    if location_lower.endswith(" fl") or location_lower.endswith(",fl"):
        return True
    if any(keyword in location_lower for keyword in florida_keywords):
        return True

    # Check for Florida counties (Adzuna often returns locations like "Tampa, Hillsborough County")
    florida_counties = [
        "miami-dade", "broward", "palm beach", "hillsborough", "orange",
        "pinellas", "duval", "lee", "polk", "brevard", "volusia", "pasco",
        "seminole", "sarasota", "manatee", "collier", "escambia", "osceola",
        "marion", "st. lucie", "lake", "hernando", "charlotte", "alachua"
    ]
    if any(county in location_lower for county in florida_counties):
        return True

    # Check for Seattle metro area — Adzuna returns "Seattle, King County",
    # "Bellevue, King County", "Redmond, King County", "Seattle, WA", etc.
    seattle_keywords = ["seattle", ", wa", " wa,"]
    if location_lower.endswith(" wa") or location_lower.endswith(",wa"):
        return True
    if any(keyword in location_lower for keyword in seattle_keywords):
        return True
    if "king county" in location_lower:
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

    # Filter 6: Location filtering
    jobs = [j for j in jobs if is_allowed_location(j)]
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
        elif not is_allowed_location(job):
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

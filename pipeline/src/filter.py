"""
Filter — remove jobs that don't meet requirements.

Applies multiple filters:
1. Red flags (keywords/phrases to avoid)
2. Location restrictions (remote or Tampa/Orlando area only)
3. Job age (filter out old postings)
4. Title restrictions (no intern roles)
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root so we can import config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import load_profile, load_red_flags

from .models import Job


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
        # Parse ISO format: "2026-01-05T10:41:12Z"
        posted_date = datetime.fromisoformat(job.posted_at.replace("Z", "+00:00"))
        age_days = (datetime.now(posted_date.tzinfo) - posted_date).days
        return age_days > max_age_days
    except (ValueError, AttributeError):
        # If we can't parse the date, keep the job
        return False


def should_filter(job: Job, red_flags: dict | None = None) -> bool:
    """
    Return True if the job should be filtered out (has red flags).

    Args:
        job: The job to check.
        red_flags: Optional dict from load_red_flags(). Loaded if not provided.
    """
    if red_flags is None:
        red_flags = load_red_flags()

    text = f"{job.title} {job.description}".lower()
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
    
    The LLM scorer will consider location preference (Tampa/Orlando) as part of the match score.
    
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
    min_salary = profile.get("salary_min", 100000)
    title_keywords = profile.get("title_keywords", [])
    
    initial_count = len(jobs)
    
    # Filter 1: Title keywords (must match target roles)
    jobs = [j for j in jobs if matches_title_keywords(j, title_keywords)]
    print(f"  After title keyword filter: {len(jobs)}/{initial_count} jobs")
    
    # Filter 2: Salary minimum
    jobs = [j for j in jobs if meets_salary_requirement(j, min_salary)]
    print(f"  After salary filter (>=${min_salary:,}): {len(jobs)}/{initial_count} jobs")
    
    # Filter 3: Remove intern roles
    jobs = [j for j in jobs if not is_intern_role(j)]
    print(f"  After intern filter: {len(jobs)}/{initial_count} jobs")
    
    # Filter 4: Remove old postings
    jobs = [j for j in jobs if not is_too_old(j, max_age_days)]
    print(f"  After age filter ({max_age_days} days): {len(jobs)}/{initial_count} jobs")
    
    # Filter 5: Remove red flags
    jobs = [j for j in jobs if not should_filter(j, red_flags)]
    print(f"  After red flag filter: {len(jobs)}/{initial_count} jobs")
    
    # Filter 6: Location filtering
    jobs = [j for j in jobs if is_allowed_location(j)]
    print(f"  After location filter: {len(jobs)}/{initial_count} jobs")
    
    return jobs

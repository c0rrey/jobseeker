"""
Normalizer — convert raw fetcher output to the common Job schema.

Each fetcher returns data in its own format. This module maps those
formats into the Job dataclass so the rest of the pipeline is
source-agnostic.
"""

import json

from .models import Job


def normalize_mock(raw: dict) -> Job:
    """Convert mock fetcher output to Job."""
    return Job(
        title=raw["title"],
        company=raw["company"],
        url=raw["url"],
        description=raw["description"],
        source="mock",
        location=raw.get("location"),
        raw_json=json.dumps(raw),
    )


def normalize_adzuna(raw: dict) -> Job:
    """
    Convert Adzuna API response to Job.
    
    Adzuna response structure:
    {
        "id": "12345",
        "title": "Senior Data Engineer",
        "company": {"display_name": "Example Corp"},
        "location": {"display_name": "San Francisco, CA"},
        "description": "Job description...",
        "redirect_url": "https://...",
        "salary_min": 100000,
        "salary_max": 150000,
        "created": "2024-01-15T12:00:00Z"
    }
    """
    return Job(
        title=raw.get("title", ""),
        company=raw.get("company", {}).get("display_name", "Unknown"),
        url=raw.get("redirect_url", ""),
        description=raw.get("description", ""),
        source="adzuna",
        location=raw.get("location", {}).get("display_name"),
        salary_min=raw.get("salary_min"),
        salary_max=raw.get("salary_max"),
        posted_at=raw.get("created"),
        raw_json=json.dumps(raw),
    )


def normalize_remoteok(raw: dict) -> Job:
    """
    Convert RemoteOK API response to Job.
    
    RemoteOK response structure:
    {
        "slug": "job-slug",
        "id": "12345",
        "epoch": 1234567890,
        "date": "2024-01-15T12:00:00Z",
        "company": "Example Corp",
        "position": "Senior Data Engineer",
        "tags": ["python", "sql", "remote"],
        "description": "<p>HTML description...</p>",
        "location": "Worldwide" or "USA",
        "apply_url": "https://...",
        "url": "https://remoteok.com/...",
        "salary_min": 100000,
        "salary_max": 150000
    }
    """
    return Job(
        title=raw.get("position", ""),
        company=raw.get("company", "Unknown"),
        url=raw.get("apply_url") or raw.get("url", ""),
        description=raw.get("description", ""),
        source="remoteok",
        location=raw.get("location"),
        salary_min=raw.get("salary_min") if raw.get("salary_min", 0) > 0 else None,
        salary_max=raw.get("salary_max") if raw.get("salary_max", 0) > 0 else None,
        posted_at=raw.get("date"),
        raw_json=json.dumps(raw),
    )


def normalize_linkedin(raw: dict) -> Job:
    """
    Convert a RapidAPI LinkedIn job dict to a V2 Job.

    RapidAPI LinkedIn Jobs Search response structure (representative):
    {
        "job_id":          "3812345678",
        "job_url":         "https://www.linkedin.com/jobs/view/3812345678",
        "job_title":       "Senior Data Engineer",
        "company_name":    "Acme Corp",
        "job_location":    "Miami, FL (Remote)",
        "job_description": "We are looking for ...",
        "job_posted_at":   "2026-03-01",
        "min_salary":      140000,
        "max_salary":      180000
    }
    """
    return Job(
        title=raw.get("job_title", ""),
        company=raw.get("company_name", "Unknown"),
        url=raw.get("job_url", ""),
        description=raw.get("job_description"),
        source="linkedin",
        source_type="api",
        location=raw.get("job_location"),
        salary_min=raw.get("min_salary"),
        salary_max=raw.get("max_salary"),
        posted_at=raw.get("job_posted_at"),
        external_id=raw.get("job_id"),
        raw_json=json.dumps(raw),
    )


def normalize_greenhouse(raw: dict) -> Job:
    """
    Convert a Greenhouse job board API response dict to a V2 Job.

    Greenhouse boards-api response structure:
    {
        "id":          12345,
        "title":       "Data Engineer",
        "updated_at":  "2026-03-01T08:00:00.000Z",
        "location":    {"name": "Remote"},
        "absolute_url":"https://boards.greenhouse.io/acme/jobs/12345",
        "metadata":    [...],
        "_ats_platform":  "greenhouse",   // injected by ATSFetcher
        "_company_name":  "Acme Corp"     // injected by ATSFetcher
    }
    """
    return Job(
        title=raw.get("title", ""),
        company=raw.get("_company_name", "Unknown"),
        url=raw.get("absolute_url", ""),
        description=None,  # Greenhouse boards list endpoint omits description
        source="greenhouse",
        source_type="ats_feed",
        location=raw.get("location", {}).get("name") if isinstance(raw.get("location"), dict) else raw.get("location"),
        posted_at=raw.get("updated_at"),
        ats_platform="greenhouse",
        external_id=str(raw["id"]) if raw.get("id") is not None else None,
        raw_json=json.dumps(raw),
    )


def normalize_lever(raw: dict) -> Job:
    """
    Convert a Lever postings API response dict to a V2 Job.

    Lever postings API response structure:
    {
        "id":          "abc-123-def",
        "text":        "Data Engineer",
        "createdAt":   1709289600000,
        "categories":  {"location": "Remote", "team": "Engineering"},
        "hostedUrl":   "https://jobs.lever.co/acme/abc-123-def",
        "_ats_platform":  "lever",        // injected by ATSFetcher
        "_company_name":  "Acme Corp"     // injected by ATSFetcher
    }
    """
    created_ms = raw.get("createdAt")
    posted_at = None
    if created_ms:
        try:
            from datetime import datetime, timezone
            posted_at = datetime.fromtimestamp(
                created_ms / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (TypeError, ValueError, OSError):
            posted_at = None

    categories = raw.get("categories", {}) if isinstance(raw.get("categories"), dict) else {}
    return Job(
        title=raw.get("text", ""),
        company=raw.get("_company_name", "Unknown"),
        url=raw.get("hostedUrl", ""),
        description=None,
        source="lever",
        source_type="ats_feed",
        location=categories.get("location"),
        posted_at=posted_at,
        ats_platform="lever",
        external_id=raw.get("id"),
        raw_json=json.dumps(raw),
    )


def normalize_ashby(raw: dict) -> Job:
    """
    Convert an Ashby job board API response dict to a V2 Job.

    Ashby posting-api response structure:
    {
        "id":              "abc-123",
        "title":           "Data Engineer",
        "publishedDate":   "2026-03-01T00:00:00.000Z",
        "jobUrl":          "https://jobs.ashbyhq.com/acme/abc-123",
        "locationName":    "Remote",
        "_ats_platform":   "ashby",       // injected by ATSFetcher
        "_company_name":   "Acme Corp"    // injected by ATSFetcher
    }
    """
    return Job(
        title=raw.get("title", ""),
        company=raw.get("_company_name", "Unknown"),
        url=raw.get("jobUrl", ""),
        description=None,
        source="ashby",
        source_type="ats_feed",
        location=raw.get("locationName"),
        posted_at=raw.get("publishedDate"),
        ats_platform="ashby",
        external_id=raw.get("id"),
        raw_json=json.dumps(raw),
    )


def normalize_career_page(raw: dict) -> Job:
    """
    Convert a CareerPageFetcher-extracted dict to a V2 Job.

    CareerPageFetcher produces:
    {
        "title":                    "Data Engineer",
        "url":                      "https://careers.acme.com/jobs/123",
        "location":                 "Remote",              // optional
        "description":              "Build data pipelines",// optional
        "source":                   "career_page",
        "_career_page_config_id":   7                      // for tracing
    }
    """
    return Job(
        title=raw.get("title", ""),
        company=raw.get("company", "Unknown"),
        url=raw.get("url", ""),
        description=raw.get("description"),
        source="career_page",
        source_type="career_page",
        location=raw.get("location"),
        raw_json=json.dumps(raw),
    )


def normalize(raw_list: list[dict], source: str) -> list[Job]:
    """
    Normalize a list of raw job dicts to Job objects.

    Args:
        raw_list: Raw output from a fetcher.
        source: Identifier for the fetcher (e.g. "mock", "adzuna").

    Returns:
        List of Job instances.
    """
    if source == "mock":
        return [normalize_mock(r) for r in raw_list]
    elif source == "adzuna":
        return [normalize_adzuna(r) for r in raw_list]
    elif source == "remoteok":
        return [normalize_remoteok(r) for r in raw_list]
    elif source == "linkedin":
        return [normalize_linkedin(r) for r in raw_list]
    elif source == "greenhouse":
        return [normalize_greenhouse(r) for r in raw_list]
    elif source == "lever":
        return [normalize_lever(r) for r in raw_list]
    elif source == "ashby":
        return [normalize_ashby(r) for r in raw_list]
    elif source == "career_page":
        return [normalize_career_page(r) for r in raw_list]

    raise ValueError(f"Unknown source: {source}")

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
    
    raise ValueError(f"Unknown source: {source}")

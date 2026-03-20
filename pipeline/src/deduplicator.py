"""
Deduplicator — track seen jobs and filter out duplicates.

Uses a content hash (title + company + url) to identify jobs.
Stores seen hashes in data/seen_jobs.json so we don't email
the same posting twice.
"""

import hashlib
import json
from pathlib import Path

from .models import Job

# Default path for the seen-jobs store
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SEEN_JOBS_PATH = DATA_DIR / "seen_jobs.json"


def _job_hash(job: Job) -> str:
    """Compute a stable hash for deduplication."""
    content = f"{job.title}|{job.company}|{job.url}"
    return hashlib.sha256(content.encode()).hexdigest()


def load_seen_hashes() -> set[str]:
    """Load the set of previously seen job hashes."""
    if not SEEN_JOBS_PATH.exists():
        return set()
    with SEEN_JOBS_PATH.open() as f:
        data = json.load(f)
    return set(data.get("hashes", []))


def save_seen_hashes(hashes: set[str]) -> None:
    """Persist seen job hashes to disk."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with SEEN_JOBS_PATH.open("w") as f:
        json.dump({"hashes": list(hashes)}, f, indent=2)


def filter_new_only(jobs: list[Job]) -> list[Job]:
    """
    Return only jobs we haven't seen before.

    Updates the seen-jobs store with any new job hashes.
    """
    seen = load_seen_hashes()
    new_jobs = []
    for job in jobs:
        h = _job_hash(job)
        if h not in seen:
            seen.add(h)
            new_jobs.append(job)
    save_seen_hashes(seen)
    return new_jobs

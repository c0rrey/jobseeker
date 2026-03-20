"""
Shared data models for the job digest pipeline.

The Job dataclass is the common schema all fetchers normalize to.
This keeps the rest of the pipeline source-agnostic.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Job:
    """
    Normalized job posting used throughout the pipeline.

    All fetchers convert their raw format to this schema.
    Optional fields may be None when a source doesn't provide them.
    """

    title: str
    company: str
    url: str
    description: str
    source: str  # e.g. "adzuna", "mock"

    # Optional fields — some sources don't provide these
    location: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    posted_at: Optional[str] = None
    
    @property
    def posted_date(self) -> Optional[str]:
        """Alias for posted_at for compatibility."""
        return self.posted_at

    # Filled by the matcher; 0-100 score
    match_score: Optional[int] = None
    
    # LLM reasoning for the score (when using LLM scorer)
    match_reasoning: Optional[str] = None
    
    # Database ID (when loaded from database)
    db_id: Optional[int] = None

    # Raw data from the source (for debugging or LLM context)
    raw: Optional[dict] = field(default=None, repr=False)

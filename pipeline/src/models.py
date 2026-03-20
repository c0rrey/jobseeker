"""
Shared data models for the job digest pipeline.

The Job dataclass is the common schema all fetchers normalize to.
This keeps the rest of the pipeline source-agnostic.

V2 additions: Company, ScoreDimension, Feedback, ProfileSnapshot,
CareerPageConfig, and ProfileSuggestion dataclasses matching the V2
SQLite schema exactly.
"""

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# V1 core model (extended for V2)
# ---------------------------------------------------------------------------


@dataclass
class Job:
    """
    Normalized job posting used throughout the pipeline.

    All fetchers convert their raw format to this schema.
    Optional fields may be None when a source doesn't provide them.

    V2 additions:
        source_type: Broad category of the fetch mechanism.
        company_id: FK to the companies table once matched/created.
        ats_platform: ATS name when sourced from a structured ATS feed.
        dedup_hash: Title + company fingerprint for fuzzy cross-source dedup.
        last_seen_at: ISO-8601 timestamp updated when a URL re-appears.
        external_id: Source-specific job identifier.
    """

    # --- Required fields ---
    title: str
    company: str
    url: str
    description: str
    source: str  # e.g. "adzuna", "remoteok", "career_page"
    source_type: str  # e.g. "api", "career_page", "ats_feed"

    # --- Optional location / compensation ---
    location: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    posted_at: Optional[str] = None

    # --- V1 compatibility ---
    @property
    def posted_date(self) -> Optional[str]:
        """Alias for posted_at for compatibility."""
        return self.posted_at

    # --- Scoring (filled by matcher/subagent) ---
    match_score: Optional[int] = None
    match_reasoning: Optional[str] = None

    # --- Database identity ---
    db_id: Optional[int] = None

    # --- V2 company linkage and source metadata ---
    company_id: Optional[int] = None
    ats_platform: Optional[str] = None       # 'greenhouse', 'lever', 'workday', 'ashby'
    dedup_hash: Optional[str] = None          # title + company fingerprint
    last_seen_at: Optional[str] = None        # ISO-8601; updated on re-fetch
    external_id: Optional[str] = None         # source-specific job ID

    # --- Raw data (debugging / LLM context) ---
    raw: Optional[dict] = field(default=None, repr=False)


# ---------------------------------------------------------------------------
# V2 entities
# ---------------------------------------------------------------------------


@dataclass
class Company:
    """
    Company record, matching the ``companies`` table schema.

    Fields with database defaults (``is_target``, ``created_at``) are
    Optional here because Python-side objects may be created before the
    DB row exists and those values are assigned by SQLite.
    """

    name: str

    # DB surrogate key; None until persisted
    id: Optional[int] = None

    domain: Optional[str] = None
    career_page_url: Optional[str] = None
    ats_platform: Optional[str] = None       # 'greenhouse', 'lever', 'workday', 'ashby'
    size_range: Optional[str] = None         # '1-50', '51-200', '201-1000', etc.
    industry: Optional[str] = None
    funding_stage: Optional[str] = None      # 'seed', 'series_a', ..., 'public'
    glassdoor_rating: Optional[float] = None
    glassdoor_url: Optional[str] = None
    tech_stack: Optional[str] = None         # JSON array string
    crunchbase_data: Optional[str] = None    # JSON blob string
    enriched_at: Optional[str] = None        # ISO-8601
    is_target: int = 0                       # 0 / 1; user-pinned company
    created_at: Optional[str] = None         # ISO-8601; set by DB default


@dataclass
class ScoreDimension:
    """
    Multi-dimensional score for a single job pass, matching the
    ``score_dimensions`` table schema.

    ``pass_num`` maps to the SQL column ``pass`` (a Python reserved word).
    ``UNIQUE(job_id, pass)`` in the DB means re-scoring upserts this row.
    """

    job_id: int
    pass_num: int          # 1 = fast filter, 2 = deep analysis
    overall: int           # 0-100 weighted composite (required by schema)

    # Dimension scores (0-100); absent on Pass 1 fast-filter rows
    role_fit: Optional[int] = None
    skills_gap: Optional[int] = None
    culture_signals: Optional[int] = None
    growth_potential: Optional[int] = None
    comp_alignment: Optional[int] = None

    # Metadata
    reasoning: Optional[str] = None         # JSON: per-dimension explanations
    scored_at: Optional[str] = None         # ISO-8601; set by DB default
    profile_hash: Optional[str] = None      # SHA256 of profile + snapshot

    # DB surrogate key; None until persisted
    id: Optional[int] = None


@dataclass
class Feedback:
    """
    User feedback signal for a job, matching the ``feedback`` table schema.

    ``signal`` must be one of 'thumbs_up' or 'thumbs_down' (enforced by a
    DB CHECK constraint; validated at the application layer as well).
    """

    job_id: int
    signal: str            # 'thumbs_up' | 'thumbs_down'

    note: Optional[str] = None
    created_at: Optional[str] = None  # ISO-8601; set by DB default

    # DB surrogate key; None until persisted
    id: Optional[int] = None


@dataclass
class ProfileSnapshot:
    """
    Point-in-time snapshot of the candidate profile, matching the
    ``profile_snapshots`` table schema.

    Created whenever the resume PDF hash changes and the resume-sync
    subagent re-parses the document.
    """

    profile_yaml: str          # full contents of profile.yaml at snapshot time

    resume_hash: Optional[str] = None        # SHA256 of the resume PDF
    extracted_skills: Optional[str] = None   # JSON: skills parsed from resume
    created_at: Optional[str] = None         # ISO-8601; set by DB default

    # DB surrogate key; None until persisted
    id: Optional[int] = None


@dataclass
class CareerPageConfig:
    """
    Crawl configuration for a company's career page, matching the
    ``career_page_configs`` table schema.

    ``status`` must be one of 'active', 'broken', or 'disabled' (DB CHECK
    constraint).
    """

    company_id: int
    url: str
    discovery_method: str      # 'auto' | 'manual'

    scrape_strategy: Optional[str] = None    # JSON: LLM-generated extraction rules
    last_crawled_at: Optional[str] = None    # ISO-8601
    status: str = "active"                   # 'active' | 'broken' | 'disabled'
    created_at: Optional[str] = None         # ISO-8601; set by DB default

    # DB surrogate key; None until persisted
    id: Optional[int] = None


@dataclass
class ProfileSuggestion:
    """
    Profile evolution suggestion generated by the profile-evolution
    subagent, matching the ``profile_suggestions`` table schema.

    ``status`` must be one of 'pending', 'approved', or 'rejected' (DB
    CHECK constraint).  The user approves or rejects suggestions in the
    web UI.
    """

    suggestion_type: str       # 'add_skill', 'remove_skill', 'adjust_weight', etc.
    description: str
    reasoning: str
    suggested_change: str      # JSON: the specific YAML diff

    status: str = "pending"    # 'pending' | 'approved' | 'rejected'
    created_at: Optional[str] = None    # ISO-8601; set by DB default
    resolved_at: Optional[str] = None  # ISO-8601; set when approved/rejected

    # DB surrogate key; None until persisted
    id: Optional[int] = None

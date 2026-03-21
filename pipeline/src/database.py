"""
SQLite database module for the Jobseeker V2 pipeline.

Provides schema initialization (init_db) and connection management
(get_connection). Every connection is opened with WAL journal mode and a
busy_timeout of 5000 ms so that concurrent readers from the Next.js UI and
sequential writers from the pipeline do not deadlock.

Tables (in dependency order so FK targets are created first):
    companies
    job_duplicate_groups
    jobs
    score_dimensions
    feedback
    profile_snapshots
    career_page_configs
    profile_suggestions
    glassdoor_api_usage
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


# ---------------------------------------------------------------------------
# DDL — one constant per table + indices, in FK-dependency order
# ---------------------------------------------------------------------------

_CREATE_COMPANIES = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    domain TEXT,
    career_page_url TEXT,
    ats_platform TEXT,
    size_range TEXT,
    industry TEXT,
    funding_stage TEXT,
    glassdoor_rating REAL,
    glassdoor_url TEXT,
    tech_stack TEXT,
    crunchbase_data TEXT,
    enriched_at TEXT,
    is_target INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_COMPANIES_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);",
    "CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain);",
]

_CREATE_JOB_DUPLICATE_GROUPS = """
CREATE TABLE IF NOT EXISTS job_duplicate_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    external_id TEXT,
    url TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    company_id INTEGER REFERENCES companies(id),
    location TEXT,
    description TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_currency TEXT,
    posted_at TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    ats_platform TEXT,
    raw_json TEXT,
    dedup_hash TEXT,
    full_description TEXT,
    dup_group_id INTEGER REFERENCES job_duplicate_groups(id),
    is_representative INTEGER NOT NULL DEFAULT 0
);
"""

_CREATE_JOBS_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_jobs_company_id ON jobs(company_id);",
    "CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);",
    "CREATE INDEX IF NOT EXISTS idx_jobs_posted_at ON jobs(posted_at);",
]

_CREATE_SCORE_DIMENSIONS = """
CREATE TABLE IF NOT EXISTS score_dimensions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    pass INTEGER NOT NULL,
    role_fit INTEGER,
    skills_match INTEGER,
    culture_signals INTEGER,
    growth_potential INTEGER,
    comp_alignment INTEGER,
    overall INTEGER NOT NULL,
    reasoning TEXT,
    scored_at TEXT NOT NULL DEFAULT (datetime('now')),
    profile_hash TEXT,
    UNIQUE(job_id, pass)
);
"""

_CREATE_SCORE_DIMENSIONS_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_score_dimensions_job_id ON score_dimensions(job_id);",
    "CREATE INDEX IF NOT EXISTS idx_score_dimensions_overall ON score_dimensions(overall);",
]

_CREATE_FEEDBACK = """
CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    signal TEXT NOT NULL CHECK (signal IN ('thumbs_up', 'thumbs_down')),
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_FEEDBACK_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_feedback_job_id ON feedback(job_id);",
]

_CREATE_PROFILE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS profile_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_yaml TEXT NOT NULL,
    resume_hash TEXT,
    extracted_skills TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_CAREER_PAGE_CONFIGS = """
CREATE TABLE IF NOT EXISTS career_page_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    url TEXT NOT NULL,
    discovery_method TEXT NOT NULL,
    scrape_strategy TEXT,
    last_crawled_at TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'broken', 'disabled')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_CAREER_PAGE_CONFIGS_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_career_page_configs_company_id ON career_page_configs(company_id);",
    "CREATE INDEX IF NOT EXISTS idx_career_page_configs_status ON career_page_configs(status);",
]

_CREATE_PROFILE_SUGGESTIONS = """
CREATE TABLE IF NOT EXISTS profile_suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    suggestion_type TEXT NOT NULL,
    description TEXT NOT NULL,
    reasoning TEXT NOT NULL,
    suggested_change TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT
);
"""

_CREATE_PROFILE_SUGGESTIONS_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_profile_suggestions_status ON profile_suggestions(status);",
]

_CREATE_GLASSDOOR_API_USAGE = """
CREATE TABLE IF NOT EXISTS glassdoor_api_usage (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    month   TEXT    NOT NULL,
    count   INTEGER NOT NULL DEFAULT 0
);
"""

# Ordered list: (CREATE TABLE sql, [index sqls, ...])
_SCHEMA: list[tuple[str, list[str]]] = [
    (_CREATE_COMPANIES, _CREATE_COMPANIES_INDICES),
    (_CREATE_JOB_DUPLICATE_GROUPS, []),
    (_CREATE_JOBS, _CREATE_JOBS_INDICES),
    (_CREATE_SCORE_DIMENSIONS, _CREATE_SCORE_DIMENSIONS_INDICES),
    (_CREATE_FEEDBACK, _CREATE_FEEDBACK_INDICES),
    (_CREATE_PROFILE_SNAPSHOTS, []),
    (_CREATE_CAREER_PAGE_CONFIGS, _CREATE_CAREER_PAGE_CONFIGS_INDICES),
    (_CREATE_PROFILE_SUGGESTIONS, _CREATE_PROFILE_SUGGESTIONS_INDICES),
    (_CREATE_GLASSDOOR_API_USAGE, []),
]

# Expected table names — used by callers that verify schema completeness.
EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        "jobs",
        "companies",
        "score_dimensions",
        "feedback",
        "profile_snapshots",
        "career_page_configs",
        "profile_suggestions",
        "job_duplicate_groups",
        "glassdoor_api_usage",
    }
)

# Expected index names — used by callers that verify index completeness.
EXPECTED_INDICES: frozenset[str] = frozenset(
    {
        "idx_jobs_company_id",
        "idx_jobs_source",
        "idx_jobs_posted_at",
        "idx_companies_name",
        "idx_companies_domain",
        "idx_score_dimensions_job_id",
        "idx_score_dimensions_overall",
        "idx_feedback_job_id",
        "idx_career_page_configs_company_id",
        "idx_career_page_configs_status",
        "idx_profile_suggestions_status",
    }
)

# WAL and timeout settings applied on every connection open.
_BUSY_TIMEOUT_MS: int = 5000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_connection_settings(conn: sqlite3.Connection) -> None:
    """Apply WAL mode, busy_timeout, and foreign_keys = ON to an open connection.

    Args:
        conn: An open SQLite connection.
    """
    conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_db(db_path: str | Path) -> None:
    """Create the database file and apply the full V2 schema.

    Idempotent — safe to call on an existing database; all DDL uses
    IF NOT EXISTS. Does not drop or modify existing tables.

    Args:
        db_path: Filesystem path for the SQLite database file.
            Parent directories must already exist.

    Raises:
        sqlite3.Error: If any DDL statement fails.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    try:
        _apply_connection_settings(conn)
        for create_sql, index_sqls in _SCHEMA:
            conn.execute(create_sql)
            for idx_sql in index_sqls:
                conn.execute(idx_sql)
        # Migration: rename skills_gap → skills_match for existing databases.
        # PRAGMA table_info returns (cid, name, type, notnull, dflt_value, pk); [1] is the column name.
        cols = {row[1] for row in conn.execute("PRAGMA table_info(score_dimensions)")}
        if "skills_gap" in cols:
            conn.execute(
                "ALTER TABLE score_dimensions RENAME COLUMN skills_gap TO skills_match"
            )
        # Migration: add full_description column to jobs for existing databases.
        # PRAGMA table_info returns (cid, name, type, notnull, dflt_value, pk); [1] is the column name.
        jobs_cols = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
        if "full_description" not in jobs_cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN full_description TEXT")
        # Migration: add duplicate-detection columns to jobs for existing databases.
        if "dup_group_id" not in jobs_cols:
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN dup_group_id INTEGER"
                " REFERENCES job_duplicate_groups(id)"
            )
        if "is_representative" not in jobs_cols:
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN is_representative INTEGER NOT NULL DEFAULT 0"
            )
        conn.commit()
    finally:
        conn.close()


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and busy_timeout configured.

    Callers are responsible for closing the connection.  Note: sqlite3's
    context manager only commits or rolls back the current transaction on
    exit — it does **not** close the connection.  Always call
    ``conn.close()`` explicitly::

        conn = get_connection("data/jobs.db")
        try:
            rows = conn.execute("SELECT * FROM jobs").fetchall()
        finally:
            conn.close()

    WAL mode is enforced on every open so that connections created after
    init_db still have WAL active (not just the init connection).

    Args:
        db_path: Filesystem path for the SQLite database file.

    Returns:
        An open sqlite3.Connection with WAL mode, busy_timeout = 5000 ms,
        and foreign_keys = ON.

    Raises:
        FileNotFoundError: If *db_path* does not exist.  Prevents sqlite3
            from silently creating an empty database at a mistyped path.
        sqlite3.Error: If the connection cannot be established.
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Database file not found: {path!r}. "
            "Run init_db() first or check the path."
        )
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    _apply_connection_settings(conn)
    return conn

# Jobseeker V2 — Design Spec

## Overview

An intelligent, local-first job search system that automatically fetches job postings from multiple sources, enriches them with company and compensation data, scores them against your resume using multi-dimensional LLM analysis via Claude Code subagents, and presents results in an interactive web UI with feedback-driven profile evolution.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CLAUDE CODE SESSION                      │
│                                                              │
│  "run my job pipeline"                                       │
│       │                                                      │
│       ▼                                                      │
│  ┌──────────────────────┐    ┌───────────────────────────┐  │
│  │  Python Pipeline      │    │  Claude Code Subagents    │  │
│  │  (data plumbing)      │    │  (all LLM reasoning)      │  │
│  │                        │    │                           │  │
│  │  • Fetch from APIs     │    │  • Fast filter (Pass 1)  │  │
│  │  • Crawl career pages  │    │  • Deep scoring (Pass 2) │  │
│  │  • Deduplicate         │    │  • Resume parsing        │  │
│  │  • Enrich via APIs     │    │  • Career page discovery │  │
│  │  • DB read/write       │    │  • Profile evolution     │  │
│  └──────────┬─────────────┘    └──────────┬──────────────┘  │
│             │                              │                  │
│             ▼                              ▼                  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    SQLite Database                       │ │
│  │                    (data/jobs.db)                        │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ reads / writes feedback
                              ▼
              ┌───────────────────────────────┐
              │        Next.js Web UI          │
              │    (http://localhost:3000)      │
              │                                │
              │  • Browse & filter scored jobs  │
              │  • Multi-dimensional score view │
              │  • Thumbs up/down feedback      │
              │  • Company management           │
              │  • Profile evolution review     │
              └───────────────────────────────┘
```

**Key principle:** Python owns data plumbing. Claude Code subagents own all LLM reasoning. Next.js owns the interactive UI. SQLite is the shared contract between all three.

## Database Schema

V2 starts with a fresh database. V1 data is not migrated — the schema changes are extensive enough that a clean start is simpler than migration. The V1 database remains in the old repo as a reference.

The database MUST be opened with `PRAGMA journal_mode=WAL` to allow concurrent reads (Next.js UI) with sequential writes (pipeline or subagents). Only one writer operates at a time.

### Tables

**`jobs`** — enhanced with company FK and source metadata:

```sql
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,              -- 'adzuna', 'remoteok', 'linkedin', 'career_page', 'ats_feed'
    source_type TEXT NOT NULL,         -- 'api', 'career_page', 'ats_feed'
    external_id TEXT,
    url TEXT UNIQUE NOT NULL,          -- primary dedup key (replaces V1's UNIQUE(source, external_id))
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    company_id INTEGER REFERENCES companies(id),
    location TEXT,
    description TEXT,
    salary_min REAL,
    salary_max REAL,
    posted_at TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),  -- updated on re-fetch when job already exists
    ats_platform TEXT,                 -- 'greenhouse', 'lever', 'workday', 'ashby', null
    raw_json TEXT,
    dedup_hash TEXT                    -- title + company fingerprint for fuzzy cross-source dedup
);

CREATE INDEX idx_jobs_company_id ON jobs(company_id);
CREATE INDEX idx_jobs_source ON jobs(source);
CREATE INDEX idx_jobs_posted_at ON jobs(posted_at);
```

Note: V1 used `UNIQUE(source, external_id)` for deduplication. V2 uses `url UNIQUE` as the primary dedup key instead, supplemented by `dedup_hash` for fuzzy cross-source matching (same job posted on multiple boards with different URLs).

**`companies`**:

```sql
CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    domain TEXT,
    career_page_url TEXT,
    ats_platform TEXT,                 -- 'greenhouse', 'lever', 'workday', 'ashby', null
    size_range TEXT,                   -- '1-50', '51-200', '201-1000', '1001-5000', '5000+'
    industry TEXT,
    funding_stage TEXT,                -- 'seed', 'series_a', 'series_b', ..., 'public'
    glassdoor_rating REAL,
    glassdoor_url TEXT,
    tech_stack TEXT,                   -- JSON array
    crunchbase_data TEXT,              -- JSON blob
    enriched_at TEXT,
    is_target INTEGER NOT NULL DEFAULT 0,  -- user-pinned company
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_companies_name ON companies(name);
CREATE INDEX idx_companies_domain ON companies(domain);
```

**`score_dimensions`** — replaces V1's `llm_scores`:

```sql
CREATE TABLE score_dimensions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    pass INTEGER NOT NULL,            -- 1 = fast filter, 2 = deep analysis
    role_fit INTEGER,                 -- 0-100
    skills_match INTEGER,              -- 0-100
    culture_signals INTEGER,          -- 0-100
    growth_potential INTEGER,         -- 0-100
    comp_alignment INTEGER,           -- 0-100
    overall INTEGER NOT NULL,         -- weighted composite
    reasoning TEXT,                   -- JSON: per-dimension explanations
    scored_at TEXT NOT NULL DEFAULT (datetime('now')),
    profile_hash TEXT,                -- SHA256 of profile.yaml + latest profile_snapshot
    UNIQUE(job_id, pass)              -- one score per job per pass; re-scoring upserts
);

CREATE INDEX idx_score_dimensions_job_id ON score_dimensions(job_id);
CREATE INDEX idx_score_dimensions_overall ON score_dimensions(overall);
```

`profile_hash` is computed as `SHA256(profile.yaml contents + latest profile_snapshot.extracted_skills JSON)`. When the hash changes, existing scores are considered stale and eligible for re-scoring.

**`feedback`**:

```sql
CREATE TABLE feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    signal TEXT NOT NULL CHECK (signal IN ('thumbs_up', 'thumbs_down')),
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_feedback_job_id ON feedback(job_id);
```

**`profile_snapshots`**:

```sql
CREATE TABLE profile_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_yaml TEXT NOT NULL,
    resume_hash TEXT,
    extracted_skills TEXT,            -- JSON: skills parsed from resume PDF
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

**`career_page_configs`**:

```sql
CREATE TABLE career_page_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    url TEXT NOT NULL,
    discovery_method TEXT NOT NULL,   -- 'auto', 'manual'
    scrape_strategy TEXT,             -- JSON: LLM-generated extraction instructions
    last_crawled_at TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'broken', 'disabled')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_career_page_configs_company_id ON career_page_configs(company_id);
CREATE INDEX idx_career_page_configs_status ON career_page_configs(status);
```

**`profile_suggestions`**:

```sql
CREATE TABLE profile_suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    suggestion_type TEXT NOT NULL,    -- 'add_skill', 'remove_skill', 'adjust_weight', 'add_keyword', etc.
    description TEXT NOT NULL,
    reasoning TEXT NOT NULL,
    suggested_change TEXT NOT NULL,   -- JSON: the specific YAML diff
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT
);

CREATE INDEX idx_profile_suggestions_status ON profile_suggestions(status);
```

### Deprecated V1 tables (not carried forward)

- **`filter_results`** — V1 used this to debug deterministic filter stages. In V2, the fast filter (Pass 1) is LLM-based and its results are stored in `score_dimensions`. Not needed.
- **`email_history`** — V1 tracked emailed jobs. In V2, the web UI replaces email delivery. Not needed.
- **`llm_scores`** — Replaced by `score_dimensions` with multi-dimensional scoring.

## Pipeline Stages

### Stage 1: Fetch

Three sub-fetchers running sequentially:

**API fetchers** (Adzuna, RemoteOK, LinkedIn via RapidAPI):
- Targeted keyword searches from `profile.yaml` title_keywords
- Each fetcher returns raw dicts, normalizer converts to Job model
- Deduplication by URL + title+company fingerprint

**ATS feed fetcher:**
- For companies in `companies` table with known `ats_platform`
- Hits structured JSON feeds directly:
  - Greenhouse: `boards-api.greenhouse.io/v1/boards/{company}/jobs`
  - Lever: `api.lever.co/v0/postings/{company}`
  - Ashby: `api.ashbyhq.com/posting-api/job-board/{company}`
- No scraping needed, structured data

**Career page crawler:**
- Iterates `career_page_configs` where `status = 'active'`
- Uses stored `scrape_strategy` to extract job listings
- If extraction returns 0 results for a previously-working config, flags as potentially broken

All fetched jobs get deduplicated and inserted into the `jobs` table. For jobs that already exist (matched by URL), `last_seen_at` is updated to the current timestamp.

### Stage 1.5: Pre-filter (deterministic)

Before LLM scoring, the ported V1 `filter.py` runs as a cheap deterministic pre-filter:
- Red flag keyword matching (from `red_flags.yaml`)
- Salary minimum check (from `profile.yaml`)
- Intern/junior role exclusion
- Job age check (max_job_age_days)

Jobs that fail pre-filtering are marked in the database and skipped by the scoring stages. This reduces the number of jobs sent to the LLM fast filter, saving time.

### Stage 2: Enrich

Runs on jobs with `company_id` where the company has no enrichment or stale enrichment (>30 days).

**Enrichment sources (each independent, partial failure OK):**

| Source | Data | Feeds into |
|--------|------|------------|
| Crunchbase (free tier) | Size, funding, industry, HQ | `companies` metadata |
| Glassdoor (scrape/API) | Overall rating, culture, WLB | `culture_signals` dimension |
| levels.fyi | Comp data by company+role+level | `comp_alignment` dimension |
| StackShare + job desc analysis | Tech stack | `skills_match` dimension |

Each enrichment function writes to the `companies` table independently.

**Degraded enrichment handling:** When an enrichment source is unavailable (API down, no data for that company, rate limited), the corresponding `companies` fields remain NULL. The scoring subagent prompt explicitly handles this: "If no Glassdoor data is available, score `culture_signals` based only on signals in the job description (team mentions, WLB language, etc.). If no comp data is available, score `comp_alignment` based on the posted salary vs. profile expectations only." Partial enrichment is always better than blocking the pipeline.

**Feasibility note:** Crunchbase free tier, Glassdoor, and levels.fyi all have access limitations. During implementation, each enrichment source should be treated as best-effort. If a source proves too unreliable or restricted to be useful, it can be dropped without affecting the rest of the system. The LLM scoring model is designed to work with whatever data is available.

### Stage 3: Resume Sync

Runs when the resume PDF hash differs from the latest `profile_snapshots` entry.

**Two-step process:**
1. **Python pre-processing** — `pdfplumber` extracts raw text from the resume PDF and writes it to a temp file. This is needed because Claude Code's Read tool can view PDFs but pdfplumber provides cleaner text extraction for structured parsing.
2. **Claude Code subagent** — receives the extracted text and performs the intelligent work:
   - Extract: skills, experience timeline, accomplishments, seniority indicators
   - Structure as JSON matching the `extracted_skills` schema
   - Diff against current `profile.yaml` and generate suggestions for any gaps
   - Write results to `profile_snapshots` table and any suggestions to `profile_suggestions`

### Stage 4: Score (Two-Pass)

**Pass 1 — Fast filter (subagent):**
- All jobs with no `score_dimensions` row
- Batched: one subagent receives ~40-50 jobs
- Prompt: "For each job, is this worth deeper analysis for this candidate? Yes/No + confidence"
- Jobs below threshold get `pass=1, overall=0`
- Jobs above threshold proceed to Pass 2

**Pass 2 — Deep analysis (parallel subagents):**
- Only Pass 1 survivors (typically 30-50 jobs)
- Split across 3-4 parallel subagents (~10-15 jobs each)
- Each subagent receives: job details + company enrichment data + full profile snapshot
- Returns all 5 dimension scores (0-100) + per-dimension reasoning
- Results written to `score_dimensions` with `pass=2`

**Scoring dimensions:**

| Dimension | Weight | What it measures |
|-----------|--------|-----------------|
| Role fit | 30% | Title/description alignment, seniority match, responsibility alignment |
| Skills gap | 25% | Required vs. candidate skills overlap, tech stack alignment |
| Culture signals | 15% | Company ratings, team structure, work-life indicators, product vs. agency |
| Growth potential | 15% | Career trajectory fit, learning opportunities, scope of impact |
| Comp alignment | 15% | Posted salary vs. expectations, market benchmarks from enrichment |

### Stage 5: Profile Evolution

Runs when there are 20+ new feedback signals since last evolution check.

**Claude Code subagent task:**
- Read all `feedback` rows with associated job `score_dimensions`
- Read current `profile.yaml`
- Analyze patterns: "You thumbs-up'd 8 jobs that emphasize ML infrastructure but your profile doesn't list ML. You thumbs-down'd 3 agency roles even though your profile doesn't exclude them."
- Generate specific suggestions, written to `profile_suggestions` table
- User approves/rejects in the web UI

## Company Discovery

When a user adds a company by name in the web UI:

1. **Search** — Python script uses SerpAPI (or similar) to find `"{company}" careers jobs`
2. **ATS detection** — Claude Code subagent checks if the career page URL maps to a known ATS platform. If yes, store the ATS type in `companies.ats_platform` and create an entry in `career_page_configs` pointing to the ATS feed URL. Done.
3. **LLM-assisted extraction** — If not an ATS, subagent fetches the career page HTML and generates a `scrape_strategy` JSON (CSS selectors, URL patterns, field extraction rules). Stored in `career_page_configs`.
4. **Validation** — Subagent runs the extraction, verifies results look like real job listings. If it fails, marks config as `broken`.

**Manual override:** User can add a company with a specific career page URL, skipping the search step.

**Maintenance:** Each pipeline run, if a previously-working config returns 0 results, re-run discovery once. If it fails again, mark `broken` and surface in the UI.

## Web UI

### Tech stack

- Next.js 14+ with App Router
- Server components read SQLite via `better-sqlite3`
- Server actions for writes (feedback, company management)
- Tailwind CSS + shadcn/ui
- Recharts for score dimension visualizations

### Pages

**Dashboard** (`/`):
- Summary stats: total jobs, scored this run, average scores, new high matches
- Alerts: broken career page configs, pending profile suggestions, resume out of sync
- Quick access to latest high-scoring jobs

**Jobs** (`/jobs`):
- Sortable, filterable table of scored jobs
- Filter by: overall score range, individual dimension ranges, company, source, date range, feedback status
- Columns: title, company, overall score, dimension mini-bars, salary, location, posted date
- Click row → job detail page

**Job Detail** (`/jobs/[id]`):
- Overall score prominently displayed
- Five dimension scores as radar chart or horizontal bars
- Per-dimension reasoning expandable
- Company enrichment sidebar: size, funding stage, rating, tech stack
- Comp comparison: posted range vs. market data (from levels.fyi enrichment)
- Thumbs up/down buttons + optional note
- Link to original posting

**Companies** (`/companies`):
- List of tracked companies with enrichment status
- Add company by name or URL
- See crawl status per company (active, broken, disabled)
- Edit/override career page URLs
- View all jobs from a specific company

**Profile** (`/profile`):
- View current `profile.yaml` contents
- Side-by-side: extracted resume skills vs. YAML skills (gaps highlighted)
- Pending profile evolution suggestions with approve/reject buttons
- History of past suggestions and their resolution

**Feedback History** (`/feedback`):
- Chronological list of all feedback signals
- Filter by thumbs_up/thumbs_down
- Shows job title, company, scores, and any notes
- Summary patterns (if available from last evolution analysis)

### Data flow

```
Python pipeline writes → SQLite ← Next.js reads (server components)
                         SQLite ← Next.js writes (server actions: feedback, company configs)
Python pipeline reads ← SQLite   (feedback for profile evolution)
```

No WebSocket or real-time sync. Run pipeline, refresh page, new data appears.

## Project Structure

```
jobseeker-v2/
├── pipeline/                        # Python pipeline (all backend data plumbing)
│   ├── cli.py                       # Entry point: --fetch, --enrich, --prefilter, --all
│   ├── config/
│   │   ├── profile.yaml             # ← PORTED from v1
│   │   ├── red_flags.yaml           # ← PORTED from v1
│   │   └── settings.py              # ← PORTED from v1 (env var loader)
│   ├── resume/
│   │   └── (user's PDF resumes)
│   ├── src/
│   │   ├── models.py                # Job, Company, ScoreDimension dataclasses
│   │   ├── database.py              # Schema definitions, migrations (new schema)
│   │   ├── fetchers/
│   │   │   ├── base.py              # ← PORTED from v1
│   │   │   ├── adzuna.py            # ← PORTED from v1
│   │   │   ├── remoteok.py          # ← PORTED from v1
│   │   │   ├── linkedin.py          # New: via RapidAPI
│   │   │   ├── career_page.py       # New: LLM-assisted scraper
│   │   │   └── ats.py               # New: Greenhouse/Lever/Ashby feeds
│   │   ├── enrichment/
│   │   │   ├── crunchbase.py        # New
│   │   │   ├── glassdoor.py         # New
│   │   │   ├── levelsfy.py          # New
│   │   │   ├── stackshare.py        # New
│   │   │   └── orchestrator.py      # New: runs all enrichers, handles failures
│   │   ├── normalizer.py            # ← PORTED from v1 (extended for new sources)
│   │   ├── deduplicator.py          # ← PORTED from v1 (adapted for DB-based dedup)
│   │   └── filter.py                # ← PORTED from v1
│   ├── prompts/                     # Subagent prompt templates
│   │   ├── fast_filter.md
│   │   ├── deep_scorer.md
│   │   ├── resume_parser.md
│   │   ├── company_discovery.md
│   │   └── profile_evolution.md
│   ├── requirements.txt
│   └── tests/
│
├── web/                             # Next.js app
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx                 # Dashboard
│   │   ├── jobs/
│   │   │   ├── page.tsx             # Job list with filters
│   │   │   └── [id]/
│   │   │       └── page.tsx         # Job detail + feedback
│   │   ├── companies/
│   │   │   └── page.tsx             # Company management
│   │   ├── profile/
│   │   │   └── page.tsx             # Profile + evolution suggestions
│   │   └── feedback/
│   │       └── page.tsx             # Feedback history
│   ├── lib/
│   │   ├── db.ts                    # better-sqlite3 connection + query functions
│   │   └── types.ts                 # TypeScript types mirroring DB schema
│   ├── components/
│   │   ├── score-radar.tsx          # Dimension score radar/bar chart
│   │   ├── job-table.tsx            # Sortable/filterable job list
│   │   ├── comp-comparison.tsx      # Salary vs. market chart
│   │   ├── feedback-buttons.tsx     # Thumbs up/down + note
│   │   └── dimension-bars.tsx       # Mini horizontal bars for table rows
│   ├── package.json
│   ├── tailwind.config.ts
│   └── tsconfig.json
│
├── data/
│   └── jobs.db                      # Shared SQLite database
│
├── Makefile
│   # make setup        → install Python + Node dependencies
│   # make fetch        → python pipeline/cli.py --fetch
│   # make enrich       → python pipeline/cli.py --enrich
│   # make prefilter    → python pipeline/cli.py --prefilter
│   # make all          → python pipeline/cli.py --all (fetch + enrich + prefilter)
│   # make web          → cd web && npm run dev
│   # make db-reset     → reset database
│
└── README.md
```

## Files Ported from V1

The following files from the current repository should be copied into the new project as starting points. Each will need minor adaptation (import paths, DB interface changes) but the core logic is reusable.

| V1 path | V2 destination | Adaptation needed |
|---------|---------------|-------------------|
| `src/models.py` | `pipeline/src/models.py` | Add new fields: `source_type`, `company_id`, `ats_platform`, `dedup_hash`. Add Company and ScoreDimension dataclasses. |
| `src/fetchers/base.py` | `pipeline/src/fetchers/base.py` | Update import paths |
| `src/fetchers/adzuna.py` | `pipeline/src/fetchers/adzuna.py` | Update imports, write to new DB schema |
| `src/fetchers/remoteok.py` | `pipeline/src/fetchers/remoteok.py` | Update imports, write to new DB schema |
| `src/normalizer.py` | `pipeline/src/normalizer.py` | Update imports, add new source normalizers |
| `src/deduplicator.py` | `pipeline/src/deduplicator.py` | Switch from JSON file to DB-based dedup |
| `src/filter.py` | `pipeline/src/filter.py` | Update imports, read from new DB schema. Runs as Stage 1.5 pre-filter before LLM scoring. |
| `config/profile.yaml` | `pipeline/config/profile.yaml` | No changes needed |
| `config/red_flags.yaml` | `pipeline/config/red_flags.yaml` | No changes needed |
| `config/settings.py` | `pipeline/config/settings.py` | Add new env vars for enrichment APIs |

## Pipeline Invocation

The pipeline is invoked from within a Claude Code session. The Python CLI handles data plumbing stages (fetch, enrich, prefilter). All LLM-dependent stages (scoring, resume sync, profile evolution) are handled by Claude Code subagents that read from and write to the database directly.

Typical flow:

```
User: "run my job pipeline"

Claude Code orchestration:
  1. python pipeline/cli.py --all            # Fetch + enrich + prefilter (pure Python)
  2. Subagent: resume sync                   # If resume PDF changed
  3. Subagent: fast filter (Pass 1)          # Batch-evaluate unscored jobs
  4. 3-4 parallel subagents: deep scoring    # Multi-dimensional scoring on survivors
  5. Subagent: profile evolution             # If 20+ new feedback signals
  6. "Done — open http://localhost:3000"
```

Pass 2 subagents query the database directly for Pass 1 survivors (`SELECT * FROM score_dimensions WHERE pass = 1 AND overall > 0`), joined with job and company enrichment data. No intermediate export step needed — SQLite is the handoff mechanism.

Individual stages can also be run:
- "just fetch new jobs" → step 1 only
- "score the unscored jobs" → steps 3-4
- "check if my profile needs updating" → step 5

## Dependencies

### Python (pipeline/requirements.txt)

```
requests>=2.31.0          # HTTP client for APIs
pyyaml>=6.0               # Config file parsing
python-dotenv>=1.0.0      # Environment variable management
pdfplumber>=0.10.0        # PDF text extraction (resume sync pre-processing)
```

Note: `anthropic` package is NOT needed. All LLM work goes through Claude Code subagents.

### Node.js (web/package.json)

Exact versions determined at project initialization via `npx create-next-app@latest` and `npx shadcn@latest init`. Key dependencies:

```
next                       # 14+ with App Router
react                      # 18+
better-sqlite3             # Direct synchronous SQLite access
tailwindcss                # Utility CSS
recharts                   # Score dimension visualizations
```

Note: shadcn/ui is not an npm package — components are installed individually via `npx shadcn@latest add <component>` and live in the project source tree.

## Success Criteria

- [ ] Fetch jobs from 3+ API sources and target company career pages
- [ ] Enrich companies with metadata from 4 external sources
- [ ] Auto-parse resume PDF into structured skills profile
- [ ] Two-pass LLM scoring via Claude Code subagents (no API costs)
- [ ] Multi-dimensional scores (5 dimensions) with per-dimension reasoning
- [ ] Interactive web UI for browsing, filtering, and sorting by any dimension
- [ ] Thumbs up/down feedback persisted and visible in UI
- [ ] Profile evolution suggestions generated from feedback patterns
- [ ] Company discovery by name with ATS detection and LLM-assisted extraction
- [ ] All state in a single SQLite database shared between pipeline and UI
- [ ] Pipeline runs on-demand from Claude Code, UI runs independently via `make web`

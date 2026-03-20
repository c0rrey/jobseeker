# Decomposition Brief

**Spec**: /Users/correy/projects/jseeker/.crumbs/sessions/_decompose-20260320-102407/spec.md
**Date**: 2026-03-20
**Codebase mode**: mixed (brownfield V1 ported files + greenfield V2 new code)
**Trails created**: 8
**Crumbs created**: 25

## Codebase Map

The jseeker repository contains ported V1 files and empty directory scaffolding for V2:

**Existing files (brownfield -- need modification):**
- `pipeline/src/models.py` -- Job dataclass (V1 schema, needs V2 fields added)
- `pipeline/src/fetchers/base.py` -- BaseFetcher ABC (needs source_type property)
- `pipeline/src/fetchers/adzuna.py` -- Adzuna API fetcher (needs import path updates)
- `pipeline/src/fetchers/remoteok.py` -- RemoteOK API fetcher (needs import path updates)
- `pipeline/src/normalizer.py` -- Normalizer for mock, adzuna, remoteok (needs new source normalizers)
- `pipeline/src/deduplicator.py` -- JSON-file-based dedup (needs full rewrite to DB-backed)
- `pipeline/src/filter.py` -- Deterministic filter (needs import path updates, DB integration)
- `pipeline/config/settings.py` -- Env var loader (needs new API key functions, dotenv)
- `pipeline/config/profile.yaml` -- Job search profile (no changes needed)
- `pipeline/config/red_flags.yaml` -- Red flag rules (no changes needed)
- `.gitignore` -- Needs .env added

**Empty directories (scaffolded but no files):**
- `pipeline/tests/`
- `pipeline/prompts/`
- `pipeline/resume/` (contains user's PDF resume)
- `web/`
- `data/`

**Greenfield (new files to create):**
- `pipeline/src/database.py` -- V2 SQLite schema with 7 tables
- `pipeline/src/fetchers/linkedin.py` -- LinkedIn via RapidAPI
- `pipeline/src/fetchers/ats.py` -- Greenhouse/Lever/Ashby feeds
- `pipeline/src/fetchers/career_page.py` -- LLM-assisted career page crawler
- `pipeline/src/enrichment/` -- All enrichment modules (4 sources + orchestrator)
- `pipeline/src/resume_sync.py` -- PDF extraction and subagent coordination
- `pipeline/src/scorer.py` -- Pass 1/Pass 2 scoring orchestration
- `pipeline/src/profile_evolution.py` -- Feedback analysis and suggestion generation
- `pipeline/src/company_discovery.py` -- Company search and ATS detection
- `pipeline/prompts/*.md` -- 5 subagent prompt templates
- `pipeline/cli.py` -- CLI entry point
- `web/` -- Entire Next.js application
- `Makefile`, `pipeline/requirements.txt`, `README.md`, `.env.example`

## Trail Structure

### Trail: Establish database schema and core data models (seek-T1)
- **Requirements covered**: REQ-1, REQ-18, REQ-3 (settings)
- **Deployability rationale**: Foundation layer. All other trails depend on the database schema, data models, and settings being available. Must be completed first.
- **Crumbs** (3):
  1. Create SQLite database schema with all tables (seek-1) -- python-pro -- files: 2
  2. Extend data models with Company and ScoreDimension dataclasses (seek-2) -- python-pro -- files: 2
  3. Extend settings module with new API environment variables (seek-3) -- python-pro -- files: 4

### Trail: Build job fetching pipeline (seek-T2)
- **Requirements covered**: REQ-2, REQ-3, REQ-4, REQ-19, REQ-20
- **Deployability rationale**: The entire data ingestion pathway: API fetchers, ATS feeds, career page crawler, normalization, and deduplication. Can be deployed and tested independently by running fetchers and verifying data lands in the jobs table.
- **Crumbs** (5):
  1. Adapt Adzuna and RemoteOK fetchers for V2 database schema (seek-4) -- python-pro -- files: 5
  2. Add LinkedIn fetcher via RapidAPI integration (seek-5) -- python-pro -- files: 3
  3. Add ATS feed fetcher for Greenhouse Lever and Ashby (seek-6) -- python-pro -- files: 3
  4. Add career page crawler with scrape strategy execution (seek-7) -- python-pro -- files: 3
  5. Adapt deduplicator for database-backed URL and fuzzy dedup (seek-8) -- python-pro -- files: 2

### Trail: Implement pre-filter and enrichment stages (seek-T3)
- **Requirements covered**: REQ-5, REQ-6
- **Deployability rationale**: Data processing stages between ingestion and scoring. Pre-filtering reduces the job pool before expensive LLM scoring. Enrichment adds company metadata that scoring uses. Both can be tested independently.
- **Crumbs** (3):
  1. Adapt V1 filter module as deterministic pre-filter stage (seek-9) -- python-pro -- files: 2
  2. Create enrichment source modules for company metadata (seek-10) -- python-pro -- files: 6
  3. Create enrichment orchestrator with failure handling (seek-11) -- python-pro -- files: 2

### Trail: Create scoring and resume sync subagent system (seek-T4)
- **Requirements covered**: REQ-7, REQ-8, REQ-21 (partial: 3 of 5 prompts)
- **Deployability rationale**: All LLM-powered scoring and resume parsing. These are the core intelligence features that differentiate V2 from V1. Can be tested end-to-end once data is in the DB.
- **Crumbs** (3):
  1. Implement resume sync with PDF extraction and prompt template (seek-12) -- python-pro -- files: 3
  2. Create fast filter Pass 1 scoring prompt and orchestration (seek-13) -- python-pro -- files: 3
  3. Create deep scoring Pass 2 prompt and parallel orchestration (seek-14) -- python-pro -- files: 3

### Trail: Implement profile evolution and company discovery (seek-T5)
- **Requirements covered**: REQ-9, REQ-10, REQ-21 (partial: 2 of 5 prompts)
- **Deployability rationale**: Advanced feedback-driven features. Profile evolution requires accumulated feedback data. Company discovery enables new data sources. Both are independently deployable after the foundation is in place.
- **Crumbs** (2):
  1. Implement profile evolution analysis and suggestion generation (seek-15) -- python-pro -- files: 3
  2. Implement company discovery with ATS detection and scrape strategy (seek-16) -- python-pro -- files: 3

### Trail: Build web UI foundation and job views (seek-T6)
- **Requirements covered**: REQ-11, REQ-12, REQ-13, REQ-24
- **Deployability rationale**: The core web UI: scaffold, dashboard, job browsing, and job detail with feedback. Users can browse scored jobs and provide feedback once this trail is complete. Independent of the Python pipeline.
- **Crumbs** (4):
  1. Scaffold Next.js app with SQLite connection and shared layout (seek-17) -- typescript-pro -- files: 7
  2. Build dashboard page with summary stats and alerts (seek-18) -- typescript-pro -- files: 4
  3. Build jobs list page with sortable filterable table (seek-19) -- typescript-pro -- files: 4
  4. Build job detail page with scores radar chart and feedback (seek-20) -- typescript-pro -- files: 6

### Trail: Build web UI management pages (seek-T7)
- **Requirements covered**: REQ-14, REQ-15, REQ-16
- **Deployability rationale**: Secondary UI pages for company management, profile viewing, and feedback history. These extend the UI after core job views are working.
- **Crumbs** (3):
  1. Build companies management page with add and status views (seek-21) -- typescript-pro -- files: 5
  2. Build profile page with evolution suggestions and resume diff (seek-22) -- typescript-pro -- files: 5
  3. Build feedback history page with filters and patterns (seek-23) -- typescript-pro -- files: 3

### Trail: Wire CLI orchestration and project setup (seek-T8)
- **Requirements covered**: REQ-17, REQ-22, REQ-23
- **Deployability rationale**: The CLI integrates all pipeline stages into a single entry point. The Makefile and README enable project bootstrapping. This trail is last because it depends on all pipeline stages being implemented.
- **Crumbs** (2):
  1. Create CLI entry point with stage subcommands (seek-24) -- python-pro -- files: 4
  2. Create Makefile and requirements files for project setup (seek-25) -- general-purpose -- files: 3

## Spec Coverage

| Spec Requirement | Covered by Crumb(s) | Coverage Status |
|-----------------|---------------------|-----------------|
| REQ-1: Database schema (7 tables, WAL, indices) | seek-1 | COVERED |
| REQ-2: Fetch from 3+ API sources (Adzuna, RemoteOK, LinkedIn) | seek-4, seek-5 | COVERED |
| REQ-3: ATS feed fetcher (Greenhouse, Lever, Ashby) | seek-6 | COVERED |
| REQ-4: Career page crawler (LLM-assisted) | seek-7 | COVERED |
| REQ-5: Pre-filter (deterministic, V1 port) | seek-9 | COVERED |
| REQ-6: Company enrichment (4 external sources) | seek-10, seek-11 | COVERED |
| REQ-7: Resume sync (PDF extract + subagent parsing) | seek-12 | COVERED |
| REQ-8: Two-pass LLM scoring (5 dimensions, weighted) | seek-13, seek-14 | COVERED |
| REQ-9: Profile evolution (feedback patterns, suggestions) | seek-15 | COVERED |
| REQ-10: Company discovery (ATS detection, scrape strategy) | seek-16 | COVERED |
| REQ-11: Dashboard page (stats, alerts, top matches) | seek-18 | COVERED |
| REQ-12: Jobs list page (sortable, filterable table) | seek-19 | COVERED |
| REQ-13: Job detail page (scores, radar chart, feedback) | seek-20 | COVERED |
| REQ-14: Companies page (management, add, status) | seek-21 | COVERED |
| REQ-15: Profile page (YAML view, skills diff, suggestions) | seek-22 | COVERED |
| REQ-16: Feedback history page (chronological, filters) | seek-23 | COVERED |
| REQ-17: CLI entry point (--fetch, --enrich, --prefilter, --all) | seek-24 | COVERED |
| REQ-18: Data models (Job, Company, ScoreDimension, etc.) | seek-2 | COVERED |
| REQ-19: Deduplication (URL unique + dedup_hash fuzzy) | seek-8 | COVERED |
| REQ-20: Normalizer (extended for all new sources) | seek-4, seek-5, seek-6, seek-7 | COVERED |
| REQ-21: Subagent prompt templates (5 prompts) | seek-12, seek-13, seek-14, seek-15, seek-16 | COVERED |
| REQ-22: Project setup (Makefile, requirements, README) | seek-25 | COVERED |
| REQ-23: Pipeline invocation from Claude Code session | seek-24 | COVERED |
| REQ-24: Web UI shared layout and DB connection layer | seek-17 | COVERED |

**Coverage verdict**: 24/24 requirements covered -- PASS

## Dependency Graph

```
Trail: Establish database schema and core data models (seek-T1):
  seek-1 (schema) [no blockers] -- READY
  seek-2 (models) [no blockers] -- READY
  seek-3 (settings) [no blockers] -- READY

Trail: Build job fetching pipeline (seek-T2):
  seek-4 (adzuna/remoteok) <- blocked by seek-2, seek-3
  seek-5 (linkedin) <- blocked by seek-2, seek-3
  seek-6 (ATS feed) <- blocked by seek-1, seek-2
  seek-7 (career page) <- blocked by seek-1, seek-2
  seek-8 (dedup) <- blocked by seek-1, seek-2

Trail: Implement pre-filter and enrichment stages (seek-T3):
  seek-9 (prefilter) <- blocked by seek-1, seek-2, seek-3
  seek-10 (enrichment sources) <- blocked by seek-1, seek-3
  seek-11 (enrichment orch) <- blocked by seek-10

Trail: Create scoring and resume sync subagent system (seek-T4):
  seek-12 (resume sync) <- blocked by seek-1, seek-2
  seek-13 (fast filter) <- blocked by seek-1, seek-2
  seek-14 (deep scorer) <- blocked by seek-13

Trail: Implement profile evolution and company discovery (seek-T5):
  seek-15 (profile evolution) <- blocked by seek-1
  seek-16 (company discovery) <- blocked by seek-1, seek-3

Trail: Build web UI foundation and job views (seek-T6):
  seek-17 (scaffold) [no blockers] -- READY
  seek-18 (dashboard) <- blocked by seek-17
  seek-19 (jobs list) <- blocked by seek-17
  seek-20 (job detail) <- blocked by seek-17

Trail: Build web UI management pages (seek-T7):
  seek-21 (companies) <- blocked by seek-17
  seek-22 (profile) <- blocked by seek-17
  seek-23 (feedback) <- blocked by seek-17

Trail: Wire CLI orchestration and project setup (seek-T8):
  seek-24 (CLI) <- blocked by seek-1, seek-4, seek-8, seek-9, seek-11
  seek-25 (Makefile) <- blocked by seek-24
```

### Note on shared files

Several crumbs touch the same files (notably `pipeline/src/normalizer.py`, `web/lib/queries.ts`, `web/app/actions.ts`). File-conflict avoidance is the Scout's responsibility during wave scheduling — it may serialize these crumbs across waves or bundle them into a single subagent task. No artificial dependency edges are added for scheduling concerns.

## Cross-Trail Dependencies

| Blocker Crumb | Blocker Trail | Blocked Crumb | Blocked Trail | Justification |
|---------------|---------------|---------------|---------------|---------------|
| seek-1 (schema) | seek-T1 | seek-6 (ATS), seek-7 (career page), seek-8 (dedup) | seek-T2 | Fetchers that write to or query the DB need the schema |
| seek-2 (models) | seek-T1 | seek-4, seek-5, seek-6, seek-7, seek-8 | seek-T2 | All fetchers produce Job dataclasses |
| seek-3 (settings) | seek-T1 | seek-4, seek-5 | seek-T2 | API fetchers need env var loaders |
| seek-1 (schema) | seek-T1 | seek-9, seek-10 | seek-T3 | Pre-filter and enrichment read/write DB |
| seek-2 (models) | seek-T1 | seek-9 | seek-T3 | Pre-filter uses Job model |
| seek-3 (settings) | seek-T1 | seek-9, seek-10 | seek-T3 | Pre-filter needs config; enrichment needs API keys |
| seek-1 (schema) | seek-T1 | seek-12, seek-13 | seek-T4 | Scoring reads/writes score_dimensions |
| seek-2 (models) | seek-T1 | seek-12, seek-13 | seek-T4 | Scoring uses data models |
| seek-1 (schema) | seek-T1 | seek-15, seek-16 | seek-T5 | Evolution and discovery read/write DB |
| seek-3 (settings) | seek-T1 | seek-16 | seek-T5 | Discovery needs SerpAPI key |
| seek-17 (scaffold) | seek-T6 | seek-21, seek-22, seek-23 | seek-T7 | Management pages need the Next.js scaffold, layout, db.ts |
| seek-1 (schema) | seek-T1 | seek-24 | seek-T8 | CLI initializes DB |
| seek-4 (adzuna/remoteok) | seek-T2 | seek-24 | seek-T8 | CLI calls fetchers |
| seek-8 (dedup) | seek-T2 | seek-24 | seek-T8 | CLI calls deduplicator |
| seek-9 (prefilter) | seek-T3 | seek-24 | seek-T8 | CLI calls pre-filter |
| seek-11 (enrichment orch) | seek-T3 | seek-24 | seek-T8 | CLI calls enrichment orchestrator |

Total cross-trail dependencies: 16

All cross-trail dependencies flow from foundation (T1) to consumers (T2-T5, T8), from scaffold (T6) to management pages (T7), and one new edge from T6 job detail (seek-20) to T7 companies (seek-21) to serialize shared file edits. None can be eliminated by reordering -- these represent genuine data, API, and file-scope dependencies.

## Agent Type Summary

| Agent Type | Crumb Count | Crumb IDs |
|------------|-------------|-----------|
| python-pro | 17 | seek-1, seek-2, seek-3, seek-4, seek-5, seek-6, seek-7, seek-8, seek-9, seek-10, seek-11, seek-12, seek-13, seek-14, seek-15, seek-16, seek-24 |
| typescript-pro | 7 | seek-17, seek-18, seek-19, seek-20, seek-21, seek-22, seek-23 |
| general-purpose | 1 | seek-25 |

## Research Integration

- **Stack**: Stack research confirmed all dependencies are compatible (Python 3.14.2, Node.js 25.5.0). Next.js version drift (spec says 14+, latest is 16.2.0) is a non-issue per research. better-sqlite3 compatibility with Node 25 confirmed. pdfplumber needs installation (not yet present). These findings shaped the Makefile crumb (seek-25) to include pdfplumber in requirements.txt and the scaffold crumb (seek-17) to use latest Next.js.

- **Architecture**: Architecture research identified critical design decisions that shaped crumb boundaries: (1) WAL mode enforcement in both database.py and db.ts -- reflected in seek-1 and seek-17 acceptance criteria. (2) SQLITE_BUSY handling for parallel subagents -- reflected in seek-1's busy_timeout requirement. (3) Stage ordering dependencies -- shaped the CLI crumb (seek-24) dependency chain. (4) Profile evolution "last check" cursor -- resolved by using latest profile_snapshots.created_at as the cursor, documented in seek-15.

- **Pitfall**: Pitfall research identified HIGH-priority risks that are mitigated in crumb acceptance criteria: (1) WAL concurrent write race -- seek-1 requires busy_timeout. (2) Non-atomic score writes -- seek-14 notes transaction wrapping. (3) dedup_hash consistency -- seek-8 specifies exact normalization algorithm. (4) profile_hash bootstrap on empty table -- seek-12 and seek-14 acceptance criteria require handling empty profile_snapshots. (5) Career page 0-result detection -- seek-7 acceptance criteria specify broken status detection logic. (6) pdfplumber temp file leak -- seek-12 notes context manager requirement.

- **Pattern**: Pattern research was SKIPPED (greenfield project). Conventions are established from scratch per the spec's project structure and the decomposition's consistent use of: dataclasses for models, BaseFetcher ABC for fetchers, enrich() standard interface for enrichment sources, server components for reads and server actions for writes in Next.js.

## Gaps (RESOLVED 2026-03-20)

All three gaps identified during decomposition have been resolved and incorporated into crumb acceptance criteria:

1. **Stale score re-scoring trigger** — RESOLVED in seek-13, seek-14. Auto re-score on pipeline run: `get_stale_scored_jobs()` in scorer.py detects profile_hash mismatches and includes stale jobs in the Pass 1 pool alongside unscored jobs. Pass 2 similarly re-scores stale deep analysis results.

2. **Profile suggestion apply mechanism** — RESOLVED in seek-15. Pipeline applies on next run: `apply_approved_suggestions(db_connection, profile_yaml_path)` in profile_evolution.py reads approved suggestions, applies JSON diffs to profile.yaml atomically (temp file + rename), and marks them as applied. Conflict handling included (status='conflict' if target key missing).

3. **Pass 1 overflow batching** — RESOLVED in seek-13. Parallel batches: when unscored + stale pool exceeds 40, `split_into_batches()` divides into 40-job batches and runs up to 3 subagents in parallel.

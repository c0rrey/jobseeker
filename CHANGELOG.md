# Changelog

## 2026-03-20 — Session 20260320-124152 (V2 Pipeline: Fetchers, Filters, Web UI + P1/P2 Bug Fixes)

### Summary

Two-phase session completing the core V2 feature set and immediately resolving all review-surfaced P1/P2 bugs. Phase 1 implemented 18 tasks across 3 parallel/serial waves: fetcher adapters (Adzuna, RemoteOK, LinkedIn, ATS feed, career page), pipeline stages (deduplicator, pre-filter, enrichment sources, resume sync, Pass 1 scorer, profile evolution, company discovery), and the complete Next.js web UI (dashboard, jobs list, job detail, companies, profile, feedback history). Phase 2 fixed all 4 P1s and 8 P2s surfaced by Nitpicker review (normalizer `source_type` gap, career page company injection, ATS source filter drift, scrape strategy key mismatch, plus 8 edge-case/correctness fixes). 8 P3 findings filed as seek-47–seek-54. 28 commits, 56 files changed, +11,823/−64 lines.

### Implementation — Wave 1 (7 agents parallel)

- **seek-4**: feat: adapt Adzuna and RemoteOK fetchers for V2 schema; add abstract `source_type` property to `BaseFetcher`
- **seek-8**: feat: rewrite deduplicator for DB-backed URL and fuzzy hash dedup
- **seek-9**: feat: add deterministic pre-filter stage adapted from V1 filter module
- **seek-10**: feat: create enrichment source modules (Crunchbase, Glassdoor, Levels.fyi, StackShare)
- **seek-12**: feat: implement resume sync with SHA256 change detection and pdfplumber extraction
- **seek-13**: feat: create Pass 1 scoring with fast-filter prompt and Claude API orchestration
- **seek-33**: fix: resolve `DB_PATH` empty-string edge case with `||` fallback (deferred from prior session)

### Implementation — Wave 2 (4 agents parallel)

- **seek-5+6+7** (batch): feat: add LinkedIn fetcher (RapidAPI), ATS feed fetcher (Greenhouse/Lever/Ashby), and career page crawler (bs4 CSS selectors); add five normalizer functions
- **seek-15**: feat: implement profile evolution analysis and suggestion generation
- **seek-16**: feat: implement company discovery with ATS detection and LLM scrape strategy
- **seek-18+19** (batch): feat: build dashboard page (stats/alerts/top matches) and jobs list page (sortable/filterable table with dimension bars)

### Implementation — Wave 3 (1 agent serial)

- **seek-20+21+22+23** (batch): feat: build job detail page (Recharts radar chart), companies management page, profile page with skills comparison, feedback history page; add `actions.ts` server actions for all mutations

### Fix Cycle — P1 Bugs (4 tasks)

- **seek-35**: fix: add `source_type` to V1 normalizers (`normalize_adzuna`, `normalize_remoteok`, `normalize_mock`)
- **seek-36**: fix: align `company_discovery.md` scrape_strategy keys with `CareerPageFetcher` expectations
- **seek-37**: fix: replace `ats_feed` source filter with platform-specific values (greenhouse, lever, ashby)
- **seek-38**: fix: inject `_company_name` into `CareerPageFetcher` output dict and read in `normalize_career_page`

### Fix Cycle — P2 Bugs (8 tasks)

- **seek-39**: fix: clarify `crunchbase_data` column stores Levels.fyi compensation data
- **seek-40**: fix: replace bare `except:` with specific exceptions in `adzuna._fetch_page`
- **seek-41**: fix: guard against explicit `None` in nested dict access in normalizers
- **seek-42**: fix: guard against empty LLM content list before indexing in `company_discovery`
- **seek-43**: fix: chunk `IN`-clause queries into batches of 500 to avoid SQLite variable limit
- **seek-44**: fix: check `fs.existsSync(DB_PATH)` before opening with `better-sqlite3`
- **seek-45**: fix: handle both array and object formats in `parseSkills` for `extracted_skills`
- **seek-46**: fix: correct column name in `resume_parser.md` prompt (`content` → `suggested_change`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|-----|---------|
| 1 | Implementation output | 4 | 8 | 7 | PASS WITH ISSUES |
| 2 | Fix cycle output | 0 | 0 | 1 | PASS |

22 raw findings consolidated to 20 root causes. All P1/P2 fixed in-session. 8 P3 items filed (seek-47–seek-54).

---

## 2026-03-20 — Session 20260320-111254 (V2 Foundation: Database, Models, Settings, Web Scaffold)

### Summary

Four foundational tasks were completed in a single parallel wave: the V2 SQLite database module (7 tables, WAL mode, 11 indices), extended Python data models (6 new dataclasses, 6 new Job fields), updated settings module with dotenv loading and new API key getters, and a full Next.js 16 web scaffold with better-sqlite3, Tailwind v4, and shadcn/ui. Three Nitpicker review rounds followed, surfacing 5 P2 drift issues and 2 P1+P2 regressions introduced by the fix commits themselves. All 7 P1/P2 findings were fixed in-session. One deferred R3 P1 — normalizer.py missing required `source_type` argument (seek-34) — was filed for the next session. 9 commits, 44 files changed.

### Implementation (Wave 1 — 4 agents parallel)

- **seek-1**: feat: add SQLite database module with 7-table V2 schema (`pipeline/src/database.py`, `pipeline/tests/test_database.py`)
- **seek-2**: feat: extend data models with V2 dataclasses and Job fields (`pipeline/src/models.py`, `pipeline/tests/test_models.py`, `pipeline/tests/__init__.py`)
- **seek-3**: feat: extend settings with dotenv loading and new API key getters (`pipeline/config/settings.py`, `.env.example`, `.gitignore`, `pipeline/tests/test_settings.py`, `pipeline/tests/conftest.py`)
- **seek-17**: feat: scaffold Next.js app with SQLite connection and shared layout (`web/` — 30+ files including `lib/db.ts`, `lib/types.ts`, `app/layout.tsx`, shadcn/ui components)

### Review Fixes (R1 — 5 P2 root causes)

- **RC-1 + RC-2 (seek-26, seek-27)**: fix: anchor `DB_PATH` to `__dirname` and add `busy_timeout` pragma (`web/lib/db.ts`, `web/next.config.ts`)
- **RC-3 + RC-4 (seek-28, seek-29)**: fix: rename `Job.raw` → `raw_json`, make `description` Optional (`pipeline/src/models.py`, `pipeline/tests/test_models.py`)
- **RC-5 (seek-30)**: fix: unify test import strategy with `pyproject.toml pythonpath` (`pyproject.toml`, `pipeline/tests/conftest.py`)

### Review Fixes (R2 — 1 P1, 1 P2)

- **RC-A (seek-31)**: fix: update all `normalizer.py` `Job()` calls from `raw=` to `raw_json=json.dumps()` (`pipeline/src/normalizer.py`)
- **RC-B (seek-32)**: fix: guard `job.raw` property against `JSONDecodeError` (`pipeline/src/models.py`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|-----|---------|
| 1 | 4 tasks, fed14bf..bc5ae55 | 0 | 5 | 6 | PASS WITH ISSUES |
| 2 | 5 fix tasks, bc5ae55..dafe334 | 1 | 1 | 1 | PASS WITH ISSUES |
| 3 | 2 fix tasks, dafe334..HEAD | 1 | 0 | 0 | PASS WITH ISSUES (deferred) |

19 raw findings across 3 rounds consolidated to 15 root causes. 7 P1/P2 root causes fixed in-session; 1 P3 (seek-33, DB_PATH `??` edge case) deferred; 1 P1 (seek-34, normalizer.py `source_type` gap) deferred to next session.

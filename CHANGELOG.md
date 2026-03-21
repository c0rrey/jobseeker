# Changelog

## 2026-03-21 — Session 20260321-164550 (Scorer File-Based Output + Code Quality Fixes + 2-Round Review)

### Summary

Session completed 16 tasks across three implementation waves plus a two-round review cycle. Wave 1 refactored LLM scoring in `scorer.py` to a file-based intermediate output pattern, eliminating concurrent SQLite write contention. Waves 2 and 3 addressed 12 pre-planned code quality tasks: numeric input validation, stale docstrings, test fixture cleanup, Python 2-style counter idioms, and defensive guards in `company_discovery.py` and `duplicate_detector.py`. An unlabeled commit also introduced a title similarity gate and a new `fetch_descriptions_ldjson.py` script. Round 1 review found 3 P2 and 13 P3 issues; the 3 P2s were auto-fixed and verified clean in round 2. The 13 P3s were filed as seek-147 through seek-159 for a future cleanup session. 14 commits total.

### Implementation (Waves 1–3)

- **seek-140**: refactor: file-based LLM scoring output with sequential DB upsert — split write/upsert responsibilities in `scorer.py`; restructure `test_scorer.py` and `test_scorer_deep.py` (`cb6b03e`)
- **seek-112 + seek-116 + seek-133**: fix/docs: numeric input guards in `scorer.py`, `full_description_fetcher.py`, `fetch_descriptions.py`; document `PASS_REJECTED` sentinel; fix stale docstrings in `settings.py`, `enrichment/__init__.py`, `duplicate_detector.py`, `scorer.py` (`f26a022`)
- **seek-113**: docs: add explanatory comment above import alias in `cli.py` (`5e7fff6`)
- **seek-114**: fix(config): add `[SOFT]`/`[HARD]` inline tags to `salary_min`/`salary_floor` in `profile.yaml` (`f80d835`)
- **seek-115 + seek-136**: docs: fix `_apply_connection_settings` docstring and document PRAGMA table_info schema in `database.py` (`4949a63`)
- **seek-132**: fix: replace stale crunchbase source keys with glassdoor/levelsfy in `test_cli.py` fixtures (`079866d`)
- **seek-134**: refactor: replace Python 2-style counters with `itertools.count()` in `test_duplicate_detector.py` and `test_scorer.py` (`c2fe69a`)
- **(unlabeled)**: fix: add title similarity gate to `duplicate_detector.py` and introduce `fetch_descriptions_ldjson.py` LD+JSON fetcher (`1015724`)
- **seek-135 + seek-137**: fix: rename `is_zero_rating` → `is_no_data`; add `hasattr` guard for non-text LLM content blocks in `company_discovery.py` (`fa21096`)
- **seek-138**: fix: exclude empty-string company names from duplicate grouping in `duplicate_detector.py` (`877a6f9`)

### Review Fixes (Round 1)

- **seek-146**: fix: raise `ValueError` for `n_batches<=0` in `split_into_batches`; add two new tests (`ff69553`)
- **seek-145**: fix: iterate all content blocks in `_call_llm` to find first `TextBlock`; remove dead phases 3–5; update `test_company_discovery.py` (`64adc8b`, `c281209`)
- **seek-144**: fix: rename `salary_min`/`salary_floor` → `salary_target`/`salary_min` across `profile.yaml`, `filter.py`, `adzuna.py`, prompts, and five test files (`ba0b111`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|-----|---------|
| 1 | 13 tasks, 24 files | 0 | 3 | 13 | PASS WITH ISSUES |
| 2 | 3 fix commits | 0 | 0 | 0 | PASS |

16 root causes consolidated (0 merges). All 3 P2s auto-fixed and verified clean. 13 P3s filed as seek-147–seek-159.


## 2026-03-21 — Session 20260321-123003 (Glassdoor Enrichment & Discovery Migration + Content Deduplication — T13/T14/T15)

### Summary

Session completed 11 implementation tasks and 4 P2 review-fix commits across 15 total commits. Three epics advanced to done: T13 migrated enrichment from four paid sources (Crunchbase, StackShare, old Glassdoor partner API) to two (Glassdoor RapidAPI + levels.fyi); T14 replaced the SerpAPI-based company discovery flow with Glassdoor API + career URL probing; T15 introduced content-based deduplication with Union-Find grouping, scorer query filters, and score propagation to group members. One review round ran with 17 raw findings consolidated to 10 root causes (P1: 0, P2: 3, P3: 7); all 3 P2s were fixed same-session. Seven P3 crumbs filed (seek-132 – seek-138). Test suite: 584 passed, 1 pre-existing failure.

### Implementation — T13: Glassdoor RapidAPI Enrichment

- **seek-117**: feat: add Glassdoor RapidAPI enrichment module with 90-call/month budget tracker, response cache, and 7-subrating blob — new `glassdoor_rapidapi.py` (519 lines) (`e35430f`)
- **seek-118**: refactor: update enrichment orchestrator to unconditional 2-source dispatch (glassdoor_rapidapi + levelsfy); remove crunchbase/stackshare imports and `is_crunchbase_enabled` (`69f219e`)
- **seek-119**: refactor: delete stale paid enrichment modules (crunchbase.py, stackshare.py, glassdoor.py) and 3 stale settings functions (`a0d1932`)
- **seek-120**: feat: add 34 tests for Glassdoor enrichment module; update orchestrator tests to 2-source architecture (`c51898d`)

### Implementation — T14: Glassdoor-Based Company Discovery

- **seek-121**: feat: replace SerpAPI discovery with Glassdoor API + career URL probing; new `CompanyRecord` datatype; `_probe_career_url` with HEAD→GET fallback (`03cda63`)
- **seek-122**: refactor: remove all SerpAPI references from discovery module and tests; replace 10 stale test methods with Glassdoor-equivalent tests (`2fa7d15`)
- **seek-123**: test: add Glassdoor discovery coverage (5 unit tests) and CLI discover stage tests (12 tests) (`02fdea6`)

### Implementation — T15: Content-Based Deduplication

- **seek-124**: feat: add `duplicate_detector.py` with Union-Find grouping; DB schema — `job_duplicate_groups` table, `dup_group_id`/`is_representative` columns on `jobs` with migration (`d52ba14`)
- **seek-125**: feat: wire `_DUP_FILTER` into 3 scorer queries and `detect_duplicates()` into `run_fetch()` CLI (`93e2308`)
- **seek-126**: feat: add `propagate_scores(conn, pass_number)` — copies representative scores to all group members via `executemany` (`b3cd6ee`)
- **seek-127**: test: add 35 deduplication tests, 14 scorer duplicate-awareness tests, 4 CLI wiring tests (`5ae5728`)

### Review Fixes (P2 — same session)

- **seek-129**: fix: remove `enriched_at` write from `glassdoor_rapidapi._update_company` (premature timestamp, RC-1) (`2484572`)
- **seek-139**: fix: remove `enriched_at` write from `levelsfy._update_company` (same root cause, proactive) (`fbbd183`)
- **seek-130**: fix: validate `yaml.safe_load` result and guard `get_db_path` against empty-string env var (RC-2) (`238bfc2`)
- **seek-131**: fix: replace misleading sanitization comment and rename `safe_name` in company_discovery (RC-3) (`effd4f2`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|----|---------|
| 1 | 11 tasks, 15 files | 0 | 3 | 7 | PASS WITH ISSUES |

10 root causes consolidated from 17 raw findings (2 cross-session dedups: C-2→seek-113, E-6→seek-112). All 3 P2s auto-fixed same session. 7 P3s filed as seek-132 – seek-138.

## 2026-03-21 — Session 20260320-233459 (Code Quality Sweep — P3 Polish Wave)

### Summary

Session completed 17 tasks across one parallel implementation wave and one review fix cycle. All 15 Wave 1 tasks were P3 fixes inherited from the prior session (seek-95 through seek-109), addressing type annotation style, defensive hardening, connection lifecycle, dead code, CLI argument consistency, and documentation accuracy. Two review rounds ran: round 1 found 2 P2 issues (misleading `get_connection` context manager docstring and a missing `try/finally` in `fetch_descriptions.py:run()`) plus 5 P3 items; both P2s were fixed in a same-session fix wave (seek-110, seek-111); round 2 returned a CLEAN PASS. 5 P3 issues filed as seek-112 through seek-116 for the next session. 20 commits, 17 tasks closed.

### Implementation (Wave 1 — Code Quality, 4 parallel agents)

- **seek-98**: refactor: replace `Optional[str]` with `str | None` union syntax in `full_description_fetcher.py` (`21ac94a`)
- **seek-95**: fix(prompts): clarify soft vs hard salary threshold roles in `fast_filter.md` and `profile.yaml` comments (`84089ba`)
- **seek-96**: fix: remove dead `skipped` variable and its always-zero log reference in `fetch_descriptions.py` (`f1f1708`)
- **seek-97**: fix: rename `--db-path` to `--db` in `fetch_descriptions.py` to match CLI convention (`334d3bf`)
- **seek-99**: refactor: move misplaced helpers under correct section headers in `database.py` and `scorer.py` (`31ad864`)
- **seek-100**: fix: document `pass=0` sentinel as `PASS_REJECTED` constant in `scorer.py` (`3868837`)
- **seek-101**: docs: add `reasoning` field to `write_pass1_results` docstring in `scorer.py` (`19bcd26`)
- **seek-102**: fix: add `try/finally` connection cleanup to `run_discover` in `cli.py` (`ad9340b`)
- **seek-103**: fix: guard `sources_succeeded`/`sources_failed` against explicit `None` in summary printers (`da8aaa6`)
- **seek-104**: fix: use `time.monotonic()` instead of `time.time()` in rate limiter (`316eabd`)
- **seek-105**: fix: guard against `None` URL in `fetch_full_description` before attribute access (`b07cbd3`)
- **seek-106**: fix: reject non-positive `limit` values in `fetch_descriptions` with `ValueError` (`76cf903`)
- **seek-107**: fix: raise `FileNotFoundError` on missing DB path in `get_connection` instead of silent creation (`769cb79`)
- **seek-108**: fix: correct regex ordering in `_clean_text` — second substitution was a no-op (`d91f57f`)
- **seek-109**: fix: replace stale line-number cross-reference with constant name in `scorer.py` (`73c6118`)

### Review Fixes (Round 1 — P2s)

- **seek-110**: fix: clarify `get_connection` docstring — sqlite3 context manager commits/rolls back only, does not close (`c05c6dd`)
- **seek-111**: fix: wrap `run()` body in `try/finally` to guarantee `conn.close()` in `fetch_descriptions.py` (`b036084`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|----|---------|
| 1 | 15 tasks, 8 files | 0 | 2 | 5 | PASS WITH ISSUES |
| 2 | 2 fix tasks, 2 files | 0 | 0 | 0 | CLEAN PASS |

7 root causes consolidated from 10 raw findings. Both P2s auto-fixed same session; 5 P3s filed as seek-112 through seek-116.

## 2026-03-20 — Session 20260320-220445 (Full-Description Enrichment, Company Discovery, Salary Templating)

### Summary

Session completed 9 tasks across three implementation waves and one review fix cycle. The main feature work added the full-description fetcher ported from v1.5, wired COALESCE fallback into the LLM scorer, introduced company auto-discovery from Pass 1 survivors, integrated it into the CLI, and made fast_filter salary thresholds configurable via template variables. Two review rounds ran: round 1 found 4 P2 issues (documentation drift, missing config variable, stale truncation lengths, None-verdict logic bug) and 15 P3 items; all 4 P2s were fixed in a same-session fix wave; round 2 returned a CLEAN PASS. 15 P3 issues were filed as seek-95 through seek-109 for the next session. 10 commits, 9 tasks closed.

### Implementation (Waves 1–3 — Feature)

- **seek-88**: feat(pipeline): port full-description fetcher from v1.5, add full_description column — new `full_description_fetcher.py`, `fetch_descriptions.py`, `scripts/__init__.py`; DB migration in `database.py`; `beautifulsoup4` added to `requirements.txt` (`47f141b`)
- **seek-90**: feat: replace hardcoded salary thresholds in fast_filter.md with template variables — `fast_filter.md` lines 22 and 28 use `{{ salary_min }}` and `{{ salary_floor }}` (`d2e09a6`)
- **seek-86**: feat: add company auto-discovery script for Pass 1 survivors — new `discover_companies.py` with SQL anti-join idempotency and `run_enrichment()` integration (`2f36a2d`)
- **seek-89**: feat: prefer full_description in LLM scorer payload with length caps — `scorer.py` SQL queries updated with `COALESCE` + new `FAST_FILTER_DESC_CHARS`/`DEEP_SCORER_DESC_CHARS` constants (`ece3ddd`)
- **seek-87**: feat(cli): add --discover flag wiring company auto-discovery into CLI — `cli.py` gains `run_discover()`, `_print_discover_summary()`, `--discover` flag; `--all` explicitly excludes it (`01767b6`)
- **seek-86/87/88** (docs): docs: update README with new pipeline stages and scripts — `README.md` and `Makefile` updated (`3c11892`)

### Review Fixes (Round 1 — P2s)

- **seek-91**: fix(cli): correct --all stage order documentation to fetch→prefilter→enrich — `cli.py` docstring, argparse help, `Makefile`, `test_cli.py` (`cac1e12`)
- **seek-92**: fix(config): add salary_floor to profile.yaml — `profile.yaml` gains `salary_floor: 120000` (`4f1ab0f`)
- **seek-93**: fix(prompts): update truncation lengths to match scorer.py constants — `fast_filter.md` → "4000 characters", `deep_scorer.md` → "8000 characters" (`c186a56`)
- **seek-94**: fix(scorer): use allowlist for verdict so None/unknown default to reject — `scorer.py` verdict check changed to `overall = confidence if verdict == "yes" else 0` (`b2e4830`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|----|---------|
| 1 | 5 tasks, 13 files | 0 | 4 | 15 | PASS WITH ISSUES |
| 2 | 4 fix tasks, 7 files | 0 | 0 | 0 | CLEAN PASS |

19 root causes consolidated from 22 raw findings. All 4 P2s auto-fixed same session; 15 P3s filed as seek-95 through seek-109.

## 2026-03-20 — Session 20260320-181429 (CLI Entry Point, Code Quality Fixes, Dual Fix Cycle)

### Summary

Session completed 18 tasks across one feature wave and two fix cycles. Wave 1 delivered the pipeline CLI entry point (seek-24: `--fetch`, `--enrich`, `--prefilter`, `--all` with 37 tests) and 10 code quality fixes across fetchers, filter, scorer, and enrichment modules. Two review rounds ran: round 1 found a P1 data-loss bug (seek-70 partial `skills_gap→skills_match` rename left the DB INSERT broken) plus 4 P2s — all 5 fixed. Round 2 found a P1 ImportError in `test_cli.py` after the seek-77 rename plus a P2 mock semantics break — both fixed. 16 commits, 18 tasks closed.

### Implementation (Wave 1 — Feature + Code Quality)

- **seek-24**: feat: CLI entry point with `--fetch`/`--enrich`/`--prefilter`/`--all` stage flags; 37 tests (`8dda4be`)
- **seek-63/68**: fix: validate RemoteOK API response type and align `User-Agent` header (`0f467c5`)
- **seek-64**: fix: preserve exception chain in Adzuna error handler (`c37f79e`)
- **seek-65**: docs: rewrite `filter.py` module docstring (`04a55e1`)
- **seek-66**: refactor: rename `should_filter` → `has_red_flags` (`16b736d`)
- **seek-67**: refactor: convert f-string logger calls to lazy `%s`/`%d` formatting in fetchers (`cfcf125`)
- **seek-69**: fix: move `urlparse` import to module top level (`9f8b901`)
- **seek-70**: fix: rename `skills_gap` → `skills_match` in scorer and prompt (`c786d0c`)
- **seek-71**: fix: grammatically correct company count pluralization (`cf23498`)
- **seek-72**: fix: guard non-dict JSON in levelsfy Crunchbase merge (`9101171`)

### Review Fixes (Round 1 — P2/P1 Remediation)

- **seek-73**: fix: complete `skills_gap→skills_match` rename across DB schema, models, profile evolution, tests, design doc — 10+ files (`68ae146`, `488b9c1`)
- **seek-74**: fix: catch `ValueError` (JSONDecodeError) alongside `RequestException` in levelsfy and remoteok (`69e98e5`)
- **seek-75**: fix: move `init_db` inside CLI error boundary
- **seek-76**: refactor: rename `all_raw_jobs` → `all_job_pairs` (`894e23c`)
- **seek-77**: refactor: rename `run_prefilter_stage` → `run_prefilter` (`4305aa6`)

### Review Fixes (Round 2 — Seek-77 Propagation)

- **seek-78/79**: fix: update `test_cli.py` import and `_PATCH_RUN_PREFILTER` mock target after seek-77 rename (`82ddb36`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|-----|---------|
| 1 | 11 tasks, Wave 1 | 1 | 4 | 12 | FIX — 5 crumbs filed (seek-73–77) |
| 2 | 5 fix commits | 1 | 1 | 1 | FIX — 2 crumbs filed (seek-78–79), 1 P3 auto-filed (seek-80) |

26 raw findings → 17 root causes in round 1. 4 raw findings → 3 root causes in round 2. All P1/P2 fixed in-session.

### Open Issues

- **seek-25** (P1): Makefile + requirements files — unblocked now that seek-24 is closed
- **seek-80** (P3): `profile_evolution.md:35` description reads "skills gap score" instead of "skills match score"

---

## 2026-03-20 — Session 20260320-160447 (Enrichment Orchestrator, Deep Scorer, Pipeline Fix Cycle)

### Summary

Session completed 18 tasks across two waves: 10 feature and bug-fix tasks in wave 1 (enrichment orchestrator, deep scoring Pass 2, logging standardization, career page LEFT JOIN, server actions validation) and 8 P2 fix tasks generated by round-1 code review. Two review rounds ran; round 1 found 8 P2 root causes (all fixed), round 2 verified all 8 fixes and found 1 new P3 (auto-filed). 10 P3 polish crumbs were deferred to the backlog. Session closed with 17 commits across 17 files.

### Implementation (Wave 1 — Feature and Bug Fixes)

- **seek-48**: fix: rename `normalise` → `normalize` in `web/components/skills-comparison.tsx` (6 occurrences) (`35192e5`)
- **seek-47/49/50/53**: fix: replace `print()` with structured logger calls, remove emoji, fix naive datetime UTC comparison, fix None interpolation in `filter.py` — `adzuna.py`, `remoteok.py`, `filter.py` (`62c177a`)
- **seek-54**: fix: use LEFT JOIN for career_page_configs to include orphaned rows; add warning log for NULL company names — `career_page.py` (`01ab194`)
- **seek-51**: fix: replace truthiness checks with `Number.isNaN` in server actions — `actions.ts` (`0d74876`)
- **seek-52**: fix: add `company_id` parameter to enrichment `enrich()` signatures, switch `UPDATE WHERE` to `id` — `crunchbase.py`, `glassdoor.py`, `levelsfy.py`, `stackshare.py`, `test_enrichment_sources.py` (`136e303`, `f6eaaec`)
- **seek-11**: feat: create enrichment orchestrator with exponential-backoff source dispatch, staleness selection, 27 tests — `orchestrator.py`, `test_enrichment_orchestrator.py` (`ff25c57`, `43bb030`)
- **seek-14**: feat: create deep scoring Pass 2 prompt and orchestration, 5 dimensions with weights, 35 tests — `deep_scorer.md`, `scorer.py`, `test_scorer_deep.py` (`2c4789b`)

### Review Fixes (Round 1 — P2 Remediation)

- **seek-55**: fix: use `company_id` in `_update_enriched_at` to prevent name-collision bug — `orchestrator.py` (`3e85209`)
- **seek-56/57/58/59**: fix: per-result LLM validation, levelsfy JSON merge (preserve crunchbase_data), schema gaps (glassdoor_url, salary_currency), prefilter exclusion in `get_unscored_jobs` — `scorer.py`, `levelsfy.py`, `deep_scorer.md`, `test_scorer_deep.py` (`3f08fad`)
- **seek-60**: fix: handle null categories in crunchbase enrichment — `crunchbase.py` (`b5b4253`)
- **seek-61**: fix: handle City, ST end-of-string format in Florida location filter — `filter.py` (`346e91f`)
- **seek-62**: fix: replace `Number.isNaN` with `!id` validation in `actions.ts` to also reject null→0 (`2e7cce9`)
- **seek-55 through seek-62**: chore: update crumb state — close all 8 fix crumbs (`544a5ad`)

### Review Statistics

| Round | Scope | P1 | P2 | P3 | Verdict |
|-------|-------|----|----|-----|---------|
| 1 | 10 tasks, 17 files | 0 | 8 | 9 | PASS WITH ISSUES |
| 2 | 8 fix commits | 0 | 0 | 1 | PASS |

17 root causes consolidated in round 1 (8 merges from 25 raw findings). All 8 P2 fixes verified 24/24 acceptance criteria in round 2. 10 P3 findings deferred to backlog (seek-63 through seek-72).

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

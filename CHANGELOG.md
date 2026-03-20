# Changelog

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

<!-- ant-farm:start -->
# Global User Instructions

## Parallel Work Mode ("Let's get to work")

**Trigger**: When the user says "let's get to work" (case-insensitive, anywhere in message).

**CRITICAL — read before doing ANYTHING:**
- **NEVER** call the `crumb_show`, `crumb_ready`, `crumb_list`, `crumb_blocked` MCP tools (or their CLI equivalents `crumb show`, `crumb ready`, `crumb list`, `crumb blocked`), or any other `crumb` query command — the Recon Planner does this
- NEVER read task/issue details from the user's message and act on them directly.
- NEVER set `run_in_background` on Task agents. Multiple Task calls in one message already run concurrently. Background mode causes raw JSONL transcript leakage into your context.
- Read `~/.claude/orchestration/RULES.md` FIRST and ALONE — no parallel tool calls. Then follow it.

**Process**: Read `~/.claude/orchestration/RULES.md` and follow the workflow steps. RULES.md contains the step sequence, hard gates, concurrency rules, and a template lookup table pointing to the specific template files needed at each phase.

**Process Documentation:** See `~/.claude/orchestration/` for detailed workflows:
- `RULES.md` — Workflow steps, hard gates, concurrency rules (always loaded)
- `templates/` — Agent prompts, checkpoints, reviews (read on demand)
- `reference/` — Dependency analysis, known failures (read when needed)

**Key rule**: After startup-check PASS, the Orchestrator auto-proceeds to Step 2. No user approval required for execution strategy.

## Plan Mode ("/ant-farm-plan")

**Trigger**: When the user invokes `/ant-farm-plan`, `/ant-farm-plan <spec>`, or `/ant-farm-plan --prd <file>`.

**CRITICAL — read before doing ANYTHING:**
- Read `~/.claude/skills/plan.md` FIRST and ALONE — no parallel tool calls. Then follow it.

**Input modes**:
- `/ant-farm-plan <idea or inline text>` — freeform: Spec Writer asks clarifying questions and writes spec.md
- `/ant-farm-plan path/to/spec.md` — structured spec: Spec Writer skipped; user may optionally run Researchers
- `/ant-farm-plan --prd path/to/prd.md` — PRD import: Spec Writer skipped; PRD Importer extracts requirements into spec.md format and confirms with user before Researchers spawn

**When to use `--prd`**: Use when you have an existing Product Requirements Document to decompose directly. The Spec Writer is skipped entirely. Use standard invocation when starting from scratch or from a lightweight outline.

**Key rule**: All three input modes feed the same Researcher and Task Decomposer pipeline. The spec.md format is identical regardless of source.

## Lite Mode ("ant-farm-quick")

**Trigger**: When the user invokes `/ant-farm-quick` or `/ant-farm-quick <crumb-id>`.

**CRITICAL — read before doing ANYTHING:**
- Read `~/.claude/skills/quick.md` FIRST and ALONE — no parallel tool calls. Then follow it.
- Do NOT trigger this for "let's get to work" — that phrase triggers full mode only.

**Process**: Read `~/.claude/skills/quick.md` and follow the workflow. The skill validates the crumb (or prompts for one), then reads `~/.claude/orchestration/RULES-lite.md` and executes the lite mode pipeline.

**Key rule**: Lite mode is opt-in. Full mode (`/ant-farm-work`) remains the default for multi-crumb sessions.

## Landing the Plane (Session Completion)

(Corresponds to RULES.md Step 6.)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Review-findings gate** — If reviews ran and found P1 issues, present findings to user before proceeding. User decides: fix now, or document deferred P1s in CHANGELOG and push. Do NOT push with undisclosed P1 blockers. If no reviews ran or no P1s exist, proceed.
4. **Update issue status** - Close finished work, update in-progress items
5. **Run Session Scribe** — Spawn the Session Scribe (`ant-farm-session-scribe`, `model: "sonnet"`) to write `{SESSION_DIR}/exec-summary.md` and prepend a CHANGELOG entry. Use `orchestration/templates/scribe-skeleton.md` (repo path; synced to `~/.claude/` by setup.sh) as the prompt template. Commit CHANGELOG.md only — NEVER `git add` any file under `.crumbs/` (the entire directory is gitignored).
6. **Session-complete gate** — Spawn the Checkpoint Auditor (`ant-farm-checkpoint-auditor`, `model: "haiku"`) for Exec Summary Verification. Pass `{SESSION_DIR}` and `orchestration/templates/checkpoints/common.md` + `orchestration/templates/checkpoints/session-complete.md`. session-complete must PASS before pushing. On FAIL: re-spawn Session Scribe with violations (max 1 retry); if still failing, present to user.
7. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
8. **Sync global runtime files** — Run `./scripts/setup.sh` to copy any changed orchestration files, agents, skills, and scripts to `~/.claude/`.
9. **Clean up** - Clear stashes, prune remote branches
   (Session artifacts in .crumbs/sessions/_session-*/ are retained for posterity. Prune old sessions manually when needed.)
10. **Verify** - All changes committed AND pushed
11. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- ant-farm:end -->

## jseeker Pipeline Reference

### Architecture

jseeker is a job search automation platform: Python pipeline (fetchers, filters, LLM scoring) + Next.js 16 dashboard. SQLite with WAL mode for concurrent reads/writes.

### Pipeline stages (in order)

| Stage | Command | What it does |
|-------|---------|--------------|
| **fetch** | `python3 -m pipeline.cli --fetch` | Pull from Adzuna, RemoteOK, LinkedIn, ATS feeds, career pages → dedup → insert |
| **prefilter** | `python3 -m pipeline.cli --prefilter` | Deterministic filters: salary floor, location, seniority, red flags. Writes `pass=0` rejection rows to `score_dimensions` |
| **Pass 1 scoring** | Manual — spawn parallel agents | Fast filter via LLM. Batches of ~40 jobs. Agents write JSON to `data/pass1_results/`, then upsert sequentially |
| **fetch-descriptions** | `python3 -m pipeline.scripts.fetch_descriptions --rate-limit 1.5` | Fetch full descriptions for Pass 1 survivors. Use `--rate-limit 1.5` to avoid Adzuna throttling. Optional `--since DATETIME` limits to jobs fetched after the given timestamp |
| **Pass 2 scoring** | Manual — spawn parallel agents | Deep 5-dimension scoring via LLM. Batches of ~25. Agents write JSON to `data/pass2_results/`, then upsert sequentially |
| **enrich** | `python3 -m pipeline.cli --enrich` | Glassdoor + levels.fyi + Crunchbase data for companies |
| **discover** | `python3 -m pipeline.cli --discover` | Find new companies from Pass 1 survivors |

### Scoring agent workflow

Scoring (Pass 1 and Pass 2) runs via Claude Code subagents, NOT the CLI. The pattern:

1. **Prepare batches**: Use `scorer.get_unscored_jobs()` or `scorer.get_pass1_survivors(conn, since=...)` + `scorer.split_into_batches()`. `count_pass2_eligible(conn, since=...)` returns the total count without fetching rows
2. **Write batch files**: JSON to `/tmp/pass{1,2}_batch_{N}.json`
3. **Spawn parallel agents**: One per batch, model=sonnet. Each agent reads the batch + profile + prompt template, scores, and calls `write_pass1_results()` / `write_pass2_results()`
4. **Upsert sequentially**: After ALL agents complete, call `upsert_pass1_results_from_files()` / `upsert_pass2_results_from_files()` once in the main process
5. **Pass 2 reasoning field**: The `reasoning` field must be a JSON **string**, not a dict. If agents return dicts, serialize them before upsert: `result["reasoning"] = json.dumps(result["reasoning"])`

### Known gotchas

- **Adzuna `land/ad/` URLs**: These redirect URLs always return 403. The description fetcher skips them automatically. The dedup representative selector prefers jobs with `details/` URLs so the representative can have its description fetched.
- **Location filter** (`pipeline/src/filter.py:is_allowed_location`): Profile-driven — reads `preferred_locations` from `profile.yaml` and derives the allowlist dynamically (city name, state name/abbreviation, county synonyms). Remote keywords always accepted. To add a new target location, update `preferred_locations` in `profile.yaml` only — no code changes needed.
- **Adzuna fetcher locations** (`pipeline/src/fetchers/adzuna.py`): Reads `preferred_locations` from `profile.yaml` and builds location queries dynamically. "Remote" entries are skipped (remote jobs captured by city searches). To add a new city, update `preferred_locations` in `profile.yaml` only.
- **`--fetch-descriptions` not in CLI**: The `--rate-limit` and `--since` flags are only available via `python3 -m pipeline.scripts.fetch_descriptions`, not `python3 -m pipeline.cli --fetch-descriptions`.
- **Prefilter rejections are sticky**: Jobs rejected by prefilter get a `pass=0, overall=-1` row in `score_dimensions`. If you change filter rules and want jobs re-evaluated, you must DELETE the old rejection rows first, then re-run `--prefilter`.
- **Dedup representative timing**: Dedup runs during `--fetch` before descriptions are fetched. Representative selection uses URL format (prefers `details/` over `land/ad/`), not `full_description` presence.
- **react-is**: recharts peer dependency. If `npm install` in `web/` doesn't pull it, run `npm install react-is` manually.

### Timeouts

- `--fetch` and `fetch_descriptions`: use `timeout: 600000` (10m) — fetches routinely take 3–5 minutes
- Default 2m Bash timeout will cut these off mid-run

### Webapp

```bash
cd web && npm run dev   # http://localhost:3000
```

The Makefile `web` target must be run from the project root. `npm run dev` must be run from `web/`.

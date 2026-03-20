<\!-- ant-farm:start -->
# Global User Instructions

## Parallel Work Mode ("Let's get to work")

**Trigger**: When the user says "let's get to work" (case-insensitive, anywhere in message).

**CRITICAL — read before doing ANYTHING:**
- **NEVER** run `crumb show`, `crumb ready`, `crumb list`, `crumb blocked`, or any `crumb` query command — the Scout does this
- NEVER read task/issue details from the user's message and act on them directly.
- NEVER set `run_in_background` on Task agents. Multiple Task calls in one message already run concurrently. Background mode causes raw JSONL transcript leakage into your context.
- Read `~/.claude/orchestration/RULES.md` FIRST and ALONE — no parallel tool calls. Then follow it.

**Process**: Read `~/.claude/orchestration/RULES.md` and follow the workflow steps. RULES.md contains the step sequence, hard gates, concurrency rules, and a template lookup table pointing to the specific template files needed at each phase.

**Process Documentation:** See `~/.claude/orchestration/` for detailed workflows:
- `RULES.md` — Workflow steps, hard gates, concurrency rules (always loaded)
- `templates/` — Agent prompts, checkpoints, reviews (read on demand)
- `reference/` — Dependency analysis, known failures (read when needed)

**Key rule**: After SSV PASS, the Queen auto-proceeds to Step 2. No user approval required for execution strategy.

## Landing the Plane (Session Completion)

(Corresponds to RULES.md Step 6.)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Review-findings gate** — If reviews ran and found P1 issues, present findings to user before proceeding. User decides: fix now, or document deferred P1s in CHANGELOG and push. Do NOT push with undisclosed P1 blockers. If no reviews ran or no P1s exist, proceed.
4. **Update issue status** - Close finished work, update in-progress items
5. **Run Scribe** — Spawn the Scribe (`technical-writer`, `model: "sonnet"`) to write `{SESSION_DIR}/exec-summary.md` and prepend a CHANGELOG entry. Use `orchestration/templates/scribe-skeleton.md` as the prompt template. Commit the Scribe output.
6. **ESV gate** — Spawn Pest Control (`pest-control`, `model: "haiku"`) for Exec Summary Verification. Pass `{SESSION_DIR}` and `orchestration/templates/checkpoints/common.md` + `orchestration/templates/checkpoints/esv.md`. ESV must PASS before pushing. On FAIL: re-spawn Scribe with violations (max 1 retry); if still failing, present to user.
7. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
8. **Clean up** - Clear stashes, prune remote branches
   (Session artifacts in .crumbs/sessions/_session-*/ are retained for posterity. Prune old sessions manually when needed.)
9. **Verify** - All changes committed AND pushed
10. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<\!-- ant-farm:end -->

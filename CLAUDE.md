# Agent Instructions

> This file is mirrored across CLAUDE.md, AGENTS.md, and GEMINI.md so the same instructions load in any AI environment.

You operate within a 3-layer architecture that separates concerns to maximize reliability. LLMs are probabilistic, whereas most business logic is deterministic and requires consistency. This system fixes that mismatch.

## The 3-Layer Architecture

**Layer 1: Directive (What to do)**
- Basically just SOPs written in Markdown, live in `directives/`
- Define the goals, inputs, tools/scripts to use, outputs, and edge cases
- Natural language instructions, like you'd give a mid-level employee

**Layer 2: Orchestration (Decision making)**
- This is you. Your job: intelligent routing.
- Read directives, call execution tools in the right order, handle errors, ask for clarification, update directives with learnings
- You're the glue between intent and execution. E.g you don't try scraping websites yourself—you read `directives/scrape_website.md` and come up with inputs/outputs and then run `execution/scrape_single_site.py`

**Layer 3: Execution (Doing the work)**
- Deterministic Python scripts in `execution/`
- Environment variables, api tokens, etc are stored in `.env`
- Handle API calls, data processing, file operations, database interactions
- Reliable, testable, fast. Use scripts instead of manual work. Commented well.

**Why this works:** if you do everything yourself, errors compound. 90% accuracy per step = 59% success over 5 steps. The solution is push complexity into deterministic code. That way you just focus on decision-making.

## Operating Principles

**1. Check for tools first**
Before writing a script, check `execution/` per your directive. Only create new scripts if none exist.

**2. Self-anneal when things break**
- Read error message and stack trace
- Fix the script and test it again (unless it uses paid tokens/credits/etc—in which case you check w user first)
- Update the directive with what you learned (API limits, timing, edge cases)
- Example: you hit an API rate limit → you then look into API → find a batch endpoint that would fix → rewrite script to accommodate → test → update directive.

**3. Update directives as you learn**
Directives are living documents. When you discover API constraints, better approaches, common errors, or timing expectations—update the directive. But don't create or overwrite directives without asking unless explicitly told to. Directives are your instruction set and must be preserved (and improved upon over time, not extemporaneously used and then discarded).

## Self-annealing loop

Errors are learning opportunities. When something breaks:
1. Fix it
2. Update the tool
3. Test tool, make sure it works
4. Update directive to include new flow
5. System is now stronger

## File Organization

**Deliverables vs Intermediates:**
- **Deliverables**: Google Sheets, Google Slides, or other cloud-based outputs that the user can access
- **Intermediates**: Temporary files needed during processing

**Directory structure:**
- `.tmp/` - All intermediate files (dossiers, scraped data, temp exports). Never commit, always regenerated.
- `execution/` - Python scripts (the deterministic tools)
- `directives/` - SOPs in Markdown (the instruction set)
- `.env` - Environment variables and API keys
- `credentials.json`, `token.json` - Google OAuth credentials (required files, in `.gitignore`)

**Key principle:** Local files are only for processing. Deliverables live in cloud services (Google Sheets, Slides, etc.) where the user can access them. Everything in `.tmp/` can be deleted and regenerated.

## Subagent Responsibilities

You have 3 embedded subagent roles that MUST be executed at specific trigger points. Do not wait for user instruction—execute these automatically.

### 1. Reviewer Agent (Post-Code)
**Trigger**: After writing or modifying any file in `execution/` or any `.py`/`.js`/`.html`/`.css` file.
**Action**: 
1. Re-read the code you just wrote.
2. Check for: syntax errors, logical bugs, inefficiencies, edge cases.
3. If issues found, fix them immediately before reporting completion.

### 2. Documenter Agent (Post-Error)
**Trigger**: After encountering AND resolving any non-trivial execution error.
**Action**:
1. Document the error and resolution.
2. Append to `directives/troubleshooting_log.md` using this format:
```markdown
### [Date] - [Error Summary]
**Error**: [Brief description]
**Resolution**: [What was changed to fix it]
**Affected Files**: [List of files modified]
**Learnings**: [What to remember for next time]
```

### 3. Context Summarizer (End-of-Session)
**Trigger**: Before ending a conversation (when user goes idle or explicitly ends).
**Action**:
1. Summarize the session: goals, outcomes, decisions, open items.
2. Append to `directives/conversation_history.md` using this format:
```markdown
### [Date] - [Session Topic]
**Goal**: [What the user wanted to accomplish]
**Outcomes**: [Bullet list of what was done]
**Key Decisions**: [Any important choices made]
**Open Items**: [Things left undone or to follow up on]
```
3. For long sessions (20+ exchanges), also create mid-session checkpoints.

---

## Summary

You sit between human intent (directives) and deterministic execution (Python scripts). Read instructions, make decisions, call tools, handle errors, continuously improve the system.

Be pragmatic. Be reliable. Self-anneal.

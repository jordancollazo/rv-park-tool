# Conversation History

> This log summarizes conversations for context continuity. Maintained by the **Context Summarizer Agent**.

---

### 2026-01-06 - Subagent Architecture Implementation
**Goal**: Add 3 auto-running subagents (Reviewer, Documenter, Context Summarizer) to the project.
**Outcomes**: 
- Created implementation plan for embedded subagent behavioral triggers
- Added "Subagent Responsibilities" section to `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`
- Created `directives/troubleshooting_log.md` for error documentation
- Created `directives/conversation_history.md` for session summaries
**Key Decisions**: 
- Subagents implemented as mandatory operating principles (not separate LLM instances)
- Conversation history stored in `directives/` (permanent, version-controlled)
- Only non-trivial errors logged; reviews logged only when issues found
**Open Items**: None

---

### 2026-01-06 - Financial Calculator Tool Build
**Goal**: Build a 3-stage financial modeling tool for MHP/RV park listings (OCR upload → data review → interactive calculator)
**Outcomes**:
- Created full directive: `directives/financial_calculator.md`
- Built Stage 1 OCR script: `execution/financial_calc_ocr.py` (pytesseract + Claude API)
- Built Flask backend: `execution/financial_calc_server.py` with all API endpoints
- Built frontend: `.tmp/calculator_option_1/static/calculator.html` + `calculator.js`
- Created directory structure: `.tmp/calculator_option_1/{uploads,leads,scenarios,exports,static}`
- Added setup automation: `install.bat`, `start_server.bat`
- Comprehensive README with installation, usage, API docs, troubleshooting
**Key Decisions**:
- Module A: Financing structure with bank/seller toggle, balloon payments, interest-only periods, hard money loans
- Module B: LP investment tracker with unlimited investors, pro-rata distributions, Chart.js waterfall visualization
- Uses Claude Sonnet 4.5 for parsing OCR text into structured listing data
- JSON file storage (not database) for leads/scenarios
- Browser print for PDF export (server-side generation deferred)
**Open Items**:
- Install pytesseract, anthropic packages before first run
- User needs Tesseract OCR installed on system
- Future enhancements: Google Vision API fallback, weasyprint PDF generation, IRR/CoC calculations

---

<!-- Entries will be appended below this line -->

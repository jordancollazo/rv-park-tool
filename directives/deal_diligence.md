# Deal Diligence Agent Directive

## Goal
Analyze deal documents for MHP acquisitions using AI to generate comprehensive investment reports and provide interactive Q&A for due diligence.

## Architecture Overview
3-stage workflow:
1. **Document Upload & Processing**: Upload documents → extract text (OCR for images/scanned PDFs, parse Excel/PDFs) → save extracted text
2. **AI Analysis & Report Generation**: Load all documents → call Claude API with analysis prompt → generate markdown report with financial metrics, risks, value-add opportunities
3. **Interactive Q&A**: Chat with AI about the deal → AI references analysis report and documents → maintains conversation history

## Inputs
- Property documents:
  - Offering Memorandums (OMs) - PDF, images
  - Financial Statements (P&L, rent rolls, T12) - Excel, PDF, images
  - Property Photos - JPG, PNG
  - Legal Documents (contracts, leases, environmental studies) - PDF
- CRM lead data (optional, if deal linked to existing lead)
- User questions during chat phase

## Execution Tools

### Stage 1: Document Upload & Processing
**Script**: `execution/diligence_document_processor.py`
- Create new deal: `create_deal("Sunset MHP")`
- Upload documents: `upload_document(deal_id, file_path, doc_type)`
- Extract text from:
  - **PDFs**: PyPDF2 (text-based) → OCR fallback for scanned PDFs (placeholder)
  - **Images**: pytesseract OCR (same pattern as financial_calc_ocr.py)
  - **Excel/CSV**: pandas/openpyxl for data extraction
- Save extracted text to `.tmp/diligence/{deal_id}/documents/extracted/`
- Save metadata with confidence scores: `{filename}_metadata.json`

**Functions**:
- `create_deal(name, lead_id=None)` → returns deal_id
- `upload_document(deal_id, file_path, doc_type)` → extracts text, returns metadata
- `extract_text_from_pdf(path)` → uses PyPDF2
- `extract_text_from_image(path)` → uses pytesseract
- `extract_text_from_excel(path)` → uses pandas
- `get_deal_documents(deal_id)` → list all documents
- `get_deal_metadata(deal_id)` → load metadata.json

### Stage 2: AI Analysis & Report Generation
**Script**: `execution/diligence_analyzer.py`
- Load all extracted documents from `documents/extracted/`
- Build document context (concatenate + structure, prioritize OMs and financials)
- Fetch CRM lead data if deal is linked (via `diligence_crm_lookup.py`)
- Call Claude API (Claude Sonnet 4.5) with comprehensive analysis prompt
- Parse JSON response with structured fields:
  - `executive_summary`
  - `property_overview` (name, location, units, purchase_price, etc.)
  - `financial_analysis` (NOI, cap_rate, cash-on-cash, DSCR, expense_ratio, key_metrics)
  - `deal_structure` (financing type, down payment, interest rate, contingencies)
  - `risk_factors` (category, severity, description, mitigation)
  - `value_add_opportunities` (category, impact, description, estimated_upside, timeline)
  - `red_flags` (list of warnings)
  - `data_gaps` (list of missing information)
  - `next_steps` (recommended actions)
  - `confidence_score` (0-100)
- Generate markdown report: `generate_report_markdown(analysis_data)`
- Save:
  - `analysis/initial_report_raw.json` - structured data
  - `analysis/initial_report.md` - formatted markdown report
- Update deal metadata: `analysis_completed = True`, `status = "analyzed"`

**Functions**:
- `analyze_deal(deal_id)` → generates full analysis, returns analysis_data dict
- `build_document_context(deal_id, max_chars)` → concatenates all extracted documents
- `get_crm_lead_context(deal_id)` → fetches CRM lead data for context
- `generate_report_markdown(analysis_data)` → converts JSON to formatted markdown

**Markdown Report Structure**:
```markdown
# Deal Analysis: {Property Name}

## Executive Summary
{summary}

## Property Overview
- Units/Lots, Purchase Price, Year Built, Lot Size, Occupancy

## Financial Analysis
### Key Metrics
- NOI, Gross Income, Operating Expenses, Cap Rate, Price Per Unit, Cash-on-Cash, DSCR, Expense Ratio
### Analysis Insights
- {list of key metrics with interpretation}

## Deal Structure
- Financing Type, Down Payment, Interest Rate, Loan Term, Contingencies

## Risk Factors
### {Category} - {Severity}
{Description}
**Mitigation**: {mitigation}

## Value-Add Opportunities
### {Category} - {Impact}
{Description}
**Estimated Upside**: {upside}
**Timeline**: {timeline}

## Red Flags
- {list of red flags}

## Data Gaps
- {list of missing data}

## Recommended Next Steps
1. {step 1}
2. {step 2}
```

### Stage 3: Interactive Q&A
**Script**: `execution/diligence_chat.py`
- Start chat session: `start_chat_session(deal_id)`
- Send messages: `send_chat_message(deal_id, "What is the current occupancy rate?")`
- Build chat context from:
  - Deal analysis report (executive summary, key metrics, risks, opportunities)
  - Document list (available documents with types and extraction status)
  - Conversation history (last 10 messages for context window)
- Call Claude API with system prompt:
  - "You are a commercial RE analyst helping evaluate this MHP deal"
  - Include deal summary (property name, location, units, price, NOI, cap rate, confidence score)
  - Include key risks and value-add opportunities
  - Include data gaps
- Maintain conversation history in `chat/chat_history.json`
- Auto-condense context after 20+ messages (keep recent 10 verbatim, summarize old)

**Functions**:
- `start_chat_session(deal_id)` → initializes chat_history.json
- `send_chat_message(deal_id, user_message)` → calls Claude API, appends to history, returns response
- `get_chat_history(deal_id)` → loads all messages
- `build_chat_context(deal_id)` → creates context string for AI
- `clear_chat_history(deal_id)` → resets conversation

### Stage 4: CRM Integration (Optional)
**Script**: `execution/diligence_crm_lookup.py`
- Link deal to CRM lead: `link_deal_to_lead(deal_id, lead_id)`
- Fetch lead data for AI context: `get_lead_data_for_deal(deal_id)` → uses `db.get_lead_by_id()`
- Update lead with diligence summary: `update_lead_with_diligence(lead_id, deal_id, summary)`
  - Update lead status based on confidence score:
    - ≥70: `reviewed_interested`
    - 40-69: `docs_received`
    - <40: keep current status
  - Add note with exec summary, red flags, value-add opportunities
  - Update lead fields: `asking_price`, `lot_count`, `noi`, `cap_rate`
- Get all deals for a lead: `get_deals_for_lead(lead_id)` → filters by lead_id

**Functions**:
- `link_deal_to_lead(deal_id, lead_id)` → updates metadata, adds CRM note
- `get_lead_data_for_deal(deal_id)` → returns lead dict
- `update_lead_with_diligence(lead_id, deal_id, summary)` → syncs back to CRM
- `get_deals_for_lead(lead_id)` → lists all diligence deals for a lead

### Stage 5: Web Interface
**Script**: `execution/diligence_server.py` (Flask server)
- Run on port 8001 (standalone)
- API endpoints:
  - `GET /api/diligence/deals` - List all deals
  - `POST /api/diligence/deals` - Create new deal
  - `GET /api/diligence/deals/<id>` - Get deal details
  - `POST /api/diligence/deals/<id>/documents` - Upload document
  - `POST /api/diligence/deals/<id>/analyze` - Run AI analysis
  - `GET /api/diligence/deals/<id>/report` - Get markdown report
  - `POST /api/diligence/deals/<id>/chat` - Send chat message
  - `GET /api/diligence/deals/<id>/chat` - Get chat history
  - `POST /api/diligence/deals/<id>/link` - Link to CRM lead
- Web pages:
  - `/diligence` - Main UI (served from `static/diligence.html`)
  - `/` - Root redirect with quick start guide

**Frontend**: `static/diligence.html`
- Deals list view (grid of deal cards)
- Single deal view with tabs:
  - **Documents**: Upload zone (drag & drop), document list
  - **Analysis**: Run analysis button, markdown report display
  - **Chat**: Message list, input box
- Create new deal modal
- Responsive design with modern UI

## Outputs

**Deliverables** (Cloud):
- None initially (all local processing in `.tmp/diligence/`)
- Future: Export reports to Google Docs, email summaries

**Intermediates** (`.tmp/`):
- `.tmp/diligence/{deal_id}/metadata.json` - Deal info, status, linked lead_id, timestamps
- `.tmp/diligence/{deal_id}/documents/original/` - Uploaded files (PDFs, images, Excel)
- `.tmp/diligence/{deal_id}/documents/extracted/` - Extracted text + metadata with confidence scores
- `.tmp/diligence/{deal_id}/analysis/initial_report.md` - AI-generated markdown report
- `.tmp/diligence/{deal_id}/analysis/initial_report_raw.json` - Structured analysis data
- `.tmp/diligence/{deal_id}/chat/chat_history.json` - Q&A conversation history
- `.tmp/diligence/{deal_id}/exports/` - PDF exports (future)

## Tech Stack
- **Backend**: Flask (Python)
- **Document Processing**: PyPDF2, pytesseract, openpyxl, pandas
- **AI**: Claude API (Claude Sonnet 4.5) via Anthropic library
- **Frontend**: Vanilla JS (no frameworks)
- **Storage**: Filesystem (`.tmp/diligence/`) - no database initially
- **CRM Integration**: Uses existing `db.py` functions

## Dependencies
```
flask
PyPDF2
pytesseract
pillow
openpyxl
pandas
anthropic
werkzeug
```

**System Dependency**: Tesseract OCR (already installed for financial calculator)

## Edge Cases & Error Handling

1. **Scanned PDFs (no text layer)**:
   - PyPDF2 returns empty text → currently shows error message requesting text-based PDFs or images
   - TODO: Implement PDF-to-image conversion + OCR (requires pdf2image + poppler)

2. **Large documents (>100 pages)**:
   - Text extraction truncates at 150,000 characters to stay within token limits
   - Prioritizes OMs and financials over photos/other docs

3. **Missing financial data**:
   - AI identifies data gaps in report under "Data Gaps" section
   - Chat agent can ask user to provide missing information
   - Analysis includes "next_steps" with recommended actions

4. **Corrupted or password-protected files**:
   - Extraction returns error in metadata: `{"error": "extraction failed"}`
   - Document marked as failed, continues with other documents
   - User can re-upload fixed version

5. **AI parsing errors**:
   - Catch JSON decode errors from Claude response
   - Return fallback structure: `{"executive_summary": "Parse Error", "confidence_score": 0, "error": "..."}`
   - Same pattern as financial_calc_ocr.py (lines 122-138)

6. **Multiple document uploads in parallel**:
   - Flask handles concurrent uploads
   - Metadata updates use file locking (implicit via filesystem operations)

7. **Chat context too long (>200k tokens)**:
   - Currently keeps last 10 messages in context window
   - TODO: Implement chat condensing after 20 messages (summarize old, keep recent 10 verbatim)

8. **No documents uploaded before analysis**:
   - API returns 400 error: "No documents uploaded. Please upload documents before analyzing."
   - Frontend disables analyze button until documents are present

9. **Analysis called before chat**:
   - Chat context includes note: "No analysis report available yet. Please analyze the deal first."
   - AI can still answer questions but with limited context

## API Rate Limits
- **Claude API**: 50 requests/min (Tier 1)
  - Initial analysis: 1 request (~30-60 seconds)
  - Chat: 1 request per message (~2-5 seconds)
  - Well within limits for typical usage (1 analysis + 10-20 chat messages per session)

## Workflow Example

```bash
# Start the server
cd execution
python diligence_server.py

# Opens browser to http://localhost:8001/diligence

# User workflow:
1. Click "New Deal" → Enter deal name "Sunset MHP"
   → Optionally link to CRM lead ID
2. Upload documents:
   - offering_memo.pdf (OM with property details)
   - financials_2023.xlsx (P&L statement)
   - property_photos.zip (exterior/interior photos)
   → Files processed, text extracted (30-60 seconds per file)
3. Click "Analyze Deal" → AI processes documents (30-60 seconds)
   → Report generated with:
      - Executive summary
      - Financial metrics (NOI, cap rate, cash-on-cash, DSCR)
      - Risk factors (Financial, Operational, Market, Legal)
      - Value-add opportunities (Revenue, Expense, Operational)
      - Red flags
      - Data gaps
4. Chat with AI (interactive Q&A):
   - "What is the current cap rate?" → "Based on the OM, the cap rate is 10.0%..."
   - "What are the biggest risks?" → "Top 3 risks: 1) High occupancy may be difficult to maintain..."
   - "How much upside is there from rent increases?" → "Lot rents are $50/month below market. Estimated upside: $61,500/year..."
5. (Optional) Link to CRM lead record
   → Lead status updated, note added with summary
6. Export report to PDF (browser print, or future server-side PDF generation)
```

## Testing Checklist

### Document Processing
- [x] Upload PDF with text layer → verify extraction works
- [ ] Upload scanned PDF → verify OCR fallback (TODO: implement PDF-to-image)
- [x] Upload Excel financial statements → verify data extraction
- [x] Upload images (property photos) → verify OCR extraction
- [ ] Upload multiple files at once → verify parallel processing
- [ ] Upload corrupted file → verify error handling
- [ ] Upload very large file (>50MB) → verify size limit rejection

### AI Analysis
- [ ] Test with complete document set (OM + financials) → verify comprehensive analysis
- [ ] Test with incomplete documents (missing financials) → verify data gaps identified
- [ ] Test with poor quality scanned documents → verify low confidence score
- [ ] Verify all report sections generated (exec summary, financials, risks, value-add, red flags, data gaps)
- [ ] Verify confidence score calculation (0-100 scale)

### Chat Q&A
- [ ] Test simple questions ("What is the cap rate?") → verify factual answers
- [ ] Test complex questions ("What are the top 3 risks?") → verify analysis synthesis
- [ ] Test follow-up questions (10+ message conversation) → verify context preservation
- [ ] Test questions about missing data → verify AI acknowledges gaps
- [ ] Test chat before analysis → verify AI requests analysis first

### CRM Integration
- [ ] Link deal to existing CRM lead → verify link created
- [ ] Verify lead data appears in analysis context
- [ ] Verify lead status updated after analysis (based on confidence score)
- [ ] Verify lead note added with summary
- [ ] Verify lead fields updated (asking_price, lot_count, noi, cap_rate)
- [ ] Get all deals for a lead → verify filtering works

### Web Interface
- [ ] Create new deal → verify deal appears in list
- [ ] View deal → verify all tabs render (Documents, Analysis, Chat)
- [ ] Upload document via drag & drop → verify upload works
- [ ] Upload document via file picker → verify upload works
- [ ] Run analysis → verify loading state, then report displays
- [ ] Send chat message → verify message appears, AI responds
- [ ] Navigate back to deals list → verify state preserved

### Error Handling
- [ ] Create deal without name → verify validation error
- [ ] Upload document to non-existent deal → verify 404 error
- [ ] Analyze deal with no documents → verify 400 error
- [ ] Send empty chat message → verify validation error
- [ ] Simulate API key missing → verify error message
- [ ] Simulate network error during upload → verify error handling

## Learnings & Updates

### Initial Build - 2026-01-09
- **Core Architecture**: Built complete 3-layer system (Utils → Document Processor → Analyzer → Chat → CRM Lookup → Server)
- **Document Processing**: Implemented PyPDF2 for text-based PDFs, pytesseract for images, pandas for Excel
  - Scanned PDF support (PDF-to-image → OCR) is placeholder, needs pdf2image library
  - Text extraction includes confidence scoring for OCR results
- **AI Analysis**: Claude Sonnet 4.5 integration following financial_calc_ocr.py pattern
  - Comprehensive prompt with structured JSON response
  - Fallback error handling for JSON parse failures
  - Generates formatted markdown reports with all required sections
- **Chat Interface**: Conversation history with last 10 messages in context window
  - System prompt includes deal summary, key metrics, risks, opportunities
  - TODO: Implement chat condensing for 20+ messages
- **CRM Integration**: Links to existing db.py functions for lead lookup and updates
  - Auto-updates lead status based on analysis confidence score
  - Syncs financial metrics back to CRM (asking_price, noi, cap_rate)
- **Web Interface**: Single-page app with deals list, document upload, analysis display, chat
  - Responsive design with modern UI (no frameworks, vanilla JS)
  - Drag & drop file upload
  - Real-time chat with AI
- **File Structure**: All files in `.tmp/diligence/{deal_id}/` with organized subfolders
  - Follows directive architecture (no deliverables, only intermediates)
  - Ready for .gitignore (won't commit uploaded documents or analysis data)

### Known Limitations (MVP)
- **Scanned PDF Support**: Not yet implemented (needs pdf2image + poppler)
  - Current workaround: Ask users to upload images or text-based PDFs
- **Chat Condensing**: Keeps last 10 messages, but doesn't auto-summarize old messages
  - Works fine for typical sessions (10-20 messages)
  - TODO: Implement AI-powered summarization for 20+ messages
- **PDF Export**: Uses browser print for MVP
  - TODO: Add server-side PDF generation with weasyprint
- **Database Persistence**: Uses filesystem only (no SQLite integration)
  - Future: Add `diligence_deals` table to `data/leads.db`
- **Web Search**: Not implemented (optional feature for future)
  - Would enhance analysis with market comps and property research

### Next Steps for Enhancement
1. **Implement scanned PDF support**: Add pdf2image library, convert PDFs to images, then OCR
2. **Add server-side PDF export**: Use weasyprint to generate styled PDF reports
3. **Implement chat condensing**: Auto-summarize conversations after 20 messages
4. **Add database persistence**: Migrate from filesystem to SQLite (integrate with CRM DB)
5. **Web search integration**: Add property comp lookup, market research
6. **Batch analysis**: Analyze multiple deals at once, compare side-by-side
7. **Email notifications**: Send report summaries via email
8. **Team collaboration**: Share deals, add comments, approval workflows

---
**Created**: 2026-01-09
**Last Updated**: 2026-01-09

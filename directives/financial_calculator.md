# Financial Modeling Tool Directive

## Goal
Build an interactive financial calculator for analyzing MHP/RV park listings with OCR parsing, data validation, and real-time financing/LP investment calculations.

## Architecture Overview
3-stage workflow:
1. **Stage 1**: Screenshot upload → OCR → LLM parsing → JSON output
2. **Stage 2**: Editable form review with validation
3. **Stage 3**: Interactive dual-module calculator (Financing + LP Tracking)

## Inputs
- Screenshot of property listing (drag-drop or file picker)
- Manual corrections to OCR-extracted data
- Financing parameters (down payment %, interest rate, terms, etc.)
- LP investor details (names, investment amounts)

## Execution Tools

### Stage 1: OCR & Parsing
**Script**: `execution/financial_calc_ocr.py`
- Accept image upload (PNG, JPG, PDF)
- Use pytesseract for OCR extraction (fallback: Google Vision API)
- Send OCR text to Claude API for structured parsing
- Output: JSON with extracted fields
  ```json
  {
    "property_name": "Sunset MHP",
    "description": "123-unit mobile home park...",
    "purchase_price": 4500000,
    "unit_count": 123,
    "location": "Tampa, FL",
    "noi": 450000,
    "cap_rate": 10.0,
    "raw_ocr_text": "..."
  }
  ```

### Stage 2: Data Validation
**Script**: `execution/financial_calc_validator.py`
- Load extracted JSON from Stage 1
- Validate required fields (property_name, purchase_price)
- Flag missing/uncertain values
- Save corrected data to `.tmp/calculator_option_1/leads/{lead_id}.json`

### Stage 3: Financial Calculator
**Script**: `execution/financial_calc_server.py` (Flask backend)
- Serve single-page app (HTML/CSS/JS)
- Real-time API endpoints:
  - `POST /calculate/financing` - Returns P&I payment, total interest, payoff schedule
  - `POST /calculate/lp_waterfall` - Returns month-by-month cash flow distribution
  - `POST /save_scenario` - Saves scenario to lead folder
  - `GET /export/pdf` - Generates PDF export

**Frontend**: `.tmp/calculator_option_1/static/calculator.html`
- Module A: Financing Structure Builder
  - Bank vs Seller financing toggle
  - Sliders: down payment %, interest rate, amortization period
  - Checkboxes: balloon payment, interest-only period
  - Hard money loan section (amount, rate, term)
- Module B: LP Investment Tracker
  - Dynamic investor cards (add/remove)
  - Auto-calculated ownership %
  - Month-by-month waterfall display

## Outputs
**Deliverables** (Cloud):
- None (local tool only)

**Intermediates** (.tmp/):
- `.tmp/calculator_option_1/leads/{lead_id}.json` - Extracted/corrected data
- `.tmp/calculator_option_1/scenarios/{scenario_id}.json` - Saved scenarios
- `.tmp/calculator_option_1/exports/{scenario_id}.pdf` - PDF exports

## Tech Stack
- **Backend**: Flask (Python)
- **Frontend**: Vanilla JS, Chart.js for visualizations
- **OCR**: pytesseract (primary), Google Vision API (fallback)
- **LLM**: Claude API (via existing ANTHROPIC_API_KEY in .env)
- **PDF Export**: weasyprint or reportlab

## Dependencies
```
flask
pytesseract
pillow
anthropic
weasyprint
```

## Edge Cases & Error Handling
1. **OCR Fails**: Show raw OCR text, allow full manual entry
2. **LLM Can't Parse**: Flag all fields as "needs review", show confidence scores
3. **Missing Purchase Price**: Block Stage 3 calculations until provided
4. **LP Funds + Hard Money < Down Payment**: Show warning, allow user to adjust
5. **Negative Cash Flow**: Highlight in red, show breakeven analysis

## API Rate Limits
- Claude API: 50 requests/min (Tier 1) - batched parsing should stay well below
- Google Vision API: 1800 requests/min (free tier) - no concern for single-image uploads

## Workflow Example
```bash
# Start the calculator server
python execution/financial_calc_server.py

# Opens browser to localhost:5000
# User uploads screenshot → OCR runs → LLM parses
# User reviews/edits form → Proceeds to calculator
# User adjusts financing sliders → Real-time updates
# User adds LP investors → Waterfall auto-calculates
# User exports to PDF
```

## Testing Checklist
- [ ] Upload screenshot, verify OCR extraction
- [ ] Test LLM parsing accuracy (5+ sample listings)
- [ ] Validate form prevents proceeding without required fields
- [ ] Test all slider/toggle combinations in Module A
- [ ] Test adding/removing LP investors in Module B
- [ ] Verify waterfall math: LP funds + hard money = down payment
- [ ] Test PDF export renders correctly
- [ ] Test saving/loading scenarios

## Learnings & Updates

### 2026-01-06 - Initial Build
- **Stage 1**: Built OCR script with pytesseract + Claude API parsing
  - Uses Claude Sonnet 4.5 for high-accuracy extraction
  - Outputs confidence scoring to alert user of parsing quality
  - Graceful fallback when OCR fails (shows raw text, allows manual entry)

- **Stage 2**: Integrated validation into Flask server
  - Real-time field validation before proceeding to calculator
  - PUT endpoint to update lead data after manual corrections

- **Stage 3**: Complete dual-module calculator built
  - Module A handles complex financing scenarios (balloon, interest-only, hard money)
  - Module B tracks unlimited LP investors with pro-rata distributions
  - Chart.js waterfall visualization shows 12-month projection
  - Real-time recalculation on all slider/input changes

- **Setup Scripts**: Created install.bat and start_server.bat for Windows
  - Automated dependency installation
  - Tesseract OCR verification
  - One-click server startup

- **File Structure**: All intermediates properly organized in .tmp/calculator_option_1/
  - Follows directive architecture (no deliverables, only local processing)
  - Ready for .gitignore (won't commit uploaded screenshots or lead data)

### Known Limitations
- PDF export currently uses browser print (not server-side generation)
- Google Vision API fallback not yet implemented (pytesseract only)
- No database persistence (JSON file storage only)
- Single-user (no authentication/multi-tenancy)

### Next Steps for Enhancement
1. Add weasyprint for proper PDF exports with styling
2. Implement Google Vision API as OCR fallback
3. Add IRR/CoC return calculations
4. Build scenario comparison view (side-by-side analysis)
5. Add email sharing functionality
6. Implement SQLite for lead/scenario persistence

---
**Created**: 2026-01-06
**Last Updated**: 2026-01-06

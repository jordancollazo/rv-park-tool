# Deal Diligence Agent - Quick Start Guide

AI-powered deal diligence for MHP acquisitions. Upload documents, get instant AI analysis, and chat with your diligence agent.

## Features

- **📄 Document Processing**: Upload Offering Memos, Financial Statements, Photos, Legal docs
- **🤖 AI Analysis**: Automatic investment report generation with financial metrics, risks, and opportunities
- **💬 Interactive Chat**: Ask questions about the deal and get instant answers
- **🔗 CRM Integration**: Links to existing CRM lead data and updates lead status
- **📊 Comprehensive Reports**: Executive summary, financial analysis, value-add opportunities, red flags

## Quick Start

### 1. Install Dependencies

```bash
pip install flask PyPDF2 pytesseract pillow openpyxl pandas anthropic werkzeug
```

**System Requirements**:
- Python 3.8+
- Tesseract OCR (already installed if you're using the financial calculator)
- Anthropic API key in `.env` file: `ANTHROPIC_API_KEY=sk-...`

### 2. Start the Server

```bash
cd execution
python diligence_server.py
```

Server will start on **http://localhost:8001**

### 3. Access the Web Interface

Open your browser to: **http://localhost:8001/diligence**

## Usage Workflow

### Step 1: Create a New Deal

1. Click **"+ New Deal"** button
2. Enter deal name (e.g., "Sunset MHP")
3. (Optional) Link to existing CRM lead by entering Lead ID
4. Click **"Create Deal"**

### Step 2: Upload Documents

1. Open the deal you just created
2. Go to **"Documents"** tab
3. Drag & drop files or click to browse:
   - **Offering Memorandums** (PDFs, images)
   - **Financial Statements** (Excel, CSV, PDFs)
   - **Property Photos** (JPG, PNG)
   - **Legal Documents** (PDFs)
4. Select document type from dropdown
5. Files will be processed automatically (text extraction via OCR)

### Step 3: Run AI Analysis

1. Go to **"Analysis"** tab
2. Click **"🚀 Run Analysis"** button
3. Wait 30-60 seconds for AI to analyze all documents
4. View comprehensive report with:
   - Executive Summary
   - Property Overview
   - Financial Analysis (NOI, Cap Rate, Cash-on-Cash, DSCR)
   - Deal Structure
   - Risk Factors with mitigation strategies
   - Value-Add Opportunities with estimated upside
   - Red Flags
   - Data Gaps
   - Recommended Next Steps

### Step 4: Chat with AI

1. Go to **"Chat"** tab
2. Ask questions about the deal:
   - "What is the current cap rate?"
   - "What are the biggest risks?"
   - "How much upside is there from rent increases?"
   - "What should I focus on during inspection?"
3. AI will reference the analysis report and documents to provide detailed answers

## File Structure

All deal data is stored in `.tmp/diligence/`:

```
.tmp/diligence/{deal_id}/
├── metadata.json                    # Deal info, status, timestamps
├── documents/
│   ├── original/                    # Your uploaded files
│   └── extracted/                   # Extracted text + metadata
├── analysis/
│   ├── initial_report.md           # Formatted markdown report
│   └── initial_report_raw.json     # Structured data
└── chat/
    └── chat_history.json           # Conversation history
```

## API Endpoints

For programmatic access:

### Deals
- `GET /api/diligence/deals` - List all deals
- `POST /api/diligence/deals` - Create new deal
- `GET /api/diligence/deals/<id>` - Get deal details

### Documents
- `POST /api/diligence/deals/<id>/documents` - Upload document

### Analysis
- `POST /api/diligence/deals/<id>/analyze` - Run AI analysis
- `GET /api/diligence/deals/<id>/report` - Get markdown report

### Chat
- `POST /api/diligence/deals/<id>/chat` - Send message
- `GET /api/diligence/deals/<id>/chat` - Get history

### CRM Integration
- `POST /api/diligence/deals/<id>/link` - Link to CRM lead
- `GET /api/diligence/leads/<lead_id>/deals` - Get all deals for a lead

## CRM Integration

If you link a deal to a CRM lead:

1. **Auto-sync lead data**: Analysis includes property details from CRM (address, asking price, lot count, etc.)
2. **Update lead status** based on analysis confidence score:
   - ≥70% confidence → `reviewed_interested`
   - 40-69% confidence → `docs_received`
   - <40% confidence → keep current status
3. **Add note to CRM** with executive summary, red flags, and value-add opportunities
4. **Sync financial metrics** back to CRM (asking_price, noi, cap_rate, lot_count)

## Supported File Types

### Documents
- **PDFs**: Text-based PDFs (with selectable text)
  - *Note*: Scanned PDFs (images) not yet supported - please upload as images instead
- **Images**: JPG, PNG, GIF, BMP, TIFF (OCR extraction)
- **Excel**: XLSX, XLS, CSV (data extraction)

### Document Types
- **Offering Memorandums**: Property details, financials, photos
- **Financial Statements**: P&L, rent rolls, T12 statements, operating expense reports
- **Photos**: Property photos, aerial views, unit interiors
- **Legal Documents**: Contracts, leases, environmental studies, zoning docs
- **Other**: Any other relevant documents

## Tips for Best Results

### Document Quality
- **Use text-based PDFs** when possible (better than scanned images)
- **High-resolution images** for OCR (300+ DPI recommended)
- **Clear, legible scans** (avoid blurry or skewed images)
- **Complete financials** (3+ years of historical data)

### Analysis Accuracy
- **Upload complete document set** before analyzing (OM + financials minimum)
- **Provide context via CRM link** if property is in your CRM
- **Review confidence score** - scores <40% indicate missing data
- **Check "Data Gaps" section** to see what information is missing

### Chat Effectiveness
- **Run analysis first** before chatting (AI needs context)
- **Be specific** with questions ("What is the cap rate?" vs "Tell me about this deal")
- **Reference specific concerns** ("What are the financing risks?" vs "Tell me the risks")
- **Ask follow-up questions** to drill into details

## Troubleshooting

### Common Issues

**"No text extracted from image"**
- Image quality may be too low
- Try uploading a higher resolution scan
- Verify Tesseract OCR is installed

**"Analysis failed - missing documents"**
- Upload at least one document before analyzing
- Verify documents were successfully processed (check extraction status)

**"Chat not responding"**
- Check ANTHROPIC_API_KEY is set in .env file
- Verify you have API credits remaining
- Check for API rate limits (50 req/min)

**"PDF extraction returned empty text"**
- PDF may be scanned (image-based)
- Convert PDF pages to images and upload separately
- Or provide text-based version of PDF

### Getting Help

1. **Check the logs**: Server prints detailed error messages to console
2. **Review the directive**: See [directives/deal_diligence.md](directives/deal_diligence.md) for complete technical documentation
3. **Examine the code**: All Python scripts in `execution/` have detailed docstrings
4. **Check file structure**: Look at `.tmp/diligence/{deal_id}/` to see what was processed

## Architecture Overview

The Deal Diligence Agent follows the 3-layer architecture:

1. **Layer 1 (Directive)**: [directives/deal_diligence.md](directives/deal_diligence.md) - SOP documentation
2. **Layer 2 (Orchestration)**: AI routing and decision-making
3. **Layer 3 (Execution)**: Deterministic Python scripts:
   - `diligence_utils.py` - Utility functions
   - `diligence_document_processor.py` - Document upload & text extraction
   - `diligence_analyzer.py` - AI analysis & report generation
   - `diligence_chat.py` - Interactive Q&A
   - `diligence_crm_lookup.py` - CRM integration
   - `diligence_server.py` - Flask web server

## Future Enhancements

Coming soon:
- ✅ Scanned PDF support (PDF-to-image + OCR)
- ✅ Server-side PDF export (styled reports)
- ✅ Chat condensing (auto-summarize long conversations)
- ✅ Database persistence (SQLite integration)
- ✅ Web search (market comps, property research)
- ✅ Batch analysis (compare multiple deals)
- ✅ Email notifications (report summaries)
- ✅ Team collaboration (share deals, comments)

---

**Created**: 2026-01-09
**Version**: 1.0.0 (MVP)
**Documentation**: See [directives/deal_diligence.md](directives/deal_diligence.md)
**Support**: Check console logs and error messages

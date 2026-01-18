# MHP Proprietary Outreach Tool

Lead generation and deal management system for Mobile Home Park (MHP) and RV Park acquisitions.

## Quick Start

**To start "the tool" (CRM map interface):**

```bash
# Windows
start_tool.bat

# Mac/Linux
./start_tool.sh
```

Then open your browser to: **http://localhost:8000**

## What You'll See

The CRM map interface includes:
- 📍 Interactive map with all leads across Florida
- 🏘️ **Crexi** leads (purple markers)
- 🏢 **LoopNet** leads (orange markers)
- 📊 **Pipeline** management for deal stages
- 🎯 Opportunity scoring and zone analysis
- 📈 Growth, affordability, displacement risk metrics
- 💧 Flood zone identification
- 🏦 Tax shock scores
- 🛡️ Insurance risk scores
- 🔍 Advanced filtering and zone builder tools
- 🌓 Light/dark mode toggle

## Project Structure

```
mhp-proprietary-outreach-tool/
├── start_tool.bat           # Quick start script (Windows)
├── start_tool.sh            # Quick start script (Mac/Linux)
├── execution/               # Python scripts (deterministic tools)
│   ├── crm_server.py       # Flask web server for CRM
│   ├── map_html.py         # HTML/JS for interactive map
│   └── ...                 # Other execution scripts
├── directives/              # SOPs and instructions (Markdown)
│   ├── crm_workflow.md     # CRM usage guide
│   ├── leadgen_rv_mhp.md   # Lead generation workflow
│   └── ...                 # Other directives
├── data/                    # SQLite database
│   └── leads.db            # All lead data
├── output/                  # Generated files (CSVs, HTML)
└── .tmp/                    # Temporary/intermediate files
```

## Key Directives

- **[crm_workflow.md](directives/crm_workflow.md)** - How to use the CRM tool
- **[leadgen_rv_mhp.md](directives/leadgen_rv_mhp.md)** - How to generate leads
- **[ingest_loopnet_leads.md](directives/ingest_loopnet_leads.md)** - LoopNet lead ingestion
- **[financial_calculator.md](directives/financial_calculator.md)** - Deal analysis calculator

## Architecture

This system follows a 3-layer architecture:

1. **Directives** (Layer 1) - Natural language SOPs defining what to do
2. **Orchestration** (Layer 2) - AI agent decision-making and routing
3. **Execution** (Layer 3) - Deterministic Python scripts that do the work

See [CLAUDE.md](CLAUDE.md) for detailed architecture documentation.

## Requirements

- Python 3.8+
- Dependencies: `flask`, `sqlite3`, `requests`, `beautifulsoup4`, `pandas`, `geopy`
- Google Cloud credentials for Gmail sync (optional)

## Support

For issues or questions, refer to the directive files in `directives/` or check `directives/troubleshooting_log.md`.

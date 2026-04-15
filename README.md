# MHP / RV Park Deal Scanner

Lead generation and deal management system for Mobile Home Park (MHP) and RV Park acquisitions. Scrapes Crexi and LoopNet, scores each property across flood zone, displacement risk, tax shock, and insurance risk, and surfaces the result as an interactive Florida-wide map.

## What it does

- **Scrapes** MHP/RV listings from Crexi and LoopNet via Apify
- **Geocodes** every listing and places it on an interactive map
- **Scores** each property on multiple risk dimensions:
  - Flood zone (FEMA NFHL)
  - Displacement risk (ZCTA-level demographics)
  - Tax shock (county-level millage trends)
  - Insurance risk (storm pressure grid)
  - Affordability and growth metrics
- **Tracks** each lead through a sales pipeline (not contacted → contacted → interested → under contract → etc.)
- **Logs** calls, emails, and notes per lead
- **Manages** broker contacts associated with each listing

## Interactive dashboard

- Map of all leads across Florida, with source-colored markers (Crexi = purple, LoopNet = orange)
- Side-panel pipeline view with drag/drop status changes
- Click any marker to pull full lead details, broker info, risk scores, and activity history
- Advanced filters and a zone-builder tool for geographic targeting

## Stack

- **Backend**: Flask (Python) + SQLite
- **Frontend**: HTML / JS / Leaflet map, no framework
- **Data sources**: Crexi, LoopNet, FEMA, census ZCTA, county tax records, IBTrACS storm history
- **Deploy**: Gunicorn on Railway, path-mounted via Next.js rewrites on the main domain

## Quick start (local)

```bash
pip install -r requirements.txt
cd execution
python crm_server.py
# Open http://localhost:8000
```

## Repo layout

```
execution/           # Python code
  crm_server.py      # Flask server (entry point)
  db.py              # SQLite layer
  map_html.py        # Generates the interactive map
  enrich_*.py        # One-off enrichment scripts (flood, tax, storms)
directives/          # Markdown SOPs for the workflow
data/                # SQLite DB + reference data (tax, census, tiger)
scripts/             # Non-core utilities
static/              # Standalone HTML pages
```

## Deploy (Railway)

The repo ships with `railway.toml`. Push to Railway, connect GitHub, set any needed env vars (Apify key for fresh scrapes, Google creds for Gmail sync, Twilio for call logging), and Railway builds with Nixpacks and starts via:

```
gunicorn --chdir execution --bind 0.0.0.0:$PORT --timeout 120 crm_server:app
```

## Known limitations

- **Single-market focused** (Florida). Scoring logic is tuned for FL county tax patterns, hurricane exposure, and ZCTA demographics. Would need retuning for other states.
- **Storm pressure grid is precomputed** (IBTrACS-based). Not included in the repo due to size, see `execution/build_storm_pressure_grid.py` to rebuild.
- **Ephemeral DB on Railway default.** To persist runtime changes across deploys, mount a Railway Volume at `/app/data`.
- **Scrapers depend on Apify.** Structural changes to Crexi / LoopNet periodically break the source actors.

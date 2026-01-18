# Lead Generation: RV Parks & Mobile Home Parks

## Purpose
Identify RV parks and mobile home parks in a geographic area for **proprietary acquisition outreach**. Score their website effectiveness to find digitally under-optimized properties where a buyer can add value through:
- Improved digital marketing & online presence
- Increased occupancy rates
- Property improvements & rent optimization

**Low website scores = high acquisition opportunity.** Unsophisticated digital presence often correlates with unsophisticated operations and owners who may be open to selling.

## Inputs

| Input | Type | Default | Notes |
|-------|------|---------|-------|
| `area` | string | required | Geographic area (e.g., "Broward County, FL") |
| `limit` | int | 200 | Max places to fetch |
| `keywords` | list | see below | Search terms |

**Default keywords:**
- RV park
- mobile home park
- manufactured home community
- trailer park *(noisier, but acceptable)*

## Outputs

### Primary Output
- `output/leads_{area}_{date}.csv` - Final scored leads

### Intermediate Files (in `.tmp/`)
- `raw_places.json` - Raw Apify response
- `normalized_places.json` - Cleaned place records
- `crawled_sites.json` - Website crawl data

## Execution Flow

```
1. run_places_search.py
   └─> .tmp/raw_places.json

2. normalize_places.py
   └─> .tmp/normalized_places.json

3. crawl_website.py (for each place with website)
   └─> .tmp/crawled_sites.json

4. score_website.py
   └─> .tmp/scored_sites.json

5. pipeline.py (generates final CSV)
   └─> output/leads_{area}_{date}.csv
```

## Scripts

| Script | Purpose |
|--------|---------|
| `run_places_search.py` | Call Apify actor, save raw data |
| `normalize_places.py` | Clean/normalize place records |
| `crawl_website.py` | Fetch website HTML + metadata |
| `score_website.py` | Deterministic 1-10 scoring |
| `pipeline.py` | CLI orchestration |

## Website Scoring Rubric

**Range:** 1-10 (deterministic, no LLM)

### Forced Rules
- No website → **score = 1**
- Facebook-only or aggregator-only → **score ≤ 3**

### Weighted Components

| Component | Weight | Checks |
|-----------|--------|--------|
| Technical Basics | 20% | HTTPS, title, meta description, H1, broken links |
| Mobile Usability | 20% | Viewport meta, responsive CSS indicators |
| Performance | 15% | Page weight, request count, image optimization |
| Conversion Clarity | 25% | Phone visible, CTA present, address shown |
| Trust Signals | 10% | Reviews/testimonials, company info, policies |
| Modernity | 10% | No outdated patterns, no "under construction" |

### Score Output
```json
{
  "site_score_1_10": 6,
  "score_breakdown_json": {
    "technical": 7,
    "mobile": 5,
    "performance": 6,
    "conversion": 8,
    "trust": 4,
    "modernity": 6
  },
  "score_reasons": "Missing meta description. No testimonials. Good phone visibility."
}
```

## Edge Cases

| Case | Handling |
|------|----------|
| No website | score = 1, crawl_status = "no_website" |
| Facebook-only | score ≤ 3, note "facebook_only" |
| Aggregator site (yelp, etc.) | score ≤ 3, note "aggregator_only" |
| Dead link (404/timeout) | score = 1, crawl_status = "failed", log error |
| Redirect loop | score = 1, crawl_status = "failed" |
| HTTPS redirect | Follow redirect, note in crawl |

## Known Limitations

- **Apify rate limits:** ~5000 places/day on free tier
- **Apify cost:** ~$5/1000 places
- **Google Maps coverage:** Some rural areas may have sparse data
- **Website crawl depth:** Only homepage + 2-3 internal links

## CLI Usage

```bash
# Full pipeline
python execution/pipeline.py --area "Broward County, FL" --limit 200

# Custom keywords
python execution/pipeline.py --area "Miami-Dade, FL" --keywords "RV park,campground"

# Skip steps
python execution/pipeline.py --area "Palm Beach, FL" --skip-crawl
python execution/pipeline.py --area "Palm Beach, FL" --skip-score

# CSV output instead of Google Sheets
python execution/pipeline.py --area "Broward County, FL" --limit 100 --csv

# Generate interactive map (in addition to CSV/Sheets)
python execution/pipeline.py --area "Broward County, FL" --limit 100 --map

# Launch CRM (Web Interface)
python execution/crm_server.py
# Then open http://localhost:5000
```

## Agent Workflow

When user says something like "scrape 100 leads in Broward County, FL":
1. Run the pipeline with specified area and limit
2. Wait for completion
3. Return the Google Sheets link directly to user
4. **Highlight low-scoring properties (≤5)** as top acquisition prospects—these have the most value-add potential

Example: "scrape 100 leads in broward county, florida" → Run pipeline → Return spreadsheet URL

## How to Rerun from Scratch

1. Delete `.tmp/` contents
2. Run full pipeline:
   ```bash
   python execution/pipeline.py --area "YOUR_AREA" --limit 200
   ```

## Dependencies

- `apify-client` - Apify SDK
- `requests` - HTTP requests
- `beautifulsoup4` - HTML parsing
- `python-dotenv` - Environment variables

## Environment Variables

Required in `.env`:
```
APIFY_API_TOKEN=your_token_here
```

## Learnings & Troubleshooting

### Map Visualization
- **Issue:** Map markers missing after scraping.
- **Cause:** `pipeline.py` CSV export was missing `latitude` and `longitude` columns, causing data loss during import.
- **Fix:** Ensure `export_to_csv` (and any data migration scripts) explicitly include coordinate fields.
- **Recovery:** Use `enrich_coords.py` or `recover_coords.py` to restore missing location data from `.tmp/scored_sites.json` or by re-fetching from Apify.

### CRM Persistence
- **Database:** Stored in `data/leads.db` (SQLite).
- **Update Logic:** `upsert_lead` in `db.py` is designed to preserve CRM statuses (calls, notes) while overwriting scrape data. Use `update_lead_fields` with caution; ensure allowed_fields whitelist includes necessary columns.

---

## Opportunity Zone Analysis

### Purpose
Identify Florida zip codes with high population growth and affordable housing—areas likely to see increased demand for MHPs/RV parks before prices catch up.

### Thesis
Population migration momentum + affordable housing = undervalued opportunity. The system computes an **Opportunity Score** (0-100) based on:

| Factor | Weight | Signal |
|--------|--------|--------|
| Population Growth (5yr) | 45% | Demand driver—where people are moving |
| Affordability (1/PTI) | 35% | Room for appreciation |
| Mobile Home % | 10% | Existing MHP market presence |
| Vacancy Rate | 10% | Sweet spot = 5-10% (turnover, not distress) |

### CLI Usage

```bash
# Step 1: Fetch Census data for all Florida zip codes
python execution/fetch_florida_zcta_data.py

# Step 2: Fetch zip code boundary polygons (for choropleth)
python execution/fetch_zcta_boundaries.py

# Step 3: Generate opportunity zone map
python execution/export_opportunity_map.py

# Options
python execution/export_opportunity_map.py --no-leads     # Choropleth only
python execution/export_opportunity_map.py --output output/custom_map.html
```

### Map Features
- **Toggle layers:** Opportunity Score (purple), Population Growth (red), Affordability (green), Lead Markers
- **Hover tooltips:** Zip code metrics (growth %, PTI, MH%, vacancy, median home value)
- **Hot zone highlighting:** Top 20% opportunity zones get gold borders
- **Score filter:** Slide to filter by minimum opportunity score

### Data Sources
- **Census ACS 5-Year** (2022 + 2018 for growth calculation)
- **Census TIGER/Line** for zip code boundaries
- No API key required (free Census API)

---

*Last updated: 2025-12-29*


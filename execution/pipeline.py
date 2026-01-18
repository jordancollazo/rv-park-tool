"""
pipeline.py
One-command orchestration for the full lead generation pipeline.

Usage:
    python execution/pipeline.py --area "Broward County, FL" --limit 200
    python execution/pipeline.py --area "Miami-Dade, FL" --keywords "RV park,campground"
    python execution/pipeline.py --area "Palm Beach, FL" --skip-crawl
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Import pipeline steps
from run_places_search import run_places_search, save_raw_results, DEFAULT_KEYWORDS
from normalize_places import normalize_places, save_normalized
from crawl_website import crawl_all_websites, save_crawled
from score_website import score_all_websites, save_scored
from score_owner_fatigue import score_all_owner_fatigue, save_owner_fatigue_scored
from export_to_sheets import export_to_sheets
from export_to_map import export_to_map
from db import bulk_upsert_leads, init_db

# Paths
TMP_DIR = Path(".tmp")
OUTPUT_DIR = Path("output")


def export_to_csv(results: list[dict], area: str) -> Path:
    """Export final results to CSV."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Create filename with area and date
    safe_area = area.lower().replace(" ", "_").replace(",", "")
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = OUTPUT_DIR / f"leads_{safe_area}_{date_str}.csv"
    
    # Define CSV columns
    columns = [
        "source_query",
        "area",
        "name",
        "address",
        "city",
        "state",
        "zip",
        "phone",
        "website",
        "maps_url",
        "place_id",
        "google_rating",
        "review_count",
        "site_score_1_10",
        "score_breakdown_json",
        "score_reasons",
        "crawl_status",
        "crawl_notes",
        "last_crawled_utc",
        "latitude",
        "longitude",
    ]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        
        for place in results:
            row = {col: place.get(col, "") for col in columns}
            
            # Convert breakdown dict to JSON string
            breakdown = place.get("score_breakdown_json", {})
            if isinstance(breakdown, dict):
                row["score_breakdown_json"] = json.dumps(breakdown)
            
            # Add crawl timestamp
            row["last_crawled_utc"] = place.get("crawled_at", "")
            
            writer.writerow(row)
    
    print(f"\nExported to: {output_path}")
    return output_path


def run_pipeline(
    area: str,
    limit: int = 200,
    keywords: list[str] | None = None,
    skip_crawl: bool = False,
    skip_score: bool = False,
    to_sheets: bool = True,
    generate_map: bool = False,
) -> str:
    """
    Run the full lead generation pipeline.
    
    Steps:
    1. Search for places via Apify
    2. Normalize place records
    3. Crawl websites (optional)
    4. Score websites (optional)
    5. Export to Google Sheets (or CSV as backup)
    
    Returns:
        Spreadsheet URL or CSV path
    """
    print("=" * 60)
    print("LEAD GENERATION PIPELINE")
    print("=" * 60)
    print(f"Area: {area}")
    print(f"Limit: {limit}")
    print(f"Keywords: {keywords or DEFAULT_KEYWORDS}")
    print(f"Skip crawl: {skip_crawl}")
    print(f"Skip score: {skip_score}")
    print(f"Output: {'Google Sheets' if to_sheets else 'CSV'}")
    print(f"Generate map: {generate_map}")
    print("=" * 60)
    
    # Step 1: Search
    print("\n[1/5] SEARCHING FOR PLACES...")
    items = run_places_search(area=area, keywords=keywords, limit=limit)
    if not items:
        print("ERROR: No places found. Exiting.")
        sys.exit(1)
    save_raw_results(items, area, keywords or DEFAULT_KEYWORDS)
    
    # Step 2: Normalize
    print("\n[2/5] NORMALIZING PLACES...")
    normalized = normalize_places()
    save_normalized(normalized)
    
    # Step 3: Crawl (optional)
    if skip_crawl:
        print("\n[3/5] SKIPPING CRAWL...")
        # Load normalized for next step
        with open(TMP_DIR / "normalized_places.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        crawled = data.get("places", [])
        # Add default crawl fields
        for place in crawled:
            place["crawl_status"] = "skipped"
            place["pages"] = []
    else:
        print("\n[3/5] CRAWLING WEBSITES...")
        crawled = crawl_all_websites()
        save_crawled(crawled)
    
    # Step 4: Score (optional)
    if skip_score:
        print("\n[4/5] SKIPPING SCORE...")
        scored = crawled
        for place in scored:
            place["site_score_1_10"] = 0
            place["score_breakdown_json"] = {}
            place["score_reasons"] = "Scoring skipped"
    else:
        print("\n[4/6] SCORING WEBSITES...")
        # Need to save crawled first for score step
        if skip_crawl:
            save_crawled(crawled)
        scored = score_all_websites()
        save_scored(scored)
    
    # Step 4.5: Owner Fatigue Score
    if not skip_score:
        print("\n[4.5/6] SCORING OWNER FATIGUE...")
        fatigue_scored = score_all_owner_fatigue()
        save_owner_fatigue_scored(fatigue_scored)
        # Merge fatigue scores into scored list
        fatigue_by_place_id = {p.get('place_id'): p for p in fatigue_scored}
        for place in scored:
            place_id = place.get('place_id')
            if place_id and place_id in fatigue_by_place_id:
                fatigue_data = fatigue_by_place_id[place_id]
                place['owner_fatigue_score_0_100'] = fatigue_data.get('owner_fatigue_score_0_100')
                place['owner_fatigue_confidence'] = fatigue_data.get('owner_fatigue_confidence')
                place['owner_fatigue_reasons_json'] = fatigue_data.get('owner_fatigue_reasons_json')
                place['owner_fatigue_breakdown_json'] = fatigue_data.get('owner_fatigue_breakdown_json')
    
    # Step 5: Persist to DB
    print("\n[5/6] SAVING TO CRM DATABASE...")
    init_db()
    inserted, updated = bulk_upsert_leads(scored)
    print(f"Saved to DB: {inserted} new, {updated} updated")
    
    # Step 5: Export
    if to_sheets:
        print("\n[5/5] EXPORTING TO GOOGLE SHEETS...")
        try:
            spreadsheet_id = export_to_sheets(scored, area)
            output_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        except Exception as e:
            print(f"Google Sheets export failed: {e}")
            print("Falling back to CSV...")
            output_url = str(export_to_csv(scored, area))
    else:
        print("\n[5/5] EXPORTING TO CSV...")
        output_url = str(export_to_csv(scored, area))
    
    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Total places: {len(scored)}")
    
    if not skip_score:
        scores = [p.get("site_score_1_10", 0) for p in scored]
        avg = sum(scores) / len(scores) if scores else 0
        low = sum(1 for s in scores if s <= 3)
        print(f"Average score: {avg:.1f}/10")
        print(f"Low scores (≤3): {low} ({100*low/len(scores):.0f}% - high opportunity)")
    
    print(f"\nOutput: {output_url}")
    
    # Generate map if requested
    map_path = None
    if generate_map:
        print("\n[BONUS] GENERATING MAP...")
        safe_area = area.lower().replace(" ", "_").replace(",", "")
        map_output = OUTPUT_DIR / f"map_{safe_area}.html"
        map_path = export_to_map(scored, map_output, f"MHP/RV Leads - {area}")
        print(f"Map: file://{map_path.absolute()}")
    
    return output_url


def main():
    parser = argparse.ArgumentParser(
        description="Run the full RV/MHP lead generation pipeline"
    )
    parser.add_argument(
        "--area",
        required=True,
        help="Geographic area to search (e.g., 'Broward County, FL')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of places to fetch (default: 200)",
    )
    parser.add_argument(
        "--keywords",
        type=str,
        default=None,
        help="Comma-separated list of search keywords",
    )
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        help="Skip website crawling step",
    )
    parser.add_argument(
        "--skip-score",
        action="store_true",
        help="Skip website scoring step",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Export to CSV instead of Google Sheets",
    )
    parser.add_argument(
        "--map",
        action="store_true",
        help="Generate interactive HTML map",
    )
    
    args = parser.parse_args()
    
    # Parse keywords
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",")]
    
    run_pipeline(
        area=args.area,
        limit=args.limit,
        keywords=keywords,
        skip_crawl=args.skip_crawl,
        skip_score=args.skip_score,
        to_sheets=not args.csv,
        generate_map=args.map,
    )


if __name__ == "__main__":
    main()

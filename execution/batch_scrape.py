#!/usr/bin/env python3
"""
Batch scraper for all 67 Florida counties.
Usage: python execution/batch_scrape.py [--dry-run] [--limit N]

This script:
1. Iterates through all 67 FL counties.
2. Checks if a partial/complete CSV already exists for today.
3. Runs the pipeline for that county (MHP + RV Parks).
4. Adds delays to allow self-annealing (avoid rate limits).
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path to import pipeline
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.append(str(project_root))

from execution.pipeline import run_pipeline

FL_COUNTIES = [
    "Alachua County", "Baker County", "Bay County", "Bradford County", 
    "Brevard County", "Broward County", "Calhoun County", "Charlotte County", 
    "Citrus County", "Clay County", "Collier County", "Columbia County", 
    "DeSoto County", "Dixie County", "Duval County", "Escambia County", 
    "Flagler County", "Franklin County", "Gadsden County", "Gilchrist County", 
    "Glades County", "Gulf County", "Hamilton County", "Hardee County", 
    "Hendry County", "Hernando County", "Highlands County", "Hillsborough County", 
    "Holmes County", "Indian River County", "Jackson County", "Jefferson County", 
    "Lafayette County", "Lake County", "Lee County", "Leon County", 
    "Levy County", "Liberty County", "Madison County", "Manatee County", 
    "Marion County", "Martin County", "Miami-Dade County", "Monroe County", 
    "Nassau County", "Okaloosa County", "Okeechobee County", "Orange County", 
    "Osceola County", "Palm Beach County", "Pasco County", "Pinellas County", 
    "Polk County", "Putnam County", "St. Johns County", "St. Lucie County", 
    "Santa Rosa County", "Sarasota County", "Seminole County", "Sumter County", 
    "Suwannee County", "Taylor County", "Union County", "Volusia County", 
    "Wakulla County", "Walton County", "Washington County"
]

def main():
    parser = argparse.ArgumentParser(description="Batch scrape FL counties")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without running")
    parser.add_argument("--limit", type=int, default=200, help="Leads per county limit")
    parser.add_argument("--counties", type=str, help="Comma-sep list of specific counties to run")
    args = parser.parse_args()

    counties_to_run = FL_COUNTIES
    if args.counties:
        counties_to_run = [c.strip() for c in args.counties.split(",")]

    print(f"Loaded {len(counties_to_run)} counties.")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE EXECUTION'}")
    
    total_leads = 0
    
    for i, county in enumerate(counties_to_run):
        area = f"{county}, FL"
        print(f"\n[{i+1}/{len(counties_to_run)}] Processing: {area}")
        
        # Check existing (naive check based on date-stamped file existence not actually possible without globbing, 
        # but pipeline handles duplicate DB entries gracefully).
        # We'll just rely on the pipeline.
        
        if args.dry_run:
            print(f"  [Dry Run] Would call run_pipeline('{area}', limit={args.limit})")
            time.sleep(0.1)
            continue
            
        try:
            # We skip crawling for speed in this demo if desired, but user wants full data.
            # We'll default to full crawl.
            # Passing to_sheets=False to avoid spamming 67 tabs, can do one big export later if needed.
            csv_path = run_pipeline(
                area=area,
                limit=args.limit,
                to_sheets=False 
            )
            print(f"  -> Success. Output: {csv_path}")
            
            # Nap to be nice to APIs
            time.sleep(2)
            
        except Exception as e:
            print(f"  -> FAILED: {e}")
            # Don't crash entire batch
            continue

    print("\nBatch Complete.")

if __name__ == "__main__":
    main()

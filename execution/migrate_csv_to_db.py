"""
migrate_csv_to_db.py
Import legacy CSV leads into the SQLite database.
"""

import csv
import json
import sys
from pathlib import Path

# Import from db module
from db import init_db, upsert_lead

def migrate_csv(csv_path: Path):
    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        return

    print(f"Migrating {csv_path}...")
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        
        for row in reader:
            # Parse score breakdown
            try:
                breakdown = json.loads(row.get("score_breakdown_json", "{}"))
            except:
                breakdown = {}
                
            # Parse lat/long
            try:
                lat = float(row.get("latitude")) if row.get("latitude") else None
                lng = float(row.get("longitude")) if row.get("longitude") else None
            except:
                lat = None
                lng = None

            lead = {
                "place_id": row.get("place_id"),
                "name": row.get("name"),
                "address": row.get("address"),
                "city": row.get("city"),
                "state": row.get("state"),
                "zip": row.get("zip"),
                "phone": row.get("phone"),
                "website": row.get("website"),
                "maps_url": row.get("maps_url"),
                "latitude": lat,
                "longitude": lng,
                "google_rating": row.get("google_rating"),
                "review_count": row.get("review_count"),
                "site_score_1_10": int(row.get("site_score_1_10", 0)),
                "score_breakdown_json": breakdown,
                "score_reasons": row.get("score_reasons"),
                "crawl_status": row.get("crawl_status"),
                "source_query": row.get("source_query"),
                "area": row.get("area"),
            }
            
            if lead["place_id"]:
                upsert_lead(lead)
                count += 1
                
    print(f"Imported {count} leads from {csv_path.name}")

if __name__ == "__main__":
    init_db()
    
    # Check for specific file arg or default to output directory
    if len(sys.argv) > 1:
        migrate_csv(Path(sys.argv[1]))
    else:
        # Import all CSVs in output dir
        output_dir = Path("output")
        for csv_file in output_dir.glob("leads_*.csv"):
            migrate_csv(csv_file)

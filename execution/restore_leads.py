
import csv
import json
import glob
import os
import sys
from pathlib import Path
from datetime import datetime

# Add execution dir to path to import db
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from execution import db

def restore_leads():
    # Find all leads_*.csv in output/
    csv_files = glob.glob("output/leads_*.csv")
    print(f"Found {len(csv_files)} CSV files to restore.")
    
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    
    for csv_path in csv_files:
        print(f"Processing {csv_path}...")
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                leads_batch = []
                
                for row in reader:
                    # Map CSV fields to DB fields
                    # Handle JSON fields
                    score_breakdown = {}
                    try:
                        if row.get("score_breakdown_json"):
                            score_breakdown = json.loads(row.get("score_breakdown_json"))
                    except:
                        pass
                        
                    # Handle numeric fields
                    def safe_float(val):
                        if not val: return None
                        try: return float(val)
                        except: return None
                        
                    def safe_int(val):
                        if not val: return None
                        try: return int(val)
                        except: return None

                    # Construct lead dict
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
                        "google_rating": safe_float(row.get("google_rating")),
                        "review_count": safe_int(row.get("review_count")),
                        "site_score_1_10": safe_int(row.get("site_score_1_10")),
                        "score_breakdown_json": score_breakdown,
                        "score_reasons": row.get("score_reasons"),
                        "crawl_status": row.get("crawl_status"),
                        "source_query": row.get("source_query"),
                        "area": row.get("area"),
                        "last_scraped_at": row.get("last_crawled_utc"), # Map CSV col to DB col
                        
                        # Lat/Lon
                        "latitude": safe_float(row.get("latitude")),
                        "longitude": safe_float(row.get("longitude")),
                        
                        # Infer category if missing (useful for UI)
                        "category": row.get("source_query") # Use source query as category proxy if real category missing
                    }
                    
                    if lead["place_id"]:
                        leads_batch.append(lead)
                        
                # Bulk upsert
                if leads_batch:
                    inserted, updated = db.bulk_upsert_leads(leads_batch)
                    print(f"  -> Inserted: {inserted}, Updated: {updated}")
                    total_inserted += inserted
                    total_updated += updated
                else:
                    print("  -> No valid leads found in file.")
                    
        except Exception as e:
            print(f"Error processing {csv_path}: {e}")
            
    print(f"\nRESTORATION COMPLETE.")
    print(f"Total Inserted: {total_inserted}")
    print(f"Total Updated: {total_updated}")

if __name__ == "__main__":
    restore_leads()

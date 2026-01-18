
import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def show_detailed_samples():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Get 5 recent LoopNet leads
    query = """
        SELECT *
        FROM leads 
        WHERE source_query = 'LoopNet Scraper'
        LIMIT 5
    """
    
    rows = conn.execute(query).fetchall()
    conn.close()
    
    if not rows:
        print("No LoopNet leads found.")
        return

    print(f"Found {len(rows)} samples:\n")
    
    for i, row in enumerate(rows, 1):
        print(f"--- Sample {i} ---")
        # Convert row to dict
        data = dict(row)
        
        # Filter out empty/None values for cleaner display if desired, 
        # but user asked for "all the data", so let's show important fields first then the rest.
        
        # Key fields first
        priority_keys = [
            "name", "address", "city", "state", "list_price", "cap_rate", "noi", 
            "occupancy_rate", "year_built", "building_size", "lot_size",
            "broker_name", "broker_firm", "loopnet_url", 
            "listing_status", "detailed_description"
        ]
        
        for k in priority_keys:
            val = data.get(k)
            if val is not None and val != "":
                print(f"{k.ljust(20)}: {val}")
                
        print("\n[Other Fields]")
        for k, v in data.items():
            if k not in priority_keys and v is not None and v != "":
                # Don't show large JSON blobs unless relevant
                if "json" in k and v == "{}": continue
                print(f"{k.ljust(20)}: {str(v)[:100]}") # Truncate very long fields
        print("\n")

if __name__ == "__main__":
    show_detailed_samples()

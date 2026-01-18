"""
enrich_coords.py
Fetch coordinates for Broward leads by re-running search, 
and update DB without overwriting scores.
"""

from db import init_db, update_lead_fields, get_lead_by_place_id
from run_places_search import run_places_search
from normalize_places import normalize_place
import json

def enrich_broward():
    area = "Broward County, FL"
    print(f"Fetching fresh data for {area}...")
    
    # Run search
    raw_results = run_places_search(area=area, limit=100)
    print(f"Found {len(raw_results)} places via Apify")
    
    updated_count = 0
    init_db()
    
    for raw in raw_results:
        # Normalize
        lead = normalize_place(raw, "enrichment", area)
        place_id = lead.get("place_id")
        
        if not place_id:
            continue
            
        # Check DB
        db_lead = get_lead_by_place_id(place_id)
        if db_lead:
            lat = lead.get("latitude")
            lng = lead.get("longitude")
            
            if lat and lng:
                success = update_lead_fields(
                    db_lead["id"],
                    latitude=lat,
                    longitude=lng,
                    maps_url=lead.get("maps_url")
                )
                if success:
                    updated_count += 1
                    
    print(f"Enriched coordinates for {updated_count} leads")

if __name__ == "__main__":
    enrich_broward()

"""
recover_coords.py
Recover missing latitude/longitude from .tmp/scored_sites.json and update DB.
"""

import json
from pathlib import Path
from db import init_db, update_lead_fields, get_lead_by_id, get_lead_by_place_id

def recover():
    input_path = Path(".tmp/scored_sites.json")
    if not input_path.exists():
        print("scored_sites.json not found")
        return

    init_db()
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    places = data.get("places", [])
    updated_count = 0
    
    print(f"Checking {len(places)} places for coordinate recovery...")
    
    for place in places:
        place_id = place.get("place_id")
        lat = place.get("latitude")
        lng = place.get("longitude")
        
        # Check alternates
        if not lat or not lng:
            loc = place.get("location", {})
            lat = loc.get("lat") or lat
            lng = loc.get("lng") or lng
            
        if place_id and lat is not None and lng is not None:
            # Get internal ID
            db_lead = get_lead_by_place_id(place_id)
            if db_lead:
                # Update fields
                success = update_lead_fields(
                    db_lead["id"],
                    latitude=lat,
                    longitude=lng
                )
                if success:
                    updated_count += 1
                    print(f"Updated {place.get('name')}: {lat}, {lng}")
                else:
                    print(f"Failed to update {place.get('name')}")
            else:
                print(f"Lead not found in DB: {place.get('name')}")
                
    print(f"Recovered coordinates for {updated_count} leads.")

if __name__ == "__main__":
    recover()

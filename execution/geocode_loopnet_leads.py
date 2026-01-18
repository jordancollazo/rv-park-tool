"""
geocode_loopnet_leads.py
Geocode LoopNet leads that don't have coordinates using Google Maps Geocoding API.
"""

import os
import time
import sqlite3
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not GOOGLE_API_KEY:
    print("GOOGLE_MAPS_API_KEY not found in environment.")
    GOOGLE_API_KEY = input("Please enter your Google Maps API Key: ").strip()

def geocode_address(address, city, state, zip_code):
    """Geocode an address using Google Maps Geocoding API."""
    if not GOOGLE_API_KEY:
        print("WARNING: GOOGLE_MAPS_API_KEY not set, skipping geocoding")
        return None, None
    
    # Build full address
    parts = [p for p in [address, city, state, zip_code] if p]
    full_address = ", ".join(parts)
    
    if not full_address or len(full_address) < 5:
        return None, None
    
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": full_address,
        "key": GOOGLE_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("status") == "OK" and data.get("results"):
            location = data["results"][0]["geometry"]["location"]
            return location["lat"], location["lng"]
        else:
            print(f"  Geocode failed for '{full_address}': {data.get('status')}")
            return None, None
    except Exception as e:
        print(f"  Geocode error for '{full_address}': {e}")
        return None, None


def geocode_loopnet_leads():
    """Geocode all LoopNet leads that are missing coordinates."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Find LoopNet leads without coordinates
    leads = conn.execute("""
        SELECT id, name, address, city, state, zip
        FROM leads
        WHERE latitude IS NULL OR longitude IS NULL
    """).fetchall()
    
    print(f"Found {len(leads)} LoopNet leads without coordinates")
    
    if not leads:
        conn.close()
        return
    
    geocoded = 0
    failed = 0
    
    for lead in leads:
        print(f"Geocoding: {lead['name'][:50]}...")
        
        lat, lng = geocode_address(
            lead["address"],
            lead["city"],
            lead["state"],
            lead["zip"]
        )
        
        if lat and lng:
            conn.execute(
                "UPDATE leads SET latitude = ?, longitude = ? WHERE id = ?",
                (lat, lng, lead["id"])
            )
            conn.commit()
            geocoded += 1
            print(f"  -> {lat}, {lng}")
        else:
            failed += 1
        
        # Rate limit
        time.sleep(0.2)
    
    conn.close()
    print(f"\nGeocoding complete: {geocoded} successful, {failed} failed")


if __name__ == "__main__":
    geocode_loopnet_leads()

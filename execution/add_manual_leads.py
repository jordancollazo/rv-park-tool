"""
add_manual_leads.py
Inserts specific manual leads into the database.
Uses a generated UUID for place_id to satisfy the UNIQUE NOT NULL constraint.
"""

import sqlite3
import uuid
import datetime
from db import get_db

def insert_lead(lead_data):
    # Generate a fake place_id if not provided
    place_id = f"manual_{uuid.uuid4()}"
    
    # Base query
    keys = list(lead_data.keys())
    keys.append("place_id")
    keys.append("first_scraped_at")
    keys.append("last_scraped_at")
    
    placeholders = ",".join(["?"] * len(keys))
    columns = ",".join(keys)
    
    values = list(lead_data.values())
    values.append(place_id)
    now = datetime.datetime.now().isoformat()
    values.append(now)
    values.append(now)
    
    sql = f"INSERT INTO leads ({columns}) VALUES ({placeholders})"
    
    try:
        with get_db() as conn:
            cursor = conn.execute(sql, values)
            lead_id = cursor.lastrowid
            print(f"Successfully inserted lead: {lead_data.get('name', 'Unknown')} (ID: {lead_id})")
            conn.commit()
    except Exception as e:
        print(f"Error inserting lead {lead_data.get('name')}: {e}")

def main():
    # Lead 1: 1211 Cypress Rd
    lead1 = {
        "name": "1211 Cypress Rd Mobile Home Park",
        "address": "1211 Cypress Rd",
        "city": "Saint Augustine",
        "state": "FL",
        "zip": "32086",
        "asking_price": 1500000,
        "lot_count": 14, # From "No. Units"
        "price_per_unit": 107143,
        "year_built": 1974,
        "lot_size_text": "7.17 AC",
        "building_size_text": "4,378 SF",
        "listing_source_url": "https://www.loopnet.com/Listing/1211-Cypress-Rd-Saint-Augustine-FL/29358929/",
        "listing_source_id": "29358929",
        "broker_name": "Frank Perez-Andreu (Keller Williams)",
        "is_manual_entry": 1,
        "status": "not_contacted",
        "score_reasons": "Manual Entry - LoopNet Listing",
        # Geolocation for map (Approximate)
        "latitude": 29.8378, 
        "longitude": -81.3344
    }

    # Lead 2: 1247 LPGA Blvd (Hillside Mobile Home Park)
    lead2 = {
        "name": "Hillside Mobile Home Park",
        "address": "1247 LPGA Blvd",
        "city": "Daytona Beach",
        "state": "FL",
        "zip": "32117",
        "asking_price": 1600000,
        "cap_rate": 8.62,
        "noi": 137927,
        "lot_count": 20, # Listed as Units
        "price_per_unit": 69565, # Using listing data
        "lot_size_text": "1.615 AC",
        "listing_source_url": "https://www.crexi.com/properties/1702272/florida-hillside-mobile-home-park",
        "listing_source_id": "1702272",
        "broker_name": "Michael Baxter (SVN)",
        "is_manual_entry": 1,
        "status": "not_contacted",
        "score_reasons": "Manual Entry - Crexi/LandWatch",
        # Geolocation for map
        "latitude": 29.2260,
        "longitude": -81.0770
    }

    print("Inserting Manual Leads...")
    insert_lead(lead1)
    insert_lead(lead2)

if __name__ == "__main__":
    main()

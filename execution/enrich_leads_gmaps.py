
import argparse
import googlemaps
import sqlite3
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_PATH = "data/leads.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Ensure amenity columns exist in the leads table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Add columns if they don't exist
    columns = [
        "nearest_supermarket_name", "nearest_supermarket_dist",
        "nearest_hospital_name", "nearest_hospital_dist",
        "nearest_school_name", "nearest_school_dist",
        "amenity_score"
    ]
    
    cursor.execute("PRAGMA table_info(leads)")
    existing_columns = [row["name"] for row in cursor.fetchall()]
    
    for col in columns:
        if col not in existing_columns:
            col_type = "REAL" if "dist" in col or "score" in col else "TEXT"
            print(f"Adding column: {col}")
            cursor.execute(f"ALTER TABLE leads ADD COLUMN {col} {col_type}")
            
    conn.commit()
    conn.close()

def get_leads_to_enrich(limit=None, force=False):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Prioritize leads that haven't been enriched yet (amenity_score is NULL)
    if force:
        query = "SELECT * FROM leads WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    else:
        query = "SELECT * FROM leads WHERE amenity_score IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL"

    if limit:
        query += f" LIMIT {limit}"
        
    cursor.execute(query)
    leads = cursor.fetchall()
    conn.close()
    return leads

def calculate_distance(origin, destination):
    """
    Calculate distance using Google Maps Distance Matrix API.
    Returns distance in miles.
    """
    # Note: For simple proximity, we could use the 'distance' from the Place Search result if available, 
    # or Haversine calc locally. But for "driving distance" (which is more useful), we'd use Distance Matrix.
    # However, to save API costs and latency for this enriched scoring, we'll rely on the Geometry from available results 
    # and do a quick Haversine or just straight line if needed.
    # actually, the prompt asked for "Nearby Search", which returns locations. 
    # Calculating straight-line distance locally is FREE and usually sufficient for "proximity".
    
    from math import radians, cos, sin, asin, sqrt

    lat1, lon1 = origin
    lat2, lon2 = destination
    
    # Haversine formula
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 3956 # Radius of earth in miles
    return c * r

def find_nearest_amenity(gmaps, location, keyword, type_filter=None):
    """
    Finds the nearest amenity using Google Places Nearby Search.
    Returns (name, distance_miles) or (None, None).
    """
    try:
        # Search radius: 10 miles (approx 16000 meters)
        places = gmaps.places_nearby(
            location=location,
            keyword=keyword,
            type=type_filter,
            rank_by="distance"
        )
        
        if places.get('status') == 'OK' and places.get('results'):
            nearest = places['results'][0]
            name = nearest.get('name')
            geometry = nearest.get('geometry', {}).get('location')
            
            if geometry:
                dest_lat = geometry.get('lat')
                dest_lng = geometry.get('lng')
                dist = calculate_distance(location, (dest_lat, dest_lng))
                return name, round(dist, 2)
                
    except Exception as e:
        print(f"  Error searching for {keyword}: {e}")
        
    return None, None

def calculate_amenity_score(supermarket_dist, hospital_dist, school_dist):
    """
    Compute amenity score (0-100).
    Lower distance = Higher score.
    """
    score = 0
    
    # Supermarket (40 pts) - Ideal < 3 miles
    if supermarket_dist is not None:
        if supermarket_dist <= 3:
            score += 40
        elif supermarket_dist <= 10:
            score += 40 * ((10 - supermarket_dist) / 7)
            
    # Hospital (30 pts) - Ideal < 10 miles
    if hospital_dist is not None:
        if hospital_dist <= 5:
            score += 30
        elif hospital_dist <= 20:
            score += 30 * ((20 - hospital_dist) / 15)
            
    # School (30 pts) - Ideal < 5 miles
    if school_dist is not None:
        if school_dist <= 2:
            score += 30
        elif school_dist <= 10:
            score += 30 * ((10 - school_dist) / 8)
            
    return round(score, 1)

def enrich_leads(limit=None, force=False):
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_MAPS_API_KEY not found in .env")
        return

    try:
        gmaps = googlemaps.Client(key=api_key)
    except Exception as e:
        print(f"Error initializing Google Maps client: {e}")
        return

    init_db()
    leads = get_leads_to_enrich(limit, force)
    
    if not leads:
        print("No leads found requiring enrichment.")
        return

    print(f"Enriching {len(leads)} leads with amenity data...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for i, lead in enumerate(leads):
        lead_id = lead["id"]
        lat = lead["latitude"]
        lng = lead["longitude"]
        name = lead["name"]
        
        print(f"[{i+1}/{len(leads)}] Processing {name}...")
        
        location = (lat, lng)
        
        # 1. Supermarket
        market_name, market_dist = find_nearest_amenity(gmaps, location, "supermarket", "supermarket")
        
        # 2. Hospital
        hospital_name, hospital_dist = find_nearest_amenity(gmaps, location, "hospital", "hospital")
        
        # 3. School
        school_name, school_dist = find_nearest_amenity(gmaps, location, "school", "school")
        
        # Calculate score
        score = calculate_amenity_score(market_dist, hospital_dist, school_dist)
        
        # Update DB
        cursor.execute("""
            UPDATE leads SET 
                nearest_supermarket_name = ?, nearest_supermarket_dist = ?,
                nearest_hospital_name = ?, nearest_hospital_dist = ?,
                nearest_school_name = ?, nearest_school_dist = ?,
                amenity_score = ?
            WHERE id = ?
        """, (
            market_name, market_dist,
            hospital_name, hospital_dist,
            school_name, school_dist,
            score,
            lead_id
        ))
        
        print(f"  -> Score: {score}/100 | Market: {market_dist}mi | Hosp: {hospital_dist}mi | School: {school_dist}mi")
        conn.commit()

    conn.close()
    print("Enrichment complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich leads with Google Maps Amenity Data")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of leads to process")
    parser.add_argument("--force", action="store_true", help="Force re-enrichment of all leads")
    args = parser.parse_args()
    
    enrich_leads(limit=args.limit, force=args.force)

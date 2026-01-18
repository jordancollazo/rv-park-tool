"""
enrich_nfhl_flood_zone.py

Enriches leads with FEMA NFHL (National Flood Hazard Layer) flood zone data.
Queries the ArcGIS REST service to determine the flood zone for each lead's coordinates.

Usage:
    python execution/enrich_nfhl_flood_zone.py [--limit N]
"""

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
import urllib.request
import urllib.error

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

# FEMA NFHL ArcGIS REST endpoint - Layer 28 is Flood Hazard Zones
NFHL_QUERY_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"

# Coordinate rounding precision for cache (4 decimals = ~11m)
COORD_PRECISION = 4

# Cache TTL in days
CACHE_TTL_DAYS = 90

# Rate limiting
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 3

# Flood zone risk scoring (0-50 scale)
FLOOD_ZONE_SCORES = {
    # Highest risk - Coastal velocity zones
    "VE": 50,
    "V": 45,
    # High risk - 1% annual flood zones
    "AE": 40,
    "AH": 35,
    "AO": 35,
    "A": 35,
    # Moderate risk - Levee protected
    "A99": 25,
    "AR": 25,
    # Undetermined
    "D": 20,
    # Lower risk - 0.2% annual (shaded X)
    "X_SHADED": 15,
    # Minimal risk
    "X": 5,
}


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def round_coord(value: float) -> float:
    """Round coordinate to cache precision."""
    return round(value, COORD_PRECISION)


def get_cached_flood_zone(conn: sqlite3.Connection, lat: float, lon: float) -> dict | None:
    """Check cache for existing flood zone data."""
    lat_round = round_coord(lat)
    lon_round = round_coord(lon)
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT flood_zone, zone_subtype, sfha_tf, fetched_at
        FROM nfhl_cache
        WHERE lat_round = ? AND lon_round = ?
    """, (lat_round, lon_round))
    
    row = cursor.fetchone()
    if row:
        # Check if cache is still valid
        fetched_at = datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00"))
        age_days = (datetime.now(timezone.utc) - fetched_at).days
        
        if age_days < CACHE_TTL_DAYS:
            return {
                "flood_zone": row["flood_zone"],
                "zone_subtype": row["zone_subtype"],
                "sfha_tf": row["sfha_tf"],
                "source": "cache",
            }
    
    return None


def cache_flood_zone(conn: sqlite3.Connection, lat: float, lon: float, 
                     flood_zone: str, zone_subtype: str | None, sfha_tf: str | None):
    """Cache flood zone data for coordinates."""
    lat_round = round_coord(lat)
    lon_round = round_coord(lon)
    fetched_at = datetime.now(timezone.utc).isoformat()
    
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO nfhl_cache 
        (lat_round, lon_round, flood_zone, zone_subtype, sfha_tf, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (lat_round, lon_round, flood_zone, zone_subtype, sfha_tf, fetched_at))
    conn.commit()


def query_nfhl_api(lat: float, lon: float) -> dict | None:
    """
    Query FEMA NFHL ArcGIS REST API for flood zone at given coordinates.
    
    Returns dict with flood_zone, zone_subtype, sfha_tf or None on failure.
    """
    params = {
        "geometry": json.dumps({"x": lon, "y": lat}),
        "geometryType": "esriGeometryPoint",
        "inSR": "4269",  # NAD83
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "FLD_ZONE,ZONE_SUBTY,SFHA_TF",
        "returnGeometry": "false",
        "f": "json",
    }
    
    url = f"{NFHL_QUERY_URL}?{urlencode(params)}"
    
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "MHP-Outreach-Tool/1.0",
                "Accept": "application/json",
            })
            
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))
            
            # Check for API errors
            if "error" in data:
                print(f"    NFHL API error: {data['error']}")
                return None
            
            # Parse features
            features = data.get("features", [])
            if not features:
                # No flood zone data for this location (likely outside NFHL coverage)
                return {
                    "flood_zone": "X",
                    "zone_subtype": "AREA OF MINIMAL FLOOD HAZARD",
                    "sfha_tf": "F",
                }
            
            # Get attributes from first matching feature
            attrs = features[0].get("attributes", {})
            flood_zone = attrs.get("FLD_ZONE", "").strip()
            zone_subtype = attrs.get("ZONE_SUBTY", "")
            sfha_tf = attrs.get("SFHA_TF", "")
            
            # Handle null subtypes
            if zone_subtype in (None, "<Null>", ""):
                zone_subtype = None
            
            return {
                "flood_zone": flood_zone or "X",
                "zone_subtype": zone_subtype,
                "sfha_tf": sfha_tf,
            }
            
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Rate limited
                wait_time = (attempt + 1) * 5
                print(f"    Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"    HTTP error {e.code}: {e.reason}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(REQUEST_DELAY_SECONDS * (attempt + 1))
                    
        except urllib.error.URLError as e:
            print(f"    Network error: {e.reason}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(REQUEST_DELAY_SECONDS * (attempt + 1))
                
        except Exception as e:
            print(f"    Unexpected error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(REQUEST_DELAY_SECONDS * (attempt + 1))
    
    return None


def get_flood_zone_score(flood_zone: str, zone_subtype: str | None) -> int:
    """
    Get flood zone risk score (0-50).
    
    Considers zone type and subtype for nuanced scoring.
    """
    if not flood_zone:
        return FLOOD_ZONE_SCORES.get("X", 5)
    
    # Normalize zone
    zone = flood_zone.upper().strip()
    
    # Check for shaded X zone (0.2% annual chance)
    if zone == "X" and zone_subtype:
        subtype_upper = zone_subtype.upper()
        if "0.2 PCT" in subtype_upper or "0.2 PERCENT" in subtype_upper:
            return FLOOD_ZONE_SCORES.get("X_SHADED", 15)
        if "FUTURE CONDITIONS" in subtype_upper:
            return FLOOD_ZONE_SCORES.get("X_SHADED", 15)
    
    # Return base zone score
    return FLOOD_ZONE_SCORES.get(zone, FLOOD_ZONE_SCORES.get("X", 5))


def get_flood_zone_description(flood_zone: str, zone_subtype: str | None) -> str:
    """Get human-readable description of flood zone."""
    zone = (flood_zone or "X").upper().strip()
    
    descriptions = {
        "VE": "Coastal high hazard zone with velocity wave action",
        "V": "Coastal high hazard zone",
        "AE": "1% annual flood risk with base flood elevations",
        "AH": "1% annual flood risk with shallow flooding (1-3ft depth)",
        "AO": "1% annual flood risk with sheet flow flooding",
        "A": "1% annual flood risk (no base flood elevations determined)",
        "A99": "1% annual flood risk protected by federal levee under construction",
        "AR": "1% annual flood risk, temporarily increased due to levee restoration",
        "D": "Undetermined flood hazard (possible but not mapped)",
        "X": "Minimal flood hazard (outside 0.2% annual flood zone)",
    }
    
    base_desc = descriptions.get(zone, "Unknown flood zone classification")
    
    # Add subtype context if available
    if zone_subtype and zone == "X":
        if "0.2 PCT" in zone_subtype.upper() or "0.2 PERCENT" in zone_subtype.upper():
            return "0.2% annual flood risk (500-year flood zone)"
    
    return base_desc


import concurrent.futures

def process_lead_concurrent(lead, force_refresh):
    """Worker function for concurrent processing."""
    lead_id = lead["id"]
    lat = lead["latitude"]
    lon = lead["longitude"]
    
    # Each thread needs its own DB connection for reading cache if needed
    # But to simplify, we'll skip cache check in worker or handle it carefully.
    # Actually, for speed, let's assume we want to hit API or check cache safely.
    # Better: Perform the API call here, return result to main thread for DB write.
    
    # We won't check cache in worker to avoid DB locking issues or complexity.
    # The main thread handles filtering.
    
    result = query_nfhl_api(lat, lon)
    return lead_id, lat, lon, result

def enrich_flood_zones(limit: int | None = None, force_refresh: bool = False):
    """
    Enrich leads with FEMA NFHL flood zone data using multi-threading.
    """
    print("=" * 60)
    print("FEMA NFHL FLOOD ZONE ENRICHMENT (MULTI-THREADED)")
    print("=" * 60)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get leads
    if force_refresh:
        query = "SELECT id, name, latitude, longitude FROM leads WHERE latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY id"
    else:
        query = "SELECT id, name, latitude, longitude FROM leads WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND (flood_zone IS NULL OR flood_zone = '') ORDER BY id"
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    leads = cursor.fetchall()
    conn.close() # Close main conn, will reopen for writes
    
    print(f"Found {len(leads)} leads to process")
    if not leads:
        return

    # Process concurrently
    # Default to 10 workers to be polite but faster
    MAX_WORKERS = 10 
    
    updated_count = 0
    error_count = 0
    
    print(f"Starting {MAX_WORKERS} worker threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Map leads to futures
        params = [(l, force_refresh) for l in leads]
        future_to_lead = {executor.submit(process_lead_concurrent, l, force_refresh): l for l in leads}
        
        # Re-open DB for writing
        conn = get_db()
        cursor = conn.cursor()
        
        try:
            for i, future in enumerate(concurrent.futures.as_completed(future_to_lead)):
                lead = future_to_lead[future]
                try:
                    lead_id, lat, lon, result = future.result()
                    
                    if result:
                        flood_zone = result["flood_zone"]
                        zone_subtype = result["zone_subtype"]
                        sfha_tf = result["sfha_tf"]
                        source = "NFHL"
                        
                        # Cache it (write to DB)
                        cache_flood_zone(conn, lat, lon, flood_zone, zone_subtype, sfha_tf)
                        
                        # Update lead
                        cursor.execute("""
                            UPDATE leads
                            SET flood_zone = ?, flood_zone_source = ?
                            WHERE id = ?
                        """, (flood_zone, source, lead_id))
                        
                        updated_count += 1
                        print(f"[{i+1}/{len(leads)}] {lead['name'][:30]} -> {flood_zone}")
                    else:
                        error_count += 1
                        print(f"[{i+1}/{len(leads)}] {lead['name'][:30]} -> ERROR")
                        
                    if updated_count % 20 == 0:
                        conn.commit()
                        
                except Exception as exc:
                    print(f"Generated an exception: {exc}")
                    error_count += 1
                    
        finally:
            conn.commit()
            conn.close()
            
    print("\n" + "=" * 60)
    print(f"COMPLETE. Updated: {updated_count}, Errors: {error_count}")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    enrich_flood_zones(limit=args.limit, force_refresh=args.force)

if __name__ == "__main__":
    main()

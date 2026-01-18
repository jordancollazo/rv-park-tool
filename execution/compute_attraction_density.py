"""
compute_attraction_density.py

Computes tourist attraction density scores for Florida ZCTAs.
Queries Google Places Nearby Search for tourist POIs and stores
attraction_density_score in the zcta_metrics table.

Usage:
    python execution/compute_attraction_density.py --dry-run --limit 5  # Test without API
    python execution/compute_attraction_density.py --limit 5             # Test with 5 ZCTAs
    python execution/compute_attraction_density.py                       # Full run
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from math import radians, cos, sin, asin, sqrt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Paths
DB_PATH = Path("data/leads.db")
GEOJSON_PATH = Path(".tmp/florida_zcta_boundaries.geojson")

# Google Places POI types to query
POI_TYPES = [
    "tourist_attraction",
    "amusement_park",
    "campground",
    "rv_park",
    "beach",
]

# Search radius in meters (10 miles ≈ 16,093 meters)
SEARCH_RADIUS_METERS = 16000


def get_db_connection():
    """Get database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_columns_exist():
    """Add attraction density columns if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(zcta_metrics)")
    existing_columns = {row["name"] for row in cursor.fetchall()}
    
    columns_to_add = [
        ("attraction_density_score", "REAL"),
        ("attraction_count", "INTEGER"),
        ("attraction_data_json", "TEXT"),
    ]
    
    for col_name, col_type in columns_to_add:
        if col_name not in existing_columns:
            print(f"Adding column: {col_name}")
            cursor.execute(f"ALTER TABLE zcta_metrics ADD COLUMN {col_name} {col_type}")
    
    conn.commit()
    conn.close()


def calculate_centroid(geometry):
    """
    Calculate approximate centroid of a GeoJSON geometry.
    Returns (lat, lon) or None.
    """
    if not geometry:
        return None
        
    coords = []
    
    def extract_coords(coord_list):
        for item in coord_list:
            if isinstance(item[0], (list, tuple)):
                extract_coords(item)
            else:
                coords.append(item)
    
    g_type = geometry.get("type")
    raw_coords = geometry.get("coordinates", [])
    
    if g_type == "Polygon":
        if raw_coords:
            extract_coords(raw_coords[0])
    elif g_type == "MultiPolygon":
        extract_coords(raw_coords)
    else:
        return None
        
    if not coords:
        return None
        
    sum_lon = sum(c[0] for c in coords)
    sum_lat = sum(c[1] for c in coords)
    n = len(coords)
    
    return (sum_lat / n, sum_lon / n)


def load_zcta_centroids():
    """
    Load ZCTA centroids from GeoJSON file.
    Returns dict of {zcta: (lat, lon)}.
    """
    if not GEOJSON_PATH.exists():
        print(f"ERROR: GeoJSON file not found at {GEOJSON_PATH}")
        print("Run: python execution/fetch_zcta_boundaries.py")
        return {}
    
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geojson = json.load(f)
    
    centroids = {}
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        zcta = props.get("ZCTA5CE20") or props.get("ZCTA5CE10") or props.get("zcta")
        
        if not zcta:
            continue
        
        geometry = feature.get("geometry")
        centroid = calculate_centroid(geometry)
        
        if centroid:
            centroids[zcta] = centroid
    
    return centroids


def get_zctas_needing_enrichment(limit=None):
    """Get ZCTAs that don't have attraction data yet."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT zcta FROM zcta_metrics 
        WHERE attraction_density_score IS NULL
        ORDER BY vacation_score DESC NULLS LAST
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    zctas = [row["zcta"] for row in cursor.fetchall()]
    conn.close()
    
    return zctas


def query_places_nearby(gmaps, location, poi_type):
    """
    Query Google Places Nearby Search for a specific POI type.
    Returns list of places found.
    """
    try:
        result = gmaps.places_nearby(
            location=location,
            radius=SEARCH_RADIUS_METERS,
            type=poi_type,
        )
        
        if result.get("status") == "OK":
            return result.get("results", [])
        elif result.get("status") == "ZERO_RESULTS":
            return []
        else:
            print(f"    Warning: Places API returned {result.get('status')}")
            return []
            
    except Exception as e:
        print(f"    Error querying {poi_type}: {e}")
        return []


def compute_density_score(total_count):
    """
    Normalize attraction count to 0-100 score.
    0 attractions = 0 score
    20+ attractions = 100 score (saturates)
    """
    if total_count <= 0:
        return 0.0
    elif total_count >= 20:
        return 100.0
    else:
        return round((total_count / 20) * 100, 2)


def enrich_zcta(gmaps, zcta, centroid, dry_run=False):
    """
    Enrich a single ZCTA with attraction density data.
    Returns (attraction_count, attraction_density_score, data_json).
    """
    lat, lon = centroid
    
    if dry_run:
        print(f"  [DRY RUN] Would query {len(POI_TYPES)} POI types at ({lat:.4f}, {lon:.4f})")
        return None, None, None
    
    all_places = {}
    total_count = 0
    
    for poi_type in POI_TYPES:
        places = query_places_nearby(gmaps, (lat, lon), poi_type)
        count = len(places)
        total_count += count
        all_places[poi_type] = count
        
        # Rate limit: 1 request per second
        time.sleep(1)
    
    score = compute_density_score(total_count)
    data_json = json.dumps(all_places)
    
    return total_count, score, data_json


def update_zcta_in_db(zcta, count, score, data_json):
    """Update a single ZCTA's attraction data in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE zcta_metrics SET
            attraction_count = ?,
            attraction_density_score = ?,
            attraction_data_json = ?
        WHERE zcta = ?
    """, (count, score, data_json, zcta))
    
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Compute attraction density for Florida ZCTAs")
    parser.add_argument("--dry-run", action="store_true", help="Test without API calls")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of ZCTAs to process")
    parser.add_argument("--force", action="store_true", help="Re-process ZCTAs that already have data")
    args = parser.parse_args()
    
    print("=" * 60)
    print("GOOGLE PLACES ATTRACTION DENSITY ENRICHMENT")
    print("=" * 60)
    
    if args.dry_run:
        print("MODE: DRY RUN (no API calls)")
    else:
        print("MODE: LIVE (will make API calls)")
    
    if args.limit:
        print(f"LIMIT: {args.limit} ZCTAs")
    
    # Check for API key
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key and not args.dry_run:
        print("\nERROR: GOOGLE_MAPS_API_KEY not found in .env")
        print("Add your API key to .env file:")
        print('  GOOGLE_MAPS_API_KEY="your-api-key-here"')
        sys.exit(1)
    
    # Initialize Google Maps client
    gmaps = None
    if not args.dry_run:
        try:
            import googlemaps
            gmaps = googlemaps.Client(key=api_key)
            print("Google Maps client initialized")
        except ImportError:
            print("\nERROR: googlemaps package not installed")
            print("Run: pip install googlemaps")
            sys.exit(1)
        except Exception as e:
            print(f"\nERROR initializing Google Maps client: {e}")
            sys.exit(1)
    
    # Ensure columns exist
    print("\nEnsuring database columns exist...")
    ensure_columns_exist()
    
    # Load centroids
    print("Loading ZCTA centroids from GeoJSON...")
    centroids = load_zcta_centroids()
    print(f"  Found {len(centroids)} centroids")
    
    if not centroids:
        print("\nERROR: No centroids found. Run fetch_zcta_boundaries.py first.")
        sys.exit(1)
    
    # Get ZCTAs needing enrichment
    if args.force:
        # Get all ZCTAs
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT zcta FROM zcta_metrics"
        if args.limit:
            query += f" LIMIT {args.limit}"
        cursor.execute(query)
        zctas_to_process = [row["zcta"] for row in cursor.fetchall()]
        conn.close()
    else:
        zctas_to_process = get_zctas_needing_enrichment(args.limit)
    
    # Filter to those with centroids
    zctas_with_centroids = [z for z in zctas_to_process if z in centroids]
    
    print(f"\nZCTAs to process: {len(zctas_with_centroids)}")
    
    if not zctas_with_centroids:
        print("No ZCTAs need enrichment (or no matching centroids found).")
        return
    
    # Estimate cost
    if not args.dry_run:
        total_calls = len(zctas_with_centroids) * len(POI_TYPES)
        cost_estimate = total_calls * 0.017
        print(f"\nEstimated API calls: {total_calls}")
        print(f"Estimated cost: ${cost_estimate:.2f}")
    
    print("\n" + "-" * 60)
    print("PROCESSING")
    print("-" * 60)
    
    processed = 0
    for i, zcta in enumerate(zctas_with_centroids):
        centroid = centroids[zcta]
        print(f"\n[{i+1}/{len(zctas_with_centroids)}] ZCTA {zcta} @ ({centroid[0]:.4f}, {centroid[1]:.4f})")
        
        count, score, data_json = enrich_zcta(gmaps, zcta, centroid, args.dry_run)
        
        if count is not None:
            update_zcta_in_db(zcta, count, score, data_json)
            print(f"  -> {count} attractions, score: {score}/100")
            processed += 1
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"ZCTAs processed: {processed}")
    
    if not args.dry_run and processed > 0:
        print("\nSample results:")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT zcta, attraction_count, attraction_density_score 
            FROM zcta_metrics 
            WHERE attraction_density_score IS NOT NULL 
            ORDER BY attraction_density_score DESC 
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(f"  ZCTA {row['zcta']}: {row['attraction_count']} attractions, score {row['attraction_density_score']}")
        conn.close()


if __name__ == "__main__":
    main()

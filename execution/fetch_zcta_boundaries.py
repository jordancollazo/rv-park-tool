"""
fetch_zcta_boundaries.py

Downloads Florida ZCTA (zip code) boundary polygons from Census TIGER/Line files.
Outputs GeoJSON for use in choropleth map rendering.

The boundaries are used to color zip code areas by opportunity score, population
growth, or housing affordability.

Usage:
    python execution/fetch_zcta_boundaries.py
    python execution/fetch_zcta_boundaries.py --refresh
"""

import argparse
import json
import sqlite3
import zipfile
from io import BytesIO
from pathlib import Path

import requests

# Census TIGER/Line Shapefiles - ZCTA boundaries
# Using 2022 TIGER/Line files (most recent available)
TIGER_ZCTA_URL = "https://www2.census.gov/geo/tiger/TIGER2022/ZCTA520/tl_2022_us_zcta520.zip"

# Alternative: Pre-converted GeoJSON from alternative source (smaller file)
GEOJSON_URL = "https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/fl_florida_zip_codes_geo.min.json"

# Output paths
OUTPUT_GEOJSON = Path(".tmp/florida_zcta_boundaries.geojson")
DB_PATH = Path("data/leads.db")

# Florida bounding box for filtering
FL_BOUNDS = {
    "min_lat": 24.396308,
    "max_lat": 31.000968,
    "min_lon": -87.634896,
    "max_lon": -79.974306
}


def fetch_geojson_direct() -> dict | None:
    """
    Fetch pre-converted GeoJSON from alternative source.
    This is faster and smaller than processing shapefiles.
    """
    print("  Fetching Florida zip code GeoJSON...")
    
    try:
        response = requests.get(GEOJSON_URL, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        print(f"  Retrieved {len(data.get('features', []))} zip code boundaries")
        return data
        
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return None


def filter_florida_zips(geojson: dict, florida_zctas: set[str] | None = None) -> dict:
    """
    Filter GeoJSON features to only Florida zip codes.
    
    Args:
        geojson: Full GeoJSON FeatureCollection
        florida_zctas: Optional set of known Florida ZCTAs to filter by
    
    Returns:
        Filtered GeoJSON with only Florida features
    """
    features = geojson.get("features", [])
    florida_features = []
    
    for feature in features:
        props = feature.get("properties", {})
        
        # Try different property names for zip code
        zcta = (
            props.get("ZCTA5CE20") or 
            props.get("ZCTA5CE10") or 
            props.get("ZCTA") or
            props.get("GEOID20") or
            props.get("GEOID10") or
            props.get("GEOID") or
            props.get("zip") or
            props.get("ZIP") or
            props.get("ZIPCODE")
        )
        
        if not zcta:
            continue
        
        zcta = str(zcta).zfill(5)
        
        # If we have a known list of Florida ZCTAs, use that
        if florida_zctas and zcta in florida_zctas:
            feature["properties"]["zcta"] = zcta
            florida_features.append(feature)
            continue
        
        # Otherwise filter by Florida zip code prefixes (32xxx, 33xxx, 34xxx)
        if zcta.startswith(("32", "33", "34")):
            feature["properties"]["zcta"] = zcta
            florida_features.append(feature)
    
    return {
        "type": "FeatureCollection",
        "features": florida_features
    }


def get_florida_zctas_from_db() -> set[str]:
    """Get set of Florida ZCTAs from the zcta_metrics table."""
    if not DB_PATH.exists():
        return set()
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT zcta FROM zcta_metrics")
        zctas = {row[0] for row in cursor.fetchall()}
        conn.close()
        return zctas
    except sqlite3.Error:
        return set()


def merge_metrics_to_geojson(geojson: dict) -> dict:
    """
    Merge opportunity metrics from database into GeoJSON properties.
    This allows the map to render choropleth colors based on the metrics.
    """
    if not DB_PATH.exists():
        print("  Database not found, skipping metric merge")
        return geojson
    

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        

        cursor.execute("""
            SELECT zcta, population_growth_rate, price_to_income_ratio,
                   mobile_home_percentage, vacancy_rate, opportunity_score,
                   opportunity_rank, median_home_value, median_household_income,
                   population_2023 as population_2022, -- Alias for backward compat
                   displacement_score, path_of_progress_score, snowbird_score,
                   slumlord_rehab_score, exurb_score, distance_to_nearest_metro,
                   rent_burden, senior_percentage, income_growth_rate
            FROM zcta_metrics
        """)
        
        metrics_by_zcta = {row["zcta"]: dict(row) for row in cursor.fetchall()}
        conn.close()
        
        merged_count = 0
        for feature in geojson.get("features", []):
            zcta = feature.get("properties", {}).get("zcta")
            if zcta and zcta in metrics_by_zcta:
                feature["properties"].update(metrics_by_zcta[zcta])
                merged_count += 1
        
        print(f"  Merged metrics for {merged_count} zip codes")
        return geojson
        
    except sqlite3.Error as e:
        print(f"  ERROR merging metrics: {e}")
        return geojson


def simplify_geometry(geojson: dict, tolerance: float = 0.001) -> dict:
    """
    Simplify GeoJSON geometries to reduce file size.
    Uses a simple point reduction algorithm.
    
    Note: For production, use shapely or similar library.
    This is a basic implementation that just reduces coordinate precision.
    """
    for feature in geojson.get("features", []):
        geometry = feature.get("geometry", {})
        if geometry.get("type") == "Polygon":
            coords = geometry.get("coordinates", [])
            simplified = []
            for ring in coords:
                # Reduce precision to 4 decimal places (~11m accuracy)
                simplified_ring = [[round(c[0], 4), round(c[1], 4)] for c in ring]
                simplified.append(simplified_ring)
            geometry["coordinates"] = simplified
        elif geometry.get("type") == "MultiPolygon":
            coords = geometry.get("coordinates", [])
            simplified = []
            for polygon in coords:
                simplified_polygon = []
                for ring in polygon:
                    simplified_ring = [[round(c[0], 4), round(c[1], 4)] for c in ring]
                    simplified_polygon.append(simplified_ring)
                simplified.append(simplified_polygon)
            geometry["coordinates"] = simplified
    
    return geojson


def save_geojson(geojson: dict, output_path: Path):
    """Save GeoJSON to file."""
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f)
    
    size_kb = output_path.stat().st_size / 1024
    print(f"\nSaved to {output_path} ({size_kb:.1f} KB)")



# Major Metro Coordinates (Lat, Lon)
METROS = {
    "orlando": (28.5383, -81.3792),
    "tampa": (27.9506, -82.4572),
    "jacksonville": (30.3322, -81.6557),
    "miami": (25.7617, -80.1918)
}

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in miles between two points 
    on the earth (specified in decimal degrees)
    """
    import math
    
    # Convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 3956 # Radius of earth in miles. Use 6371 for km
    return c * r

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
    
    # Handle different geometry types
    g_type = geometry.get("type")
    raw_coords = geometry.get("coordinates", [])
    
    if g_type == "Polygon":
        # Polygon coordinates are list of rings (list of points)
        # We only care about the outer ring (first one)
        if raw_coords:
            extract_coords(raw_coords[0])
    elif g_type == "MultiPolygon":
        # MultiPolygon is list of Polygons
        # We'll just dump all points from all polygons for a rough center
        extract_coords(raw_coords)
    else:
        return None
        
    if not coords:
        return None
        
    # Simple average of points (adequate for ZCTA centers)
    # Be careful with 180th meridian but not an issue for Florida
    sum_lon = sum(c[0] for c in coords)
    sum_lat = sum(c[1] for c in coords)
    n = len(coords)
    
    return (sum_lat / n, sum_lon / n)

def update_db_with_geojson(geojson: dict):
    """
    Update zcta_metrics table with GeoJSON and distance metrics for each ZCTA.
    """
    if not DB_PATH.exists():
        print("  Database not found, skipping GeoJSON storage")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Add columns if they don't exist
        cursor.execute("PRAGMA table_info(zcta_metrics)")
        columns = [row[1] for row in cursor.fetchall()]
        
        new_cols = [
            ("geojson", "TEXT"),
            ("distance_to_orlando", "REAL"),
            ("distance_to_tampa", "REAL"),
            ("distance_to_jacksonville", "REAL"),
            ("distance_to_miami", "REAL"),
            ("distance_to_nearest_metro", "REAL")
        ]
        
        for col_name, col_type in new_cols:
            if col_name not in columns:
                print(f"  Adding column {col_name}...")
                cursor.execute(f"ALTER TABLE zcta_metrics ADD COLUMN {col_name} {col_type}")
        
        # Update each ZCTA with its boundary and calculated distances
        updated = 0
        for feature in geojson.get("features", []):
            zcta = feature.get("properties", {}).get("zcta")
            if zcta:
                geometry = feature.get("geometry", {})
                geom_json = json.dumps(geometry)
                
                # Calculate distances
                centroid = calculate_centroid(geometry)
                distances = {}
                min_dist = 9999
                
                if centroid:
                    lat, lon = centroid
                    for city, (c_lat, c_lon) in METROS.items():
                        dist = haversine_distance(lat, lon, c_lat, c_lon)
                        distances[f"distance_to_{city}"] = dist
                        if dist < min_dist:
                            min_dist = dist
                    
                    distances["distance_to_nearest_metro"] = min_dist
                
                # Update query
                cursor.execute("""
                    UPDATE zcta_metrics SET 
                        geojson = ?,
                        distance_to_orlando = ?,
                        distance_to_tampa = ?,
                        distance_to_jacksonville = ?,
                        distance_to_miami = ?,
                        distance_to_nearest_metro = ?
                    WHERE zcta = ?
                """, (
                    geom_json,
                    distances.get("distance_to_orlando"),
                    distances.get("distance_to_tampa"),
                    distances.get("distance_to_jacksonville"),
                    distances.get("distance_to_miami"),
                    distances.get("distance_to_nearest_metro"),
                    zcta
                ))
                
                if cursor.rowcount > 0:
                    updated += 1
        
        conn.commit()
        conn.close()
        print(f"  Updated {updated} ZCTAs with boundary geometry and distances")
        
    except sqlite3.Error as e:
        print(f"  ERROR updating database: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch Florida ZCTA boundaries")
    parser.add_argument("--refresh", action="store_true", help="Force refresh existing data")
    parser.add_argument("--no-db", action="store_true", help="Skip database operations")
    parser.add_argument("--no-simplify", action="store_true", help="Skip geometry simplification")
    
    args = parser.parse_args()
    
    print("="*60)
    print("FLORIDA ZCTA BOUNDARY FETCH")
    print("="*60)
    
    # Check for existing data
    if OUTPUT_GEOJSON.exists() and not args.refresh:
        print(f"\nExisting data found at {OUTPUT_GEOJSON}")
        print("Use --refresh to re-fetch")
        
        with open(OUTPUT_GEOJSON, "r") as f:
            existing = json.load(f)
        print(f"Contains {len(existing.get('features', []))} boundaries")
        return
    
    # Fetch GeoJSON
    print("\n[1/4] Fetching boundary data...")
    geojson = fetch_geojson_direct()
    
    if not geojson:
        print("ERROR: Failed to fetch boundary data")
        return
    
    # Filter to Florida
    print("\n[2/4] Filtering to Florida zip codes...")
    florida_zctas = get_florida_zctas_from_db() if not args.no_db else None
    geojson = filter_florida_zips(geojson, florida_zctas)
    print(f"  Florida zip codes: {len(geojson.get('features', []))}")
    
    # Simplify geometries
    if not args.no_simplify:
        print("\n[3/4] Simplifying geometries...")
        geojson = simplify_geometry(geojson)
    
    # Merge metrics from database
    if not args.no_db:
        print("\n[4/4] Merging opportunity metrics...")
        geojson = merge_metrics_to_geojson(geojson)
    
    # Save results
    save_geojson(geojson, OUTPUT_GEOJSON)
    
    if not args.no_db:
        update_db_with_geojson(geojson)
    
    print(f"\n{'='*60}")
    print("COMPLETE")
    print(f"{'='*60}")
    print(f"Boundaries saved: {len(geojson.get('features', []))}")
    print(f"Output file: {OUTPUT_GEOJSON}")


if __name__ == "__main__":
    main()

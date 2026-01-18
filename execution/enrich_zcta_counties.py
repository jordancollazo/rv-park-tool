"""
enrich_zcta_counties.py

Adds county names to ZCTA metrics using free zip-to-county API.

Usage:
    python execution/enrich_zcta_counties.py
"""

import json
import sqlite3
import time
from pathlib import Path

import requests

DB_PATH = Path("data/leads.db")

# Simple API for zip-to-county lookup
ZIPPOPOTAM_URL = "https://api.zippopotam.us/us/{zip_code}"


def get_county_for_zip(zip_code: str) -> str | None:
    """Get county name for a zip code using free API."""
    try:
        response = requests.get(
            ZIPPOPOTAM_URL.format(zip_code=zip_code),
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            places = data.get("places", [])
            if places:
                # Get county from first place (usually the primary one)
                county = places[0].get("state abbreviation", "")
                # Zippopotam doesn't give county directly, need alternate
                return None
    except Exception:
        pass
    return None


def build_florida_zip_county_map() -> dict:
    """
    Build a zip-to-county mapping for Florida using Census crosswalk.
    Downloads the 2020 ZCTA-to-County relationship file.
    """
    # First, get all Florida county names with FIPS codes
    print("Fetching Florida county list...")
    county_url = "https://api.census.gov/data/2020/dec/dhc"
    params = {
        "get": "NAME",
        "for": "county:*",
        "in": "state:12"
    }
    
    try:
        response = requests.get(county_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Build FIPS -> County Name map
        fips_to_county = {}
        for row in data[1:]:  # Skip header
            name = row[0].replace(", Florida", "").replace(" County", "")
            county_fips = row[2]
            fips_to_county[county_fips] = name
        
        print(f"  Found {len(fips_to_county)} Florida counties")
        return fips_to_county
        
    except Exception as e:
        print(f"  ERROR: {e}")
        return {}


def enrich_from_geojson():
    """
    Try to get county info from the GeoJSON properties.
    """
    geojson_path = Path(".tmp/florida_zcta_boundaries.geojson")
    if not geojson_path.exists():
        return {}
    
    with open(geojson_path, "r") as f:
        data = json.load(f)
    
    zip_to_county = {}
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        zcta = props.get("zcta") or props.get("ZCTA5CE20") or props.get("GEOID20")
        county = props.get("COUNTY") or props.get("county")
        if zcta and county:
            zip_to_county[str(zcta)] = county
    
    return zip_to_county


def update_database_with_counties(zip_to_county: dict):
    """Update zcta_metrics table with county names."""
    if not DB_PATH.exists():
        print("Database not found")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Add county column if it doesn't exist
    cursor.execute("PRAGMA table_info(zcta_metrics)")
    columns = [row[1] for row in cursor.fetchall()]
    if "county" not in columns:
        cursor.execute("ALTER TABLE zcta_metrics ADD COLUMN county TEXT")
        print("Added county column to zcta_metrics")
    
    # Update counties
    updated = 0
    for zcta, county in zip_to_county.items():
        cursor.execute(
            "UPDATE zcta_metrics SET county = ? WHERE zcta = ?",
            (county, zcta)
        )
        if cursor.rowcount > 0:
            updated += 1
    
    conn.commit()
    conn.close()
    
    print(f"Updated {updated} ZCTAs with county names")


# Florida ZIP code prefix to county mapping (major areas)
# This is a simplified mapping based on ZIP code ranges
FL_ZIP_COUNTY_APPROX = {
    # North Florida
    "320": "Duval",
    "321": "Duval",
    "322": "Duval", 
    "323": "Columbia",
    "324": "Bay",
    "325": "Leon",
    "326": "Alachua",
    "327": "Marion",
    "328": "Volusia",
    "329": "Brevard",
    # Central Florida
    "330": "Miami-Dade",
    "331": "Miami-Dade",
    "332": "Miami-Dade",
    "333": "Broward",
    "334": "Palm Beach",
    "335": "Hendry",
    "336": "Hillsborough",
    "337": "Hillsborough",
    "338": "Polk",
    "339": "Brevard",
    # West Florida
    "340": "Charlotte",
    "341": "Manatee",
    "342": "Pasco",
    "344": "Citrus",
    "346": "Pinellas",
    "347": "Orange",
    "348": "Osceola",
    "349": "Lake",
}


def get_county_from_zip_prefix(zcta: str) -> str:
    """Get approximate county from ZIP prefix."""
    prefix = zcta[:3]
    return FL_ZIP_COUNTY_APPROX.get(prefix, "")


def enrich_via_geocoding():
    """
    Enrich ZCTAs with county names using the GeoJSON centroid
    and Census reverse geocoding.
    """
    geojson_path = Path(".tmp/florida_zcta_boundaries.geojson")
    if not geojson_path.exists():
        print("GeoJSON not found")
        return {}
    
    print("Loading GeoJSON boundaries...")
    with open(geojson_path, "r") as f:
        data = json.load(f)
    
    # Get FIPS to county name mapping
    fips_to_county = build_florida_zip_county_map()
    if not fips_to_county:
        return {}
    
    zip_to_county = {}
    
    print("Getting county names via reverse geocoding...")
    total = len(data.get("features", []))
    
    for i, feature in enumerate(data.get("features", [])):
        props = feature.get("properties", {})
        zcta = props.get("zcta")
        
        if not zcta:
            continue
        
        # Get centroid of the geometry
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates", [])
        
        # Calculate rough centroid (just use first point for speed)
        try:
            if geometry.get("type") == "Polygon":
                first_ring = coords[0] if coords else []
                if first_ring:
                    lon = first_ring[0][0]
                    lat = first_ring[0][1]
            elif geometry.get("type") == "MultiPolygon":
                first_polygon = coords[0] if coords else []
                first_ring = first_polygon[0] if first_polygon else []
                if first_ring:
                    lon = first_ring[0][0]
                    lat = first_ring[0][1]
            else:
                continue
            
            # Reverse geocode to get county
            url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
            params = {
                "x": lon,
                "y": lat,
                "benchmark": "Public_AR_Current",
                "vintage": "Current_Current",
                "format": "json"
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                result = response.json()
                counties = result.get("result", {}).get("geographies", {}).get("Counties", [])
                if counties:
                    county_fips = counties[0].get("COUNTY", "")
                    county_name = fips_to_county.get(county_fips, counties[0].get("NAME", ""))
                    if county_name:
                        zip_to_county[zcta] = county_name.replace(" County", "")
            
            # Progress every 50
            if (i + 1) % 50 == 0:
                print(f"  Processed {i+1}/{total} ZCTAs...")
            
            # Rate limit
            time.sleep(0.1)
            
        except Exception as e:
            # Use prefix fallback
            county = get_county_from_zip_prefix(zcta)
            if county:
                zip_to_county[zcta] = county
    
    return zip_to_county


def quick_enrich_via_prefix():
    """Quick enrichment using ZIP prefix approximation."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Add county column if it doesn't exist
    cursor.execute("PRAGMA table_info(zcta_metrics)")
    columns = [row[1] for row in cursor.fetchall()]
    if "county" not in columns:
        cursor.execute("ALTER TABLE zcta_metrics ADD COLUMN county TEXT")
        conn.commit()
        print("Added county column")
    
    # Get all ZCTAs
    cursor.execute("SELECT zcta FROM zcta_metrics")
    zctas = [row[0] for row in cursor.fetchall()]
    
    # Get FIPS to county name mapping
    fips_to_county = build_florida_zip_county_map()
    
    updated = 0
    for zcta in zctas:
        county = get_county_from_zip_prefix(zcta)
        if county:
            cursor.execute(
                "UPDATE zcta_metrics SET county = ? WHERE zcta = ? AND (county IS NULL OR county = '')",
                (county, zcta)
            )
            if cursor.rowcount > 0:
                updated += 1
    
    conn.commit()
    conn.close()
    
    print(f"Quick-enriched {updated} ZCTAs with approximate county names")


def main():
    print("="*60)
    print("ZCTA COUNTY ENRICHMENT")
    print("="*60)
    
    # Try quick prefix-based enrichment first
    quick_enrich_via_prefix()
    
    # Then try geocoding for accuracy (slower)
    print("\nDo you want to run full geocoding enrichment? (slower but more accurate)")
    print("This is optional - the prefix-based enrichment covers most cases.")


if __name__ == "__main__":
    main()

import sqlite3
import json
import logging
import requests
from pathlib import Path
from shapely.geometry import shape, Point

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = Path("data/leads.db")
DATA_DIR = Path("data/tiger")
GEOJSON_PATH = DATA_DIR / "fl_counties.geojson"
# Source: Plotly's GitHub (Reliable mirror of Census data)
GEOJSON_URL = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

def download_geojson():
    """Download county GeoJSON if needed."""
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True)
    
    if GEOJSON_PATH.exists():
        return

    logging.info("Downloading County GeoJSON...")
    try:
        r = requests.get(GEOJSON_URL, timeout=60)
        r.raise_for_status()
        data = r.json()
        
        # Filter for Florida (State FIPS 12)
        fl_features = [f for f in data['features'] if f['id'].startswith('12')]
        
        fl_data = {
            "type": "FeatureCollection",
            "features": fl_features
        }
        
        with open(GEOJSON_PATH, 'w') as f:
            json.dump(fl_data, f)
        
        logging.info(f"Saved {len(fl_features)} Florida counties to local cache.")
        
    except Exception as e:
        logging.error(f"Failed to download GeoJSON: {e}")
        raise

def enrich_leads():
    """Assign county_fips to leads without it."""
    if not GEOJSON_PATH.exists():
        download_geojson()
        
    with open(GEOJSON_PATH, 'r') as f:
        geojson = json.load(f)
    
    # Prepare shapes
    counties = []
    for feature in geojson['features']:
        geom = shape(feature['geometry'])
        fips = feature['id']
        name = feature['properties']['NAME']
        counties.append((fips, name, geom))
        
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get leads with missing county_fips but valid lat/lon
    cursor.execute("SELECT id, latitude, longitude FROM leads WHERE county_fips IS NULL AND latitude IS NOT NULL")
    leads = cursor.fetchall()
    
    if not leads:
        logging.info("No leads need enrichment.")
        return

    logging.info(f"Enriching {len(leads)} leads...")
    
    updates = []
    
    for lead in leads:
        pt = Point(lead['longitude'], lead['latitude'])
        match = None
        
        # Simple linear search (Optimization: Spatial Index/R-tree if slow, but for <10k leads and 67 counties, this is fast enough)
        for fips, name, geom in counties:
            if geom.contains(pt):
                match = (fips, name)
                break
        
        if match:
            updates.append((match[0], match[1], lead['id']))
            
    if updates:
        cursor.executemany("UPDATE leads SET county_fips = ?, county_name = ? WHERE id = ?", updates)
        conn.commit()
        logging.info(f"Updated {len(updates)} leads with county info.")
    else:
        logging.info("No county matches found (leads might be outside FL).")
        
    conn.close()

if __name__ == "__main__":
    enrich_leads()

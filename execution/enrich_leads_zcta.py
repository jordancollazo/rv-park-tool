
import sqlite3
import json
import logging
from pathlib import Path
from shapely.geometry import shape, Point

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = Path('data/leads.db')
GEOJSON_PATH = Path('.tmp/florida_zcta_boundaries.geojson')

def enrich_leads_zcta():
    if not GEOJSON_PATH.exists():
        logging.error(f"GeoJSON not found at {GEOJSON_PATH}")
        return

    logging.info("Loading ZCTA boundaries...")
    with open(GEOJSON_PATH, 'r') as f:
        geojson = json.load(f)

    # Prepare polygons
    zcta_polygons = []
    for feature in geojson['features']:
        props = feature['properties']
        zcta = props.get('zcta') or props.get('ZCTA5CE10')
        geom = shape(feature['geometry'])
        zcta_polygons.append((zcta, geom))
    
    logging.info(f"Loaded {len(zcta_polygons)} ZCTA polygons.")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get leads with lat/lon
    cursor.execute("SELECT id, name, latitude, longitude FROM leads WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    leads = cursor.fetchall()
    logging.info(f"Processing {len(leads)} leads...")

    updates = []
    matches = 0

    for lead in leads:
        pt = Point(lead['longitude'], lead['latitude'])
        found_zcta = None
        
        # Simple linear search (spatial index would be faster but unnecessary for <1000 leads)
        for zcta, poly in zcta_polygons:
            if poly.contains(pt):
                found_zcta = zcta
                matches += 1
                break
        
        updates.append((found_zcta, lead['id']))

    cursor.executemany("UPDATE leads SET zcta_derived = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()
    
    logging.info(f"Enrichment complete. {matches}/{len(leads)} leads assigned to a ZCTA.")

if __name__ == "__main__":
    enrich_leads_zcta()

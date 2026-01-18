
import sqlite3
import json
from pathlib import Path

DB_PATH = Path("data/leads.db")

def verify():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("Querying Crexi leads...")
    rows = cursor.execute("""
        SELECT 
            name, 
            address,
            city, 
            state,
            zip,
            asking_price, 
            cap_rate,
            noi, 
            occupancy,
            sq_ft,
            year_built,
            lease_type,
            tenancy,
            days_on_market,
            broker_name,
            broker_company,
            tax_shock_score_0_100, 
            owner_fatigue_score_0_100,
            scrape_source,
            listing_url
        FROM leads 
        WHERE scrape_source='crexi'
    """).fetchall()
    
    results = [dict(r) for r in rows]
    print(json.dumps(results, indent=2))
    conn.close()

if __name__ == "__main__":
    verify()


import sqlite3

def inspect():
    conn = sqlite3.connect("data/leads.db")
    cursor = conn.execute("PRAGMA table_info(leads)")
    rows = cursor.fetchall()
    print(f"Total columns: {len(rows)}")
    existing = {r[1] for r in rows}
    
    expected = [
        'nearest_supermarket_name', 'nearest_supermarket_dist',
        'nearest_hospital_name', 'nearest_hospital_dist',
        'nearest_school_name', 'nearest_school_dist',
        'amenity_score', 'archived',
        'asking_price', 'cap_rate', 'noi', 'price_per_unit', 'lot_count',
        'is_manual_entry', 'owner_fatigue_score_0_100',
        'insurance_pressure_score_0_100', 'flood_zone',
        'storm_proximity_score', 'disaster_pressure_score',
        'loopnet_url', 'list_price', 'broker_name', 'broker_firm',
        'scrape_source', 'listing_url', 'broker_company'
    ]
    
    missing = [c for c in expected if c not in existing]
    print(f"Missing columns: {missing}")
    print(f"Existing count: {len(existing)}")
    conn.close()

if __name__ == "__main__":
    inspect()


import sqlite3
import pandas as pd

def inspect_leads():
    conn = sqlite3.connect('data/leads.db')
    conn.row_factory = sqlite3.Row
    
    print("--- Leads Inspection ---")
    
    # Total count
    count = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    print(f"Total leads: {count}")
    
    # Lat/Lon check
    valid_coords = conn.execute("SELECT COUNT(*) FROM leads WHERE latitude IS NOT NULL AND longitude IS NOT NULL").fetchone()[0]
    print(f"Leads with valid lat/lon: {valid_coords}")
    
    # State check
    fl_leads = conn.execute("SELECT COUNT(*) FROM leads WHERE UPPER(TRIM(state)) IN ('FL', 'FLORIDA')").fetchone()[0]
    print(f"Leads in FL: {fl_leads}")
    
    # Source check
    loopnet = conn.execute("SELECT COUNT(*) FROM leads WHERE source_query = 'LoopNet Scraper'").fetchone()[0]
    crexi = conn.execute("SELECT COUNT(*) FROM leads WHERE scrape_source = 'crexi'").fetchone()[0]
    regular = count - loopnet - crexi
    print(f"LoopNet leads: {loopnet}")
    print(f"Crexi leads: {crexi}")
    print(f"Regular leads: {regular}")

    # Sample lead
    print("\n--- Sample Lead ---")
    sample = conn.execute("SELECT id, name, city, state, latitude, longitude, source_query, scrape_source, status FROM leads LIMIT 1").fetchone()
    if sample:
        print(dict(sample))
    else:
        print("No leads found.")

if __name__ == "__main__":
    inspect_leads()

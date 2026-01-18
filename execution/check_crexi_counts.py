"""Quick script to check Crexi lead counts and URL status."""
import sqlite3
from pathlib import Path

DB_PATH = Path("data/leads.db")

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total Crexi leads
    cursor.execute("SELECT COUNT(*) FROM leads WHERE scrape_source = 'crexi'")
    total = cursor.fetchone()[0]
    
    # Crexi leads with URLs
    cursor.execute("""
        SELECT COUNT(*) FROM leads 
        WHERE scrape_source = 'crexi' 
        AND listing_url IS NOT NULL 
        AND listing_url != ''
    """)
    with_url = cursor.fetchone()[0]
    
    # Sample URLs
    cursor.execute("""
        SELECT name, listing_url FROM leads 
        WHERE scrape_source = 'crexi' 
        AND listing_url IS NOT NULL 
        LIMIT 5
    """)
    samples = cursor.fetchall()
    
    print(f"Crexi Lead Statistics:")
    print(f"  Total Crexi leads: {total}")
    print(f"  With listing URLs: {with_url}")
    print(f"  Missing URLs: {total - with_url}")
    print(f"\nSample URLs:")
    for name, url in samples:
        print(f"  {name[:40]}: {url}")
    
    conn.close()

if __name__ == "__main__":
    main()

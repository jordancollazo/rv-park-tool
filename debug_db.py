import sqlite3
import os

DB_PATH = 'data/leads.db'

def check_db():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        
        # Check for Deerfield
        print("Searching for 'Deerfield'...")
        rows = conn.execute("SELECT id, name, amenity_score, archived FROM leads WHERE name LIKE '%Deerfield%'").fetchall()
        for row in rows:
            print(dict(row))
            
        if not rows:
            print("No lead found with name containing 'Deerfield'")
            
        # Check for ANY lead with amenity_score
        print("\nChecking for any lead with amenity_score...")
        rows = conn.execute("SELECT count(*) as c FROM leads WHERE amenity_score IS NOT NULL").fetchone()
        print(f"Count of enriched leads: {rows['c']}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db()

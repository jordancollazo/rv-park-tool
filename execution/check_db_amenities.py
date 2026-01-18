import sqlite3
import pandas as pd

DB_PATH = 'data/leads.db'

def check_amenities():
    conn = sqlite3.connect(DB_PATH)
    try:
        # Check if columns exist
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(leads)")
        columns = [info[1] for info in cursor.fetchall()]
        print(f"Columns in leads table: {columns}")
        
        required = ['amenity_score', 'nearest_supermarket_name']
        missing = [c for c in required if c not in columns]
        if missing:
            print(f"Missing columns: {missing}")
        else:
            # Check for data
            df = pd.read_sql("SELECT id, name, amenity_score, nearest_supermarket_name FROM leads WHERE amenity_score IS NOT NULL LIMIT 5", conn)
            print("\nEnriched Leads Sample:")
            print(df.to_string())
            
            count = pd.read_sql("SELECT count(*) as count FROM leads WHERE amenity_score IS NOT NULL", conn).iloc[0]['count']
            print(f"\nTotal enriched leads: {count}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_amenities()

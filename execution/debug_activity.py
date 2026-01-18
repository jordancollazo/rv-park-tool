
import sqlite3
import json

def find_dataset_ids():
    try:
        conn = sqlite3.connect('data/leads.db')
        conn.row_factory = sqlite3.Row
        
        # Look for 'scraped' events in activity_log
        rows = conn.execute("SELECT * FROM activity_log WHERE activity_type = 'scraped' ORDER BY created_at DESC LIMIT 10").fetchall()
        
        print("--- Recent Scrape Activities ---")
        for row in rows:
            print(f"Time: {row['created_at']}, Desc: {row['description']}")
            if row['metadata_json']:
                print(f"Metadata: {row['metadata_json']}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_dataset_ids()

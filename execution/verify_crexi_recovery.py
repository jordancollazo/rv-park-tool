
import sqlite3
from pathlib import Path

def verify():
    db_path = Path("data/leads.db")
    with sqlite3.connect(db_path) as conn:
        print("\n--- Crexi Leads Breakdown ---")
        cursor = conn.execute("SELECT category, COUNT(*) FROM leads WHERE scrape_source='crexi' GROUP BY category")
        rows = cursor.fetchall()
        for cat, count in rows:
            print(f"{cat}: {count}")
            
        print("\n--- Crexi Leads by property_type ---")
        # Assuming property_type is stored in description_keywords or similar if not its own column yet, 
        # but normalize_crexi_item puts it in 'category' mostly. 
        # Let's check 'description' substring if needed.
        
        cursor = conn.execute("SELECT COUNT(*) FROM leads WHERE scrape_source='crexi'")
        print(f"Total Crexi: {cursor.fetchone()[0]}")

if __name__ == "__main__":
    verify()


import sqlite3
from pathlib import Path

def verify():
    db_path = Path("data/leads.db")
    with sqlite3.connect(db_path) as conn:
        print("--- Leads by Scrape Source ---")
        cursor = conn.execute("SELECT scrape_source, COUNT(*) FROM leads GROUP BY scrape_source")
        rows = cursor.fetchall()
        for source, count in rows:
            print(f"{source}: {count}")
            
        print("\n--- Leads by Source Query ---")
        cursor = conn.execute("SELECT source_query, COUNT(*) FROM leads GROUP BY source_query")
        rows = cursor.fetchall()
        for source, count in rows:
            print(f"{source}: {count}")
            
        print("\n--- Total Leads ---")
        cursor = conn.execute("SELECT COUNT(*) FROM leads")
        print(f"Total: {cursor.fetchone()[0]}")

if __name__ == "__main__":
    verify()

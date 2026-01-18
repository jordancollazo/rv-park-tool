
import sqlite3
import json

def test_insert():
    try:
        conn = sqlite3.connect('data/leads.db')
        conn.row_factory = sqlite3.Row
        
        # Insert minimal lead
        cursor = conn.execute("""
            INSERT INTO leads (place_id, name, first_scraped_at, last_scraped_at)
            VALUES ('test_place_123', 'Test Park', '2023-01-01', '2023-01-01')
        """)
        lead_id = cursor.lastrowid
        print(f"Inserted dummy lead with ID: {lead_id}")
        
        # Try updating detailed_description
        conn.execute("UPDATE leads SET detailed_description = ? WHERE id = ?", ("Some description", lead_id))
        print("Updated detailed_description successfully.")
        
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_insert()

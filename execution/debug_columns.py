
import sqlite3

def check_columns():
    conn = sqlite3.connect('data/leads.db')
    cursor = conn.execute("PRAGMA table_info(leads)")
    rows = cursor.fetchall()
    
    print("--- Columns in leads table ---")
    cols = [r[1] for r in rows]
    print(cols)
    
    missing = ['detailed_description', 'social_facebook', 'crexi_id']
    for m in missing:
        print(f"{m}: {'FOUND' if m in cols else 'MISSING'}")

if __name__ == "__main__":
    check_columns()

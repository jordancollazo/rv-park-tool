
import sqlite3
import os

def inspect_mhp_db():
    db_path = "data/mhp_leads.db"
    if not os.path.exists(db_path):
        print(f"{db_path} does not exist.")
        return

    try:
        conn = sqlite3.connect(db_path)
        # Check for leads table
        try:
            count = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            print(f"Leads in mhp_leads.db: {count}")
        except:
            print("No 'leads' table in mhp_leads.db")
            
        # Check tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Tables:", [t[0] for t in tables])
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_mhp_db()

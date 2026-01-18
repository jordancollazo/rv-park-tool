
import sqlite3

def list_tables():
    try:
        conn = sqlite3.connect('data/leads.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("--- Tables in leads.db ---")
        for table in tables:
            name = table[0]
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                print(f"{name}: {count} rows")
            except Exception as e:
                print(f"{name}: Error getting count ({e})")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables()


import sqlite3

def check_triggers():
    try:
        conn = sqlite3.connect('data/leads.db')
        
        # List triggers
        cursor = conn.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'")
        triggers = cursor.fetchall()
        
        print(f"Found {len(triggers)} triggers.")
        for name, tbl, sql in triggers:
            print(f"Trigger: {name} on {tbl}")
            print(f"SQL: {sql}\n")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_triggers()

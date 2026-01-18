
import sqlite3

def find_leads_old():
    try:
        conn = sqlite3.connect('data/leads.db')
        
        print("--- SEARCHING FOR leads_old ---")
        rows = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql LIKE '%leads_old%'").fetchall()
        
        if not rows:
            print("No references to leads_old found.")
        
        for type_, name, tbl_name, sql in rows:
            print(f"Type: {type_}")
            print(f"Name: {name}")
            print(f"Table: {tbl_name}")
            print(f"SQL: {sql}\n")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_leads_old()

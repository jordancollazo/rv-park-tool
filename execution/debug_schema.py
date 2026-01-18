
import sqlite3

def dump_schema():
    try:
        conn = sqlite3.connect('data/leads.db')
        
        print("--- SCHEMA DUMP ---")
        rows = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_master").fetchall()
        for type_, name, tbl_name, sql in rows:
            print(f"Type: {type_}")
            print(f"Name: {name}")
            print(f"Table: {tbl_name}")
            print(f"SQL: {sql}\n")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dump_schema()

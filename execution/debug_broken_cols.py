
import sqlite3

def check_broken_cols():
    conn = sqlite3.connect('data/leads.db')
    
    for table in ['calls_broken', 'notes_broken']:
        print(f"--- {table} ---")
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        for r in rows:
            print(f"{r[1]} ({r[2]})")

if __name__ == "__main__":
    check_broken_cols()

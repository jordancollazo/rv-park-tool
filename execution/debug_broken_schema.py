
import sqlite3

def dump_broken():
    conn = sqlite3.connect('data/leads.db')
    
    rows = conn.execute("SELECT name, sql FROM sqlite_master WHERE name IN ('calls_broken', 'notes_broken')").fetchall()
    
    for name, sql in rows:
        print(f"--- {name} ---")
        print(sql)
        print("-" * 20)

if __name__ == "__main__":
    dump_broken()

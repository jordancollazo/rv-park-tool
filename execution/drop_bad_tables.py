
import sqlite3

def drop_bad_tables():
    conn = sqlite3.connect('data/leads.db')
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute("DROP TABLE calls")
        print("Dropped calls")
    except: pass
    
    try:
        conn.execute("DROP TABLE notes")
        print("Dropped notes")
    except: pass

if __name__ == "__main__":
    drop_bad_tables()


import sqlite3

def check_broken_data():
    conn = sqlite3.connect('data/leads.db')
    try:
        count = conn.execute("SELECT COUNT(*) FROM calls_broken").fetchone()[0]
        print(f"calls_broken: {count} rows")
    except:
        print("calls_broken does not exist")

    try:
        count = conn.execute("SELECT COUNT(*) FROM notes_broken").fetchone()[0]
        print(f"notes_broken: {count} rows")
    except:
        print("notes_broken does not exist")

    try:
        count = conn.execute("SELECT COUNT(*) FROM calls").fetchone()[0]
        print(f"calls (current): {count} rows")
    except:
        print("calls (current) does not exist")

if __name__ == "__main__":
    check_broken_data()

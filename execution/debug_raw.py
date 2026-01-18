
import sqlite3

def check_raw_data():
    try:
        conn = sqlite3.connect('data/leads.db')
        count = conn.execute("SELECT COUNT(*) FROM landwatch_raw_listings").fetchone()[0]
        print(f"Raw Landwatch listings: {count}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_raw_data()

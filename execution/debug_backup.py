
import sqlite3
import os

def inspect_backup():
    backup_path = "data/leads.db.bak"
    if not os.path.exists(backup_path):
        print(f"Backup file {backup_path} does not exist.")
        return

    try:
        conn = sqlite3.connect(backup_path)
        count = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        print(f"Leads in backup: {count}")
    except Exception as e:
        print(f"Error reading backup: {e}")

if __name__ == "__main__":
    inspect_backup()

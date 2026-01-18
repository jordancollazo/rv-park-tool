"""
check_db_schema.py
Check the schema of the leads table.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def check_schema():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get table info
    cursor.execute("PRAGMA table_info(leads)")
    columns = cursor.fetchall()

    print("=" * 80)
    print("LEADS TABLE SCHEMA")
    print("=" * 80)
    print()
    print(f"{'Column Name':<40} {'Type':<20} {'Not Null':<10} {'Default':<20}")
    print("-" * 80)

    for col in columns:
        col_id, name, col_type, not_null, default_val, pk = col
        not_null_str = "Yes" if not_null else "No"
        default_str = str(default_val) if default_val is not None else ""
        print(f"{name:<40} {col_type:<20} {not_null_str:<10} {default_str:<20}")

    print()
    print(f"Total columns: {len(columns)}")
    print("=" * 80)

    conn.close()

if __name__ == "__main__":
    check_schema()

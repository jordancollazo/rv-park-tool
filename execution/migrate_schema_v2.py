"""
migrate_schema_v2.py
Adds new columns to the `leads` table to support manual broker entries and financial data.
"""

import sqlite3
import os

DB_PATH = "data/leads.db"

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # List of new columns to add
    new_columns = [
        ("cap_rate", "REAL"),
        ("noi", "REAL"),
        ("price_per_unit", "REAL"),
        ("occupancy_rate", "REAL"),
        ("year_built", "INTEGER"),
        ("lot_size_text", "TEXT"),
        ("building_size_text", "TEXT"),
        ("listing_source_id", "TEXT"),
        ("listing_source_url", "TEXT"),
        ("listing_date", "TEXT"),
        ("broker_name", "TEXT"),
        ("is_manual_entry", "INTEGER DEFAULT 0"),
        ("manual_notes", "TEXT")
    ]

    print("Checking for column updates...")
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(leads)")
    existing_cols = [row[1] for row in cursor.fetchall()]

    for col_name, col_type in new_columns:
        if col_name not in existing_cols:
            try:
                print(f"Adding column: {col_name} ({col_type})")
                cursor.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError as e:
                print(f"Error adding {col_name}: {e}")
        else:
            print(f"Column {col_name} already exists.")

    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    migrate()

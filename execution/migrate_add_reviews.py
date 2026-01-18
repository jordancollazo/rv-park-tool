"""
Database migration: Add review data columns to leads table.

Adds:
- review_count: Number of Google reviews
- rating: Google star rating (0.0-5.0)
- review_sentiment: AI-generated sentiment summary
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "crm.db"

def migrate():
    print(f"Migrating database at {DB_PATH}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Add review columns
        new_columns = {
            "review_count": "INTEGER DEFAULT 0",
            "rating": "REAL DEFAULT 0.0",
            "review_sentiment": "TEXT"
        }
        
        for col, dtype in new_columns.items():
            try:
                print(f"Adding column: {col}")
                cursor.execute(f"ALTER TABLE leads ADD COLUMN {col} {dtype}")
            except sqlite3.OperationalError as e:
                print(f"  -> Skipped (exists or error): {e}")
        
        conn.commit()
    
    print("✓ Migration complete.")

if __name__ == "__main__":
    migrate()

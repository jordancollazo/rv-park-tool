
import sqlite3
from pathlib import Path
import sys

# Define fields to ensure
REQUIRED_COLUMNS = [
    ("broker_name", "TEXT"),
    ("broker_company", "TEXT"), 
    ("broker_phone", "TEXT"),
    ("broker_email", "TEXT"),
    ("listing_url", "TEXT"),
    ("crexi_id", "TEXT"),
    ("asking_price", "REAL"),
    ("social_facebook", "TEXT"),
    ("social_instagram", "TEXT"), 
    ("social_linkedin", "TEXT"),
    ("owner_name", "TEXT"),
    ("is_enriched", "INTEGER DEFAULT 0"),
    ("owner_fatigue_score_0_100", "REAL"),
    ("owner_fatigue_confidence", "TEXT"),
    ("owner_fatigue_reasons_json", "TEXT"),
    ("owner_fatigue_breakdown_json", "TEXT"),
    ("cap_rate", "REAL"),
    ("noi", "REAL"),
    ("occupancy", "REAL"),
    ("price_per_unit", "REAL"),
    ("scrape_source", "TEXT")
]

def migrate():
    db_path = Path("data/leads.db")
    if not db_path.exists():
        print("Database not found!")
        return
        
    print(f"Checking columns in {db_path}...")
    
    with sqlite3.connect(db_path) as conn:
        # Get existing columns
        cursor = conn.execute("PRAGMA table_info(leads)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        
        for col_name, col_type in REQUIRED_COLUMNS:
            if col_name not in existing_cols:
                print(f"Adding column: {col_name} ({col_type})")
                try:
                    conn.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}")
                except Exception as e:
                    print(f"Error adding {col_name}: {e}")
            else:
                pass # print(f"Column {col_name} exists.")
                
        conn.commit()
    print("Migration complete.")

if __name__ == "__main__":
    migrate()


import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def migrate():
    print(f"Migrating database at {DB_PATH}...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # columns to add
    cols = [
        ("loopnet_id", "TEXT"),
        ("loopnet_url", "TEXT"),
        ("listing_status", "TEXT"),
        ("list_price", "REAL"),
        ("cap_rate", "REAL"),
        ("noi", "REAL"),
        ("occupancy_rate", "REAL"),
        ("broker_name", "TEXT"),
        ("broker_firm", "TEXT"),
        ("description_keywords", "TEXT")
    ]
    
    for col_name, col_type in cols:
        try:
            print(f"Adding column {col_name}...")
            cursor.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e):
                print(f"Column {col_name} already exists.")
            else:
                # SQLite often throws "duplicate column" even if message is different, check carefully
                # In older sqlite versions it might fail differently.
                # But usually `Add COLUMN` fails if it exists.
                print(f"Skipping {col_name}: {e}")

    # Add index on loopnet_id for faster lookups
    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_loopnet_id ON leads(loopnet_id)")
        print("Added index on loopnet_id")
    except Exception as e:
        print(f"Index creation failed (might exist): {e}")

    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    migrate()

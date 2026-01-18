
import sqlite3
from pathlib import Path

DB_PATH = Path("data/leads.db")

def migrate_schema():
    print(f"Migrating schema at {DB_PATH}...")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Columns to add
        new_columns = {
            "registered_agent_name": "TEXT",
            "registered_agent_address": "TEXT",
            "utilities_status": "TEXT",
            "rent_info": "TEXT"
        }
        
        for col, dtype in new_columns.items():
            try:
                print(f"Adding column: {col}")
                cursor.execute(f"ALTER TABLE leads ADD COLUMN {col} {dtype}")
            except sqlite3.OperationalError as e:
                # Column likely exists
                print(f"  -> Skipped (exists or error): {e}")
                
        conn.commit()
    
    print("Migration complete.")

if __name__ == "__main__":
    migrate_schema()

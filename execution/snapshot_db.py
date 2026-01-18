"""
snapshot_db.py

Creates a "Golden Copy" of the entire database state.
1. Copies the physical .db file to backups/snapshots/
2. Dumps the 'leads' table to a human-readable JSON file for easy recovery/inspection.

Usage:
    python execution/snapshot_db.py
"""

import sqlite3
import shutil
import json
import os
from datetime import datetime
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "leads.db"
BACKUP_DIR = BASE_DIR / "backups" / "snapshots"

def row_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def snapshot_db():
    print("Starting full database snapshot...")
    
    # Ensure backup directory exists
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Physical DB Copy
    if DB_PATH.exists():
        backup_db_path = BACKUP_DIR / f"leads_{timestamp}.db.bak"
        shutil.copy2(DB_PATH, backup_db_path)
        print(f"✅ Physical DB backup created: {backup_db_path.name}")
    else:
        print("❌ DB file not found!")
        return

    # 2. JSON Dump of Leads (Human Readable / Portable)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = row_factory
        cursor = conn.cursor()
        
        # Get all leads
        cursor.execute("SELECT * FROM leads ORDER BY id")
        leads = cursor.fetchall()
        
        json_path = BACKUP_DIR / f"leads_export_{timestamp}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(leads, f, indent=2, default=str)
            
        print(f"✅ JSON export created: {json_path.name} ({len(leads)} leads)")
        
        # Get stats
        cursor.execute("SELECT count(*) as c FROM leads WHERE latitude IS NOT NULL")
        geocoded = cursor.fetchone()['c']
        
        cursor.execute("SELECT count(*) as c FROM leads WHERE insurance_pressure_score_0_100 IS NOT NULL")
        enriched = cursor.fetchone()['c']
        
        print("\nSnapshot Stats:")
        print(f"  - Total Leads: {len(leads)}")
        print(f"  - Geocoded:    {geocoded}")
        print(f"  - Enriched:    {enriched}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating JSON dump: {e}")

if __name__ == "__main__":
    snapshot_db()

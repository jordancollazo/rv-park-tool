"""
migrate_add_owner_fatigue.py

One-time migration to add Owner Fatigue Score fields to the leads table.

Fields added:
- owner_fatigue_score_0_100: Overall fatigue score (0-100, higher = more neglected)
- owner_fatigue_confidence: Confidence level (high|medium|low)
- owner_fatigue_reasons_json: JSON array of scoring reasons
- owner_fatigue_breakdown_json: JSON object with component scores

Usage:
    python execution/migrate_add_owner_fatigue.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def migrate():
    """Add owner fatigue columns to leads table."""
    print(f"Migrating database: {DB_PATH}")
    
    if not DB_PATH.exists():
        print("ERROR: Database does not exist. Run init_db() first.")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(leads)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    migrations = []
    
    # Owner Fatigue Score fields
    if 'owner_fatigue_score_0_100' not in existing_columns:
        migrations.append((
            "owner_fatigue_score_0_100",
            "ALTER TABLE leads ADD COLUMN owner_fatigue_score_0_100 REAL"
        ))
    if 'owner_fatigue_confidence' not in existing_columns:
        migrations.append((
            "owner_fatigue_confidence",
            "ALTER TABLE leads ADD COLUMN owner_fatigue_confidence TEXT"
        ))
    if 'owner_fatigue_reasons_json' not in existing_columns:
        migrations.append((
            "owner_fatigue_reasons_json",
            "ALTER TABLE leads ADD COLUMN owner_fatigue_reasons_json TEXT"
        ))
    if 'owner_fatigue_breakdown_json' not in existing_columns:
        migrations.append((
            "owner_fatigue_breakdown_json",
            "ALTER TABLE leads ADD COLUMN owner_fatigue_breakdown_json TEXT"
        ))
    
    if not migrations:
        print("✓ All owner fatigue columns already exist. No migration needed.")
        conn.close()
        return True
    
    print(f"\nAdding {len(migrations)} new columns:")
    for col_name, sql in migrations:
        print(f"  - {col_name}")
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            print(f"    WARNING: {e}")
    
    # Create index for efficient filtering by owner fatigue score
    index_migrations = [
        (
            "idx_owner_fatigue_score",
            "CREATE INDEX IF NOT EXISTS idx_owner_fatigue_score ON leads(owner_fatigue_score_0_100)"
        ),
    ]
    
    print("\nCreating indexes:")
    for idx_name, sql in index_migrations:
        print(f"  - {idx_name}")
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            print(f"    WARNING: {e}")
    
    conn.commit()
    conn.close()
    
    print("\n✓ Migration completed successfully!")
    print(f"  Database: {DB_PATH}")
    print(f"  New columns: {len(migrations)}")
    
    return True


if __name__ == "__main__":
    migrate()

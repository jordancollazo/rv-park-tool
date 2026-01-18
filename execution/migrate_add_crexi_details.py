"""
migrate_add_crexi_details.py

Second migration to add even more Crexi details for "maximum data extraction":
- Lease info (type, expiration)
- Property specifics (sq ft, year built, tenancy)
- Market data (days on market, date listed)

Usage:
    python execution/migrate_add_crexi_details.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def migrate():
    """Add detailed Crexi columns to leads table."""
    print(f"Migrating database (Phase 2): {DB_PATH}")
    
    if not DB_PATH.exists():
        print("ERROR: Database does not exist. Run init_db() first.")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(leads)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    migrations = []
    
    # Lease & Tenant Info
    if 'lease_type' not in existing_columns:
        migrations.append(("lease_type", "ALTER TABLE leads ADD COLUMN lease_type TEXT"))
    if 'tenancy' not in existing_columns:
        migrations.append(("tenancy", "ALTER TABLE leads ADD COLUMN tenancy TEXT"))
    if 'lease_expiration' not in existing_columns:
        migrations.append(("lease_expiration", "ALTER TABLE leads ADD COLUMN lease_expiration TEXT"))
    
    # Property Physical Details
    if 'sq_ft' not in existing_columns:
        migrations.append(("sq_ft", "ALTER TABLE leads ADD COLUMN sq_ft REAL"))
    if 'year_built' not in existing_columns:
        migrations.append(("year_built", "ALTER TABLE leads ADD COLUMN year_built INTEGER"))
    if 'sub_type' not in existing_columns:
        migrations.append(("sub_type", "ALTER TABLE leads ADD COLUMN sub_type TEXT"))
    
    # Market Stats
    if 'date_listed' not in existing_columns:
        migrations.append(("date_listed", "ALTER TABLE leads ADD COLUMN date_listed TEXT"))
    if 'days_on_market' not in existing_columns:
        migrations.append(("days_on_market", "ALTER TABLE leads ADD COLUMN days_on_market INTEGER"))
    
    # Additional Context
    if 'investment_highlights' not in existing_columns:
        migrations.append(("investment_highlights", "ALTER TABLE leads ADD COLUMN investment_highlights TEXT"))
    
    
    if not migrations:
        print("✓ All detailed Crexi columns already exist. No migration needed.")
        conn.close()
        return True
    
    print(f"\nAdding {len(migrations)} new columns:")
    for col_name, sql in migrations:
        print(f"  - {col_name}")
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            print(f"    WARNING: {e}")
            
    conn.commit()
    conn.close()
    
    print("\n✓ Migration (Phase 2) completed successfully!")
    print(f"  Database: {DB_PATH}")
    print(f"  New columns: {len(migrations)}")
    
    return True


if __name__ == "__main__":
    migrate()

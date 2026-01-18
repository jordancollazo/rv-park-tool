"""
migrate_add_crexi_fields.py

One-time migration to add Crexi-specific fields to the leads table:
- Financial metrics (Cap rate, NOI, Occupancy)
- Broker details
- Listing metadata

Usage:
    python execution/migrate_add_crexi_fields.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def migrate():
    """Add Crexi columns to leads table."""
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
    
    # Financial metrics
    if 'cap_rate' not in existing_columns:
        migrations.append(("cap_rate", "ALTER TABLE leads ADD COLUMN cap_rate REAL"))
    if 'noi' not in existing_columns:
        migrations.append(("noi", "ALTER TABLE leads ADD COLUMN noi REAL"))
    if 'occupancy' not in existing_columns:
        migrations.append(("occupancy", "ALTER TABLE leads ADD COLUMN occupancy REAL"))
    if 'price_per_unit' not in existing_columns:
        migrations.append(("price_per_unit", "ALTER TABLE leads ADD COLUMN price_per_unit REAL"))
        
    # Broker details
    if 'broker_name' not in existing_columns:
        migrations.append(("broker_name", "ALTER TABLE leads ADD COLUMN broker_name TEXT"))
    if 'broker_company' not in existing_columns:
        migrations.append(("broker_company", "ALTER TABLE leads ADD COLUMN broker_company TEXT"))
    if 'broker_phone' not in existing_columns:
        migrations.append(("broker_phone", "ALTER TABLE leads ADD COLUMN broker_phone TEXT"))
    if 'broker_email' not in existing_columns:
        migrations.append(("broker_email", "ALTER TABLE leads ADD COLUMN broker_email TEXT"))

    # Listing metadata
    if 'listing_url' not in existing_columns:
        migrations.append(("listing_url", "ALTER TABLE leads ADD COLUMN listing_url TEXT"))
    if 'description' not in existing_columns:
        migrations.append(("description", "ALTER TABLE leads ADD COLUMN description TEXT"))
    if 'scrape_source' not in existing_columns:
        migrations.append(("scrape_source", "ALTER TABLE leads ADD COLUMN scrape_source TEXT"))
    if 'crexi_id' not in existing_columns:
        migrations.append(("crexi_id", "ALTER TABLE leads ADD COLUMN crexi_id TEXT"))
    
    if not migrations:
        print("✓ All Crexi columns already exist. No migration needed.")
        conn.close()
        return True
    
    print(f"\nAdding {len(migrations)} new columns:")
    for col_name, sql in migrations:
        print(f"  - {col_name}")
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            print(f"    WARNING: {e}")
    
    # Create indexes
    index_migrations = [
        ("idx_scrape_source", "CREATE INDEX IF NOT EXISTS idx_scrape_source ON leads(scrape_source)"),
        ("idx_crexi_id", "CREATE INDEX IF NOT EXISTS idx_crexi_id ON leads(crexi_id)"),
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

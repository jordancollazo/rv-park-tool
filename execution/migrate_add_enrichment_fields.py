"""
migrate_add_enrichment_fields.py

One-time migration to add enrichment fields to the leads table:
- SMS capability validation fields
- Census data fields (population, housing)

Usage:
    python execution/migrate_add_enrichment_fields.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def migrate():
    """Add enrichment columns to leads table."""
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
    
    # SMS capability fields
    if 'sms_capable' not in existing_columns:
        migrations.append(("sms_capable", "ALTER TABLE leads ADD COLUMN sms_capable BOOLEAN DEFAULT NULL"))
    if 'carrier_type' not in existing_columns:
        migrations.append(("carrier_type", "ALTER TABLE leads ADD COLUMN carrier_type TEXT"))
    if 'carrier_name' not in existing_columns:
        migrations.append(("carrier_name", "ALTER TABLE leads ADD COLUMN carrier_name TEXT"))
    if 'phone_validated_at' not in existing_columns:
        migrations.append(("phone_validated_at", "ALTER TABLE leads ADD COLUMN phone_validated_at TEXT"))
    
    # Census data fields
    if 'census_tract' not in existing_columns:
        migrations.append(("census_tract", "ALTER TABLE leads ADD COLUMN census_tract TEXT"))
    if 'population_density' not in existing_columns:
        migrations.append(("population_density", "ALTER TABLE leads ADD COLUMN population_density REAL"))
    if 'median_home_value' not in existing_columns:
        migrations.append(("median_home_value", "ALTER TABLE leads ADD COLUMN median_home_value INTEGER"))
    if 'housing_affordability_index' not in existing_columns:
        migrations.append(("housing_affordability_index", "ALTER TABLE leads ADD COLUMN housing_affordability_index REAL"))
    
    # Enrichment metadata fields (from previous enrichment work)
    if 'social_facebook' not in existing_columns:
        migrations.append(("social_facebook", "ALTER TABLE leads ADD COLUMN social_facebook TEXT"))
    if 'social_instagram' not in existing_columns:
        migrations.append(("social_instagram", "ALTER TABLE leads ADD COLUMN social_instagram TEXT"))
    if 'social_linkedin' not in existing_columns:
        migrations.append(("social_linkedin", "ALTER TABLE leads ADD COLUMN social_linkedin TEXT"))
    if 'is_enriched' not in existing_columns:
        migrations.append(("is_enriched", "ALTER TABLE leads ADD COLUMN is_enriched BOOLEAN DEFAULT 0"))
    if 'registered_agent_name' not in existing_columns:
        migrations.append(("registered_agent_name", "ALTER TABLE leads ADD COLUMN registered_agent_name TEXT"))
    if 'registered_agent_address' not in existing_columns:
        migrations.append(("registered_agent_address", "ALTER TABLE leads ADD COLUMN registered_agent_address TEXT"))
    if 'utilities_status' not in existing_columns:
        migrations.append(("utilities_status", "ALTER TABLE leads ADD COLUMN utilities_status TEXT"))
    if 'rent_info' not in existing_columns:
        migrations.append(("rent_info", "ALTER TABLE leads ADD COLUMN rent_info TEXT"))
    
    if not migrations:
        print("✓ All enrichment columns already exist. No migration needed.")
        conn.close()
        return True
    
    print(f"\nAdding {len(migrations)} new columns:")
    for col_name, sql in migrations:
        print(f"  - {col_name}")
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            print(f"    WARNING: {e}")
    
    # Create indexes for new columns
    index_migrations = [
        ("idx_sms_capable", "CREATE INDEX IF NOT EXISTS idx_sms_capable ON leads(sms_capable)"),
        ("idx_census_tract", "CREATE INDEX IF NOT EXISTS idx_census_tract ON leads(census_tract)"),
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

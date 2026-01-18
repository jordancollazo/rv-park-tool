"""
migrate_add_insurance_pressure.py

One-time migration to add Insurance Pressure Index fields to the leads table
and create cache tables for NFHL, storm grid, and FEMA disaster data.

Usage:
    python scripts/migrate_add_insurance_pressure.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"


def migrate():
    """Add insurance pressure columns and cache tables."""
    print(f"Migrating database: {DB_PATH}")
    
    if not DB_PATH.exists():
        print("ERROR: Database does not exist. Run init_db() first.")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get existing columns in leads table
    cursor.execute("PRAGMA table_info(leads)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    migrations = []
    
    # Insurance Pressure Index fields
    if 'insurance_pressure_score_0_100' not in existing_columns:
        migrations.append(("insurance_pressure_score_0_100", 
            "ALTER TABLE leads ADD COLUMN insurance_pressure_score_0_100 REAL"))
    if 'insurance_pressure_confidence' not in existing_columns:
        migrations.append(("insurance_pressure_confidence", 
            "ALTER TABLE leads ADD COLUMN insurance_pressure_confidence TEXT"))
    if 'flood_zone' not in existing_columns:
        migrations.append(("flood_zone", 
            "ALTER TABLE leads ADD COLUMN flood_zone TEXT"))
    if 'flood_zone_source' not in existing_columns:
        migrations.append(("flood_zone_source", 
            "ALTER TABLE leads ADD COLUMN flood_zone_source TEXT"))
    if 'storm_proximity_score' not in existing_columns:
        migrations.append(("storm_proximity_score", 
            "ALTER TABLE leads ADD COLUMN storm_proximity_score REAL"))
    if 'disaster_pressure_score' not in existing_columns:
        migrations.append(("disaster_pressure_score", 
            "ALTER TABLE leads ADD COLUMN disaster_pressure_score REAL"))
    if 'insurance_pressure_reasons_json' not in existing_columns:
        migrations.append(("insurance_pressure_reasons_json", 
            "ALTER TABLE leads ADD COLUMN insurance_pressure_reasons_json TEXT"))
    if 'insurance_pressure_breakdown_json' not in existing_columns:
        migrations.append(("insurance_pressure_breakdown_json", 
            "ALTER TABLE leads ADD COLUMN insurance_pressure_breakdown_json TEXT"))
    if 'county_fips' not in existing_columns:
        migrations.append(("county_fips", 
            "ALTER TABLE leads ADD COLUMN county_fips TEXT"))
    
    if migrations:
        print(f"\nAdding {len(migrations)} new columns to leads table:")
        for col_name, sql in migrations:
            print(f"  - {col_name}")
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                print(f"    WARNING: {e}")
    else:
        print("✓ All insurance pressure columns already exist in leads table.")
    
    # Create cache tables
    cache_tables = []
    
    # NFHL cache table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='nfhl_cache'")
    if not cursor.fetchone():
        cache_tables.append(("nfhl_cache", """
            CREATE TABLE nfhl_cache (
                lat_round REAL NOT NULL,
                lon_round REAL NOT NULL,
                flood_zone TEXT,
                zone_subtype TEXT,
                sfha_tf TEXT,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (lat_round, lon_round)
            )
        """))
    
    # FEMA disaster cache table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fema_disaster_cache'")
    if not cursor.fetchone():
        cache_tables.append(("fema_disaster_cache", """
            CREATE TABLE fema_disaster_cache (
                county_fips TEXT PRIMARY KEY,
                declaration_count_5yr INTEGER DEFAULT 0,
                declaration_count_20yr INTEGER DEFAULT 0,
                last_disaster_date TEXT,
                disaster_score REAL,
                metrics_json TEXT,
                fetched_at TEXT NOT NULL
            )
        """))
    
    # Storm grid cache table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='storm_grid_cache'")
    if not cursor.fetchone():
        cache_tables.append(("storm_grid_cache", """
            CREATE TABLE storm_grid_cache (
                grid_lat REAL NOT NULL,
                grid_lon REAL NOT NULL,
                storm_count INTEGER DEFAULT 0,
                intensity_weighted_count REAL DEFAULT 0,
                computed_at TEXT NOT NULL,
                PRIMARY KEY (grid_lat, grid_lon)
            )
        """))
    
    if cache_tables:
        print(f"\nCreating {len(cache_tables)} cache tables:")
        for table_name, sql in cache_tables:
            print(f"  - {table_name}")
            try:
                cursor.execute(sql)
            except sqlite3.OperationalError as e:
                print(f"    WARNING: {e}")
    else:
        print("✓ All cache tables already exist.")
    
    # Create indexes
    index_migrations = [
        ("idx_leads_insurance_pressure", 
         "CREATE INDEX IF NOT EXISTS idx_leads_insurance_pressure ON leads(insurance_pressure_score_0_100)"),
        ("idx_leads_flood_zone", 
         "CREATE INDEX IF NOT EXISTS idx_leads_flood_zone ON leads(flood_zone)"),
        ("idx_leads_county_fips", 
         "CREATE INDEX IF NOT EXISTS idx_leads_county_fips ON leads(county_fips)"),
        ("idx_storm_grid_location", 
         "CREATE INDEX IF NOT EXISTS idx_storm_grid_location ON storm_grid_cache(grid_lat, grid_lon)"),
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
    
    return True


if __name__ == "__main__":
    migrate()

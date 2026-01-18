
import sqlite3

def migrate():
    conn = sqlite3.connect("data/leads.db")
    conn.execute("PRAGMA foreign_keys = OFF")
    
    try:
        print("Creating status_history table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                changed_at TEXT DEFAULT (datetime('now')),
                changed_by TEXT DEFAULT 'user',
                notes TEXT,
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status_history_lead ON status_history(lead_id);")

        print("Analyzing leads table schema...")
        cursor = conn.execute("PRAGMA table_info(leads)")
        columns_info = cursor.fetchall()
        
        # Build column definitions
        col_defs = []
        col_names = []
        
        new_status_check = """status TEXT DEFAULT 'not_contacted' CHECK (status IN (
            'not_contacted', 'contacted', 'interested', 'not_interested',
            'docs_requested', 'docs_received', 'reviewed_interested',
            'reviewed_not_interested', 'negotiating', 'acquired', 'dead'
        ))"""
        
        for col in columns_info:
            cid, name, dtype, notnull, dflt_val, pk = col
            col_names.append(name)
            
            if name == 'status':
                col_defs.append(new_status_check)
                continue
                
            # Reconstruct definition
            # Note: valid types in SQLite are optional but we use what we found.
            parts = [name, dtype]
            if pk:
                parts.append("PRIMARY KEY AUTOINCREMENT")
            if notnull:
                parts.append("NOT NULL")
            if dflt_val is not None:
                # dflt_val comes as string/repr? e.g. "'not_contacted'" or "0"
                parts.append(f"DEFAULT {dflt_val}")
                
            # Place ID unique constraint? PRAGMA doesn't show it.
            # We know place_id is UNIQUE from schema.sql.
            # We should add it explicitly if name is place_id.
            if name == 'place_id':
                 parts.append("UNIQUE")

            col_defs.append(" ".join(parts))
            
        # Inject missing columns required by CRM Server
        known_missing = {
            'nearest_supermarket_name': 'TEXT',
            'nearest_supermarket_dist': 'REAL',
            'nearest_hospital_name': 'TEXT',
            'nearest_hospital_dist': 'REAL',
            'nearest_school_name': 'TEXT',
            'nearest_school_dist': 'REAL',
            'amenity_score': 'REAL',
            'archived': 'INTEGER DEFAULT 0',
            'asking_price': 'REAL', 
            'cap_rate': 'REAL', 
            'noi': 'REAL', 
            'price_per_unit': 'REAL', 
            'lot_count': 'INTEGER', 
            'is_manual_entry': 'INTEGER DEFAULT 0',
            'owner_fatigue_score_0_100': 'REAL', 
            'owner_fatigue_confidence': 'TEXT', 
            'owner_fatigue_reasons_json': 'TEXT',
            'insurance_pressure_score_0_100': 'REAL', 
            'insurance_pressure_confidence': 'TEXT',
            'flood_zone': 'TEXT', 
            'storm_proximity_score': 'REAL', 
            'disaster_pressure_score': 'REAL',
            'insurance_pressure_reasons_json': 'TEXT', 
            'insurance_pressure_breakdown_json': 'TEXT',
            'tax_shock_score_0_100': 'REAL', 
            'tax_shock_confidence': 'TEXT', 
            'county_name': 'TEXT', 
            'county_fips': 'TEXT',
            'tax_shock_reasons_json': 'TEXT', 
            'county_millage_change': 'REAL', 
            'zcta_derived': 'TEXT',
            'loopnet_url': 'TEXT',
            'list_price': 'REAL',
            'broker_name': 'TEXT',
            'broker_firm': 'TEXT',
            'scrape_source': 'TEXT',
            'listing_url': 'TEXT',
            'broker_company': 'TEXT'
        }
        
        for col, dtype in known_missing.items():
            if col not in col_names:
                print(f"Adding missing column: {col}")
                col_defs.append(f"{col} {dtype}")
                # Do NOT append to col_names, as we can't copy it from old table.
        
        print(f"Detected {len(col_names)} existing columns. Final schema has {len(col_defs)} columns.")
        
        # 1. Rename
        print("Renaming old table...")
        conn.execute("DROP TABLE IF EXISTS leads_old")
        conn.execute("ALTER TABLE leads RENAME TO leads_old")
        
        # 2. Create New
        create_sql = f"CREATE TABLE leads ({', '.join(col_defs)})"
        print("Creating new table...")
        conn.execute(create_sql)
        
        # 3. Copy
        # We only select columns that exist in leads_old.
        # col_names contains ORIGINAL columns.
        cols_str = ", ".join(col_names)
        print(f"Copying data for {len(col_names)} columns...")
        # Since we append new cols to TABLE but not to SELECT list, they will be NULL/Default.
        conn.execute(f"INSERT INTO leads ({cols_str}) SELECT {cols_str} FROM leads_old")
        
        # 4. Drop Old
        conn.execute("DROP TABLE leads_old")
        
        # 5. Indexes
        print("Recreating indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_area ON leads(area)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(site_score_1_10)")
        
        conn.commit()
        print("Migration successful!")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()

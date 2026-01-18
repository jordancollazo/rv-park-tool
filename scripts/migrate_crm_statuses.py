
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/leads.db")
BACKUP_PATH = Path("data/leads.db.bak")

def migrate():
    if not DB_PATH.exists():
        print("Database not found!")
        return

    print(f"Backing up database to {BACKUP_PATH}...")
    shutil.copy(DB_PATH, BACKUP_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF")
    
    try:
        conn.execute("DROP TABLE IF EXISTS leads_old")
        
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

        print("Migrating leads table schema...")
        # 1. Rename existing table
        conn.execute("ALTER TABLE leads RENAME TO leads_old")

        # 2. Create new table with updated CHECK constraint
        conn.execute("""
            CREATE TABLE leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                place_id TEXT UNIQUE NOT NULL,
                crexi_id TEXT,
                loopnet_id TEXT,
                
                -- Basic info from scrape
                name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                phone TEXT,
                website TEXT,
                maps_url TEXT,
                latitude REAL,
                longitude REAL,
                
                -- Google data
                google_rating REAL,
                review_count INTEGER,
                category TEXT,

                -- Social / Enrichment
                social_facebook TEXT,
                social_instagram TEXT,
                social_linkedin TEXT,
                is_enriched INTEGER DEFAULT 0,
                registered_agent_name TEXT,
                registered_agent_address TEXT,
                utilities_status TEXT,
                rent_info TEXT,
                
                -- Scoring
                site_score_1_10 INTEGER DEFAULT 0,
                score_breakdown_json TEXT,
                score_reasons TEXT,
                crawl_status TEXT,
                
                -- Owner Fatigue Score (acquisition targeting)
                owner_fatigue_score_0_100 REAL,
                owner_fatigue_confidence TEXT,
                owner_fatigue_reasons_json TEXT,
                owner_fatigue_breakdown_json TEXT,
                
                -- Scrape metadata
                source_query TEXT,
                loopnet_url TEXT,
                listing_url TEXT,
                scrape_source TEXT,
                area TEXT,
                zcta_derived TEXT,
                first_scraped_at TEXT NOT NULL,
                last_scraped_at TEXT NOT NULL,

                -- Amenity Data (Points of Interest)
                nearest_supermarket_name TEXT,
                nearest_supermarket_dist REAL,
                nearest_hospital_name TEXT,
                nearest_hospital_dist REAL,
                nearest_school_name TEXT,
                nearest_school_dist REAL,
                amenity_score INTEGER,
                
                -- CRM fields
                status TEXT DEFAULT 'not_contacted' CHECK (status IN (
                    'not_contacted',
                    'contacted',
                    'interested',
                    'not_interested',
                    'docs_requested',
                    'docs_received',
                    'reviewed_interested',
                    'reviewed_not_interested',
                    'negotiating',
                    'acquired',
                    'dead'
                )),
                owner_name TEXT,
                owner_email TEXT,
                owner_phone TEXT,
                list_price REAL,
                broker_name TEXT,
                broker_firm TEXT,
                broker_company TEXT,
                asking_price REAL,
                cap_rate REAL,
                noi REAL,
                price_per_unit REAL,
                lot_count INTEGER,
                occupancy_rate REAL,
                occupancy REAL,
                is_manual_entry INTEGER DEFAULT 0,

                -- Expanded Property Details
                year_built INTEGER,
                building_size REAL,
                lot_size REAL,
                sq_ft REAL,
                detailed_description TEXT,
                description_keywords TEXT,
                listing_status TEXT,
                lease_type TEXT,
                tenancy TEXT,
                lease_expiration TEXT,
                sub_type TEXT,
                date_listed TEXT,
                days_on_market INTEGER,
                investment_highlights TEXT,

                next_followup DATE,
                
                -- Tax Shock Risk Score
                tax_shock_score_0_100 REAL,
                tax_shock_confidence TEXT,
                county_name TEXT,
                county_fips TEXT,
                county_millage_change REAL,
                county_taxes_levied_growth REAL,
                tax_shock_reasons_json TEXT,
                tax_shock_breakdown_json TEXT,

                -- Insurance / Risk Data
                insurance_pressure_score_0_100 REAL,
                insurance_pressure_confidence TEXT,
                flood_zone TEXT,
                storm_proximity_score REAL,
                disaster_pressure_score REAL,
                insurance_pressure_reasons_json TEXT,
                insurance_pressure_breakdown_json TEXT,

                -- Aggregated stats (updated by calls/emails triggers or code)
                call_count INTEGER DEFAULT 0,
                last_called_at TEXT,
                last_call_outcome TEXT,
                email_count INTEGER DEFAULT 0,
                last_email_at TEXT,
                
                archived INTEGER DEFAULT 0
            )
        """)

        # 3. Copy data dynamically
        print("Copying data...")
        
        # Get columns from old table
        cursor = conn.execute("PRAGMA table_info(leads_old)")
        old_cols = {row[1] for row in cursor.fetchall()}
        
        # Get columns from new table (we just created it, but good to be extensible)
        cursor = conn.execute("PRAGMA table_info(leads)")
        new_cols = {row[1] for row in cursor.fetchall()}
        
        # Intersection of columns
        common_cols = list(old_cols.intersection(new_cols))
        
        if not common_cols:
            raise Exception("No common columns found between old and new tables!")
            
        cols_str = ", ".join(common_cols)
        print(f"Migrating columns: {cols_str}")
        
        conn.execute(f"INSERT INTO leads ({cols_str}) SELECT {cols_str} FROM leads_old")

        # 4. Drop old table
        print("Dropping old table...")
        conn.execute("DROP TABLE leads_old")

        # 5. Recreate indexes
        print("Recreating indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_area ON leads(area)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(site_score_1_10)")

        conn.commit()
        print("Migration successful!")

    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
        # Restore from backup processing? 
        # For now just printing error. User might need to manually restore if it failed mid-way (but rollback handles transaction).
        # EXCEPT if we renamed the table and then failed.
        # If leads_old exists and leads doesn't (or partial), relying on transaction.
        # SQLite DDL is transactional.
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()

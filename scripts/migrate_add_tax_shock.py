import sqlite3
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = Path("data/leads.db")

def migrate():
    """Add tax shock columns and table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 1. Add columns to leads table
        columns_to_add = [
            ("tax_shock_score_0_100", "REAL"),
            ("tax_shock_confidence", "TEXT"),
            ("county_name", "TEXT"),
            ("county_fips", "TEXT"),
            ("county_millage_change", "REAL"),
            ("county_taxes_levied_growth", "REAL"),
            ("tax_shock_reasons_json", "TEXT"),
            ("tax_shock_breakdown_json", "TEXT")
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                cursor.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}")
                logging.info(f"Added column: {col_name}")
            except sqlite3.OperationalError as e:
                # Column likely exists
                if "duplicate column name" in str(e):
                    logging.info(f"Column {col_name} already exists. Skipping.")
                else:
                    raise e

        # 2. Create county_tax_metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS county_tax_metrics (
                county_fips TEXT,
                year INTEGER,
                total_millage REAL,
                taxes_levied REAL,
                yoy_millage_change REAL,
                yoy_taxes_growth REAL,
                computed_at TEXT,
                PRIMARY KEY (county_fips, year)
            )
        """)
        logging.info("Created table: county_tax_metrics")
        
        conn.commit()
        logging.info("Migration successful.")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()

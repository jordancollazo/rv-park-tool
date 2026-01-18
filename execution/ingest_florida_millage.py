"""
ingest_florida_millage.py
Downloads and parses Florida DOR millage data.
Stores county-level millage rates for tax shock scoring.
"""

import os
import requests
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

URL = "https://floridarevenue.com/property/Documents/millage_taxes_levied.xlsx"
DATA_DIR = Path("data/fl_dor")
DB_PATH = Path("data/leads.db")
EXCEL_PATH = DATA_DIR / "millage_taxes_levied.xlsx"

# Florida County FIPS mapping
FIPS_MAP = {
    'ALACHUA': '12001', 'BAKER': '12003', 'BAY': '12005', 'BRADFORD': '12007',
    'BREVARD': '12009', 'BROWARD': '12011', 'CALHOUN': '12013', 'CHARLOTTE': '12015',
    'CITRUS': '12017', 'CLAY': '12019', 'COLLIER': '12021', 'COLUMBIA': '12023',
    'MIAMI-DADE': '12086', 'DADE': '12086', 'DESOTO': '12027', 'DIXIE': '12029',
    'DUVAL': '12031', 'ESCAMBIA': '12033', 'FLAGLER': '12035', 'FRANKLIN': '12037',
    'GADSDEN': '12039', 'GILCHRIST': '12041', 'GLADES': '12043', 'GULF': '12045',
    'HAMILTON': '12047', 'HARDEE': '12049', 'HENDRY': '12051', 'HERNANDO': '12053',
    'HIGHLANDS': '12055', 'HILLSBOROUGH': '12057', 'HOLMES': '12059',
    'INDIAN RIVER': '12061', 'JACKSON': '12063', 'JEFFERSON': '12065',
    'LAFAYETTE': '12067', 'LAKE': '12069', 'LEE': '12071', 'LEON': '12073',
    'LEVY': '12075', 'LIBERTY': '12077', 'MADISON': '12079', 'MANATEE': '12081',
    'MARION': '12083', 'MARTIN': '12085', 'MONROE': '12087', 'NASSAU': '12089',
    'OKALOOSA': '12091', 'OKEECHOBEE': '12093', 'ORANGE': '12095',
    'OSCEOLA': '12097', 'PALM BEACH': '12099', 'PASCO': '12101',
    'PINELLAS': '12103', 'POLK': '12105', 'PUTNAM': '12107', 
    'ST JOHNS': '12109', 'ST. JOHNS': '12109', 'SAINT JOHNS': '12109',
    'ST LUCIE': '12111', 'ST. LUCIE': '12111', 'SAINT LUCIE': '12111',
    'SANTA ROSA': '12113', 'SARASOTA': '12115', 'SEMINOLE': '12117',
    'SUMTER': '12119', 'SUWANNEE': '12121', 'TAYLOR': '12123', 'UNION': '12125',
    'VOLUSIA': '12127', 'WAKULLA': '12129', 'WALTON': '12131', 'WASHINGTON': '12133'
}

def get_fips(county_name):
    """Convert county name to FIPS code."""
    name = str(county_name).upper().replace(' COUNTY', '').strip()
    return FIPS_MAP.get(name, None)

def download_data():
    """Download the Excel file if needed."""
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True)
    
    logging.info(f"Downloading {URL}...")
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        with open(EXCEL_PATH, 'wb') as f:
            f.write(response.content)
        logging.info("Download complete.")
    except Exception as e:
        logging.error(f"Download failed: {e}")
        if not EXCEL_PATH.exists():
            raise

def parse_and_store():
    """Parse Excel and store millage metrics."""
    if not EXCEL_PATH.exists():
        logging.error("No data file found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        xls = pd.ExcelFile(EXCEL_PATH)
        logging.info(f"Sheets: {xls.sheet_names}")
        
        # Read Millage Rates sheet with proper header
        df = pd.read_excel(xls, sheet_name='Millage Rates', header=3)
        
        # Find the County column and Total/Countywide Millage column
        county_col = None
        millage_col = None
        
        for col in df.columns:
            col_str = str(col).lower()
            if 'county' in col_str and county_col is None:
                county_col = col
            if 'total' in col_str or 'countywide' in col_str or 'aggregate' in col_str:
                millage_col = col
        
        # If no total column, try to find a numeric column that looks like total millage (typically 15-25 range)
        if millage_col is None:
            for col in df.columns:
                if col != county_col:
                    try:
                        sample = pd.to_numeric(df[col].dropna().head(5), errors='coerce')
                        if sample.mean() > 10 and sample.mean() < 30:  # Typical FL millage range
                            millage_col = col
                            break
                    except:
                        continue
        
        if county_col is None:
            logging.error("Could not find County column")
            return
            
        logging.info(f"Using columns: County='{county_col}', Millage='{millage_col}'")
        
        # Process data
        current_year = datetime.now().year
        metrics_list = []
        
        for _, row in df.iterrows():
            county_name = row[county_col]
            if pd.isna(county_name) or 'total' in str(county_name).lower():
                continue
                
            fips = get_fips(county_name)
            if fips is None:
                continue
            
            # Get millage value - try multiple columns if needed
            millage = None
            if millage_col and pd.notna(row.get(millage_col)):
                millage = float(row[millage_col])
            else:
                # Sum up available millage columns
                total = 0
                for col in df.columns:
                    if col != county_col and 'millage' not in str(col).lower():
                        try:
                            val = float(row[col])
                            if pd.notna(val) and 0 < val < 20:  # Individual millage typically < 20
                                total += val
                        except:
                            continue
                if total > 0:
                    millage = total
            
            if millage and millage > 0:
                metrics_list.append((
                    fips,
                    current_year,
                    millage,
                    0,  # taxes_levied (not parsing this sheet)
                    0,  # yoy_millage_change (single year data)
                    0,  # yoy_taxes_growth
                    datetime.now().isoformat()
                ))
        
        if metrics_list:
            cursor.executemany("""
                INSERT OR REPLACE INTO county_tax_metrics 
                (county_fips, year, total_millage, taxes_levied, yoy_millage_change, yoy_taxes_growth, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, metrics_list)
            conn.commit()
            logging.info(f"Inserted {len(metrics_list)} county records.")
        else:
            logging.warning("No data extracted.")

    except Exception as e:
        logging.error(f"Processing error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    download_data()
    parse_and_store()

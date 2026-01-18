"""
ingest_crexi_export.py

Ingests leads from a Crexi Excel export file into the leads database.

Usage:
    python execution/ingest_crexi_export.py --file data/Export\ Results.xlsx
    python execution/ingest_crexi_export.py --file data/Export\ Results.xlsx --dry-run
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Add execution directory to path for db import
sys.path.insert(0, str(Path(__file__).parent))
import db


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    if not text:
        return ""
    text = str(text).lower()
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'[^a-z0-9-]', '', text)
    text = re.sub(r'-+', '-', text)
    text = text.strip('-')
    return text


def extract_crexi_id(url: str) -> str | None:
    """Extract Crexi ID from URL like https://www.crexi.com/properties/1234567/..."""
    if not url or not isinstance(url, str):
        return None
    match = re.search(r'/properties/(\d+)', url)
    return match.group(1) if match else None


def parse_price(value) -> float | None:
    """Parse price value, handling $ and commas."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    # Remove $ and commas
    cleaned = str(value).replace('$', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except:
        return None


def parse_percent(value) -> float | None:
    """Parse percentage value."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace('%', '').strip()
    try:
        return float(cleaned)
    except:
        return None


def parse_int(value) -> int | None:
    """Parse integer value."""
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except:
        return None


def normalize_crexi_export_row(row: dict) -> dict | None:
    """Normalize a row from Crexi export to our database schema."""
    
    url = row.get('Property Link')
    crexi_id = extract_crexi_id(url)
    
    if not crexi_id:
        return None
    
    name = row.get('Property Name') or "Unknown Property"
    state = row.get('State')
    
    # Determine category based on Type field
    prop_type = str(row.get('Type', '')).lower()
    if 'mobile' in prop_type or 'manufactured' in prop_type:
        category = "Mobile Home Park"
    elif 'rv' in prop_type:
        category = "RV Park"
    else:
        category = "Mobile Home Park"  # Default for this export
    
    return {
        "place_id": f"crexi:{crexi_id}",
        "crexi_id": crexi_id,
        "scrape_source": "crexi",
        
        # Basic Info
        "name": name,
        "address": row.get('Address'),
        "city": row.get('City'),
        "state": state,
        "zip": row.get('Zip'),
        "latitude": row.get('Latitude'),
        "longitude": row.get('Longitude'),
        
        # Financials
        "asking_price": parse_price(row.get('Asking Price')),
        "cap_rate": parse_percent(row.get('Cap Rate')),
        "noi": parse_price(row.get('NOI')),
        "price_per_unit": parse_price(row.get('Price/Unit')),
        
        # Property Details
        "lot_count": parse_int(row.get('Units')),
        "listing_url": url,
        "category": category,
        "sq_ft": parse_price(row.get('SqFt')),
        "days_on_market": parse_int(row.get('Days on Market')),
        
        # CRM defaults
        "status": "not_contacted",
    }


def ingest_crexi_export(file_path: str, dry_run: bool = False):
    """Ingest Crexi export Excel file."""
    
    print(f"Reading: {file_path}")
    
    # Read Excel, skip the first row which has "129 properties found"
    # The actual headers are in row 1 (0-indexed row 0 after header=1)
    df = pd.read_excel(file_path, header=1)
    
    # The first data row contains actual column names, so we need to use them
    column_names = df.iloc[0].tolist()
    df = pd.read_excel(file_path, skiprows=2, names=column_names)
    
    print(f"Found {len(df)} rows")
    
    leads_to_upsert = []
    skipped = 0
    
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        normalized = normalize_crexi_export_row(row_dict)
        
        if normalized:
            leads_to_upsert.append(normalized)
        else:
            skipped += 1
    
    print(f"Valid leads: {len(leads_to_upsert)}, Skipped: {skipped}")
    
    if dry_run:
        print("\n[DRY RUN] Sample leads:")
        for lead in leads_to_upsert[:5]:
            print(f"  - {lead['name']} ({lead['city']}, {lead['state']}) | ${lead['asking_price']} | {lead['cap_rate']}%")
        print(f"  ... and {len(leads_to_upsert) - 5} more")
        return
    
    # Upsert to database
    print(f"\nUpserting {len(leads_to_upsert)} leads...")
    inserted, updated = db.bulk_upsert_leads(leads_to_upsert)
    print(f"Done! Inserted: {inserted}, Updated: {updated}")


def main():
    parser = argparse.ArgumentParser(description="Ingest Crexi Excel export")
    parser.add_argument("--file", required=True, help="Path to Excel export file")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    
    args = parser.parse_args()
    
    if not Path(args.file).exists():
        print(f"ERROR: File not found: {args.file}")
        sys.exit(1)
    
    ingest_crexi_export(args.file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

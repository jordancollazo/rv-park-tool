"""
enrich_zcta_risk.py

Aggregates lead-level risk metrics to ZCTA (Zip Code) level.
Calculates:
- Average Insurance Pressure Score
- Flood Zone % (Percentage of leads in high-risk zones)
- Storm Proximity Score (Average)

Usage:
    python execution/enrich_zcta_risk.py
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/leads.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def enrich_zcta_risk():
    print("="*60)
    print("ZCTA RISK AGGREGATION")
    print("="*60)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check for columns i zcta_metrics
    cursor.execute("PRAGMA table_info(zcta_metrics)")
    columns = [row[1] for row in cursor.fetchall()]
    
    new_cols = [
        ("avg_insurance_pressure", "REAL"),
        ("avg_flood_risk_score", "REAL"),
        ("high_flood_risk_pct", "REAL"),
        ("avg_storm_score", "REAL"),
        ("risk_data_count", "INTEGER"),
        ("avg_tax_shock", "REAL"),
        ("tax_data_count", "INTEGER"),
        ("avg_millage_increase", "REAL"),
        ("avg_levy_growth", "REAL"),
        ("dominant_city", "TEXT"),
        ("dominant_county", "TEXT")
    ]
    
    for col_name, col_type in new_cols:
        if col_name not in columns:
            print(f"Adding column {col_name}...")
            cursor.execute(f"ALTER TABLE zcta_metrics ADD COLUMN {col_name} {col_type}")
    
    # 1. Get all leads with risk data, grouped by ZCTA
    print("Aggregating lead risks...")
    
    cursor.execute("""
        SELECT 
            COALESCE(zcta_derived, substr(zip, 1, 5)) as zcta,
            count(*) as count,
            avg(insurance_pressure_score_0_100) as avg_pressure,
            avg(storm_proximity_score) as avg_storm,
            sum(CASE WHEN flood_zone IN ('A', 'AE', 'AH', 'AO', 'VE', 'V') THEN 1 ELSE 0 END) as flood_count
        FROM leads
        WHERE (zcta_derived IS NOT NULL OR zip IS NOT NULL)
          AND insurance_pressure_score_0_100 IS NOT NULL
        GROUP BY COALESCE(zcta_derived, substr(zip, 1, 5))
    """)
    
    risk_rows = cursor.fetchall()
    print(f"Found risk data for {len(risk_rows)} zip codes")

    # 2. Aggregating Tax Shock separately
    print("Aggregating Tax Shock...")
    cursor.execute("""
        SELECT 
            COALESCE(zcta_derived, substr(zip, 1, 5)) as zcta,
            count(*) as count,
            avg(tax_shock_score_0_100) as avg_tax,
            avg(county_millage_change) as avg_millage,
            avg(county_taxes_levied_growth) as avg_levy
        FROM leads
        WHERE (zcta_derived IS NOT NULL OR zip IS NOT NULL)
          AND tax_shock_score_0_100 IS NOT NULL
        GROUP BY COALESCE(zcta_derived, substr(zip, 1, 5))
    """)
    tax_rows = cursor.fetchall()
    tax_map = {r["zcta"]: {
        "avg": r["avg_tax"], "count": r["count"],
        "millage": r["avg_millage"], "levy": r["avg_levy"]
    } for r in tax_rows}

    # 3. Aggregating Dominant Location (City/County)
    # We find the most frequent city/county for each ZCTA
    print("Aggregating Locations...")
    cursor.execute("""
        SELECT 
            zcta, 
            city, 
            county_name,
            count(*) as freq
        FROM (
            SELECT 
                COALESCE(zcta_derived, substr(zip, 1, 5)) as zcta,
                city,
                county_name
            FROM leads
            WHERE (zcta_derived IS NOT NULL OR zip IS NOT NULL)
        )
        GROUP BY zcta, city, county_name
        ORDER BY zcta, freq DESC
    """)
    loc_rows = cursor.fetchall()
    
    # Simple logic: first row for each ZCTA is the dominant one (since ordered by freq DESC)
    loc_map = {}
    for r in loc_rows:
        z = r["zcta"]
        if z not in loc_map:
            loc_map[z] = {"city": r["city"], "county": r["county_name"]}

    updated = 0
    # Update risk metrics loop (augmented to include location)
    # Note: We iterate over risk_rows, but we should actually iterate over ALL ZCTAs we have data for.
    # To cover ZCTAs that might stick to just one data source, we can collect all ZCTAs in a set first.
    
    all_zctas = set([r["zcta"] for r in risk_rows] + list(tax_map.keys()) + list(loc_map.keys()))
    
    risk_map = {r["zcta"]: r for r in risk_rows}
    
    for zcta in all_zctas:
        if not zcta or len(zcta) != 5: continue
        
        # Risk Data
        risk = risk_map.get(zcta)
        if risk:
            count = risk["count"]
            avg_pressure = risk["avg_pressure"]
            avg_storm = risk["avg_storm"]
            flood_count = risk["flood_count"]
            high_flood_pct = (flood_count / count) * 100 if count > 0 else 0
        else:
            count = 0
            avg_pressure = None
            avg_storm = None
            high_flood_pct = None

        # Tax Data
        tax = tax_map.get(zcta, {})
        avg_tax = tax.get("avg")
        tax_count = tax.get("count")
        avg_millage = tax.get("millage")
        avg_levy = tax.get("levy")
        
        # Location Data
        loc = loc_map.get(zcta, {})
        dom_city = loc.get("city")
        dom_county = loc.get("county")

        cursor.execute("""
            UPDATE zcta_metrics
            SET avg_insurance_pressure = ?,
                avg_storm_score = ?,
                high_flood_risk_pct = ?,
                risk_data_count = ?,
                avg_tax_shock = ?,
                tax_data_count = ?,
                avg_millage_increase = ?,
                avg_levy_growth = ?,
                dominant_city = ?,
                dominant_county = ?
            WHERE zcta = ?
        """, (
            round(avg_pressure, 1) if avg_pressure is not None else None,
            round(avg_storm, 1) if avg_storm is not None else None,
            round(high_flood_pct, 1) if high_flood_pct is not None else None,
            count,
            round(avg_tax, 1) if avg_tax is not None else None,
            tax_count,
            round(avg_millage, 2) if avg_millage is not None else None,
            round(avg_levy, 2) if avg_levy is not None else None,
            dom_city,
            dom_county,
            zcta
        ))
        
        if cursor.rowcount > 0:
            updated += 1
            
    conn.commit()
    conn.close()
    
    print(f"\nUpdated {updated} ZCTAs with full metrics (Risk + Tax + Location)")
    
    print(f"\nUpdated {updated} ZCTAs with risk metrics")

if __name__ == "__main__":
    enrich_zcta_risk()

"""
check_enrichment_status.py
Check which enrichments have been run on LoopNet and Crexi leads.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def check_enrichment_status():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get enrichment status for LoopNet leads
    cursor.execute("""
        SELECT
            'loopnet' as source,
            COUNT(*) as total_leads,
            SUM(CASE WHEN tax_shock_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as tax_shock_enriched,
            SUM(CASE WHEN flood_zone IS NOT NULL THEN 1 ELSE 0 END) as flood_zone_enriched,
            SUM(CASE WHEN insurance_pressure_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as insurance_enriched,
            SUM(CASE WHEN storm_proximity_score IS NOT NULL THEN 1 ELSE 0 END) as storm_enriched,
            SUM(CASE WHEN disaster_pressure_score IS NOT NULL THEN 1 ELSE 0 END) as disaster_enriched
        FROM leads
        WHERE loopnet_id IS NOT NULL
        AND (archived = 0 OR archived IS NULL)
    """)

    loopnet = cursor.fetchone()

    # Get enrichment status for Crexi leads
    cursor.execute("""
        SELECT
            'crexi' as source,
            COUNT(*) as total_leads,
            SUM(CASE WHEN tax_shock_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as tax_shock_enriched,
            SUM(CASE WHEN flood_zone IS NOT NULL THEN 1 ELSE 0 END) as flood_zone_enriched,
            SUM(CASE WHEN insurance_pressure_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as insurance_enriched,
            SUM(CASE WHEN storm_proximity_score IS NOT NULL THEN 1 ELSE 0 END) as storm_enriched,
            SUM(CASE WHEN disaster_pressure_score IS NOT NULL THEN 1 ELSE 0 END) as disaster_enriched
        FROM leads
        WHERE crexi_id IS NOT NULL
        AND (archived = 0 OR archived IS NULL)
    """)

    crexi = cursor.fetchone()

    results = [loopnet, crexi]

    print("=" * 80)
    print("ENRICHMENT STATUS - LOOPNET & CREXI LEADS")
    print("=" * 80)
    print()

    for row in results:
        source = row['source'].upper()
        total = row['total_leads']

        if total == 0:
            print(f"{source} LEADS: 0 total (no leads found)")
            print()
            continue

        print(f"{source} LEADS: {total} total")
        print(f"  Tax Shock Score:          {row['tax_shock_enriched']:3d}/{total} ({row['tax_shock_enriched']/total*100:.1f}%)")
        print(f"  Flood Zone:               {row['flood_zone_enriched']:3d}/{total} ({row['flood_zone_enriched']/total*100:.1f}%)")
        print(f"  Insurance Pressure Index: {row['insurance_enriched']:3d}/{total} ({row['insurance_enriched']/total*100:.1f}%)")
        print(f"  Storm Pressure Score:     {row['storm_enriched']:3d}/{total} ({row['storm_enriched']/total*100:.1f}%)")
        print(f"  Disaster Pressure Score:  {row['disaster_enriched']:3d}/{total} ({row['disaster_enriched']/total*100:.1f}%)")
        print()

    # Overall summary
    cursor.execute("""
        SELECT
            COUNT(*) as total_leads,
            SUM(CASE WHEN tax_shock_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as tax_shock_enriched,
            SUM(CASE WHEN flood_zone IS NOT NULL THEN 1 ELSE 0 END) as flood_zone_enriched,
            SUM(CASE WHEN insurance_pressure_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as insurance_enriched
        FROM leads
        WHERE (loopnet_id IS NOT NULL OR crexi_id IS NOT NULL)
        AND (archived = 0 OR archived IS NULL)
    """)

    summary = cursor.fetchone()
    total = summary['total_leads']

    if total > 0:
        print("COMBINED TOTAL (LoopNet + Crexi): {} leads".format(total))
        print(f"  Tax Shock Score:          {summary['tax_shock_enriched']:3d}/{total} ({summary['tax_shock_enriched']/total*100:.1f}%)")
        print(f"  Flood Zone:               {summary['flood_zone_enriched']:3d}/{total} ({summary['flood_zone_enriched']/total*100:.1f}%)")
        print(f"  Insurance Pressure Index: {summary['insurance_enriched']:3d}/{total} ({summary['insurance_enriched']/total*100:.1f}%)")
        print()
    print("=" * 80)

    conn.close()

if __name__ == "__main__":
    check_enrichment_status()

"""
check_all_enrichments.py
Check all enrichment fields to see which ones are populated for LoopNet/Crexi leads.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def check_all_enrichments():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get total LoopNet/Crexi leads
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM leads
        WHERE (loopnet_id IS NOT NULL OR crexi_id IS NOT NULL)
        AND (archived = 0 OR archived IS NULL)
    """)
    total = cursor.fetchone()['total']

    if total == 0:
        print("No LoopNet or Crexi leads found.")
        return

    # Check all enrichment-related fields
    enrichment_fields = [
        # Location enrichments
        ('county_name', 'County Name'),
        ('county_fips', 'County FIPS Code'),
        ('zcta_derived', 'ZCTA (Zip Code)'),

        # Tax enrichments
        ('tax_shock_score_0_100', 'Tax Shock Score'),
        ('tax_shock_confidence', 'Tax Shock Confidence'),
        ('county_millage_change', 'County Millage Rate'),

        # Insurance/Risk enrichments
        ('flood_zone', 'Flood Zone'),
        ('flood_zone_source', 'Flood Zone Source'),
        ('insurance_pressure_score_0_100', 'Insurance Pressure Score'),
        ('insurance_pressure_confidence', 'Insurance Pressure Confidence'),
        ('storm_proximity_score', 'Storm Proximity Score'),
        ('disaster_pressure_score', 'Disaster Pressure Score'),

        # Amenity enrichments
        ('nearest_supermarket_name', 'Nearest Supermarket'),
        ('nearest_hospital_name', 'Nearest Hospital'),
        ('nearest_school_name', 'Nearest School'),
        ('amenity_score', 'Amenity Score'),

        # Owner fatigue enrichments
        ('owner_fatigue_score_0_100', 'Owner Fatigue Score'),
        ('owner_fatigue_confidence', 'Owner Fatigue Confidence'),

        # Website/scraping enrichments
        ('website', 'Website'),
        ('crawl_status', 'Website Crawl Status'),
        ('site_score_1_10', 'Website Score'),

        # Basic enrichments
        ('google_rating', 'Google Rating'),
        ('review_count', 'Review Count'),
        ('is_enriched', 'General Enrichment Flag'),
    ]

    print("=" * 90)
    print(f"ENRICHMENT STATUS FOR LOOPNET & CREXI LEADS ({total} total)")
    print("=" * 90)
    print()
    print(f"{'Enrichment Field':<40} {'Populated':<15} {'Percentage':<15}")
    print("-" * 90)

    results = []

    for field_name, field_label in enrichment_fields:
        cursor.execute(f"""
            SELECT
                SUM(CASE
                    WHEN {field_name} IS NOT NULL
                    AND {field_name} != ''
                    AND {field_name} != 0
                    THEN 1
                    ELSE 0
                END) as populated
            FROM leads
            WHERE (loopnet_id IS NOT NULL OR crexi_id IS NOT NULL)
            AND (archived = 0 OR archived IS NULL)
        """)

        populated = cursor.fetchone()['populated']
        percentage = (populated / total * 100) if total > 0 else 0
        results.append((field_label, populated, percentage))

        # Color coding in text
        status = "[OK]" if percentage >= 95 else "[--]" if percentage >= 50 else "[  ]"
        print(f"{field_label:<40} {populated:>3}/{total:<8} {percentage:>6.1f}%  {status}")

    print()
    print("=" * 90)
    print("Legend: [OK] = 95%+  [--] = 50-94%  [  ] = <50%")
    print("=" * 90)
    print()

    # Identify missing enrichments
    missing = [r for r in results if r[2] < 95]
    if missing:
        print("ENRICHMENTS NEEDING ATTENTION (< 95% complete):")
        print("-" * 90)
        for field_label, populated, percentage in missing:
            print(f"  • {field_label}: {populated}/{total} ({percentage:.1f}%)")
        print()
    else:
        print("All enrichments are 95%+ complete!")
        print()

    conn.close()

if __name__ == "__main__":
    check_all_enrichments()

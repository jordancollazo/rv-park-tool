"""Quick verification script for enrichment coverage."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Get enrichment coverage for LoopNet/Crexi leads
cursor.execute("""
    SELECT 
        scrape_source, 
        COUNT(*) as total, 
        SUM(CASE WHEN flood_zone IS NOT NULL THEN 1 ELSE 0 END) as has_flood,
        SUM(CASE WHEN tax_shock_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as has_tax,
        SUM(CASE WHEN insurance_pressure_score_0_100 IS NOT NULL THEN 1 ELSE 0 END) as has_insurance,
        SUM(CASE WHEN amenity_score IS NOT NULL THEN 1 ELSE 0 END) as has_amenity
    FROM leads 
    WHERE scrape_source IN ('loopnet', 'crexi')
    GROUP BY scrape_source
""")
results = cursor.fetchall()

print("=" * 70)
print("ENRICHMENT COVERAGE FOR LOOPNET/CREXI LEADS")
print("=" * 70)
print(f"{'Source':<12} | {'Total':<8} | {'Flood':<8} | {'Tax':<8} | {'Insurance':<10} | {'Amenity':<8}")
print("-" * 70)
for r in results:
    print(f"{r[0] or 'N/A':<12} | {r[1]:<8} | {r[2]:<8} | {r[3]:<8} | {r[4]:<10} | {r[5]:<8}")

# Show some sample enriched leads
print("\n" + "=" * 70)
print("SAMPLE ENRICHED LOOPNET/CREXI LEADS")
print("=" * 70)
cursor.execute("""
    SELECT name, scrape_source, flood_zone, tax_shock_score_0_100, 
           insurance_pressure_score_0_100, amenity_score
    FROM leads 
    WHERE scrape_source IN ('loopnet', 'crexi')
    LIMIT 10
""")
samples = cursor.fetchall()
for s in samples:
    print(f"{s[0][:40]:<42} | {s[1] or 'N/A':<8} | Flood: {s[2] or 'N/A':<4} | Tax: {s[3] or 0:.0f} | Ins: {s[4] or 0:.0f} | Amenity: {s[5] or 0}")

conn.close()

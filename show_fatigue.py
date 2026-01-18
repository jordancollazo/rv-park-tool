import sqlite3
import json
from pathlib import Path

db_path = Path('data/leads.db')
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Get the 20 most recently scored leads (sorted by latitude ascending = south first)
cur.execute('''
    SELECT name, city, latitude, owner_fatigue_score_0_100 as score, 
           owner_fatigue_confidence as conf, owner_fatigue_reasons_json as reasons
    FROM leads 
    WHERE owner_fatigue_confidence IS NOT NULL
    ORDER BY latitude ASC
    LIMIT 20
''')

results = cur.fetchall()
print("FIRST 20 LEADS (South to North) - Owner Fatigue Scores")
print("=" * 70)
print()

for name, city, lat, score, conf, reasons_json in results:
    reasons = json.loads(reasons_json) if reasons_json else []
    top_reason = reasons[0][:50] if reasons else "N/A"
    print(f"  {score}/100 ({conf:6}) | Lat {lat:.2f} | {name[:30]:<30}")
    if reasons:
        print(f"                      Reason: {top_reason}")

print()
print("SUMMARY:")
cur.execute('SELECT AVG(owner_fatigue_score_0_100) FROM leads WHERE owner_fatigue_confidence IS NOT NULL')
avg = cur.fetchone()[0] or 0
cur.execute('SELECT COUNT(*) FROM leads WHERE owner_fatigue_score_0_100 >= 40')
high = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM leads WHERE owner_fatigue_score_0_100 >= 30 AND owner_fatigue_score_0_100 < 40')
med = cur.fetchone()[0]
print(f"  Average Score: {avg:.1f}/100")
print(f"  High Fatigue (>=40): {high}")
print(f"  Medium Fatigue (30-39): {med}")

conn.close()

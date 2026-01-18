"""Count Crexi leads in database."""
import sqlite3

conn = sqlite3.connect('data/leads.db')
r = conn.execute("SELECT COUNT(*) FROM leads WHERE scrape_source='crexi'").fetchone()
print(f"Total Crexi leads in database: {r[0]}")

# Also show breakdown by state
print("\nBreakdown by state:")
rows = conn.execute("""
    SELECT state, COUNT(*) as cnt 
    FROM leads 
    WHERE scrape_source='crexi' 
    GROUP BY state 
    ORDER BY cnt DESC 
    LIMIT 10
""").fetchall()
for state, cnt in rows:
    print(f"  {state}: {cnt}")

"""Check sample of remaining leads to see what's still showing."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== Sample of remaining Crexi leads ===")
cur.execute("""
    SELECT id, name, category, description
    FROM leads 
    WHERE scrape_source = 'crexi'
    ORDER BY RANDOM()
    LIMIT 30
""")
for row in cur.fetchall():
    desc_snippet = (row['description'] or '')[:80].replace('\n', ' ')
    print(f"  [{row['id']}] {row['name']}")
    print(f"      Cat: {row['category']} | Desc: {desc_snippet}...")
    print()

conn.close()

import sqlite3
from pathlib import Path

db = sqlite3.connect(Path(__file__).parent.parent / "data" / "leads.db")
cursor = db.execute("SELECT COUNT(*) FROM leads WHERE source_query = 'LoopNet Scraper'")
print(f"Total LoopNet leads in database: {cursor.fetchone()[0]}")

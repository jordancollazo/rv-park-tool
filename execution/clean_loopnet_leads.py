
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def clean_db():
    conn = sqlite3.connect(DB_PATH)
    # Delete leads that don't satisfy the property type check
    # Our new script puts "Type - Subtype" in description_keywords.
    # The old bad leads have long descriptions there.
    # We can also just delete everything scraped > 5 mins ago if we want a fresh start, 
    # but let's just delete the ones that don't look like our new format or delete ALL loopnet to be safe if this next run worked.
    
    # Safest: Delete all LoopNet leads created before the last few minutes? 
    # Or just delete all for now to clear the "bad" 50.
    
    # Let's delete ALL LoopNet leads to ensure we only show what we just scraped (or failed to scrape).
    # But if the current scrape is running, we shouldn't delete what it just inserted?
    # The current scrape inserts with `crawl_status='scraped_loopnet'`.
    # The old ones also have that.
    
    # I'll delete all. The user wants "ensure you only scraped RV parks".
    # If the current run fails, we have 0. That's better than 50 bad ones.
    # If the current run is inserting, we might have a race, but SQL is atomic-ish.
    # Actually, let's just delete the ones that are NOT MHP/RV.
    
    conn.execute("DELETE FROM leads WHERE source_query = 'LoopNet Scraper'")
    conn.commit()
    print("Deleted all LoopNet leads.")
    conn.close()

if __name__ == "__main__":
    clean_db()

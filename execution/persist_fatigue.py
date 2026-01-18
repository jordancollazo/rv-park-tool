
import json
import sqlite3
from pathlib import Path

TMP_PATH = Path(".tmp/owner_fatigue_scored.json")
DB_PATH = Path("data/leads.db")

def persist():
    if not TMP_PATH.exists():
        print(f"Error: {TMP_PATH} not found.")
        return

    with open(TMP_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    conn = sqlite3.connect(DB_PATH)
    
    print(f"Persisting scores for {len(data.get('places', []))} items...")
    count = 0
    for item in data.get("places", []):
        score = item.get("owner_fatigue_score_0_100")
        place_id = item.get("place_id")
        
        if place_id and score is not None:
            conn.execute("""
                UPDATE leads 
                SET owner_fatigue_score_0_100 = ?,
                    owner_fatigue_confidence = ?,
                    owner_fatigue_reasons_json = ?,
                    owner_fatigue_breakdown_json = ?
                WHERE place_id = ?
            """, (
                score,
                item.get("owner_fatigue_confidence"),
                json.dumps(item.get("owner_fatigue_reasons_json")),
                json.dumps(item.get("owner_fatigue_breakdown_json")),
                place_id
            ))
            count += 1
            
    conn.commit()
    conn.close()
    print(f"Updated {count} leads.")

if __name__ == "__main__":
    persist()

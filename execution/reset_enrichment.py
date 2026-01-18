"""
reset_enrichment.py
Reset is_enriched flag for testing purposes.
"""

from db import get_db

def reset_enrichment(limit=10):
    with get_db() as conn:
        # Get IDs of leads to reset
        rows = conn.execute("""
            SELECT id 
            FROM leads 
            WHERE website IS NOT NULL 
            AND website != ''
            LIMIT ?
        """, (limit,)).fetchall()
        
        if not rows:
            print("No leads with websites found.")
            return
        
        ids = [row['id'] for row in rows]
        placeholders = ','.join('?' * len(ids))
        
        # Reset enrichment flag
        cursor = conn.execute(f"""
            UPDATE leads 
            SET is_enriched = 0 
            WHERE id IN ({placeholders})
        """, ids)
        
        conn.commit()
        count = cursor.rowcount
        print(f"Reset {count} leads for re-enrichment")
        
        # Show which leads were reset
        rows = conn.execute("""
            SELECT id, name, website 
            FROM leads 
            WHERE is_enriched = 0 
            LIMIT ?
        """, (limit,)).fetchall()
        
        print("\nLeads ready for enrichment:")
        for row in rows:
            print(f"  - {row['name']} ({row['website']})")

if __name__ == "__main__":
    reset_enrichment(10)

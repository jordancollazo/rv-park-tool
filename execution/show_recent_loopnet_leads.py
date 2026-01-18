
import sqlite3
from pathlib import Path
from tabulate import tabulate # Assuming tabulate might be installed, if not fallback to simple print

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"

def show_leads():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Query for LoopNet leads
    query = """
        SELECT 
            name, city, state, list_price, cap_rate, broker_firm, loopnet_url
        FROM leads 
        WHERE source_query = 'LoopNet Scraper'
        ORDER BY last_scraped_at DESC
        LIMIT 20
    """
    
    rows = conn.execute(query).fetchall()
    conn.close()
    
    if not rows:
        print("No LoopNet leads found in the database.")
        return

    # Prepare data for display
    data = []
    for row in rows:
        price = f"${row['list_price']:,.0f}" if row['list_price'] else "N/A"
        cap = f"{row['cap_rate']}%" if row['cap_rate'] else "N/A"
        data.append([
            row['name'][:30],
            row['city'],
            row['state'],
            price,
            cap,
            (row['broker_firm'] or "")[:20],
            (row['loopnet_url'] or "")[:40]
        ])
    
    headers = ["Name", "City", "State", "Price", "Cap Rate", "Broker", "URL"]
    
    try:
        from tabulate import tabulate
        print(tabulate(data, headers=headers, tablefmt="grid"))
    except ImportError:
        # Fallback if tabulate is not installed
        print(f"{'Name':<30} | {'City':<15} | {'State':<5} | {'Price':<12} | {'Cap':<8} | {'Broker':<20}")
        print("-" * 100)
        for row in data:
            print(f"{row[0]:<30} | {row[1]:<15} | {row[2]:<5} | {row[3]:<12} | {row[4]:<8} | {row[5]:<20}")

if __name__ == "__main__":
    show_leads()

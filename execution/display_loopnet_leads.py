import sqlite3
from pathlib import Path

db = sqlite3.connect(Path(__file__).parent.parent / "data" / "leads.db")
db.row_factory = sqlite3.Row

rows = db.execute("""
    SELECT *
    FROM leads 
    WHERE source_query = 'LoopNet Scraper' 
    ORDER BY last_scraped_at DESC 
    LIMIT 5
""").fetchall()

output = []
output.append("")
output.append("=" * 60)
output.append("   DETAILED LOOPNET LEADS (5 most recent)")
output.append("=" * 60)
output.append("")

for i, r in enumerate(rows, 1):
    output.append("-" * 60)
    output.append(f"  LEAD #{i}")
    output.append("-" * 60)
    
    # Basic Info
    name = r['name'] or 'N/A'
    output.append(f"  Name:         {name}")
    output.append(f"  Address:      {r['address'] or 'N/A'}")
    output.append(f"  City:         {r['city'] or 'N/A'}")
    output.append(f"  State:        {r['state'] or 'N/A'}")
    output.append(f"  ZIP:          {r['zip'] or 'N/A'}")
    
    # Financials
    output.append("")
    output.append("  --- FINANCIALS ---")
    if r['list_price']:
        output.append(f"  List Price:   ${r['list_price']:,.0f}")
    else:
        output.append("  List Price:   N/A")
    
    if r['cap_rate']:
        output.append(f"  Cap Rate:     {r['cap_rate']}%")
    else:
        output.append("  Cap Rate:     N/A")
    
    if r['noi']:
        output.append(f"  NOI:          ${r['noi']:,.0f}")
    else:
        output.append("  NOI:          N/A")
    
    # Property Details
    output.append("")
    output.append("  --- PROPERTY DETAILS ---")
    output.append(f"  Year Built:   {r['year_built'] or 'N/A'}")
    output.append(f"  Building Size:{r['building_size'] or 'N/A'}")
    output.append(f"  Lot Size:     {r['lot_size'] or 'N/A'}")
    
    # Broker Info
    output.append("")
    output.append("  --- BROKER INFO ---")
    output.append(f"  Broker Name:  {r['broker_name'] or 'N/A'}")
    output.append(f"  Broker Firm:  {r['broker_firm'] or 'N/A'}")
    
    # URL
    output.append("")
    output.append("  --- LINK ---")
    output.append(f"  LoopNet URL:  {r['loopnet_url'] or 'N/A'}")
    
    # Description
    output.append("")
    output.append("  --- DESCRIPTION ---")
    desc = r['detailed_description'] or r['description_keywords'] or 'N/A'
    if len(str(desc)) > 400:
        output.append(f"  {desc[:400]}...")
    else:
        output.append(f"  {desc}")
    
    output.append("")
    output.append("")

# Write to file
out_file = Path(__file__).parent.parent / "loopnet_leads_details.txt"
out_file.write_text("\n".join(output), encoding='utf-8')
print(f"Output written to: {out_file}")

"""
generate_validation_report.py

Generate a comprehensive HTML report of all leads that need validation,
organized by confidence level, with direct links to listings.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "leads.db"
OUTPUT_PATH = Path(__file__).parent.parent / "output" / "validation_report.html"

def generate_html_report():
    """Generate HTML validation report."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all non-dead LoopNet/Crexi leads
    cursor.execute("""
        SELECT id, name, address, city, state, status, scrape_source,
               loopnet_url, listing_url, description, detailed_description,
               asking_price, list_price, cap_rate,
               (SELECT COUNT(*) FROM notes WHERE lead_id = leads.id AND content LIKE '%VALIDATION%') as has_flag
        FROM leads
        WHERE scrape_source IN ('loopnet', 'crexi')
        AND status NOT IN ('dead', 'acquired')
        ORDER BY has_flag DESC, id
    """)

    leads = cursor.fetchall()

    # Categorize leads
    flagged_for_review = []
    confirmed_mhp = []
    needs_enrichment = []

    for lead in leads:
        if lead['has_flag'] > 0:
            flagged_for_review.append(lead)
        elif lead['description'] and len(lead['description']) > 50:
            confirmed_mhp.append(lead)
        else:
            needs_enrichment.append(lead)

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>MHP/RV Park Validation Report</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 40px;
            border-left: 4px solid #e74c3c;
            padding-left: 15px;
        }}
        h2.ok {{
            border-left-color: #27ae60;
        }}
        h2.warn {{
            border-left-color: #f39c12;
        }}
        .summary {{
            background-color: #ecf0f1;
            padding: 20px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .summary .stat {{
            display: inline-block;
            margin-right: 30px;
            font-size: 18px;
        }}
        .summary .stat strong {{
            color: #2c3e50;
            font-size: 24px;
        }}
        .lead-card {{
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 20px;
            margin: 15px 0;
            background-color: #fafafa;
            transition: box-shadow 0.2s;
        }}
        .lead-card:hover {{
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }}
        .lead-card.flagged {{
            border-left: 5px solid #e74c3c;
            background-color: #fff5f5;
        }}
        .lead-card.ok {{
            border-left: 5px solid #27ae60;
        }}
        .lead-card.warn {{
            border-left: 5px solid #f39c12;
            background-color: #fffbf0;
        }}
        .lead-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}
        .lead-title {{
            font-size: 20px;
            font-weight: bold;
            color: #2c3e50;
        }}
        .lead-id {{
            background-color: #3498db;
            color: white;
            padding: 5px 10px;
            border-radius: 3px;
            font-size: 14px;
        }}
        .lead-location {{
            color: #7f8c8d;
            margin-bottom: 10px;
        }}
        .lead-details {{
            margin: 15px 0;
        }}
        .detail-row {{
            margin: 5px 0;
        }}
        .detail-label {{
            font-weight: bold;
            color: #34495e;
            display: inline-block;
            width: 120px;
        }}
        .description {{
            background-color: white;
            padding: 15px;
            border-radius: 3px;
            border: 1px solid #e0e0e0;
            margin-top: 10px;
            font-style: italic;
            color: #555;
        }}
        .actions {{
            margin-top: 15px;
            display: flex;
            gap: 10px;
        }}
        .btn {{
            padding: 8px 16px;
            border-radius: 4px;
            text-decoration: none;
            font-weight: bold;
            display: inline-block;
        }}
        .btn-primary {{
            background-color: #3498db;
            color: white;
        }}
        .btn-success {{
            background-color: #27ae60;
            color: white;
        }}
        .btn-warning {{
            background-color: #f39c12;
            color: white;
        }}
        .btn-danger {{
            background-color: #e74c3c;
            color: white;
        }}
        .tag {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            margin-right: 5px;
        }}
        .tag.source {{
            background-color: #9b59b6;
            color: white;
        }}
        .tag.status {{
            background-color: #16a085;
            color: white;
        }}
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
            text-align: right;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>MHP/RV Park Validation Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <div class="summary">
            <div class="stat"><strong>{len(leads)}</strong> Total Leads</div>
            <div class="stat"><strong style="color: #e74c3c;">{len(flagged_for_review)}</strong> Flagged for Review</div>
            <div class="stat"><strong style="color: #27ae60;">{len(confirmed_mhp)}</strong> Confirmed MHP/RV</div>
            <div class="stat"><strong style="color: #f39c12;">{len(needs_enrichment)}</strong> Need Enrichment</div>
        </div>
"""

    # Section 1: Flagged for Review
    if flagged_for_review:
        html += """
        <h2>Flagged for Review (Suspected Non-MHP Properties)</h2>
        <p>These properties have been automatically flagged as potentially NOT being MHP/RV parks based on their descriptions. <strong>Please review and mark as "dead" if confirmed as non-MHP.</strong></p>
"""
        for lead in flagged_for_review:
            url = lead['loopnet_url'] or lead['listing_url'] or '#'
            price_str = f"${lead['asking_price']:,.0f}" if lead['asking_price'] else \
                       (f"${lead['list_price']:,.0f}" if lead['list_price'] else "N/A")

            html += f"""
        <div class="lead-card flagged">
            <div class="lead-header">
                <div class="lead-title">{lead['name']}</div>
                <div class="lead-id">ID: {lead['id']}</div>
            </div>
            <div class="lead-location">{lead['address']}, {lead['city']}, {lead['state']}</div>
            <div>
                <span class="tag source">{lead['scrape_source'].upper()}</span>
                <span class="tag status">{lead['status'].replace('_', ' ').title()}</span>
            </div>
            <div class="lead-details">
                <div class="detail-row">
                    <span class="detail-label">Price:</span>
                    <span>{price_str}</span>
                </div>
"""
            if lead['cap_rate']:
                html += f"""
                <div class="detail-row">
                    <span class="detail-label">Cap Rate:</span>
                    <span>{lead['cap_rate']}%</span>
                </div>
"""

            if lead['description']:
                html += f"""
                <div class="description">{lead['description'][:300]}{"..." if len(lead['description']) > 300 else ""}</div>
"""

            html += f"""
            </div>
            <div class="actions">
                <a href="{url}" class="btn btn-primary" target="_blank">View Listing</a>
                <a href="http://localhost:8000" class="btn btn-danger">Mark as Dead</a>
            </div>
        </div>
"""

    # Section 2: Confirmed MHP/RV Parks
    if confirmed_mhp:
        html += """
        <h2 class="ok">Confirmed MHP/RV Parks</h2>
        <p>These properties have strong indicators of being legitimate MHP or RV parks.</p>
"""
        for lead in confirmed_mhp[:20]:  # Show first 20
            url = lead['loopnet_url'] or lead['listing_url'] or '#'
            price_str = f"${lead['asking_price']:,.0f}" if lead['asking_price'] else \
                       (f"${lead['list_price']:,.0f}" if lead['list_price'] else "N/A")

            html += f"""
        <div class="lead-card ok">
            <div class="lead-header">
                <div class="lead-title">{lead['name']}</div>
                <div class="lead-id">ID: {lead['id']}</div>
            </div>
            <div class="lead-location">{lead['address']}, {lead['city']}, {lead['state']}</div>
            <div>
                <span class="tag source">{lead['scrape_source'].upper()}</span>
                <span class="tag status">{lead['status'].replace('_', ' ').title()}</span>
            </div>
            <div class="lead-details">
                <div class="detail-row">
                    <span class="detail-label">Price:</span>
                    <span>{price_str}</span>
                </div>
"""
            if lead['cap_rate']:
                html += f"""
                <div class="detail-row">
                    <span class="detail-label">Cap Rate:</span>
                    <span>{lead['cap_rate']}%</span>
                </div>
"""

            if lead['description']:
                html += f"""
                <div class="description">{lead['description'][:200]}{"..." if len(lead['description']) > 200 else ""}</div>
"""

            html += f"""
            </div>
            <div class="actions">
                <a href="{url}" class="btn btn-primary" target="_blank">View Listing</a>
                <a href="http://localhost:8000" class="btn btn-success">View in CRM</a>
            </div>
        </div>
"""
        if len(confirmed_mhp) > 20:
            html += f"<p><em>... and {len(confirmed_mhp) - 20} more confirmed leads</em></p>"

    # Section 3: Needs Enrichment
    if needs_enrichment:
        html += """
        <h2 class="warn">Needs More Information</h2>
        <p>These properties have minimal description data. Consider re-scraping with detailed listing scraper.</p>
"""
        for lead in needs_enrichment[:15]:  # Show first 15
            url = lead['loopnet_url'] or lead['listing_url'] or '#'
            price_str = f"${lead['asking_price']:,.0f}" if lead['asking_price'] else \
                       (f"${lead['list_price']:,.0f}" if lead['list_price'] else "N/A")

            html += f"""
        <div class="lead-card warn">
            <div class="lead-header">
                <div class="lead-title">{lead['name']}</div>
                <div class="lead-id">ID: {lead['id']}</div>
            </div>
            <div class="lead-location">{lead['address']}, {lead['city']}, {lead['state']}</div>
            <div>
                <span class="tag source">{lead['scrape_source'].upper()}</span>
                <span class="tag status">{lead['status'].replace('_', ' ').title()}</span>
            </div>
            <div class="lead-details">
                <div class="detail-row">
                    <span class="detail-label">Price:</span>
                    <span>{price_str}</span>
                </div>
"""
            if lead['description']:
                html += f"""
                <div class="description">{lead['description'][:200]}</div>
"""
            else:
                html += """
                <div class="description" style="color: #e74c3c;">No description available - needs enrichment</div>
"""

            html += f"""
            </div>
            <div class="actions">
                <a href="{url}" class="btn btn-primary" target="_blank">View Listing</a>
                <a href="http://localhost:8000" class="btn btn-warning">View in CRM</a>
            </div>
        </div>
"""
        if len(needs_enrichment) > 15:
            html += f"<p><em>... and {len(needs_enrichment) - 15} more leads needing enrichment</em></p>"

    html += f"""
        <div class="timestamp">
            Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""

    # Write to file
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    conn.close()
    return OUTPUT_PATH

def main():
    print("=" * 80)
    print("GENERATING VALIDATION REPORT")
    print("=" * 80)

    report_path = generate_html_report()

    print(f"\nReport generated successfully:")
    print(f"  {report_path}")
    print(f"\nOpen in browser:")
    print(f"  file:///{report_path.absolute()}")

if __name__ == "__main__":
    main()

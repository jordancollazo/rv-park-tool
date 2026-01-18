
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT = Path("output/debug_map.html")

def generate_minimal_map_html(places: List[Dict], zcta_metrics: List[Dict], geojson: Dict, title: str) -> str:
    """
    Generates a minimal HTML map based on the working hybrid_test.html structure.
    """
    
    # process data for JSON embedding
    places_json = json.dumps(places)
    
    # Calculate center
    center_lat = 27.66
    center_lng = -81.51
    if places:
        lats = [p['latitude'] for p in places if p.get('latitude')]
        lngs = [p['longitude'] for p in places if p.get('longitude')]
        if lats and lngs:
            center_lat = sum(lats) / len(lats)
            center_lng = sum(lngs) / len(lngs)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        html {{
            height: 100%;
        }}

        body {{
            font-family: -apple-system, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            height: 100%;
            display: flex;
            flex-direction: column;
        }}

        .header {{
            background: #1e293b;
            padding: 1rem 2rem;
            border-bottom: 1px solid #334155;
        }}

        .header h1 {{
            font-size: 1.5rem;
            color: #a855f7;
        }}

        .controls {{
            background: #1e293b;
            padding: 0.75rem 2rem;
            border-bottom: 1px solid #334155;
        }}

        .toggle-btn {{
            padding: 0.5rem 1rem;
            border-radius: 6px;
            border: 1px solid #475569;
            background: transparent;
            color: #94a3b8;
            cursor: pointer;
            margin-right: 0.5rem;
        }}

        .toggle-btn.active {{
            background: #9333ea;
            color: white;
            border-color: transparent;
        }}

        /* CRITICAL: This is the key difference from the failing version */
        .map-container {{
            flex: 1;
            display: flex;
            min-height: 0;
            /* CRITICAL for flex shrinking */
        }}

        #map {{
            flex: 1;
            /* NO height: 100% - let flex handle it */
        }}

        .sidebar {{
            width: 280px;
            background: #1e293b;
            padding: 1rem;
            border-left: 1px solid #334155;
        }}
    </style>
</head>

<body>
    <div class="header">
        <h1>📍 {title} (Minimal Debug)</h1>
    </div>
    <div class="controls">
        <button class="toggle-btn active">🎯 Opportunity Score</button>
        <button class="toggle-btn">📍 Leads</button>
    </div>
    <div class="map-container">
        <div id="map"></div>
        <div class="sidebar">
            <h3 style="color:#f1f5f9">Debug Panel</h3>
            <p style="color:#94a3b8;margin-top:1rem">
                Leads: {len(places)}<br>
                Zones: {len(zcta_metrics)}
            </p>
        </div>
    </div>

    <script>
        console.log('Initializing minimal debug map...');

        // Initialize map with OpenStreetMap tiles (known working)
        var map = L.map('map').setView([{center_lat}, {center_lng}], 7);
        console.log('Map object created');

        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; OpenStreetMap contributors'
        }}).addTo(map);
        console.log('Tile layer added');

        // Add lead markers
        const leads = {places_json};
        
        leads.forEach(function(d) {{
            if (d.latitude && d.longitude) {{
                 L.marker([d.latitude, d.longitude])
                    .addTo(map)
                    .bindPopup(d.name || 'Unknown');
            }}
        }});
        console.log('Markers added');

        // Force size recalculation
        setTimeout(function () {{
            map.invalidateSize();
            console.log('Map size invalidated');
        }}, 100);
    </script>
</body>
</html>
"""
    return html

def export_opportunity_map(
    output_file: Path = DEFAULT_OUTPUT,
    no_leads: bool = False,
    title: str = "MHP Opportunity Zones"
):
    """
    Main execution function to generate map file from DB or local data.
    """
    # === MOCK DATA FOR DEBUGGING ===
    print("Using MOCK DATA for debugging...")
    zcta_metrics = [
        {"zcta": "33825", "opportunity_score": 80},
        {"zcta": "33826", "opportunity_score": 65}
    ]
    
    geojson = {} # Unused in minimal
    
    places = [
        {"latitude": 27.65, "longitude": -81.50, "name": "Test Lead 1"},
        {"latitude": 27.62, "longitude": -81.52, "name": "Test Lead 2"},
        {"latitude": 27.58, "longitude": -81.60, "name": "Test Lead 3"}
    ]
    # === END MOCK DATA ===
    
    print(f"Generating map with {len(zcta_metrics)} zones and {len(places)} leads...")
    
    # CALL new minimal functional
    html = generate_minimal_map_html(places, zcta_metrics, geojson, title)
    
    # Save to file
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"Map saved to {output_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Export MHP Opportunity Map")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output HTML file path")
    parser.add_argument("--no-leads", action="store_true", help="Skip loading leads from DB")
    parser.add_argument("--title", type=str, default="MHP Opportunity Zones - Florida", help="Map title")
    
    args = parser.parse_args()
    
    export_opportunity_map(
        output_file=args.output,
        no_leads=args.no_leads,
        title=args.title
    )

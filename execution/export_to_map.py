"""
export_to_map.py
Generates an interactive HTML map from scored MHP/RV park leads.

Features:
- Color-coded markers by website score (red=low/hot prospect, green=high)
- Hover popups with property details
- Click to open Google Maps
- Score filter slider
- Responsive design

Usage:
    python execution/export_to_map.py
    python execution/export_to_map.py --input .tmp/scored_sites.json --output output/map.html
"""

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


# Paths
DEFAULT_INPUT = Path(".tmp/scored_sites.json")
DEFAULT_CSV_INPUT = Path("output")
DEFAULT_OUTPUT = Path("output/leads_map.html")


def get_score_color(score: int) -> str:
    """
    Return hex color based on score.
    Low scores = red (hot prospect)
    High scores = green (cold prospect)
    """
    if score <= 2:
        return "#dc2626"  # Red - hot
    elif score <= 4:
        return "#f97316"  # Orange
    elif score <= 6:
        return "#eab308"  # Yellow
    elif score <= 8:
        return "#22c55e"  # Light green
    else:
        return "#16a34a"  # Green - cold


def load_scored_data(input_path: Path) -> list[dict]:
    """Load scored sites from JSON."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return data.get("places", data) if isinstance(data, dict) else data


def load_from_csv(csv_path: Path) -> list[dict]:
    """Load leads from CSV output file."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    places = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse score
            try:
                score = int(row.get("site_score_1_10", 0))
            except (ValueError, TypeError):
                score = 0
            
            places.append({
                "name": row.get("name", "Unknown"),
                "address": row.get("address", ""),
                "city": row.get("city", ""),
                "state": row.get("state", ""),
                "phone": row.get("phone", ""),
                "website": row.get("website", ""),
                "maps_url": row.get("maps_url", ""),
                "google_rating": row.get("google_rating", ""),
                "review_count": row.get("review_count", ""),
                "site_score_1_10": score,
                "score_reasons": row.get("score_reasons", ""),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
            })
    
    return places


def generate_map_html(places: list[dict], title: str = "MHP/RV Park Leads") -> str:
    """
    Generate HTML with embedded Leaflet.js map.
    """
    # Filter places with valid coordinates
    valid_places = []
    for p in places:
        lat = p.get("latitude")
        lng = p.get("longitude")
        if lat and lng:
            try:
                valid_places.append({
                    **p,
                    "latitude": float(lat),
                    "longitude": float(lng),
                })
            except (ValueError, TypeError):
                continue
    
    if not valid_places:
        raise ValueError("No places with valid coordinates found")
    
    # Calculate map center
    avg_lat = sum(p["latitude"] for p in valid_places) / len(valid_places)
    avg_lng = sum(p["longitude"] for p in valid_places) / len(valid_places)
    
    # Prepare markers data as JSON
    markers_data = []
    for p in valid_places:
        score = p.get("site_score_1_10", 0) or 0
        markers_data.append({
            "lat": p["latitude"],
            "lng": p["longitude"],
            "name": p.get("name", "Unknown"),
            "address": p.get("address", ""),
            "phone": p.get("phone", ""),
            "website": p.get("website", ""),
            "mapsUrl": p.get("maps_url", ""),
            "rating": p.get("google_rating", ""),
            "reviews": p.get("review_count", ""),
            "score": score,
            "reasons": p.get("score_reasons", ""),
            "color": get_score_color(score),
        })
    
    markers_json = json.dumps(markers_data, ensure_ascii=False)
    
    # Count by score tier
    score_counts = {"hot": 0, "warm": 0, "cold": 0}
    for m in markers_data:
        if m["score"] <= 3:
            score_counts["hot"] += 1
        elif m["score"] <= 6:
            score_counts["warm"] += 1
        else:
            score_counts["cold"] += 1
    
    html = f'''<!DOCTYPE html>
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
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
        }}
        .header {{
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            padding: 1rem 2rem;
            border-bottom: 1px solid #334155;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }}
        .header h1 {{
            font-size: 1.5rem;
            font-weight: 600;
            background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        .stats {{
            display: flex;
            gap: 1.5rem;
        }}
        .stat {{
            text-align: center;
        }}
        .stat-value {{
            font-size: 1.5rem;
            font-weight: 700;
        }}
        .stat-label {{
            font-size: 0.75rem;
            color: #94a3b8;
            text-transform: uppercase;
        }}
        .stat.hot .stat-value {{ color: #f87171; }}
        .stat.warm .stat-value {{ color: #fbbf24; }}
        .stat.cold .stat-value {{ color: #4ade80; }}
        .controls {{
            background: #1e293b;
            padding: 1rem 2rem;
            display: flex;
            align-items: center;
            gap: 1rem;
            flex-wrap: wrap;
            border-bottom: 1px solid #334155;
        }}
        .controls label {{
            font-size: 0.875rem;
            color: #94a3b8;
        }}
        .filter-group {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        input[type="range"] {{
            width: 200px;
            accent-color: #60a5fa;
        }}
        .score-display {{
            background: #334155;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            min-width: 80px;
            text-align: center;
        }}
        .legend {{
            display: flex;
            gap: 1rem;
            margin-left: auto;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 0.75rem;
            color: #94a3b8;
        }}
        .legend-dot {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }}
        #map {{
            height: calc(100vh - 130px);
            width: 100%;
        }}
        .leaflet-popup-content-wrapper {{
            background: #1e293b;
            color: #e2e8f0;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.5);
        }}
        .leaflet-popup-tip {{
            background: #1e293b;
        }}
        .popup-content {{
            min-width: 250px;
            padding: 0.5rem;
        }}
        .popup-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 0.75rem;
        }}
        .popup-name {{
            font-size: 1rem;
            font-weight: 600;
            color: #f1f5f9;
            margin-right: 0.5rem;
        }}
        .popup-score {{
            padding: 0.25rem 0.5rem;
            border-radius: 6px;
            font-size: 0.875rem;
            font-weight: 600;
            white-space: nowrap;
        }}
        .popup-row {{
            display: flex;
            margin-bottom: 0.5rem;
            font-size: 0.875rem;
        }}
        .popup-label {{
            color: #64748b;
            min-width: 70px;
        }}
        .popup-value {{
            color: #cbd5e1;
        }}
        .popup-value a {{
            color: #60a5fa;
            text-decoration: none;
        }}
        .popup-value a:hover {{
            text-decoration: underline;
        }}
        .popup-reasons {{
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid #334155;
            font-size: 0.8rem;
            color: #94a3b8;
            font-style: italic;
        }}
        .popup-actions {{
            margin-top: 0.75rem;
            display: flex;
            gap: 0.5rem;
        }}
        .popup-btn {{
            flex: 1;
            padding: 0.5rem;
            border-radius: 6px;
            text-decoration: none;
            text-align: center;
            font-size: 0.8rem;
            font-weight: 500;
            transition: all 0.2s;
        }}
        .popup-btn.maps {{
            background: #3b82f6;
            color: white;
        }}
        .popup-btn.maps:hover {{
            background: #2563eb;
        }}
        .popup-btn.website {{
            background: #334155;
            color: #e2e8f0;
        }}
        .popup-btn.website:hover {{
            background: #475569;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🏕️ {title}</h1>
        <div class="stats">
            <div class="stat hot">
                <div class="stat-value">{score_counts["hot"]}</div>
                <div class="stat-label">Hot (≤3)</div>
            </div>
            <div class="stat warm">
                <div class="stat-value">{score_counts["warm"]}</div>
                <div class="stat-label">Warm (4-6)</div>
            </div>
            <div class="stat cold">
                <div class="stat-value">{score_counts["cold"]}</div>
                <div class="stat-label">Cold (7+)</div>
            </div>
        </div>
    </div>
    <div class="controls">
        <div class="filter-group">
            <label>Max Score:</label>
            <input type="range" id="scoreFilter" min="1" max="10" value="10">
            <span class="score-display" id="scoreValue">≤ 10</span>
        </div>
        <div class="legend">
            <div class="legend-item">
                <div class="legend-dot" style="background: #dc2626;"></div>
                <span>1-2 (Hot)</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background: #f97316;"></div>
                <span>3-4</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background: #eab308;"></div>
                <span>5-6</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background: #22c55e;"></div>
                <span>7-8</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background: #16a34a;"></div>
                <span>9-10 (Cold)</span>
            </div>
        </div>
    </div>
    <div id="map"></div>
    
    <script>
        const markersData = {markers_json};
        
        // Initialize map
        const map = L.map('map').setView([{avg_lat}, {avg_lng}], 8);
        
        // Add dark tile layer
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
            maxZoom: 19
        }}).addTo(map);
        
        // Store markers for filtering
        const markers = [];
        
        // Create markers
        markersData.forEach(data => {{
            const marker = L.circleMarker([data.lat, data.lng], {{
                radius: 10,
                fillColor: data.color,
                color: '#ffffff',
                weight: 2,
                opacity: 1,
                fillOpacity: 0.8
            }});
            
            marker.score = data.score;
            
            // Build popup content
            let popupHtml = `
                <div class="popup-content">
                    <div class="popup-header">
                        <span class="popup-name">${{data.name}}</span>
                        <span class="popup-score" style="background: ${{data.color}}; color: white;">
                            ${{data.score}}/10
                        </span>
                    </div>
                    <div class="popup-row">
                        <span class="popup-label">Address:</span>
                        <span class="popup-value">${{data.address}}</span>
                    </div>
            `;
            
            if (data.phone) {{
                popupHtml += `
                    <div class="popup-row">
                        <span class="popup-label">Phone:</span>
                        <span class="popup-value"><a href="tel:${{data.phone}}">${{data.phone}}</a></span>
                    </div>
                `;
            }}
            
            if (data.rating) {{
                popupHtml += `
                    <div class="popup-row">
                        <span class="popup-label">Rating:</span>
                        <span class="popup-value">⭐ ${{data.rating}} (${{data.reviews}} reviews)</span>
                    </div>
                `;
            }}
            
            if (data.reasons) {{
                popupHtml += `<div class="popup-reasons">${{data.reasons}}</div>`;
            }}
            
            popupHtml += `<div class="popup-actions">`;
            if (data.mapsUrl) {{
                popupHtml += `<a href="${{data.mapsUrl}}" target="_blank" class="popup-btn maps">📍 Google Maps</a>`;
            }}
            if (data.website) {{
                popupHtml += `<a href="${{data.website}}" target="_blank" class="popup-btn website">🌐 Website</a>`;
            }}
            popupHtml += `</div></div>`;
            
            marker.bindPopup(popupHtml, {{ maxWidth: 350 }});
            marker.addTo(map);
            markers.push(marker);
        }});
        
        // Score filter
        const scoreFilter = document.getElementById('scoreFilter');
        const scoreValue = document.getElementById('scoreValue');
        
        scoreFilter.addEventListener('input', function() {{
            const maxScore = parseInt(this.value);
            scoreValue.textContent = '≤ ' + maxScore;
            
            markers.forEach(marker => {{
                if (marker.score <= maxScore) {{
                    marker.addTo(map);
                }} else {{
                    marker.remove();
                }}
            }});
        }});
        
        // Fit bounds to markers
        if (markers.length > 0) {{
            const group = L.featureGroup(markers);
            map.fitBounds(group.getBounds().pad(0.1));
        }}
    </script>
</body>
</html>'''
    
    return html


def export_to_map(
    places: list[dict],
    output_path: Path = DEFAULT_OUTPUT,
    title: str = "MHP/RV Park Leads"
) -> Path:
    """
    Export places to an interactive HTML map.
    
    Returns the path to the generated HTML file.
    """
    output_path.parent.mkdir(exist_ok=True)
    
    html = generate_map_html(places, title)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"Map exported to: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate interactive HTML map from scored leads"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input JSON file (scored_sites.json) or CSV file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT),
        help="Output HTML file path",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="MHP/RV Park Leads",
        help="Map title",
    )
    
    args = parser.parse_args()
    
    # Determine input source
    if args.input:
        input_path = Path(args.input)
        if input_path.suffix == ".csv":
            places = load_from_csv(input_path)
        else:
            places = load_scored_data(input_path)
    elif DEFAULT_INPUT.exists():
        places = load_scored_data(DEFAULT_INPUT)
    else:
        # Try to find most recent CSV
        csv_files = list(DEFAULT_CSV_INPUT.glob("leads_*.csv"))
        if csv_files:
            latest = max(csv_files, key=lambda f: f.stat().st_mtime)
            print(f"Using latest CSV: {latest}")
            places = load_from_csv(latest)
        else:
            print("ERROR: No input file found")
            print("Run the pipeline first or specify --input")
            return
    
    output_path = Path(args.output)
    export_to_map(places, output_path, args.title)
    
    # Print summary
    scores = [p.get("site_score_1_10", 0) for p in places]
    hot = sum(1 for s in scores if s <= 3)
    print(f"\nTotal properties: {len(places)}")
    print(f"Hot prospects (score ≤3): {hot}")
    print(f"\nOpen in browser: file://{output_path.absolute()}")


if __name__ == "__main__":
    main()

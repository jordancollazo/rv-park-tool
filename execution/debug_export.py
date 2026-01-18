"""
export_opportunity_map.py

Generates an interactive HTML map with choropleth layers showing:
- Population growth rate (red gradient)
- Housing affordability (green gradient)  
- Opportunity score composite (purple gradient)
- MHP/RV park lead markers (existing functionality)

Features:
- Toggle controls for each layer
- Bivariate mode when multiple layers active
- Hover tooltips with zip code metrics
- Hot zone highlighting (top 20% opportunity)
- Score filter slider for leads

Usage:
    python execution/export_opportunity_map.py
    python execution/export_opportunity_map.py --no-leads  # Choropleth only
    python execution/export_opportunity_map.py --output output/opportunity_map.html
"""

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

# Import existing map functionality
try:
    from export_to_map import load_scored_data, load_from_csv, get_score_color
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from export_to_map import load_scored_data, load_from_csv, get_score_color


# Paths
DEFAULT_OUTPUT = Path("output/opportunity_map.html")
GEOJSON_PATH = Path(".tmp/florida_zcta_boundaries.geojson")
DB_PATH = Path("data/leads.db")


def load_zcta_metrics() -> list[dict]:
    """Load ZCTA metrics from database."""
    if not DB_PATH.exists():
        print("WARNING: Database not found, no ZCTA metrics available")
        return []
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT zcta, name, population_growth_rate, price_to_income_ratio,
                   mobile_home_percentage, vacancy_rate, opportunity_score,
                   opportunity_rank, median_home_value, median_household_income,
                   population_2023, median_age, senior_percentage, rent_burden,
                   income_growth_rate, displacement_score, path_of_progress_score,
                   snowbird_score, slumlord_rehab_score, exurb_score
            FROM zcta_metrics
            WHERE opportunity_score IS NOT NULL
            ORDER BY opportunity_score DESC
        """)
        
        metrics = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return metrics
        
    except sqlite3.Error as e:
        print(f"WARNING: Could not load ZCTA metrics: {e}")
        conn.close()
        return []


def load_geojson_boundaries() -> dict | None:
    """Load ZCTA boundary GeoJSON."""
    if not GEOJSON_PATH.exists():
        print(f"WARNING: GeoJSON not found at {GEOJSON_PATH}")
        print("Run: python execution/fetch_zcta_boundaries.py")
        return None
    
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_growth_color(growth_rate: float | None) -> str:
    """
    Return color for population growth rate.
    Red gradient: light pink (low/negative) to dark red (high growth).
    """
    if growth_rate is None:
        return "#808080"  # Gray for missing data
    
    if growth_rate <= -5:
        return "#fecaca"  # Light red (decline)
    elif growth_rate <= 0:
        return "#fca5a5"
    elif growth_rate <= 5:
        return "#f87171"
    elif growth_rate <= 10:
        return "#ef4444"
    elif growth_rate <= 15:
        return "#dc2626"
    else:
        return "#991b1b"  # Dark red (high growth)


def get_affordability_color(pti: float | None) -> str:
    """
    Return color for price-to-income ratio.
    Green gradient: dark green (affordable) to light green (expensive).
    Inverted because LOW PTI = affordable = good.
    """
    if pti is None:
        return "#808080"  # Gray for missing data
    
    if pti <= 2.5:
        return "#14532d"  # Dark green (very affordable)
    elif pti <= 3.5:
        return "#166534"
    elif pti <= 4.5:
        return "#15803d"
    elif pti <= 5.5:
        return "#22c55e"
    elif pti <= 6.5:
        return "#4ade80"
    else:
        return "#bbf7d0"  # Light green (expensive)


def get_opportunity_color(score: float | None) -> str:
    """
    Return color for opportunity score.
    Purple gradient: light (low opportunity) to dark (high opportunity).
    """
    if score is None:
        return "#808080"  # Gray for missing data
    
    if score <= 20:
        return "#f3e8ff"  # Very light purple
    elif score <= 35:
        return "#e9d5ff"
    elif score <= 45:
        return "#d8b4fe"
    elif score <= 55:
        return "#c084fc"
    elif score <= 65:
        return "#a855f7"
    elif score <= 75:
        return "#9333ea"
    else:
        return "#7c3aed"  # Dark purple (high opportunity)


def get_displacement_color(score: float | None) -> str:
    """Red gradient: light (low displacement pressure) to dark (high pressure)."""
    if score is None:
        return "#808080"
    if score <= 20:
        return "#fecaca"
    elif score <= 40:
        return "#fca5a5"
    elif score <= 60:
        return "#f87171"
    elif score <= 80:
        return "#ef4444"
    else:
        return "#dc2626"


def get_progress_color(score: float | None) -> str:
    """Green gradient: light (stagnant) to dark (high income growth + affordable)."""
    if score is None:
        return "#808080"
    if score <= 20:
        return "#dcfce7"
    elif score <= 40:
        return "#86efac"
    elif score <= 60:
        return "#4ade80"
    elif score <= 80:
        return "#22c55e"
    else:
        return "#16a34a"


def get_snowbird_color(score: float | None) -> str:
    """Blue gradient: light (young) to dark (retiree haven)."""
    if score is None:
        return "#808080"
    if score <= 20:
        return "#dbeafe"
    elif score <= 40:
        return "#93c5fd"
    elif score <= 60:
        return "#60a5fa"
    elif score <= 80:
        return "#3b82f6"
    else:
        return "#2563eb"


def get_slumlord_color(score: float | None) -> str:
    """Orange gradient: light (stable) to dark (high distress/turnaround potential)."""
    if score is None:
        return "#808080"
    if score <= 20:
        return "#fed7aa"
    elif score <= 40:
        return "#fdba74"
    elif score <= 60:
        return "#fb923c"
    elif score <= 80:
        return "#f97316"
    else:
        return "#ea580c"


def get_exurb_color(score: float | None) -> str:
    """Magenta gradient: light (not exurb) to dark (remote work exurb sweet spot)."""
    if score is None:
        return "#808080"
    if score <= 20:
        return "#fce7f3"
    elif score <= 40:
        return "#f9a8d4"
    elif score <= 60:
        return "#f472b6"
    elif score <= 80:
        return "#ec4899"
    else:
        return "#db2777"


def generate_opportunity_map_html(
    places: list[dict] | None,
    zcta_metrics: list[dict],
    geojson: dict | None,
    title: str = "MHP Opportunity Zones"
) -> str:
    """
    Generate HTML with choropleth layers and lead markers.
    """
    # Process lead markers (if provided)
    markers_data = []
    if places:
        for p in places:
            lat = p.get("latitude")
            lng = p.get("longitude")
            if lat and lng:
                try:
                    score = p.get("site_score_1_10", 0) or 0
                    distress = p.get("distress_score", 0) or 0
                    
                    # Color by distress score (red=hot target) or site score
                    if distress >= 8:
                        marker_color = "#ef4444"  # Red - critical distress
                    elif distress >= 6:
                        marker_color = "#f97316"  # Orange - high distress  
                    else:
                        marker_color = get_score_color(score)
                    
                    markers_data.append({
                        "lat": float(lat),
                        "lng": float(lng),
                        "name": p.get("name", "Unknown"),
                        "address": p.get("address", ""),
                        "city": p.get("city", ""),
                        "phone": p.get("phone", ""),
                        "website": p.get("website", ""),
                        "mapsUrl": p.get("maps_url", ""),
                        "rating": p.get("google_rating", ""),
                        "reviews": p.get("review_count", ""),
                        "score": score,
                        "distress": distress,
                        "reasons": p.get("score_reasons", ""),
                        "color": marker_color,
                    })
                except (ValueError, TypeError):
                    continue
    
    markers_json = json.dumps(markers_data, ensure_ascii=True).replace('<', '\\u003c').replace('>', '\\u003e')
    
    # Process choropleth features
    choropleth_features = []
    if geojson:
        for feature in geojson.get("features", []):
            props = feature.get("properties", {})
            zcta = props.get("zcta")
            
            choropleth_features.append({
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": {
                    "zcta": zcta,
                    "county": props.get("county", ""),
                    "growth": props.get("population_growth_rate"),
                    "pti": props.get("price_to_income_ratio"),
                    "score": props.get("opportunity_score"),
                    "rank": props.get("opportunity_rank"),
                    "mhPct": props.get("mobile_home_percentage"),
                    "vacancy": props.get("vacancy_rate"),
                    "homeValue": props.get("median_home_value"),
                    "income": props.get("median_household_income"),
                    "population": props.get("population_2023") or props.get("population_2022"),
                    "medianAge": props.get("median_age"),
                    "seniorPct": props.get("senior_percentage"),
                    "rentBurden": props.get("rent_burden"),
                    "incomeGrowth": props.get("income_growth_rate"),
                    # Thesis scores
                    "displacementScore": props.get("displacement_score"),
                    "progressScore": props.get("path_of_progress_score"),
                    "snowbirdScore": props.get("snowbird_score"),
                    "slumlordScore": props.get("slumlord_rehab_score"),
                    "exurbScore": props.get("exurb_score"),
                    # Normalized scores for filtering
                    "growthScore": max(0, min(100, (props.get("population_growth_rate", 0) + 5) / 20 * 100)) if props.get("population_growth_rate") is not None else 0,
                    "affordScore": max(0, min(100, (7.5 - props.get("price_to_income_ratio", 7.5)) / 5 * 100)) if props.get("price_to_income_ratio") is not None else 0,
                    # Colors
                    "growthColor": get_growth_color(props.get("population_growth_rate")),
                    "affordColor": get_affordability_color(props.get("price_to_income_ratio")),
                    "scoreColor": get_opportunity_color(props.get("opportunity_score")),
                    "displacementColor": get_displacement_color(props.get("displacement_score")),
                    "progressColor": get_progress_color(props.get("path_of_progress_score")),
                    "snowbirdColor": get_snowbird_color(props.get("snowbird_score")),
                    "slumlordColor": get_slumlord_color(props.get("slumlord_rehab_score")),
                    "exurbColor": get_exurb_color(props.get("exurb_score")),
                }
            })
    
    # If we don't have GeoJSON but have metrics with geometry, build from DB
    elif zcta_metrics:
        for m in zcta_metrics:
            if m.get("geojson"):
                try:
                    geometry = json.loads(m["geojson"])
                    choropleth_features.append({
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": {
                            "zcta": m["zcta"],
                            "county": m.get("county", ""),
                            "growth": m.get("population_growth_rate"),
                            "pti": m.get("price_to_income_ratio"),
                            "score": m.get("opportunity_score"),
                            "rank": m.get("opportunity_rank"),
                            "mhPct": m.get("mobile_home_percentage"),
                            "vacancy": m.get("vacancy_rate"),
                            "homeValue": m.get("median_home_value"),
                            "income": m.get("median_household_income"),
                            "population": m.get("population_2022"),
                            "growthScore": max(0, min(100, (m.get("population_growth_rate", 0) + 5) / 20 * 100)) if m.get("population_growth_rate") is not None else 0,
                            "affordScore": max(0, min(100, (7.5 - m.get("price_to_income_ratio", 7.5)) / 5 * 100)) if m.get("price_to_income_ratio") is not None else 0,
                            "growthColor": get_growth_color(m.get("population_growth_rate")),
                            "affordColor": get_affordability_color(m.get("price_to_income_ratio")),
                            "scoreColor": get_opportunity_color(m.get("opportunity_score")),
                        }
                    })
                except (json.JSONDecodeError, TypeError):
                    continue
    
    choropleth_json = json.dumps({
        "type": "FeatureCollection",
        "features": choropleth_features
    }, ensure_ascii=True).replace('<', '\\u003c').replace('>', '\\u003e')
    
    # Calculate map center (Florida)
    center_lat = 27.6648
    center_lng = -81.5158
    
    if markers_data:
        center_lat = sum(m["lat"] for m in markers_data) / len(markers_data)
        center_lng = sum(m["lng"] for m in markers_data) / len(markers_data)
    
    # Stats
    total_zctas = len(choropleth_features)
    total_leads = len(markers_data)
    top_zones = len([m for m in zcta_metrics if m.get("opportunity_score", 0) >= 60])
    
    # Generate HTML
    html = rf'''<!DOCTYPE html>
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
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            height: 100%;
            display: flex;
            flex-direction: column;
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
            background: linear-gradient(135deg, #a855f7 0%, #ec4899 100%);
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
            font-size: 1.25rem;
            font-weight: 700;
        }}
        .stat-label {{
            font-size: 0.7rem;
            color: #94a3b8;
            text-transform: uppercase;
        }}
        .stat.purple .stat-value {{ color: #a855f7; }}
        .stat.blue .stat-value {{ color: #60a5fa; }}
        .stat.green .stat-value {{ color: #4ade80; }}
        
        .controls {{
            background: #1e293b;
            padding: 0.75rem 2rem;
            display: flex;
            align-items: center;
            gap: 1.5rem;
            flex-wrap: wrap;
            border-bottom: 1px solid #334155;
        }}
        .layer-toggles {{
            display: flex;
            gap: 0.75rem;
        }}
        .toggle-btn {{
            padding: 0.5rem 1rem;
            border-radius: 6px;
            border: 1px solid #475569;
            background: transparent;
            color: #94a3b8;
            cursor: pointer;
            font-size: 0.8rem;
            transition: all 0.2s;
        }}
        .toggle-btn.active {{
            border-color: transparent;
            color: white;
        }}
        .toggle-btn.growth.active {{ background: #dc2626; }}
        .toggle-btn.afford.active {{ background: #16a34a; }}
        .toggle-btn.opportunity.active {{ background: #9333ea; }}
        .toggle-btn.leads.active {{ background: #3b82f6; }}
        .toggle-btn.filter-leads.active {{ background: #f59e0b; color: white; border-color: transparent; }}
        .toggle-btn.displacement.active {{ background: #dc2626; }}
        .toggle-btn.progress.active {{ background: #16a34a; }}
        .toggle-btn.snowbird.active {{ background: #2563eb; }}
        .toggle-btn.slumlord.active {{ background: #ea580c; }}
        .toggle-btn.exurb.active {{ background: #d946ef; }}
        
        .filter-group {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-left: auto;
        }}
        .filter-group label {{
            font-size: 0.8rem;
            color: #94a3b8;
        }}
        input[type="range"] {{
            width: 120px;
            accent-color: #a855f7;
        }}
        .score-display {{
            background: #334155;
            padding: 0.25rem 0.5rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            min-width: 60px;
            text-align: center;
        }}
        
        .legends {{
            display: flex;
            gap: 2rem;
            padding: 0.5rem 2rem;
            background: #1e293b;
            border-bottom: 1px solid #334155;
            font-size: 0.7rem;
        }}
        .legend {{
            display: flex;
            align-items: center;
            gap: 0.25rem;
        }}
        .legend-title {{
            color: #94a3b8;
            margin-right: 0.5rem;
        }}
        .legend-bar {{
            display: flex;
            height: 12px;
            border-radius: 2px;
            overflow: hidden;
        }}
        .legend-bar span {{
            width: 20px;
        }}
        .legend-labels {{
            display: flex;
            justify-content: space-between;
            width: 100px;
            color: #64748b;
            font-size: 0.65rem;
        }}
        
        .map-container {{
            display: flex;
            flex: 1;
            min-height: 500px; /* Explicit fallback height */
        }}
        #map {{
            flex: 1;
            min-height: 500px; /* Explicit fallback height */
        }}
        #thesis-info-panel {{
            width: 280px;
            background: #1e293b;
            border-left: 1px solid #334155;
            padding: 1rem;
            overflow-y: auto;
            font-size: 0.85rem;
        }}
        .thesis-panel-title {{
            font-size: 1.1rem;
            font-weight: 600;
            color: #f1f5f9;
            margin-bottom: 0.75rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid #475569;
        }}
        .thesis-section {{
            margin-bottom: 1rem;
        }}
        .thesis-section-header {{
            color: #94a3b8;
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 0.25rem;
        }}
        .thesis-section-content {{
            color: #cbd5e1;
            line-height: 1.5;
        }}
        .thesis-formula {{
            background: #0f172a;
            border-radius: 6px;
            padding: 0.5rem;
            font-family: monospace;
            font-size: 0.75rem;
            color: #a5b4fc;
        }}
        .thesis-formula-line {{
            margin: 0.25rem 0;
        }}
        .thesis-usecase {{
            background: linear-gradient(135deg, #1e3a5f 0%, #1e293b 100%);
            border-radius: 6px;
            padding: 0.5rem;
            border-left: 3px solid #3b82f6;
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
            min-width: 220px;
            padding: 0.25rem;
        }}
        .popup-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 0.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid #334155;
        }}
        .popup-zcta {{
            font-size: 1.1rem;
            font-weight: 600;
            color: #f1f5f9;
        }}
        .popup-score-badge {{
            padding: 0.25rem 0.5rem;
            border-radius: 6px;
            font-size: 0.8rem;
            font-weight: 600;
            color: white;
        }}
        .popup-metrics {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.5rem;
            font-size: 0.8rem;
        }}
        .popup-metric {{
            display: flex;
            flex-direction: column;
        }}
        .popup-metric-label {{
            color: #64748b;
            font-size: 0.7rem;
        }}
        .popup-metric-value {{
            color: #e2e8f0;
            font-weight: 500;
        }}
        .popup-metric-value.positive {{ color: #4ade80; }}
        .popup-metric-value.negative {{ color: #f87171; }}
        
        /* Lead popup styles */
        .popup-name {{
            font-size: 1rem;
            font-weight: 600;
            color: #f1f5f9;
        }}
        .popup-row {{
            display: flex;
            margin-bottom: 0.4rem;
            font-size: 0.85rem;
        }}
        .popup-label {{
            color: #64748b;
            min-width: 65px;
        }}
        .popup-value {{
            color: #cbd5e1;
        }}
        .popup-value a {{
            color: #60a5fa;
            text-decoration: none;
        }}
        .popup-actions {{
            margin-top: 0.5rem;
            display: flex;
            gap: 0.5rem;
        }}
        .popup-btn {{
            flex: 1;
            padding: 0.4rem;
            border-radius: 6px;
            text-decoration: none;
            text-align: center;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .popup-btn.maps {{ background: #3b82f6; color: white; }}
        .popup-btn.website {{ background: #334155; color: #e2e8f0; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📍 {title}</h1>
        <div class="stats">
            <div class="stat purple">
                <div class="stat-value" id="stat-hot-zones">{top_zones}</div>
                <div class="stat-label" id="label-hot-zones">Hot Zones (60+)</div>
            </div>
            <div class="stat blue">
                <div class="stat-value" id="stat-zips">{total_zctas}</div>
                <div class="stat-label">Zip Codes</div>
            </div>
            <div class="stat green">
                <div class="stat-value" id="stat-leads">{total_leads}</div>
                <div class="stat-label">Leads</div>
            </div>
        </div>
    </div>
    
    <div class="controls">
        <div class="layer-toggles">
            <button class="toggle-btn opportunity active" onclick="toggleLayer('opportunity')">
                🎯 Opportunity Score
            </button>
            <button class="toggle-btn growth" onclick="toggleLayer('growth')">
                📈 Population Growth
            </button>
            <button class="toggle-btn afford" onclick="toggleLayer('afford')">
                🏠 Affordability
            </button>
            <button class="toggle-btn displacement" onclick="toggleLayer('displacement')">
                🔴 Displacement Play
            </button>
            <button class="toggle-btn progress" onclick="toggleLayer('progress')">
                💰 Path of Progress
            </button>
            <button class="toggle-btn snowbird" onclick="toggleLayer('snowbird')">
                🌴 Snowbird Haven
            </button>
            <button class="toggle-btn slumlord" onclick="toggleLayer('slumlord')">
                🔧 Slumlord Rehab
            </button>
            <button class="toggle-btn exurb" onclick="toggleLayer('exurb')">
                🏡 Remote Work Exurb
            </button>
            <button class="toggle-btn leads active" onclick="toggleLayer('leads')">
                📍 Leads
            </button>
            <button class="toggle-btn filter-leads" onclick="toggleLayer('filterLeads')" title="Only show leads inside visible opportunity zones">
                🛡️ Filter Leads in Zones
            </button>
        </div>
        <div class="filter-group">
            <label>Min Score:</label>
            <input type="range" id="minScoreFilter" min="0" max="100" value="0">
            <span class="score-display" id="minScoreValue">≥ 0</span>
        </div>
    </div>
    
    <div class="legends">
        <div class="legend" id="legend-opportunity">
            <span class="legend-title">Opportunity:</span>
            <div class="legend-bar">
                <span style="background:#f3e8ff"></span>
                <span style="background:#d8b4fe"></span>
                <span style="background:#a855f7"></span>
                <span style="background:#7c3aed"></span>
            </div>
            <div class="legend-labels"><span>Low</span><span>High</span></div>
        </div>
        <div class="legend" id="legend-growth" style="display:none;">
            <span class="legend-title">Growth:</span>
            <div class="legend-bar">
                <span style="background:#fecaca"></span>
                <span style="background:#f87171"></span>
                <span style="background:#dc2626"></span>
                <span style="background:#991b1b"></span>
            </div>
            <div class="legend-labels"><span>-5%</span><span>+15%</span></div>
        </div>
        <div class="legend" id="legend-afford" style="display:none;">
            <span class="legend-title">Affordability:</span>
            <div class="legend-bar">
                <span style="background:#14532d"></span>
                <span style="background:#22c55e"></span>
                <span style="background:#4ade80"></span>
                <span style="background:#bbf7d0"></span>
            </div>
            <div class="legend-labels"><span>Cheap</span><span>Expensive</span></div>
        </div>
        <div class="legend" id="legend-displacement" style="display:none;">
            <span class="legend-title">Displacement:</span>
            <div class="legend-bar">
                <span style="background:#fee2e2"></span>
                <span style="background:#fca5a5"></span>
                <span style="background:#ef4444"></span>
                <span style="background:#b91c1c"></span>
            </div>
            <div class="legend-labels"><span>Low</span><span>High</span></div>
        </div>
        <div class="legend" id="legend-progress" style="display:none;">
            <span class="legend-title">Path of Progress:</span>
            <div class="legend-bar">
                <span style="background:#dcfce7"></span>
                <span style="background:#86efac"></span>
                <span style="background:#22c55e"></span>
                <span style="background:#15803d"></span>
            </div>
            <div class="legend-labels"><span>Low</span><span>High</span></div>
        </div>
        <div class="legend" id="legend-snowbird" style="display:none;">
            <span class="legend-title">Snowbird Haven:</span>
            <div class="legend-bar">
                <span style="background:#dbeafe"></span>
                <span style="background:#93c5fd"></span>
                <span style="background:#3b82f6"></span>
                <span style="background:#1d4ed8"></span>
            </div>
            <div class="legend-labels"><span>Low</span><span>High</span></div>
        </div>
        <div class="legend" id="legend-slumlord" style="display:none;">
            <span class="legend-title">Slumlord Rehab:</span>
            <div class="legend-bar">
                <span style="background:#ffedd5"></span>
                <span style="background:#fdba74"></span>
                <span style="background:#f97316"></span>
                <span style="background:#c2410c"></span>
            </div>
            <div class="legend-labels"><span>Low</span><span>High</span></div>
        </div>
        <div class="legend" id="legend-exurb" style="display:none;">
            <span class="legend-title">Remote Work Exurb:</span>
            <div class="legend-bar">
                <span style="background:#fae8ff"></span>
                <span style="background:#e879f9"></span>
                <span style="background:#d946ef"></span>
                <span style="background:#a21caf"></span>
            </div>
            <div class="legend-labels"><span>Low</span><span>High</span></div>
        </div>
    </div>
    
    <div class="map-container">
        <div id="map"></div>
        <div id="thesis-info-panel">
            <div class="thesis-panel-title" id="thesis-title">🎯 Opportunity Score</div>
            <div class="thesis-section">
                <div class="thesis-section-header">What It Measures</div>
                <div class="thesis-section-content" id="thesis-description">
                    The "All-Rounder" score combining population growth, housing affordability, mobile home market presence, and healthy vacancy rates.
                </div>
            </div>
            <div class="thesis-section">
                <div class="thesis-section-header">How It's Scored</div>
                <div class="thesis-formula" id="thesis-formula">
                    <div class="thesis-formula-line">45% Population Growth</div>
                    <div class="thesis-formula-line">35% Affordability (Low PTI)</div>
                    <div class="thesis-formula-line">10% Mobile Home %</div>
                    <div class="thesis-formula-line">10% Vacancy Rate (5-10%)</div>
                </div>
            </div>
            <div class="thesis-section">
                <div class="thesis-section-header">Best For</div>
                <div class="thesis-usecase" id="thesis-usecase">
                    First-time investors or those seeking low-risk "Core+" assets with balanced growth and stability.
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Data
        const markersData = {markers_json};
        const choroplethData = {choropleth_json};
        
        // Layer state
        const layerState = {{
            opportunity: true,
            growth: false,
            afford: false,
            displacement: false,
            progress: false,
            snowbird: false,
            slumlord: false,
            exurb: false,
            leads: true,
            filterLeads: false
        }};
        
        // Thesis descriptions for info panel
        const thesisDescriptions = {{
            opportunity: {{
                title: '🎯 Opportunity Score',
                description: 'The "All-Rounder" score combining population growth, housing affordability, mobile home market presence, and healthy vacancy rates.',
                formula: [
                    '45% Population Growth',
                    '35% Affordability (Low PTI)',
                    '10% Mobile Home %',
                    '10% Vacancy Rate (5-10%)'
                ],
                usecase: 'First-time investors or those seeking low-risk "Core+" assets with balanced growth and stability.'
            }},
            growth: {{
                title: '📈 Population Growth',
                description: 'Identifies areas where people are moving. High growth = increasing demand for housing of all types, including affordable MH lots.',
                formula: [
                    '100% Population Change',
                    'Compares 2019 vs 2023',
                    '+20% growth = Score 100'
                ],
                usecase: '"Buy and Hold" strategy. If population grows, lot rents rise naturally over time.'
            }},
            afford: {{
                title: '🏠 Affordability',
                description: 'Finds markets where traditional housing is unaffordable, creating strong demand for MH alternatives.',
                formula: [
                    '100% Price-to-Income Ratio',
                    'PTI ≤ 2.5 = Very Affordable',
                    'PTI ≥ 6.5 = Expensive'
                ],
                usecase: '"Recession Proof" thesis. In expensive markets, MH is the only option—low turnover, high occupancy.'
            }},
            displacement: {{
                title: '🔴 Displacement Play',
                description: 'Identifies areas where renters are stretched thin—high rent burden + low vacancy = captive audience for affordable MH.',
                formula: [
                    '40% Rent Burden (>40% = stressed)',
                    '30% Low Vacancy (tight market)',
                    '30% Still Affordable (PTI < 6)'
                ],
                usecase: '"Value-Add Aggressive" strategy. Target areas with under-market rents where you can raise to market and still be cheapest option.'
            }},
            progress: {{
                title: '💰 Path of Progress',
                description: 'Finds the next boomtown—areas where incomes are rising but home prices haven\\'t caught up yet.',
                formula: [
                    '50% Income Growth Rate',
                    '50% Still Affordable (low PTI)'
                ],
                usecase: '"Equity Flip" strategy. Buy cheap, wait for gentrification, sell at higher cap rate in 5-7 years.'
            }},
            snowbird: {{
                title: '🌴 Snowbird Haven',
                description: 'Targets 55+ communities—areas with high median age and large senior populations (65+).',
                formula: [
                    '40% Median Age (55+ ideal)',
                    '60% Senior Percentage (30%+ = haven)'
                ],
                usecase: 'Senior parks are quieter, easier to manage, and have lower capex. Perfect for "hands-off" investors.'
            }},
            slumlord: {{
                title: '🔧 Slumlord Rehab',
                description: 'Finds distressed areas with turnaround potential—high vacancy, cheap prices, but existing MH market.',
                formula: [
                    '40% High Vacancy (10-25%)',
                    '30% Low Home Value (<$150k)',
                    '30% High Mobile Home % (>15%)'
                ],
                usecase: '"Heavy Lift" strategy. Buy mismanaged parks cheap, fix infrastructure, clean up tenants, double property value.'
            }},
            exurb: {{
                title: '🏡 Remote Work Exurb',
                description: 'The "Goldilocks Zone"—45-75 miles from metro. Close enough to commute, far enough to be affordable.',
                formula: [
                    '40% Distance Sweet Spot (45-75mi)',
                    '30% Affordability (PTI < 5)',
                    '30% Population Growth'
                ],
                usecase: '"Yield Maximizer" thesis. Big investors ignore these areas—less competition, better prices for quality parks.'
            }}
        }};
        
        function updateThesisPanel(thesis) {{
            const info = thesisDescriptions[thesis];
            if (!info) return;
            
            document.getElementById('thesis-title').textContent = info.title;
            document.getElementById('thesis-description').textContent = info.description;
            document.getElementById('thesis-formula').innerHTML = info.formula.map(f => 
                `<div class="thesis-formula-line">${{f}}</div>`
            ).join('');
            document.getElementById('thesis-usecase').textContent = info.usecase;
        }}
        
        // Thesis-to-metrics mapping for contextual popups
        const thesisMetrics = {{
            opportunity: {{
                name: '🎯 Opportunity Score',
                scoreKey: 'score',
                metrics: [
                    {{ key: 'growth', label: 'Pop Growth', format: 'percent' }},
                    {{ key: 'pti', label: 'Price/Income', format: 'ratio' }},
                    {{ key: 'mhPct', label: 'Mobile Home %', format: 'percent' }},
                    {{ key: 'vacancy', label: 'Vacancy', format: 'percent' }}
                ]
            }},
            growth: {{
                name: '📈 Population Growth',
                scoreKey: 'growthScore', // Assuming this is calculated or we use raw
                metrics: [
                    {{ key: 'growth', label: 'Pop Growth', format: 'percent' }}
                ]
            }},
            afford: {{
                name: '🏠 Affordability',
                scoreKey: 'affordScore',
                metrics: [
                    {{ key: 'pti', label: 'Price/Income', format: 'ratio' }},
                    {{ key: 'homeValue', label: 'Home Value', format: 'currency' }}
                ]
            }},
            displacement: {{
                name: '📉 Displacement Risk',
                scoreKey: 'displacementScore',
                metrics: [
                    {{ key: 'rentBurden', label: 'Rent Burden', format: 'percent' }},
                    {{ key: 'vacancy', label: 'Vacancy', format: 'percent' }},
                    {{ key: 'pti', label: 'Price/Income', format: 'ratio' }}
                ]
            }},
            progress: {{
                name: '🏘️ Path of Progress',
                scoreKey: 'progressScore',
                metrics: [
                    {{ key: 'incomeGrowth', label: 'Income Growth', format: 'percent' }},
                    {{ key: 'pti', label: 'Price/Income', format: 'ratio' }}
                ]
            }},
            snowbird: {{
                name: '🦩 Snowbird Haven',
                scoreKey: 'snowbirdScore',
                metrics: [
                    {{ key: 'medianAge', label: 'Median Age', format: 'number' }},
                    {{ key: 'seniorPct', label: 'Senior %', format: 'percent' }}
                ]
            }},
            slumlord: {{
                name: '🛠️ Slumlord Rehab',
                scoreKey: 'slumlordScore',
                metrics: [
                    {{ key: 'vacancy', label: 'Vacancy', format: 'percent' }},
                    {{ key: 'homeValue', label: 'Home Value', format: 'currency' }},
                    {{ key: 'mhPct', label: 'Mobile Home %', format: 'percent' }}
                ]
            }},
            exurb: {{
                name: '🏡 Remote Work Exurb',
                scoreKey: 'exurbScore',
                metrics: [
                    {{ key: 'distanceToMetro', label: 'Dist to Metro', format: 'miles' }},
                    {{ key: 'growth', label: 'Pop Growth', format: 'percent' }},
                    {{ key: 'pti', label: 'Price/Income', format: 'ratio' }}
                ]
            }}
        }};
        
        // Find ZCTA zone for a given lat/lng
        function findZoneForPoint(lat, lng) {{
            for (let feature of choroplethData.features) {{
                if (isPointInTarget(lat, lng, feature)) {{
                    return feature.properties;
                }}
            }}
            return null;
        }}
        
        // Format a metric value based on type
        function formatMetricValue(value, format) {{
            if (value === null || value === undefined) return 'N/A';
            switch (format) {{
                case 'percent':
                    return (value >= 0 ? '+' : '') + value.toFixed(1) + '%';
                case 'ratio':
                    return value.toFixed(1) + 'x';
                case 'currency':
                    return '$' + (value >= 1000000 ? (value/1000000).toFixed(1) + 'M' : 
                                  value >= 1000 ? (value/1000).toFixed(0) + 'K' : value);
                case 'miles':
                    return value.toFixed(0) + ' mi';
                case 'number':
                default:
                    return value.toFixed(1);
            }}
        }}
        
        // Build zone insights HTML for active active theses
        function buildZoneInsightsHtml(zoneProps) {{
            if (!zoneProps) return '';
            
            const activeLayers = Object.keys(thesisMetrics).filter(k => layerState[k]);
            if (activeLayers.length === 0) return '';
            
            let html = '<div style="border-top:1px solid #334155;margin-top:8px;padding-top:8px">';
            html += '<div style="font-weight:600;color:#94a3b8;font-size:0.7rem;text-transform:uppercase;margin-bottom:6px">Zone Insights (ZIP ' + zoneProps.zcta + ')</div>';
            
            activeLayers.forEach(layer => {{
                const thesis = thesisMetrics[layer];
                const score = zoneProps[thesis.scoreKey];
                const scoreDisplay = score !== null && score !== undefined ? score.toFixed(0) : 'N/A';
                
                html += '<div style="margin-bottom:8px">';
                html += '<div style="font-weight:600;color:#e2e8f0;font-size:0.8rem">' + thesis.name + ': ' + scoreDisplay + '</div>';
                
                thesis.metrics.forEach(m => {{
                    let val = zoneProps[m.key];
                    // Handle special key mapping for exurb distance
                    if (m.key === 'distanceToMetro') val = zoneProps['distance_to_nearest_metro'];
                    
                    html += '<div style="display:flex;justify-content:space-between;font-size:0.75rem;color:#94a3b8;padding-left:8px">';
                    html += '<span>' + m.label + ':</span>';
                    html += '<span style="color:#e2e8f0">' + formatMetricValue(val, m.format) + '</span>';
                    html += '</div>';
                }});
                html += '</div>';
            }});
            
            html += '</div>';
            return html;
        }}

        
        // Initialize map
        window.map = L.map('map').setView([{center_lat}, {center_lng}], 7);
        const map = window.map;
        
        // Create custom pane for lead markers with higher z-index than choropleth
        map.createPane('leadMarkers');
        map.getPane('leadMarkers').style.zIndex = 650;  // overlayPane is 400, shadowPane is 500
        
        // Tile layer - using OpenStreetMap (known working)
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; OpenStreetMap contributors',
            maxZoom: 19
        }}).addTo(map);
        
        // Force Leaflet to recalculate size after DOM is ready
        setTimeout(function() {{
            map.invalidateSize();
            console.log('Map size invalidated');
        }}, 100);
        
        // Choropleth layer
        let choroplethLayer = null;
        
        function getChoroplethStyle(feature) {{
            const props = feature.properties;
            
            // Collect active layer colors
            const colors = [];
            if (layerState.opportunity && props.scoreColor && props.scoreColor !== '#808080') {{
                colors.push(props.scoreColor);
            }}
            if (layerState.growth && props.growthColor && props.growthColor !== '#808080') {{
                colors.push(props.growthColor);
            }}
            if (layerState.afford && props.affordColor && props.affordColor !== '#808080') {{
                colors.push(props.affordColor);
            }}
            if (layerState.displacement && props.displacementColor && props.displacementColor !== '#808080') {{
                colors.push(props.displacementColor);
            }}
            if (layerState.progress && props.progressColor && props.progressColor !== '#808080') {{
                colors.push(props.progressColor);
            }}
            if (layerState.snowbird && props.snowbirdColor && props.snowbirdColor !== '#808080') {{
                colors.push(props.snowbirdColor);
            }}
            if (layerState.slumlord && props.slumlordColor && props.slumlordColor !== '#808080') {{
                colors.push(props.slumlordColor);
            }}
            if (layerState.exurb && props.exurbColor && props.exurbColor !== '#808080') {{
                colors.push(props.exurbColor);
            }}
            
            // Blend colors if multiple active, otherwise use single or default gray
            let fillColor = '#334155'; // Default dark blue-gray for "no data/low score"
            if (colors.length === 1) {{
                fillColor = colors[0];
            }} else if (colors.length > 1) {{
                fillColor = blendColors(colors);
            }}
            
            // Highlight top opportunity zones
            const isHotZone = props.score && props.score >= 60;
            
            return {{
                fillColor: fillColor,
                fillOpacity: 0.6,
                color: isHotZone ? '#fbbf24' : '#334155',
                weight: isHotZone ? 2 : 0.5,
                opacity: 1
            }};
        }}
        
        // Blend multiple hex colors by averaging RGB
        function blendColors(hexColors) {{
            let r = 0, g = 0, b = 0;
            hexColors.forEach(hex => {{
                const result = /^#?([a-f\\d]{{2}})([a-f\\d]{{2}})([a-f\\d]{{2}})$/i.exec(hex);
                if (result) {{
                    r += parseInt(result[1], 16);
                    g += parseInt(result[2], 16);
                    b += parseInt(result[3], 16);
                }}
            }});
            const n = hexColors.length;
            r = Math.round(r / n);
            g = Math.round(g / n);
            b = Math.round(b / n);
            return '#' + [r, g, b].map(x => x.toString(16).padStart(2, '0')).join('');
        }}
        
        function formatNumber(num) {{
            if (num === null || num === undefined) return 'N/A';
            if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
            if (num >= 1000) return (num / 1000).toFixed(0) + 'K';
            return num.toLocaleString();
        }}
        
        function formatPercent(num) {{
            if (num === null || num === undefined) return 'N/A';
            const sign = num >= 0 ? '+' : '';
            return sign + num.toFixed(1) + '%';
        }}
        
        function onEachFeature(feature, layer) {{
            const props = feature.properties;
            
            const growthClass = props.growth > 0 ? 'positive' : 'negative';
            const ptiLabel = props.pti ? (props.pti <= 4 ? 'Affordable' : props.pti <= 6 ? 'Moderate' : 'Expensive') : 'N/A';
            
            const popup = `
                <div class="popup-content">
                    <div class="popup-header">
                        <span class="popup-zcta">ZIP ${{props.zcta}}</span>
                        <span class="popup-score-badge" style="background:${{props.scoreColor || '#666'}}">
                            ${{props.score ? props.score.toFixed(0) : 'N/A'}} pts
                        </span>
                    </div>
                    <div class="popup-metrics">
                        <div class="popup-metric">
                            <span class="popup-metric-label">Pop Growth (5yr)</span>
                            <span class="popup-metric-value ${{growthClass}}">${{formatPercent(props.growth)}}</span>
                        </div>
                        <div class="popup-metric">
                            <span class="popup-metric-label">Price/Income</span>
                            <span class="popup-metric-value">${{props.pti ? props.pti.toFixed(1) + 'x' : 'N/A'}} (${{ptiLabel}})</span>
                        </div>
                        <div class="popup-metric">
                            <span class="popup-metric-label">Mobile Home %</span>
                            <span class="popup-metric-value">${{props.mhPct ? props.mhPct.toFixed(1) + '%' : 'N/A'}}</span>
                        </div>
                        <div class="popup-metric">
                            <span class="popup-metric-label">Vacancy</span>
                            <span class="popup-metric-value">${{props.vacancy ? props.vacancy.toFixed(1) + '%' : 'N/A'}}</span>
                        </div>
                        <div class="popup-metric">
                            <span class="popup-metric-label">Med. Home Value</span>
                            <span class="popup-metric-value">$${{formatNumber(props.homeValue)}}</span>
                        </div>
                        <div class="popup-metric">
                            <span class="popup-metric-label">Population</span>
                            <span class="popup-metric-value">${{formatNumber(props.population)}}</span>
                        </div>
                    </div>
                </div>
            `;
            
            layer.bindPopup(popup, {{ maxWidth: 300 }});
            
            layer.on({{
                mouseover: function(e) {{
                    const layer = e.target;
                    layer.setStyle({{
                        weight: 3,
                        color: '#fff',
                        fillOpacity: 0.8
                    }});
                    layer.bringToFront();
                }},
                mouseout: function(e) {{
                    choroplethLayer.resetStyle(e.target);
                }}
            }});
        }}
        
        function updateChoropleth() {{
            if (choroplethLayer) {{
                map.removeLayer(choroplethLayer);
            }}
            
            const minScore = parseInt(document.getElementById('minScoreFilter').value);
            
            // Determine active thesis keys that have scores
            const activeThesisKeys = Object.keys(thesisMetrics).filter(k => layerState[k]);
            
            // Filter features by min score on ANY active thesis
            const filteredData = {{
                type: 'FeatureCollection',
                features: choroplethData.features.filter(f => {{
                    // If no scored layers are active (e.g. only Growth/Afford), show all
                    if (activeThesisKeys.length === 0) return true;
                    
                    // Show feature if it meets the threshold for AT LEAST ONE active thesis
                    return activeThesisKeys.some(key => {{
                        const scoreKey = thesisMetrics[key].scoreKey;
                        const score = f.properties[scoreKey] || 0;
                        return score >= minScore;
                    }});
                }})
            }};
            
            // Update Stats
            document.getElementById('stat-zips').textContent = filteredData.features.length;
            document.getElementById('label-hot-zones').textContent = `Hot Zones (${{minScore}}+)`;
            document.getElementById('stat-hot-zones').textContent = filteredData.features.length; // Simply count visible zones as "Hot" for the current filter
            
            if (layerState.opportunity || layerState.growth || layerState.afford || layerState.displacement || layerState.progress || layerState.snowbird || layerState.slumlord || layerState.exurb) {{
                choroplethLayer = L.geoJSON(filteredData, {{
                    style: getChoroplethStyle,
                    onEachFeature: onEachFeature
                }}).addTo(map);
            }}
        }}
        
        // Lead markers
        const leadMarkers = [];
        
        markersData.forEach(data => {{
            const marker = L.circleMarker([data.lat, data.lng], {{
                radius: 8,
                fillColor: data.color,
                color: '#ffffff',
                weight: 2,
                opacity: 1,
                fillOpacity: 0.9,
                pane: 'leadMarkers'  // Use custom pane to stay above choropleth
            }});
            
            const distressLabel = data.distress >= 8 ? '<span style="background:#ef4444;color:white;padding:2px 6px;border-radius:4px;font-size:0.7rem;margin-left:6px">🔥 HOT TARGET</span>' : 
                                 data.distress >= 6 ? '<span style="background:#f97316;color:white;padding:2px 6px;border-radius:4px;font-size:0.7rem;margin-left:6px">⚠️ DISTRESSED</span>' : '';
            
            let popupHtml = `
                <div class="popup-content">
                    <div class="popup-header">
                        <div>
                            <span class="popup-name">${{data.name}}</span>${{distressLabel}}
                            ${{data.city ? `<div style="font-size:0.75rem;color:#94a3b8;margin-top:2px">${{data.city}}</div>` : ''}}
                        </div>
                        <span class="popup-score-badge" style="background:${{data.color}}">
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
            
            popupHtml += `<div class="popup-actions">`;
            if (data.mapsUrl) {{
                popupHtml += `<a href="${{data.mapsUrl}}" target="_blank" class="popup-btn maps">📍 Maps</a>`;
            }}
            if (data.website) {{
                popupHtml += `<a href="${{data.website}}" target="_blank" class="popup-btn website">🌐 Web</a>`;
            }}
            popupHtml += `</div></div>`;
            
            // Store lat/lng for dynamic zone lookup on popup open
            marker._leadData = data;
            
            marker.on('click', function(e) {{
                const zoneProps = findZoneForPoint(data.lat, data.lng);
                const zoneInsights = buildZoneInsightsHtml(zoneProps);
                const fullPopupHtml = popupHtml + zoneInsights;
                this.bindPopup(fullPopupHtml, {{ maxWidth: 320 }}).openPopup();
            }});
            
            leadMarkers.push(marker);
            
            if (layerState.leads) {{
                marker.addTo(map);
            }}
        }});

        // Debounce helper
        function debounce(func, wait) {{
            let timeout;
            return function() {{
                const context = this, args = arguments;
                clearTimeout(timeout);
                timeout = setTimeout(() => func.apply(context, args), wait);
            }};
        }}

        // Filtering Logic
        function updateVisibleLeads() {{
             if (!layerState.leads) return;
             
             let visibleCount = 0;
             
             // If filter is OFF, just show all
             if (!layerState.filterLeads) {{
                 leadMarkers.forEach(m => {{
                     m.addTo(map);
                     visibleCount++;
                 }});
             }} else {{
                // If filter is ON, filter by visible zones
                const minScore = parseInt(document.getElementById('minScoreFilter').value);
                const validFeatures = choroplethData.features.filter(f => {{
                     const score = f.properties.score || 0; 
                     // IMPORTANT: This filter logic MUST match updateChoropleth for consistency
                     const activeThesisKeys = Object.keys(thesisMetrics).filter(k => layerState[k]);
                     if (activeThesisKeys.length === 0) return true;
                     return activeThesisKeys.some(key => {{
                        const scoreKey = thesisMetrics[key].scoreKey;
                        const s = f.properties[scoreKey] || 0;
                        return s >= minScore;
                     }});
                }});
                
                if (validFeatures.length === 0) {{
                    leadMarkers.forEach(m => m.remove());
                }} else {{
                    leadMarkers.forEach(marker => {{
                        const lat = marker.getLatLng().lat;
                        const lng = marker.getLatLng().lng;
                        let isInside = false;
                        for (let i = 0; i < validFeatures.length; i++) {{
                            if (isPointInTarget(lat, lng, validFeatures[i])) {{
                                isInside = true;
                                break;
                            }}
                        }}
                        if (isInside) {{
                            marker.addTo(map);
                            visibleCount++;
                        }} else {{
                            marker.remove();
                        }}
                    }});
                }}
             }}
             // Update lead stat
             document.getElementById('stat-leads').textContent = visibleCount;
        }}
        
        // Toggle layer visibility
        function toggleLayer(layer) {{
            layerState[layer] = !layerState[layer];
            
            // Update button states - map CSS class to layer name
            document.querySelectorAll('.toggle-btn').forEach(btn => {{
                const layerName = btn.classList.contains('opportunity') ? 'opportunity' :
                                  btn.classList.contains('growth') ? 'growth' :
                                  btn.classList.contains('afford') ? 'afford' :
                                  btn.classList.contains('displacement') ? 'displacement' :
                                  btn.classList.contains('progress') ? 'progress' :
                                  btn.classList.contains('snowbird') ? 'snowbird' :
                                  btn.classList.contains('slumlord') ? 'slumlord' :
                                  btn.classList.contains('exurb') ? 'exurb' :
                                  btn.classList.contains('filter-leads') ? 'filterLeads' :
                                  btn.classList.contains('leads') ? 'leads' : null;
                if (layerName) {{
                    btn.classList.toggle('active', layerState[layerName]);
                }}
            }});
            
            // Update legends
            document.getElementById('legend-opportunity').style.display = layerState.opportunity ? 'flex' : 'none';
            document.getElementById('legend-growth').style.display = layerState.growth ? 'flex' : 'none';
            document.getElementById('legend-afford').style.display = layerState.afford ? 'flex' : 'none';
            document.getElementById('legend-displacement').style.display = layerState.displacement ? 'flex' : 'none';
            document.getElementById('legend-progress').style.display = layerState.progress ? 'flex' : 'none';
            document.getElementById('legend-snowbird').style.display = layerState.snowbird ? 'flex' : 'none';
            document.getElementById('legend-slumlord').style.display = layerState.slumlord ? 'flex' : 'none';
            document.getElementById('legend-exurb').style.display = layerState.exurb ? 'flex' : 'none';
            
            // Update thesis info panel for the selected thesis
            const thesisKeys = ['opportunity', 'growth', 'afford', 'displacement', 'progress', 'snowbird', 'slumlord', 'exurb'];
            if (thesisKeys.includes(layer) && layerState[layer]) {{
                updateThesisPanel(layer);
            }}
            
            // Update choropleth
            updateChoropleth();
            
             // Update lead markers
            // Update lead markers
            if (layer === 'leads') {{
                 // If toggling main leads switch
                 leadMarkers.forEach(marker => {{
                    if (layerState.leads) {{
                        marker.addTo(map);
                    }} else {{
                        marker.remove();
                    }}
                }});
                // Re-apply spatial filter if needed
                if (layerState.leads) updateVisibleLeads();
            }} else if (layer === 'filterLeads') {{
                updateVisibleLeads();
            }} else {{
                // Toggling any thesis layer potentially changes the visible zones
                if (layerState.filterLeads) updateVisibleLeads();
            }}
        }}
        
        // Min score filter
        const minScoreFilter = document.getElementById('minScoreFilter');
        const minScoreValue = document.getElementById('minScoreValue');
        
        const debouncedUpdate = debounce(() => {{
            updateChoropleth();
            if (layerState.filterLeads) updateVisibleLeads();
        }}, 250);

        minScoreFilter.addEventListener('input', function() {{
            minScoreValue.textContent = '≥ ' + this.value;
            debouncedUpdate();
        }});
        
        // Initial render
        updateChoropleth();
        updateVisibleLeads();
        
        // --- Point in Polygon Logic ---
        
        function isPointInPolygon(point, vs) {{
            // ray-casting algorithm based on
            // https://github.com/substack/point-in-polygon
            
            var x = point[0], y = point[1];
            
            var inside = false;
            for (var i = 0, j = vs.length - 1; i < vs.length; j = i++) {{
                var xi = vs[i][0], yi = vs[i][1];
                var xj = vs[j][0], yj = vs[j][1];
                
                var intersect = ((yi > y) != (yj > y))
                    && (x < (xj - xi) * (y - yi) / (yj - yi) + xi);
                if (intersect) inside = !inside;
            }}
            
            return inside;
        }}

        function isPointInMultiPolygon(point, multiPoly) {{
            // multiPoly is an array of polygons
            // GeoJSON Polygon: [ [ [x,y]... ] ] (coordinates is array of rings)
            // GeoJSON MultiPolygon: [ [ [ [x,y]... ] ] ] (coordinates is array of polygons)
            
            // In GeoJSON structure from Leaflet/feature content:
            // geometry.type will be "Polygon" or "MultiPolygon"
            // geometry.coordinates will be the data
            
            // We need to handle this at the feature level
            return false;
        }}

        function isPointInTarget(lat, lng, feature) {{
            if (!feature || !feature.geometry) return false;
            
            const x = lng;
            const y = lat;
            const geom = feature.geometry;
            const coords = geom.coordinates;
            
            if (geom.type === 'Polygon') {{
                // coords[0] is the outer ring
                return isPointInPolygon([x, y], coords[0]);
            }} else if (geom.type === 'MultiPolygon') {{
                for (let i = 0; i < coords.length; i++) {{
                    // coords[i][0] is the outer ring of the i-th polygon
                    if (isPointInPolygon([x, y], coords[i][0])) {{
                        return true;
                    }}
                }}
            }}
            return false;
        }}

        // --- Filtering Logic ---

        function updateVisibleLeads() {{
            if (!layerState.leads) return; // If leads are off globaly, nothing to do
            
            // If filter is OFF, just show all valid leads
            if (!layerState.filterLeads) {{
                leadMarkers.forEach(m => m.addTo(map));
                return;
            }}
            
            // If filter is ON, we need the currently visible zones
            // We can get them from the global 'choroplethData' using current filters
            const minScore = parseInt(document.getElementById('minScoreFilter').value);
            const validFeatures = choroplethData.features.filter(f => {{
                const score = f.properties.score || 0;
                return score >= minScore;
            }});
            
            // Optimization: if no zones visible, hide all leads
            if (validFeatures.length === 0) {{
                leadMarkers.forEach(m => m.remove());
                return;
            }}
            
            // Check each marker
            leadMarkers.forEach(marker => {{
                const lat = marker.getLatLng().lat;
                const lng = marker.getLatLng().lng;
                
                let isInside = false;
                // Ideally use spatial index, but for <2k polygons and <5k points this O(N*M) might fly 
                // if we don't drag slider too fast. 
                // Let's implement a quick bounding box check or just iterate.
                for (let i = 0; i < validFeatures.length; i++) {{
                    if (isPointInTarget(lat, lng, validFeatures[i])) {{
                        isInside = true;
                        break;
                    }}
                }}
                
                if (isInside) {{
                    marker.addTo(map);
                }} else {{
                    marker.remove();
                }}
            }});
        }}

    </script>
</body>
</html>'''
    
    return html



def get_crm_map_html() -> str:
    """
    Generate the full HTML for the CRM Opportunity Map view.
    Fetches live data from the database.
    """
    try:
        from db import get_all_leads
    except ImportError:
        # Fallback if running from a different context
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from db import get_all_leads

    # Load All Data
    leads = get_all_leads()
    zcta_metrics = load_zcta_metrics()
    geojson = load_geojson_boundaries()
    
    # Generate HTML
    html = generate_opportunity_map_html(
        places=leads,
        zcta_metrics=zcta_metrics,
        geojson=geojson,
        title="MHP Opportunity Zones - Florida"
    )
    
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
        {"zcta": "33825", "opportunity_score": 80, "population_growth_rate": 5.2, "price_to_income_ratio": 3.5, "median_home_value": 150000, "population_2022": 5000},
        {"zcta": "33826", "opportunity_score": 65, "population_growth_rate": 3.1, "price_to_income_ratio": 4.0, "median_home_value": 180000, "population_2022": 8000}
    ]
    
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-81.55, 27.60], [-81.45, 27.60], [-81.45, 27.70], [-81.55, 27.70], [-81.55, 27.60]]]
                },
                "properties": {"zcta": "33825", "opportunity_score": 80}
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-81.65, 27.55], [-81.55, 27.55], [-81.55, 27.65], [-81.65, 27.65], [-81.65, 27.55]]]
                },
                "properties": {"zcta": "33826", "opportunity_score": 65}
            }
        ]
    }
    
    places = [
        {"latitude": 27.65, "longitude": -81.50, "name": "Test Lead 1", "site_score_1_10": 8, "distress_score": 2, "address": "123 Main St", "city": "Avon Park", "phone": "555-1234"},
        {"latitude": 27.62, "longitude": -81.52, "name": "Test Lead 2", "site_score_1_10": 5, "distress_score": 9, "address": "456 Elm St", "city": "Avon Park", "phone": "555-5678"},
        {"latitude": 27.58, "longitude": -81.60, "name": "Test Lead 3", "site_score_1_10": 7, "distress_score": 4, "address": "789 Oak Ave", "city": "Sebring", "phone": "555-9012"}
    ]
    # === END MOCK DATA ===
    
    print(f"Generating map with {len(zcta_metrics)} zones and {len(places)} leads...")
    html = generate_opportunity_map_html(places, zcta_metrics, geojson, title)
    
    output_file.parent.mkdir(exist_ok=True, parents=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"Map saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Opportunity Map")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output HTML file")
    parser.add_argument("--no-leads", action="store_true", help="Don't plot leads")
    parser.add_argument("--title", type=str, default="MHP Opportunity Zones - Florida", help="Map title")
    
    args = parser.parse_args()
    
    export_opportunity_map(
        output_file=args.output,
        no_leads=args.no_leads,
        title=args.title
    )


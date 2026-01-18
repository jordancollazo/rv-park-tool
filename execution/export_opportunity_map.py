
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

# Try to import DB functions, fallback for flexibility
try:
    from db import get_all_leads
except ImportError:
    # If standard import fails, try relative import for execution from different dirs
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    try:
        from db import get_all_leads
    except ImportError:
        get_all_leads = None

# Paths
DEFAULT_OUTPUT = Path("output/opportunity_map.html")
GEOJSON_PATH = Path(".tmp/florida_zcta_boundaries.geojson")
DB_PATH = Path("data/leads.db")

# --- COLOR SCALES ---
def get_growth_color(growth_rate: float | None) -> str:
    if growth_rate is None: return "#808080"
    if growth_rate <= -5: return "#fecaca"
    elif growth_rate <= 0: return "#fca5a5"
    elif growth_rate <= 5: return "#f87171"
    elif growth_rate <= 10: return "#ef4444"
    elif growth_rate <= 15: return "#dc2626"
    else: return "#991b1b"

def get_affordability_color(pti: float | None) -> str:
    if pti is None: return "#808080"
    if pti <= 2.5: return "#14532d"
    elif pti <= 3.5: return "#166534"
    elif pti <= 4.5: return "#15803d"
    elif pti <= 5.5: return "#22c55e"
    elif pti <= 6.5: return "#4ade80"
    else: return "#bbf7d0"

def get_opportunity_color(score: float | None) -> str:
    if score is None: return "#808080"
    if score <= 20: return "#f3e8ff"
    elif score <= 35: return "#e9d5ff"
    elif score <= 45: return "#d8b4fe"
    elif score <= 55: return "#c084fc"
    elif score <= 65: return "#a855f7"
    elif score <= 75: return "#9333ea"
    else: return "#7c3aed"

def get_displacement_color(score: float | None) -> str:
    if score is None: return "#808080"
    if score <= 20: return "#fecaca"
    elif score <= 40: return "#fca5a5"
    elif score <= 60: return "#f87171"
    elif score <= 80: return "#ef4444"
    else: return "#dc2626"

def get_progress_color(score: float | None) -> str:
    if score is None: return "#808080"
    if score <= 20: return "#dcfce7"
    elif score <= 40: return "#86efac"
    elif score <= 60: return "#4ade80"
    elif score <= 80: return "#22c55e"
    else: return "#16a34a"

def get_snowbird_color(score: float | None) -> str:
    if score is None: return "#808080"
    if score <= 20: return "#dbeafe"
    elif score <= 40: return "#93c5fd"
    elif score <= 60: return "#60a5fa"
    elif score <= 80: return "#3b82f6"
    else: return "#2563eb"

def get_slumlord_color(score: float | None) -> str:
    if score is None: return "#808080"
    if score <= 20: return "#fed7aa"
    elif score <= 40: return "#fdba74"
    elif score <= 60: return "#fb923c"
    elif score <= 80: return "#f97316"
    else: return "#ea580c"

def get_exurb_color(score: float | None) -> str:
    if score is None: return "#808080"
    if score <= 20: return "#fce7f3"
    elif score <= 40: return "#f9a8d4"
    elif score <= 60: return "#f472b6"
    elif score <= 80: return "#ec4899"
    else: return "#db2777"


def load_zcta_metrics() -> list[dict]:
    """Load ZCTA metrics from database."""
    if not DB_PATH.exists():
        print("WARNING: Database not found, no ZCTA metrics available")
        return []
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='zcta_metrics'")
        if not cursor.fetchone():
            return []

        cursor.execute("""
            SELECT zcta, name, population_growth_rate, price_to_income_ratio,
                   mobile_home_percentage, vacancy_rate, opportunity_score,
                   opportunity_rank, median_home_value, median_household_income,
                   population_2023, median_age, senior_percentage, rent_burden,
                   income_growth_rate, displacement_score, path_of_progress_score,
                   snowbird_score, slumlord_rehab_score, exurb_score,
                   distance_to_nearest_metro
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
        return None
    
    try:
        with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading GeoJSON: {e}")
        return None

def generate_opportunity_map_html(
    places: list[dict] | None,
    zcta_metrics: list[dict],
    geojson: dict | None,
    title: str = "MHP Opportunity Zones"
) -> str:
    """
    Generate HTML with choropleth layers and lead markers.
    Uses strict double-brace escaping for JS/CSS compatibility.
    """
    
    # 1. Process Markers
    markers_data = []
    if places:
        for p in places:
            lat = p.get("latitude")
            lng = p.get("longitude")
            if lat and lng:
                try:
                    score = p.get("site_score_1_10", 0) or 0
                    distress = p.get("distress_score", 0) or 0
                    
                    # Manual color logic based on distress
                    if distress >= 8: marker_color = "#ef4444"
                    elif distress >= 6: marker_color = "#f97316"
                    else: 
                        # Simple score color fallbacks
                        if score >= 8: marker_color = "#22c55e" # Green
                        elif score >= 5: marker_color = "#eab308" # Yellow
                        else: marker_color = "#ef4444" # Red

                    markers_data.append({
                        "lat": float(lat), "lng": float(lng),
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
                        "color": marker_color
                    })
                except (ValueError, TypeError): continue

    markers_json = json.dumps(markers_data)
    
    # 2. Process Choropleth Features
    features_list = []
    
    # Map metrics by ZCTA for O(1) lookup if building from GeoJSON
    metrics_map = {m["zcta"]: m for m in zcta_metrics}
    
    if geojson:
        for feature in geojson.get("features", []):
            zcta = feature.get("properties", {}).get("zcta")
            if zcta and zcta in metrics_map:
                m = metrics_map[zcta]
                # Combine geometry with metric properties
                props = {
                    "zcta": zcta,
                    "growth": m.get("population_growth_rate"),
                    "pti": m.get("price_to_income_ratio"),
                    "score": m.get("opportunity_score"),
                    "homeValue": m.get("median_home_value"),
                    "population": m.get("population_2023"),
                    "vacancy": m.get("vacancy_rate"),
                    "mhPct": m.get("mobile_home_percentage"),
                    "medianAge": m.get("median_age"),
                    "rentBurden": m.get("rent_burden"),
                    # Thesis Scores
                    "displacementScore": m.get("displacement_score"),
                    "progressScore": m.get("path_of_progress_score"),
                    "snowbirdScore": m.get("snowbird_score"),
                    "slumlordScore": m.get("slumlord_rehab_score"),
                    "exurbScore": m.get("exurb_score"),
                    "distanceToMetro": m.get("distance_to_nearest_metro"),
                    # Colors
                    "scoreColor": get_opportunity_color(m.get("opportunity_score")),
                    "growthColor": get_growth_color(m.get("population_growth_rate")),
                    "affordColor": get_affordability_color(m.get("price_to_income_ratio")),
                    "displacementColor": get_displacement_color(m.get("displacement_score")),
                    "progressColor": get_progress_color(m.get("path_of_progress_score")),
                    "snowbirdColor": get_snowbird_color(m.get("snowbird_score")),
                    "slumlordColor": get_slumlord_color(m.get("slumlord_rehab_score")),
                    "exurbColor": get_exurb_color(m.get("exurb_score"))
                }
                features_list.append({
                    "type": "Feature",
                    "geometry": feature["geometry"],
                    "properties": props
                })

    choropleth_json = json.dumps({"type": "FeatureCollection", "features": features_list})

    # Stats
    top_zones = len([m for m in zcta_metrics if (m.get("opportunity_score") or 0) >= 60])
    total_leads = len(markers_data)
    total_zips = len(features_list)

    # Calculate Center
    center_lat = 27.66
    center_lng = -81.51
    if markers_data:
        center_lat = sum(m["lat"] for m in markers_data) / len(markers_data)
        center_lng = sum(m["lng"] for m in markers_data) / len(markers_data)

    # 3. Generate HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        html, body {{ height: 100%; font-family: -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; display: flex; flex-direction: column; }}
        
        /* HEADER & CONTROLS */
        .header {{ background: #1e293b; padding: 1rem 2rem; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }}
        .header h1 {{ font-size: 1.5rem; color: #a855f7; }}
        .stats {{ display: flex; gap: 1.5rem; }}
        .stat-value {{ font-size: 1.25rem; font-weight: 700; text-align: center; }}
        .stat-label {{ font-size: 0.7rem; color: #94a3b8; text-transform: uppercase; }}
        
        .controls {{ background: #1e293b; padding: 0.75rem 2rem; border-bottom: 1px solid #334155; display: flex; gap: 1rem; flex-wrap: wrap; align-items: center; }}
        .toggle-btn {{ padding: 0.5rem 1rem; border-radius: 6px; border: 1px solid #475569; background: transparent; color: #94a3b8; cursor: pointer; transition: all 0.2s; }}
        .toggle-btn:hover {{ border-color: #94a3b8; }}
        .toggle-btn.active {{ border-color: transparent; color: white; }}
        
        /* Button Colors */
        .toggle-btn.opportunity.active {{ background: #9333ea; }}
        .toggle-btn.growth.active {{ background: #dc2626; }}
        .toggle-btn.afford.active {{ background: #16a34a; }}
        .toggle-btn.leads.active {{ background: #3b82f6; }}
        .toggle-btn.filter.active {{ background: #f59e0b; color: white; }}
        
        .toggle-btn.displacement.active {{ background: #dc2626; }}
        .toggle-btn.progress.active {{ background: #16a34a; }}
        .toggle-btn.snowbird.active {{ background: #2563eb; }}
        .toggle-btn.slumlord.active {{ background: #ea580c; }}
        .toggle-btn.exurb.active {{ background: #d946ef; }}

        /* DYNAMIC SLIDER STYLES */
        .sliders-container {{ width: 100%; display: flex; flex-wrap: wrap; gap: 1.5rem; margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #334155; }}
        .slider-group {{ display: flex; align-items: center; gap: 0.75rem; background: #334155; padding: 0.5rem 1rem; border-radius: 12px; }}
        .slider-label {{ font-size: 0.8rem; color: #cbd5e1; font-weight: 600; text-transform: uppercase; }}
        input[type="range"] {{ width: 120px; accent-color: #a855f7; }}
        .score-val {{ font-size: 0.9rem; font-weight: 700; color: #fff; min-width: 30px; text-align: right; }}

        /* MAP CONTAINER */
        .map-container {{ flex: 1; display: flex; min-height: 0; position: relative; }}
        #map {{ flex: 1; }} 
        
        .sidebar {{ width: 300px; background: #1e293b; border-left: 1px solid #334155; padding: 1rem; overflow-y: auto; display: none; }}
        .sidebar.active {{ display: block; }}
        
        /* POPUPS */
        .leaflet-popup-content-wrapper {{ background: #1e293b; color: #e2e8f0; border-radius: 8px; }}
        .leaflet-popup-tip {{ background: #1e293b; }}
        .popup-header {{ border-bottom: 1px solid #334155; padding-bottom: 0.5rem; margin-bottom: 0.5rem; }}
        .popup-title {{ font-weight: 600; font-size: 1rem; color: #f1f5f9; }}
        .popup-metric {{ display: flex; justify-content: space-between; font-size: 0.85rem; margin-bottom: 0.25rem; }}
        .popup-metric span:first-child {{ color: #94a3b8; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📍 {title}</h1>
        <div class="stats">
            <div>
                <div class="stat-value" id="stat-match" style="color:#fbbf24">{top_zones}</div>
                <div class="stat-label">Matching Zones</div>
            </div>
            <div>
                <div class="stat-value" style="color:#60a5fa">{total_zips}</div>
                <div class="stat-label">Total Zip Codes</div>
            </div>
            <div>
                <div class="stat-value" style="color:#4ade80">{total_leads}</div>
                <div class="stat-label">Leads</div>
            </div>
        </div>
    </div>

    <div class="controls">
        <div style="display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center; width:100%">
            <button class="toggle-btn opportunity active" onclick="toggleLayer('opportunity')">🎯 Opportunity</button>
            <button class="toggle-btn growth" onclick="toggleLayer('growth')">📈 Growth</button>
            <button class="toggle-btn afford" onclick="toggleLayer('afford')">🏠 Affordability</button>
            <div style="width: 1px; height: 24px; background: #475569; margin: 0 0.5rem;"></div>
            
            <button class="toggle-btn displacement" onclick="toggleLayer('displacement')">🔴 Displacement Risk</button>
            <button class="toggle-btn progress" onclick="toggleLayer('progress')">💰 Path of Progress</button>
            <button class="toggle-btn snowbird" onclick="toggleLayer('snowbird')">🌴 Snowbird Haven</button>
            <button class="toggle-btn slumlord" onclick="toggleLayer('slumlord')">🔧 Distressed Rehab</button>
            <button class="toggle-btn exurb" onclick="toggleLayer('exurb')">🏡 Exurb</button>
            
            <div style="flex:1"></div>
            <button class="toggle-btn leads active" onclick="toggleLayer('leads')">📍 Leads</button>
            <button class="toggle-btn filter" onclick="toggleFilter()" id="btn-filter" title="Only show leads inside visible zones">🛡️ Filter Leads</button>
        </div>

        <div class="sliders-container" id="sliders-container">
            <!-- Dynamic Sliders Injected Here -->
        </div>
    </div>

    <div class="map-container">
        <div id="map"></div>
        <div class="sidebar"></div>
    </div>

    <script>
        console.log("Initializing Map...");
        
        // --- DATA ---
        const markersData = {markers_json};
        const choroplethData = {choropleth_json};

        // --- DEFINE LAYERS & CONFIG ---
        const thesisConfig = {{
            opportunity: {{ name: "Opportunity Score", color: "#9333ea", prop: "score", isScore: true }},
            growth: {{ name: "Growth %", color: "#dc2626", prop: "growth", isScore: false, min: -5, max: 15 }},
            afford: {{ name: "Affordability (PTI)", color: "#16a34a", prop: "pti", isScore: false, min: 2, max: 8, invert: true }},
            displacement: {{ name: "Displacement Score", color: "#dc2626", prop: "displacementScore", isScore: true }},
            progress: {{ name: "Progress Score", color: "#16a34a", prop: "progressScore", isScore: true }},
            snowbird: {{ name: "Snowbird Score", color: "#2563eb", prop: "snowbirdScore", isScore: true }},
            slumlord: {{ name: "Rehab Score", color: "#ea580c", prop: "slumlordScore", isScore: true }},
            exurb: {{ name: "Exurb Score", color: "#d946ef", prop: "exurbScore", isScore: true }}
        }};

        // --- STATE ---
        const state = {{
            activeLayers: ['opportunity'], 
            leads: true,
            filterLeads: false,
            filters: {{
                opportunity: 60,
                growth: 5,
                afford: 4.5,
                displacement: 50,
                progress: 50,
                snowbird: 50,
                slumlord: 50,
                exurb: 50
            }}
        }};

        // --- MAP SETUP ---
        var map = L.map('map').setView([{center_lat}, {center_lng}], 7);
        
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            attribution: '&copy; CARTO',
            maxZoom: 19
        }}).addTo(map);

        map.createPane('choropleth');
        map.getPane('choropleth').style.zIndex = 400;
        map.createPane('leads');
        map.getPane('leads').style.zIndex = 450;
        
        // We will store reference to multiple active geojson layers here
        let activeGeoJsonLayers = []; 
        let markersLayerGroup = L.layerGroup().addTo(map);

        // --- PATTERN GENERATOR ---
        // Dynamically create SVG patterns for stripes
        // --- PATTERN GENERATOR ---
        // Dynamically create SVG patterns for stripes
        function getPatternId(color, index) {{
            const id = 'pattern-' + color.replace('#', '') + '-' + index;
            if (document.getElementById(id)) return id;

            // Create SVG defs if not exists
            let svg = document.getElementById('map-patterns');
            if (!svg) {{
                svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
                svg.id = 'map-patterns';
                svg.style.position = 'absolute';
                svg.style.width = 0;
                svg.style.height = 0;
                // Add defs
                const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
                svg.appendChild(defs);
                document.body.appendChild(svg);
            }}

            const defs = svg.querySelector('defs');
            const pattern = document.createElementNS('http://www.w3.org/2000/svg', 'pattern');
            pattern.setAttribute('id', id);
            pattern.setAttribute('patternUnits', 'userSpaceOnUse');
            pattern.setAttribute('width', '10');
            pattern.setAttribute('height', '10'); 

            // Rotation depends on index to allow crossing patterns for 3rd layer
            const rot = (index % 2 === 0) ? 45 : -45; 
            pattern.setAttribute('patternTransform', `rotate(${{rot}})`);

            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', '0');
            line.setAttribute('y1', '0');
            line.setAttribute('x2', '0');
            line.setAttribute('y2', '10');
            line.setAttribute('stroke', color);
            line.setAttribute('stroke-width', '4'); // Thickness

            pattern.appendChild(line);
            defs.appendChild(pattern);

            return id;
        }}


        // --- LOGIC: MATCHING ---
        function checkLayerMatch(props, layerKey) {{
            const conf = thesisConfig[layerKey];
            const val = props[conf.prop];
            const threshold = state.filters[layerKey];
            
            if (val === undefined || val === null) return false;
            
            if (conf.invert) {{
                 if (val > threshold) return false;
            }} else {{
                 if (val < threshold) return false;
            }}
            return true;
        }}

        // --- STYLE FACTORY ---
        function getStyleForLayer(layerKey, layerIndex) {{
            return function(feature) {{
                const props = feature.properties;
                const matches = checkLayerMatch(props, layerKey);
                const conf = thesisConfig[layerKey];
                
                if (!matches) {{
                    return {{ fillOpacity: 0, opacity: 0, stroke: false }};
                }}

                // If Layer 0 (Base): Solid Fill
                // If Layer 1+ (Overlay): Stripe Pattern
                
                let fillColor = conf.color;
                let fillOpacity = 0.5;
                
                if (layerIndex > 0) {{
                    // Use Pattern
                    const patId = getPatternId(conf.color, layerIndex);
                    fillColor = `url(#${{patId}})`;
                    fillOpacity = 0.8; // Higher opacity for stripes so they show up
                }}

                return {{
                    fillColor: fillColor,
                    fillOpacity: fillOpacity,
                    weight: 1,
                    color: conf.color, // Border color matches thesis
                    opacity: (layerIndex === 0) ? 0.2 : 0 // Only base layer gets faint border
                }};
            }}
        }}

        function onEachFeature(feature, layer) {{
            const p = feature.properties;
            const popup = `
                <div class="popup-header"><div class="popup-title">ZIP ${{p.zcta}}</div></div>
                <div class="popup-metric"><span>Opp Score:</span> <span style="font-weight:700; color:#a855f7">${{p.score?.toFixed(0)}}</span></div>
                <div class="popup-metric"><span>Growth:</span> <span>${{p.growth?.toFixed(1)}}%</span></div>
                <div class="popup-metric"><span>PTI:</span> <span>${{p.pti?.toFixed(1)}}x</span></div>
                <div class="popup-metric"><span>Displacement:</span> <span>${{p.displacementScore?.toFixed(0)}}</span></div>
            `;
            layer.bindPopup(popup);
        }}

        // --- RENDER FUNCTIONS ---
        function updateMap() {{
            // 1. Clear existing layers
            activeGeoJsonLayers.forEach(l => map.removeLayer(l));
            activeGeoJsonLayers = [];

            // 2. Render each active layer (Stacked)
            state.activeLayers.forEach((key, index) => {{
                const layer = L.geoJSON(choroplethData, {{
                    style: getStyleForLayer(key, index),
                    onEachFeature: onEachFeature, // Popups on all layers
                    pane: 'choropleth'
                }});
                layer.addTo(map);
                activeGeoJsonLayers.push(layer);
            }});

            // 3. Count Matches (Union count)
            // A zone matches if it matches ANY active layer? Or do we want Intersection Stat?
            // Let's show Union count for now ("Active zones on map")
            let matchCount = 0;
            choroplethData.features.forEach(f => {{
                let visible = false;
                for (let key of state.activeLayers) {{
                    if (checkLayerMatch(f.properties, key)) {{ visible = true; break; }}
                }}
                if (visible) matchCount++;
            }});
            document.getElementById('stat-match').innerText = matchCount;

            // 4. Update Markers
            markersLayerGroup.clearLayers();
            if (state.leads) {{
                markersData.forEach(d => {{
                    // Simple Visibility toggle
                    const m = L.circleMarker([d.lat, d.lng], {{
                        radius: 6,
                        fillColor: d.color, color: '#fff', weight: 1, fillOpacity: 0.9, pane: 'leads'
                    }});
                    
                    // Richer Popup
                    const stars = d.rating ? '⭐ ' + d.rating : '';
                    const reviews = d.reviews ? `(${{d.reviews}} reviews)` : '';
                    
                    // Note: We use double curly braces for JS block escapes in Python f-string
                    // and ${{var}} for JS template literal variables
                    const popup = `
                        <div class="popup-header">
                            <div class="popup-title">${{d.name}}</div>
                            <div style="font-size:0.75rem; color:${{d.color}}">Score: ${{d.score}} | Distress: ${{d.distress}}</div>
                        </div>
                        <div style="margin-top:0.5rem; font-size:0.85rem">
                            ${{d.address ? `<div class="popup-metric">📍 ${{d.address}}, ${{d.city}}</div>` : ''}}
                            ${{d.phone ? `<div class="popup-metric">📞 <a href="tel:${{d.phone}}" style="color:#60a5fa">${{d.phone}}</a></div>` : ''}}
                            ${{d.rating ? `<div class="popup-metric" style="color:#fbbf24">${{stars}} <span style="color:#94a3b8; font-size:0.75rem">${{reviews}}</span></div>` : ''}}
                        </div>
                        <div style="margin-top:0.75rem; display:flex; gap:0.5rem">
                            ${{d.website ? `<a href="${{d.website}}" target="_blank" style="background:#3b82f6; color:white; padding:4px 8px; border-radius:4px; text-decoration:none; font-size:0.75rem; flex:1; text-align:center">🌐 Website</a>` : ''}}
                            ${{d.mapsUrl ? `<a href="${{d.mapsUrl}}" target="_blank" style="background:#059669; color:white; padding:4px 8px; border-radius:4px; text-decoration:none; font-size:0.75rem; flex:1; text-align:center">🗺️ Google Maps</a>` : ''}}
                        </div>
                    `;
                    m.bindPopup(popup);
                    markersLayerGroup.addLayer(m);
                }});
            }}
        }}

        function renderSliders() {{
            const container = document.getElementById('sliders-container');
            container.innerHTML = '';
            
            state.activeLayers.forEach(key => {{
                const conf = thesisConfig[key];
                const val = state.filters[key];
                
                let min = 0, max = 100, step = 1;
                if (conf.min !== undefined) min = conf.min;
                if (conf.max !== undefined) max = conf.max;
                if (conf.name.includes('PTI') || conf.name.includes('Growth')) step = 0.1;
                
                const div = document.createElement('div');
                div.className = 'slider-group';
                div.innerHTML = `
                    <div class="slider-label" style="color:${{conf.color}}">${{conf.name}}</div>
                    <div style="font-size:0.7rem; color:#94a3b8">${{conf.invert ? '<' : '>'}}</div>
                    <input type="range" class="dynamic-slider" 
                           min="${{min}}" max="${{max}}" step="${{step}}" value="${{val}}"
                           data-key="${{key}}">
                    <div class="score-val">${{val}}</div>
                `;
                container.appendChild(div);
            }});

            document.querySelectorAll('.dynamic-slider').forEach(input => {{
                input.addEventListener('input', (e) => {{
                    const key = e.target.dataset.key;
                    state.filters[key] = parseFloat(e.target.value);
                    e.target.nextElementSibling.innerText = e.target.value;
                    updateMap();
                }});
            }});
        }}

        // --- CONTROLS ---
        // --- CONTROLS ---
        window.toggleLayer = function(layerName) {{
            if (layerName === 'leads' || layerName === 'filterLeads') {{
                if (layerName === 'leads') state.leads = !state.leads;
                if (layerName === 'filterLeads') state.filterLeads = !state.filterLeads;
            }} else {{
                const index = state.activeLayers.indexOf(layerName);
                if (index > -1) {{
                    state.activeLayers.splice(index, 1);
                }} else {{
                    state.activeLayers.push(layerName);
                }}
            }}
            updateButtons();
            renderSliders();
            updateSidebar(state.activeLayers.length > 0 ? state.activeLayers[state.activeLayers.length-1] : null);
            updateMap();
        }};
        
        window.toggleFilter = () => window.toggleLayer('filterLeads');

        function updateButtons() {{
            document.querySelectorAll('.toggle-btn').forEach(btn => btn.classList.remove('active'));
            if (state.leads) document.querySelector('.toggle-btn.leads').classList.add('active');
            if (state.filterLeads) document.querySelector('.toggle-btn.filter').classList.add('active');
            state.activeLayers.forEach(key => {{
                const btn = document.querySelector('.toggle-btn.' + key);
                if (btn) btn.classList.add('active');
            }});
        }}

        // THESIS DESCRIPTIONS 
        const thesisDescriptions = {{
            opportunity: {{ title: "Opportunity Score", desc: "Composite score.", calc: "Growth + Affordability.", relevance: "Core+ Strategy." }}
             // ... others ...
        }}; 
        const fullThesisDescriptions = {{
            opportunity: {{
                title: "🎯 Opportunity Score",
                desc: "The 'All-Rounder' score. Identifies valid markets with the best balance of growth, stability, and demand.",
                calc: "Weighted composite of Population Growth (45%), Affordability (35%), Mobile Home Density (10%), vacancy (10%).",
                relevance: "Best for Core+ acquisitions."
            }},
            growth: {{ title: "📈 Population Growth", desc: "Identifies areas where the customer base is expanding rapidly.", calc: "Pct change 2019-2023.", relevance: "Strong demand driver." }},
            afford: {{ title: "🏠 Affordability", desc: "Markets where wages cannot support homeownership.", calc: "Price-to-Income Ratio.", relevance: "Recession resistance." }},
            displacement: {{ title: "🔴 Displacement Risk", desc: "High costs squeezing renters.", calc: "Rent Burden + Low Vacancy.", relevance: "Captive audience." }},
            progress: {{ title: "💰 Path of Progress", desc: "Emerging markets.", calc: "Income Growth + Low Home Values.", relevance: "Appreciation play." }},
            snowbird: {{ title: "🌴 Snowbird Haven", desc: "55+ and seasonal communities.", calc: "Age + Senior %.", relevance: "Sticky tenants." }},
            slumlord: {{ title: "🔧 Distressed / Turnaround", desc: "Neglected markets.", calc: "Vacancy + Low Value + MH Density.", relevance: "Deep value." }},
            exurb: {{ title: "🏡 Remote Work Exurb", desc: "Zoom Towns.", calc: "Distance + Affordability.", relevance: "Lifestyle shift." }}
        }};

        function updateSidebar(layerName) {{
            const sidebar = document.querySelector('.sidebar');
            if (!layerName) {{ sidebar.classList.remove('active'); return; }}
            
            // Use full description if available
            const data = fullThesisDescriptions[layerName];
            if (data) {{
                sidebar.innerHTML = `
                    <div style="font-size:1.1rem; font-weight:700; color:#f1f5f9; margin-bottom:0.5rem">${{data.title}}</div>
                    <div style="font-size:0.9rem; color:#cbd5e1; margin-bottom:1rem; line-height:1.4">${{data.desc}}</div>
                    <div style="font-size:0.75rem; color:#94a3b8; text-transform:uppercase; font-weight:600; margin-bottom:0.25rem">Calculated By</div>
                    <div style="font-size:0.85rem; color:#e2e8f0; background:#0f172a; padding:0.5rem; border-radius:6px; margin-bottom:1rem; border:1px solid #334155">${{data.calc}}</div>
                    <div style="font-size:0.75rem; color:#94a3b8; text-transform:uppercase; font-weight:600; margin-bottom:0.25rem">Relevance</div>
                    <div style="font-size:0.85rem; color:#e2e8f0; border-left:3px solid #a855f7; padding-left:0.75rem">${{data.relevance}}</div>
                `;
            }} else {{
                sidebar.innerHTML = `<div style="color:#fff"><b>${{layerName}}</b></div>`;
            }}
            sidebar.classList.add('active');
        }}

        // --- INIT ---
        updateButtons();
        renderSliders();
        updateMap();
        setTimeout(() => map.invalidateSize(), 200);

    </script>
</body>
</html>"""
    
    return html


def export_opportunity_map(output_file=DEFAULT_OUTPUT, no_leads=False, title="MHP Opportunity Zones"):
    print(f"Loading Data...")
    if not output_file.parent.exists(): output_file.parent.mkdir(parents=True)

    zcta = load_zcta_metrics()
    geojson = load_geojson_boundaries()
    leads = []
    if not no_leads and get_all_leads:
        leads = get_all_leads()
    
    print(f"Generating Map for {len(zcta)} zones and {len(leads)} leads...")
    html = generate_opportunity_map_html(leads, zcta, geojson, title)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-leads", action="store_true")
    parser.add_argument("--title", type=str, default="MHP Opportunity Zones")
    args = parser.parse_args()
    
    export_opportunity_map(args.output, args.no_leads, args.title)

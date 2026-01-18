"""
fetch_florida_zcta_data.py

Fetches comprehensive Census ACS data for all Florida zip codes (ZCTAs).
Computes opportunity scores for MHP/RV park investment targeting.

Metrics collected:
- Population (current and 5-year historical for growth calculation)
- Median household income
- Median home value
- Median rent
- Housing unit counts (total and mobile homes)
- Vacancy rates

Usage:
    python execution/fetch_florida_zcta_data.py
    python execution/fetch_florida_zcta_data.py --test --limit 10
    python execution/fetch_florida_zcta_data.py --refresh
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Florida state FIPS code
FLORIDA_FIPS = "12"

# Census API base URLs (using most recent available)
ACS_CURRENT_URL = "https://api.census.gov/data/2023/acs/acs5"  # 2023 5-year (2019-2023)
ACS_HISTORICAL_URL = "https://api.census.gov/data/2019/acs/acs5"  # 2019 5-year (2015-2019)

# Output paths
OUTPUT_JSON = Path(".tmp/florida_zcta_census.json")
DB_PATH = Path("data/leads.db")

# Census variables to fetch
CENSUS_VARIABLES = {
    # Population
    "B01003_001E": "total_population",
    
    # Age Demographics (for Snowbird thesis)
    "B01002_001E": "median_age",
    # Male 65+ (sum these for male seniors)
    "B01001_020E": "male_65_66",
    "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74",
    "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84",
    "B01001_025E": "male_85_plus",
    # Female 65+ (sum these for female seniors)
    "B01001_044E": "female_65_66",
    "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74",
    "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84",
    "B01001_049E": "female_85_plus",
    
    # Income
    "B19013_001E": "median_household_income",
    
    # Housing Values
    "B25077_001E": "median_home_value",
    "B25064_001E": "median_rent",
    
    # Housing Units
    "B25001_001E": "total_housing_units",
    "B25024_010E": "mobile_home_units",  # Mobile home/trailer
    
    # Vacancy
    # Vacancy
    "B25002_001E": "total_occupancy_status",  # Total units
    "B25002_003E": "vacant_units",  # Vacant units
    
    # --- PHASE 4: Vibe / Livability Variables ---
    
    # Poverty (Table B17001)
    "B17001_001E": "poverty_total", # Total population for whom poverty status is determined
    "B17001_002E": "poverty_below", # Number below poverty level
    
    # Employment (Table B23025)
    "B23025_003E": "labor_force_civilian", # Civilian labor force
    "B23025_005E": "unemployed", # Unemployed
    
    # Education (Table B15003) - Population 25 years and over
    "B15003_001E": "education_total",
    "B15003_022E": "bachelors_degree", # Bachelor's degree
    "B15003_023E": "masters_degree", # Master's degree
    "B15003_024E": "professional_degree", # Professional school degree
    "B15003_025E": "doctorate_degree", # Doctorate degree
    
    # Commute (Table B08303 & B08006)
    # B08303 is "Travel Time to Work" (Buckets, not aggregate). _001E is just Total Workers.
    # We need B08136: "AGGREGATE TRAVEL TIME TO WORK (IN MINUTES)"
    "B08136_001E": "aggregate_travel_time", 
    "B08006_001E": "total_workers",
    "B08006_017E": "workers_at_home",
    
    # Family Stability (Table B11003)
    "B11003_001E": "total_families",
    "B11003_003E": "families_with_kids", # Married-couple with own children < 18
    # Actually B11003 has "Married-couple" and "Other". 
    # Better: B11005 "Households by Presence of People Under 18 Years"
    "B11005_001E": "total_households_kids", 
    "B11005_002E": "households_with_kids",
    
    # --- PHASE 5: Vacation/Tourism Variables ---
    "B25004_006E": "seasonal_housing_units",  # For seasonal, recreational, or occasional use
}


def fetch_acs_data(year_url: str, variables: list[str], limit: int | None = None) -> dict:
    """
    Fetch ACS data for all Florida ZCTAs.
    
    Note: Census ZCTA data doesn't support state filtering, so we fetch all
    ZCTAs and filter to Florida prefixes (32xxx, 33xxx, 34xxx).
    
    Args:
        year_url: Base URL for the ACS year
        variables: List of Census variable codes
        limit: Optional limit for testing
    
    Returns:
        Dictionary mapping ZCTA to variable values
    """
    var_string = ",".join(variables)
    
    # ZCTAs don't nest within states in Census hierarchy
    # Fetch all and filter by Florida prefixes
    params = {
        "get": f"NAME,{var_string}",
        "for": "zip code tabulation area:*"
    }
    
    print(f"  Fetching from {year_url}...")
    
    try:
        response = requests.get(year_url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        if len(data) < 2:
            print("  WARNING: No data returned")
            return {}
        
        # Parse response (first row is headers)
        headers = data[0]
        results = {}
        
        # Florida zip code prefixes
        FL_PREFIXES = ("32", "33", "34")
        
        for row in data[1:]:
            row_dict = dict(zip(headers, row))
            zcta = row_dict.get("zip code tabulation area", "")
            
            if not zcta:
                continue
            
            # Filter to Florida zip codes only
            if not zcta.startswith(FL_PREFIXES):
                continue
            
            # Parse numeric values
            parsed = {"zcta": zcta, "name": row_dict.get("NAME", "")}
            for var_code in variables:
                raw_value = row_dict.get(var_code)
                try:
                    # Census uses -666666666 for missing/suppressed data
                    if raw_value is None or raw_value == "":
                        val = None
                    else:
                        val = float(raw_value)
                        if val < -1000000:  # Census missing data flags are large negative
                            val = None
                    parsed[var_code] = val
                except (ValueError, TypeError):
                    parsed[var_code] = None
            
            results[zcta] = parsed
            
            if limit and len(results) >= limit:
                break
        
        print(f"  Retrieved {len(results)} Florida ZCTAs")
        return results
        
    except requests.RequestException as e:
        print(f"  ERROR fetching data: {e}")
        return {}



def load_distances_from_db() -> dict:
    """Load pre-calculated metro distances from database."""
    if not DB_PATH.exists():
        return {}
        
    distances = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check if columns exist
        cursor.execute("PRAGMA table_info(zcta_metrics)")
        columns = [row[1] for row in cursor.fetchall()]
        if "distance_to_nearest_metro" not in columns:
            conn.close()
            return {}
            
        cursor.execute("""
            SELECT zcta, distance_to_orlando, distance_to_tampa, 
                   distance_to_jacksonville, distance_to_miami, 
                   distance_to_nearest_metro
            FROM zcta_metrics
        """)
        
        for row in cursor.fetchall():
            distances[row["zcta"]] = dict(row)
            
        conn.close()
        print(f"  Loaded distances for {len(distances)} ZCTAs")
        return distances
        
    except sqlite3.Error:
        return {}


def compute_metrics(current_data: dict, historical_pop: dict, historical_income: dict, distances: dict = None) -> list[dict]:
    """
    Compute derived metrics and opportunity scores.
    
    Args:
        current_data: 2023 ACS data by ZCTA
        historical_pop: 2019 ACS population data by ZCTA (for growth calc)
        historical_income: 2019 ACS income data by ZCTA (for income growth)
        distances: Dictionary of pre-calculated distances by ZCTA
    
    Returns:
        List of ZCTA metric dictionaries
    """
    results = []
    distances = distances or {}
    
    for zcta, current in current_data.items():
        hist_pop = historical_pop.get(zcta, {})
        hist_inc = historical_income.get(zcta, {})
        dist_data = distances.get(zcta, {})
        
        # Extract raw values
        pop_current = current.get("B01003_001E")
        pop_historical = hist_pop.get("B01003_001E")
        median_income = current.get("B19013_001E")
        income_historical = hist_inc.get("B19013_001E")
        median_home_value = current.get("B25077_001E")
        median_rent = current.get("B25064_001E")
        total_housing = current.get("B25001_001E")
        mobile_homes = current.get("B25024_010E")
        total_occupancy = current.get("B25002_001E")
        vacant = current.get("B25002_003E")
        seasonal_housing = current.get("B25004_006E")
        
        # Age demographics
        median_age = current.get("B01002_001E")
        
        # Sum 65+ population (male + female)
        senior_pop = 0
        for var in ["B01001_020E", "B01001_021E", "B01001_022E", 
                    "B01001_023E", "B01001_024E", "B01001_025E",
                    "B01001_044E", "B01001_045E", "B01001_046E",
                    "B01001_047E", "B01001_048E", "B01001_049E"]:
            val = current.get(var)
            if val and val > 0:
                senior_pop += val
        
        
        # Compute derived metrics
        
        # New Vibe Metrics extraction
        poverty_total = current.get("B17001_001E")
        poverty_below = current.get("B17001_002E")
        
        labor_force = current.get("B23025_003E")
        unemployed = current.get("B23025_005E")
        
        edu_total = current.get("B15003_001E")
        # Sum higher ed
        bachelors = current.get("B15003_022E") or 0
        masters = current.get("B15003_023E") or 0
        prof = current.get("B15003_024E") or 0
        phd = current.get("B15003_025E") or 0
        higher_ed_count = bachelors + masters + prof + phd
        
        commute_agg = current.get("B08136_001E")
        workers_total = current.get("B08006_001E")
        workers_home = current.get("B08006_017E")
        
        hh_total_kids_universe = current.get("B11005_001E")
        hh_with_kids = current.get("B11005_002E")

        
        # Compute derived metrics
        metrics = {
            "zcta": zcta,
            "name": current.get("name", ""),
            
            # Raw values
            "population_2023": pop_current,
            "population_2019": pop_historical,
            "median_household_income": median_income,
            "income_2019": income_historical,
            "median_home_value": median_home_value,
            "median_rent": median_rent,
            "total_housing_units": total_housing,
            "mobile_home_units": mobile_homes,
            "median_age": median_age,
            "senior_population": senior_pop if senior_pop > 0 else None,
            
            # Distances
            "distance_to_orlando": dist_data.get("distance_to_orlando"),
            "distance_to_tampa": dist_data.get("distance_to_tampa"),
            "distance_to_jacksonville": dist_data.get("distance_to_jacksonville"),
            "distance_to_miami": dist_data.get("distance_to_miami"),
            "distance_to_nearest_metro": dist_data.get("distance_to_nearest_metro"),
            
            # Computed metrics
            "population_growth_rate": None,
            "income_growth_rate": None,
            "price_to_income_ratio": None,
            "rent_burden": None,  # rent as % of monthly income
            "mobile_home_percentage": None,
            "vacancy_rate": None,
            "senior_percentage": None,
            
            # Phase 4: Vibe Metrics
            "poverty_rate": None,
            "unemployment_rate": None,
            "bachelors_degree_pct": None,
            "avg_commute_time": None,
            "families_with_kids_pct": None,
            "vibe_badge": None,
            
            # Phase 5: Vacation/Tourism
            "seasonal_housing_units": None,
            "seasonal_housing_pct": None,
            
            # Thesis scores (0-100)
            "opportunity_score": None,
            "displacement_score": None,
            "path_of_progress_score": None,
            "snowbird_score": None,
            "slumlord_rehab_score": None,
            "exurb_score": None,
            "vacation_score": None,
        }
        
        # Population growth rate (4-year, 2019->2023)
        if pop_current and pop_historical and pop_historical > 0:
            metrics["population_growth_rate"] = round(
                ((pop_current - pop_historical) / pop_historical) * 100, 2
            )
        
        # Income growth rate (for Path of Progress thesis)
        if median_income and income_historical and income_historical > 0:
            metrics["income_growth_rate"] = round(
                ((median_income - income_historical) / income_historical) * 100, 2
            )
            
        # Vibe Metrics Calculation
        if poverty_total and poverty_total > 0 and poverty_below is not None:
             metrics["poverty_rate"] = round((poverty_below / poverty_total) * 100, 1)
             
        if labor_force and labor_force > 0 and unemployed is not None:
             metrics["unemployment_rate"] = round((unemployed / labor_force) * 100, 1)
             
        if edu_total and edu_total > 0:
             metrics["bachelors_degree_pct"] = round((higher_ed_count / edu_total) * 100, 1)
             
        if commute_agg is not None and workers_total and workers_total > 0:
             # Commuters = Total Workers - Worked at Home
             commuters = workers_total - (workers_home or 0)
             if commuters > 0:
                 metrics["avg_commute_time"] = round(commute_agg / commuters, 1)
        
        if hh_total_kids_universe and hh_total_kids_universe > 0 and hh_with_kids is not None:
             metrics["families_with_kids_pct"] = round((hh_with_kids / hh_total_kids_universe) * 100, 1)
             
        # Determine Vibe Badge
        # Logic:
        # - "Blue Collar Strong": Low Poverty (<15%), Low Bachelors (<20%), High Employment (Unemp < 6%)
        # - "Retiree Haven": High Age (>50), Low Poverty (<15%)
        # - "Distressed": High Poverty (>20%), High Unemployment (>8%)
        # - "Commuter Bedroom": High Commute (>30), High Income (>$60k), High Families (>30%)
        # - "Wealthy/Gentrified": High Bachelors (>40%), High Income (>$80k)
        
        badge = "Standard"
        poverty = metrics["poverty_rate"] or 0
        unemp = metrics["unemployment_rate"] or 0
        bach = metrics["bachelors_degree_pct"] or 0
        age = median_age or 0
        commute = metrics["avg_commute_time"] or 0
        income = median_income or 0
        kids = metrics["families_with_kids_pct"] or 0

        if poverty > 25 or (poverty > 20 and unemp > 8):
            badge = "Distressed"
        elif age > 55 and poverty < 15:
            badge = "Retiree Haven"
        elif bach > 40 and income > 75000:
            badge = "Wealthy/Gentrified"
        elif commute > 30 and income > 60000 and kids > 30:
            badge = "Commuter Bedroom"
        elif poverty < 15 and bach < 25 and unemp < 7:
            badge = "Blue Collar Strong"
        elif kids > 40:
             badge = "Family Roots"
             
        metrics["vibe_badge"] = badge
        
        # Price-to-income ratio
        if median_home_value and median_income and median_income > 0:
            metrics["price_to_income_ratio"] = round(
                median_home_value / median_income, 2
            )
        
        # Rent burden (rent as % of monthly income) - for Displacement thesis
        if median_rent and median_income and median_income > 0:
            monthly_income = median_income / 12
            metrics["rent_burden"] = round(
                (median_rent / monthly_income) * 100, 2
            )
        
        # Mobile home percentage
        if mobile_homes is not None and total_housing and total_housing > 0:
            metrics["mobile_home_percentage"] = round(
                (mobile_homes / total_housing) * 100, 2
            )
        
        # Vacancy rate
        if vacant is not None and total_occupancy and total_occupancy > 0:
            metrics["vacancy_rate"] = round(
                (vacant / total_occupancy) * 100, 2
            )
        
        # Seasonal housing percentage (for Vacation thesis)
        metrics["seasonal_housing_units"] = seasonal_housing
        if seasonal_housing is not None and total_housing and total_housing > 0:
            metrics["seasonal_housing_pct"] = round(
                (seasonal_housing / total_housing) * 100, 2
            )
        
        # Senior percentage (for Snowbird thesis)
        if senior_pop and pop_current and pop_current > 0:
            metrics["senior_percentage"] = round(
                (senior_pop / pop_current) * 100, 2
            )
        
        # Compute all thesis scores
        metrics["opportunity_score"] = compute_opportunity_score(metrics)
        metrics["displacement_score"] = compute_displacement_score(metrics)
        metrics["path_of_progress_score"] = compute_path_of_progress_score(metrics)
        metrics["snowbird_score"] = compute_snowbird_score(metrics)
        metrics["slumlord_rehab_score"] = compute_slumlord_rehab_score(metrics)
        metrics["exurb_score"] = compute_exurb_score(metrics)
        metrics["vacation_score"] = compute_vacation_score(metrics)
        
        results.append(metrics)
    
    # Rank by opportunity score
    results.sort(key=lambda x: x.get("opportunity_score") or 0, reverse=True)
    for i, m in enumerate(results, 1):
        m["opportunity_rank"] = i
    
    return results


def compute_opportunity_score(metrics: dict) -> float | None:
    """
    Compute weighted opportunity score.
    
    Formula (weights when all data available):
    - 45% population growth rate (normalized)
    - 35% affordability (inverse of price-to-income, normalized)
    - 10% mobile home concentration
    - 10% vacancy rate (moderate is good, too high is bad)
    
    If growth data is unavailable, re-weight to:
    - 0% population growth (unavailable)
    - 60% affordability
    - 20% mobile home concentration  
    - 20% vacancy rate
    
    Returns score 0-100, or None if insufficient data (need at least PTI).
    """
    growth = metrics.get("population_growth_rate")
    pti = metrics.get("price_to_income_ratio")
    mh_pct = metrics.get("mobile_home_percentage")
    vacancy = metrics.get("vacancy_rate")
    
    # Need at minimum PTI to compute any score
    if pti is None:
        return None
    
    score = 0.0
    has_growth = growth is not None
    
    # Determine weights based on available data
    if has_growth:
        growth_weight = 45
        afford_weight = 35
        mh_weight = 10
        vacancy_weight = 10
    else:
        # Re-weight when growth unavailable
        growth_weight = 0
        afford_weight = 60
        mh_weight = 20
        vacancy_weight = 20
    
    # Growth component
    if has_growth:
        # Normalize: -5% to +20% growth maps to 0-45 points
        if growth <= -5:
            growth_score = 0
        elif growth >= 20:
            growth_score = growth_weight
        else:
            growth_score = ((growth + 5) / 25) * growth_weight
        score += growth_score
    
    # Affordability component
    # Lower PTI = more affordable = higher score
    # PTI of 2 = very affordable (max pts), PTI of 8+ = unaffordable (0 pts)
    if pti <= 2:
        afford_score = afford_weight
    elif pti >= 8:
        afford_score = 0
    else:
        afford_score = ((8 - pti) / 6) * afford_weight
    score += afford_score
    
    # Mobile home percentage
    # Higher MH% = existing market = good
    if mh_pct is not None:
        if mh_pct >= 20:
            mh_score = mh_weight
        else:
            mh_score = (mh_pct / 20) * mh_weight
        score += mh_score
    
    # Vacancy rate
    # Sweet spot is 5-10% vacancy (indicates turnover, not distress)
    if vacancy is not None:
        if 5 <= vacancy <= 10:
            vacancy_score = vacancy_weight
        elif vacancy < 5:
            vacancy_score = (vacancy / 5) * vacancy_weight
        elif vacancy <= 20:
            vacancy_score = ((20 - vacancy) / 10) * vacancy_weight
        else:
            vacancy_score = 0
        score += vacancy_score
    
    return round(score, 2)


def compute_displacement_score(metrics: dict) -> float | None:
    """
    Displacement Play thesis: High rent burden + Low vacancy + Affordable area.
    Target areas where renters are being priced out.
    
    Formula:
    - 40% rent burden (rent as % of monthly income > 30% is stressed)
    - 30% low vacancy (demand pressure)
    - 30% still somewhat affordable (not already too expensive)
    """
    rent_burden = metrics.get("rent_burden")
    vacancy = metrics.get("vacancy_rate")
    pti = metrics.get("price_to_income_ratio")
    
    if rent_burden is None:
        return None
    
    score = 0.0
    
    # Rent burden: 25% is low, 40%+ is high (struggling renters)
    if rent_burden >= 40:
        score += 40
    elif rent_burden >= 25:
        score += ((rent_burden - 25) / 15) * 40
    
    # Low vacancy = high demand (0-5% is tight, 15%+ is loose)
    if vacancy is not None:
        if vacancy <= 5:
            score += 30
        elif vacancy <= 15:
            score += ((15 - vacancy) / 10) * 30
    
    # Want areas that are still somewhat affordable (PTI < 6)
    if pti is not None:
        if pti <= 4:
            score += 30
        elif pti <= 6:
            score += ((6 - pti) / 2) * 30
    
    return round(score, 2)


def compute_path_of_progress_score(metrics: dict) -> float | None:
    """
    Path of Progress thesis: High income growth + Still affordable.
    Buy ahead of gentrification - incomes rising, prices haven't caught up.
    
    Formula:
    - 50% income growth rate (incomes rising = improving area)
    - 50% still affordable (PTI is low = buy before it spikes)
    """
    income_growth = metrics.get("income_growth_rate")
    pti = metrics.get("price_to_income_ratio")
    
    if income_growth is None or pti is None:
        return None
    
    score = 0.0
    
    # Income growth: 0-5% is stagnant, 20%+ is booming
    if income_growth >= 20:
        score += 50
    elif income_growth > 0:
        score += (income_growth / 20) * 50
    
    # Affordability: low PTI = still cheap
    if pti <= 3:
        score += 50
    elif pti <= 6:
        score += ((6 - pti) / 3) * 50
    
    return round(score, 2)


def compute_snowbird_score(metrics: dict) -> float | None:
    """
    Snowbird/Retiree Haven thesis: High median age + High senior population.
    Target areas with stable retiree demographics (55+ parks).
    
    Formula:
    - 40% median age (higher = more retirees)
    - 60% senior percentage (% of pop 65+)
    """
    median_age = metrics.get("median_age")
    senior_pct = metrics.get("senior_percentage")
    
    
    # If both are missing, we can't score
    if median_age is None and senior_pct is None:
        return None
    
    score = 0.0
    
    # Median age: 35 is young, 55+ is retirement community
    if median_age is not None:
        if median_age >= 55:
            score += 40
        elif median_age >= 35:
            score += ((median_age - 35) / 20) * 40
        else:
            # If age is low, we only get score from senior_pct
            pass
    
    # Senior percentage: 10% is typical, 30%+ is retiree haven
    if senior_pct is not None:
        if senior_pct >= 30:
            score += 60
        elif senior_pct >= 10:
            score += ((senior_pct - 10) / 20) * 60
            
    # If median_age was missing, re-weight senior_pct to 100%
    if median_age is None and senior_pct is not None:
        # Normalize senior component (max 60 pts) to 100 pts
        # If senior_pct was 30+ (60 pts), it becomes 100
        score = (score / 60) * 100 if score > 0 else 0
        
    return round(score, 2)


def compute_slumlord_rehab_score(metrics: dict) -> float | None:
    """
    Slumlord Rehab thesis: High vacancy + Low home value + High MH%.
    Target distressed areas with turnaround potential.
    
    Formula:
    - 40% high vacancy (distressed, available inventory)
    - 30% low home value (cheap entry point)
    - 30% high mobile home % (proven market, likely mismanaged parks)
    """
    vacancy = metrics.get("vacancy_rate")
    home_value = metrics.get("median_home_value")
    mh_pct = metrics.get("mobile_home_percentage")
    
    if vacancy is None:
        return None
    
    score = 0.0
    
    # High vacancy = distress (10-25% is target range)
    if vacancy >= 25:
        score += 40
    elif vacancy >= 10:
        score += ((vacancy - 10) / 15) * 40
    
    # Low home value = cheap entry (< $150k is cheap, > $300k is expensive)
    if home_value is not None:
        if home_value <= 150000:
            score += 30
        elif home_value <= 300000:
            score += ((300000 - home_value) / 150000) * 30
    
    # High mobile home % = existing market (> 15% is significant)
    if mh_pct is not None:
        if mh_pct >= 25:
            score += 30
        elif mh_pct >= 5:
            score += ((mh_pct - 5) / 20) * 30
    
    return round(score, 2)



def compute_exurb_score(metrics: dict) -> float | None:
    """
    Remote Work Exurb thesis: 
    Target areas within 30-90 miles of a major metro.
    Too close = too expensive/crowded. Too far = disconnected.
    Plus affordable housing and population growth.
    
    Formula:
    - 40% Distance sweet spot (30-90 miles)
    - 30% Affordability (remote workers want space for less $)
    - 30% Population Growth (others are already moving there)
    """
    distance = metrics.get("distance_to_nearest_metro")
    pti = metrics.get("price_to_income_ratio")
    pop_growth = metrics.get("population_growth_rate")
    
    if distance is None or pti is None or pop_growth is None:
        return None
        
    score = 0.0
    
    # Distance sweet spot (30-90 miles)
    # Peak score at 60 miles
    dist_score = 0
    if 30 <= distance <= 90:
        # Ideal range
        if 45 <= distance <= 75:
            dist_score = 100
        elif distance < 45:
            dist_score = ((distance - 30) / 15) * 100
        else: # > 75
            dist_score = ((90 - distance) / 15) * 100
            
    score += (dist_score / 100) * 40
    
    # Affordability (want cheap, PTI < 5)
    if pti <= 3:
        score += 30
    elif pti <= 6:
        score += ((6 - pti) / 3) * 30
        
    # Population Growth (want > 5%)
    if pop_growth >= 15:
        score += 30
    elif pop_growth > 0:
        score += (pop_growth / 15) * 30
        
    return round(score, 2)


def compute_vacation_score(metrics: dict) -> float | None:
    """
    Vacation/STR Demand thesis: High seasonal housing + Low vacancy.
    Target areas with established vacation rental markets.
    
    Formula:
    - 70% seasonal housing % (higher = more established vacation market)
    - 30% low vacancy (indicates demand for seasonal rentals)
    
    Interpretation:
    - High seasonal housing % means this ZIP already has vacation homes/rentals
    - Low vacancy means demand is strong (units are occupied/rented)
    """
    seasonal_pct = metrics.get("seasonal_housing_pct")
    vacancy = metrics.get("vacancy_rate")
    
    if seasonal_pct is None:
        return None
    
    score = 0.0
    
    # Seasonal housing percentage
    # 5% is typical, 20%+ is vacation-heavy (e.g., beach towns, Keys)
    if seasonal_pct >= 20:
        score += 70
    elif seasonal_pct >= 5:
        score += ((seasonal_pct - 5) / 15) * 70
    elif seasonal_pct > 0:
        # Give partial credit for any seasonal housing
        score += (seasonal_pct / 5) * 20
    
    # Low vacancy = high demand (inverse relationship)
    # For vacation areas, low vacancy means units are being rented
    if vacancy is not None:
        if vacancy <= 5:
            score += 30
        elif vacancy <= 15:
            score += ((15 - vacancy) / 10) * 30
    
    return round(score, 2)


def save_to_json(metrics: list[dict], output_path: Path):
    """Save metrics to JSON file."""
    output_path.parent.mkdir(exist_ok=True)
    
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_vintage": "ACS 2023 5-Year with 2019 5-Year for growth",
        "total_zctas": len(metrics),
        "zctas": metrics
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nSaved to {output_path}")



def save_to_database(metrics: list[dict]):
    """Save metrics to SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Drop and recreate table with new schema
    cursor.execute("DROP TABLE IF EXISTS zcta_metrics")
    
    cursor.execute("""
        CREATE TABLE zcta_metrics (
            zcta TEXT PRIMARY KEY,
            name TEXT,
            
            -- Population
            population_2019 INTEGER,
            population_2023 INTEGER,
            population_growth_rate REAL,
            
            -- Income
            median_household_income INTEGER,
            income_2019 INTEGER,
            income_growth_rate REAL,
            
            -- Housing
            median_home_value INTEGER,
            median_rent INTEGER,
            price_to_income_ratio REAL,
            rent_burden REAL,
            
            -- Demographics
            median_age REAL,
            senior_population INTEGER,
            senior_percentage REAL,
            
            -- MHP-Specific
            total_housing_units INTEGER,
            mobile_home_units INTEGER,
            mobile_home_percentage REAL,
            vacancy_rate REAL,
            
            -- Phase 4: Vibe / Livability
            poverty_rate REAL,
            unemployment_rate REAL,
            bachelors_degree_pct REAL,
            avg_commute_time REAL,
            families_with_kids_pct REAL,
            vibe_badge TEXT,
            
            -- Thesis Scores (0-100)
            opportunity_score REAL,
            opportunity_rank INTEGER,
            displacement_score REAL,
            path_of_progress_score REAL,
            snowbird_score REAL,
            slumlord_rehab_score REAL,
            exurb_score REAL,
            
            -- Phase 5: Vacation/Tourism
            seasonal_housing_units INTEGER,
            seasonal_housing_pct REAL,
            vacation_score REAL,
            
            -- Distance to metros (miles)
            distance_to_orlando REAL,
            distance_to_tampa REAL,
            distance_to_jacksonville REAL,
            distance_to_miami REAL,
            distance_to_nearest_metro REAL,
            
            -- Metadata
            data_vintage TEXT,
            last_updated TEXT,
            
            -- GIS
            geojson TEXT
        )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zcta_opportunity ON zcta_metrics(opportunity_score DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zcta_growth ON zcta_metrics(population_growth_rate DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zcta_displacement ON zcta_metrics(displacement_score DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zcta_snowbird ON zcta_metrics(snowbird_score DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zcta_vibe ON zcta_metrics(vibe_badge)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zcta_vacation ON zcta_metrics(vacation_score DESC)")
    
    # Insert data
    now = datetime.now(timezone.utc).isoformat()
    vintage = "ACS 2023 5-Year"
    
    for m in metrics:
        cursor.execute("""
            INSERT OR REPLACE INTO zcta_metrics (
                zcta, name, population_2019, population_2023, population_growth_rate,
                median_household_income, income_2019, income_growth_rate,
                median_home_value, median_rent, price_to_income_ratio, rent_burden,
                median_age, senior_population, senior_percentage,
                total_housing_units, mobile_home_units, mobile_home_percentage, vacancy_rate,
                poverty_rate, unemployment_rate, bachelors_degree_pct, avg_commute_time, 
                families_with_kids_pct, vibe_badge,
                opportunity_score, opportunity_rank, displacement_score, 
                path_of_progress_score, snowbird_score, slumlord_rehab_score, exurb_score,
                seasonal_housing_units, seasonal_housing_pct, vacation_score,
                distance_to_orlando, distance_to_tampa, distance_to_jacksonville,
                distance_to_miami, distance_to_nearest_metro,
                data_vintage, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            m["zcta"], m.get("name"), m.get("population_2019"), m.get("population_2023"),
            m.get("population_growth_rate"), m.get("median_household_income"),
            m.get("income_2019"), m.get("income_growth_rate"),
            m.get("median_home_value"), m.get("median_rent"),
            m.get("price_to_income_ratio"), m.get("rent_burden"),
            m.get("median_age"), m.get("senior_population"), m.get("senior_percentage"),
            m.get("total_housing_units"), m.get("mobile_home_units"),
            m.get("mobile_home_percentage"), m.get("vacancy_rate"),
            m.get("poverty_rate"), m.get("unemployment_rate"), m.get("bachelors_degree_pct"),
            m.get("avg_commute_time"), m.get("families_with_kids_pct"), m.get("vibe_badge"),
            m.get("opportunity_score"), m.get("opportunity_rank"),
            m.get("displacement_score"), m.get("path_of_progress_score"),
            m.get("snowbird_score"), m.get("slumlord_rehab_score"), m.get("exurb_score"),
            m.get("seasonal_housing_units"), m.get("seasonal_housing_pct"), m.get("vacation_score"),
            m.get("distance_to_orlando"), m.get("distance_to_tampa"), 
            m.get("distance_to_jacksonville"), m.get("distance_to_miami"),
            m.get("distance_to_nearest_metro"),
            vintage, now
        ))
    
    conn.commit()
    conn.close()
    
    print(f"Saved {len(metrics)} ZCTAs to database")


def print_top_opportunities(metrics: list[dict], n: int = 20):
    """Print top N opportunity zones."""
    print(f"\n{'='*80}")
    print(f"TOP {n} OPPORTUNITY ZONES")
    print(f"{'='*80}")
    print(f"{'Rank':<5} {'ZCTA':<7} {'Score':<7} {'Growth%':<9} {'PTI':<6} {'MH%':<6} {'Vacancy%':<9}")
    print(f"{'-'*80}")
    
    for m in metrics[:n]:
        if m.get("opportunity_score") is None:
            continue
        print(
            f"{m.get('opportunity_rank', '-'):<5} "
            f"{m['zcta']:<7} "
            f"{m.get('opportunity_score', 0):<7.1f} "
            f"{m.get('population_growth_rate', 0) or 0:<9.1f} "
            f"{m.get('price_to_income_ratio', 0) or 0:<6.1f} "
            f"{m.get('mobile_home_percentage', 0) or 0:<6.1f} "
            f"{m.get('vacancy_rate', 0) or 0:<9.1f}"
        )



def main():
    parser = argparse.ArgumentParser(description="Fetch Florida ZCTA Census data")
    parser.add_argument("--test", action="store_true", help="Test mode (limit results)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of ZCTAs")
    parser.add_argument("--refresh", action="store_true", help="Force refresh existing data")
    parser.add_argument("--no-db", action="store_true", help="Skip database save")
    
    args = parser.parse_args()
    
    limit = args.limit or (10 if args.test else None)
    
    print("="*60)
    print("FLORIDA ZCTA CENSUS DATA FETCH")
    print("="*60)
    print(f"Mode: {'TEST' if args.test else 'FULL'}")
    if limit:
        print(f"Limit: {limit} ZCTAs")
    print()
    
    # Pre-load distances from DB if available
    print("\n[0/4] Loading pre-calculated distances...")
    distances = load_distances_from_db()
    
    # Check for existing data
    if OUTPUT_JSON.exists() and not args.refresh:
        print(f"Existing data found at {OUTPUT_JSON}")
        print("Use --refresh to re-fetch")
        
        with open(OUTPUT_JSON, "r") as f:
            existing = json.load(f)
        print(f"Contains {existing.get('total_zctas', 0)} ZCTAs")
        print_top_opportunities(existing.get("zctas", []))
        return
    
    # Fetch 2023 data (current)
    print("\n[1/4] Fetching 2023 ACS 5-Year data (current)...")
    variables = list(CENSUS_VARIABLES.keys())
    current_data = fetch_acs_data(ACS_CURRENT_URL, variables, limit)
    
    if not current_data:
        print("ERROR: Failed to fetch current data")
        sys.exit(1)
    
    # Fetch 2019 data (historical for growth calc)
    print("\n[2/4] Fetching 2019 ACS 5-Year data (for population growth)...")
    time.sleep(1)  # Be nice to the API
    historical_pop = fetch_acs_data(ACS_HISTORICAL_URL, ["B01003_001E"], limit)
    
    # Fetch 2019 income data (for income growth - Path of Progress thesis)
    print("\n[3/4] Fetching 2019 income data (for income growth)...")
    time.sleep(1)
    historical_income = fetch_acs_data(ACS_HISTORICAL_URL, ["B19013_001E"], limit)
    
    # Compute metrics
    print("\n[4/4] Computing metrics and opportunity scores...")
    metrics = compute_metrics(current_data, historical_pop, historical_income, distances)
    
    # Filter to those with valid scores
    scored = [m for m in metrics if m.get("opportunity_score") is not None]
    print(f"  ZCTAs with valid scores: {len(scored)} / {len(metrics)}")
    
    # Save results
    save_to_json(metrics, OUTPUT_JSON)
    
    if not args.no_db:
        save_to_database(metrics)
    
    # Print summary
    print_top_opportunities(metrics)
    
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total ZCTAs: {len(metrics)}")
    print(f"With opportunity scores: {len(scored)}")
    
    if scored:
        avg_score = sum(m["opportunity_score"] for m in scored) / len(scored)
        top_score = scored[0]["opportunity_score"] if scored else 0
        print(f"Average score: {avg_score:.1f}")
        print(f"Top score: {top_score:.1f} (ZCTA {scored[0]['zcta']})")


if __name__ == "__main__":
    main()

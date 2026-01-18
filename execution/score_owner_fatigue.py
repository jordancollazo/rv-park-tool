"""
score_owner_fatigue.py
Deterministic Owner Fatigue scoring (0-100 scale, no LLM).

Owner Fatigue Score identifies neglected/hands-off properties as acquisition targets.
Higher scores = more neglected = better acquisition opportunity.

Scoring Components:
A) Site Maintenance Neglect (0-30 pts)
B) Operational Modernity Gap (0-25 pts)
C) Listing/Comms Friction (0-25 pts)
D) Customer Friction Text Signals (0-20 pts)

Usage:
    python execution/score_owner_fatigue.py
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Paths
INPUT_PATH = Path(".tmp/scored_sites.json")
OUTPUT_PATH = Path(".tmp/owner_fatigue_scored.json")

# Friction keywords to detect in reviews/testimonials
FRICTION_KEYWORDS = [
    "no response",
    "never called back",
    "voicemail",
    "can't reach",
    "cannot reach",
    "no one answers",
    "unreachable",
    "didn't respond",
    "didn't call back",
    "never responded",
    "hard to reach",
    "difficult to contact",
    "never available",
    "left message",
    "no reply",
    "ignored",
]

# Call-for-rates patterns (indicates no online booking/pricing)
CALL_FOR_RATES_PATTERNS = [
    r"call for (?:rates?|availability|pricing|info)",
    r"contact us for (?:rates?|availability|pricing)",
    r"rates? upon request",
    r"pricing upon request",
    r"call to (?:book|reserve)",
    r"call for (?:more information|details)",
]


def detect_copyright_year(text: str) -> Optional[int]:
    """
    Detect copyright year from page text.
    
    Looks for patterns like:
    - © 2019 Company Name
    - Copyright 2019
    - (c) 2019
    
    Returns the most recent year found, or None if no copyright detected.
    """
    if not text:
        return None
    
    # Pattern matches © 2019, Copyright 2019, (c) 2019, etc.
    patterns = [
        r'©\s*(\d{4})',
        r'copyright\s*(\d{4})',
        r'\(c\)\s*(\d{4})',
        r'©\s*(?:20\d{2}\s*[-–]\s*)?(\d{4})',  # © 2019-2023
    ]
    
    years = []
    text_lower = text.lower()
    
    for pattern in patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            try:
                year = int(match)
                if 1990 <= year <= datetime.now().year:
                    years.append(year)
            except ValueError:
                continue
    
    return max(years) if years else None


def calculate_broken_link_ratio(pages: list[dict]) -> float:
    """
    Calculate ratio of failed/broken internal page fetches.
    
    Returns a ratio from 0.0 to 1.0.
    """
    if not pages or len(pages) <= 1:
        return 0.0
    
    # Skip homepage (index 0), count internal pages
    internal_pages = pages[1:]
    if not internal_pages:
        return 0.0
    
    failed = sum(1 for p in internal_pages if p.get("status") != "success")
    return failed / len(internal_pages)


def detect_call_for_rates(text: str) -> bool:
    """
    Detect "call for rates/availability" language without online pricing.
    """
    if not text:
        return False
    
    text_lower = text.lower()
    
    for pattern in CALL_FOR_RATES_PATTERNS:
        if re.search(pattern, text_lower):
            # Check if there's actual pricing visible (dollar signs with numbers)
            has_pricing = bool(re.search(r'\$\s*\d{2,}', text))
            if not has_pricing:
                return True
    
    return False


def count_friction_keywords(text: str) -> int:
    """
    Count occurrences of friction keywords in text.
    
    Keywords indicate poor responsiveness:
    "no response", "voicemail", "can't reach", etc.
    """
    if not text:
        return 0
    
    text_lower = text.lower()
    count = 0
    
    for keyword in FRICTION_KEYWORDS:
        # Count each occurrence
        count += len(re.findall(re.escape(keyword), text_lower))
    
    return count


def get_page_text(pages: list[dict]) -> str:
    """
    Extract combined text from all crawled pages.
    Uses title, meta_description, h1, and any extracted text.
    """
    if not pages:
        return ""
    
    text_parts = []
    
    for page in pages:
        if page.get("title"):
            text_parts.append(page["title"])
        if page.get("meta_description"):
            text_parts.append(page["meta_description"])
        if page.get("h1"):
            text_parts.append(page["h1"])
        # Note: Full HTML text isn't stored in crawl output to save space
        # We rely on extracted fields
    
    return " ".join(text_parts)


def get_internal_link_count(pages: list[dict]) -> int:
    """Count internal pages crawled (excluding homepage)."""
    if not pages:
        return 0
    return max(0, len(pages) - 1)  # Subtract 1 for homepage


def score_site_maintenance_neglect(place: dict) -> tuple[int, list[str]]:
    """
    Score component A: Site Maintenance Neglect (0-30 pts).
    
    - Copyright year ≤ 2018: +10 pts
    - Copyright year ≤ 2015: +15 pts (replaces above)
    - Broken link ratio > 20%: +10 pts
    - Contact form/page issues: +5 pts
    """
    score = 0
    reasons = []
    pages = place.get("pages", [])
    
    if not pages:
        return 0, []
    
    homepage = pages[0] if pages else {}
    page_text = get_page_text(pages)
    
    # Copyright year check
    copyright_year = detect_copyright_year(page_text)
    current_year = datetime.now().year
    
    if copyright_year:
        years_old = current_year - copyright_year
        if copyright_year <= 2015:
            score += 15
            reasons.append(f"Copyright year {copyright_year} (very outdated)")
        elif copyright_year <= 2018:
            score += 10
            reasons.append(f"Copyright year {copyright_year} (outdated)")
    
    # Broken link ratio
    broken_ratio = calculate_broken_link_ratio(pages)
    if broken_ratio > 0.2:
        score += 10
        reasons.append(f"High broken link ratio ({broken_ratio:.0%})")
    
    # Contact page issues
    has_contact_page = homepage.get("has_contact_page", False)
    has_email = homepage.get("email_visible", False)
    
    if not has_contact_page and not has_email:
        score += 5
        reasons.append("No contact page or email visible")
    
    return min(30, score), reasons


def score_operational_modernity_gap(place: dict) -> tuple[int, list[str]]:
    """
    Score component B: Operational Modernity Gap (0-25 pts).
    
    - No HTTPS: +10 pts
    - No viewport meta tag: +10 pts
    - "Call for rates" language without pricing: +5 pts
    """
    score = 0
    reasons = []
    pages = place.get("pages", [])
    
    if not pages:
        return 0, []
    
    homepage = pages[0] if pages else {}
    page_text = get_page_text(pages)
    
    # No HTTPS
    if not homepage.get("has_https", True):
        score += 10
        reasons.append("No HTTPS (insecure)")
    
    # No viewport (not mobile-friendly)
    if not homepage.get("has_viewport", True):
        score += 10
        reasons.append("No viewport meta (not mobile-friendly)")
    
    # Call for rates with no pricing
    if detect_call_for_rates(page_text):
        score += 5
        reasons.append("'Call for rates' with no online pricing")
    
    return min(25, score), reasons


def score_listing_comms_friction(place: dict) -> tuple[int, list[str]]:
    """
    Score component C: Listing/Comms Friction (0-25 pts).
    
    - Lead has phone but site doesn't show it: +10 pts
    - No email AND no contact page: +10 pts
    - Thin site (≤2 internal links): +5 pts
    """
    score = 0
    reasons = []
    pages = place.get("pages", [])
    
    if not pages:
        # No website at all is handled by confidence, not friction score
        return 0, []
    
    homepage = pages[0] if pages else {}
    
    # Phone mismatch
    lead_has_phone = bool(place.get("phone"))
    site_shows_phone = homepage.get("phone_visible", False)
    
    if lead_has_phone and not site_shows_phone:
        score += 10
        reasons.append("Phone not visible on website")
    
    # No email AND no contact page
    has_email = homepage.get("email_visible", False)
    has_contact_page = homepage.get("has_contact_page", False)
    
    if not has_email and not has_contact_page:
        score += 10
        reasons.append("No email or contact page visible")
    
    # Thin site (few internal links crawled)
    internal_link_count = get_internal_link_count(pages)
    if internal_link_count <= 2:
        score += 5
        reasons.append(f"Thin site ({internal_link_count} internal pages)")
    
    return min(25, score), reasons


def score_customer_friction_signals(place: dict) -> tuple[int, list[str]]:
    """
    Score component D: Customer Friction Text Signals (0-20 pts).
    
    Keywords in reviews/testimonials indicating poor responsiveness.
    - 1 keyword hit: +5 pts
    - 2-3 keyword hits: +10 pts
    - 4+ keyword hits: +20 pts
    """
    score = 0
    reasons = []
    
    # Collect text from various sources
    text_sources = []
    
    # Google review snippets (if available in place data)
    if place.get("reviews"):
        for review in place.get("reviews", []):
            if isinstance(review, dict):
                text_sources.append(review.get("text", ""))
                text_sources.append(review.get("snippet", ""))
            elif isinstance(review, str):
                text_sources.append(review)
    
    # Page text (might contain testimonials)
    pages = place.get("pages", [])
    page_text = get_page_text(pages)
    text_sources.append(page_text)
    
    combined_text = " ".join(filter(None, text_sources))
    keyword_count = count_friction_keywords(combined_text)
    
    if keyword_count >= 4:
        score = 20
        reasons.append(f"Multiple friction signals detected ({keyword_count} matches)")
    elif keyword_count >= 2:
        score = 10
        reasons.append(f"Friction signals detected ({keyword_count} matches)")
    elif keyword_count >= 1:
        score = 5
        reasons.append(f"Friction signal detected ({keyword_count} match)")
    
    return min(20, score), reasons


def determine_confidence(place: dict) -> str:
    """
    Determine confidence level for the score.
    
    - high: Website crawled successfully + enough content
    - medium: Partial crawl or thin content
    - low: No website, crawl failed, or aggregator only
    """
    crawl_status = place.get("crawl_status", "")
    pages = place.get("pages", [])
    
    # Low confidence cases
    if crawl_status in ("no_website", "failed"):
        return "low"
    if place.get("is_aggregator") or place.get("is_facebook_only"):
        return "low"
    if not pages:
        return "low"
    
    # Check content amount
    page_text = get_page_text(pages)
    text_length = len(page_text)
    
    # High confidence: successful crawl with substantial content
    if crawl_status == "success" and text_length >= 100:
        # Also check if we got multiple pages
        if len(pages) >= 2:
            return "high"
        return "medium"
    
    # Medium confidence: some content but limited
    if crawl_status == "success":
        return "medium"
    
    return "low"


def score_owner_fatigue(place: dict) -> dict:
    """
    Score a single lead for owner fatigue.
    
    Returns place data with owner fatigue scoring fields added.
    """
    result = {
        "owner_fatigue_score_0_100": 0,
        "owner_fatigue_confidence": "low",
        "owner_fatigue_reasons_json": "[]",
        "owner_fatigue_breakdown_json": "{}",
    }
    
    # Handle no website / failed crawl cases
    crawl_status = place.get("crawl_status", "")
    
    if crawl_status == "no_website":
        # No website = moderate fatigue score (they're not investing in online presence)
        result["owner_fatigue_score_0_100"] = 40
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["No website - not investing in online presence"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 0,
            "operational_modernity_gap": 20,
            "listing_comms_friction": 15,
            "customer_friction_signals": 5,
        })
        return {**place, **result}
    
    if crawl_status == "failed":
        # Failed crawl = moderate fatigue score
        result["owner_fatigue_score_0_100"] = 35
        result["owner_fatigue_confidence"] = "low"
        notes = place.get("crawl_notes", "Unknown error")
        result["owner_fatigue_reasons_json"] = json.dumps([f"Website unreachable: {notes}"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 15,
            "operational_modernity_gap": 10,
            "listing_comms_friction": 10,
            "customer_friction_signals": 0,
        })
        return {**place, **result}
    
    if place.get("is_facebook_only"):
        # Facebook only = moderate-high fatigue
        result["owner_fatigue_score_0_100"] = 45
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["Facebook page only - no real website"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 10,
            "operational_modernity_gap": 15,
            "listing_comms_friction": 15,
            "customer_friction_signals": 5,
        })
        return {**place, **result}
    
    if place.get("is_aggregator"):
        # Aggregator only = moderate fatigue
        result["owner_fatigue_score_0_100"] = 35
        result["owner_fatigue_confidence"] = "low"
        result["owner_fatigue_reasons_json"] = json.dumps(["Aggregator listing only - no owned website"])
        result["owner_fatigue_breakdown_json"] = json.dumps({
            "site_maintenance_neglect": 10,
            "operational_modernity_gap": 10,
            "listing_comms_friction": 10,
            "customer_friction_signals": 5,
        })
        return {**place, **result}
    
    # Score each component
    all_reasons = []
    
    maintenance_score, maintenance_reasons = score_site_maintenance_neglect(place)
    modernity_score, modernity_reasons = score_operational_modernity_gap(place)
    friction_score, friction_reasons = score_listing_comms_friction(place)
    signals_score, signals_reasons = score_customer_friction_signals(place)
    
    all_reasons.extend(maintenance_reasons)
    all_reasons.extend(modernity_reasons)
    all_reasons.extend(friction_reasons)
    all_reasons.extend(signals_reasons)
    
    # Calculate total score
    total_score = maintenance_score + modernity_score + friction_score + signals_score
    
    breakdown = {
        "site_maintenance_neglect": maintenance_score,
        "operational_modernity_gap": modernity_score,
        "listing_comms_friction": friction_score,
        "customer_friction_signals": signals_score,
    }
    
    confidence = determine_confidence(place)
    
    result["owner_fatigue_score_0_100"] = min(100, total_score)
    result["owner_fatigue_confidence"] = confidence
    result["owner_fatigue_reasons_json"] = json.dumps(all_reasons)
    result["owner_fatigue_breakdown_json"] = json.dumps(breakdown)
    
    return {**place, **result}


def score_all_owner_fatigue(input_path: Path = INPUT_PATH) -> list[dict]:
    """Score all places for owner fatigue."""
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print("Run crawl_website.py and score_website.py first")
        sys.exit(1)
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    places = data.get("places", [])
    print(f"Loaded {len(places)} places to score for owner fatigue")
    print("-" * 50)
    
    scored = []
    for i, place in enumerate(places, 1):
        result = score_owner_fatigue(place)
        scored.append(result)
        
        name = place.get("name", "Unknown")[:30]
        fatigue_score = result["owner_fatigue_score_0_100"]
        confidence = result["owner_fatigue_confidence"]
        print(f"[{i}/{len(places)}] {name}: {fatigue_score}/100 ({confidence})")
    
    return scored


def save_owner_fatigue_scored(results: list[dict], output_path: Path = OUTPUT_PATH) -> Path:
    """Save owner fatigue scored results to JSON."""
    output_path.parent.mkdir(exist_ok=True)
    
    output_data = {
        "metadata": {
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(results),
            "score_type": "owner_fatigue",
        },
        "places": results,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved owner fatigue results to: {output_path}")
    return output_path


def main():
    results = score_all_owner_fatigue()
    save_owner_fatigue_scored(results)
    
    # Summary
    scores = [r["owner_fatigue_score_0_100"] for r in results]
    avg_score = sum(scores) / len(scores) if scores else 0
    high_fatigue = sum(1 for s in scores if s >= 60)
    medium_fatigue = sum(1 for s in scores if 30 <= s < 60)
    low_fatigue = sum(1 for s in scores if s < 30)
    
    print(f"\nOwner Fatigue Summary:")
    print(f"  Average score: {avg_score:.1f}/100")
    print(f"  High fatigue (≥60): {high_fatigue} ({100*high_fatigue/len(scores):.0f}%)")
    print(f"  Medium fatigue (30-59): {medium_fatigue} ({100*medium_fatigue/len(scores):.0f}%)")
    print(f"  Low fatigue (<30): {low_fatigue} ({100*low_fatigue/len(scores):.0f}%)")
    
    # Confidence breakdown
    high_conf = sum(1 for r in results if r["owner_fatigue_confidence"] == "high")
    med_conf = sum(1 for r in results if r["owner_fatigue_confidence"] == "medium")
    low_conf = sum(1 for r in results if r["owner_fatigue_confidence"] == "low")
    
    print(f"\nConfidence Breakdown:")
    print(f"  High: {high_conf}")
    print(f"  Medium: {med_conf}")
    print(f"  Low: {low_conf}")


if __name__ == "__main__":
    main()

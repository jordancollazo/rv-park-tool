"""
score_website.py
Deterministic website scoring (1-10 scale, no LLM).

Usage:
    python execution/score_website.py
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Paths
INPUT_PATH = Path(".tmp/crawled_sites.json")
OUTPUT_PATH = Path(".tmp/scored_sites.json")

# Scoring weights (must sum to 1.0)
WEIGHTS = {
    "technical": 0.15,
    "mobile": 0.15,
    "performance": 0.10,
    "conversion": 0.20,
    "trust": 0.10,
    "modernity": 0.10,
    "reviews": 0.20,  # NEW: Review-based scoring (high weight for acquisition targeting)
}


def score_technical(pages: list[dict]) -> tuple[int, list[str]]:
    """
    Score technical basics (0-10).
    Checks: HTTPS, title, meta description, H1.
    """
    if not pages:
        return 0, ["No pages crawled"]
    
    homepage = pages[0]
    score = 0
    reasons = []
    
    # HTTPS (2 points)
    if homepage.get("has_https"):
        score += 2
    else:
        reasons.append("No HTTPS")
    
    # Title (3 points)
    title = homepage.get("title")
    if title and len(title) > 5:
        score += 3
        if len(title) > 60:
            score -= 1
            reasons.append("Title too long")
    else:
        reasons.append("Missing or short title")
    
    # Meta description (3 points)
    meta_desc = homepage.get("meta_description")
    if meta_desc and len(meta_desc) > 20:
        score += 3
    else:
        reasons.append("Missing meta description")
    
    # H1 (2 points)
    if homepage.get("h1"):
        score += 2
    else:
        reasons.append("Missing H1")
    
    return min(10, score), reasons


def score_mobile(pages: list[dict]) -> tuple[int, list[str]]:
    """
    Score mobile usability (0-10).
    Checks: viewport meta tag.
    """
    if not pages:
        return 0, ["No pages crawled"]
    
    homepage = pages[0]
    score = 0
    reasons = []
    
    # Viewport (10 points - it's critical)
    if homepage.get("has_viewport"):
        score += 10
    else:
        reasons.append("No viewport meta tag (not mobile-friendly)")
    
    return min(10, score), reasons


def score_performance(pages: list[dict]) -> tuple[int, list[str]]:
    """
    Score performance (0-10).
    Checks: page size, load time.
    """
    if not pages:
        return 0, ["No pages crawled"]
    
    homepage = pages[0]
    score = 10  # Start high, deduct for issues
    reasons = []
    
    # Page size (deduct for large pages)
    page_size = homepage.get("page_size_bytes", 0)
    if page_size > 5_000_000:  # > 5MB
        score -= 5
        reasons.append("Page extremely large (>5MB)")
    elif page_size > 2_000_000:  # > 2MB
        score -= 3
        reasons.append("Page large (>2MB)")
    elif page_size > 1_000_000:  # > 1MB
        score -= 1
        reasons.append("Page moderately large (>1MB)")
    
    # Load time
    load_time = homepage.get("load_time_ms", 0)
    if load_time > 10000:  # > 10s
        score -= 4
        reasons.append("Very slow load time (>10s)")
    elif load_time > 5000:  # > 5s
        score -= 2
        reasons.append("Slow load time (>5s)")
    elif load_time > 3000:  # > 3s
        score -= 1
        reasons.append("Moderate load time (>3s)")
    
    return max(0, min(10, score)), reasons


def score_conversion(pages: list[dict]) -> tuple[int, list[str]]:
    """
    Score conversion clarity (0-10).
    Checks: phone visible, email visible, contact page.
    """
    if not pages:
        return 0, ["No pages crawled"]
    
    homepage = pages[0]
    score = 0
    reasons = []
    
    # Phone visible (4 points - highest priority)
    if homepage.get("phone_visible"):
        score += 4
    else:
        reasons.append("Phone not visible on homepage")
    
    # Email visible (2 points)
    if homepage.get("email_visible"):
        score += 2
    else:
        reasons.append("Email not visible")
    
    # Contact page (2 points)
    if homepage.get("has_contact_page"):
        score += 2
    else:
        reasons.append("No obvious contact page link")
    
    # Bonus for both phone and contact page
    if homepage.get("phone_visible") and homepage.get("has_contact_page"):
        score += 2
    
    return min(10, score), reasons


def score_trust(pages: list[dict]) -> tuple[int, list[str]]:
    """
    Score trust signals (0-10).
    Limited checks without full content analysis.
    """
    if not pages:
        return 0, ["No pages crawled"]
    
    # Base score - we can't deeply analyze content
    # Give moderate score by default
    score = 5
    reasons = ["Trust signals require manual review"]
    
    # Multiple pages crawled = slightly better
    if len(pages) >= 3:
        score += 2
        reasons.append("Multiple pages available")
    
    return min(10, score), reasons


def score_modernity(pages: list[dict]) -> tuple[int, list[str]]:
    """
    Score modernity (0-10).
    Checks for outdated patterns.
    """
    if not pages:
        return 0, ["No pages crawled"]
    
    homepage = pages[0]
    score = 7  # Default to moderate
    reasons = []
    
    # HTTPS is modern
    if homepage.get("has_https"):
        score += 1
    
    # Viewport = responsive design = modern
    if homepage.get("has_viewport"):
        score += 2
    else:
        score -= 2
        reasons.append("Non-responsive design indicates older site")
    
    return max(0, min(10, score)), reasons


def score_reviews(place: dict) -> tuple[int, list[str]]:
    """
    Score based on Google reviews (0-10).
    INVERSE SCORING: Lower ratings and fewer reviews = HIGHER score
    (indicates distressed/less established properties = better acquisition targets)
    """
    score = 5  # Base score
    reasons = []
    
    rating = place.get("google_rating") or place.get("rating")
    review_count = place.get("review_count")
    
    # No review data available
    if rating is None and review_count is None:
        return 5, ["No review data available"]
    
    # Rating-based scoring (INVERSE)
    if rating is not None:
        if rating < 2.5:
            score += 3
            reasons.append(f"Very low rating ({rating:.1f}★) - high acquisition potential")
        elif rating < 3.5:
            score += 2
            reasons.append(f"Low rating ({rating:.1f}★) - good acquisition target")
        elif rating < 4.0:
            score += 1
            reasons.append(f"Below-average rating ({rating:.1f}★)")
        elif rating >= 4.5:
            score -= 1
            reasons.append(f"High rating ({rating:.1f}★) - well-managed, competitive")
    
    # Review count-based scoring (INVERSE)
    if review_count is not None:
        if review_count < 10:
            score += 1
            reasons.append(f"Few reviews ({review_count}) - less established")
        elif review_count < 25:
            score += 0.5
        elif review_count > 100:
            score -= 0.5
            reasons.append(f"Many reviews ({review_count}) - well-established")
    
    return max(0, min(10, int(score))), reasons


def calculate_final_score(subscores: dict[str, int]) -> int:
    """Calculate weighted final score (1-10)."""
    weighted_sum = sum(
        subscores[key] * WEIGHTS[key]
        for key in WEIGHTS
    )
    # Convert to 1-10 scale, round to nearest int
    return max(1, min(10, round(weighted_sum)))


def score_website(place: dict) -> dict:
    """
    Score a single website.
    
    Returns place data with scoring fields added.
    """
    result = {
        "site_score_1_10": 1,
        "score_breakdown_json": {},
        "score_reasons": "",
    }
    
    # No website
    if place.get("crawl_status") == "no_website":
        result["site_score_1_10"] = 1
        result["score_reasons"] = "No website"
        return {**place, **result}
    
    # Facebook only
    if place.get("is_facebook_only"):
        result["site_score_1_10"] = 2
        result["score_reasons"] = "Facebook page only - no real website"
        return {**place, **result}
    
    # Aggregator only
    if place.get("is_aggregator"):
        result["site_score_1_10"] = 3
        result["score_reasons"] = "Aggregator/directory listing only"
        return {**place, **result}
    
    # Crawl failed
    if place.get("crawl_status") == "failed":
        result["site_score_1_10"] = 1
        result["score_reasons"] = f"Crawl failed: {place.get('crawl_notes', 'Unknown error')}"
        return {**place, **result}
    
    # Score each component
    pages = place.get("pages", [])
    all_reasons = []
    
    technical_score, technical_reasons = score_technical(pages)
    mobile_score, mobile_reasons = score_mobile(pages)
    performance_score, performance_reasons = score_performance(pages)
    conversion_score, conversion_reasons = score_conversion(pages)
    trust_score, trust_reasons = score_trust(pages)
    modernity_score, modernity_reasons = score_modernity(pages)
    reviews_score, reviews_reasons = score_reviews(place)  # NEW: Review-based scoring
    
    subscores = {
        "technical": technical_score,
        "mobile": mobile_score,
        "performance": performance_score,
        "conversion": conversion_score,
        "trust": trust_score,
        "modernity": modernity_score,
        "reviews": reviews_score,  # NEW
    }
    
    all_reasons.extend(technical_reasons)
    all_reasons.extend(mobile_reasons)
    all_reasons.extend(performance_reasons)
    all_reasons.extend(conversion_reasons)
    all_reasons.extend(modernity_reasons)
    all_reasons.extend(reviews_reasons)  # NEW
    
    final_score = calculate_final_score(subscores)
    
    result["site_score_1_10"] = final_score
    result["score_breakdown_json"] = subscores
    result["score_reasons"] = "; ".join(all_reasons) if all_reasons else "Good overall"
    
    return {**place, **result}


def score_all_websites(input_path: Path = INPUT_PATH) -> list[dict]:
    """Score all crawled websites."""
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print("Run crawl_website.py first")
        sys.exit(1)
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    places = data.get("places", [])
    print(f"Loaded {len(places)} places to score")
    print("-" * 50)
    
    scored = []
    for i, place in enumerate(places, 1):
        result = score_website(place)
        scored.append(result)
        
        name = place.get("name", "Unknown")[:30]
        score = result["site_score_1_10"]
        print(f"[{i}/{len(places)}] {name}: {score}/10")
    
    return scored


def save_scored(results: list[dict], output_path: Path = OUTPUT_PATH) -> Path:
    """Save scored results to JSON."""
    output_path.parent.mkdir(exist_ok=True)
    
    output_data = {
        "metadata": {
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "total_records": len(results),
        },
        "places": results,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved scored results to: {output_path}")
    return output_path


def main():
    results = score_all_websites()
    save_scored(results)
    
    # Summary
    scores = [r["site_score_1_10"] for r in results]
    avg_score = sum(scores) / len(scores) if scores else 0
    low_scores = sum(1 for s in scores if s <= 3)
    
    print(f"\nSummary:")
    print(f"  Average score: {avg_score:.1f}/10")
    print(f"  Low scores (≤3): {low_scores} ({100*low_scores/len(scores):.0f}%)")


if __name__ == "__main__":
    main()

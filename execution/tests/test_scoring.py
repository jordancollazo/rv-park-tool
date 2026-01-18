"""
test_scoring.py
Fixture-based tests for deterministic website scoring.

Run with: python -m pytest execution/tests/test_scoring.py -v
"""

import sys
from pathlib import Path

# Add execution directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from score_website import (
    score_technical,
    score_mobile,
    score_performance,
    score_conversion,
    score_website,
    calculate_final_score,
)


class TestScoreTechnical:
    """Tests for technical scoring component."""
    
    def test_empty_pages(self):
        """No pages = score 0."""
        score, reasons = score_technical([])
        assert score == 0
        assert "No pages crawled" in reasons
    
    def test_full_marks(self):
        """Page with all technical elements."""
        pages = [{
            "has_https": True,
            "title": "Example RV Park - Book Your Stay",
            "meta_description": "Best RV park in Florida with full hookups and amenities.",
            "h1": "Welcome to Example RV Park",
        }]
        score, reasons = score_technical(pages)
        assert score == 10
        assert len(reasons) == 0
    
    def test_no_https(self):
        """Missing HTTPS loses 2 points."""
        pages = [{
            "has_https": False,
            "title": "Example RV Park",
            "meta_description": "A great place to stay.",
            "h1": "Welcome",
        }]
        score, reasons = score_technical(pages)
        assert score == 8
        assert "No HTTPS" in reasons
    
    def test_missing_meta(self):
        """Missing meta description loses 3 points."""
        pages = [{
            "has_https": True,
            "title": "Example RV Park",
            "meta_description": None,
            "h1": "Welcome",
        }]
        score, reasons = score_technical(pages)
        assert score == 7
        assert "Missing meta description" in reasons


class TestScoreMobile:
    """Tests for mobile usability scoring."""
    
    def test_has_viewport(self):
        """Viewport meta = full marks."""
        pages = [{"has_viewport": True}]
        score, _ = score_mobile(pages)
        assert score == 10
    
    def test_no_viewport(self):
        """No viewport = 0."""
        pages = [{"has_viewport": False}]
        score, reasons = score_mobile(pages)
        assert score == 0
        assert any("viewport" in r.lower() for r in reasons)


class TestScorePerformance:
    """Tests for performance scoring."""
    
    def test_fast_small_page(self):
        """Small, fast page = full marks."""
        pages = [{
            "page_size_bytes": 500_000,  # 500KB
            "load_time_ms": 1000,  # 1s
        }]
        score, _ = score_performance(pages)
        assert score == 10
    
    def test_large_page(self):
        """Large page loses points."""
        pages = [{
            "page_size_bytes": 3_000_000,  # 3MB
            "load_time_ms": 1000,
        }]
        score, reasons = score_performance(pages)
        assert score < 10
        assert any("large" in r.lower() for r in reasons)


class TestScoreConversion:
    """Tests for conversion clarity scoring."""
    
    def test_all_elements(self):
        """Phone, email, contact page = full marks."""
        pages = [{
            "phone_visible": True,
            "email_visible": True,
            "has_contact_page": True,
        }]
        score, _ = score_conversion(pages)
        assert score == 10
    
    def test_no_phone(self):
        """No phone visible loses points."""
        pages = [{
            "phone_visible": False,
            "email_visible": True,
            "has_contact_page": True,
        }]
        score, reasons = score_conversion(pages)
        assert score < 10
        assert any("phone" in r.lower() for r in reasons)


class TestScoreWebsite:
    """Integration tests for full website scoring."""
    
    def test_no_website(self):
        """No website = score 1."""
        place = {"crawl_status": "no_website"}
        result = score_website(place)
        assert result["site_score_1_10"] == 1
        assert "No website" in result["score_reasons"]
    
    def test_facebook_only(self):
        """Facebook only = score 2."""
        place = {
            "crawl_status": "aggregator",
            "is_facebook_only": True,
        }
        result = score_website(place)
        assert result["site_score_1_10"] == 2
    
    def test_aggregator_only(self):
        """Aggregator = score 3."""
        place = {
            "crawl_status": "aggregator",
            "is_aggregator": True,
            "is_facebook_only": False,
        }
        result = score_website(place)
        assert result["site_score_1_10"] == 3
    
    def test_crawl_failed(self):
        """Failed crawl = score 1."""
        place = {
            "crawl_status": "failed",
            "crawl_notes": "Connection timeout",
        }
        result = score_website(place)
        assert result["site_score_1_10"] == 1


class TestCalculateFinalScore:
    """Tests for weighted final score calculation."""
    
    def test_all_tens(self):
        """All 10s = final 10."""
        subscores = {
            "technical": 10,
            "mobile": 10,
            "performance": 10,
            "conversion": 10,
            "trust": 10,
            "modernity": 10,
        }
        assert calculate_final_score(subscores) == 10
    
    def test_all_zeros(self):
        """All 0s = final 1 (minimum)."""
        subscores = {
            "technical": 0,
            "mobile": 0,
            "performance": 0,
            "conversion": 0,
            "trust": 0,
            "modernity": 0,
        }
        assert calculate_final_score(subscores) == 1
    
    def test_mixed_scores(self):
        """Mixed scores produce weighted average."""
        subscores = {
            "technical": 5,
            "mobile": 5,
            "performance": 5,
            "conversion": 5,
            "trust": 5,
            "modernity": 5,
        }
        assert calculate_final_score(subscores) == 5


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])

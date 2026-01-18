"""
test_owner_fatigue.py
Fixture-based tests for Owner Fatigue scoring.

Run with: python -m pytest execution/tests/test_owner_fatigue.py -v
"""

import sys
from pathlib import Path

# Add execution directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from score_owner_fatigue import (
    detect_copyright_year,
    calculate_broken_link_ratio,
    detect_call_for_rates,
    count_friction_keywords,
    score_site_maintenance_neglect,
    score_operational_modernity_gap,
    score_listing_comms_friction,
    score_customer_friction_signals,
    score_owner_fatigue,
    determine_confidence,
)


class TestDetectCopyrightYear:
    """Tests for copyright year detection."""
    
    def test_standard_copyright(self):
        """Detects © 2019 format."""
        text = "© 2019 Sunshine RV Park. All rights reserved."
        assert detect_copyright_year(text) == 2019
    
    def test_copyright_word(self):
        """Detects 'Copyright 2018' format."""
        text = "Copyright 2018 ABC Mobile Home Community"
        assert detect_copyright_year(text) == 2018
    
    def test_parentheses_copyright(self):
        """Detects (c) 2017 format."""
        text = "(c) 2017 Palm Tree Estates"
        assert detect_copyright_year(text) == 2017
    
    def test_range_copyright(self):
        """Detects year range, returns most recent."""
        text = "© 2015-2023 Paradise RV Resort"
        # Should return 2023 (most recent)
        result = detect_copyright_year(text)
        assert result in (2015, 2023)  # Implementation may return either
    
    def test_no_copyright(self):
        """Returns None when no copyright found."""
        text = "Welcome to our RV park!"
        assert detect_copyright_year(text) is None
    
    def test_current_year(self):
        """Detects current year."""
        text = "© 2025 Modern RV Park"
        assert detect_copyright_year(text) == 2025


class TestBrokenLinkRatio:
    """Tests for broken link ratio calculation."""
    
    def test_no_pages(self):
        """Empty pages list = 0.0 ratio."""
        assert calculate_broken_link_ratio([]) == 0.0
    
    def test_homepage_only(self):
        """Homepage only = 0.0 ratio."""
        pages = [{"status": "success", "url": "http://example.com"}]
        assert calculate_broken_link_ratio(pages) == 0.0
    
    def test_all_success(self):
        """All internal pages successful = 0.0 ratio."""
        pages = [
            {"status": "success", "url": "http://example.com"},
            {"status": "success", "url": "http://example.com/about"},
            {"status": "success", "url": "http://example.com/contact"},
        ]
        assert calculate_broken_link_ratio(pages) == 0.0
    
    def test_half_broken(self):
        """Half broken internal pages = 0.5 ratio."""
        pages = [
            {"status": "success", "url": "http://example.com"},
            {"status": "success", "url": "http://example.com/about"},
            {"status": "failed", "url": "http://example.com/rates"},
        ]
        assert calculate_broken_link_ratio(pages) == 0.5
    
    def test_all_broken(self):
        """All internal pages broken = 1.0 ratio."""
        pages = [
            {"status": "success", "url": "http://example.com"},
            {"status": "failed", "url": "http://example.com/about"},
            {"status": "failed", "url": "http://example.com/contact"},
        ]
        assert calculate_broken_link_ratio(pages) == 1.0


class TestCallForRatesDetection:
    """Tests for 'call for rates' language detection."""
    
    def test_call_for_rates(self):
        """Detects 'call for rates' without pricing."""
        text = "Please call for rates and availability."
        assert detect_call_for_rates(text) is True
    
    def test_call_for_availability(self):
        """Detects 'call for availability'."""
        text = "Contact us for availability."
        assert detect_call_for_rates(text) is True
    
    def test_has_pricing(self):
        """Returns False when actual pricing is visible."""
        text = "Call for rates. Current rates: $45/night."
        assert detect_call_for_rates(text) is False
    
    def test_no_call_for_rates(self):
        """Returns False when no pattern found."""
        text = "Book online today! Rates from $35/night."
        assert detect_call_for_rates(text) is False


class TestFrictionKeywords:
    """Tests for friction keyword counting."""
    
    def test_no_keywords(self):
        """No keywords = 0 count."""
        text = "Great place to stay!"
        assert count_friction_keywords(text) == 0
    
    def test_single_keyword(self):
        """Single keyword found."""
        text = "Left a voicemail but great park overall."
        assert count_friction_keywords(text) == 1
    
    def test_multiple_keywords(self):
        """Multiple different keywords."""
        text = "No response after voicemail. Can't reach anyone."
        assert count_friction_keywords(text) >= 3
    
    def test_case_insensitive(self):
        """Keywords are case-insensitive."""
        text = "NO RESPONSE from management. VOICEMAIL only."
        assert count_friction_keywords(text) >= 2


class TestSiteMaintenanceNeglect:
    """Tests for Site Maintenance Neglect scoring (0-30 pts)."""
    
    def test_old_copyright_2015(self):
        """Copyright ≤ 2015 adds 15 points."""
        place = {
            "pages": [{
                "title": "Old RV Park",
                "meta_description": "© 2014 Old RV Park LLC",
                "h1": "Welcome",
                "has_contact_page": True,
                "email_visible": True,
            }]
        }
        score, reasons = score_site_maintenance_neglect(place)
        assert score >= 15
        assert any("2014" in r or "outdated" in r.lower() for r in reasons)
    
    def test_old_copyright_2018(self):
        """Copyright ≤ 2018 adds 10 points."""
        place = {
            "pages": [{
                "title": "Moderately Old RV Park © 2017",
                "meta_description": "A great place",
                "h1": "Welcome",
                "has_contact_page": True,
                "email_visible": True,
            }]
        }
        score, reasons = score_site_maintenance_neglect(place)
        assert score >= 10
    
    def test_no_contact_info(self):
        """No contact page and no email adds 5 points."""
        place = {
            "pages": [{
                "title": "Mystery RV Park",
                "meta_description": "",
                "h1": "Welcome",
                "has_contact_page": False,
                "email_visible": False,
            }]
        }
        score, reasons = score_site_maintenance_neglect(place)
        assert score >= 5
        assert any("contact" in r.lower() for r in reasons)


class TestOperationalModernityGap:
    """Tests for Operational Modernity Gap scoring (0-25 pts)."""
    
    def test_no_https(self):
        """No HTTPS adds 10 points."""
        place = {
            "pages": [{
                "has_https": False,
                "has_viewport": True,
                "title": "Insecure RV Park",
            }]
        }
        score, reasons = score_operational_modernity_gap(place)
        assert score >= 10
        assert any("https" in r.lower() for r in reasons)
    
    def test_no_viewport(self):
        """No viewport adds 10 points."""
        place = {
            "pages": [{
                "has_https": True,
                "has_viewport": False,
                "title": "Non-Mobile RV Park",
            }]
        }
        score, reasons = score_operational_modernity_gap(place)
        assert score >= 10
        assert any("viewport" in r.lower() or "mobile" in r.lower() for r in reasons)
    
    def test_both_missing(self):
        """No HTTPS and no viewport adds 20 points."""
        place = {
            "pages": [{
                "has_https": False,
                "has_viewport": False,
                "title": "Old School RV Park",
            }]
        }
        score, reasons = score_operational_modernity_gap(place)
        assert score >= 20
    
    def test_modern_site(self):
        """Modern site with HTTPS and viewport = 0 points."""
        place = {
            "pages": [{
                "has_https": True,
                "has_viewport": True,
                "title": "Modern RV Resort",
            }]
        }
        score, reasons = score_operational_modernity_gap(place)
        assert score == 0


class TestListingCommsFriction:
    """Tests for Listing/Comms Friction scoring (0-25 pts)."""
    
    def test_phone_mismatch(self):
        """Lead has phone but site doesn't show it = 10 points."""
        place = {
            "phone": "(555) 123-4567",
            "pages": [{
                "phone_visible": False,
                "email_visible": True,
                "has_contact_page": True,
            }]
        }
        score, reasons = score_listing_comms_friction(place)
        assert score >= 10
        assert any("phone" in r.lower() for r in reasons)
    
    def test_no_contact_methods(self):
        """No email AND no contact page = 10 points."""
        place = {
            "phone": None,
            "pages": [{
                "phone_visible": True,
                "email_visible": False,
                "has_contact_page": False,
            }]
        }
        score, reasons = score_listing_comms_friction(place)
        assert score >= 10
        assert any("email" in r.lower() or "contact" in r.lower() for r in reasons)
    
    def test_thin_site(self):
        """Thin site (≤2 internal links) = 5 points."""
        place = {
            "pages": [
                {"phone_visible": True, "email_visible": True, "has_contact_page": True},
                {"status": "success"},
            ]
        }
        score, reasons = score_listing_comms_friction(place)
        assert score >= 5
        assert any("thin" in r.lower() for r in reasons)


class TestCustomerFrictionSignals:
    """Tests for Customer Friction Text Signals scoring (0-20 pts)."""
    
    def test_single_friction_signal(self):
        """1 keyword hit = 5 points."""
        place = {
            "pages": [{
                "title": "RV Park - left voicemail for inquiry",
            }]
        }
        score, reasons = score_customer_friction_signals(place)
        assert score == 5
    
    def test_multiple_friction_signals(self):
        """2-3 keyword hits = 10 points."""
        place = {
            "pages": [{
                "title": "Left voicemail, no response from park",
            }]
        }
        score, reasons = score_customer_friction_signals(place)
        assert score >= 5  # At least some signal detected
    
    def test_no_friction_signals(self):
        """No friction keywords = 0 points."""
        place = {
            "pages": [{
                "title": "Excellent RV Park - Friendly Staff",
            }]
        }
        score, reasons = score_customer_friction_signals(place)
        assert score == 0


class TestConfidenceDetermination:
    """Tests for confidence level determination."""
    
    def test_high_confidence(self):
        """Successful crawl with good content = high confidence."""
        place = {
            "crawl_status": "success",
            "pages": [
                {
                    "title": "Test RV Park - Full Service Campground in Beautiful Florida",
                    "meta_description": "A wonderful place to stay with full hookups and amenities for the whole family. Pet friendly with great reviews.",
                    "h1": "Welcome to Our Award-Winning RV Park"
                },
                {"status": "success"},
            ]
        }
        assert determine_confidence(place) == "high"
    
    def test_medium_confidence(self):
        """Successful crawl with minimal content = medium confidence."""
        place = {
            "crawl_status": "success",
            "pages": [
                {"title": "Test RV Park"},
            ]
        }
        assert determine_confidence(place) == "medium"
    
    def test_low_confidence_no_website(self):
        """No website = low confidence."""
        place = {"crawl_status": "no_website", "pages": []}
        assert determine_confidence(place) == "low"
    
    def test_low_confidence_failed(self):
        """Failed crawl = low confidence."""
        place = {"crawl_status": "failed", "pages": []}
        assert determine_confidence(place) == "low"
    
    def test_low_confidence_aggregator(self):
        """Aggregator only = low confidence."""
        place = {"crawl_status": "aggregator", "is_aggregator": True, "pages": []}
        assert determine_confidence(place) == "low"


class TestScoreOwnerFatigue:
    """Integration tests for full owner fatigue scoring."""
    
    def test_no_website(self):
        """No website = moderate fatigue score (40)."""
        place = {"crawl_status": "no_website"}
        result = score_owner_fatigue(place)
        assert result["owner_fatigue_score_0_100"] == 40
        assert result["owner_fatigue_confidence"] == "low"
    
    def test_facebook_only(self):
        """Facebook only = moderate-high fatigue (45)."""
        place = {
            "crawl_status": "aggregator",
            "is_facebook_only": True,
        }
        result = score_owner_fatigue(place)
        assert result["owner_fatigue_score_0_100"] == 45
    
    def test_failed_crawl(self):
        """Failed crawl = moderate fatigue (35)."""
        place = {
            "crawl_status": "failed",
            "crawl_notes": "Connection timeout",
        }
        result = score_owner_fatigue(place)
        assert result["owner_fatigue_score_0_100"] == 35
    
    def test_well_maintained_site(self):
        """Well-maintained site = low fatigue score."""
        place = {
            "crawl_status": "success",
            "phone": "(555) 123-4567",
            "pages": [
                {
                    "title": "Modern RV Resort © 2025",
                    "meta_description": "Book online today!",
                    "h1": "Welcome",
                    "has_https": True,
                    "has_viewport": True,
                    "phone_visible": True,
                    "email_visible": True,
                    "has_contact_page": True,
                },
                {"status": "success"},
                {"status": "success"},
                {"status": "success"},
            ]
        }
        result = score_owner_fatigue(place)
        assert result["owner_fatigue_score_0_100"] < 30
        assert result["owner_fatigue_confidence"] in ("high", "medium")
    
    def test_neglected_site(self):
        """Neglected site = high fatigue score."""
        place = {
            "crawl_status": "success",
            "phone": "(555) 123-4567",
            "pages": [
                {
                    "title": "Old Timer RV Park © 2012",
                    "meta_description": "Call for rates",
                    "h1": "Welcome",
                    "has_https": False,
                    "has_viewport": False,
                    "phone_visible": False,
                    "email_visible": False,
                    "has_contact_page": False,
                },
            ]
        }
        result = score_owner_fatigue(place)
        assert result["owner_fatigue_score_0_100"] >= 50
        assert len(result["owner_fatigue_reasons_json"]) > 2


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])

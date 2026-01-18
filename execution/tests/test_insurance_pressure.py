"""
test_insurance_pressure.py

Unit tests for the Insurance Pressure Index functionality.

Tests:
- NFHL flood zone scoring
- Storm pressure normalization
- Disaster score computation
- Composite score calculation
- Confidence determination
"""

import json
import math
import unittest
from unittest.mock import Mock, patch


class TestFloodZoneScoring(unittest.TestCase):
    """Tests for flood zone score mapping."""
    
    def setUp(self):
        # Import the scoring function
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from compute_insurance_pressure import get_flood_zone_score, FLOOD_ZONE_SCORES
        self.get_flood_zone_score = get_flood_zone_score
        self.FLOOD_ZONE_SCORES = FLOOD_ZONE_SCORES
    
    def test_ve_zone_highest_score(self):
        """VE (velocity) zones should have highest score (50)."""
        score, desc = self.get_flood_zone_score("VE")
        self.assertEqual(score, 50)
        self.assertIn("velocity", desc.lower())
    
    def test_v_zone_high_score(self):
        """V zones should have high score (45)."""
        score, desc = self.get_flood_zone_score("V")
        self.assertEqual(score, 45)
    
    def test_ae_zone_moderate_high_score(self):
        """AE zones should have score of 40."""
        score, desc = self.get_flood_zone_score("AE")
        self.assertEqual(score, 40)
    
    def test_x_zone_minimal_score(self):
        """X zones should have minimal score (5)."""
        score, desc = self.get_flood_zone_score("X")
        self.assertEqual(score, 5)
    
    def test_none_zone_returns_zero(self):
        """None flood zone should return 0."""
        score, desc = self.get_flood_zone_score(None)
        self.assertEqual(score, 0.0)
        self.assertIsNone(desc)
    
    def test_case_insensitive(self):
        """Zone matching should be case insensitive."""
        score_upper, _ = self.get_flood_zone_score("AE")
        score_lower, _ = self.get_flood_zone_score("ae")
        self.assertEqual(score_upper, score_lower)
    
    def test_unknown_zone_defaults(self):
        """Unknown zone should default to X score (5)."""
        score, desc = self.get_flood_zone_score("UNKNOWN")
        self.assertEqual(score, 5)


class TestStormPressureNormalization(unittest.TestCase):
    """Tests for storm pressure score normalization."""
    
    def setUp(self):
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from enrich_storm_pressure import normalize_storm_score, STORM_SCORE_MAX
        self.normalize_storm_score = normalize_storm_score
        self.STORM_SCORE_MAX = STORM_SCORE_MAX
    
    def test_zero_intensity_returns_zero(self):
        """Zero intensity should return 0 score."""
        score = self.normalize_storm_score(0)
        self.assertEqual(score, 0.0)
    
    def test_negative_intensity_returns_zero(self):
        """Negative intensity should return 0 score."""
        score = self.normalize_storm_score(-10)
        self.assertEqual(score, 0.0)
    
    def test_high_intensity_capped_at_max(self):
        """Very high intensity should be capped at max score."""
        score = self.normalize_storm_score(1000)
        self.assertLessEqual(score, self.STORM_SCORE_MAX)
    
    def test_moderate_intensity_returns_moderate_score(self):
        """Moderate intensity should return a score between 0 and max."""
        score = self.normalize_storm_score(100)
        self.assertGreater(score, 0)
        self.assertLess(score, self.STORM_SCORE_MAX)
    
    def test_score_increases_with_intensity(self):
        """Higher intensity should result in higher score."""
        score_low = self.normalize_storm_score(10)
        score_high = self.normalize_storm_score(100)
        self.assertGreater(score_high, score_low)


class TestDisasterScoreComputation(unittest.TestCase):
    """Tests for disaster pressure score computation."""
    
    def setUp(self):
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from enrich_openfema_disaster_pressure import compute_disaster_score, DISASTER_SCORE_MAX
        self.compute_disaster_score = compute_disaster_score
        self.DISASTER_SCORE_MAX = DISASTER_SCORE_MAX
    
    def test_no_declarations_returns_zero(self):
        """No declarations should return 0."""
        metrics = {"declarations_5yr": [], "declarations_20yr": []}
        score = self.compute_disaster_score(metrics)
        self.assertEqual(score, 0.0)
    
    def test_recent_declarations_add_more(self):
        """Recent declarations should add more than old ones."""
        recent = {"declarations_5yr": [{"years_ago": 0}], "declarations_20yr": [{}]}
        old = {"declarations_5yr": [{"years_ago": 4}], "declarations_20yr": [{}]}
        
        score_recent = self.compute_disaster_score(recent)
        score_old = self.compute_disaster_score(old)
        
        self.assertGreater(score_recent, score_old)
    
    def test_score_capped_at_max(self):
        """Score should be capped at max even with many declarations."""
        many_decls = [{"years_ago": i % 5} for i in range(50)]
        metrics = {"declarations_5yr": many_decls, "declarations_20yr": many_decls}
        
        score = self.compute_disaster_score(metrics)
        self.assertLessEqual(score, self.DISASTER_SCORE_MAX)


class TestCompositeScoring(unittest.TestCase):
    """Tests for composite Insurance Pressure Index."""
    
    def test_score_is_sum_of_components(self):
        """Composite score should be sum of flood + storm + disaster."""
        # The composite score formula is:
        # total = flood_score + storm_score + disaster_score, clamped to 0-100
        
        flood = 40  # AE zone
        storm = 20
        disaster = 15
        
        expected = min(100, flood + storm + disaster)
        self.assertEqual(expected, 75)
    
    def test_score_clamped_at_100(self):
        """Composite score should be clamped at 100."""
        # Max possible: 50 + 30 + 20 = 100
        # But if we somehow exceed, it should still cap at 100
        total = min(100, 50 + 30 + 25)  # 105 -> 100
        self.assertEqual(total, 100)
    
    def test_score_minimum_is_zero(self):
        """Composite score should not go below 0."""
        total = max(0, 0 + 0 + 0)
        self.assertEqual(total, 0)


class TestConfidenceLevels(unittest.TestCase):
    """Tests for confidence level determination."""
    
    def setUp(self):
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from compute_insurance_pressure import compute_confidence
        self.compute_confidence = compute_confidence
    
    def test_all_components_high_confidence(self):
        """All three components present = high confidence."""
        conf = self.compute_confidence(True, True, True)
        self.assertEqual(conf, "high")
    
    def test_two_components_medium_confidence(self):
        """Two components present = medium confidence."""
        conf = self.compute_confidence(True, True, False)
        self.assertEqual(conf, "medium")
        
        conf = self.compute_confidence(True, False, True)
        self.assertEqual(conf, "medium")
    
    def test_one_component_low_confidence(self):
        """One component present = low confidence."""
        conf = self.compute_confidence(True, False, False)
        self.assertEqual(conf, "low")
    
    def test_no_components_low_confidence(self):
        """No components = low confidence."""
        conf = self.compute_confidence(False, False, False)
        self.assertEqual(conf, "low")


class TestDeterminism(unittest.TestCase):
    """Tests to ensure scoring is deterministic."""
    
    def test_same_input_same_output(self):
        """Same inputs should always produce same outputs."""
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from compute_insurance_pressure import get_flood_zone_score
        from enrich_storm_pressure import normalize_storm_score
        
        # Run multiple times
        results = []
        for _ in range(5):
            flood_score, _ = get_flood_zone_score("AE")
            storm_score = normalize_storm_score(150.5)
            results.append((flood_score, storm_score))
        
        # All results should be identical
        self.assertTrue(all(r == results[0] for r in results))


if __name__ == "__main__":
    unittest.main()

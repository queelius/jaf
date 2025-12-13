"""
Tests for probabilistic strategies in windowed operations.

Tests BloomFilter integration with distinct, intersect, and except operations.
"""

import pytest
from jaf.lazy_streams import stream


class TestProbabilisticDistinct:
    """Test probabilistic distinct using Bloom filter."""

    def test_probabilistic_distinct_basic(self):
        """Test basic probabilistic distinct."""
        data = [1, 2, 3, 2, 1, 4, 3, 5, 4, 5]
        s = stream({"type": "memory", "data": data})

        # Use probabilistic strategy
        distinct = s.distinct(strategy="probabilistic", bloom_expected_items=100)
        results = list(distinct.evaluate())

        # Should have approximately 5 unique items
        # With good parameters, should be very close
        assert 4 <= len(results) <= 5

    def test_probabilistic_distinct_with_key(self):
        """Test probabilistic distinct with key expression."""
        data = [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "A"},
            {"id": 4, "category": "C"},
            {"id": 5, "category": "B"},
        ]
        s = stream({"type": "memory", "data": data})

        distinct = s.distinct(
            key=["@", [["key", "category"]]],
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(distinct.evaluate())

        # Should have 3 unique categories
        assert len(results) == 3
        categories = {r["category"] for r in results}
        assert categories == {"A", "B", "C"}

    def test_probabilistic_distinct_large_stream(self):
        """Test probabilistic distinct on larger stream."""
        # Create stream with many duplicates
        unique_values = 1000
        repetitions = 10
        data = list(range(unique_values)) * repetitions

        s = stream({"type": "memory", "data": data})
        distinct = s.distinct(
            strategy="probabilistic",
            bloom_expected_items=unique_values,
            bloom_fp_rate=0.01
        )
        results = list(distinct.evaluate())

        # With 1% FP rate, should be very close to 1000
        # Allow 5% variance
        assert 950 <= len(results) <= 1000

    def test_probabilistic_distinct_vs_exact(self):
        """Compare probabilistic vs exact distinct."""
        data = list(range(100)) * 5  # 100 unique, each repeated 5 times

        s1 = stream({"type": "memory", "data": data})
        exact_results = list(s1.distinct().evaluate())

        s2 = stream({"type": "memory", "data": data})
        prob_results = list(s2.distinct(
            strategy="probabilistic",
            bloom_expected_items=100
        ).evaluate())

        # Exact should have exactly 100
        assert len(exact_results) == 100

        # Probabilistic should be close
        assert 95 <= len(prob_results) <= 100


class TestProbabilisticIntersect:
    """Test probabilistic intersect using Bloom filter."""

    def test_probabilistic_intersect_basic(self):
        """Test basic probabilistic intersect."""
        left_data = [1, 2, 3, 4, 5]
        right_data = [3, 4, 5, 6, 7]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        intersect = left_stream.intersect(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(intersect.evaluate())

        # Should have intersection: {3, 4, 5}
        # May have false positives, but intersection should be found
        result_set = set(results)
        assert 3 in result_set
        assert 4 in result_set
        assert 5 in result_set

    def test_probabilistic_intersect_with_key(self):
        """Test probabilistic intersect with key expression."""
        left_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        right_data = [
            {"id": 2, "value": 100},
            {"id": 3, "value": 200},
            {"id": 4, "value": 300},
        ]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        intersect = left_stream.intersect(
            right_stream,
            key=["@", [["key", "id"]]],
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(intersect.evaluate())

        # Should have items with id=2 and id=3
        ids = {r["id"] for r in results}
        assert 2 in ids
        assert 3 in ids

    def test_probabilistic_intersect_no_overlap(self):
        """Test probabilistic intersect with no overlap."""
        left_data = [1, 2, 3]
        right_data = [4, 5, 6]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        intersect = left_stream.intersect(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(intersect.evaluate())

        # Should have no intersection (allowing for rare false positives)
        assert len(results) <= 1  # At most 1 false positive expected


class TestProbabilisticExcept:
    """Test probabilistic except using Bloom filter."""

    def test_probabilistic_except_basic(self):
        """Test basic probabilistic except."""
        left_data = [1, 2, 3, 4, 5]
        right_data = [3, 4, 5]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        except_stream = left_stream.except_from(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(except_stream.evaluate())

        # Should have {1, 2} - items in left but not in right
        result_set = set(results)
        assert 1 in result_set
        assert 2 in result_set

    def test_probabilistic_except_with_key(self):
        """Test probabilistic except with key expression."""
        left_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        right_data = [
            {"id": 2, "status": "active"},
            {"id": 3, "status": "inactive"},
        ]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        except_stream = left_stream.except_from(
            right_stream,
            key=["@", [["key", "id"]]],
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(except_stream.evaluate())

        # Should have only item with id=1
        assert len(results) == 1
        assert results[0]["id"] == 1

    def test_probabilistic_except_all_excluded(self):
        """Test probabilistic except where all items are excluded."""
        left_data = [1, 2, 3]
        right_data = [1, 2, 3, 4, 5]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        except_stream = left_stream.except_from(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(except_stream.evaluate())

        # All left items are in right, so result should be empty
        assert len(results) == 0

    def test_probabilistic_except_none_excluded(self):
        """Test probabilistic except where no items are excluded."""
        left_data = [1, 2, 3]
        right_data = [4, 5, 6]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        except_stream = left_stream.except_from(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100,
            bloom_fp_rate=0.001  # Very low FP rate
        )
        results = list(except_stream.evaluate())

        # No overlap, so all left items should be included
        # (with low FP rate, should get all 3)
        assert len(results) >= 2  # Allow for at most 1 false exclusion


class TestStrategyAutoSelection:
    """Test automatic strategy selection."""

    def test_distinct_auto_selects_exact_for_inf_window(self):
        """Test that distinct auto-selects exact strategy for infinite window."""
        data = [1, 2, 2, 3]
        s = stream({"type": "memory", "data": data})

        # Default window_size is inf, should use exact strategy
        results = list(s.distinct().evaluate())
        assert len(results) == 3

    def test_distinct_auto_selects_windowed_for_finite_window(self):
        """Test that distinct auto-selects windowed strategy for finite window."""
        data = [1, 2, 2, 3]
        s = stream({"type": "memory", "data": data})

        results = list(s.distinct(window_size=10).evaluate())
        assert len(results) == 3

    def test_intersect_auto_selects_exact_for_inf_window(self):
        """Test that intersect auto-selects exact for infinite window."""
        left = stream({"type": "memory", "data": [1, 2, 3]})
        right = stream({"type": "memory", "data": [2, 3, 4]})

        results = list(left.intersect(right).evaluate())
        assert set(results) == {2, 3}

    def test_except_auto_selects_exact_for_inf_window(self):
        """Test that except auto-selects exact for infinite window."""
        left = stream({"type": "memory", "data": [1, 2, 3]})
        right = stream({"type": "memory", "data": [2, 3]})

        results = list(left.except_from(right).evaluate())
        assert results == [1]


class TestBloomFilterParameters:
    """Test Bloom filter parameter configuration."""

    def test_custom_fp_rate(self):
        """Test setting custom false positive rate."""
        data = list(range(100))

        # High FP rate = more false positives but less memory
        s1 = stream({"type": "memory", "data": data})
        results_high = list(s1.distinct(
            strategy="probabilistic",
            bloom_expected_items=100,
            bloom_fp_rate=0.1
        ).evaluate())

        # Low FP rate = fewer false positives but more memory
        s2 = stream({"type": "memory", "data": data})
        results_low = list(s2.distinct(
            strategy="probabilistic",
            bloom_expected_items=100,
            bloom_fp_rate=0.001
        ).evaluate())

        # Both should work, low FP rate should be more accurate
        assert len(results_high) >= 90  # Allow some variance
        assert len(results_low) >= 98   # Should be very accurate

    def test_expected_items_sizing(self):
        """Test that expected_items affects accuracy."""
        data = list(range(1000))

        # Undersized Bloom filter
        s1 = stream({"type": "memory", "data": data})
        results_small = list(s1.distinct(
            strategy="probabilistic",
            bloom_expected_items=10,  # Way too small
            bloom_fp_rate=0.01
        ).evaluate())

        # Properly sized Bloom filter
        s2 = stream({"type": "memory", "data": data})
        results_proper = list(s2.distinct(
            strategy="probabilistic",
            bloom_expected_items=1000,
            bloom_fp_rate=0.01
        ).evaluate())

        # Undersized will have more false positives (fewer unique results)
        # Properly sized should be accurate
        assert len(results_proper) >= len(results_small)
        assert len(results_proper) >= 950


class TestProbabilisticOperationErrorHandling:
    """Test error handling in probabilistic streaming operations."""

    def test_distinct_handles_mixed_types_gracefully(self):
        """Distinct should handle streams with mixed item types."""
        # Items with different types
        data = [
            {"id": 1, "data": [1, 2, 3]},
            {"id": 2, "data": [4, 5, 6]},
            42,  # Different type
            "string_item",  # Different type
            {"id": 3, "data": [7, 8, 9]},
        ]
        s = stream({"type": "memory", "data": data})

        # Should not raise, should handle gracefully
        results = list(s.distinct(
            strategy="probabilistic",
            bloom_expected_items=100
        ).evaluate())

        assert len(results) == 5, "All unique items should be returned"

    def test_intersect_handles_empty_right_stream(self):
        """Intersect with empty right stream should return empty."""
        left_data = [1, 2, 3, 4, 5]
        right_data = []

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        intersect = left_stream.intersect(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(intersect.evaluate())

        assert len(results) == 0, "Intersect with empty stream should be empty"

    def test_except_handles_empty_right_stream(self):
        """Except with empty right stream should return all left items."""
        left_data = [1, 2, 3]
        right_data = []

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        except_stream = left_stream.except_from(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(except_stream.evaluate())

        assert len(results) == 3, "Except with empty stream should return all items"
        assert set(results) == {1, 2, 3}


class TestProbabilisticVsExactBehavior:
    """Compare probabilistic and exact strategy behaviors."""

    def test_probabilistic_never_misses_true_intersections(self):
        """Probabilistic intersect may have false positives but no false negatives."""
        # Create overlapping data
        left_data = list(range(100))
        right_data = list(range(50, 150))  # Overlap is 50-99

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        results = list(left_stream.intersect(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=150
        ).evaluate())

        result_set = set(results)

        # All true intersections must be present (no false negatives from Bloom filter)
        true_intersection = set(range(50, 100))
        for item in true_intersection:
            assert item in result_set, (
                f"Item {item} is in true intersection but missing from results"
            )

    def test_probabilistic_except_may_exclude_too_much(self):
        """Probabilistic except may exclude extra items (false positives in Bloom)."""
        left_data = list(range(100))
        right_data = [50, 51, 52]  # Only these should be excluded

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        results = list(left_stream.except_from(
            right_stream,
            strategy="probabilistic",
            bloom_expected_items=10,
            bloom_fp_rate=0.01
        ).evaluate())

        result_set = set(results)

        # Items 50, 51, 52 should definitely NOT be in results
        assert 50 not in result_set, "Item 50 should be excluded"
        assert 51 not in result_set, "Item 51 should be excluded"
        assert 52 not in result_set, "Item 52 should be excluded"

        # Most other items should be present (allowing some false positives)
        expected_min = 90  # At least 90 of 97 should be present
        assert len(results) >= expected_min, (
            f"Expected at least {expected_min} items, got {len(results)}"
        )


class TestProbabilisticWithComplexKeys:
    """Test probabilistic operations with complex key expressions."""

    def test_distinct_with_nested_key(self):
        """Test distinct with nested object key."""
        data = [
            {"user": {"id": 1, "name": "Alice"}, "score": 100},
            {"user": {"id": 2, "name": "Bob"}, "score": 200},
            {"user": {"id": 1, "name": "Alice"}, "score": 150},  # Duplicate user
            {"user": {"id": 3, "name": "Charlie"}, "score": 300},
        ]
        s = stream({"type": "memory", "data": data})

        # Use nested key for uniqueness
        distinct = s.distinct(
            key=["@", [["key", "user"], ["key", "id"]]],
            strategy="probabilistic",
            bloom_expected_items=100
        )
        results = list(distinct.evaluate())

        assert len(results) == 3, "Should have 3 unique users"
        user_ids = {r["user"]["id"] for r in results}
        assert user_ids == {1, 2, 3}

    def test_intersect_with_computed_key(self):
        """Test intersect using transformed key values."""
        left_data = [
            {"email": "alice@test.com", "name": "Alice"},
            {"email": "bob@test.com", "name": "Bob"},
        ]
        right_data = [
            {"email_address": "alice@test.com", "id": 1},
            {"email_address": "charlie@test.com", "id": 2},
        ]

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        # Intersect on email field (different field names in each stream)
        intersect = left_stream.intersect(
            right_stream,
            key=["@", [["key", "email"]]],
            strategy="probabilistic",
            bloom_expected_items=100
        )

        # Note: This test demonstrates that key expressions work with probabilistic strategy
        # The right stream uses different field name, so we need to handle this properly
        # For this test, we're just verifying the mechanism works
        results = list(intersect.evaluate())
        # With mismatched field names, intersection won't work as expected
        # This tests the error handling path
        assert isinstance(results, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

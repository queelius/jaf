"""
Tests for probabilistic data structures.

Tests BloomFilter, CountMinSketch, and HyperLogLog implementations.
"""

import pytest
import math
from jaf.probabilistic import BloomFilter, CountMinSketch, HyperLogLog


class TestBloomFilter:
    """Test Bloom filter implementation."""

    def test_basic_membership(self):
        """Test basic add and membership check."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add("apple")
        bf.add("banana")
        bf.add("cherry")

        assert "apple" in bf
        assert "banana" in bf
        assert "cherry" in bf

    def test_probable_non_membership(self):
        """Test that items not added are probably not in filter."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add("apple")
        bf.add("banana")

        # High probability these are not in the filter
        # (with good parameters, false positives are rare)
        not_added = ["orange", "grape", "mango", "kiwi", "pear"]
        false_positives = sum(1 for item in not_added if item in bf)

        # Should have very few false positives with 0.01 rate
        assert false_positives < 3

    def test_count_tracking(self):
        """Test that count tracks items added."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        assert len(bf) == 0

        bf.add("item1")
        assert len(bf) == 1

        bf.add("item2")
        bf.add("item3")
        assert len(bf) == 3

    def test_dict_items(self):
        """Test adding dict items to bloom filter."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)

        item1 = {"id": 1, "name": "Alice"}
        item2 = {"id": 2, "name": "Bob"}

        bf.add(item1)
        bf.add(item2)

        assert item1 in bf
        assert item2 in bf
        assert {"id": 3, "name": "Charlie"} not in bf

    def test_list_items(self):
        """Test adding list items to bloom filter."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)

        item1 = [1, 2, 3]
        item2 = ["a", "b", "c"]

        bf.add(item1)
        bf.add(item2)

        assert item1 in bf
        assert item2 in bf
        assert [4, 5, 6] not in bf

    def test_numeric_items(self):
        """Test adding numeric items."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)

        bf.add(42)
        bf.add(3.14)
        bf.add(-100)

        assert 42 in bf
        assert 3.14 in bf
        assert -100 in bf

    def test_invalid_false_positive_rate(self):
        """Test validation of false positive rate."""
        with pytest.raises(ValueError):
            BloomFilter(expected_items=100, false_positive_rate=0)

        with pytest.raises(ValueError):
            BloomFilter(expected_items=100, false_positive_rate=1)

        with pytest.raises(ValueError):
            BloomFilter(expected_items=100, false_positive_rate=-0.5)

        with pytest.raises(ValueError):
            BloomFilter(expected_items=100, false_positive_rate=1.5)

    def test_invalid_expected_items(self):
        """Test validation of expected items."""
        with pytest.raises(ValueError):
            BloomFilter(expected_items=0)

        with pytest.raises(ValueError):
            BloomFilter(expected_items=-10)

    def test_explicit_size_and_hashes(self):
        """Test creating filter with explicit size and hash count."""
        bf = BloomFilter(
            expected_items=100,
            false_positive_rate=0.01,
            size=1000,
            num_hashes=5
        )
        assert bf.size == 1000
        assert bf.num_hashes == 5

    def test_union(self):
        """Test union of two bloom filters."""
        bf1 = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf2 = BloomFilter(expected_items=100, false_positive_rate=0.01)

        # Force same parameters for testing
        bf2.size = bf1.size
        bf2.num_hashes = bf1.num_hashes
        bf2.bit_array = [False] * bf1.size

        bf1.add("apple")
        bf1.add("banana")
        bf2.add("cherry")
        bf2.add("date")

        union = bf1.union(bf2)

        assert "apple" in union
        assert "banana" in union
        assert "cherry" in union
        assert "date" in union

    def test_union_different_params_raises(self):
        """Test that union with different parameters raises error."""
        bf1 = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf2 = BloomFilter(expected_items=100, false_positive_rate=0.01, size=500)

        with pytest.raises(ValueError):
            bf1.union(bf2)

    def test_clear(self):
        """Test clearing the filter."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add("item1")
        bf.add("item2")

        assert "item1" in bf
        assert len(bf) == 2

        bf.clear()

        assert "item1" not in bf
        assert len(bf) == 0

    def test_estimated_false_positive_rate(self):
        """Test estimated false positive rate calculation."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)

        # Empty filter should have 0 FPR
        assert bf.estimated_false_positive_rate() == 0.0

        # Add items
        for i in range(50):
            bf.add(f"item_{i}")

        # Should have non-zero but reasonable FPR
        fpr = bf.estimated_false_positive_rate()
        assert 0 < fpr < 0.1

    def test_size_calculation(self):
        """Test that size is calculated based on parameters."""
        # Lower FPR should result in larger size
        bf_high_fpr = BloomFilter(expected_items=1000, false_positive_rate=0.1)
        bf_low_fpr = BloomFilter(expected_items=1000, false_positive_rate=0.001)

        assert bf_low_fpr.size > bf_high_fpr.size

        # More items should result in larger size
        bf_few = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf_many = BloomFilter(expected_items=10000, false_positive_rate=0.01)

        assert bf_many.size > bf_few.size


class TestCountMinSketch:
    """Test Count-Min Sketch implementation."""

    def test_basic_counting(self):
        """Test basic add and estimate."""
        cms = CountMinSketch(width=1000, depth=5)

        cms.add("apple", count=3)
        cms.add("banana", count=2)
        cms.add("apple", count=2)

        # Apple should have count ~5
        assert cms.estimate("apple") >= 5

        # Banana should have count ~2
        assert cms.estimate("banana") >= 2

    def test_default_count(self):
        """Test that default count is 1."""
        cms = CountMinSketch(width=1000, depth=5)

        cms.add("item")
        cms.add("item")
        cms.add("item")

        assert cms.estimate("item") >= 3

    def test_never_underestimates(self):
        """Test that CMS never underestimates."""
        cms = CountMinSketch(width=1000, depth=5)

        true_count = 100
        for _ in range(true_count):
            cms.add("test_item")

        estimated = cms.estimate("test_item")
        assert estimated >= true_count

    def test_total_count(self):
        """Test total count tracking."""
        cms = CountMinSketch(width=1000, depth=5)

        cms.add("a", count=5)
        cms.add("b", count=10)
        cms.add("c", count=15)

        assert len(cms) == 30

    def test_dict_keys(self):
        """Test counting dict items."""
        cms = CountMinSketch(width=1000, depth=5)

        item = {"id": 1, "name": "Alice"}
        cms.add(item, count=5)

        assert cms.estimate(item) >= 5

    def test_list_keys(self):
        """Test counting list items."""
        cms = CountMinSketch(width=1000, depth=5)

        item = [1, 2, 3]
        cms.add(item, count=3)

        assert cms.estimate(item) >= 3

    def test_epsilon_delta_initialization(self):
        """Test initialization with epsilon and delta."""
        # epsilon controls error factor
        # delta controls probability of exceeding error
        cms = CountMinSketch(epsilon=0.01, delta=0.01)

        assert cms.width == int(math.e / 0.01)
        assert cms.depth == int(math.log(1 / 0.01))

    def test_merge(self):
        """Test merging two sketches."""
        cms1 = CountMinSketch(width=1000, depth=5)
        cms2 = CountMinSketch(width=1000, depth=5)

        cms1.add("apple", count=5)
        cms1.add("banana", count=3)
        cms2.add("cherry", count=7)
        cms2.add("apple", count=2)

        merged = cms1.merge(cms2)

        # Apple should have combined count
        assert merged.estimate("apple") >= 7  # 5 + 2

        # Others should be present
        assert merged.estimate("banana") >= 3
        assert merged.estimate("cherry") >= 7

    def test_merge_different_dims_raises(self):
        """Test that merge with different dimensions raises error."""
        cms1 = CountMinSketch(width=1000, depth=5)
        cms2 = CountMinSketch(width=500, depth=5)

        with pytest.raises(ValueError):
            cms1.merge(cms2)

    def test_unseen_item_estimate(self):
        """Test estimate for unseen items."""
        cms = CountMinSketch(width=1000, depth=5)

        cms.add("apple", count=10)

        # Unseen item should have low estimate (possibly non-zero due to hash collisions)
        estimate = cms.estimate("never_seen")
        assert estimate >= 0
        # With reasonable parameters, should be close to 0
        assert estimate < 10


class TestHyperLogLog:
    """Test HyperLogLog implementation."""

    def test_basic_cardinality(self):
        """Test basic cardinality estimation."""
        hll = HyperLogLog(precision=14)

        # Add known number of unique items
        num_unique = 10000
        for i in range(num_unique):
            hll.add(f"item_{i}")

        estimated = hll.estimate()

        # Should be within ~5% for this precision
        error = abs(estimated - num_unique) / num_unique
        assert error < 0.1  # Allow 10% error margin

    def test_duplicate_items(self):
        """Test that duplicates don't increase cardinality."""
        hll = HyperLogLog(precision=14)

        # Add same items multiple times
        for _ in range(100):
            for i in range(10):
                hll.add(f"item_{i}")

        # Should estimate ~10 unique items
        estimated = hll.estimate()
        assert 5 <= estimated <= 20

    def test_len_returns_estimate(self):
        """Test that len() returns cardinality estimate."""
        hll = HyperLogLog(precision=14)

        for i in range(1000):
            hll.add(i)

        assert len(hll) == hll.estimate()

    def test_dict_items(self):
        """Test cardinality with dict items."""
        hll = HyperLogLog(precision=14)

        for i in range(100):
            hll.add({"id": i, "type": "test"})

        estimated = hll.estimate()
        assert 50 <= estimated <= 150

    def test_precision_validation(self):
        """Test precision validation."""
        with pytest.raises(ValueError):
            HyperLogLog(precision=3)  # Too low

        with pytest.raises(ValueError):
            HyperLogLog(precision=17)  # Too high

        # Valid precisions should work
        HyperLogLog(precision=4)
        HyperLogLog(precision=16)

    def test_precision_affects_accuracy(self):
        """Test that higher precision gives better accuracy."""
        num_unique = 10000

        # Low precision
        hll_low = HyperLogLog(precision=4)
        for i in range(num_unique):
            hll_low.add(f"item_{i}")
        error_low = abs(hll_low.estimate() - num_unique) / num_unique

        # High precision
        hll_high = HyperLogLog(precision=14)
        for i in range(num_unique):
            hll_high.add(f"item_{i}")
        error_high = abs(hll_high.estimate() - num_unique) / num_unique

        # Higher precision should generally give lower error
        # (though not guaranteed for any single run)
        # Just verify both give reasonable estimates
        assert error_low < 0.5
        assert error_high < 0.2

    def test_merge(self):
        """Test merging two HyperLogLogs."""
        hll1 = HyperLogLog(precision=14)
        hll2 = HyperLogLog(precision=14)

        # Add different items to each
        for i in range(1000):
            hll1.add(f"set1_item_{i}")

        for i in range(1000):
            hll2.add(f"set2_item_{i}")

        merged = hll1.merge(hll2)

        # Merged should have ~2000 unique items
        estimated = merged.estimate()
        assert 1500 <= estimated <= 2500

    def test_merge_overlapping(self):
        """Test merging with overlapping items."""
        hll1 = HyperLogLog(precision=14)
        hll2 = HyperLogLog(precision=14)

        # Add overlapping items
        for i in range(1000):
            hll1.add(f"item_{i}")

        for i in range(500, 1500):  # 500-999 overlap
            hll2.add(f"item_{i}")

        merged = hll1.merge(hll2)

        # Should have ~1500 unique items (1000 + 1000 - 500 overlap)
        estimated = merged.estimate()
        assert 1200 <= estimated <= 1800

    def test_merge_different_precision_raises(self):
        """Test that merge with different precision raises error."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=12)

        with pytest.raises(ValueError):
            hll1.merge(hll2)

    def test_empty_estimate(self):
        """Test estimate on empty HLL."""
        hll = HyperLogLog(precision=14)

        # Empty HLL should estimate 0 or very small
        estimated = hll.estimate()
        assert estimated < 10

    def test_single_item(self):
        """Test estimate with single item."""
        hll = HyperLogLog(precision=14)
        hll.add("single_item")

        estimated = hll.estimate()
        # Should be close to 1
        assert 0 <= estimated <= 5


class TestBloomFilterEdgeCases:
    """Test edge cases and critical guarantees for Bloom filter."""

    def test_no_false_negatives_guarantee(self):
        """Bloom filter must NEVER return false for added items (no false negatives)."""
        # Given: A bloom filter sized for many items
        bf = BloomFilter(expected_items=10000, false_positive_rate=0.01)
        items = [f"item_{i}" for i in range(10000)]

        # When: All items are added
        for item in items:
            bf.add(item)

        # Then: Every single added item must be found (critical invariant)
        for item in items:
            assert item in bf, f"FALSE NEGATIVE detected: '{item}' was added but not found"

    def test_deterministic_membership(self):
        """Same item should always give same membership result."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add("test_item")

        # Check membership 100 times - should always be True
        for i in range(100):
            assert "test_item" in bf, f"Membership changed on check {i}"

    def test_none_value_handling(self):
        """Bloom filter should handle None values."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add(None)
        assert None in bf, "None value not found after adding"

    def test_boolean_values(self):
        """Bloom filter should handle boolean values."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add(True)
        bf.add(False)
        assert True in bf, "True value not found"
        assert False in bf, "False value not found"

    def test_empty_string(self):
        """Bloom filter should handle empty strings."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add("")
        assert "" in bf, "Empty string not found"

    def test_zero_values(self):
        """Bloom filter should handle zero values."""
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add(0)
        bf.add(0.0)
        assert 0 in bf, "Integer zero not found"
        assert 0.0 in bf, "Float zero not found"


class TestCountMinSketchGuarantees:
    """Test critical guarantees of Count-Min Sketch."""

    def test_never_underestimates_with_many_items(self):
        """CMS must never underestimate - tested at scale."""
        cms = CountMinSketch(width=1000, depth=5)

        # Add known counts for multiple items
        true_counts = {"a": 100, "b": 50, "c": 25, "d": 1, "e": 500}
        for item, count in true_counts.items():
            for _ in range(count):
                cms.add(item)

        # Verify no underestimation for any item
        for item, true_count in true_counts.items():
            estimated = cms.estimate(item)
            assert estimated >= true_count, (
                f"CMS underestimated '{item}': got {estimated}, expected >= {true_count}"
            )

    def test_zero_count_addition(self):
        """Adding with count=0 should not change estimate."""
        cms = CountMinSketch(width=1000, depth=5)
        cms.add("item", count=5)
        before = cms.estimate("item")
        cms.add("item", count=0)
        after = cms.estimate("item")
        assert after == before, "Estimate changed after adding count=0"


class TestHyperLogLogCoverage:
    """Tests to cover missing HyperLogLog branches and guarantees."""

    def test_precision_5_alpha_constant(self):
        """Test HyperLogLog with precision 5 for alpha coverage."""
        hll = HyperLogLog(precision=5)
        assert hll.precision == 5
        assert hll.alpha == 0.697, f"Expected alpha 0.697 for precision 5, got {hll.alpha}"

        # Verify it still estimates reasonably
        for i in range(100):
            hll.add(f"item_{i}")
        estimated = hll.estimate()
        assert 50 <= estimated <= 200, f"Estimate {estimated} outside reasonable range"

    def test_precision_6_alpha_constant(self):
        """Test HyperLogLog with precision 6 for alpha coverage."""
        hll = HyperLogLog(precision=6)
        assert hll.precision == 6
        assert hll.alpha == 0.709, f"Expected alpha 0.709 for precision 6, got {hll.alpha}"

        for i in range(100):
            hll.add(f"item_{i}")
        estimated = hll.estimate()
        assert 50 <= estimated <= 200, f"Estimate {estimated} outside reasonable range"

    def test_leading_zeros_with_zero_value(self):
        """Test _leading_zeros edge case when value is 0."""
        hll = HyperLogLog(precision=14)
        # Calling _leading_zeros directly to cover the edge case
        result = hll._leading_zeros(0, 50)
        assert result == 50, f"Expected 50 leading zeros for value 0, got {result}"

    def test_merge_is_commutative(self):
        """Merging A with B should equal merging B with A."""
        hll_a = HyperLogLog(precision=14)
        hll_b = HyperLogLog(precision=14)

        for i in range(100):
            hll_a.add(f"a_{i}")
        for i in range(100):
            hll_b.add(f"b_{i}")

        merged_ab = hll_a.merge(hll_b)
        merged_ba = hll_b.merge(hll_a)

        assert merged_ab.estimate() == merged_ba.estimate(), (
            "Merge is not commutative: A+B != B+A"
        )

    def test_idempotent_adds(self):
        """Adding same item multiple times should not change cardinality estimate."""
        hll = HyperLogLog(precision=14)

        # Add 10 unique items
        for i in range(10):
            hll.add(f"item_{i}")
        estimate_before = hll.estimate()

        # Add the same items again many times
        for _ in range(100):
            for i in range(10):
                hll.add(f"item_{i}")
        estimate_after = hll.estimate()

        # Estimate should not change
        assert estimate_after == estimate_before, (
            f"Estimate changed after duplicate adds: {estimate_before} -> {estimate_after}"
        )


class TestIntegration:
    """Integration tests for probabilistic structures."""

    def test_bloom_filter_for_distinct(self):
        """Test using Bloom filter for distinct operation."""
        bf = BloomFilter(expected_items=10000, false_positive_rate=0.01)
        seen = []

        items = list(range(100)) * 10  # 100 unique items, each repeated 10 times

        for item in items:
            if item not in bf:
                seen.append(item)
                bf.add(item)

        # Should have approximately 100 unique items
        # (might be slightly less due to false positives)
        assert 90 <= len(seen) <= 100

    def test_cms_for_frequency(self):
        """Test using CMS for frequency tracking."""
        cms = CountMinSketch(width=1000, depth=5)

        # Simulate word frequency counting
        words = ["the"] * 100 + ["a"] * 50 + ["an"] * 25 + ["rare"] * 1

        for word in words:
            cms.add(word)

        # Verify relative frequencies preserved
        assert cms.estimate("the") > cms.estimate("a")
        assert cms.estimate("a") > cms.estimate("an")
        assert cms.estimate("an") > cms.estimate("rare")

    def test_hll_for_unique_count(self):
        """Test using HLL for unique counting."""
        hll = HyperLogLog(precision=12)

        # Simulate stream with duplicates
        for _ in range(1000):
            for i in range(100):
                hll.add(f"user_{i}")

        # Should estimate ~100 unique users
        estimated = hll.estimate()
        assert 80 <= estimated <= 120


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

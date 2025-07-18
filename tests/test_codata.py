"""
Test codata/infinite collections and lazy streaming operations.
"""

import pytest
import time
from jaf.lazy_streams import stream
from jaf.streaming_loader import StreamingLoader


class TestCodataLoaders:
    """Test infinite/codata collection loaders"""

    def test_prng_basic(self):
        """Test basic PRNG data generation"""
        source = {
            "type": "prng",
            "seed": 42,
            "template": {
                "id": {"$random": "int", "min": 1, "max": 100},
                "name": {"$random": "choice", "choices": ["Alice", "Bob", "Charlie"]},
                "active": {"$random": "bool"},
            },
            "limit": 10,  # For testing, limit to 10 items
        }

        result = stream(source)
        items = list(result.evaluate())

        assert len(items) == 10
        # Check structure
        for item in items:
            assert "id" in item
            assert "name" in item
            assert "active" in item
            assert 1 <= item["id"] <= 100
            assert item["name"] in ["Alice", "Bob", "Charlie"]
            assert isinstance(item["active"], bool)

        # Test determinism with same seed
        result2 = stream(source)
        items2 = list(result2.take(10).evaluate())
        assert items == items2

    def test_fibonacci_stream(self):
        """Test Fibonacci sequence generation"""
        source = {"type": "fibonacci", "include_metadata": True}

        # Get first 10 Fibonacci numbers
        result = stream(source)
        items = list(result.take(10).evaluate())

        assert len(items) == 10
        # Check first few values
        assert items[0]["value"] == 0
        assert items[1]["value"] == 1
        assert items[2]["value"] == 1
        assert items[3]["value"] == 2
        assert items[4]["value"] == 3
        assert items[5]["value"] == 5

        # Check metadata
        assert items[0]["index"] == 0
        assert items[5]["digits"] == 1
        assert items[5]["is_even"] == False

    def test_time_series_patterns(self):
        """Test time series generation with patterns"""
        source = {
            "type": "time_series",
            "pattern": "sine",
            "interval_seconds": 1,
            "noise": 0,
            "start_time": "2024-01-01T00:00:00",
        }

        result = stream(source)
        items = list(result.take(5).evaluate())

        assert len(items) == 5
        for i, item in enumerate(items):
            assert "timestamp" in item
            assert "value" in item
            assert "step" in item
            assert item["step"] == i
            # With sine pattern, values should oscillate around 50
            assert 30 <= item["value"] <= 70

    def test_prime_stream(self):
        """Test prime number generation"""
        source = {"type": "prime", "include_gaps": True}

        # Find primes with last digit 7
        s = stream(source)
        filtered = s.filter(["eq?", ["@", [["key", "last_digit"]]], 7])
        primes_ending_in_7 = list(filtered.take(5).evaluate())

        assert len(primes_ending_in_7) == 5
        assert all(p["last_digit"] == 7 for p in primes_ending_in_7)
        # First few primes ending in 7: 7, 17, 37, 47, 67
        assert primes_ending_in_7[0]["value"] == 7
        assert primes_ending_in_7[1]["value"] == 17
        assert primes_ending_in_7[2]["value"] == 37

    def test_composite_streams(self):
        """Test composing multiple streams"""
        source = {
            "type": "composite",
            "mode": "zip",
            "sources": [{"type": "fibonacci"}, {"type": "prime"}],
            "transform": {
                "fib_value": {"$source": 0, "field": "value"},
                "prime_value": {"$source": 1, "field": "value"},
                "product": {"$source": 0},  # Will get whole fib object for now
            },
        }

        result = stream(source)
        items = list(result.take(5).evaluate())

        assert len(items) == 5
        for item in items:
            assert "fib_value" in item
            assert "prime_value" in item


class TestLazyOperations:
    """Test lazy streaming operations on query sets"""

    def test_take_operation(self):
        """Test take() limits results"""
        source = {
            "type": "prng",
            "seed": 123,
            "template": {"n": {"$random": "int", "min": 1, "max": 1000}},
        }

        result = stream(source)
        items = list(result.take(5).evaluate())

        assert len(items) == 5

    def test_skip_operation(self):
        """Test skip() skips initial items"""
        source = {"type": "fibonacci"}

        result = stream(source)
        items = list(result.skip(5).take(3).evaluate())

        assert len(items) == 3
        # After skipping 0,1,1,2,3 we should get 5,8,13
        assert items[0]["value"] == 5
        assert items[1]["value"] == 8
        assert items[2]["value"] == 13

    def test_slice_operation(self):
        """Test slice() for range selection"""
        source = {"type": "prime"}

        result = stream(source)
        # Get primes from index 10 to 15
        items = list(result.slice(10, 15).evaluate())

        assert len(items) == 5
        # 11th prime is 31, 15th is 47
        assert items[0]["value"] == 31
        assert items[-1]["value"] == 47

    def test_take_while_operation(self):
        """Test take_while() with predicate"""
        source = {"type": "fibonacci"}

        result = stream(source)
        # Take while value < 100
        items = list(
            result.take_while(["lt?", ["@", [["key", "value"]]], 100]).evaluate()
        )

        # Should get 0,1,1,2,3,5,8,13,21,34,55,89
        assert len(items) == 12
        assert all(item["value"] < 100 for item in items)
        assert items[-1]["value"] == 89

    def test_batch_operation(self):
        """Test batch() groups items"""
        source = {"type": "prime"}

        s = stream(source)
        batches = list(s.take(10).batch(3).evaluate())

        assert len(batches) == 4  # 3 full batches + 1 partial
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1  # Last batch has remainder

    def test_enumerate_operation(self):
        """Test enumerate() adds indices"""
        source = {"type": "fibonacci"}

        result = stream(source)
        items = list(result.skip(5).enumerate(start=100).take(3).evaluate())

        assert len(items) == 3
        assert items[0]["index"] == 100  # Index
        assert items[0]["value"]["value"] == 5  # Fibonacci value
        assert items[1]["index"] == 101
        assert items[2]["index"] == 102


class TestInfiniteStreamFiltering:
    """Test filtering on infinite streams"""

    def test_filter_prng_data(self):
        """Test filtering PRNG-generated data"""
        source = {
            "type": "prng",
            "seed": 999,
            "template": {
                "age": {"$random": "int", "min": 18, "max": 80},
                "score": {"$random": "float", "min": 0, "max": 100},
                "status": {
                    "$random": "choice",
                    "choices": ["active", "inactive", "pending"],
                },
            },
        }

        # Find active users with high scores
        s = stream(source)
        filtered = s.filter(
            [
                "and",
                ["eq?", ["@", [["key", "status"]]], "active"],
                ["gt?", ["@", [["key", "score"]]], 75],
            ]
        )

        matches = list(filtered.take(10).evaluate())
        assert len(matches) <= 10  # Might be less if not enough match
        for item in matches:
            assert item["status"] == "active"
            assert item["score"] > 75

    def test_complex_filtering_on_time_series(self):
        """Test complex filtering on time series"""
        source = {
            "type": "time_series",
            "pattern": "sine",
            "interval_seconds": 3600,  # Hourly
            "start_time": "2024-01-01T00:00:00",
        }

        # Find weekend data points with high values
        s = stream(source)
        filtered = s.filter(
            [
                "and",
                ["eq?", ["@", [["key", "is_weekend"]]], True],
                ["gt?", ["@", [["key", "value"]]], 60],
            ]
        )

        weekend_highs = list(filtered.take(5).evaluate())
        assert all(item["is_weekend"] for item in weekend_highs)
        assert all(item["value"] > 60 for item in weekend_highs)

    def test_chained_operations(self):
        """Test chaining multiple lazy operations"""
        source = {
            "type": "prng",
            "seed": 777,
            "template": {
                "id": {"$random": "int", "min": 1, "max": 10000},
                "type": {"$random": "choice", "choices": ["A", "B", "C"]},
                "value": {"$random": "float", "min": 0, "max": 1},
            },
        }

        # Complex pipeline: filter type A, skip first 10, take while value < 0.5, batch by 5
        s = stream(source)
        filtered = s.filter(["eq?", ["@", [["key", "type"]]], "A"])

        # Skip first 10 type A items
        pipeline = filtered.skip(10)
        # Take while value < 0.5
        pipeline = pipeline.take_while(["lt?", ["@", [["key", "value"]]], 0.5])
        # Get first 20 of those
        items = list(pipeline.take(20).evaluate())

        # Verify all constraints
        assert all(item["type"] == "A" for item in items)
        assert all(item["value"] < 0.5 for item in items)
        assert len(items) <= 20


class TestPerformance:
    """Test performance with large/infinite streams"""

    def test_early_termination(self):
        """Test that take() terminates early on infinite streams"""
        source = {"type": "fibonacci"}

        result = stream(source)

        start_time = time.time()
        items = list(result.take(1000).evaluate())
        elapsed = time.time() - start_time

        assert len(items) == 1000
        # Should be very fast since we stop after 1000
        assert elapsed < 0.1  # Should take less than 100ms

    def test_filtered_infinite_stream(self):
        """Test filtering doesn't load entire infinite stream"""
        source = {
            "type": "prng",
            "seed": 555,
            "template": {"n": {"$random": "int", "min": 1, "max": 100}},
        }

        # More reasonable filter - looking for a value in 1-100 range
        s = stream(source)
        result = s.filter(["eq?", ["@", [["key", "n"]]], 42])

        # This should eventually find a match without exhausting memory
        start_time = time.time()
        matches = list(result.take(1).evaluate())
        elapsed = time.time() - start_time

        # Should find at least one match reasonably quickly
        if matches:
            assert matches[0]["n"] == 42
        # But not take forever
        assert elapsed < 5.0  # Give it up to 5 seconds

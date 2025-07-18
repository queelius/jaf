"""
Tests for lazy evaluation functionality and the evaluate() method.
These tests ensure the lazy evaluation system works correctly and covers edge cases.
"""

import pytest
from jaf.lazy_streams import stream, FilteredStream
from jaf.exceptions import JAFError, UnknownOperatorError, InvalidArgumentCountError


class TestLazyEvaluation:
    """Test lazy evaluation behavior and evaluate() method"""

    def setup_method(self):
        """Set up test data"""
        self.test_data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 30,
                "active": True,
                "tags": ["admin", "user"],
            },
            {"id": 2, "name": "Bob", "age": 25, "active": False, "tags": ["user"]},
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "active": True,
                "tags": ["user", "premium"],
            },
            {"id": 4, "name": "Diana", "age": 28, "active": True, "tags": []},
        ]

    def test_evaluate_basic_functionality(self):
        """Test basic evaluate() functionality"""
        query = ["eq?", ["@", [["key", "active"]]], True]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        # Query creation should be instant (lazy)
        assert isinstance(result, FilteredStream)
        assert result.query == query

        # Evaluation should return actual matching objects
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 3  # Alice, Charlie, Diana
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", "Charlie", "Diana"}

    def test_evaluate_is_repeatable(self):
        """Test that evaluate() can be called multiple times with same results"""
        query = ["gt?", ["@", [["key", "age"]]], 30]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        # First evaluation
        first_eval = list(result.evaluate())
        assert len(first_eval) == 1
        assert first_eval[0]["name"] == "Charlie"

        # Second evaluation should give same results
        second_eval = list(result.evaluate())
        assert first_eval == second_eval

        # Third evaluation should also be consistent
        third_eval = list(result.evaluate())
        assert first_eval == third_eval

    def test_evaluate_with_empty_results(self):
        """Test evaluate() when query matches nothing"""
        query = ["eq?", ["@", [["key", "name"]]], "Nobody"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 0
        assert matching_objects == []

    def test_evaluate_with_empty_data(self):
        """Test evaluate() with empty data collection"""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        s = stream({"type": "memory", "data": []})
        result = s.filter(query)

        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 0
        assert matching_objects == []

    def test_evaluate_with_complex_query(self):
        """Test evaluate() with complex nested query"""
        query = [
            "and",
            ["eq?", ["@", [["key", "active"]]], True],
            ["gt?", ["@", [["key", "age"]]], 25],
            ["in?", "user", ["@", [["key", "tags"]]]],
        ]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 2  # Alice and Charlie
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", "Charlie"}

    def test_evaluate_with_invalid_query_fails_clearly(self):
        """Test that evaluate() fails clearly for invalid queries"""
        query = ["unknown-operator", "arg"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        # Query creation should succeed (lazy)
        assert isinstance(result, FilteredStream)

        # But evaluation should fail with clear error
        with pytest.raises(
            UnknownOperatorError,
            match="Unknown operator: unknown-operator",
        ):
            list(result.evaluate())

    def test_evaluate_with_malformed_data_handles_gracefully(self):
        """Test that evaluate() handles malformed data gracefully"""
        malformed_data = [
            {"id": 1, "name": "Alice"},  # Good
            "not a dict",  # Bad - should be skipped
            {"id": 2, "name": "Bob"},  # Good
            None,  # Bad - should be skipped
            123,  # Bad - should be skipped
            {"id": 3, "name": "Charlie"},  # Good
        ]

        query = ["eq?", ["@", [["key", "name"]]], "Bob"]
        s = stream({"type": "memory", "data": malformed_data})
        result = s.filter(query)

        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1
        assert matching_objects[0]["name"] == "Bob"

    def test_evaluate_with_missing_fields_handles_gracefully(self):
        """Test that evaluate() handles missing fields gracefully"""
        data_with_missing_fields = [
            {"id": 1, "name": "Alice", "age": 30},  # Has age
            {"id": 2, "name": "Bob"},  # Missing age
            {"id": 3, "name": "Charlie", "age": 35},  # Has age
        ]

        query = ["gt?", ["@", [["key", "age"]]], 25]
        s = stream({"type": "memory", "data": data_with_missing_fields})
        result = s.filter(query)

        # Should only match objects that have the field
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 2  # Alice and Charlie
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", "Charlie"}

    def test_lazy_boolean_operations_evaluate_correctly(self):
        """Test that boolean operations on queries evaluate correctly"""
        query1 = ["eq?", ["@", [["key", "active"]]], True]
        query2 = ["gt?", ["@", [["key", "age"]]], 30]

        s = stream({"type": "memory", "data": self.test_data})
        rs1 = s.filter(query1)
        rs2 = s.filter(query2)

        # Test AND operation
        and_result = rs1.AND(rs2)
        and_matches = list(and_result.evaluate())
        assert len(and_matches) == 1  # Only Charlie (active AND age > 30)
        assert and_matches[0]["name"] == "Charlie"

        # Test OR operation
        or_result = rs1.OR(rs2)
        or_matches = list(or_result.evaluate())
        assert len(or_matches) == 3  # Alice, Charlie, Diana (active OR age > 30)
        or_names = {obj["name"] for obj in or_matches}
        assert or_names == {"Alice", "Charlie", "Diana"}

        # Test NOT operation
        not_result = rs1.NOT()
        not_matches = list(not_result.evaluate())
        assert len(not_matches) == 1  # Only Bob (not active)
        assert not_matches[0]["name"] == "Bob"

    def test_evaluate_preserves_object_identity(self):
        """Test that evaluate() returns the original objects, not copies"""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1

        # Should be the same object reference (not a copy)
        original_alice = self.test_data[0]
        evaluated_alice = matching_objects[0]
        assert original_alice is evaluated_alice

    def test_collection_source_metadata_preserved(self):
        """Test that collection source metadata is preserved through evaluation"""
        query = ["eq?", ["@", [["key", "active"]]], True]
        collection_source = {
            "type": "memory",
            "data": self.test_data,
            "description": "test data",
        }

        # For collection_source, we use it directly with stream
        s = stream(collection_source)
        result = s.filter(query)
        result.collection_id = "test_collection"

        # Metadata should be preserved
        assert result.collection_id == "test_collection"
        assert result.collection_source["type"] == "filter"  # Filter wraps the source

        # Evaluation should still work
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 3

    def test_evaluate_with_collection_source_in_memory(self):
        """Test evaluate() works correctly with in-memory collection source"""
        query = ["eq?", ["@", [["key", "name"]]], "Bob"]

        # This should create in-memory collection source
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        # Should have filter wrapping memory source
        assert result.collection_source["type"] == "filter"
        assert result.collection_source["inner_source"]["type"] == "memory"
        assert result.collection_source["inner_source"]["data"] == self.test_data

        # Evaluation should work
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1
        assert matching_objects[0]["name"] == "Bob"


class TestEvaluatePerformance:
    """Test evaluate() performance characteristics"""

    def test_lazy_creation_is_fast(self):
        """Test that query creation is fast (doesn't evaluate immediately)"""
        # Create large dataset
        large_data = [{"id": i, "value": i * 2} for i in range(1000)]

        query = ["gt?", ["@", [["key", "value"]]], 500]

        # Query creation should be instant (no actual filtering)
        s = stream({"type": "memory", "data": large_data})
        result = s.filter(query)
        assert isinstance(result, FilteredStream)
        assert result.query == query

        # Only evaluate() should do the work
        matching_objects = list(result.evaluate())
        assert len(matching_objects) > 0  # Should find matches

    def test_evaluate_multiple_times_performance(self):
        """Test that multiple evaluate() calls don't re-evaluate from scratch"""
        # Note: Current implementation re-evaluates each time
        # This test documents current behavior and can be optimized later
        data = [{"id": i, "active": i % 2 == 0} for i in range(100)]
        query = ["eq?", ["@", [["key", "active"]]], True]

        s = stream({"type": "memory", "data": data})
        result = s.filter(query)

        # Multiple evaluations should work
        eval1 = list(result.evaluate())
        eval2 = list(result.evaluate())
        eval3 = list(result.evaluate())

        # Results should be consistent
        assert eval1 == eval2 == eval3
        assert len(eval1) == 50  # Half should be active

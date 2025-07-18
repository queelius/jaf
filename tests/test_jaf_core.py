"""
Core JAF functionality tests.
Tests the main jaf() function and basic filtering.
"""

import pytest
from jaf.jaf import jaf, jafError
from jaf.result_set import JafQuerySet, JafQuerySetError  # Added import


class TestJAFCore:
    """Test core JAF functionality"""

    def setup_method(self):
        """Set up test data"""
        self.test_data = [
            {"id": 1, "name": "Alice", "age": 30, "active": True},
            {"id": 2, "name": "Bob", "age": 25, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "active": True},
            {"id": 4, "name": "Diana", "age": 28, "active": True},
        ]
        self.test_data_collection_id = "test_data_v1"

    def test_jaf_returns_jafresultset(self):
        """Test that jaf returns a JafQuerySet instance with correct query."""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        result = jaf(self.test_data, query)

        assert isinstance(result, JafQuerySet)
        assert result.query == query
        assert result.collection_id is None
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1
        assert matching_objects[0]["name"] == "Alice"

    def test_jaf_with_collection_id(self):
        """Test that jaf correctly assigns collection_id to JafQuerySet."""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        result = jaf(self.test_data, query, collection_id=self.test_data_collection_id)

        assert isinstance(result, JafQuerySet)
        assert result.query == query
        assert result.collection_id == self.test_data_collection_id
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1
        assert matching_objects[0]["name"] == "Alice"

    def test_multiple_matches(self):
        """Test multiple matching objects"""
        query = ["eq?", ["@", [["key", "active"]]], True]
        result = jaf(self.test_data, query)
        assert isinstance(result, JafQuerySet)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 3  # Alice, Charlie, Diana
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", "Charlie", "Diana"}

    def test_no_matches(self):
        """Test query with no matches"""
        query = ["eq?", ["@", [["key", "name"]]], "Nobody"]
        result = jaf(self.test_data, query)
        assert isinstance(result, JafQuerySet)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 0  # No matches

    def test_empty_data(self):
        """Test with empty data array"""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        result = jaf([], query)
        assert isinstance(result, JafQuerySet)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 0  # No data to match

    def test_invalid_query_raises_error(self):
        """Test that invalid queries raise appropriate errors"""
        # With lazy evaluation, invalid queries are only detected during evaluation
        query = ["unknown-operator", "arg"]
        result = jaf(self.test_data, query)

        # Query creation should succeed
        assert isinstance(result, JafQuerySet)
        assert result.query == query

        # But evaluation should fail with clear error
        with pytest.raises(
            JafQuerySetError,
            match="Query evaluation failed: Unknown operator: unknown-operator",
        ):
            list(result.evaluate())

    def test_empty_query_raises_error(self):
        """Test that empty query raises jafError"""
        with pytest.raises(jafError, match="No query provided."):
            jaf(self.test_data, None)  # type: ignore

        with pytest.raises(jafError, match="No query provided."):
            jaf(self.test_data, "")

    def test_non_dict_objects_skipped(self):
        """Test that non-dictionary objects are skipped"""
        mixed_data = [
            {"name": "Alice"},
            "not a dict",
            {"name": "Bob"},
            123,
            {"name": "Charlie"},
        ]
        query = ["eq?", ["@", [["key", "name"]]], "Bob"]
        result = jaf(mixed_data, query)
        assert isinstance(result, JafQuerySet)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1  # Only Bob matches
        assert matching_objects[0]["name"] == "Bob"

    def test_malformed_data_object_input(
        self,
    ):  # Renamed to avoid conflict if None was a valid JafQuerySet input
        """Test handling of data arrays containing non-dictionary elements like None"""
        malformed_data = [
            {"name": "Alice"},
            None,  # This object will be skipped
            {"name": "Bob"},
        ]
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        result = jaf(malformed_data, query)
        assert isinstance(result, JafQuerySet)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1  # Only Alice matches
        assert matching_objects[0]["name"] == "Alice"

    def test_input_data_not_a_list_raises_error(self):
        """Test that jaf raises jafError if input data is not a list."""
        with pytest.raises(jafError, match="Input data must be a list."):
            jaf({"not": "a list"}, ["eq?", ["@", [["key", "name"]]], "Alice"])  # type: ignore

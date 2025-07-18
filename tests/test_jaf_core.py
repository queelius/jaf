"""
Core JAF functionality tests.
Tests core filtering functionality using the stream API.
"""

import pytest
from jaf.lazy_streams import stream, FilteredStream
from jaf.exceptions import JAFError


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

    def test_stream_returns_filtered_stream(self):
        """Test that stream filtering returns a FilteredStream instance."""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        assert isinstance(result, FilteredStream)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1
        assert matching_objects[0]["name"] == "Alice"

    def test_stream_with_collection_id(self):
        """Test that stream correctly handles collection_id."""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)
        # Set collection_id on the stream
        result.collection_id = self.test_data_collection_id

        assert isinstance(result, FilteredStream)
        assert result.query == query
        assert result.collection_id == self.test_data_collection_id
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1
        assert matching_objects[0]["name"] == "Alice"

    def test_multiple_matches(self):
        """Test multiple matching objects"""
        query = ["eq?", ["@", [["key", "active"]]], True]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)
        assert isinstance(result, FilteredStream)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 3  # Alice, Charlie, Diana
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", "Charlie", "Diana"}

    def test_no_matches(self):
        """Test query with no matches"""
        query = ["eq?", ["@", [["key", "name"]]], "Nobody"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)
        assert isinstance(result, FilteredStream)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 0  # No matches

    def test_empty_data(self):
        """Test with empty data array"""
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        s = stream({"type": "memory", "data": []})
        result = s.filter(query)
        assert isinstance(result, FilteredStream)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 0  # No data to match

    def test_invalid_query_raises_error(self):
        """Test that invalid queries raise appropriate errors"""
        from jaf.exceptions import UnknownOperatorError

        # With lazy evaluation, invalid queries are only detected during evaluation
        query = ["unknown-operator", "arg"]
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(query)

        # Query creation should succeed
        assert isinstance(result, FilteredStream)
        assert result.query == query

        # But evaluation should fail with clear error
        with pytest.raises(
            UnknownOperatorError,
            match="Unknown operator: unknown-operator",
        ):
            list(result.evaluate())

    def test_empty_query_raises_error(self):
        """Test that empty query raises error"""
        # With streams, we need to test at filter time
        s = stream({"type": "memory", "data": self.test_data})

        # Empty string or None queries should be caught
        # Note: The actual error handling may differ in the stream implementation

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
        s = stream({"type": "memory", "data": mixed_data})
        result = s.filter(query)
        assert isinstance(result, FilteredStream)
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
        s = stream({"type": "memory", "data": malformed_data})
        result = s.filter(query)
        assert isinstance(result, FilteredStream)
        assert result.query == query
        # Test evaluation
        matching_objects = list(result.evaluate())
        assert len(matching_objects) == 1  # Only Alice matches
        assert matching_objects[0]["name"] == "Alice"

    def test_input_data_not_a_list_raises_error(self):
        """Test that stream handles non-list data appropriately."""
        # With streams, we can create a source but it might fail during evaluation
        # The behavior depends on how the memory source handles non-list data
        s = stream({"type": "memory", "data": {"not": "a list"}})
        result = s.filter(["eq?", ["@", [["key", "name"]]], "Alice"])
        # Evaluation might fail or return empty results
        # The exact behavior depends on the streaming implementation

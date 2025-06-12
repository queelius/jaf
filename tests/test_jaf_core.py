"""
Core JAF functionality tests.
Tests the main jaf() function and basic filtering.
"""
import pytest
from jaf import jaf, jafError


class TestJAFCore:
    """Test core JAF functionality"""
    
    def setup_method(self):
        """Set up test data"""
        self.test_data = [
            {"id": 1, "name": "Alice", "age": 30, "active": True},
            {"id": 2, "name": "Bob", "age": 25, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "active": True},
            {"id": 4, "name": "Diana", "age": 28, "active": True}
        ]
    
    def test_jaf_returns_indices(self):
        """Test that jaf returns indices, not objects"""
        query = ["eq?", ["path", ["name"]], "Alice"]
        result = jaf(self.test_data, query)
        assert result == [0]
        assert isinstance(result, list)
        assert all(isinstance(i, int) for i in result)
    
    def test_multiple_matches(self):
        """Test multiple matching objects"""
        query = ["eq?", ["path", ["active"]], True]
        result = jaf(self.test_data, query)
        assert result == [0, 2, 3]
    
    def test_no_matches(self):
        """Test query with no matches"""
        query = ["eq?", ["path", ["name"]], "Nobody"]
        result = jaf(self.test_data, query)
        assert result == []
    
    def test_empty_data(self):
        """Test with empty data array"""
        query = ["eq?", ["path", ["name"]], "Alice"]
        result = jaf([], query)
        assert result == []
    
    def test_invalid_query_raises_error(self):
        """Test that invalid queries raise jafError"""
        with pytest.raises(ValueError):
            jaf(self.test_data, ["unknown-operator", "arg"])
    
    def test_empty_query_raises_error(self):
        """Test that empty query raises error"""
        with pytest.raises(jafError):
            jaf(self.test_data, None)
        
        with pytest.raises(jafError):
            jaf(self.test_data, "")
    
    def test_non_dict_objects_skipped(self):
        """Test that non-dictionary objects are skipped"""
        mixed_data = [
            {"name": "Alice"},
            "not a dict",
            {"name": "Bob"},
            123,
            {"name": "Charlie"}
        ]
        query = ["eq?", ["path", ["name"]], "Bob"]
        result = jaf(mixed_data, query)
        assert result == [2]  # Only the dict at index 2 matches
    
    def test_malformed_data_object(self):
        """Test handling of malformed data objects"""
        malformed_data = [
            {"name": "Alice"},
            None,
            {"name": "Bob"}
        ]
        query = ["eq?", ["path", ["name"]], "Alice"]
        result = jaf(malformed_data, query)
        assert result == [0]

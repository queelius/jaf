"""
Core JAF functionality tests.
Tests the main jaf() function and basic filtering.
"""
import pytest
from jaf.jaf import jaf, jafError
from jaf.result_set import JafResultSet # Added import


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
        self.test_data_collection_id = "test_data_v1"
    
    def test_jaf_returns_jafresultset(self):
        """Test that jaf returns a JafResultSet instance with correct indices."""
        query = ["eq?", ["path", [["key", "name"]]], "Alice"]
        result = jaf(self.test_data, query)
        
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(self.test_data)
        assert result.collection_id is None
        assert all(isinstance(i, int) for i in result) # Test __iter__

    def test_jaf_with_collection_id(self):
        """Test that jaf correctly assigns collection_id to JafResultSet."""
        query = ["eq?", ["path", [["key", "name"]]], "Alice"]
        result = jaf(self.test_data, query, collection_id=self.test_data_collection_id)
        
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(self.test_data)
        assert result.collection_id == self.test_data_collection_id

    def test_multiple_matches(self):
        """Test multiple matching objects"""
        query = ["eq?", ["path", [["key", "active"]]], True]
        result = jaf(self.test_data, query)
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2, 3]
        assert result.collection_size == len(self.test_data)
    
    def test_no_matches(self):
        """Test query with no matches"""
        query = ["eq?", ["path", [["key", "name"]]], "Nobody"]
        result = jaf(self.test_data, query)
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == []
        assert len(result) == 0 # Test __len__
        assert result.collection_size == len(self.test_data)
    
    def test_empty_data(self):
        """Test with empty data array"""
        query = ["eq?", ["path", [["key", "name"]]], "Alice"]
        result = jaf([], query)
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == []
        assert result.collection_size == 0
    
    def test_invalid_query_raises_error(self):
        """Test that invalid queries raise appropriate errors"""
        # This tests for errors during query parsing/evaluation setup,
        # before a JafResultSet would typically be formed.
        # jaf_eval.eval raises ValueError for unknown operators,
        # which jaf() then wraps in a jafError.
        with pytest.raises(jafError, match="Unexpected JAF evaluation error: Unknown operator: unknown-operator"):
            jaf(self.test_data, ["unknown-operator", "arg"])
    
    def test_empty_query_raises_error(self):
        """Test that empty query raises jafError"""
        with pytest.raises(jafError, match="No query provided."):
            jaf(self.test_data, None) # type: ignore 
        
        with pytest.raises(jafError, match="No query provided."):
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
        query = ["eq?", ["path", [["key", "name"]]], "Bob"]
        result = jaf(mixed_data, query)
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [2]  # Only the dict at index 2 matches
        assert result.collection_size == len(mixed_data)
    
    def test_malformed_data_object_input(self): # Renamed to avoid conflict if None was a valid JafResultSet input
        """Test handling of data arrays containing non-dictionary elements like None"""
        malformed_data = [
            {"name": "Alice"},
            None, # This object will be skipped
            {"name": "Bob"}
        ]
        query = ["eq?", ["path", [["key", "name"]]], "Alice"]
        result = jaf(malformed_data, query)
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(malformed_data)

    def test_input_data_not_a_list_raises_error(self):
        """Test that jaf raises jafError if input data is not a list."""
        with pytest.raises(jafError, match="Input data must be a list."):
            jaf({"not": "a list"}, ["eq?", ["path", [["key", "name"]]], "Alice"]) # type: ignore

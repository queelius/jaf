"""
Test suite for critical bugs identified in code review.
These tests demonstrate bugs that need to be fixed.
"""

import pytest
from jaf.lazy_streams import stream, FilteredStream
from jaf.jaf_eval import jaf_eval
from jaf.path_evaluation import eval_path, exists
from jaf.path_types import MISSING_PATH


class TestFalsyValuesBug:
    """Test cases for the bug where path evaluation returns False for existing keys with falsy values."""
    
    def test_path_with_zero_value(self):
        """Test that a path to a key with value 0 is correctly identified as existing."""
        data = {"count": 0, "name": "test"}
        
        # Test with exists? operator
        result = jaf_eval.eval(["exists?", "@count"], data)
        assert result == True, "Key 'count' exists with value 0, should return True"
        
        # Test path evaluation
        path_ast = [["key", "count"]]
        value = eval_path(path_ast, data)
        assert value == 0, "Should return the value 0, not an empty list"
        
        # Test exists function directly
        assert exists(path_ast, data) == True, "exists() should return True for key with value 0"
    
    def test_path_with_false_value(self):
        """Test that a path to a key with value False is correctly identified as existing."""
        data = {"active": False, "name": "test"}
        
        # Test with exists? operator
        result = jaf_eval.eval(["exists?", "@active"], data)
        assert result == True, "Key 'active' exists with value False, should return True"
        
        # Test path evaluation
        path_ast = [["key", "active"]]
        value = eval_path(path_ast, data)
        assert value == False, "Should return the value False, not an empty list"
        
        # Test exists function directly
        assert exists(path_ast, data) == True, "exists() should return True for key with value False"
    
    def test_path_with_empty_string_value(self):
        """Test that a path to a key with empty string value is correctly identified as existing."""
        data = {"description": "", "name": "test"}
        
        # Test with exists? operator
        result = jaf_eval.eval(["exists?", "@description"], data)
        assert result == True, "Key 'description' exists with empty string, should return True"
        
        # Test path evaluation
        path_ast = [["key", "description"]]
        value = eval_path(path_ast, data)
        assert value == "", "Should return the empty string, not an empty list"
        
        # Test exists function directly
        assert exists(path_ast, data) == True, "exists() should return True for key with empty string"
    
    def test_path_with_null_value(self):
        """Test that a path to a key with null/None value is correctly identified as existing."""
        data = {"metadata": None, "name": "test"}
        
        # Test with exists? operator
        result = jaf_eval.eval(["exists?", "@metadata"], data)
        assert result == True, "Key 'metadata' exists with None value, should return True"
        
        # Test path evaluation  
        path_ast = [["key", "metadata"]]
        value = eval_path(path_ast, data)
        assert value is None, "Should return None, not an empty list"
        
        # Test exists function directly
        assert exists(path_ast, data) == True, "exists() should return True for key with None value"
    
    def test_filtering_with_falsy_values(self):
        """Test that filtering works correctly with falsy values."""
        data = [
            {"id": 1, "count": 0},
            {"id": 2, "count": 5},
            {"id": 3, "active": False},
            {"id": 4, "active": True},
            {"id": 5, "description": ""},
            {"id": 6, "description": "hello"},
        ]
        
        # Filter for items where count exists (should include count: 0)
        s = stream({"type": "memory", "data": data})
        result = list(s.filter(["exists?", "@count"]).evaluate())
        assert len(result) == 2, "Should find both items with 'count' key"
        assert result[0]["id"] == 1 and result[0]["count"] == 0
        assert result[1]["id"] == 2 and result[1]["count"] == 5
        
        # Filter for items where active is False
        s = stream({"type": "memory", "data": data})
        result = list(s.filter(["eq?", "@active", False]).evaluate())
        assert len(result) == 1, "Should find item with active: False"
        assert result[0]["id"] == 3
        
        # Filter for items with empty description
        s = stream({"type": "memory", "data": data})
        result = list(s.filter(["eq?", "@description", ""]).evaluate())
        assert len(result) == 1, "Should find item with empty description"
        assert result[0]["id"] == 5


class TestEmptyArrayVsNonExistentPath:
    """Test cases for distinguishing between empty arrays and non-existent paths."""
    
    def test_empty_array_vs_missing_key(self):
        """Test that we can distinguish between an empty array and a missing key."""
        data_with_empty = {"items": [], "name": "test"}
        data_without_key = {"name": "test"}
        
        # Test exists? on empty array
        result = jaf_eval.eval(["exists?", "@items"], data_with_empty)
        assert result == True, "Key 'items' exists with empty array, should return True"
        
        # Test exists? on missing key
        result = jaf_eval.eval(["exists?", "@items"], data_without_key)
        assert result == False, "Key 'items' does not exist, should return False"
        
        # Test path evaluation on empty array
        path_ast = [["key", "items"]]
        value = eval_path(path_ast, data_with_empty)
        assert value == [], "Should return empty array for existing key with empty array"
        
        # Test path evaluation on missing key
        value = eval_path(path_ast, data_without_key)
        # Now correctly returns MISSING_PATH for non-existent keys
        assert value is MISSING_PATH, "Should return MISSING_PATH sentinel for missing key"
    
    def test_nested_empty_array_vs_missing(self):
        """Test distinguishing empty arrays from missing keys in nested structures."""
        data = {
            "user": {
                "posts": [],
                "name": "Alice"
            }
        }
        
        # Test exists? on nested empty array
        result = jaf_eval.eval(["exists?", "@user.posts"], data)
        assert result == True, "Nested key 'posts' exists with empty array"
        
        # Test exists? on missing nested key
        result = jaf_eval.eval(["exists?", "@user.comments"], data)
        assert result == False, "Nested key 'comments' does not exist"
        
        # Test filtering based on empty arrays
        data_list = [
            {"id": 1, "tags": []},
            {"id": 2, "tags": ["python", "jaf"]},
            {"id": 3}  # No tags key
        ]
        
        s = stream({"type": "memory", "data": data_list})
        
        # Should find items where tags exists (including empty)
        result = list(s.filter(["exists?", "@tags"]).evaluate())
        assert len(result) == 2, "Should find items with 'tags' key (including empty)"
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        
        # Should find items where tags is empty
        result = list(s.filter(["is-empty?", "@tags"]).evaluate())
        assert len(result) == 1, "Should find only item with empty tags"
        assert result[0]["id"] == 1
    
    def test_is_empty_vs_not_exists(self):
        """Test that is-empty? behaves correctly with non-existent paths."""
        data_with_empty = {"items": [], "name": "test"}
        data_without_key = {"name": "test"}
        
        # is-empty? on existing empty array should be True
        result = jaf_eval.eval(["is-empty?", "@items"], data_with_empty)
        assert result == True, "is-empty? should return True for empty array"
        
        # is-empty? on non-existent key - what should it return?
        # Currently might return True (treating missing as empty)
        # Should probably return False or raise an error
        result = jaf_eval.eval(["is-empty?", "@items"], data_without_key)
        # Document current behavior and expected behavior
        # assert result == False, "is-empty? should return False for non-existent key"


class TestJoinOperationBugs:
    """Test cases for join operation issues."""
    
    def test_right_join_incomplete(self):
        """Test that right join is properly implemented."""
        left_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        right_data = [
            {"id": 2, "city": "NYC"},
            {"id": 3, "city": "LA"},
        ]
        
        # Create streams
        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})
        
        # Perform right join
        joined = left_stream.join(right_stream, on=["@", [["key", "id"]]], how="right")
        result = list(joined.evaluate())
        
        # Should include all right-side records
        assert len(result) == 2, "Right join should include all right-side records"
        
        # Check that we have the unmatched right record
        unmatched = [r for r in result if r["right"]["id"] == 3]
        assert len(unmatched) == 1, "Should have right record with id=3"
        assert unmatched[0]["left"] is None, "Left side should be None for unmatched right record"
        assert unmatched[0]["right"]["city"] == "LA"
    
    def test_outer_join_incomplete(self):
        """Test that outer join is properly implemented."""
        left_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        right_data = [
            {"id": 2, "city": "NYC"},
            {"id": 3, "city": "LA"},
        ]
        
        # Create streams
        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})
        
        # Perform outer join
        joined = left_stream.join(right_stream, on=["@", [["key", "id"]]], how="outer")
        result = list(joined.evaluate())
        
        # Should include all records from both sides
        assert len(result) == 3, "Outer join should include all records from both sides"
        
        # Check we have the unmatched left record
        left_only = [r for r in result if r["left"] and r["left"]["id"] == 1]
        assert len(left_only) == 1, "Should have left record with id=1"
        assert left_only[0]["right"] is None, "Right side should be None for unmatched left record"
        
        # Check we have the unmatched right record
        right_only = [r for r in result if r["right"] and r["right"]["id"] == 3]
        assert len(right_only) == 1, "Should have right record with id=3"
        assert right_only[0]["left"] is None, "Left side should be None for unmatched right record"
        
        # Check we have the matched record
        matched = [r for r in result if r["left"] and r["right"] and r["left"]["id"] == 2]
        assert len(matched) == 1, "Should have matched record with id=2"
        assert matched[0]["left"]["name"] == "Bob"
        assert matched[0]["right"]["city"] == "NYC"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
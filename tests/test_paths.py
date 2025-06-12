"""
Tests for JAF path system and wildcard functionality.
"""
import pytest
from jaf.jaf_eval import jaf_eval
from jaf.utils import path_values, exists


class TestPathSystem:
    """Test path access and wildcards"""
    
    def setup_method(self):
        """Set up test data"""
        self.nested_data = {
            "user": {
                "name": "Alice",
                "email": "alice@example.com",
                "profile": {
                    "settings": {
                        "theme": "dark"
                    }
                }
            },
            "items": [
                {"id": 1, "status": "done", "tags": ["urgent", "bug"]},
                {"id": 2, "status": "pending", "tags": ["feature"]},
                {"id": 3, "status": "done", "tags": ["enhancement"]}
            ],
            "metadata": {
                "version": "1.0",
                "errors": [
                    {"type": "warning", "message": "deprecated"},
                    {"type": "error", "message": "failed"}
                ]
            }
        }
    
    def test_simple_path_access(self):
        """Test basic path access"""
        result = path_values(["user", "name"], self.nested_data)
        assert result == "Alice"
    
    def test_nested_path_access(self):
        """Test deeply nested path access"""
        result = path_values(["user", "profile", "settings", "theme"], self.nested_data)
        assert result == "dark"
    
    def test_array_index_access(self):
        """Test array index access"""
        result = path_values(["items", 0, "status"], self.nested_data)
        assert result == "done"
        
        result = path_values(["items", 1, "id"], self.nested_data)
        assert result == 2
    
    def test_nonexistent_path(self):
        """Test accessing non-existent paths"""
        result = path_values(["nonexistent"], self.nested_data)
        assert result == []
        
        result = path_values(["user", "nonexistent"], self.nested_data)
        assert result == []
    
    def test_wildcard_single_level(self):
        """Test single-level wildcard '*'"""
        # Get all item statuses
        result = path_values(["items", "*", "status"], self.nested_data)
        assert set(result) == {"done", "pending", "done"}
        assert len(result) == 3
    
    def test_wildcard_recursive(self):
        """Test recursive wildcard '**'"""
        # Find all "type" fields anywhere
        result = path_values(["**", "type"], self.nested_data)
        assert set(result) == {"warning", "error"}
    
    def test_path_special_form(self):
        """Test path special form in evaluator"""
        query = ["path", ["user", "name"]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result == "Alice"
    
    def test_exists_function(self):
        """Test exists? special form"""
        # Existing path
        query = ["exists?", ["path", ["user", "email"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is True
        
        # Non-existing path
        query = ["exists?", ["path", ["user", "phone"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is False
    
    def test_exists_with_wildcards(self):
        """Test exists? with wildcard paths"""
        # Check if any item has status field
        query = ["exists?", ["path", ["items", "*", "status"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is True
        
        # Check for non-existent field with wildcard
        query = ["exists?", ["path", ["items", "*", "nonexistent"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is False
    
    def test_path_argument_validation(self):
        """Test path argument validation"""
        # Path must be a list
        with pytest.raises(ValueError, match="path argument must be a list"):
            jaf_eval.eval(["path", "not.a.list"], self.nested_data)
    
    def test_empty_path_components(self):
        """Test empty path returns root object"""
        result = path_values([], self.nested_data)
        assert result == self.nested_data


class TestWildcardEdgeCases:
    """Test edge cases for wildcard functionality"""
    
    def test_wildcard_on_array(self):
        """Test wildcards on array structures"""
        data = {
            "matrix": [
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9]
            ]
        }
        
        # Access all first elements of sub-arrays
        result = path_values(["matrix", "*", 0], data)
        assert result == [1, 4, 7]
    
    def test_recursive_wildcard_depth(self):
        """Test recursive wildcard at various depths"""
        data = {
            "level1": {
                "target": "found1",
                "level2": {
                    "target": "found2",
                    "level3": {
                        "target": "found3"
                    }
                }
            }
        }
        
        result = path_values(["**", "target"], data)
        assert set(result) == {"found1", "found2", "found3"}
    
    def test_mixed_wildcards(self):
        """Test combining * and ** wildcards"""
        data = {
            "categories": {
                "tech": {
                    "items": [{"name": "laptop"}, {"name": "phone"}]
                },
                "books": {
                    "items": [{"name": "novel"}, {"name": "manual"}]
                }
            }
        }
        
        # Get all item names from any category
        result = path_values(["categories", "*", "items", "*", "name"], data)
        assert set(result) == {"laptop", "phone", "novel", "manual"}
    
    def test_wildcard_no_matches(self):
        """Test wildcard when no matches found"""
        data = {"empty": {}}
        
        result = path_values(["empty", "*", "anything"], data)
        assert result == []
        
        result = path_values(["**", "nonexistent"], data)
        assert result == []

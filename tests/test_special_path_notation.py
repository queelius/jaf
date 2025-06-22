"""
Unit tests for JAF @ syntax functionality.

Tests the new @ prefix syntax for path operations, including:
- @string format: "@user.name"  
- @ with string AST: ["@", "user.name"]
- @ with path AST: ["@", [["key", "user"], ["key", "name"]]]
- Integration with exists?, predicates, etc.
"""

import pytest
from jaf.jaf_eval import jaf_eval
from jaf.path_evaluation import eval_path, exists
from jaf.path_conversion import string_to_path_ast
from jaf.path_exceptions import PathSyntaxError


class TestAtSyntaxBasics:
    """Test basic @ syntax functionality"""
    
    def test_at_string_simple_key(self):
        """Test @key syntax for simple object property access"""
        obj = {"name": "John", "age": 30}
        
        # @ string syntax
        result = jaf_eval.eval("@name", obj)
        assert result == "John"
        
        # Should be equivalent to traditional path syntax
        traditional_result = jaf_eval.eval(["path", "name"], obj)
        assert result == traditional_result
    
    def test_at_string_nested_path(self):
        """Test @key.subkey syntax for nested object access"""
        obj = {
            "user": {
                "profile": {
                    "name": "Alice",
                    "email": "alice@example.com"
                }
            }
        }
        
        result = jaf_eval.eval("@user.profile.name", obj)
        assert result == "Alice"
        
        result2 = jaf_eval.eval("@user.profile.email", obj)
        assert result2 == "alice@example.com"
    
    def test_at_string_with_arrays(self):
        """Test @ syntax with array indexing"""
        obj = {
            "items": [
                {"name": "item1", "value": 10},
                {"name": "item2", "value": 20},
                {"name": "item3", "value": 30}
            ]
        }
        
        # Access specific array element
        result = jaf_eval.eval("@items[0].name", obj)
        assert result == "item1"
        
        result2 = jaf_eval.eval("@items[1].value", obj)
        assert result2 == 20
    
    def test_at_string_with_wildcards(self):
        """Test @ syntax with wildcard operations"""
        obj = {
            "products": [
                {"name": "A", "price": 100},
                {"name": "B", "price": 200}
            ]
        }
        
        # Wildcard access
        result = jaf_eval.eval("@products.*.name", obj)
        assert len(result) == 2
        assert "A" in result
        assert "B" in result
    
    def test_at_string_nonexistent_path(self):
        """Test @ syntax with non-existent paths"""
        obj = {"name": "John"}
        
        result = jaf_eval.eval("@age", obj)
        assert result == []  # Should return empty list for non-existent path
        
        result2 = jaf_eval.eval("@user.profile.name", obj)
        assert result2 == []


class TestAtSyntaxInPredicates:
    """Test @ syntax used within predicate expressions"""
    
    def test_eq_with_at_syntax(self):
        """Test equality predicates with @ syntax"""
        obj = {"name": "John", "age": 30, "status": "active"}
        
        # Test eq? with @ syntax
        assert jaf_eval.eval(["eq?", "@name", "John"], obj) == True
        assert jaf_eval.eval(["eq?", "@name", "Jane"], obj) == False
        assert jaf_eval.eval(["eq?", "@age", 30], obj) == True
        assert jaf_eval.eval(["eq?", "@age", 25], obj) == False
    
    def test_comparison_with_at_syntax(self):
        """Test comparison predicates with @ syntax"""
        obj = {"score": 85, "count": 10}
        
        assert jaf_eval.eval(["gt?", "@score", 80], obj) == True
        assert jaf_eval.eval(["gt?", "@score", 90], obj) == False
        assert jaf_eval.eval(["gte?", "@score", 85], obj) == True
        assert jaf_eval.eval(["lt?", "@count", 15], obj) == True
        assert jaf_eval.eval(["lte?", "@count", 10], obj) == True
    
    def test_in_predicate_with_at_syntax(self):
        """Test in? predicate with @ syntax"""
        obj = {
            "tags": ["python", "json", "filter"],
            "roles": {"admin": True, "user": True}
        }
        
        # Test in? with array
        assert jaf_eval.eval(["in?", "python", "@tags"], obj) == True
        assert jaf_eval.eval(["in?", "javascript", "@tags"], obj) == False
        
        # Test in? with dict keys
        assert jaf_eval.eval(["in?", "admin", ["keys", "@roles"]], obj) == True
    
    def test_exists_with_at_syntax(self):
        """Test exists? predicate with @ syntax"""
        obj = {
            "user": {"name": "John", "email": "john@example.com"},
            "settings": None
        }
        
        # Test exists? with @ string syntax
        # assert jaf_eval.eval(["exists?", "@user.name"], obj) == True
        assert jaf_eval.eval(["exists?", "@user.age"], obj) == False
        # assert jaf_eval.eval(["exists?", "@settings"], obj) == True  # exists but is None
        # assert jaf_eval.eval(["exists?", "@nonexistent"], obj) == False


class TestAtSyntaxASTForms:
    """Test @ syntax with explicit AST forms"""
    
    def test_at_with_string_ast(self):
        """Test ["@", "path.string"] syntax"""
        obj = {"user": {"name": "Alice"}}
        
        # Explicit @ with string
        result = jaf_eval.eval(["@", "user.name"], obj)
        assert result == "Alice"
        
        # Should be equivalent to @ string syntax
        string_result = jaf_eval.eval("@user.name", obj)
        assert result == string_result
    
    def test_at_with_path_ast(self):
        """Test ["@", path_ast] syntax"""
        obj = {"user": {"profile": {"name": "Bob"}}}
        
        # Explicit @ with path AST
        path_ast = [["key", "user"], ["key", "profile"], ["key", "name"]]
        result = jaf_eval.eval(["@", path_ast], obj)
        assert result == "Bob"
        
        # Should be equivalent to other @ forms
        string_result = jaf_eval.eval("@user.profile.name", obj)
        assert result == string_result
    
    def test_at_in_exists_explicit_forms(self):
        """Test exists? with explicit @ AST forms"""
        obj = {"data": {"items": [1, 2, 3]}}
        
        # exists? with ["@", string]
        assert jaf_eval.eval(["exists?", ["@", "data.items"]], obj) == True
        assert jaf_eval.eval(["exists?", ["@", "data.missing"]], obj) == False
        
        # exists? with ["@", path_ast]
        path_ast = [["key", "data"], ["key", "items"]]
        assert jaf_eval.eval(["exists?", ["@", path_ast]], obj) == True


class TestAtSyntaxComplexExpressions:
    """Test @ syntax in complex expressions"""

    def test_at_syntax_in(self):
        # Complex or expression

        obj = {
            "user": {"name": "John", "age": 30, "active": True},
            "permissions": ["read", "write"]
        }
        query = ["in?", "read", ["@", "permissions"]]
        assert jaf_eval.eval(query, obj) == True  # True because age > 18

        query2 = ["in?", "admin", ["@", "permissions"]]
        assert jaf_eval.eval(query2, obj) == False  # False because "admin" is not in permissions

        query3 = "@permissions"
        eval3 = jaf_eval.eval(query3, obj)
        query4 = ["@", "permissions"]
        eval4 = jaf_eval.eval(query4, obj)
        assert eval3 == eval4  # Both should yield the same result

        query5 = ["in?", "read", "@permissions"]
        assert jaf_eval.eval(query5, obj) == True  # True because "read" is in permissions


    def test_at_syntax_in_and(self):
        """Test @ syntax within and/or expressions"""
        obj = {
            "user": {"name": "John", "age": 30, "active": True},
            "permissions": ["read", "write"]
        }
        
        # Complex and expression
        query = ["and", 
                 ["eq?", "@user.name", "John"],
                 ["gt?", "@user.age", 25],
                 ["eq?", "@user.active", True]]
        assert jaf_eval.eval(query, obj) == True
        
    def test_at_syntax_in_or(self):
        # Complex or expression

        obj = {
            "user": {"name": "John", "age": 30, "active": True},
            "permissions": ["read", "write"]
        }
        query = ["or",
                  ["eq?", "@user.name", "Jane"],
                  ["in?", "admin", "@permissions"],
                  ["gt?", "@user.age", 18]]
        assert jaf_eval.eval(query, obj) == True  # True because age > 18
    
    def test_at_syntax_with_length_and_operations(self):
        """Test @ syntax with value extractor operations"""
        obj = {
            "items": ["a", "b", "c", "d"],
            "metadata": {"tags": {"python": True, "json": True}}
        }
        
        # Length with @ syntax
        assert jaf_eval.eval(["gt?", ["length", "@items"], 3], obj) == True
        assert jaf_eval.eval(["eq?", ["length", "@items"], 4], obj) == True
        
        # Keys with @ syntax
        keys_result = jaf_eval.eval(["keys", "@metadata.tags"], obj)
        assert "python" in keys_result
        assert "json" in keys_result
    
    def test_nested_at_expressions(self):
        """Test multiple @ expressions in the same query"""
        obj = {
            "user1": {"score": 100},
            "user2": {"score": 85},
            "threshold": 90
        }
        
        # Compare two @ expressions
        query = ["and",
                 ["gt?", "@user1.score", "@threshold"],
                 ["lt?", "@user2.score", "@threshold"]]
        assert jaf_eval.eval(query, obj) == True


class TestAtSyntaxErrorHandling:
    """Test error handling for @ syntax"""
    
    def test_empty_at_expression(self):
        """Test empty @ expression handling"""
        obj = {"name": "John"}
        
        with pytest.raises(PathSyntaxError, match="Empty path expression after @"):
            jaf_eval.eval("@", obj)
    
    def test_invalid_at_path(self):
        """Test invalid path expressions with @"""
        obj = {"name": "John"}
        
        # Invalid path syntax should raise PathSyntaxError
        with pytest.raises(PathSyntaxError):
            jaf_eval.eval("@[invalid", obj)  # Unterminated bracket
    
    def test_at_with_wrong_argument_count(self):
        """Test @ operator with wrong number of arguments"""
        obj = {"name": "John"}
        
        # @ operator should expect exactly 1 argument
        with pytest.raises(ValueError, match="'@' operator expects exactly one argument"):
            jaf_eval.eval(["@", "name", "extra"], obj)
        
        with pytest.raises(ValueError, match="'@' operator expects exactly one argument"):
            jaf_eval.eval(["@"], obj)

class TestAtSyntaxEquivalence:
    """Test that @ syntax is equivalent to traditional path syntax"""
    
    def test_equivalence_simple_paths(self):
        """Test equivalence for simple path expressions"""
        test_cases = [
            {"obj": {"name": "John"}, "path": "name"},
            {"obj": {"user": {"email": "test@example.com"}}, "path": "user.email"},
            {"obj": {"items": [1, 2, 3]}, "path": "items[0]"},
            {"obj": {"data": {"values": [{"x": 10}]}}, "path": "data.values[0].x"}
        ]
        
        for case in test_cases:
            obj = case["obj"]
            path = case["path"]
            
            # Compare all three forms
            at_string = jaf_eval.eval(f"@{path}", obj)
            at_explicit = jaf_eval.eval(["@", path], obj)
            traditional = jaf_eval.eval(["path", path], obj)
            
            assert at_string == at_explicit == traditional, f"Mismatch for path: {path}"
    
    def test_equivalence_with_wildcards(self):
        """Test equivalence for wildcard expressions"""
        obj = {
            "products": [
                {"name": "A", "tags": ["new", "sale"]},
                {"name": "B", "tags": ["popular"]}
            ]
        }
        
        path = "products.*.name"
        
        at_result = jaf_eval.eval(f"@{path}", obj)
        traditional_result = jaf_eval.eval(["path", path], obj)
        
        # Both should return PathValues with same content
        assert len(at_result) == len(traditional_result)
        assert set(at_result) == set(traditional_result)
    
class TestAtSyntaxIntegration:
    """Integration tests for @ syntax with the broader JAF system"""
    
    def test_real_world_filtering_example(self):
        """Test @ syntax in realistic filtering scenarios"""
        data = [
            {
                "user": {"name": "Alice", "age": 28, "role": "admin"},
                "activity": {"login_count": 150, "last_login": "2024-01-15"}
            },
            {
                "user": {"name": "Bob", "age": 35, "role": "user"},
                "activity": {"login_count": 45, "last_login": "2024-01-10"}
            },
            {
                "user": {"name": "Charlie", "age": 22, "role": "user"},
                "activity": {"login_count": 200, "last_login": "2024-01-20"}
            }
        ]
        
        # Filter for active admin users
        query = ["and",
                 ["eq?", "@user.role", "admin"],
                 ["gt?", "@activity.login_count", 100]]
        
        results = [jaf_eval.eval(query, obj) for obj in data]
        assert results == [True, False, False]  # Only Alice matches
        
        # Filter for young, active users
        query2 = ["and",
                  ["lt?", "@user.age", 30],
                  ["gt?", "@activity.login_count", 100]]
        
        results2 = [jaf_eval.eval(query2, obj) for obj in data]
        assert results2 == [True, False, True]  # Alice and Charlie match
    
    def test_at_syntax_with_mixed_operators(self):
        """Test @ syntax mixed with other JAF operators"""
        obj = {
            "config": {
                "features": ["auth", "api", "dashboard"],
                "limits": {"max_users": 1000, "max_requests": 5000}
            },
            "status": "production"
        }
        
        # Complex query mixing @ syntax with various operators
        query = ["and",
                 ["eq?", "@status", "production"],
                 ["in?", "api", "@config.features"],
                 ["gt?", ["length", "@config.features"], 2],
                 ["gte?", "@config.limits.max_users", 500]]
        
        assert jaf_eval.eval(query, obj) == True
        
        # Test with if statement
        if_query = ["if",
                    ["eq?", "@status", "production"],
                    ["gt?", "@config.limits.max_requests", 1000],
                    ["gt?", "@config.limits.max_requests", 100]]
        
        assert jaf_eval.eval(if_query, obj) == True  # production branch taken


if __name__ == "__main__":
    pytest.main([__file__])
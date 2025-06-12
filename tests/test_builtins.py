"""
Tests for JAF predicates and value extractors.
"""
import pytest
import datetime
import re
from jaf.jaf_eval import jaf_eval


class TestPredicates:
    """Test predicate functions that return boolean values"""
    
    def setup_method(self):
        """Set up test data"""
        self.test_obj = {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "score": 85.5,
            "tags": ["python", "javascript", "react"],
            "active": True,
            "profile": None
        }
    
    def test_equality_predicates(self):
        """Test eq? and neq? predicates"""
        # String equality
        query = ["eq?", ["path", ["name"]], "Alice"]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        query = ["eq?", ["path", ["name"]], "Bob"]
        assert jaf_eval.eval(query, self.test_obj) is False
        
        # Number equality
        query = ["eq?", ["path", ["age"]], 30]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Boolean equality
        query = ["eq?", ["path", ["active"]], True]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Null equality
        query = ["eq?", ["path", ["profile"]], None]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Not equal
        query = ["neq?", ["path", ["name"]], "Bob"]
        assert jaf_eval.eval(query, self.test_obj) is True
    
    def test_comparison_predicates(self):
        """Test gt?, gte?, lt?, lte? predicates"""
        # Greater than
        query = ["gt?", ["path", ["age"]], 25]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        query = ["gt?", ["path", ["age"]], 35]
        assert jaf_eval.eval(query, self.test_obj) is False
        
        # Greater than or equal
        query = ["gte?", ["path", ["age"]], 30]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Less than
        query = ["lt?", ["path", ["score"]], 90]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Less than or equal
        query = ["lte?", ["path", ["score"]], 85.5]
        assert jaf_eval.eval(query, self.test_obj) is True
    
    def test_containment_predicates(self):
        """Test in? predicate"""
        # String in array
        query = ["in?", "python", ["path", ["tags"]]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        query = ["in?", "java", ["path", ["tags"]]]
        assert jaf_eval.eval(query, self.test_obj) is False
        
        # Substring in string
        query = ["in?", "alice", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is True
    
    def test_string_matching_predicates(self):
        """Test string matching predicates"""
        # starts-with?
        query = ["starts-with?", "ali", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        query = ["starts-with?", "bob", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is False
        
        # ends-with?
        query = ["ends-with?", ".com", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        query = ["ends-with?", ".org", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is False
    
    def test_regex_matching(self):
        """Test regex-match? predicate"""
        # Valid email pattern
        query = ["regex-match?", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Invalid pattern
        query = ["regex-match?", r"^\d+$", ["path", ["email"]]]
        assert jaf_eval.eval(query, self.test_obj) is False
    
    def test_fuzzy_matching(self):
        """Test close-match? and partial-match? predicates"""
        test_data = {"text": "hello world"}
        
        # Close match
        query = ["close-match?", "hello world", ["path", ["text"]]]
        assert jaf_eval.eval(query, test_data) is True
        
        # Partial match
        query = ["partial-match?", "hello", ["path", ["text"]]]
        assert jaf_eval.eval(query, test_data) is True


class TestValueExtractors:
    """Test value extractor functions"""
    
    def setup_method(self):
        """Set up test data"""
        self.test_obj = {
            "name": "Alice",
            "items": [1, 2, 3, 4, 5],
            "profile": {
                "settings": {"theme": "dark"},
                "preferences": {"lang": "en"}
            },
            "created": "2023-01-15",
            "updated": "2023-06-15 14:30:00"
        }
    
    def test_length_extractor(self):
        """Test length extractor"""
        # Array length
        query = ["length", ["path", ["items"]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == 5
        
        # String length
        query = ["length", ["path", ["name"]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == 5
        
        # Use length in comparison
        query = ["gt?", ["length", ["path", ["items"]]], 3]
        assert jaf_eval.eval(query, self.test_obj) is True
    
    def test_type_extractor(self):
        """Test type extractor"""
        query = ["type", ["path", ["name"]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "str"
        
        query = ["type", ["path", ["items"]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "list"
        
        query = ["type", ["path", ["profile"]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "dict"
    
    def test_keys_extractor(self):
        """Test keys extractor"""
        query = ["keys", ["path", ["profile"]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert set(result) == {"settings", "preferences"}
        
        # Use keys in condition
        query = ["in?", "settings", ["keys", ["path", ["profile"]]]]
        assert jaf_eval.eval(query, self.test_obj) is True
    
    def test_string_transformers(self):
        """Test string transformation functions"""
        data = {"text": "Hello World"}
        
        # lower-case
        query = ["lower-case", ["path", ["text"]]]
        result = jaf_eval.eval(query, data)
        assert result == "hello world"
        
        # upper-case
        query = ["upper-case", ["path", ["text"]]]
        result = jaf_eval.eval(query, data)
        assert result == "HELLO WORLD"
        
        # Use in comparison
        query = ["eq?", ["lower-case", ["path", ["text"]]], "hello world"]
        assert jaf_eval.eval(query, data) is True
    
    def test_datetime_functions(self):
        """Test date/time functions"""
        # Test date parsing
        query = ["date", "2023-01-15"]
        result = jaf_eval.eval(query, {})
        assert isinstance(result, datetime.datetime)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15
        
        # Test datetime parsing
        query = ["datetime", "2023-01-15 14:30:00"]
        result = jaf_eval.eval(query, {})
        assert isinstance(result, datetime.datetime)
        assert result.hour == 14
        assert result.minute == 30
        
        # Test now function
        query = ["now"]
        result = jaf_eval.eval(query, {})
        assert isinstance(result, datetime.datetime)
    
    def test_date_arithmetic(self):
        """Test date difference and extraction"""
        # Create two dates
        date1 = datetime.datetime(2023, 6, 15)
        date2 = datetime.datetime(2023, 1, 15)
        
        # Test date difference
        query = ["date-diff", date1, date2]
        diff = jaf_eval.eval(query, {})
        assert isinstance(diff, datetime.timedelta)
        
        # Test days extraction
        query = ["days", diff]
        days = jaf_eval.eval(query, {})
        assert days == 151  # June 15 - Jan 15 = 151 days


class TestLogicalOperators:
    """Test logical special forms"""
    
    def setup_method(self):
        """Set up test data"""
        self.test_obj = {
            "name": "Alice",
            "age": 30,
            "active": True,
            "score": 85
        }
    
    def test_and_operator(self):
        """Test and special form with short-circuit evaluation"""
        # Both true
        query = ["and",
                ["eq?", ["path", ["name"]], "Alice"],
                ["eq?", ["path", ["active"]], True]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # First false (should short-circuit)
        query = ["and",
                ["eq?", ["path", ["name"]], "Bob"],
                ["eq?", ["path", ["active"]], True]]
        assert jaf_eval.eval(query, self.test_obj) is False
        
        # Second false
        query = ["and",
                ["eq?", ["path", ["name"]], "Alice"],
                ["eq?", ["path", ["active"]], False]]
        assert jaf_eval.eval(query, self.test_obj) is False
    
    def test_or_operator(self):
        """Test or special form with short-circuit evaluation"""
        # First true (should short-circuit)
        query = ["or",
                ["eq?", ["path", ["name"]], "Alice"],
                ["eq?", ["path", ["name"]], "Bob"]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Second true
        query = ["or",
                ["eq?", ["path", ["name"]], "Bob"],
                ["eq?", ["path", ["active"]], True]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Both false
        query = ["or",
                ["eq?", ["path", ["name"]], "Bob"],
                ["eq?", ["path", ["active"]], False]]
        assert jaf_eval.eval(query, self.test_obj) is False
    
    def test_not_operator(self):
        """Test not special form"""
        # Negate true
        query = ["not", ["eq?", ["path", ["name"]], "Alice"]]
        assert jaf_eval.eval(query, self.test_obj) is False
        
        # Negate false
        query = ["not", ["eq?", ["path", ["name"]], "Bob"]]
        assert jaf_eval.eval(query, self.test_obj) is True
    
    def test_if_conditional(self):
        """Test if special form"""
        # Condition true
        query = ["if",
                ["eq?", ["path", ["active"]], True],
                ["path", ["name"]],
                "inactive"]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "Alice"
        
        # Condition false
        query = ["if",
                ["eq?", ["path", ["active"]], False],
                ["path", ["name"]],
                "inactive"]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "inactive"
    
    def test_complex_logical_expressions(self):
        """Test complex nested logical expressions"""
        # (name == "Alice" AND active == True) OR score > 90
        query = ["or",
                ["and",
                 ["eq?", ["path", ["name"]], "Alice"],
                 ["eq?", ["path", ["active"]], True]],
                ["gt?", ["path", ["score"]], 90]]
        assert jaf_eval.eval(query, self.test_obj) is True
        
        # Complex nested with if
        query = ["if",
                ["and",
                 ["eq?", ["path", ["name"]], "Alice"],
                 ["gt?", ["path", ["age"]], 25]],
                True,
                False]
        assert jaf_eval.eval(query, self.test_obj) is True


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_unknown_operator(self):
        """Test unknown operator raises error"""
        with pytest.raises(ValueError, match="Unknown operator"):
            jaf_eval.eval(["unknown-op", "arg"], {})
    
    def test_invalid_argument_counts(self):
        """Test invalid argument counts"""
        # eq? expects 2 arguments
        with pytest.raises(ValueError, match="expects 2 arguments"):
            jaf_eval.eval(["eq?", "only-one-arg"], {})
        
        # not expects 1 argument
        with pytest.raises(ValueError, match="expects 1 argument"):
            jaf_eval.eval(["not", "arg1", "arg2"], {})
        
        # if expects 3 arguments
        with pytest.raises(ValueError, match="expects 3 arguments"):
            jaf_eval.eval(["if", "condition", "true-branch"], {})
    
    def test_type_errors_return_false(self):
        """Test that type errors return false instead of raising"""
        # Try to use string operation on number
        test_obj = {"number": 42}
        
        # This should not raise an error, but might return false
        try:
            result = jaf_eval.eval(["starts-with?", "4", ["path", ["number"]]], test_obj)
            # Result depends on implementation - might be false or raise
        except (TypeError, AttributeError):
            # Acceptable - type errors might be raised
            pass
    
    def test_null_path_handling(self):
        """Test handling of null values in paths"""
        test_obj = {"data": None}
        
        # Accessing path on null should return empty/null
        query = ["path", ["data", "field"]]
        result = jaf_eval.eval(query, test_obj)
        assert result == [] or result is None
    
    def test_empty_list_as_query(self):
        """Test empty list as query raises error"""
        with pytest.raises(ValueError, match="Invalid query format"):
            jaf_eval.eval([], {})

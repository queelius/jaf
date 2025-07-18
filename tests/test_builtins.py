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
            "profile": None,
        }

    def test_equality_predicates(self):
        """Test eq? and neq? predicates"""
        # String equality
        query = ["eq?", ["@", [["key", "name"]]], "Alice"]
        assert jaf_eval.eval(query, self.test_obj) is True

        query = ["eq?", ["@", [["key", "name"]]], "Bob"]
        assert jaf_eval.eval(query, self.test_obj) is False

        # Number equality
        query = ["eq?", ["@", [["key", "age"]]], 30]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Boolean equality
        query = ["eq?", ["@", [["key", "active"]]], True]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Null equality
        query = ["eq?", ["@", [["key", "profile"]]], None]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Not equal
        query = ["neq?", ["@", [["key", "name"]]], "Bob"]
        assert jaf_eval.eval(query, self.test_obj) is True

    def test_comparison_predicates(self):
        """Test gt?, gte?, lt?, lte? predicates"""
        # Greater than
        query = ["gt?", ["@", [["key", "age"]]], 25]
        assert jaf_eval.eval(query, self.test_obj) is True

        query = ["gt?", ["@", [["key", "age"]]], 35]
        assert jaf_eval.eval(query, self.test_obj) is False

        # Greater than or equal
        query = ["gte?", ["@", [["key", "age"]]], 30]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Less than
        query = ["lt?", ["@", [["key", "score"]]], 90]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Less than or equal
        query = ["lte?", ["@", [["key", "score"]]], 85.5]
        assert jaf_eval.eval(query, self.test_obj) is True

    def test_containment_predicates(self):
        """Test in? predicate"""
        # String in array
        query = ["in?", "python", ["@", [["key", "tags"]]]]
        assert jaf_eval.eval(query, self.test_obj) is True

        query = ["in?", "java", ["@", [["key", "tags"]]]]
        assert jaf_eval.eval(query, self.test_obj) is False

        # Substring in string
        query = ["in?", "alice", ["@", [["key", "email"]]]]
        assert jaf_eval.eval(query, self.test_obj) is True

    def test_string_matching_predicates(self):
        """Test string matching predicates"""
        # starts-with?
        query = ["starts-with?", ["@", [["key", "email"]]], "ali"]
        assert jaf_eval.eval(query, self.test_obj) is True

        query = ["starts-with?", ["@", [["key", "email"]]], "bob"]
        assert jaf_eval.eval(query, self.test_obj) is False

        # ends-with?
        query = ["ends-with?", ["@", [["key", "email"]]], ".com"]
        assert jaf_eval.eval(query, self.test_obj) is True

        query = ["ends-with?", ["@", [["key", "email"]]], ".org"]
        assert jaf_eval.eval(query, self.test_obj) is False

    def test_regex_matching(self):
        """Test regex-match? predicate"""
        # Valid email pattern
        query = [
            "regex-match?",
            ["@", [["key", "email"]]],
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        ]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Invalid pattern
        query = ["regex-match?", ["@", [["key", "email"]]], r"^\\\\d+$"]
        assert jaf_eval.eval(query, self.test_obj) is False

    def test_fuzzy_matching(self):
        """Test close-match? and partial-match? predicates"""
        test_data = {"text": "hello world"}

        # Close match
        query = ["close-match?", "hello world", ["@", [["key", "text"]]]]
        assert jaf_eval.eval(query, test_data) is True

        # Partial match
        query = ["partial-match?", "hello", ["@", [["key", "text"]]]]
        assert jaf_eval.eval(query, test_data) is True


class TestValueExtractors:
    """Test value extractor functions"""

    def setup_method(self):
        """Set up test data"""
        self.test_obj = {
            "name": "Alice",
            "items": [1, 2, 3, 4, 5],
            "profile": {"settings": {"theme": "dark"}, "preferences": {"lang": "en"}},
            "created": "2023-01-15",
            "updated": "2023-06-15 14:30:00",
        }

    def test_length_extractor(self):
        """Test length extractor"""
        # Array length
        query = ["length", ["@", [["key", "items"]]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == 5

        # String length
        query = ["length", ["@", [["key", "name"]]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == 5

        # Use length in comparison
        query = ["gt?", ["length", ["@", [["key", "items"]]]], 3]
        assert jaf_eval.eval(query, self.test_obj) is True

    def test_type_extractor(self):
        """Test type extractor"""
        query = ["type", ["@", [["key", "name"]]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "str"

        query = ["type", ["@", [["key", "items"]]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "list"

        query = ["type", ["@", [["key", "profile"]]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert (
            result == "dict"
        )  # Should be dict, not NoneType, if profile is None, path eval returns []

    def test_keys_extractor(self):
        """Test keys extractor"""
        query = ["keys", ["@", [["key", "profile"]]]]
        result = jaf_eval.eval(query, self.test_obj)
        assert set(result) == {"settings", "preferences"}

        # Use keys in condition
        query = ["in?", "settings", ["keys", ["@", [["key", "profile"]]]]]
        assert jaf_eval.eval(query, self.test_obj) is True

    def test_string_transformers(self):
        """Test string transformation functions"""
        data = {"text": "Hello World"}

        # lower-case
        query = ["lower-case", ["@", [["key", "text"]]]]
        result = jaf_eval.eval(query, data)
        assert result == "hello world"

        # upper-case
        query = ["upper-case", ["@", [["key", "text"]]]]
        result = jaf_eval.eval(query, data)
        assert result == "HELLO WORLD"

        # Use in comparison
        query = ["eq?", ["lower-case", ["@", [["key", "text"]]]], "hello world"]
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
        self.test_obj = {"name": "Alice", "age": 30, "active": True, "score": 85}

    def test_and_operator(self):
        """Test and special form with short-circuit evaluation"""
        # Both true
        query = [
            "and",
            ["eq?", ["@", [["key", "name"]]], "Alice"],
            ["eq?", ["@", [["key", "active"]]], True],
        ]
        assert jaf_eval.eval(query, self.test_obj) is True

        # First false (should short-circuit)
        query = [
            "and",
            ["eq?", ["@", [["key", "name"]]], "Bob"],
            ["eq?", ["@", [["key", "active"]]], True],
        ]  # This part won't be eval'd by a short-circuiting 'and'
        assert jaf_eval.eval(query, self.test_obj) is False

        # Second false
        query = [
            "and",
            ["eq?", ["@", [["key", "name"]]], "Alice"],
            ["eq?", ["@", [["key", "active"]]], False],
        ]
        assert jaf_eval.eval(query, self.test_obj) is False

    def test_or_operator(self):
        """Test or special form with short-circuit evaluation"""
        # First true (should short-circuit)
        query = [
            "or",
            ["eq?", ["@", [["key", "name"]]], "Alice"],
            ["eq?", ["@", [["key", "name"]]], "Bob"],
        ]  # This part won't be eval'd
        assert jaf_eval.eval(query, self.test_obj) is True

        # Second true
        query = [
            "or",
            ["eq?", ["@", [["key", "name"]]], "Bob"],
            ["eq?", ["@", [["key", "active"]]], True],
        ]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Both false
        query = [
            "or",
            ["eq?", ["@", [["key", "name"]]], "Bob"],
            ["eq?", ["@", [["key", "active"]]], False],
        ]
        assert jaf_eval.eval(query, self.test_obj) is False

    def test_not_operator(self):
        """Test not special form"""
        # Negate true
        query = ["not", ["eq?", ["@", [["key", "name"]]], "Alice"]]
        assert jaf_eval.eval(query, self.test_obj) is False

        # Negate false
        query = ["not", ["eq?", ["@", [["key", "name"]]], "Bob"]]
        assert jaf_eval.eval(query, self.test_obj) is True

    def test_if_conditional(self):
        """Test if special form"""
        # Condition true
        query = [
            "if",
            ["eq?", ["@", [["key", "active"]]], True],
            ["@", [["key", "name"]]],
            "inactive",
        ]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "Alice"

        # Condition false
        query = [
            "if",
            ["eq?", ["@", [["key", "active"]]], False],
            ["@", [["key", "name"]]],
            "inactive",
        ]
        result = jaf_eval.eval(query, self.test_obj)
        assert result == "inactive"

    def test_complex_logical_expressions(self):
        """Test complex nested logical expressions"""
        # (name == "Alice" AND active == True) OR score > 90
        query = [
            "or",
            [
                "and",
                ["eq?", ["@", [["key", "name"]]], "Alice"],
                ["eq?", ["@", [["key", "active"]]], True],
            ],
            ["gt?", ["@", [["key", "score"]]], 90],
        ]
        assert jaf_eval.eval(query, self.test_obj) is True

        # Complex nested with if
        query = [
            "if",
            [
                "and",
                ["eq?", ["@", [["key", "name"]]], "Alice"],
                ["gt?", ["@", [["key", "age"]]], 25],
            ],
            True,
            False,
        ]
        assert jaf_eval.eval(query, self.test_obj) is True


class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_unknown_operator(self):
        """Test unknown operator raises error"""
        from jaf.exceptions import UnknownOperatorError

        with pytest.raises(UnknownOperatorError, match="Unknown operator: unknown-op"):
            jaf_eval.eval(["unknown-op", "arg"], {})

    def test_invalid_argument_counts(self):
        """Test invalid argument counts"""
        from jaf.exceptions import InvalidArgumentCountError

        # eq? expects 2 arguments
        with pytest.raises(
            InvalidArgumentCountError, match=r"'eq\?' expects 2 arguments, got 1"
        ):
            jaf_eval.eval(["eq?", "only-one-arg"], {})

        # not expects 1 argument
        with pytest.raises(
            InvalidArgumentCountError, match="'not' expects 1 arguments, got 2"
        ):
            jaf_eval.eval(["not", "arg1", "arg2"], {})

        # if expects 3 arguments
        with pytest.raises(
            InvalidArgumentCountError, match="'if' expects 3 arguments, got 2"
        ):
            jaf_eval.eval(["if", "condition", "true-branch"], {})

    def test_type_errors_return_false(self):
        """Test that type errors in predicates generally lead to false, or PathValues behavior for adapt_jaf_operator."""
        test_obj = {"number": 42, "text": "abc"}

        # Try to use string operation on number with a predicate like 'starts-with?'
        # The adapt_jaf_operator should handle this gracefully.
        # For predicates, a type mismatch in an argument often means the condition is false.
        query = ["starts-with?", "4", ["@", [["key", "number"]]]]
        # jaf_eval.eval will use adapt_jaf_operator, which should manage type issues.
        # For predicates, a type mismatch in an argument often means the condition is false.
        assert jaf_eval.eval(query, test_obj) is False

        # Try to use a numeric comparison on a string
        query = ["gt?", ["@", [["key", "text"]]], 10]
        assert (
            jaf_eval.eval(query, test_obj) is False
        )  # Or should raise specific JAF error if desired

    def test_null_path_handling(self):
        """Test handling of null values in paths"""
        test_obj = {"data": None, "nested": {"value": None}}

        # Accessing path on null should return empty list from eval_path
        query = ["@", [["key", "data"], ["key", "field"]]]
        result = jaf_eval.eval(query, test_obj)
        assert result == []

        # Path leading to a None value
        query = ["@", [["key", "nested"], ["key", "value"]]]
        result = jaf_eval.eval(query, test_obj)
        assert result is None  # Direct access to a None value

        # Using a path that results in None/[] with a predicate
        query = ["eq?", ["@", [["key", "data"], ["key", "field"]]], "something"]
        # 'path' will return [], 'eq?' will see [] == "something", which is False.
        assert jaf_eval.eval(query, test_obj) is False

        query = ["eq?", ["@", [["key", "nested"], ["key", "value"]]], None]
        # 'path' will return None, 'eq?' will see None == None, which is True.
        assert jaf_eval.eval(query, test_obj) is True

    def test_empty_list_as_query(self):
        """Test empty list as query raises error"""
        from jaf.exceptions import InvalidQueryFormatError

        with pytest.raises(InvalidQueryFormatError, match="Query cannot be empty"):
            jaf_eval.eval([], {})

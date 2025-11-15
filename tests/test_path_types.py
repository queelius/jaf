"""
Comprehensive test suite for path_types.py

Tests MissingPath sentinel and PathValues container behavior.
Follows TDD principles: tests focus on contracts and behaviors, not implementation.
"""

import pytest
from jaf.path_types import MissingPath, MISSING_PATH, PathValues


class TestMissingPath:
    """Test MissingPath sentinel class behavior"""

    def test_missing_path_is_falsy(self):
        """MissingPath should evaluate to False in boolean context"""
        missing = MissingPath()
        assert not missing
        assert bool(missing) is False

    def test_missing_path_repr(self):
        """MissingPath should have clear string representation"""
        missing = MissingPath()
        assert repr(missing) == "MissingPath()"
        assert str(missing) == "MissingPath()"

    def test_missing_path_equality(self):
        """MissingPath instances should be equal to each other"""
        missing1 = MissingPath()
        missing2 = MissingPath()

        assert missing1 == missing2
        assert missing1 == MISSING_PATH
        assert missing2 == MISSING_PATH

    def test_missing_path_inequality_with_other_types(self):
        """MissingPath should not equal other types"""
        missing = MissingPath()

        assert missing != None
        assert missing != False
        assert missing != 0
        assert missing != ""
        assert missing != []
        assert missing != {}

    def test_missing_path_singleton(self):
        """MISSING_PATH should be a reusable singleton instance"""
        assert isinstance(MISSING_PATH, MissingPath)
        assert not MISSING_PATH
        assert MISSING_PATH == MissingPath()

    def test_missing_path_in_conditionals(self):
        """MissingPath should behave correctly in if statements"""
        missing = MissingPath()

        # Should take the 'else' branch
        if missing:
            pytest.fail("MissingPath should be falsy")
        else:
            pass  # Expected behavior

        # Should work with 'not'
        if not missing:
            pass  # Expected behavior
        else:
            pytest.fail("'not MissingPath' should be truthy")

    def test_missing_path_distinct_from_none(self):
        """MissingPath should be distinguishable from None"""
        missing = MissingPath()

        # Both are falsy, but not equal
        assert not missing
        assert missing is not None
        assert missing != None

        # Can distinguish in code
        def check_value(val):
            if val is None:
                return "none"
            elif isinstance(val, MissingPath):
                return "missing"
            else:
                return "other"

        assert check_value(None) == "none"
        assert check_value(missing) == "missing"
        assert check_value(MISSING_PATH) == "missing"
        assert check_value(0) == "other"


class TestPathValuesBasicBehavior:
    """Test PathValues basic list-like behavior"""

    def test_pathvalues_creation_empty(self):
        """PathValues can be created empty"""
        pv = PathValues()
        assert len(pv) == 0
        assert list(pv) == []
        assert not pv  # Empty list is falsy

    def test_pathvalues_creation_from_list(self):
        """PathValues can be created from a list"""
        data = [1, 2, 3]
        pv = PathValues(data)

        assert len(pv) == 3
        assert list(pv) == [1, 2, 3]
        assert pv  # Non-empty list is truthy

    def test_pathvalues_creation_from_iterable(self):
        """PathValues can be created from any iterable"""
        # From generator
        pv1 = PathValues(x * 2 for x in range(3))
        assert list(pv1) == [0, 2, 4]

        # From tuple
        pv2 = PathValues((1, 2, 3))
        assert list(pv2) == [1, 2, 3]

        # From set (order may vary)
        pv3 = PathValues({1, 2, 3})
        assert len(pv3) == 3
        assert set(pv3) == {1, 2, 3}

    def test_pathvalues_repr(self):
        """PathValues should have clear string representation"""
        pv = PathValues([1, 2, 3])
        assert repr(pv) == "PathValues([1, 2, 3])"

        pv_empty = PathValues()
        assert repr(pv_empty) == "PathValues([])"

    def test_pathvalues_list_operations(self):
        """PathValues should support standard list operations"""
        pv = PathValues([1, 2, 3])

        # Append
        pv.append(4)
        assert list(pv) == [1, 2, 3, 4]

        # Extend
        pv.extend([5, 6])
        assert list(pv) == [1, 2, 3, 4, 5, 6]

        # Insert
        pv.insert(0, 0)
        assert list(pv) == [0, 1, 2, 3, 4, 5, 6]

        # Remove
        pv.remove(0)
        assert list(pv) == [1, 2, 3, 4, 5, 6]

        # Pop
        last = pv.pop()
        assert last == 6
        assert list(pv) == [1, 2, 3, 4, 5]


class TestPathValuesIndexing:
    """Test PathValues indexing and slicing behavior"""

    def test_pathvalues_single_index_access(self):
        """Single index access should return the item, not PathValues"""
        pv = PathValues([10, 20, 30, 40, 50])

        assert pv[0] == 10
        assert pv[2] == 30
        assert pv[-1] == 50
        assert pv[-2] == 40

        # Returned item should not be PathValues
        assert not isinstance(pv[0], PathValues)
        assert isinstance(pv[0], int)

    def test_pathvalues_slice_access(self):
        """Slice access should return PathValues instance"""
        pv = PathValues([10, 20, 30, 40, 50])

        # Basic slices
        result = pv[1:3]
        assert isinstance(result, PathValues)
        assert list(result) == [20, 30]

        # Slice with step
        result2 = pv[::2]
        assert isinstance(result2, PathValues)
        assert list(result2) == [10, 30, 50]

        # Negative indices
        result3 = pv[-3:-1]
        assert isinstance(result3, PathValues)
        assert list(result3) == [30, 40]

    def test_pathvalues_index_out_of_bounds(self):
        """Indexing out of bounds should raise IndexError"""
        pv = PathValues([1, 2, 3])

        with pytest.raises(IndexError):
            _ = pv[10]

        with pytest.raises(IndexError):
            _ = pv[-10]

    def test_pathvalues_empty_slice(self):
        """Empty slices should return empty PathValues"""
        pv = PathValues([1, 2, 3])

        result = pv[5:10]
        assert isinstance(result, PathValues)
        assert len(result) == 0
        assert list(result) == []


class TestPathValuesConvenienceMethods:
    """Test PathValues convenience methods: first, last, one, one_or_none"""

    def test_first_with_values(self):
        """first() should return first element when list is non-empty"""
        pv = PathValues([10, 20, 30])
        assert pv.first() == 10

        pv_single = PathValues([42])
        assert pv_single.first() == 42

    def test_first_with_empty_list(self):
        """first() should return default when list is empty"""
        pv = PathValues()
        assert pv.first() is None
        assert pv.first(default="nothing") == "nothing"
        assert pv.first(default=0) == 0

    def test_last_with_values(self):
        """last() should return last element when list is non-empty"""
        pv = PathValues([10, 20, 30])
        assert pv.last() == 30

        pv_single = PathValues([42])
        assert pv_single.last() == 42

    def test_last_with_empty_list(self):
        """last() should return default when list is empty"""
        pv = PathValues()
        assert pv.last() is None
        assert pv.last(default="nothing") == "nothing"
        assert pv.last(default=0) == 0

    def test_one_with_single_element(self):
        """one() should return element when exactly one exists"""
        pv = PathValues([42])
        assert pv.one() == 42

        pv_str = PathValues(["hello"])
        assert pv_str.one() == "hello"

    def test_one_with_empty_list(self):
        """one() should raise ValueError when list is empty"""
        pv = PathValues()

        with pytest.raises(ValueError, match="empty.*exactly one"):
            pv.one()

    def test_one_with_multiple_elements(self):
        """one() should raise ValueError when multiple elements exist"""
        pv = PathValues([1, 2, 3])

        with pytest.raises(ValueError, match="contains 3.*exactly one"):
            pv.one()

        pv_two = PathValues([1, 2])
        with pytest.raises(ValueError, match="contains 2.*exactly one"):
            pv_two.one()

    def test_one_or_none_with_single_element(self):
        """one_or_none() should return element when exactly one exists"""
        pv = PathValues([42])
        assert pv.one_or_none() == 42

    def test_one_or_none_with_empty_list(self):
        """one_or_none() should return None when list is empty"""
        pv = PathValues()
        assert pv.one_or_none() is None

    def test_one_or_none_with_multiple_elements(self):
        """one_or_none() should raise ValueError when multiple elements exist"""
        pv = PathValues([1, 2, 3])

        with pytest.raises(ValueError, match="contains 3.*one or none"):
            pv.one_or_none()


class TestPathValuesContains:
    """Test PathValues membership testing"""

    def test_contains_with_present_item(self):
        """__contains__ should return True for items in the list"""
        pv = PathValues([1, 2, 3, 4, 5])

        assert 1 in pv
        assert 3 in pv
        assert 5 in pv

    def test_contains_with_absent_item(self):
        """__contains__ should return False for items not in the list"""
        pv = PathValues([1, 2, 3])

        assert 0 not in pv
        assert 10 not in pv
        assert "hello" not in pv

    def test_contains_with_empty_list(self):
        """__contains__ should return False for empty PathValues"""
        pv = PathValues()

        assert 1 not in pv
        assert None not in pv
        assert "" not in pv

    def test_contains_with_duplicates(self):
        """__contains__ should work correctly with duplicates"""
        pv = PathValues([1, 2, 2, 3, 3, 3])

        assert 1 in pv
        assert 2 in pv
        assert 3 in pv
        assert 4 not in pv

    def test_contains_with_complex_types(self):
        """__contains__ should work with dicts and lists"""
        pv = PathValues([
            {"name": "Alice"},
            {"name": "Bob"},
            [1, 2, 3]
        ])

        assert {"name": "Alice"} in pv
        assert [1, 2, 3] in pv
        assert {"name": "Charlie"} not in pv


class TestPathValuesEdgeCases:
    """Test PathValues edge cases and special scenarios"""

    def test_pathvalues_with_none_values(self):
        """PathValues should handle None values correctly"""
        pv = PathValues([1, None, 3, None])

        assert len(pv) == 4
        assert pv[1] is None
        assert pv[3] is None
        assert None in pv

        # first/last with None
        pv_none_first = PathValues([None, 1, 2])
        assert pv_none_first.first() is None

        pv_none_last = PathValues([1, 2, None])
        assert pv_none_last.last() is None

    def test_pathvalues_with_missing_path_values(self):
        """PathValues should handle MissingPath instances"""
        missing = MissingPath()
        pv = PathValues([1, missing, 3])

        assert len(pv) == 3
        assert isinstance(pv[1], MissingPath)
        assert missing in pv

    def test_pathvalues_with_nested_pathvalues(self):
        """PathValues can contain other PathValues instances"""
        inner1 = PathValues([1, 2])
        inner2 = PathValues([3, 4])
        outer = PathValues([inner1, inner2])

        assert len(outer) == 2
        assert isinstance(outer[0], PathValues)
        assert list(outer[0]) == [1, 2]
        assert list(outer[1]) == [3, 4]

    def test_pathvalues_equality(self):
        """PathValues should compare equal if contents are equal"""
        pv1 = PathValues([1, 2, 3])
        pv2 = PathValues([1, 2, 3])
        regular_list = [1, 2, 3]

        # PathValues compares equal to list with same contents
        assert pv1 == pv2
        assert pv1 == regular_list
        assert pv2 == regular_list

        # Not equal with different contents
        pv3 = PathValues([1, 2, 4])
        assert pv1 != pv3

    def test_pathvalues_ordering_preserved(self):
        """PathValues should preserve insertion order"""
        items = [5, 3, 8, 1, 9, 2]
        pv = PathValues(items)

        assert list(pv) == items

        # After operations
        pv.append(0)
        assert list(pv) == [5, 3, 8, 1, 9, 2, 0]

    def test_pathvalues_allows_duplicates(self):
        """PathValues should allow duplicate values"""
        pv = PathValues([1, 2, 2, 3, 3, 3])

        assert len(pv) == 6
        assert pv.count(1) == 1
        assert pv.count(2) == 2
        assert pv.count(3) == 3

    def test_pathvalues_iteration(self):
        """PathValues should support iteration"""
        pv = PathValues([1, 2, 3])

        result = []
        for item in pv:
            result.append(item * 2)

        assert result == [2, 4, 6]

        # List comprehension
        doubled = [x * 2 for x in pv]
        assert doubled == [2, 4, 6]

    def test_pathvalues_with_heterogeneous_types(self):
        """PathValues should handle mixed types"""
        pv = PathValues([1, "hello", 3.14, True, None, {"key": "value"}])

        assert len(pv) == 6
        assert pv[0] == 1
        assert pv[1] == "hello"
        assert pv[2] == 3.14
        assert pv[3] is True
        assert pv[4] is None
        assert pv[5] == {"key": "value"}


class TestPathValuesUseCases:
    """Test PathValues in realistic use cases"""

    def test_pathvalues_for_wildcard_results(self):
        """PathValues represents results from wildcard path operations"""
        # Simulating path evaluation returning multiple matches
        data = {
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35}
            ]
        }

        # Simulating @users.*.name evaluation
        names = PathValues(["Alice", "Bob", "Charlie"])

        assert len(names) == 3
        assert "Alice" in names
        assert names.first() == "Alice"
        assert names.last() == "Charlie"

    def test_pathvalues_empty_for_no_matches(self):
        """Empty PathValues indicates no matches found"""
        # Simulating a path that matched nothing
        no_matches = PathValues()

        assert len(no_matches) == 0
        assert not no_matches
        assert no_matches.first(default="none") == "none"

    def test_pathvalues_distinguishes_from_regular_list(self):
        """PathValues type indicates multi-match path result"""
        # This is important for operator adaptation
        regular_value = [1, 2, 3]  # A field that IS a list
        path_result = PathValues([1, 2, 3])  # Result from @items.* path

        assert regular_value == path_result  # Same content
        assert not isinstance(regular_value, PathValues)
        assert isinstance(path_result, PathValues)

        # Can distinguish in code
        def handle_value(val):
            if isinstance(val, PathValues):
                return "multi-match"
            elif isinstance(val, list):
                return "list-value"
            else:
                return "other"

        assert handle_value(regular_value) == "list-value"
        assert handle_value(path_result) == "multi-match"

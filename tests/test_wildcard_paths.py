import pytest
from jaf.utils import path_values, WildcardResultsList

class TestWildcardPathEvaluation:
    def test_single_star_in_dict(self):
        """Test a single '*' wildcard matching dictionary keys."""
        data = {
            "a": {
                "b1": {"c": 10, "d": 1},
                "b2": {"c": 20, "d": 2},
                "b3": {"c": 30, "d": 3}
            },
            "e": 40
        }
        # Path: a.*.c
        result = path_values(["a", "*", "c"], data)
        assert isinstance(result, WildcardResultsList)
        assert sorted(list(result)) == [10, 20, 30]

        # Path: a.*.d
        result_d = path_values(["a", "*", "d"], data)
        assert isinstance(result_d, WildcardResultsList)
        assert sorted(list(result_d)) == [1, 2, 3]
        
        # Path: a.*.non_existent
        result_none = path_values(["a", "*", "non_existent"], data)
        assert isinstance(result_none, WildcardResultsList)
        assert list(result_none) == []

    def test_single_star_in_list(self):
        """Test a single '*' wildcard matching list indices."""
        data = {
            "items": [
                {"id": "A", "value": 100},
                {"id": "B", "value": 200},
                {"id": "C", "value": 300}
            ]
        }
        # Path: items.*.value
        result = path_values(["items", "*", "value"], data)
        assert isinstance(result, WildcardResultsList)
        assert sorted(list(result)) == [100, 200, 300]

        # Path: items.*.id
        result_id = path_values(["items", "*", "id"], data)
        assert isinstance(result_id, WildcardResultsList)
        assert sorted(list(result_id)) == ["A", "B", "C"]

        # Path: items.* (get all items in the list)
        # This should return the items themselves if '*' is the last component
        # or if the subsequent path part is applied to each item.
        # If items.*.value gets values, items.* should get the dicts.
        result_items = path_values(["items", "*"], data)
        assert isinstance(result_items, WildcardResultsList)
        # The order might not be guaranteed by dict iteration in _match_path,
        # but for lists, it should be. Let's check content.
        assert len(result_items) == 3
        assert {"id": "A", "value": 100} in result_items
        assert {"id": "B", "value": 200} in result_items
        assert {"id": "C", "value": 300} in result_items


    def test_single_star_no_match(self):
        """Test '*' wildcard with no matching children or keys."""
        data = {
            "a": {
                "b1": {"c": 10},
            },
            "empty_dict": {},
            "empty_list": []
        }
        # Path: a.*.z (z does not exist in children of a)
        result = path_values(["a", "*", "z"], data)
        assert isinstance(result, WildcardResultsList)
        assert list(result) == []

        # Path: empty_dict.*.key
        result_empty_dict = path_values(["empty_dict", "*", "key"], data)
        assert isinstance(result_empty_dict, WildcardResultsList)
        assert list(result_empty_dict) == []
        
        # Path: empty_list.*.key
        result_empty_list = path_values(["empty_list", "*", "key"], data)
        assert isinstance(result_empty_list, WildcardResultsList)
        assert list(result_empty_list) == []

    def test_basic_double_star_in_dict(self):
        """Test a basic 'recursive descent' (**) wildcard in dictionaries."""
        data = {
            "a": {
                "target": 1,
                "b": {
                    "target": 2,
                    "c": {
                        "d": 3, # no target here
                        "target": 4
                    }
                }
            },
            "target": 5, # top-level target
            "f": {
                "g": { "h" : "no target here"}
            }
        }
        # Path: **.target
        result = path_values(["**", "target"], data)
        assert isinstance(result, WildcardResultsList)
        # The order of results from '**' can be complex due to traversal order.
        # We should check for the presence and count of expected values.
        # Expected values: 1, 2, 4, 5
        assert len(result) == 4 
        assert 1 in result
        assert 2 in result
        assert 4 in result
        assert 5 in result
        
        # Path: a.**.target (recursive search within 'a')
        result_a_recursive = path_values(["a", "**", "target"], data)
        assert isinstance(result_a_recursive, WildcardResultsList)
        # Expected values from within 'a': 1, 2, 4
        assert len(result_a_recursive) == 3
        assert 1 in result_a_recursive
        assert 2 in result_a_recursive
        assert 4 in result_a_recursive

    def test_double_star_in_list_and_dict(self):
        """Test '**' wildcard traversing lists and dictionaries."""
        data = {
            "id": "doc1",
            "sections": [
                {"name": "s1", "target": 100},
                {"name": "s2", "content": {"text": "hello", "target": 200}},
                {"name": "s3", "items": [{"id": "i1"}, {"id": "i2", "target": 300}]}
            ],
            "metadata": {"target": 400}
        }
        # Path: **.target
        result = path_values(["**", "target"], data)
        assert isinstance(result, WildcardResultsList)
        # Expected: 100, 200, 300, 400
        assert len(result) == 4
        assert 100 in result
        assert 200 in result
        assert 300 in result
        assert 400 in result

        # Path: sections.**.target (recursive search within 'sections' list)
        result_sections_recursive = path_values(["sections", "**", "target"], data)
        assert isinstance(result_sections_recursive, WildcardResultsList)
        # Expected: 100, 200, 300
        assert len(result_sections_recursive) == 3
        assert 100 in result_sections_recursive
        assert 200 in result_sections_recursive
        assert 300 in result_sections_recursive

    def test_double_star_no_match(self):
        """Test '**' wildcard with no matches."""
        data = {
            "a": {"b": {"c": 1}},
            "d": [1, 2, {"e": 3}]
        }
        result = path_values(["**", "non_existent_key"], data)
        assert isinstance(result, WildcardResultsList)
        assert list(result) == []

        result_scoped = path_values(["a", "**", "non_existent_key"], data)
        assert isinstance(result_scoped, WildcardResultsList)
        assert list(result_scoped) == []
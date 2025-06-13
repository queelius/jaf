"""
Tests for JAF path system and wildcard functionality.
"""
import pytest
from jaf.jaf_eval import jaf_eval # Corrected import for jaf_eval
from jaf.path_utils import eval_path, exists, PathValues


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
            },
            "mixed_list": [
                {"type": "a", "value": 1},
                "string_item",
                {"type": "b", "value": 2}
            ]
        }

    def test_simple_path_access(self):
        """Test basic path access"""
        # path: ["user", "name"]
        result = eval_path([["key", "user"], ["key", "name"]], self.nested_data)
        assert result == "Alice"

    def test_nested_path_access(self):
        """Test deeply nested path access"""
        # path: ["user", "profile", "settings", "theme"]
        result = eval_path([["key", "user"], ["key", "profile"], ["key", "settings"], ["key", "theme"]], self.nested_data)
        assert result == "dark"

    def test_array_index_access(self):
        """Test array index access"""
        # path: ["items", 0, "status"]
        result = eval_path([["key", "items"], ["index", 0], ["key", "status"]], self.nested_data)
        assert result == "done"

        # path: ["items", 1, "id"]
        result = eval_path([["key", "items"], ["index", 1], ["key", "id"]], self.nested_data)
        assert result == 2

    def test_nonexistent_path(self):
        """Test accessing non-existent paths"""
        # path: ["nonexistent"]
        result = eval_path([["key", "nonexistent"]], self.nested_data)
        assert result == []

        # path: ["user", "nonexistent"]
        result = eval_path([["key", "user"], ["key", "nonexistent"]], self.nested_data)
        assert result == []
        
        # path: ["items", 5, "status"] (index out of bounds)
        result = eval_path([["key", "items"], ["index", 5], ["key", "status"]], self.nested_data)
        assert result == []

        # path: ["items", 0, "nonkey"] (key not in dict at index)
        result = eval_path([["key", "items"], ["index", 0], ["key", "nonkey"]], self.nested_data)
        assert result == []

    def test_wildcard_single_level(self):
        """Test single-level wildcard '*'"""
        # Get all item statuses
        # path: ["items", "*", "status"]
        result = eval_path([["key", "items"], ["wc_level"], ["key", "status"]], self.nested_data)
        assert isinstance(result, PathValues)
        assert set(result) == {"done", "pending", "done"} # Order might not be guaranteed, use set
        assert len(result) == 3

    def test_wildcard_recursive(self):
        """Test recursive wildcard '**'"""
        # Find all "type" fields anywhere
        # path: ["**", "type"]
        result = eval_path([["wc_recursive"], ["key", "type"]], self.nested_data)
        assert isinstance(result, PathValues)
        # The order can vary based on traversal, so use set for comparison
        expected_types = {"warning", "error", "a", "b"}
        assert set(result) == expected_types
        assert len(result) == len(expected_types)

    def test_wildcard_no_matches(self):
        """Test wildcard when no matches found"""
        data = {"empty_dict": {}, "empty_list": [], "a": {"b": 1}} # Added more data for new cases
        # path: ["empty", "*", "anything"] - old test, assuming "empty" key in self.nested_data
        # For self.nested_data, this would be eval_path([["key", "empty"], ...]) if "empty" existed
        # Let's use the local `data` for clarity on these specific no-match cases.
        
        result = eval_path([["key", "non_existent_key"], ["wc_level"], ["key", "anything"]], data)
        assert result == []

        # path: ["**", "nonexistent"] in self.nested_data
        result = eval_path([["wc_recursive"], ["key", "nonexistent"]], self.nested_data)
        assert result == []

        # New cases from test_wildcard_paths.py:
        # Path: empty_dict.*.key => [["key", "empty_dict"], ["wc_level"], ["key", "key"]]
        result_empty_dict = eval_path([["key", "empty_dict"], ["wc_level"], ["key", "key"]], data)
        assert isinstance(result_empty_dict, PathValues)
        assert list(result_empty_dict) == []
        
        # Path: empty_list.*.key => [["key", "empty_list"], ["wc_level"], ["key", "key"]]
        result_empty_list = eval_path([["key", "empty_list"], ["wc_level"], ["key", "key"]], data)
        assert isinstance(result_empty_list, PathValues)
        assert list(result_empty_list) == []

        # Path: a.*.z (z does not exist in children of a) => [["key", "a"], ["wc_level"], ["key", "z"]]
        result_a_z = eval_path([["key", "a"], ["wc_level"], ["key", "z"]], data)
        assert isinstance(result_a_z, PathValues)
        assert list(result_a_z) == []

    def test_path_special_form(self):
        """Test path special form in evaluator"""
        # path: ["user", "name"]
        query = ["path", [["key", "user"], ["key", "name"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result == "Alice"

    def test_exists_function(self):
        """Test exists? special form"""
        # Existing path
        # path: ["user", "email"]
        query = ["exists?", ["path", [["key", "user"], ["key", "email"]]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is True

        # Non-existing path
        # path: ["user", "phone"]
        query = ["exists?", ["path", [["key", "user"], ["key", "phone"]]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is False

    def test_exists_with_wildcards(self):
        """Test exists? with wildcard paths"""
        # Check if any item has status field
        # path: ["items", "*", "status"]
        query = ["exists?", ["path", [["key", "items"], ["wc_level"], ["key", "status"]]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is True

        # Check for non-existent field with wildcard
        # path: ["items", "*", "nonexistent"]
        query = ["exists?", ["path", [["key", "items"], ["wc_level"], ["key", "nonexistent"]]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result is False # eval_path returns [], so exists should be False

    def test_path_argument_validation(self):
        """Test path argument validation in jaf_eval for 'path' operator"""
        # Path argument to 'path' operator must be a list of components
        with pytest.raises(ValueError, match="path argument must be a list of path components"): 
            jaf_eval.eval(["path", "not.a.list.of.components"], self.nested_data)

        # Each component in the path must be a list
        # This validation is now primarily within _match_recursive or its callers in utils.py
        # jaf_eval's _eval_special_form for 'path' does basic list checks.
        # Let's test that jaf_eval still catches non-list components if the outer path is a list.
        with pytest.raises(ValueError, match="Path component must be a list"):
             jaf_eval.eval(["path", [["key", "user"], "not-a-component-list"]], self.nested_data)

        # Test that an empty list of components is handled by eval_path, not an error here
        # but jaf_eval itself might have opinions if the path *expression* is just ["path"]
        # For ["path", []], it should return the root object.
        assert jaf_eval.eval(["path", []], self.nested_data) == self.nested_data

        # Test that a path component that is not a list and not a string (if we were parsing strings)
        # also raises an error.
        with pytest.raises(ValueError, match="Path component must be a list"):
            jaf_eval.eval(["path", [123]], self.nested_data)

        # Test that a path component list item is not an empty list
        with pytest.raises(ValueError, match="Path component cannot be empty"):
            jaf_eval.eval(["path", [[]]], self.nested_data)
        
        # Test that a path component list item has a valid operation string
        with pytest.raises(ValueError, match="Path component operation must be a string"):
            jaf_eval.eval(["path", [[123, "arg"]]], self.nested_data)

        # Test for unknown operation
        with pytest.raises(ValueError, match="Unknown path operation: unknown_op"):
            jaf_eval.eval(["path", [["unknown_op", "arg"]]], self.nested_data)


    def test_empty_path_components(self):
        """Test empty path returns root object"""
        result = eval_path([], self.nested_data)
        assert result == self.nested_data

    def test_indices_access(self):
        """Test path access using specific list indices ['indices', [...]]"""
        data = {"arr": ["a", "b", "c", "d", "e"]}
        # path: ["arr", ["indices", [0, 2, 4]]]
        result = eval_path([["key", "arr"], ["indices", [0, 2, 4]]], data)
        assert isinstance(result, PathValues)
        assert result == ["a", "c", "e"]

        # path: ["arr", ["indices", [4, 2, 0]]] (order should be preserved)
        result = eval_path([["key", "arr"], ["indices", [4, 2, 0]]], data)
        assert isinstance(result, PathValues)
        assert result == ["e", "c", "a"]
        
        # path: ["items", ["indices", [0,2]], "tags"]
        result = eval_path([["key", "items"], ["indices", [0,2]], ["key", "tags"]], self.nested_data)
        assert isinstance(result, PathValues)
        assert result == [["urgent", "bug"], ["enhancement"]] # This will be a PathValues of lists

        # path: ["items", ["indices", [0,2]], "tags", 0]
        result = eval_path([["key", "items"], ["indices", [0,2]], ["key", "tags"], ["index", 0]], self.nested_data)
        assert isinstance(result, PathValues)
        assert result == ["urgent", "enhancement"]


    def test_slice_access(self):
        """Test path access using list slicing ['slice', start, stop, step]"""
        data = {"arr": ["a", "b", "c", "d", "e", "f"]}
        # path: ["arr", ["slice", 1, 4, 1]] -> arr[1:4:1] -> ["b", "c", "d"]
        result = eval_path([["key", "arr"], ["slice", 1, 4, 1]], data)
        assert isinstance(result, PathValues)
        assert result == ["b", "c", "d"]

        # path: ["arr", ["slice", None, None, 2]] -> arr[::2] -> ["a", "c", "e"]
        result = eval_path([["key", "arr"], ["slice", None, None, 2]], data)
        assert isinstance(result, PathValues)
        assert result == ["a", "c", "e"]

        # path: ["arr", ["slice", 3, None, None]] -> arr[3:] -> ["d", "e", "f"]
        result = eval_path([["key", "arr"], ["slice", 3, None, None]], data) # Step defaults to 1
        assert isinstance(result, PathValues)
        assert result == ["d", "e", "f"]

        # path: ["items", ["slice", 0, 2, 1], "status"]
        result = eval_path([["key", "items"], ["slice", 0, 2, 1], ["key", "status"]], self.nested_data)
        assert isinstance(result, PathValues)
        assert result == ["done", "pending"]


    def test_regex_key_access(self):
        """Test path access using regex key matching ['regex_key', pattern]"""
        data = {
            "user_alice": {"id": 1, "status": "active"},
            "user_bob": {"id": 2, "status": "inactive"},
            "item_1": {"id": 3, "status": "pending"}
        }
        # path: [["regex_key", "^user_.*"], "status"]
        result = eval_path([["regex_key", "^user_.*"], ["key", "status"]], data)
        assert isinstance(result, PathValues)
        assert set(result) == {"active", "inactive"} # Order of dict keys not guaranteed

        # path: [["regex_key", ".*_bob"], "id"]
        result = eval_path([["regex_key", ".*_bob"], ["key", "id"]], data)
        assert result == [2] # PathValues, but only one match

    def test_regex_key_no_match(self):
        """Test regex key matching with no matches"""
        data = {"user_alice": {"id": 1}}
        # path: [["regex_key", "^item_.*"]]
        result = eval_path([["regex_key", "^item_.*"]], data)
        assert result == []

    def test_mixed_component_types(self):
        """Test paths combining various new component types"""
        # path: ["items", ["slice", 0, 2, 1], "tags", ["index", 0]]
        # items[0:2] -> items[0], items[1]
        # items[0].tags[0] -> "urgent"
        # items[1].tags[0] -> "feature"
        result = eval_path(
            [["key", "items"], ["slice", 0, 2, 1], ["key", "tags"], ["index", 0]],
            self.nested_data
        )
        assert isinstance(result, PathValues)
        assert result == ["urgent", "feature"]

        # path: ["metadata", ["regex_key", "err.*"], ["index", 0], "message"]
        # metadata.errors[0].message -> "deprecated"
        result = eval_path(
            [["key", "metadata"], ["regex_key", "err.*"], ["index", 0], ["key", "message"]],
            self.nested_data
        )
        # This will match "errors" key. Then ["index", 0] applies to the list of errors.
        # Then ["key", "message"] applies to the first error object.
        assert result == ["deprecated"] # PathValues with one item

        # path: ["items", ["indices", [0, 2]], ["key", "tags"], ["wc_level"]]
        # items[0].tags.* -> "urgent", "bug"
        # items[2].tags.* -> "enhancement"
        result = eval_path(
            [["key", "items"], ["indices", [0, 2]], ["key", "tags"], ["wc_level"]],
            self.nested_data
        )
        assert isinstance(result, PathValues)
        assert set(result) == {"urgent", "bug", "enhancement"}


class TestWildcardEdgeCases:
    """Test edge cases for wildcard functionality"""

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
        # path: ["categories", "*", "items", "*", "name"]
        path = [
            ["key", "categories"], ["wc_level"], 
            ["key", "items"], ["wc_level"], 
            ["key", "name"]
        ]
        result = eval_path(path, data)
        assert isinstance(result, PathValues)
        assert set(result) == {"laptop", "phone", "novel", "manual"}

    def test_wc_level_on_list_of_dicts(self):
        """Test wc_level directly on a list of dictionaries."""
        data = {
            "items": [
                {"id": "A", "value": 100},
                {"id": "B", "value": 200},
                {"id": "C", "value": 300}
            ]
        }
        # Path: items.* => [["key", "items"], ["wc_level"]]
        result_items = eval_path([["key", "items"], ["wc_level"]], data)
        assert isinstance(result_items, PathValues)
        assert len(result_items) == 3
        assert result_items[0] == {"id": "A", "value": 100}
        assert result_items[1] == {"id": "B", "value": 200}
        assert result_items[2] == {"id": "C", "value": 300}

    def test_scoped_wc_recursive_in_dict(self):
        """Test recursive wildcard scoped within a dictionary key."""
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
            "target": 5, # top-level target (should not be found by scoped search)
        }
        # Path: a.**.target => [["key", "a"], ["wc_recursive"], ["key", "target"]]
        result_a_recursive = eval_path([["key", "a"], ["wc_recursive"], ["key", "target"]], data)
        assert isinstance(result_a_recursive, PathValues)
        assert len(result_a_recursive) == 3
        assert set(result_a_recursive) == {1, 2, 4}

    def test_scoped_wc_recursive_in_list_of_dicts(self):
        """Test recursive wildcard scoped within a list of dictionaries."""
        data = {
            "id": "doc1",
            "sections": [
                {"name": "s1", "target": 100},
                {"name": "s2", "content": {"text": "hello", "target": 200}},
                {"name": "s3", "items": [{"id": "i1"}, {"id": "i2", "target": 300}]}
            ],
            "metadata": {"target": 400} # top-level target (should not be found)
        }
        # Path: sections.**.target => [["key", "sections"], ["wc_recursive"], ["key", "target"]]
        result_sections_recursive = eval_path([["key", "sections"], ["wc_recursive"], ["key", "target"]], data)
        assert isinstance(result_sections_recursive, PathValues)
        assert len(result_sections_recursive) == 3
        assert set(result_sections_recursive) == {100, 200, 300}

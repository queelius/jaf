"""
Tests for JAF path system and wildcard functionality.
"""
import pytest
from jaf.jaf_eval import jaf_eval # Corrected import for jaf_eval
from jaf.path_evaluation import eval_path, exists, is_valid_path_str
from jaf.path_types import PathValues
from jaf.path_exceptions import PathSyntaxError


class TestPathSystem:
    """Test path access and wildcards"""

    def setup_method(self):
        """Set up test data"""
        self.nested_data = {
            "user": {
                "name": "Alice",
                "email": "alice@example.com",
                "active": True,
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
            ],
            "config": {
                "global_setting": "enabled"
            }
        }

    def test_is_valid_path_str(self):
        """Test if a path is valid"""
        # Valid path: ["user", "name"]
        assert is_valid_path_str("user.name")
        assert is_valid_path_str("user.profile.settings.theme")
        # Invalid path: "user.name." (trailing dot)
        # assert not is_valid_path_str("user.name.")
        # Invalid path: "user..name" (double dot)
        assert not is_valid_path_str("user..name")
        # wildcards
        assert is_valid_path_str("user.*.name")
        assert is_valid_path_str("user.**.name")
        assert is_valid_path_str("user[*].name.*")

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
        
        # path: ["items", -1, "status"]
        result = eval_path([["key", "items"], ["index", -1], ["key", "status"]], self.nested_data)
        assert result == "done"


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
        
        # path: ["items", -10, "status"] (negative index out of bounds)
        result = eval_path([["key", "items"], ["index", -10], ["key", "status"]], self.nested_data)
        assert result == []

    def test_wildcard_single_level(self):
        """Test single-level wildcard '*'"""
        # Get all item statuses
        # path: ["items", "*", "status"]
        result = eval_path([["key", "items"], ["wc_level"], ["key", "status"]], self.nested_data)
        assert isinstance(result, PathValues)
        assert set(result) == {"done", "pending"} # "done" appears twice, PathValues preserves duplicates if underlying data has them at different points
        assert sorted(list(result)) == sorted(["done", "pending", "done"])


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
        data = {"empty_dict": {}, "empty_list": [], "a": {"b": 1}} 
        
        result = eval_path([["key", "non_existent_key"], ["wc_level"], ["key", "anything"]], data)
        assert result == [] # Specific path part fails before wildcard

        result = eval_path([["wc_recursive"], ["key", "nonexistent"]], self.nested_data)
        assert isinstance(result, PathValues) # wc_recursive always implies multi-match potential
        assert list(result) == []


        result_empty_dict = eval_path([["key", "empty_dict"], ["wc_level"], ["key", "key"]], data)
        assert isinstance(result_empty_dict, PathValues)
        assert list(result_empty_dict) == []
        
        result_empty_list = eval_path([["key", "empty_list"], ["wc_level"], ["key", "key"]], data)
        assert isinstance(result_empty_list, PathValues)
        assert list(result_empty_list) == []

        result_a_z = eval_path([["key", "a"], ["wc_level"], ["key", "z"]], data)
        assert isinstance(result_a_z, PathValues)
        assert list(result_a_z) == []

    def test_root_operator_evaluation_return_root(self):
        """Test the ['root'] operator in eval_path"""
        # Path: [["root"]] -> should return the root object itself.
        result = eval_path([["root"]], self.nested_data)
        assert result == self.nested_data

    def test_root_operator_evaluation_alice(self):
        """Test root operator in various paths"""

        # Path: [["root"], ["key", "user"], ["key", "name"]] -> "Alice"
        result = eval_path([["root"], ["key", "user"], ["key", "name"]], self.nested_data)
        assert result == "Alice"

    def test_root_operator_evaluation_complex_paths(self):
        # Path: [["key", "user"], ["key", "profile"], ["root"], ["key", "items"], ["index", 0], ["key", "status"]] -> "done"
        path = [
            ["key", "user"], ["key", "profile"], 
            ["root"], 
            ["key", "items"], ["index", 0], ["key", "status"]
        ]
        result = eval_path(path, self.nested_data)
        assert result == "done"

    def test_root_operator_evaluation_alice_email(self):
        # Path: [["key", "items"], ["index", 0], ["root"], ["key", "user"], ["key", "email"]] -> "alice@example.com"
        path = [
            ["key", "items"], ["index", 0],
            ["root"],
            ["key", "user"], ["key", "email"]
        ]
        result = eval_path(path, self.nested_data)
        assert result == "alice@example.com"

    def test_root_operator_evaluation_with_nonexistent_key_before_root(self):

        path_fail_then_root = [["key", "nonexistent"], ["root"], ["key", "user"]]
        result = eval_path(path_fail_then_root, self.nested_data)
        assert result == []

    def test_root_operator_evaluation_with_multi_roots_and_nonexistent_keys(self):

        path_root_fail_root = [["root"], ["key", "nonexistent"], ["root"], ["key", "user"]]
        result = eval_path(path_root_fail_root, self.nested_data)
        assert result == []

    def test_root_operator_evaluation_with_successive_roots(self):

        # Path: [["root"], ["root"], ["key", "user"]] -> user object (double root should be fine)
        result = eval_path([["root"], ["root"], ["key", "user"]], self.nested_data)
        assert result == self.nested_data["user"]

        # Path: [["key", "user"], ["root"], ["key", "config"], ["root"], ["key", "items"], ["index", 1]]
        path_multiple_roots = [
            ["key", "user"], ["root"], ["key", "config"], ["root"], ["root"], ["root"], ["key", "items"], ["index", 1]
        ]
        result = eval_path(path_multiple_roots, self.nested_data)
        assert result == self.nested_data["items"][1]

    def test_path_special_form(self):
        """Test path special form in evaluator"""
        # path: ["user", "name"]
        query = ["path", [["key", "user"], ["key", "name"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result == "Alice"

    def test_path_special_form_2(self):
        """Test path special form in evaluator"""
        # path: ["user", "name"]
        query = ["path", [["key", "user"], ["key", "active"]]]
        result = jaf_eval.eval(query, self.nested_data)
        assert result


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
        
        # Path to null/None (if it existed) should exist
        temp_data = {"a": None}
        query_null = ["exists?", ["path", [["key", "a"]]]]
        assert jaf_eval.eval(query_null, temp_data) is True


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
        assert result is False 

    def test_path_argument_validation_in_eval_path(self):
        """Test path argument validation directly with eval_path for PathSyntaxError"""
        with pytest.raises(PathSyntaxError, match="Path expression must be a list of components."):
            eval_path("not.a.list", self.nested_data) # type: ignore

        with pytest.raises(PathSyntaxError, match="Each path component must be a non-empty list starting with an operation string."):
            eval_path([["key", "user"], "not-a-component-list"], self.nested_data) # type: ignore
        
        with pytest.raises(PathSyntaxError, match="Each path component must be a non-empty list starting with an operation string."):
            eval_path([123], self.nested_data) # type: ignore

        with pytest.raises(PathSyntaxError, match="Each path component must be a non-empty list starting with an operation string."):
            eval_path([[]], self.nested_data)

        with pytest.raises(PathSyntaxError, match="Each path component must be a non-empty list starting with an operation string."):
            eval_path([[123, "arg"]], self.nested_data) # type: ignore
        
        # These internal errors are caught by _match_recursive
        with pytest.raises(PathSyntaxError, match="Unknown path operation: 'unknown_op'"):
            eval_path([["unknown_op", "arg"]], self.nested_data)
        
        with pytest.raises(PathSyntaxError, match="'key' operation expects a single string argument."):
            eval_path([["key", 123]], self.nested_data) # type: ignore


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
        assert result == [["urgent", "bug"], ["enhancement"]] 

        # path: ["items", ["indices", [0,2]], "tags", 0]
        result = eval_path([["key", "items"], ["indices", [0,2]], ["key", "tags"], ["index", 0]], self.nested_data)
        assert isinstance(result, PathValues)
        assert result == ["urgent", "enhancement"]
        
        # Indices out of bounds
        result = eval_path([["key", "items"], ["indices", [0, 10]]], self.nested_data) # 10 is out of bounds
        assert isinstance(result, PathValues)
        assert result == [self.nested_data["items"][0]]


    def test_slice_access(self):
        """Test path access using list slicing ['slice', start, stop, step]"""
        data = {"arr": ["a", "b", "c", "d", "e", "f"]}
        # path: ["arr", ["slice", 1, 4, 1]] -> arr[1:4:1] -> ["b", "c", "d"]
        result = eval_path([["key", "arr"], ["slice", 1, 4, 1]], data)
        assert isinstance(result, PathValues)
        assert result == ["b", "c", "d"]

        # path: ["arr", ["slice", None, None, 2]] -> arr[::2] -> ["a", "c", "e"]
        result = eval_path([["key", "arr"], ["slice", 0, -1, 2]], data)
        assert isinstance(result, PathValues)
        assert result == ["a", "c", "e"]

        # path: ["arr", ["slice", 3, None, None]] -> arr[3:] -> ["d", "e", "f"]
        result = eval_path([["key", "arr"], ["slice", 3, None, None]], data) 
        assert isinstance(result, PathValues)
        assert result == ["d", "e", "f"]

        # path: ["items", ["slice", 0, 2, 1], "status"]
        result = eval_path([["key", "items"], ["slice", 0, 2, 1], ["key", "status"]], self.nested_data)
        assert isinstance(result, PathValues)
        assert result == ["done", "pending"]
        
        # Slice yielding empty list
        result = eval_path([["key", "arr"], ["slice", 5, 2, 1]], data) # start > stop
        assert isinstance(result, PathValues)
        assert result == []


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
        assert set(result) == {"active", "inactive"} 

        # path: [["regex_key", ".*_bob"], "id"]
        result = eval_path([["regex_key", ".*_bob"], ["key", "id"]], data)
        assert isinstance(result, PathValues) 
        assert result == [2] 

    def test_regex_key_no_match(self):
        """Test regex key matching with no matches"""
        data = {"user_alice": {"id": 1}}
        # path: [["regex_key", "^item_.*"]]
        result = eval_path([["regex_key", "^item_.*"]], data)
        assert isinstance(result, PathValues)
        assert result == []
        
        with pytest.raises(PathSyntaxError, match="'regex_key' operation: invalid regex pattern"):
            eval_path([["regex_key", "["]], data)


    def test_mixed_component_types(self):
        """Test paths combining various new component types"""
        result = eval_path(
            [["key", "items"], ["slice", 0, 2, 1], ["key", "tags"], ["index", 0]],
            self.nested_data
        )
        assert isinstance(result, PathValues)
        assert result == ["urgent", "feature"]

        result = eval_path(
            [["key", "metadata"], ["regex_key", "err.*"], ["index", 0], ["key", "message"]],
            self.nested_data
        )
        assert isinstance(result, PathValues)
        assert result == ["deprecated"] 

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
                        "d": 3, 
                        "target": 4
                    }
                }
            },
            "target": 5, 
        }
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
            "metadata": {"target": 400} 
        }
        result_sections_recursive = eval_path([["key", "sections"], ["wc_recursive"], ["key", "target"]], data)
        assert isinstance(result_sections_recursive, PathValues)
        assert len(result_sections_recursive) == 3
        assert set(result_sections_recursive) == {100, 200, 300}

    def test_wc_recursive_then_accessor(self):
        """Test recursive wildcard followed by an accessor like index."""
        data = {
            "a": {"list": [10, 20, {"list": [30, 40]}]},
            "b": {"list": [50, 60]}
        }
        # Path: **.list[0]
        result = eval_path([["wc_recursive"], ["key", "list"], ["index", 0]], data)
        assert isinstance(result, PathValues)
        assert set(result) == {10, 30, 50} # Collects the first item of every 'list' found

    def test_wc_level_then_accessor(self):
        """Test level wildcard followed by an accessor like index."""
        data = {
            "level1_a": {"list_a": [1,2], "name": "obj_a"},
            "level1_b": {"list_b": [3,4], "name": "obj_b"},
            "level1_c": [5,6] # A list directly
        }
        # Path: *.[index:0] (get first element of any list at the first level of values)
        # This is a bit ambiguous. If wc_level matches a list, then index 0 applies.
        # If wc_level matches a dict, then index 0 on dict fails.
        # The current implementation of wc_level iterates values of dicts or items of lists.
        # So, if a value is a list, then [index,0] applies to it.
        
        # Path: *.list_a[0] - this is more specific
        # This would be [["wc_level"], ["key", "list_a"], ["index", 0]]
        # This would only match if a top-level key has a "list_a"
        
        # Let's test: Get the first item of any list found under any key at the first level.
        # Path: *[*][0] - this is not how JAF paths are structured.
        # Path: *[0] - if the value matched by * is a list.
        # Path: [["wc_level"], ["index", 0]]
        result = eval_path([["wc_level"], ["index", 0]], data)
        assert isinstance(result, PathValues)
        # Values of data:
        # {"list_a": [1,2], "name": "obj_a"} -> index 0 fails
        # {"list_b": [3,4], "name": "obj_b"} -> index 0 fails
        # [5,6] -> index 0 is 5
        assert result == [5]

        # Path: *.list_a[0]
        # This should be [["wc_level"], ["key", "list_a"], ["index", 0]]
        # This means, for each value at root: value.list_a[0]
        # data["level1_a"].list_a[0] -> 1
        # data["level1_b"].list_a -> error (no list_a)
        # data["level1_c"].list_a -> error (not a dict)
        result2 = eval_path([["wc_level"], ["key", "list_a"], ["index", 0]], data)
        assert isinstance(result2, PathValues)
        assert result2 == [1]

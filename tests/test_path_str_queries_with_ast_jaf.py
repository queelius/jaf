import unittest
from jaf.jaf_eval import jaf_eval, jafError
from jaf.path_exceptions import PathSyntaxError


class TestJafEvalPathStrings(unittest.TestCase):
    """Test JAF evaluation with path strings that get converted to AST"""

    def setUp(self):
        """Set up test data"""
        self.test_data = {
            "user": {
                "name": "John Doe",
                "age": 30,
                "email": "john@example.com",
                "tags": ["developer", "python", "javascript"],
                "profile": {
                    "bio": "Software developer",
                    "skills": {
                        "languages": ["Python", "JavaScript", "Go"],
                        "frameworks": ["Django", "React", "Vue"]
                    }
                }
            },
            "user-test-wc": {
                "name": "Jane Doe",
            },
            "items": [
                {"id": 1, "name": "Item 1", "active": True},
                {"id": 2, "name": "Item 2", "active": False},
                {"id": 3, "name": "Item 3", "active": True}
            ],
            "config": {
                "api_version": "v1.2.3",
                "debug_mode": True,
                "timeout": 30
            }
        }

    def test_simple_key_path_string(self):
        """Test simple key access using path string"""
        # String path
        result = jaf_eval.eval(["path", "user.name"], self.test_data)
        self.assertEqual(result, "John Doe")
        
    def test_simple_wc(self):
        """Test simple key access using path string"""
        # String path
        # result = jaf_eval.eval(["path", [["wc_level"], ['key', 'name']]], self.test_data)
        result = jaf_eval.eval(["path", "*.name"], self.test_data)

        # check to ensure it has both John Doe and Jane Doe
        self.assertEqual(set(result), {"John Doe", "Jane Doe"})

    def test_array_index_path_string(self):
        """Test array index access using path string"""
        result = jaf_eval.eval(["path", "items[0].name"], self.test_data)
        self.assertEqual(result, "Item 1")
        
        result = jaf_eval.eval(["path", "items[1].id"], self.test_data)
        self.assertEqual(result, 2)

    def test_array_slice_path_string(self):
        """Test array slicing using path string"""
        result = jaf_eval.eval(["path", "items[0:2].name"], self.test_data)
        self.assertEqual(set(result), {"Item 1", "Item 2"})
        
        result = jaf_eval.eval(["path", "items[1:].id"], self.test_data)
        self.assertEqual(set(result), {2, 3})

    def test_array_indices_path_string(self):
        """Test specific array indices using path string"""
        result = jaf_eval.eval(["path", "items[0,2].name"], self.test_data)
        self.assertEqual(set(result), {"Item 1", "Item 3"})

    def test_wildcard_level_path_string(self):
        """Test level wildcard using path string"""
        result = jaf_eval.eval(["path", "items[*].name"], self.test_data)
        self.assertEqual(set(result), {"Item 1", "Item 2", "Item 3"})
        
        # Test explicit bracket form
        result2 = jaf_eval.eval(["path", "items[*].id"], self.test_data)
        self.assertEqual(set(result2), {1, 2, 3})

    def test_wildcard_recursive_path_string(self):
        """Test recursive wildcard using path string"""
        result = jaf_eval.eval(["path", "**.name"], self.test_data)
        expected_names = {"John Doe", "Jane Doe", "Item 1", "Item 2", "Item 3"}
        self.assertEqual(set(result), expected_names)

    def test_complex_nested_path_string(self):
        """Test complex nested path with wildcards"""
        result = jaf_eval.eval(["path", "user.profile.skills[*][*]"], self.test_data)
        expected_skills = {"Python", "JavaScript", "Go", "Django", "React", "Vue"}
        self.assertEqual(set(result), expected_skills)

    def test_path_string_vs_ast_equivalence(self):
        """Test that string paths and equivalent AST paths produce the same results"""
        test_cases = [
            ("user.name", [["key", "user"], ["key", "name"]]),
            ("items[0]", [["key", "items"], ["index", 0]]),
            ("items[*].name", [["key", "items"], ["wc_level"], ["key", "name"]]),
            ("items[0:2]", [["key", "items"], ["slice", 0, 2]]),
            ("items[0,2]", [["key", "items"], ["indices", [0, 2]]]),
        ]
        
        for path_string, path_ast in test_cases:
            with self.subTest(path_string=path_string):
                string_result = jaf_eval.eval(["path", path_string], self.test_data)
                ast_result = jaf_eval.eval(["path", path_ast], self.test_data)
                self.assertEqual(string_result, ast_result)

    def test_path_string_with_exists_predicate_1(self):
        """Test path strings used within exists? predicate"""
        # Test with existing path
        result = jaf_eval.eval(["exists?", ["path", "user.email"]], self.test_data)
        self.assertTrue(result)

    def test_path_string_with_exists_predicate_2(self):
        # Test with non-existing path
        result = jaf_eval.eval(["exists?", ["path", "user.phone"]], self.test_data)
        print(f"Result for non-existing path: {result}")
        self.assertFalse(result)
        
    def test_path_string_with_exists_predicate_3(self):
        # Test with array element
        result = jaf_eval.eval(["exists?", ["path", "items[0].name"]], self.test_data)
        self.assertTrue(result)

    def test_path_string_in_filter_expressions(self):
        """Test path strings used in complex filter expressions"""
        # Check if user's age is greater than 25
        result = jaf_eval.eval(
            ["gt?", ["path", "user.age"], 25], 
            self.test_data
        )
        self.assertTrue(result)
        
        # Check if first item is active
        result = jaf_eval.eval(
            ["eq?", ["path", "items[0].active"], True], 
            self.test_data
        )
        self.assertTrue(result)

    def test_path_string_with_conditional_logic(self):
        """Test path strings in if/and/or expressions"""
        # If user age > 25, return user name, otherwise return "Unknown"
        result = jaf_eval.eval([
            "if",
            ["gt?", ["path", "user.age"], 25],
                    ["path", "user.name"],
            "Unknown"
        ], self.test_data)
        self.assertEqual(result, "John Doe")
        
        # Check if user exists and has email
        result = jaf_eval.eval([
            "and",
            ["exists?", ["path", "user"]],
            ["exists?", ["path", "user.email"]]
        ], self.test_data)
        self.assertTrue(result)

    def test_invalid_path_string_syntax(self):
        """Test error handling for invalid path string syntax"""
        invalid_paths = [
            "user[",         # Unterminated bracket
            "user..",        # Double dot
            "user[]",        # Empty brackets
            "user[abc]",     # Invalid index
            "user[1:2:3:4]", # Too many colons
            "~/unterminated", # Unterminated regex
        ]
        
        for invalid_path in invalid_paths:
            with self.subTest(path=invalid_path):
                with self.assertRaises(PathSyntaxError):
                    jaf_eval.eval(["path", invalid_path], self.test_data)

    def test_path_argument_validation(self):
        """Test validation of path arguments"""
        # Test wrong number of arguments
        with self.assertRaisesRegex(ValueError, "'path' operator expects exactly one argument"):
            jaf_eval.eval(["path"], self.test_data)
        
        
    def test_path_argument_validation_2(self):
        with self.assertRaisesRegex(ValueError, "'path' operator expects exactly one argument"):
            jaf_eval.eval(["path", "user.name", "extra"], self.test_data)
        
    def test_path_argument_validation_3(self):
        # Test invalid path argument types
        with self.assertRaisesRegex(ValueError, "path argument must be a list of path components"):
            jaf_eval.eval(["path", 123], self.test_data)
    
    def test_path_component_validation(self):
        """Test validation of path components after string conversion"""
        # This would be caught after string_to_path_ast conversion
        # if we had an invalid operation in the converted AST
        
        # Test manually constructed invalid AST to verify validation
        with self.assertRaisesRegex(ValueError, "Unknown path operation"):
            jaf_eval.eval(["path", [["invalid_op", "test"]]], self.test_data)
        
        with self.assertRaisesRegex(ValueError, "Path component must be a list"):
            jaf_eval.eval(["path", ["not_a_list"]], self.test_data)
        
        with self.assertRaisesRegex(ValueError, "Path component cannot be empty"):
            jaf_eval.eval(["path", [[]]], self.test_data)

    def test_empty_results_from_path_strings(self):
        """Test handling of paths that return no results"""
        # Non-existent key
        result = jaf_eval.eval(["path", "user.nonexistent"], self.test_data)
        self.assertEqual(result, [])
        
        # Out of bounds index
        result = jaf_eval.eval(["path", "items[10]"], self.test_data)
        self.assertEqual(result, [])
        
        # Slice with no matching elements
        result = jaf_eval.eval(["path", "items[10:20]"], self.test_data)
        self.assertEqual(result, [])

    def test_path_strings_with_special_characters(self):
        """Test path strings that include keys with special characters"""
        special_data = {
            "api_version": "1.0",
            "data-field": "test",
            "_private": "secret"
        }
        
        # These should work as regular keys
        result = jaf_eval.eval(["path", "api_version"], special_data)
        self.assertEqual(result, "1.0")
        
        # Keys with hyphens or underscores
        result = jaf_eval.eval(["path", "_private"], special_data)
        self.assertEqual(result, "secret")


if __name__ == '__main__':
    unittest.main()
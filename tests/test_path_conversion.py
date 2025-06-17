import unittest
from jaf.path_conversion import path_ast_to_string, string_to_path_ast
from jaf.path_exceptions import PathSyntaxError

class TestPathAstToString(unittest.TestCase):

    def test_empty_path(self):
        self.assertEqual(path_ast_to_string([]), "")

    def test_single_key(self):
        self.assertEqual(path_ast_to_string([["key", "name"]]), "name")

    def test_multiple_keys(self):
        self.assertEqual(path_ast_to_string([["key", "user"], ["key", "address"], ["key", "street"]]), "user.address.street")

    def test_key_and_index(self):
        self.assertEqual(path_ast_to_string([["key", "items"], ["index", 0]]), "items[0]")
        self.assertEqual(path_ast_to_string([["key", "items"], ["index", -1]]), "items[-1]")

    def test_key_and_indices(self):
        self.assertEqual(path_ast_to_string([["key", "tags"], ["indices", [0, 2, 5]]]), "tags[0,2,5]")
        self.assertEqual(path_ast_to_string([["key", "tags"], ["indices", [1]]]), "tags[1]")

    def test_key_and_slice(self):
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 1, 10, 2]]), "data[1:10:2]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, 5, 1]]), "data[:5]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 2, None, 1]]), "data[2:]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 1, 5, None]]), "data[1:5]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, None, 2]]), "data[::2]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, 5, None]]), "data[:5]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 1, None, None]]), "data[1:]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, None, None]]), "data[:]")
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, None, 1]]), "data[:]")


    def test_key_and_regex_key(self):
        self.assertEqual(path_ast_to_string([["key", "headers"], ["regex_key", "^X-"]]), "headers.~/^X-/")

    def test_key_and_level_wildcard(self):
        self.assertEqual(path_ast_to_string([["key", "items"], ["wc_level"]]), "items[*]") # Corrected: removed dot

    def test_key_and_recursive_wildcard(self):
        self.assertEqual(path_ast_to_string([["key", "data"], ["wc_recursive"]]), "data.**")

    def test_recursive_wildcard_at_start(self):
        self.assertEqual(path_ast_to_string([["wc_recursive"], ["key", "id"]]), "**.id")

    def test_level_wildcard_at_start(self):
        self.assertEqual(path_ast_to_string([["wc_level"], ["key", "name"]]), "[*].name")

    def test_root_operator_string(self):
        self.assertEqual(path_ast_to_string([["root"]]), "#")
        self.assertEqual(path_ast_to_string([["root"], ["key", "config"]]), "#.config")
        self.assertEqual(path_ast_to_string([["key", "user"], ["root"], ["key", "config"]]), "user.#.config")
        self.assertEqual(path_ast_to_string([["key", "user"], ["index", 0], ["root"], ["key", "config"]]), "user[0].#.config")
        self.assertEqual(path_ast_to_string([["key", "user"], ["slice", 0, 1, None], ["root"], ["key", "config"]]), "user[0:1].#.config")


    def test_complex_path(self):
        ast = [
            ["key", "users"], 
            ["index", 0], 
            ["key", "profile"], 
            ["wc_level"], 
            ["key", "detail"],
            ["slice", 1, None, None],
            ["root"],
            ["key", "metadata"],
            ["regex_key", "version_\\d+"]
        ]
        # Corrected: removed dot before [*]
        self.assertEqual(path_ast_to_string(ast), "users[0].profile[*].detail[1:].#.metadata.~/version_\\d+/")

    def test_regex_key_at_start(self):
        self.assertEqual(path_ast_to_string([["regex_key", "error_\\d+"]]), "~/error_\\d+/")

    def test_indices_at_start(self):
        self.assertEqual(path_ast_to_string([["indices", [0,1]]]), "[0,1]")
    
    def test_slice_at_start(self):
        self.assertEqual(path_ast_to_string([["slice", 0,1,None]]), "[0:1]")

    def test_invalid_component_format(self):
        # Test message now matches implementation
        with self.assertRaisesRegex(PathSyntaxError, "Invalid AST component format: expected a non-empty list."):
            path_ast_to_string([["key", "user"], "not-a-list"]) # type: ignore
        with self.assertRaisesRegex(PathSyntaxError, "Invalid AST component format: expected a non-empty list."):
            path_ast_to_string([["key", "user"], []])


    def test_invalid_regex_key(self):
        # Test message now matches implementation
        with self.assertRaisesRegex(PathSyntaxError, "'regex_key' operation expects a string pattern argument."):
            path_ast_to_string([["regex_key", 123]]) # type: ignore

    def test_invalid_operation_arguments(self):
        # Test messages now match implementation
        with self.assertRaisesRegex(PathSyntaxError, "'key' operation expects a single string argument."):
            path_ast_to_string([["key", 123]]) # type: ignore
        with self.assertRaisesRegex(PathSyntaxError, "'index' operation expects a single integer argument."):
            path_ast_to_string([["index", "abc"]]) # type: ignore
        with self.assertRaisesRegex(PathSyntaxError, "'indices' operation expects a single list of integers argument."):
            path_ast_to_string([["indices", [1, "b"]]]) # type: ignore
        with self.assertRaisesRegex(PathSyntaxError, "'slice' operation expects 1 to 3 integer or None arguments"):
            path_ast_to_string([["slice", "a", "b", "c"]]) # type: ignore
        with self.assertRaisesRegex(PathSyntaxError, "Slice step cannot be zero."):
            path_ast_to_string([["slice", None, None, 0]])
        with self.assertRaisesRegex(PathSyntaxError, "'wc_level' operation expects no arguments."):
            path_ast_to_string([["wc_level", "arg"]])
        with self.assertRaisesRegex(PathSyntaxError, "'wc_recursive' operation expects no arguments."):
            path_ast_to_string([["wc_recursive", "arg"]])
        with self.assertRaisesRegex(PathSyntaxError, "'root' operation expects no arguments."):
            path_ast_to_string([["root", "arg"]])
        with self.assertRaisesRegex(PathSyntaxError, "Unknown JAF path component operation: 'unknown_op'"):
            path_ast_to_string([["unknown_op", "arg"]])


class TestStringToPathAst(unittest.TestCase):

    def test_empty_string(self):
        self.assertEqual(string_to_path_ast(""), [])
        self.assertEqual(string_to_path_ast("   "), []) # With whitespace

    def test_single_key(self):
        self.assertEqual(string_to_path_ast("name"), [["key", "name"]])

    def test_multiple_keys(self):
        self.assertEqual(string_to_path_ast("user.name"), [["key", "user"], ["key", "name"]])
        self.assertEqual(string_to_path_ast("a.b.c"), [["key", "a"], ["key", "b"], ["key", "c"]])

    def test_index(self):
        self.assertEqual(string_to_path_ast("[0]"), [["index", 0]])
        self.assertEqual(string_to_path_ast("items[0]"), [["key", "items"], ["index", 0]])
        self.assertEqual(string_to_path_ast("items[-1].value"), [["key", "items"], ["index", -1], ["key", "value"]])
        self.assertEqual(string_to_path_ast("items[  10  ]"), [["key", "items"], ["index", 10]]) # Whitespace

    def test_indices(self):
        self.assertEqual(string_to_path_ast("[0,1,2]"), [["indices", [0, 1, 2]]])
        self.assertEqual(string_to_path_ast("items[0,1,-1]"), [["key", "items"], ["indices", [0, 1, -1]]])
        self.assertEqual(string_to_path_ast("items[ 0 , 1 , 2 ]"), [["key", "items"], ["indices", [0, 1, 2]]]) # Whitespace

    def test_slice_1(self):
        self.assertEqual(string_to_path_ast("[:]"), [["slice", 0, None]]) # Step defaults to None in parser
    def test_slice_2(self):
        self.assertEqual(string_to_path_ast("[1:]"), [["slice", 1, None]])
    def test_slice_3(self):
        self.assertEqual(string_to_path_ast("[:5]"), [["slice", 0, 5]])
    def test_slice_4(self):
        self.assertEqual(string_to_path_ast("[1:5]"), [["slice", 1, 5]])
    def test_slice_5(self):
        self.assertEqual(string_to_path_ast("[1:10:2]"), [["slice", 1, 10, 2]])
    def test_slice_6(self):
        self.assertEqual(string_to_path_ast("[::2]"), [["slice", 0, None, 2]])
    def test_slice_7(self):
        self.assertEqual(string_to_path_ast("[10:1:-1]"), [["slice", 10, 1, -1]])
    def test_slice_8(self):
        self.assertEqual(string_to_path_ast("items[1:5:2]"), [["key", "items"], ["slice", 1, 5, 2]])
    def test_slice_9(self):
        self.assertEqual(string_to_path_ast("items[ : ]"), [["key", "items"], ["slice", 0,  None]]) # Whitespace
    def test_slice_10(self):
        self.assertEqual(string_to_path_ast("items[1 : 5 : 2]"), [["key", "items"], ["slice", 1, 5, 2]]) # Whitespace

    def test_wc_level(self):
        self.assertEqual(string_to_path_ast("[*]"), [["wc_level"]])
        self.assertEqual(string_to_path_ast("items[*]"), [["key", "items"], ["wc_level"]])
        self.assertEqual(string_to_path_ast("items[*].name"), [["key", "items"], ["wc_level"], ["key", "name"]])

    def test_wc_recursive(self):
        self.assertEqual(string_to_path_ast("**"), [["wc_recursive"]])
        self.assertEqual(string_to_path_ast("**.name"), [["wc_recursive"], ["key", "name"]])
        self.assertEqual(string_to_path_ast("data.**"), [["key", "data"], ["wc_recursive"]]) # At end
        self.assertEqual(string_to_path_ast("data.**.name"), [["key", "data"], ["wc_recursive"], ["key", "name"]])


    def test_regex_key(self):
        self.assertEqual(string_to_path_ast("~/^item_\\d+$/"), [["regex_key", "^item_\\d+$"]])
        self.assertEqual(string_to_path_ast("data.~/pattern/"), [["key", "data"], ["regex_key", "pattern"]])
        self.assertEqual(string_to_path_ast("~/^a.b$/[0]"), [["regex_key", "^a.b$"], ["index", 0]])

    def test_root(self):
        self.assertEqual(string_to_path_ast("#"), [["root"]])
        self.assertEqual(string_to_path_ast("#.config"), [["root"], ["key", "config"]])
        self.assertEqual(string_to_path_ast("user.#.name"), [["key", "user"], ["root"], ["key", "name"]])

    def test_dot_handling(self):
        self.assertEqual(string_to_path_ast("key1.key2"), [["key", "key1"], ["key", "key2"]])
        self.assertEqual(string_to_path_ast("key1[0]"), [["key", "key1"], ["index", 0]])
        # The current parser's eat_dot() will consume a dot if present after any component.
        self.assertEqual(string_to_path_ast("key1.[0]"), [["key", "key1"], ["index", 0]])
        self.assertEqual(string_to_path_ast("key1.[*]"), [["key", "key1"], ["wc_level"]])
        self.assertEqual(string_to_path_ast("key1.~/regex/"), [["key", "key1"], ["regex_key", "regex"]])


    def test_complex_paths_1(self):
        self.assertEqual(string_to_path_ast("user.tags[*].name"),
                         [["key", "user"], ["key", "tags"], ["wc_level"], ["key", "name"]])
    def test_complex_paths_2(self):
        self.assertEqual(string_to_path_ast("**.items[0,1].value"),
                         [["wc_recursive"], ["key", "items"], ["indices", [0,1]], ["key", "value"]])
    def test_complex_paths_3(self):
        self.assertEqual(string_to_path_ast("data[1:5:2].*.id"), # Assuming `.*` is now parsed as wc_level
                         [["key", "data"], ["slice", 1, 5, 2], ["wc_level"], ["key", "id"]]) # Changed to wc_level
    def test_complex_paths_4(self):
        # To test `data[1:5:2].[*].id`
        self.assertEqual(string_to_path_ast("data[1:5:2].[*].id"),
                         [["key", "data"], ["slice", 1, 5, 2], ["wc_level"], ["key", "id"]])
    def test_complex_paths_5(self):
        self.assertEqual(string_to_path_ast("#.data[0].~/^field\\w+$/"),
                         [["root"],["key", "data"], ["index", 0], ["regex_key", "^field\\w+$"]])

    def test_invalid_syntax_1(self):
        with self.assertRaisesRegex(PathSyntaxError, "Unterminated '\\['"):
            string_to_path_ast("items[0")
    def test_invalid_syntax_2(self):
        with self.assertRaisesRegex(PathSyntaxError, "Unterminated '\\['"):
            string_to_path_ast("items[0,1") # Will be caught by find(']')
    def test_invalid_syntax_3(self):
        with self.assertRaisesRegex(PathSyntaxError, "Malformed slice"): # Or specific int parsing error
            string_to_path_ast("items[abc:]")
    def test_invalid_syntax_4(self):
        with self.assertRaisesRegex(PathSyntaxError, "Expected integer"):
            string_to_path_ast("items[abc]")
    def test_invalid_syntax_5(self):
        with self.assertRaisesRegex(PathSyntaxError, "Expected integer"):
            string_to_path_ast("items[0,abc,1]")
    def test_invalid_syntax_6(self):
        with self.assertRaisesRegex(PathSyntaxError, "Unterminated regex key"):
            string_to_path_ast("~/pattern")
    def test_invalid_syntax_7(self):
        with self.assertRaisesRegex(PathSyntaxError, "Unexpected token"):
            string_to_path_ast("item!")
    def test_invalid_syntax_8(self):
        with self.assertRaisesRegex(PathSyntaxError, "Unexpected token"): # Dot at start
            string_to_path_ast(".item")

    def test_slice_parsing_edge_cases_1(self):
        x=string_to_path_ast("data[1:]")
        self.assertEqual(x, [["key", "data"], ["slice", 1, None]])
    def test_slice_parsing_edge_cases_2(self):
        self.assertEqual(string_to_path_ast("data[:5:]"), [["key", "data"], ["slice", 0, 5]]) # step is None
    def test_slice_parsing_edge_cases_3(self):
        self.assertEqual(string_to_path_ast("data[::]"), [["key", "data"], ["slice", 0, None]]) # step is None
    def test_slice_parsing_edge_cases_4(self):
        # Test for slice step zero - current string_to_path_ast does not validate this, path_ast_to_string does.
        # The parser should accept it, validation can be a separate step if needed.
        self.assertEqual(string_to_path_ast("[::0]"), [["slice", 0, None, 0]])

if __name__ == '__main__':
    unittest.main()


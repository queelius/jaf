import pytest

class TestPathStringToAst(unittest.TestCase):

    def test_empty_string(self):
        self.assertEqual(path_string_to_ast(""), [])

    def test_single_key(self):
        self.assertEqual(path_string_to_ast("name"), [["key", "name"]])

    def test_multiple_keys(self):
        self.assertEqual(path_string_to_ast("user.address.street"), [["key", "user"], ["key", "address"], ["key", "street"]])

    def test_key_with_hyphen_and_underscore(self):
        self.assertEqual(path_string_to_ast("user-profile.address_street"), [["key", "user-profile"], ["key", "address_street"]])

    def test_key_and_index(self):
        self.assertEqual(path_string_to_ast("items[0]"), [["key", "items"], ["index", 0]])
        self.assertEqual(path_string_to_ast("items[-1]"), [["key", "items"], ["index", -1]])

    def test_key_and_indices(self):
        self.assertEqual(path_string_to_ast("tags[0,2,5]"), [["key", "tags"], ["indices", [0, 2, 5]]])
        self.assertEqual(path_string_to_ast("tags[1]"), [["key", "tags"], ["index", 1]]) # Single index in brackets is just an index

    def test_key_and_slice(self):
        self.assertEqual(path_string_to_ast("data[1:10:2]"), [["key", "data"], ["slice", 1, 10, 2]])
        self.assertEqual(path_string_to_ast("data[:5]"), [["key", "data"], ["slice", None, 5, 1]])
        self.assertEqual(path_string_to_ast("data[2:]"), [["key", "data"], ["slice", 2, None, 1]])
        self.assertEqual(path_string_to_ast("data[1:5]"), [["key", "data"], ["slice", 1, 5, 1]])
        self.assertEqual(path_string_to_ast("data[::2]"), [["key", "data"], ["slice", None, None, 2]])
        self.assertEqual(path_string_to_ast("data[:]"), [["key", "data"], ["slice", None, None, 1]])

    def test_key_and_regex_key(self):
        self.assertEqual(path_string_to_ast("headers.~/^X-/"), [["key", "headers"], ["regex_key", "^X-"]])

    def test_key_and_level_wildcard(self):
        self.assertEqual(path_string_to_ast("items.[*]"), [["key", "items"], ["wc_level"]])
        self.assertEqual(path_string_to_ast("items.*"), [["key", "items"], ["wc_level"]])


    def test_key_and_recursive_wildcard(self):
        self.assertEqual(path_string_to_ast("data.**"), [["key", "data"], ["wc_recursive"]])

    def test_recursive_wildcard_at_start(self):
        self.assertEqual(path_string_to_ast("**.id"), [["wc_recursive"], ["key", "id"]])

    def test_level_wildcard_at_start(self):
        self.assertEqual(path_string_to_ast("[*].name"), [["wc_level"], ["key", "name"]])
        self.assertEqual(path_string_to_ast("*.name"), [["wc_level"], ["key", "name"]])


    def test_complex_path(self):
        ast = [
            ["key", "users"],
            ["index", 0],
            ["key", "profile"],
            ["wc_level"],
            ["key", "detail"],
            ["slice", 1, None, 1]
        ]
        self.assertEqual(path_string_to_ast("users[0].profile.[*].detail[1:]"), ast)
        self.assertEqual(path_string_to_ast("users[0].profile.*.detail[1:]"), ast)


    def test_regex_key_at_start(self):
        self.assertEqual(path_string_to_ast("~/error_\\\\d+/"), [["regex_key", "error_\\\\d+"]])

    def test_indices_at_start(self):
        self.assertEqual(path_string_to_ast("[0,1]"), [["indices", [0,1]]])
        self.assertEqual(path_string_to_ast("[-2,5]"), [["indices", [-2,5]]])

    def test_slice_at_start(self):
        self.assertEqual(path_string_to_ast("[0:1]"), [["slice", 0,1,1]])
        self.assertEqual(path_string_to_ast("[:10:2]"), [["slice", None,10,2]])
        self.assertEqual(path_string_to_ast("[::-1]"), [["slice", None,None,-1]])

    def test_invalid_syntax(self):
        invalid_paths = [
            "name.", ".name", "name[", "name]", "name[0", "name[0,", "name[0,]", "name[1:2:", "name[1::]",
            "~/", "data.~/", "data.~/regex", "data.~/regex/", "data.~/^/", # Valid regex but empty
            "data.**.", "**.**", "*.*", "[*].[*]",
            "key..key", "key[0]key", "key[0][1]", "key.[*]key" 
        ]
        for path_str in invalid_paths:
            with self.subTest(path=path_str):
                with self.assertRaises(PathSyntaxError):
                    path_string_to_ast(path_str)
    
    def test_dot_before_bracket(self):
        # foo.[0] is valid and means foo[0]
        self.assertEqual(path_string_to_ast("foo.[0]"), [["key", "foo"], ["index", 0]])
        # foo.[0,1]
        self.assertEqual(path_string_to_ast("foo.[0,1]"), [["key", "foo"], ["indices", [0,1]]])
        # foo.[:1]
        self.assertEqual(path_string_to_ast("foo.[:1]"), [["key", "foo"], ["slice", None, 1, 1]])

    def test_whitespace_handling(self):
        self.assertEqual(path_string_to_ast(" user . name "), [["key", "user"], ["key", "name"]])
        self.assertEqual(path_string_to_ast(" items[ 0 ] "), [["key", "items"], ["index", 0]])
        self.assertEqual(path_string_to_ast(" items[ 0 , 1 ] "), [["key", "items"], ["indices", [0,1]]])
        self.assertEqual(path_string_to_ast(" data[ 1 : 5 : 2 ] "), [["key", "data"], ["slice", 1,5,2]])
        self.assertEqual(path_string_to_ast(" data . [*] "), [["key", "data"], ["wc_level"]])
        self.assertEqual(path_string_to_ast(" data . ** "), [["key", "data"], ["wc_recursive"]])
        self.assertEqual(path_string_to_ast(" ~/^X- / "), [["regex_key", "^X- "]]) # Regex content itself is not stripped

    def test_path_string_to_ast_roundtrip(self):
        test_asts = [
            [],
            [["key", "name"]],
            [["key", "user"], ["key", "address"], ["key", "street"]],
            [["key", "items"], ["index", 0]],
            [["key", "tags"], ["indices", [0, 2, 5]]],
            [["key", "data"], ["slice", 1, 10, 2]],
            [["key", "data"], ["slice", None, 5, 1]],
            [["key", "data"], ["slice", 2, None, 1]],
            [["key", "data"], ["slice", None, None, 1]], # data[:]
            [["key", "headers"], ["regex_key", "^X-"]],
            [["key", "items"], ["wc_level"]],
            [["key", "data"], ["wc_recursive"]],
            [["wc_recursive"], ["key", "id"]],
            [["wc_level"], ["key", "name"]],
            [["key", "users"], ["index", 0], ["key", "profile"], ["wc_level"], ["key", "detail"], ["slice", 1, None, 1]],
            [["regex_key", "error_\\\\d+"]],
            [["indices", [0,1]]],
            [["slice", 0,1,1]]
        ]
        for ast in test_asts:
            with self.subTest(ast=ast):
                string_repr = path_ast_to_string(ast)
                # print(f"AST: {ast} -> String: '{string_repr}'")
                reconstructed_ast = path_string_to_ast(string_repr)
                # print(f"String: '{string_repr}' -> AST: {reconstructed_ast}")
                self.assertEqual(reconstructed_ast, ast)
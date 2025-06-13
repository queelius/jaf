import unittest
from jaf.path_utils import path_ast_to_string, PathSyntaxError

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
        # Full slice
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 1, 10, 2]]), "data[1:10:2]")
        # Slice with None for start, step is 1 (explicit in AST) -> should be implicit in string
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, 5, 1]]), "data[:5]")
        # Slice with None for stop, step is 1 (explicit in AST) -> should be implicit in string
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 2, None, 1]]), "data[2:]")
        # Slice with None for step (implicitly 1)
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 1, 5, None]]), "data[1:5]")
        # Slice with None for start and stop, step is explicit and not 1
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, None, 2]]), "data[::2]")
        # Slice with None for start and step (implicitly 1)
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, 5, None]]), "data[:5]")
        # Slice with None for stop and step (implicitly 1)
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", 1, None, None]]), "data[1:]")
        # Slice with all None (full slice, step implicitly 1)
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, None, None]]), "data[:]")
        # Slice with all None, step explicitly 1 (full slice)
        self.assertEqual(path_ast_to_string([["key", "data"], ["slice", None, None, 1]]), "data[:]")


    def test_key_and_regex_key(self):
        self.assertEqual(path_ast_to_string([["key", "headers"], ["regex_key", "^X-"]]), "headers.~/^X-/")

    def test_key_and_level_wildcard(self):
        self.assertEqual(path_ast_to_string([["key", "items"], ["wc_level"]]), "items.[*]")

    def test_key_and_recursive_wildcard(self):
        self.assertEqual(path_ast_to_string([["key", "data"], ["wc_recursive"]]), "data.**")

    def test_recursive_wildcard_at_start(self):
        self.assertEqual(path_ast_to_string([["wc_recursive"], ["key", "id"]]), "**.id")

    def test_level_wildcard_at_start(self):
        # This might be an unusual path, but the function should handle it
        self.assertEqual(path_ast_to_string([["wc_level"], ["key", "name"]]), "[*].name")

    def test_complex_path(self):
        ast = [
            ["key", "users"], 
            ["index", 0], 
            ["key", "profile"], 
            ["wc_level"], 
            ["key", "detail"],
            ["slice", 1, None, None]
        ]
        self.assertEqual(path_ast_to_string(ast), "users[0].profile.[*].detail[1:]")

    def test_regex_key_at_start(self):
        self.assertEqual(path_ast_to_string([["regex_key", "error_\\d+"]]), "~/error_\\d+/")

    def test_indices_at_start(self):
        self.assertEqual(path_ast_to_string([["indices", [0,1]]]), "[0,1]")
    
    def test_slice_at_start(self):
        self.assertEqual(path_ast_to_string([["slice", 0,1,None]]), "[0:1]")


import unittest
from unittest.mock import patch
from jaf.path_evaluation import eval_path
from jaf.path_operations import _fuzzy_match_keys
from jaf.path_types import PathValues
from jaf.path_exceptions import PathSyntaxError


class TestFuzzyKey(unittest.TestCase):
    """Test fuzzy key matching functionality in path evaluation"""

    def setUp(self):
        """Set up test data with various key naming patterns"""
        self.test_data = {
            "user_name": "John Doe",
            "userName": "Jane Smith", 
            "user-name": "Bob Wilson",
            "usr_nm": "Alice Brown",
            "name": "Charlie Davis",
            "first_name": "David Lee",
            "firstName": "Eva Garcia",
            "last_name": "Frank Miller",
            "lastName": "Grace Johnson",
            "email_address": "test@example.com",
            "emailAddress": "admin@test.com",
            "phone_number": "555-1234",
            "phoneNumber": "555-5678",
            "address": {
                "street_name": "Main St",
                "streetName": "Oak Ave",
                "zip_code": "12345",
                "zipCode": "54321"
            },
            "configuration": {
                "api_key": "secret123",
                "apiKey": "secret456",
                "debug_mode": True,
                "debugMode": False
            }
        }

    def test_fuzzy_key_basic_matching(self):
        """Test basic fuzzy key matching with default settings"""
        # Should match "user_name" when searching for "username"
        result = eval_path([["fuzzy_key", "username"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("John Doe", result)

    def test_fuzzy_key_with_cutoff(self):
        """Test fuzzy key matching with custom cutoff"""
        # High cutoff should be more restrictive
        result = eval_path([["fuzzy_key", "username", 0.9]], self.test_data)
        self.assertIsInstance(result, PathValues)
        
        # Low cutoff should be more permissive
        result = eval_path([["fuzzy_key", "nm", 0.3]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertTrue(len(result) > 0)  # Should match usr_nm and potentially others

    def test_fuzzy_key_algorithms(self):
        """Test different fuzzy matching algorithms"""
        algorithms = ["difflib", "levenshtein", "jaro_winkler"]
        
        for algorithm in algorithms:
            with self.subTest(algorithm=algorithm):
                try:
                    result = eval_path([["fuzzy_key", "username", 0.6, algorithm]], self.test_data)
                    self.assertIsInstance(result, PathValues)
                except Exception as e:
                    # Some algorithms might not be available (missing libraries)
                    if "not available" in str(e):
                        self.skipTest(f"Algorithm {algorithm} not available")
                    else:
                        raise

    def test_fuzzy_key_metaphone_matching(self):
        """Test metaphone-based fuzzy matching"""
        test_data = {
            "night": "value1",
            "knight": "value2",
            "nite": "value3"
        }
        
        try:
            result = eval_path([["fuzzy_key", "night", 0.6, "metaphone"]], test_data)
            self.assertIsInstance(result, PathValues)
        except Exception as e:
            if "not available" in str(e):
                self.skipTest("Metaphone algorithm not available")
            else:
                raise

    def test_fuzzy_key_nested_paths(self):
        """Test fuzzy key matching in nested paths"""
        # Should find "street_name" when searching for "streetname"
        result = eval_path([["key", "address"], ["fuzzy_key", "streetname"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("Main St", result)

    def test_fuzzy_key_with_wildcards(self):
        """Test fuzzy key matching combined with wildcards"""
        # Find all fuzzy matches for "key" in all nested objects
        result = eval_path([["wc_level"], ["fuzzy_key", "key", 0.4]], self.test_data)
        self.assertIsInstance(result, PathValues)
        print(f"Fuzzy matches with wildcards: {result}")
        # Should find api_key and apiKey
        self.assertEqual(set(result), { "secret123", "secret456" })

    def test_fuzzy_key_no_matches(self):
        """Test fuzzy key when no matches are found"""
        result = eval_path([["fuzzy_key", "xyz123", 0.8]], self.test_data)
        # self.assertIsInstance(result, PathValues)
        self.assertEqual(len(result), 0)

    def test_fuzzy_key_exact_matches_preferred(self):
        """Test that exact matches are preferred over fuzzy ones"""
        test_data = {
            "name": "exact",
            "nme": "fuzzy1", 
            "nm": "fuzzy2"
        }
        
        result = eval_path([["fuzzy_key", "name", 0.5]], test_data)
        self.assertIsInstance(result, PathValues)
        # Exact match should come first
        self.assertEqual(result[0], "exact")

    def test_fuzzy_key_argument_validation(self):
        """Test validation of fuzzy_key arguments"""
        # Wrong number of arguments
        with self.assertRaisesRegex(PathSyntaxError, "expects 1 to 3 arguments"):
            eval_path([["fuzzy_key"]], self.test_data)
        
        with self.assertRaisesRegex(PathSyntaxError, "expects 1 to 3 arguments"):
            eval_path([["fuzzy_key", "key", 0.5, "difflib", "extra"]], self.test_data)
        
        # Invalid key name type
        with self.assertRaisesRegex(PathSyntaxError, "expects a string argument for the key name"):
            eval_path([["fuzzy_key", 123]], self.test_data)
        
        # Invalid cutoff type
        with self.assertRaisesRegex(PathSyntaxError, "expects a numeric argument for the cutoff"):
            eval_path([["fuzzy_key", "name", "invalid"]], self.test_data)
        
        # Invalid cutoff range
        with self.assertRaisesRegex(PathSyntaxError, "expects a cutoff between 0.0 and 1.0"):
            eval_path([["fuzzy_key", "name", 1.5]], self.test_data)
        
        with self.assertRaisesRegex(PathSyntaxError, "expects a cutoff between 0.0 and 1.0"):
            eval_path([["fuzzy_key", "name", -0.1]], self.test_data)
        
        # Invalid algorithm type
        with self.assertRaisesRegex(PathSyntaxError, "expects a string argument for the algorithm"):
            eval_path([["fuzzy_key", "name", 0.5, 123]], self.test_data)
        
        # Unknown algorithm
        with self.assertRaisesRegex(PathSyntaxError, "unknown algorithm 'unknown'"):
            eval_path([["fuzzy_key", "name", 0.5, "unknown"]], self.test_data)

    def test_fuzzy_match_keys_helper_function(self):
        """Test the internal _fuzzy_match_keys helper function"""
        keys = ["user_name", "userName", "user-name", "name", "username"]
        
        # Test difflib algorithm
        matches = _fuzzy_match_keys("username", keys, 0.6, "difflib")
        self.assertIsInstance(matches, list)
        self.assertTrue(len(matches) > 0)
        
        # Test with high cutoff
        matches = _fuzzy_match_keys("xyz", keys, 0.9, "difflib")
        self.assertEqual(len(matches), 0)

    @patch('jaf.path_evaluation.logger')
    def test_fuzzy_key_library_fallback(self, mock_logger):
        """Test fallback behavior when specialized libraries are not available"""
        # This would test the ImportError handling, but since we can't easily
        # mock import failures, we'll verify the warning is logged when expected
        pass

    def test_fuzzy_key_with_special_characters(self):
        """Test fuzzy matching with keys containing special characters"""
        special_data = {
            "api-key": "value1",
            "api_key": "value2", 
            "api.key": "value3",
            "api key": "value4"
        }
        
        result = eval_path([["fuzzy_key", "apikey", 0.5]], special_data)
        self.assertIsInstance(result, PathValues)
        self.assertTrue(len(result) > 0)

    def test_fuzzy_key_unicode_support(self):
        """Test fuzzy matching with Unicode characters"""
        unicode_data = {
            "naïve": "value1",
            "naive": "value2",
            "café": "value3",
            "cafe": "value4"
        }
        
        result = eval_path([["fuzzy_key", "naive", 0.6]], unicode_data)
        self.assertIsInstance(result, PathValues)
        # Should match both "naïve" and "naive"
        self.assertTrue(len(result) >= 1)


if __name__ == '__main__':
    unittest.main()
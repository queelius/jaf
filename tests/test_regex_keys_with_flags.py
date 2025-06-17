import unittest
import re
from jaf.path_evaluation import eval_path
from jaf.path_types import PathValues
from jaf.path_exceptions import PathSyntaxError


class TestRegexKeyWithFlags(unittest.TestCase):
    """Test regex key matching with flag support"""

    def setUp(self):
        """Set up test data with various key patterns"""
        self.test_data = {
            "UserName": "John Doe",
            "username": "jane_smith", 
            "EMAIL": "test@example.com",
            "email_address": "admin@test.com",
            "Phone": "555-1234",
            "phone_number": "555-5678",
            "API_KEY": "secret123",
            "api_key": "secret456",
            "debug_MODE": True,
            "debug_mode": False,
            "config": {
                "Database_URL": "postgres://localhost",
                "database_url": "mysql://localhost",
                "Cache_TTL": 3600,
                "cache_ttl": 1800
            },
            "metrics": {
                "response_time_ms": 150,
                "Response_Time_MS": 200,
                "error_count": 5,
                "ERROR_COUNT": 10
            }
        }

    def test_regex_key_case_insensitive(self):
        """Test case-insensitive regex matching"""
        # Match any variation of "username" case-insensitively
        result = eval_path([["regex_key", "username", "i"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("John Doe", result)  # UserName
        self.assertIn("jane_smith", result)  # username

    def test_regex_key_multiple_flags(self):
        """Test multiple regex flags"""
        # Case-insensitive and multiline
        result = eval_path([["regex_key", "^user", "im"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertTrue(len(result) >= 2)  # Should match UserName and username

    def test_regex_key_integer_flags(self):
        """Test using integer flag constants"""
        # Using re.IGNORECASE as integer
        result = eval_path([["regex_key", "email", re.IGNORECASE]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("test@example.com", result)  # EMAIL
        self.assertIn("admin@test.com", result)   # email_address

    def test_regex_key_combined_integer_flags(self):
        """Test combining multiple integer flags"""
        # Case-insensitive + multiline
        flags = re.IGNORECASE | re.MULTILINE
        result = eval_path([["regex_key", "api", flags]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("secret123", result)  # API_KEY
        self.assertIn("secret456", result)  # api_key

    def test_regex_key_nested_with_flags(self):
        """Test regex matching in nested objects with flags"""
        result = eval_path([["key", "config"], ["regex_key", "database", "i"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("postgres://localhost", result)  # Database_URL
        self.assertIn("mysql://localhost", result)     # database_url

    def test_regex_key_complex_patterns_with_flags(self):
        """Test complex regex patterns with flags"""
        # Match keys ending with "_ms" or "_MS" case-insensitively
        result = eval_path([["key", "metrics"], ["regex_key", ".*_ms$", "i"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn(150, result)  # response_time_ms
        self.assertIn(200, result)  # Response_Time_MS

    def test_regex_key_word_boundaries_with_flags(self):
        """Test word boundary patterns with flags"""
        # Match keys containing "count" case-insensitively (not requiring word boundaries)
        result = eval_path([["key", "metrics"], ["regex_key", ".*count", "i"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn(5, result)   # error_count
        self.assertIn(10, result)  # ERROR_COUNT

    def test_regex_key_dotall_flag(self):
        """Test dotall flag (though less common for key matching)"""
        multiline_data = {
            "key\nwith\nnewlines": "value1",
            "normal_key": "value2"
        }
        
        # Without dotall, . doesn't match newlines
        result = eval_path([["regex_key", "key.*newlines"]], multiline_data)
        self.assertEqual(len(result), 0)
        
        # With dotall, . matches newlines
        result = eval_path([["regex_key", "key.*newlines", "s"]], multiline_data)
        self.assertIn("value1", result)

    def test_regex_key_verbose_flag(self):
        """Test verbose flag for readable patterns"""
        # Complex pattern with verbose flag for readability
        verbose_pattern = r"""
            ^               # Start of string
            (api|API)       # Match "api" or "API"
            _?              # Optional underscore
            (key|KEY)       # Match "key" or "KEY"
            $               # End of string
        """
        
        result = eval_path([["regex_key", verbose_pattern, "x"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("secret123", result)  # API_KEY
        self.assertIn("secret456", result)  # api_key

    def test_regex_key_flag_validation(self):
        """Test validation of regex flags"""
        # Invalid flag character
        with self.assertRaisesRegex(PathSyntaxError, "unknown flag 'z'"):
            eval_path([["regex_key", "test", "z"]], self.test_data)
        
        # Invalid flag type
        with self.assertRaisesRegex(PathSyntaxError, "expects a string or integer argument for flags"):
            eval_path([["regex_key", "test", ["invalid"]]], self.test_data)

    def test_regex_key_invalid_pattern_with_flags(self):
        """Test error handling for invalid regex patterns"""
        # Invalid regex pattern
        with self.assertRaisesRegex(PathSyntaxError, "invalid regex pattern"):
            eval_path([["regex_key", "[invalid", "i"]], self.test_data)

    def test_regex_key_no_flags_backward_compatibility(self):
        """Test that regex_key works without flags (backward compatibility)"""
        result = eval_path([["regex_key", "^user"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("jane_smith", result)  # Should match "username"
        # Should NOT match "UserName" without case-insensitive flag

    def test_regex_key_empty_flags(self):
        """Test regex_key with empty flags string"""
        result = eval_path([["regex_key", "username", ""]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertIn("jane_smith", result)  # Should match "username"

    def test_regex_key_all_valid_flags(self):
        """Test all valid flag characters"""
        valid_flags = "imsxa"
        # This should not raise an error
        try:
            result = eval_path([["regex_key", "test", valid_flags]], self.test_data)
            self.assertIsInstance(result, PathValues)
        except PathSyntaxError:
            self.fail("Valid flags should not raise PathSyntaxError")

    def test_regex_key_argument_count_validation(self):
        """Test validation of argument count"""
        # Too few arguments
        with self.assertRaisesRegex(PathSyntaxError, "expects 1 or 2 arguments"):
            eval_path([["regex_key"]], self.test_data)
        
        # Too many arguments
        with self.assertRaisesRegex(PathSyntaxError, "expects 1 or 2 arguments"):
            eval_path([["regex_key", "pattern", "i", "extra"]], self.test_data)

    def test_regex_key_non_string_pattern(self):
        """Test validation of pattern argument type"""
        with self.assertRaisesRegex(PathSyntaxError, "expects a string argument for the pattern"):
            eval_path([["regex_key", 123, "i"]], self.test_data)

    def test_regex_key_with_wildcards_and_flags(self):
        """Test regex_key combined with wildcards and flags"""
        # Find all keys containing "time" case-insensitively in any nested object
        result = eval_path([["wc_level"], ["regex_key", "time", "i"]], self.test_data)
        self.assertIsInstance(result, PathValues)
        self.assertTrue(len(result) >= 2)  # Should find response_time_ms and Response_Time_MS

    def test_regex_key_flag_combinations(self):
        """Test various flag combinations"""
        flag_combinations = [
            "i",     # case-insensitive
            "im",    # case-insensitive + multiline
            "is",    # case-insensitive + dotall
            "ix",    # case-insensitive + verbose
            "imsxa" # all flags
        ]
        
        for flags in flag_combinations:
            with self.subTest(flags=flags):
                try:
                    result = eval_path([["regex_key", "test", flags]], self.test_data)
                    self.assertIsInstance(result, PathValues)
                except Exception as e:
                    self.fail(f"Flag combination '{flags}' should not raise an exception: {e}")

if __name__ == '__main__':
    unittest.main()
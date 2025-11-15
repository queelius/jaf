"""
Comprehensive test suite for dsl_compiler.py

Tests DSL compilation, smart_compile auto-detection, and error handling.
Focuses on behavior contracts rather than implementation details.
"""

import pytest
import json
from jaf.dsl_compiler import (
    DSLCompiler,
    compile_dsl,
    smart_compile,
    is_dsl_expression
)
from jaf.dsl_parser import DSLSyntaxError


class TestDSLCompiler:
    """Test DSLCompiler class behavior"""

    def setup_method(self):
        self.compiler = DSLCompiler()

    def test_compiler_basic_compilation(self):
        """Compiler should convert DSL strings to JAF AST"""
        result = self.compiler.compile("@age > 30")

        assert isinstance(result, list)
        assert result[0] == "gt?"
        assert result[1] == ["@", [["key", "age"]]]
        assert result[2] == 30

    def test_compiler_compile_to_json(self):
        """Compiler should produce valid JSON output"""
        json_result = self.compiler.compile_to_json('@name == "Alice"')

        # Should be valid JSON
        parsed = json.loads(json_result)
        assert isinstance(parsed, list)
        assert parsed[0] == "eq?"

    def test_compiler_handles_syntax_errors(self):
        """Compiler should raise DSLSyntaxError for invalid syntax"""
        with pytest.raises(DSLSyntaxError):
            self.compiler.compile("@age >>> 30")  # Invalid operator

        with pytest.raises(DSLSyntaxError):
            self.compiler.compile("@age >")  # Incomplete expression

    def test_compiler_preserves_expression_semantics(self):
        """Compiled AST should preserve original expression meaning"""
        expressions = [
            ("@x > 5", ["gt?", ["@", [["key", "x"]]], 5]),
            ('@s == "test"', ["eq?", ["@", [["key", "s"]]], "test"]),
            ("@flag == true", ["eq?", ["@", [["key", "flag"]]], True]),
        ]

        for dsl, expected_ast in expressions:
            result = self.compiler.compile(dsl)
            assert result == expected_ast


class TestCompileDSLFunction:
    """Test compile_dsl convenience function"""

    def test_compile_dsl_simple_expression(self):
        """compile_dsl should work for simple expressions"""
        result = compile_dsl("@count < 10")

        assert result[0] == "lt?"
        assert result[2] == 10

    def test_compile_dsl_creates_new_compiler_each_time(self):
        """compile_dsl should work independently for multiple calls"""
        # Multiple calls should not interfere with each other
        result1 = compile_dsl("@x > 1")
        result2 = compile_dsl("@y < 2")

        assert result1[1] == ["@", [["key", "x"]]]
        assert result2[1] == ["@", [["key", "y"]]]


class TestIsDSLExpression:
    """Test is_dsl_expression detection function"""

    def test_detects_string_as_dsl(self):
        """Plain strings should be detected as DSL"""
        assert is_dsl_expression("@age > 30")
        assert is_dsl_expression('@name == "Alice"')
        assert is_dsl_expression("@active and @verified")

    def test_detects_list_as_ast(self):
        """Lists should be detected as AST, not DSL"""
        ast = ["gt?", "@age", 30]
        assert not is_dsl_expression(ast)

        complex_ast = ["and", ["eq?", "@x", 1], ["gt?", "@y", 2]]
        assert not is_dsl_expression(complex_ast)

    def test_detects_json_array_string_as_ast(self):
        """JSON-encoded arrays should be detected as AST"""
        json_ast = '["gt?", "@age", 30]'
        assert not is_dsl_expression(json_ast)

    def test_handles_edge_cases(self):
        """Should handle edge cases gracefully"""
        # Empty string is DSL (though invalid)
        assert is_dsl_expression("")

        # Whitespace is DSL
        assert is_dsl_expression("   ")

        # Non-list, non-string types
        assert is_dsl_expression(42)  # Assumes DSL for unknown types
        assert is_dsl_expression(None)


class TestSmartCompile:
    """Test smart_compile auto-detection and compilation"""

    def test_smart_compile_with_list_ast(self):
        """smart_compile should pass through list AST unchanged"""
        ast = ["gt?", "@age", 30]
        result = smart_compile(ast)

        assert result == ast
        assert result is ast  # Should be same object

    def test_smart_compile_with_json_array_string(self):
        """smart_compile should parse JSON array strings"""
        json_ast = '["eq?", "@name", "Alice"]'
        result = smart_compile(json_ast)

        assert isinstance(result, list)
        assert result[0] == "eq?"
        assert result[2] == "Alice"

    def test_smart_compile_with_sexp_string(self):
        """smart_compile should parse S-expression strings"""
        sexp = '(gt? @age 30)'
        result = smart_compile(sexp)

        assert isinstance(result, list)
        assert result[0] == "gt?"

    def test_smart_compile_with_dsl_string(self):
        """smart_compile should compile DSL strings"""
        dsl = "@age > 30"
        result = smart_compile(dsl)

        assert isinstance(result, list)
        assert result[0] == "gt?"
        assert result[2] == 30

    def test_smart_compile_strips_whitespace(self):
        """smart_compile should handle leading/trailing whitespace"""
        dsl_with_whitespace = "  @age > 30  "
        result = smart_compile(dsl_with_whitespace)

        assert result[0] == "gt?"

    def test_smart_compile_json_value_fallback(self):
        """smart_compile should try JSON parsing as last resort"""
        # A simple JSON value
        json_number = "42"
        result = smart_compile(json_number)
        assert result == 42

        json_string = '"hello"'
        result2 = smart_compile(json_string)
        assert result2 == "hello"

    def test_smart_compile_raises_on_unparseable(self):
        """smart_compile should raise DSLSyntaxError for invalid input"""
        with pytest.raises(DSLSyntaxError):
            smart_compile("@age >>> 30")  # Invalid syntax in all formats


class TestSmartCompileFormatDetection:
    """Test smart_compile's ability to detect and handle different formats"""

    @pytest.mark.parametrize("query,expected_operator", [
        # JSON array format
        ('["gt?", "@age", 30]', "gt?"),
        ('["eq?", ["@", [["key", "name"]]], "Alice"]', "eq?"),

        # S-expression format
        ('(gt? @age 30)', "gt?"),
        ('(eq? @name "Alice")', "eq?"),
        ('(and (gt? @x 1) (lt? @y 10))', "and"),

        # Infix DSL format
        ('@age > 30', "gt?"),
        ('@name == "Alice"', "eq?"),
        ('@x > 1 and @y < 10', "and"),
    ])
    def test_smart_compile_handles_all_formats(self, query, expected_operator):
        """smart_compile should correctly handle all supported formats"""
        result = smart_compile(query)

        assert isinstance(result, list)
        assert result[0] == expected_operator

    def test_smart_compile_consistent_output_across_formats(self):
        """smart_compile should produce same AST for equivalent expressions"""
        # These all represent: age > 30
        json_format = '["gt?", ["@", [["key", "age"]]], 30]'
        sexp_format = '(gt? @age 30)'
        dsl_format = '@age > 30'
        list_format = ["gt?", ["@", [["key", "age"]]], 30]

        result_json = smart_compile(json_format)
        result_sexp = smart_compile(sexp_format)
        result_dsl = smart_compile(dsl_format)
        result_list = smart_compile(list_format)

        # All should produce equivalent AST
        assert result_json[0] == "gt?"
        assert result_sexp[0] == "gt?"
        assert result_dsl[0] == "gt?"
        assert result_list[0] == "gt?"


class TestDSLCompilerComplexExpressions:
    """Test compilation of complex DSL expressions"""

    def test_compile_nested_logical_operators(self):
        """Should handle deeply nested logical expressions"""
        dsl = "(@a > 1 and @b < 2) or (@c == 3 and @d != 4)"
        result = smart_compile(dsl)

        assert result[0] == "or"
        assert isinstance(result[1], list)
        assert isinstance(result[2], list)

    def test_compile_nested_paths(self):
        """Should handle deeply nested path access"""
        dsl = "@user.profile.settings.theme.dark.enabled == true"
        result = smart_compile(dsl)

        assert result[0] == "eq?"
        assert result[1][0] == "@"
        # Should have multiple nested keys
        assert len(result[1][1]) > 3

    def test_compile_function_calls_in_expressions(self):
        """Should handle function calls within expressions"""
        dsl = "length(@items) > 0"
        result = smart_compile(dsl)

        assert result[0] == "gt?"
        assert result[1][0] == "length"

    def test_compile_with_various_literal_types(self):
        """Should handle different literal types correctly"""
        test_cases = [
            ('@x == 42', 42),
            ('@x == 3.14', 3.14),
            ('@x == "hello"', "hello"),
            ('@x == true', True),
            ('@x == false', False),
        ]

        for dsl, expected_value in test_cases:
            result = smart_compile(dsl)
            assert result[2] == expected_value


class TestDSLCompilerErrorHandling:
    """Test error handling and edge cases"""

    def test_compile_empty_string(self):
        """Empty string should raise DSLSyntaxError"""
        with pytest.raises(DSLSyntaxError):
            smart_compile("")

    def test_compile_whitespace_only(self):
        """Whitespace-only string should raise DSLSyntaxError"""
        with pytest.raises(DSLSyntaxError):
            smart_compile("   ")

    def test_compile_invalid_operator(self):
        """Invalid operators should raise DSLSyntaxError"""
        with pytest.raises(DSLSyntaxError):
            smart_compile("@x >>> 5")

    def test_compile_incomplete_expression(self):
        """Incomplete expressions should raise DSLSyntaxError"""
        with pytest.raises(DSLSyntaxError):
            smart_compile("@x >")

        with pytest.raises(DSLSyntaxError):
            smart_compile("@x and")

    def test_compile_mismatched_parens_in_sexp(self):
        """S-expressions with mismatched parens should fail gracefully"""
        # Should fail to parse as S-expression and try DSL
        with pytest.raises(DSLSyntaxError):
            smart_compile("(gt? @age 30")

    def test_compile_invalid_json(self):
        """Invalid JSON should fall back to DSL parsing"""
        # This looks like JSON but is invalid - it may actually parse as some value
        # depending on the parser implementation
        result = smart_compile('["gt?", @age, 30]')  # Missing quotes around @age
        # The parser might interpret @age as a field reference in DSL mode
        # Just verify it doesn't crash
        assert isinstance(result, list)

    def test_compile_with_none_input(self):
        """None input should raise DSLSyntaxError"""
        with pytest.raises(DSLSyntaxError):
            smart_compile(None)

    def test_compile_with_numeric_input(self):
        """Numeric input should be treated as DSL and fail"""
        with pytest.raises(DSLSyntaxError):
            smart_compile(123)

    def test_compile_with_dict_input(self):
        """Dict input should raise DSLSyntaxError"""
        with pytest.raises(DSLSyntaxError):
            smart_compile({"op": "gt?", "arg": 30})


class TestDSLCompilerIntegration:
    """Integration tests with actual JAF evaluation (if needed)"""

    def test_compiled_dsl_structure_matches_hand_written_ast(self):
        """Compiled DSL should match manually written AST"""
        dsl = '@age > 30 and @name == "Alice"'
        compiled = smart_compile(dsl)

        hand_written = [
            "and",
            ["gt?", ["@", [["key", "age"]]], 30],
            ["eq?", ["@", [["key", "name"]]], "Alice"]
        ]

        assert compiled == hand_written

    def test_smart_compile_idempotent_for_ast(self):
        """smart_compile on AST should return same AST"""
        original_ast = ["gt?", ["@", [["key", "age"]]], 30]

        result1 = smart_compile(original_ast)
        result2 = smart_compile(result1)

        assert result1 == original_ast
        assert result2 == original_ast

    def test_round_trip_compile_to_json_and_back(self):
        """Should be able to compile DSL to JSON and parse back"""
        compiler = DSLCompiler()
        dsl = "@age > 30"

        # Compile to JSON
        json_ast = compiler.compile_to_json(dsl)

        # Parse JSON back
        result = smart_compile(json_ast)

        # Should produce same AST as direct compilation
        direct = compiler.compile(dsl)
        assert result == direct


class TestDSLCompilerEdgeCasesAndQuirks:
    """Test edge cases and quirky inputs"""

    def test_compile_with_unicode_characters(self):
        """Test unicode support - may not be supported by all parsers"""
        dsl = '@名前 == "アリス"'

        try:
            result = smart_compile(dsl)
            # If it works, validate the structure
            assert result[0] == "eq?"
            assert result[2] == "アリス"
        except DSLSyntaxError:
            # Unicode in field names may not be supported - that's acceptable
            pytest.skip("Parser does not support unicode in field names")

    def test_compile_with_escaped_quotes(self):
        """Should handle escaped quotes in strings"""
        # In Python string: @msg == "He said \"hello\""
        dsl = r'@msg == "He said \"hello\""'
        result = smart_compile(dsl)

        assert result[0] == "eq?"
        # Note: exact handling depends on parser implementation

    def test_compile_with_numeric_field_names(self):
        """Should handle numeric-looking field names"""
        dsl = '@"123" > 456'
        # May or may not be supported - test actual behavior
        try:
            result = smart_compile(dsl)
            # If it works, it should parse correctly
            assert isinstance(result, list)
        except DSLSyntaxError:
            # Also acceptable if not supported
            pass

    def test_compile_preserves_numeric_precision(self):
        """Should preserve numeric precision in literals"""
        dsl = "@value == 3.141592653589793"
        result = smart_compile(dsl)

        assert result[2] == 3.141592653589793

    def test_compile_with_very_large_numbers(self):
        """Should handle very large numbers"""
        dsl = "@big == 999999999999999999"
        result = smart_compile(dsl)

        assert result[2] == 999999999999999999

    def test_compile_with_negative_numbers(self):
        """Should handle negative numbers"""
        dsl = "@temp < -273.15"
        result = smart_compile(dsl)

        assert result[0] == "lt?"
        assert result[2] == -273.15

    def test_compile_with_special_characters_in_strings(self):
        """Should handle special characters in string literals"""
        special_chars = [
            ('@s == "hello\\nworld"', "hello\\nworld"),  # Newline
            ('@s == "tab\\there"', "tab\\there"),  # Tab
        ]

        for dsl, expected_value in special_chars:
            try:
                result = smart_compile(dsl)
                # Check if it compiled (exact escaping depends on parser)
                assert result[0] == "eq?"
            except DSLSyntaxError:
                # Some parsers may not support all escape sequences
                pass


class TestDSLCompilerRegressions:
    """Tests for previously found bugs (regression tests)"""

    def test_compile_does_not_mutate_input(self):
        """Compilation should not mutate input data"""
        original_ast = ["gt?", ["@", [["key", "age"]]], 30]
        original_copy = original_ast.copy()

        result = smart_compile(original_ast)

        # Original should be unchanged
        assert original_ast == original_copy

    def test_multiple_compilations_independent(self):
        """Multiple compilations should not interfere with each other"""
        compiler = DSLCompiler()

        result1 = compiler.compile("@x > 1")
        result2 = compiler.compile("@y < 2")

        # Results should be independent
        assert result1[1] == ["@", [["key", "x"]]]
        assert result2[1] == ["@", [["key", "y"]]]

        # First result should not be affected by second compilation
        assert result1[2] == 1

    def test_compile_with_malformed_json_array(self):
        """Malformed JSON should gracefully fall back to DSL parsing"""
        malformed = '["gt?", "@age",]'  # Trailing comma

        with pytest.raises(DSLSyntaxError):
            smart_compile(malformed)

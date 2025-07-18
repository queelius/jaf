"""
Tests for JAF DSL (Domain Specific Language) functionality.
Tests both DSL parsing/compilation and integration with JAF evaluation.
"""

import pytest
from jaf.lazy_streams import stream
from jaf.dsl_parser import DSLParser, DSLSyntaxError
from jaf.dsl_compiler import DSLCompiler, compile_dsl, smart_compile, is_dsl_expression


class TestDSLParser:
    """Test the DSL parser functionality"""

    def setup_method(self):
        self.parser = DSLParser()

    def test_basic_comparisons(self):
        """Test basic comparison operations"""
        test_cases = [
            ("@age > 30", ["gt?", ["@", [["key", "age"]]], 30]),
            ('@name == "Alice"', ["eq?", ["@", [["key", "name"]]], "Alice"]),
            ("@score >= 80", ["gte?", ["@", [["key", "score"]]], 80]),
            ("@count < 5", ["lt?", ["@", [["key", "count"]]], 5]),
            ("@price <= 100", ["lte?", ["@", [["key", "price"]]], 100]),
            ('@status != "pending"', ["neq?", ["@", [["key", "status"]]], "pending"]),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_logical_operations(self):
        """Test logical AND, OR, NOT operations"""
        test_cases = [
            (
                "@age > 30 and @active == true",
                [
                    "and",
                    ["gt?", ["@", [["key", "age"]]], 30],
                    ["eq?", ["@", [["key", "active"]]], True],
                ],
            ),
            (
                '@score > 80 or @grade == "A"',
                [
                    "or",
                    ["gt?", ["@", [["key", "score"]]], 80],
                    ["eq?", ["@", [["key", "grade"]]], "A"],
                ],
            ),
            ("not @active", ["not", ["@", [["key", "active"]]]]),
            (
                "@age >= 18 and @age < 65",
                [
                    "and",
                    ["gte?", ["@", [["key", "age"]]], 18],
                    ["lt?", ["@", [["key", "age"]]], 65],
                ],
            ),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_nested_paths(self):
        """Test nested object path access"""
        test_cases = [
            ("@user.age > 30", ["gt?", ["@", [["key", "user"], ["key", "age"]]], 30]),
            (
                '@user.profile.settings.theme == "dark"',
                [
                    "eq?",
                    [
                        "@",
                        [
                            ["key", "user"],
                            ["key", "profile"],
                            ["key", "settings"],
                            ["key", "theme"],
                        ],
                    ],
                    "dark",
                ],
            ),
            (
                "length(@data.items) > 0",
                ["gt?", ["length", ["@", [["key", "data"], ["key", "items"]]]], 0],
            ),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_function_calls(self):
        """Test function call syntax"""
        test_cases = [
            ('startswith(@name, "A")', ["starts-with?", ["@", [["key", "name"]]], "A"]),
            (
                'endswith(@email, "@company.com")',
                ["ends-with?", ["@", [["key", "email"]]], "@company.com"],
            ),
            (
                'contains(@tags, "admin")',
                ["contains?", ["@", [["key", "tags"]]], "admin"],
            ),
            (
                'matches(@description, "^[A-Z]")',
                ["regex-match?", ["@", [["key", "description"]]], "^[A-Z]"],
            ),
            ("length(@items)", ["length", ["@", [["key", "items"]]]]),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_data_types(self):
        """Test parsing of different data types"""
        test_cases = [
            ("@active == true", ["eq?", ["@", [["key", "active"]]], True]),
            ("@deleted == false", ["eq?", ["@", [["key", "deleted"]]], False]),
            ("@value == null", ["eq?", ["@", [["key", "value"]]], None]),
            ("@count == 42", ["eq?", ["@", [["key", "count"]]], 42]),
            ("@price == 19.99", ["eq?", ["@", [["key", "price"]]], 19.99]),
            (
                "@name == 'single quotes'",
                ["eq?", ["@", [["key", "name"]]], "single quotes"],
            ),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_complex_expressions(self):
        """Test complex multi-part expressions"""
        test_cases = [
            (
                "@age >= 18 and @age < 65 and @active == true",
                [
                    "and",
                    [
                        "and",
                        ["gte?", ["@", [["key", "age"]]], 18],
                        ["lt?", ["@", [["key", "age"]]], 65],
                    ],
                    ["eq?", ["@", [["key", "active"]]], True],
                ],
            ),
            (
                '@type == "user" or @type == "admin"',
                [
                    "or",
                    ["eq?", ["@", [["key", "type"]]], "user"],
                    ["eq?", ["@", [["key", "type"]]], "admin"],
                ],
            ),
            (
                'contains(@tags, "critical") and not @archived',
                [
                    "and",
                    ["contains?", ["@", [["key", "tags"]]], "critical"],
                    ["not", ["@", [["key", "archived"]]]],
                ],
            ),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_parentheses_support(self):
        """Test parentheses for grouping expressions"""
        test_cases = [
            # Basic parentheses
            ("(@age > 30)", ["gt?", ["@", [["key", "age"]]], 30]),
            # Parentheses changing precedence
            (
                '(@age > 30 and @active == true) or @role == "admin"',
                [
                    "or",
                    [
                        "and",
                        ["gt?", ["@", [["key", "age"]]], 30],
                        ["eq?", ["@", [["key", "active"]]], True],
                    ],
                    ["eq?", ["@", [["key", "role"]]], "admin"],
                ],
            ),
            # Multiple grouped expressions
            (
                '(@age >= 18 and @age < 65) and (@active == true or @role == "admin")',
                [
                    "and",
                    [
                        "and",
                        ["gte?", ["@", [["key", "age"]]], 18],
                        ["lt?", ["@", [["key", "age"]]], 65],
                    ],
                    [
                        "or",
                        ["eq?", ["@", [["key", "active"]]], True],
                        ["eq?", ["@", [["key", "role"]]], "admin"],
                    ],
                ],
            ),
            # Nested parentheses
            (
                '((@age > 30 and @active == true) or @role == "admin") and @verified == true',
                [
                    "and",
                    [
                        "or",
                        [
                            "and",
                            ["gt?", ["@", [["key", "age"]]], 30],
                            ["eq?", ["@", [["key", "active"]]], True],
                        ],
                        ["eq?", ["@", [["key", "role"]]], "admin"],
                    ],
                    ["eq?", ["@", [["key", "verified"]]], True],
                ],
            ),
            # Function calls in parentheses
            (
                '(contains(@tags, "admin") or contains(@tags, "super")) and @active == true',
                [
                    "and",
                    [
                        "or",
                        ["contains?", ["@", [["key", "tags"]]], "admin"],
                        ["contains?", ["@", [["key", "tags"]]], "super"],
                    ],
                    ["eq?", ["@", [["key", "active"]]], True],
                ],
            ),
        ]

        for dsl, expected_ast in test_cases:
            result = self.parser.parse(dsl)
            assert result == expected_ast, f"Failed for '{dsl}'"

    def test_error_handling(self):
        """Test error handling for invalid expressions"""
        invalid_expressions = [
            "",  # Empty expression
            "@age >",  # Incomplete comparison
            "@age > 30 and",  # Incomplete logical expression
            # Note: unknown functions are not errors in the parser - they're checked in the evaluator
        ]

        for expr in invalid_expressions:
            with pytest.raises(DSLSyntaxError):
                self.parser.parse(expr)

    def test_parentheses_error_handling(self):
        """Test error handling for parentheses issues"""
        invalid_expressions = [
            "(@age > 30",  # Unmatched opening parenthesis
            "@age > 30)",  # Unmatched closing parenthesis
            "((@age > 30)",  # Unmatched nested opening parenthesis
            "(@age > 30))",  # Unmatched nested closing parenthesis
            "()",  # Empty parentheses
            "(@age >)",  # Incomplete expression in parentheses
        ]

        for expr in invalid_expressions:
            with pytest.raises(DSLSyntaxError):
                self.parser.parse(expr)


class TestDSLCompiler:
    """Test the DSL compiler functionality"""

    def setup_method(self):
        self.compiler = DSLCompiler()

    def test_compile_basic(self):
        """Test basic compilation functionality"""
        dsl = "@age > 30"
        ast = self.compiler.compile(dsl)
        expected = ["gt?", ["@", [["key", "age"]]], 30]
        assert ast == expected

    def test_compile_to_json(self):
        """Test compilation to JSON format"""
        dsl = '@name == "Alice"'
        json_result = self.compiler.compile_to_json(dsl)
        assert '"eq?"' in json_result
        assert '"Alice"' in json_result

    def test_smart_compile_dsl(self):
        """Test smart_compile with DSL input"""
        dsl = "@age > 30"
        result = smart_compile(dsl)
        expected = ["gt?", ["@", [["key", "age"]]], 30]
        assert result == expected

    def test_smart_compile_ast(self):
        """Test smart_compile with AST input"""
        ast = ["eq?", ["@", [["key", "name"]]], "Alice"]
        result = smart_compile(ast)
        assert result == ast  # Should return unchanged

    def test_smart_compile_json_ast(self):
        """Test smart_compile with JSON-encoded AST"""
        json_ast = '["eq?", ["@", [["key", "name"]]], "Alice"]'
        result = smart_compile(json_ast)
        expected = ["eq?", ["@", [["key", "name"]]], "Alice"]
        assert result == expected

    def test_is_dsl_expression(self):
        """Test DSL vs AST detection"""
        # DSL expressions
        assert is_dsl_expression("@age > 30") == True
        assert is_dsl_expression('@name == "Alice"') == True

        # AST expressions
        assert is_dsl_expression(["eq?", ["@", [["key", "name"]]], "Alice"]) == False
        assert is_dsl_expression('["eq?", ["@", [["key", "name"]]], "Alice"]') == False


class TestDSLIntegration:
    """Test DSL integration with JAF evaluation"""

    def setup_method(self):
        self.test_data = [
            {"name": "Alice", "age": 30, "active": True, "tags": ["admin", "user"]},
            {"name": "Bob", "age": 25, "active": False, "tags": ["user"]},
            {"name": "Charlie", "age": 35, "active": True, "tags": ["user", "premium"]},
            {"name": "Diana", "age": 28, "active": True, "tags": []},
        ]

    def test_dsl_evaluation_basic(self):
        """Test DSL evaluation with basic expressions"""
        dsl = "@age > 30"
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(ast)
        matches = list(result.evaluate())

        assert len(matches) == 1
        assert matches[0]["name"] == "Charlie"

    def test_dsl_evaluation_logical(self):
        """Test DSL evaluation with logical operations"""
        dsl = "@age > 25 and @active == true"
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(ast)
        matches = list(result.evaluate())

        assert len(matches) == 3  # Alice, Charlie, Diana
        names = {obj["name"] for obj in matches}
        assert names == {"Alice", "Charlie", "Diana"}

    def test_dsl_evaluation_function_calls(self):
        """Test DSL evaluation with function calls"""
        dsl = 'contains(@tags, "admin")'
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(ast)
        matches = list(result.evaluate())

        assert len(matches) == 1
        assert matches[0]["name"] == "Alice"

    def test_dsl_evaluation_string_functions(self):
        """Test DSL evaluation with string functions"""
        dsl = 'startswith(@name, "A")'
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(ast)
        matches = list(result.evaluate())

        assert len(matches) == 1
        assert matches[0]["name"] == "Alice"

    def test_dsl_evaluation_complex(self):
        """Test DSL evaluation with complex expressions"""
        dsl = '@age >= 30 and (@active == true or contains(@tags, "premium"))'
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(ast)
        matches = list(result.evaluate())

        assert len(matches) == 2  # Alice and Charlie
        names = {obj["name"] for obj in matches}
        assert names == {"Alice", "Charlie"}

    def test_dsl_vs_ast_equivalence(self):
        """Test that DSL and equivalent AST produce same results"""
        # DSL version
        dsl = "@age > 30 and @active == true"
        dsl_ast = compile_dsl(dsl)
        s1 = stream({"type": "memory", "data": self.test_data})
        dsl_result = list(s1.filter(dsl_ast).evaluate())

        # Equivalent AST version
        ast = [
            "and",
            ["gt?", ["@", [["key", "age"]]], 30],
            ["eq?", ["@", [["key", "active"]]], True],
        ]
        s2 = stream({"type": "memory", "data": self.test_data})
        ast_result = list(s2.filter(ast).evaluate())

        # Should produce identical results
        assert dsl_result == ast_result

    def test_dsl_nested_objects(self):
        """Test DSL with nested object structures"""
        nested_data = [
            {"user": {"profile": {"name": "Alice", "age": 30}}, "active": True},
            {"user": {"profile": {"name": "Bob", "age": 25}}, "active": False},
        ]

        dsl = "@user.profile.age > 27 and @active == true"
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": nested_data})
        result = s.filter(ast)
        matches = list(result.evaluate())

        assert len(matches) == 1
        assert matches[0]["user"]["profile"]["name"] == "Alice"

    def test_dsl_error_handling_in_evaluation(self):
        """Test error handling when DSL compiles but evaluation fails"""
        # Valid DSL that refers to non-existent field
        dsl = "@nonexistent_field > 10"
        ast = compile_dsl(dsl)
        s = stream({"type": "memory", "data": self.test_data})
        result = s.filter(ast)

        # Should not crash, just return no matches
        matches = list(list(result.evaluate()))
        assert len(matches) == 0


class TestDSLFeatures:
    """Test advanced DSL features and edge cases"""

    def test_operator_precedence(self):
        """Test that logical operators have correct precedence"""
        # AND should have higher precedence than OR
        dsl = "@age > 30 or @active == true and @age < 50"
        ast = compile_dsl(dsl)

        # Should parse as: age > 30 OR (active == true AND age < 50)
        expected = [
            "or",
            ["gt?", ["@", [["key", "age"]]], 30],
            [
                "and",
                ["eq?", ["@", [["key", "active"]]], True],
                ["lt?", ["@", [["key", "age"]]], 50],
            ],
        ]
        assert ast == expected

    def test_quotes_in_strings(self):
        """Test handling of quotes in string literals"""
        test_cases = [
            ('@name == "Alice"', "Alice"),
            ("@name == 'Bob'", "Bob"),
            (
                '@description == "User said \\"hello\\""',
                'User said "hello"',
            ),  # Escaped quotes
        ]

        for dsl, expected_value in test_cases:
            ast = compile_dsl(dsl)
            assert ast[2] == expected_value

    def test_numeric_edge_cases(self):
        """Test numeric parsing edge cases"""
        test_cases = [
            ("@value == 0", 0),
            ("@price == 0.0", 0.0),
            ("@negative == -42", -42),
            ("@decimal == 3.14159", 3.14159),
        ]

        for dsl, expected_value in test_cases:
            ast = compile_dsl(dsl)
            assert ast[2] == expected_value

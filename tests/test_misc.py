"""
Tests for JAF AST syntax and potential DSL considerations.
This module tests the current JSON AST format and explores areas where
a more user-friendly DSL might be beneficial.
"""
import pytest
from jaf import jaf, jaf_eval, JafResultSet


class TestJsonAstReadability:
    """Test cases that highlight JSON AST readability patterns"""
    
    def setup_method(self):
        """Set up test data"""
        self.test_data = [
            {
                "id": 1,
                "user": {
                    "name": "Alice Johnson",
                    "email": "alice@example.com",
                    "profile": {
                        "settings": {"theme": "dark", "notifications": True},
                        "preferences": {"language": "en"}
                    }
                },
                "projects": [
                    {"name": "WebApp", "status": "completed", "priority": "high"},
                    {"name": "API", "status": "in-progress", "priority": "medium"}
                ],
                "tags": ["python", "web", "api"],
                "active": True,
                "score": 95.5
            },
            {
                "id": 2,
                "user": {
                    "name": "Bob Smith",
                    "email": "bob@example.com",
                    "profile": {
                        "settings": {"theme": "light", "notifications": False},
                        "preferences": {"language": "es"}
                    }
                },
                "projects": [
                    {"name": "Mobile", "status": "pending", "priority": "low"}
                ],
                "tags": ["mobile", "ios"],
                "active": False,
                "score": 78.2
            }
        ]

    def test_simple_equality_queries(self):
        """Test simple equality queries that could benefit from infix syntax"""
        
        # Current JSON AST
        query = ["eq?", ["path", "user.name"], "Alice Johnson"]
        result = jaf(self.test_data, query)
        assert len(result.indices) == 1
        assert 0 in result.indices
        
        # Multiple simple comparisons
        queries_and_expected = [
            (["eq?", ["path", "active"], True], {0}),
            (["eq?", ["path", "user.profile.settings.theme"], "dark"], {0}),
            (["gt?", ["path", "score"], 80], {0}),
            (["lt?", ["path", "score"], 80], {1}),
            (["in?", "python", ["path", "tags"]], {0}),
        ]
        
        for query, expected_indices in queries_and_expected:
            result = jaf(self.test_data, query)
            assert result.indices == expected_indices, f"Query {query} failed"

    def test_complex_logical_combinations(self):
        """Test complex logical combinations that show AST verbosity"""
        
        # Complex AND/OR that could be more readable with infix
        query = [
            "and",
            ["eq?", ["path", "active"], True],
            ["or",
                ["gt?", ["path", "score"], 90],
                ["in?", "web", ["path", "tags"]]
            ]
        ]
        result = jaf(self.test_data, query)
        assert len(result.indices) == 1
        assert 0 in result.indices
        
        # Nested logical with multiple conditions
        query = [
            "or",
            [
                "and",
                ["eq?", ["path", "user.profile.settings.theme"], "dark"],
                ["gte?", ["path", "score"], 95]
            ],
            [
                "and", 
                ["eq?", ["path", "user.profile.settings.theme"], "light"],
                ["lt?", ["path", "score"], 80]
            ]
        ]
        result = jaf(self.test_data, query)
        assert len(result.indices) == 2  # Both users match one of the conditions

    def test_path_expressions_with_wildcards(self):
        """Test path expressions that show the power of the current path system"""
        
        # Wildcard queries that are quite readable with path strings
        queries_and_expected = [
            # Find items with any completed project
            (["eq?", ["path", "projects[*].status"], "completed"], {0}),
            
            # Find items with high priority projects
            (["eq?", ["path", "projects[*].priority"], "high"], {0}),
            
            # Find items with specific settings anywhere in profile
            (["eq?", ["path", "user.profile.**.theme"], "dark"], {0}),
            
            # Find items with notification settings
            (["exists?", ["path", "user.profile.settings.notifications"]], {0, 1}),
        ]
        
        for query, expected_indices in queries_and_expected:
            result = jaf(self.test_data, query)
            assert result.indices == expected_indices, f"Query {query} failed"

    def test_function_composition_patterns(self):
        """Test function composition that shows AST structure benefits"""
        
        # Function composition with length, type checking
        queries_and_expected = [
            # Items with more than 2 tags
            (["gt?", ["length", ["path", "tags"]], 2], {0}),
            
            # Items with exactly 2 projects
            (["eq?", ["length", ["path", "projects"]], 2], {0}),
            
            # String manipulation
            (["eq?", ["upper-case", ["path", "user.name"]], "ALICE JOHNSON"], {0}),
            
            # Type checking
            (["eq?", ["type", ["path", "score"]], "float"], {0, 1}),
        ]
        
        for query, expected_indices in queries_and_expected:
            result = jaf(self.test_data, query)
            assert result.indices == expected_indices, f"Query {query} failed"

    def test_conditional_logic_patterns(self):
        """Test conditional logic that shows where DSL might help"""
        
        # Conditional expressions
        query = [
            "if",
            ["eq?", ["path", "active"], True],
            ["gt?", ["path", "score"], 90],  # If active, check high score
            ["lt?", ["path", "score"], 80]   # If not active, check low score  
        ]
        result = jaf(self.test_data, query)
        assert len(result.indices) == 2  # Both conditions met by different users


class TestAstVersusHypotheticalDsl:
    """Test cases that compare current AST with hypothetical DSL syntax"""
    
    def setup_method(self):
        """Set up test data"""
        self.test_data = [
            {"name": "Alice", "age": 30, "dept": "Engineering", "salary": 95000},
            {"name": "Bob", "age": 25, "dept": "Marketing", "salary": 65000},
            {"name": "Charlie", "age": 35, "dept": "Engineering", "salary": 110000}
        ]

    def test_readability_comparison_comments(self):
        """Document what hypothetical infix DSL might look like"""
        
        # Current JSON AST
        json_ast_query = [
            "and",
            ["eq?", ["path", "dept"], "Engineering"],
            ["gt?", ["path", "salary"], 80000]
        ]
        
        # Hypothetical infix DSL would be:
        # dept == "Engineering" && salary > 80000
        # or: (eq? dept "Engineering") and (gt? salary 80000)
        
        result = jaf(self.test_data, json_ast_query)
        assert result.indices == {0, 2}  # Alice and Charlie
        
        # Another example - current AST
        complex_query = [
            "or",
            ["and", ["eq?", ["path", "dept"], "Engineering"], ["gt?", ["path", "age"], 30]],
            ["and", ["eq?", ["path", "dept"], "Marketing"], ["lt?", ["path", "salary"], 70000]]
        ]
        
        # Hypothetical DSL:
        # (dept == "Engineering" && age > 30) || (dept == "Marketing" && salary < 70000)
        
        result = jaf(self.test_data, complex_query)
        assert result.indices == {1, 2}  # Bob and Charlie

    def test_ast_programmatic_advantages(self):
        """Test advantages of JSON AST for programmatic construction"""
        
        def build_dept_filter(department):
            """Function to programmatically build dept filter"""
            return ["eq?", ["path", "dept"], department]
        
        def build_salary_filter(min_salary):
            """Function to programmatically build salary filter"""
            return ["gte?", ["path", "salary"], min_salary]
        
        def combine_filters(*filters):
            """Combine multiple filters with AND"""
            if len(filters) == 1:
                return filters[0]
            return ["and"] + list(filters)
        
        # Build query programmatically
        dept_filter = build_dept_filter("Engineering")
        salary_filter = build_salary_filter(90000)
        combined_query = combine_filters(dept_filter, salary_filter)
        
        result = jaf(self.test_data, combined_query)
        assert result.indices == {0, 2}  # High-earning engineers
        
        # This kind of programmatic construction would be much harder
        # with an infix DSL that required string parsing


class TestJsonAstConsistency:
    """Test the consistency and predictability of JSON AST"""
    
    def test_uniform_operator_structure(self):
        """Test that all operators follow consistent patterns"""
        
        data = [{"x": 10, "y": "hello", "z": [1, 2, 3]}]
        
        # All predicates follow [operator, arg1, arg2, ...]
        predicates = [
            ["eq?", ["path", "x"], 10],
            ["gt?", ["path", "x"], 5], 
            ["in?", 2, ["path", "z"]],
            ["starts-with?", "hel", ["path", "y"]],
            ["exists?", ["path", "x"]],
        ]
        
        for pred in predicates:
            result = jaf_eval.eval(pred, data[0])
            assert isinstance(result, bool), f"Predicate {pred[0]} should return boolean"
        
        # All value extractors follow same pattern
        extractors = [
            ["length", ["path", "y"]],
            ["type", ["path", "x"]],
            ["upper-case", ["path", "y"]],
        ]
        
        for ext in extractors:
            result = jaf_eval.eval(ext, data[0])
            assert result is not None, f"Extractor {ext[0]} should return value"

    def test_composability(self):
        """Test that AST expressions compose naturally"""
        
        data = [{"items": ["a", "bb", "ccc"]}]
        
        # Nested composition: length of first item
        query = ["length", ["path", "items[0]"]]
        result = jaf_eval.eval(query, data[0])
        assert result == 1
        
        # Double composition: check if length of first item equals 1
        query = ["eq?", ["length", ["path", "items[0]"]], 1]
        result = jaf_eval.eval(query, data[0])
        assert result is True
        
        # Triple composition in filter context
        query = ["eq?", ["length", ["path", "items[0]"]], 1]
        result = jaf(data, query)
        assert len(result.indices) == 1


class TestAstVerbosityAnalysis:
    """Analyze where AST verbosity might be problematic"""
    
    def test_simple_comparisons_verbosity(self):
        """Identify simple cases where AST is verbose"""
        
        data = [{"name": "Alice", "age": 30}]
        
        # Simple equality - 4 tokens in AST vs 3 in infix
        # JSON AST: ["eq?", ["path", "name"], "Alice"]
        # Infix:    name == "Alice"
        query = ["eq?", ["path", "name"], "Alice"]
        result = jaf(data, query)
        assert len(result.indices) == 1
        
        # The path wrapper adds verbosity for simple cases
        # But provides power for complex paths like "user.profile.*.settings"
        
    def test_complex_expressions_readability(self):
        """Test where complex expressions might benefit from infix"""
        
        data = [
            {"name": "Alice", "age": 30, "dept": "Eng", "active": True},
            {"name": "Bob", "age": 25, "dept": "Sales", "active": False}
        ]
        
        # Complex logical expression
        query = [
            "and",
            ["or", 
                ["eq?", ["path", "dept"], "Eng"],
                ["eq?", ["path", "dept"], "Sales"]
            ],
            ["and",
                ["gt?", ["path", "age"], 25],
                ["eq?", ["path", "active"], True]
            ]
        ]
        
        # Infix equivalent would be:
        # (dept == "Eng" || dept == "Sales") && (age > 25 && active == true)
        
        result = jaf(data, query)
        assert result.indices == {0}  # Only Alice matches


if __name__ == "__main__":
    pytest.main([__file__])
"""
JAF DSL Compiler

Compiles DSL expressions to JAF AST and integrates with the JAF evaluation system.
Provides a bridge between human-friendly syntax and JAF's AST-based evaluation.

Supports multiple syntaxes:
1. JSON arrays: ["eq?", "@name", "Alice"]
2. Infix DSL: name == "Alice" and age > 25
3. S-expressions: (and (eq? @name "Alice") (gt? @age 25))
"""

from typing import List, Any, Union
import json
import logging
from .dsl_parser import DSLParser, DSLSyntaxError
from .sexp_parser import sexp_to_jaf

logger = logging.getLogger(__name__)


class DSLCompiler:
    """
    Compiler that converts DSL expressions to JAF AST format.
    """

    def __init__(self):
        self.parser = DSLParser()

    def compile(self, expression: str) -> List[Any]:
        """
        Compile a DSL expression to JAF AST format.

        Args:
            expression: DSL expression string

        Returns:
            JAF AST representation

        Raises:
            DSLSyntaxError: If compilation fails
        """
        logger.debug(f"Compiling DSL expression: {expression}")

        try:
            ast = self.parser.parse(expression)
            logger.debug(f"Compiled to AST: {ast}")
            return ast
        except DSLSyntaxError:
            raise
        except Exception as e:
            raise DSLSyntaxError(f"Compilation failed: {e}") from e

    def compile_to_json(self, expression: str) -> str:
        """
        Compile a DSL expression to JSON-formatted JAF AST.

        Args:
            expression: DSL expression string

        Returns:
            JSON string representation of JAF AST
        """
        ast = self.compile(expression)
        return json.dumps(ast)


def compile_dsl(expression: str) -> List[Any]:
    """
    Convenience function to compile a DSL expression.

    Args:
        expression: DSL expression string

    Returns:
        JAF AST representation
    """
    compiler = DSLCompiler()
    return compiler.compile(expression)


def is_dsl_expression(query: Union[str, List[Any]]) -> bool:
    """
    Determine if a query is a DSL expression or already AST format.

    Args:
        query: Query string or AST list

    Returns:
        True if it's a DSL expression, False if it's already AST
    """
    if isinstance(query, str):
        # If it's a string that looks like JSON AST, it's probably AST
        try:
            parsed = json.loads(query)
            if isinstance(parsed, list) and len(parsed) > 0:
                return False  # Looks like AST
        except (json.JSONDecodeError, ValueError):
            pass
        # Otherwise assume it's DSL
        return True
    elif isinstance(query, list):
        # Already AST format
        return False
    else:
        # Unknown format, assume DSL
        return True


def smart_compile(query: Union[str, List[Any]]) -> List[Any]:
    """
    Smart compilation that auto-detects format and compiles to JAF AST.
    
    Supports:
    1. JSON arrays: ["eq?", "@name", "Alice"]
    2. S-expressions: (eq? @name "Alice")
    3. Infix DSL: name == "Alice"

    Args:
        query: Query in any supported format

    Returns:
        JAF AST representation
    """
    if isinstance(query, list):
        # Already AST format
        return query
    elif isinstance(query, str):
        # Remove leading/trailing whitespace
        query = query.strip()
        
        # Check if it's JSON-encoded AST
        if query.startswith('['):
            try:
                parsed = json.loads(query)
                if isinstance(parsed, list):
                    return parsed  # JSON-encoded AST
            except (json.JSONDecodeError, ValueError):
                pass
        
        # Check if it's an S-expression
        if query.startswith('('):
            try:
                return sexp_to_jaf(query)
            except Exception as e:
                logger.debug(f"Not an S-expression: {e}")
                # Fall through to try DSL
        
        # Try the infix DSL parser
        try:
            return compile_dsl(query)
        except DSLSyntaxError:
            # If DSL fails and it looks like it might be a simple value
            # Try parsing as JSON value
            try:
                value = json.loads(query)
                return value
            except:
                raise DSLSyntaxError(f"Could not parse query in any known format: {query}")
    else:
        raise DSLSyntaxError(f"Invalid query type: {type(query)}")


# Example usage
if __name__ == "__main__":
    # Test the compiler
    compiler = DSLCompiler()

    test_expressions = [
        "age > 30",
        'name == "Alice" and active == true',
        'user.profile.settings.theme == "dark"',
        'tags.contains("admin") or role == "superuser"',
        "not (age < 18 or age > 65)",
        'items.length > 0 and items[0].status == "completed"',
    ]

    print("DSL Compilation Test Results:")
    print("=" * 50)

    for expr in test_expressions:
        try:
            ast = compiler.compile(expr)
            json_ast = compiler.compile_to_json(expr)
            print(f"DSL: {expr}")
            print(f"AST: {ast}")
            print(f"JSON: {json_ast}")
            print("-" * 30)
        except DSLSyntaxError as e:
            print(f"DSL: {expr}")
            print(f"ERROR: {e}")
            print("-" * 30)

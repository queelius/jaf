"""
JAF DSL (Domain Specific Language) Parser using Lark

A clean grammar-based implementation of the DSL parser that converts
human-friendly filter expressions into JAF AST format.

Grammar Features:
- Operator precedence: or < and < not < comparison
- Function calls: length(@items), contains(@tags, "admin")
- Path expressions: @user.name, @items[0]
- Parentheses for grouping: (@age > 30 and @active) or @admin
- All standard literals: strings, numbers, booleans, null
"""

from lark import Lark, Transformer, v_args
from typing import List, Any, Union


# Define the grammar for our DSL
DSL_GRAMMAR = r"""
    ?start: or_expr

    ?or_expr: and_expr
            | or_expr "or" and_expr -> or_op

    ?and_expr: not_expr
             | and_expr "and" not_expr -> and_op

    ?not_expr: "not" atom -> not_op
             | atom

    ?atom: comparison
         | function_call
         | path
         | literal
         | "(" or_expr ")"

    comparison: atom comp_op atom

    function_call: IDENTIFIER "(" [arg_list] ")"

    path: "@" path_component ("." path_component)*

    arg_list: atom ("," atom)*

    path_component: IDENTIFIER
                  | "[" (NUMBER | STRING) "]" -> index_component

    comp_op: "==" -> eq
           | "!=" -> neq
           | ">"  -> gt
           | ">=" -> gte
           | "<"  -> lt
           | "<=" -> lte

    ?literal: STRING
            | NUMBER
            | "true"  -> true
            | "false" -> false
            | "null"  -> null
            | array

    array: "[" [atom ("," atom)*] "]"

    IDENTIFIER: /[a-zA-Z_][a-zA-Z0-9_]*/
    STRING: /"([^"\\]|\\.)*"/ | /'([^'\\]|\\.)*'/
    NUMBER: /-?\d+(\.\d+)?/

    %import common.WS
    %ignore WS
"""


@v_args(inline=True)  # Automatically unpack arguments
class DSLTransformer(Transformer):
    """Transform the parse tree into JAF AST format"""

    def start(self, expr):
        return expr

    # Logical operators
    def or_op(self, left, right):
        return ["or", left, right]

    def and_op(self, left, right):
        return ["and", left, right]

    def not_op(self, expr):
        return ["not", expr]

    # Comparison operators
    def comparison(self, left, op, right):
        return [op, left, right]

    def eq(self):
        return "eq?"

    def neq(self):
        return "neq?"

    def gt(self):
        return "gt?"

    def gte(self):
        return "gte?"

    def lt(self):
        return "lt?"

    def lte(self):
        return "lte?"

    # Function calls
    def function_call(self, name, args=None):
        func_name = str(name)
        args_list = args if args else []

        # Simple syntactic mapping: add '?' to predicates that need it
        # and handle hyphenation for multi-word functions
        hyphenated_predicates = {
            "contains": "contains?",
            "startswith": "starts-with?",
            "endswith": "ends-with?",
            "matches": "regex-match?",
            "regexmatch": "regex-match?",
            "closematch": "close-match?",
            "partialmatch": "partial-match?",
            "isstring": "is-string?",
            "isnumber": "is-number?",
            "isarray": "is-array?",
            "isobject": "is-object?",
            "isnull": "is-null?",
            "isempty": "is-empty?",
            "exists": "exists?",
        }

        # Apply mapping if it exists, otherwise pass through
        jaf_op = hyphenated_predicates.get(func_name, func_name)

        return [jaf_op, *args_list]

    def arg_list(self, *args):
        return list(args)

    # Path expressions
    def path(self, *components):
        path_parts = []
        for comp in components:
            if isinstance(comp, list):
                path_parts.extend(comp)
            else:
                path_parts.append(comp)
        return ["@", path_parts]

    def path_component(self, identifier):
        return [["key", str(identifier)]]

    def index_component(self, index):
        if isinstance(index, str):
            # String index like ["key"]
            return [["key", index]]
        else:
            # Numeric index like [0]
            return [["index", int(index)]]

    # Literals
    def STRING(self, s):
        # Remove quotes and handle escape sequences
        content = s[1:-1]  # Remove surrounding quotes
        content = content.replace('\\"', '"').replace("\\'", "'")
        content = content.replace("\\n", "\n").replace("\\t", "\t")
        content = content.replace("\\\\", "\\")
        return content

    def NUMBER(self, n):
        if "." in n:
            return float(n)
        return int(n)

    def IDENTIFIER(self, name):
        return str(name)

    def true(self):
        return True

    def false(self):
        return False

    def null(self):
        return None

    def array(self, *items):
        return list(items)


class DSLParser:
    """Parser for JAF's human-friendly DSL syntax using Lark"""

    def __init__(self):
        self.parser = Lark(DSL_GRAMMAR, parser="lalr", transformer=DSLTransformer())

    def parse(self, dsl_expression: str) -> List[Any]:
        """
        Parse a DSL expression into JAF AST format.

        Args:
            dsl_expression: Human-friendly filter expression

        Returns:
            JAF AST representation

        Raises:
            DSLSyntaxError: If the expression cannot be parsed
        """
        try:
            if not dsl_expression.strip():
                raise DSLSyntaxError("Empty expression")

            return self.parser.parse(dsl_expression)

        except Exception as e:
            # Convert Lark errors to our DSLSyntaxError
            raise DSLSyntaxError(f"Failed to parse expression: {e}") from e


class DSLSyntaxError(Exception):
    """Exception raised for DSL syntax errors"""

    pass


# Convenience function
def parse_dsl(expression: str) -> List[Any]:
    """Parse a DSL expression string into JAF AST format"""
    parser = DSLParser()
    return parser.parse(expression)

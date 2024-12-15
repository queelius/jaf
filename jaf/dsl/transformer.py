# siftarray/dsl/transformer.py

from lark import Transformer, v_args
from jaf.ast.ast_nodes import Condition, LogicalOperation

@v_args(inline=True)
class DSLTransformer(Transformer):
    """Transformer to convert Lark parse trees to AST objects."""

    def and_op(self, left, right):
        return LogicalOperation(operator='and', operands=[left, right])

    def or_op(self, left, right):
        return LogicalOperation(operator='or', operands=[left, right])

    def not_op(self, operand):
        return LogicalOperation(operator='not', operands=[operand])

    def condition(self, field, comparator, value):
        return Condition(field=field, operator=str(comparator), value=value)

    def field(self, *items):
        # Concatenate nested fields with dots
        return '.'.join(items)

    def string(self, s):
        # Remove surrounding quotes
        return str(s)[1:-1]

    def number(self, n):
        num_str = n
        if '.' in num_str or 'e' in num_str.lower():
            return float(num_str)
        return int(num_str)

    def true(self):
        return True

    def false(self):
        return False

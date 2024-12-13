# siftarray/ast/ast_nodes.py

from typing import List, Dict, Any
import re
from siftarray.ast.evaluator import evaluate_condition

class FilterError(Exception):
    """Custom exception for filter operations."""
    pass

class Condition:
    """Represents a single condition in the filter query."""

    comparison_operators = {'eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'contains', 'regex', 'startswith', 'endswith'}

    def __init__(self, field: str, operator: str, value: Any):
        if not isinstance(operator, str):
            raise FilterError(f"Invalid operator type: {type(operator)} in condition.")
        operator_lower = operator.lower()
        if operator_lower not in self.comparison_operators:
            raise FilterError(f"Unsupported comparison operator: {operator_lower}")
        self.field = field
        self.operator = operator_lower
        self.value = self.parse_value(value)

    @staticmethod
    def parse_value(value: Any) -> Any:
        """Parse the value into an appropriate type."""
        if isinstance(value, str):
            if re.fullmatch(r'^-?\d+$', value):
                return int(value)
            elif re.fullmatch(r'^-?\d+\.\d+$', value):
                return float(value)
            elif value.lower() == 'true':
                return True
            elif value.lower() == 'false':
                return False
        return value

    def get_nested_value(self, obj: Dict, key: str) -> Any:
        """Retrieve a nested value from a dictionary using dot notation."""
        parts = key.split('.')
        for part in parts:
            if isinstance(obj, dict):
                obj = obj.get(part)
                if obj is None:
                    return None
            else:
                return None
        return obj

    def evaluate(self, repo: Dict) -> bool:
        """Evaluate the condition against a repository."""
        repo_value = self.get_nested_value(repo, self.field)

        # Handle missing fields
        if repo_value is None:
            if self.operator == 'neq':
                return True  # None != any value except None
            elif self.operator == 'eq':
                return self.value is None
            else:
                return False  # For other operators, treat missing as False

        try:
            return evaluate_condition(repo_value, self.operator, self.value)
        except TypeError as e:
            raise FilterError(f"Type error in condition: {e}")

class LogicalOperation:
    """Represents a logical operation (AND/OR/NOT) in the filter query."""

    logical_operators = {'and', 'or', 'not'}

    def __init__(self, operator: str, operands: List[Any]):
        if not isinstance(operator, str):
            raise FilterError(f"Invalid logical operator type: {type(operator)}")
        operator_lower = operator.lower()
        if operator_lower not in self.logical_operators:
            raise FilterError(f"Unsupported logical operator: {operator_lower}")
        self.operator = operator_lower
        self.operands = operands

        # Validate operand count for 'not' operator
        if self.operator == 'not' and len(self.operands) != 1:
            raise FilterError(f"'not' operator requires exactly one operand, got {len(self.operands)}")
        if self.operator in {'and', 'or'} and len(self.operands) < 1:
            raise FilterError(f"'{self.operator}' operator requires at least one operand, got {len(self.operands)}")

    def evaluate(self, repo: Dict) -> bool:
        """Evaluate the logical operation against a repository."""
        if self.operator == 'and':
            return all(operand.evaluate(repo) for operand in self.operands)
        elif self.operator == 'or':
            return any(operand.evaluate(repo) for operand in self.operands)
        elif self.operator == 'not':
            return not self.operands[0].evaluate(repo)
        else:
            raise FilterError(f"Unsupported logical operator: {self.operator}")

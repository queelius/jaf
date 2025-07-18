"""
JAF Custom Exception Classes
============================

This module defines custom exceptions for better error handling and debugging.
Exceptions are organized into categories based on when they occur and how they
should be handled.
"""


class JAFError(Exception):
    """Base exception for all JAF errors"""

    pass


# Query/Syntax Errors (should fail fast during evaluation)
class QueryError(JAFError):
    """Base for all query-related errors that indicate invalid queries"""

    pass


class UnknownOperatorError(QueryError):
    """Raised when query contains unknown operator"""

    def __init__(self, operator):
        super().__init__(f"Unknown operator: {operator}")
        self.operator = operator


class InvalidArgumentCountError(QueryError):
    """Raised when operator gets wrong number of arguments"""

    def __init__(self, operator, expected, got):
        if expected == -1:
            # Variadic operator
            super().__init__(f"'{operator}' expects variable arguments, got {got}")
        else:
            super().__init__(f"'{operator}' expects {expected} arguments, got {got}")
        self.operator = operator
        self.expected = expected
        self.got = got


class InvalidQueryFormatError(QueryError):
    """Raised when query has invalid format"""

    pass


# Path Errors
class PathError(JAFError):
    """Base for path-related errors"""

    pass


class PathSyntaxError(PathError):
    """Raised when path has syntax errors"""

    def __init__(self, message, path_segment=None):
        super().__init__(message)
        self.path_segment = path_segment


class UnknownPathOperationError(PathError):
    """Raised when path contains unknown operation"""

    def __init__(self, operation):
        super().__init__(f"Unknown path operation: {operation}")
        self.operation = operation


# Evaluation Errors (might be item-specific, safe to skip)
class EvaluationError(JAFError):
    """Base for errors during evaluation that might be item-specific"""

    pass


class PathNotFoundError(EvaluationError):
    """Raised when path doesn't exist in data (like KeyError)"""

    pass


class TypeMismatchError(EvaluationError):
    """Raised when types don't match expected (like TypeError)"""

    pass


class IndexOutOfBoundsError(EvaluationError):
    """Raised when array index is out of bounds"""

    pass

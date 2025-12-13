import datetime
import re
import rapidfuzz
import logging
import functools
import math
from .path_evaluation import exists, eval_path
from .path_types import PathValues, MISSING_PATH
from .utils import adapt_jaf_operator
from .path_conversion import string_to_path_ast
from .exceptions import (
    UnknownOperatorError,
    InvalidArgumentCountError,
    InvalidQueryFormatError,
    PathSyntaxError,
    UnknownPathOperationError,
)

# Set up the logger
logger = logging.getLogger(__name__)


class jafError(Exception):
    pass


def _jaf_subtract(*args, obj):
    if not args:
        return 0
    if len(args) == 1:
        return -args[0]
    return functools.reduce(lambda x, y: x - y, args)


def _jaf_divide(*args, obj):
    if not args:
        raise ValueError("Division operator requires at least one argument.")
    if len(args) == 1:
        return 1 / args[0]
    try:
        return functools.reduce(lambda x, y: x / y, args)
    except ZeroDivisionError:
        raise ValueError("Division by zero.")


def _try_to_number(x):
    """Try to convert value to number, return None if not possible"""
    if isinstance(x, (int, float)):
        return x
    if isinstance(x, str):
        try:
            if "." in x:
                return float(x)
            else:
                return int(x)
        except ValueError:
            return None
    return None


class jaf_eval:
    """
    JSON Abstract Filter (JAF) evaluator class. This only applies to a single
    object in the array.
    """

    # Special forms don't have their arguments evaluated before the operator is called.
    # This allows for custom logic like short-circuiting (and, or) or preventing evaluation (literal).
    special_forms = {"if", "and", "or", "not", "exists?", "is-empty?", "@", "self", "literal"}

    # Regular functions that evaluate all arguments first
    funcs = {
        # predicates with strict type checking
        "eq?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 == x2),
        "=": adapt_jaf_operator(3, lambda x1, x2, obj: x1 == x2),
        "neq?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 != x2),
        "!=": adapt_jaf_operator(3, lambda x1, x2, obj: x1 != x2),
        "gt?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 > x2),
        ">": adapt_jaf_operator(3, lambda x1, x2, obj: x1 > x2),
        "gte?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 >= x2),
        ">=": adapt_jaf_operator(3, lambda x1, x2, obj: x1 >= x2),
        "lt?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 < x2),
        "<": adapt_jaf_operator(3, lambda x1, x2, obj: x1 < x2),
        "lte?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 <= x2),
        "<=": adapt_jaf_operator(3, lambda x1, x2, obj: x1 <= x2),
        "in?": adapt_jaf_operator(3, lambda x1, x2, obj: x1 in x2),
        "contains?": adapt_jaf_operator(
            3, lambda container, item, obj: item in container
        ),
        "starts-with?": adapt_jaf_operator(
            3, lambda value, prefix, obj: value.startswith(prefix)
        ),
        "ends-with?": adapt_jaf_operator(
            3, lambda value, suffix, obj: value.endswith(suffix)
        ),
        # string matching
        "regex-match?": adapt_jaf_operator(
            3, lambda value, pattern, obj: re.match(pattern, value) is not None
        ),
        "close-match?": adapt_jaf_operator(
            3, lambda x1, x2, obj: rapidfuzz.fuzz.ratio(x1, x2) > 80
        ),
        "partial-match?": adapt_jaf_operator(
            3, lambda x1, x2, obj: rapidfuzz.fuzz.partial_ratio(x1, x2) > 80
        ),
        # --- Type Predicates ---
        "is-string?": adapt_jaf_operator(2, lambda x, obj: isinstance(x, str)),
        "is-number?": adapt_jaf_operator(2, lambda x, obj: isinstance(x, (int, float))),
        "is-array?": adapt_jaf_operator(2, lambda x, obj: isinstance(x, list)),
        "is-object?": adapt_jaf_operator(2, lambda x, obj: isinstance(x, dict)),
        "is-null?": adapt_jaf_operator(2, lambda x, obj: x is None),
        "is-empty?": adapt_jaf_operator(
            2, lambda x, obj: x is None or (hasattr(x, "__len__") and len(x) == 0)
        ),
        # value extractors
        "length": adapt_jaf_operator(
            2, lambda x, obj: len(x) if hasattr(x, "__len__") else None
        ),
        "type": adapt_jaf_operator(2, lambda x, obj: type(x).__name__),
        "keys": adapt_jaf_operator(2, lambda d, obj: list(d.keys())),
        "values": adapt_jaf_operator(2, lambda x, obj: list(x.values())),
        "first": adapt_jaf_operator(2, lambda l, obj: l[0] if l else None),
        "last": adapt_jaf_operator(2, lambda l, obj: l[-1] if l else None),
        "get": adapt_jaf_operator(
            3,
            lambda l, i, obj: (
                l[i]
                if isinstance(l, list) and isinstance(i, int) and -len(l) <= i < len(l)
                else None
            ),
        ),
        "items": adapt_jaf_operator(
            2, lambda x, obj: list(x.items()) if isinstance(x, dict) else []
        ),
        "unique": adapt_jaf_operator(2, lambda l, obj: list(dict.fromkeys(l))),
        # Type coercion operators
        "to-string": adapt_jaf_operator(
            2, lambda x, obj: str(x) if x is not None else ""
        ),
        "to-number": adapt_jaf_operator(2, lambda x, obj: _try_to_number(x)),
        "to-boolean": adapt_jaf_operator(
            2, lambda x, obj: bool(x) if x is not None else False
        ),
        "to-list": adapt_jaf_operator(
            2,
            lambda x, obj: (
                list(x)
                if hasattr(x, "__iter__") and not isinstance(x, (str, dict))
                else [x]
            ),
        ),
        # datetime functions
        "now": adapt_jaf_operator(1, lambda obj: datetime.datetime.now()),
        "date": adapt_jaf_operator(
            2, lambda x, obj: datetime.datetime.strptime(x, "%Y-%m-%d")
        ),
        "datetime": adapt_jaf_operator(
            2, lambda x, obj: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        ),
        "date-diff": adapt_jaf_operator(3, lambda date1, date2, obj: date1 - date2),
        "days": adapt_jaf_operator(2, lambda datediff, obj: datediff.days),
        "seconds": adapt_jaf_operator(2, lambda datediff, obj: datediff.seconds),
        # string functions
        "lower-case": adapt_jaf_operator(2, lambda s, obj: s.lower()),
        "upper-case": adapt_jaf_operator(2, lambda s, obj: s.upper()),
        "trim": adapt_jaf_operator(
            2, lambda s, obj: s.strip() if isinstance(s, str) else s
        ),
        "split": adapt_jaf_operator(3, lambda s, delim, obj: s.split(delim)),
        "join": adapt_jaf_operator(3, lambda l, delim, obj: delim.join(l)),
        # Arithmetic Operators
        "+": adapt_jaf_operator(
            -1, lambda *args, obj: functools.reduce(lambda x, y: x + y, args, 0)
        ),
        "-": adapt_jaf_operator(-1, _jaf_subtract),
        "*": adapt_jaf_operator(
            -1, lambda *args, obj: functools.reduce(lambda x, y: x * y, args, 1)
        ),
        "/": adapt_jaf_operator(-1, _jaf_divide),
        "%": adapt_jaf_operator(3, lambda x, y, obj: x % y),
        # Mathematical Functions
        "abs": adapt_jaf_operator(2, lambda x, obj: abs(x)),
        "round": adapt_jaf_operator(3, lambda x, digits, obj: round(x, digits)),
        "floor": adapt_jaf_operator(2, lambda x, obj: math.floor(x)),
        "ceil": adapt_jaf_operator(2, lambda x, obj: math.ceil(x)),
        "max": adapt_jaf_operator(-1, lambda *args, obj: max(args) if args else None),
        "min": adapt_jaf_operator(-1, lambda *args, obj: min(args) if args else None),
        # Aggregation operators (operate on arrays)
        "sum": adapt_jaf_operator(2, lambda arr, obj: sum(arr) if isinstance(arr, list) else arr),
        "mean": adapt_jaf_operator(2, lambda arr, obj: sum(arr) / len(arr) if isinstance(arr, list) and len(arr) > 0 else None),
        "count": adapt_jaf_operator(2, lambda arr, obj: len(arr) if hasattr(arr, '__len__') else 1),
        # Object construction
        "dict": adapt_jaf_operator(
            -1, lambda *args, obj: dict(zip(args[::2], args[1::2]))
        ),
    }

    @staticmethod
    def eval(query, obj):
        """
        Evaluates the query against the provided object.

        :param query: The query AST as a list.
        :param obj: The dictionary object to evaluate.
        :return: Result of the evaluation.
        """

        # Handle @ prefix strings - convert to path operation
        if isinstance(query, str) and query.startswith("@"):
            path_string = query[1:]  # Remove @
            if not path_string:
                raise PathSyntaxError("Empty path expression after @", path_segment="@")
            path_ast = string_to_path_ast(path_string)
            logger.debug(f"Converted @{path_string} to path AST: {path_ast}")
            result = eval_path(path_ast, obj)
            # Convert MISSING_PATH to [] for backwards compatibility
            if result is MISSING_PATH:
                return []
            return result

        # Handle non-list values (literals)
        if not isinstance(query, list):
            return query

        if not query:
            raise InvalidQueryFormatError("Query cannot be empty")

        # Get the operator (first element)
        op = query[0]
        args = query[1:]

        logger.debug(f"Evaluating operator: '{op}' with args: {args}")

        # Handle special forms first
        if op in jaf_eval.special_forms:
            return jaf_eval._eval_special_form(op, args, obj)

        # Handle regular functions
        if op in jaf_eval.funcs:
            return jaf_eval._eval_function(op, args, obj)

        raise UnknownOperatorError(op)

    @staticmethod
    def _eval_special_form(op, args, obj):
        """Handle special forms that need custom evaluation logic"""
        if op == "self":
            if args:
                raise InvalidArgumentCountError("self", 0, len(args))
            return obj

        elif op == "literal":
            if len(args) != 1:
                raise InvalidArgumentCountError("literal", 1, len(args))
            return args[0]  # Return the argument unevaluated

        elif op == "@":
            if len(args) != 1:
                raise InvalidArgumentCountError(op, 1, len(args))
            path_expr = args[0]

            if isinstance(path_expr, str):
                path_expr_ast = string_to_path_ast(path_expr)
                if not path_expr_ast:
                    raise PathSyntaxError("Invalid path expression: empty or malformed")
                path_expr = path_expr_ast

            if not isinstance(path_expr, list):
                raise InvalidQueryFormatError(
                    "Path argument must be a list of path components"
                )

            # Validate each component of the path expression
            known_path_ops = {
                "key",
                "index",
                "indices",
                "slice",
                "regex_key",
                "wc_level",
                "wc_recursive",
            }
            for component in path_expr:
                if not isinstance(component, list):
                    raise InvalidQueryFormatError("Path component must be a list")
                if not component:
                    raise InvalidQueryFormatError("Path component cannot be empty")
                if not isinstance(component[0], str):
                    raise InvalidQueryFormatError(
                        "Path component operation must be a string"
                    )
                if component[0] not in known_path_ops:
                    raise UnknownPathOperationError(component[0])

            # TODO: BUGFIX - Need to properly distinguish between empty arrays and non-existent paths
            # Currently eval_path returns [] for both cases, which makes exists? return False
            # for keys that exist with empty array values. This needs a deeper fix in the
            # path evaluation system. For now, keeping original behavior to avoid breaking tests.
            # See: https://github.com/anthropics/jaf/issues/XXX

            res = eval_path(path_expr, obj)
            
            # Check if path doesn't exist
            if res is MISSING_PATH:
                return []  # Return empty list for non-existent paths

            # For simple paths (no wildcards), return single value
            # Check if path contains wildcards
            has_wildcards = any(
                component[0] in ["wc_level", "wc_recursive", "regex_key", "fuzzy_key"]
                for component in path_expr
                if isinstance(component, list) and len(component) > 0
            )

            if not has_wildcards:
                # For simple paths, return the single value
                if isinstance(res, list):
                    if len(res) == 0:
                        return []  # Empty array is a valid value
                    elif len(res) == 1:
                        return res[0]
                    else:
                        return res  # Multiple values, return as list
                else:
                    # eval_path returned the value directly
                    return res
            else:
                # Path with wildcards - return full list
                return res

        elif op == "is-empty?":
            if len(args) != 1:
                raise InvalidArgumentCountError("is-empty?", 1, len(args))
            
            # Special handling for path expressions to check existence first
            arg = args[0]
            if isinstance(arg, str) and arg.startswith("@"):
                # Convert @ prefixed strings to path expressions
                path_string = arg[1:]
                path_ast = string_to_path_ast(path_string)
                path_expr = ["@", path_ast]
                
                # First check if path exists
                if not exists(path_ast, obj):
                    return False  # Non-existent paths are not considered empty
                
                # Path exists, now check if it's empty
                value = eval_path(path_ast, obj)
                return value is None or (hasattr(value, "__len__") and len(value) == 0)
            else:
                # Not a path expression, evaluate normally
                value = jaf_eval.eval(arg, obj)
                return value is None or (hasattr(value, "__len__") and len(value) == 0)
        
        elif op == "exists?":
            if len(args) != 1:
                raise InvalidArgumentCountError("exists?", 1, len(args))

            if isinstance(args[0], str) and args[0].startswith("@"):
                # Convert @ prefixed strings to path expressions
                path_string = args[0][1:]
                args[0] = string_to_path_ast(path_string)
                args[0] = ["@", args[0]] if isinstance(args[0], list) else args[0]

            # The argument should be a path expression like ["path", ["user", "email"]]
            arg = args[0]
            if (
                isinstance(arg, list)
                and len(arg) == 2
                and (arg[0] == "path" or arg[0] == "@")
            ):
                # Extract the path components directly
                path_components = arg[1]

                if isinstance(path_components, str):
                    # Convert string path to AST
                    path_components = string_to_path_ast(path_components)
                    if not path_components:
                        raise PathSyntaxError(
                            "Invalid path expression: empty or malformed"
                        )
                if not isinstance(path_components, list):
                    raise InvalidQueryFormatError(
                        "Path argument must be a list of path components"
                    )
                return exists(path_components, obj)
            else:
                raise InvalidQueryFormatError(
                    "exists? argument must be a path expression"
                )

        elif op == "if":
            if len(args) != 3:
                raise InvalidArgumentCountError("if", 3, len(args))
            cond_expr, true_expr, false_expr = args

            # Evaluate condition
            cond_result = jaf_eval.eval(cond_expr, obj)

            # Return appropriate branch without evaluating the other
            if cond_result:
                return jaf_eval.eval(true_expr, obj)
            else:
                return jaf_eval.eval(false_expr, obj)

        elif op == "and":
            # Short-circuit evaluation - stop at first falsy value
            for arg in args:
                result = jaf_eval.eval(arg, obj)
                if not result:
                    return False
            return True

        elif op == "or":
            # Short-circuit evaluation - stop at first truthy value
            for arg in args:
                result = jaf_eval.eval(arg, obj)
                if result:
                    return True
            return False

        elif op == "not":
            if len(args) != 1:
                raise InvalidArgumentCountError("not", 1, len(args))
            result = jaf_eval.eval(args[0], obj)
            return not result

        else:
            raise UnknownOperatorError(op)

    @staticmethod
    def _eval_function(op, args, obj):
        """Handle regular functions that evaluate all arguments first"""

        func, nargs = jaf_eval.funcs[op]

        # Check argument count for non-variadic functions
        if nargs != -1 and len(args) != nargs - 1:
            raise InvalidArgumentCountError(op, nargs - 1, len(args))

        # Evaluate all arguments
        eval_args = []
        for arg in args:
            if isinstance(arg, str) and arg.startswith("@"):
                # Convert @ prefixed strings to path expressions
                path_string = arg[1:]
                arg = string_to_path_ast(path_string)
                arg = ["@", arg] if isinstance(arg, list) else arg

            if isinstance(arg, list):
                val = jaf_eval.eval(arg, obj)
                eval_args.append(val)
            else:
                eval_args.append(arg)

        # Call the function with type error handling for predicates
        try:
            result = func(*eval_args, obj=obj)
            logger.debug(f"Result of '{op}': {result}")
            return result
        except Exception as e:
            logger.debug(f"Error evaluating '{op}' with args {eval_args}: {e}")
            # For predicates (functions ending with '?'), return False on type errors
            if op.endswith("?"):
                return False
            else:
                # For non-predicates, re-raise the exception
                raise e

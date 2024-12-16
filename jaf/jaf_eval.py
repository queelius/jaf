import datetime
import re
import rapidfuzz
import statistics
import logging
from .utils import _exists, _path_value, _wrap_2, _wrap_3, _wrap_4

# Set up the logger
logger = logging.getLogger(__name__)

class jafError(Exception):
    pass

class jaf_eval:
    """
    JSON Abstract Filter (JAF) evaluator class. This only applies to a single
    object in the array.
    """
    
    funcs = {
        # predicates
        'eq?': (_wrap_3(lambda x1, x2, obj: x1 == x2), 3),
        'neq?': (_wrap_3(lambda x1, x2, obj: x1 != x2), 3),
        'gt?': (_wrap_3(lambda x1, x2, obj: x1 > x2), 3),
        'gte?': (_wrap_3(lambda x1, x2, obj: x1 >= x2), 3),
        'lt?': (_wrap_3(lambda x1, x2, obj: x1 < x2), 3),
        'lte?': (_wrap_3(lambda x1, x2, obj: x1 <= x2), 3),
        'in?': (_wrap_3(lambda x1, x2, obj: x1 in x2), 3),
        'exists?': (_wrap_2(lambda path, obj: _exists(path, obj)), 2),
        'starts-with?': (_wrap_3(lambda start, value, obj: value.startswith(start)), 3),
        'ends-with?': (_wrap_3(lambda end, value, obj: value.endswith(end)), 3),

        # string matching
        'regex-match?': (_wrap_3(lambda pattern, value, obj: re.match(pattern, value) is not None), 3),
        'close-match?': (_wrap_3(lambda x1, x2, obj: rapidfuzz.fuzz.ratio(x1, x2) > 80), 3),
        'partial-match?': (_wrap_3(lambda x1, x2, obj: rapidfuzz.fuzz.partial_ratio(x1, x2) > 80), 3),

        # logical operators
        'and': (lambda *args, obj: all(args), -1),
        'or': (lambda *args, obj: any(args), -1),
        'not': (_wrap_2(lambda x, obj: not x), 2),

        # functions
        'path': (_wrap_2(lambda path, obj: _path_value(path, obj)), 2),
        'if': (_wrap_4(lambda cond, true_cond, false_cond, obj: [true_cond] if cond else [false_cond]), 4),

        # datetime functions
        'now': (lambda obj: datetime.datetime.now(), 1),
        'date': (_wrap_2(lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d')), 2),
        'datetime': (_wrap_3(lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')), 2),
        'date-diff': (_wrap_3(lambda date1, date2, obj: date1 - date2), 3),
        'years': (_wrap_2(lambda datediff, obj: datediff.days / 365.25), 2),
        'seconds': (_wrap_2(lambda datediff, obj: datediff.seconds), 2),

        # string functions
        'lower-case': (_wrap_2(lambda s, obj: s.lower()), 2),

        # higher-order functions       
        'map': (_wrap_3(lambda lst, func, obj: [func(item, obj) for item in lst]), 3),
        'filter': (_wrap_3(lambda lst, func, obj: [item for item in lst if func(item, obj)]), 3),
        'nth': (_wrap_3(lambda lst, n, obj: lst[n]), 3),


        # math functions
        'sum': (_wrap_2(lambda lst, obj: sum(lst)), 2),
        'max': (_wrap_2(lambda lst, obj: max(lst)), 2),
        'min': (_wrap_2(lambda lst, obj: min(lst)), 2),
        'abs': (_wrap_2(lambda x, obj: abs(x)), 2),
        'round': (_wrap_3(lambda x, ndigits, obj: round(x, ndigits)), 3),

        # statistical functions
        'stddev': (_wrap_3(lambda lst, obj: statistics.stdev(lst)), 3),
        'mean': (_wrap_3(lambda lst, obj: statistics.mean(lst)), 3),
        'median': (_wrap_3(lambda lst, obj: statistics.median(lst)), 3),
    }

    @staticmethod
    def eval(query, obj):
        """
        Evaluates the query against the provided object.

        :param query: The query AST as a list.
        :param obj: The dictionary object to evaluate.
        :return: Result of the evaluation.
        """
        if not isinstance(query, list) or not query:
            raise ValueError("Invalid query format.")

        op_query = query[0]
        if isinstance(op_query, list):
            logger.debug(f"Operator is a sub-query: {op_query}")
            op = jaf_eval.eval(op_query, obj)
            logger.debug(f"Operator query: {op}: {type(op)}")

            if not isinstance(op, (str, list)) or not op or (isinstance(op, list) and len(op) != 1):
                logger.debug(f"Operator query is invalid (does not resolve to a string): {op}")
                return f"{op_query} not found"
            
            if isinstance(op, str):
                logger.debug(f"Operator query is valid (resolves to a string): {op}")
                op = op
            else:
                op = op[0]

        else:
            op = op_query
            logger.debug(f"Operator is self-evaluating: {op}")

        if op not in jaf_eval.funcs:
            raise ValueError(f"Unknown operator: {op}")

        logger.debug(f"Evaluating operator: '{op}'")

        func, nargs = jaf_eval.funcs[op]
        logger.debug(f"Function signature: {func.__code__.co_varnames}, nargs: {nargs}")

        args = query[1:]
        logger.debug(f"Unevaluated args: {args}")

        if nargs != -1 and len(args) != nargs-1:
            logger.error(f"'{op}' expects {nargs} args, got {len(args)}.")
            raise ValueError(f"'{op}' expects {nargs} args, got {len(args)}.")

        eval_args = []
        logger.debug(f"Evaluating args {args}.")
        for arg in args:
            if isinstance(arg, list):
                logger.debug(f"Arg {arg} is a sub-query.")
                val = jaf_eval.eval(arg, obj)
                logger.debug(f"Evaluated arg: {val}")
                eval_args.append(val)
            else:
                logger.debug(f"Arg {arg} is self-evaluating.")
                eval_args.append(arg)

        try:
            result = func(*eval_args, obj=obj)
            logger.debug(f"Result-of '{op}': {result}")
        except Exception as e:
            logger.error(f"Error evaluating '{op}' with args {args}: {e}")
            raise e

        return result

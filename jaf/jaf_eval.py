import datetime
import re
import rapidfuzz
import statistics
import logging
from .utils import _exists, _path_value, wrap

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
        'eq?': wrap(3, lambda x1, x2, obj: x1 == x2),
        'neq?': wrap(3, lambda x1, x2, obj: x1 != x2),
        'gt?': wrap(3, lambda x1, x2, obj: x1 > x2),
        'gte?': wrap(3, lambda x1, x2, obj: x1 >= x2),
        'lt?': wrap(3, lambda x1, x2, obj: x1 < x2),
        'lte?': wrap(3, lambda x1, x2, obj: x1 <= x2),
        'in?': wrap(3, lambda x1, x2, obj: x1 in x2),
        'exists?': wrap(2, lambda path, obj: _exists(path, obj)),
        'starts-with?': wrap(3, lambda start, value, obj: value.startswith(start)),
        'ends-with?': wrap(3, lambda end, value, obj: value.endswith(end)),

        # string matching
        'regex-match?': wrap(3, lambda pattern, value, obj: re.match(pattern, value) is not None),
        'close-match?': wrap(3, lambda x1, x2, obj: rapidfuzz.fuzz.ratio(x1, x2) > 80),
        'partial-match?': wrap(3, lambda x1, x2, obj: rapidfuzz.fuzz.partial_ratio(x1, x2) > 80),

        # logical operators
        'and': (lambda *args, obj: all(args), -1),
        'or': (lambda *args, obj: any(args), -1),
        'not': wrap(2, lambda x, obj: not x),

        # functions
        'path': (lambda path, obj: _path_value(path, obj), 2),
        'if': (lambda cond, true_cond, false_cond, obj: true_cond if cond else false_cond),
        'length': (lambda x, obj: len(x), 2),
        'type': (lambda x, obj: type(x).__name__, 2),
        'keys': (lambda x, obj: list(x.keys()), 2),
        'sort': (lambda x, obj: sorted(x), 2),
        'reverse': (lambda x, obj: [x[::-1]], 2),
        'flatten': (flatten, 2),
        'unique': (lambda x, obj: list(set(x)), 2),
        'slice': (lambda x, start, end, obj: x[start:end], 4),
        'index': (lambda x, i, obj: x[i], 3),
        'list': (lambda *args, obj: list(args), -1),

        # datetime functions
        'now': (lambda obj: datetime.datetime.now(), 1),
        'date': wrap(2, lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d')),
        'datetime': wrap(3, lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')),
        'date-diff': wrap(3, lambda date1, date2, obj: date1 - date2),
        'years': wrap(2, lambda datediff, obj: datediff.days / 365.25),
        'seconds': wrap(2, lambda datediff, obj: datediff.seconds),

        # string functions
        'lower-case': (lambda s, obj: s.lower(), 2),
        'upper-case': (lambda s, obj: s.upper(), 2),
        'title-case': (lambda s, obj: s.title(), 2),
        'concat': (lambda lst, obj: [''.join([str(x) for x in lst])] if isinstance(lst, list) else lst, 2),

        # higher-order functions       
        'map': (lambda lst, func, obj: [func(item, obj) for item in lst], 2),
        'filter': (lambda lst, func, obj: [item for item in lst if func(item, obj)], 2),
        'nth': (lambda lst, n, obj: lst[n], 2),


        # math functions
        'sum': (lambda lst, obj: sum(lst), 2),
        'max': (lambda lst, obj: max(lst), 2),
        'min': (lambda lst, obj: min(lst), 2),
        'abs': (lambda x, obj: abs(x), 2),
        'round': (lambda x, ndigits, obj: round(x, ndigits), 2),

        # statistical functions
        'stddev': (lambda lst, obj: statistics.stdev(lst), 2),
        'mean': (lambda lst, obj: statistics.mean(lst), 2),
        'median': (lambda lst, obj: statistics.median(lst), 2),
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
            logger.debug(f"Operator {op_query} is a sub-query")
            op = jaf_eval.eval(op_query, obj)
            logger.debug(f"Operator query: {op}: {type(op)}")

            if not isinstance(op, (str, list)) or not op or (isinstance(op, list) and len(op) != 1):
                logger.debug(f"Operator query {op} is invalid (does not resolve to a string)")
                return f"{op_query} not found"
            
            if isinstance(op, str):
                logger.debug(f"Operator query {op} is valid")
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

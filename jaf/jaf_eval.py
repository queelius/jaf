import datetime
import re
import rapidfuzz
import statistics
import logging
from .utils import exists, path_values, wrap, flatten

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
        'exists?': wrap(2, lambda path, obj: exists(path, obj)),
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
        'path': wrap(2, path_values),
        'if': wrap(4, lambda cond, true_cond, false_cond, obj: true_cond if cond else false_cond),
        'length': wrap(2, lambda x, obj: len(x)),
        'type': wrap(2, lambda x, obj: type(x).__name__),
        'keys': wrap(2, lambda x, obj: list(x.keys())),
        'sort': wrap(2, lambda x, obj: sorted(x)),
        'reverse': wrap(2, lambda x, obj: [x[::-1]]),
        'flatten': wrap(flatten, 2),
        'unique': wrap(2, lambda x, obj: list(set(x))),
        'slice': wrap(4, lambda x, start, end, obj: x[start:end]),
        'index': wrap(3, lambda x, i, obj: x[i]),
        'list': wrap(-1, lambda *args, obj: list(args)),

        # datetime functions
        'now': wrap(1, lambda obj: datetime.datetime.now()),
        'date': wrap(2, lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d')),
        'datetime': wrap(3, lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')),
        'date-diff': wrap(3, lambda date1, date2, obj: date1 - date2),
        'days': wrap(2, lambda datediff, obj: datediff.days),
        'seconds': wrap(2, lambda datediff, obj: datediff.seconds),

        # string functions
        'lower-case': wrap(2, lambda s, obj: s.lower()),
        'upper-case': wrap(2, lambda s, obj: s.upper()),
        'concat': wrap(2, lambda lst, obj: [''.join([str(x) for x in lst])] if isinstance(lst, list) else lst),

        # higher-order functions       
        'map': wrap(2, lambda lst, func, obj: [func(item, obj) for item in lst]),
        'filter': wrap(2, lambda lst, func, obj: [item for item in lst if func(item, obj)]),
        'nth': wrap(2, lambda lst, n, obj: lst[n]),


        # math functions
        'sum': wrap(2, lambda lst, obj: sum(lst)),
        'max': wrap(2, lambda lst, obj: max(lst)),
        'min': wrap(2, lambda lst, obj: min(lst)),
        'abs': wrap(2, lambda x, obj: abs(x)),
        'round': wrap(2, lambda x, ndigits, obj: round(x, ndigits)),
        'stddev': wrap(2, lambda lst, obj: statistics.stdev(lst)),
        'mean': wrap(2, lambda lst, obj: statistics.mean(lst)),
        'median': wrap(2, lambda lst, obj: statistics.median(lst))
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

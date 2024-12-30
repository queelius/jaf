# import datetime
# import re
# import rapidfuzz
# import statistics
# import logging
# from functools import reduce
# from .utils import wrap, flatten
# from .path import path_values, has_path

# # Set up the logger
# logger = logging.getLogger(__name__)

# class jafError(Exception):
#     pass

# class jaf_eval:
#     """
#     JSON Abstract Filter (JAF) evaluator class. This only applies to a single
#     object in the array.
#     """

#     #def _path_values(path, obj):
#     #    results = path_values(path, obj)
#     #    vals = [result.value for result in results]
#     #    paths = [result.path for result in results]
#     #    logger.debug(f"Path values for {path}: {vals}, {paths}")
#     #    return vals

#     funcs = {
#         # predicates
#         'eq?': wrap(lambda x1, x2, obj: x1 == x2, "eq?"),
#         '==': wrap( lambda x1, x2, obj: x1 == x2, "=="),
#         'neq?': wrap(lambda x1, x2, obj: x1 != x2, "neq?"),
#         '!=': wrap(lambda x1, x2, obj: x1 != x2, "!="),
#         'gt?': wrap(lambda x1, x2, obj: x1 > x2, "gt?"),
#         '>': wrap(lambda x1, x2, obj: x1 > x2, ">"),
#         'gte?': wrap(lambda x1, x2, obj: x1 >= x2, "gte?"),
#         '>=': wrap(lambda x1, x2, obj: x1 >= x2, ">="),
#         'lt?': wrap(lambda x1, x2, obj: x1 < x2, "lt?"),
#         '<': wrap(lambda x1, x2, obj: x1 < x2, "<"),
#         'lte?': wrap(lambda x1, x2, obj: x1 <= x2, "lte?"),
#         '<=': wrap(lambda x1, x2, obj: x1 <= x2, "<="),
#         'in?': wrap(lambda x1, x2, obj: x1 in x2, "in?"),
#         'exists?': wrap(has_path, "exists?"),
#         'starts-with?': wrap(lambda start, value, obj: value.startswith(start), "starts-with?"),
#         'ends-with?': wrap(lambda end, value, obj: value.endswith(end), "ends-with?"),

#         # string matching
#         'regex-match?': wrap(lambda pattern, value, obj: re.match(pattern, value) is not None, "regex-match?"),
#         'close-match?': wrap(lambda x1, x2, obj: rapidfuzz.fuzz.ratio(x1, x2) > 80, "close-match?"),
#         'partial-match?': wrap(lambda x1, x2, obj: rapidfuzz.fuzz.partial_ratio(x1, x2) > 80, "partial-match?"),

#         # logical operators
#          'and': lambda *args, obj: all(args),
#          '&': lambda *args, obj: all(args),
#          'or': lambda *args, obj: any(args),
#          '|': lambda *args, obj: any(args),
#         'not': wrap(lambda x, obj: not x, "not"),
#         '!': wrap(lambda x, obj: not x, "!"),

#         # functions
#         'path': wrap(path_values, "path"),
#         'if': wrap(lambda cond, true_cond, false_cond, obj: true_cond if cond else false_cond, "if"),
#         'len': wrap(lambda x, obj: len(x), "len"),
#         'type': wrap(lambda x, obj: type(x).__name__, "type"),
#         'keys': wrap(lambda x, obj: list(x.keys()), "keys"),
#         'sort': wrap(lambda x, obj: sorted(x), "sort"),
#         'reverse': wrap(lambda x, obj: list(reversed(x)), "reverse"),
#         'flatten': wrap(flatten, "flatten"),
#         'unique': wrap(lambda x, obj: list(set(x)), "unique"),
#         'slice': wrap(lambda x, start, end, obj: x[start:end], "slice"),
#         'index': wrap(lambda x, i, obj: x[i], "index"),
#         'list': wrap(lambda *args, obj: list(args), "list"),

#         # datetime functions
#         'now': wrap(lambda obj: datetime.datetime.now(), "now"),
#         'date': wrap(lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d'), "date"),
#         'datetime': wrap(lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), "datetime"),
#         'date-diff': wrap(lambda date1, date2, obj: date1 - date2, "date-diff"),
#         'days': wrap(lambda datediff, obj: datediff.days, "days"),
#         'seconds': wrap(lambda datediff, obj: datediff.seconds, "seconds"),

#         # string functions
#         'lower-case': wrap(lambda s, obj: s.lower(), "lower-case"),
#         'upper-case': wrap(lambda s, obj: s.upper(), "upper-case"),
#         'concat': wrap(lambda lst, obj: ''.join([str(x) for x in lst]), "concat"),

#         # higher-order functions 
#         'map': wrap(lambda lst, func, obj: [func(item, obj) for item in lst], "map"),
#         'filter': wrap(lambda lst, func, obj: [item for item in lst if func(item, obj)], "filter"),
#         'nth': wrap(lambda lst, n, obj: lst[n], "nth"),

#         # math functions
#         'sum': wrap(lambda lst, obj: sum(lst), "sum"),
#         'product': wrap(lambda lst, obj: reduce(lambda x, y: x * y, lst), "product"),
#         '*': wrap(lambda x, y, obj: x * y, "*"),        
#         '/': wrap(lambda x, y, obj: x / y, "/"),
#         '-': wrap(lambda x, y, obj: x - y, "-"),
#         '+': wrap(lambda x, y, obj: x + y, "+"),
#         'max': wrap(lambda lst, obj: max(lst), "max"),
#         'min': wrap(lambda lst, obj: min(lst), "min"),
#         'abs': wrap(lambda x, obj: abs(x), "abs"),
#         'round': wrap(lambda x, obj: round(x), "round"),
#         'round_n': wrap(lambda x, ndigits, obj: round(x, ndigits), "round_n"),
#         'sd': wrap(lambda lst, obj: statistics.stdev(lst), "sd"),
#         'mean': wrap(lambda lst, obj: statistics.mean(lst), "mean"),
#         'median': wrap(lambda lst, obj: statistics.median(lst), "median"),
#         'mode': wrap(lambda lst, obj: statistics.mode(lst), "mode"),
#         'variance': wrap(lambda lst, obj: statistics.variance(lst), "variance"),
#         'quantile': wrap(lambda lst, p, obj: statistics.quantile(lst, p), "quantile"),
#         'percentile': wrap(lambda lst, p, obj: statistics.percentile(lst, p), "percentile"),
#     }

#     @staticmethod
#     def eval(query, obj):
#         """
#         Evaluates the query against the provided object.

#         :param query: The query AST as a list.
#         :param obj: The dictionary object to evaluate.
#         :return: Result of the evaluation.
#         """
#         if not isinstance(query, list) or not query:
#             raise ValueError("Invalid query format.")

#         op_query = query[0]
#         if isinstance(op_query, list):
#             logger.debug(f"Operator {op_query} is a sub-query")
#             op = jaf_eval.eval(op_query, obj)
#             logger.debug(f"Operator query: {op}: {type(op)}")

#             if not isinstance(op, (str, list)) or not op or (isinstance(op, list) and len(op) != 1):
#                 logger.debug(f"Operator query {op} is invalid (does not resolve to a string)")
#                 return f"{op_query} not found"
            
#             if isinstance(op, str):
#                 logger.debug(f"Operator query {op} is valid")
#                 op = op
#             else:
#                 op = op[0]

#         else:
#             op = op_query
#             logger.debug(f"Operator is self-evaluating: {op}")

#         if op not in jaf_eval.funcs:
#             raise ValueError(f"Unknown operator: {op}")

#         logger.debug(f"Evaluating operator: '{op}'")

#         func = jaf_eval.funcs[op]
#         args = query[1:]
#         logger.debug(f"Unevaluated args: {args}")

#         eval_args = []
#         logger.debug(f"Evaluating args {args}.")
#         for arg in args:
#             if isinstance(arg, list):
#                 logger.debug(f"Arg {arg} is a sub-query.")
#                 val = jaf_eval.eval(arg, obj)
#                 logger.debug(f"Evaluated arg: {val}")
#                 eval_args.append(val)
#             else:
#                 logger.debug(f"Arg {arg} is self-evaluating.")
#                 eval_args.append(arg)

#         logger.debug(f"Evaluated args: {eval_args}")

#         try:
#             result = func(*eval_args, obj=obj)
#             logger.debug(f"Result '{op}': {result}")
#         except Exception as e:
#             logger.error(f"Error evaluating '{op}' with args {args}: {e}. Skipping...")

#         return result
    

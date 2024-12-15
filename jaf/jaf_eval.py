import datetime
import re
import rapidfuzz
import functools
import math

def get_path_value(path, obj):
    """
    Retrieves a value from the nested dictionary using dot notation.

    :param obj: The dictionary object.
    :param path: The dot-separated path string.
    :return: The retrieved value.
    """
    parts = path.split('.')
    cur = obj
    for part in parts:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

def path_exists(path, obj):
    """
    Checks if the path exists in the object.

    :param obj: The dictionary object.
    :param path: The dot-separated path string.
    :return: Boolean indicating if the path exists.
    """
    return get_path_value(path, obj) is not None

class jaf_eval:
    """
    JSON Abstract Filter (JAF) evaluator class. This only applies to a single
    object in the array. See `jaf.py` for the class that applies to the entire
    array.
    """
    funcs = {
        # predicates
        'eq?': (lambda x1, x2, _: x1 == x2, 3),
        'neq?': (lambda args, obj: jaf_eval.eval_oper(args[0], obj) != jaf_eval.eval_oper(args[1], obj), 2),
        'gt?': (lambda x1, x2, _: x1 > x2, 3),
        'gte?': (lambda args, obj: jaf_eval.eval_oper(args[0], obj) >= jaf_eval.eval_oper(args[1], obj), 2),
        'lt?': (lambda x1, x2, _: x1 < x2, 3),
        'lte?': (lambda args, obj: jaf_eval.eval_oper(args[0], obj) <= jaf_eval.eval_oper(args[1], obj), 2),
        'in?': (lambda x1, x2, _: x1 in x2, 3),
        'empty?': (lambda args, _: not args, 2),
        'path-exists?': (lambda path, obj: path_exists(path, obj), 2),
        'starts-with?': (lambda start, value, _: value.startswith(start), 3),
        'ends-with?': (lambda end, value, _: value.endswith(end), 3),

        # string matching
        'regex-match?': (lambda pattern, value, _: re.match(pattern, value) is not None, 3),
        'close-match?': (lambda args, obj: rapidfuzz.fuzz.partial_ratio(args[0], args[1]) > 90, 2),
        'partial-match?': (lambda args, obj: rapidfuzz.fuzz.partial_ratio(args[0], args[1]) > 90, 2),

        # logical operators
        'and': (lambda *args, _: all(args), -1),
        'or': (lambda *args, _: any(args), -1),
        'not': (lambda x, _: not x, 2),

        # functions
        'path': (lambda path, obj: get_path_value(path, obj), 2),
        'if': (lambda cond, true, false, _: true if cond else false, 4),
        'cond': (lambda *args, _: next((result for cond, result in args if cond), None), -1),

        # list functions
        'head': (lambda *args, _: args[0], 1),
        'tail': (lambda *args, _: args[1:], 1),

        # type conversions
        'dict': (lambda *args, _: dict(args), -1),
        'list': (lambda *args, _: list(args), -1),
        'bool': (lambda x, _: bool(x), 2),
        'str': (lambda x, _: str(x), 2),
        'int': (lambda x, _: int(x), 2),

        # datetime functions
        'now': (lambda: datetime.datetime.now(), 1),
        'date': (lambda x, _: datetime.datetime.strptime(x, '%Y-%m-%d'), 2),
        'time': (lambda x, _: datetime.datetime.strptime(x, '%H:%M:%S'), 2),
        'datetime': (lambda x, _: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), 2),
        'timestamp': (lambda x, _: datetime.datetime.fromtimestamp(x), 2),
        'format-date': (lambda x, fmt, _: x.strftime(fmt), 3),
        'add-time': (lambda x, y, _: x + datetime.timedelta(seconds=y), 3),
        'sub-time': (lambda x, y, _: x - datetime.timedelta(seconds=y), 3),
        'diff-time': (lambda x, y, _: x - y, 3),

        # meta-programming functions
        'eval': (lambda expr, obj: jaf_eval.eval(expr, obj), 2),
        'apply': (lambda func, lst, obj: func(lst, obj), 3),
        'quote': (lambda x, _: x, 2),

        # dictionary functions
        'keys': (lambda obj: obj.keys(), 1),
        'values': (lambda obj: obj.values(), 1),
        'get-value': (lambda key, obj: obj[key], 2),
        'get-base-obj': (lambda obj: obj, 1),

        # list functions
        'sort': (lambda lst, _: sorted(lst), 2),
        'reverse': (lambda lst, _: lst[::-1], 2),
        'len': (lambda lst, _: len(lst), 2),
        'nth': (lambda lst, n, _: lst[n], 3),
        'merge': (lambda lst, _: {k: v for d in lst for k, v in d.items()}, 2),
        'slice': (lambda lst, start, end, _: lst[start:end], 4),

        # string functions
        'upper-case': (lambda s, _: s.upper(), 2),
        'lower-case': (lambda s, _: s.lower(), 2),
        'concat': (lambda lst, _: ''.join(lst), 2),

        # higher-order functions       
        'map': (lambda lst, func, obj: [func(item, obj) for item in lst], 3),
        'filter': (lambda lst, func, obj: [item for item in lst if func(item, obj)], 3),
        'reduce': (lambda lst, func, obj: functools.reduce(func, lst), 3),
        'gather': (lambda lst, func, obj: [item for item in lst if func(item, obj)], 3),
        'zip': (lambda *args, _: list(zip(*args)), -1),

        # math functions
        'sum': (lambda lst, _: sum(lst), 2),
        'max': (lambda lst, _: max(lst), 2),
        'min': (lambda lst, _: min(lst), 2),
        'abs': (lambda x, _: abs(x), 2),
        'round': (lambda x, ndigits, _: round(x, ndigits), 3),
        'pow': (lambda x, y, _: math.pow(x, y), 3),
        'sqrt': (lambda x, _: math.sqrt(x), 2),
        'log': (lambda x, base, _: math.log(x, base), 3),
        'ln': (lambda x, _: math.log(x), 2),
        'exp': (lambda x, _: math.exp(x), 2),

        # statistical functions
        'stddev': (lambda args, obj: statistics.stdev(args), 1),
        'mean': (lambda args, obj: statistics.mean(args), 1),
        'median': (lambda args, obj: statistics.median(args), 1),
        'mode': (lambda args, obj: statistics.mode(args), 1),
        'variance': (lambda args, obj: statistics.variance(args), 1),
        'percentile': (lambda args, obj: np.percentile(args, 50), 1),
        'correlation': (lambda args, obj: np.corrcoef(args), 1),
        'covariance': (lambda args, obj: np.cov(args), 1),

        # random sampling functions
        'normal-distribution': (lambda args, obj: np.random.normal(args), 1),
        'uniform-distribution': (lambda args, obj: np.random.uniform(args), 1),
        'exponential-distribution': (lambda args, obj: np.random.exponential(args), 1),
        'poisson-distribution': (lambda args, obj: np.random.poisson(args), 1),
        'binomial-distribution': (lambda args, obj: np.random.binomial(args), 1),
        'chi-square-distribution': (lambda args, obj: np.random.chisquare(args), 1),
        'f-distribution': (lambda args, obj: np.random.f(args), 1),
        't-distribution': (lambda args, obj: np.random.t(args), 1),
        'weibull-distribution': (lambda args, obj: np.random.weibull(args), 1),
        'log-normal-distribution': (lambda args, obj: np.random.lognormal(args), 1),
        'gamma-distribution': (lambda args, obj: np.random.gamma(args), 1),
        'beta-distribution': (lambda args, obj: np.random.beta(args), 1),
        'pareto-distribution': (lambda args, obj: np.random.pareto(args), 1),
        'triangular-distribution': (lambda args, obj: np.random.triangular(args), 1),
        'uniform-distribution': (lambda args, obj: np.random.uniform(args), 1),
        'random-choice': (lambda args, obj: np.random.choice(args), 1),
        'random-sample': (lambda args, obj: np.random.sample(args), 1),
        'random-shuffle': (lambda args, obj: np.random.shuffle(args), 1),
        'random-seed': (lambda args, obj: np.random.seed(args), 1)
    }

    @staticmethod
    def eval(query, obj):
        """
        Evaluates the query against the provided object.

        :param query: The query AST as a list.
        :param obj: The dictionary object to evaluate.
        :return: Boolean indicating if the object satisfies the query.
        """
        if not isinstance(query, list) or not query:
            raise ValueError("Invalid query format.")

        op = query[0].lower()
        if op not in jaf_eval.funcs:
            raise ValueError(f"Unknown operator: {op}")

        print(f"Operator: {op}")

        func, nargs = jaf_eval.funcs[op]
        print("func_args", func.__code__.co_varnames)
        print("func_nargs", nargs)

        args = query[1:] + [obj]
        print(f"Arguments: {args}")
        if nargs != -1 and len(args) != nargs:
            raise ValueError(f"'{op}' expects {nargs} arguments.")

        args = [jaf_eval.eval_oper(arg, obj) for arg in args]
        print(f"Evaluated arguments: {args}")

        # Call the function with the evaluated arguments
        result = func(*args)
        print(f"Result: {result}")

        return result

    @staticmethod
    def eval_oper(expression, obj):
        """
        Evaluates an operand which can be a value or a sub-query.

        :param expression: The expression to evaluate.
        :param obj: The dictionary object.
        :return: The evaluated value.
        """
        print(f"eval_oper::expression: {expression}")
        print(f"eval_oper::object: {obj}")

        if isinstance(expression, list):
            print(f"Evaluating expression: {expressionr}")
            val = jaf_eval.eval(expression, obj)
            print(f"Evaluated expression: {val}")
            return val
        else:
            print(f"Self-evaluating expression: {expression}")
            return expression


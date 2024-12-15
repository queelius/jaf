import datetime
import re
import rapidfuzz
import random
import functools
import math
import statistics

class jaf_eval:
    """
    JSON Abstract Filter (JAF) evaluator class. This only applies to a single
    object in the array. See `jaf.py` for the class that applies to the entire
    array.
    """

    funcs = {
        'eq?': (lambda args, obj: jaf.eval_oper(args[0], obj) == jaf.eval_oper(args[1], obj), 2),
        'gt?': (lambda args, obj: jaf.eval_oper(args[0], obj) > jaf.eval_oper(args[1], obj), 2),
        'lt?': (lambda args, obj: jaf.eval_oper(args[0], obj) < jaf.eval_oper(args[1], obj), 2),
        'contains?': (lambda args, obj: jaf.eval_oper(args[1], obj) in jaf.eval_oper(args[0], obj), 2),
        'in?': (lambda args, obj: jaf.eval_oper(args[1], obj) in jaf.eval_oper(args[0], obj), 2),
        'lower-case': (lambda args, obj: jaf.eval_oper(args[0], obj).lower(), 1),
        'date-diff': (lambda args, obj: abs(jaf.eval_oper(args[0], obj) - jaf.eval_oper(args[1], obj)), 2),
        'now': (lambda _, __: datetime.datetime.now().year, 0),
        'starts-with?': (lambda args, obj: jaf.eval_oper(args[0], obj).startswith(jaf.eval_oper(args[1], obj)), 2),
        'ends-with?': (lambda args, obj: jaf.eval_oper(args[0], obj).endswith(jaf.eval_oper(args[1], obj)), 2),
        'neq?': (lambda args, obj: jaf.eval_oper(args[0], obj) != jaf.eval_oper(args[1], obj), 2),
        'gte?': (lambda args, obj: jaf.eval_oper(args[0], obj) >= jaf.eval_oper(args[1], obj), 2),
        'lte?': (lambda args, obj: jaf.eval_oper(args[0], obj) <= jaf.eval_oper(args[1], obj), 2),
        'empty?': (lambda args, obj: not jaf.eval(args[0], obj), 1),
        'xor': (lambda args, obj: jaf.eval(args[0], obj) ^ jaf.eval(args[1], obj), 2),
        'true': (lambda _, __: True, 0),
        'false': (lambda _, __: False, 0),
        'null': (lambda _, __: None, 0),
        'upper-case': (lambda args, obj: jaf.eval_oper(args[0], obj).upper(), 1),
        'random-choice': (lambda args, obj: random.choice(args), -1),
        'gauss': (lambda args, obj: random.gauss(jaf.eval_oper(args[0], obj), jaf.eval_oper(args[1], obj)), 2),
        'stddev': (lambda args, obj: statistics.stdev(args), 1),
        'mean': (lambda args, obj: statistics.mean(args), 1),
        'median': (lambda args, obj: statistics.median(args), 1),
        'mode': (lambda args, obj: statistics.mode(args), 1),
        'regex-match?': (lambda args, obj: re.match(jaf.eval_oper(args[1], obj), jaf.eval_oper(args[0], obj)), 2),
        'close-match?': (lambda args, obj: rapidfuzz.fuzz.partial_ratio(jaf.eval_oper(args[0], obj), jaf.eval_oper(args[1], obj)) > 90, 2),
        'partial-match?': (lambda args, obj: rapidfuzz.fuzz.partial_ratio(jaf.eval_oper(args[0], obj), jaf.eval_oper(args[1], obj)) > 90, 2),
        'random': (lambda _, __: random.random(), 0),
        'random-int': (lambda args, _: random.randint(jaf.eval_oper(args[0], obj), jaf.eval_oper(args[1], obj)), 2),
        'and': (lambda args, obj: all(jaf.eval(sub, obj) for sub in args), -1),
        'or': (lambda args, obj: any(jaf.eval(sub, obj) for sub in args), -1),
        'not': (lambda args, obj: not jaf.eval(args[0], obj), 1),
        'if': (lambda args, obj: jaf.eval(args[1], obj) if jaf.eval(args[0], obj) else jaf.eval(args[2], obj), 3),
        'cond': (lambda args, obj: next(jaf.eval(sub[1], obj) for sub in args if jaf.eval(sub[0], obj)), -1),
        'quote': (lambda args, _: args[0], 1),
        'list': (lambda args, _: args, -1),
        'head': (lambda args, _: args[0], 1),
        'tail': (lambda args, _: args[1:], 1),
        'concat': (lambda args, _: ''.join(args), -1),
        'len': (lambda args, _: len(args[0]), 1),
        'nth': (lambda args, _: args[0][args[1]], 2),
        'map': (lambda args, _: [jaf.eval([args[0], sub], obj) for sub in args[1]], 2),
        'filter': (lambda args, _: [sub for sub in args[1] if jaf.eval([args[0], sub], obj)], 2),
        'reduce': (lambda args, _: functools.reduce(jaf.eval(args[0], obj), args[1]), 2),
        'apply': (lambda args, _: jaf.eval(args[0], obj)(*args[1:]), -1),
        'eval': (lambda args, _: jaf.eval(jaf.eval(args[0], obj), obj), 1),
        'gather': (lambda args, _: {k: v for k, v in args}, -1),
        'get': (lambda args, _: jaf.get_val(obj, args[0]), 1),
        'set': (lambda args, _: jaf.set_val(obj, args[0], args[1]), 2),
        'del': (lambda args, _: jaf.del_val(obj, args[0]), 1),
        'keys': (lambda args, _: obj.keys(), 0),
        'values': (lambda args, _: obj.values(), 0),
        'items': (lambda args, _: obj.items(), 0),
        'merge': (lambda args, _: {**args[0], **args[1]}, 2),
        'sort': (lambda args, _: sorted(args[0]), 1),
        'reverse': (lambda args, _: list(reversed(args[0])), 1),
        'slice': (lambda args, _: args[0][args[1]:args[2]], 3),
        'zip': (lambda args, _: list(zip(*args)), -1),
        'sum': (lambda args, _: sum(args[0]), 1),
        'max': (lambda args, _: max(args[0]), 1),
        'min': (lambda args, _: min(args[0]), 1),
        'abs': (lambda args, _: abs(args[0]), 1),
        'round': (lambda args, _: round(args[0]), 1),
        'pow': (lambda args, _: pow(args[0], args[1]), 2),
        'sqrt': (lambda args, _: pow(args[0], 0.5), 1),
        'log': (lambda args, _: math.log(args[0], args[1]) if len(args) == 2 else math.log(args[0]), 1),
        'exp': (lambda args, _: math.exp(args[0]), 1),
        'sin': (lambda args, _: math.sin(args[0]), 1),
        'cos': (lambda args, _: math.cos(args[0]), 1),
        'tan': (lambda args, _: math.tan(args[0]), 1),
        'asin': (lambda args, _: math.asin(args[0]), 1)
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
        if op in jaf.funcs:
            func, nargs = jaf.funcs[op]
            args = query[1:]
            if len(args) != nargs:
                raise ValueError(f"'{op}' expects {nargs} arguments.")
            return func(args, obj)

        else:
            raise ValueError(f"Unknown operator: {op}")

    def eval_oper(oper, obj):
        """
        Evaluates an operand which can be a value or a sub-query.

        :param operand: The operand to evaluate.
        :param obj: The dictionary object.
        :return: The evaluated value.
        """
        if isinstance(oper, list):
            return eval(oper, obj)
        elif isinstance(oper, str):
            # Retrieve the value from the object using dot notation
            return jaf.get_val(obj, oper)
        else:
            # Literal value
            return oper

    @staticmethod
    def get_val(obj, path):
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
                raise KeyError(f"Path '{path}' not found in the object.")
        return cur

# Example Usage
if __name__ == "__main__":
    query = [
        'and',
            [
                'or',
                    ['gt', ['date-diff', ['now'], 'owner.dob'], 18]
            ],
            ['eq', ['lower-case', 'owner.name'], 'alex'],
            ['lt', 'owner.age', 80],
        [
            'and',
                ['contains', 'asset.description', 'bitcoin'],
                ['gt', 'asset.amount', 1]
        ]
    ]

    obj = {
        'owner': {
            'name': 'Alex',
            'dob': 1985,  # Assuming 'dob' is the year of birth
            'age': 49,
            'city': 'no where'
        },
        'asset': {
            'description': 'bitcoin',
            'amount': 34
        }
    }

    j = jaf()

    # Mock 'now' function to return 2024 for consistent date-diff calculation
    def mock_now(args, obj):
        return 2024
    j.funcs['now'] = (mock_now, 0)

    try:
        result = eval(query, obj)
        print("Does the object satisfy the query?", result)
    except Exception as e:
        print("Error during evaluation:", e)

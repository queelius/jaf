import logging
from .path import PathValue, OpValue, Value, path_values
from .utils import wrap
from typing import Any

logger = logging.getLogger(__name__)

class jaf_eval:
    funcs = {
        'gt?': wrap(lambda x, y, obj: x > y, "gt?"),
        'gte?': wrap(lambda x, y, obj: x >= y, "gte?"),
        'lt?': wrap(lambda x, y, obj: x < y, "lt?"),
        'lte?': wrap(lambda x, y, obj: x <= y, "lte?"),
        'eq?': wrap(lambda x, y, obj: x == y, "eq?"),
        'sum': wrap(lambda *args, obj: sum(args), "sum"),
        'if': wrap(lambda condition, true_val, false_val, obj: true_val if condition else false_val, "if"),
        'and': wrap(lambda *args, obj: all(args), "and"),
        'or': wrap(lambda *args, obj: any(args), "or"),
        'path': lambda path, obj: path_values(path, obj),
        # ... other operators ...
    }

    @staticmethod
    def eval(query: Any, obj: Any) -> Any:
        if not isinstance(query, list) or not query:
            return query

        op = jaf_eval(query[0], obj)
        logger.debug(f"Operator query: {op}")

        if op not in jaf_eval.funcs:
            raise ValueError(f"Unknown operator: {op}")

        func = jaf_eval.funcs[op]
        args = query[1:]
        logger.debug(f"Unevaluated args: {args}")

        eval_args = []
        for arg in args:
            if isinstance(arg, list):
                logger.debug(f"Arg {arg} is a operator.")
                val = jaf_eval.eval(arg, obj)
                eval_args.append(val)
            else:
                logger.debug(f"Arg {arg} is a non-operator.")
                eval_args.append(OpValue(value=arg, derived_from=None, composition=[]))

        return func(eval_args, obj)
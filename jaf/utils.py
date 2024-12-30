import itertools
import logging
from .path import PathValue, OpValue
from typing import Any, List, Type, Callable, Optional, Tuple
import inspect

logger = logging.getLogger(__name__)


def func_namer(func):
    if func.__name__ == "<lambda>":
        body = inspect.getsource(func).split(":")[1].strip()
        args = inspect.getfullargspec(func).args
        args = ', '.join(args)
        return f"λ {args} ↦ {body}"
    else:
        return func.__name__


#def wrap(n, func, func_name=None):
def wrap(func: Callable,
         func_name: Optional[str] = None):

    if not callable(func):
        raise TypeError("func must be a callable")

    if func_name is None:
        func_name = func_namer(func)

    def _wrapper(*args, obj):

        results = []

        logger.debug(f"{func_name=} applied to {args=}")

        try:
            if isinstance(args, PathValue):
                logger.debug(f"path-value with args {args}")
                val = func(args.value, obj)
                logger.debug(f"apply {func_name} to {args.value} -> {val}")
                op_val = OpValue(value=val, derived_from=args.value, composition=[func_name])
                logger.debug(f"op_val: {op_val}")
                results.append(op_val)

            elif isinstance(args, OpValue):
                logger.debug(f"op-value with args {args}")
                val = func(args.value, obj)
                logger.debug(f"apply {func_name} to {args.value} -> {val}")
                op_val = OpValue(value=val, derived_from=args.value, composition=[func_name] + args.composition)
                logger.debug(f"op_val: {op_val}")
                results.append(op_val)

            else:
                logger.debug(f"************ new-list-path-value with arg {args}")
                args = [arg.value if isinstance(arg, PathValue) else arg for arg in args[0]]
                logger.debug(f"new-list-path-value with arg {args}") 
                val = func(*args, obj)
                logger.debug(f"apply {func_name} to {args} -> {val}")
                results.append(val)


        except Exception as e:
            logger.error(f"Error applying {func_name} to {args}: {e}, skipping...")

        logger.debug(f"results: {results}")    
        return results
        
    return _wrapper

def flatten(lst, obj):
    if not isinstance(lst, list):
        return lst
    
    def _helper(lst):
        if not lst:
            return []
        if isinstance(lst[0], list):
            return _helper(lst[0]) + _helper(lst[1:])
        return lst[:1] + _helper(lst[1:])

    return _helper(lst)

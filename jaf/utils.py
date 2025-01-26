import itertools
import logging
from typing import Callable, Optional, List, Any
from .path import Value, OpValue, PathValue

logger = logging.getLogger(__name__)

def wrap(func: Callable, func_name: Optional[str] = None):
    if not callable(func):
        raise TypeError("func must be a callable")

    func_name = func_name or func.__name__

    def _wrapper(args: List[Value], obj):
        logger.debug(f"Applying {func_name} to {args}")
        try:
            # Extract values from Value instances
            extracted_args = []
            for arg in args:
                if isinstance(arg, Value):
                    extracted_args.append(arg.value)
                else:
                    extracted_args.append(arg)

            # Flatten arguments for combinatorial application
            arg_lists = []
            for arg in extracted_args:
                if isinstance(arg, list):
                    arg_lists.append(arg)
                else:
                    arg_lists.append([arg])

            combinations = list(itertools.product(*arg_lists))
            logger.debug(f"Combinations for '{func_name}': {combinations}")

            results = []
            for combo in combinations:
                result = func(*combo, obj=obj)
                op_val = OpValue(value=result, derived_from=list(combo), composition=[func_name])
                results.append(op_val)
                logger.debug(f"Result for combination {combo}: {op_val}")

            return results

        except Exception as e:
            logger.error(f"Error in '{func_name}': {e}")
            return []

    return _wrapper
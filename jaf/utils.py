import itertools
import logging

logger = logging.getLogger(__name__)

def _path_value(path, obj):
    """
    Retrieves values from a nested dictionary using dot notation with support for wildcards.

    :param obj: The dictionary object.
    :param path: The dot-separated path string.
                 - Normal keys match exact dictionary keys.
                 - '*' matches exactly one level of any key.
                 - '**' matches zero or more levels of any keys.
    :return: A list of all matched values.
    """
    parts = path.split('.')

    def _match_path(cur_obj, parts):
        if not parts:
            # No more parts to match, return the current object as a match
            return [cur_obj]

        part = parts[0]

        if part == '**':
            # '**' matches zero or more levels
            results = []

            # 1. Try skipping this wildcard (zero levels)
            results.extend(_match_path(cur_obj, parts[1:]))

            # 2. If current is a dict, try going deeper on all keys
            if isinstance(cur_obj, dict):
                for k, v in cur_obj.items():
                    # Keep '**' in the current pattern to allow multiple steps down
                    results.extend(_match_path(v, parts))
            
            logger.debug(f"[match_path] ** results: {results}")
            return results

        elif part == '*':
            # '*' matches exactly one level of any key
            results = []
            if isinstance(cur_obj, dict):
                for k, v in cur_obj.items():
                    results.extend(_match_path(v, parts[1:]))

            logger.debug(f"[match_path] * results: {results}")
            return results

        else:
            results = []
            # Normal key
            if isinstance(cur_obj, dict) and part in cur_obj:
                results.extend(_match_path(cur_obj[part], parts[1:]))
            logger.debug(f"[match_path] Normal key results: {results}")
            return results

    return _match_path(obj, parts)

def _exists(query, obj):
    """
    Checks if the path exists in the object.
    For wildcard paths, checks if there's at least one match.
    """
    def _helper(o, type):
        if isinstance(o, dict):
            return any([_helper(v) for v in o.values()])
        elif isinstance(o, list):
            return any([_helper(v) for v in o])
        elif isinstance(o, type):
            return o == args[0]
        else:
            return False

    if isinstance(query, list):
        op = query[0]
        args = query[1:]

        if len(args) == 0:
            raise ValueError("No arguments provided for 'exists' function.")

        if op == 'path':
            if len(args) == 1:
                return bool(_path_value(args[0], obj))
            else:
                return all([_exists(x, obj) for x in args])
            
        # elif op == 'op':
        #     if len(args) == 1:
        #         return args[0] in obj
        #     else:
        #         return all([_exists(x, obj) for x in args])
        
        elif op == 'number':
            if len(args) == 1:
                return _helper(obj, (int, float))
            else:
                return all([_exists(x, obj) for x in args])

        elif op == 'str':
            if len(args) == 1:
                return _helper(obj, str)
            else:
                return all([_exists(x, obj) for x in args])
            
        else:
            raise ValueError(f"Unknown operator: {op}")

def wrap(n, func):
    """
    A generic wrapper that handles functions with varying numbers of arguments.

    :param n: Number of expected arguments. Use -1 for variable arguments.
    :param func: The function to wrap.
    :return: Wrapped function.
    """
    def wrapper(*args, obj):
        try:
            if len(args) != n-1:
                logger.debug(f"[wrap_func] Args: {args}, expected {n} args, got {len(args)+1}.") 
                raise ValueError("Unexpected number of arguments.")

            # Convert non-list arguments to single-element lists
            arg_lists = [arg if isinstance(arg, list) else [arg] for arg in args]

            # Generate all possible combinations of arguments
            if n != -1:
                combinations = list(itertools.product(*arg_lists))
            else:
                # For variable arguments, assume all combinations are direct
                combinations = [args]

            results = [func(*combo, obj) for combo in combinations]

            logger.debug(f"[wrap_func] Results: {results}")

            # If all results are booleans, aggregate them using `any`
            if all(isinstance(x, bool) for x in results):
                aggregated_result = any(results)
                logger.debug(f"[wrap_func] Aggregated Result: {aggregated_result}")
                return aggregated_result

            # If there's only one result, return it directly
            if len(results) == 1:
                return results[0]

            return results

        except Exception as e:
            logger.error(f"Error evaluating function: {e}")
            raise e
    return wrapper

def _wrap_1(func):
    return wrap(1, func)

def _wrap_2(func):
    return wrap(2, func)

def _wrap_3(func):
    return wrap(3, func)

def _wrap_4(func):
    return wrap(4, func)
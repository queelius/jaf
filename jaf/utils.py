import itertools
import logging
from typing import Any, List, NewType

# Define custom types
WildcardResults = NewType('WildcardResults', List[Any])

logger = logging.getLogger(__name__)

def path_values(path_components: list, obj: Any):
    """
    Retrieves values from a nested dictionary using a list of path components.
    
    :param path_components: List of path components (strings, integers, or wildcards)
    :param obj: The dictionary object.
    :return: A single value for non-wildcard paths, or a list for wildcard paths.
    """
    if not path_components:
        return obj
    
    has_wildcards = any(comp in ['*', '**'] for comp in path_components)
    
    def _match_path(cur_obj: Any, parts: List[str]):
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
                for v in cur_obj.values():
                    # Keep '**' in the current pattern to allow multiple steps down
                    results.extend(_match_path(v, parts))
            # 3. If current is a list, try going deeper on all elements
            elif isinstance(cur_obj, list):
                for item in cur_obj:
                    results.extend(_match_path(item, parts))

            logger.debug(f"[match_path] ** results: {results}")
            return results

        elif part == '*':
            # '*' matches exactly one level of any key
            results = []
            if isinstance(cur_obj, dict):
                for v in cur_obj.values():
                    results.extend(_match_path(v, parts[1:]))
            elif isinstance(cur_obj, list):
                for item in cur_obj:
                    results.extend(_match_path(item, parts[1:]))

            logger.debug(f"[match_path] * results: {results}")
            return results

        else:
            results = []
            # Handle both string keys and integer indices
            if isinstance(cur_obj, dict) and part in cur_obj:
                results.extend(_match_path(cur_obj[part], parts[1:]))
            elif isinstance(cur_obj, list) and isinstance(part, int) and 0 <= part < len(cur_obj):
                results.extend(_match_path(cur_obj[part], parts[1:]))
            logger.debug(f"[match_path] Normal key results: {results}")
            return results

    results = _match_path(obj, path_components)
    
    # Always return empty list for no results
    if len(results) == 0:
        return []
    
    # For non-wildcard paths, if exactly one result, return the value directly
    # For wildcard paths, always return lists
    if not has_wildcards and len(results) == 1:
        return results[0]
    
    return results

def exists(path, obj):
    """
    Checks if the path exists in the object.
    For wildcard paths, checks if there's at least one match.
    """
    try:
        values = path_values(path, obj)
        if isinstance(values, list):
            return len(values) > 0
        else:
            return values is not None
    except:
        return False

def wrap(n, func):
    """
    A generic wrapper that handles functions with varying numbers of arguments.
    The function is expected to take `n` arguments, where `n` can be -1 for variable arguments.
    The wrapper will generate all possible combinations of arguments and call the function with them.
    It will then test if all evaluations are booleans and return True if any of them are True,
    or False if all of them are False. If the results are not all booleans, it will return a list of results.

    :param n: Number of expected arguments. Use -1 for variable arguments.
    :param func: The function to wrap.
    :return: Wrapped function.
    """
    def wrapper(*args, obj):
        try:
            if len(args) != n-1:
                logger.debug(f"[wrap_func] Args: {args}, expected {n} args, got {len(args)+1}.") 
                raise ValueError("Unexpected number of arguments.")

            # Debug logging for length function
            if hasattr(func, '__name__') or 'length' in str(func):
                print(f"DEBUG WRAP: Function called with args: {args}, obj keys: {list(obj.keys()) if isinstance(obj, dict) else 'not dict'}")

            # For simple cases with no wildcard expansion needed, call directly
            results = [func(*args, obj)]

            # Debug logging for length function
            if hasattr(func, '__name__') or 'length' in str(func):
                print(f"DEBUG WRAP: Function results: {results}")

            logger.debug(f"[wrap_func] Results: {results}")

            # If all results are booleans, aggregate them using `any`
            if all(isinstance(x, bool) for x in results):
                aggregated_result = any(results)
                logger.debug(f"[wrap_func] Aggregated Result: {aggregated_result}")
                return aggregated_result

            # If there's only one result, return it directly
            if len(results) == 1:
                result = results[0]
                
                # Special case: if the single result is a list containing exactly one list,
                # flatten it one level (this handles cases where wildcard paths return [[data]])
                if isinstance(result, list) and len(result) == 1 and isinstance(result[0], list):
                    return result[0]
                
                return result

            return results

        except (TypeError, AttributeError) as e:
            # Handle type errors gracefully by returning False
            logger.debug(f"[wrap_func] Type error: {e}")
            print(f"DEBUG WRAP: Type error caught: {e}, args: {args}")
            return False
        except Exception as e:
            logger.error(f"Error evaluating function: {e}")
            raise e
    
    return (wrapper, n)


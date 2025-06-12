import itertools
import logging
from typing import Any, List, NewType

# Define custom types
# WildcardResults = NewType('WildcardResults', List[Any]) # Can be kept for type hinting if desired

# Actual class for isinstance checks
class WildcardResultsList(list):
    """A list subclass to mark results from wildcard path expansions."""
    pass

logger = logging.getLogger(__name__)

def path_values(path_components: list, obj: Any):
    """
    Retrieves values from a nested dictionary using a list of path components.
    
    :param path_components: List of path components (strings, integers, or wildcards)
    :param obj: The dictionary object.
    :return: A single value for non-wildcard paths, 
             a WildcardResultsList for wildcard paths, 
             or an empty list if no values found.
    """
    if not path_components:
        return obj # Return the object itself if path is empty
    
    has_wildcards = any(comp in ['*', '**'] for comp in path_components)
    
    # _match_path is assumed to return a flat list of all matched values.
    # e.g., path "a.b" to value "v" -> _match_path returns ["v"]
    # e.g., path "a.*.c" to values "v1", "v2" -> _match_path returns ["v1", "v2"]
    # e.g., path "a.d" to value [1,2] (a list) -> _match_path returns [[1,2]] if not careful,
    # but it should ideally return the list itself as an item: [[1,2]] if path is "a.d" and obj.a.d = [1,2]
    # Let's assume _match_path returns a list of items found at the end of the path.
    # If the path points to a scalar, it's [scalar]. If it points to a list, it's [list_obj].
    
    def _match_path(cur_obj: Any, parts: List[str]):
        # ... existing _match_path implementation ...
        # Ensure _match_path returns a flat list of found terminal values.
        # If path "x.y" resolves to obj[x][y] = "foo", _match_path returns ["foo"]
        # If path "x.z" resolves to obj[x][z] = [1,2], _match_path returns [[1,2]]
        # This was the previous behavior. Let's stick to it.
        if not parts:
            return [cur_obj]

        part = parts[0]

        if part == '**':
            results = []
            results.extend(_match_path(cur_obj, parts[1:])) # Match zero levels
            if isinstance(cur_obj, dict):
                for v in cur_obj.values():
                    results.extend(_match_path(v, parts))
            elif isinstance(cur_obj, list):
                for item in cur_obj:
                    results.extend(_match_path(item, parts))
            # Deduplicate results for '**' to avoid excessive processing if paths overlap
            # This might be complex; for now, assume raw results are fine.
            # A simple deduplication for hashable items:
            # unique_results = []
            # seen = set()
            # for r in results:
            #    try:
            #        if r not in seen: # This check is slow for unhashable items
            #            unique_results.append(r)
            #            seen.add(r)
            #    except TypeError: # Unhashable item
            #        if r not in unique_results: # Slower check
            #             unique_results.append(r)
            # return unique_results
            return results


        elif part == '*':
            results = []
            if isinstance(cur_obj, dict):
                for v in cur_obj.values():
                    results.extend(_match_path(v, parts[1:]))
            elif isinstance(cur_obj, list):
                for item in cur_obj:
                    results.extend(_match_path(item, parts[1:]))
            return results
        else:
            results = []
            if isinstance(cur_obj, dict) and part in cur_obj:
                results.extend(_match_path(cur_obj[part], parts[1:]))
            elif isinstance(cur_obj, list) and isinstance(part, int) and 0 <= part < len(cur_obj):
                results.extend(_match_path(cur_obj[part], parts[1:]))
            return results

    matched_values = _match_path(obj, path_components)

    if has_wildcards:
        # For wildcard paths, always return a WildcardResultsList,
        # even if it's empty.
        return WildcardResultsList(matched_values)
    else:
        # For non-wildcard paths:
        if not matched_values:
            return [] # Return a plain empty list if no match
        # Otherwise, _match_path returns a list containing a single element: the actual value.
        return matched_values[0]

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
    If arguments include WildcardResultsList, it expands them using Cartesian product.
    It then tests if all evaluations are booleans and return True if any of them are True,
    or False if all of them are False. If the results are not all booleans, it will return a list of results.

    :param n: Number of expected arguments (including obj).
    :param func: The function to wrap.
    :return: Wrapped function.
    """
    def wrapper(*args, obj):
        try:
            if n != -1 and len(args) != n - 1: # n includes obj, args doesn't
                logger.debug(f"[wrap_func] Args: {args}, expected {n-1} data args, got {len(args)}.") 
                raise ValueError(f"Unexpected number of arguments for function {func.__name__ if hasattr(func, '__name__') else 'lambda'}. Expected {n-1}, got {len(args)}.")

            if hasattr(func, '__name__') or 'length' in str(func):
                print(f"DEBUG WRAP: Function called with args: {args}, obj keys: {list(obj.keys()) if isinstance(obj, dict) else 'not dict'}")

            wildcard_arg_indices = [i for i, arg_val in enumerate(args) if isinstance(arg_val, WildcardResultsList)]
            
            evaluated_results = []

            if not wildcard_arg_indices:
                evaluated_results.append(func(*args, obj))
            else:
                iterables_for_product = []
                has_empty_wildcard_list = False
                for i, arg_val in enumerate(args):
                    if i in wildcard_arg_indices: # isinstance(arg_val, WildcardResultsList)
                        if not arg_val: # Empty WildcardResultsList
                            has_empty_wildcard_list = True
                            break
                        iterables_for_product.append(arg_val)
                    else:
                        iterables_for_product.append([arg_val]) # Wrap non-wildcard args for product

                if has_empty_wildcard_list:
                    # If any wildcard list is empty, the product is empty.
                    # For predicates, this means False. For others, an empty list.
                    if hasattr(func, '__name__') and func.__name__.endswith('?'):
                        return False 
                    else:
                        evaluated_results = [] # Will result in returning [] later
                else:
                    for combo in itertools.product(*iterables_for_product):
                        try:
                            res = func(*combo, obj=obj)
                            evaluated_results.append(res)
                        except (TypeError, AttributeError): # Error within a specific combination
                            if hasattr(func, '__name__') and func.__name__.endswith('?'):
                                evaluated_results.append(False) # Predicate combo error -> False for that combo
                            else:
                                raise # Re-raise for non-predicates, to be caught by outer handler

            if hasattr(func, '__name__') or 'length' in str(func):
                print(f"DEBUG WRAP: Evaluated results: {evaluated_results}")
            logger.debug(f"[wrap_func] Evaluated results: {evaluated_results}")

            if not evaluated_results: # Can happen if has_empty_wildcard_list was true for non-predicate
                return []

            is_predicate_like = all(isinstance(x, bool) for x in evaluated_results)
            if is_predicate_like:
                aggregated_result = any(evaluated_results)
                logger.debug(f"[wrap_func] Aggregated Boolean Result: {aggregated_result}")
                return aggregated_result

            if len(evaluated_results) == 1:
                single_result = evaluated_results[0]
                # Flatten if it's a list containing a single list (e.g., [[data]] -> [data])
                # but not if it's a WildcardResultsList itself that needs to be returned as is (though unlikely here)
                if isinstance(single_result, list) and len(single_result) == 1 and isinstance(single_result[0], list):
                     # Check that it's not a WildcardResultsList that should remain wrapped,
                     # though wrap usually returns plain lists or scalars.
                     # This specific flattening is usually for non-wildcard paths returning [[data]].
                    return single_result[0]
                return single_result

            return evaluated_results

        except (TypeError, AttributeError) as e:
            logger.debug(f"[wrap_func] Type/Attribute error in wrapper: {e}, args: {args}")
            print(f"DEBUG WRAP: Type/Attribute error caught in wrapper: {e}, args: {args}")
            return False # Consistent with existing behavior for overall errors
        except Exception as e:
            logger.error(f"Error evaluating function in wrapper: {e}")
            raise e
    
    return (wrapper, n)


import itertools
import logging
import re
from typing import Any, List

# Actual class for isinstance checks
class PathValues(list): # Renamed from WildcardResultsList
    """
    A list subclass used to distinguish results from path evaluations 
    that might yield multiple values (e.g., from wildcards, slices, or indices).
    This helps functions like `adapt_jaf_operator` to correctly handle
    Cartesian products of arguments if necessary.
    """
    pass

logger = logging.getLogger(__name__)

def _path_has_multi_match_components(path_components_list: List[List[Any]]) -> bool:
    """
    Internal helper to determine if a path expression's components
    include operations that inherently imply multiple matches (e.g., wildcards, slices).
    """
    if not path_components_list:
        return False
    for component_list_item in path_components_list:
        if not isinstance(component_list_item, list) or not component_list_item:
            # Should not be reached with a correctly formed AST path component
            continue 
        op = component_list_item[0]
        # These operations can lead to multiple results from a single collection
        if op in ["indices", "slice", "regex_key", "wc_level", "wc_recursive"]:
            return True
    return False

def _match_recursive(current_obj: Any, components: List[List[Any]]) -> List[Any]:
    """
    Internal recursive engine for `eval_path`.
    It interprets the path 'components' against the 'current_obj'.
    Returns a flat list of all resolved values.
    """
    # Base Case: Cannot traverse None if path program is not exhausted
    if current_obj is None and components:
        return []
        
    # Base Case: Path program exhausted, return the current object as a result
    if not components: 
        return [current_obj]

    # Fetch current instruction: [op_code, ...op_args]
    component = components[0]
    remaining_components = components[1:] # Remainder of the program
    op = component[0]
    args = component[1:]
    
    collected_values = [] # Aggregates results from successful program executions

    # Dispatch based on operation code (op)
    if op == "key":
        if not (args and isinstance(args[0], str)):
            logger.debug(f"Invalid key argument for 'key' op: {args}")
            return []
        key_name = args[0]
        if isinstance(current_obj, dict) and key_name in current_obj:
            collected_values.extend(_match_recursive(current_obj[key_name], remaining_components))
    
    elif op == "index":
        if not (args and isinstance(args[0], int)):
            logger.debug(f"Invalid index argument for 'index' op: {args}")
            return []
        idx_val = args[0]
        if isinstance(current_obj, list):
            if -len(current_obj) <= idx_val < len(current_obj): # Valid index
                 collected_values.extend(_match_recursive(current_obj[idx_val], remaining_components))
    
    elif op == "indices":
        if not (args and isinstance(args[0], list)):
            logger.debug(f"Invalid indices argument for 'indices' op: {args}")
            return []
        idx_list = args[0]
        if isinstance(current_obj, list):
            for idx_val in idx_list:
                if isinstance(idx_val, int) and -len(current_obj) <= idx_val < len(current_obj):
                    collected_values.extend(_match_recursive(current_obj[idx_val], remaining_components))
    
    elif op == "slice":
        if not (2 <= len(args) <= 3):
            logger.debug(f"Invalid number of arguments for 'slice' op: {args}")
            return []

        start = args[0]
        stop = args[1]
        step = args[2] if len(args) > 2 else None # step can be None from AST
        if step is None: step = 1 # Default step to 1 if None or not provided

        if not (isinstance(step, int) and step > 0):
            logger.debug(f"Slice step must be a positive integer, got: {step}")
            return []
        if not (start is None or isinstance(start, int)):
            logger.debug(f"Slice start must be an integer or null, got: {start}")
            return []
        if not (stop is None or isinstance(stop, int)):
            logger.debug(f"Slice stop must be an integer or null, got: {stop}")
            return []

        if isinstance(current_obj, list):
            try:
                s = slice(start, stop, step)
                sliced_items = current_obj[s]
                for item in sliced_items:
                    collected_values.extend(_match_recursive(item, remaining_components))
            except (TypeError, ValueError) as e:
                logger.debug(f"Error during slicing for {current_obj} with {s}: {e}")
                
    elif op == "regex_key":
        if not (args and isinstance(args[0], str)):
            logger.debug(f"Invalid pattern argument for 'regex_key' op: {args}")
            return []
        pattern_str = args[0]
        try:
            regex = re.compile(pattern_str)
            if isinstance(current_obj, dict):
                for k, v_obj in current_obj.items():
                    if regex.match(k):
                        collected_values.extend(_match_recursive(v_obj, remaining_components))
        except re.error:
            logger.debug(f"Invalid regex pattern in path component: {pattern_str}")
            
    elif op == "wc_level": # '*' functionality
        if isinstance(current_obj, dict):
            for v_obj in current_obj.values():
                collected_values.extend(_match_recursive(v_obj, remaining_components))
        elif isinstance(current_obj, list):
            for item in current_obj:
                collected_values.extend(_match_recursive(item, remaining_components))
                
    elif op == "wc_recursive": # '**' functionality
        # 1. Attempt to match remaining_components starting from current_obj.
        collected_values.extend(_match_recursive(current_obj, remaining_components))
        
        # 2. Recursively apply this ["wc_recursive"] op (and remaining_components)
        #    to all children.
        current_recursive_op_and_remainder = [component] + remaining_components
        if isinstance(current_obj, dict):
            for v_obj in current_obj.values():
                collected_values.extend(_match_recursive(v_obj, current_recursive_op_and_remainder))
        elif isinstance(current_obj, list):
            for item in current_obj:
                collected_values.extend(_match_recursive(item, current_recursive_op_and_remainder))
    else:
        logger.warning(f"Unknown path operation code encountered: {op}")

    return collected_values

def eval_path(path_components_list: List[List[Any]], obj: Any) -> Any:
    """
    Evaluates a path expression against an object and retrieves values.

    The path_components_list is a list of lists, where each inner list
    defines a path segment (e.g., [["key", "name"], ["index", 0]]).

    Returns:
    - The direct value if the path is specific and resolves to a single item.
    - An empty list `[]` if a specific path resolves to no items.
    - A `PathValues` list if the path involves multi-match components 
      (like wildcards or slices) or if a specific-looking path resolves to 
      multiple distinct items due to data structure (e.g., path `a.b` on `{"a": [{"b":1}, {"b":2}]}`).
    - The original object `obj` if `path_components_list` is empty.
    """
    if not path_components_list: # Handles an empty path like ["path", []]
        return obj

    matched_values = _match_recursive(obj, path_components_list)

    is_specific_path_intent = not _path_has_multi_match_components(path_components_list)

    if is_specific_path_intent:
        if not matched_values:
            return [] 
        elif len(matched_values) == 1:
            return matched_values[0]
        else:
            # A path with specific-intent components yielded multiple results.
            # This occurs if a path like `a.b` is applied to `{"a": [{"b":1}, {"b":2}]}`.
            # The results `[1, 2]` should be treated as a collection.
            logger.debug(
                f"Path with specific-intent components yielded {len(matched_values)} results. "
                f"Path: {path_components_list}. Wrapping in PathValues."
            )
            return PathValues(matched_values)
    else:
        # Path had multi-match components, or a specific path unexpectedly yielded multiple values.
        return PathValues(matched_values)

def exists(path_components_list: List[List[Any]], obj: Any) -> bool:
    """
    Checks if the given path expression resolves to any value(s) in the object,
    including a literal `None` (null) value at the end of the path.
    Returns `False` only if the path does not lead to any data segment.
    """
    try:
        resolved_value = eval_path(path_components_list, obj)

        if isinstance(resolved_value, PathValues):
            return len(resolved_value) > 0
        elif isinstance(resolved_value, list) and not resolved_value: 
            # This specific return `[]` from eval_path means a specific path found nothing.
            return False
        # All other cases mean the path resolved to something:
        # - A single scalar value (str, int, bool, None)
        # - A single data structure (dict, or a list that is actual data, not the empty `[]` marker)
        return True 
    except Exception: 
        logger.debug(f"Exception during 'exists' check for path {path_components_list}", exc_info=True)
        return False # Path evaluation failed, so it effectively doesn't exist in a usable way.
    
def adapt_jaf_operator(n: int, func: callable) -> tuple[callable, int]:
    """
    Adapts a Python function to serve as a JAF operator.

    The wrapper handles:
    1. Argument Count Validation: Checks if the correct number of evaluated
       arguments (excluding the `obj` context) are passed.
    2. `PathValues` Expansion: If any arguments are `PathValues` instances
       (results of multi-value path evaluations), it computes the Cartesian 
       product of these arguments. Non-`PathValues` arguments are treated as
       single-element lists for this product. The underlying `func` is then
       called for each combination.
    3. Result Aggregation for Predicates: If all results from all combinations
       (or the single call) are boolean, it performs an 'any' aggregation
       (True if any result is True). These are typically functions ending with '?'.
    4. Result Handling for Value Extractors/Transformers:
       - If one `PathValues` argument was empty, an empty list `[]` is returned.
       - If a single value results, it's returned directly.
       - If a single list-of-lists like `[[data]]` results, it's flattened to `[data]`.
       - Otherwise, a list of all results from combinations is returned.
    5. Error Handling: Type/Attribute errors within `func` for a specific
       combination cause that combination to yield `False` if `func` is a
       predicate; otherwise, the error propagates, and the wrapper returns `False`.

    :param n: Number of expected arguments for `func` as defined in JAF 
              (e.g., for `["eq?", arg1, arg2]`, n=3 including `obj`).
              Use -1 for variadic functions (not currently standard in JAF ops).
    :param func: The Python function to adapt.
    :return: A tuple containing the wrapped function and `n`.
    """
    def wrapper(*args, obj): # These `args` are already evaluated by jaf_eval
        try:
            # `n` includes `obj`, but `obj` is passed as a keyword arg to `wrapper`
            # So, `args` here are the data arguments for `func`.
            expected_data_args = n -1 if n != -1 else -1 # -1 if func is variadic
            
            if expected_data_args != -1 and len(args) != expected_data_args:
                func_name = func.__name__ if hasattr(func, '__name__') else 'lambda'
                logger.debug(f"[{func_name}] Args: {args}, expected {expected_data_args} data args, got {len(args)}.") 
                raise ValueError(f"Incorrect arg count for {func_name}. Expected {expected_data_args}, got {len(args)}.")

            # DEBUG: print(f"DEBUG ADAPT_JAF_OPERATOR: Func {func.__name__ if hasattr(func, '__name__') else 'lambda'} called with args: {args}, obj keys: {list(obj.keys()) if isinstance(obj, dict) else 'not dict'}")

            path_values_arg_indices = [i for i, arg_val in enumerate(args) if isinstance(arg_val, PathValues)]
            
            evaluated_results = []

            if not path_values_arg_indices: # No PathValues, direct call
                evaluated_results.append(func(*args, obj=obj))
            else: # One or more PathValues arguments, use Cartesian product
                iterables_for_product = []
                has_empty_path_values = False
                for i, arg_val in enumerate(args):
                    if i in path_values_arg_indices: # This arg is a PathValues instance
                        if not arg_val: # This PathValues is empty
                            has_empty_path_values = True
                            break
                        iterables_for_product.append(arg_val)
                    else: # Regular argument
                        iterables_for_product.append([arg_val]) # Wrap for product

                if has_empty_path_values:
                    # If any PathValues arg is empty, the product is empty.
                    # For predicates (existential), this means False.
                    # For value extractors, this means no values produced, so [].
                    if hasattr(func, '__name__') and func.__name__.endswith('?'):
                        return False 
                    else:
                        # evaluated_results remains empty, will return [] later
                        pass
                else:
                    for combo in itertools.product(*iterables_for_product):
                        try:
                            res = func(*combo, obj=obj) # Pass obj explicitly
                            evaluated_results.append(res)
                        except (TypeError, AttributeError): 
                            # Error within a specific combination for the underlying func
                            if hasattr(func, '__name__') and func.__name__.endswith('?'):
                                evaluated_results.append(False) # Predicate combo error -> False for that combo
                            else:
                                # For non-predicates, re-raise to be caught by the outer handler of this wrapper
                                raise 
            
            # logger.debug(f"[{func.__name__ if hasattr(func, '__name__') else 'lambda'}] Raw results: {evaluated_results}")

            if not evaluated_results: # Can happen if has_empty_path_values was true for non-predicate
                return []

            # Check if all results are boolean (typical for predicates)
            is_predicate_like = all(isinstance(x, bool) for x in evaluated_results)
            if is_predicate_like:
                aggregated_result = any(evaluated_results) # Existential quantifier
                # logger.debug(f"[{func.__name__ if hasattr(func, '__name__') else 'lambda'}] Aggregated Boolean: {aggregated_result}")
                return aggregated_result

            # Handle results for value extractors/transformers
            if len(evaluated_results) == 1:
                single_result = evaluated_results[0]
                # Flatten if it's a list containing a single list (e.g., [[data]] -> [data])
                # This is mainly for functions that might return lists, and no PathValues expansion happened.
                if isinstance(single_result, list) and \
                   len(single_result) == 1 and \
                   isinstance(single_result[0], list) and \
                   not isinstance(single_result, PathValues): # Don't unwrap PathValues itself
                    return single_result[0]
                return single_result

            return evaluated_results # Return list of results for non-predicates with multiple results

        except (TypeError, AttributeError) as e:
            # Catches errors from non-PathValues calls, or re-raised from PathValues combo for non-predicates
            func_name = func.__name__ if hasattr(func, '__name__') else 'lambda'
            logger.debug(f"[{func_name}] Type/Attribute error in adapted operator: {e}, for args: {args}")
            # For filtering, a False return on type error is a safe default.
            return False 
        except ValueError as e: # Catch arg count errors specifically
            logger.debug(f"ValueError in adapted operator: {e}")
            return False # Or re-raise if preferred: raise
        except Exception as e:
            func_name = func.__name__ if hasattr(func, '__name__') else 'lambda'
            logger.error(f"Unexpected error in adapted operator [{func_name}]: {e}", exc_info=True)
            raise # Re-raise unexpected errors
    
    return (wrapper, n)
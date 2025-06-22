import itertools
import logging

logger = logging.getLogger(__name__)

def adapt_jaf_operator(n: int, func: callable) -> tuple[callable, int]:
    """
    Adapts a Python function to serve as a JAF operator.

    The wrapper handles:
    1. Argument Count Validation: Checks if the correct number of evaluated
       arguments (excluding the `obj` context) are passed. This check occurs
       before other processing, and if it fails, a `ValueError` is raised.
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
    5. Error Handling: 
       - `TypeError` or `AttributeError` within `func` for a specific
         combination (or direct call) cause that call to yield `False` if `func` 
         is a predicate; for non-predicates, the error is re-raised and then
         caught by the wrapper's `TypeError`/`AttributeError` handler, which returns `False`.
       - `ValueError` raised by `func` is propagated.
       - Other unexpected exceptions from `func` are propagated.


    :param n: Number of expected arguments for `func` as defined in JAF 
              (e.g., for `[\"eq?\", arg1, arg2]`, n=3 including `obj`).
              Use -1 for variadic functions (not currently standard in JAF ops).
    :param func: The Python function to adapt.
    :return: A tuple containing the wrapped function and `n`.
    """
    from .path_types import PathValues

    def wrapper(*args, obj): # These `args` are already evaluated by jaf_eval
        func_name = func.__name__ if hasattr(func, '__name__') else 'lambda'
        # `n` includes `obj`, but `obj` is passed as a keyword arg to `wrapper`
        # So, `args` here are the data arguments for `func`.
        expected_data_args = n - 1 if n != -1 else -1 # -1 if func is variadic
        
        # Argument count validation:
        # Skip check for variadic functions (where n is -1)
        if expected_data_args != -1 and len(args) != expected_data_args:
            raise ValueError(f"'{func_name}' expects {expected_data_args} arguments, got {len(args)}")

        try:
            # DEBUG: print(f"DEBUG ADAPT_JAF_OPERATOR: Func {func_name} called with args: {args}, obj keys: {list(obj.keys()) if isinstance(obj, dict) else 'not dict'}")

            path_values_arg_indices = [i for i, arg_val in enumerate(args) if isinstance(arg_val, PathValues)]
            
            evaluated_results = []

            if not path_values_arg_indices: # No PathValues, direct call
                # func() can raise TypeError, AttributeError, ValueError, or other exceptions.
                # These will be handled by the except blocks below.
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
                    # else: evaluated_results remains empty, will return [] later
                else:
                    for combo in itertools.product(*iterables_for_product):
                        try:
                            # func() can raise TypeError, AttributeError, ValueError, or other exceptions.
                            # These will be handled by the except blocks below if not caught here.
                            res = func(*combo, obj=obj) # Pass obj explicitly
                            evaluated_results.append(res)
                        except (TypeError, AttributeError): 
                            # Error within a specific combination for the underlying func
                            if hasattr(func, '__name__') and func.__name__.endswith('?'):
                                evaluated_results.append(False) # Predicate combo error -> False for that combo
                            else:
                                # For non-predicates, re-raise to be caught by the outer (TypeError, AttributeError) handler
                                raise 
                        # ValueErrors from func(*combo) will be caught by the outer `except ValueError`
            
            # logger.debug(f"[{func_name}] Raw results: {evaluated_results}")

            if not evaluated_results: # Can happen if has_empty_path_values was true for non-predicate
                return []

            # Check if all results are boolean (typical for predicates)
            is_predicate_like = all(isinstance(x, bool) for x in evaluated_results)
            if is_predicate_like:
                aggregated_result = any(evaluated_results) # Existential quantifier
                # logger.debug(f"[{func_name}] Aggregated Boolean: {aggregated_result}")
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

        except (TypeError, AttributeError) as e_user_func_type_attr_error:
            # Catches TypeErrors/AttributeErrors from func (direct call or re-raised from combo)
            # For filtering, a False return on type error is a safe default.
            logger.debug(f"[{func_name}] Type/Attribute error from wrapped function: {e_user_func_type_attr_error}, for args: {args}")
            return False 
        
        except ValueError as e_user_func_value_error:
            # Catches ValueErrors from func (direct call or from combo).
            # This is to ensure test_exception_propagation passes by propagating the original ValueError.
            logger.debug(f"[{func_name}] ValueError from wrapped function: {e_user_func_value_error}, for args: {args}")
            raise e_user_func_value_error # Re-raise it

        except Exception as e_unexpected:
            # Catches any other unexpected errors from func or the wrapper logic
            func_name_for_log = func.__name__ if hasattr(func, '__name__') else 'lambda'
            logger.error(f"Unexpected error in adapted operator [{func_name_for_log}] or wrapped function: {e_unexpected}", exc_info=True)
            raise # Re-raise unexpected errors
    
    return (wrapper, n)
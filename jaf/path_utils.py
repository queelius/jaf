import logging
import re
from typing import Any, List, Optional
from .path_conversion import PathSyntaxError, path_ast_to_string, string_to_path_ast

logger = logging.getLogger(__name__)

class PathValues(list):
    """
    A list subclass representing a collection of values obtained from a JAF path
    evaluation. It's used particularly when the path could naturally yield
    multiple results (e.g., from wildcards, slices, multiple indices, or
    regex key matching).

    It behaves like a standard list but serves as a distinct type to inform
    JAF's evaluation logic (e.g., `adapt_jaf_operator`) on how to handle
    arguments that are collections of path results. This typically involves
    iterating over the values or applying Cartesian product logic if multiple
    PathValues are arguments to an operator.

    Conceptually, it holds the "set of findings" or "resolved values" from the
    data for a given path. While it's a list (and thus can hold duplicates
    and maintains order of discovery), its primary distinction is signaling
    its origin from a potentially multi-valued path.

    An empty PathValues instance (e.g., PathValues([])) signifies that a
    multi-match path expression found no values. This is distinct from an
    empty list `[]` which might be returned by `eval_path` when a specific,
    non-multi-match path is not found.
    """
    def __init__(self, iterable: Optional[Any] = None):
        super().__init__(iterable if iterable is not None else [])

    def __repr__(self) -> str:
        return f"PathValues({super().__repr__()})"

    def __add__(self, other: Any) -> 'PathValues':
        if isinstance(other, list): # Handles list and PathValues
            return PathValues(super().__add__(other))
        return NotImplemented # type: ignore

    def __iadd__(self, other: Any) -> 'PathValues':
        super().__iadd__(other) # Modifies in place
        return self

    def __mul__(self, n: int) -> 'PathValues':
        return PathValues(super().__mul__(n))

    def __rmul__(self, n: int) -> 'PathValues':
        return PathValues(super().__rmul__(n)) # For n * PathValues

    def __getitem__(self, key: Any) -> Any:
        result = super().__getitem__(key)
        if isinstance(key, slice):
            return PathValues(result)
        # Single item access returns the item itself, not PathValues(item)
        return result

    def copy(self) -> 'PathValues':
        """Return a shallow copy of the PathValues."""
        return PathValues(self)

    def first(self, default: Optional[Any] = None) -> Any:
        """
        Returns the first element, or `default` if the list is empty.
        """
        return self[0] if self else default

    def last(self, default: Optional[Any] = None) -> Any:
        """
        Returns the last element, or `default` if the list is empty.
        """
        return self[-1] if self else default

    def one(self) -> Any:
        """
        Returns the single element if the list contains exactly one item.

        Raises:
            ValueError: If the list does not contain exactly one item.
        """
        if len(self) == 1:
            return self[0]
        elif not self:
            raise ValueError("PathValues is empty; expected exactly one element.")
        else:
            raise ValueError(f"PathValues contains {len(self)} elements; expected exactly one.")

    def one_or_none(self) -> Optional[Any]:
        """
        Returns the single element if the list contains exactly one item,
        or None if the list is empty.

        Raises:
            ValueError: If the list contains more than one item.
        """
        if len(self) == 1:
            return self[0]
        elif not self:
            return None
        else:
            raise ValueError(f"PathValues contains {len(self)} elements; expected one or none.")
        
    def __in__(self, item: Any) -> bool:
        """
        Check if an item is in the PathValues.
        This uses the base list's __contains__ method.
        """
        return super().__contains__(item)

def _path_has_multi_match_components(path_components_list: List[List[Any]]) -> bool:
    """
    Internal helper to determine if a path expression's components
    include operations that inherently imply multiple matches (e.g., wildcards, slices).
    """
    if not path_components_list:
        return False
    for component_list_item in path_components_list:
        if not isinstance(component_list_item, list) or not component_list_item:
            continue 
        op = component_list_item[0]
        if op in ["indices", "slice", "regex_key", "wc_level", "wc_recursive"]:
            return True
    return False

def _fuzzy_match_keys(target_key: str, available_keys: List[str], cutoff: float, algorithm: str) -> List[str]:
    """
    Internal helper function to perform fuzzy matching of keys using various algorithms.
    
    Args:
        target_key: The key to search for
        available_keys: List of available keys to match against
        cutoff: Minimum similarity score (0.0 to 1.0)
        algorithm: The fuzzy matching algorithm to use
    
    Returns:
        List of keys that match above the cutoff threshold, sorted by similarity (best first)
    """
    matches = []
    
    if algorithm == "difflib":
        import difflib
        # Use difflib's get_close_matches for sequence matching
        matches = difflib.get_close_matches(target_key, available_keys, n=len(available_keys), cutoff=cutoff)
    
    elif algorithm == "levenshtein":
        try:
            import Levenshtein
            # Calculate Levenshtein distance ratio
            similarities = []
            for key in available_keys:
                ratio = Levenshtein.ratio(target_key.lower(), key.lower())
                if ratio >= cutoff:
                    similarities.append((key, ratio))
            # Sort by similarity (highest first)
            similarities.sort(key=lambda x: x[1], reverse=True)
            matches = [key for key, _ in similarities]
        except ImportError:
            logger.warning("Levenshtein library not available. Falling back to difflib.")
            import difflib
            matches = difflib.get_close_matches(target_key, available_keys, n=len(available_keys), cutoff=cutoff)
    
    elif algorithm == "jaro_winkler":
        try:
            import jellyfish
            similarities = []
            for key in available_keys:
                similarity = jellyfish.jaro_winkler_similarity(target_key.lower(), key.lower())
                if similarity >= cutoff:
                    similarities.append((key, similarity))
            similarities.sort(key=lambda x: x[1], reverse=True)
            matches = [key for key, _ in similarities]
        except ImportError:
            logger.warning("jellyfish library not available. Falling back to difflib.")
            import difflib
            matches = difflib.get_close_matches(target_key, available_keys, n=len(available_keys), cutoff=cutoff)
    
    elif algorithm == "soundex":
        try:
            import jellyfish
            target_soundex = jellyfish.soundex(target_key)
            matches = [key for key in available_keys if jellyfish.soundex(key) == target_soundex]
        except ImportError:
            logger.warning("jellyfish library not available. Falling back to difflib.")
            import difflib
            matches = difflib.get_close_matches(target_key, available_keys, n=len(available_keys), cutoff=cutoff)
    
    elif algorithm == "metaphone":
        try:
            import jellyfish
            target_metaphone = jellyfish.metaphone(target_key)
            matches = [key for key in available_keys if jellyfish.metaphone(key) == target_metaphone]
        except ImportError:
            logger.warning("jellyfish library not available. Falling back to difflib.")
            import difflib
            matches = difflib.get_close_matches(target_key, available_keys, n=len(available_keys), cutoff=cutoff)
    
    return matches

class PathOperationDispatcher:
    """Dispatcher for path operations"""
    
    def __init__(self):
        self.operations = {
            "key": self._handle_key,
            "index": self._handle_index,
            "indices": self._handle_indices,
            "slice": self._handle_slice,
            "regex_key": self._handle_regex_key,
            "fuzzy_key": self._handle_fuzzy_key,
            "wc_level": self._handle_wc_level,
            "wc_recursive": self._handle_wc_recursive,
            "root": self._handle_root,
        }
    
    def dispatch(self, op: str, args: List[Any], current_obj: Any, 
                remaining_components: List[List[Any]], 
                full_path_ast_for_error: List[List[Any]], 
                root_obj_for_path: Any) -> List[Any]:
        """Dispatch to appropriate handler"""
        if op not in self.operations:
            raise PathSyntaxError(f"Unknown path operation: '{op}'", 
                                path_segment=[op] + args, 
                                full_path_ast=full_path_ast_for_error)
        
        return self.operations[op](args, current_obj, remaining_components, 
                                 full_path_ast_for_error, root_obj_for_path)
    
    def _handle_key(self, args: List[Any], current_obj: Any, 
                   remaining_components: List[List[Any]], 
                   full_path_ast_for_error: List[List[Any]], 
                   root_obj_for_path: Any) -> List[Any]:
        if not (len(args) == 1 and isinstance(args[0], str)):
            raise PathSyntaxError("'key' operation expects a single string argument.", 
                                path_segment=["key"] + args, 
                                full_path_ast=full_path_ast_for_error)
        key_name = args[0]
        if isinstance(current_obj, dict) and key_name in current_obj:
            return _match_recursive(current_obj[key_name], remaining_components, 
                                  full_path_ast_for_error, root_obj_for_path)
        return []
    
    def _handle_index(self, args: List[Any], current_obj: Any, 
                    remaining_components: List[List[Any]], 
                    full_path_ast_for_error: List[List[Any]], 
                    root_obj_for_path: Any) -> List[Any]:
        if not (len(args) == 1 and isinstance(args[0], int)):
            raise PathSyntaxError("'index' operation expects a single integer argument.", 
                                path_segment=["index"] + args, 
                                full_path_ast=full_path_ast_for_error)
        idx_val = args[0]
        if isinstance(current_obj, list):
            if -len(current_obj) <= idx_val < len(current_obj):
                 return _match_recursive(current_obj[idx_val], remaining_components, 
                                       full_path_ast_for_error, root_obj_for_path)
        return []
    
    def _handle_indices(self, args: List[Any], current_obj: Any, 
                      remaining_components: List[List[Any]], 
                      full_path_ast_for_error: List[List[Any]], 
                      root_obj_for_path: Any) -> List[Any]:
        if not (len(args) == 1 and isinstance(args[0], list) and all(isinstance(idx, int) for idx in args[0])):
            raise PathSyntaxError("'indices' operation expects a single list of integers argument.", 
                                path_segment=["indices"] + args, 
                                full_path_ast=full_path_ast_for_error)
        idx_list = args[0]
        if isinstance(current_obj, list):
            collected_values = []
            for idx_val in idx_list:
                if isinstance(idx_val, int) and -len(current_obj) <= idx_val < len(current_obj):
                    collected_values.extend(_match_recursive(current_obj[idx_val], remaining_components, 
                                                            full_path_ast_for_error, root_obj_for_path))
            return collected_values
        return []
    
    def _handle_slice(self, args: List[Any], current_obj: Any, 
                     remaining_components: List[List[Any]], 
                     full_path_ast_for_error: List[List[Any]], 
                     root_obj_for_path: Any) -> List[Any]:
        if not (1 <= len(args) <= 3):
            raise PathSyntaxError("'slice' operation expects 1 to 3 arguments for start, stop, step.", 
                                path_segment=["slice"] + args, 
                                full_path_ast=full_path_ast_for_error)

        start_val = args[0]
        stop_val = args[1] if len(args) > 1 else None
        step_val = args[2] if len(args) > 2 else None
        
        if not (start_val is None or isinstance(start_val, int)):
            raise PathSyntaxError("Slice start must be an integer or null.", path_segment=args, full_path_ast=full_path_ast_for_error)
        if not (stop_val is None or isinstance(stop_val, int)):
            raise PathSyntaxError("Slice stop must be an integer or null.", path_segment=args, full_path_ast=full_path_ast_for_error)
        
        actual_step = step_val if step_val is not None else 1
        if not (isinstance(actual_step, int) and actual_step != 0):
            raise PathSyntaxError("Slice step must be a non-zero integer.", path_segment=args, full_path_ast=full_path_ast_for_error)

        if isinstance(current_obj, list):
            try:
                s = slice(start_val, stop_val, actual_step)
                sliced_items = current_obj[s]
                collected_values = []
                for item in sliced_items:
                    collected_values.extend(_match_recursive(item, remaining_components, full_path_ast_for_error, root_obj_for_path))
                return collected_values
            except (TypeError, ValueError) as e: # Should be rare if AST validation is correct
                logger.debug(f"Error during slicing for {current_obj} with slice({start_val},{stop_val},{actual_step}): {e}")
        return []
    
    def _handle_regex_key(self, args: List[Any], current_obj: Any, 
                         remaining_components: List[List[Any]], 
                         full_path_ast_for_error: List[List[Any]], 
                         root_obj_for_path: Any) -> List[Any]:
        if not (1 <= len(args) <= 2):
            raise PathSyntaxError("'regex_key' operation expects 1 or 2 arguments: pattern, [flags].", 
                                path_segment=["regex_key"] + args, 
                                full_path_ast=full_path_ast_for_error)
        
        if not isinstance(args[0], str):
            raise PathSyntaxError("'regex_key' operation expects a string argument for the pattern.", 
                                path_segment=["regex_key"] + args, 
                                full_path_ast=full_path_ast_for_error)
        
        pattern = args[0]
        flags = 0  # Default no flags
        
        # Parse optional flags argument
        if len(args) == 2:
            if isinstance(args[1], str):
                # String flags like "i", "m", "s", etc.
                flag_str = args[1].lower()
                for flag_char in flag_str:
                    if flag_char == 'i':
                        flags |= re.IGNORECASE
                    elif flag_char == 'm':
                        flags |= re.MULTILINE
                    elif flag_char == 's':
                        flags |= re.DOTALL
                    elif flag_char == 'x':
                        flags |= re.VERBOSE
                    elif flag_char == 'a':
                        flags |= re.ASCII
                    else:
                        raise PathSyntaxError(f"'regex_key' operation: unknown flag '{flag_char}'. Valid flags: i, m, s, x, a.", 
                                            path_segment=["regex_key"] + args, 
                                            full_path_ast=full_path_ast_for_error)
            elif isinstance(args[1], int):
                # Integer flags (direct re module constants)
                flags = args[1]
            else:
                raise PathSyntaxError("'regex_key' operation expects a string or integer argument for flags.", 
                                    path_segment=["regex_key"] + args, 
                                    full_path_ast=full_path_ast_for_error)
        
        if isinstance(current_obj, dict):
            collected_values = []  
            try:
                compiled_pattern = re.compile(pattern, flags)
                for key in current_obj:
                    if compiled_pattern.search(key):  
                        collected_values.extend(_match_recursive(current_obj[key], remaining_components, 
                                                               full_path_ast_for_error, root_obj_for_path))
            except re.error as e:
                raise PathSyntaxError(f"'regex_key' operation: invalid regex pattern '{pattern}': {e}", 
                                    path_segment=["regex_key"] + args, 
                                    full_path_ast=full_path_ast_for_error)
            return collected_values
        
        return []  # Add this line
    
    def _handle_wc_level(self, args: List[Any], current_obj: Any, 
                        remaining_components: List[List[Any]], 
                        full_path_ast_for_error: List[List[Any]], 
                        root_obj_for_path: Any) -> List[Any]:
        if args:
            raise PathSyntaxError("'wc_level' operation expects no arguments.", path_segment=["wc_level"] + args, full_path_ast=full_path_ast_for_error)
        if isinstance(current_obj, dict):
            collected_values = []
            for v_obj in current_obj.values():
                collected_values.extend(_match_recursive(v_obj, remaining_components, full_path_ast_for_error, root_obj_for_path))
            return collected_values
        elif isinstance(current_obj, list):
            collected_values = []
            for item in current_obj:
                collected_values.extend(_match_recursive(item, remaining_components, full_path_ast_for_error, root_obj_for_path))
            return collected_values
        return []
    
    def _handle_wc_recursive(self, args: List[Any], current_obj: Any, 
                       remaining_components: List[List[Any]], 
                       full_path_ast_for_error: List[List[Any]], 
                       root_obj_for_path: Any) -> List[Any]:
        if args:
            raise PathSyntaxError("'wc_recursive' operation expects no arguments.", 
                                path_segment=["wc_recursive"] + args, 
                                full_path_ast=full_path_ast_for_error)
        
        collected_values = []
        
        # Match current level against remainder
        collected_values.extend(_match_recursive(current_obj, remaining_components, 
                                               full_path_ast_for_error, root_obj_for_path))
        
        # Match children against the wc_recursive op + remainder  
        if remaining_components:  # Only recurse if there are remaining components
            current_recursive_op_and_remainder = [["wc_recursive"]] + remaining_components
            if isinstance(current_obj, dict):
                for v_obj in current_obj.values():
                    collected_values.extend(_match_recursive(v_obj, current_recursive_op_and_remainder, 
                                                           full_path_ast_for_error, root_obj_for_path))
            elif isinstance(current_obj, list):
                for item in current_obj:
                    collected_values.extend(_match_recursive(item, current_recursive_op_and_remainder, 
                                                           full_path_ast_for_error, root_obj_for_path))
        
        return collected_values
    
    def _handle_root(self, args: List[Any], current_obj: Any, 
                    remaining_components: List[List[Any]], 
                    full_path_ast_for_error: List[List[Any]], 
                    root_obj_for_path: Any) -> List[Any]:
        if args:
            raise PathSyntaxError("'root' operation expects no arguments.", path_segment=["root"] + args, full_path_ast=full_path_ast_for_error)
        # Reset current_obj to the absolute root and continue with remaining components
        return _match_recursive(root_obj_for_path, remaining_components, full_path_ast_for_error, root_obj_for_path)

    def _handle_fuzzy_key(self, args: List[Any], current_obj: Any, 
                        remaining_components: List[List[Any]], 
                        full_path_ast_for_error: List[List[Any]], 
                        root_obj_for_path: Any) -> List[Any]:
        if len(args) not in [1, 2, 3]:
            raise PathSyntaxError("'fuzzy_key' operation expects 1 to 3 arguments: key_name, [cutoff], [algorithm].", 
                                path_segment=["fuzzy_key"] + args, 
                                full_path_ast=full_path_ast_for_error)

        if not isinstance(args[0], str):
            raise PathSyntaxError("'fuzzy_key' operation expects a string argument for the key name.", 
                                path_segment=["fuzzy_key"] + args, 
                                full_path_ast=full_path_ast_for_error)
        
        key_name = args[0]
        cutoff = 0.6  # Default fuzzy match cutoff
        algorithm = "difflib"  # Default algorithm
        
        # Parse optional cutoff argument
        if len(args) >= 2:
            if not isinstance(args[1], (float, int)):
                raise PathSyntaxError("'fuzzy_key' operation expects a numeric argument for the cutoff.", 
                                    path_segment=["fuzzy_key"] + args, 
                                    full_path_ast=full_path_ast_for_error)
            cutoff = float(args[1])
            if not (0.0 <= cutoff <= 1.0):
                raise PathSyntaxError("'fuzzy_key' operation expects a cutoff between 0.0 and 1.0.", 
                                    path_segment=["fuzzy_key"] + args, 
                                    full_path_ast=full_path_ast_for_error)
        
        # Parse optional algorithm argument
        if len(args) == 3:
            if not isinstance(args[2], str):
                raise PathSyntaxError("'fuzzy_key' operation expects a string argument for the algorithm.", 
                                    path_segment=["fuzzy_key"] + args, 
                                    full_path_ast=full_path_ast_for_error)
            algorithm = args[2].lower()
            valid_algorithms = ["difflib", "levenshtein", "jaro_winkler", "soundex", "metaphone"]
            if algorithm not in valid_algorithms:
                raise PathSyntaxError(f"'fuzzy_key' operation: unknown algorithm '{algorithm}'. Valid options: {', '.join(valid_algorithms)}.", 
                                    path_segment=["fuzzy_key"] + args, 
                                    full_path_ast=full_path_ast_for_error)

        if isinstance(current_obj, dict):
            collected_values = []
            matches = _fuzzy_match_keys(key_name, list(current_obj.keys()), cutoff, algorithm)
            for matched_key in matches:
                collected_values.extend(_match_recursive(current_obj[matched_key], remaining_components, 
                                                    full_path_ast_for_error, root_obj_for_path))
            return collected_values
        
        return []

# Global dispatcher instance
_path_dispatcher = PathOperationDispatcher()

def _match_recursive(current_obj: Any, components: List[List[Any]], 
                    full_path_ast_for_error: List[List[Any]], 
                    root_obj_for_path: Any) -> List[Any]:
    if current_obj is None and components:
        return []
        
    if not components: 
        return [current_obj]

    component = components[0]
    remaining_components = components[1:]

    if not isinstance(component, list) or not component:
        raise PathSyntaxError("Path component must be a non-empty list.", 
                            path_segment=component, 
                            full_path_ast=full_path_ast_for_error)
    
    op = component[0]
    args = component[1:]
    
    return _path_dispatcher.dispatch(op, args, current_obj, remaining_components, 
                                   full_path_ast_for_error, root_obj_for_path)

def eval_path(path_components_list: List[List[Any]], obj: Any) -> Any:
    """
    Evaluates a path expression against an object and retrieves values.
    Raises PathSyntaxError for malformed path ASTs.
    """
    if not isinstance(path_components_list, list):
        raise PathSyntaxError("Path expression must be a list of components.", full_path_ast=path_components_list) # type: ignore

    if not path_components_list: # Empty path means the object itself
        return obj

    # Validate all components before starting recursion for early failure
    # Validate each component of the path expression
    known_path_ops = {"key", "index", "indices", "slice", "regex_key", "fuzzy_key", "wc_level", "wc_recursive", "root"}
    for component in path_components_list:
        if not isinstance(component, list) or not component or not isinstance(component[0], str):
            raise PathSyntaxError(
                "Each path component must be a non-empty list starting with an operation string.",
                path_segment=component,
                full_path_ast=path_components_list
            )

    matched_values = _match_recursive(obj, path_components_list, full_path_ast_for_error=path_components_list, root_obj_for_path=obj)

    is_specific_path_intent = not _path_has_multi_match_components(path_components_list)

    if is_specific_path_intent:
        if not matched_values:
            return [] 
        elif len(matched_values) == 1:
            return matched_values[0]
        else:
            logger.debug(
                f"Path with specific-intent components yielded {len(matched_values)} results. "
                f"Path: {path_components_list}. Wrapping in PathValues."
            )
            return PathValues(matched_values)
    else:
        return PathValues(matched_values)

def exists(path_components_list: List[List[Any]], obj: Any) -> bool:
    """
    Checks if the given path expression resolves to any value(s) in the object,
    including a literal `None` (null) value at the end of the path.
    Returns `False` only if the path does not lead to any data segment.
    May raise PathSyntaxError if the path_components_list is malformed.
    """
    try:
        resolved_value = eval_path(path_components_list, obj)
        if isinstance(resolved_value, PathValues):
            return len(resolved_value) > 0
        # If eval_path returns an empty list for a specific path (not PathValues),
        # it means the path resolved to nothing, so it doesn't exist.
        elif isinstance(resolved_value, list) and not resolved_value and not _path_has_multi_match_components(path_components_list):
            # This condition is tricky: eval_path might return [] if a specific path didn't find anything.
            # If it was a multi-match path, PathValues([]) would be returned, handled above.
            # If it was a specific path like ["key", "nonexistent"], eval_path returns [].
            # This should mean exists is False.
            return False
        return True 
    except PathSyntaxError: # Re-raise PathSyntaxError as it's an issue with the path itself
        raise
    except Exception: 
        logger.debug(f"Exception during 'exists' check for path {path_components_list}", exc_info=True)
        return False # Other exceptions imply the path didn't resolve or data was incompatible


def is_valid_path_str(path: str) -> bool:
    """
    Checks if the provided path string is a valid JAF path expression.
    Returns True if valid, False otherwise.
    
    Args:
        path (str): The path string to validate.
    Returns:
        bool: True if the path is valid, False otherwise.
    """
    if not isinstance(path, str):
        logger.debug(f"Invalid path type: {type(path)}. Expected a string.")
        return False
    try:
        # Attempt to parse the path string using the JAF path syntax
        string_to_path_ast(path)
        return True
    except PathSyntaxError:
        return False
    except Exception as e:
        logger.debug(f"Unexpected error validating path '{path}': {e}", exc_info=True)
        return False
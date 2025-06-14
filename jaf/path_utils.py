import logging
import re
from typing import Any, List, Optional

class PathSyntaxError(ValueError):
    """
    Custom exception for errors in JAF path syntax.
    Raised when a path string or AST component is malformed or unsupported.
    """
    def __init__(self, message: str, path_segment: Optional[Any] = None, full_path_ast: Optional[List[List[Any]]] = None):
        super().__init__(message)
        self.message = message
        self.path_segment = path_segment
        self.full_path_ast = full_path_ast

    def __str__(self):
        msg = super().__str__()
        if self.path_segment is not None:
            # Try to format AST segment nicely for error messages
            segment_str = f"'{self.path_segment}'"
            if isinstance(self.path_segment, list):
                try:
                    segment_str = path_ast_to_string([self.path_segment])
                except Exception: # Avoid error in error reporting
                    pass # Keep default string representation
            msg += f" (Segment: {segment_str})"
        if self.full_path_ast is not None:
            full_path_str = "N/A"
            try:
                full_path_str = path_ast_to_string(self.full_path_ast)
            except Exception:
                pass
            msg += f" (Full Path AST: {full_path_str})"
        return msg

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

logger = logging.getLogger(__name__)

def _format_slice_part(part: Optional[int]) -> str:
    """Helper to format optional integer parts of a slice component."""
    return str(part) if part is not None else ""

def path_ast_to_string(path_ast: List[List[Any]]) -> str:
    """
    Converts a JAF path AST (list of components) into its string representation.

    Example:
        [["key", "user"], ["index", 0], ["key", "name"]] -> "user[0].name"
        [["wc_recursive"], ["key", "id"]] -> "**.id"
        [["root"], ["key", "config"]] -> "#.config"
    """
    if not path_ast:
        return ""
    
    result_parts: List[str] = []
    for i, component_list_item in enumerate(path_ast):
        if not isinstance(component_list_item, list) or not component_list_item:
            raise PathSyntaxError(f"Invalid AST component format: expected a non-empty list.", path_segment=component_list_item, full_path_ast=path_ast)
        
        op = component_list_item[0]
        args = component_list_item[1:]
        cur_str = ""

        is_accessor = op in ["index", "indices", "slice"] # Operations that don't get a preceding dot

        if i > 0 and not is_accessor:
            # Ensure the previous op also wasn't an accessor that would be followed by a non-accessor
            # This simple check works because accessors are always directly appended.
            # Keys, wildcards, regex, and root will need a separator if not first.
            prev_op = path_ast[i-1][0]
            if prev_op not in ["index", "indices", "slice"]: # Avoid a..[*] or a.~/regex/
                 # Check if previous op was a key-like op.
                 # More robustly, only add dot if current op is key-like and previous was also key-like
                 # or if current op is key-like and it's not the first element.
                 # The current simple `if i > 0:` for key-like ops handles this.
                 pass # Dot is handled by specific key-like ops below

        if op == "key":
            if not (len(args) == 1 and isinstance(args[0], str)):
                raise PathSyntaxError(f"'key' operation expects a single string argument.", path_segment=component_list_item, full_path_ast=path_ast)
            if i > 0 : result_parts.append(".")
            cur_str = args[0]
        elif op == "index":
            if not (len(args) == 1 and isinstance(args[0], int)):
                raise PathSyntaxError(f"'index' operation expects a single integer argument.", path_segment=component_list_item, full_path_ast=path_ast)
            cur_str = f"[{args[0]}]"
        elif op == "indices":
            if not (len(args) == 1 and isinstance(args[0], list) and all(isinstance(idx, int) for idx in args[0])):
                raise PathSyntaxError(f"'indices' operation expects a single list of integers argument.", path_segment=component_list_item, full_path_ast=path_ast)
            cur_str = f"[{','.join(map(str, args[0]))}]"
        elif op == "slice":
            if not (1 <= len(args) <= 3 and (args[0] is None or isinstance(args[0], int)) and \
                    (len(args) < 2 or args[1] is None or isinstance(args[1], int)) and \
                    (len(args) < 3 or args[2] is None or isinstance(args[2], int))):
                raise PathSyntaxError(f"'slice' operation expects 1 to 3 integer or None arguments for start, stop, step.", path_segment=component_list_item, full_path_ast=path_ast)
            start = args[0] 
            stop = args[1] if len(args) > 1 else None
            step = args[2] if len(args) > 2 else None

            s_start = _format_slice_part(start)
            s_stop = _format_slice_part(stop)
            
            if start is None and stop is None and (step is None or step == 1):
                cur_str = "[:]" 
            elif step is None or step == 1: 
                cur_str = f"[{s_start}:{s_stop}]"
            else: 
                s_step = _format_slice_part(step)
                cur_str = f"[{s_start}:{s_stop}:{s_step}]"
        elif op == "regex_key":
            if not (len(args) == 1 and isinstance(args[0], str)):
                raise PathSyntaxError(f"'regex_key' operation expects a single string pattern argument.", path_segment=component_list_item, full_path_ast=path_ast)
            if i > 0 : result_parts.append(".")
            cur_str = f"~/{args[0]}/"
        elif op == "wc_level":
            if args:
                raise PathSyntaxError(f"'wc_level' operation expects no arguments.", path_segment=component_list_item, full_path_ast=path_ast)
            if i > 0 : result_parts.append(".")
            cur_str = "[*]"
        elif op == "wc_recursive":
            if args:
                raise PathSyntaxError(f"'wc_recursive' operation expects no arguments.", path_segment=component_list_item, full_path_ast=path_ast)
            if i > 0 : result_parts.append(".")
            cur_str = "**"
        elif op == "root":
            if args:
                raise PathSyntaxError(f"'root' operation expects no arguments.", path_segment=component_list_item, full_path_ast=path_ast)
            if i > 0 : result_parts.append(".")
            cur_str = "#" # Using '#' to represent the root operator
        else:
            raise PathSyntaxError(f"Unknown JAF path component operation: '{op}'", path_segment=component_list_item, full_path_ast=path_ast)

        result_parts.append(cur_str)

    return "".join(result_parts)

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

def _match_recursive(current_obj: Any, components: List[List[Any]], full_path_ast_for_error: List[List[Any]], root_obj_for_path: Any) -> List[Any]:
    """
    Internal recursive engine for `eval_path`.
    It interprets the path 'components' against the 'current_obj'.
    `root_obj_for_path` is the absolute root object for the entire path evaluation.
    Returns a flat list of all resolved values.
    Raises PathSyntaxError for malformed AST components.
    """
    if current_obj is None and components:
        return []
        
    if not components: 
        return [current_obj]

    component = components[0]
    remaining_components = components[1:]

    if not isinstance(component, list) or not component:
        raise PathSyntaxError("Path component must be a non-empty list.", path_segment=component, full_path_ast=full_path_ast_for_error)
    
    op = component[0]
    args = component[1:]
    
    collected_values = []

    if op == "key":
        if not (len(args) == 1 and isinstance(args[0], str)):
            raise PathSyntaxError("'key' operation expects a single string argument.", path_segment=component, full_path_ast=full_path_ast_for_error)
        key_name = args[0]
        if isinstance(current_obj, dict) and key_name in current_obj:
            collected_values.extend(_match_recursive(current_obj[key_name], remaining_components, full_path_ast_for_error, root_obj_for_path))
    
    elif op == "index":
        if not (len(args) == 1 and isinstance(args[0], int)):
            raise PathSyntaxError("'index' operation expects a single integer argument.", path_segment=component, full_path_ast=full_path_ast_for_error)
        idx_val = args[0]
        if isinstance(current_obj, list):
            if -len(current_obj) <= idx_val < len(current_obj):
                 collected_values.extend(_match_recursive(current_obj[idx_val], remaining_components, full_path_ast_for_error, root_obj_for_path))
    
    elif op == "indices":
        if not (len(args) == 1 and isinstance(args[0], list) and all(isinstance(idx, int) for idx in args[0])):
            raise PathSyntaxError("'indices' operation expects a single list of integers argument.", path_segment=component, full_path_ast=full_path_ast_for_error)
        idx_list = args[0]
        if isinstance(current_obj, list):
            for idx_val in idx_list:
                if isinstance(idx_val, int) and -len(current_obj) <= idx_val < len(current_obj):
                    collected_values.extend(_match_recursive(current_obj[idx_val], remaining_components, full_path_ast_for_error, root_obj_for_path))
    
    elif op == "slice":
        if not (1 <= len(args) <= 3):
            raise PathSyntaxError("'slice' operation expects 1 to 3 arguments for start, stop, step.", path_segment=component, full_path_ast=full_path_ast_for_error)

        start_val = args[0]
        stop_val = args[1] if len(args) > 1 else None
        step_val = args[2] if len(args) > 2 else None
        
        if not (start_val is None or isinstance(start_val, int)):
            raise PathSyntaxError("Slice start must be an integer or null.", path_segment=component, full_path_ast=full_path_ast_for_error)
        if not (stop_val is None or isinstance(stop_val, int)):
            raise PathSyntaxError("Slice stop must be an integer or null.", path_segment=component, full_path_ast=full_path_ast_for_error)
        
        actual_step = step_val if step_val is not None else 1
        if not (isinstance(actual_step, int) and actual_step != 0):
            raise PathSyntaxError("Slice step must be a non-zero integer.", path_segment=component, full_path_ast=full_path_ast_for_error)

        if isinstance(current_obj, list):
            try:
                s = slice(start_val, stop_val, actual_step)
                sliced_items = current_obj[s]
                for item in sliced_items:
                    collected_values.extend(_match_recursive(item, remaining_components, full_path_ast_for_error, root_obj_for_path))
            except (TypeError, ValueError) as e: # Should be rare if AST validation is correct
                logger.debug(f"Error during slicing for {current_obj} with slice({start_val},{stop_val},{actual_step}): {e}")
                
    elif op == "regex_key":
        if not (len(args) == 1 and isinstance(args[0], str)):
            raise PathSyntaxError("'regex_key' operation expects a single string pattern argument.", path_segment=component, full_path_ast=full_path_ast_for_error)
        pattern_str = args[0]
        try:
            regex = re.compile(pattern_str)
            if isinstance(current_obj, dict):
                for k, v_obj in current_obj.items():
                    if regex.match(k):
                        collected_values.extend(_match_recursive(v_obj, remaining_components, full_path_ast_for_error, root_obj_for_path))
        except re.error as e:
            raise PathSyntaxError(f"Invalid regex pattern in path component: '{pattern_str}'. Error: {e}", path_segment=component, full_path_ast=full_path_ast_for_error)
            
    elif op == "wc_level":
        if args:
            raise PathSyntaxError("'wc_level' operation expects no arguments.", path_segment=component, full_path_ast=full_path_ast_for_error)
        if isinstance(current_obj, dict):
            for v_obj in current_obj.values():
                collected_values.extend(_match_recursive(v_obj, remaining_components, full_path_ast_for_error, root_obj_for_path))
        elif isinstance(current_obj, list):
            for item in current_obj:
                collected_values.extend(_match_recursive(item, remaining_components, full_path_ast_for_error, root_obj_for_path))
                
    elif op == "wc_recursive":
        if args:
            raise PathSyntaxError("'wc_recursive' operation expects no arguments.", path_segment=component, full_path_ast=full_path_ast_for_error)
        # Match current level against remainder
        collected_values.extend(_match_recursive(current_obj, remaining_components, full_path_ast_for_error, root_obj_for_path))
        # Match children against the wc_recursive op + remainder
        current_recursive_op_and_remainder = [component] + remaining_components
        if isinstance(current_obj, dict):
            for v_obj in current_obj.values():
                collected_values.extend(_match_recursive(v_obj, current_recursive_op_and_remainder, full_path_ast_for_error, root_obj_for_path))
        elif isinstance(current_obj, list):
            for item in current_obj:
                collected_values.extend(_match_recursive(item, current_recursive_op_and_remainder, full_path_ast_for_error, root_obj_for_path))
    elif op == "root":
        if args:
            raise PathSyntaxError("'root' operation expects no arguments.", path_segment=component, full_path_ast=full_path_ast_for_error)
        # Reset current_obj to the absolute root and continue with remaining components
        collected_values.extend(_match_recursive(root_obj_for_path, remaining_components, full_path_ast_for_error, root_obj_for_path))
    else:
        raise PathSyntaxError(f"Unknown path operation code encountered: '{op}'", path_segment=component, full_path_ast=full_path_ast_for_error)

    return collected_values

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


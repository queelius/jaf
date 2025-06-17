"""
JAF Path Evaluation
===================

This module contains the core path evaluation functions for JAF.
It provides the main interface for evaluating path expressions against data objects.
"""

import logging
from typing import Any, List

from .path_conversion import path_ast_to_string, string_to_path_ast
from .path_exceptions import PathSyntaxError
from .path_types import PathValues
from .path_operations import PathOperationDispatcher

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
            continue 
        op = component_list_item[0]
        if op in ["indices", "slice", "regex_key", "fuzzy_key", "wc_level", "wc_recursive"]:
            return True
    return False


# Global dispatcher instance
_path_dispatcher = PathOperationDispatcher()


def _match_recursive(current_obj: Any, components: List[List[Any]], 
                    full_path_ast_for_error: List[List[Any]], 
                    root_obj_for_path: Any) -> List[Any]:
    """
    Recursively match path components against the current object.
    
    Args:
        current_obj: The current object being traversed
        components: Remaining path components to process
        full_path_ast_for_error: Complete path AST for error reporting
        root_obj_for_path: The root object for this path evaluation
        
    Returns:
        List of matching values
    """
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
    
    This is the main function for path evaluation in JAF. It takes a path
    expression in AST format and evaluates it against the provided object.
    
    Args:
        path_components_list: List of path components in AST format
        obj: The object to evaluate the path against
        
    Returns:
        - For specific paths: the value at that path, or [] if not found
        - For multi-match paths: PathValues containing all matching values
        
    Raises:
        PathSyntaxError: For malformed path ASTs
    """
    if not isinstance(path_components_list, list):
        raise PathSyntaxError("Path expression must be a list of components.", full_path_ast=path_components_list) # type: ignore

    if not path_components_list: # Empty path means the object itself
        return obj

    # Validate all components before starting recursion for early failure
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
    Checks if the given path expression resolves to any value(s) in the object.
    
    This function determines whether a path exists in the data, including
    paths that resolve to None/null values.
    
    Args:
        path_components_list: List of path components in AST format
        obj: The object to check the path against
        
    Returns:
        True if the path exists (even if the value is None), False otherwise
        
    Raises:
        PathSyntaxError: For malformed path ASTs
    """
    try:
        resolved_value = eval_path(path_components_list, obj)
        if isinstance(resolved_value, PathValues):
            return len(resolved_value) > 0
        # If eval_path returns an empty list for a specific path (not PathValues),
        # it means the path resolved to nothing, so it doesn't exist.
        elif isinstance(resolved_value, list) and not resolved_value and not _path_has_multi_match_components(path_components_list):
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
    
    Args:
        path: The path string to validate
        
    Returns:
        True if the path is valid, False otherwise
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

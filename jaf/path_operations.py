"""
JAF Path Operations
===================

This module contains the path operation dispatcher and all path operation handlers.
"""

import logging
import re
from typing import Any, List
import rapidfuzz.distance as distance
import fuzzy

from .path_exceptions import PathSyntaxError

logger = logging.getLogger(__name__)


def _fuzzy_match_keys(target_key: str, available_keys: List[str], cutoff: float, algorithm: str) -> List[str]:
    """
    Find keys that fuzzy match the target key.
    
    Args:
        target_key: The key to match against
        available_keys: List of available keys to search
        cutoff: Minimum similarity score (0.0 to 1.0)
        algorithm: Algorithm to use ('difflib', 'levenshtein', 'jaro_winkler', 'soundex', 'metaphone')
    
    Returns:
        List of matching keys
    """
    if algorithm == "difflib":
        import difflib
        matches = difflib.get_close_matches(target_key, available_keys, cutoff=cutoff)
        return matches
    elif algorithm == "levenshtein":
        matches = []
        for key in available_keys:
            similarity = 1 - (distance.Levenshtein.distance(target_key, key) / max(len(target_key), len(key)))
            if similarity >= cutoff:
                matches.append(key)
        return matches
    elif algorithm == "jaro_winkler":
        matches = []
        for key in available_keys:
            similarity = distance.JaroWinkler.similarity(target_key, key)
            if similarity >= cutoff:
                matches.append(key)
        return matches
    elif algorithm == "metaphone":
        target_metaphone = fuzzy.DMetaphone()(target_key)[0]
        matches = []
        for key in available_keys:
            key_metaphone = fuzzy.DMetaphone()(key)[0]
            if target_metaphone and target_metaphone == key_metaphone:
                matches.append(key)
        return matches
    else:
        raise ValueError(f"Unknown fuzzy matching algorithm: {algorithm}")

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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        
        return []
    
    def _handle_wc_level(self, args: List[Any], current_obj: Any, 
                        remaining_components: List[List[Any]], 
                        full_path_ast_for_error: List[List[Any]], 
                        root_obj_for_path: Any) -> List[Any]:
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
        if args:
            raise PathSyntaxError("'root' operation expects no arguments.", path_segment=["root"] + args, full_path_ast=full_path_ast_for_error)
        # Reset current_obj to the absolute root and continue with remaining components
        return _match_recursive(root_obj_for_path, remaining_components, full_path_ast_for_error, root_obj_for_path)
    
    def _handle_fuzzy_key(self, args: List[Any], current_obj: Any, 
                        remaining_components: List[List[Any]], 
                        full_path_ast_for_error: List[List[Any]], 
                        root_obj_for_path: Any) -> List[Any]:
        # Import here to avoid circular imports
        from .path_evaluation import _match_recursive
        
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

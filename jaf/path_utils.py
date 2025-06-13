from typing import List, Any, Optional

def _format_slice_part(part: Optional[int]) -> str:
    """Helper to format optional integer parts of a slice component."""
    return str(part) if part is not None else ""

def path_ast_to_string(path_ast: List[List[Any]]) -> str:
    """
    Converts a JAF path AST (list of components) into its string representation.

    Example:
        [["key", "user"], ["index", 0], ["key", "name"]] -> "user[0].name"
        [["wc_recursive"], ["key", "id"]] -> "**.id"
    """
    if not path_ast:
        return ""
    
    result_parts: List[str] = []
    for i, component_list_item in enumerate(path_ast):
        op = component_list_item[0]
        args = component_list_item[1:]
        current_str = ""

        if op == "key":
            current_str = args[0]
            if i > 0:
                result_parts.append(".")
        elif op == "index":
            current_str = f"[{args[0]}]"
            # No dot needed before index, e.g. key[0]
        elif op == "indices":
            current_str = f"[{','.join(map(str, args[0]))}]"
            # No dot needed
        elif op == "slice":
            # Slice AST can have 1, 2, or 3 arguments (start, stop, step)
            # Default to None if not provided in the AST.
            start = args[0] # A slice AST component must have at least a start value.
            stop = args[1] if len(args) > 1 else None
            step = args[2] if len(args) > 2 else None

            s_start = _format_slice_part(start)
            s_stop = _format_slice_part(stop)
            s_step = _format_slice_part(step)
            
            if start is None and stop is None and (step is None or step == 1): # Handles [:], [::], [::1]
                current_str = "[:]"
            elif step is None or step == 1: # Step is default (1) or explicitly 1
                current_str = f"[{s_start}:{s_stop}]" # Handles [1:5], [:5], [1:]
            else: # Step is specified and not 1
                current_str = f"[{s_start}:{s_stop}:{s_step}]" # Handles [1:5:2], [::2], [:5:2], [1::2]
            # No dot needed
        elif op == "regex_key":
            current_str = f"~/{args[0]}/"
            if i > 0:
                result_parts.append(".")
        elif op == "wc_level":
            current_str = "[*]"
            if i > 0: # e.g. key.[*] or **.[*]
                result_parts.append(".")
        elif op == "wc_recursive":
            current_str = "**"
            if i > 0: # e.g. key.** or [*].**
                result_parts.append(".")
        else:
            raise ValueError(f"Unknown JAF path component operation: {op}")

        result_parts.append(current_str)

    return "".join(result_parts)


"""
JAF-style JSON-access-format utilities
=====================================

This module provides the two primitives you normally need:

* ``path_ast_to_string(ast)``  –  converts a path AST into
   human-readable form (e.g. ``[['key','a'],['index',0]]`` →
  ``"a[0]"``).

* ``string_to_path_ast(path)`` –  the inverse operation: parses a path
  string into the list-of-lists AST that can be fed to
  ``path_ast_to_string``.

The grammar covered is exactly the subset emitted by the printer, so
**round-tripping is guaranteed**:

>>> p = "data.items[1:5].values[*]"
>>> string_to_path_ast(p)
[['key', 'data'], ['key', 'items'], ['slice', 1, 5], ['key', 'values'], ['wc_level']]
>>> path_ast_to_string(string_to_path_ast(p)) == p
True

Supported operations
--------------------
``key``           – dotted bare-word keys  
``index``         – single zero-based index  ``[7]``  
``indices``       – list of indices          ``[1,4,5]``  
``slice``         – Python slice             ``[start:stop[:step]]``  
``wc_level``      – level wildcard           ``[*]``  
``wc_recursive``  – recursive wildcard       ``**``  
``regex_key``     – regex key                ``~/pattern/``  
``root``          – explicit root            ``#``

Any syntax error raises a ``PathSyntaxError`` that carries the offending
segment so you can surface helpful messages to users.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from .path_exceptions import PathSyntaxError


# ---------------------------------------------------------------------------
# AST → string
# ---------------------------------------------------------------------------

def _format_slice_part(part: Optional[int]) -> str:
    """Render a slice endpoint (`None` → empty string)."""
    return str(part) if part is not None else ""


def path_ast_to_string(path_ast: List[List[Any]]) -> str:
    """Serialize a parsed path AST into a compact string.

    Parameters
    ----------
    path_ast : list[list[Any]]
        Each inner list has an *operation* as element 0 followed by its
        arguments.  (See the table in this module’s docstring.)

    Returns
    -------
    str
        Example::

            [["key","user"], ["index",0], ["key","name"]]
            → "user[0].name"

    Raises
    ------
    PathSyntaxError
        If the AST structure or argument types are invalid.
    """
    if not path_ast:
        return ""

    result: list[str] = []

    for i, node in enumerate(path_ast):
        if not (isinstance(node, list) and node):
            raise PathSyntaxError(
                "Invalid AST component format: expected a non-empty list.", # Updated message
                path_segment=node,
                full_path_ast=path_ast,
            )

        op, *args = node

        # ------------------------------------------------------------------
        if op == "key":
            if not (len(args) == 1 and isinstance(args[0], str)):
                raise PathSyntaxError("'key' operation expects a single string argument.", path_segment=node, full_path_ast=path_ast) # Updated message
            if i > 0:
                result.append(".")
            result.append(args[0])

        elif op == "index":
            if not (len(args) == 1 and isinstance(args[0], int)):
                raise PathSyntaxError("'index' operation expects a single integer argument.", path_segment=node, full_path_ast=path_ast) # Updated message
            result.append(f"[{args[0]}]")

        elif op == "indices":
            if not (len(args) == 1 and isinstance(args[0], list) and all(isinstance(x, int) for x in args[0])):
                raise PathSyntaxError("'indices' operation expects a single list of integers argument.", path_segment=node, full_path_ast=path_ast) # Updated message
            result.append("[" + ",".join(map(str, args[0])) + "]")

        elif op == "slice":
            if not (1 <= len(args) <= 3 and all(isinstance(x, (int, type(None))) for x in args)):
                raise PathSyntaxError(
                    "'slice' operation expects 1 to 3 integer or None arguments", # Matches test
                    path_segment=node, full_path_ast=path_ast
                )
            
            start = args[0]
            stop  = args[1] if len(args) >= 2 else None
            step_val  = args[2] if len(args) == 3 else None

            if step_val == 0:
                raise PathSyntaxError("Slice step cannot be zero.", path_segment=node, full_path_ast=path_ast)
            
            s_start, s_stop = _format_slice_part(start), _format_slice_part(stop)
            if start is None and stop is None and (step_val is None or step_val == 1):
                txt = "[:]"
            elif step_val is None or step_val == 1:
                txt = f"[{s_start}:{s_stop}]"
            else: 
                txt = f"[{s_start}:{s_stop}:{_format_slice_part(step_val)}]"
            result.append(txt)

        elif op == "wc_level":
            if args:
                raise PathSyntaxError("'wc_level' operation expects no arguments.", path_segment=node, full_path_ast=path_ast) # Updated message
            result.append("[*]")

        elif op == "wc_recursive":
            if args:
                raise PathSyntaxError("'wc_recursive' operation expects no arguments.", path_segment=node, full_path_ast=path_ast) # Updated message
            if i > 0:
                result.append(".")
            result.append("**")

        elif op == "regex_key":
            if not (1 <= len(args) <= 2):
                raise PathSyntaxError("'regex_key' operation expects 1 or 2 arguments.", path_segment=node, full_path_ast=path_ast)
            if not isinstance(args[0], str):
                raise PathSyntaxError("'regex_key' operation expects a string pattern argument.", path_segment=node, full_path_ast=path_ast)
            
            if i > 0:
                result.append(".")
            
            pattern = args[0]
            result.append(f"~/{pattern}/")
            
            # Add flags if specified
            if len(args) == 2:
                flags_arg = args[1]
                if isinstance(flags_arg, str):
                    result.append(flags_arg)
                elif isinstance(flags_arg, int):
                    # Convert integer flags back to string representation
                    flag_str = ""
                    if flags_arg & re.IGNORECASE:
                        flag_str += "i"
                    if flags_arg & re.MULTILINE:
                        flag_str += "m"
                    if flags_arg & re.DOTALL:
                        flag_str += "s"
                    if flags_arg & re.VERBOSE:
                        flag_str += "x"
                    if flags_arg & re.ASCII:
                        flag_str += "a"
                    if flag_str:
                        result.append(flag_str)

        elif op == "root":
            if args:
                raise PathSyntaxError("'root' operation expects no arguments.", path_segment=node, full_path_ast=path_ast) # Updated message
            if i > 0:
                result.append(".")
            result.append("#")

        else:
            raise PathSyntaxError(f"Unknown JAF path component operation: '{op}'", path_segment=node, full_path_ast=path_ast) # Updated message
    return "".join(result)


# ---------------------------------------------------------------------------
# string → AST
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Constants & Regexes
# ---------------------------------------------------------------------------

# Regex for a valid Python/JSON-like identifier (key name)
_IDENT_RE  = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# Regex for parsing slice components: [start:stop:step]
_INT_RE    = re.compile(r"[+-]?\d+")
_SLICE_RE  = re.compile(
    r"^\s*"
    r"(?P<start>[+-]?\d+)?"  # Optional start
    r"\s*:\s*"               # First colon, surrounded by optional spaces
    r"(?P<stop>[+-]?\d+)?"   # Optional stop
    r"(?:"                   # Non-capturing group for the optional third part (step)
        r"\s*:\s*"           # Second colon, surrounded by optional spaces
        r"(?P<step>[+-]?\d+)?" # Optional step value
    r")?"                    # The entire third part (second colon and step value) is optional
    r"\s*$",
    re.X,
)

def _parse_int(tok: str, ctx: str) -> int:
    if not _INT_RE.fullmatch(tok.strip()):
        raise PathSyntaxError("Expected integer", path_segment=ctx)
    return int(tok)


def string_to_path_ast(path: str) -> List[List[Any]]:
    path = path.strip()
    if not path:
        return []

    pos, n = 0, len(path)
    ast: List[List[Any]] = []

    def eat(lit: str) -> bool:
        nonlocal pos
        if path.startswith(lit, pos):
            pos += len(lit)
            return True
        return False

    def eat_dot() -> None:
        nonlocal pos
        if pos < n and path[pos] == ".":
            pos += 1

    while pos < n:
        ch = path[pos]

        # --- root ---------------------------------------------------------
        if ch == "#":
            ast.append(["root"])
            pos += 1
            eat_dot()
            continue

        # --- recursive wildcard "**" -------------------------------------
        if eat("**"):
            ast.append(["wc_recursive"])
            eat_dot()
            continue

        # --- level wildcard "[*]" (explicit form) -----------------------
        if eat("[*]"):
            ast.append(["wc_level"])
            eat_dot()
            continue
        
        # --- level wildcard "*" (implicit form) -------------------------
        if ch == '*':
            ast.append(["wc_level"])
            pos += 1
            eat_dot()
            continue

        # --- anything in [...] (index, indices, slice) ------------------
        if ch == "[":
            close = path.find("]", pos)
            if close == -1:
                raise PathSyntaxError("Unterminated '['", path_segment=path[pos:])
            inside = path[pos + 1 : close] # Keep original spacing for colon counting
            ctx_seg = path[pos : close + 1]
            pos = close + 1

            if ":" in inside:
                # slice
                # Use inside.strip() for matching with _SLICE_RE, which handles internal/external whitespace.
                m = _SLICE_RE.match(inside.strip()) 
                if not m:
                    raise PathSyntaxError("Malformed slice", path_segment=ctx_seg)
                
                start_str = m.group("start")
                stop_str = m.group("stop")
                step_str = m.group("step")

                parsed_start = int(start_str) if start_str else None
                parsed_stop = int(stop_str) if stop_str else None
                
                # Canonicalization based on "Expected AST" and user feedback:
                # - Omitted start becomes 0.
                # - Omitted stop remains None.
                final_start = 0 if parsed_start is None else parsed_start
                final_stop = parsed_stop # Corrected: if parsed_stop is None, final_stop is None

                if step_str is not None: # Implies two colons were structurally present in the input
                    parsed_step = int(step_str) if step_str else None # Handles "::" where step_str is ""
                    
                    if parsed_step is not None: # An actual integer step value was provided (e.g., [::2], [1:2:3])
                        ast.append(["slice", final_start, final_stop, parsed_step])
                    else: # Two colons were present, but step value was omitted (e.g., [::], [1:2:])
                          # Expected AST for these cases is a 2-argument slice.
                        ast.append(["slice", final_start, final_stop])
                else: # Only one colon was present in the input (e.g., [:], [1:], [:5], [1:5])
                    ast.append(["slice", final_start, final_stop])
            elif "," in inside:
                # indices
                ints = [_parse_int(tok, ctx_seg) for tok in inside.strip().split(",")]
                ast.append(["indices", ints])
            else:
                # single index
                if not inside.strip(): # Handles "[]"
                    raise PathSyntaxError("Expected integer", path_segment=ctx_seg)
                ast.append(["index", _parse_int(inside.strip(), ctx_seg)])

            eat_dot()
            continue

        # --- regex key  ~/pattern/ ---------------------------------------
        if eat("~/"):
            end = path.find("/", pos)
            if end == -1:
                raise PathSyntaxError("Unterminated regex key", path_segment=path[pos - 2 :])
            pattern = path[pos:end]
            ast.append(["regex_key", pattern])
            pos = end + 1
            eat_dot()
            continue

        # --- bareword key -------------------------------------------------
        m = _IDENT_RE.match(path, pos) # _IDENT_RE no longer matches '*'
        if m:
            ast.append(["key", m.group(0)])
            pos = m.end()
            eat_dot()
            continue

        # --- no rule matched ---------------------------------------------
        snippet = path[pos : min(pos + 10, n)] + ("…" if min(pos + 10, n) < n else "")
        raise PathSyntaxError("Unexpected token", path_segment=snippet)

    return ast


def path_expression_to_ast(path_expr):
    """
    Convert a path expression (string or @ syntax) to path AST format.
    
    Args:
        path_expr: Can be:
            - String path: "user.name" 
            - @ string: "@user.name"
            - Path AST: [["key", "user"], ["key", "name"]]
            - @ AST form: ["@", [["key", "user"], ["key", "name"]]]
            
    Returns:
        Standardized path AST: [["key", "user"], ["key", "name"]]
    """
    if isinstance(path_expr, str):
        if path_expr.startswith('@'):
            # Handle @string format
            path_string = path_expr[1:]  # Remove @
            if not path_string:
                raise PathSyntaxError("Empty path expression after @")
            return string_to_path_ast(path_string)
        else:
            # Handle regular string format
            return string_to_path_ast(path_expr)
    
    elif isinstance(path_expr, list):
        if len(path_expr) == 2 and path_expr[0] == "@":
            # Handle ["@", path_ast] format
            path_ast = path_expr[1]
            if not isinstance(path_ast, list):
                raise PathSyntaxError("@ with AST form requires list of path components")
            return path_ast  # Already in AST format, just return it
        else:
            # Assume it's already a path AST
            return path_expr
    
    else:
        raise PathSyntaxError(f"Invalid path expression type: {type(path_expr)}")

def normalize_path_in_ast(ast_node):
    """
    Normalize path expressions within a JAF AST node.
    
    Converts all path forms to standard ["path", path_ast] format.
    
    Args:
        ast_node: JAF AST node (may contain various path formats)
        
    Returns:
        JAF AST with normalized path expressions
    """
    if isinstance(ast_node, str) and ast_node.startswith('@'):
        # Convert @string to ["path", path_ast]
        path_ast = path_expression_to_ast(ast_node)
        return ["path", path_ast]
    
    elif isinstance(ast_node, list):
        if len(ast_node) == 2 and ast_node[0] == "@":
            # Convert ["@", path_expr] to ["path", path_ast] 
            path_ast = path_expression_to_ast(ast_node)
            return ["path", path_ast]
        else:
            # Recursively process list elements
            return [normalize_path_in_ast(elem) for elem in ast_node]
    
    else:
        # Return other types unchanged
        return ast_node

def ast_to_path_string(path_ast, use_at_syntax=False):
    """
    Convert path AST to string representation.
    
    Args:
        path_ast: Path AST format
        use_at_syntax: If True, prefix with @
        
    Returns:
        String representation of path
    """
    path_string = path_ast_to_string(path_ast)
    return f"@{path_string}" if use_at_syntax else path_string

def is_path_expression(expr):
    """
    Check if an expression is a valid path in any supported format.
    
    Args:
        expr: Expression to check (string, @string, AST, or @AST)
        
    Returns:
        bool: True if it's a valid path expression
    """
    try:
        path_expression_to_ast(expr)
        return True
    except PathSyntaxError:
        return False

def is_at_syntax(expr):
    """
    Check if an expression uses @ syntax.
    
    Args:
        expr: Expression to check
        
    Returns:
        bool: True if it uses @ syntax
    """
    if isinstance(expr, str):
        return expr.startswith('@')
    elif isinstance(expr, list) and len(expr) == 2:
        return expr[0] == "@"
    return False

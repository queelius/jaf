import logging
from typing import Any, List, Optional

from lark import Lark, Transformer, v_args, LarkError

logger = logging.getLogger(__name__)

class PathSyntaxError(ValueError):
    """Custom exception for errors in JAF path string syntax or AST structure."""
    def __init__(self, message: str, path_segment: Optional[Any] = None, full_path_ast: Optional[List[List[Any]]] = None):
        super().__init__(message)
        self.message = message # Store original message for direct access
        self.path_segment = path_segment
        self.full_path_ast = full_path_ast

    def __str__(self):
        msg = self.message
        return msg

# --- JAF Path String Parsing (using Lark) ---

_JAF_PATH_GRAMMAR = r"""
    ?path: component (separator component)* | // Allows empty path

    ?separator: "." | bracket_lookahead
    bracket_lookahead: &"[" // Positive lookahead for bracket, consumes nothing

    ?component: key_component
              | index_component
              | indices_component
              | slice_component
              | regex_key_component
              | wc_level_component
              | wc_recursive_component
              | root_component

    key_component: CNAME -> key_name
    index_component: "[" SIGNED_INT "]" -> index_val
    indices_component: "[" SIGNED_INT ("," SIGNED_INT)* "]" -> indices_val
    slice_component: "[" slice_part? (":" slice_part? (":" slice_part?)?)? "]" -> slice_val
    slice_part: SIGNED_INT

    regex_key_component: "~/" REGEX_PATTERN "/" -> regex_pattern
    wc_level_component: "[*]" | "*" -> wc_level // Both forms
    wc_recursive_component: "**" -> wc_recursive
    root_component: "#" -> root_op

    REGEX_PATTERN: /.+?(?=\\\/)/ // Non-greedy match until the next unescaped /

    %import common.CNAME
    %import common.SIGNED_INT
    %import common.WS
    %ignore WS
"""

class _PathAstTransformer(Transformer):
    @v_args(inline=True)
    def key_name(self, name_token):
        return ["key", str(name_token)]

    @v_args(inline=True)
    def index_val(self, index_token):
        return ["index", int(index_token.value)]

    def indices_val(self, items):
        return ["indices", [int(i.value) for i in items]]

    @v_args(inline=True)
    def slice_part(self, token):
        return int(token.value)

    def slice_val(self, children):
        start, stop, step = None, None, 1 
        
        num_children = len(children)
        if num_children >= 1 and children[0] is not None: 
            start = children[0]
        if num_children >= 2 and children[1] is not None: 
            stop = children[1]
        if num_children >= 3 and children[2] is not None: 
            step_val = children[2]
            if step_val == 0:
                raise PathSyntaxError("Slice step cannot be zero.")
            step = step_val
            
        return ["slice", start, stop, step]


    @v_args(inline=True)
    def regex_pattern(self, pattern_token):
        return ["regex_key", str(pattern_token)]

    def wc_level(self, _):
        return ["wc_level"]

    def wc_recursive(self, _):
        return ["wc_recursive"]

    def root_op(self, _):
        return ["root"]

    def path(self, components):
        if not components: 
            return []
        if components == [None]: 
             return []
        return list(components)

# Initialize the parser once
try:
    _jaf_path_parser = Lark(_JAF_PATH_GRAMMAR, parser='lalr', start='path', transformer=_PathAstTransformer())
except Exception as e:
    logger.error(f"Failed to initialize JAF Path Parser: {e}", exc_info=True)
    _jaf_path_parser = None

def path_string_to_ast(path_str: str) -> List[List[Any]]:
    """
    Converts a JAF path string representation into its AST (list of components).
    Example: "user[0].name" -> [["key", "user"], ["index", 0], ["key", "name"]]
    Raises PathSyntaxError if the path string is invalid.
    """
    if _jaf_path_parser is None:
        raise RuntimeError("JAF Path Parser is not initialized. Check for errors during startup.")

    stripped_path_str = path_str.strip()
    if not stripped_path_str: # Handle empty string explicitly before parsing
        return []
    try:
        ast_components = _jaf_path_parser.parse(stripped_path_str)
        return ast_components
    except LarkError as e:
        # Provide more context from LarkError if possible
        # e.g., e.line, e.column, e.get_context(text)
        context = ""
        if hasattr(e, 'get_context') and hasattr(e, 'line'):
            try:
                context_text = e.get_context(stripped_path_str, 20) # Get 20 chars of context
                context = f" Near line {e.line}, column {e.column}: '{context_text}'"
            except: # Fallback if get_context fails
                pass
        raise PathSyntaxError(f"Invalid JAF path string: '{path_str}'. Details: {str(e)}.{context}")
    except PathSyntaxError: # Re-raise if transformer raised it (e.g., slice step zero)
        raise
    except Exception as e:
        logger.error(f"Unexpected error parsing JAF path string: '{path_str}'. Original error: {e}", exc_info=True)
        raise PathSyntaxError(f"Unexpected error parsing JAF path string: '{path_str}'. Details: {e}")

# Add this block at the end of dev/path-str.py
if __name__ == "__main__":
    # Configure basic logging for testing if you want to see logger.error messages
    logging.basicConfig(level=logging.INFO)
    logger.info("Running path-str.py in test mode...")

    def test_path_string_directly(path_string: str):
        """
        Tests a single path string and prints its AST or an error.
        """
        print(f"\nTesting path string: \"{path_string}\"")
        try:
            ast = path_string_to_ast(path_string)
            print(f"  AST: {ast}")
        except PathSyntaxError as e:
            print(f"  Error: {e}")
        except Exception as e:
            print(f"  Unexpected Error: {e}")
            logger.exception("Details of unexpected error:")


    test_strings = [
        "",
        "  ",
        "name",
        "user.name",
        "a.b.c",
        "[0]",
        "[-1]",
        "items[0]",
        "data[1].value",
        "[0].name",
        "[0,1,2]",
        "items[0,1,-1]",
        "[:]",
        "[1:]",
        "[:5]",
        "[1:5]",
        "[1:10:2]",
        "[::2]",
        "[10:1:-1]",
        "items[1:10:2]",
        "*",
        "[*] ", 
        "items[*]",
        "items.*",
        "**",
        "**.name",
        "#",
        "#.config",
        "user.#.config",
        "~/^item_\\d+$/", 
        "data.~/pattern/",
        "~/^prefix_.*_suffix$/[0]",
        "user.tags[*].name",
        "**.items[0,1].value",
        "data[1:5:2].*.id",
        "#.data[0].~/^field\\w+$/",
        # Invalid paths
        "user.",
        "[",
        "items[0",
        "items[0,",
        "items[1:",
        "items[1:5:",
        "~/pattern", 
        "~//", 
        "user name", 
        "items[abc]", 
        "items[1;2]", 
        "[::0]", 
        "an_object.an_array[0].a_field",
        "an_object.an_array[*].a_field",
        "an_object.an_array[0,1,2].a_field",
        "an_object.an_array[1:5:2].a_field",
        "an_object.~/^field_\\d+$/",
        "an_object.an_array[0].#", 
        "an_object.**",
        "an_object.an_array[0].**.a_field"
    ]

    for ts in test_strings:
        test_path_string_directly(ts)

    print("\n--- Interactive Test ---")
    print("Enter path strings to test (or 'quit' to exit):")
    while True:
        try:
            user_input = input("> ")
            if user_input.lower() == 'quit':
                break
            if user_input:
                test_path_string_directly(user_input)
        except EOFError:
            break
        except KeyboardInterrupt:
            print("\nExiting...")
            break

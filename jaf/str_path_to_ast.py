from lark import Lark, Transformer, Token
from typing import List, Any, Optional

_JAF_PATH_GRAMMAR = r"""
    ?start: path_element*

    path_element: IDENTIFIER_TOKEN
                | DOT_IDENTIFIER_TOKEN
                | INDEX_TOKEN
                | INDICES_TOKEN
                | SLICE_TOKEN
                | REGEX_KEY_TOKEN
                | LEVEL_WC_TOKEN
                | RECURSIVE_WC_TOKEN
                | DOT_RECURSIVE_WC_TOKEN

    IDENTIFIER_TOKEN: /[a-zA-Z_][a-zA-Z0-9_]*/
    DOT_IDENTIFIER_TOKEN: "." IDENTIFIER_TOKEN
    INDEX_TOKEN: "[" SIGNED_INT "]"
    INDICES_TOKEN: "[" SIGNED_INT ("," SIGNED_INT)* "]"
    SLICE_TOKEN: "[" SLICE_CONTENT "]"
    REGEX_KEY_TOKEN: "~/" REGEX_BODY "/"
    LEVEL_WC_TOKEN: "[*]"
    RECURSIVE_WC_TOKEN: "**"
    DOT_RECURSIVE_WC_TOKEN: ".**"
    
    SLICE_CONTENT: /[^\]]*/ /* Matches anything inside slice brackets */
    REGEX_BODY: /([^\/]|\\\/)*/ /* Matches regex body, allowing escaped slashes */

    %import common.SIGNED_INT
    %import common.WS
    %ignore WS
"""

class _PathAstTransformer(Transformer):
    def SIGNED_INT(self, token_list: List[Token]) -> int:
        return int(token_list[0].value)

    def IDENTIFIER_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["key", token_list[0].value]

    def DOT_IDENTIFIER_TOKEN(self, token_list: List[Token]) -> List[Any]:
        # token_list[0] is DOT, token_list[1] is IDENTIFIER_TOKEN
        return ["key", token_list[1].value] 

    def INDEX_TOKEN(self, token_list: List[Token]) -> List[Any]:
        # token_list = [LBRACKET_TOKEN, SIGNED_INT_val, RBRACKET_TOKEN]
        return ["index", token_list[1]] 

    def INDICES_TOKEN(self, token_list: List[Token]) -> List[Any]:
        # For terminal INDICES_TOKEN: "[" SIGNED_INT ("," SIGNED_INT)* "]"
        # The token_list[0].value will be the full string like "[1,2,3]"
        content = token_list[0].value[1:-1] 
        if not content: # Should not happen if grammar requires at least one SIGNED_INT
             raise ValueError("Indices content cannot be empty.")
        return ["indices", [int(x.strip()) for x in content.split(',')]]

    def SLICE_TOKEN(self, token_list: List[Token]) -> List[Any]:
        full_slice_str = token_list[0].value
        content = full_slice_str[1:-1]
        
        if not content:
            raise ValueError(f"Invalid slice '{full_slice_str}': content cannot be empty.")
        if content.count(':') > 2:
            raise ValueError(f"Invalid slice '{full_slice_str}': too many colons.")

        start: Optional[int] = None
        stop: Optional[int] = None
        step: Optional[int] = None
        
        parts = content.split(':', 2)

        if content.count(':') == 0: # e.g. "[abc]" or "[1]" - should not be SLICE_TOKEN
            raise ValueError(f"Invalid slice content '{content}'. Slice must contain ':'.")

        if parts[0]:
            start = int(parts[0])
        
        if len(parts) > 1 and parts[1]:
            stop = int(parts[1])
        
        if len(parts) > 2 and parts[2]:
            step = int(parts[2])
            
        return ["slice", start, stop, step]

    def REGEX_KEY_TOKEN(self, token_list: List[Token]) -> List[Any]:
        body = token_list[0].value[2:-1] # Remove "~/" and "/"
        return ["regex_key", body]

    def LEVEL_WC_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["wc_level"]

    def RECURSIVE_WC_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["wc_recursive"]
    
    def DOT_RECURSIVE_WC_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["wc_recursive"]

    def start(self, children: List[List[Any]]) -> List[List[Any]]:
        # Correct the first key if it was parsed by DOT_IDENTIFIER_TOKEN (e.g. path ".foo")
        # This case should ideally be handled by grammar disallowing leading dot for first identifier
        # or by ensuring IDENTIFIER_TOKEN is tried first for non-dotted identifiers.
        # The current grammar structure with path_element* and distinct token types handles this.
        return children

_path_parser = Lark(_JAF_PATH_GRAMMAR, parser='lalr', transformer=_PathAstTransformer())

def path_string_to_ast(path_str: str) -> List[List[Any]]:
    """
    Converts a JAF path string representation into its AST (list of components).

    Example:
        "user[0].name" -> [["key", "user"], ["index", 0], ["key", "name"]]
        "**.id" -> [["wc_recursive"], ["key", "id"]]
    """
    if not path_str.strip():
        return []
    try:
        # The transformer's `start` method will return the list of components directly.
        ast_components = _path_parser.parse(path_str)
        return ast_components
    except Exception as e: # Catch Lark specific errors for better messages if needed
        raise ValueError(f"Invalid JAF path string: '{path_str}'. Error: {e}")

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
    
    SLICE_CONTENT: /[^]]*/
    REGEX_BODY: /([^\/]|\\\/)*/

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
        return ["key", token_list[1].value] 

    def INDEX_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["index", token_list[1]] 

    def INDICES_TOKEN(self, token_list: List[Token]) -> List[Any]:
        content = token_list[0].value[1:-1] 
        if not content: 
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

        if content.count(':') == 0:
            raise ValueError(f"Invalid slice content '{content}'. Slice must contain ':'.")

        if parts[0]:
            start = int(parts[0])
        
        if len(parts) > 1 and parts[1]:
            stop = int(parts[1])
        
        if len(parts) > 2 and parts[2]:
            step = int(parts[2])
            
        return ["slice", start, stop, step]

    def REGEX_KEY_TOKEN(self, token_list: List[Token]) -> List[Any]:
        body = token_list[0].value[2:-1]
        return ["regex_key", body]

    def LEVEL_WC_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["wc_level"]

    def RECURSIVE_WC_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["wc_recursive"]
    
    def DOT_RECURSIVE_WC_TOKEN(self, token_list: List[Token]) -> List[Any]:
        return ["wc_recursive"]

    def start(self, children: List[List[Any]]) -> List[List[Any]]:
        return children

_path_parser = Lark(_JAF_PATH_GRAMMAR, parser='lalr', transformer=_PathAstTransformer())

def path_string_to_ast(path_str: str) -> List[List[Any]]:
    """
    Converts a JAF path string representation into its AST (list of components).
    """
    if not path_str.strip():
        return []
    try:
        ast_components = _path_parser.parse(path_str)
        return ast_components
    except Exception as e:
        raise ValueError(f"Invalid JAF path string: '{path_str}'. Error: {e}")

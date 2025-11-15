"""
S-expression parser for JAF queries.

This module provides a Lisp/Scheme-like syntax for writing JAF queries,
which is more human-friendly than JSON arrays while maintaining the same
expressive power.

Syntax Examples:
    S-expression syntax -> JSON array syntax
    
    (eq? @name "Alice") -> ["eq?", "@name", "Alice"]
    (gt? @age 25) -> ["gt?", "@age", 25]
    (and (gt? @age 18) (eq? @active true)) -> ["and", ["gt?", "@age", 18], ["eq?", "@active", true]]
    (filter @users (gt? @.age 25)) -> ["filter", "@users", ["gt?", "@.age", 25]]
    
Special features:
    - Numbers are automatically parsed (42, 3.14, -10)
    - Booleans: true, false
    - null is recognized
    - Strings can be quoted or unquoted if they're identifiers
    - @ paths are preserved as strings
    - Nested s-expressions are recursively parsed
"""

import re
from typing import Any, List, Union


class SExpParser:
    """Parser for S-expression syntax to JAF AST."""
    
    def __init__(self):
        # Token patterns
        self.token_patterns = [
            ('LPAREN', r'\('),
            ('RPAREN', r'\)'),
            ('STRING', r'"(?:[^"\\]|\\.)*"'),  # Quoted strings
            ('NUMBER', r'-?\d+\.?\d*'),  # Numbers (int or float)
            ('BOOL', r'\b(true|false)\b'),  # Booleans
            ('NULL', r'\bnull\b'),  # Null
            ('PATH', r'@[\w.\[\]]*'),  # Path expressions like @user.name, @.id
            ('SYMBOL', r'[a-zA-Z_\-\+\*\/\?!<>=][\w\-\+\*\/\?!<>=]*'),  # Symbols/identifiers
            ('WHITESPACE', r'\s+'),  # Whitespace
            ('COMMENT', r';[^\n]*'),  # Comments (Lisp-style)
        ]
        
        # Compile the combined regex
        self.token_regex = re.compile('|'.join(
            f'(?P<{name}>{pattern})' 
            for name, pattern in self.token_patterns
        ))
    
    def tokenize(self, text: str) -> List[tuple]:
        """Tokenize the input text."""
        tokens = []
        for match in self.token_regex.finditer(text):
            kind = match.lastgroup
            value = match.group()
            
            # Skip whitespace and comments
            if kind in ('WHITESPACE', 'COMMENT'):
                continue
                
            tokens.append((kind, value))
        
        return tokens
    
    def parse_value(self, token_type: str, token_value: str) -> Any:
        """Parse a single token value."""
        if token_type == 'STRING':
            # Remove quotes and handle escape sequences
            unquoted = token_value[1:-1]
            # Handle common escape sequences
            unquoted = unquoted.replace('\\n', '\n')
            unquoted = unquoted.replace('\\t', '\t')
            unquoted = unquoted.replace('\\r', '\r')
            unquoted = unquoted.replace('\\"', '"')
            unquoted = unquoted.replace('\\\\', '\\')
            return unquoted
        elif token_type == 'NUMBER':
            # Parse as int or float
            if '.' in token_value:
                return float(token_value)
            return int(token_value)
        elif token_type == 'BOOL':
            return token_value == 'true'
        elif token_type == 'NULL':
            return None
        elif token_type == 'PATH':
            # Keep paths as strings (they're special in JAF)
            return token_value
        elif token_type == 'SYMBOL':
            # Symbols stay as strings
            return token_value
        else:
            raise ValueError(f"Unknown token type: {token_type}")
    
    def parse_sexp(self, tokens: List[tuple], index: int = 0) -> tuple[Any, int]:
        """
        Parse an S-expression from tokens starting at index.
        Returns (parsed_expression, next_index).
        """
        if index >= len(tokens):
            raise ValueError("Unexpected end of input")
        
        token_type, token_value = tokens[index]
        
        if token_type == 'LPAREN':
            # Parse a list
            index += 1
            elements = []
            
            while index < len(tokens):
                token_type, token_value = tokens[index]
                
                if token_type == 'RPAREN':
                    # End of list
                    return elements, index + 1
                
                # Parse the next element
                element, index = self.parse_sexp(tokens, index)
                elements.append(element)
            
            raise ValueError("Unclosed parenthesis")
        
        else:
            # Parse an atom
            value = self.parse_value(token_type, token_value)
            return value, index + 1
    
    def parse(self, text: str) -> Any:
        """
        Parse an S-expression string into a JAF AST.
        
        Args:
            text: S-expression string like "(gt? @age 25)"
            
        Returns:
            JAF AST as nested lists/values like ["gt?", "@age", 25]
        """
        tokens = self.tokenize(text)
        
        if not tokens:
            raise ValueError("Empty input")
        
        result, index = self.parse_sexp(tokens, 0)
        
        if index < len(tokens):
            raise ValueError(f"Unexpected tokens after expression: {tokens[index:]}")
        
        return result


def sexp_to_jaf(sexp: str) -> Any:
    """
    Convert an S-expression string to JAF AST format.
    
    Examples:
        >>> sexp_to_jaf('(eq? @name "Alice")')
        ['eq?', '@name', 'Alice']
        
        >>> sexp_to_jaf('(and (gt? @age 18) (eq? @active true))')
        ['and', ['gt?', '@age', 18], ['eq?', '@active', True]]
        
        >>> sexp_to_jaf('42')
        42
        
        >>> sexp_to_jaf('@user.name')
        '@user.name'
    """
    parser = SExpParser()
    return parser.parse(sexp)


def jaf_to_sexp(ast: Any) -> str:
    """
    Convert a JAF AST back to S-expression format.
    
    Examples:
        >>> jaf_to_sexp(['eq?', '@name', 'Alice'])
        '(eq? @name "Alice")'
        
        >>> jaf_to_sexp(['and', ['gt?', '@age', 18], ['eq?', '@active', True]])
        '(and (gt? @age 18) (eq? @active true))'
        
        >>> jaf_to_sexp(42)
        '42'
    """
    if isinstance(ast, list):
        # Convert list to S-expression
        elements = [jaf_to_sexp(elem) for elem in ast]
        return f"({' '.join(elements)})"
    elif isinstance(ast, str):
        # Check if it's a path
        if ast.startswith('@'):
            return ast
        # Check if it's a known operator/function (symbols that should not be quoted)
        elif ast in ['eq?', 'neq?', 'gt?', 'gte?', 'lt?', 'lte?', 'and', 'or', 'not',
                     'contains?', 'starts-with?', 'ends-with?', 'close-match?',
                     'filter', 'map', 'take', 'skip', 'dict', 'if', 'exists?',
                     'is-string?', 'is-number?', 'is-array?', 'is-object?', 'is-null?']:
            return ast
        # For other strings, quote them (they're likely string values)
        else:
            escaped = ast.replace('\\', '\\\\').replace('"', '\\"')
            # Also escape newlines and tabs for readability
            escaped = escaped.replace('\n', '\\n').replace('\t', '\\t')
            return f'"{escaped}"'
    elif isinstance(ast, bool):
        return 'true' if ast else 'false'
    elif ast is None:
        return 'null'
    else:
        # Numbers and other types
        return str(ast)


# Convenience function for use in CLI and other tools
def compile_sexp(sexp: str) -> Any:
    """
    Compile an S-expression to JAF AST format.
    This is an alias for sexp_to_jaf for clarity in different contexts.
    """
    return sexp_to_jaf(sexp)


if __name__ == "__main__":
    # Test examples
    examples = [
        '(eq? @name "Alice")',
        '(gt? @age 25)',
        '(and (gt? @age 18) (eq? @active true))',
        '(or (eq? @role "admin") (eq? @role "moderator"))',
        '(not (contains? @tags "banned"))',
        '(filter @users (gt? @.age 25))',
        '(map @items (dict "id" @.id "name" @.name))',
        '(close-match? @title "Hello World" 0.8)',
        '42',
        '"a string with spaces"',
        'true',
        'null',
        '@user.profile.name',
    ]
    
    print("S-expression to JAF AST examples:\n")
    for sexp in examples:
        try:
            jaf = sexp_to_jaf(sexp)
            back = jaf_to_sexp(jaf)
            print(f"Input:  {sexp}")
            print(f"JAF:    {jaf}")
            print(f"Back:   {back}")
            print()
        except Exception as e:
            print(f"Error parsing '{sexp}': {e}\n")
"""
Tests for S-expression parser.
"""

import pytest
from jaf.sexp_parser import (
    SExpParser, 
    sexp_to_jaf, 
    jaf_to_sexp,
    compile_sexp
)


class TestSExpParser:
    """Test the S-expression parser."""
    
    def test_simple_function_call(self):
        """Test simple function calls."""
        assert sexp_to_jaf('(eq? @name "Alice")') == ['eq?', '@name', 'Alice']
        assert sexp_to_jaf('(gt? @age 25)') == ['gt?', '@age', 25]
        assert sexp_to_jaf('(lt? @score 100)') == ['lt?', '@score', 100]
    
    def test_boolean_operators(self):
        """Test boolean operators."""
        assert sexp_to_jaf('(and (gt? @age 18) (eq? @active true))') == [
            'and', ['gt?', '@age', 18], ['eq?', '@active', True]
        ]
        assert sexp_to_jaf('(or (eq? @role "admin") (eq? @role "mod"))') == [
            'or', ['eq?', '@role', 'admin'], ['eq?', '@role', 'mod']
        ]
        assert sexp_to_jaf('(not (eq? @banned true))') == [
            'not', ['eq?', '@banned', True]
        ]
    
    def test_nested_expressions(self):
        """Test nested expressions."""
        result = sexp_to_jaf('(and (or (eq? @a 1) (eq? @b 2)) (not (eq? @c 3)))')
        assert result == [
            'and',
            ['or', ['eq?', '@a', 1], ['eq?', '@b', 2]],
            ['not', ['eq?', '@c', 3]]
        ]
    
    def test_literals(self):
        """Test literal values."""
        assert sexp_to_jaf('42') == 42
        assert sexp_to_jaf('3.14') == 3.14
        assert sexp_to_jaf('-10') == -10
        assert sexp_to_jaf('true') == True
        assert sexp_to_jaf('false') == False
        assert sexp_to_jaf('null') == None
        assert sexp_to_jaf('"hello world"') == 'hello world'
    
    def test_paths(self):
        """Test path expressions."""
        assert sexp_to_jaf('@name') == '@name'
        assert sexp_to_jaf('@user.profile.email') == '@user.profile.email'
        assert sexp_to_jaf('@items[0]') == '@items[0]'
    
    def test_string_handling(self):
        """Test string with special characters."""
        assert sexp_to_jaf('"string with spaces"') == 'string with spaces'
        assert sexp_to_jaf(r'"\""') == '"'
        assert sexp_to_jaf(r'"\\"') == '\\'
        assert sexp_to_jaf('"line\\nbreak"') == 'line\nbreak'
    
    def test_comments(self):
        """Test that comments are ignored."""
        result = sexp_to_jaf('(eq? @name "Alice") ; this is a comment')
        assert result == ['eq?', '@name', 'Alice']
        
        result = sexp_to_jaf('; comment line\n(gt? @age 25)')
        assert result == ['gt?', '@age', 25]
    
    def test_whitespace_handling(self):
        """Test various whitespace."""
        result = sexp_to_jaf('''
            (and
                (gt? @age 18)
                (eq? @active true)
            )
        ''')
        assert result == ['and', ['gt?', '@age', 18], ['eq?', '@active', True]]
    
    def test_complex_operations(self):
        """Test more complex JAF operations."""
        # Map operation
        result = sexp_to_jaf('(map @items (dict "id" @.id "name" @.name))')
        assert result == ['map', '@items', ['dict', 'id', '@.id', 'name', '@.name']]
        
        # Filter operation
        result = sexp_to_jaf('(filter @users (gt? @.age 25))')
        assert result == ['filter', '@users', ['gt?', '@.age', 25]]
        
        # Close match
        result = sexp_to_jaf('(close-match? @title "Hello World" 0.8)')
        assert result == ['close-match?', '@title', 'Hello World', 0.8]
    
    def test_empty_list(self):
        """Test empty list."""
        assert sexp_to_jaf('()') == []
    
    def test_symbols_with_special_chars(self):
        """Test symbols with special characters."""
        assert sexp_to_jaf('(eq?)') == ['eq?']
        assert sexp_to_jaf('(starts-with?)') == ['starts-with?']
        assert sexp_to_jaf('(<=)') == ['<=']
        assert sexp_to_jaf('(!=)') == ['!=']


class TestJafToSExp:
    """Test converting JAF AST back to S-expressions."""
    
    def test_simple_conversion(self):
        """Test simple conversions."""
        assert jaf_to_sexp(['eq?', '@name', 'Alice']) == '(eq? @name "Alice")'
        assert jaf_to_sexp(['gt?', '@age', 25]) == '(gt? @age 25)'
    
    def test_nested_conversion(self):
        """Test nested structure conversion."""
        ast = ['and', ['gt?', '@age', 18], ['eq?', '@active', True]]
        expected = '(and (gt? @age 18) (eq? @active true))'
        assert jaf_to_sexp(ast) == expected
    
    def test_literal_conversion(self):
        """Test literal value conversion."""
        assert jaf_to_sexp(42) == '42'
        assert jaf_to_sexp(3.14) == '3.14'
        assert jaf_to_sexp(True) == 'true'
        assert jaf_to_sexp(False) == 'false'
        assert jaf_to_sexp(None) == 'null'
        # String values should be quoted (unless they're operators)
        assert jaf_to_sexp('simple') == '"simple"'
        assert jaf_to_sexp('with spaces') == '"with spaces"'
        # Operators should not be quoted
        assert jaf_to_sexp('eq?') == 'eq?'
        assert jaf_to_sexp('and') == 'and'
    
    def test_path_conversion(self):
        """Test path conversion."""
        assert jaf_to_sexp('@user.name') == '@user.name'
        assert jaf_to_sexp(['eq?', '@user.name', 'Alice']) == '(eq? @user.name "Alice")'
    
    def test_roundtrip(self):
        """Test that we can convert back and forth."""
        original_sexp = '(and (gt? @age 18) (eq? @active true))'
        ast = sexp_to_jaf(original_sexp)
        back_to_sexp = jaf_to_sexp(ast)
        assert sexp_to_jaf(back_to_sexp) == ast


class TestSExpParserErrors:
    """Test error handling."""
    
    def test_unclosed_paren(self):
        """Test unclosed parenthesis."""
        with pytest.raises(ValueError, match="Unclosed parenthesis"):
            sexp_to_jaf('(eq? @name "Alice"')
    
    def test_unexpected_close_paren(self):
        """Test unexpected closing parenthesis."""
        with pytest.raises(ValueError, match="Unexpected tokens"):
            sexp_to_jaf('(eq? @name "Alice"))')
    
    def test_empty_input(self):
        """Test empty input."""
        with pytest.raises(ValueError, match="Empty input"):
            sexp_to_jaf('')
        with pytest.raises(ValueError, match="Empty input"):
            sexp_to_jaf('   ')
        with pytest.raises(ValueError, match="Empty input"):
            sexp_to_jaf('; just a comment')


class TestCompileSExp:
    """Test the compile_sexp function."""
    
    def test_compile_alias(self):
        """Test that compile_sexp is an alias for sexp_to_jaf."""
        sexp = '(eq? @name "Alice")'
        assert compile_sexp(sexp) == sexp_to_jaf(sexp)
        assert compile_sexp(sexp) == ['eq?', '@name', 'Alice']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
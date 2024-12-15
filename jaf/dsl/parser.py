from lark import Lark
from .transformer import DSLTransformer
from pathlib import Path

# Load grammar from the separate file
grammar_path = Path(__file__).parent / 'grammar.lark'
with grammar_path.open('r') as f:
    dsl_grammar = f.read()

# Initialize the Lark parser with the grammar and transformer
dsl_parser = Lark(dsl_grammar, parser='lalr', transformer=DSLTransformer())

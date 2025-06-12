#!/usr/bin/env python3
"""
parse.py – Compile a JAF-DSL expression into an AST
• LALR parser, C-style function calls only
• Wild-cards in paths (*, **), numeric indices, hyphenated identifiers
• CLI: --examples  --json
"""

import os
import argparse
import json
from pprint import pprint
from lark import Lark, Transformer, Token


# ────────────────────────────────────────────────────────────
# Transformer
# ────────────────────────────────────────────────────────────
class DSLTransformer(Transformer):
    # Entry helpers -----------------------------------------------------------
    def start(self, items):
        return items[0]

    def expr(self, items):
        return items[0]

    # Boolean logic -----------------------------------------------------------
    # items = [lhs, OR/AND token, rhs]   ← the middle token is ignored
    def or_operation(self, items):
        left, _, right = items
        return ["or", left, right]

    def and_operation(self, items):
        left, _, right = items
        return ["and", left, right]

    def not_operation(self, items):
        return ["not", items[0]]

    # Comparisons / exists ----------------------------------------------------
    def condition(self, items):
        left, op_tok, right = items
        return [str(op_tok), left, right]

    def unary_exists_expr(self, items):
        return ["exists?", items[0]]

    # Function call -----------------------------------------------------------
    def function_call(self, items):
        fname = str(items[0])
        return [fname] + items[1:]

    # Path handling -----------------------------------------------------------
    def path_component(self, items):
        token_or_val = items[0]
        if isinstance(token_or_val, Token):
            t = token_or_val
            if t.type == "STAR":
                return "*"
            if t.type == "DOUBLESTAR":
                return "**"
            if t.type == "INT":
                return int(t.value)
            raise ValueError(f"Unexpected token {t.type} in path_component")
        return token_or_val  # identifier string

    def path(self, items):
        out = []
        for c in items:
            if c == "*":
                out.append(["wc_level"])
            elif c == "**":
                out.append(["wc_recursive"])
            elif isinstance(c, int):
                out.append(["index", c])
            else:  # identifier
                out.append(["key", c])
        return ["path", out]

    # Primitive literals ------------------------------------------------------
    def value(self, items):
        return items[0]

    def BOOLEAN(self, tok):
        return tok.value == "true"

    def SIGNED_NUMBER(self, tok):
        try:
            return int(tok.value)
        except ValueError:
            return float(tok.value)

    def IDENTIFIER(self, tok):
        return tok.value

    def ESCAPED_STRING(self, tok):
        s = tok.value[1:-1]  # strip quotes
        return s.replace(r'\"', '"').replace(r"\\", "\\")

    def __default_token__(self, tok):
        return None if tok.value == "null" else tok.value


# ────────────────────────────────────────────────────────────
# Parser wrapper
# ────────────────────────────────────────────────────────────
def _get_parser() -> Lark:
    grammar_path = os.path.join(os.path.dirname(__file__), "grammar.lark")
    return Lark.open(grammar_path, parser="lalr", start="start", maybe_placeholders=False)


def parse_dsl(text: str):
    parser = _get_parser()
    tree = parser.parse(text)
    return DSLTransformer().transform(tree)


# ────────────────────────────────────────────────────────────
# CLI helper
# ────────────────────────────────────────────────────────────
EXAMPLES = [
    ':name eq? "John"',
    ":user.email exists?",
    ':language eq? "python" AND :stars gt? 100',
    'lower-case(:language) eq? "python"',
    ':items.*.status eq? "completed"',
    ":data.0.value gt? 50",
    ":**.error exists?",
    "(:owner.active eq? true) AND (:stars gt? 100 OR :forks gt? 50)",
]


def _run_examples(as_json=False):
    print("JAF DSL to AST Examples:\n")
    for i, q in enumerate(EXAMPLES, 1):
        try:
            ast = parse_dsl(q)
            print(f"#{i}. DSL: {q}")
            if as_json:
                print(json.dumps(ast, indent=2))
            else:
                print(f"   {ast!r}")
        except Exception as e:
            print(f"Failed to parse query: {q}\nError: {e}")
        print("-" * 40)


def main():
    ap = argparse.ArgumentParser(description="Compile a JAF DSL expression into its AST.")
    ap.add_argument("dsl_expression", nargs="?", help="DSL expression to parse (omit for --examples).")
    ap.add_argument("--examples", action="store_true", help="Show built-in example queries.")
    ap.add_argument("--json", action="store_true", help="Output AST in JSON.")

    args = ap.parse_args()
    if args.examples:
        _run_examples(args.json)
        return

    if not args.dsl_expression:
        ap.print_help()
        return

    try:
        ast = parse_dsl(args.dsl_expression)
        print(json.dumps(ast, indent=2) if args.json else pprint(ast))
    except Exception as e:
        print(f"Failed to parse: {args.dsl_expression}\nError: {e}")


if __name__ == "__main__":
    main()

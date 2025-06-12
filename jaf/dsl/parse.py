#!/usr/bin/env python3
"""
parse.py – JAF DSL → AST compiler
• Recognises prefix special forms (if, and, or, not)
• Infix AND / OR / NOT still work
• Wild-cards, numeric indices, double-star
"""

import os, json, argparse
from pprint import pprint
from lark import Lark, Transformer, Token

# ───────────────────────────────────────── Transformer ─────
class DSLTransformer(Transformer):
    # entry
    def start(self, items): return items[0]
    def expr(self, items):  return items[0]

    # infix boolean
    def or_operation(self, items):
        left, _, right = items
        return ["or", left, right]

    def and_operation(self, items):
        left, _, right = items
        return ["and", left, right]

    def not_operation(self, items):
        return ["not", items[0]]

    # prefix special forms  if(), and(), or(), not()
    def special_form(self, items):
        name_token, *args = items
        return [str(name_token)] + args

    # comparison & exists?
    def condition(self, items):
        left, op_tok, right = items
        return [str(op_tok), left, right]

    def unary_exists_expr(self, items):
        return ["exists?", items[0]]

    # function call
    def function_call(self, items):
        fname = str(items[0])
        return [fname] + items[1:]
    


    # path components
    def path_component(self, items):
        x = items[0]
        if isinstance(x, Token):
            if x.type == "STAR":         return "*"
            if x.type == "DOUBLESTAR":   return "**"
            if x.type == "INT":          return int(x.value)
        return x

    def path(self, items):
        out = []
        for comp in items:
            if comp == "*":   out.append(["wc_level"])
            elif comp == "**":out.append(["wc_recursive"])
            elif isinstance(comp, int):
                out.append(["index", comp])
            else:             out.append(["key", comp])
        return ["path", out]


    # Handle path as boolean evaluation
    def eval_path_directly_as_boolean(self, items):
        # When a path is used directly as a boolean expression (like in :user.email)
        # we can either return the path directly, or wrap it in an exists? check
        # For now, let's return the path directly and let the evaluator decide
        # how to handle it in boolean context
        return items[0]  # items[0] is the transformed path AST


    # literals
    def INT(self, t): return int(t.value)
    def BOOLEAN(self, t):      return t.value == "true"
    def SIGNED_NUMBER(self, t):
        try: return int(t.value)
        except ValueError: return float(t.value)
    def ESCAPED_STRING(self, t):
        return t.value[1:-1].encode('utf-8').decode('unicode_escape')
    def IDENTIFIER(self, t):   return t.value
    def __default_token__(self, t):
        return None if t.value == "null" else t.value

# ──────────────────────────────────────── Parser helper ────
def _parser() -> Lark:
    here = os.path.dirname(__file__)
    return Lark.open(
        os.path.join(here, "grammar.lark"),
        parser="lalr",
        start="start",
        maybe_placeholders=False,
    )

def parse_dsl(text: str):
    tree = _parser().parse(text)
    return DSLTransformer().transform(tree)

# ───────────────────────────────────────── CLI / examples ──
EXAMPLES = [
    ':name eq? "John"',
    ":user.email exists?",
    ':language eq? "python" AND :stars gt? 100',
    'lower-case(:language) eq? "python"',
    ':items.*.status eq? "completed"',
    ":data.0.value gt? 50",
    ":**.error exists?",
    '(:owner.active eq? true) AND (:stars gt? 100 OR :forks gt? 50)',
    'if(:active, "yes", "no")',                 # new special form
]

def run_examples(as_json=False):
    print("JAF DSL → AST examples\n")
    for i, q in enumerate(EXAMPLES, 1):
        try:
            ast = parse_dsl(q)
            print(f"#{i}. {q}")
            print(json.dumps(ast, indent=2) if as_json else ast)
        except Exception as e:
            print(f"#{i}. FAILED  {q}\n   {e}")
        print("-" * 40)

def main():
    ap = argparse.ArgumentParser("JAF DSL compiler")
    ap.add_argument("expr", nargs="?", help="DSL string")
    ap.add_argument("--examples", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.examples:
        run_examples(args.json)
        return

    if not args.expr:
        ap.print_help(); return

    try:
        ast = parse_dsl(args.expr)
        print(json.dumps(ast, indent=2) if args.json else pprint(ast))
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()

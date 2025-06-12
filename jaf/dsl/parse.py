import os
from lark import Lark, Transformer, v_args, Token
import argparse
from pprint import pprint

class DSLTransformer(Transformer):
    def start(self, items):
        return items[0]

    def expr(self, items):
        return items[0]

    def bool_expr(self, items):
        return items[0]

    def or_operation(self, items):
        left, _, right = items
        return ['or', left, right]

    def and_operation(self, items):
        left, _, right = items
        return ['and', left, right]

    def condition(self, items):
        left_operand, op, right_operand = items
        return [str(op), left_operand, right_operand]

    def operator(self, items):
        return str(items[0])

    def operand(self, items):
        return items[0]

    def function_call(self, items):
        function_name = str(items[0])
        arguments = items[1:]
        return [function_name] + arguments

    def path_component(self, items):
        component = items[0]
        if isinstance(component, Token):
            if component.type == 'STAR':
                return '*'
            elif component.type == 'DOUBLESTAR':
                return '**'
            elif component.type == 'NUMBER':
                try:
                    return int(component.value)
                except ValueError:
                    return float(component.value)
            else:
                return str(component.value)
        return component

    def path(self, items):
        if len(items) < 1:
            raise ValueError("Path must contain at least one component after ':'")
        
        # Convert to list format: ["path", [component1, component2, ...]]
        components = []
        for item in items:
            components.append(item)
        
        return ['path', components]

    def value(self, items):
        return items[0]

    def BOOLEAN(self, token):
        return token.value == 'true'

    def NUMBER(self, token):
        try:
            return int(token.value)
        except ValueError:
            return float(token.value)

    def IDENTIFIER(self, token):
        return str(token.value)

    def ESCAPED_STRING(self, token):
        # Remove surrounding quotes and handle escape sequences
        raw_string = token.value[1:-1]
        raw_string = raw_string.replace('\\"', '"')
        raw_string = raw_string.replace('\\\\', '\\')
        return raw_string

    def __default_token__(self, token):
        if token.value == "null":
            return None
        return token.value

# Function to parse DSL expression into AST
def parse_dsl(expr):
    # Get the absolute path to 'grammar.lark'
    grammar_path = os.path.join(os.path.dirname(__file__), 'grammar.lark')

    # Initialize the parser
    dsl_parser = Lark.open(grammar_path, parser='lalr', start='start')

    # Parse the expression
    parse_tree = dsl_parser.parse(expr)

    # Transform the parse tree into AST
    transformer = DSLTransformer()
    ast = transformer.transform(parse_tree)

    return ast

def main():
    parser = argparse.ArgumentParser(description="Parse a DSL expression into its AST representation.")
    parser.add_argument("--expr", type=str, help="DSL expression to parse.")
    parser.add_argument("--examples", action="store_true", help="Show example DSL expressions.")
    args = parser.parse_args()

    if args.examples:
        queries = [
            ':name eq? "John"',
            ':user.email exists?',
            ':language eq? "python" AND :stars gt? 100',
            '(lower-case :language) eq? "python"',
            ':items.*.status eq? "completed"',
            ':data.0.value gt? 50',
            ':**.error exists?',
            '(:owner.active eq? true) AND (:stars gt? 100 OR :forks gt? 50)',
        ]
        for q in queries:
            try:
                ast = parse_dsl(q)
                print(f"DSL: {q}")
                print("AST:", end=" ")
                pprint(ast)
                print()
            except Exception as e:
                print(f"Failed to parse query: {q}")
                print(f"Error: {e}\n")
    elif args.expr:
        try:
            ast = parse_dsl(args.expr)
            pprint(ast)
        except Exception as e:
            print(f"Failed to parse expression: {args.expr}")
            print(f"Error: {e}")
    else:
        print("Please provide a DSL expression using --expr or use --examples to see examples.")

if __name__ == "__main__":
    main()
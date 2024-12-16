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
        return [str(op), left_operand, right_operand]  # Convert op to string

    def operator(self, items):
        return str(items[0])  # Convert operator to string

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
            if component.type == 'DOUBLESTAR':
                return '**'
            return str(component)
        return str(component)

    def path(self, items):
        if len(items) < 1:
            raise ValueError("Path must contain at least one component after ':'")
        
        components = [str(item) for item in items]  # Include all components
        path_str = '.'.join(components)
        return ['path', path_str]

    def bare_path(self, items):
        """
        Handles paths without the ':' prefix, used in function calls like path-exists?
        Example: owner.*.active
        """
        path_str = '.'.join([str(item) for item in items])
        return ['path', path_str]

    def value(self, items):
        return items[0]

    def BOOLEAN(self, token):
        return token.value == 'True'

    def NUMBER(self, token):
        try:
            return int(token)
        except ValueError:
            return float(token)

    def IDENTIFIER(self, token):
        return str(token)

    def ESCAPED_STRING(self, token):
        return token.value[1:-1]  # Remove surrounding quotes

    
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
            ':language eq? Python',
            ':language eq? "No Language"',
            ':stars gt? 100',
            ':asset.type eq? image AND :asset.size gt? 100',
            ':asset.type eq? image AND (:asset.size gt? 100 OR :asset.size lt? 10)',
            '(lower-case :owner.name) eq? "alex"',
            '(path-exists? :asset.amount) OR :language eq? "Python"',
            '(lower-case :owner.name) eq? "alex" OR ("bitcoin" in? :asset.description AND :asset.amount gt? 1)',
        ]
        for q in queries:
            try:
                ast = parse_dsl(q)
                print(f"Query: {q}\n")
                print("--- [AST] ---")
                pprint(ast)
                print("-------------\n")
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
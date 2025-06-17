from jaf.path_conversion import string_to_path_ast

paths = [
    # Indexing
    "data[0]",
    "data[-1]",
    "data[10]",
    'data.key',
    "data.key[10]",

    # Slicing
    "data[:]",
    "data[::]",
    "data[1:]",
    "data[:3]",
    "data[1:3]",
    "data[1:3:2]",
    "data[::-1]",
    "data[::0]",
    "data[1:3:-1]",

    # Multiple Indexing
    "data[0][1]",
    'data.a.b',
    "data[0:5][1:3]",
    'data[0].key',

    # Wildcard / Regex (if supported)
    "data[*]",
    "data./^key\\d+/",
    'data["*"]',

    # Nested Paths
    "data.users[0].name",

    # Mixed Types / Syntax Edge Cases
    "data[ 1 : 5 ]",
    "data[1 : 5: ]",
    "data[ : : ]",
    "data[]",
    "data[0,1]",
    "[0,1]"
]

# Header
print(f"{'Input':<35} {'Parsed AST':<60} {'Expected'}")

for path in paths:
    try:
        ast = string_to_path_ast(path)
        expected = repr(ast)  # Mirror actual for now
        print(f"{path:<35} {str(ast):<60} {expected}")
    except Exception as e:
        print(f"{path:<35} ERROR: {e}")

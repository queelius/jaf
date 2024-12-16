# `jaf` - JSON Array Filter

`jaf` is a versatile filtering system designed to sift through JSON arrays.
It allows users to filter JSON arrays based on complex conditions using a
simple and intuitive query language. The query language is designed to be
easy to use and understand, while still being powerful enough to handle
complex filtering tasks.

## Builtins

We refer to the available operators as `builtins`. These are the functions that
are available to the user to use in their queries. The `builtins` are functions
that are used to compare values, perform operations, or combine other functions
to create complex queries. The `builtins` are the core of the filtering system
and are used to create queries that can filter JSON arrays based on the
specified conditions.

Predicates conanically end with a `?`, e.g., `eq?` (eauals) and `lt?` (less-than).
General operators do not canocially end with a `?`, e.g., `lower-case` and `or`.
The predicates are used to compare values, while the operators are used to combine
predicates or perform other operations that make the desired comparison possible
or the desired result achievable.

> *Note*: We do not use operators like `==` or `>`, but instead use `eq?` and
> `gt?`. The primary reason for this choice is that we provide a command-line
> tool, and if we used `>` it would be interpreted as a redirection operator
> by the shell.

For example, the `lower-case` operator is used to convert a string to lowercase before
comparison, so that the comparison is case-insensitive. Here is an example
query that uses the `lower-case` operator:

```python
['and', ['lower-case', ['path', 'language']], ['eq?', 'python']]
```

This query will filter repositories where the `language` field is equal to
`"python"`, regardless of the case of the letters.

> *Note*: Depending on the `builtins`, the query language can be Turing complete.
> e.g., it would be trivial to add a `lambda` builtin that allows users to define
> their own functions. However, this is not a safe practice, as it would allow
> users to execute arbitrary code. Therefore, we have chosen to limit the default
> `builtins` to a safe set of functions that are useful for filtering JSON arrays.
> If you need additional functionality, you can always extend or provide your own
> set of `builtins` to include the functions you need. As a limiting case, a
> `lambda` builtin could be added to the `builtins` to allow users to define their
> own functions.

## Query Language

Queries are represented using an Abstract Syntax Tree (AST) based on nested
lists, where each list takes the form of `[<expressio>, <arg1>, <arg2>,...]`.

We also provide a Domain-Specific Language (DSL) that allows users to craft
queries using an intuitive infix notation. The DSL is converted into the AST
before being evaluated. Here is the EBNF for the query language:

```ebnf
%import common.WS
%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%ignore WS

start: expr

expr: bool_expr

?bool_expr: or_expr

?or_expr: and_expr
        | or_expr OR and_expr -> or_operation

?and_expr: primary
        | and_expr AND primary -> and_operation

?primary: operand
       | "(" bool_expr ")"

?operand: condition
       | function_call
       | path
       | bare_path
       | value

condition: operand operator operand

operator: IDENTIFIER

function_call: "(" IDENTIFIER operand+ ")"

path: ":" path_component ("." path_component)*

bare_path: path_component ("." path_component)*

path_component: IDENTIFIER 
             | STAR  
             | DOUBLESTAR

STAR: "*" 
DOUBLESTAR: "**"

value: ESCAPED_STRING
     | NUMBER
     | BOOLEAN

BOOLEAN: "True" | "False"
NUMBER: SIGNED_NUMBER

IDENTIFIER: /[a-zA-Z][a-zA-Z0-9_\-\?]*/

OR: "OR"
AND: "AND"
```

For example, consider the following query AST:

```python
['and',
    ['eq?', ['lower-case', ['path', 'language']], 'python'],
    ['gt?', ['path', 'stars'], 100],
    ['eq?', ['path','owner.name'], ['path': 'user.name']]]
```

It has an equivalent DSL given by:

```text
(lower-case :language) eq? "python" AND :stars gt? 100 AND :owner.name eq? :user.name
```

We see that we have a special notation for `path` commands: we prefix the field
name with a colon: `:`, such as `:language` and `:owner.name`. This is to distinguish
field names from other strings in the query. The `path`command is used to
access the value of a field in the JSON array. For example, `:owner.name` will
access the value of the `name` field in the `owner` object where as `owner.name`
will be interpreted as a string.

The DSL is converted into the AST (see the above EBNF) before being evaluated.
This query AST is evaluated against each element of the JSON array, and if it
returns `True`, the corresponding index into the JSON array for that element is
added to the result. This is how we filter the JSON array. Alternatively, since
queries can also specify general functions, the result may be a value rather
than a Boolean, e.g., `['lower-case', 'Python']` will return `'python`.

## Relative Advantages of AST and DSL

Both have their own advantages and can be used interchangeably based on the
user's preference. The AST is:

- programmatic
- easily manipulated
- can be generated from a DSL
- easily serialized for storage or transmission
- allows for operators to be queries, facilitating some meta-programming

The DSL is:

- More human-readable, e.g. infix notation for logical operators
- Easier to write and understand
- Compact

## Installation

You can install `jaf` via PyPI:

```bash
pip install `jaf`
```

Or install directly from the source:

```bash
git clone https://github.com/queelius/jaf.git
cd jaf
pip install .
```

## Examples

Suppose we have a list of repositories in the following format:

```python
repos = [
    {
        'id': 1,
        'name': 'DataScienceRepo',
        'language': 'Python',
        'stars': 150,
        'forks': 30,
        'description': 'A repository for data science projects.',
        'owner': {
            'name': 'alice',
            'active': True
        }
    },
    # ... other repositories ...
]
```

### AST-Based Query

Filter repositories where the lower-case of `language` is `"python"`,
`owner.active` is `True`, and `stars` are greater than `100`:

```python
query = ['and',
    ['eq?',
        ['lower-case', ['path', 'language'], 'Python']],
        ['path', 'owner.active'],
        ['gt?', ['path', 'stars'], 100]]

filtered = jaf(sample_repos, query_ast)
print("Filtered Repositories:")
pprint(filtered)
# Output: [1, ...]
```

### DSL-Based Query

The equivalent query using the DSL:

```python
query = '(lower-case :language) eq? "python" AND :owner.active AND :stars gt? 100'
filtered = jaf(repos, query)
print("Filtered Repositories:")
print(filtered)
# Output: [1, ...]
```

### Complex Queries

Combine multiple conditions with logical operators.

```python
query = ':language neq? "R" AND (:stars gt? 100 OR :forks gt? 50)'
filtered = jaf(repos, query)
print("Filtered Repositories:")
```

### Handling Errors

Catch and handle filtering errors gracefully.

```python
try:
    invalid_query = 'language unknown "Python"'
    jaf(repos, invalid_query)
except FilterError as e:
    print(f"Error: {e}")
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

# `jaf` - JSON Array Filter

`jaf` is a versatile filtering system designed to sift through JSON arrays.
Queries are represented using an Abstract Syntax Tree (AST) based on nested
lists, where each list takes the form:

```python
[<operator>, <arguments>]
```

For example, the query

```python
['and', ['language', 'eq?', 'Python'], ['stars', 'gt?', 100]]
```

We also provide a DSL (Domain-Specific Language) that allows users to craft
queries using an intuitive infix notation. For example, the query

```python
'language eq? "Python" AND stars gt? 100'
```

The DSL is converted into an AST before being evaluated. The AST is then used
to filter the JSON array based on the specified conditions.

Both have their own advantages and can be used interchangeably based on the
user's preference. The AST is programmatic and can be easily manipulated, while
the DSL is more human-readable and compact.

Note that we do not use operators like `==` or `>`, but instead use `eq?` and
`gt?`. The primary reason for this choice is that we provide a command-line
tool, and if we used `>` it would be interpreted as a redirection operator
by the shell.

We provide both predicates, which end with a `?`, e.g., `eq?`, and general
operators, e.g., `and`, `lower-case`, etc. The predicates are used to compare
values, while the operators are used to combine predicates or perform other
operations that make the desired comparison possible. For example, the
`lower-case` operator is used to convert a string to lowercase before
comparison, so that the comparison is case-insensitive. Here is an example
query that uses the `lower-case` operator:

```python
['and', ['lower-case', 'language'], ['eq?', 'python']]
```

This query will filter repositories where the `language` field is equal to
`"python"`, regardless of the case of the letters.


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
sample_repos = [
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

Filter repositories where `language` is `"Python"`.

```python
query = ['language', 'eq', 'Python']
filtered = jaf(sample_repos, query_ast)
print("Filtered Repositories:", [repo['id'] for repo in filtered])
# Output: [1, 3]
```

### DSL-Based Query

Filter repositories where `language` is `"Python"` and `stars` are greater than `100`

```python
query = 'language eq "Python" AND stars gt 100'
filtered = jaf(sample_repos, query)
print("Filtered Repositories:", [repo['id'] for repo in filtered])
# Output: [1, 3]
```

### Complex Queries

Combine multiple conditions with logical operators.

```python
query = 'NOT language eq "R" AND (stars gt 100 OR forks gt 50)'
filtered = jaf(sample_repos, query)
print("Filtered Repositories:", [repo['id'] for repo in filtered])
# Output: [1, 3, 6]
```

### Handling Errors

Catch and handle filtering errors gracefully.

```python
try:
    invalid_query = 'language unknown "Python"'
    jaf(sample_repos, invalid_query)
except FilterError as e:
    print(f"Error: {e}")
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

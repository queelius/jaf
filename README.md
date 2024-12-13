# SiftArray

SiftArray is a versatile filtering system designed to sift through JSON arrays using advanced filtering queries based on AST (Abstract Syntax Tree) and DSL (Domain-Specific Language). Whether you're dealing with simple conditions or complex nested queries, SiftArray offers a robust and intuitive solution.

## Features

- **AST-Based Filtering**: Define filters using nested lists or dictionaries.
- **DSL Support**: Utilize an intuitive, infix notation for crafting complex queries.
- **Extensible Grammar**: Easily add new operators or modify existing ones.
- **Comprehensive Error Handling**: Receive clear and descriptive error messages for invalid queries.
- **Modular Design**: Separate components for AST, DSL, and evaluation logic ensure maintainability and scalability.

## Installation

You can install SiftArray via PyPI:

```bash
pip install siftarray
```

Or install directly from the source:

```bash
git clone https://github.com/yourusername/SiftArray.git
cd SiftArray
pip install .
```

## Usage

### Importing SiftArray

```python
from siftarray.core import sift_array, FilterError
```

### Defining Your Data

Ensure your JSON data is a list of dictionaries.

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
query_ast = ['language', 'eq', 'Python']
filtered = sift_array(sample_repos, query_ast)
print("Filtered Repositories:", [repo['id'] for repo in filtered])
# Output: [1, 3]
```

### DSL-Based Query

Filter repositories where `language` is `"Python"` and `stars` are greater than `100`

```python
query_dsl = 'language eq "Python" AND stars gt 100'
filtered = sift_array(sample_repos, query_dsl, is_dsl=True)
print("Filtered Repositories:", [repo['id'] for repo in filtered])
# Output: [1, 3]
```

### Complex Queries

Combine multiple conditions with logical operators.

```python
query_complex = 'NOT language eq "R" AND (stars gt 100 OR forks gt 50)'
filtered = sift_array(sample_repos, query_complex, is_dsl=True)
print("Filtered Repositories:", [repo['id'] for repo in filtered])
# Output: [1, 3, 6]
```

### Handling Errors

Catch and handle filtering errors gracefully.

```python
try:
    invalid_query = 'language unknown "Python"'
    sift_array(sample_repos, invalid_query, is_dsl=True)
except FilterError as e:
    print(f"Error: {e}")
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

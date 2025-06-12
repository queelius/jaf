# `jaf` - JSON Array Filter

`jaf` is a simple, focused filtering system for JSON arrays. It's designed to be **not Turing-complete** and focuses solely on filtering with predictable boolean results.

## Core Philosophy

- **Simple**: Easy to understand and debug
- **Predictable**: Every query returns boolean results for filtering
- **Secure**: No arbitrary code execution or side effects
- **Focused**: Designed specifically for JSON array filtering

## Query Formats

JAF supports two query formats:

### 1. AST Format (Nested Lists)
Direct S-expression syntax: `[operator, arg1, arg2, ...]`

```python
["eq?", ["path", ["name"]], "John"]
["and", 
  ["exists?", ["path", ["email"]]], 
  ["gt?", ["path", ["stars"]], 100]]
```

### 2. DSL Format (Human-Readable)
Intuitive infix notation that compiles to AST:

```text
:name eq? "John"
:email exists? AND :stars gt? 100
```

## DSL Grammar

The DSL provides a more natural syntax that gets parsed into the AST format:

### Basic Syntax
```text
# Path access (prefixed with :)
:name                    # ["path", ["name"]]
:user.email             # ["path", ["user", "email"]]
:items.*.status         # ["path", ["items", "*", "status"]]
:data.0.value           # ["path", ["data", 0, "value"]]

# Predicates
:name eq? "John"        # ["eq?", ["path", ["name"]], "John"]
:stars gt? 100          # ["gt?", ["path", ["stars"]], 100]

# Function calls
(lower-case :language) eq? "python"   # ["eq?", ["lower-case", ["path", ["language"]]], "python"]
(length :items) gt? 5                 # ["gt?", ["length", ["path", ["items"]]], 5]

# Logical operations
:active eq? true AND :stars gt? 100   # ["and", ["eq?", ["path", ["active"]], true], ["gt?", ["path", ["stars"]], 100]]
:type eq? "repo" OR :type eq? "fork"  # ["or", ["eq?", ["path", ["type"]], "repo"], ["eq?", ["path", ["type"]], "fork"]]

# Grouping with parentheses
(:stars gt? 100) AND (:forks gt? 50)
```

### DSL to AST Examples

| DSL | AST |
|-----|-----|
| `:name eq? "John"` | `["eq?", ["path", ["name"]], "John"]` |
| `(lower-case :language) eq? "python"` | `["eq?", ["lower-case", ["path", ["language"]]], "python"]` |
| `:items.*.status eq? "done"` | `["eq?", ["path", ["items", "*", "status"]], "done"]` |
| `:active eq? true AND :stars gt? 100` | `["and", ["eq?", ["path", ["active"]], true], ["gt?", ["path", ["stars"]], 100]]` |

## Builtins

### 1. Special Forms (Custom Evaluation)
- `path` - Extract values from nested structures
- `exists?` - Check if a path exists
- `if` - Conditional evaluation
- `and`, `or`, `not` - Logical operations with short-circuit evaluation

### 2. Predicates (Return Boolean)
Functions that canonically end with `?` and return true/false:
- **Comparison**: `eq?`, `neq?`, `gt?`, `gte?`, `lt?`, `lte?`
- **Containment**: `in?`
- **String matching**: `starts-with?`, `ends-with?`, `regex-match?`, `close-match?`, `partial-match?`

### 3. Value Extractors (Support Predicates)
Functions that extract or transform values for comparison:
- **Data access**: `length`, `type`, `keys`
- **String transformation**: `lower-case`, `upper-case`
- **Date/time**: `now`, `date`, `datetime`, `date-diff`, `days`, `seconds`

## Path System

Paths support:
- **Nested objects**: `:user.email` → `obj["user"]["email"]`
- **Array indices**: `:data.0.value` → `obj["data"][0]["value"]`
- **Wildcards**: 
  - `*` matches any single field/index: `:items.*.name`
  - `**` matches any field at any depth: `:**.error`

If a wildcard path matches multiple values, the predicate succeeds if **any** match satisfies the condition.

## Examples

Given this data:
```python
repos = [
    {
        'name': 'DataScienceRepo',
        'language': 'Python', 
        'stars': 150,
        'owner': {'name': 'alice', 'active': True},
        'items': [{'status': 'done'}, {'status': 'pending'}]
    },
    # ... more repos
]
```

### DSL Examples
```python
import jaf

# Basic filtering
result = jaf(repos, ':language eq? "Python"')

# Case-insensitive search  
result = jaf(repos, '(lower-case :language) eq? "python"')

# Complex conditions
result = jaf(repos, ':owner.active eq? true AND :stars gt? 100')

# Wildcard usage
result = jaf(repos, ':items.*.status eq? "done"')

# Multiple conditions with grouping
result = jaf(repos, '(:stars gt? 100) AND (:language eq? "Python" OR :language eq? "JavaScript")')
```

### AST Examples
```python
# Same queries using direct AST format
result = jaf(repos, ["eq?", ["path", ["language"]], "Python"])

result = jaf(repos, ["eq?", ["lower-case", ["path", ["language"]]], "python"])

result = jaf(repos, ["and", 
    ["eq?", ["path", ["owner", "active"]], True],
    ["gt?", ["path", ["stars"]], 100]])
```

## Output Format

JAF returns a simple list of indices for objects that matched the filter:

```python
repos = [{"name": "A"}, {"name": "B"}, {"name": "C"}]
result = jaf(repos, ':name eq? "B"')
print(result)  # [1] - index of the matching object
```

## Installation

```bash
pip install jaf
```

## Testing DSL Parsing

You can test DSL parsing using the included parser:

```bash
# Parse a single expression
python -m jaf.dsl.parse --expr ':name eq? "John" AND :stars gt? 100'

# See example expressions
python -m jaf.dsl.parse --examples
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

MIT License. See `LICENSE` file for details.

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
name eq? "John"
(exists? email) and (stars gt? 100)
```
*(Note: DSL parser is a planned feature or may be provided separately.)*

## Path System

Paths are used to access data within JSON objects. In the AST format, they are represented as lists of components.

Paths support:
- **Nested objects**: `["path", ["user", "email"]]` corresponds to `obj["user"]["email"]`
- **Array indices**: `["path", ["data", 0, "value"]]` corresponds to `obj["data"][0]["value"]`
- **Empty path**: An empty path `["path", []]` resolves to the current root object.
- **Wildcards**:
  - `*` matches any single field name or array index at the current level (e.g., `["path", ["items", "*", "name"]]`).
  - `**` matches any field name recursively at any depth (e.g., `["path", ["**", "error"]]`). It also matches the current level if the subsequent path parts match.

**Motivation for Wildcards**:
Wildcards are powerful tools for querying data when:
- The exact structure is variable (e.g., different objects have different sets of keys under a common parent).
- You need to check or aggregate across multiple sub-items without knowing their specific keys or indices (e.g., "do any of the tasks in this list have status 'urgent'?").
- You need to search for data deeply nested within an object without specifying the full path (e.g., "is there an 'error_code' field anywhere in this log entry?").

**Path Resolution**:
- Accessing a path that doesn't exist (e.g., `["path", ["user", "nonexistent_field"]]`) results in a value that behaves as "not found" in predicates (e.g., `exists?` returns `false`, comparisons typically `false`). Internally, this is represented as an empty list.
- Wildcard paths (e.g., `["path", ["items", "*", "status"]]`) collect all matching values into a special list. If no values match, this special list will be empty.

**Wildcard Predicate Behavior (Existential Quantification)**:
When a path containing wildcards is used as an argument to a predicate, JAF applies the concept of **existential quantification**. This means the predicate evaluates to `true` if **there exists at least one value** (or combination of values, if multiple wildcard paths are involved) among those matched by the wildcard(s) that satisfies the predicate's condition.

For example:
- `["eq?", ["path", ["projects", "*", "status"]], "completed"]`
  This query asks: "Does there exist any project whose status is 'completed'?" If even one project has this status, the entire expression is `true`.

This "any match is sufficient" behavior is intuitive for filtering tasks where you're looking for the presence of a certain characteristic within a collection or across a structure. Universal quantification ("for all items to match") is not directly supported by the `path` operator itself, as `path` is fundamentally about data extraction. Achieving universal checks would typically involve different logical constructs (e.g., negating an existential check on the opposite condition).

## Available Operators

### Special Forms
- `path`: `["path", path_components_list]` - Extracts value(s) at the given path.
- `if`: `["if", condition_expr, true_expr, false_expr]` - Conditional evaluation.
- `and`: `["and", expr1, expr2, ...]` - Logical AND (short-circuiting).
- `or`: `["or", expr1, expr2, ...]` - Logical OR (short-circuiting).
- `not`: `["not", expr]` - Logical NOT.
- `exists?`: `["exists?", ["path", path_components_list]]` - Checks if a path resolves to any value(s).

### Predicates (Return Boolean)
- `eq?`: `["eq?", arg1, arg2]` - Equal.
- `neq?`: `["neq?", arg1, arg2]` - Not equal.
- `gt?`: `["gt?", arg1, arg2]` - Greater than.
- `gte?`: `["gte?", arg1, arg2]` - Greater than or equal.
- `lt?`: `["lt?", arg1, arg2]` - Less than.
- `lte?`: `["lte?", arg1, arg2]` - Less than or equal.
- `in?`: `["in?", item, list_or_string]` - Membership check.
- `starts-with?`: `["starts-with?", prefix, string]` - String starts with.
- `ends-with?`: `["ends-with?", suffix, string]` - String ends with.
- `regex-match?`: `["regex-match?", pattern, string]` - Regex match.
- `close-match?`: `["close-match?", str1, str2]` - Fuzzy string match (e.g., Levenshtein distance based).
- `partial-match?`: `["partial-match?", str1, str2]` - Fuzzy partial string match.

### Value Extractors & Transformers
- `length`: `["length", list_or_string]` - Returns length.
- `type`: `["type", arg]` - Returns type name as string (e.g., "string", "number", "list").
- `keys`: `["keys", object]` - Returns list of object keys.
- `lower-case`: `["lower-case", string]` - Converts string to lower case.
- `upper-case`: `["upper-case", string]` - Converts string to upper case.
- `now`: `["now"]` - Current datetime.
- `date`: `["date", string_date]` - Parses string to date.
- `datetime`: `["datetime", string_datetime]` - Parses string to datetime.
- `date-diff`: `["date-diff", date1, date2]` - Difference between two dates.
- `days`: `["days", timedelta]` - Extracts days from timedelta.
- `seconds`: `["seconds", timedelta]` - Extracts seconds from timedelta.

## Installation

```bash
pip install jaf # (Hypothetical, if published)
# Or clone and install locally
```

## Usage

```python
from jaf import jaf

data = [
    {"name": "Alice", "age": 30, "skills": ["python", "js"]},
    {"name": "Bob", "age": 24, "skills": ["java", "c++"]},
    {"name": "Charlie", "age": 30, "skills": ["python", "go"]}
]

# Find users named Alice
query_ast = ["eq?", ["path", ["name"]], "Alice"]
result_indices = jaf(data, query_ast)
print(result_indices)  # Output: [0]

# Find users older than 25 with python skills
query_ast_complex = [
    "and",
    ["gt?", ["path", ["age"]], 25],
    ["in?", "python", ["path", ["skills"]]]
]
result_indices_complex = jaf(data, query_ast_complex)
print(result_indices_complex)  # Output: [0, 2]

# Using DSL (if available)
# query_dsl = "(age gt? 25) and ('python' in? skills)"
# result_indices_dsl = jaf(data, query_dsl)
# print(result_indices_dsl) # Output: [0, 2]
```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.

## License
MIT

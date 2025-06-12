# `jaf` - JSON Array Filter

`jaf` is a simple, focused filtering system for JSON arrays. It\'s designed to be **not Turing-complete** and focuses solely on filtering with predictable boolean results.

## Core Philosophy

- **Simple**: Easy to understand and debug
- **Predictable**: Every query returns boolean results for filtering
- **Secure**: No arbitrary code execution or side effects
- **Focused**: Designed specifically for JSON array filtering

## Query Format (AST)

JAF queries use an S-expression syntax (nested lists):
`[operator, arg1, arg2, ...]`

**Examples**:

```python
# Find objects where name is "John"
["eq?", ["path", [["key", "name"]]], "John"]

# Find objects where email exists AND stars are greater than 100
["and", 
  ["exists?", ["path", [["key", "email"]]]], 
  ["gt?", ["path", [["key", "stars"]]], 100]
]
```

(A human-readable DSL format may be a planned feature but is not part of the core AST evaluation.)

## Path System (Tagged AST)

At the heart of data access in JAF is its Path System. This system can be thought of as a mini-language for navigating JSON structures. Paths are represented as a **list of tagged components**, forming their own Abstract Syntax Tree (AST) for data traversal.

**Path Component Tags:**

- `["key", <string_key_name>]`: Accesses an object property.
  - Example: `[["key", "user"], ["key", "email"]]` (for `obj["user"]["email"]`)
- `["index", <integer_index>]`: Accesses an array element (supports negative indexing).
  - Example: `[["key", "data"], ["index", 0], ["key", "value"]]` (for `obj["data"][0]["value"]`)
- `["indices", [<int_idx1>, ...]]`: Accesses multiple array elements by specific indices.
- `["slice", <start_or_null>, <stop_or_null>, <step_or_null>]`: Accesses a slice of an array (Python-like slicing, `null` can be used for defaults, e.g., `null` for `start` means from the beginning, `null` for `stop` means till the end, `null` for `step` means `1`).
- `["regex_key", <pattern_string>]`: Accesses object properties where keys match a regex.
- `["wc_level"]`: Wildcard for the current level.
  - Example: `[["key", "items"], ["wc_level"], ["key", "name"]]` (gets all names from items)
- `["wc_recursive"]`: Recursive wildcard.
  - Example: `[["wc_recursive"], ["key", "error"]]` (finds any "error" key at any depth)

**Path Evaluation (`eval_path`):**

The `["path", path_components_list]` special form uses an internal `eval_path` function to resolve these path expressions against a JSON object.

- **Single Value**: If a path resolves to one specific value (including `null`), that value is returned.
- **Multiple Values (`PathValues`)**: If a path uses components like `indices`, `slice`, `regex_key`, `wc_level`, or `wc_recursive`, it can return multiple values. These are returned in a special list-like object called `PathValues`.
- **Not Found**: If a path segment doesn't match (e.g., key not found, index out of bounds for a specific index), `eval_path` returns an empty list `[]` to signify "not found".
- **Empty Path**: `["path", []]` returns the original object.

**Path Expressions in JAF Queries:**

The `["path", path_components_list]` expression is how this path language is embedded into JAF queries. The result of this expression (a single value, `None`, `[]`, or a `PathValues` list) is then used by the enclosing JAF operator.

**Wildcard/Multi-Value Path Behavior in Predicates (Existential Quantification - ∃):**

When a path expression results in a `PathValues` list (due to wildcards, slices, etc.) and is used as an argument to a predicate:

- The predicate is `true` if **at least one** value (or combination of values, if multiple such paths are arguments) from the `PathValues` satisfies the predicate.
- Example: `["eq?", ["path", [["key", "projects"], ["wc_level"], ["key", "status"]]], "completed"]`
  This is true if *any* project has its status as "completed".

This "any match is sufficient" behavior (existential quantification) is intuitive for filtering. Universal quantification ("for all items to match") can often be constructed using negation and existential checks.

## Available Operators

### Special Forms

- `path`: `["path", path_components_list]` - Extracts value(s) using the tagged AST path.
- `if`: `["if", condition_expr, true_expr, false_expr]` - Conditional.
- `and`: `["and", expr1, expr2, ...]` - Logical AND (short-circuiting).
- `or`: `["or", expr1, expr2, ...]` - Logical OR (short-circuiting).
- `not`: `["not", expr]` - Logical NOT.
- `exists?`: `["exists?", ["path", path_components_list]]` - Checks if `eval_path` returns anything other than `[]` (empty list). A path to `null` *does* exist.

### Predicates (Return Boolean)

- `eq?`, `neq?`, `gt?`, `gte?`, `lt?`, `lte?` (Comparison)
- `in?` (Membership)
- `starts-with?`, `ends-with?`, `regex-match?` (String matching)
- `close-match?`, `partial-match?` (Fuzzy string matching)

### Value Extractors & Transformers

- `length`: `["length", list_or_string]`
- `type`: `["type", arg]`
- `keys`: `["keys", object]`
- `lower-case`, `upper-case` (String case)
- `now`, `date`, `datetime`, `date-diff`, `days`, `seconds` (Date/Time utilities)

## Full Specification

For detailed information on all operators, path components, and evaluation rules, please see `SPECIFICATION.md`.

## Installation

```bash
# (Assuming package is published)
# pip install jaf
```

## Usage Example (Python)

```python
from jaf import jaf

data = [
    {"id": 1, "name": "Alice", "tags": ["dev", "python"], "status": "active"},
    {"id": 2, "name": "Bob", "tags": ["qa", "java"], "status": "inactive"},
    {"id": 3, "name": "Charlie", "tags": ["dev", "go"], "status": "active", "extra": {"priority": "high"}}
]

# Find active developers (status is "active" and "dev" is in tags)
query_active_devs = [
    "and",
    ["eq?", ["path", [["key", "status"]]], "active"],
    ["in?", "dev", ["path", [["key", "tags"]]]]
]

result_indices = jaf(data, query_active_devs)
print(f"Active developers found at indices: {result_indices}")
# Output: Active developers found at indices: [0, 2]

# Find items where any field under 'extra' (if it exists) has the value 'high'
query_extra_high = [
    "eq?", ["path", [["key", "extra"], ["wc_level"]]], "high"]

result_indices_extra = jaf(data, query_extra_high)
print(f"Items with 'high' in extra fields: {result_indices_extra}")
# Output: Items with 'high' in extra fields: [2]

# Find items with a name ending in 'ice' using a direct string predicate
query_name_ends_ice_direct = [
    "ends-with?", "ice", ["path", [["key", "name"]]]
]
result_name_ice_direct = jaf(data, query_name_ends_ice_direct)
print(f"Items with name ending in 'ice' (direct): {result_name_ice_direct}")
# Output: Items with name ending in 'ice' (direct): [0]

# Check if any user has "python" as one of their first two tags
query_python_first_two_tags = [
    "in?", "python", ["path", [["key", "tags"], ["slice", null, 2, null]]]
]
result_python_tags = jaf(data, query_python_first_two_tags)
print(f"Users with 'python' in first two tags: {result_python_tags}")
# Output: Users with 'python' in first two tags: [0]
```

This README provides a high-level overview. For the complete details, refer to `SPECIFICATION.md`.

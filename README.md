# `jaf` - JSON Array Filter

`jaf` is a simple, focused filtering system for JSON arrays, with added support for boolean algebra on filter results and resolving results to original data. It's designed to be **not Turing-complete** and focuses solely on filtering with predictable boolean results.

## Core Philosophy

- **Simple**: Easy to understand and debug
- **Predictable**: Every query returns boolean results for filtering
- **Secure**: No arbitrary code execution or side effects
- **Focused**: Designed specifically for JSON array filtering and result set manipulation

## Output: `JafResultSet`

The `jaf filter` command, and boolean operations, produce a `JafResultSet`. This is a JSON object containing:
- `indices`: A sorted list of integers representing the indices of matched items from the original data.
- `collection_size`: The total number of items in the original data collection.
- `collection_id`: An identifier for the original data collection (e.g., file path, directory path, or a custom ID).
- `filenames_in_collection` (optional): A sorted list of unique file paths that contributed to the collection, especially when filtering a directory.

Example `JafResultSet` (compact JSON output):
```json
{"indices":[0,2],"collection_size":3,"collection_id":"/path/to/data.json","filenames_in_collection":["/path/to/data.json"]}
```

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
- `["root"]`: Represents the root of the object being evaluated.
  - Example: `[["root"], ["key", "config"]]` accesses `obj["config"]` assuming `obj` is the root.

**Path Evaluation (`eval_path`):**

The `["path", path_components_list]` special form uses an internal `eval_path` function to resolve these path expressions against a JSON object.

- **Single Value**: If a path that does not contain multi-match components (like wildcards or slices) resolves to one specific value (including `null`), that value is returned directly.
- **Multiple Values (`PathValues`)**: If a path uses components that can naturally yield multiple results (e.g., `indices`, `slice`, `regex_key`, `wc_level`, or `wc_recursive`), it returns a `PathValues` object. `PathValues` is a specialized list subclass that holds the collection of all values found by the path. It preserves the order of discovery and can contain duplicates if the data and path logic lead to them. It also provides convenience methods like `first()`, `one()`, etc.
- **Not Found (Specific Path)**: If a path that does *not* contain multi-match components fails to resolve (e.g., key not found, index out of bounds for a specific index access), `eval_path` returns an empty list `[]` to signify "not found".
- **Not Found (Multi-match Path)**: If a path *with* multi-match components finds no values, it returns an empty `PathValues` object (e.g., `PathValues([])`). This is distinct from the `[]` returned for a specific path not found.
- **Empty Path**: `["path", []]` returns the original object.

**Wildcard/Multi-Value Path Behavior in Predicates (Existential Quantification - ∃):**

When a path expression results in a `PathValues` object (due to wildcards, slices, etc.) and is used as an argument to a predicate:

- The predicate is `true` if **at least one** value (or combination of values, if multiple such paths are arguments) from the `PathValues` collection satisfies the predicate.
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
- `exists?`: `["exists?", ["path", path_components_list]]` - Checks if `eval_path` returns anything other than `[]` (empty list for specific paths not found) or an empty `PathValues` (for multi-match paths not found). A path to `null` *does* exist.

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

## Command-Line Interface (CLI)

`jaf` provides a CLI for filtering data and performing operations on result sets.

### `jaf filter`

Filters JSON/JSONL data from a file or directory.

```bash
jaf filter <input_source> --query '<query_ast_json_string>' [options]
```
- `<input_source>`: Path to a JSON/JSONL file or a directory.
- `--query`: JAF query string (JSON AST format).
- `--recursive`: Recursively search directories (if `input_source` is a directory).
- `--collection-id <id>`: Optional ID for the data collection.
- `--resolve`: Output matching original JSON objects (JSONL) instead of `JafResultSet` JSON.
- `--log-level <LEVEL>`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

**Example:**
```bash
jaf filter data.jsonl --query '["eq?", ["path", [["key", "status"]]], "active"]'
# Outputs a JafResultSet JSON

jaf filter data_dir --query '["gt?", ["path", [["key", "count"]]], 10]' --recursive --resolve
# Outputs matching objects as JSONL from files in data_dir
```

### Result Set Operations (Boolean Algebra)

Treating JAF query results (`JafResultSet` instances) as sets allows us to apply boolean algebra (AND, OR, NOT, etc.) to combine them. This is beneficial for modularity, clarity, and reusability. Inputs for these commands are `JafResultSet` JSON, typically from `stdin` or files. Output is a new `JafResultSet` JSON.

- **`jaf and [rs1_path_or_-] [rs2_path_or_-]`**: Logical AND (intersection).
- **`jaf or [rs1_path_or_-] [rs2_path_or_-]`**: Logical OR (union).
- **`jaf not [rs_path_or_-]`**: Logical NOT (complement).
- **`jaf xor [rs1_path_or_-] [rs2_path_or_-]`**: Logical XOR (symmetric difference).
- **`jaf difference [rs1_path_or_-] [rs2_path_or_-]`**: Logical SUBTRACT (rs1 - rs2).

Inputs can be file paths or `-` for `stdin`.
- `jaf op - file2.json`: rs1 from stdin, rs2 from file2.json
- `jaf op file1.json -`: rs1 from file1.json, rs2 from stdin (less common for binary ops)
- `jaf op file1.json file2.json`: rs1 from file1.json, rs2 from file2.json
- `jaf op file.json`: For binary ops, this means rs1 from stdin, rs2 from file.json. For unary ops (like `not`), rs1 is from file.json.
- `jaf op -`: For unary ops, rs1 from stdin. For binary ops, this is an error (cannot read both from stdin).

**Example Workflow:**
```bash
# rs_active.json contains indices of active users
jaf filter users.jsonl --query '["eq?",["path",[["key","status"]]],"active"]' > rs_active.json

# rs_dev.json contains indices of developers
jaf filter users.jsonl --query '["in?","dev",["path",[["key","tags"]]]]' > rs_dev.json

# Find active developers
jaf and rs_active.json rs_dev.json > rs_active_devs.json

# Alternatively, using pipes:
jaf filter users.jsonl --query '["eq?",["path",[["key","status"]]],"active"]' | \
  jaf and - <(jaf filter users.jsonl --query '["in?","dev",["path",[["key","tags"]]]]') > rs_active_devs_pipe.json
```

### `jaf resolve`

Resolves a `JafResultSet` (from file or `stdin`) to the original JSON objects, outputting them as JSONL. This requires the `JafResultSet` to have a resolvable `collection_id` (as a file path) or `filenames_in_collection`.

```bash
jaf resolve [jrs_path_or_-]
```
**Example:**
```bash
# Assuming rs_active_devs.json was created as above
jaf resolve rs_active_devs.json
# Outputs the original JSON objects for active developers as JSONL
```

## Full Specification

For detailed information on all operators, path components, `JafResultSet` structure, and evaluation rules, please see `SPECIFICATION.md`.

## Installation

```bash
# (Assuming package is published)
# pip install jaf

# For local development:
# git clone <repository_url>
# cd jaf
# pip install -e .
```

## Usage Example (Python)

```python
from jaf import jaf, JafResultSet

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

# The jaf function returns a JafResultSet instance
result_set_active_devs: JafResultSet = jaf(data, query_active_devs, collection_id="my_data_v1")
print(f"Active developers JafResultSet: {result_set_active_devs}")
print(f"Indices of active developers: {list(result_set_active_devs)}") # Iterate or convert to list

# To get the actual objects:
active_dev_objects = [data[i] for i in result_set_active_devs.indices]
print(f"Active developer objects: {active_dev_objects}")

# Example of using JafResultSet methods (programmatic boolean algebra)
query_python_users = ["in?", "python", ["path", [["key", "tags"]]]]
rs_python: JafResultSet = jaf(data, query_python_users, collection_id="my_data_v1")

# Active Python developers
rs_active_python_devs = result_set_active_devs.AND(rs_python) # or result_set_active_devs & rs_python
print(f"Active Python developers (indices): {list(rs_active_python_devs)}")

# Using the get_matching_objects method (if JRS was from a file source and had metadata)
# For this example, data is in-memory, so direct indexing is shown above.
# If rs_active_python_devs was loaded from a file and had filenames_in_collection:
# try:
#   original_objects = rs_active_python_devs.get_matching_objects()
#   print(f"Original objects for active Python devs: {original_objects}")
# except JafResultSetError as e:
#   print(f"Could not resolve objects: {e}")

```

This README provides a high-level overview. For the complete details, refer to `SPECIFICATION.md`.

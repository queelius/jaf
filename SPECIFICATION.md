# JAF (JSON Array Filter) - Specification v1.3

## Overview

JAF is a simple, focused domain-specific language for filtering JSON arrays. It's designed to be **not Turing-complete** and focuses solely on filtering with predictable boolean results. It also supports boolean algebraic operations on sets of filter results.

## Core Philosophy

- **Simple**: Easy to understand and debug
- **Predictable**: Every query returns boolean results for filtering
- **Secure**: No arbitrary code execution or side effects
- **Focused**: Designed specifically for JSON array filtering and result set manipulation

## Data Model

**Input to `jaf` function**: `Array<JsonValue>` - An array of any valid JSON values.
**Output of `jaf` function**: `JafResultSet` - An object representing the filter results.

## `JafResultSet` Object

A `JafResultSet` is a JSON-serializable object that represents the outcome of a `jaf` filter operation.

**Core Attributes:**

When serialized to JSON (e.g., by the CLI), a `JafResultSet` has the following structure:

-   `indices`: `Array<Number>` (integer)
    -   A sorted list of unique, 0-based indices of the items from the original data array that matched the query.
-   `collection_size`: `Number` (integer)
    -   The total number of items in the original data collection from which the indices were derived. This is crucial for operations like `NOT`.
-   `collection_id`: `Any` (string, number, null, etc.)
    -   An optional identifier for the original data collection. This helps ensure that boolean operations are performed between result sets derived from the same logical collection. It can be a file path, directory path, or a user-defined ID.
-   `collection_source`: (Optional) A dictionary containing metadata about the original data source, enabling data resolution.
    -   `{"type": "jsonl", "path": "/path/to/file.jsonl"}`
    -   `{"type": "json_array", "path": "/path/to/file.json"}`
    -   `{"type": "directory", "path": "/path/to/dir", "files": ["/path/to/dir/a.json", ...]}`
    -   `{"type": "buffered_stdin", "format": "jsonl", "content": [...]}`: Used when `stdin` is piped to `jaf filter`. The content is buffered to allow for subsequent resolution in a pipe chain.
-   `query`: (Optional) The JAF query AST that produced this result set.

**Example JSON Output (compact):**
```json
{"indices":[0,2],"collection_size":3,"collection_id":"/path/to/data_dir","collection_source":{"type":"directory","path":"/path/to/data_dir","files":["/path/to/data_dir/a.json"]}}
```
If `collection_source` is not present or `null`, it's omitted from the JSON.

## Query Format

JAF queries are represented as an Abstract Syntax Tree (AST) using JSON arrays (lists in Python). The first element of the array is a string representing the operator, and the subsequent elements are its arguments.

```python
# Example: Find objects where the "status" field is "active"
["eq?", ["path", [["key", "status"]]], "active"]
```

## @ Special Path Notation

JAF provides a concise `@` syntax as an alternative to the explicit `["path", ...]` form. This notation significantly reduces verbosity while maintaining full compatibility with the path system.

**Syntax Forms:**

1. **String Format**: `"@path.expression"`
   - Most concise form for simple path access
   - Example: `"@user.name"`, `"@items.*.status"`, `"@data[0].value"`

2. **Explicit AST with String**: `["@", "path.expression"]`
   - Explicit operator form using string path
   - Example: `["@", "user.name"]`

3. **Explicit AST with Path Components**: `["@", path_components_list]`
   - Explicit operator form using tagged path components
   - Example: `["@", [["key", "user"], ["key", "name"]]]`

**Equivalence:**

All three forms are functionally equivalent to the traditional `["path", ...]` syntax:

```python
# These are all equivalent:
"@user.name"                                    # Concise string format
["@", "user.name"]                              # Explicit with string
["@", [["key", "user"], ["key", "name"]]]       # Explicit with AST
["path", "user.name"]                           # Traditional syntax
["path", [["key", "user"], ["key", "name"]]]    # Traditional with AST
```

**Motivation:**
- **Conciseness**: `"@user.name"` vs `["path", "user.name"]`
- **Readability**: Reduces visual clutter in complex queries
- **Familiarity**: `@` is commonly used for references in many languages
- **Full Compatibility**: Works with all path features (wildcards, indexing, etc.)
- **Coexistence**: Traditional `["path", ...]` syntax remains fully supported

## Path System

The JAF Path System is a small, dedicated sub-language for data traversal within JSON structures. Paths are **lists of tagged components** used within the `["path", path_components_list]` special form or the `@` notation. This tagged structure (its own AST) provides a uniform and explicit way to define how to traverse the JSON data.

Each component in the `path_components_list` is a list itself, where the first element is a tag (string) indicating the type of path segment, and subsequent elements are arguments for that segment type.

**Supported Path Component Tags:**

1.  `["key", <string_key_name>]`
    *   Accesses an object's property by its string key.
    *   Example: `[["key", "user"]]` accesses `obj["user"]`.

2.  `["index", <integer_index_value>]`
    *   Accesses an array element by its integer index (supports negative indexing).
    *   Example: `[["key", "items"], ["index", 0]]` accesses `obj["items"][0]`.

3.  `["indices", [<int_idx1>, <int_idx2>, ...]]`
    *   Accesses multiple array elements by a list of specific integer indices.
    *   Returns a list of values found at these indices.
    *   Example: `[["key", "tags"], ["indices", [0, 2, 4]]]` accesses `obj["tags"][0]`, `obj["tags"][2]`, and `obj["tags"][4]`.

4.  `["slice", <start_val_or_null> [, <stop_val_or_null> [, <step_val_or_null>]]]`
    *   Accesses a slice of an array. The AST component will have 1, 2, or 3 values after the "slice" tag, corresponding to `start`, `stop`, and `step`.
    *   `start_val_or_null`: The starting index. If `null` or omitted, defaults to the beginning of the array.
    *   `stop_val_or_null`: The ending index (exclusive). If `null` or omitted, defaults to the end of the array.
    *   `step_val_or_null`: The step value. If `null` or omitted, defaults to `1`. A step of `0` is invalid.
    *   Example ASTs:
        *   `[["slice", 0, 10, 2]]` (explicit start, stop, step)
        *   `[["slice", null, 5]]` (start from beginning, up to 5, step 1) -> equivalent to `[["slice", null, 5, null]]` for evaluation
        *   `[["slice", 2]]` (start from 2, to end, step 1) -> equivalent to `[["slice", 2, null, null]]` for evaluation
    *   Example from query: `[["key", "data"], ["slice", 0, 10, 2]]` accesses elements from index 0 up to (but not including) 10, with a step of 2.
    *   `[["key", "data"], ["slice", null, 5]]` accesses elements from the beginning up to index 5 with a default step of 1.

5.  `["regex_key", <pattern_string>]`
    *   Accesses object properties where keys match the given regular expression pattern.
    *   Returns a list of values from matching keys.
    *   Example: `[["regex_key", "^error_\\d+$"]]` accesses values for keys like "error_1", "error_2", etc. (Note: `\` in regex might need escaping depending on the string representation in the query).

6.  `["fuzzy_key", <target_key_string> [, <cutoff_float> [, <algorithm_string>]]]`
    *   Accesses object properties where keys are similar to the target key using fuzzy string matching.
    *   **Arguments**:
        *   `target_key_string`: The key name to search for (required string).
        *   `cutoff_float`: Minimum similarity score between 0.0 and 1.0 (optional, default: 0.6).
        *   `algorithm_string`: Matching algorithm (optional, default: "difflib"). Supported: "difflib", "levenshtein", "jaro_winkler", "soundex", "metaphone".
    *   **Behavior**: Returns a list of values from keys that meet the similarity threshold, sorted by similarity (best matches first). Exact matches are prioritized.
    *   **Library Dependencies**: Some algorithms require optional libraries (Levenshtein, jellyfish). Falls back to difflib if libraries are unavailable.
    *   **Examples**: 
        *   `[["fuzzy_key", "username"]]` might match "user_name", "userName", "usr_nm"
        *   `[["fuzzy_key", "apikey", 0.8, "levenshtein"]]` uses Levenshtein distance with high precision

7.  `["wc_level"]` (Wildcard - Current Level)
    *   Matches any single field name or array index at the current level of the object or array.
    *   **Motivation**: Useful for iterating over elements of a list or values of a dictionary when the keys/indices are not known beforehand, or when an operation needs to be applied to all direct children.
    *   Example: `[["key", "tasks"], ["wc_level"], ["key", "status"]]` accesses the "status" of each task in the "tasks" list.

8.  `["wc_recursive"]` (Wildcard - Recursive Descent)
    *   Matches any field name recursively at any depth within the current object structure. It also considers the current level for matches if the subsequent path parts align.
    *   **Motivation**: Essential for deep searches where the target data might be nested at varying or unknown depths.
    *   Example: `[["wc_recursive"], ["key", "error_code"]]` finds any "error_code" field anywhere in the object.

9.  `["root"]`
    *   Represents the root of the object against which the entire path expression is being evaluated.
    *   Allows paths to "restart" or reference from the top-level object, even if the current evaluation context is nested due to prior path components.
    *   Example: `[["key", "user"], ["root"], ["key", "config"]]` would first navigate to `user`, then jump back to the root to access `config`.

**Path Evaluation Return Values:**

-   If the path involves components that can naturally yield multiple values (e.g., `["indices", ...]`, `["slice", ...]`, `["regex_key", ...]`, `["wc_level"]`, `["wc_recursive"]`), `eval_path` returns a `PathValues` object. `PathValues` is a specialized list subclass that holds the collection of all values found by the path. It preserves the order of discovery and can contain duplicates if the data and path logic lead to them. It offers convenience methods for accessing its contents (e.g., `first()`, `one()`).
-   If a path that does *not* contain multi-match components fails to resolve at any point (e.g., a key not found, an index out of bounds for a specific index access), `eval_path` returns an empty list `[]`. This signifies "not found" or "no value" for a specific path.
-   If a path *with* multi-match components finds no values, it returns an empty `PathValues` object (e.g., `PathValues([])`). This is distinct from the `[]` returned for a specific path not found.
-   If the `path_components_list` is empty (e.g., `["path", []]` or `["@", ""]`), `eval_path` returns the original `obj`.
-   In rare cases where a path *without* multi-match components unexpectedly yields multiple distinct results, `eval_path` may also wrap these results in a `PathValues` object with a warning.

**Examples of Path Syntax:**

```python
# Traditional syntax:
["path", [["key", "name"]]]                    # Access a top-level key
["path", [["key", "user"], ["key", "email"]]]  # Access a nested key
["path", [["key", "items"], ["wc_level"], ["key", "status"]]]  # Wildcard access

# With @ syntax (equivalent):
"@name"                     # Access a top-level key
"@user.email"               # Access a nested key  
"@items.*.status"           # Wildcard access

# Array operations:
["path", [["key", "data"], ["index", 0], ["key", "value"]]]    # Traditional
"@data[0].value"                                               # @ syntax

# Complex path operations:
["path", [["key", "measurements"], ["slice", 10, 20, null]]]   # Traditional
"@measurements[10:20]"                                         # @ syntax

# Multiple indices:
["path", [["key", "users"], ["indices", [1, 3, 5]]]]          # Traditional
"@users[1,3,5]"                                                # @ syntax

# Regex matching:
["path", [["key", "logs"], ["regex_key", "session_\\w+"]]]     # Traditional
# (@ syntax uses the same string path format for regex patterns)
```

**Behavior of Path Special Forms:**

- When `["path", ...]` or `@` forms are evaluated, they internally call `eval_path` with the current object and the provided path specification.
- The result of `eval_path` (a single value, `None`, `[]`, or a `PathValues` list) is then used as the value of the path expression in the broader JAF query.

## Operator Categories

### 1. Special Forms (Custom Evaluation)

- `path` - Extract values: `["path", [["key", "field"], ["key", "subfield"]]]`
- `@` - Concise path notation: `"@field.subfield"` or `["@", path_expr]`
- `exists?` - Check existence: `["exists?", path_expr]`
- `self` - Reference the current root object: `["self"]`
- `if` - Conditional: `["if", condition, true-expr, false-expr]`
- `and` - Logical AND with short-circuit: `["and", expr1, expr2, ...]`
- `or` - Logical OR with short-circuit: `["or", expr1, expr2, ...]`
- `not` - Logical negation: `["not", expr]`

### 2. Predicates (Return Boolean)

```python
# Comparison
"eq?", "neq?", "gt?", "gte?", "lt?", "lte?"

# Containment / Membership
"in?", "contains?"

# String matching
"starts-with?", "ends-with?", "regex-match?", "close-match?", "partial-match?"

# Type checking
"is-string?", "is-number?", "is-list?", "is-dict?", "is-null?", "is-empty?"
```

### 3. Value Extractors (Support Predicates)

```python
# Data access
"length", "type", "keys", "first", "last", "unique"

# String transformation
"lower-case", "upper-case", "split", "join"

# Arithmetic (variadic)
"+", "-", "*", "/"

# Arithmetic (binary)
"%"

# Date/time
"now", "date", "datetime", "date-diff", "days", "seconds"
```

## Function Signatures

This section provides a brief overview of the function signatures. For detailed behavior, especially with `PathValues`, see the Evaluation Rules.

**Example Signatures:**
```python
["eq?", value1, value2]                                         # value1 == value2
["gt?", value1, value2]                                         # value1 > value2
["in?", item, container]                                        # item in container
["starts-with?", ["lower-case", "@email"], "admin"]             # email.lower().startswith("admin")
```

**New Function Signatures:**
- `["self"]`: Returns the entire root object being evaluated.
- `["contains?", container, item]`: Returns true if `container` (a list or string) contains `item`.
- `["is-string?", value]`, `["is-number?", value]`, etc.: Type-checking predicates.
- `["is-empty?", value]`: Returns true if a list, string, or dict is empty, or if the value is `None`.
- `["first", list]`, `["last", list]`: Return the first or last element of a list.
- `["unique", list]`: Returns a new list with duplicates removed while preserving order.
- `["split", string, delimiter]`, `["join", list, delimiter]`: String manipulation.
- `["+", num1, num2, ...]`: Variadic addition. `(+)` is 0, `(+ a)` is `a`.
- `["*", num1, num2, ...]`: Variadic multiplication. `(*)` is 1, `(* a)` is `a`.
- `["-", num1, num2, ...]`: Variadic subtraction. `(-)` is 0, `(- a)` is `-a`, `(- a b)` is `a-b`.
- `["/", num1, num2, ...]`: Variadic division. `(/)` is an error, `(/ a)` is `1/a`, `(/ a b)` is `a/b`.

## Evaluation Rules

### 1. Literals
- Numbers, strings, booleans, and `null` evaluate to themselves.

### 2. Special Forms
- Special forms have custom evaluation logic and do not necessarily evaluate all their arguments.
- `if`: Evaluates `condition`, then either `true-expr` or `false-expr`.
- `and`/`or`: Evaluate arguments from left to right with short-circuiting.
- `not`: Evaluates its single argument and returns its boolean negation.
- `path`/`@`: Evaluate the path expression against the current object.
- `exists?`: Evaluates the path expression and returns `true` if it resolves to a value (including `null`), `false` otherwise.
- `self`: Returns the current root object without evaluating any arguments.

### 3. Regular Functions (Predicates and Value Extractors)
- All arguments are evaluated first.
- The results are passed to the function's implementation.

### 4. `PathValues` in Predicates and Functions (Interaction with `adapt_jaf_operator`)

When a `PathValues` object (the result of a path expression involving components like `wc_level`, `wc_recursive`, `indices`, `slice`, or `regex_key`) is used as an argument to a predicate or a value-transforming function, JAF (via the `adapt_jaf_operator` utility) employs a specific evaluation strategy. `PathValues` represents the collection of all values found by such a path.

**a. Argument Expansion (Cartesian Product):**

- If one or more arguments to a function are `PathValues` lists, the system generates all possible combinations of individual values from these lists. This is equivalent to a Cartesian product.
- Arguments that are not `PathValues` lists (i.e., single values) are treated as single-element lists for this product generation.
- The underlying function (predicate or transformer) is then invoked for each unique combination of arguments derived from this expansion.

**b. Predicate Evaluation (Existential Quantification - ∃):**

- If the function being called is a **predicate** (typically its name ends with `?`):
  - The predicate evaluates to `true` if **there exists at least one combination** of expanded arguments for which the predicate's condition holds.
  - If all combinations evaluate to `false`, or if any `PathValues` argument was initially empty (resulting in no combinations to test), the overall predicate evaluates to `false`.
  - **Example**: `["eq?", ["path", [["key", "items"], ["wc_level"], ["key", "status"]]], "completed"]` or `["eq?", "@items.*.status", "completed"]`. Let `S = eval_path(obj, path_expr_for_items_status)`. The predicate is true if ∃ *s* ∈ `S` such that `eq?(s, "completed")` is true.
  - Type errors or attribute errors encountered during the evaluation of a specific combination for a predicate cause that particular combination to yield `false`. The overall predicate can still be `true` if another combination succeeds.

**c. Value Extractor/Transformer Evaluation:**

- If the function is a **value extractor or transformer**:
  - The function is called for each combination of arguments generated as per (4a).
  - A list containing the results from all these individual function calls is collected.
  - **Result Aggregation**:
    - If this list of results contains exactly one item, that single item is returned.
    - If this list contains one item which is itself a list (e.g., `[[1,2,3]]` where the inner list was the actual return value of the function for the single combination), this inner list is "unwrapped" and returned (e.g., `[1,2,3]`). This rule does not apply if the single item is already a `PathValues` instance or if the outer list is a `PathValues` instance.
    - Otherwise (multiple results from multiple combinations, or an empty list if no combinations were processed due to an empty `PathValues`), the collected list of results is returned (often as a `PathValues` object if multiple results arose from expansion).
    - If any `PathValues` argument was empty, leading to no combinations, an empty list `[]` is typically returned by the wrapper for the value extractor.

**d. Universal Quantification (`∀`) and `path`:**

- The `path` operator (and `@` notation) itself is designed for *data extraction* – it gathers all values that match a given path. It does not inherently perform universal quantification ("for all").
- Universal quantification is a *checking* operation. While not directly supported by `path`, such checks can often be constructed using negation and existential quantification (e.g., "it is NOT true that there EXISTS an item that does NOT satisfy the condition"). For example, to check if all items in a list have `status == "active"`: `["not", ["in?", false, ["map", ["lambda", "x", ["eq?", ["path", [["key", "x"], ["key", "status"]]], "active"]], ["path", [["key", "items"]]]]]]` (assuming a hypothetical `map` and `lambda` for illustration; JAF does not have these directly but similar logic can be built with `and`/`or` over known items or by checking for the non-existence of a counter-example). A simpler approach for "all items satisfy X" is often "NOT (EXISTS an item that does NOT satisfy X)".

## Boolean Algebra on Result Sets

The `JafResultSet` class provides methods for performing boolean algebra. This allows combining results from multiple `jaf` filter operations.

**Motivation:**
-   **Modularity**: Build complex filters from simpler, pre-computed results.
-   **Clarity**: Easier to express and understand intricate logic.
-   **Reusability**: Saved result sets can be reused.
-   **Formal Semantics**: Leverages well-understood set theory.

**Core Methods (Python `JafResultSet` class):**

Each method returns a *new* `JafResultSet` instance.

1.  `AND(self, other: 'JafResultSet') -> 'JafResultSet'` (or `self & other`)
    *   Performs a set intersection of `self.indices` and `other.indices`.
    *   Requires compatibility (see below).
    *   The resulting `collection_id` is `self.collection_id` if not None, else `other.collection_id`.
    *   The resulting `collection_source` is `self.collection_source` if not None, else `other.collection_source`.

2.  `OR(self, other: 'JafResultSet') -> 'JafResultSet'` (or `self | other`)
    *   Performs a set union of `self.indices` and `other.indices`.
    *   Requires compatibility.
    *   Metadata propagation for `collection_id` and `collection_source` is the same as `AND`.

3.  `NOT(self) -> 'JafResultSet'` (or `~self`)
    *   Calculates the complement relative to `self.collection_size`.
    *   `new_indices = {0, ..., self.collection_size - 1} - self.indices`.
    *   Preserves `self.collection_id` and `self.collection_source`.

4.  `XOR(self, other: 'JafResultSet') -> 'JafResultSet'` (or `self ^ other`)
    *   Performs a set symmetric difference.
    *   Requires compatibility.
    *   Metadata propagation is the same as `AND`.

5.  `SUBTRACT(self, other: 'JafResultSet') -> 'JafResultSet'` (or `self - other`)
    *   Performs set difference (`self.indices - other.indices`).
    *   Requires compatibility.
    *   Metadata propagation is the same as `AND`.

**Compatibility Check (`_check_compatibility`):**
Before performing binary operations (AND, OR, XOR, SUBTRACT), `JafResultSet` instances must be compatible:
-   `collection_size` must be identical.
-   If both `collection_id`s are not None, they must be identical.
-   If these conditions are not met, a `JafResultSetError` is raised.
-   Differences in `collection_source` between two compatible result sets do not raise an error but may result in the output `JafResultSet` inheriting this attribute from one of the operands (typically `self`).

## Resolving Result Sets to Original Data

The `JafResultSet` class provides a method to retrieve the original data objects corresponding to its `indices`.

**`get_matching_objects(self) -> List[Any]`:**
-   Attempts to load the original data objects by using the `collection_source` metadata.
-   **Data Source Determination**:
    1.  The method inspects the `collection_source` dictionary.
    2.  It uses a `CollectionLoader` to find a registered loader function that matches the `type` key in `collection_source` (e.g., "jsonl", "directory", "buffered_stdin").
    3.  The appropriate loader is invoked with the `collection_source` dictionary to retrieve the full list of original objects.
    4.  If `collection_source` is missing or no suitable loader is found, a `JafResultSetError` is raised.
-   **Validation**: After loading, it verifies that the total number of loaded objects matches `self.collection_size`. If not, a `JafResultSetError` is raised.
-   **Return**: A list containing the original data objects at the indices specified in `self.indices`. The objects are returned in the order of the sorted indices.
-   **Errors**: Raises `JafResultSetError` for issues like unresolvable data sources, file not found, or data inconsistencies.

## Error Handling

### Path Errors

- Non-existent specific paths (key not found, index out of bounds for specific index access) result in `eval_path` returning `[]` (empty list).
- A multi-match path that finds no values results in `eval_path` returning an empty `PathValues` object.
- A path to a field that exists but has a `null` value will result in `eval_path` returning `None` (if it's a specific path resolving to that `null`).
- Use `exists?` to check path existence. `exists?` returns `true` if `eval_path` for the given path expression returns anything other than an empty list `[]` (when the path is specific and not found) or an empty `PathValues` (when the path is multi-match and not found). A path to a `null` value *does* exist and `exists?` will return `true`.

### @ Notation Errors

- Empty `@` expressions (e.g., `"@"`) raise `PathSyntaxError`.
- Invalid path syntax in `@` expressions (e.g., `"@[invalid"`) raise `PathSyntaxError` with details about the syntax error.
- Wrong argument count for the `@` operator (when used as `["@", ...]`) raises `ValueError`.

### Type Errors

- Type mismatches within predicate/function evaluations (after `PathValues` expansion) generally cause that specific evaluation instance to return `false` (for predicates) or contribute an error marker/skip (for transformers), rather than halting the entire query. The `adapt_jaf_operator` handles this gracefully.
- Invalid query structure or unknown operators raise `jafError` or `ValueError`.

### `JafResultSet` Errors
-   Attempting boolean operations on incompatible `JafResultSet` instances raises `JafResultSetError`.
-   Failure to load or deserialize a `JafResultSet` (e.g., from JSON) raises `ValueError` or `JafResultSetError`.
-   Failure in `get_matching_objects()` (e.g., source not found, size mismatch) raises `JafResultSetError`.

## Example Queries

### Basic Filtering
```python
# Find all objects where the "active" field is true
["eq?", "@active", true]

# Find all objects where the "type" is "admin"
["eq?", "@type", "admin"]
```

### Complex Conditions
```python
# Find all objects where "active" is true AND "score" is greater than 90
["and",
  ["eq?", "@active", true],
  ["gt?", "@score", 90]
]

# Find all objects where "type" is "guest" OR "status" is "pending"
["or",
  ["eq?", "@type", "guest"],
  ["eq?", "@status", "pending"]
]
```

### Path System Examples

```python
# Traditional syntax vs @ syntax comparisons:

# Any item in "items" list has status "completed"
["eq?", ["path", [["key", "items"], ["wc_level"], ["key", "status"]]], "completed"]
["eq?", "@items.*.status", "completed"]  # @ syntax

# Deep search for any "error" field that exists
["exists?", ["path", [["wc_recursive"], ["key", "error"]]]]
["exists?", "@**.error"]  # @ syntax (if recursive wildcard syntax supported)

# Get names of users at specific indices 0 and 2
["path", [["key", "users"], ["indices", [0, 2]], ["key", "name"]]]
"@users[0,2].name"  # @ syntax

# Check if any log entry with a key matching "event_.*" has a "level" of "critical"
["eq?", ["path", [["key", "logs"], ["regex_key", "event_.*"], ["key", "level"]]], "critical"]
```

### Advanced @ Syntax Examples

```python
# Complex nested access:
["eq?", "@user.profile.settings.theme", "dark"]

# Array indexing:
["gt?", "@scores[0]", 85]
["eq?", "@data[-1].status", "final"]

# Wildcard operations:
["exists?", "@projects.*.deadline"]
["in?", "urgent", "@tasks.*.priority"]

# Existence checks:
["exists?", "@user.preferences.notifications"]
["not", ["exists?", "@temp_data"]]

# In complex boolean expressions:
["or",
  ["and", ["eq?", "@status", "active"], ["gt?", "@score", 90]],
  ["and", ["eq?", "@status", "trial"], ["gt?", "@score", 95]]
]
```

## Design Constraints

1. **No Turing Completeness**: No loops, recursion (in query language itself), or unbounded computation.
2. **Filtering Focus**: Designed specifically for boolean filtering operations.
3. **Tagged AST Paths**: Uniform, explicit path component representation.
4. **Predictable Performance**: All operations have bounded execution time relative to data size and path complexity.
5. **Boolean Results for Filtering**: Top-level queries (or conditions in `if`, `and`, `or`) must resolve to boolean values for filtering.
6. **Syntax Coexistence**: The `@` notation coexists with traditional `["path", ...]` syntax, maintaining full backward compatibility.

This specification defines a minimal, focused JSON filtering language that's powerful enough for real-world use cases while remaining simple and predictable. The path system, with its tagged AST and concise `@` notation, enhances both explicitness and readability. The `JafResultSet` provides a robust mechanism for working with and combining filter

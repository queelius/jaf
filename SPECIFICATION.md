# JAF (JSON Array Filter) - Specification v1.1

## Overview

JAF is a simple, focused domain-specific language for filtering JSON arrays. It's designed to be **not Turing-complete** and focuses solely on filtering with predictable boolean results. It also supports boolean algebraic operations on sets of filter results.

## Core Philosophy

- **Simple**: Easy to understand and debug
- **Predictable**: Every query returns boolean results for filtering
- **Secure**: No arbitrary code execution or side effects
- **Focused**: Designed specifically for JSON array filtering and result set manipulation

## Data Model

**Input to `jaf` function**: `Array<Object>` - An array of JSON objects.
**Output of `jaf` function**: `JafResultSet` - An object representing the filter results.

## `JafResultSet` Object

A `JafResultSet` represents the set of indices from a data collection that satisfy a JAF query, along with metadata to ensure logical consistency when combining results or resolving them back to original data.

**Attributes / JSON Structure:**

When serialized to JSON (e.g., by the CLI), a `JafResultSet` has the following structure:

-   `indices`: `Array<Number>` (integer)
    -   A sorted list of unique, 0-based indices of the objects from the original data array that matched the query.
-   `collection_size`: `Number` (integer)
    -   The total number of items in the original data collection from which the indices were derived. This is crucial for operations like `NOT`.
-   `collection_id`: `Any` (string, number, null, etc.)
    -   An optional identifier for the original data collection. This helps ensure that boolean operations are performed between result sets derived from the same logical collection. It can be a file path, directory path, or a user-defined ID.
-   `filenames_in_collection`: `Array<String>` (optional)
    -   A sorted list of unique, absolute file paths that contributed data to form the collection. This is primarily populated when the input to `jaf filter` is a directory. It's used by `jaf resolve` (and `JafResultSet.get_matching_objects()`) to locate the original data.

**Example JSON Output (compact):**
```json
{"indices":[0,2],"collection_size":3,"collection_id":"/path/to/data.json","filenames_in_collection":["/path/to/data.json"]}
```
If `filenames_in_collection` is not present or `null`, it's omitted from the JSON.

## Query Format

Queries use S-expression syntax: `[operator, arg1, arg2, ...]`

**Examples**:

```python
["eq?", ["path", [["key", "name"]]], "John"]
["and", 
  ["exists?", ["path", [["key", "email"]]]], 
  ["gt?", ["length", ["path", [["key", "items"]]]], 5]
]
```

## Path System

The JAF Path System is a small, dedicated sub-language for data traversal within JSON objects. Paths are **lists of tagged components** used within the `["path", path_components_list]` special form. This tagged structure (its own AST) provides a uniform and explicit way to define how to traverse the JSON data.

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

6.  `["wc_level"]` (Wildcard - Current Level)
    *   Matches any single field name or array index at the current level of the object or array.
    *   **Motivation**: Useful for iterating over elements of a list or values of a dictionary when the keys/indices are not known beforehand, or when an operation needs to be applied to all direct children.
    *   Example: `[["key", "tasks"], ["wc_level"], ["key", "status"]]` accesses the "status" of each task in the "tasks" list.

7.  `["wc_recursive"]` (Wildcard - Recursive Descent)
    *   Matches any field name recursively at any depth within the current object structure. It also considers the current level for matches if the subsequent path parts align.
    *   **Motivation**: Essential for deep searches where the target data might be nested at varying or unknown depths.
    *   Example: `[["wc_recursive"], ["key", "error_code"]]` finds any "error_code" field anywhere in the object.
8.  `["root"]`
    *   Represents the root of the object against which the entire path expression is being evaluated.
    *   Allows paths to "restart" or reference from the top-level object, even if the current evaluation context is nested due to prior path components.
    *   Example: `[["key", "user"], ["root"], ["key", "config"]]` - if `obj` is `{"user": {"name": "A"}, "config": {"setting": "X"}}`, this path would first go to `obj["user"]`, then `["root"]` would reset the context to `obj`, and `["key", "config"]` would access `obj["config"]`.

**Path Evaluation (`eval_path` function):**

The `eval_path(obj, path_components_list)` function (internally used by the `["path", ...]` special form) evaluates the given path against the `obj`.

- **Return Value**:
    - If the path does not contain multi-match components (i.e., only uses `["key", ...]` or `["index", ...]`) and successfully resolves to a single, definite value, that value is returned directly. This includes `None` if the field exists and its value is `null`.
    - If the path involves components that can naturally yield multiple values (e.g., `["indices", ...]`, `["slice", ...]`, `["regex_key", ...]`, `["wc_level"]`, `["wc_recursive"]`), `eval_path` returns a `PathValues` object. `PathValues` is a specialized list subclass that holds the collection of all values found by the path. It preserves the order of discovery and can contain duplicates if the data and path logic lead to them. It offers convenience methods for accessing its contents (e.g., `first()`, `one()`).
    - If a path that does *not* contain multi-match components fails to resolve at any point (e.g., a key not found, an index out of bounds for a specific index access), `eval_path` returns an empty list `[]`. This signifies "not found" or "no value" for a specific path.
    - If a path *with* multi-match components finds no values, it returns an empty `PathValues` object (e.g., `PathValues([])`). This is distinct from the `[]` returned for a specific path not found.
    - If the `path_components_list` is empty (e.g., `["path", []]`), `eval_path` returns the original `obj`.
    - In rare cases where a path *without* multi-match components unexpectedly yields multiple distinct results, `eval_path` may also wrap these results in a `PathValues` object with a warning.

**Examples of Path Syntax:**

```python
# Access a top-level key:
["path", [["key", "name"]]]

# Access a nested key:
["path", [["key", "user"], ["key", "email"]]]

# Access all "status" fields from items in a list:
["path", [["key", "items"], ["wc_level"], ["key", "status"]]]

# Access an element by index:
["path", [["key", "data"], ["index", 0], ["key", "value"]]]

# Access elements via slice:
["path", [["key", "measurements"], ["slice", 10, 20, null]]]

# Access elements via specific indices:
["path", [["key", "users"], ["indices", [1, 3, 5]]]]

# Access elements via regex on keys:
["path", [["key", "logs"], ["regex_key", "session_\\w+"]]]
```

**Behavior of the `["path", ...]` special form:**

- When the `["path", ...]` form is evaluated, it internally calls `eval_path` with the current object and the provided `path_components_list`.
- The result of `eval_path` (a single value, `None`, `[]`, or a `PathValues` list) is then used as the value of the `["path", ...]` expression in the broader JAF query.

## Operator Categories

### 1. Special Forms (Custom Evaluation)

- `path` - Extract values: `["path", [["key", "field"], ["key", "subfield"]]]`
- `exists?` - Check existence: `["exists?", ["path", [["key", "field"]]]]`
  - `exists?` returns `true` if `eval_path` for the given path components returns anything other than an empty list `[]` (when the path is specific and not found) or an empty `PathValues` (when the path is multi-match and not found). A path to a `null` value *does* exist and `exists?` will return `true`.
- `if` - Conditional: `["if", condition, true-expr, false-expr]`
- `and` - Logical AND with short-circuit: `["and", expr1, expr2, ...]`
- `or` - Logical OR with short-circuit: `["or", expr1, expr2, ...]`
- `not` - Logical negation: `["not", expr]`

### 2. Predicates (Return Boolean)

```python
# Comparison
"eq?", "neq?", "gt?", "gte?", "lt?", "lte?"

# Containment  
"in?"

# String matching
"starts-with?", "ends-with?", "regex-match?", "close-match?", "partial-match?"
```

### 3. Value Extractors (Support Predicates)

```python
# Data access
"length", "type", "keys"

# String transformation  
"lower-case", "upper-case"

# Date/time
"now", "date", "datetime", "date-diff", "days", "seconds"
```

## Function Signatures

All functions follow this pattern:

```python
[function-name, arg1, arg2, ...]
```

**Examples**:

```python
["eq?", ["path", [["key", "name"]]], "John"]                    # name == "John"
["gt?", ["length", ["path", [["key", "items"]]]], 5]            # len(items) > 5  
["starts-with?", ["lower-case", ["path", [["key", "email"]]]], "admin"]  # email.lower().startswith("admin")
```

## Evaluation Rules

### 1. Literals

- Strings, numbers, booleans, null are returned as-is.
- `"hello"` → `"hello"`
- `42` → `42`
- `null` → `None` (in Python representation)

### 2. Special Forms

- Evaluated with custom logic (don't evaluate all args first).
- Handle control flow and path access.

### 3. Regular Functions (Predicates and Value Extractors)

- All arguments evaluated first, then function called.
- Predictable evaluation order.

### 4. `PathValues` in Predicates and Functions (Interaction with `adapt_jaf_operator`)

When a `PathValues` object (the result of a `["path", ...]` expression involving components like `wc_level`, `wc_recursive`, `indices`, `slice`, or `regex_key`) is used as an argument to a predicate or a value-transforming function, JAF (via the `adapt_jaf_operator` utility) employs a specific evaluation strategy. `PathValues` represents the collection of all values found by such a path.

**a. Argument Expansion (Cartesian Product):**

- If one or more arguments to a function are `PathValues` lists, the system generates all possible combinations of individual values from these lists. This is equivalent to a Cartesian product.
- Arguments that are not `PathValues` lists (i.e., single values) are treated as single-element lists for this product generation.
- The underlying function (predicate or transformer) is then invoked for each unique combination of arguments derived from this expansion.

**b. Predicate Evaluation (Existential Quantification - ∃):**

- If the function being called is a **predicate** (typically its name ends with `?`):
  - The predicate evaluates to `true` if **there exists at least one combination** of expanded arguments for which the predicate's condition holds.
  - If all combinations evaluate to `false`, or if any `PathValues` argument was initially empty (resulting in no combinations to test), the overall predicate evaluates to `false`.
  - **Example**: `["eq?", ["path", [["key", "items"], ["wc_level"], ["key", "status"]]], "completed"]`.
    Let `S = eval_path(obj, [["key", "items"], ["wc_level"], ["key", "status"]]])`. The predicate is true if ∃ *s* ∈ `S` such that `eq?(s, "completed")` is true.
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

- The `path` operator itself is designed for *data extraction* – it gathers all values that match a given path. It does not inherently perform universal quantification ("for all").
- Universal quantification is a *checking* operation. While not directly supported by `path`, such checks can often be constructed using negation and existential quantification (e.g., "it is NOT true that there EXISTS an item that does NOT satisfy the condition"). For example, to check if all items in a list have `status == "active"`: `["not", ["in?", false, ["map", ["lambda", "x", ["eq?", ["path", [["key", "x"], ["key", "status"]]], "active"]], ["path", [["key", "items"]]]]]]` (assuming a hypothetical `map` and `lambda` for illustration; JAF does not have these directly but similar logic can be built with `and`/`or` over known items or by checking for the non-existence of a counter-example). A simpler approach for "all items satisfy X" is often "NOT (EXISTS item that does NOT satisfy X)".

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
    *   The resulting `filenames_in_collection` is `self.filenames_in_collection` if not None, else `other.filenames_in_collection`.

2.  `OR(self, other: 'JafResultSet') -> 'JafResultSet'` (or `self | other`)
    *   Performs a set union of `self.indices` and `other.indices`.
    *   Requires compatibility.
    *   Metadata propagation for `collection_id` and `filenames_in_collection` is the same as `AND`.

3.  `NOT(self) -> 'JafResultSet'` (or `~self`)
    *   Calculates the complement relative to `self.collection_size`.
    *   `new_indices = {0, ..., self.collection_size - 1} - self.indices`.
    *   Preserves `self.collection_id` and `self.filenames_in_collection`.

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
-   Differences in `filenames_in_collection` between two compatible result sets do not raise an error but may result in the output `JafResultSet` inheriting this attribute from one of the operands (typically `self`).

## Resolving Result Sets to Original Data

The `JafResultSet` class provides a method to retrieve the original data objects corresponding to its `indices`.

**`get_matching_objects(self) -> List[Any]`:**
-   Attempts to load the original data objects.
-   **Data Source Determination**:
    1.  If `self.filenames_in_collection` (a list of file paths) is present, these files are loaded in their sorted order to reconstruct the original collection.
    2.  Else, if `self.collection_id` is a string representing a path to a single existing file, that file is loaded.
    3.  If neither of the above provides a loadable source, a `JafResultSetError` is raised.
-   **Validation**: After loading, it verifies that the total number of loaded objects matches `self.collection_size`. If not, a `JafResultSetError` is raised.
-   **Return**: A list containing the original data objects at the indices specified in `self.indices`. The objects are returned in the order of the sorted indices.
-   **Errors**: Raises `JafResultSetError` for issues like unresolvable data sources, file not found, or data inconsistencies.

## Error Handling

### Path Errors

- Non-existent specific paths (key not found, index out of bounds for specific index access) result in `eval_path` returning `[]` (empty list).
- A multi-match path that finds no values results in `eval_path` returning an empty `PathValues` object.
- A path to a field that exists but has a `null` value will result in `eval_path` returning `None` (if it's a specific path resolving to that `null`).
- Use `exists?` to check path existence. `exists?` returns `true` if `eval_path` for the given path components returns anything other than an empty list `[]` (when the path is specific and not found) or an empty `PathValues` (when the path is multi-match and not found). A path to a `null` value *does* exist and `exists?` will return `true`.

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
# Find objects where name is "John"
["eq?", ["path", [["key", "name"]]], "John"]

# Find objects with more than 5 items
["gt?", ["length", ["path", [["key", "items"]]]], 5]
```

### Complex Conditions

```python
# Active users with email addresses
["and", 
  ["eq?", ["path", [["key", "active"]]], true],
  ["exists?", ["path", [["key", "email"]]]]
]

# Case-insensitive language check
["eq?", ["lower-case", ["path", [["key", "language"]]]], "python"]
```

### Path System Examples

```python
# Any item in "items" list has status "completed"  
["eq?", ["path", [["key", "items"], ["wc_level"], ["key", "status"]]], "completed"]

# Deep search for any "error" field that exists
["exists?", ["path", [["wc_recursive"], ["key", "error"]]]]

# Get names of users at specific indices 0 and 2
# (This path would likely be used with a function that can handle a list of names,
# or in a context where multiple names are expected)
["path", [["key", "users"], ["indices", [0, 2]], ["key", "name"]]] 

# Check if any log entry with a key matching "event_.*" has a "level" of "critical"
["eq?", ["path", [["regex_key", "event_.*"], ["key", "level"]]], "critical"]

# Check if the first three tags include "urgent"
["in?", "urgent", ["path", [["key", "tags"], ["slice", null, 3, null]]]]
```

## Design Constraints

1. **No Turing Completeness**: No loops, recursion (in query language itself), or unbounded computation.
2. **Filtering Focus**: Designed specifically for boolean filtering operations.
3. **Tagged AST Paths**: Uniform, explicit path component representation.
4. **Predictable Performance**: All operations have bounded execution time relative to data size and path complexity.
5. **Boolean Results for Filtering**: Top-level queries (or conditions in `if`, `and`, `or`) must resolve to boolean values for filtering.

This specification defines a minimal, focused JSON filtering language that's powerful enough for real-world use cases while remaining simple and predictable. The path system, with its tagged AST, enhances its explicitness and maintainability. The `JafResultSet` provides a robust mechanism for working with and combining filter results.

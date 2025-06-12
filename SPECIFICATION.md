# JAF (JSON Array Filter) - Specification v1.0

## Overview

JAF is a simple, focused domain-specific language for filtering JSON arrays. It's designed to be **not Turing-complete** and focuses solely on filtering with predictable boolean results.

## Core Philosophy

- **Simple**: Easy to understand and debug
- **Predictable**: Every query returns boolean results for filtering
- **Secure**: No arbitrary code execution or side effects
- **Focused**: Designed specifically for JSON array filtering

## Data Model

**Input**: `Array<Object>` - An array of JSON objects
**Output**: `Array<Number>` - Indices of objects that matched the filter (returned `true`)

## Query Format

Queries use S-expression syntax: `[operator, arg1, arg2, ...]`

**Examples**:
```python
["eq?", ["path", ["name"]], "John"]
["and", ["exists?", ["path", ["email"]]], ["gt?", ["length", ["path", ["items"]]], 5]]
```

## Path System

Paths are **lists of components** (not strings) used within the `["path", path_components_list]` special form.

```python
["path", ["name"]]                    # obj["name"]  
["path", ["user", "email"]]           # obj["user"]["email"]
["path", ["items", "*", "name"]]      # obj["items"][*]["name"] (wildcard)
["path", ["data", 0, "value"]]        # obj["data"][0]["value"] (array index)
```

### Wildcards
- `"*"` (Star): Matches any single field name or array index at the current level of the object or array.
    - **Motivation**: Useful for iterating over elements of a list or values of a dictionary when the keys/indices are not known beforehand, or when an operation needs to be applied to all direct children. Examples: checking the status of all tasks in a `tasks` list (`["tasks", "*", "status"]`), extracting all phone numbers from a `contacts` object where each key is a contact name (`["contacts", "*", "phoneNumber"]`).
- `"**"` (Double Star / Recursive Descent): Matches any field name recursively at any depth within the current object structure. It also considers the current level for matches if the subsequent path parts align.
    - **Motivation**: Essential for deep searches where the target data might be nested at varying or unknown depths. Examples: finding any occurrence of an `"error_code"` field anywhere in a complex log object (`["**", "error_code"]`), locating all `"commentText"` fields regardless of their nesting level within a document structure (`["**", "commentText"]`).

**Behavior of the `["path", ...]` special form:**
- When the `["path", ...]` form is evaluated, it produces a `WildcardResultsList`.
- A `WildcardResultsList` is a list of values that result from following the path components in the context of each item in the input array.
- If a path component is a wildcard (`*` or `**`), it alters how the `WildcardResultsList` is constructed:
    - `*` matches any single field or array index at the current level.
    - `**` matches any field or array index at any depth, including the current level if applicable.
- The resulting `WildcardResultsList` can then be used as input to predicates or value-transforming functions.

## Operator Categories

### 1. Special Forms (Custom Evaluation)
- `path` - Extract values: `["path", ["field", "subfield"]]`
- `exists?` - Check existence: `["exists?", ["path", ["field"]]]`
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
["eq?", ["path", ["name"]], "John"]                    # name == "John"
["gt?", ["length", ["path", ["items"]]], 5]            # len(items) > 5  
["starts-with?", ["lower-case", ["path", ["email"]]], "admin"]  # email.lower().startswith("admin")
```

## Evaluation Rules

### 1. Literals
- Strings, numbers, booleans, null are returned as-is
- `"hello"` → `"hello"`
- `42` → `42`

### 2. Special Forms
- Evaluated with custom logic (don't evaluate all args first)
- Handle control flow and path access

### 3. Regular Functions (Predicates and Value Extractors)
- All arguments evaluated first, then function called
- Predictable evaluation order

### 4. Wildcards in Predicates and Functions (Theory of Wildcard Path Evaluation)

When a `WildcardResultsList` (the result of a `["path", ...]` expression involving `*` or `**`) is used as an argument to a predicate or a value-transforming function, JAF employs a specific evaluation strategy based on existential quantification for predicates.

**a. Argument Expansion (Cartesian Product):**
- If one or more arguments to a function are `WildcardResultsList`s, the system generates all possible combinations of individual values from these lists. This is equivalent to a Cartesian product (e.g., via `itertools.product`).
- Arguments that are not `WildcardResultsList`s are treated as single-element lists for the purpose of this product generation.
- The underlying function (predicate or transformer) is then invoked for each unique combination of arguments derived from this expansion.

**b. Predicate Evaluation (Existential Quantification - ∃):**
- If the function being called is a **predicate** (typically its name ends with `?`):
    - The predicate evaluates to `true` if **there exists at least one combination** of expanded arguments for which the predicate's condition holds.
    - If all combinations evaluate to `false`, or if any `WildcardResultsList` argument was initially empty (resulting in no combinations to test), the overall predicate evaluates to `false`.
    - **Example**: `["eq?", ["path", ["items", "*", "status"]], "completed"]`.
        Let `S = path_values(["items", "*", "status"], obj)`. The predicate is true if ∃ *s* ∈ `S` such that `eq?(s, "completed")` is true.
    - Type errors or attribute errors encountered during the evaluation of a specific combination for a predicate cause that particular combination to yield `false`. The overall predicate can still be `true` if another combination succeeds.

**c. Value Extractor/Transformer Evaluation:**
- If the function is a **value extractor or transformer**:
    - The function is called for each combination of arguments generated as per (4a).
    - A list containing the results from all these individual function calls is collected.
    - **Result Aggregation**:
        - If this list of results contains exactly one item, that single item is returned.
        - If this list contains one item which is itself a list (e.g., `[[1,2,3]]` where the inner list was the actual return value of the function for the single combination), this inner list is "unwrapped" and returned (e.g., `[1,2,3]`). This flattening is primarily relevant when the original function was expected to return a list, and no significant wildcard expansion occurred that would naturally produce multiple distinct results.
        - Otherwise (multiple results from multiple combinations, or an empty list if no combinations were processed due to an empty `WildcardResultsList`), the collected list of results is returned as is.
        - If any `WildcardResultsList` argument was empty, leading to no combinations, an empty list `[]` is typically returned by the wrapper for the value extractor.

**d. Universal Quantification (`∀`) and `path`:**
- The `path` operator itself is designed for *data extraction* – it gathers all values that match a given path. It does not inherently perform universal quantification ("for all").
- Universal quantification is a *checking* operation. While not directly supported by `path`, such checks can often be constructed using negation and existential quantification (e.g., "it is NOT true that there EXISTS an item that does NOT satisfy the condition").
- A dedicated higher-order operator like `["all?", collection_expr, predicate_expr]` could provide direct universal quantification, but this is beyond the current scope of JAF's core `path` and predicate interaction, which focuses on the more common filtering need of existential matches.

## Error Handling

### Path Errors
- Non-existent paths return `null`
- Use `exists?` to check path existence

### Type Errors
- Type mismatches return `false` (don't raise errors)
- Invalid arguments raise `ValueError`

## Example Queries

### Basic Filtering
```python
# Find objects where name is "John"
["eq?", ["path", ["name"]], "John"]

# Find objects with more than 5 items
["gt?", ["length", ["path", ["items"]]], 5]
```

### Complex Conditions
```python
# Active users with email addresses
["and", 
  ["eq?", ["path", ["active"]], true],
  ["exists?", ["path", ["email"]]]]

# Case-insensitive language check
["eq?", ["lower-case", ["path", ["language"]]], "python"]
```

### Wildcard Usage
```python
# Any item has status "completed"  
["eq?", ["path", ["items", "*", "status"]], "completed"]

# Deep search for any "error" field
["exists?", ["path", ["**", "error"]]]
```

## Design Constraints

1. **No Turing Completeness**: No loops, recursion, or unbounded computation
2. **Filtering Focus**: Designed specifically for boolean filtering operations
3. **List-based Paths**: No string parsing within the evaluator
4. **Predictable Performance**: All operations have bounded execution time
5. **Boolean Results Only**: All queries must return boolean values for filtering

This specification defines a minimal, focused JSON filtering language that's powerful enough for real-world use cases while remaining simple and predictable.

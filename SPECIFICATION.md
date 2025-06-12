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

Paths are **lists of components** (not strings):

```python
["path", ["name"]]                    # obj["name"]  
["path", ["user", "email"]]           # obj["user"]["email"]
["path", ["items", "*", "name"]]      # obj["items"][*]["name"] (wildcard)
["path", ["data", 0, "value"]]        # obj["data"][0]["value"] (array index)
```

### Wildcards
- `"*"`: Matches any single field/index
- `"**"`: Matches any field at any depth (recursive)

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

### 3. Regular Functions  
- All arguments evaluated first, then function called
- Predictable evaluation order

### 4. Wildcards
- If wildcard path matches multiple values, predicate succeeds if ANY match satisfies condition
- Uses existential quantification (∃)

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

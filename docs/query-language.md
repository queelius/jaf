# JAF Query Language Reference

JAF uses an S-expression based query language for filtering and transforming JSON data. This reference covers all available operators and syntax.

## Query Syntax

Queries are represented as JSON arrays with the operator as the first element:

```python
[operator, arg1, arg2, ...]
```

## Path Navigation with @

The `@` prefix provides concise path navigation:

### Basic Path Access

```python
# Simple field access
"@name"                          # obj["name"]
"@user.email"                    # obj["user"]["email"]
"@address.street.number"         # obj["address"]["street"]["number"]

# Array indexing
"@items.0"                       # obj["items"][0]
"@data.-1"                       # obj["data"][-1] (last element)
```

### Wildcards

```python
# Single-level wildcard
"@users.*.name"                  # All names from users array
"@data.*.status"                 # All status fields

# Recursive wildcard
"@**.error"                      # Find 'error' at any depth
"@**.id"                         # All 'id' fields recursively
```

### Advanced Path Features

```python
# Slice notation
"@items[0:5]"                    # First 5 items
"@data[::2]"                     # Every other item
"@records[-10:]"                 # Last 10 items

# Multiple indices
"@values[0,2,4]"                 # Specific indices

# Regex matching on keys
"@~user_.*"                      # Keys matching pattern
"@data.~.*_id"                   # Fields ending with '_id'
```

## Predicate Operators (Return Boolean)

### Comparison Operators

```python
# Equality
["eq?", "@status", "active"]            # status == "active"
["neq?", "@type", "test"]               # type != "test"

# Numeric comparison
["gt?", "@age", 18]                     # age > 18
["gte?", "@score", 90]                  # score >= 90
["lt?", "@price", 100]                  # price < 100
["lte?", "@count", 5]                   # count <= 5
```

### Membership and Containment

```python
# Check if value is in array/string
["in?", "admin", "@roles"]              # "admin" in roles
["in?", "@status", ["active", "pending"]]  # status in ["active", "pending"]

# Check if array/string contains value
["contains?", "@tags", "urgent"]        # tags contains "urgent"
["contains?", "@message", "error"]      # message contains "error"
```

### String Predicates

```python
# String matching
["starts-with?", "@email", "admin@"]    # email starts with "admin@"
["ends-with?", "@file", ".json"]        # file ends with ".json"

# Regular expression matching
["regex-match?", "@phone", "^\\d{3}-\\d{3}-\\d{4}$"]  # Phone format

# Fuzzy matching
["fuzzy-match?", "@name", "John", 0.8]  # Fuzzy match with threshold
["close-match?", "@title", "Manager", 3] # Edit distance <= 3
```

### Type Checking

```python
# Check JSON types
["is-string?", "@value"]                # Is string
["is-number?", "@count"]                # Is number (int or float)
["is-int?", "@id"]                      # Is integer
["is-float?", "@price"]                 # Is float
["is-bool?", "@active"]                 # Is boolean
["is-null?", "@deleted_at"]             # Is null
["is-array?", "@items"]                 # Is array
["is-object?", "@metadata"]             # Is object

# Special checks
["is-empty?", "@results"]               # Empty array/object/string
["exists?", "@optional_field"]          # Field exists (even if null)
```

## Logical Operators

```python
# AND - all conditions must be true
["and",
  ["gt?", "@age", 18],
  ["eq?", "@verified", true],
  ["contains?", "@roles", "user"]
]

# OR - at least one condition must be true
["or",
  ["eq?", "@status", "active"],
  ["eq?", "@status", "pending"]
]

# NOT - negation
["not", ["is-empty?", "@items"]]       # items is not empty

# Complex nesting
["and",
  ["or",
    ["eq?", "@type", "premium"],
    ["gt?", "@purchases", 10]
  ],
  ["not", ["eq?", "@status", "suspended"]]
]
```

## Value Extractors and Transformers

### Basic Extractors

```python
# Get metadata
["length", "@items"]                    # Length of array/string
["type", "@value"]                      # Type as string ("string", "number", etc.)
["keys", "@data"]                       # Object keys as array
["values", "@data"]                     # Object values as array

# Array operations
["first", "@items"]                     # First element
["last", "@items"]                      # Last element
["unique", "@tags"]                     # Remove duplicates
```

### String Operations

```python
# Case conversion
["lower-case", "@name"]                 # Convert to lowercase
["upper-case", "@code"]                 # Convert to uppercase

# String manipulation
["trim", "@input"]                      # Remove whitespace
["split", "@csv", ","]                  # Split string
["join", "@parts", "-"]                 # Join array to string
["concat", "@first", " ", "@last"]      # Concatenate strings
["replace", "@text", "old", "new"]      # Replace in string
```

### Type Conversion

```python
# Convert between types
["to-string", "@number"]                # Convert to string
["to-number", "@string_id"]             # Convert to number
["to-int", "@float_value"]              # Convert to integer
["to-float", "@int_value"]              # Convert to float
["to-bool", "@flag"]                    # Convert to boolean
["to-list", "@single_value"]            # Wrap in array
```

### Mathematical Operations

```python
# Basic arithmetic (variadic)
["+", "@price", "@tax", "@shipping"]    # Addition
["-", "@total", "@discount"]            # Subtraction
["*", "@quantity", "@unit_price"]       # Multiplication
["/", "@total", "@count"]               # Division
["%", "@value", 10]                     # Modulo

# Math functions
["abs", "@difference"]                  # Absolute value
["round", "@price", 2]                  # Round to decimals
["floor", "@value"]                     # Round down
["ceil", "@value"]                      # Round up
["min", "@values"]                      # Minimum (array or variadic)
["max", "@values"]                      # Maximum (array or variadic)
```

### Date/Time Operations

```python
# Current date/time
["now"]                                 # Current timestamp
["today"]                               # Today's date

# Date parsing and formatting
["date", "@timestamp"]                  # Extract date part
["datetime", "@date_string"]            # Parse datetime

# Date arithmetic
["date-diff", "@end_date", "@start_date"]  # Difference between dates
["days", <date-diff-result>]           # Convert to days
["seconds", <date-diff-result>]         # Convert to seconds
```

## Special Forms

### Conditional Expression

```python
# If-then-else
["if", 
  ["gt?", "@score", 90],               # Condition
  "excellent",                          # Then value
  "needs improvement"                   # Else value
]

# Nested conditionals
["if",
  ["gt?", "@age", 65],
  "senior",
  ["if",
    ["gt?", "@age", 18],
    "adult",
    "minor"
  ]
]
```

### Dictionary Construction

```python
# Create new object
["dict",
  "id", "@user_id",
  "name", ["upper-case", "@username"],
  "active", ["gt?", "@last_login", "2024-01-01"]
]

# Conditional fields
["dict",
  "type", "@product_type",
  "discount", ["if", ["eq?", "@member", true], 0.1, 0]
]
```

### Self Reference

```python
# Return the current object
["self"]                               # Returns the entire object being evaluated
```

## Complex Query Examples

### User Validation

```python
# Active premium users with recent activity
["and",
  ["eq?", "@status", "active"],
  ["eq?", "@subscription.type", "premium"],
  ["gt?", "@last_login", "2024-01-01"],
  ["or",
    ["gte?", "@posts_count", 10],
    ["contains?", "@badges", "contributor"]
  ]
]
```

### Data Transformation

```python
# Transform user data for export
["dict",
  "user_id", "@id",
  "display_name", ["concat", "@first_name", " ", "@last_name"],
  "account_age_days", ["days", ["date-diff", ["now"], "@created_at"]],
  "status", ["if",
    ["gt?", "@last_login", "2024-06-01"],
    "active",
    "inactive"
  ],
  "tags", ["unique", ["concat", "@interests", "@skills"]]
]
```

### Error Detection

```python
# Find logs with errors or warnings in specific services
["and",
  ["or",
    ["eq?", "@level", "ERROR"],
    ["and",
      ["eq?", "@level", "WARN"],
      ["contains?", "@message", "critical"]
    ]
  ],
  ["in?", "@service", ["api", "payment", "auth"]],
  ["gt?", "@timestamp", "2024-01-01T00:00:00Z"]
]
```

## Best Practices

1. **Use @ notation**: It's more concise than explicit path arrays
2. **Filter early**: Put selective filters first to reduce processing
3. **Avoid deep nesting**: Break complex queries into smaller parts
4. **Use appropriate operators**: `in?` for membership, `contains?` for containment
5. **Type check when needed**: Use `is-*?` predicates for mixed data

## Next Steps

- See the [Cookbook](cookbook.md) for practical query examples
- Learn about [Boolean Operations](api-guide.md#boolean-operations) for combining filters
- Explore the [API Guide](api-guide.md) for using queries in code
# Getting Started with JAF

This guide will walk you through installing JAF and using its core features.

## Installation

### From PyPI

```bash
pip install jaf
```

### From Source

```bash
git clone https://github.com/queelius/jaf.git
cd jaf
pip install -e .
```

### Development Installation

```bash
git clone https://github.com/queelius/jaf.git
cd jaf
make install-dev
```

## Your First JAF Pipeline

Let's start with a simple example. Suppose you have a file `users.jsonl` with user data:

```json
{"name": "Alice", "age": 30, "role": "admin", "active": true}
{"name": "Bob", "age": 25, "role": "user", "active": true}
{"name": "Charlie", "age": 35, "role": "user", "active": false}
{"name": "Diana", "age": 28, "role": "admin", "active": true}
```

### Using the CLI

#### Basic Filtering

```bash
# Find all active users (outputs a stream descriptor)
jaf filter users.jsonl '["eq?", "@active", true]'

# To see the actual data, use --eval
jaf filter users.jsonl '["eq?", "@active", true]' --eval
```

#### Chaining Operations

```bash
# Find active admins and get their names
jaf filter users.jsonl '["and", ["eq?", "@active", true], ["eq?", "@role", "admin"]]' | \
jaf map - "@name" | \
jaf eval -
```

#### Using the Stream Command

The `stream` command evaluates by default:

```bash
# Same as above but in one command
jaf stream users.jsonl \
  --filter '["and", ["eq?", "@active", true], ["eq?", "@role", "admin"]]' \
  --map "@name"
```

### Using the Python API

```python
from jaf import stream

# Load the data
users = stream("users.jsonl")

# Find active admins
active_admins = users.filter(["and",
    ["eq?", "@active", True],
    ["eq?", "@role", "admin"]
])

# Get their names
admin_names = active_admins.map("@name")

# Execute the pipeline
for name in admin_names.evaluate():
    print(f"Admin: {name}")
```

## Understanding Lazy Evaluation

JAF uses lazy evaluation, meaning operations don't execute until you explicitly request results:

```python
# This creates a pipeline but doesn't read any data
pipeline = stream("huge_file.jsonl") \
    .filter(["gt?", "@score", 90]) \
    .map(["dict", "id", "@id", "score", "@score"]) \
    .take(10)

# Data is only read when we evaluate
results = list(pipeline.evaluate())  # Reads just enough to get 10 matches
```

## Working with Different Data Sources

### Files

```python
# JSON array file
s1 = stream("data.json")

# JSONL (newline-delimited JSON)
s2 = stream("data.jsonl")

# Gzipped files
s3 = stream("data.jsonl.gz")
```

### Directories

```python
# Process all JSON/JSONL files in a directory
s = stream({
    "type": "directory",
    "path": "/path/to/data",
    "recursive": True  # Include subdirectories
})
```

### In-Memory Data

```python
data = [
    {"id": 1, "value": 100},
    {"id": 2, "value": 200}
]

s = stream({"type": "memory", "data": data})
```

### Standard Input

```bash
# From command line
echo '[{"x": 1}, {"x": 2}]' | jaf filter - '["gt?", "@x", 1]' --eval

# In Python
s = stream({"type": "stdin"})
```

## Basic Query Patterns

### Simple Comparisons

```python
# Equality
["eq?", "@status", "active"]

# Greater than
["gt?", "@age", 25]

# Contains (for arrays/strings)
["contains?", "@tags", "python"]

# Exists
["exists?", "@email"]
```

### Boolean Logic

```python
# AND - all conditions must be true
["and",
    ["gt?", "@age", 18],
    ["eq?", "@verified", True]
]

# OR - at least one condition must be true
["or",
    ["eq?", "@role", "admin"],
    ["eq?", "@role", "moderator"]
]

# NOT - negation
["not", ["eq?", "@status", "deleted"]]
```

### Working with Nested Data

```python
# Access nested fields
["eq?", "@address.city", "New York"]

# Check array elements
["contains?", "@skills", "Python"]

# Wildcard access
["any", ["eq?", "@orders.*.status", "pending"]]
```

## Common Operations

### Filtering

```python
# Get users over 25
users = stream("users.jsonl")
adults = users.filter(["gt?", "@age", 25])
```

### Mapping/Transformation

```python
# Extract specific fields
names = users.map("@name")

# Create new structure
summaries = users.map(["dict",
    "name", "@name",
    "age_group", ["if", ["gt?", "@age", 30], "senior", "junior"]
])
```

### Limiting Results

```python
# Take first 10
first_ten = users.take(10)

# Skip first 5, then take 10
paginated = users.skip(5).take(10)

# Take while condition is true
young_users = users.take_while(["lt?", "@age", 30])
```

### Batching

```python
# Process in batches of 100
batches = users.batch(100)

for batch in batches.evaluate():
    # batch is a list of up to 100 items
    process_batch(batch)
```

## Error Handling

JAF distinguishes between two types of errors:

1. **Query Errors**: Invalid queries fail immediately
2. **Item Errors**: Errors processing individual items are logged but don't stop the stream

```python
try:
    # This fails immediately - invalid operator
    result = stream("data.jsonl").filter(["invalid-op", "@x"])
except UnknownOperatorError as e:
    print(f"Query error: {e}")

# Item errors are handled gracefully
pipeline = stream("mixed_data.jsonl").map(["div", "@value", "@divisor"])
for result in pipeline.evaluate():
    # Items with divisor=0 are skipped with a warning
    print(result)
```

## Next Steps

- Learn about [Advanced Filtering](query-language.md) with the full query language
- Master the [Fluent API](api-guide.md) for complex pipelines
- Explore [Boolean Operations](api-guide.md#boolean-operations) for combining filters
- See practical examples in the [Cookbook](cookbook.md)
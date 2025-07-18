# JAF Fluent API Guide

The JAF (Just Another Flow) Python API provides a fluent interface for building data processing pipelines. This guide covers all available operations and patterns.

## Creating Streams

### The `stream()` Function

The `stream()` function is your entry point to the JAF API:

```python
from jaf import stream

# From a file path
s = stream("data.jsonl")

# From a source descriptor
s = stream({
    "type": "file",
    "path": "data.json"
})
```

### Source Types

#### File Sources

```python
# Simple file
s = stream("users.json")

# With explicit type
s = stream({
    "type": "file",
    "path": "users.jsonl"
})

# Gzipped file
s = stream("users.jsonl.gz")
```

#### Directory Sources

```python
# All JSON/JSONL files in directory
s = stream({
    "type": "directory",
    "path": "/data",
    "recursive": True,      # Include subdirectories
    "pattern": "*.json*"    # File pattern (optional)
})
```

Alterntively, you can use the kwargs in the `stream()` function:

```python
s = stream(directory="directory",
           path="/data",
           recursive=True,
           pattern="*.json*")
```

#### Memory Sources

```python
data = [{"id": 1}, {"id": 2}, {"id": 3}]

s = stream({
    "type": "memory",
    "data": data
})
```

#### Standard Input

```python
# Read from stdin
s = stream({"type": "stdin"})
```

#### Infinite/Codata Sources

```python
# Fibonacci sequence
s = stream({
    "type": "fibonacci",
    "include_metadata": True
})

# Prime numbers
s = stream({"type": "primes"})

# Random data
s = stream({
    "type": "prng",
    "seed": 42,
    "template": {
        "id": {"$random": "int", "min": 1, "max": 1000},
        "value": {"$random": "float", "min": 0, "max": 1}
    }
})
```

## Core Stream Operations

All operations return new stream objects, allowing method chaining:

### Filtering

Filter items based on a predicate query:

```python
# Simple filter
active_users = stream("users.jsonl").filter(["eq?", "@active", True])

# Complex filter
premium_active = stream("users.jsonl").filter(["and",
    ["eq?", "@active", True],
    ["eq?", "@plan", "premium"],
    ["gt?", "@created_at", "2023-01-01"]
])
```

### Mapping/Transformation

Transform each item in the stream:

```python
# Extract a single field
names = stream("users.jsonl").map("@name")

# Create new structure
summaries = stream("users.jsonl").map(["dict",
    "id", "@id",
    "display_name", ["upper-case", "@name"],
    "account_age", ["days", ["date-diff", "now", "@created_at"]]
])

# Conditional transformation
tagged = stream("items.jsonl").map(["dict",
    "id", "@id",
    "category", ["if", ["gt?", "@price", 100], "premium", "standard"]
])
```

### Slicing Operations

#### Take and Skip

```python
# First 10 items
first_ten = stream("data.jsonl").take(10)

# Skip first 100, take next 20
page = stream("data.jsonl").skip(100).take(20)
```

#### Take/Skip While

```python
# Take while price is under 100
cheap_items = stream("products.jsonl").take_while(["lt?", "@price", 100])

# Skip header rows
data = stream("file.jsonl").skip_while(["eq?", "@type", "header"])
```

#### Slice

```python
# Python-style slicing: items 10-20
subset = stream("data.jsonl").slice(10, 20)

# Every other item
evens = stream("data.jsonl").slice(0, None, 2)

# Last 10 items (negative indexing)
last_ten = stream("data.jsonl").slice(-10, None)
```

### Batching and Enumeration

#### Batching

```python
# Process in batches of 100
batched = stream("large_file.jsonl").batch(100)

for batch in batched.evaluate():
    # batch is a list of up to 100 items
    bulk_insert(batch)
```

#### Enumeration

```python
# Add index to each item
numbered = stream("items.jsonl").enumerate()

# Start from 100
numbered = stream("items.jsonl").enumerate(start=100)

# Results in: {"index": 0, "value": <original_item>}
```

## Boolean Operations

When working with `FilteredStream` objects, you can combine them using boolean operations:

```python
# Create filtered streams
active = stream("users.jsonl").filter(["eq?", "@active", True])
premium = stream("users.jsonl").filter(["eq?", "@plan", "premium"])
verified = stream("users.jsonl").filter(["eq?", "@verified", True])

# Combine with boolean operations
active_premium = active.AND(premium)  # or active & premium
active_or_premium = active.OR(premium)  # or active | premium
not_active = active.NOT()  # or ~active
active_not_premium = active.DIFFERENCE(premium)  # or active - premium
active_xor_premium = active.XOR(premium)  # or active ^ premium
```

## Advanced Patterns

### Chaining Multiple Operations

```python
# Complex pipeline
result = stream("logs.jsonl") \
    .filter(["eq?", "@level", "ERROR"]) \
    .map(["dict",
        "timestamp", "@timestamp",
        "message", "@message",
        "service", "@service"
    ]) \
    .filter(["in?", "@service", ["api", "web", "worker"]]) \
    .batch(50) \
    .take(1000)
```

### Working with Nested Streams

```python
# Batch returns a stream of lists
batches = stream("data.jsonl").batch(10)

# Process each batch
for batch in batches.evaluate():
    # batch is a list of items
    process_batch(batch)
```

### Chaining Data Sources

```python
# Process multiple files as one stream
source = {
    "type": "chain",
    "sources": [
        {"type": "file", "path": "data1.jsonl"},
        {"type": "file", "path": "data2.jsonl"},
        {"type": "memory", "data": [{"id": 999}]}
    ]
}
combined = stream(source)
```

## Evaluation

Streams are lazy and only execute when evaluated:

```python
# Build pipeline (no execution)
pipeline = stream("huge_file.jsonl") \
    .filter(["contains?", "@tags", "important"]) \
    .map("@message") \
    .take(100)

# Execute and get results
# Option 1: Iterator
for message in pipeline.evaluate():
    print(message)

# Option 2: Collect all results
all_messages = list(pipeline.evaluate())

# Option 3: Get one result
first_message = next(pipeline.evaluate(), None)
```

## Stream Information

Get metadata about a stream without evaluating:

```python
s = stream("data.jsonl").filter(["gt?", "@value", 100]).take(50)

info = s.info()
# {
#     "type": "LazyDataStream",
#     "source_type": "take",
#     "collection_id": None,
#     "pipeline": "take(50) → filter → jsonl → file(data.jsonl)"
# }
```

## Serialization

Streams can be serialized to JSON for storage or transmission:

```python
# Create a pipeline
pipeline = stream("data.jsonl") \
    .filter(["gt?", "@score", 90]) \
    .map("@id") \
    .take(10)

# Serialize to dict
descriptor = pipeline.to_dict()

# Save to file
import json
with open("pipeline.json", "w") as f:
    json.dump(descriptor, f)

# Later, reconstruct from CLI
# jaf eval pipeline.json
```

## Error Handling

```python
from jaf.exceptions import JAFError, UnknownOperatorError, InvalidQueryFormatError

try:
    # Query errors fail immediately
    s = stream("data.jsonl").filter(["invalid-op", "@x"])
except UnknownOperatorError as e:
    print(f"Invalid query: {e}")

# Item errors during evaluation are logged but don't stop the stream
pipeline = stream("mixed.jsonl").map(["div", "@value", "@divisor"])
for result in pipeline.evaluate():
    # Items where divisor=0 are skipped
    print(result)
```

## Performance Considerations

### Streaming vs Memory

JAF is designed for streaming, but be aware that some patterns may require more memory:

```python
# Good - processes one item at a time
stream("huge_file.jsonl").filter(expr).map(transform).evaluate()

# Caution - batching holds batch_size items in memory
stream("huge_file.jsonl").batch(10000).evaluate()
```

### Early Termination

```python
# These operations can terminate early
stream("huge_file.jsonl").take(10)  # Stops after 10 items
stream("huge_file.jsonl").take_while(predicate)  # Stops when false

# Good for finding first match
first_error = next(stream("logs.jsonl")
    .filter(["eq?", "@level", "ERROR"])
    .evaluate(), None)
```

### Pipeline Optimization

```python
# Less efficient - maps all items then filters
stream("data.jsonl") \
    .map(expensive_transform) \
    .filter(predicate)

# More efficient - filter first, map fewer items
stream("data.jsonl") \
    .filter(predicate) \
    .map(expensive_transform)
```

## Integration with Other Tools

JAF is designed to work well with other tools in the Unix tradition:

```python
# Export filtered data for other tools
pipeline = stream("data.jsonl") \
    .filter(["eq?", "@status", "active"]) \
    .map(["dict", "id", "@id", "name", "@name"])

# Option 1: Evaluate and pipe to other tools
for item in pipeline.evaluate():
    print(json.dumps(item))
# Then: python script.py | ja groupby name

# Option 2: Save stream descriptor for later use
with open("active_users.json", "w") as f:
    json.dump(pipeline.to_dict(), f)
# Then: jaf eval active_users.json | other-tool
```

### Working with jsonl-algebra

Since JAF focuses on filtering and transformation, it pairs well with `jsonl-algebra` for relational operations:

```bash
# Filter with JAF, aggregate with jsonl-algebra
jaf filter logs.jsonl '["eq?", "@level", "ERROR"]' --eval | \
ja groupby service --aggregate 'count:count'

# Complex pipeline
jaf stream users.jsonl \
    --filter '["gt?", "@last_login", "2024-01-01"]' \
    --map '["dict", "id", "@id", "dept", "@department", "salary", "@salary"]' | \
ja groupby dept --aggregate 'avg_salary:salary:avg'
```

## Best Practices

1. **Filter Early**: Apply filters before expensive transformations
2. **Use Appropriate Tools**: JAF for filtering/transformation, specialized tools for aggregation
3. **Lazy Evaluation**: Build complex pipelines without worrying about immediate execution
4. **Stream Large Files**: JAF handles files larger than memory efficiently
5. **Compose Operations**: Break complex logic into simple, reusable operations

## Next Steps

- Learn about [Query Language](query-language.md) for complex predicates
- See practical examples in the [Cookbook](cookbook.md)
- Explore [Advanced Topics](advanced.md) for performance tuning
- Check the [CLI Reference](cli-reference.md) for command-line usage
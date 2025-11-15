# JAF - Just Another Flow

[![PyPI version](https://badge.fury.io/py/jaf.svg)](https://badge.fury.io/py/jaf)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Test Coverage](https://img.shields.io/badge/coverage-68%25-yellow.svg)](https://github.com/queelius/jaf)
[![Tests](https://img.shields.io/badge/tests-474%20passing-brightgreen.svg)](https://github.com/queelius/jaf)

JAF (Just Another Flow) is a powerful streaming data processing system for JSON/JSONL data with a focus on lazy evaluation, composability, and a fluent API.

## Features

- 🚀 **Streaming Architecture** - Process large datasets without loading everything into memory
- 🔗 **Lazy Evaluation** - Build complex pipelines that only execute when needed
- 🎯 **Fluent API** - Intuitive method chaining for readable code
- 🧩 **Composable** - Combine operations freely, integrate with other tools
- 📦 **Multiple Sources** - Files, directories, stdin, memory, compressed files, infinite streams
- 🛠️ **Unix Philosophy** - Works great with pipes and other command-line tools

## Installation

```bash
pip install jaf
```

## Quick Start

### Command Line

```bash
# Filter JSON data using S-expressions (lazy by default)
jaf filter users.jsonl '(gt? @age 25)'

# Or use JSON array syntax
jaf filter users.jsonl '["gt?", "@age", 25]'

# Or use infix DSL (note: paths need @ prefix)
jaf filter users.jsonl '@age > 25'

# Evaluate immediately with --eval
jaf filter users.jsonl '(gt? @age 25)' --eval

# Chain operations
jaf filter users.jsonl '(eq? @status "active")' | \
jaf map - "@email" | \
jaf eval -

# Complex queries with nested logic
jaf filter logs.jsonl '(and (eq? @level "ERROR") (gt? @timestamp "2024-01-01"))' --eval

# Combine with other tools
jaf filter logs.jsonl '(eq? @level "ERROR")' --eval | \
ja groupby service
```

### Python API

```python
from jaf import stream

# Build a pipeline
pipeline = stream("users.jsonl") \
    .filter(["gt?", "@age", 25]) \
    .map(["dict", "name", "@name", "email", "@email"]) \
    .take(10)

# Execute when ready
for user in pipeline.evaluate():
    print(user)
```

## Core Concepts

### Lazy Evaluation

Operations don't execute until you call `.evaluate()` or use `--eval`:

```python
# This doesn't read any data yet
pipeline = stream("huge_file.jsonl") \
    .filter(["contains?", "@tags", "important"]) \
    .map("@message")

# Now it processes data
for message in pipeline.evaluate():
    process(message)
```

### Query Language

JAF supports multiple query syntaxes for flexibility:

#### 1. S-Expression Syntax (Lisp-like)
```lisp
# Simple comparisons
(eq? @status "active")              # status == "active"
(gt? @age 25)                       # age > 25
(contains? @tags "python")          # "python" in tags

# Boolean logic
(and 
    (gte? @age 18)
    (eq? @verified true))

# Nested expressions
(or (eq? @role "admin") 
    (and (eq? @role "user") 
         (gt? @score 100)))
```

#### 2. JSON Array Syntax
```python
# Same queries in JSON array format
["eq?", "@status", "active"]
["gt?", "@age", 25]
["contains?", "@tags", "python"]

["and", 
    ["gte?", "@age", 18],
    ["eq?", "@verified", true]
]
```

#### 3. Infix DSL Syntax
```python
# Natural infix notation (paths need @ prefix)
@status == "active"
@age > 25 and @verified == true
@role == "admin" or (@role == "user" and @score > 100)
```

All three syntaxes compile to the same internal representation. Use whichever feels most natural for your use case!

### Streaming Operations

- **filter** - Keep items matching a predicate
- **map** - Transform each item
- **take/skip** - Limit or paginate results
- **batch** - Group items into chunks
- **Boolean ops** - AND, OR, NOT on filtered streams

## Documentation

- [Getting Started](https://queelius.github.io/jaf/getting-started/) - Installation and first steps
- [API Guide](https://queelius.github.io/jaf/api-guide/) - Complete Python API reference
- [Query Language](https://queelius.github.io/jaf/query-language/) - Query syntax and operators
- [CLI Reference](https://queelius.github.io/jaf/cli-reference/) - Command-line usage
- [Cookbook](https://queelius.github.io/jaf/cookbook/) - Practical examples

## Examples

### Log Analysis

```python
# Find errors in specific services
errors = stream("app.log.jsonl") \
    .filter(["and",
        ["eq?", "@level", "ERROR"],
        ["in?", "@service", ["api", "auth"]]
    ]) \
    .map(["dict", 
        "time", "@timestamp",
        "service", "@service",
        "message", "@message"
    ]) \
    .evaluate()
```

### Data Validation

```python
# Find invalid records
invalid = stream("users.jsonl") \
    .filter(["or",
        ["not", ["exists?", "@email"]],
        ["not", ["regex-match?", "@email", "^[^@]+@[^@]+\\.[^@]+$"]]
    ]) \
    .evaluate()
```

### ETL Pipeline

```python
# Transform and filter data
pipeline = stream("raw_sales.jsonl") \
    .filter(["eq?", "@status", "completed"]) \
    .map(["dict",
        "date", ["date", "@timestamp"],
        "amount", "@amount",
        "category", ["if", ["gt?", "@amount", 1000], "high", "low"]
    ]) \
    .batch(1000)

# Process in chunks
for batch in pipeline.evaluate():
    bulk_insert(batch)
```

## Integration

JAF works seamlessly with other tools:

```bash
# With jsonl-algebra
jaf filter orders.jsonl '["gt?", "@amount", 100]' --eval | \
ja groupby customer_id --aggregate 'total:amount:sum'

# With jq
jaf filter data.jsonl '["exists?", "@metadata"]' --eval | \
jq '.metadata'

# With standard Unix tools
jaf map users.jsonl "@email" --eval | sort | uniq -c
```

## Performance

JAF is designed for streaming large datasets:

- Processes one item at a time
- Minimal memory footprint
- Early termination (e.g., with `take`)
- Efficient pipeline composition

## Windowed Operations

JAF supports windowed operations for memory-efficient processing of large datasets:

- **distinct**, **groupby**, **join**, **intersect**, **except** all support `window_size` parameter
- Use `window_size=float('inf')` for exact results (default)
- Finite windows trade accuracy for memory efficiency
- **Warning**: For intersect/except, window size must be large enough to capture overlapping items

```python
# Exact distinct (uses more memory)
stream("data.jsonl").distinct(window_size=float('inf'))

# Windowed distinct (bounded memory)
stream("data.jsonl").distinct(window_size=1000)

# Tumbling window groupby
stream("logs.jsonl").groupby(key="@level", window_size=100)
```

## Future Work

### Probabilistic Data Structures
- **Bloom Filters** for memory-efficient approximate set operations (intersect, except, distinct)
- **Count-Min Sketch** for frequency estimation and heavy hitters detection
- **HyperLogLog** for cardinality estimation
- These would provide controllable accuracy/memory tradeoffs with theoretical guarantees

### Additional Features
- **Top-K operations** - Find most frequent items in streams
- **Sampling strategies** - Reservoir sampling, stratified sampling
- **Time-based windows** - Process data in time intervals
- **Exactly-once semantics** - Checkpointing and recovery
- **Parallel processing** - Multi-threaded stream processing

### Integrations
- **FastAPI** - REST API for stream processing
- **Model Context Protocol (MCP)** - LLM integration
- **Apache Kafka** - Stream from/to Kafka topics
- **Cloud Storage** - S3, GCS, Azure Blob support

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

JAF is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Related Projects

- [jsonl-algebra](https://github.com/queelius/jsonl-algebra) - Relational operations on JSONL
- [jq](https://github.com/stedolan/jq) - Command-line JSON processor
- [dotsuite](https://github.com/realazthat/dotsuite) - Pedagogical ecosystem demonstrating the concepts behind JAF through simple, composable tools. Great for understanding the theory and building blocks that JAF productionizes.
# JAF - Just Another Flow

[![PyPI version](https://badge.fury.io/py/jaf.svg)](https://badge.fury.io/py/jaf)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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
# Filter JSON data (lazy by default)
jaf filter users.jsonl '["gt?", "@age", 25]'

# Evaluate immediately
jaf filter users.jsonl '["gt?", "@age", 25]' --eval

# Chain operations
jaf filter users.jsonl '["eq?", "@status", "active"]' | \
jaf map - "@email" | \
jaf eval -

# Combine with other tools
jaf filter logs.jsonl '["eq?", "@level", "ERROR"]' --eval | \
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

JAF uses S-expression syntax for queries:

```python
# Simple comparisons
["eq?", "@status", "active"]         # status == "active"
["gt?", "@age", 25]                  # age > 25
["contains?", "@tags", "python"]     # "python" in tags

# Boolean logic
["and", 
    ["gte?", "@age", 18],
    ["eq?", "@verified", true]
]

# Path navigation with @
["eq?", "@user.profile.name", "Alice"]  # Nested access
["any", "@items.*.inStock"]             # Wildcard
["exists?", "@**.error"]                # Recursive search
```

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

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

JAF is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Related Projects

- [jsonl-algebra](https://github.com/queelius/jsonl-algebra) - Relational operations on JSONL
- [jq](https://github.com/stedolan/jq) - Command-line JSON processor
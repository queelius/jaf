# JAF - Just Another Flow

JAF (Just Another Flow) is a powerful streaming data processing system for JSON/JSONL data with a focus on lazy evaluation, composability, and a fluent API. It provides both a command-line interface and a Python API for filtering, transforming, and analyzing JSON data streams.

## Key Features

- **Streaming Architecture**: Process large datasets efficiently without loading everything into memory
- **Lazy Evaluation**: Operations are only executed when results are needed
- **Fluent API**: Chain operations together in a readable, intuitive way
- **Composable Operations**: Combine filters, maps, and other transformations
- **Boolean Algebra**: Perform AND, OR, NOT operations on filtered streams
- **Multiple Data Sources**: Files, directories, stdin, memory, gzip, and infinite streams
- **Unix Philosophy**: Individual commands can be piped together with other tools

## Quick Start

### Installation

```bash
pip install jaf
```

### Command Line

```bash
# Filter JSON data (outputs stream descriptor by default)
jaf filter users.jsonl '["gt?", "@age", 25]'

# Evaluate immediately with --eval
jaf filter users.jsonl '["gt?", "@age", 25]' --eval

# Pipe operations together
jaf filter users.jsonl '["eq?", "@status", "active"]' | \
jaf map - "@name" | \
jaf eval -

# Use the stream command for eager evaluation
jaf stream users.jsonl --filter '["gt?", "@age", 25]' --map "@name"

# Combine with other Unix tools
jaf filter users.jsonl '["eq?", "@role", "admin"]' --eval | \
ja groupby department
```

### Python API

```python
from jaf import stream

# Create a stream and build a pipeline
pipeline = stream("users.jsonl") \
    .filter(["gt?", "@age", 25]) \
    .map("@name") \
    .take(10)

# Execute the pipeline
for name in pipeline.evaluate():
    print(name)
```

## Core Concepts

### Streaming Architecture

JAF uses a lazy streaming architecture where operations build pipelines rather than immediately processing data:

```python
# This doesn't process any data yet
pipeline = stream("large_file.jsonl") \
    .filter(["eq?", "@type", "error"]) \
    .map(["dict", "time", "@timestamp", "msg", "@message"])

# Data is processed only when we evaluate
for error in pipeline.evaluate():
    print(error)
```

### Query Language

JAF uses an S-expression syntax for queries and expressions:

```python
# Simple equality check
["eq?", "@name", "Alice"]

# Complex boolean logic
["and",
  ["gt?", "@age", 18],
  ["or",
    ["eq?", "@status", "active"],
    ["in?", "@role", ["admin", "moderator"]]
  ]
]

# Path navigation with @
["eq?", "@user.profile.verified", true]
```

### The @ Notation

The `@` prefix provides concise path navigation:

- `"@name"` → Get the "name" field
- `"@user.email"` → Navigate nested objects
- `"@items.*.price"` → Use wildcards
- `"@**.error"` → Recursive search

## What JAF Does Well

JAF focuses on:

1. **Filtering**: Complex boolean queries on JSON data
2. **Transformation**: Mapping and reshaping data
3. **Stream Processing**: Efficient handling of large datasets
4. **Composition**: Building complex pipelines from simple operations

For operations like grouping, joining, or aggregation, JAF works great with specialized tools:
- Use `jsonl-algebra` for relational operations
- Use `jq` for complex JSON transformations
- Use `pandas` or `duckdb` for analytical queries

## Learn More

- **[Getting Started](getting-started.md)**: Detailed installation and first steps
- **[Fluent API Guide](api-guide.md)**: Complete guide to the Python API
- **[Query Language](query-language.md)**: JAF query syntax and operators
- **[CLI Reference](cli-reference.md)**: Command-line interface documentation
- **[Cookbook](cookbook.md)**: Common patterns and recipes
- **[Advanced Topics](advanced.md)**: Performance, infinite streams, and more

## Philosophy

JAF is designed around several key principles:

1. **Do One Thing Well**: Focus on filtering and transforming JSON streams
2. **Lazy by Default**: Build complex pipelines without immediate execution
3. **Composability**: All operations can be freely combined
4. **Streaming First**: Handle large datasets that don't fit in memory
5. **Unix Philosophy**: Work well with other tools in a pipeline

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for a comprehensive introduction
- Explore the [Fluent API Guide](api-guide.md) to master the Python interface
- Check out the [Cookbook](cookbook.md) for practical examples
- Reference the [Query Language](query-language.md) documentation for all available operators
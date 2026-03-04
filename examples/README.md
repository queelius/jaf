# JAF Examples

This directory contains practical examples demonstrating JAF's capabilities.

## Python Examples

### basic_filtering.py
Fundamental JAF operations: filtering, mapping, and chaining operations.

```bash
python examples/basic_filtering.py
```

### probabilistic_dedup.py
Memory-efficient deduplication using probabilistic data structures:
- Bloom filters for set membership
- HyperLogLog for cardinality estimation
- Count-Min Sketch for frequency estimation

```bash
python examples/probabilistic_dedup.py
```

### log_analysis.py
Real-world example: analyzing structured log data with JAF.
- Error analysis
- Slow request detection
- Aggregation and grouping
- Unique user counting

```bash
python examples/log_analysis.py
```

## CLI Examples

### cli_examples.sh
Demonstrates JAF command-line patterns:
- Basic filtering with different syntaxes
- Map/transform operations
- Stream chaining
- Distinct with various strategies
- Piping and lazy evaluation

```bash
bash examples/cli_examples.sh
```

## Quick Start

```python
from jaf import stream

# Filter and transform data
results = stream("data.jsonl") \
    .filter(["gt?", "@age", 25]) \
    .map(["dict", "name", "@name"]) \
    .take(10) \
    .evaluate()

for item in results:
    print(item)
```

## Query Syntaxes

JAF supports three equivalent syntaxes:

```python
# S-expression (Lisp-like)
.filter("(gt? @age 25)")

# JSON array
.filter(["gt?", "@age", 25])

# Infix DSL
.filter("@age > 25")
```

## See Also

- [Full Documentation](https://queelius.github.io/jaf/)
- [API Guide](https://queelius.github.io/jaf/api-guide/)
- [Query Language](https://queelius.github.io/jaf/query-language/)

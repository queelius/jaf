# JAF CLI Reference

The JAF command-line interface provides tools for filtering, transforming, and processing JSON/JSONL data streams.

## Global Options

```bash
jaf [--help] [--version] [--log-level LEVEL]
```

- `--help`, `-h`: Show help message
- `--version`, `-v`: Show version number
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Commands Overview

JAF provides two types of commands:

1. **Individual Operations** (lazy by default): `filter`, `map`, `take`, etc.
2. **Combined Operation**: `stream` (eager by default)

## Individual Operation Commands

These commands output stream descriptors by default, enabling Unix-style piping.

### filter

Filter items based on a predicate query.

```bash
jaf filter <input> <query> [--eval]
```

**Arguments:**
- `<input>`: Input file, directory, or `-` for stdin
- `<query>`: JAF query expression

**Options:**
- `--eval`: Evaluate immediately instead of outputting stream descriptor

**Examples:**
```bash
# Output stream descriptor (default)
jaf filter users.jsonl '["gt?", "@age", 25]'

# Evaluate and output results
jaf filter users.jsonl '["gt?", "@age", 25]' --eval

# Filter from stdin
echo '[{"x": 1}, {"x": 2}]' | jaf filter - '["gt?", "@x", 1]' --eval
```

### map

Transform each item using an expression.

```bash
jaf map <input> <expression> [--eval]
```

**Arguments:**
- `<input>`: Input file, directory, or `-` for stdin
- `<expression>`: JAF transformation expression

**Examples:**
```bash
# Extract names
jaf map users.jsonl "@name" --eval

# Create new structure
jaf map data.jsonl '["dict", "id", "@id", "value", ["*", "@count", 2]]'

# Chain with filter
jaf filter users.jsonl '["eq?", "@active", true]' | jaf map - "@email"
```

### take

Take the first N items from the stream.

```bash
jaf take <input> <count> [--eval]
```

**Examples:**
```bash
# Take first 10 items
jaf take data.jsonl 10 --eval

# Combine with filter
jaf filter logs.jsonl '["eq?", "@level", "ERROR"]' | jaf take - 5 --eval
```

### skip

Skip the first N items from the stream.

```bash
jaf skip <input> <count> [--eval]
```

**Examples:**
```bash
# Skip first 100 items
jaf skip data.jsonl 100 --eval

# Pagination
jaf skip users.jsonl 50 | jaf take - 10 --eval
```

### batch

Group items into batches of specified size.

```bash
jaf batch <input> <size> [--eval]
```

**Examples:**
```bash
# Process in batches of 100
jaf batch large_file.jsonl 100 --eval

# Each output line will be an array of up to 100 items
```

### distinct

Remove duplicate items from a stream.

```bash
jaf distinct <input> [--key EXPR] [--strategy {exact,windowed,probabilistic}] [--window-size N] [--bloom-expected-items N] [--bloom-fp-rate RATE] [--eval]
```

**Options:**
- `--key`, `-k`: Key expression for deduplication (e.g., `@user_id`)
- `--strategy`, `-s`: `exact` (default), `windowed`, or `probabilistic`
- `--window-size`: Number of items to remember (windowed strategy)
- `--bloom-expected-items`: Expected item count for Bloom filter sizing (probabilistic)
- `--bloom-fp-rate`: Target false positive rate, default `0.01` (probabilistic)
- `--eval`: Evaluate immediately

**Examples:**
```bash
# Remove exact duplicates
jaf distinct events.jsonl --eval

# Deduplicate by email field
jaf distinct users.jsonl --key '@email' --eval

# Memory-efficient dedup for large files
jaf distinct huge.jsonl --key '@id' \
    --strategy probabilistic \
    --bloom-expected-items 1000000 \
    --eval

# Windowed dedup (remember last 5000 items)
jaf distinct logs.jsonl --key '@request_id' \
    --strategy windowed \
    --window-size 5000 \
    --eval

# Chain with other operations
jaf filter users.jsonl '["eq?", "@active", true]' | \
jaf distinct - --key '@email' --eval
```

### eval

Evaluate a stream descriptor to get actual results.

```bash
jaf eval <input>
```

**Arguments:**
- `<input>`: Stream descriptor file or `-` for stdin

**Examples:**
```bash
# Evaluate a saved pipeline
jaf filter data.jsonl '["gt?", "@score", 90]' > pipeline.json
jaf eval pipeline.json

# Evaluate from pipe
jaf filter data.jsonl '["contains?", "@tags", "urgent"]' | \
jaf map - '["dict", "id", "@id", "title", "@title"]' | \
jaf eval -
```

## Boolean Operation Commands

These commands work with FilteredStream objects to perform set operations.

### and

Logical AND of two filtered streams.

```bash
jaf and <left> <right> [--eval]
```

**Examples:**
```bash
# Find active premium users
jaf filter users.jsonl '["eq?", "@active", true]' > active.json
jaf filter users.jsonl '["eq?", "@plan", "premium"]' > premium.json
jaf and active.json premium.json --eval
```

### or

Logical OR of two filtered streams.

```bash
jaf or <left> <right> [--eval]
```

### not

Logical NOT of a filtered stream.

```bash
jaf not <input> [--eval]
```

### xor

Logical XOR (exclusive OR) of two filtered streams.

```bash
jaf xor <left> <right> [--eval]
```

### difference

Set difference (items in left but not in right).

```bash
jaf difference <left> <right> [--eval]
```

## Combined Operation Command

### stream

Build and execute a pipeline with multiple operations. Evaluates by default.

```bash
jaf stream <input> [options] [--lazy]
```

**Options:**
- `--filter <query>`, `-f`: Apply filter (can be used multiple times)
- `--map <expr>`, `-m`: Apply transformation (can be used multiple times)
- `--take <n>`, `-t`: Take first N items
- `--skip <n>`, `-s`: Skip first N items
- `--batch <size>`, `-b`: Group into batches
- `--enumerate`, `-e`: Add index to each item
- `--distinct`: Remove duplicate items
- `--distinct-key <expr>`: Key expression for deduplication
- `--strategy {exact,windowed,probabilistic}`: Strategy for set operations (default: `exact`)
- `--window-size <n>`: Window size for windowed strategy
- `--bloom-expected-items <n>`: Expected items for probabilistic strategy
- `--bloom-fp-rate <rate>`: False positive rate for probabilistic strategy (default: `0.01`)
- `--lazy`, `-l`: Output stream descriptor instead of evaluating

**Examples:**
```bash
# Complex pipeline with immediate evaluation (default)
jaf stream users.jsonl \
  --filter '["eq?", "@active", true]' \
  --map '["dict", "name", "@name", "email", "@email"]' \
  --take 10

# Multiple filters and maps
jaf stream logs.jsonl \
  --filter '["eq?", "@level", "ERROR"]' \
  --filter '["gt?", "@timestamp", "2024-01-01"]' \
  --map '@message' \
  --take 100

# Output stream descriptor for later use
jaf stream data.jsonl \
  --filter '["contains?", "@tags", "important"]' \
  --map '@content' \
  --lazy > important_content.json

# Deduplicate within a pipeline
jaf stream events.jsonl \
    --filter '["eq?", "@type", "purchase"]' \
    --distinct --distinct-key '@transaction_id' \
    --take 100

# Probabilistic dedup in a pipeline
jaf stream huge_log.jsonl \
    --distinct --distinct-key '@request_id' \
    --strategy probabilistic \
    --bloom-expected-items 500000
```

## Utility Commands

### info

Display information about a stream without evaluating it.

```bash
jaf info <input>
```

**Examples:**
```bash
# Show stream info
jaf filter data.jsonl '["gt?", "@value", 100]' > filtered.json
jaf info filtered.json

# Output:
# {
#   "type": "LazyDataStream",
#   "source_type": "filter",
#   "pipeline": "filter → jsonl → file(data.jsonl)"
# }
```

## Input Sources

JAF can read from various sources:

### Files
```bash
jaf filter data.json <query>        # JSON array file
jaf filter data.jsonl <query>       # JSONL file
jaf filter data.json.gz <query>     # Gzipped file
```

### Directories
```bash
jaf filter /path/to/data/ <query>   # All JSON/JSONL files in directory
```

### Standard Input
```bash
cat data.jsonl | jaf filter - <query>
echo '[{"x": 1}]' | jaf filter - <query>
```

### Stream Descriptors
```bash
jaf eval saved_pipeline.json
cat pipeline.json | jaf eval -
```

## Common Patterns

### Piping Operations

```bash
# Filter → Map → Take
jaf filter users.jsonl '["eq?", "@role", "admin"]' | \
jaf map - '["dict", "id", "@id", "name", "@name"]' | \
jaf take - 10 | \
jaf eval -
```

### Saving and Reusing Pipelines

```bash
# Save a complex pipeline
jaf filter logs.jsonl '["eq?", "@severity", "high"]' | \
jaf map - '["dict", "time", "@timestamp", "msg", "@message"]' > high_severity.json

# Later: evaluate the saved pipeline
jaf eval high_severity.json
```

### Integration with Other Tools

```bash
# With jsonl-algebra
jaf filter sales.jsonl '["gt?", "@amount", 1000]' --eval | \
ja groupby region --aggregate 'total:amount:sum'

# With jq
jaf filter data.jsonl '["exists?", "@metadata"]' --eval | \
jq '.metadata'

# With standard Unix tools
jaf map users.jsonl "@email" --eval | sort | uniq
```

### Debugging

```bash
# Use info to understand pipeline structure
jaf filter data.jsonl <complex_query> | jaf info -

# Use --log-level for debugging
jaf --log-level DEBUG filter data.jsonl <query>

# Test with small data first
head -10 big_file.jsonl | jaf filter - <query> --eval
```

## Query Language Quick Reference

### Basic Comparisons
- `["eq?", "@field", value]` - Equality
- `["gt?", "@field", value]` - Greater than
- `["contains?", "@array", value]` - Array/string contains

### Boolean Logic
- `["and", expr1, expr2, ...]` - All must be true
- `["or", expr1, expr2, ...]` - At least one true
- `["not", expr]` - Negation

### Path Access
- `"@field"` - Simple field
- `"@obj.nested.field"` - Nested access
- `"@items.*.name"` - Wildcard
- `"@**.error"` - Recursive search

See the [Query Language](query-language.md) documentation for the complete reference.

## Exit Codes

- `0`: Success
- `1`: General error
- `2`: Invalid arguments
- Other: Specific error conditions

## Environment Variables

- `JAF_LOG_LEVEL`: Default log level if not specified via --log-level
- `NO_COLOR`: Disable colored output

## Performance Tips

1. **Filter early**: Apply selective filters before transformations
2. **Use --eval sparingly**: Lazy evaluation is more memory efficient
3. **Batch large datasets**: Use the batch command for bulk operations
4. **Chain operations**: Build complex pipelines without intermediate files

## See Also

- [Query Language Reference](query-language.md)
- [API Guide](api-guide.md)
- [Cookbook](cookbook.md)
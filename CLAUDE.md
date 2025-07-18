# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

JAF (JSON Array Filter) is a domain-specific language for filtering JSON arrays using AST-based queries. It's designed to be non-Turing-complete, focusing solely on filtering with predictable boolean results and boolean algebraic operations on result sets.

## Core Architecture

### Main Components

- **`jaf/lazy_streams.py`**: Core streaming classes (LazyDataStream, FilteredStream, MappedStream)
- **`jaf/jaf_eval.py`**: AST evaluation engine that processes query expressions
- **`jaf/path_evaluation.py`**: Path resolution system for navigating JSON structures using tagged AST components
- **`jaf/streaming_loader.py`**: Streaming data loader that dispatches to various source types
- **`jaf/console_script.py`**: CLI interface providing `jaf filter`, `jaf map`, `jaf stream`, etc. commands
- **`jaf/collection_loader.py`**: Data loading from files, directories, and stdin
- **`jaf/lazy_ops_loader.py`**: Lazy operation implementations (filter, map, take, groupby, join, etc.)

### Query System

JAF uses S-expression syntax with JSON arrays: `[operator, arg1, arg2, ...]`

The path system supports:
- Key access: `[["key", "name"]]` 
- Array indexing: `[["index", 0]]`
- Wildcards: `[["wc_level"]]`, `[["wc_recursive"]]`
- Regex keys: `[["regex_key", "pattern"]]`
- Fuzzy matching: `[["fuzzy_key", "target", 0.7]]`

Special `@` syntax provides concise path notation: `"@user.name"` equals `["@", [["key", "user"], ["key", "name"]]]`

### Available Operators

**Predicates (return boolean):**
- Comparison: `eq?`, `neq?`, `gt?`, `gte?`, `lt?`, `lte?`
- Membership: `in?`, `contains?`
- String matching: `starts-with?`, `ends-with?`, `regex-match?`
- Fuzzy matching: `close-match?`, `partial-match?`
- Type checking: `is-string?`, `is-number?`, `is-array?`, `is-object?`, `is-null?`, `is-empty?`

**Value Extractors & Transformers:**
- Data access: `length`, `type`, `keys`, `values`, `first`, `last`, `unique`
- String operations: `lower-case`, `upper-case`, `split`, `join`, `trim`
- Mathematical: `+`, `-`, `*`, `/`, `%`, `abs`, `round`, `floor`, `ceil`, `max`, `min`
- Type coercion: `to-string`, `to-number`, `to-boolean`, `to-list`
- Date/time: `now`, `date`, `datetime`, `date-diff`, `days`, `seconds`

**Special Forms:**
- Logical: `and`, `or`, `not`
- Conditional: `if`
- Path access: `@`
- Existence: `exists?`

### Streaming Architecture

JAF now uses a lazy streaming architecture where operations build pipelines:
- `LazyDataStream`: Base class for all streams
- `FilteredStream`: Stream filtered by a predicate query
- `MappedStream`: Stream with transformed values
- Operations are composable: `stream(file).filter(query).map(expr).take(10)`
- Lazy evaluation - nothing runs until `.evaluate()` is called

## TODO / Future Work

### Streaming vs Non-Streaming Operations
Currently, some operations (join, groupby, distinct, intersect, except) are non-streaming and load entire datasets into memory. Consider implementing windowed versions:
- Add `window_size` parameter (default: 1000) to make operations truly streaming
- Sliding window for distinct, join operations  
- Tumbling window for groupby
- Document approximate nature of windowed results
- Allow `window_size=float('inf')` for exact results with memory warning

### Code Modularization
- Better separation of concerns between streaming infrastructure and operations
- Separate modules for different operation types (stateless, windowed, aggregating)
- More consistent error handling across operations

## Development Commands

### Installation
```bash
# Set up complete development environment
make install-dev

# Or just install package
make install
```

### Testing
```bash
# Run all tests
make test

# Run tests with coverage
make test-cov

# Run specific test file
venv/bin/pytest tests/test_jaf_core.py

# Run tests matching pattern
venv/bin/pytest -k "test_path"
```

### Linting & Formatting
```bash
# Run flake8 linting
make lint

# Format code with black (optional)
make format

# Type checking with mypy (optional)
make type-check

# Run all checks
make check
```

### Documentation
```bash
# Serve docs locally
make docs-serve

# Build documentation
make docs-build

# Deploy to GitHub Pages
make docs-deploy
```

### Release Management
```bash
# Build distribution packages
make build

# Bump version (patch/minor/major)
make bump-patch
make bump-minor
make bump-major

# Create git tag and release
make tag-release

# Full release process
make release
```

### Cleanup
```bash
# Clean build artifacts
make clean

# Clean everything including venv
make clean-all
```

## CLI Usage

The `jaf` command provides filtering and boolean operations:

```bash
# Simple filtering
jaf filter data.jsonl --query '["eq?", "@status", "active"]'

# Complex filtering with new operators
jaf filter data.jsonl --query '["gt?", ["abs", "@temperature"], 100]'
jaf filter data.jsonl --query '["eq?", ["to-string", "@id"], "123"]'

# Boolean operations on result sets
jaf and result1.json result2.json
jaf or result1.json --query '["gt?", "@count", 10]'
jaf not result.json

# Resolve results back to original data
jaf resolve result.json --resolve
```

## Key Dependencies

- `rapidfuzz`: For fuzzy string matching in path operations
- `pytest`: Testing framework
- `mkdocs`: Documentation generation
- `lark-parser`: For potential DSL parsing (legacy dependency)

## Test Structure

Tests are organized in `tests/` directory:
- `test_jaf_core.py`: Core filtering functionality
- `test_path_*.py`: Path system tests
- `test_result_set.py`: Result set operations
- `test_collection_loader.py`: Data loading tests
- `test_console_script.py`: CLI interface tests
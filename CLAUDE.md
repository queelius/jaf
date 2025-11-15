# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

JAF (Just Another Flow) is a streaming data processing system for JSON/JSONL with lazy evaluation, composability, and a fluent API. It processes data using S-expression queries, supporting multiple syntaxes (S-expr, JSON arrays, infix DSL), and emphasizes memory-efficient streaming operations.

## Core Architecture

### Main Components

- **`jaf/lazy_streams.py`**: Core streaming infrastructure (LazyDataStream, FilteredStream, MappedStream)
- **`jaf/jaf_eval.py`**: Expression evaluation engine executing query ASTs against JSON data
- **`jaf/path_evaluation.py`**: Path resolution for navigating JSON with wildcards, regex, fuzzy matching
- **`jaf/streaming_loader.py`**: Multi-source data loading (files, directories, stdin, memory, generators, compressed)
- **`jaf/lazy_ops_loader.py`**: Lazy operations (filter, map, take, skip, batch, groupby, join, distinct, intersect, except)
- **`jaf/console_script.py`**: CLI with commands: filter, map, stream, eval, groupby, join, distinct, etc.
- **`jaf/api.py`**: FastAPI REST server with streaming endpoints and WebSocket support
- **`jaf/mcp_server.py`**: Model Context Protocol server for LLM integration
- **`jaf/sexp_parser.py`**: S-expression parser converting Lisp-like syntax to JSON AST
- **`jaf/dsl_compiler.py`**: Infix DSL compiler ("@age > 25" → JSON AST)
- **`jaf/io_utils.py`**: File I/O utilities for loading collections and walking data files

### Query System & Syntax

JAF supports **three query syntaxes** that compile to the same internal JSON AST:

1. **S-expression** (Lisp-like): `(gt? @age 25)`
2. **JSON arrays**: `["gt?", "@age", 25]`
3. **Infix DSL**: `@age > 25`

All three are interchangeable; use whichever feels natural for your context.

**Path notation** uses `@` prefix for concise access:
- Simple: `@name` → `["@", [["key", "name"]]]`
- Nested: `@user.email` → `["@", [["key", "user"], ["key", "email"]]]`
- Array index: `@items.0` → `["@", [["key", "items"], ["index", 0]]]`
- Wildcards: `@*.name` (one level), `@**.value` (recursive)
- Regex: `[["regex_key", "^user_.*"]]`
- Fuzzy: `[["fuzzy_key", "name", 0.8]]`

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

JAF uses **lazy evaluation** where operations build pipelines without executing:

```python
# Build pipeline (no execution yet)
pipeline = stream("data.jsonl") \
    .filter(["gt?", "@age", 25]) \
    .map(["dict", "name", "@name"]) \
    .take(100)

# Execute when needed
for item in pipeline.evaluate():
    process(item)
```

**Key Classes:**
- `LazyDataStream`: Base class, provides `.filter()`, `.map()`, `.take()`, `.skip()`, etc.
- `FilteredStream`: Wraps a stream with a predicate query
- `MappedStream`: Wraps a stream with a transformation expression
- Operations chain via fluent API; nothing executes until `.evaluate()`

**CLI Lazy Behavior:**
- Default: outputs stream descriptor (lazy)
- `--eval` flag: executes immediately and outputs results

## Data Flow & Integration Points

### Source Types
JAF supports multiple data sources via `stream()` or CLI:
- **File**: `stream("data.jsonl")` or `jaf filter data.jsonl`
- **Directory**: `stream(type="directory", path="/data", pattern="*.json", recursive=True)`
- **stdin**: `jaf filter -` (dash for stdin)
- **Compressed**: Automatic detection (.gz, .bz2, etc.)
- **Memory**: `stream(type="memory", data=[...])`
- **Generator**: `stream(type="generator", generator=func)` for infinite streams

### Integration Options
- **Python API**: `from jaf import stream` for programmatic pipelines
- **CLI**: `jaf filter`, `jaf map`, `jaf stream`, etc. with Unix pipes
- **FastAPI**: `jaf/api.py` REST server with streaming endpoints & WebSockets (install: `pip install jaf[api]`)
- **MCP Server**: `jaf/mcp_server.py` for LLM integration (install: `pip install jaf[mcp]`)
- **Web Frontend**: `frontend/index.html` connects to API server via WebSockets

See `INTEGRATIONS.md` for detailed integration examples.

## Recent Work & Known State

### Completed Features
- ✅ Windowed operations (distinct, groupby, join, intersect, except) with `window_size` parameter
- ✅ S-expression parser and infix DSL compiler
- ✅ FastAPI REST server with streaming & WebSocket support
- ✅ MCP server for LLM integration
- ✅ Web frontend dashboard
- ✅ Path evaluation fixes (empty arrays vs missing paths, `MissingPath` sentinel)
- ✅ Right/outer join support

### Known Limitations
- Windowed intersect/except require careful window sizing for accurate results (trade accuracy for memory)
- Some generator source tests may need attention
- Test coverage at 68%, with 474 tests passing

### Future Work
- **Probabilistic data structures**: Bloom filters, Count-Min Sketch, HyperLogLog for memory-efficient approximate operations
- **Code modularization**: Better separation of stateless/windowed/aggregating operations
- **Additional integrations**: Apache Kafka, cloud storage (S3, GCS, Azure)

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
# or: venv/bin/pytest tests/

# Run tests with coverage report
make test-cov

# Run specific test file
venv/bin/pytest tests/test_jaf_core.py -v

# Run specific test by name
venv/bin/pytest tests/test_jaf_core.py::TestJAFCore::test_simple_eq_query -v

# Run tests matching pattern
venv/bin/pytest -k "test_path"

# Run with specific verbosity/output
venv/bin/pytest tests/ --tb=line      # Brief traceback
venv/bin/pytest tests/ -x             # Stop on first failure
venv/bin/pytest tests/ --collect-only # List tests without running
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

The `jaf` command provides streaming operations with lazy evaluation by default:

```bash
# Simple filtering (lazy - outputs stream descriptor)
jaf filter data.jsonl '["eq?", "@status", "active"]'

# Evaluate immediately with --eval
jaf filter data.jsonl '["gt?", "@age", 25]' --eval

# Multiple query syntaxes supported
jaf filter data.jsonl '(gt? @age 25)'          # S-expression
jaf filter data.jsonl '["gt?", "@age", 25]'    # JSON array
jaf filter data.jsonl '@age > 25'              # Infix DSL

# Map/transform operations
jaf map data.jsonl '@email' --eval
jaf map data.jsonl '["dict", "name", "@name", "age", "@age"]' --eval

# Chain operations with Unix pipes
jaf filter users.jsonl '["eq?", "@status", "active"]' | \
jaf map - '["dict", "id", "@id", "email", "@email"]' | \
jaf eval -

# Stream operations with grouping and joins
jaf stream sales.jsonl \
    --filter '["eq?", "@status", "completed"]' \
    --groupby '["@", [["key", "category"]]]' \
    --aggregate '{"total": ["sum", "@amount"]}' \
    --eval

# Test individual expressions
jaf filter users.jsonl '(and (gt? @age 25) (eq? @status "active"))' --eval

# Working with CLI script directly
venv/bin/jaf filter data.jsonl '(gt? @age 25)' --eval
```

## Key Dependencies

**Core:**
- `rapidfuzz`: Fuzzy string matching for path operations
- `lark`: S-expression and DSL parsing

**Development:**
- `pytest`, `pytest-cov`, `pytest-asyncio`: Testing framework
- `mkdocs`, `mkdocs-material`: Documentation generation

**Optional Integrations:**
- `fastapi`, `uvicorn`, `pydantic`, `websockets`: REST API server (`pip install jaf[api]`)
- `mcp`: Model Context Protocol server (`pip install jaf[mcp]`)

## Test Structure

Tests organized in `tests/` (474 tests, 68% coverage):
- `test_jaf_core.py`: Core evaluation and filtering
- `test_lazy_streams.py`, `test_streaming.py`, `test_lazy_evaluation.py`: Streaming architecture
- `test_paths.py`, `test_path_*.py`: Path resolution and notation
- `test_dsl.py`, `test_sexp_parser.py`: Query language parsing
- `test_console_script_*.py`, `test_cli_*.py`: CLI interface
- `test_api*.py`: FastAPI REST server
- `test_mcp_server*.py`: MCP integration
- `test_windowed_operations.py`: Windowed operations (groupby, join, distinct, etc.)
- `test_critical_bugs.py`: Regression tests for fixed bugs
- `test_lazy_ops_coverage.py`: Coverage tests for operations

## Development Patterns

### When Adding New Operators
1. Add operator implementation to `jaf/jaf_eval.py` in the appropriate section (predicates, transformers, special forms)
2. Update operator documentation in README and docs
3. Add comprehensive tests covering edge cases, type handling, and null/missing values
4. Ensure operator works with path expressions (`@field`) and nested data
5. Consider adding examples to `test_builtins.py` or relevant test file

### When Modifying Path System
- Path operations in `jaf/path_evaluation.py`, types in `jaf/path_types.py`
- Use `MissingPath` sentinel to distinguish non-existent paths from `None`/empty values
- Test with `test_paths.py` and `test_path_*.py` files
- Verify wildcard (`@*.field`, `@**.field`) and special paths work correctly

### When Working with Streaming Operations
- Lazy operations in `jaf/lazy_ops_loader.py`, base classes in `jaf/lazy_streams.py`
- Ensure operations are truly lazy (don't consume iterator until `.evaluate()`)
- For windowed operations, support `window_size` parameter with default `float('inf')`
- Test memory efficiency for large datasets
- Add tests to `test_lazy_streams.py` or `test_windowed_operations.py`

### When Adding CLI Commands
- Add command parser in `jaf/console_script.py`
- Support both lazy (default) and immediate evaluation (`--eval` flag)
- Accept stdin with `-` as input source
- Test with `test_console_script_*.py` and `test_cli_*.py`
- Update CLI examples in this file and README

### Testing Best Practices
- Write tests for new features/operators FIRST (TDD approach per global CLAUDE.md)
- Run coverage to identify gaps: `make test-cov`
- Use parametrized tests for operators with multiple input types
- Test edge cases: empty collections, null values, missing paths, type mismatches
- For streaming operations, test with different source types (file, memory, generator)
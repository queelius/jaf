# JAF Command-Line Interface (CLI) Reference

This page details the usage of the `jaf` command-line tool.

## Global Options

These options apply to most `jaf` subcommands:

-   `--log-level <LEVEL>`: Set the logging level. Choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Default: `WARNING`.
-   `--drop-filenames`: For boolean operations, omit `filenames_in_collection` from the output `JafResultSet` JSON.

## `jaf filter`

Filters JSON/JSONL data from a file or directory based on a JAF query.

**Usage:**

```bash
jaf filter <input_source> --query '<query_ast_json_string>' [options]
```

**Arguments & Options:**

-   `<input_source>` (required): Path to a JSON/JSONL file or a directory containing such files.
-   `--query '<query_ast_json_string>'` (required): The JAF query string, formatted as a JSON Abstract Syntax Tree (AST).
    -   Example: `'["eq?", ["path", [["key", "status"]]], "active"]'`
-   `--recursive`: If `<input_source>` is a directory, search for data files recursively.
-   `--collection-id <id>`: Assign a custom ID to the data collection. This ID will be stored in the output `JafResultSet`. If not provided, a default ID (usually the absolute path of the input source) is used.
-   `--resolve`: Instead of outputting a `JafResultSet` JSON, resolve the filter results and print the actual matching JSON objects, one per line (JSONL format).

**Output:**

-   By default: A `JafResultSet` JSON object to `stdout`.
-   With `--resolve`: A stream of matching JSON objects (JSONL) to `stdout`. Informational messages are printed to `stderr`.

**Examples:**

```bash
# Filter data.jsonl for items where status is "active", output JafResultSet
jaf filter data.jsonl --query '["eq?", ["path", [["key", "status"]]], "active"]'

# Filter data_dir recursively for items where count > 10, output matching objects
jaf filter data_dir --query '["gt?", ["path", [["key", "count"]]], 10]' --recursive --resolve

# Assign a custom collection ID
jaf filter logs.jsonl --query '["exists?", ["path", [["key", "error"]]]]' --collection-id "error_logs_v1"
```

## Result Set Operations (Boolean Algebra)

These commands perform boolean algebra on `JafResultSet` instances. The output is always a new `JafResultSet` JSON to `stdout`.

**Operand Types:**
- **First Operand**: A `JafResultSet` provided via a file path or from `stdin` (`-`).
- **Second Operand (for binary ops)**: Can be either:
    1. A second `JafResultSet` from a file path.
    2. A JAF query string provided via the `--query` option. This query is executed against the data source of the first `JafResultSet`.

**General Input Handling:**

For binary operations (e.g., `and`, `or`, `xor`, `difference`):
-   `jaf <op> <rs1_path_or_-> <rs2_path_or_->`: Specifies both inputs.
-   `jaf <op> <file.json>`: Assumes first input from `stdin`, second from `<file.json>`.
-   `jaf <op> - <file.json>`: First input from `stdin`, second from `<file.json>`.
-   `jaf <op> <file.json> -`: First input from `<file.json>`, second from `stdin`. (Less common for CLI piping)

For unary operations (e.g., `not`):
-   `jaf not <rs_path_or_->`: Specifies the input.
-   `jaf not`: Assumes input from `stdin`.

It's an error to try to read both inputs from `stdin` for a binary operation.

### `jaf and`

Performs a logical AND (intersection).

**Usage:**
```bash
# With two JafResultSet inputs
jaf and [rs1_path_or_-] [rs2_path_or_-]

# With one JafResultSet and one on-the-fly query
jaf and [rs1_path_or_-] --query '<query_ast_json_string>'
```

### `jaf or`

Performs a logical OR (union).

**Usage:**
```bash
jaf or [rs1_path_or_-] [rs2_path_or_-]
jaf or [rs1_path_or_-] --query '<query_ast_json_string>'
```

### `jaf not`

Performs a logical NOT (complement) on a `JafResultSet`.

**Usage:** `jaf not [rs_path_or_-]`

### `jaf xor`

Performs a logical XOR (symmetric difference).

**Usage:**
```bash
jaf xor [rs1_path_or_-] [rs2_path_or_-]
jaf xor [rs1_path_or_-] --query '<query_ast_json_string>'
```

### `jaf difference`

Performs a logical set difference (`rs1 - rs2`).

**Usage:**
```bash
jaf difference [rs1_path_or_-] [rs2_path_or_-]
jaf difference [rs1_path_or_-] --query '<query_ast_json_string>'
```

**Boolean Operation Examples:**

```bash
# Create result sets
jaf filter users.jsonl --query '["eq?",["path",[["key","status"]]],"active"]' > active.jrs
jaf filter users.jsonl --query '["in?","dev",["path",[["key","tags"]]]]' > devs.jrs

# Active developers
jaf and active.jrs devs.jrs > active_devs.jrs

# Users who are active OR developers
jaf or active.jrs devs.jrs

# Users who are NOT active
jaf not active.jrs

# Users who are active but NOT developers
jaf difference active.jrs devs.jrs

# Users who are active but NOT developers (using --query)
jaf filter users.jsonl --query '["eq?", "@status", "active"]' | jaf difference --query '["in?","dev","@tags"]'

# Using stdin for the first input
cat active.jrs | jaf and - devs.jrs
```

## `jaf resolve`

Resolves a `JafResultSet` (from a file or `stdin`) back to the original data or derived values.

**Usage:**
```bash
jaf resolve [jrs_path_or_-] [options]
```

**Arguments:**

-   `[jrs_path_or_-]`: Path to the `JafResultSet` JSON file. If omitted or `-`, reads from `stdin`.

**Output Formatting Options:**

These options are mutually exclusive.

-   `--output-jsonl`: Output the results as JSONL, one object per line (this is the default behavior).
-   `--output-json-array`: Output the results as a single, pretty-printed JSON array.
-   `--output-indices`: Output a simple JSON array of the matching indices.
-   `--extract-path <PATH_STR>`: For each matching object, extract the value at the given JAF path string (e.g., `'@user.name'`).
-   `--apply-query <QUERY_AST>`: For each matching object, apply the given JAF query and print the result. The query does not need to be a predicate.

**Output:**

-   A stream of data to `stdout` in the format specified by the output options.

**Example:**

```bash
# Assuming active_devs.jrs was created by a filter or boolean operation
# and contains metadata pointing to the original users.jsonl
jaf resolve active_devs.jrs
# This will print the full JSON objects for each active developer.

# Piping from a boolean operation
jaf and active.jrs devs.jrs | jaf resolve

# Extract just the user IDs from the result
jaf resolve active_devs.jrs --extract-path '@user.id'

# Apply a query to transform the results
jaf resolve active_devs.jrs --apply-query '["+", "@score", 5]'

# Output as a JSON array
jaf resolve active_devs.jrs --output-json-array
```

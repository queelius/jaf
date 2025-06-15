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

These commands perform boolean algebra on `JafResultSet` instances. Inputs are typically `JafResultSet` JSON provided via file paths or `stdin` (`-`). The output is always a new `JafResultSet` JSON to `stdout`.

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

Performs a logical AND (intersection) on two `JafResultSet`s.

**Usage:** `jaf and [rs1_path_or_-] [rs2_path_or_-]`

### `jaf or`

Performs a logical OR (union) on two `JafResultSet`s.

**Usage:** `jaf or [rs1_path_or_-] [rs2_path_or_-]`

### `jaf not`

Performs a logical NOT (complement) on a `JafResultSet`.

**Usage:** `jaf not [rs_path_or_-]`

### `jaf xor`

Performs a logical XOR (symmetric difference) on two `JafResultSet`s.

**Usage:** `jaf xor [rs1_path_or_-] [rs2_path_or_-]`

### `jaf difference`

Performs a logical set difference (`rs1 - rs2`) on two `JafResultSet`s.

**Usage:** `jaf difference [rs1_path_or_-] [rs2_path_or_-]`

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

# Using stdin for the first input
cat active.jrs | jaf and - devs.jrs
```

## `jaf resolve`

Resolves a `JafResultSet` (from a file or `stdin`) back to the original JSON objects that its indices point to. Outputs a stream of these objects in JSONL format to `stdout`.

This command requires the input `JafResultSet` to contain sufficient metadata to locate the original data:
-   Either `filenames_in_collection` (a list of file paths) must be present and valid.
-   Or, `collection_id` must be a string representing a path to a single, existing data file.

**Usage:**

```bash
jaf resolve [jrs_path_or_-]
```

**Arguments:**

-   `[jrs_path_or_-]`: Path to the `JafResultSet` JSON file. If omitted or `-`, reads from `stdin`.

**Output:**

-   A stream of original JSON objects (JSONL) to `stdout`.

**Example:**

```bash
# Assuming active_devs.jrs was created by a filter or boolean operation
# and contains metadata pointing to the original users.jsonl
jaf resolve active_devs.jrs
# This will print the full JSON objects for each active developer.

# Piping from a boolean operation
jaf and active.jrs devs.jrs | jaf resolve
```

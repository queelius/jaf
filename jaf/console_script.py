import os
import argparse
import json
import logging
import sys
from typing import List, Dict, Optional, Union, Any
from .lazy_streams import stream, FilteredStream, MappedStream
from .result_set import JafQuerySet, JafQuerySetError  # Keep for backward compatibility during transition
from . import __version__
from .jaf_eval import jaf_eval
from .exceptions import JAFError
from .io_utils import (
    walk_data_files,
    load_objects_from_file,
    load_collection,
    load_objects_from_string,
)
from .path_conversion import string_to_path_ast
from .path_exceptions import PathSyntaxError
from .dsl_compiler import smart_compile, DSLSyntaxError

logger = logging.getLogger(__name__)

# load_objects_from_file and walk_data_files are now imported from io_utils


def main():
    parser = argparse.ArgumentParser(
        description="JAF: JSON Array Filter and Result Set Operations.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )
    # --pretty-print argument removed
    # The --drop-filenames argument is now obsolete with collection_source
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Sub-command to execute"
    )

    # --- 'filter' Subcommand ---
    filter_parser = subparsers.add_parser(
        "filter", help="Filter JSON/JSONL data using a JAF query."
    )
    filter_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file or directory. Defaults to '-' to read from stdin.",
    )
    filter_parser.add_argument(
        "query",
        type=str,
        help="A JAF query string (in JSON format) to filter the data.",
    )
    filter_parser.add_argument(
        "--stdin-max-buffer-size",
        type=int,
        default=1048576,  # 1 MiB
        metavar="BYTES",
        help="Max size in bytes to buffer stdin for later resolution. If stdin exceeds this, the result set will not be resolvable. Default: 1048576 (1 MiB).",
    )
    filter_parser.add_argument(
        "--collection-id",
        type=str,
        default=None,
        help="Optional ID for the data collection (used in JafQuerySet).",
    )
    filter_parser.add_argument(
        "-l",
        "--lazy",
        action="store_true",
        help="Output stream descriptor JSON for later composition instead of actual matching objects.",
    )

    # --- 'map' Subcommand ---
    map_parser = subparsers.add_parser(
        "map", help="Transform JSON/JSONL data using a JAF expression."
    )
    map_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor. Defaults to '-' to read from stdin.",
    )
    map_parser.add_argument(
        "expression",
        type=str,
        help="A JAF expression (in JSON format) to transform each item.",
    )
    map_parser.add_argument(
        "-l",
        "--lazy",
        action="store_true",
        help="Output stream descriptor JSON for later composition.",
    )

    # --- 'take' Subcommand ---
    take_parser = subparsers.add_parser(
        "take", help="Take the first N items from a data stream."
    )
    take_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor.",
    )
    take_parser.add_argument(
        "count",
        type=int,
        help="Number of items to take.",
    )
    take_parser.add_argument(
        "-l",
        "--lazy",
        action="store_true",
        help="Output stream descriptor JSON for later composition.",
    )

    # --- 'skip' Subcommand ---
    skip_parser = subparsers.add_parser(
        "skip", help="Skip the first N items from a data stream."
    )
    skip_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor.",
    )
    skip_parser.add_argument(
        "count",
        type=int,
        help="Number of items to skip.",
    )
    skip_parser.add_argument(
        "-l",
        "--lazy",
        action="store_true",
        help="Output stream descriptor JSON for later composition.",
    )

    # --- 'batch' Subcommand ---
    batch_parser = subparsers.add_parser(
        "batch", help="Group items into batches of specified size."
    )
    batch_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor.",
    )
    batch_parser.add_argument(
        "size",
        type=int,
        help="Size of each batch.",
    )
    batch_parser.add_argument(
        "-l",
        "--lazy",
        action="store_true",
        help="Output stream descriptor JSON for later composition.",
    )

    # --- 'stream' Subcommand ---
    stream_parser = subparsers.add_parser(
        "stream", help="Build and execute a stream processing pipeline with chained operations."
    )
    stream_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor. Defaults to '-' to read from stdin.",
    )
    # Operations can be repeated and will be applied in order
    stream_parser.add_argument(
        "--filter", "-f",
        action="append",
        metavar="QUERY",
        help="Filter with a JAF query. Can be used multiple times.",
    )
    stream_parser.add_argument(
        "--map", "-m",
        action="append",
        metavar="EXPR",
        help="Transform with a JAF expression. Can be used multiple times.",
    )
    stream_parser.add_argument(
        "--take", "-t",
        type=int,
        metavar="N",
        help="Take the first N items.",
    )
    stream_parser.add_argument(
        "--skip", "-s",
        type=int,
        metavar="N",
        help="Skip the first N items.",
    )
    stream_parser.add_argument(
        "--batch", "-b",
        type=int,
        metavar="SIZE",
        help="Group items into batches of SIZE.",
    )
    stream_parser.add_argument(
        "--enumerate", "-e",
        action="store_true",
        help="Add index to each item.",
    )
    stream_parser.add_argument(
        "--lazy", "-l",
        action="store_true",
        help="Output stream descriptor instead of evaluating.",
    )

    # --- 'eval' Subcommand ---
    eval_parser = subparsers.add_parser(
        "eval", help="Evaluate a lazy stream descriptor."
    )
    eval_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to stream descriptor JSON file or '-' for stdin.",
    )

    # --- Boolean Algebra Subcommands ---
    # Helper to add common args for boolean ops
    def add_boolean_op_args(p, op_name: str, num_inputs=2):
        if num_inputs == 1:  # For unary operations like NOT
            p.add_argument(
                "input_rs1",
                type=str,
                nargs="?",
                default="-",
                help="Path to the JafQuerySet JSON file, or '-' for stdin. Defaults to stdin if no path is given.",
            )
        elif num_inputs >= 2:  # For binary operations like AND, OR
            p.add_argument(
                "input_rs1",
                type=str,
                nargs="?",
                default="-",
                help=f"Path to the first JafQuerySet for '{op_name}'. Defaults to stdin.",
            )

            # The second operand can be another JRS or a query to be run on the fly.
            group = p.add_mutually_exclusive_group()
            group.add_argument(
                "input_rs2",
                type=str,
                nargs="?",
                default=None,
                help="Path to the second JafQuerySet. Cannot be used with --query.",
            )
            group.add_argument(
                "-q",
                "--query",
                type=str,
                help="A JAF query to run against the first input's data source. Cannot be used with a second JRS path.",
            )

    and_parser = subparsers.add_parser(
        "and", help="Perform logical AND on two JafQuerySets, or a JRS and a new query."
    )
    add_boolean_op_args(and_parser, "and", 2)

    or_parser = subparsers.add_parser(
        "or", help="Perform logical OR on two JafQuerySets, or a JRS and a new query."
    )
    add_boolean_op_args(or_parser, "or", 2)

    not_parser = subparsers.add_parser(
        "not", help="Perform logical NOT on a JafQuerySet."
    )
    add_boolean_op_args(not_parser, "not", 1)

    xor_parser = subparsers.add_parser(
        "xor", help="Perform logical XOR on two JafQuerySets, or a JRS and a new query."
    )
    add_boolean_op_args(xor_parser, "xor", 2)

    diff_parser = subparsers.add_parser(
        "difference",
        help="Perform logical DIFFERENCE (rs1 - rs2) on two JafQuerySets, or a JRS and a new query.",
    )
    add_boolean_op_args(diff_parser, "difference", 2)

    # --- 'resolve' Subcommand ---
    resolve_parser = subparsers.add_parser(
        "resolve",
        help="Resolve a JafQuerySet to original JSON objects or derived values. Reads JRS from stdin or file.",
    )
    resolve_parser.add_argument(
        "input_jrs",
        type=str,
        nargs="?",
        default="-",
        help="Path to the JafQuerySet JSON file, or '-' for stdin. Defaults to stdin if no path is given.",
    )

    # Add mutually exclusive output format options
    output_group = resolve_parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--output-jsonl",
        action="store_true",
        help="Output the results as JSONL, one object per line (default behavior).",
    )
    output_group.add_argument(
        "--output-json-array",
        action="store_true",
        help="Output the results as a single, pretty-printed JSON array.",
    )
    output_group.add_argument(
        "--output-indices",
        action="store_true",
        help="Output a simple JSON array of the matching indices.",
    )
    output_group.add_argument(
        "--extract-path",
        type=str,
        metavar="PATH_STR",
        help="For each matching object, extract and print the value at the given JAF path string (e.g., '@user.name').",
    )
    output_group.add_argument(
        "--apply-query",
        type=str,
        metavar="QUERY_AST",
        help="For each matching object, apply the given JAF query and print the result. The query does not need to be a predicate.",
    )

    # --- 'info' Subcommand ---
    info_parser = subparsers.add_parser(
        "info", help="Display summary information about a JafQuerySet."
    )
    info_parser.add_argument(
        "input_jrs",
        type=str,
        nargs="?",
        default="-",
        help="Path to the JafQuerySet JSON file, or '-' for stdin. Defaults to stdin.",
    )

    # --- 'validate' Subcommand ---
    validate_parser = subparsers.add_parser("validate", help="Validate a JafQuerySet.")
    validate_parser.add_argument(
        "input_jrs",
        type=str,
        nargs="?",
        default="-",
        help="Path to the JafQuerySet JSON file, or '-' for stdin. Defaults to stdin.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # --- Command Dispatch ---
    if args.command == "filter":
        handle_filter_command(args)
    elif args.command == "map":
        handle_map_command(args)
    elif args.command == "take":
        handle_take_command(args)
    elif args.command == "skip":
        handle_skip_command(args)
    elif args.command == "batch":
        handle_batch_command(args)
    elif args.command == "stream":
        handle_stream_command(args)
    elif args.command == "eval":
        handle_eval_command(args)
    elif args.command == "resolve":
        handle_resolve_command(args)
    elif args.command in {"and", "or", "not", "xor", "difference"}:
        handle_boolean_command(args)
    elif args.command == "info":
        handle_info_command(args)
    elif args.command == "validate":
        handle_validate_command(args)
    else:
        parser.print_help()
        sys.exit(1)


# Helper function to print objects as JSONL to stdout
def _print_objects_as_jsonl(objects: List[Any]):
    """Prints a list of objects as JSONL, one compact JSON per line."""
    for obj in objects:
        # Compact JSON: no indent, no spaces after separators
        print(json.dumps(obj, indent=None, separators=(",", ":")))


def handle_filter_command(args):
    logger.debug(f"Filter command with args: {args}\n")
    try:
        logger.debug(f"Parsing query: {args.query}\n")
        # Use smart_compile to handle both DSL and AST formats
        query_ast = smart_compile(args.query)
        logger.debug(f"Compiled to AST: {query_ast}\n")
    except (json.JSONDecodeError, DSLSyntaxError) as e:
        logger.error(f"Invalid query (neither valid JSON AST nor valid DSL): {e}\n")
        sys.exit(1)

    if not isinstance(query_ast, list) and query_ast is not None:
        logger.error(f"Invalid query AST type: {type(query_ast)}")
        sys.exit(1)

    input_source_path = args.input_source
    all_objects_to_filter: List[Any] = []
    collection_source: Dict[str, Any] = {}
    effective_collection_id: Optional[Any] = args.collection_id

    if input_source_path == "-":
        logger.info("Reading data from stdin.")
        stdin_content = sys.stdin.read()

        if len(stdin_content.encode("utf-8")) > args.stdin_max_buffer_size:
            logger.warning(
                f"stdin content size ({len(stdin_content.encode('utf-8'))} bytes) exceeds --stdin-max-buffer-size "
                f"({args.stdin_max_buffer_size} bytes). The resulting JafQuerySet will not be resolvable."
            )
            loaded_objects, detected_format = load_objects_from_string(stdin_content)
            collection_source = {"type": "stdin", "format": detected_format}
        else:
            logger.info(
                "stdin content is within buffer size limit; result set will be resolvable."
            )
            loaded_objects, detected_format = load_objects_from_string(stdin_content)
            # Embed the actual data into the source for later resolution.
            collection_source = {
                "type": "buffered_stdin",
                "format": detected_format,
                "content": loaded_objects,
            }

        if loaded_objects:
            all_objects_to_filter = loaded_objects

        if not effective_collection_id:
            effective_collection_id = "<stdin>"

    elif os.path.isdir(input_source_path):
        abs_dir_path = os.path.abspath(input_source_path)
        if not effective_collection_id:
            effective_collection_id = abs_dir_path

        file_paths_contributing = []
        for file_path in walk_data_files(args.input_source, args.recursive):
            loaded_objects = load_objects_from_file(file_path)
            if loaded_objects:
                all_objects_to_filter.extend(loaded_objects)
                file_paths_contributing.append(os.path.abspath(file_path))

        if file_paths_contributing:
            collection_source = {
                "type": "directory",
                "path": abs_dir_path,
                "files": list(set(file_paths_contributing)),
            }

    elif os.path.isfile(input_source_path):
        abs_file_path = os.path.abspath(input_source_path)
        if not effective_collection_id:
            effective_collection_id = abs_file_path

        loaded_objects = load_objects_from_file(args.input_source)
        if loaded_objects:
            all_objects_to_filter = loaded_objects

        if args.input_source.endswith(".jsonl"):
            collection_source = {"type": "jsonl", "path": abs_file_path}
        elif args.input_source.endswith(".json"):
            collection_source = {"type": "json_array", "path": abs_file_path}

    else:
        print(
            f"Error: Input source '{input_source_path}' is not a valid file, directory, or stdin ('-').",
            file=sys.stderr,
        )
        sys.exit(1)

    if not all_objects_to_filter:
        logger.info(
            f"No data could be loaded or no filterable JSON values found in the provided source(s): {args.input_source}"
        )
    # If all_objects_to_filter is empty, jaf() will correctly produce a JafQuerySet with empty indices and collection_size=0.

    try:
        logger.info(
            f"Applying JAF query to {len(all_objects_to_filter)} aggregated items. Collection ID: {effective_collection_id}"
        )
        
        # Create stream from the appropriate source
        if collection_source.get("type") == "buffered_stdin":
            # Use the buffered content directly
            data_stream = stream({"type": "memory", "data": collection_source["content"]})
        else:
            # Use memory source with the loaded data
            data_stream = stream({"type": "memory", "data": all_objects_to_filter})
        
        # Apply filter
        result_stream = data_stream.filter(query_ast)
        result_stream.collection_id = effective_collection_id
        
        if args.lazy:
            # Output stream descriptor JSON for later composition
            output_dict = result_stream.to_dict()
            # Always compact JSON for stream output
            print(json.dumps(output_dict, indent=None, separators=(",", ":")))
        else:
            # Default: eager evaluation - output actual matching objects
            matched_data = list(result_stream.evaluate())

            if logger.isEnabledFor(logging.INFO):
                print(f"--- Filtering Complete ---", file=sys.stderr)
                print(
                    f"Summary: Matched {len(matched_data)} item(s) out of {len(all_objects_to_filter)}.",
                    file=sys.stderr,
                )
                source_desc = collection_source.get("path") or args.input_source
                if collection_source.get("type") == "directory":
                    print(f"Source Directory: {source_desc}", file=sys.stderr)
                    print(
                        f"Files contributing to collection ({len(collection_source.get('files',[]))}): {', '.join(collection_source.get('files',[]))}",
                        file=sys.stderr,
                    )
                elif source_desc:
                    print(f"Source File: {source_desc}", file=sys.stderr)
                print(f"--- Matches (JSONL to stdout) ---", file=sys.stderr)

            _print_objects_as_jsonl(matched_data)

    except JAFError as e_jaf:
        logger.error(f"JAF Error processing: {e_jaf}", exc_info=True)
        print(f"JAF Error: {e_jaf}", file=sys.stderr)
        sys.exit(1)
    except Exception as e_proc:  # Catch any other unexpected errors
        logger.error(f"Unexpected JAF processing error: {e_proc}", exc_info=True)
        print(
            f"An unexpected error occurred during JAF processing: {e_proc}",
            file=sys.stderr,
        )
        sys.exit(1)


def handle_map_command(args):
    """Handles the 'map' subcommand for transforming data."""
    logger.debug(f"Map command with args: {args}")
    
    try:
        # Parse the expression
        expression_ast = smart_compile(args.expression)
        logger.debug(f"Compiled expression to AST: {expression_ast}")
    except (json.JSONDecodeError, DSLSyntaxError) as e:
        logger.error(f"Invalid expression: {e}")
        sys.exit(1)
    
    # Load input stream
    input_stream = _load_input_stream(args.input_source)
    
    # Apply map transformation
    result_stream = input_stream.map(expression_ast)
    
    if args.lazy:
        # Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))
    else:
        # Evaluate and output results
        _print_objects_as_jsonl(list(result_stream.evaluate()))


def handle_take_command(args):
    """Handles the 'take' subcommand."""
    logger.debug(f"Take command with args: {args}")
    
    # Load input stream
    input_stream = _load_input_stream(args.input_source)
    
    # Apply take operation
    result_stream = input_stream.take(args.count)
    
    if args.lazy:
        # Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))
    else:
        # Evaluate and output results
        _print_objects_as_jsonl(list(result_stream.evaluate()))


def handle_skip_command(args):
    """Handles the 'skip' subcommand."""
    logger.debug(f"Skip command with args: {args}")
    
    # Load input stream
    input_stream = _load_input_stream(args.input_source)
    
    # Apply skip operation
    result_stream = input_stream.skip(args.count)
    
    if args.lazy:
        # Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))
    else:
        # Evaluate and output results
        _print_objects_as_jsonl(list(result_stream.evaluate()))


def handle_batch_command(args):
    """Handles the 'batch' subcommand."""
    logger.debug(f"Batch command with args: {args}")
    
    # Load input stream
    input_stream = _load_input_stream(args.input_source)
    
    # Apply batch operation
    result_stream = input_stream.batch(args.size)
    
    if args.lazy:
        # Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))
    else:
        # Evaluate and output results
        _print_objects_as_jsonl(list(result_stream.evaluate()))


def _load_input_stream(input_source: str):
    """Helper to load a stream from various input sources."""
    if input_source == "-":
        # Read from stdin
        stdin_content = sys.stdin.read()
        
        # Try to parse as stream descriptor first
        try:
            stream_desc = json.loads(stdin_content)
            if isinstance(stream_desc, dict) and "collection_source" in stream_desc:
                # It's a stream descriptor
                return stream(stream_desc["collection_source"])
        except json.JSONDecodeError:
            pass
        
        # Otherwise treat as data
        loaded_objects, _ = load_objects_from_string(stdin_content)
        return stream({"type": "memory", "data": loaded_objects})
    
    elif os.path.isfile(input_source):
        # Check if it's a stream descriptor file
        if input_source.endswith(".json"):
            try:
                with open(input_source, "r") as f:
                    stream_desc = json.load(f)
                if isinstance(stream_desc, dict) and "collection_source" in stream_desc:
                    return stream(stream_desc["collection_source"])
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Otherwise load as data file
        return stream(input_source)
    
    elif os.path.isdir(input_source):
        # Create a directory source
        return stream({"type": "directory", "path": os.path.abspath(input_source)})
    
    else:
        print(f"Error: Input source '{input_source}' not found.", file=sys.stderr)
        sys.exit(1)


def handle_stream_command(args):
    """Handles the 'stream' subcommand for building pipelines."""
    logger.debug(f"Stream command with args: {args}")
    
    # Load input stream
    current_stream = _load_input_stream(args.input_source)
    
    # Track if we've entered lazy mode
    is_lazy = args.lazy
    
    # Build the pipeline by applying operations in order
    operations = []
    
    # Collect all operations with their types and arguments
    if args.filter:
        for query in args.filter:
            operations.append(('filter', query))
    
    if args.map:
        for expr in args.map:
            operations.append(('map', expr))
    
    if args.take is not None:
        operations.append(('take', args.take))
    
    if args.skip is not None:
        operations.append(('skip', args.skip))
    
    if args.batch is not None:
        operations.append(('batch', args.batch))
    
    if args.enumerate:
        operations.append(('enumerate', None))
    
    # Apply operations in the order they were specified
    # Note: argparse doesn't preserve the exact command line order for different argument types
    # For now, we apply in a fixed order: filter, map, take, skip, batch, enumerate
    # TODO: Consider using a custom action to preserve exact order
    
    for op_type, op_arg in operations:
        if op_type == 'filter':
            try:
                query_ast = smart_compile(op_arg)
                current_stream = current_stream.filter(query_ast)
            except (json.JSONDecodeError, DSLSyntaxError) as e:
                logger.error(f"Invalid filter query: {e}")
                sys.exit(1)
        
        elif op_type == 'map':
            try:
                expr_ast = smart_compile(op_arg)
                current_stream = current_stream.map(expr_ast)
            except (json.JSONDecodeError, DSLSyntaxError) as e:
                logger.error(f"Invalid map expression: {e}")
                sys.exit(1)
        
        elif op_type == 'take':
            current_stream = current_stream.take(op_arg)
        
        elif op_type == 'skip':
            current_stream = current_stream.skip(op_arg)
        
        elif op_type == 'batch':
            current_stream = current_stream.batch(op_arg)
        
        elif op_type == 'enumerate':
            current_stream = current_stream.enumerate()
    
    # Output based on lazy flag
    if is_lazy:
        # Output stream descriptor
        print(json.dumps(current_stream.to_dict(), separators=(",", ":")))
    else:
        # Evaluate and output results
        try:
            _print_objects_as_jsonl(list(current_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)


def handle_eval_command(args):
    """Handles the 'eval' subcommand to evaluate lazy streams."""
    logger.debug(f"Eval command with args: {args}")
    
    # Load the stream descriptor
    if args.input_source == "-":
        stdin_content = sys.stdin.read()
        try:
            stream_desc = json.loads(stdin_content)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON stream descriptor: {e}")
            sys.exit(1)
    else:
        try:
            with open(args.input_source, "r") as f:
                stream_desc = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error loading stream descriptor: {e}")
            sys.exit(1)
    
    # Create stream from descriptor
    if not isinstance(stream_desc, dict) or "collection_source" not in stream_desc:
        logger.error("Invalid stream descriptor: missing collection_source")
        sys.exit(1)
    
    try:
        data_stream = stream(stream_desc["collection_source"])
        _print_objects_as_jsonl(list(data_stream.evaluate()))
    except JAFError as e:
        logger.error(f"Error evaluating stream: {e}")
        sys.exit(1)


def handle_validate_command(args):
    """Handles the 'validate' subcommand."""
    logger.debug(f"Validate command with args: {args}")
    try:
        jrs = load_jaf_result_set_from_input(
            args.input_jrs, "input JafQuerySet for validate"
        )
        jrs.validate()
        print("JafQuerySet is valid.")
    except (JafQuerySetError, ValueError) as e:
        logger.error(f"Validation failed: {e}", exc_info=False)
        print(f"Validation failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during validation: {e}", exc_info=True
        )
        print(f"An unexpected error occurred during validation: {e}", file=sys.stderr)
        sys.exit(1)


def handle_info_command(args):
    """Handles the 'info' subcommand."""
    logger.debug(f"Info command with args: {args}")
    try:
        jrs = load_jaf_result_set_from_input(
            args.input_jrs, "input JafQuerySet for info"
        )

        print(f"JAF Result Set Summary")
        print(f"-----------------------")
        print(f"Collection ID:     {jrs.collection_id}")
        # Evaluate the query to get actual results for info display
        try:
            matching_objects = jrs.evaluate()
            print(f"Number of Matches: {len(matching_objects)}")
            print(f"Query: {jrs.query}")

            if matching_objects:
                print(f"Match Status: Found {len(matching_objects)} matching objects")
        except Exception as e:
            print(f"Error evaluating query: {e}")
            print(f"Query: {jrs.query}")
            return

        if jrs.collection_source:
            print(f"Collection Source:")
            source_type = jrs.collection_source.get("type", "N/A")
            source_path = jrs.collection_source.get("path", "N/A")
            print(f"  Type: {source_type}")
            print(f"  Path: {source_path}")
            if "files" in jrs.collection_source:
                print(f"  Files ({len(jrs.collection_source['files'])}):")
                for f in jrs.collection_source["files"][:5]:  # Print first 5
                    print(f"    - {f}")
                if len(jrs.collection_source["files"]) > 5:
                    print(
                        f"    ... and {len(jrs.collection_source['files']) - 5} more."
                    )

    except JafQuerySetError as e:
        logger.error(f"Error during info operation: {e}", exc_info=False)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during info operation: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


def handle_boolean_command(args):
    logger.debug(f"Boolean command '{args.command}' with args: {args}")

    final_result_set: Optional[JafQuerySet] = None
    rs1: Optional[JafQuerySet] = None
    rs2: Optional[JafQuerySet] = None

    try:
        if args.command == "not":
            path_for_rs1 = args.input_rs1
            rs1 = load_jaf_result_set_from_input(path_for_rs1, "input_rs1 for NOT")
            final_result_set = rs1.NOT()

        else:  # Handle binary operations (and, or, xor, difference)
            if args.query:
                # Mode 1: One JRS input, one query argument
                path_for_rs1 = args.input_rs1
                if args.input_rs2 is not None:
                    print(
                        f"Error: Cannot specify a second JafQuerySet path when using --query for '{args.command}'.",
                        file=sys.stderr,
                    )
                    sys.exit(1)

                rs1 = load_jaf_result_set_from_input(
                    path_for_rs1, f"input JafQuerySet for {args.command}"
                )

                if not rs1.collection_source:
                    print(
                        f"Error: Input JafQuerySet from '{path_for_rs1}' has no 'collection_source' and cannot be used with --query.",
                        file=sys.stderr,
                    )
                    sys.exit(1)

                try:
                    query_ast = smart_compile(args.query)
                except (json.JSONDecodeError, DSLSyntaxError) as e:
                    print(
                        f"Error: --query argument is neither valid JSON nor valid DSL: {e}",
                        file=sys.stderr,
                    )
                    sys.exit(1)

                logger.info(
                    f"Loading collection from source to apply new query: {rs1.collection_source}"
                )
                all_objects = load_collection(rs1.collection_source)
                rs2 = jaf(
                    all_objects,
                    query_ast,
                    collection_id=rs1.collection_id,
                    collection_source=rs1.collection_source,
                )

            else:
                # Mode 2: Two JRS inputs (original behavior)
                path_for_rs1_arg = args.input_rs1
                path_for_rs2_arg = args.input_rs2

                if path_for_rs2_arg is None:
                    if path_for_rs1_arg == "-":
                        print(
                            f"Error: {args.command.upper()} operation requires two inputs. Second input is missing (first is stdin).",
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    else:
                        actual_path_for_rs1 = "-"
                        actual_path_for_rs2 = path_for_rs1_arg
                else:
                    actual_path_for_rs1 = path_for_rs1_arg
                    actual_path_for_rs2 = path_for_rs2_arg

                if actual_path_for_rs1 == "-" and actual_path_for_rs2 == "-":
                    print(
                        f"Error: Cannot read both JafQuerySet inputs from stdin for {args.command.upper()} operation.",
                        file=sys.stderr,
                    )
                    sys.exit(1)

                rs1 = load_jaf_result_set_from_input(
                    actual_path_for_rs1, f"input_rs1 for {args.command.upper()}"
                )
                rs2 = load_jaf_result_set_from_input(
                    actual_path_for_rs2, f"input_rs2 for {args.command.upper()}"
                )

            # Perform the operation
            if args.command == "and":
                final_result_set = rs1.AND(rs2)
            elif args.command == "or":
                final_result_set = rs1.OR(rs2)
            elif args.command == "xor":
                final_result_set = rs1.XOR(rs2)
            elif args.command == "difference":
                final_result_set = rs1.SUBTRACT(rs2)

    except JafQuerySetError as e:
        logger.error(
            f"JafQuerySetError during boolean operation '{args.command}': {e}",
            exc_info=True,
        )
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:  # Catches ValueErrors from JRS.from_dict or our own checks
        logger.error(
            f"ValueError during boolean operation setup for '{args.command}': {e}",
            exc_info=True,
        )
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # Catch-all for other unexpected errors during the try block
        logger.error(
            f"Unexpected error during boolean operation '{args.command}': {e}",
            exc_info=True,
        )
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

    # If we reach here, an operation should have been performed and final_result_set assigned,
    # or an error should have caused an exit.
    if final_result_set is not None:
        output_dict = final_result_set.to_dict()
        # Always compact JSON for JafQuerySet output
        print(json.dumps(output_dict, indent=None, separators=(",", ":")))
    else:
        # This block is the fallback. If reached, it means final_result_set is None
        # without a prior exit, which indicates an unexpected issue in the logic above
        # (e.g., a new command added to dispatcher but not handled in the if/elif chain,
        # and the 'unhandled command' else block somehow failed) or in JRS methods
        # if they could return None (which they are not designed to do).
        logger.warning(
            f"final_result_set was None after command '{args.command}' without prior exit; "
            "attempting to generate a default empty JafQuerySet. This may indicate a logic flaw."
        )

        # Try to use rs1 or rs2 for metadata if they were loaded before an issue
        metadata_source_jrs: Optional[JafQuerySet] = rs1 if rs1 else rs2

        if metadata_source_jrs:
            logger.info(
                f"Using metadata from a previously loaded JafQuerySet (ID: {metadata_source_jrs.collection_id}, "
                f"ID: {metadata_source_jrs.collection_id}) for default empty result."
            )
            # Create empty result with a query that matches nothing
            empty_rs = JafQuerySet(
                query=[
                    "eq?",
                    ["@", [["key", "__never_matches__"]]],
                    "__impossible_value__",
                ],
                collection_id=metadata_source_jrs.collection_id,
                collection_source=metadata_source_jrs.collection_source,
            )
            output_dict = empty_rs.to_dict()
            # The --drop-filenames argument is obsolete and has been removed.
            print(json.dumps(output_dict, indent=None, separators=(",", ":")))
        else:
            # This is a more critical state: no metadata source to create even a default empty JRS.
            logger.error(
                "Cannot generate default empty JafQuerySet: No source JRS metadata available "
                "(rs1 and rs2 are None). This indicates a severe prior error or unhandled execution path."
            )
            print(
                "Error: Boolean operation failed critically, and context for a default empty result is missing.",
                file=sys.stderr,
            )
            sys.exit(1)


def handle_resolve_command(args):  # Renamed from handle_explode_command
    """Handles the 'resolve' subcommand."""
    logger.debug(f"Resolve command with args: {args}")
    try:
        input_jrs = load_jaf_result_set_from_input(
            args.input_jrs, "input JafQuerySet for resolve"
        )

        if args.output_indices:
            # For indices output, we need to evaluate and get indices
            matching_objects = input_jrs.evaluate()
            # This is a bit artificial since we don't track indices anymore
            print(json.dumps(list(range(len(matching_objects)))))
            return

        matching_objects = input_jrs.evaluate()

        # Note: We can no longer check if there were supposed to be matches
        # since we don't track indices anymore. This is expected with lazy evaluation.
        if not matching_objects:
            logger.warning(
                f"Resolve command: JafQuerySet has indices but no matching objects were ultimately retrieved. "
                f"Source: {input_jrs.collection_source or input_jrs.collection_id}"
            )

        # Determine the final list of results to be printed
        final_results = matching_objects  # Default case
        if args.extract_path:
            path_str = args.extract_path
            if path_str.startswith("@"):
                path_str = path_str[1:]
            try:
                path_ast = string_to_path_ast(path_str)
                query_ast = ["@", path_ast]
                final_results = [
                    jaf_eval.eval(query_ast, obj) for obj in matching_objects
                ]
            except PathSyntaxError as e:
                print(
                    f"Error: Invalid path string for --extract-path: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)

        elif args.apply_query:
            try:
                query_ast = json.loads(args.apply_query)
                final_results = [
                    jaf_eval.eval(query_ast, obj) for obj in matching_objects
                ]
            except json.JSONDecodeError:
                print(
                    f"Error: --apply-query argument is not valid JSON: {args.apply_query}",
                    file=sys.stderr,
                )
                sys.exit(1)

        # Format and print the final results
        if args.output_json_array:
            print(json.dumps(final_results, indent=2))
        else:  # Default is JSONL
            _print_objects_as_jsonl(final_results)

    except JafQuerySetError as e:
        logger.error(f"Error during resolve operation: {e}", exc_info=False)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except (ValueError, jafError) as e:
        logger.error(f"ValueError during resolve operation setup: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during resolve operation: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


def load_jaf_result_set_from_input(input_spec: str, arg_name: str) -> JafQuerySet:
    """Loads a JafQuerySet from a file path or stdin."""
    data_str = ""
    source_description = ""
    if input_spec == "-":
        logger.info(f"Reading JafQuerySet for {arg_name} from stdin.")
        source_description = "stdin"
        try:
            data_str = sys.stdin.read()
        except Exception as e:
            logger.error(f"Error reading JafQuerySet from stdin: {e}", exc_info=True)
            print(f"Error: Could not read JafQuerySet from stdin. {e}", file=sys.stderr)
            sys.exit(1)
    else:
        logger.info(f"Reading JafQuerySet for {arg_name} from file: {input_spec}")
        source_description = input_spec
        try:
            with open(input_spec, "r", encoding="utf-8") as f:
                data_str = f.read()
        except FileNotFoundError:
            logger.error(f"JafQuerySet file not found: {input_spec}")
            print(f"Error: JafQuerySet file '{input_spec}' not found.", file=sys.stderr)
            sys.exit(1)
        except IOError as e:
            logger.error(
                f"IOError reading JafQuerySet file {input_spec}: {e}", exc_info=True
            )
            print(
                f"Error: Could not read JafQuerySet file '{input_spec}'. {e}",
                file=sys.stderr,
            )
            sys.exit(1)

    if not data_str.strip():
        logger.error(f"No data received for JafQuerySet from {source_description}.")
        print(
            f"Error: No data provided for JafQuerySet from {source_description}.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        data_dict = json.loads(data_str)
        return JafQuerySet.from_dict(data_dict)
    except json.JSONDecodeError as e:
        logger.error(
            f"Invalid JSON for JafQuerySet from {source_description}: {e}",
            exc_info=True,
        )
        print(
            f"Error: JafQuerySet input from {source_description} is not valid JSON. {e}",
            file=sys.stderr,
        )
        sys.exit(1)
    except (TypeError, ValueError, KeyError) as e:  # Errors from JafQuerySet.from_dict
        logger.error(
            f"Invalid JafQuerySet structure from {source_description}: {e}",
            exc_info=True,
        )
        print(
            f"Error: JafQuerySet input from {source_description} has invalid structure. {e}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

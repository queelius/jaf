import os
import argparse
import json
import logging
import sys
from typing import List, Dict, Optional, Union, Any
from .lazy_streams import stream, LazyDataStream, FilteredStream, MappedStream
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


def add_source_arguments(parser):
    """Add common source-related arguments to a parser."""
    # Directory source options
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively scan directories (only applies to directory sources).",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        help="File pattern to match (e.g., '*.jsonl') when scanning directories.",
    )
    # CSV source options
    parser.add_argument(
        "--delimiter",
        type=str,
        help="CSV delimiter character (default: comma).",
    )
    parser.add_argument(
        "--headers",
        action="store_true",
        default=None,
        help="CSV has headers (default: auto-detect).",
    )
    parser.add_argument(
        "--no-headers",
        action="store_true",
        help="CSV does not have headers.",
    )


def main():
    parser = argparse.ArgumentParser(
        description="JAF: JSON Array Filter and Stream Processing.",
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
        help="The JAF query to apply (JSON or string format).",
    )
    filter_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )
    add_source_arguments(filter_parser)

    # --- 'map' Subcommand ---
    map_parser = subparsers.add_parser(
        "map", help="Transform JSON/JSONL data using a JAF expression."
    )
    map_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file or directory. Defaults to '-' to read from stdin.",
    )
    map_parser.add_argument(
        "expression",
        type=str,
        help="The JAF expression to apply to each item.",
    )
    map_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )
    add_source_arguments(map_parser)

    # --- 'take' Subcommand ---
    take_parser = subparsers.add_parser(
        "take", help="Take the first N items from a stream."
    )
    take_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor. Defaults to '-' to read from stdin.",
    )
    take_parser.add_argument(
        "count",
        type=int,
        help="Number of items to take.",
    )
    take_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )
    add_source_arguments(take_parser)

    # --- 'skip' Subcommand ---
    skip_parser = subparsers.add_parser(
        "skip", help="Skip the first N items from a stream."
    )
    skip_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor. Defaults to '-' to read from stdin.",
    )
    skip_parser.add_argument(
        "count",
        type=int,
        help="Number of items to skip.",
    )
    skip_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )
    add_source_arguments(skip_parser)

    # --- 'batch' Subcommand ---
    batch_parser = subparsers.add_parser(
        "batch", help="Group items from a stream into batches."
    )
    batch_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the input JSON/JSONL file, directory, or stream descriptor. Defaults to '-' to read from stdin.",
    )
    batch_parser.add_argument(
        "size",
        type=int,
        help="Batch size.",
    )
    batch_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )
    add_source_arguments(batch_parser)

    # --- 'stream' Subcommand ---
    stream_parser = subparsers.add_parser(
        "stream",
        help="Build and execute a stream processing pipeline with chained operations.",
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
        "--filter",
        "-f",
        action="append",
        metavar="QUERY",
        help="Filter with a JAF query. Can be used multiple times.",
    )
    stream_parser.add_argument(
        "--map",
        "-m",
        action="append",
        metavar="EXPR",
        help="Transform with a JAF expression. Can be used multiple times.",
    )
    stream_parser.add_argument(
        "--take",
        "-t",
        type=int,
        metavar="N",
        help="Take the first N items.",
    )
    stream_parser.add_argument(
        "--skip",
        "-s",
        type=int,
        metavar="N",
        help="Skip the first N items.",
    )
    stream_parser.add_argument(
        "--batch",
        "-b",
        type=int,
        metavar="SIZE",
        help="Group items into batches of SIZE.",
    )
    stream_parser.add_argument(
        "--enumerate",
        "-e",
        action="store_true",
        help="Add index to each item.",
    )
    stream_parser.add_argument(
        "--lazy",
        "-l",
        action="store_true",
        help="Output stream descriptor JSON instead of evaluating (default is to evaluate).",
    )
    add_source_arguments(stream_parser)

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
    add_source_arguments(eval_parser)

    # --- Boolean Operations ---
    # 'and' Subcommand
    and_parser = subparsers.add_parser(
        "and", help="Perform logical AND on two filtered streams."
    )
    and_parser.add_argument(
        "left",
        type=str,
        help="Path to the first filtered stream or '-' for stdin.",
    )
    and_parser.add_argument(
        "right",
        type=str,
        help="Path to the second filtered stream.",
    )
    and_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )

    # 'or' Subcommand
    or_parser = subparsers.add_parser(
        "or", help="Perform logical OR on two filtered streams."
    )
    or_parser.add_argument(
        "left",
        type=str,
        help="Path to the first filtered stream or '-' for stdin.",
    )
    or_parser.add_argument(
        "right",
        type=str,
        help="Path to the second filtered stream.",
    )
    or_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )

    # 'not' Subcommand
    not_parser = subparsers.add_parser(
        "not", help="Perform logical NOT on a filtered stream."
    )
    not_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to the filtered stream or '-' for stdin.",
    )
    not_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )

    # 'xor' Subcommand
    xor_parser = subparsers.add_parser(
        "xor", help="Perform logical XOR (exclusive OR) on two filtered streams."
    )
    xor_parser.add_argument(
        "left",
        type=str,
        help="Path to the first filtered stream or '-' for stdin.",
    )
    xor_parser.add_argument(
        "right",
        type=str,
        help="Path to the second filtered stream.",
    )
    xor_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )

    # 'difference' Subcommand
    difference_parser = subparsers.add_parser(
        "difference",
        help="Perform logical DIFFERENCE (left AND NOT right) on two filtered streams.",
    )
    difference_parser.add_argument(
        "left",
        type=str,
        help="Path to the first filtered stream or '-' for stdin.",
    )
    difference_parser.add_argument(
        "right",
        type=str,
        help="Path to the second filtered stream to subtract.",
    )
    difference_parser.add_argument(
        "--eval",
        action="store_true",
        help="Evaluate and output results (default: output stream descriptor).",
    )

    # --- 'info' Subcommand ---
    info_parser = subparsers.add_parser(
        "info", help="Display information about a stream without evaluating it."
    )
    info_parser.add_argument(
        "input_source",
        type=str,
        nargs="?",
        default="-",
        help="Path to stream descriptor JSON file or '-' for stdin.",
    )
    add_source_arguments(info_parser)

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.debug(f"Starting JAF with command: {args.command}")

    # Dispatch based on command
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
    elif args.command == "and":
        handle_and_command(args)
    elif args.command == "or":
        handle_or_command(args)
    elif args.command == "not":
        handle_not_command(args)
    elif args.command == "xor":
        handle_xor_command(args)
    elif args.command == "difference":
        handle_difference_command(args)
    elif args.command == "info":
        handle_info_command(args)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


def handle_filter_command(args):
    """Handles the 'filter' subcommand."""
    logger.debug(f"Filter command with args: {args}")

    # Load the data stream
    data_stream = _load_input_stream(args.input_source, args)

    # Parse the query
    try:
        query_ast = smart_compile(args.query)
    except (json.JSONDecodeError, DSLSyntaxError) as e:
        logger.error(f"Invalid query: {e}")
        sys.exit(1)

    # Apply filter
    filtered_stream = data_stream.filter(query_ast)

    if args.eval:
        # Evaluate and output results
        try:
            _print_objects_as_jsonl(list(filtered_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating filter: {e}")
            sys.exit(1)
    else:
        # Default: Output stream descriptor
        print(json.dumps(filtered_stream.to_dict(), separators=(",", ":")))


def handle_map_command(args):
    """Handles the 'map' subcommand."""
    logger.debug(f"Map command with args: {args}")

    # Load the data stream
    data_stream = _load_input_stream(args.input_source, args)

    # Parse the expression
    try:
        expr_ast = smart_compile(args.expression)
    except (json.JSONDecodeError, DSLSyntaxError) as e:
        logger.error(f"Invalid expression: {e}")
        sys.exit(1)

    # Apply map
    mapped_stream = data_stream.map(expr_ast)

    if args.eval:
        # Evaluate and output results
        try:
            _print_objects_as_jsonl(list(mapped_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating map: {e}")
            sys.exit(1)
    else:
        # Default: Output stream descriptor
        print(json.dumps(mapped_stream.to_dict(), separators=(",", ":")))


def handle_take_command(args):
    """Handles the 'take' subcommand."""
    logger.debug(f"Take command with args: {args}")

    # Load input stream
    input_stream = _load_input_stream(args.input_source, args)

    # Apply take operation
    result_stream = input_stream.take(args.count)

    if args.eval:
        # Evaluate and output results
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        # Default: Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_skip_command(args):
    """Handles the 'skip' subcommand."""
    logger.debug(f"Skip command with args: {args}")

    # Load input stream
    input_stream = _load_input_stream(args.input_source, args)

    # Apply skip operation
    result_stream = input_stream.skip(args.count)

    if args.eval:
        # Evaluate and output results
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        # Default: Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_batch_command(args):
    """Handles the 'batch' subcommand."""
    logger.debug(f"Batch command with args: {args}")

    # Load input stream
    input_stream = _load_input_stream(args.input_source, args)

    # Apply batch operation
    result_stream = input_stream.batch(args.size)

    if args.eval:
        # Evaluate and output results
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        # Default: Output stream descriptor
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def _load_input_stream(input_source: str, args=None):
    """Helper to load a stream from various input sources.
    
    Args:
        input_source: Path to file/directory or '-' for stdin
        args: Optional argparse namespace with source configuration flags
    """
    if input_source == "-":
        # Read from stdin
        stdin_content = sys.stdin.read()

        # Try to parse as stream descriptor first
        try:
            stream_desc = json.loads(stdin_content)
            if isinstance(stream_desc, dict) and "collection_source" in stream_desc:
                # It's a stream descriptor - check for stream type
                return _reconstruct_stream(stream_desc)
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
                    return _reconstruct_stream(stream_desc)
            except (json.JSONDecodeError, KeyError):
                pass

        # Build source configuration with any CSV options
        # Always treat .tsv files as CSV with tab delimiter
        if args and (input_source.endswith((".csv", ".tsv")) or (
            (hasattr(args, "delimiter") and args.delimiter) or
            (hasattr(args, "headers") and args.headers) or
            (hasattr(args, "no_headers") and args.no_headers)
        )):
            # Build CSV wrapper with custom options
            csv_config = {
                "type": "csv",
                "inner_source": {"type": "file", "path": input_source}
            }
            
            # Set delimiter - default to tab for .tsv files
            if hasattr(args, "delimiter") and args.delimiter:
                csv_config["delimiter"] = args.delimiter
            elif input_source.endswith(".tsv"):
                csv_config["delimiter"] = "\t"
            
            if hasattr(args, "headers") and args.headers:
                csv_config["has_header"] = True
            elif hasattr(args, "no_headers") and args.no_headers:
                csv_config["has_header"] = False
            
            return stream(**csv_config)
        
        # Otherwise load as regular data file
        return stream(input_source)

    elif os.path.isdir(input_source):
        # Create a directory source with optional pattern/recursive
        source_config = {"type": "directory", "path": os.path.abspath(input_source)}
        
        if args:
            if hasattr(args, "recursive") and args.recursive:
                source_config["recursive"] = True
            if hasattr(args, "pattern") and args.pattern:
                source_config["pattern"] = args.pattern
        
        return stream(**source_config)

    else:
        print(f"Error: Input source '{input_source}' not found.", file=sys.stderr)
        sys.exit(1)


def _reconstruct_stream(stream_desc: Dict[str, Any]) -> LazyDataStream:
    """Reconstruct the appropriate stream type from a descriptor."""
    stream_type = stream_desc.get("stream_type", "LazyDataStream")

    if stream_type == "FilteredStream" and "query" in stream_desc:
        # Reconstruct FilteredStream
        inner_source = stream_desc["collection_source"].get("inner_source")
        if inner_source:
            base_stream = stream(inner_source)
            return FilteredStream(stream_desc["query"], base_stream)

    elif stream_type == "MappedStream" and "expression" in stream_desc:
        # Reconstruct MappedStream
        inner_source = stream_desc["collection_source"].get("inner_source")
        if inner_source:
            base_stream = stream(inner_source)
            return MappedStream(stream_desc["expression"], base_stream)

    # Default: create stream from collection_source
    return stream(stream_desc["collection_source"])


def handle_stream_command(args):
    """Handles the 'stream' subcommand for building pipelines."""
    logger.debug(f"Stream command with args: {args}")

    # Load input stream
    current_stream = _load_input_stream(args.input_source, args)

    # Track if we've entered lazy mode
    is_lazy = args.lazy

    # Build the pipeline by applying operations in order
    operations = []

    # Collect all operations with their types and arguments
    if args.filter:
        for query in args.filter:
            operations.append(("filter", query))

    if args.map:
        for expr in args.map:
            operations.append(("map", expr))

    if args.take is not None:
        operations.append(("take", args.take))

    if args.skip is not None:
        operations.append(("skip", args.skip))

    if args.batch is not None:
        operations.append(("batch", args.batch))

    if args.enumerate:
        operations.append(("enumerate", None))

    # Apply operations in the order they were specified
    # Note: argparse doesn't preserve the exact command line order for different argument types
    # For now, we apply in a fixed order: filter, map, take, skip, batch, enumerate
    # TODO: Consider using a custom action to preserve exact order

    for op_type, op_arg in operations:
        if op_type == "filter":
            try:
                query_ast = smart_compile(op_arg)
                current_stream = current_stream.filter(query_ast)
            except (json.JSONDecodeError, DSLSyntaxError) as e:
                logger.error(f"Invalid filter query: {e}")
                sys.exit(1)

        elif op_type == "map":
            try:
                expr_ast = smart_compile(op_arg)
                current_stream = current_stream.map(expr_ast)
            except (json.JSONDecodeError, DSLSyntaxError) as e:
                logger.error(f"Invalid map expression: {e}")
                sys.exit(1)

        elif op_type == "take":
            current_stream = current_stream.take(op_arg)

        elif op_type == "skip":
            current_stream = current_stream.skip(op_arg)

        elif op_type == "batch":
            current_stream = current_stream.batch(op_arg)

        elif op_type == "enumerate":
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


def handle_and_command(args):
    """Handles the 'and' subcommand for logical AND of filtered streams."""
    logger.debug(f"AND command with args: {args}")

    # Load both streams
    left_stream = _load_filtered_stream(args.left)
    right_stream = _load_filtered_stream(args.right)

    # Perform AND operation
    result_stream = left_stream.AND(right_stream)

    if args.eval:
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_or_command(args):
    """Handles the 'or' subcommand for logical OR of filtered streams."""
    logger.debug(f"OR command with args: {args}")

    # Load both streams
    left_stream = _load_filtered_stream(args.left)
    right_stream = _load_filtered_stream(args.right)

    # Perform OR operation
    result_stream = left_stream.OR(right_stream)

    if args.eval:
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_not_command(args):
    """Handles the 'not' subcommand for logical NOT of a filtered stream."""
    logger.debug(f"NOT command with args: {args}")

    # Load the stream
    input_stream = _load_filtered_stream(args.input_source)

    # Perform NOT operation
    result_stream = input_stream.NOT()

    if args.eval:
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_xor_command(args):
    """Handles the 'xor' subcommand for logical XOR of filtered streams."""
    logger.debug(f"XOR command with args: {args}")

    # Load both streams
    left_stream = _load_filtered_stream(args.left)
    right_stream = _load_filtered_stream(args.right)

    # Perform XOR operation
    result_stream = left_stream.XOR(right_stream)

    if args.eval:
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_difference_command(args):
    """Handles the 'difference' subcommand for logical DIFFERENCE of filtered streams."""
    logger.debug(f"DIFFERENCE command with args: {args}")

    # Load both streams
    left_stream = _load_filtered_stream(args.left)
    right_stream = _load_filtered_stream(args.right)

    # Perform DIFFERENCE operation
    result_stream = left_stream.DIFFERENCE(right_stream)

    if args.eval:
        try:
            _print_objects_as_jsonl(list(result_stream.evaluate()))
        except JAFError as e:
            logger.error(f"Error evaluating stream: {e}")
            sys.exit(1)
    else:
        print(json.dumps(result_stream.to_dict(), separators=(",", ":")))


def handle_info_command(args):
    """Handles the 'info' subcommand to display stream information."""
    logger.debug(f"Info command with args: {args}")

    # Load the stream
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
        info = data_stream.info()

        # Pretty print the info
        print(json.dumps(info, indent=2))
    except Exception as e:
        logger.error(f"Error getting stream info: {e}")
        sys.exit(1)


def _load_filtered_stream(input_source: str) -> FilteredStream:
    """Load a stream and ensure it's a FilteredStream."""
    data_stream = _load_input_stream(input_source)

    # Check if it's already a filtered stream
    if isinstance(data_stream, FilteredStream):
        return data_stream

    # Otherwise, we need to error - boolean operations only work on filtered streams
    logger.error("Boolean operations require filtered streams as input")
    sys.exit(1)


def _print_objects_as_jsonl(objects: List[Any]) -> None:
    """Helper to print objects as JSONL."""
    for obj in objects:
        try:
            print(json.dumps(obj, separators=(",", ":")))
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing object to JSON: {e}")
            logger.debug(f"Object that failed to serialize: {obj}")


if __name__ == "__main__":
    main()

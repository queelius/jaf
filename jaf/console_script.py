import os
import argparse
import json
import logging
import sys
from typing import List, Dict, Optional, Union, Any
from .jaf import jaf, jafError
from .result_set import JafResultSet, JafResultSetError
# Import from the new io_utils module
from .io_utils import walk_data_files, load_objects_from_file

logger = logging.getLogger(__name__)

# load_objects_from_file and walk_data_files are now imported from io_utils

def main():
    parser = argparse.ArgumentParser(
        description="JAF: JSON Array Filter and Result Set Operations.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level."
    )
    # --pretty-print argument removed
    parser.add_argument(
        "--drop-filenames",
        action="store_true",
        help="For boolean operations, omit 'filenames_in_collection' from the output JafResultSet JSON."
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-command to execute")

    # --- 'filter' Subcommand ---
    filter_parser = subparsers.add_parser("filter", help="Filter JSON/JSONL data using a JAF query.")
    filter_parser.add_argument(
        "input_source",
        type=str,
        help="Path to a JSON/JSONL file or a directory."
    )
    filter_parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="JAF query string (JSON AST)."
    )
    filter_parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively search directories."
    )
    filter_parser.add_argument(
        "--collection-id",
        type=str,
        default=None,
        help="Optional ID for the data collection (used in JafResultSet)."
    )
    filter_parser.add_argument(
        "--resolve", # Renamed from --output-matches
        action="store_true",
        help="Resolve and print actual matching JSON objects (JSONL) instead of JafResultSet JSON."
    )

    # --- Boolean Algebra Subcommands ---
    # Helper to add common args for boolean ops
    def add_boolean_op_args(p, num_inputs=2):
        if num_inputs == 1: # For unary operations like NOT
            p.add_argument(
                "input_rs1",
                type=str,
                nargs='?',        # Makes it optional on the command line
                default="-",      # Defaults to stdin if not explicitly provided
                help="Path to the JafResultSet JSON file, or '-' for stdin. Defaults to stdin if no path is given."
            )
        elif num_inputs >= 2: # For binary operations like AND, OR
            p.add_argument(
                "input_rs1",
                type=str,
                nargs='?',        # Make optional
                default="-",      # Default to stdin. If a single path is given, it's captured by input_rs2 if input_rs1 remains '-'
                help="Path to the first JafResultSet or '-'. If only one path is given after the command (e.g. 'jaf or file.txt'), this defaults to stdin and 'file.txt' becomes the second input."
            )
            # Ensure input_rs2 is only added if num_inputs expects it.
            # For binary ops, input_rs2 captures the second explicit path, or the first if input_rs1 was stdin.
            p.add_argument(
                "input_rs2",
                type=str,
                nargs='?',        # Make optional
                default=None,     # No file if not provided as a second argument
                help="Path to the second JafResultSet. Used if two file paths are specified, or if one is specified and the first input is stdin."
            )

    and_parser = subparsers.add_parser("and", help="Perform logical AND on two JafResultSets.")
    add_boolean_op_args(and_parser, 2)

    or_parser = subparsers.add_parser("or", help="Perform logical OR on two JafResultSets.")
    add_boolean_op_args(or_parser, 2)

    not_parser = subparsers.add_parser("not", help="Perform logical NOT on a JafResultSet.")
    add_boolean_op_args(not_parser, 1)

    xor_parser = subparsers.add_parser("xor", help="Perform logical XOR on two JafResultSets.")
    add_boolean_op_args(xor_parser, 2)

    diff_parser = subparsers.add_parser("difference", help="Perform logical DIFFERENCE (rs1 - rs2) on two JafResultSets.")
    add_boolean_op_args(diff_parser, 2)

    # --- 'resolve' Subcommand ---
    resolve_parser = subparsers.add_parser(
        "resolve", 
        help="Resolve a JafResultSet to original JSON objects (JSONL). Reads JRS from stdin or file."
    )
    resolve_parser.add_argument(
        "input_jrs",
        type=str,
        nargs='?',
        default="-",
        help="Path to the JafResultSet JSON file, or '-' for stdin. Defaults to stdin if no path is given."
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # --- Command Dispatch ---
    if args.command == "filter":
        handle_filter_command(args)
    elif args.command in ["and", "or", "not", "xor", "difference"]:
        handle_boolean_command(args)
    elif args.command == "resolve":
        handle_resolve_command(args)
    else:
        parser.print_help()
        sys.exit(1)

# Helper function to print objects as JSONL to stdout
def _print_objects_as_jsonl(objects: List[Any]):
    """Prints a list of objects as JSONL, one compact JSON per line."""
    for obj in objects:
        # Compact JSON: no indent, no spaces after separators
        print(json.dumps(obj, indent=None, separators=(',', ':')))

def handle_filter_command(args):
    logger.debug(f"Filter command with args: {args}\n")
    try:
        logger.debug(f"Parsing JAF query: {args.query}\n")
        query_ast = json.loads(args.query)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JAF query JSON: {e}\n")
        print(f"Error: JAF query is not valid JSON. {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(query_ast, list) and query_ast is not None:
        logger.error(f"Invalid query AST type: {type(query_ast)}")
        print(f"Error: Query AST is not a list or null: {type(query_ast)}", file=sys.stderr)
        sys.exit(1)

    input_is_directory = os.path.isdir(args.input_source)
    all_objects_to_filter: List[Any] = []
    file_paths_contributing: List[str] = [] # Stores abspaths of files that contributed objects
    effective_collection_id: Optional[Any] = args.collection_id

    if input_is_directory:
        logger.info(f"Input source is a directory: {args.input_source}. Aggregating content.")
        discovered_files = walk_data_files(args.input_source, recursive=args.recursive)
        if not discovered_files:
            print(f"No .json or .jsonl files found in directory {args.input_source}", file=sys.stderr)
            sys.exit(0) # No files to process is not an error for filter.

        for file_path in discovered_files:
            logger.debug(f"Loading objects from file: {file_path}")
            objects_from_single_file = load_objects_from_file(file_path)
            if objects_from_single_file is not None: # Check for None (critical load error)
                all_objects_to_filter.extend(objects_from_single_file)
                if objects_from_single_file: # Only add to contributing if it actually added objects
                    file_paths_contributing.append(os.path.abspath(file_path))
                elif not objects_from_single_file and os.path.exists(file_path): # File exists, was valid (e.g. empty JSON array), but no objects
                    file_paths_contributing.append(os.path.abspath(file_path)) # Still counts as part of collection
            else:
                logger.warning(f"Skipping file {file_path} due to loading errors or it was empty and returned None.")
        
        if not effective_collection_id: # If user didn't provide a collection ID
            effective_collection_id = os.path.abspath(args.input_source) # Default to directory path

    elif os.path.isfile(args.input_source):
        if args.input_source.endswith((".json", ".jsonl")):
            logger.info(f"Input source is a single file: {args.input_source}")
            loaded_objects = load_objects_from_file(args.input_source)
            if loaded_objects is not None: # Successfully loaded (could be empty list)
                all_objects_to_filter = loaded_objects
                file_paths_contributing.append(os.path.abspath(args.input_source))
            # If loaded_objects is None, all_objects_to_filter remains empty.
            
            if not effective_collection_id: # If user didn't provide a collection ID
                effective_collection_id = os.path.abspath(args.input_source) # Default to file path
        else:
            print(f"Error: Input file '{args.input_source}' is not a .json or .jsonl file.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: Input source '{args.input_source}' is not a valid file or directory.", file=sys.stderr)
        sys.exit(1)

    if not all_objects_to_filter and not file_paths_contributing:
        # This means no files were successfully processed at all or input source was invalid initially.
        # Error messages for invalid source are handled above.
        # If files were found but all failed to load or were empty in a way that load_objects_from_file returned None for all.
        logger.info(f"No data could be loaded or no filterable JSON values found in the provided source(s): {args.input_source}")
        # For filter command, producing an empty result set for zero items is valid.
        # So, we proceed to jaf() which will return an empty JafResultSet.
    
    # If all_objects_to_filter is empty, jaf() will correctly produce a JafResultSet with empty indices and collection_size=0.

    try:
        logger.info(f"Applying JAF query to {len(all_objects_to_filter)} aggregated items. Collection ID: {effective_collection_id}")
        result_set = jaf(all_objects_to_filter, query_ast, collection_id=effective_collection_id)

        if args.resolve: # Renamed from args.output_matches
            matched_data = [all_objects_to_filter[i] for i in result_set.indices]
            
            if logger.isEnabledFor(logging.INFO):
                print(f"--- Filtering Complete (Resolving Matches) ---", file=sys.stderr)
                print(f"Summary: Matched {len(matched_data)} item(s) out of {len(all_objects_to_filter)}.", file=sys.stderr)
                if input_is_directory:
                     print(f"Source Directory: {os.path.abspath(args.input_source)}", file=sys.stderr)
                     print(f"Files contributing to collection ({len(file_paths_contributing)}): {', '.join(sorted(list(set(file_paths_contributing))))}", file=sys.stderr)
                elif file_paths_contributing: # Single file case
                     print(f"Source File: {file_paths_contributing[0]}", file=sys.stderr)
                print(f"--- Matches (JSONL to stdout) ---", file=sys.stderr)

            _print_objects_as_jsonl(matched_data)
        else:
            output_dict = result_set.to_dict()
            if file_paths_contributing:
                output_dict["filenames_in_collection"] = sorted(list(set(file_paths_contributing)))
            
            # Always compact JSON for JafResultSet output
            print(json.dumps(output_dict, indent=None, separators=(',', ':')))

    except jafError as e_jaf:
        logger.error(f"JAF Error processing: {e_jaf}", exc_info=True)
        print(f"JAF Error: {e_jaf}", file=sys.stderr)
        sys.exit(1)
    except Exception as e_proc: # Catch any other unexpected errors
        logger.error(f"Unexpected JAF processing error: {e_proc}", exc_info=True)
        print(f"An unexpected error occurred during JAF processing: {e_proc}", file=sys.stderr)
        sys.exit(1)

def handle_boolean_command(args):
    logger.debug(f"Boolean command '{args.command}' with args: {args}")

    final_result_set: Optional[JafResultSet] = None
    rs1: Optional[JafResultSet] = None
    rs2: Optional[JafResultSet] = None
    # path_for_rs1 and path_for_rs2 will be determined within each command block

    try:
        if args.command == "not":
            path_for_rs1 = args.input_rs1 # Defaults to '-' via argparse
            rs1 = load_jaf_result_set_from_input(path_for_rs1, "input_rs1 for NOT")
            final_result_set = rs1.NOT()
        
        elif args.command == "and":
            path_for_rs1_arg = args.input_rs1 # Default '-'
            path_for_rs2_arg = args.input_rs2 # Default None

            if path_for_rs2_arg is None:
                if path_for_rs1_arg == '-': # e.g. "... | jaf and" or "jaf and"
                    print("Error: AND operation requires two inputs. Second input is missing (first is stdin).", file=sys.stderr)
                    sys.exit(1)
                else: # e.g. "... | jaf and file.txt" or "jaf and file.txt"
                    actual_path_for_rs1 = "-"
                    actual_path_for_rs2 = path_for_rs1_arg
            else: # Both input_rs1 and input_rs2 from argparse have values
                  # e.g., "jaf and file1.txt file2.txt" or "... | jaf and file2.txt" (input_rs1='-', input_rs2='file2.txt')
                actual_path_for_rs1 = path_for_rs1_arg
                actual_path_for_rs2 = path_for_rs2_arg
            
            if actual_path_for_rs1 == "-" and actual_path_for_rs2 == "-":
                print("Error: Cannot read both JafResultSet inputs from stdin for AND operation.", file=sys.stderr)
                sys.exit(1)

            rs1 = load_jaf_result_set_from_input(actual_path_for_rs1, "input_rs1 for AND")
            rs2 = load_jaf_result_set_from_input(actual_path_for_rs2, "input_rs2 for AND")
            final_result_set = rs1.AND(rs2)

        elif args.command == "or":
            path_for_rs1_arg = args.input_rs1 # Default '-'
            path_for_rs2_arg = args.input_rs2 # Default None

            if path_for_rs2_arg is None:
                if path_for_rs1_arg == '-': # e.g. "... | jaf or" or "jaf or"
                    print("Error: OR operation requires two inputs. Second input is missing (first is stdin).", file=sys.stderr)
                    sys.exit(1)
                else: # e.g. "... | jaf or file.txt" or "jaf or file.txt"
                    actual_path_for_rs1 = "-"
                    actual_path_for_rs2 = path_for_rs1_arg
            else: # Both input_rs1 and input_rs2 from argparse have values
                actual_path_for_rs1 = path_for_rs1_arg
                actual_path_for_rs2 = path_for_rs2_arg

            if actual_path_for_rs1 == "-" and actual_path_for_rs2 == "-":
                print("Error: Cannot read both JafResultSet inputs from stdin for OR operation.", file=sys.stderr)
                sys.exit(1)
                
            rs1 = load_jaf_result_set_from_input(actual_path_for_rs1, "input_rs1 for OR")
            rs2 = load_jaf_result_set_from_input(actual_path_for_rs2, "input_rs2 for OR")
            final_result_set = rs1.OR(rs2)
        
        # Add elif blocks for other boolean commands like XOR, SUBTRACT here if they are implemented
        # For example:
        # elif args.command == "xor":
        #     # ... similar logic for loading two inputs ...
        #     final_result_set = rs1.XOR(rs2)

        else:
            # This 'else' is for commands that are in the main dispatcher's list 
            # for handle_boolean_command (e.g., if ["and", "or", "not", "xor"] was used in main)
            # but are not explicitly handled by an if/elif args.command == "..." above.
            # Given the main dispatcher currently only sends "and", "or", "not",
            # and all are handled, this else block should ideally be unreachable.
            # It's a safeguard for future expansion if the dispatcher list changes
            # and this function isn't updated accordingly.
            logger.error(f"Internal error: Unhandled boolean command '{args.command}' in handle_boolean_command.")
            print(f"Error: Unhandled boolean command '{args.command}'. This is an internal error.", file=sys.stderr)
            sys.exit(1) # Critical internal error
            
    except JafResultSetError as e: 
        logger.error(f"Error during boolean operation '{args.command}': {e}", exc_info=False) 
        print(f"Error: {e}", file=sys.stderr) 
        sys.exit(1)
    except ValueError as e: # Catches ValueErrors from JRS.from_dict or our own checks
        logger.error(f"ValueError during boolean operation setup for '{args.command}': {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e: # Catch-all for other unexpected errors during the try block
        logger.error(f"Unexpected error during boolean operation '{args.command}': {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

    # If we reach here, an operation should have been performed and final_result_set assigned,
    # or an error should have caused an exit.
    if final_result_set is not None:
        output_dict = final_result_set.to_dict()
        if args.drop_filenames: 
            output_dict.pop("filenames_in_collection", None)
        
        # Always compact JSON for JafResultSet output
        print(json.dumps(output_dict, indent=None, separators=(',', ':')))
    else:
        # This block is the fallback. If reached, it means final_result_set is None 
        # without a prior exit, which indicates an unexpected issue in the logic above 
        # (e.g., a new command added to dispatcher but not handled in the if/elif chain,
        # and the 'unhandled command' else block somehow failed) or in JRS methods
        # if they could return None (which they are not designed to do).
        logger.warning(
            f"final_result_set was None after command '{args.command}' without prior exit; "
            "attempting to generate a default empty JafResultSet. This may indicate a logic flaw."
        )
        
        # Try to use rs1 or rs2 for metadata if they were loaded before an issue
        metadata_source_jrs: Optional[JafResultSet] = rs1 if rs1 else rs2
        
        if metadata_source_jrs:
            logger.info(
                f"Using metadata from a previously loaded JafResultSet (ID: {metadata_source_jrs.collection_id}, "
                f"Size: {metadata_source_jrs.collection_size}) for default empty result."
            )
            empty_rs = JafResultSet(
                indices=[], 
                collection_size=metadata_source_jrs.collection_size,
                collection_id=metadata_source_jrs.collection_id,
                filenames_in_collection=metadata_source_jrs.filenames_in_collection
            )
            output_dict = empty_rs.to_dict()
            if args.drop_filenames: # Respect --drop-filenames for this default output too
                output_dict.pop("filenames_in_collection", None)
            print(json.dumps(output_dict, indent=None, separators=(',', ':')))
        else:
            # This is a more critical state: no metadata source to create even a default empty JRS.
            logger.error(
                "Cannot generate default empty JafResultSet: No source JRS metadata available "
                "(rs1 and rs2 are None). This indicates a severe prior error or unhandled execution path."
            )
            print(
                "Error: Boolean operation failed critically, and context for a default empty result is missing.",
                file=sys.stderr
            )
            sys.exit(1)

def handle_resolve_command(args): # Renamed from handle_explode_command
    """Handles the 'resolve' subcommand."""
    logger.debug(f"Resolve command with args: {args}")
    try:
        input_jrs = load_jaf_result_set_from_input(args.input_jrs, "input JafResultSet for resolve")
        matching_objects = input_jrs.get_matching_objects() 
        
        if not matching_objects and input_jrs.indices:
            logger.warning(
                f"Resolve command: JafResultSet has indices but no matching objects were ultimately retrieved. "
                f"This might occur if original data sources were empty or yielded no items for the given indices. "
                f"Source: {input_jrs.filenames_in_collection or input_jrs.collection_id}"
            )
        
        _print_objects_as_jsonl(matching_objects) # Already prints compact JSONL
            
    except JafResultSetError as e: 
        logger.error(f"Error during resolve operation: {e}", exc_info=False)
        print(f"Error: {e}", file=sys.stderr) 
        sys.exit(1)
    except ValueError as e: 
        logger.error(f"ValueError during resolve operation setup: {e}", exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e: 
        logger.error(f"Unexpected error during resolve operation: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def load_jaf_result_set_from_input(input_spec: str, arg_name: str) -> JafResultSet:
    """Loads a JafResultSet from a file path or stdin."""
    data_str = ""
    source_description = ""
    if input_spec == "-":
        logger.info(f"Reading JafResultSet for {arg_name} from stdin.")
        source_description = "stdin"
        try:
            data_str = sys.stdin.read()
        except Exception as e:
            logger.error(f"Error reading JafResultSet from stdin: {e}", exc_info=True)
            print(f"Error: Could not read JafResultSet from stdin. {e}", file=sys.stderr)
            sys.exit(1)
    else:
        logger.info(f"Reading JafResultSet for {arg_name} from file: {input_spec}")
        source_description = input_spec
        try:
            with open(input_spec, "r", encoding="utf-8") as f:
                data_str = f.read()
        except FileNotFoundError:
            logger.error(f"JafResultSet file not found: {input_spec}")
            print(f"Error: JafResultSet file '{input_spec}' not found.", file=sys.stderr)
            sys.exit(1)
        except IOError as e:
            logger.error(f"IOError reading JafResultSet file {input_spec}: {e}", exc_info=True)
            print(f"Error: Could not read JafResultSet file '{input_spec}'. {e}", file=sys.stderr)
            sys.exit(1)

    if not data_str.strip():
        logger.error(f"No data received for JafResultSet from {source_description}.")
        print(f"Error: No data provided for JafResultSet from {source_description}.", file=sys.stderr)
        sys.exit(1)
        
    try:
        data_dict = json.loads(data_str)
        return JafResultSet.from_dict(data_dict)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON for JafResultSet from {source_description}: {e}", exc_info=True)
        print(f"Error: JafResultSet input from {source_description} is not valid JSON. {e}", file=sys.stderr)
        sys.exit(1)
    except (TypeError, ValueError, KeyError) as e: # Errors from JafResultSet.from_dict
        logger.error(f"Invalid JafResultSet structure from {source_description}: {e}", exc_info=True)
        print(f"Error: JafResultSet input from {source_description} has invalid structure. {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
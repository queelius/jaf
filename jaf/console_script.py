import os
import argparse
import json
import logging
from typing import List, Dict, Optional, Union # Added Optional, Union

from .jaf import jaf, jafError

logger = logging.getLogger(__name__)

def walk_data_files(root_dir: str, recursive: bool = True) -> List[str]:
    """
    Walk through a directory to find all JSON and JSONL files.

    :param root_dir: The starting directory.
    :param recursive: Whether to walk directories recursively.
    :return: A list of file paths.
    """
    data_files = []
    for root, _, files in os.walk(root_dir):
        for file_name in files: # Renamed 'file' to 'file_name' to avoid conflict
            if file_name.endswith(".json") or file_name.endswith(".jsonl"):
                data_files.append(os.path.join(root, file_name))
        if not recursive:
            break
    return data_files

def load_objects_from_file(file_path: str) -> Optional[List[Dict]]:
    """
    Load a list of dictionary objects from a single JSON or JSONL file.

    :param file_path: Path to the JSON or JSONL file.
    :return: A list of dictionary objects, or None if a critical error occurs
             or no valid objects are found.
    """
    objects: List[Dict] = []
    is_jsonl = file_path.endswith(".jsonl")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            if is_jsonl:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            objects.append(obj)
                        else:
                            logger.warning(
                                f"Line {line_num} in JSONL file {file_path} is not a JSON object. Skipping."
                            )
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Error decoding JSONL line {line_num} in {file_path}. Skipping line.",
                            exc_info=True
                        )
            else:  # .json file
                data = json.load(f)
                if isinstance(data, list):
                    for item_idx, item in enumerate(data):
                        if isinstance(item, dict):
                            objects.append(item)
                        else:
                            logger.warning(
                                f"Item at index {item_idx} in JSON array in {file_path} is not an object. Skipping."
                            )
                elif isinstance(data, dict):
                    objects.append(data)
                else:
                    logger.warning(
                        f"JSON file {file_path} does not contain a list or object at the root. Skipping file."
                    )
                    return None
        
        if not objects:
            logger.info(f"No valid objects found in {file_path}.")
            return None
        return objects

    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error reading or parsing {file_path}: {e}", exc_info=True)
        return None
    except Exception as e_outer: # Catch-all for unexpected errors during file loading
        logger.error(f"Unexpected error loading file {file_path}: {e_outer}", exc_info=True)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Filter JSON or JSONL files/directories using the JAF filtering system."
    )
    parser.add_argument(
        "input_source", 
        type=str, 
        help="Path to a JSON/JSONL file or a directory containing JSON/JSONL files."
    )
    parser.add_argument(
        "--query", 
        type=str, 
        required=True, 
        help="Filtering query according to `jaf` JSON AST."
    )
    parser.add_argument(
        "--recursive", 
        action="store_true", 
        default=False, 
        help="Recursively search directories (if input_source is a directory)."
    )
    parser.add_argument(
        "--output-matches", 
        action="store_true", 
        default=False, 
        help="Print the actual matching JSON objects."
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        default="WARNING", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level for JAF operations."
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    query = json.loads(args.query)
    
    if not isinstance(query, list) and query is not None: # Should be a list
        print(f"Error: Query AST is not a list: {type(query)}")
        logger.error(f"Invalid query AST type: {type(query)}")
        return
    
    if query is None:
        print("Error: Query could not be parsed.")
        return

    file_paths_to_process: List[str] = []
    if os.path.isdir(args.input_source):
        print(f"Scanning directory: {args.input_source}")
        file_paths_to_process = walk_data_files(args.input_source, recursive=args.recursive)
        if not file_paths_to_process:
            print("No .json or .jsonl files found in the specified directory.")
            return
        print(f"Found {len(file_paths_to_process)} data file(s) in directory. Applying filter...\n")
    elif os.path.isfile(args.input_source):
        print(f"Processing single file: {args.input_source}")
        if args.input_source.endswith(".json") or args.input_source.endswith(".jsonl"):
            file_paths_to_process = [args.input_source]
        else:
            print(f"Error: Input file '{args.input_source}' is not a .json or .jsonl file.")
            return
        print("Applying filter...\n")
    else:
        print(f"Error: Input source '{args.input_source}' is not a valid file or directory.")
        return

    aggregated_results = []
    for current_file_path in file_paths_to_process:
        logger.info(f"Processing file: {current_file_path}")
        objects_from_file = load_objects_from_file(current_file_path)

        if objects_from_file is None or not objects_from_file:
            # load_objects_from_file logs details, so just continue
            continue
        
        try:
            matching_indices = jaf(objects_from_file, query)
            if matching_indices:
                matched_data = [objects_from_file[i] for i in matching_indices]
                aggregated_results.append({
                    "file": current_file_path,
                    "count": len(matched_data),
                    "matches": matched_data
                })
        except jafError as e_jaf:
            print(f"JAF Error processing {current_file_path}: {e_jaf}")
            logger.error(f"JAF Error for {current_file_path}: {e_jaf}", exc_info=True)
        except Exception as e_proc:
            print(f"Unexpected error processing {current_file_path} with JAF: {e_proc}")
            logger.error(f"Unexpected JAF processing error for {current_file_path}: {e_proc}", exc_info=True)

    if not aggregated_results:
        print("No matches found across all processed files.")
        return

    print("--- Filtering Complete ---")
    total_matched_files = len(aggregated_results)
    total_matched_objects = sum(r['count'] for r in aggregated_results)
    
    print(f"\nSummary: Matched {total_matched_objects} object(s) across {total_matched_files} file(s).\n")

    for result in aggregated_results:
        print(f"File: {result['file']}")
        print(f"  Matched objects in this file: {result['count']}")
        if args.output_matches:
            print("  Matches:")
            for match_obj in result['matches']:
                print(f"    {json.dumps(match_obj, indent=2)}")
        print("-" * 40)

if __name__ == "__main__":
    main()
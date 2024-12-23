import os
import argparse
import json
from .jaf import jaf, jafError

def walk_json_files(root_dir, recursive=True):
    """
    Walk through a directory to find all JSON files.

    :param root_dir: The starting directory.
    :param recursive: Whether to walk directories recursively.
    :return: A list of file paths.
    """
    json_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))
        if not recursive:
            break
    return json_files

def filter_json_files(json_files, query):
    """
    Filter JSON files using the provided JAF query.

    :param json_files: List of JSON file paths.
    :param query: Query in DSL or AST.
    :return: List of relevant files and matched content.
    """
    results = []

    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

                if isinstance(data, (list, dict)):
                    matching_results = jaf(data, query)

                    if matching_results.get("matching-indices") or matching_results.get("value-results"):
                        results.append({
                            "file": file_path,
                            "matches": matching_results
                        })
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Filter JSON files in a directory using the JAF filtering system."
    )
    parser.add_argument("dir", type=str, help="Base directory containing JSON files.")
    parser.add_argument(
        "--query", type=str, required=True, help="Filtering query according to `jaf` DSL or AST format."
    )
    parser.add_argument(
        "--recursive", action="store_true", default=False, help="Recursively search directories."
    )
    args = parser.parse_args()

    print(f"Scanning directory: {args.dir}")
    json_files = walk_json_files(args.dir, recursive=args.recursive)
    print(f"Found {len(json_files)} JSON file(s). Applying filter...\n")

    try:
        results = filter_json_files(json_files, args.query)
        print("Matching Files and Results:")
        for result in results:
            print(f"File: {result['file']}")
            print(f"Matches: {result['matches']}")
            print("-" * 40)
        print(f"Total matching files: {len(results)}")
    except jafError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()

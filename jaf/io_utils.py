import os
import json
import logging
from typing import List, Optional, Any

logger = logging.getLogger(__name__)

def walk_data_files(root_dir: str, recursive: bool = True) -> List[str]:
    """
    Walk through a directory to find all JSON and JSONL files.
    """
    data_files = []
    for r, _, files in os.walk(root_dir): # Renamed root to r to avoid conflict
        for f_name in files: # Renamed file to f_name
            if f_name.endswith(".json") or f_name.endswith(".jsonl"):
                data_files.append(os.path.join(r, f_name))
        if not recursive:
            break
    return data_files

def load_objects_from_file(file_path: str) -> Optional[List[Any]]:
    """
    Load a list of JSON values from a single JSON or JSONL file.
    Returns None if critical errors occur or no valid values are found.
    """
    objects: List[Any] = []
    is_jsonl = file_path.endswith(".jsonl")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            if is_jsonl:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        value = json.loads(line)
                        objects.append(value)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Error decoding JSONL line {line_num} in {file_path}. Skipping line.",
                            exc_info=True # Set to False if too verbose for common errors
                        )
            else:  # .json file
                data = json.load(f)
                if isinstance(data, list):
                    objects.extend(data)
                else:
                    objects.append(data)
        
        if not objects and os.path.exists(file_path): # File exists but yielded no objects
            logger.info(f"No JSON values successfully loaded from {file_path} (file might be empty or contain only invalid JSON).")
            # Return empty list for "empty but validly processed file"
            # This helps differentiate from file not found or critical parse error for the whole file.
            return [] 
        elif not objects: # File might not exist or other initial issue
             return None


        return objects

    except FileNotFoundError: # Explicitly handle file not found before IOError
        logger.error(f"File not found: {file_path}")
        return None # Critical error
    except (json.JSONDecodeError, IOError) as e: # JSONDecodeError for whole .json file, IOError for read issues
        logger.error(f"Error reading or parsing {file_path}: {e}", exc_info=True)
        return None # Critical error
    except Exception as e_outer: 
        logger.error(f"Unexpected error loading file {file_path}: {e_outer}", exc_info=True)
        return None # Critical error
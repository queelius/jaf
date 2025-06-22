import os
import json
import logging
from typing import List, Optional, Any, Iterator

logger = logging.getLogger(__name__)

def walk_data_files(directory_path: str, recursive: bool) -> Iterator[str]:
    """
    Walks a directory and yields paths to .json and .jsonl files.
    Sorts the files to ensure a consistent order.
    """
    if not os.path.isdir(directory_path):
        return # Gracefully handle non-existent directories

    found_files = []
    if recursive:
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith((".json", ".jsonl")):
                    found_files.append(os.path.join(root, file))
    else:
        for file in os.listdir(directory_path):
            if file.endswith((".json", ".jsonl")):
                full_path = os.path.join(directory_path, file)
                if os.path.isfile(full_path):
                    found_files.append(full_path)
    
    # Sort to ensure deterministic order
    for file_path in sorted(found_files):
        yield file_path

def load_objects_from_file(file_path: str) -> Optional[List[Any]]:
    """
    Loads a list of JSON objects from a .json (array) or .jsonl file.
    Returns None if the file cannot be parsed or is empty.
    """
    logger.debug(f"Attempting to load objects from: {file_path}")
    objects = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            if file_path.endswith(".jsonl"):
                for line in f:
                    line = line.strip()
                    if line:
                        objects.append(json.loads(line))
            elif file_path.endswith(".json"):
                content = f.read()
                if not content.strip():
                    logger.info(f"File is empty: {file_path}")
                    return None
                data = json.loads(content)
                if isinstance(data, list):
                    objects.extend(data)
                else:
                    # If it's a single JSON object, treat it as a collection of one.
                    objects.append(data)
            else:
                logger.warning(f"Unsupported file type, skipping: {file_path}")
                return None
        
        if not objects:
            logger.info(f"No JSON objects found or loaded from {file_path}")
            return None
            
        logger.debug(f"Successfully loaded {len(objects)} objects from {file_path}")
        return objects
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in {file_path}: {e}", exc_info=False)
        return None
    except IOError as e:
        logger.error(f"IO error reading {file_path}: {e}", exc_info=False)
        return None
    except Exception as e_outer:
        logger.error(f"Unexpected error loading file {file_path}: {e_outer}", exc_info=True)
        return None

def load_collection(collection_source: dict) -> List[Any]:
    """
    Loads a collection of objects based on the collection_source descriptor.
    """
    source_type = collection_source.get("type")
    all_objects = []

    if source_type == "buffered_stdin":
        # The content is already loaded and stored in the result set.
        return collection_source.get("content", [])
    elif source_type == "directory":
        file_paths = collection_source.get("files", [])
        for file_path in sorted(file_paths): # Ensure consistent order
            loaded = load_objects_from_file(file_path)
            if loaded:
                all_objects.extend(loaded)
    elif source_type in ("jsonl", "json_array"):
        path = collection_source.get("path")
        if path and os.path.exists(path):
            loaded = load_objects_from_file(path)
            if loaded:
                all_objects.extend(loaded)
        else:
            logger.warning(f"Path not found for collection_source: {path}")
    else:
        raise NotImplementedError(f"Unsupported collection source type: {source_type}")

    return all_objects

def load_objects_from_string(content: str) -> tuple[Optional[List[Any]], Optional[str]]:
    """
    Loads a list of JSON objects from a string, detecting the format.
    Returns a tuple of (list_of_objects, format_string) or (None, None).
    Format can be 'json_array', 'json_object', or 'jsonl'.
    """
    stripped_content = content.strip()
    if not stripped_content:
        return None, None

    # First, try to parse as a single JSON entity (array or object).
    try:
        data = json.loads(stripped_content)
        if isinstance(data, list):
            return data, "json_array"
        else:
            # A single JSON object is treated as a collection of one.
            return [data], "json_object"
    except json.JSONDecodeError:
        # If it's not a valid single JSON document, try parsing as JSONL.
        objects = []
        lines = stripped_content.split('\n')
        try:
            for line in lines:
                line = line.strip()
                if line:
                    objects.append(json.loads(line))
            if objects:
                return objects, "jsonl"
            else: # String might have just been whitespace or empty lines
                return None, None
        except json.JSONDecodeError as e:
            logger.error(f"Content could not be parsed as JSON or JSONL. Details: {e}", exc_info=False)
            return None, None
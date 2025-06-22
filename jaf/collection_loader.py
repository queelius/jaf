"""
Provides a flexible data loading system for JAF collections.

This module introduces the "CollectionLoader," a class that implements a
strategy pattern for loading collections of JSON documents. The core motivation
is to decouple the `JafResultSet`, which is a pure data container representing
filter results, from the implementation details of how and where the original
data is stored.

By using a loader, JAF can be easily extended to support new data sources
(e.g., databases, web APIs, different file formats) without modifying the core
filtering or result set logic. A `JafResultSet` only needs to store a serializable
`collection_source` dictionary, and the `CollectionLoader` uses this metadata
to dispatch to the correct loading function at runtime.

Key Components:
- JsonValue: A type alias representing any valid JSON type.
- LoaderFunc: A type alias for a function that can load a collection.
- CollectionLoader: A class that registers and dispatches to LoaderFuncs.
"""

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Union

# A type alias for any value that can be represented in JSON. This correctly
# reflects the data model JAF operates on, where a "document" can be any
# valid JSON value (object, array, string, number, etc.).
JsonValue = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

# A loader function is a callable that accepts a `collection_source` metadata
# dictionary and returns a list of JSON documents.
LoaderFunc = Callable[[Dict[str, Any]], List[JsonValue]]


class CollectionLoader:
    """
    Manages and dispatches data loading for different collection source types.

    This class acts as a registry for different data loading strategies. It maps
    a `source_type` string (e.g., "jsonl", "directory") to a specific function
    that knows how to handle that source. The `get_matching_objects` method of
    a `JafResultSet` will delegate the task of data retrieval to an instance
    of this class.
    """

    def __init__(self):
        """Initializes the CollectionLoader and registers default loaders."""
        self._loaders: Dict[str, LoaderFunc] = {}
        self._register_default_loaders()

    def register_loader(self, source_type: str, func: LoaderFunc):
        """
        Registers a new loader function for a given source type.

        This allows for extending JAF's data loading capabilities at runtime.

        Args:
            source_type: The unique string identifier for the source type
                         (e.g., "database_query").
            func: The function to execute when this source type is encountered.
        """
        self._loaders[source_type] = func

    def load(self, collection_source: Dict[str, Any]) -> List[JsonValue]:
        """
        Loads a collection of documents based on its source metadata.

        This is the main dispatch method. It inspects the 'type' key in the
        `collection_source` dictionary and calls the corresponding registered
        loader function.

        Args:
            collection_source: The source metadata dictionary from a JafResultSet.

        Returns:
            A list of documents from the specified source.

        Raises:
            ValueError: If the source type is missing, unknown, or if the
                        source metadata is invalid for the loader.
            IOError: If a file source cannot be read.
        """
        source_type = collection_source.get("type")
        if not source_type:
            raise ValueError("Collection source is missing 'type' key.")

        loader_func = self._loaders.get(source_type)
        if not loader_func:
            raise ValueError(f"No loader registered for source type: '{source_type}'")

        return loader_func(collection_source)

    def _register_default_loaders(self):
        """Pre-populates the loader with standard JAF source types."""
        self.register_loader("json_array", _load_from_json_array_file)
        self.register_loader("jsonl", _load_from_jsonl_file)
        self.register_loader("directory", _load_from_directory)


# --- Default Loader Implementations ---

def _load_from_path(path_str: str) -> List[JsonValue]:
    """
    Helper to load a collection from a single file path.

    It intelligently handles both JSONL (one JSON document per line) and
    standard files containing a single JSON array.

    Args:
        path_str: The string path to the file.

    Returns:
        A list of JSON documents.

    Raises:
        FileNotFoundError: If the path does not exist or is not a file.
        ValueError: If the file is not a valid JSONL or JSON array.
    """
    path = Path(path_str)
    if not path.is_file():
        raise FileNotFoundError(f"Data source file not found: {path}")

    content = path.read_text(encoding="utf-8").strip()
    if not content:
        return []

    # Try parsing as JSONL first, as it's more specific.
    if "\n" in content:
        try:
            return [json.loads(line) for line in content.splitlines() if line.strip()]
        except json.JSONDecodeError:
            # If it fails, it might be a single pretty-printed JSON array.
            # We'll let the next block handle it.
            pass

    # Fallback to parsing as a single JSON value, expecting a list.
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        # If it's a valid JSON value but not a list, it's an error for a collection.
        raise ValueError(f"File contains a single JSON value that is not an array: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"File is not a valid JSONL or JSON array: {path}") from e


def _load_from_json_array_file(source: Dict[str, Any]) -> List[JsonValue]:
    """Loader for a file containing a single JSON array."""
    path = Path(source["path"])
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"File is not a JSON array: {path}")
    return data


def _load_from_jsonl_file(source: Dict[str, Any]) -> List[JsonValue]:
    """Loader for a JSONL file (one JSON document per line)."""
    path = Path(source["path"])
    content = path.read_text(encoding="utf-8")
    if not content.strip():
        return []
    return [json.loads(line) for line in content.strip().splitlines() if line.strip()]


def _load_from_directory(source: Dict[str, Any]) -> List[JsonValue]:
    """
    Loader for a directory containing multiple JSON or JSONL files.

    It aggregates documents from all specified files into a single collection.
    """
    all_objects: List[JsonValue] = []
    file_paths = source.get("files")
    if not file_paths:
        raise ValueError("Directory source is missing 'files' key.")

    for file_path in file_paths:
        try:
            all_objects.extend(_load_from_path(file_path))
        except (IOError, ValueError, FileNotFoundError) as e:
            # Propagate error with context about which file failed.
            raise IOError(f"Failed to load or parse file '{file_path}' in directory source: {e}") from e
    return all_objects
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
import gzip
from pathlib import Path
from typing import Any, Callable, Dict, List, Union, Generator

from jaf.path_evaluation import eval_path

# A type alias for any value that can be represented in JSON. This correctly
# reflects the data model JAF operates on, where a "document" can be any
# valid JSON value (object, array, string, number, etc.).
JsonValue = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

# A loader function is a callable that accepts a loader instance and a
# `collection_source` metadata dictionary and returns data.
LoaderFunc = Callable[["CollectionLoader", Dict[str, Any]], Any]

# A streaming loader function yields items one by one
StreamLoaderFunc = Callable[
    ["CollectionLoader", Dict[str, Any]], Generator[JsonValue, None, None]
]


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
        """Registers or overwrites a loader function for a given source type."""
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
        source_type = collection_source.get("source_type") or collection_source.get(
            "type"
        )
        if not source_type:
            raise ValueError(
                "Collection source is missing 'source_type' or 'type' key."
            )

        loader_fn = self._loaders.get(source_type)
        if not loader_fn:
            raise ValueError(f"No loader registered for source type: '{source_type}'")

        return loader_fn(self, collection_source)

    def _register_default_loaders(self):
        """Pre-populates the loader with standard JAF source types."""
        # Compositional loaders
        self.register_loader("path_source", _load_from_path_source)
        self.register_loader("gzip_parser", _parse_gzip)
        self.register_loader("jsonl_parser", _parse_jsonl)
        self.register_loader("json_array_parser", _parse_json_array)

        # Legacy loaders
        self.register_loader("directory", _load_from_directory)
        self.register_loader("jsonl", _legacy_load_from_path)
        self.register_loader("json_array", _legacy_load_from_path)
        self.register_loader("buffered_stdin", _load_from_buffered_stdin)


# --- Compositional Loaders ---


def _load_from_path_source(loader: "CollectionLoader", source: dict) -> bytes:
    paths = source.get("paths", [])
    if len(paths) != 1:
        raise ValueError(
            "path_source with compositional parsing currently supports only one path."
        )

    path = Path(paths[0])
    if not path.is_file():
        raise FileNotFoundError(f"Data source file not found: {path}")
    return path.read_bytes()


def _parse_gzip(loader: "CollectionLoader", source: dict) -> bytes:
    inner_source = source.get("inner_source")
    if not inner_source:
        raise ValueError("gzip_parser requires an inner_source")

    gzipped_bytes = loader.load(inner_source)
    if not isinstance(gzipped_bytes, bytes):
        raise ValueError(
            f"gzip_parser expects bytes from inner_source, got {type(gzipped_bytes)}"
        )

    return gzip.decompress(gzipped_bytes)


def _parse_jsonl(loader: "CollectionLoader", source: dict) -> List[JsonValue]:
    inner_source = source.get("inner_source")
    if not inner_source:
        raise ValueError("jsonl_parser requires an inner_source")

    data = loader.load(inner_source)
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    content = data.strip()
    if not content:
        return []

    return [json.loads(line) for line in content.splitlines() if line.strip()]


def _parse_json_array(loader: "CollectionLoader", source: dict) -> List[JsonValue]:
    inner_source = source.get("inner_source")
    if not inner_source:
        raise ValueError("json_array_parser requires an inner_source")

    data = loader.load(inner_source)
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    content = data.strip()
    if not content:
        return []

    parsed_data = json.loads(content)
    if isinstance(parsed_data, list):
        return parsed_data
    else:
        return [parsed_data]


# --- Legacy Loader Implementations ---


def _load_from_buffered_stdin(loader: "CollectionLoader", source: dict) -> List[Any]:
    """Loader for data buffered directly in the collection source."""
    return source.get("content", [])


def _legacy_load_from_path(
    loader: "CollectionLoader", source: Dict[str, Any]
) -> List[JsonValue]:
    """
    Helper to load a collection from a single file path.

    It intelligently handles both JSONL (one JSON document per line) and
    standard files containing a single JSON array.
    """
    path_str = source.get("path")
    if not path_str:
        raise ValueError("Path-based source is missing 'path' key.")

    path = Path(path_str)
    if not path.is_file():
        raise FileNotFoundError(f"Data source file not found: {path}")

    if path_str.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            content = f.read().strip()
    else:
        content = path.read_text(encoding="utf-8").strip()

    if not content:
        return []

    # First, try to parse the entire content as a single JSON value.
    try:
        data = json.loads(content)
        if isinstance(data, list):
            # It's a file with a JSON array. This is a valid collection.
            return data
        else:
            # It's a file with a single JSON document that is not an array.
            return [data]
    except json.JSONDecodeError:
        # If parsing the whole content fails, it might be a multi-line JSONL file.
        try:
            return [json.loads(line) for line in content.splitlines() if line.strip()]
        except json.JSONDecodeError as e:
            # If both attempts fail, the format is invalid.
            raise ValueError(
                f"File is not a valid JSONL or a file containing a single JSON array: {path}"
            ) from e


def _load_from_directory(loader: "CollectionLoader", source: dict) -> List[Any]:
    """Loader for 'directory' source type."""
    all_objects: List[JsonValue] = []
    file_paths = source.get("files")
    if not file_paths:
        raise ValueError("Directory source is missing 'files' key.")

    for file_path in file_paths:
        try:
            all_objects.extend(_legacy_load_from_path(loader, {"path": file_path}))
        except (IOError, ValueError, FileNotFoundError) as e:
            # Propagate error with context about which file failed.
            raise IOError(
                f"Failed to load or parse file '{file_path}' in directory source: {e}"
            ) from e
    return all_objects

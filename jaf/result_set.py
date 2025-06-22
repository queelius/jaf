import json
import os
import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Union

# Import from the new io_utils module
from .io_utils import load_collection

logger = logging.getLogger(__name__)

class JafResultSetError(Exception):
    """Custom exception for errors related to JafResultSet operations."""
    pass

class JafResultSet:
    """
    Represents the set of indices from a data collection that satisfy a JAF query,
    along with metadata to ensure logical consistency when combining results.
    """

    def __init__(self, 
                 indices: Union[Iterable[int], Set[int]], 
                 collection_size: int, 
                 collection_id: Optional[Any] = None,
                 collection_source: Optional[Dict[str, Any]] = None,
                 query: Optional[Any] = None):
        if not isinstance(collection_size, int) or collection_size < 0:
            raise ValueError("collection_size must be a non-negative integer.")
        
        self.indices: Set[int] = set(indices)
        self.collection_size: int = collection_size
        self.collection_id: Optional[Any] = collection_id
        self.collection_source: Optional[Dict[str, Any]] = collection_source
        self.query: Optional[Any] = query

        # Validate indices
        if collection_size == 0 and self.indices:
            raise ValueError("Indices must be empty if collection_size is 0.")

        for i in self.indices:
            if not isinstance(i, int) or not (0 <= i < collection_size):
                valid_range_str = f"within the range [0, {collection_size - 1}]" if collection_size > 0 else "empty"
                raise ValueError(
                    f"All indices must be integers {valid_range_str}. "
                    f"Found invalid index: {i}"
                )

    def _check_compatibility(self, other: "JafResultSet") -> None:
        """
        Verifies that another JafResultSet is compatible for a binary operation.

        Compatibility requires that both result sets originate from the same logical
        data collection, which is checked by comparing `collection_size` and
        `collection_id`.

        Args:
            other: The other JafResultSet instance to compare against.

        Raises:
            JafResultSetError: If the result sets are not compatible.
            TypeError: If 'other' is not a JafResultSet.
        """
        if not isinstance(other, JafResultSet):
            raise TypeError("Operand must be an instance of JafResultSet")

        # For boolean operations, we primarily care that the collections are the
        # same size and have the same ID. The physical source representation
        # (e.g., collection_source) does not need to match, allowing for
        # operations between a result set from a directory and one from a
        # re-aggregated file, as long as the logical collection is the same.
        if self.collection_size != other.collection_size:
            raise JafResultSetError(
                f"Collection sizes do not match: {self.collection_size} != {other.collection_size}"
            )
        if self.collection_id is not None and other.collection_id is not None:
            if self.collection_id != other.collection_id:
                raise JafResultSetError(
                    f"Collection IDs do not match: '{self.collection_id}' != '{other.collection_id}'"
                )

    def AND(self, other: "JafResultSet") -> "JafResultSet":
        """
        Performs a logical AND (intersection) with another JafResultSet.

        Args:
            other: The JafResultSet to intersect with.

        Returns:
            A new JafResultSet containing the intersection of indices.
        """
        self._check_compatibility(other)
        new_indices = self.indices.intersection(other.indices)
        new_query = ["and", self.query, other.query] if self.query and other.query else None
        return JafResultSet(
            indices=new_indices,
            collection_size=self.collection_size,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source
            or other.collection_source,
            query=new_query,
        )

    def __and__(self, other: Any) -> "JafResultSet":
        """Operator overload for `&` (AND)."""
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return self.AND(other)

    def OR(self, other: "JafResultSet") -> "JafResultSet":
        """
        Performs a logical OR (union) with another JafResultSet.

        Args:
            other: The JafResultSet to union with.

        Returns:
            A new JafResultSet containing the union of indices.
        """
        self._check_compatibility(other)
        new_indices = self.indices.union(other.indices)
        new_query = ["or", self.query, other.query] if self.query and other.query else None
        return JafResultSet(
            indices=new_indices,
            collection_size=self.collection_size,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source
            or other.collection_source,
            query=new_query,
        )

    def __or__(self, other: Any) -> "JafResultSet":
        """Operator overload for `|` (OR)."""
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return self.OR(other)

    def NOT(self) -> "JafResultSet":
        """
        Performs a logical NOT (complement) on the JafResultSet.

        The complement is calculated relative to the entire collection, defined
        by `collection_size`.

        Returns:
            A new JafResultSet containing all indices from the collection
            that are not in this result set.
        """
        all_indices = set(range(self.collection_size))
        new_indices = all_indices.difference(self.indices)
        new_query = ["not", self.query] if self.query else None
        return JafResultSet(
            indices=new_indices,
            collection_size=self.collection_size,
            collection_id=self.collection_id,
            collection_source=self.collection_source,
            query=new_query,
        )

    def __invert__(self) -> "JafResultSet":
        """Operator overload for `~` (NOT)."""
        return self.NOT()

    def XOR(self, other: "JafResultSet") -> "JafResultSet":
        """
        Performs a logical XOR (symmetric difference) with another JafResultSet.

        Args:
            other: The JafResultSet to perform XOR with.

        Returns:
            A new JafResultSet containing indices that are in either set,
            but not in their intersection.
        """
        self._check_compatibility(other)
        new_indices = self.indices.symmetric_difference(other.indices)
        new_query = None
        if self.query and other.query:
            new_query = ["or", ["and", self.query, ["not", other.query]], ["and", ["not", self.query], other.query]]
        return JafResultSet(
            indices=new_indices,
            collection_size=self.collection_size,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source
            or other.collection_source,
            query=new_query,
        )

    def __xor__(self, other: Any) -> "JafResultSet":
        """Operator overload for `^` (XOR)."""
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return self.XOR(other)

    def SUBTRACT(self, other: "JafResultSet") -> "JafResultSet":
        """
        Performs a logical SUBTRACT (difference) with another JafResultSet.

        Removes indices from this set that are present in the other set.

        Args:
            other: The JafResultSet whose indices will be subtracted from this one.

        Returns:
            A new JafResultSet containing indices from this set but not
            from the other set.
        """
        self._check_compatibility(other)
        new_indices = self.indices.difference(other.indices)
        new_query = ["and", self.query, ["not", other.query]] if self.query and other.query else None
        return JafResultSet(
            indices=new_indices,
            collection_size=self.collection_size,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source
            or other.collection_source,
            query=new_query,
        )

    def __sub__(self, other: Any) -> "JafResultSet":
        """Operator overload for `-` (SUBTRACT)."""
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return self.SUBTRACT(other)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the JafResultSet to a dictionary.
        """
        return {
            "indices": sorted(list(self.indices)),
            "collection_size": self.collection_size,
            "collection_id": self.collection_id,
            "collection_source": self.collection_source,
            "query": self.query,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JafResultSet':
        """
        Deserializes a JafResultSet from a dictionary.
        """
        if "indices" not in data:
            raise ValueError("JafResultSet.from_dict: Missing required key in input data: 'indices'")
        if "collection_size" not in data:
            raise ValueError("JafResultSet.from_dict: Missing required key in input data: 'collection_size'")

        indices = data["indices"]
        if not isinstance(indices, (list, set)):
            raise ValueError("JafResultSet.from_dict: Type error in input data: 'indices' must be a list or set.")

        collection_size = data["collection_size"]
        if not isinstance(collection_size, int):
            raise ValueError("JafResultSet.from_dict: 'collection_size' must be an integer.")

        # Backward compatibility for 'filenames_in_collection'
        collection_source = data.get("collection_source")
        if not collection_source and "filenames_in_collection" in data:
            filenames = data["filenames_in_collection"]
            collection_id = data.get("collection_id")
            # Infer source from collection_id and filenames
            source_path = collection_id if isinstance(collection_id, str) and os.path.isdir(collection_id) else None
            collection_source = {
                "type": "directory",
                "path": source_path,
                "files": filenames
            }
            logger.debug(f"Converted legacy 'filenames_in_collection' to 'collection_source': {collection_source}")

        return cls(
            indices=indices,
            collection_size=collection_size,
            collection_id=data.get("collection_id"),
            collection_source=collection_source,
            query=data.get("query"),
        )

    def __repr__(self) -> str:
        return (f"JafResultSet(indices={sorted(list(self.indices))}, "
                f"collection_size={self.collection_size}, "
                f"collection_id={self.collection_id}, "
                f"collection_source={self.collection_source}, "
                f"query={self.query})")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return (self.indices == other.indices and
                self.collection_size == other.collection_size and
                self.collection_id == other.collection_id and
                self.collection_source == other.collection_source and
                self.query == other.query)
    
    def __len__(self) -> int:
        """Returns the number of matching indices in the result set."""
        return len(self.indices)

    def __iter__(self) -> Iterable[int]:
        """Returns an iterator over the sorted indices."""
        return iter(sorted(list(self.indices)))

    def __contains__(self, item: object) -> bool:
        """Checks if an index is in the result set."""
        if not isinstance(item, int):
            return False
        return item in self.indices

    def __ne__(self, other: object) -> bool:
        """Checks for inequality with another object."""
        result = self.__eq__(other)
        return not result if result is not NotImplemented else NotImplemented

    # Optional: Overload operators for more Pythonic usage
    def __and__(self, other: 'JafResultSet') -> 'JafResultSet':
        return self.AND(other)

    def __or__(self, other: 'JafResultSet') -> 'JafResultSet':
        return self.OR(other)

    def __invert__(self) -> 'JafResultSet':
        return self.NOT()
    
    def __xor__(self, other: 'JafResultSet') -> 'JafResultSet':
        return self.XOR(other)

    def __sub__(self, other: 'JafResultSet') -> 'JafResultSet':
        return self.SUBTRACT(other)

    def validate(self) -> None:
        """
        Performs a comprehensive validation of the JafResultSet.
        - Checks internal consistency.
        - Tries to load the collection to verify the source and size.
        """
        # Internal consistency is checked in __init__.
        # Now, check if the source is loadable and size matches.
        logger.info("Performing validation...")
        try:
            all_objects = load_collection(self.collection_source)
            if len(all_objects) != self.collection_size:
                raise JafResultSetError(
                    f"Collection size mismatch: JRS has {self.collection_size}, "
                    f"but source has {len(all_objects)} objects."
                )
            logger.info("Collection source loaded and size matches.")
        except JafResultSetError as e:
            raise e
        except Exception as e:
            raise JafResultSetError(f"Failed to load collection from source: {e}") from e

    def get_matching_objects(self) -> List[Any]:
        """
        Resolves indices back to the original data objects by interpreting
        the `collection_source` metadata.
        """
        source_to_load = self.collection_source
        
        if not source_to_load:
            # Fallback for older JRS or in-memory cases: try to use collection_id as a file path
            if isinstance(self.collection_id, str) and os.path.isfile(self.collection_id):
                logger.debug(f"No collection_source found, falling back to collection_id as file path: {self.collection_id}")
                source_type = "jsonl" if self.collection_id.endswith(".jsonl") else "json_array"
                source_to_load = {"type": source_type, "path": self.collection_id}
            else:
                raise JafResultSetError(
                    "JafResultSet must have a resolvable 'collection_source' or a file-path 'collection_id' to resolve objects."
                )

        try:
            all_objects = load_collection(source_to_load)
        except (NotImplementedError, FileNotFoundError) as e:
            raise JafResultSetError(f"Failed to load data from collection source: {e}") from e

        if len(all_objects) != self.collection_size:
            logger.warning(
                f"Data source size mismatch. Expected {self.collection_size} objects based on "
                f"JafResultSet, but found {len(all_objects)} in source: {source_to_load}. "
                "The original data source may have changed."
            )

        # This is inefficient for large files, but simple and correct.
        # A future optimization could stream the file and pick out lines by index.
        return [all_objects[i] for i in sorted(list(self.indices)) if i < len(all_objects)]

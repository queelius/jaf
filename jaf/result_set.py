import os # For os.path.isfile, os.path.exists
from typing import Set, Any, Optional, Iterable, Union, Dict, List
# Import the loader function
from .io_utils import load_objects_from_file 

class JafResultSetError(Exception):
    """Custom exception for JafResultSet operations."""
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
                 filenames_in_collection: Optional[List[str]] = None): # Added filenames_in_collection
        if not isinstance(collection_size, int) or collection_size < 0:
            raise ValueError("collection_size must be a non-negative integer.")
        
        self.indices: Set[int] = set(indices)
        self.collection_size: int = collection_size
        self.collection_id: Optional[Any] = collection_id
        self.filenames_in_collection: Optional[List[str]] = sorted(list(set(filenames_in_collection))) if filenames_in_collection else None # Store sorted unique

        # Validate indices
        if self.collection_size == 0:
            if self.indices:
                raise ValueError("Indices must be empty if collection_size is 0.")
        else: # collection_size > 0
            for i in self.indices:
                if not isinstance(i, int) or not (0 <= i < self.collection_size):
                    raise ValueError(
                        f"All indices must be integers within the range [0, {self.collection_size - 1}]. "
                        f"Found invalid index: {i}"
                    )

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the JafResultSet to a dictionary.
        """
        data = {
            "indices": sorted(list(self.indices)), # Store as sorted list for consistent output
            "collection_size": self.collection_size,
            "collection_id": self.collection_id
        }
        if self.filenames_in_collection is not None:
            data["filenames_in_collection"] = self.filenames_in_collection # Already sorted
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JafResultSet':
        """
        Creates a JafResultSet instance from a dictionary.
        Expected keys: "indices" (list/set of int), "collection_size" (int),
                       "collection_id" (optional), "filenames_in_collection" (optional list of str).
        """
        try:
            indices = data['indices']
            collection_size = data['collection_size']
            collection_id = data.get('collection_id') # Optional
            filenames_in_collection = data.get('filenames_in_collection') # Optional
            
            if not isinstance(indices, (list, set)):
                raise TypeError("JafResultSet.from_dict: 'indices' must be a list or set.")
            if not isinstance(collection_size, int):
                raise TypeError("JafResultSet.from_dict: 'collection_size' must be an integer.")
            if filenames_in_collection is not None and not isinstance(filenames_in_collection, list):
                raise TypeError("JafResultSet.from_dict: 'filenames_in_collection' must be a list if provided.")
            if filenames_in_collection is not None and not all(isinstance(f, str) for f in filenames_in_collection):
                raise TypeError("JafResultSet.from_dict: All items in 'filenames_in_collection' must be strings.")
            return cls(set(indices), collection_size, collection_id, filenames_in_collection)
        except KeyError as e:
            raise ValueError(f"JafResultSet.from_dict: Missing required key in input data: {e}")
        except TypeError as e: # Catch type errors from our checks
            raise ValueError(f"JafResultSet.from_dict: Type error in input data: {e}")


    def _check_compatibility(self, other: 'JafResultSet') -> None:
        """
        Checks if another JafResultSet is compatible for boolean operations.
        Raises JafResultSetError if not compatible.
        """
        if not isinstance(other, JafResultSet):
            raise TypeError("Operand must be an instance of JafResultSet.")
        if self.collection_size != other.collection_size:
            raise JafResultSetError(
                f"Collection sizes do not match: {self.collection_size} != {other.collection_size}."
            )
        if self.collection_id is not None and other.collection_id is not None and \
           self.collection_id != other.collection_id:
            raise JafResultSetError(
                f"Collection IDs do not match: '{self.collection_id}' != '{other.collection_id}'."
            )
        # Compatibility for filenames_in_collection:
        # If both are set, they should ideally be the same for the concept of a "collection" to hold.
        # However, operations like AND/OR/NOT don't strictly depend on them being identical,
        # as the core logic is on indices. We might choose to merge them or prioritize one.
        # For now, we'll propagate them. If they differ, the resulting set might inherit from 'self'.
        # A stricter check could be added here if necessary.
        # if self.filenames_in_collection is not None and \
        #    other.filenames_in_collection is not None and \
        #    set(self.filenames_in_collection) != set(other.filenames_in_collection):
        #    logger.warning("Operating on JafResultSets with different filenames_in_collection. This may be unintended.")


    def AND(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical AND (intersection) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.intersection(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        # Propagate filenames: if self has it, use it. Otherwise, if other has it, use that.
        new_filenames = self.filenames_in_collection if self.filenames_in_collection is not None else other.filenames_in_collection
        return JafResultSet(new_indices, self.collection_size, new_collection_id, new_filenames)

    def OR(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical OR (union) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.union(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        new_filenames = self.filenames_in_collection if self.filenames_in_collection is not None else other.filenames_in_collection
        return JafResultSet(new_indices, self.collection_size, new_collection_id, new_filenames)

    def NOT(self) -> 'JafResultSet':
        """
        Performs a logical NOT (complement) on this JafResultSet.
        Returns a new JafResultSet.
        """
        all_possible_indices = set(range(self.collection_size))
        new_indices = all_possible_indices.difference(self.indices)
        # NOT operation preserves the original collection's metadata
        return JafResultSet(new_indices, self.collection_size, self.collection_id, self.filenames_in_collection)

    def XOR(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical XOR (symmetric difference) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.symmetric_difference(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        new_filenames = self.filenames_in_collection if self.filenames_in_collection is not None else other.filenames_in_collection
        return JafResultSet(new_indices, self.collection_size, new_collection_id, new_filenames)

    def SUBTRACT(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical SUBTRACT (set difference, self - other) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.difference(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        new_filenames = self.filenames_in_collection if self.filenames_in_collection is not None else other.filenames_in_collection
        return JafResultSet(new_indices, self.collection_size, new_collection_id, new_filenames)

    def __contains__(self, item: Union[int,str]) -> bool:
        """
        Checks if an index or filename is in the result set.
        If item is an int, checks if it's in indices.
        If item is a str, checks if it's in filenames_in_collection.
        """
        if isinstance(item, int):
            return item in self.indices
        elif isinstance(item, str) and self.filenames_in_collection is not None:
            return item in self.filenames_in_collection
        return False
    
    def __bool__(self) -> bool:
        """
        Returns True if the result set contains any indices.
        This allows using JafResultSet in boolean contexts.
        """
        return bool(self.indices)
    
    def __getitem__(self, index: int) -> int:
        """
        Allows indexing into the result set to get a specific index.
        Raises IndexError if index is out of bounds.
        """
        if not isinstance(index, int):
            raise TypeError("Index must be an integer.")
        sorted_indices = sorted(self.indices)
        if index < 0 or index >= len(sorted_indices):
            raise IndexError("Index out of bounds.")
        return sorted_indices[index]

    def __hash__(self) -> int:
        """
        Returns a hash of the JafResultSet based on its indices, collection_size, and collection_id.
        This allows JafResultSet to be used in sets or as dictionary keys.
        """
        return hash((frozenset(self.indices), self.collection_size, self.collection_id, tuple(self.filenames_in_collection or [])))

    def __len__(self) -> int:
        """Returns the number of indices in the result set."""
        return len(self.indices)

    def __iter__(self):
        """Allows iteration over the sorted indices."""
        return iter(sorted(list(self.indices))) # Iterate in a consistent order

    def __repr__(self) -> str:
        filenames_info = f", filenames_in_collection=<{len(self.filenames_in_collection)} files>" if self.filenames_in_collection else ""
        return (f"JafResultSet(indices=<{len(self.indices)} items>, "
                f"collection_size={self.collection_size}, "
                f"collection_id='{self.collection_id}'{filenames_info})")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return (self.indices == other.indices and
                self.collection_size == other.collection_size and
                self.collection_id == other.collection_id and
                self.filenames_in_collection == other.filenames_in_collection) # Added filenames_in_collection

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

    def get_matching_objects(self) -> List[Any]:
        """
        Loads and returns the original data objects corresponding to the indices
        in this result set.
        
        Raises:
            JafResultSetError: If the original data source cannot be determined,
                               a source file is not found, or if there's a data
                               inconsistency (e.g., collection size mismatch).
        """
        all_original_objects: List[Any] = []
        files_to_load: List[str] = []
        source_description = ""

        if self.filenames_in_collection:
            files_to_load = self.filenames_in_collection
            source_description = f"files ({len(files_to_load)}) from JafResultSet.filenames_in_collection"
        elif self.collection_id and isinstance(self.collection_id, str) and os.path.isfile(self.collection_id):
            files_to_load = [self.collection_id]
            source_description = f"single file from JafResultSet.collection_id: '{self.collection_id}'"
        else:
            raise JafResultSetError(
                "Cannot get matching objects: Original data source not determinable. "
                "JafResultSet must have 'filenames_in_collection' populated, or 'collection_id' "
                "must be a path to a single existing file."
            )

        for file_path in files_to_load: # files_to_load is sorted if from filenames_in_collection
            # load_objects_from_file now handles FileNotFoundError and returns None
            objects_from_single_file = load_objects_from_file(file_path)
            
            if objects_from_single_file is None:
                # This implies a critical error like file not found (already logged by loader)
                # or unparseable JSON for a .json file.
                raise JafResultSetError(
                    f"Failed to load original data from '{file_path}' (source: {source_description}). "
                    "Check logs for details. Cannot reliably get matching objects."
                )
            all_original_objects.extend(objects_from_single_file)

        if len(all_original_objects) != self.collection_size:
            raise JafResultSetError(
                f"Data inconsistency: Loaded {len(all_original_objects)} original objects from {source_description}, "
                f"but JafResultSet.collection_size is {self.collection_size}. Cannot reliably get matching objects."
            )

        matched_data = []
        if not all_original_objects and self.indices:
            # This means collection_size was > 0, files were found and processed by load_objects_from_file,
            # but they all yielded empty lists (e.g. empty JSON arrays, or JSONL files with only blank/invalid lines).
            # The collection_size check above should ideally catch this if it's a true mismatch.
            # If collection_size is also 0, then this is fine.
            # If collection_size > 0 but all_original_objects is empty, the size check above would fail.
            # So this specific branch might be less likely if the size check is robust.
            # However, if collection_size is N > 0, and files are empty, all_original_objects will be [],
            # then len([]) != N, so the error above is raised.
            # This means if we pass the size check, all_original_objects has the expected number of items.
            pass # The size check handles this.

        for index in sorted(list(self.indices)):
            # The constructor of JafResultSet validates indices against collection_size.
            # The collection_size check above ensures len(all_original_objects) matches.
            # So, direct access should be safe.
            matched_data.append(all_original_objects[index])
        return matched_data

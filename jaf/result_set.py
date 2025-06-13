from typing import Set, Any, Optional, Iterable, Union

class JafResultSetError(Exception):
    """Custom exception for JafResultSet operations."""
    pass

class JafResultSet:
    """
    Represents the set of indices from a data collection that satisfy a JAF query,
    along with metadata to ensure logical consistency when combining results.
    """

    def __init__(self, indices: Union[Iterable[int], Set[int]], collection_size: int, collection_id: Optional[Any] = None):
        if not isinstance(collection_size, int) or collection_size < 0:
            raise ValueError("collection_size must be a non-negative integer.")
        
        self.indices: Set[int] = set(indices)
        self.collection_size: int = collection_size
        self.collection_id: Optional[Any] = collection_id

        # Validate indices
        if not all(isinstance(i, int) and 0 <= i < self.collection_size for i in self.indices):
            raise ValueError(f"All indices must be integers within the range [0, {self.collection_size -1 }].")

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

    def AND(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical AND (intersection) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.intersection(other.indices)
        # The collection_id of the new set could be self.collection_id or a combination/policy.
        # For simplicity, we'll use self.collection_id if set, otherwise other.collection_id.
        # A more sophisticated approach might involve a function to merge IDs or require them to be identical.
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        return JafResultSet(new_indices, self.collection_size, new_collection_id)

    def OR(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical OR (union) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.union(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        return JafResultSet(new_indices, self.collection_size, new_collection_id)

    def NOT(self) -> 'JafResultSet':
        """
        Performs a logical NOT (complement) on this JafResultSet.
        Returns a new JafResultSet.
        """
        all_possible_indices = set(range(self.collection_size))
        new_indices = all_possible_indices.difference(self.indices)
        return JafResultSet(new_indices, self.collection_size, self.collection_id)

    def XOR(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical XOR (symmetric difference) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.symmetric_difference(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        return JafResultSet(new_indices, self.collection_size, new_collection_id)

    def SUBTRACT(self, other: 'JafResultSet') -> 'JafResultSet':
        """
        Performs a logical SUBTRACT (set difference, self - other) with another JafResultSet.
        Returns a new JafResultSet.
        """
        self._check_compatibility(other)
        new_indices = self.indices.difference(other.indices)
        new_collection_id = self.collection_id if self.collection_id is not None else other.collection_id
        return JafResultSet(new_indices, self.collection_size, new_collection_id)

    def __len__(self) -> int:
        """Returns the number of indices in the result set."""
        return len(self.indices)

    def __iter__(self):
        """Allows iteration over the sorted indices."""
        return iter(sorted(list(self.indices))) # Iterate in a consistent order

    def __repr__(self) -> str:
        return (f"JafResultSet(indices=<{len(self.indices)} items>, "
                f"collection_size={self.collection_size}, "
                f"collection_id='{self.collection_id}')")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JafResultSet):
            return NotImplemented
        return (self.indices == other.indices and
                self.collection_size == other.collection_size and
                self.collection_id == other.collection_id)

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

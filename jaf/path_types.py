"""
JAF Path Types
==============

This module contains data types related to JAF path operations.
"""

from typing import Any, Optional


class PathValues(list):
    """
    A list subclass representing a collection of values obtained from a JAF path
    evaluation. It's used particularly when the path could naturally yield
    multiple results (e.g., from wildcards, slices, multiple indices, or
    regex key matching).

    It behaves like a standard list but serves as a distinct type to inform
    JAF's evaluation logic (e.g., `adapt_jaf_operator`) on how to handle
    arguments that are collections of path results. This typically involves
    iterating over the values or applying Cartesian product logic if multiple
    PathValues are arguments to an operator.

    Conceptually, it holds the "set of findings" or "resolved values" from the
    data for a given path. While it's a list (and thus can hold duplicates
    and maintains order of discovery), its primary distinction is signaling
    its origin from a potentially multi-valued path.

    An empty PathValues instance (e.g., PathValues([])) signifies that a
    multi-match path expression found no values. This is distinct from an
    empty list `[]` which might be returned by `eval_path` when a specific,
    non-multi-match path is not found.
    """
    def __init__(self, iterable: Optional[Any] = None):
        super().__init__(iterable if iterable is not None else [])

    def __repr__(self) -> str:
        return f"PathValues({super().__repr__()})"

    def __add__(self, other: Any) -> 'PathValues':
        if isinstance(other, list): # Handles list and PathValues
            return PathValues(super().__add__(other))
        return NotImplemented # type: ignore

    def __iadd__(self, other: Any) -> 'PathValues':
        super().__iadd__(other) # Modifies in place
        return self

    def __mul__(self, n: int) -> 'PathValues':
        return PathValues(super().__mul__(n))

    def __rmul__(self, n: int) -> 'PathValues':
        return PathValues(super().__rmul__(n)) # For n * PathValues

    def __getitem__(self, key: Any) -> Any:
        result = super().__getitem__(key)
        if isinstance(key, slice):
            return PathValues(result)
        # Single item access returns the item itself, not PathValues(item)
        return result

    def copy(self) -> 'PathValues':
        """Return a shallow copy of the PathValues."""
        return PathValues(self)

    def first(self, default: Optional[Any] = None) -> Any:
        """
        Returns the first element, or `default` if the list is empty.
        """
        return self[0] if self else default

    def last(self, default: Optional[Any] = None) -> Any:
        """
        Returns the last element, or `default` if the list is empty.
        """
        return self[-1] if self else default

    def one(self) -> Any:
        """
        Returns the single element if the list contains exactly one item.

        Raises:
            ValueError: If the list does not contain exactly one item.
        """
        if len(self) == 1:
            return self[0]
        elif not self:
            raise ValueError("PathValues is empty; expected exactly one element.")
        else:
            raise ValueError(f"PathValues contains {len(self)} elements; expected exactly one.")

    def one_or_none(self) -> Optional[Any]:
        """
        Returns the single element if the list contains exactly one item,
        or None if the list is empty.

        Raises:
            ValueError: If the list contains more than one item.
        """
        if len(self) == 1:
            return self[0]
        elif not self:
            return None
        else:
            raise ValueError(f"PathValues contains {len(self)} elements; expected one or none.")
        
    def __contains__(self, item: Any) -> bool:
        """
        Check if an item is in the PathValues.
        This uses the base list's __contains__ method.
        """
        return super().__contains__(item)

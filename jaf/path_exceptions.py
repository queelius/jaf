"""
JAF Path Exceptions
===================

This module contains exception classes related to JAF path operations.
"""

from __future__ import annotations
from typing import Any, List, Optional


class PathSyntaxError(ValueError):
    """Raised when a path string or AST is malformed.

    Attributes
    ----------
    path_segment : Any
        The offending substring / AST node (if available).
    full_path_ast : List[List[Any]] | None
        Provided when the error originates from an AST operation.
    """

    def __init__(
        self,
        message: str,
        *,
        path_segment: Any = None,
        full_path_ast: Optional[List[List[Any]]] = None,
    ):
        super().__init__(message)
        self.path_segment = path_segment
        self.full_path_ast = full_path_ast

    def __str__(self) -> str:  # pragma: no cover
        base = super().__str__()
        if self.path_segment is not None:
            base += f" | segment={self.path_segment!r}"
        if self.full_path_ast is not None:
            base += f" | ast={self.full_path_ast!r}"
        return base

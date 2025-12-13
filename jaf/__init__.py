"""
JAF - Just Another Flow

A powerful streaming data processing system for JSON/JSONL data with lazy evaluation,
composability, and a fluent API.

Key Features:
- Streaming architecture for processing large datasets
- Lazy evaluation - operations only execute when needed
- Fluent API with method chaining
- Comprehensive query language with S-expression syntax
- Multiple data sources (files, directories, stdin, memory, compressed)
- Test coverage: 68%

Example:
    >>> from jaf import stream
    >>> pipeline = stream("users.jsonl") \\
    ...     .filter(["gt?", "@age", 25]) \\
    ...     .map(["dict", "name", "@name", "email", "@email"]) \\
    ...     .take(10)
    >>> for user in pipeline.evaluate():
    ...     print(user)
"""

from .lazy_streams import stream, LazyDataStream, FilteredStream, MappedStream
from .jaf_eval import jaf_eval
from .path_evaluation import eval_path, exists
from .path_types import PathValues
from .path_exceptions import PathSyntaxError
from .path_conversion import path_ast_to_string, string_to_path_ast
from .exceptions import JAFError
from .sexp_parser import sexp_to_jaf, jaf_to_sexp, compile_sexp
from .probabilistic import BloomFilter, CountMinSketch, HyperLogLog

__version__ = "0.8.0"

__all__ = [
    "stream",
    "LazyDataStream",
    "FilteredStream",
    "MappedStream",
    "jaf_eval",
    "path_ast_to_string",
    "string_to_path_ast",
    "eval_path",
    "PathSyntaxError",
    "exists",
    "PathValues",
    "JAFError",
    "sexp_to_jaf",
    "jaf_to_sexp",
    "compile_sexp",
    # Probabilistic data structures
    "BloomFilter",
    "CountMinSketch",
    "HyperLogLog",
]

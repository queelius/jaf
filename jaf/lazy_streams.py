"""
Lazy streaming data operations for JAF.

This module provides the core streaming infrastructure for JAF, implementing
lazy evaluation patterns that allow efficient processing of large datasets
without loading everything into memory.

Key Classes:
    LazyDataStream: Base class for all streams
    FilteredStream: Stream filtered by a JAF query predicate
    MappedStream: Stream with transformed values
    
Stream operations are composable and lazy - they build a pipeline that
only executes when .evaluate() is called.

Example:
    >>> from jaf import stream
    >>> result = stream("data.jsonl") \\
    ...     .filter(["gt?", "@score", 80]) \\
    ...     .map("@name") \\
    ...     .take(5) \\
    ...     .evaluate()
    >>> list(result)  # Now the pipeline executes
    
Coverage: 83% (Good)
"""

from typing import Any, Dict, List, Optional, Generator, Union
from abc import ABC, abstractmethod
import logging

from .streaming_loader import StreamingLoader
from .jaf_eval import jaf_eval

logger = logging.getLogger(__name__)


class LazyDataStream(ABC):
    """
    Base class for all lazy data streams.

    A stream represents a lazy computation over a data source.
    Operations return new stream objects, building a pipeline.
    """

    def __init__(
        self, collection_source: Dict[str, Any], collection_id: Optional[str] = None
    ):
        self.collection_source = collection_source
        self.collection_id = collection_id

    def evaluate(self) -> Generator[Any, None, None]:
        """
        Evaluate the stream, yielding JSON values.

        Default implementation just streams from the source.
        Subclasses may override to add filtering, transformation, etc.
        """
        loader = StreamingLoader()
        yield from loader.stream(self.collection_source)

    # Composable operations - return new stream objects

    def filter(self, query: List) -> "FilteredStream":
        """Apply a filter to this stream."""
        return FilteredStream(query, self)

    def map(self, expression: List) -> "MappedStream":
        """Transform each value in this stream."""
        return MappedStream(expression, self)

    def take(self, n: int) -> "LazyDataStream":
        """Take only the first n values."""
        return LazyDataStream(
            {"type": "take", "n": n, "inner_source": self.collection_source},
            self.collection_id,
        )

    def skip(self, n: int) -> "LazyDataStream":
        """Skip the first n values."""
        return LazyDataStream(
            {"type": "skip", "n": n, "inner_source": self.collection_source},
            self.collection_id,
        )

    def slice(
        self, start: int, stop: Optional[int] = None, step: int = 1
    ) -> "LazyDataStream":
        """Slice the stream like a Python sequence."""
        return LazyDataStream(
            {
                "type": "slice",
                "start": start,
                "stop": stop,
                "step": step,
                "inner_source": self.collection_source,
            },
            self.collection_id,
        )

    def take_while(self, predicate_query: List) -> "LazyDataStream":
        """Take values while a predicate is true."""
        return LazyDataStream(
            {
                "type": "take_while",
                "query": predicate_query,
                "inner_source": self.collection_source,
            },
            self.collection_id,
        )

    def skip_while(self, predicate_query: List) -> "LazyDataStream":
        """Skip values while a predicate is true."""
        return LazyDataStream(
            {
                "type": "skip_while",
                "query": predicate_query,
                "inner_source": self.collection_source,
            },
            self.collection_id,
        )

    def batch(self, size: int) -> "LazyDataStream":
        """Batch values into groups of specified size."""
        return LazyDataStream(
            {"type": "batch", "size": size, "inner_source": self.collection_source},
            self.collection_id,
        )

    def enumerate(self, start: int = 0) -> "LazyDataStream":
        """Add index to each value."""
        return LazyDataStream(
            {
                "type": "enumerate",
                "start": start,
                "inner_source": self.collection_source,
            },
            self.collection_id,
        )
    
    def distinct(
        self,
        key: Optional[List] = None,
        window_size: float = float('inf'),
        strategy: Optional[str] = None,
        bloom_expected_items: int = 10000,
        bloom_fp_rate: float = 0.01
    ) -> "LazyDataStream":
        """Remove duplicate items from the stream.

        Args:
            key: Optional JAF expression to extract uniqueness key
            window_size: Size of sliding window (inf for exact distinct)
            strategy: Deduplication strategy ("exact", "windowed", "probabilistic")
            bloom_expected_items: Expected items (for probabilistic strategy)
            bloom_fp_rate: False positive rate (for probabilistic strategy)
        """
        return LazyDataStream(
            {
                "type": "distinct",
                "key": key,
                "window_size": window_size,
                "strategy": strategy,
                "bloom_expected_items": bloom_expected_items,
                "bloom_fp_rate": bloom_fp_rate,
                "inner_source": self.collection_source
            },
            self.collection_id
        )
    
    def groupby(self, key: List, aggregate: Optional[Dict[str, List]] = None,
                window_size: float = float('inf')) -> "LazyDataStream":
        """Group items by a key expression.
        
        Args:
            key: JAF expression to extract grouping key
            aggregate: Optional aggregation operations
            window_size: Size of tumbling window (inf for exact groupby)
        """
        return LazyDataStream(
            {
                "type": "groupby",
                "key": key,
                "aggregate": aggregate or {},
                "window_size": window_size,
                "inner_source": self.collection_source
            },
            self.collection_id
        )
    
    def intersect(
        self,
        other: "LazyDataStream",
        key: Optional[List] = None,
        window_size: float = float('inf'),
        strategy: Optional[str] = None,
        bloom_expected_items: int = 10000,
        bloom_fp_rate: float = 0.01
    ) -> "LazyDataStream":
        """Get items that appear in both streams.

        Args:
            other: Stream to intersect with
            key: Optional JAF expression for comparison key
            window_size: Size of sliding window (inf for exact intersect)
            strategy: Intersection strategy ("exact", "windowed", "probabilistic")
            bloom_expected_items: Expected items (for probabilistic strategy)
            bloom_fp_rate: False positive rate (for probabilistic strategy)
        """
        return LazyDataStream(
            {
                "type": "intersect",
                "left": self.collection_source,
                "right": other.collection_source,
                "key": key,
                "window_size": window_size,
                "strategy": strategy,
                "bloom_expected_items": bloom_expected_items,
                "bloom_fp_rate": bloom_fp_rate
            },
            self.collection_id
        )
    
    def except_from(
        self,
        other: "LazyDataStream",
        key: Optional[List] = None,
        window_size: float = float('inf'),
        strategy: Optional[str] = None,
        bloom_expected_items: int = 10000,
        bloom_fp_rate: float = 0.01
    ) -> "LazyDataStream":
        """Get items in this stream but not in the other.

        Args:
            other: Stream to subtract
            key: Optional JAF expression for comparison key
            window_size: Size of sliding window (inf for exact except)
            strategy: Except strategy ("exact", "windowed", "probabilistic")
            bloom_expected_items: Expected items (for probabilistic strategy)
            bloom_fp_rate: False positive rate (for probabilistic strategy)
        """
        return LazyDataStream(
            {
                "type": "except",
                "left": self.collection_source,
                "right": other.collection_source,
                "key": key,
                "window_size": window_size,
                "strategy": strategy,
                "bloom_expected_items": bloom_expected_items,
                "bloom_fp_rate": bloom_fp_rate
            },
            self.collection_id
        )

    def join(
        self, other: "LazyDataStream", on: List, how: str = "inner",
        on_right: Optional[List] = None, window_size: float = float('inf')
    ) -> "LazyDataStream":
        """Join with another stream.
        
        Args:
            other: The stream to join with
            on: JAF expression to extract join key from left stream
            how: Join type ('inner', 'left', 'right', 'outer')
            on_right: JAF expression for right stream key (defaults to same as on)
            window_size: Size of sliding window for join (inf for exact join)
        """
        return LazyDataStream(
            {
                "type": "join",
                "left": self.collection_source,
                "right": other.collection_source,
                "on": on,
                "on_right": on_right or on,
                "how": how,
                "window_size": window_size
            },
            self.collection_id
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the stream to a dictionary."""
        return {
            "collection_source": self.collection_source,
            "collection_id": self.collection_id,
        }

    def info(self) -> Dict[str, Any]:
        """
        Get basic information about this stream without evaluating it.
        """
        return {
            "type": self.__class__.__name__,
            "source_type": self.collection_source.get("type", "unknown"),
            "collection_id": self.collection_id,
            "pipeline": self._describe_pipeline(),
        }

    def _describe_pipeline(self) -> str:
        """Get a simple description of the pipeline."""
        return self._describe_source(self.collection_source)

    def _describe_source(self, source: Dict[str, Any]) -> str:
        """Get a human-readable description of a source."""
        source_type = source.get("type", "unknown")

        if source_type == "file":
            path = source.get("path", "unknown")
            return f"file({path.split('/')[-1]})"
        elif source_type == "directory":
            path = source.get("path", "unknown")
            return f"dir({path.split('/')[-1]})"
        elif source_type == "memory":
            data = source.get("data", [])
            return f"memory({len(data)} items)"
        elif source_type in ["filter", "map", "take", "skip", "batch"]:
            inner = self._describe_source(source.get("inner_source", {}))
            if source_type == "take":
                return f"take({source.get('n', '?')}) → {inner}"
            elif source_type == "skip":
                return f"skip({source.get('n', '?')}) → {inner}"
            elif source_type == "batch":
                return f"batch({source.get('size', '?')}) → {inner}"
            else:
                return f"{source_type} → {inner}"
        elif source_type in ["fibonacci", "prng", "distribution", "prime", "counter"]:
            return f"{source_type}()"
        else:
            return source_type

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.collection_source})"


class FilteredStream(LazyDataStream):
    """
    A stream filtered by a JAF query.

    Only values matching the query are yielded.
    """

    def __init__(self, query: List, source: LazyDataStream):
        # Build a filter source
        filter_source = {
            "type": "filter",
            "query": query,
            "inner_source": source.collection_source,
        }
        super().__init__(filter_source, source.collection_id)
        self.query = query
        self.source = source

    def AND(self, other: "FilteredStream") -> "FilteredStream":
        """Combine with another filter using logical AND."""
        combined_query = ["and", self.query, other.query]
        return FilteredStream(combined_query, self.source)

    def OR(self, other: "FilteredStream") -> "FilteredStream":
        """Combine with another filter using logical OR."""
        combined_query = ["or", self.query, other.query]
        return FilteredStream(combined_query, self.source)

    def NOT(self) -> "FilteredStream":
        """Negate this filter."""
        negated_query = ["not", self.query]
        return FilteredStream(negated_query, self.source)

    def XOR(self, other: "FilteredStream") -> "FilteredStream":
        """Combine with another filter using logical XOR (exclusive OR)."""
        # XOR is (A AND NOT B) OR (NOT A AND B)
        xor_query = [
            "or",
            ["and", self.query, ["not", other.query]],
            ["and", ["not", self.query], other.query],
        ]
        return FilteredStream(xor_query, self.source)

    def DIFFERENCE(self, other: "FilteredStream") -> "FilteredStream":
        """Subtract another filter (A AND NOT B)."""
        diff_query = ["and", self.query, ["not", other.query]]
        return FilteredStream(diff_query, self.source)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["stream_type"] = "FilteredStream"
        result["query"] = self.query
        return result


class MappedStream(LazyDataStream):
    """
    A stream with values transformed by a JAF expression.

    Each value is transformed by evaluating the expression.
    """

    def __init__(self, expression: List, source: LazyDataStream):
        # Build a map source
        map_source = {
            "type": "map",
            "expression": expression,
            "inner_source": source.collection_source,
        }
        super().__init__(map_source, source.collection_id)
        self.expression = expression
        self.source = source

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["stream_type"] = "MappedStream"
        result["expression"] = self.expression
        return result


class JoinedStream(LazyDataStream):
    """
    A stream joining two other streams.

    Values from both streams are joined based on a key expression.
    """

    def __init__(
        self, left: LazyDataStream, right: LazyDataStream, on: List, how: str = "inner"
    ):
        # Build a join source
        join_source = {
            "type": "join",
            "left": left.collection_source,
            "right": right.collection_source,
            "on": on,
            "how": how,
        }
        # Use left's collection_id by default
        super().__init__(join_source, left.collection_id)
        self.left = left
        self.right = right
        self.on = on
        self.how = how

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["stream_type"] = "JoinedStream"
        result["on"] = self.on
        result["how"] = self.how
        return result


# Factory function to create streams from sources
def stream(source: Optional[Union[str, Dict[str, Any]]] = None, **kwargs) -> LazyDataStream:
    """
    Create a lazy data stream from a source.

    Args:
        source: Either a file path, a source descriptor dict, or None (if using kwargs)
        **kwargs: Source parameters (type, path, pattern, etc.)

    Returns:
        A LazyDataStream ready for operations

    Examples:
        # Using file path
        stream("data.jsonl")
        
        # Using dict descriptor
        stream({"type": "file", "path": "data.jsonl"})
        
        # Using kwargs
        stream(type="file", path="data.jsonl")
        stream(type="directory", path="/data", recursive=True, pattern="*.json*")
        stream(type="fibonacci", limit=100)
        
        # Converting dict to kwargs
        source_dict = {"type": "directory", "path": "/data", "recursive": True}
        stream(**source_dict)
    """
    if source is None and kwargs:
        # Using kwargs style
        source_dict = dict(kwargs)
    elif isinstance(source, str) and not kwargs:
        # Simple file path (only when no kwargs provided)
        path = source

        # Build appropriate source based on extension
        if path.endswith(".gz"):
            source_dict = {
                "type": "gzip",
                "inner_source": {"type": "file", "path": path},
            }
        else:
            source_dict = {"type": "file", "path": path}

        # Add parser based on format
        if ".jsonl" in path:
            source_dict = {"type": "jsonl", "inner_source": source_dict}
        elif ".csv" in path:
            source_dict = {"type": "csv", "inner_source": source_dict}
        elif ".tsv" in path:
            source_dict = {"type": "csv", "inner_source": source_dict, "delimiter": "\t"}
        elif ".json" in path:
            source_dict = {"type": "json_array", "inner_source": source_dict}
    elif isinstance(source, dict) and not kwargs:
        # Dict descriptor (only when no kwargs provided)
        source_dict = source
    elif source is not None:
        # If source is provided with kwargs, it's an error
        raise ValueError("Cannot provide both source and kwargs")
    else:
        raise ValueError("Must provide either a source path, dict descriptor, or kwargs")

    return LazyDataStream(source_dict)

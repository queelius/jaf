"""
Lazy streaming data operations.

This module provides composable stream types that build lazy pipelines
of operations on data streams.
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
    
    def __init__(self, collection_source: Dict[str, Any], collection_id: Optional[str] = None):
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
        return LazyDataStream({
            "type": "take",
            "n": n,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def skip(self, n: int) -> "LazyDataStream":
        """Skip the first n values."""
        return LazyDataStream({
            "type": "skip",
            "n": n,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def slice(self, start: int, stop: Optional[int] = None, step: int = 1) -> "LazyDataStream":
        """Slice the stream like a Python sequence."""
        return LazyDataStream({
            "type": "slice",
            "start": start,
            "stop": stop,
            "step": step,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def take_while(self, predicate_query: List) -> "LazyDataStream":
        """Take values while a predicate is true."""
        return LazyDataStream({
            "type": "take_while",
            "query": predicate_query,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def skip_while(self, predicate_query: List) -> "LazyDataStream":
        """Skip values while a predicate is true."""
        return LazyDataStream({
            "type": "skip_while",
            "query": predicate_query,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def batch(self, size: int) -> "LazyDataStream":
        """Batch values into groups of specified size."""
        return LazyDataStream({
            "type": "batch",
            "size": size,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def enumerate(self, start: int = 0) -> "LazyDataStream":
        """Add index to each value."""
        return LazyDataStream({
            "type": "enumerate",
            "start": start,
            "inner_source": self.collection_source
        }, self.collection_id)
    
    def join(self, other: "LazyDataStream", on: List, how: str = "inner") -> "JoinedStream":
        """Join with another stream."""
        return JoinedStream(self, other, on, how)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize the stream to a dictionary."""
        return {
            "collection_source": self.collection_source,
            "collection_id": self.collection_id
        }
    
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
            "inner_source": source.collection_source
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
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
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
            "inner_source": source.collection_source
        }
        super().__init__(map_source, source.collection_id)
        self.expression = expression
        self.source = source
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["expression"] = self.expression
        return result


class JoinedStream(LazyDataStream):
    """
    A stream joining two other streams.
    
    Values from both streams are joined based on a key expression.
    """
    
    def __init__(self, left: LazyDataStream, right: LazyDataStream, on: List, how: str = "inner"):
        # Build a join source
        join_source = {
            "type": "join",
            "left": left.collection_source,
            "right": right.collection_source,
            "on": on,
            "how": how
        }
        # Use left's collection_id by default
        super().__init__(join_source, left.collection_id)
        self.left = left
        self.right = right
        self.on = on
        self.how = how
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["on"] = self.on
        result["how"] = self.how
        return result


# Factory function to create streams from sources
def stream(source: Union[str, Dict[str, Any]]) -> LazyDataStream:
    """
    Create a lazy data stream from a source.
    
    Args:
        source: Either a file path or a source descriptor dict
        
    Returns:
        A LazyDataStream ready for operations
    """
    if isinstance(source, str):
        # Simple file path
        path = source
        
        # Build appropriate source based on extension
        if path.endswith('.gz'):
            source_dict = {
                "type": "gzip",
                "inner_source": {"type": "file", "path": path}
            }
        else:
            source_dict = {"type": "file", "path": path}
        
        # Add parser based on format
        if '.jsonl' in path:
            source_dict = {"type": "jsonl", "inner_source": source_dict}
        elif '.csv' in path:
            source_dict = {"type": "csv", "inner_source": source_dict}
        elif '.json' in path:
            source_dict = {"type": "json_array", "inner_source": source_dict}
    else:
        source_dict = source
    
    return LazyDataStream(source_dict)
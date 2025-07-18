import json
import os
import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Union, Generator, Tuple

# Import from the new io_utils module
from .io_utils import load_collection
from .jaf_eval import jaf_eval
from .streaming_loader import StreamingLoader

logger = logging.getLogger(__name__)


class JafQuerySetError(Exception):
    """Custom exception for errors related to JafQuerySet operations."""

    pass


class JafQuerySet:
    """
    Represents a lazy query over a data collection, with metadata to ensure
    logical consistency when combining queries.
    """

    def __init__(
        self,
        query: Any,
        collection_id: Optional[Any] = None,
        collection_source: Optional[Dict[str, Any]] = None,
    ):
        self.query: Any = query
        self.collection_id: Optional[Any] = collection_id
        self.collection_source: Optional[Dict[str, Any]] = collection_source

    def _check_compatibility(self, other: "JafQuerySet") -> None:
        """
        Verifies that another JafQuerySet is compatible for a binary operation.

        Warns if query sets have different collection IDs, as this may indicate
        operations on different logical collections.

        Args:
            other: The other JafQuerySet instance to compare against.

        Raises:
            TypeError: If 'other' is not a JafQuerySet.
        """
        if not isinstance(other, JafQuerySet):
            raise TypeError("Operand must be an instance of JafQuerySet")

        # For boolean operations, we allow combining queries from the same logical collection
        # even if stored differently (e.g., directory vs re-aggregated file)
        if self.collection_id is not None and other.collection_id is not None:
            if self.collection_id != other.collection_id:
                logger.warning(
                    f"Combining queries from different logical collections: '{self.collection_id}' and '{other.collection_id}'. "
                    "Results may be unpredictable if collections contain different data."
                )

    def AND(self, other: "JafQuerySet") -> "JafQuerySet":
        """
        Performs a logical AND (intersection) with another JafQuerySet.

        Args:
            other: The JafQuerySet to intersect with.

        Returns:
            A new JafQuerySet containing the intersection query.
        """
        self._check_compatibility(other)
        new_query = ["and", self.query, other.query]
        return JafQuerySet(
            query=new_query,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source or other.collection_source,
        )

    def __and__(self, other: Any) -> "JafQuerySet":
        """Operator overload for `&` (AND)."""
        if not isinstance(other, JafQuerySet):
            raise TypeError(
                f"Cannot perform boolean operation with {type(other).__name__}. Expected JafQuerySet."
            )
        return self.AND(other)

    def OR(self, other: "JafQuerySet") -> "JafQuerySet":
        """
        Performs a logical OR (union) with another JafQuerySet.

        Args:
            other: The JafQuerySet to union with.

        Returns:
            A new JafQuerySet containing the union query.
        """
        self._check_compatibility(other)
        new_query = ["or", self.query, other.query]
        return JafQuerySet(
            query=new_query,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source or other.collection_source,
        )

    def __or__(self, other: Any) -> "JafQuerySet":
        """Operator overload for `|` (OR)."""
        if not isinstance(other, JafQuerySet):
            raise TypeError(
                f"Cannot perform boolean operation with {type(other).__name__}. Expected JafQuerySet."
            )
        return self.OR(other)

    def NOT(self) -> "JafQuerySet":
        """
        Performs a logical NOT (complement) on the JafQuerySet.

        The complement is calculated relative to the entire collection.

        Returns:
            A new JafQuerySet containing the negation query.
        """
        new_query = ["not", self.query]
        return JafQuerySet(
            query=new_query,
            collection_id=self.collection_id,
            collection_source=self.collection_source,
        )

    def __invert__(self) -> "JafQuerySet":
        """Operator overload for `~` (NOT)."""
        return self.NOT()

    # Lazy streaming operations
    def take(self, n: int) -> Generator[Any, None, None]:
        """
        Take the first n items from the evaluated stream.

        This is useful for infinite streams or when you only need a sample.

        Args:
            n: Number of items to take

        Yields:
            Up to n items from the stream
        """
        count = 0
        for item in self.evaluate():
            if count >= n:
                break
            yield item
            count += 1

    def skip(self, n: int) -> Generator[Any, None, None]:
        """
        Skip the first n items from the evaluated stream.

        Args:
            n: Number of items to skip

        Yields:
            Items after skipping the first n
        """
        count = 0
        for item in self.evaluate():
            if count >= n:
                yield item
            else:
                count += 1

    def take_while(self, predicate_query: List) -> Generator[Any, None, None]:
        """
        Take items from the stream while the predicate query is true.

        Stops yielding as soon as the predicate becomes false.

        Args:
            predicate_query: JAF query that returns a boolean

        Yields:
            Items while the predicate is true
        """
        for item in self.evaluate():
            try:
                result = jaf_eval.eval(predicate_query, item)
                if isinstance(result, bool) and result:
                    yield item
                else:
                    break
            except Exception:
                break

    def skip_while(self, predicate_query: List) -> Generator[Any, None, None]:
        """
        Skip items from the stream while the predicate query is true.

        Starts yielding once the predicate becomes false.

        Args:
            predicate_query: JAF query that returns a boolean

        Yields:
            Items after the predicate becomes false
        """
        skipping = True
        for item in self.evaluate():
            if skipping:
                try:
                    result = jaf_eval.eval(predicate_query, item)
                    if isinstance(result, bool) and result:
                        continue
                    else:
                        skipping = False
                except Exception:
                    skipping = False

            if not skipping:
                yield item

    def slice(
        self, start: int, stop: Optional[int] = None, step: int = 1
    ) -> Generator[Any, None, None]:
        """
        Slice the stream similar to Python's slice notation.

        Args:
            start: Starting index
            stop: Stopping index (exclusive), None for no limit
            step: Step size

        Yields:
            Items in the slice
        """
        index = 0
        for item in self.evaluate():
            if stop is not None and index >= stop:
                break

            if index >= start and (index - start) % step == 0:
                yield item

            index += 1

    def enumerate(self, start: int = 0) -> Generator[Tuple[int, Any], None, None]:
        """
        Enumerate items from the stream with an index.

        Args:
            start: Starting index value

        Yields:
            Tuples of (index, item)
        """
        index = start
        for item in self.evaluate():
            yield (index, item)
            index += 1

    def batch(self, size: int) -> Generator[List[Any], None, None]:
        """
        Batch items from the stream into groups of a specified size.

        The last batch may have fewer items than the specified size.

        Args:
            size: Number of items per batch

        Yields:
            Lists of items, each containing up to 'size' items
        """
        batch = []
        for item in self.evaluate():
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []

        # Yield any remaining items
        if batch:
            yield batch

    def XOR(self, other: "JafQuerySet") -> "JafQuerySet":
        """
        Performs a logical XOR (symmetric difference) with another JafQuerySet.

        Args:
            other: The JafQuerySet to perform XOR with.

        Returns:
            A new JafQuerySet containing the XOR query.
        """
        self._check_compatibility(other)
        new_query = [
            "or",
            ["and", self.query, ["not", other.query]],
            ["and", ["not", self.query], other.query],
        ]
        return JafQuerySet(
            query=new_query,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source or other.collection_source,
        )

    def __xor__(self, other: Any) -> "JafQuerySet":
        """Operator overload for `^` (XOR)."""
        if not isinstance(other, JafQuerySet):
            raise TypeError(
                f"Cannot perform boolean operation with {type(other).__name__}. Expected JafQuerySet."
            )
        return self.XOR(other)

    def SUBTRACT(self, other: "JafQuerySet") -> "JafQuerySet":
        """
        Performs a logical SUBTRACT (difference) with another JafQuerySet.

        Removes matching items from this query that are matched by the other query.

        Args:
            other: The JafQuerySet whose query will be subtracted from this one.

        Returns:
            A new JafQuerySet containing the subtraction query.
        """
        self._check_compatibility(other)
        new_query = ["and", self.query, ["not", other.query]]
        return JafQuerySet(
            query=new_query,
            collection_id=self.collection_id or other.collection_id,
            collection_source=self.collection_source or other.collection_source,
        )

    def __sub__(self, other: Any) -> "JafQuerySet":
        """Operator overload for `-` (SUBTRACT)."""
        if not isinstance(other, JafQuerySet):
            raise TypeError(
                f"Cannot perform boolean operation with {type(other).__name__}. Expected JafQuerySet."
            )
        return self.SUBTRACT(other)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the JafQuerySet to a dictionary.
        """
        return {
            "query": self.query,
            "collection_id": self.collection_id,
            "collection_source": self.collection_source,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JafQuerySet":
        """
        Deserializes a JafQuerySet from a dictionary.
        """
        if "query" not in data:
            raise ValueError(
                "JafQuerySet.from_dict: Missing required key in input data: 'query'"
            )

        return cls(
            query=data["query"],
            collection_id=data.get("collection_id"),
            collection_source=data.get("collection_source"),
        )

    def __repr__(self) -> str:
        return (
            f"JafQuerySet(query={self.query}, "
            f"collection_id={self.collection_id}, "
            f"collection_source={self.collection_source})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JafQuerySet):
            raise TypeError(
                f"Cannot perform boolean operation with {type(other).__name__}. Expected JafQuerySet."
            )
        return (
            self.query == other.query
            and self.collection_id == other.collection_id
            and self.collection_source == other.collection_source
        )

    # Removed __len__, __iter__, __contains__ - use evaluate() for explicit evaluation

    def __ne__(self, other: object) -> bool:
        """Checks for inequality with another object."""
        result = self.__eq__(other)
        return not result if result is not NotImplemented else NotImplemented

    # Optional: Overload operators for more Pythonic usage
    def __and__(self, other: "JafQuerySet") -> "JafQuerySet":
        return self.AND(other)

    def __or__(self, other: "JafQuerySet") -> "JafQuerySet":
        return self.OR(other)

    def __invert__(self) -> "JafQuerySet":
        return self.NOT()

    def __xor__(self, other: "JafQuerySet") -> "JafQuerySet":
        return self.XOR(other)

    def __sub__(self, other: "JafQuerySet") -> "JafQuerySet":
        return self.SUBTRACT(other)

    def validate(self) -> None:
        """
        Performs a comprehensive validation of the JafQuerySet.
        Tries to load the collection to verify the source is accessible.
        """
        logger.info("Performing validation...")
        try:
            all_objects = load_collection(self.collection_source)
            logger.info(
                f"Collection source loaded successfully with {len(all_objects)} objects."
            )
        except Exception as e:
            raise JafQuerySetError(f"Failed to load collection from source: {e}") from e

    def evaluate(self) -> Generator[Any, None, None]:
        """
        Evaluates the query against the collection and yields matching objects.

        This method streams results, yielding each matching object as it's found
        instead of loading the entire collection into memory.

        Yields:
            Matching objects from the collection

        Raises:
            JafQuerySetError: If the collection source cannot be loaded or query fails
        """
        source_to_load = self.collection_source

        if not source_to_load:
            # Fallback: try to use collection_id as a file path
            if isinstance(self.collection_id, str) and os.path.isfile(
                self.collection_id
            ):
                logger.debug(
                    f"No collection_source found, falling back to collection_id as file path: {self.collection_id}"
                )
                # Build a clean source descriptor chain
                path = self.collection_id

                # Start with file source
                source = {"type": "file", "path": path}

                # Add decompression if needed
                if path.endswith(".gz"):
                    source = {"type": "gzip", "inner_source": source}

                # Add parser based on format
                if ".jsonl" in path:
                    source = {"type": "jsonl", "inner_source": source}
                else:
                    source = {"type": "json_array", "inner_source": source}

                source_to_load = source
            else:
                raise JafQuerySetError(
                    "JafQuerySet must have a resolvable 'collection_source' or a file-path 'collection_id' to evaluate."
                )

        # Create streaming loader
        loader = StreamingLoader()

        # Stream and evaluate
        logger.debug(f"Streaming evaluation of query {self.query}")
        match_count = 0
        total_count = 0

        try:
            for item in loader.stream(source_to_load):
                total_count += 1
                try:
                    result = jaf_eval.eval(self.query, item)
                    if isinstance(result, bool) and result:
                        match_count += 1
                        logger.debug(f"Item {total_count} matched query")
                        yield item
                    elif not isinstance(result, bool):
                        logger.warning(
                            f"Query returned non-boolean value for item {total_count}: {result}. Item does not match."
                        )
                except ValueError as e:
                    # Unknown operators, syntax errors - these are real query problems
                    raise JafQuerySetError(f"Query evaluation failed: {e}") from e
                except (KeyError, AttributeError, TypeError, IndexError) as e:
                    # Expected "item doesn't match" errors - path not found, wrong types, etc.
                    logger.debug(f"Item {total_count} doesn't match query due to: {e}")
                except Exception as e:
                    # Unexpected errors - better to fail than silently ignore
                    raise JafQuerySetError(
                        f"Unexpected error evaluating query on item {total_count}: {e}"
                    ) from e

        except (FileNotFoundError, ValueError) as e:
            raise JafQuerySetError(
                f"Failed to stream data from collection source: {e}"
            ) from e

        logger.info(
            f"Streaming complete: {match_count} matches found out of {total_count} objects"
        )

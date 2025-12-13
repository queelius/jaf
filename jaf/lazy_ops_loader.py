"""
Lazy operation implementations for JAF streaming.

This module provides the core streaming operations that can be composed to build
complex data processing pipelines. All operations are lazy and streaming-friendly.

Operation Categories:
    - Basic: take, skip, slice, batch, enumerate
    - Filtering: filter, take_while, skip_while
    - Transformation: map, project
    - Set Operations: distinct, union, intersect, except
    - Aggregation: groupby, join, product
    - Combination: chain, zip

Windowed Operations:
    Several operations support a window_size parameter for memory-bounded processing:
    - distinct, groupby, join, intersect, except
    - Use window_size=float('inf') for exact results (unbounded memory)
    - Finite windows trade accuracy for memory efficiency

Example:
    >>> # Memory-efficient distinct with sliding window
    >>> source = {"type": "distinct", "inner_source": {...}, "window_size": 1000}
    
    >>> # Exact join (may use more memory)
    >>> source = {"type": "join", "left": {...}, "right": {...}, "window_size": float('inf')}

Coverage: 67% (Improved from 58%)
"""

from typing import Generator, Dict, Any, Optional, List
import itertools
from .exceptions import QueryError


def stream_take(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Take the first n items from an inner source.

    Args:
        source: Dict with:
            - inner_source: Source to take from
            - n: Number of items to take
    """
    inner_source = source.get("inner_source")
    n = source.get("n", 10)

    if not inner_source:
        raise ValueError("Take source missing 'inner_source'")

    count = 0
    for item in loader.stream(inner_source):
        if count >= n:
            break
        yield item
        count += 1


def stream_skip(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Skip the first n items from an inner source.

    Args:
        source: Dict with:
            - inner_source: Source to skip from
            - n: Number of items to skip
    """
    inner_source = source.get("inner_source")
    n = source.get("n", 0)

    if not inner_source:
        raise ValueError("Skip source missing 'inner_source'")

    for i, item in enumerate(loader.stream(inner_source)):
        if i >= n:
            yield item


def stream_slice(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Slice items from an inner source.

    Args:
        source: Dict with:
            - inner_source: Source to slice from
            - start: Starting index
            - stop: Stopping index (exclusive), None for no limit
            - step: Step size (default: 1)
    """
    inner_source = source.get("inner_source")
    start = source.get("start", 0)
    stop = source.get("stop")
    step = source.get("step", 1)

    if not inner_source:
        raise ValueError("Slice source missing 'inner_source'")

    for item in itertools.islice(loader.stream(inner_source), start, stop, step):
        yield item


def stream_filter(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Filter items from an inner source using a JAF query.

    Args:
        source: Dict with:
            - inner_source: Source to filter
            - query: JAF query to use as predicate
    """
    from .jaf_eval import jaf_eval

    inner_source = source.get("inner_source")
    query = source.get("query")

    if not inner_source:
        raise ValueError("Filter source missing 'inner_source'")
    if not query:
        raise ValueError("Filter source missing 'query'")

    for item in loader.stream(inner_source):
        try:
            result = jaf_eval.eval(query, item)
            if isinstance(result, bool) and result:
                yield item
        except (KeyError, AttributeError, TypeError, IndexError):
            # Expected "item doesn't match" errors - skip the item
            # These happen when paths don't exist, types don't match, etc.
            pass
        except QueryError:
            # Query errors (unknown operators, invalid syntax) should fail fast
            raise
        # Let other unexpected exceptions propagate


def stream_take_while(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Take items while a predicate query is true.

    Args:
        source: Dict with:
            - inner_source: Source to take from
            - query: JAF query to use as predicate
    """
    from .jaf_eval import jaf_eval

    inner_source = source.get("inner_source")
    query = source.get("query")

    if not inner_source:
        raise ValueError("Take-while source missing 'inner_source'")
    if not query:
        raise ValueError("Take-while source missing 'query'")

    for item in loader.stream(inner_source):
        try:
            result = jaf_eval.eval(query, item)
            if isinstance(result, bool) and result:
                yield item
            else:
                break
        except Exception:
            break


def stream_skip_while(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Skip items while a predicate query is true.

    Args:
        source: Dict with:
            - inner_source: Source to skip from
            - query: JAF query to use as predicate
    """
    from .jaf_eval import jaf_eval

    inner_source = source.get("inner_source")
    query = source.get("query")

    if not inner_source:
        raise ValueError("Skip-while source missing 'inner_source'")
    if not query:
        raise ValueError("Skip-while source missing 'query'")

    skipping = True
    for item in loader.stream(inner_source):
        if skipping:
            try:
                result = jaf_eval.eval(query, item)
                if isinstance(result, bool) and result:
                    continue
                else:
                    skipping = False
            except Exception:
                skipping = False

        if not skipping:
            yield item


def stream_batch(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Batch items from an inner source into groups.

    Args:
        source: Dict with:
            - inner_source: Source to batch
            - size: Batch size
    """
    inner_source = source.get("inner_source")
    size = source.get("size", 10)

    if not inner_source:
        raise ValueError("Batch source missing 'inner_source'")

    batch = []
    for item in loader.stream(inner_source):
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []

    # Yield any remaining items
    if batch:
        yield batch


def stream_enumerate(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Enumerate items with an index.

    Args:
        source: Dict with:
            - inner_source: Source to enumerate
            - start: Starting index (default: 0)
            - as_dict: If True, yield {"index": i, "value": item}, else yield [i, item]
    """
    inner_source = source.get("inner_source")
    start = source.get("start", 0)
    as_dict = source.get("as_dict", True)

    if not inner_source:
        raise ValueError("Enumerate source missing 'inner_source'")

    for i, item in enumerate(loader.stream(inner_source), start=start):
        if as_dict:
            yield {"index": i, "value": item}
        else:
            yield [i, item]


def stream_map(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Transform items using a JAF query/expression.

    Args:
        source: Dict with:
            - inner_source: Source to map
            - expression: JAF expression to evaluate for each item
    """
    from .jaf_eval import jaf_eval

    inner_source = source.get("inner_source")
    expression = source.get("expression")

    if not inner_source:
        raise ValueError("Map source missing 'inner_source'")
    if not expression:
        raise ValueError("Map source missing 'expression'")

    for item in loader.stream(inner_source):
        try:
            result = jaf_eval.eval(expression, item)
            yield result
        except Exception as e:
            # Could optionally yield None or skip
            yield None


def stream_chain(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Chain multiple sources sequentially.

    Args:
        source: Dict with:
            - sources: List of sources to chain
    """
    sources = source.get("sources", [])

    for src in sources:
        yield from loader.stream(src)


def stream_join(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Join two streams based on a key expression.

    Args:
        source: Dict with:
            - left: Left source
            - right: Right source
            - on: JAF expression to extract join key from left stream
            - on_right: JAF expression for right stream key (defaults to same as on)
            - how: Join type ("inner", "left", "right", "outer")
            - window_size: Size of sliding window for join (inf for exact join)
    """
    from .jaf_eval import jaf_eval
    import logging
    from collections import deque
    
    logger = logging.getLogger(__name__)

    left_source = source.get("left")
    right_source = source.get("right")
    on_expr = source.get("on")
    on_right_expr = source.get("on_right", on_expr)  # Default to same as on
    how = source.get("how", "inner")
    window_size = source.get("window_size", float('inf'))

    if not left_source or not right_source:
        raise ValueError("Join source missing 'left' or 'right'")
    if not on_expr:
        raise ValueError("Join source missing 'on' expression")
    
    # Validate window_size
    if isinstance(window_size, str) and window_size.lower() == 'inf':
        window_size = float('inf')
    else:
        try:
            window_size = float(window_size)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid window_size: {window_size}")
    
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    
    if window_size == float('inf'):
        # Exact join - load right side into memory
        logger.debug("Using infinite window for exact join")
        
        # Build index from right stream and track all right items for right/outer joins
        right_index = {}
        right_items_by_key = {}  # Track items for right/outer joins
        
        for item in loader.stream(right_source):
            try:
                key = jaf_eval.eval(on_right_expr, item)  # Use on_right_expr for right side
                if key not in right_index:
                    right_index[key] = []
                    right_items_by_key[key] = []
                right_index[key].append(item)
                right_items_by_key[key].append(item)
            except Exception:
                # Items that can't be keyed go into None key for right/outer joins
                if how in ("right", "outer"):
                    if None not in right_items_by_key:
                        right_items_by_key[None] = []
                    right_items_by_key[None].append(item)

        # Track which right keys have been matched (for right/outer joins)
        matched_right_keys = set()

        # Stream left and join
        for left_item in loader.stream(left_source):
            try:
                key = jaf_eval.eval(on_expr, left_item)
                right_matches = right_index.get(key, [])

                if right_matches:
                    # Mark this key as matched
                    matched_right_keys.add(key)
                    # Inner join - output combinations
                    for right_item in right_matches:
                        yield {"left": left_item, "right": right_item}
                elif how in ("left", "outer"):
                    # Left join - output with null right
                    yield {"left": left_item, "right": None}
            except Exception:
                if how in ("left", "outer"):
                    yield {"left": left_item, "right": None}

        # For right/outer joins, emit unmatched right items
        if how in ("right", "outer"):
            for key, items in right_items_by_key.items():
                if key not in matched_right_keys:
                    # These items were never matched
                    for right_item in items:
                        yield {"left": None, "right": right_item}
    else:
        # Windowed join - use sliding window buffer
        logger.debug(f"Using sliding window of size {window_size} for join")
        
        # Buffer for right side with sliding window
        right_window = deque(maxlen=int(window_size))
        right_index = {}  # Current window index
        
        # First, fill the initial window from right stream
        right_stream_iter = iter(loader.stream(right_source))
        for _ in range(int(window_size)):
            try:
                item = next(right_stream_iter)
                right_window.append(item)
                try:
                    key = jaf_eval.eval(on_right_expr, item)
                    if key not in right_index:
                        right_index[key] = []
                    right_index[key].append(item)
                except Exception:
                    pass
            except StopIteration:
                break
        
        # Now stream left and join with windowed right
        for left_item in loader.stream(left_source):
            try:
                key = jaf_eval.eval(on_expr, left_item)
                right_matches = right_index.get(key, [])
                
                if right_matches:
                    for right_item in right_matches:
                        yield {"left": left_item, "right": right_item}
                elif how in ("left", "outer"):
                    yield {"left": left_item, "right": None}
            except Exception:
                if how in ("left", "outer"):
                    yield {"left": left_item, "right": None}
            
            # Slide the window - remove old, add new
            try:
                new_item = next(right_stream_iter)
                
                # Remove oldest item from index if window is full
                if len(right_window) >= window_size:
                    old_item = right_window[0]
                    try:
                        old_key = jaf_eval.eval(on_right_expr, old_item)
                        if old_key in right_index:
                            right_index[old_key] = [i for i in right_index[old_key] if i != old_item]
                            if not right_index[old_key]:
                                del right_index[old_key]
                    except Exception:
                        pass
                
                # Add new item to window and index
                right_window.append(new_item)
                try:
                    new_key = jaf_eval.eval(on_right_expr, new_item)
                    if new_key not in right_index:
                        right_index[new_key] = []
                    right_index[new_key].append(new_item)
                except Exception:
                    pass
            except StopIteration:
                # No more right items to add
                pass
        
        # Note: For windowed joins, we don't emit unmatched right items
        # as we can't track all right items in bounded memory


def stream_groupby(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Group items from a stream by a key expression.

    Args:
        source: Dict with:
            - inner_source: Source to group
            - key: JAF expression to extract grouping key
            - aggregate: Optional aggregation operations (dict mapping field names to ops)
            - window_size: Size of tumbling window for grouping
                          (default: inf for exact groupby)

    Example:
        {
            "type": "groupby",
            "inner_source": {...},
            "key": "@category",
            "aggregate": {
                "count": ["count"],
                "total_price": ["sum", "@price"],
                "avg_price": ["mean", "@price"],
                "max_price": ["max", "@price"]
            }
        }
    """
    from .jaf_eval import jaf_eval
    import statistics

    inner_source = source.get("inner_source")
    key_expr = source.get("key")
    aggregate = source.get("aggregate", {})
    window_size = source.get("window_size", float('inf'))

    if not inner_source:
        raise ValueError("Groupby source missing 'inner_source'")
    if not key_expr:
        raise ValueError("Groupby source missing 'key' expression")
    
    # Validate window_size
    if isinstance(window_size, str) and window_size.lower() == 'inf':
        window_size = float('inf')
    else:
        try:
            window_size = float(window_size)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid window_size: {window_size}")
    
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    
    # Helper function to yield groups with aggregation
    def yield_groups(groups):
        for key, items in groups.items():
            result = {"key": key, "items": items, "count": len(items)}

            # Apply aggregations
            for field_name, agg_spec in aggregate.items():
                if not isinstance(agg_spec, list) or len(agg_spec) == 0:
                    continue

                op = agg_spec[0]
                value_expr = agg_spec[1] if len(agg_spec) > 1 else "@"

                # Extract values for aggregation
                values = []
                for item in items:
                    try:
                        val = jaf_eval.eval(value_expr, item)
                        if val is not None:
                            values.append(val)
                    except Exception:
                        pass

                # Apply aggregation operation
                if op == "count":
                    result[field_name] = len(items)
                elif op == "sum" and values:
                    result[field_name] = sum(values)
                elif op == "mean" and values:
                    result[field_name] = statistics.mean(values)
                elif op == "median" and values:
                    result[field_name] = statistics.median(values)
                elif op == "stddev" and len(values) > 1:
                    result[field_name] = statistics.stdev(values)
                elif op == "variance" and len(values) > 1:
                    result[field_name] = statistics.variance(values)
                elif op == "min" and values:
                    result[field_name] = min(values)
                elif op == "max" and values:
                    result[field_name] = max(values)
                elif op == "first" and items:
                    result[field_name] = jaf_eval.eval(value_expr, items[0])
                elif op == "last" and items:
                    result[field_name] = jaf_eval.eval(value_expr, items[-1])
                else:
                    result[field_name] = None

            yield result
    
    if window_size == float('inf'):
        # Exact groupby - collect all items
        groups = {}
        for item in loader.stream(inner_source):
            try:
                key = jaf_eval.eval(key_expr, item)
                # Convert unhashable types to strings for grouping
                if isinstance(key, (list, dict)):
                    key = str(key)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
            except Exception:
                # Items that can't be grouped go into None group
                if None not in groups:
                    groups[None] = []
                groups[None].append(item)
        
        # Yield all groups
        yield from yield_groups(groups)
    else:
        # Tumbling window groupby
        window_count = 0
        groups = {}
        
        for item in loader.stream(inner_source):
            try:
                key = jaf_eval.eval(key_expr, item)
                # Convert unhashable types to strings for grouping
                if isinstance(key, (list, dict)):
                    key = str(key)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
            except Exception:
                # Items that can't be grouped go into None group
                if None not in groups:
                    groups[None] = []
                groups[None].append(item)
            
            window_count += 1
            
            # When window is full, yield groups and reset
            if window_count >= window_size:
                yield from yield_groups(groups)
                groups = {}
                window_count = 0
        
        # Yield any remaining groups
        if groups:
            yield from yield_groups(groups)


def stream_product(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Compute the Cartesian product of two streams.

    Args:
        source: Dict with:
            - left: Left source
            - right: Right source
            - limit: Optional limit on output size (since product can be very large)
    """
    left_source = source.get("left")
    right_source = source.get("right")
    limit = source.get("limit")

    if not left_source or not right_source:
        raise ValueError("Product source missing 'left' or 'right'")

    # Collect right stream into memory (needed for product)
    right_items = list(loader.stream(right_source))

    if not right_items:
        return  # Empty product

    count = 0
    for left_item in loader.stream(left_source):
        for right_item in right_items:
            if limit and count >= limit:
                return

            yield {"left": left_item, "right": right_item}
            count += 1


def stream_distinct(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Remove duplicate items from a stream.

    Args:
        source: Dict with:
            - inner_source: Source to deduplicate
            - key: Optional JAF expression to extract uniqueness key
                  If not provided, uses the entire item
            - window_size: Size of sliding window for deduplication
                          (default: inf for exact distinct)
            - strategy: Deduplication strategy:
                - "exact": Use set for exact deduplication (default with inf window)
                - "windowed": Use sliding window (default with finite window)
                - "probabilistic": Use Bloom filter for memory-efficient approximate dedup
            - bloom_expected_items: Expected number of items (for probabilistic strategy)
            - bloom_fp_rate: False positive rate (for probabilistic strategy, default 0.01)
    """
    from .jaf_eval import jaf_eval
    import json
    import logging
    from collections import deque

    logger = logging.getLogger(__name__)

    inner_source = source.get("inner_source")
    key_expr = source.get("key")
    window_size = source.get("window_size", float('inf'))
    strategy = source.get("strategy")  # None = auto-select based on window_size
    bloom_expected_items = source.get("bloom_expected_items", 10000)
    bloom_fp_rate = source.get("bloom_fp_rate", 0.01)

    if not inner_source:
        raise ValueError("Distinct source missing 'inner_source'")
    
    # Validate window_size
    if isinstance(window_size, str) and window_size.lower() == 'inf':
        window_size = float('inf')
    else:
        try:
            window_size = float(window_size)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid window_size: {window_size}")
    
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    # Auto-select strategy if not specified
    if strategy is None:
        if window_size == float('inf'):
            strategy = "exact"
        else:
            strategy = "windowed"

    # Probabilistic strategy using Bloom filter
    if strategy == "probabilistic":
        from .probabilistic import BloomFilter

        logger.debug(f"Using Bloom filter for probabilistic distinct "
                    f"(expected={bloom_expected_items}, fp_rate={bloom_fp_rate})")

        bloom = BloomFilter(
            expected_items=bloom_expected_items,
            false_positive_rate=bloom_fp_rate
        )

        for item in loader.stream(inner_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                # Convert unhashable types to strings
                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                if key not in bloom:
                    bloom.add(key)
                    yield item
            except Exception:
                # On error, consider item unique
                yield item
        return

    # Use infinite window for exact results
    if strategy == "exact" or window_size == float('inf'):
        logger.debug("Using infinite window for exact distinct")
        seen = set()
        
        for item in loader.stream(inner_source):
            try:
                if key_expr:
                    # Use key expression for uniqueness
                    key = jaf_eval.eval(key_expr, item)
                else:
                    # Use entire item
                    key = item

                # Convert unhashable types to strings
                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                if key not in seen:
                    seen.add(key)
                    yield item
            except Exception:
                # On error, consider item unique
                yield item
    else:
        # Use sliding window for approximate distinct
        logger.debug(f"Using sliding window of size {window_size} for distinct")
        window = deque(maxlen=int(window_size))
        seen_in_window = set()
        
        for item in loader.stream(inner_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                # Convert unhashable types to strings
                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)
                
                # Always update the sliding window
                if len(window) >= window_size:
                    # Remove oldest item from seen set
                    old_item = window[0]
                    if key_expr:
                        old_key = jaf_eval.eval(key_expr, old_item)
                    else:
                        old_key = old_item
                    if isinstance(old_key, (list, dict)):
                        old_key = json.dumps(old_key, sort_keys=True)
                    seen_in_window.discard(old_key)
                
                # Add current item to window
                window.append(item)
                
                # Check if we've seen this key in current window
                if key not in seen_in_window:
                    seen_in_window.add(key)
                    yield item
                    
            except Exception:
                # On error, consider item unique
                yield item


def stream_project(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Project specific fields from items (like SQL SELECT).

    Args:
        source: Dict with:
            - inner_source: Source to project from
            - fields: Dict mapping output field names to JAF expressions

    Example:
        {
            "type": "project",
            "inner_source": {...},
            "fields": {
                "name": "@name",
                "age": "@age",
                "full_address": ["join", ", ", ["@address.street", "@address.city"]]
            }
        }
    """
    from .jaf_eval import jaf_eval

    inner_source = source.get("inner_source")
    fields = source.get("fields", {})

    if not inner_source:
        raise ValueError("Project source missing 'inner_source'")

    for item in loader.stream(inner_source):
        result = {}
        for field_name, expression in fields.items():
            try:
                result[field_name] = jaf_eval.eval(expression, item)
            except Exception:
                result[field_name] = None
        yield result


def stream_union(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Union multiple streams (concatenate with optional deduplication).

    Args:
        source: Dict with:
            - sources: List of sources to union
            - distinct: If True, remove duplicates (default: False)
    """
    import json

    sources = source.get("sources", [])
    distinct = source.get("distinct", False)

    if distinct:
        seen = set()

    for src in sources:
        for item in loader.stream(src):
            if distinct:
                # Create hashable key
                key = item
                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                if key in seen:
                    continue
                seen.add(key)

            yield item


def stream_intersect(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Intersect two streams (items that appear in both).

    Args:
        source: Dict with:
            - left: First source
            - right: Second source
            - key: Optional JAF expression for comparison key
            - window_size: Size of sliding window (inf for exact intersect)
            - strategy: Intersection strategy:
                - "exact": Load right side to memory (default with inf window)
                - "windowed": Use sliding window (default with finite window)
                - "probabilistic": Use Bloom filter for approximate membership
            - bloom_expected_items: Expected items in right stream (for probabilistic)
            - bloom_fp_rate: False positive rate (default 0.01)

    Note:
        The probabilistic strategy uses a Bloom filter to test if left items
        exist in the right stream. This trades memory for accuracy - false
        positives may cause some non-intersecting items to be included.
    """
    from .jaf_eval import jaf_eval
    import json
    import logging
    from collections import deque

    logger = logging.getLogger(__name__)

    left_source = source.get("left")
    right_source = source.get("right")
    key_expr = source.get("key")
    window_size = source.get("window_size", float('inf'))
    strategy = source.get("strategy")
    bloom_expected_items = source.get("bloom_expected_items", 10000)
    bloom_fp_rate = source.get("bloom_fp_rate", 0.01)

    if not left_source or not right_source:
        raise ValueError("Intersect source missing 'left' or 'right'")
    
    # Validate window_size
    if isinstance(window_size, str) and window_size.lower() == 'inf':
        window_size = float('inf')
    else:
        try:
            window_size = float(window_size)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid window_size: {window_size}")
    
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    # Auto-select strategy if not specified
    if strategy is None:
        if window_size == float('inf'):
            strategy = "exact"
        else:
            strategy = "windowed"

    # Probabilistic strategy using Bloom filter
    if strategy == "probabilistic":
        from .probabilistic import BloomFilter

        logger.debug(f"Using Bloom filter for probabilistic intersect "
                    f"(expected={bloom_expected_items}, fp_rate={bloom_fp_rate})")

        # Build Bloom filter from right stream
        bloom = BloomFilter(
            expected_items=bloom_expected_items,
            false_positive_rate=bloom_fp_rate
        )

        for item in loader.stream(right_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                bloom.add(key)
            except Exception:
                pass

        # Stream left and check membership in Bloom filter
        seen = set()
        for item in loader.stream(left_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                # Check Bloom filter (may have false positives)
                if key in bloom and key not in seen:
                    seen.add(key)
                    yield item
            except Exception:
                pass
        return

    if strategy == "exact" or window_size == float('inf'):
        # Exact intersect - load right side into memory
        logger.debug("Using infinite window for exact intersect")
        
        # Collect right stream into set
        right_set = set()
        for item in loader.stream(right_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                right_set.add(key)
            except Exception:
                pass

        # Stream left and check membership
        seen = set()
        for item in loader.stream(left_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                if key in right_set and key not in seen:
                    seen.add(key)
                    yield item
            except Exception:
                pass
    else:
        # Windowed intersect - use sliding window
        logger.debug(f"Using sliding window of size {window_size} for intersect")
        
        # Buffer for right side with sliding window
        right_window = deque(maxlen=int(window_size))
        right_set = set()  # Current window set
        
        # First, fill the initial window from right stream
        right_stream_iter = iter(loader.stream(right_source))
        for _ in range(int(window_size)):
            try:
                item = next(right_stream_iter)
                right_window.append(item)
                try:
                    if key_expr:
                        key = jaf_eval.eval(key_expr, item)
                    else:
                        key = item
                    if isinstance(key, (list, dict)):
                        key = json.dumps(key, sort_keys=True)
                    right_set.add(key)
                except Exception:
                    pass
            except StopIteration:
                break
        
        # Stream left and check membership in window
        seen = set()
        for item in loader.stream(left_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                if key in right_set and key not in seen:
                    seen.add(key)
                    yield item
            except Exception:
                pass
            
            # Slide the window
            try:
                new_item = next(right_stream_iter)
                
                # Remove oldest item from set if window is full
                if len(right_window) >= window_size:
                    old_item = right_window[0]
                    try:
                        if key_expr:
                            old_key = jaf_eval.eval(key_expr, old_item)
                        else:
                            old_key = old_item
                        if isinstance(old_key, (list, dict)):
                            old_key = json.dumps(old_key, sort_keys=True)
                        right_set.discard(old_key)
                    except Exception:
                        pass
                
                # Add new item to window and set
                right_window.append(new_item)
                try:
                    if key_expr:
                        new_key = jaf_eval.eval(key_expr, new_item)
                    else:
                        new_key = new_item
                    if isinstance(new_key, (list, dict)):
                        new_key = json.dumps(new_key, sort_keys=True)
                    right_set.add(new_key)
                except Exception:
                    pass
            except StopIteration:
                # No more right items
                pass


def stream_except(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Except/difference of two streams (items in left but not in right).

    Args:
        source: Dict with:
            - left: First source
            - right: Second source to subtract
            - key: Optional JAF expression for comparison key
            - window_size: Size of sliding window (inf for exact except)
            - strategy: Except strategy:
                - "exact": Load right side to memory (default with inf window)
                - "windowed": Use sliding window (default with finite window)
                - "probabilistic": Use Bloom filter for approximate exclusion
            - bloom_expected_items: Expected items in right stream (for probabilistic)
            - bloom_fp_rate: False positive rate (default 0.01)

    Note:
        The probabilistic strategy uses a Bloom filter to test if left items
        exist in the right stream. False positives will cause some items
        that should be included to be incorrectly excluded.
    """
    from .jaf_eval import jaf_eval
    import json
    import logging
    from collections import deque

    logger = logging.getLogger(__name__)

    left_source = source.get("left")
    right_source = source.get("right")
    key_expr = source.get("key")
    window_size = source.get("window_size", float('inf'))
    strategy = source.get("strategy")
    bloom_expected_items = source.get("bloom_expected_items", 10000)
    bloom_fp_rate = source.get("bloom_fp_rate", 0.01)

    if not left_source or not right_source:
        raise ValueError("Except source missing 'left' or 'right'")
    
    # Validate window_size
    if isinstance(window_size, str) and window_size.lower() == 'inf':
        window_size = float('inf')
    else:
        try:
            window_size = float(window_size)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid window_size: {window_size}")
    
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    # Auto-select strategy if not specified
    if strategy is None:
        if window_size == float('inf'):
            strategy = "exact"
        else:
            strategy = "windowed"

    # Probabilistic strategy using Bloom filter
    if strategy == "probabilistic":
        from .probabilistic import BloomFilter

        logger.debug(f"Using Bloom filter for probabilistic except "
                    f"(expected={bloom_expected_items}, fp_rate={bloom_fp_rate})")

        # Build Bloom filter from right stream
        bloom = BloomFilter(
            expected_items=bloom_expected_items,
            false_positive_rate=bloom_fp_rate
        )

        for item in loader.stream(right_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                bloom.add(key)
            except Exception:
                pass

        # Stream left and check non-membership in Bloom filter
        for item in loader.stream(left_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                # If not in Bloom filter, definitely not in right (no false negatives)
                if key not in bloom:
                    yield item
            except Exception:
                # On error, include the item
                yield item
        return

    if strategy == "exact" or window_size == float('inf'):
        # Exact except - load right side into memory
        logger.debug("Using infinite window for exact except")

        # Collect right stream into set
        right_set = set()
        for item in loader.stream(right_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                right_set.add(key)
            except Exception:
                pass

        # Stream left and check non-membership
        for item in loader.stream(left_source):
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item

                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)

                if key not in right_set:
                    yield item
            except Exception:
                # On error, include the item
                yield item
        return

    # Windowed except - use sliding window
    logger.debug(f"Using sliding window of size {window_size} for except")

    # Buffer for right side with sliding window
    right_window = deque(maxlen=int(window_size))
    right_set = set()  # Current window set

    # First, fill the initial window from right stream
    right_stream_iter = iter(loader.stream(right_source))
    for _ in range(int(window_size)):
        try:
            item = next(right_stream_iter)
            right_window.append(item)
            try:
                if key_expr:
                    key = jaf_eval.eval(key_expr, item)
                else:
                    key = item
                if isinstance(key, (list, dict)):
                    key = json.dumps(key, sort_keys=True)
                right_set.add(key)
            except Exception:
                pass
        except StopIteration:
            break

    # Stream left and check non-membership in window
    for item in loader.stream(left_source):
        try:
            if key_expr:
                key = jaf_eval.eval(key_expr, item)
            else:
                key = item

            if isinstance(key, (list, dict)):
                key = json.dumps(key, sort_keys=True)

            if key not in right_set:
                yield item
        except Exception:
            # On error, include the item
            yield item

        # Slide the window
        try:
            new_item = next(right_stream_iter)

            # Remove oldest item from set if window is full
            if len(right_window) >= window_size:
                old_item = right_window[0]
                try:
                    if key_expr:
                        old_key = jaf_eval.eval(key_expr, old_item)
                    else:
                        old_key = old_item
                    if isinstance(old_key, (list, dict)):
                        old_key = json.dumps(old_key, sort_keys=True)
                    right_set.discard(old_key)
                except Exception:
                    pass

            # Add new item to window and set
            right_window.append(new_item)
            try:
                if key_expr:
                    new_key = jaf_eval.eval(key_expr, new_item)
                else:
                    new_key = new_item
                if isinstance(new_key, (list, dict)):
                    new_key = json.dumps(new_key, sort_keys=True)
                right_set.add(new_key)
            except Exception:
                pass
        except StopIteration:
            # No more right items
            pass


# Register the lazy operation loaders
def register_lazy_ops_loaders(loader: "StreamingLoader"):
    """Register all lazy operation loaders with the streaming loader."""
    loader.register("take", stream_take)
    loader.register("skip", stream_skip)
    loader.register("slice", stream_slice)
    loader.register("filter", stream_filter)
    loader.register("take_while", stream_take_while)
    loader.register("skip_while", stream_skip_while)
    loader.register("batch", stream_batch)
    loader.register("enumerate", stream_enumerate)
    loader.register("map", stream_map)
    loader.register("chain", stream_chain)
    loader.register("join", stream_join)
    loader.register("groupby", stream_groupby)
    loader.register("product", stream_product)
    loader.register("distinct", stream_distinct)
    loader.register("project", stream_project)
    loader.register("union", stream_union)
    loader.register("intersect", stream_intersect)
    loader.register("except", stream_except)

"""
Lazy operation loaders that wrap other streams with additional functionality.

These loaders implement lazy operations like take, skip, etc. as composable
stream transformations.
"""

from typing import Generator, Dict, Any, Optional, List
import itertools
from .exceptions import QueryError


def stream_take(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Take the first n items from an inner source.
    
    Args:
        source: Dict with:
            - inner_source: Source to take from
            - n: Number of items to take
    """
    inner_source = source.get('inner_source')
    n = source.get('n', 10)
    
    if not inner_source:
        raise ValueError("Take source missing 'inner_source'")
    
    count = 0
    for item in loader.stream(inner_source):
        if count >= n:
            break
        yield item
        count += 1


def stream_skip(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Skip the first n items from an inner source.
    
    Args:
        source: Dict with:
            - inner_source: Source to skip from
            - n: Number of items to skip
    """
    inner_source = source.get('inner_source')
    n = source.get('n', 0)
    
    if not inner_source:
        raise ValueError("Skip source missing 'inner_source'")
    
    for i, item in enumerate(loader.stream(inner_source)):
        if i >= n:
            yield item


def stream_slice(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Slice items from an inner source.
    
    Args:
        source: Dict with:
            - inner_source: Source to slice from
            - start: Starting index
            - stop: Stopping index (exclusive), None for no limit
            - step: Step size (default: 1)
    """
    inner_source = source.get('inner_source')
    start = source.get('start', 0)
    stop = source.get('stop')
    step = source.get('step', 1)
    
    if not inner_source:
        raise ValueError("Slice source missing 'inner_source'")
    
    for item in itertools.islice(loader.stream(inner_source), start, stop, step):
        yield item


def stream_filter(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Filter items from an inner source using a JAF query.
    
    Args:
        source: Dict with:
            - inner_source: Source to filter
            - query: JAF query to use as predicate
    """
    from .jaf_eval import jaf_eval
    
    inner_source = source.get('inner_source')
    query = source.get('query')
    
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


def stream_take_while(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Take items while a predicate query is true.
    
    Args:
        source: Dict with:
            - inner_source: Source to take from
            - query: JAF query to use as predicate
    """
    from .jaf_eval import jaf_eval
    
    inner_source = source.get('inner_source')
    query = source.get('query')
    
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


def stream_skip_while(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Skip items while a predicate query is true.
    
    Args:
        source: Dict with:
            - inner_source: Source to skip from
            - query: JAF query to use as predicate
    """
    from .jaf_eval import jaf_eval
    
    inner_source = source.get('inner_source')
    query = source.get('query')
    
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


def stream_batch(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Batch items from an inner source into groups.
    
    Args:
        source: Dict with:
            - inner_source: Source to batch
            - size: Batch size
    """
    inner_source = source.get('inner_source')
    size = source.get('size', 10)
    
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


def stream_enumerate(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Enumerate items with an index.
    
    Args:
        source: Dict with:
            - inner_source: Source to enumerate
            - start: Starting index (default: 0)
            - as_dict: If True, yield {"index": i, "value": item}, else yield [i, item]
    """
    inner_source = source.get('inner_source')
    start = source.get('start', 0)
    as_dict = source.get('as_dict', True)
    
    if not inner_source:
        raise ValueError("Enumerate source missing 'inner_source'")
    
    for i, item in enumerate(loader.stream(inner_source), start=start):
        if as_dict:
            yield {"index": i, "value": item}
        else:
            yield [i, item]


def stream_map(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Transform items using a JAF query/expression.
    
    Args:
        source: Dict with:
            - inner_source: Source to map
            - expression: JAF expression to evaluate for each item
    """
    from .jaf_eval import jaf_eval
    
    inner_source = source.get('inner_source')
    expression = source.get('expression')
    
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


def stream_chain(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Chain multiple sources sequentially.
    
    Args:
        source: Dict with:
            - sources: List of sources to chain
    """
    sources = source.get('sources', [])
    
    for src in sources:
        yield from loader.stream(src)


def stream_join(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Join two streams based on a key expression.
    
    Args:
        source: Dict with:
            - left: Left source
            - right: Right source  
            - on: JAF expression to extract join key
            - how: Join type ("inner", "left", "right", "outer")
    """
    from .jaf_eval import jaf_eval
    
    left_source = source.get('left')
    right_source = source.get('right')
    on_expr = source.get('on')
    how = source.get('how', 'inner')
    
    if not left_source or not right_source:
        raise ValueError("Join source missing 'left' or 'right'")
    if not on_expr:
        raise ValueError("Join source missing 'on' expression")
    
    # For now, implement a simple in-memory hash join
    # TODO: For large streams, consider sort-merge join or nested loop with buffering
    
    # Build index from right stream
    right_index = {}
    for item in loader.stream(right_source):
        try:
            key = jaf_eval.eval(on_expr, item)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(item)
        except Exception:
            pass
    
    # Stream left and join
    for left_item in loader.stream(left_source):
        try:
            key = jaf_eval.eval(on_expr, left_item)
            right_matches = right_index.get(key, [])
            
            if right_matches:
                # Inner join - output combinations
                for right_item in right_matches:
                    yield {
                        "left": left_item,
                        "right": right_item
                    }
            elif how in ('left', 'outer'):
                # Left join - output with null right
                yield {
                    "left": left_item,
                    "right": None
                }
        except Exception:
            if how in ('left', 'outer'):
                yield {
                    "left": left_item,
                    "right": None
                }
    
    # For right/outer joins, emit unmatched right items
    if how in ('right', 'outer'):
        # Need to track which right items were matched
        # This simple implementation doesn't do that efficiently
        # TODO: Improve join implementation
        pass


def stream_groupby(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Group items from a stream by a key expression.
    
    Args:
        source: Dict with:
            - inner_source: Source to group
            - key: JAF expression to extract grouping key
            - aggregate: Optional aggregation operations (dict mapping field names to ops)
    
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
    
    inner_source = source.get('inner_source')
    key_expr = source.get('key')
    aggregate = source.get('aggregate', {})
    
    if not inner_source:
        raise ValueError("Groupby source missing 'inner_source'")
    if not key_expr:
        raise ValueError("Groupby source missing 'key' expression")
    
    # Collect all items into groups
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
    
    # Yield groups with optional aggregation
    for key, items in groups.items():
        result = {
            "key": key,
            "items": items,
            "count": len(items)
        }
        
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


def stream_product(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Compute the Cartesian product of two streams.
    
    Args:
        source: Dict with:
            - left: Left source
            - right: Right source
            - limit: Optional limit on output size (since product can be very large)
    """
    left_source = source.get('left')
    right_source = source.get('right')
    limit = source.get('limit')
    
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
            
            yield {
                "left": left_item,
                "right": right_item
            }
            count += 1


def stream_distinct(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Remove duplicate items from a stream.
    
    Args:
        source: Dict with:
            - inner_source: Source to deduplicate
            - key: Optional JAF expression to extract uniqueness key
                  If not provided, uses the entire item
    """
    from .jaf_eval import jaf_eval
    import json
    
    inner_source = source.get('inner_source')
    key_expr = source.get('key')
    
    if not inner_source:
        raise ValueError("Distinct source missing 'inner_source'")
    
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


def stream_project(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
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
    
    inner_source = source.get('inner_source')
    fields = source.get('fields', {})
    
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


def stream_union(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Union multiple streams (concatenate with optional deduplication).
    
    Args:
        source: Dict with:
            - sources: List of sources to union
            - distinct: If True, remove duplicates (default: False)
    """
    import json
    
    sources = source.get('sources', [])
    distinct = source.get('distinct', False)
    
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


def stream_intersect(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Intersect two streams (items that appear in both).
    
    Args:
        source: Dict with:
            - left: First source
            - right: Second source
            - key: Optional JAF expression for comparison key
    """
    from .jaf_eval import jaf_eval
    import json
    
    left_source = source.get('left')
    right_source = source.get('right')
    key_expr = source.get('key')
    
    if not left_source or not right_source:
        raise ValueError("Intersect source missing 'left' or 'right'")
    
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


def stream_except(loader: 'StreamingLoader', source: Dict[str, Any]) -> Generator[Any, None, None]:
    """
    Except/difference of two streams (items in left but not in right).
    
    Args:
        source: Dict with:
            - left: First source
            - right: Second source to subtract
            - key: Optional JAF expression for comparison key
    """
    from .jaf_eval import jaf_eval
    import json
    
    left_source = source.get('left')
    right_source = source.get('right')
    key_expr = source.get('key')
    
    if not left_source or not right_source:
        raise ValueError("Except source missing 'left' or 'right'")
    
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


# Register the lazy operation loaders
def register_lazy_ops_loaders(loader: 'StreamingLoader'):
    """Register all lazy operation loaders with the streaming loader."""
    loader.register('take', stream_take)
    loader.register('skip', stream_skip)
    loader.register('slice', stream_slice)
    loader.register('filter', stream_filter)
    loader.register('take_while', stream_take_while)
    loader.register('skip_while', stream_skip_while)
    loader.register('batch', stream_batch)
    loader.register('enumerate', stream_enumerate)
    loader.register('map', stream_map)
    loader.register('chain', stream_chain)
    loader.register('join', stream_join)
    loader.register('groupby', stream_groupby)
    loader.register('product', stream_product)
    loader.register('distinct', stream_distinct)
    loader.register('project', stream_project)
    loader.register('union', stream_union)
    loader.register('intersect', stream_intersect)
    loader.register('except', stream_except)
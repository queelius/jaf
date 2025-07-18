# Advanced Topics

This guide covers advanced JAF concepts, performance optimization, and extending the system.

## Understanding the Streaming Architecture

### How Lazy Evaluation Works

JAF builds a pipeline of operations without executing them:

```python
# This creates a pipeline description, no data is processed
pipeline = stream("huge_file.jsonl") \
    .filter(["gt?", "@value", 100]) \
    .map("@id") \
    .take(10)

# The pipeline is a tree of source descriptors
print(pipeline.to_dict())
# {
#   "type": "take",
#   "n": 10,
#   "inner_source": {
#     "type": "map",
#     "expression": ["@", [["key", "id"]]],
#     "inner_source": {
#       "type": "filter",
#       "query": ["gt?", "@value", 100],
#       "inner_source": {
#         "type": "jsonl",
#         "inner_source": {
#           "type": "file",
#           "path": "huge_file.jsonl"
#         }
#       }
#     }
#   }
# }
```

### Stream Processing Internals

When you call `.evaluate()`, JAF:

1. Traverses the source tree from outer to inner
2. Each operation requests items from its inner source
3. Items flow through the pipeline one at a time
4. Operations can short-circuit (e.g., `take` stops after N items)

```python
# This processes only enough items to find 10 matches
for item in pipeline.evaluate():
    print(item)  # Prints at most 10 items
```

## Performance Optimization

### Memory-Efficient Patterns

```python
# Good: Streaming one item at a time
def process_large_file(path):
    for item in stream(path).filter(expr).evaluate():
        process(item)

# Bad: Loading everything into memory
def process_large_file(path):
    all_items = list(stream(path).filter(expr).evaluate())
    for item in all_items:
        process(item)
```

### Filter Optimization

Order filters from most to least selective:

```python
# Efficient: Selective filter first
stream("data.jsonl") \
    .filter(["eq?", "@type", "purchase"])  # Filters out 90% of data
    .filter(["gt?", "@amount", 1000])      # Expensive calculation on 10%

# Inefficient: Expensive filter first  
stream("data.jsonl") \
    .filter(["gt?", "@amount", 1000])      # Calculates on 100% of data
    .filter(["eq?", "@type", "purchase"])  # Then filters 90%
```

### Avoiding Redundant Operations

```python
# Inefficient: Multiple passes through data
active_users = list(stream("users.jsonl").filter(["eq?", "@active", true]).evaluate())
premium_users = list(stream("users.jsonl").filter(["eq?", "@plan", "premium"]).evaluate())

# Efficient: Single pass with complex filter
users = stream("users.jsonl")
active_users = []
premium_users = []

for user in users.evaluate():
    if user.get("active"):
        active_users.append(user)
    if user.get("plan") == "premium":
        premium_users.append(user)
```

## Working with Infinite Streams

### Codata Sources

JAF supports infinite data sources:

```python
# Fibonacci sequence
fibs = stream({"type": "fibonacci"})

# Take only what you need
first_100 = fibs.take(100).evaluate()

# Find first Fibonacci > 1000
first_large = next(
    fibs.filter(["gt?", "@value", 1000]).evaluate()
)
```

### Creating Custom Infinite Sources

```python
def register_counter_source(loader):
    """Register a simple counter source"""
    def counter_generator(start=0, step=1):
        n = start
        while True:
            yield {"value": n, "index": (n - start) // step}
            n += step
    
    def load_counter(loader, source):
        start = source.get("start", 0)
        step = source.get("step", 1)
        return counter_generator(start, step)
    
    loader.streaming_loader.register_loader("counter", load_counter)

# Usage
counter = stream({"type": "counter", "start": 100, "step": 5})
# Generates: {"value": 100}, {"value": 105}, {"value": 110}, ...
```

## Extending JAF

### Custom Operators

You can add custom operators by extending the evaluation logic:

```python
from jaf.jaf_eval import jaf_eval
from jaf.exceptions import InvalidArgumentCountError

def custom_stddev(args, obj):
    """Calculate standard deviation of array"""
    if len(args) != 1:
        raise InvalidArgumentCountError("stddev", 1, len(args))
    
    values = jaf_eval.eval(args[0], obj)
    if not isinstance(values, list):
        return None
    
    if len(values) == 0:
        return 0
    
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return variance ** 0.5

# Register the operator
# Note: This requires modifying jaf_eval.py
# Better approach: Use map with custom Python functions
```

### Custom Stream Operations

Create new stream operations by composing existing ones:

```python
class ExtendedStream(LazyDataStream):
    def sample(self, rate=0.1, seed=None):
        """Random sampling of stream"""
        import hashlib
        
        def should_include(item):
            # Deterministic sampling based on item hash
            item_str = json.dumps(item, sort_keys=True)
            if seed:
                item_str = f"{seed}:{item_str}"
            hash_val = int(hashlib.md5(item_str.encode()).hexdigest(), 16)
            return (hash_val % 10000) < (rate * 10000)
        
        return self.filter(["custom", should_include])
    
    def deduplicate(self, key="@id"):
        """Remove duplicates based on key"""
        seen = set()
        
        def is_unique(item):
            val = jaf_eval.eval(key, item)
            if val in seen:
                return False
            seen.add(val)
            return True
        
        return self.filter(["custom", is_unique])
```

## Complex Data Processing Patterns

### Sessionization

Group events into sessions based on time gaps:

```python
def sessionize_events(events_file, gap_seconds=1800):
    """Group events into sessions with 30-minute gaps"""
    
    current_session = []
    last_timestamp = None
    
    events = stream(events_file) \
        .filter(["exists?", "@timestamp"]) \
        .map(["dict", 
            "time", "@timestamp",
            "user", "@user_id",
            "action", "@action",
            "data", "@"
        ])
    
    for event in events.evaluate():
        timestamp = event["time"]
        
        if last_timestamp and (timestamp - last_timestamp) > gap_seconds:
            # Gap detected, yield session
            if current_session:
                yield {
                    "session_id": f"{current_session[0]['user']}_{current_session[0]['time']}",
                    "user": current_session[0]["user"],
                    "start": current_session[0]["time"],
                    "end": current_session[-1]["time"],
                    "duration": current_session[-1]["time"] - current_session[0]["time"],
                    "events": current_session
                }
            current_session = []
        
        current_session.append(event)
        last_timestamp = timestamp
    
    # Don't forget the last session
    if current_session:
        yield {
            "session_id": f"{current_session[0]['user']}_{current_session[0]['time']}",
            "user": current_session[0]["user"],
            "start": current_session[0]["time"],
            "end": current_session[-1]["time"],
            "duration": current_session[-1]["time"] - current_session[0]["time"],
            "events": current_session
        }
```

### Streaming Joins

While JAF doesn't have built-in joins (use `ja` for that), you can implement simple streaming joins:

```python
def stream_join_small_lookup(main_file, lookup_file, join_key):
    """Join stream with small lookup table"""
    
    # Load lookup table into memory
    lookup = {}
    for item in stream(lookup_file).evaluate():
        key = jaf_eval.eval(join_key, item)
        lookup[key] = item
    
    # Stream through main file and join
    return stream(main_file) \
        .map(["dict",
            "main", "@",
            "lookup", ["get", lookup, jaf_eval.eval(join_key, "@")]
        ]) \
        .filter(["exists?", "@lookup"])
```

## Debugging and Monitoring

### Pipeline Inspection

```python
def inspect_pipeline(pipeline, sample_size=5):
    """Inspect intermediate results in a pipeline"""
    
    # Get pipeline info
    info = pipeline.info()
    print(f"Pipeline structure: {info['pipeline']}")
    
    # Sample some results
    print(f"\nFirst {sample_size} results:")
    for i, item in enumerate(pipeline.evaluate()):
        if i >= sample_size:
            break
        print(f"{i}: {json.dumps(item, indent=2)}")
```

### Performance Profiling

```python
import time
from contextlib import contextmanager

@contextmanager
def time_pipeline(name):
    """Time pipeline execution"""
    start = time.time()
    count = 0
    
    def counter(items):
        nonlocal count
        for item in items:
            count += 1
            yield item
    
    yield counter
    
    elapsed = time.time() - start
    print(f"{name}: processed {count} items in {elapsed:.2f}s ({count/elapsed:.0f} items/s)")

# Usage
with time_pipeline("Filter operation") as counter:
    results = list(counter(
        stream("large_file.jsonl")
        .filter(complex_query)
        .evaluate()
    ))
```

### Error Tracking

```python
def safe_pipeline(pipeline, error_file="errors.jsonl"):
    """Execute pipeline with error tracking"""
    
    error_count = 0
    
    with open(error_file, 'w') as f:
        for i, item in enumerate(pipeline.evaluate()):
            try:
                yield item
            except Exception as e:
                error_count += 1
                error_record = {
                    "index": i,
                    "error": str(e),
                    "type": type(e).__name__,
                    "item": item
                }
                f.write(json.dumps(error_record) + '\n')
    
    if error_count > 0:
        print(f"Warning: {error_count} errors logged to {error_file}")
```

## Integration with Data Science Tools

### Pandas Integration

```python
import pandas as pd

def jaf_to_dataframe(pipeline, max_rows=None):
    """Convert JAF pipeline results to pandas DataFrame"""
    
    if max_rows:
        data = list(pipeline.take(max_rows).evaluate())
    else:
        data = list(pipeline.evaluate())
    
    return pd.DataFrame(data)

# Usage
df = jaf_to_dataframe(
    stream("sales.jsonl")
    .filter(["gt?", "@amount", 100])
    .map(["dict", 
        "date", "@date",
        "amount", "@amount",
        "category", "@product.category"
    ])
)

# Now use pandas for analytics
monthly_sales = df.groupby(pd.to_datetime(df['date']).dt.to_period('M'))['amount'].sum()
```

### DuckDB Integration

```python
import duckdb

def jaf_to_duckdb(pipeline, table_name="jaf_data"):
    """Stream JAF results into DuckDB"""
    
    conn = duckdb.connect()
    
    # Create table from first item
    first_item = next(pipeline.evaluate(), None)
    if not first_item:
        return conn
    
    # Infer schema and create table
    columns = []
    for key, value in first_item.items():
        if isinstance(value, (int, float)):
            columns.append(f"{key} NUMERIC")
        else:
            columns.append(f"{key} VARCHAR")
    
    conn.execute(f"CREATE TABLE {table_name} ({', '.join(columns)})")
    
    # Insert data in batches
    batch = [first_item]
    for item in pipeline.evaluate():
        batch.append(item)
        if len(batch) >= 1000:
            conn.executemany(f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(columns))})", 
                           [[item.get(k) for k in first_item.keys()] for item in batch])
            batch = []
    
    # Insert remaining
    if batch:
        conn.executemany(f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(columns))})", 
                       [[item.get(k) for k in first_item.keys()] for item in batch])
    
    return conn

# Usage
conn = jaf_to_duckdb(
    stream("transactions.jsonl")
    .filter(["eq?", "@status", "completed"])
)

# Now use SQL
result = conn.execute("""
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(amount) as total
    FROM jaf_data
    GROUP BY month
    ORDER BY month
""").fetchall()
```

## Best Practices for Production

1. **Use Type Hints**: Add type hints to your pipeline functions
2. **Validate Early**: Add validation filters at the start of pipelines
3. **Monitor Memory**: Use batching for operations that accumulate state
4. **Handle Errors**: Use try-except in custom operations
5. **Document Pipelines**: Save complex pipelines with descriptive names
6. **Test with Samples**: Always test on small data before running on full datasets
7. **Profile Performance**: Measure throughput for critical pipelines
8. **Version Control**: Track pipeline definitions in git

## Future Enhancements

The JAF ecosystem continues to evolve. Some areas being explored:

- **Windowed Operations**: Time or count-based windows for streaming aggregations
- **Parallel Processing**: Multi-threaded evaluation for CPU-bound operations
- **Schema Validation**: Built-in JSON Schema validation
- **Pipeline Optimization**: Automatic query reordering for performance
- **Extended Type System**: Better support for custom types and validations

For the latest updates and to contribute, visit the [JAF GitHub repository](https://github.com/queelius/jaf).
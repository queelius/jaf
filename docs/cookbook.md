# JAF Cookbook

This cookbook provides practical examples and patterns for common data processing tasks with JAF.

## Log Analysis

### Finding Errors in Log Files

```python
from jaf import stream

# Find all error logs
errors = stream("app.log.jsonl") \
    .filter(["eq?", "@level", "ERROR"]) \
    .evaluate()

# Find errors from specific services
critical_errors = stream("app.log.jsonl") \
    .filter(["and",
        ["eq?", "@level", "ERROR"],
        ["in?", "@service", ["payment", "auth", "api"]]
    ]) \
    .evaluate()
```

### Extracting Error Summaries

```python
# Create error summaries with relevant info
error_summaries = stream("logs.jsonl") \
    .filter(["eq?", "@level", "ERROR"]) \
    .map(["dict",
        "time", "@timestamp",
        "service", "@service",
        "message", "@message",
        "trace_id", "@metadata.trace_id"
    ]) \
    .evaluate()
```

### Time-Based Log Filtering

```python
# Errors from the last 24 hours
recent_errors = stream("logs.jsonl") \
    .filter(["and",
        ["eq?", "@level", "ERROR"],
        ["gt?", "@timestamp", ["date-diff", ["now"], 86400]]  # 86400 seconds = 24 hours
    ]) \
    .evaluate()
```

## User Data Processing

### Active User Analysis

```python
# Find active users with recent activity
active_users = stream("users.jsonl") \
    .filter(["and",
        ["eq?", "@status", "active"],
        ["gt?", "@last_login", "2024-01-01"],
        ["gt?", "@total_purchases", 0]
    ]) \
    .map(["dict",
        "id", "@id",
        "email", "@email",
        "days_since_login", ["days", ["date-diff", ["now"], "@last_login"]]
    ]) \
    .evaluate()
```

### User Segmentation

```python
# Segment users by engagement level
def segment_users(file_path):
    users = stream(file_path)
    
    # High-value users
    high_value = users.filter(["and",
        ["gt?", "@total_spent", 1000],
        ["gt?", "@purchase_count", 10]
    ])
    
    # At-risk users (inactive for 30+ days)
    at_risk = users.filter(["and",
        ["eq?", "@status", "active"],
        ["lt?", "@last_login", ["date-diff", ["now"], 2592000]]  # 30 days
    ])
    
    # New users (joined in last 7 days)
    new_users = users.filter(
        ["gt?", "@created_at", ["date-diff", ["now"], 604800]]  # 7 days
    )
    
    return {
        "high_value": list(high_value.evaluate()),
        "at_risk": list(at_risk.evaluate()),
        "new": list(new_users.evaluate())
    }
```

## Data Validation

### Finding Invalid Records

```python
# Find records with missing required fields
invalid_records = stream("data.jsonl") \
    .filter(["or",
        ["not", ["exists?", "@id"]],
        ["not", ["exists?", "@email"]],
        ["not", ["regex-match?", "@email", "^[^@]+@[^@]+\\.[^@]+$"]]
    ]) \
    .map(["dict",
        "original_id", "@id",
        "issues", ["if",
            ["not", ["exists?", "@id"]], "missing_id",
            ["if",
                ["not", ["exists?", "@email"]], "missing_email",
                "invalid_email"
            ]
        ]
    ]) \
    .evaluate()
```

### Data Quality Checks

```python
# Check for suspicious data patterns
suspicious = stream("transactions.jsonl") \
    .filter(["or",
        ["gt?", "@amount", 10000],           # Unusually high amount
        ["lt?", "@amount", 0],               # Negative amount
        ["not", ["exists?", "@customer_id"]], # Missing customer
        ["and",                              # Multiple transactions in short time
            ["eq?", "@customer_id", "@previous.customer_id"],
            ["lt?", ["date-diff", "@timestamp", "@previous.timestamp"], 60]
        ]
    ]) \
    .evaluate()
```

## ETL Patterns

### Data Transformation Pipeline

```python
# Transform raw data for analytics
def transform_sales_data(input_file, output_file):
    pipeline = stream(input_file) \
        .filter(["eq?", "@status", "completed"]) \
        .map(["dict",
            "order_id", "@id",
            "customer_id", "@customer.id",
            "date", ["date", "@completed_at"],
            "amount", "@total_amount",
            "items_count", ["length", "@items"],
            "category", ["if",
                ["gt?", "@total_amount", 1000], "high_value",
                ["if",
                    ["gt?", "@total_amount", 100], "medium_value",
                    "low_value"
                ]
            ]
        ])
    
    # Write transformed data
    with open(output_file, 'w') as f:
        for item in pipeline.evaluate():
            f.write(json.dumps(item) + '\n')
```

### Data Enrichment

```python
# Enrich data with calculated fields
enriched_products = stream("products.jsonl") \
    .map(["dict",
        "id", "@id",
        "name", "@name",
        "price", "@price",
        "discount_price", ["*", "@price", 0.9],
        "margin", ["-", "@price", "@cost"],
        "margin_percent", ["*", ["/", ["-", "@price", "@cost"], "@price"], 100],
        "in_stock", ["gt?", "@quantity", 0],
        "low_stock", ["and",
            ["gt?", "@quantity", 0],
            ["lt?", "@quantity", "@reorder_point"]
        ]
    ]) \
    .evaluate()
```

## Working with Nested Data

### Flattening Nested Structures

```python
# Extract nested data into flat structure
flattened = stream("orders.jsonl") \
    .map(["dict",
        "order_id", "@id",
        "customer_name", "@customer.name",
        "customer_email", "@customer.email",
        "shipping_city", "@shipping.city",
        "shipping_state", "@shipping.state",
        "total_items", ["length", "@items"],
        "item_categories", ["unique", "@items.*.category"]
    ]) \
    .evaluate()

# Filter flattened results to find large orders from a specific city
large_seattle_orders = stream("orders.jsonl") \
    .filter(["and",
        ["eq?", "@shipping.city", "Seattle"],
        ["gt?", ["length", "@items"], 5]
    ]) \
    .map(["dict",
        "order_id", "@id",
        "customer_name", "@customer.name",
        "total_items", ["length", "@items"]
    ]) \
    .evaluate()
```

### Processing Arrays within Documents

JAF's wildcard paths (`@items.*.field`) extract values from arrays of objects. Combined with aggregate operators, you can compute over nested arrays:

```python
# Calculate order statistics using wildcard paths
order_stats = stream("orders.jsonl") \
    .map(["dict",
        "order_id", "@id",
        "item_count", ["length", "@items"],
        "categories", ["unique", "@items.*.category"]
    ]) \
    .evaluate()
```

!!! note "Nested array limitations"
    Operations like `["sum", "@items.*.price"]` work when the wildcard path resolves to a flat list. For more complex nested array processing (filtering within arrays, conditional aggregation), extract the data with JAF and process in Python.

## Performance Patterns

### Processing Large Files

```python
# Process in batches to control memory usage
def process_large_file(file_path, batch_size=1000):
    batches = stream(file_path) \
        .filter(["eq?", "@type", "transaction"]) \
        .batch(batch_size)
    
    for batch in batches.evaluate():
        # Process batch (list of items)
        results = bulk_process(batch)
        save_results(results)
```

### Early Termination

```python
# Find first matching item
first_error = next(
    stream("logs.jsonl")
    .filter(["eq?", "@level", "CRITICAL"])
    .evaluate(),
    None
)

# Check if any items match
has_errors = any(
    stream("logs.jsonl")
    .filter(["eq?", "@level", "ERROR"])
    .take(1)
    .evaluate()
)
```

### Sampling Data

```python
# Sample every 100th record
sampled = stream("large_dataset.jsonl") \
    .enumerate() \
    .filter(["eq?", ["%", "@index", 100], 0]) \
    .map("@value") \
    .evaluate()

# Deterministic sampling using modulo on a numeric ID
id_sample = stream("data.jsonl") \
    .filter(["eq?", ["%", "@id", 10], 0]) \
    .evaluate()  # ~10% sample (items where id % 10 == 0)
```

## Integration Patterns

### Preparing Data for Other Tools

```python
# Prepare data for jsonl-algebra aggregation
def prepare_for_aggregation(input_file):
    return stream(input_file) \
        .filter(["exists?", "@revenue"]) \
        .map(["dict",
            "date", ["date", "@timestamp"],
            "category", "@product.category",
            "revenue", "@revenue",
            "region", "@store.region"
        ])

# Usage with jsonl-algebra
# python script.py | ja groupby category --aggregate 'total:revenue:sum'
```

### Multi-Stage Processing

```bash
#!/bin/bash
# Complex multi-stage pipeline

# Stage 1: Filter and transform with JAF
jaf stream raw_logs.jsonl \
    --filter '["in?", "@level", ["ERROR", "CRITICAL"]]' \
    --map '["dict", "time", "@timestamp", "service", "@service", "error", "@message"]' \
    > errors.jsonl

# Stage 2: Group by service with jsonl-algebra
ja groupby service --aggregate 'count:count' < errors.jsonl > error_counts.jsonl

# Stage 3: Find top error services
jaf filter error_counts.jsonl '["gt?", "@count", 100]' --eval | \
ja sort count --reverse | \
head -10
```

## Advanced Patterns

### Stateful Processing with Windowing

```python
# Detect anomalies using rolling statistics
def detect_anomalies(metric_file, window_size=100):
    """Detect values outside 3 standard deviations"""
    
    windowed = stream(metric_file) \
        .enumerate() \
        .batch(window_size)
    
    for window in windowed.evaluate():
        values = [item["value"]["metric"] for item in window]
        mean = sum(values) / len(values)
        std_dev = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
        
        for item in window:
            if abs(item["value"]["metric"] - mean) > 3 * std_dev:
                yield {
                    **item["value"],
                    "anomaly": True,
                    "deviation": (item["value"]["metric"] - mean) / std_dev
                }
```

### Custom Stream Sources

```python
# Create custom streaming source
def csv_to_json_stream(csv_file):
    """Convert CSV to JSON stream compatible with JAF"""
    import csv
    
    data = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric strings to numbers
            for key, value in row.items():
                try:
                    row[key] = float(value)
                except ValueError:
                    pass  # Keep as string
            data.append(row)
    
    # Use with JAF
    return stream({"type": "memory", "data": data})

# Process CSV data
results = csv_to_json_stream("sales.csv") \
    .filter(["gt?", "@revenue", 1000]) \
    .map(["dict", "month", "@date", "revenue", "@revenue"]) \
    .evaluate()
```

## Tips and Best Practices

1. **Use generators for large datasets**: JAF returns generators, so you can process items one at a time
2. **Filter early**: Apply selective filters before expensive transformations
3. **Leverage laziness**: Build complex pipelines without worrying about performance until evaluation
4. **Combine with other tools**: JAF works great with `ja`, `jq`, and standard Unix tools
5. **Save pipelines**: Complex pipelines can be saved as JSON and reused
6. **Test with small data**: Use `head` or `take` to test pipelines on small samples first
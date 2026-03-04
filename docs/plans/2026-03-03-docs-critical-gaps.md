# Fix Critical Documentation Gaps — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the three worst documentation files (api-guide.md, cli-reference.md, cookbook.md) to accurately document JAF v0.8.0 features.

**Architecture:** Pure documentation edits — no code changes. Each task targets one file. All content is derived from actual method signatures in `jaf/lazy_streams.py` and argparse definitions in `jaf/console_script.py`.

**Tech Stack:** Markdown (MkDocs Material theme), verified against Python source

---

### Task 1: api-guide.md — Add Set and Aggregation Operations section

**Files:**
- Modify: `docs/api-guide.md` (insert after line ~204, the Enumeration section)

**Step 1: Fix the typo on line 54**

Change `Alterntively` → `Alternatively`

**Step 2: Insert the "Set and Aggregation Operations" section after the Enumeration section (after line 204)**

Insert the following markdown after the `#### Enumeration` section (after the `# Results in: {"index": 0, "value": <original_item>}` line):

````markdown
## Set and Aggregation Operations

These operations work on relationships between items within or across streams.

### Distinct

Remove duplicate items from a stream. Supports three strategies for different memory/accuracy trade-offs.

```python
# Basic: remove exact duplicates
unique_items = stream("events.jsonl").distinct()

# Key-based: deduplicate by a specific field
unique_users = stream("logins.jsonl").distinct(key="@user_id")

# Windowed: bounded memory, only remembers last N items
recent_unique = stream("events.jsonl").distinct(
    key="@event_id",
    window_size=10000
)

# Probabilistic: uses a Bloom filter for very large streams
# No false negatives (never misses a duplicate it saw)
# Small chance of false positives (may discard a unique item)
approx_unique = stream("huge.jsonl").distinct(
    key="@user_id",
    strategy="probabilistic",
    bloom_expected_items=1_000_000,
    bloom_fp_rate=0.01  # 1% false positive rate
)
```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `key` | `None` | JAF expression to extract uniqueness key. `None` compares whole items. |
| `window_size` | `inf` | Sliding window size. `inf` = exact (unbounded memory). |
| `strategy` | `None` | `"exact"`, `"windowed"`, or `"probabilistic"`. Auto-detected from other params if not set. |
| `bloom_expected_items` | `10000` | Expected item count for Bloom filter sizing (probabilistic only). |
| `bloom_fp_rate` | `0.01` | Target false positive rate (probabilistic only). |

### Group By

Group items by a key expression, optionally computing aggregates per group.

```python
# Group logs by level
grouped = stream("logs.jsonl").groupby(key="@level")
# Yields: {"key": "ERROR", "items": [...]}, {"key": "INFO", "items": [...]}, ...

# Group with aggregation
stats = stream("sales.jsonl").groupby(
    key="@category",
    aggregate={
        "total": ["sum", "@amount"],
        "avg_price": ["mean", "@price"],
        "num_orders": ["count", "@id"]
    }
)
# Yields: {"key": "electronics", "total": 50000, "avg_price": 299.99, "num_orders": 167}

# Tumbling window groupby (bounded memory)
windowed_stats = stream("metrics.jsonl").groupby(
    key="@service",
    aggregate={"count": ["count", "@id"]},
    window_size=1000  # Emit groups every 1000 items
)
```

**Aggregate operators:** `sum`, `mean`, `count` — each takes a path expression for the field to aggregate.

### Join

Join two streams on matching keys.

```python
users = stream("users.jsonl")
orders = stream("orders.jsonl")

# Inner join (only matching pairs)
user_orders = users.join(orders, on="@user_id")

# Left join (all users, orders where available)
all_users = users.join(orders, on="@user_id", how="left")

# Asymmetric keys (different field names in each stream)
result = users.join(orders, on="@id", on_right="@customer_id")

# Right join and outer join
right = users.join(orders, on="@user_id", how="right")
full = users.join(orders, on="@user_id", how="outer")

# Windowed join (bounded memory)
windowed = users.join(orders, on="@user_id", window_size=5000)
```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `on` | required | JAF expression for left stream's join key |
| `on_right` | same as `on` | JAF expression for right stream's join key |
| `how` | `"inner"` | Join type: `"inner"`, `"left"`, `"right"`, `"outer"` |
| `window_size` | `inf` | Sliding window size for bounded-memory joins |

### Intersect

Items that appear in both streams.

```python
list_a = stream("customers_2024.jsonl")
list_b = stream("customers_2025.jsonl")

# Exact intersection
returning = list_a.intersect(list_b)

# Key-based intersection
returning_by_email = list_a.intersect(list_b, key="@email")

# Probabilistic for large streams
returning_approx = list_a.intersect(
    list_b,
    key="@email",
    strategy="probabilistic",
    bloom_expected_items=500_000
)
```

### Except (Set Difference)

Items in the first stream that are not in the second.

```python
all_users = stream("all_users.jsonl")
blocked = stream("blocked_users.jsonl")

# Users who are not blocked
active = all_users.except_from(blocked, key="@user_id")

# Probabilistic for large streams
active_approx = all_users.except_from(
    blocked,
    key="@user_id",
    strategy="probabilistic",
    bloom_expected_items=100_000
)
```

`intersect()` and `except_from()` accept the same `key`, `window_size`, `strategy`, `bloom_expected_items`, and `bloom_fp_rate` parameters as `distinct()`.

## Windowed Operations

Several operations (`distinct`, `groupby`, `join`, `intersect`, `except_from`) support a `window_size` parameter that controls the memory/accuracy trade-off:

| Strategy | `window_size` | Memory | Accuracy | Use When |
|----------|--------------|--------|----------|----------|
| Exact | `inf` (default) | Unbounded | 100% | Data fits in memory |
| Windowed | Finite (e.g., 10000) | Bounded | Approximate | Large streams with temporal locality |
| Probabilistic | N/A | Bounded | ~99%+ configurable | Very large streams, slight inaccuracy acceptable |

!!! warning "Windowed intersect/except"
    For windowed `intersect()` and `except_from()`, the window must be large enough to capture overlapping items between the two streams. If matching items are far apart, they may be missed.
````

**Step 3: Verify the doc builds**

Run: `venv/bin/mkdocs build 2>&1 | tail -5`
Expected: Build succeeds without errors

**Step 4: Commit**

```
git add docs/api-guide.md
git commit -m "docs: add set operations, windowed ops, and aggregation to API guide"
```

---

### Task 2: cli-reference.md — Add `distinct` command and update `stream` flags

**Files:**
- Modify: `docs/cli-reference.md`

**Step 1: Fix the typo on line 71**

Change `["* ", "@count", 2]` → `["*", "@count", 2]` (remove extra space in operator)

**Step 2: Insert `distinct` command section after the `batch` section (after line ~125)**

Insert after the `### batch` section:

````markdown
### distinct

Remove duplicate items from a stream.

```bash
jaf distinct <input> [--key EXPR] [--strategy {exact,windowed,probabilistic}] [--window-size N] [--bloom-expected-items N] [--bloom-fp-rate RATE] [--eval]
```

**Options:**
- `--key`, `-k`: Key expression for deduplication (e.g., `@user_id`)
- `--strategy`, `-s`: `exact` (default), `windowed`, or `probabilistic`
- `--window-size`: Number of items to remember (windowed strategy)
- `--bloom-expected-items`: Expected item count for Bloom filter sizing (probabilistic)
- `--bloom-fp-rate`: Target false positive rate, default `0.01` (probabilistic)
- `--eval`: Evaluate immediately

**Examples:**
```bash
# Remove exact duplicates
jaf distinct events.jsonl --eval

# Deduplicate by email field
jaf distinct users.jsonl --key '@email' --eval

# Memory-efficient dedup for large files
jaf distinct huge.jsonl --key '@id' \
    --strategy probabilistic \
    --bloom-expected-items 1000000 \
    --eval

# Windowed dedup (remember last 5000 items)
jaf distinct logs.jsonl --key '@request_id' \
    --strategy windowed \
    --window-size 5000 \
    --eval

# Chain with other operations
jaf filter users.jsonl '["eq?", "@active", true]' | \
jaf distinct - --key '@email' --eval
```
````

**Step 3: Update the `stream` command section**

In the `### stream` section, add these to the **Options** list (after `--enumerate`/`-e`):

````markdown
- `--distinct`: Remove duplicate items
- `--distinct-key <expr>`: Key expression for deduplication
- `--strategy {exact,windowed,probabilistic}`: Strategy for set operations (default: `exact`)
- `--window-size <n>`: Window size for windowed strategy
- `--bloom-expected-items <n>`: Expected items for probabilistic strategy
- `--bloom-fp-rate <rate>`: False positive rate for probabilistic strategy (default: `0.01`)
````

And add a new example to the stream examples:

````markdown
```bash
# Deduplicate within a pipeline
jaf stream events.jsonl \
    --filter '["eq?", "@type", "purchase"]' \
    --distinct --distinct-key '@transaction_id' \
    --take 100

# Probabilistic dedup in a pipeline
jaf stream huge_log.jsonl \
    --distinct --distinct-key '@request_id' \
    --strategy probabilistic \
    --bloom-expected-items 500000
```
````

**Step 4: Verify the doc builds**

Run: `venv/bin/mkdocs build 2>&1 | tail -5`
Expected: Build succeeds

**Step 5: Commit**

```
git add docs/cli-reference.md
git commit -m "docs: add distinct command and dedup flags to CLI reference"
```

---

### Task 3: cookbook.md — Fix phantom operators

**Files:**
- Modify: `docs/cookbook.md`

Six sections use operators that don't exist. Fix each one:

**Step 1: Fix "Flattening Nested Structures" (lines 199-217)**

The example uses `["concat", ...]` and `["join", ["map", "@items", "@name"], ", "]`.

`concat` doesn't exist. `join` exists but takes a list and delimiter. `map` as an expression operator doesn't exist. Wildcard paths (`@items.*.name`) can extract nested arrays.

Replace lines 199-217 with:

```python
# Extract nested data into flat structure
flattened = stream("orders.jsonl") \
    .map(["dict",
        "order_id", "@id",
        "customer_name", "@customer.name",
        "customer_email", "@customer.email",
        "shipping_city", "@shipping.city",
        "shipping_state", "@shipping.state",
        "total_items", ["length", "@items"]
    ]) \
    .evaluate()
```

Remove the `concat` and `join`+`map` combo — they relied on phantom operators. The simpler example still demonstrates nested path access clearly.

**Step 2: Fix "Processing Arrays within Documents" (lines 220-241)**

This section uses `["any", ["map", ...]]`, `["sum", ["map", ...]]`, and `["unique", ["map", ...]]`. The `any` and `map` (as expression operators) don't exist.

Replace lines 220-241 with:

```python
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
    Operations like "sum of all item prices" (`["sum", "@items.*.price"]`) work when the wildcard path resolves to a flat list. For more complex nested array processing (filtering within arrays, conditional aggregation), extract the data with JAF and process in Python.
```

**Step 3: Fix "Sampling Data" random sampling example (lines 291-294)**

The example uses `["hash", "@id"]` which doesn't exist.

Replace lines 291-294 with:

```python
# Deterministic sampling using modulo on a numeric ID
id_sample = stream("data.jsonl") \
    .filter(["eq?", ["%", "@id", 10], 0])  # ~10% sample (items where id % 10 == 0)
    .evaluate()
```

This uses `%` (modulo) which does exist, and removes the dependency on `hash`.

**Step 4: Verify the doc builds**

Run: `venv/bin/mkdocs build 2>&1 | tail -5`
Expected: Build succeeds

**Step 5: Commit**

```
git add docs/cookbook.md
git commit -m "docs: fix cookbook examples using nonexistent operators"
```

---

### Task 4: Verify docs build and serve

**Files:**
- None modified (verification only)

**Step 1: Build the full docs site**

Run: `venv/bin/mkdocs build 2>&1`
Expected: Clean build, no warnings about broken links

**Step 2: Spot-check the three modified files render correctly**

Run: `venv/bin/mkdocs serve &` and visually verify the three pages.

---

### Task 5: Final commit with all changes

If any fixes were needed in Task 4, commit them:

```
git add docs/
git commit -m "docs: fix critical gaps in api-guide, cli-reference, and cookbook"
```

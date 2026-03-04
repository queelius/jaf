# Probabilistic Data Structures

JAF v0.8.0 introduces probabilistic data structures for memory-efficient approximate operations on large data streams. These structures trade a small amount of accuracy for dramatic memory savings.

## Overview

| Structure | Purpose | Guarantee | Use Case |
|-----------|---------|-----------|----------|
| **BloomFilter** | Set membership | No false negatives | Deduplication, caching |
| **CountMinSketch** | Frequency counting | Never underestimates | Top-K, frequency analysis |
| **HyperLogLog** | Cardinality estimation | ~2% error with 14-bit precision | Counting unique items |

## Bloom Filter

A Bloom filter tests whether an element is in a set. It may have false positives (reporting an element is present when it isn't) but **never has false negatives** (will never miss an element that was added).

### Basic Usage

```python
from jaf import BloomFilter

# Create filter for ~10,000 items with 1% false positive rate
bf = BloomFilter(expected_items=10000, false_positive_rate=0.01)

# Add items
bf.add("apple")
bf.add("banana")
bf.add({"user_id": 123, "action": "click"})

# Check membership
if "apple" in bf:
    print("Probably in the set")

if "cherry" not in bf:
    print("Definitely not in the set")  # No false negatives!

# Check stats
print(f"Items added: {len(bf)}")
print(f"Estimated FPR: {bf.estimated_false_positive_rate():.4f}")
```

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `expected_items` | Expected number of unique items | Required |
| `false_positive_rate` | Target false positive probability (0 < fpr < 1) | 0.01 |
| `size` | Explicit bit array size (overrides auto-calculation) | Auto |
| `num_hashes` | Number of hash functions (overrides auto-calculation) | Auto |

### Memory Efficiency

The Bloom filter uses approximately:
- **~10 bits per item** for 1% false positive rate
- **~15 bits per item** for 0.1% false positive rate

For 1 million items at 1% FPR: ~1.2 MB (vs ~50+ MB for a hash set of strings)

### Merging Filters

```python
# Combine two filters (must have same parameters)
bf1 = BloomFilter(expected_items=1000, false_positive_rate=0.01)
bf2 = BloomFilter(expected_items=1000, false_positive_rate=0.01)

# Force same size for merging
bf2.size = bf1.size
bf2.num_hashes = bf1.num_hashes
bf2.bit_array = [False] * bf1.size

bf1.add("item1")
bf2.add("item2")

merged = bf1.union(bf2)
assert "item1" in merged
assert "item2" in merged
```

## Count-Min Sketch

A Count-Min Sketch estimates the frequency of items in a stream. It **never underestimates** (always returns count ≥ true count) but may overestimate due to hash collisions.

### Basic Usage

```python
from jaf import CountMinSketch

# Create sketch with specific dimensions
cms = CountMinSketch(width=1000, depth=5)

# Or using error parameters
cms = CountMinSketch(epsilon=0.01, delta=0.01)

# Count items
cms.add("apple", count=3)
cms.add("banana")
cms.add("apple", count=2)

# Estimate counts
print(f"Apple count: >= {cms.estimate('apple')}")  # At least 5
print(f"Total items: {len(cms)}")
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `width` | Number of counters per row |
| `depth` | Number of hash functions (rows) |
| `epsilon` | Error factor (width = e/ε) |
| `delta` | Probability of exceeding error (depth = ln(1/δ)) |

### Accuracy Guarantees

With probability (1 - δ), the estimate is at most `true_count + ε * total_items`.

For example, with ε=0.01 and δ=0.01:
- Width ≈ 272 counters
- Depth ≈ 5 rows
- Error ≤ 1% of total stream size with 99% probability

### Merging Sketches

```python
cms1 = CountMinSketch(width=1000, depth=5)
cms2 = CountMinSketch(width=1000, depth=5)

cms1.add("item1", count=10)
cms2.add("item1", count=5)
cms2.add("item2", count=3)

merged = cms1.merge(cms2)
print(merged.estimate("item1"))  # >= 15
```

## HyperLogLog

HyperLogLog estimates the number of distinct elements (cardinality) in a stream using very little memory.

### Basic Usage

```python
from jaf import HyperLogLog

# Create with precision (4-16, higher = more accurate)
hll = HyperLogLog(precision=14)  # Standard error ~0.8%

# Add items (duplicates don't increase estimate)
for i in range(1000000):
    hll.add(f"user_{i % 10000}")  # Only 10,000 unique

# Estimate cardinality
print(f"Unique items: ~{hll.estimate():.0f}")  # ~10,000
print(f"Unique items: ~{len(hll):.0f}")  # Same as estimate()
```

### Precision vs Accuracy

| Precision | Registers | Memory | Standard Error |
|-----------|-----------|--------|----------------|
| 4 | 16 | 16 bytes | 26% |
| 10 | 1024 | 1 KB | 3.25% |
| 12 | 4096 | 4 KB | 1.63% |
| 14 | 16384 | 16 KB | 0.81% |
| 16 | 65536 | 64 KB | 0.41% |

### Merging HyperLogLogs

```python
hll1 = HyperLogLog(precision=14)
hll2 = HyperLogLog(precision=14)

# Add different items to each
for i in range(5000):
    hll1.add(f"set1_item_{i}")
for i in range(5000):
    hll2.add(f"set2_item_{i}")

# Merge to get union cardinality
merged = hll1.merge(hll2)
print(f"Combined unique: ~{merged.estimate():.0f}")  # ~10,000
```

## Streaming Integration

Probabilistic structures integrate with JAF's streaming operations via the `strategy="probabilistic"` parameter.

### Probabilistic Distinct

```python
from jaf import stream

# Memory-efficient distinct for large streams
results = stream("huge_data.jsonl").distinct(
    strategy="probabilistic",
    bloom_expected_items=1000000,
    bloom_fp_rate=0.01
).evaluate()

# With key expression
results = stream(data).distinct(
    key=["@", [["key", "user_id"]]],
    strategy="probabilistic",
    bloom_expected_items=100000
).evaluate()
```

### Probabilistic Intersect

```python
from jaf import stream

# Find items in both streams (memory-efficient)
left = stream("users.jsonl")
right = stream("purchases.jsonl")

# Items in both (by user_id)
common = left.intersect(
    right,
    key=["@", [["key", "user_id"]]],
    strategy="probabilistic",
    bloom_expected_items=100000
).evaluate()
```

!!! warning "False Positives in Intersect"
    Probabilistic intersect may include items that aren't actually in both streams (false positives from the Bloom filter). Use exact strategy when precision is critical.

### Probabilistic Except (Set Difference)

```python
from jaf import stream

# Items in left but not in right
left = stream("all_users.jsonl")
right = stream("banned_users.jsonl")

# Active users (excluding banned)
active = left.except_from(
    right,
    key=["@", [["key", "user_id"]]],
    strategy="probabilistic",
    bloom_expected_items=10000,
    bloom_fp_rate=0.001  # Low FPR to avoid excluding valid users
)
```

!!! warning "False Positives in Except"
    With probabilistic except, some valid items may be incorrectly excluded (false positives cause items to be filtered). Use a very low `bloom_fp_rate` when false exclusions are costly.

## Strategy Comparison

| Strategy | Memory | Accuracy | Best For |
|----------|--------|----------|----------|
| `"exact"` (default) | O(n) | 100% | Small/medium streams |
| `"windowed"` | O(window_size) | Depends on ordering | Time-ordered streams |
| `"probabilistic"` | O(expected_items) | ~99% with FPR=0.01 | Large streams, approximate OK |

## Best Practices

### Sizing Bloom Filters

```python
# Rule of thumb: expected_items should be >= actual unique items
# Undersizing causes higher false positive rate

# Good: Size for expected maximum
bf = BloomFilter(expected_items=100000, false_positive_rate=0.01)

# Bad: Undersized filter has much higher FPR
bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
# Adding 100,000 items → FPR >> 1%
```

### Choosing False Positive Rate

```python
# Higher FPR = less memory but more errors
# 1% FPR is a good default
bf_default = BloomFilter(expected_items=100000, false_positive_rate=0.01)

# Use lower FPR when false positives are costly
bf_critical = BloomFilter(expected_items=100000, false_positive_rate=0.001)

# Higher FPR acceptable for rough filtering
bf_rough = BloomFilter(expected_items=100000, false_positive_rate=0.1)
```

### When to Use Probabilistic Strategies

✅ **Good use cases:**
- Deduplicating very large streams (millions of items)
- Approximate set operations where small errors are acceptable
- Memory-constrained environments
- Streaming data that can't fit in memory

❌ **Avoid when:**
- Exact results are required (financial, medical data)
- Stream size is small enough for exact processing
- False positives/negatives have high cost

## API Reference

### BloomFilter

```python
class BloomFilter:
    def __init__(self, expected_items, false_positive_rate=0.01,
                 size=None, num_hashes=None): ...
    def add(self, item) -> None: ...
    def __contains__(self, item) -> bool: ...
    def __len__(self) -> int: ...
    def union(self, other: BloomFilter) -> BloomFilter: ...
    def clear(self) -> None: ...
    def estimated_false_positive_rate(self) -> float: ...
```

### CountMinSketch

```python
class CountMinSketch:
    def __init__(self, width=None, depth=None,
                 epsilon=None, delta=None): ...
    def add(self, item, count=1) -> None: ...
    def estimate(self, item) -> int: ...
    def __len__(self) -> int: ...  # Total count
    def merge(self, other: CountMinSketch) -> CountMinSketch: ...
```

### HyperLogLog

```python
class HyperLogLog:
    def __init__(self, precision=14): ...  # 4-16
    def add(self, item) -> None: ...
    def estimate(self) -> float: ...
    def __len__(self) -> int: ...  # Same as estimate()
    def merge(self, other: HyperLogLog) -> HyperLogLog: ...
```

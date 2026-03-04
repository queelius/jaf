#!/usr/bin/env python3
"""
Probabilistic Deduplication Examples

Demonstrates memory-efficient deduplication using Bloom filters
for large data streams.
"""

import random
import time
from jaf import stream, BloomFilter, HyperLogLog, CountMinSketch


def generate_events(n, unique_ratio=0.1):
    """Generate sample events with duplicates."""
    unique_count = int(n * unique_ratio)
    unique_ids = [f"user_{i}" for i in range(unique_count)]

    for _ in range(n):
        user_id = random.choice(unique_ids)
        yield {
            "user_id": user_id,
            "event": random.choice(["click", "view", "purchase"]),
            "timestamp": time.time()
        }


def exact_vs_probabilistic():
    """Compare exact vs probabilistic deduplication."""
    print("=== Exact vs Probabilistic Deduplication ===\n")

    # Generate 10,000 events with ~1,000 unique users
    data = list(generate_events(10000, unique_ratio=0.1))

    # Exact deduplication
    start = time.time()
    exact_results = list(
        stream({"type": "memory", "data": data})
        .distinct(key=["@", [["key", "user_id"]]], strategy="exact")
        .evaluate()
    )
    exact_time = time.time() - start
    print(f"Exact: {len(exact_results)} unique users in {exact_time:.3f}s")

    # Probabilistic deduplication (Bloom filter)
    start = time.time()
    prob_results = list(
        stream({"type": "memory", "data": data})
        .distinct(
            key=["@", [["key", "user_id"]]],
            strategy="probabilistic",
            bloom_expected_items=2000,  # Oversize for safety
            bloom_fp_rate=0.01
        )
        .evaluate()
    )
    prob_time = time.time() - start
    print(f"Probabilistic: {len(prob_results)} unique users in {prob_time:.3f}s")

    # Note: Probabilistic may have fewer due to false positives
    print(f"\nDifference: {abs(len(exact_results) - len(prob_results))} items")


def bloom_filter_basics():
    """Demonstrate direct Bloom filter usage."""
    print("\n=== Bloom Filter Basics ===\n")

    # Create a Bloom filter for 1000 items with 1% false positive rate
    bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)

    # Add some items
    for i in range(500):
        bf.add(f"item_{i}")

    print(f"Items added: {len(bf)}")
    print(f"Current estimated FPR: {bf.estimated_false_positive_rate():.4f}")

    # Test membership
    print(f"\n'item_100' in filter: {'item_100' in bf}")  # True
    print(f"'item_999' in filter: {'item_999' in bf}")    # False (definitely not)
    print(f"'unknown' in filter: {'unknown' in bf}")      # False (definitely not)


def hyperloglog_cardinality():
    """Estimate cardinality with HyperLogLog."""
    print("\n=== HyperLogLog Cardinality Estimation ===\n")

    # Create HLL with 14-bit precision (~0.81% standard error)
    hll = HyperLogLog(precision=14)

    # Add 1 million events, but only 10,000 unique
    for i in range(1_000_000):
        hll.add(f"user_{i % 10000}")

    print(f"Actual unique users: 10,000")
    print(f"HLL estimate: {len(hll):,.0f}")
    print(f"Memory used: ~16 KB (vs ~500 KB for exact set)")


def count_min_sketch_frequency():
    """Estimate frequency with Count-Min Sketch."""
    print("\n=== Count-Min Sketch Frequency Estimation ===\n")

    # Create CMS with specific error bounds
    cms = CountMinSketch(epsilon=0.01, delta=0.01)

    # Simulate click events with varying frequencies
    events = (
        ["popular"] * 1000 +
        ["medium"] * 100 +
        ["rare"] * 10
    )
    random.shuffle(events)

    for event in events:
        cms.add(event)

    print(f"Total events: {len(cms)}")
    print(f"'popular' count estimate: {cms.estimate('popular')} (actual: 1000)")
    print(f"'medium' count estimate: {cms.estimate('medium')} (actual: 100)")
    print(f"'rare' count estimate: {cms.estimate('rare')} (actual: 10)")


def windowed_dedup():
    """Demonstrate windowed deduplication for time-series data."""
    print("\n=== Windowed Deduplication ===\n")

    # Simulate time-ordered events where duplicates are nearby
    events = []
    for i in range(100):
        user = f"user_{i % 10}"  # 10 unique users, repeated
        events.append({"user": user, "seq": i})

    # Windowed dedup with window of 20
    results = list(
        stream({"type": "memory", "data": events})
        .distinct(
            key=["@", [["key", "user"]]],
            strategy="windowed",
            window_size=20
        )
        .evaluate()
    )

    print(f"Input events: 100")
    print(f"After windowed dedup (window=20): {len(results)}")
    print("Note: Items may reappear after falling out of window")


if __name__ == "__main__":
    exact_vs_probabilistic()
    bloom_filter_basics()
    hyperloglog_cardinality()
    count_min_sketch_frequency()
    windowed_dedup()

"""
Probabilistic data structures for memory-efficient streaming operations.

This module provides probabilistic data structures that trade exactness for
memory efficiency, enabling operations on very large or infinite streams.

Structures implemented:
- BloomFilter: Approximate set membership test (false positives possible)
- CountMinSketch: Approximate frequency counting
- HyperLogLog: Approximate cardinality estimation

Example usage:
    >>> from jaf.probabilistic import BloomFilter
    >>> bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)
    >>> bf.add("hello")
    >>> "hello" in bf
    True
    >>> "world" in bf  # Might be False, or might be false positive
    False
"""

import math
import hashlib
from typing import Any, Optional, List
import json


class BloomFilter:
    """
    A Bloom filter for approximate set membership testing.

    A Bloom filter is a space-efficient probabilistic data structure that
    tests whether an element is a member of a set. False positives are
    possible, but false negatives are not.

    Args:
        expected_items: Expected number of items to store
        false_positive_rate: Desired false positive rate (0 < rate < 1)
        size: Optional explicit bit array size (overrides calculated size)
        num_hashes: Optional explicit number of hash functions

    Example:
        >>> bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)
        >>> bf.add("apple")
        >>> bf.add("banana")
        >>> "apple" in bf
        True
        >>> "cherry" in bf  # Probably False
        False
    """

    def __init__(
        self,
        expected_items: int = 1000,
        false_positive_rate: float = 0.01,
        size: Optional[int] = None,
        num_hashes: Optional[int] = None
    ):
        if not 0 < false_positive_rate < 1:
            raise ValueError("false_positive_rate must be between 0 and 1")

        if expected_items <= 0:
            raise ValueError("expected_items must be positive")

        # Calculate optimal size and number of hash functions
        if size is None:
            # m = -n * ln(p) / (ln(2)^2)
            self.size = int(-expected_items * math.log(false_positive_rate) / (math.log(2) ** 2))
        else:
            self.size = size

        if num_hashes is None:
            # k = (m/n) * ln(2)
            self.num_hashes = max(1, int((self.size / expected_items) * math.log(2)))
        else:
            self.num_hashes = num_hashes

        # Initialize bit array
        self.bit_array = [False] * self.size
        self.count = 0

    def _hash(self, item: Any, seed: int) -> int:
        """Generate a hash for an item with a given seed."""
        # Convert item to string representation
        if isinstance(item, (dict, list)):
            item_str = json.dumps(item, sort_keys=True)
        else:
            item_str = str(item)

        # Create hash with seed
        h = hashlib.md5(f"{seed}:{item_str}".encode()).hexdigest()
        return int(h, 16) % self.size

    def add(self, item: Any) -> None:
        """Add an item to the Bloom filter."""
        for i in range(self.num_hashes):
            index = self._hash(item, i)
            self.bit_array[index] = True
        self.count += 1

    def __contains__(self, item: Any) -> bool:
        """Check if an item might be in the set."""
        return all(
            self.bit_array[self._hash(item, i)]
            for i in range(self.num_hashes)
        )

    def __len__(self) -> int:
        """Return the number of items added."""
        return self.count

    def estimated_false_positive_rate(self) -> float:
        """Estimate current false positive rate based on fill level."""
        if self.count == 0:
            return 0.0
        # p = (1 - e^(-k*n/m))^k
        fill_ratio = 1 - math.exp(-self.num_hashes * self.count / self.size)
        return fill_ratio ** self.num_hashes

    def union(self, other: "BloomFilter") -> "BloomFilter":
        """Return a new Bloom filter that is the union of this and other."""
        if self.size != other.size or self.num_hashes != other.num_hashes:
            raise ValueError("Cannot union Bloom filters with different parameters")

        result = BloomFilter(size=self.size, num_hashes=self.num_hashes, expected_items=1)
        result.bit_array = [a or b for a, b in zip(self.bit_array, other.bit_array)]
        result.count = self.count + other.count  # Approximate
        return result

    def clear(self) -> None:
        """Clear all items from the filter."""
        self.bit_array = [False] * self.size
        self.count = 0


class CountMinSketch:
    """
    A Count-Min Sketch for approximate frequency counting.

    The Count-Min Sketch is a probabilistic data structure that provides
    approximate counts of items in a stream. It may overestimate counts
    but never underestimates.

    Args:
        width: Width of the sketch (more = better accuracy)
        depth: Depth of the sketch (number of hash functions)
        epsilon: Error factor (alternative to width: width = e/epsilon)
        delta: Probability of exceeding error (alternative to depth: depth = ln(1/delta))

    Example:
        >>> cms = CountMinSketch(width=1000, depth=5)
        >>> cms.add("apple", count=3)
        >>> cms.add("apple", count=2)
        >>> cms.estimate("apple")
        5
    """

    def __init__(
        self,
        width: Optional[int] = None,
        depth: Optional[int] = None,
        epsilon: Optional[float] = None,
        delta: Optional[float] = None
    ):
        # Calculate dimensions from error parameters if provided
        if epsilon is not None:
            width = int(math.e / epsilon)
        if delta is not None:
            depth = int(math.log(1 / delta))

        self.width = width or 1000
        self.depth = depth or 5

        # Initialize count matrix
        self.table = [[0] * self.width for _ in range(self.depth)]
        self.total_count = 0

    def _hash(self, item: Any, seed: int) -> int:
        """Generate a hash for an item with a given seed."""
        if isinstance(item, (dict, list)):
            item_str = json.dumps(item, sort_keys=True)
        else:
            item_str = str(item)

        h = hashlib.md5(f"{seed}:{item_str}".encode()).hexdigest()
        return int(h, 16) % self.width

    def add(self, item: Any, count: int = 1) -> None:
        """Add an item to the sketch with optional count."""
        for i in range(self.depth):
            index = self._hash(item, i)
            self.table[i][index] += count
        self.total_count += count

    def estimate(self, item: Any) -> int:
        """Estimate the count of an item."""
        return min(
            self.table[i][self._hash(item, i)]
            for i in range(self.depth)
        )

    def __len__(self) -> int:
        """Return total count of all items."""
        return self.total_count

    def merge(self, other: "CountMinSketch") -> "CountMinSketch":
        """Merge another sketch into this one."""
        if self.width != other.width or self.depth != other.depth:
            raise ValueError("Cannot merge sketches with different dimensions")

        result = CountMinSketch(width=self.width, depth=self.depth)
        for i in range(self.depth):
            for j in range(self.width):
                result.table[i][j] = self.table[i][j] + other.table[i][j]
        result.total_count = self.total_count + other.total_count
        return result


class HyperLogLog:
    """
    A HyperLogLog structure for approximate cardinality estimation.

    HyperLogLog estimates the number of distinct elements in a stream
    using very little memory (a few KB can estimate billions of items).

    Args:
        precision: Number of bits for bucket addressing (4-16)
            More precision = more memory but better accuracy

    Example:
        >>> hll = HyperLogLog(precision=12)
        >>> for i in range(1000000):
        ...     hll.add(f"item_{i % 10000}")  # 10000 unique items
        >>> hll.estimate()  # Should be close to 10000
        9987
    """

    def __init__(self, precision: int = 14):
        if not 4 <= precision <= 16:
            raise ValueError("precision must be between 4 and 16")

        self.precision = precision
        self.num_buckets = 1 << precision
        self.buckets = [0] * self.num_buckets

        # Alpha constant for bias correction
        if precision == 4:
            self.alpha = 0.673
        elif precision == 5:
            self.alpha = 0.697
        elif precision == 6:
            self.alpha = 0.709
        else:
            self.alpha = 0.7213 / (1 + 1.079 / self.num_buckets)

    def _hash(self, item: Any) -> int:
        """Generate a 64-bit hash for an item."""
        if isinstance(item, (dict, list)):
            item_str = json.dumps(item, sort_keys=True)
        else:
            item_str = str(item)

        h = hashlib.md5(item_str.encode()).hexdigest()
        return int(h, 16) & ((1 << 64) - 1)

    def _leading_zeros(self, value: int, max_bits: int) -> int:
        """Count leading zeros in a value."""
        if value == 0:
            return max_bits
        count = 0
        for i in range(max_bits - 1, -1, -1):
            if value & (1 << i):
                break
            count += 1
        return count

    def add(self, item: Any) -> None:
        """Add an item to the HyperLogLog."""
        h = self._hash(item)

        # Use first precision bits for bucket index
        bucket_index = h & (self.num_buckets - 1)

        # Count leading zeros in remaining bits
        remaining = h >> self.precision
        zeros = self._leading_zeros(remaining, 64 - self.precision) + 1

        self.buckets[bucket_index] = max(self.buckets[bucket_index], zeros)

    def estimate(self) -> int:
        """Estimate the cardinality."""
        # Raw estimate
        sum_inv = sum(2 ** (-bucket) for bucket in self.buckets)
        estimate = self.alpha * self.num_buckets * self.num_buckets / sum_inv

        # Small range correction
        if estimate <= 2.5 * self.num_buckets:
            zeros = self.buckets.count(0)
            if zeros > 0:
                estimate = self.num_buckets * math.log(self.num_buckets / zeros)

        # Large range correction (for 32-bit hashes)
        # Not needed for 64-bit hashes

        return int(estimate)

    def merge(self, other: "HyperLogLog") -> "HyperLogLog":
        """Merge another HyperLogLog into this one."""
        if self.precision != other.precision:
            raise ValueError("Cannot merge HyperLogLogs with different precision")

        result = HyperLogLog(precision=self.precision)
        result.buckets = [
            max(a, b) for a, b in zip(self.buckets, other.buckets)
        ]
        return result

    def __len__(self) -> int:
        """Return estimated cardinality."""
        return self.estimate()

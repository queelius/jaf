"""
Comprehensive unit tests for the JAF streaming system.
"""

import pytest
import json
import tempfile
import os
from jaf.lazy_streams import stream, LazyDataStream, FilteredStream, MappedStream
from jaf.exceptions import JAFError


class TestBasicStreaming:
    """Test basic streaming functionality."""

    def test_stream_from_memory(self):
        """Test creating stream from in-memory data."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        s = stream({"type": "memory", "data": data})

        assert isinstance(s, LazyDataStream)
        result = list(s.evaluate())
        assert result == data

    def test_stream_from_file(self, tmp_path):
        """Test creating stream from file."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        file_path = tmp_path / "test.json"
        file_path.write_text(json.dumps(data))

        s = stream(str(file_path))
        result = list(s.evaluate())
        assert result == data

    def test_stream_from_jsonl(self, tmp_path):
        """Test creating stream from JSONL file."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        file_path = tmp_path / "test.jsonl"
        file_path.write_text("\n".join(json.dumps(item) for item in data))

        s = stream(str(file_path))
        result = list(s.evaluate())
        assert result == data


class TestFilteredStream:
    """Test FilteredStream operations."""

    def test_simple_filter(self):
        """Test basic filtering."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        s = stream({"type": "memory", "data": data})

        filtered = s.filter(["gt?", "@age", 28])
        assert isinstance(filtered, FilteredStream)

        result = list(filtered.evaluate())
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_and_operation(self):
        """Test AND of two filters."""
        data = [
            {"name": "Alice", "age": 30, "dept": "eng"},
            {"name": "Bob", "age": 25, "dept": "sales"},
            {"name": "Charlie", "age": 35, "dept": "eng"},
        ]
        s = stream({"type": "memory", "data": data})

        f1 = s.filter(["eq?", "@dept", "eng"])
        f2 = s.filter(["gt?", "@age", 32])

        combined = f1.AND(f2)
        result = list(combined.evaluate())
        assert len(result) == 1
        assert result[0]["name"] == "Charlie"

    def test_or_operation(self):
        """Test OR of two filters."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        s = stream({"type": "memory", "data": data})

        f1 = s.filter(["lt?", "@age", 27])
        f2 = s.filter(["gt?", "@age", 33])

        combined = f1.OR(f2)
        result = list(combined.evaluate())
        assert len(result) == 2
        assert {r["name"] for r in result} == {"Bob", "Charlie"}

    def test_not_operation(self):
        """Test NOT of a filter."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        s = stream({"type": "memory", "data": data})

        f = s.filter(["gt?", "@age", 28])
        negated = f.NOT()

        result = list(negated.evaluate())
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_xor_operation(self):
        """Test XOR of two filters."""
        data = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
            {"name": "Dave", "age": 22, "active": False},
        ]
        s = stream({"type": "memory", "data": data})

        f1 = s.filter(["gt?", "@age", 28])  # Alice, Charlie
        f2 = s.filter(["eq?", "@active", True])  # Alice, Charlie

        xor_result = f1.XOR(f2)
        result = list(xor_result.evaluate())
        assert len(result) == 0  # Both filters match same items

        f3 = s.filter(["lt?", "@age", 26])  # Bob, Dave
        xor_result2 = f1.XOR(f3)
        result2 = list(xor_result2.evaluate())
        assert len(result2) == 4  # All items match exactly one filter

    def test_difference_operation(self):
        """Test DIFFERENCE of two filters."""
        data = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
        ]
        s = stream({"type": "memory", "data": data})

        f1 = s.filter(["gt?", "@age", 28])  # Alice, Charlie
        f2 = s.filter(["eq?", "@active", False])  # Bob

        diff = f1.DIFFERENCE(f2)
        result = list(diff.evaluate())
        assert len(result) == 2
        assert {r["name"] for r in result} == {"Alice", "Charlie"}


class TestMappedStream:
    """Test MappedStream operations."""

    def test_simple_map(self):
        """Test basic mapping."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        s = stream({"type": "memory", "data": data})

        mapped = s.map("@name")
        assert isinstance(mapped, MappedStream)

        result = list(mapped.evaluate())
        assert result == ["Alice", "Bob"]

    def test_map_with_expression(self):
        """Test mapping with complex expression."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        s = stream({"type": "memory", "data": data})

        # Create a new object with calculated field
        mapped = s.map(["dict", "name", "@name", "next_age", ["+", "@age", 1]])

        result = list(mapped.evaluate())
        assert len(result) == 2
        assert result[0] == {"name": "Alice", "next_age": 31}
        assert result[1] == {"name": "Bob", "next_age": 26}

    def test_map_with_string_operations(self):
        """Test mapping with string operations."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        s = stream({"type": "memory", "data": data})

        mapped = s.map(["upper-case", "@name"])
        result = list(mapped.evaluate())
        assert result == ["ALICE", "BOB"]


class TestStreamOperations:
    """Test various stream operations."""

    def test_take_operation(self):
        """Test take operation."""
        data = list(range(10))
        s = stream({"type": "memory", "data": data})

        taken = s.take(3)
        result = list(taken.evaluate())
        assert result == [0, 1, 2]

    def test_skip_operation(self):
        """Test skip operation."""
        data = list(range(5))
        s = stream({"type": "memory", "data": data})

        skipped = s.skip(2)
        result = list(skipped.evaluate())
        assert result == [2, 3, 4]

    def test_batch_operation(self):
        """Test batch operation."""
        data = list(range(7))
        s = stream({"type": "memory", "data": data})

        batched = s.batch(3)
        result = list(batched.evaluate())
        assert len(result) == 3
        assert result[0] == [0, 1, 2]
        assert result[1] == [3, 4, 5]
        assert result[2] == [6]  # Last batch is partial

    def test_enumerate_operation(self):
        """Test enumerate operation."""
        data = ["a", "b", "c"]
        s = stream({"type": "memory", "data": data})

        enumerated = s.enumerate()
        result = list(enumerated.evaluate())
        assert len(result) == 3
        assert result[0] == {"index": 0, "value": "a"}
        assert result[1] == {"index": 1, "value": "b"}
        assert result[2] == {"index": 2, "value": "c"}

    def test_enumerate_with_start(self):
        """Test enumerate with custom start."""
        data = ["a", "b"]
        s = stream({"type": "memory", "data": data})

        enumerated = s.enumerate(start=10)
        result = list(enumerated.evaluate())
        assert result[0] == {"index": 10, "value": "a"}
        assert result[1] == {"index": 11, "value": "b"}


class TestChainedOperations:
    """Test chaining multiple operations."""

    def test_filter_map_chain(self):
        """Test filter followed by map."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        s = stream({"type": "memory", "data": data})

        result = list(s.filter(["gt?", "@age", 28]).map("@name").evaluate())
        assert result == ["Alice", "Charlie"]

    def test_complex_pipeline(self):
        """Test complex pipeline with multiple operations."""
        data = [
            {"name": "Alice", "score": 85},
            {"name": "Bob", "score": 92},
            {"name": "Charlie", "score": 78},
            {"name": "Dave", "score": 95},
            {"name": "Eve", "score": 88},
        ]
        s = stream({"type": "memory", "data": data})

        # Filter high scores, map to names, take first 2
        result = list(s.filter(["gt?", "@score", 85]).map("@name").take(2).evaluate())
        assert result == ["Bob", "Dave"]

    def test_filter_enumerate_batch(self):
        """Test filter, enumerate, then batch."""
        data = [{"n": i} for i in range(10)]
        s = stream({"type": "memory", "data": data})

        result = list(s.filter(["gt?", "@n", 3]).enumerate().batch(2).evaluate())

        assert len(result) == 3
        assert result[0][0]["index"] == 0
        assert result[0][0]["value"]["n"] == 4


class TestStreamSerialization:
    """Test stream serialization and deserialization."""

    def test_filtered_stream_serialization(self):
        """Test FilteredStream to_dict and reconstruction."""
        data = [{"name": "Alice", "age": 30}]
        s = stream({"type": "memory", "data": data})
        filtered = s.filter(["gt?", "@age", 25])

        # Serialize
        serialized = filtered.to_dict()
        assert serialized["stream_type"] == "FilteredStream"
        assert serialized["query"] == ["gt?", "@age", 25]

        # Verify it can be loaded (would need console_script._reconstruct_stream)
        assert "collection_source" in serialized

    def test_mapped_stream_serialization(self):
        """Test MappedStream to_dict."""
        data = [{"name": "Alice"}]
        s = stream({"type": "memory", "data": data})
        mapped = s.map("@name")

        serialized = mapped.to_dict()
        assert serialized["stream_type"] == "MappedStream"
        assert serialized["expression"] == "@name"


class TestStreamInfo:
    """Test stream info functionality."""

    def test_basic_stream_info(self):
        """Test info on basic stream."""
        s = stream({"type": "memory", "data": [1, 2, 3]})
        info = s.info()

        assert info["type"] == "LazyDataStream"
        assert info["source_type"] == "memory"
        assert "pipeline" in info

    def test_filtered_stream_info(self):
        """Test info on filtered stream."""
        s = stream({"type": "memory", "data": [{"x": 1}]})
        filtered = s.filter(["gt?", "@x", 0])

        info = filtered.info()
        assert info["type"] == "FilteredStream"
        assert "filter → memory(1 items)" in info["pipeline"]

    def test_complex_pipeline_info(self):
        """Test info on complex pipeline."""
        s = stream({"type": "memory", "data": [1, 2, 3, 4, 5]})
        pipeline = s.filter(["gt?", "@", 2]).map(["*", "@", 2]).take(2)

        info = pipeline.info()
        assert "take(2)" in info["pipeline"]
        assert "map" in info["pipeline"]
        assert "filter" in info["pipeline"]


class TestErrorHandling:
    """Test error handling in streams."""

    def test_invalid_query_in_filter(self):
        """Test that invalid queries raise appropriate errors."""
        s = stream({"type": "memory", "data": [{"x": 1}]})

        # Unknown operator should fail
        with pytest.raises(JAFError):
            list(s.filter(["unknown-operator", "@x", 1]).evaluate())

    def test_map_with_invalid_path(self):
        """Test map with non-existent path returns None."""
        s = stream({"type": "memory", "data": [{"x": 1}]})
        mapped = s.map("@.y")  # y doesn't exist

        result = list(mapped.evaluate())
        assert result == [None]  # Non-existent simple paths return None

    def test_empty_stream_operations(self):
        """Test operations on empty streams."""
        s = stream({"type": "memory", "data": []})

        # All operations should work on empty streams
        assert list(s.filter(["eq?", "@x", 1]).evaluate()) == []
        assert list(s.map("@x").evaluate()) == []
        assert list(s.take(5).evaluate()) == []
        assert list(s.skip(5).evaluate()) == []
        assert list(s.batch(3).evaluate()) == []
        assert list(s.enumerate().evaluate()) == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

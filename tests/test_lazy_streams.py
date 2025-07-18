"""
Test the new lazy streaming API with composable operations.
"""

import pytest
from jaf.lazy_streams import stream, FilteredStream, MappedStream


class TestLazyStreams:
    """Test lazy streaming operations"""

    def test_basic_stream_creation(self):
        """Test creating streams from various sources"""
        # From memory
        s = stream({"type": "memory", "data": [1, 2, 3, 4, 5]})
        items = list(s.evaluate())
        assert items == [1, 2, 3, 4, 5]

        # From fibonacci
        s = stream({"type": "fibonacci"})
        items = list(s.take(5).evaluate())
        assert len(items) == 5
        assert items[0]["value"] == 0
        assert items[1]["value"] == 1

    def test_filter_operation(self):
        """Test filtering streams"""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

        s = stream({"type": "memory", "data": data})
        filtered = s.filter(["gt?", ["@", [["key", "age"]]], 30])

        assert isinstance(filtered, FilteredStream)

        items = list(filtered.evaluate())
        assert len(items) == 1
        assert items[0]["name"] == "Charlie"

    def test_map_operation(self):
        """Test mapping/transforming streams"""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

        s = stream({"type": "memory", "data": data})
        mapped = s.map(["@", [["key", "name"]]])

        assert isinstance(mapped, MappedStream)

        items = list(mapped.evaluate())
        assert items == ["Alice", "Bob"]

    def test_composable_operations(self):
        """Test chaining multiple operations"""
        data = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
            {"name": "Diana", "age": 28, "active": True},
        ]

        # Filter active users, map to names, take first 2
        s = stream({"type": "memory", "data": data})
        result = (
            s.filter(["eq?", ["@", [["key", "active"]]], True])
            .map(["@", [["key", "name"]]])
            .take(2)
        )

        items = list(result.evaluate())
        assert items == ["Alice", "Charlie"]

    def test_filter_after_map(self):
        """Test filtering after transformation"""
        data = [
            {"user": {"name": "Alice", "score": 95}},
            {"user": {"name": "Bob", "score": 75}},
            {"user": {"name": "Charlie", "score": 85}},
        ]

        s = stream({"type": "memory", "data": data})

        # Map to extract user objects, then filter by score
        result = s.map(["@", [["key", "user"]]]).filter(
            ["gt?", ["@", [["key", "score"]]], 80]
        )

        items = list(result.evaluate())
        assert len(items) == 2
        assert {item["name"] for item in items} == {"Alice", "Charlie"}

    def test_complex_transformation(self):
        """Test complex transformations with dict construction"""
        data = [
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bob", "age": 25, "email": "bob@example.com"},
        ]

        s = stream({"type": "memory", "data": data})

        # Transform to new structure
        result = s.map(
            [
                "dict",
                "display_name",
                ["upper-case", ["@", [["key", "name"]]]],
                "contact",
                ["@", [["key", "email"]]],
                "adult",
                ["gte?", ["@", [["key", "age"]]], 18],
            ]
        )

        items = list(result.evaluate())
        assert len(items) == 2
        assert items[0]["display_name"] == "ALICE"
        assert items[0]["contact"] == "alice@example.com"
        assert items[0]["adult"] is True

    def test_stream_filter_convenience(self):
        """Test creating a stream and immediately filtering"""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

        # Create stream and filter
        s = stream({"type": "memory", "data": data})
        filtered = s.filter(["gt?", ["@", [["key", "age"]]], 30])

        # Can chain operations
        result = filtered.map(["@", [["key", "name"]]])

        items = list(result.evaluate())
        assert items == ["Charlie"]

    def test_infinite_stream_operations(self):
        """Test operations on infinite streams"""
        # Fibonacci numbers
        s = stream({"type": "fibonacci"})

        # Take first 10, filter even values, map to just the value
        result = (
            s.take(10)
            .filter(["eq?", ["@", [["key", "is_even"]]], True])
            .map(["@", [["key", "value"]]])
        )

        items = list(result.evaluate())
        # Even fibonacci numbers in first 10: 0, 2, 8, 34
        assert items == [0, 2, 8, 34]

    def test_slice_operation(self):
        """Test slicing streams"""
        s = stream({"type": "fibonacci"})

        # Get fibonacci numbers from index 5 to 10
        result = s.slice(5, 10).map(["@", [["key", "value"]]])

        items = list(result.evaluate())
        assert items == [5, 8, 13, 21, 34]  # F(5) through F(9)

    def test_pipeline_serialization(self):
        """Test that pipelines can be serialized"""
        data = [{"x": 1}, {"x": 2}, {"x": 3}]

        s = stream({"type": "memory", "data": data})
        pipeline = (
            s.filter(["gt?", ["@", [["key", "x"]]], 1])
            .map(["*", ["@", [["key", "x"]]], 10])
            .take(1)
        )

        # The pipeline should be serializable
        pipeline_dict = pipeline.to_dict()
        assert pipeline_dict["collection_source"]["type"] == "take"
        assert pipeline_dict["collection_source"]["n"] == 1

        # The inner source should be a map
        inner = pipeline_dict["collection_source"]["inner_source"]
        assert inner["type"] == "map"

        # And its inner source should be a filter
        inner_inner = inner["inner_source"]
        assert inner_inner["type"] == "filter"

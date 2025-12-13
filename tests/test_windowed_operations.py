"""
Test suite for windowed streaming operations.

These tests verify that windowed operations:
1. Produce correct results with finite windows
2. Handle edge cases (empty windows, single item windows)
3. Match exact results when window_size=float('inf')
4. Are truly streaming (don't load everything into memory)
"""

import pytest
from typing import Generator, Any, List, Dict
from jaf.lazy_streams import stream
from jaf.streaming_loader import StreamingLoader
import math


class TestWindowedDistinct:
    """Test windowed distinct operation"""
    
    def test_distinct_with_small_window(self):
        """Test distinct with small window may have duplicates across windows"""
        data = [1, 2, 3, 2, 1, 4, 3, 5, 1]
        s = stream({"type": "memory", "data": data})
        
        # With window_size=3, we process in chunks of 3
        # Window 1: [1,2,3] -> [1,2,3]
        # Window 2: [2,1,4] -> [2,1,4] (2,1 appear again)
        # Window 3: [3,5,1] -> [3,5,1] (3,1 appear again)
        result = list(s.distinct(window_size=3).evaluate())
        
        # Should have some duplicates due to windowing
        assert len(result) > 5  # More than the 5 unique values
        # But should preserve order within windows
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] == 3
    
    def test_distinct_with_infinite_window(self):
        """Test distinct with infinite window gives exact results"""
        data = [1, 2, 3, 2, 1, 4, 3, 5, 1]
        s = stream({"type": "memory", "data": data})
        
        # With infinite window, should get exact distinct
        result = list(s.distinct(window_size=float('inf')).evaluate())
        assert result == [1, 2, 3, 4, 5]
    
    def test_distinct_with_key_expression(self):
        """Test distinct with key expression and windowing"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice2"},  # Same id
            {"id": 3, "name": "Charlie"},
            {"id": 2, "name": "Bob2"},  # Same id
        ]
        s = stream({"type": "memory", "data": data})
        
        # Distinct by id with infinite window
        result = list(s.distinct(key=["@", [["key", "id"]]], window_size=float('inf')).evaluate())
        assert len(result) == 3  # Only 3 unique ids
        assert result[0]["name"] == "Alice"  # First occurrence kept
        assert result[1]["name"] == "Bob"
        assert result[2]["name"] == "Charlie"
    
    def test_distinct_empty_window(self):
        """Test distinct handles empty data"""
        s = stream({"type": "memory", "data": []})
        result = list(s.distinct(window_size=10).evaluate())
        assert result == []


class TestWindowedGroupBy:
    """Test windowed groupby operation"""
    
    def test_groupby_with_small_window(self):
        """Test groupby with tumbling windows"""
        data = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 15},
            {"category": "B", "value": 25},
            {"category": "A", "value": 30},
            {"category": "C", "value": 40},
        ]
        s = stream({"type": "memory", "data": data})
        
        # Window size 3 - process in chunks
        result = list(s.groupby(
            key=["@", [["key", "category"]]],
            window_size=3,
            aggregate={
                "count": ["count"],
                "total": ["sum", "@value"]
            }
        ).evaluate())
        
        # First window: A:10, B:20, A:15 -> groups A(2 items, total 25), B(1 item, total 20)
        # Second window: B:25, A:30, C:40 -> groups B(1 item, total 25), A(1 item, total 30), C(1 item, total 40)
        assert len(result) > 3  # More groups due to windowing
    
    def test_groupby_with_infinite_window(self):
        """Test groupby with infinite window gives exact results"""
        data = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 15},
            {"category": "B", "value": 25},
            {"category": "A", "value": 30},
        ]
        s = stream({"type": "memory", "data": data})
        
        result = list(s.groupby(
            key=["@", [["key", "category"]]],
            window_size=float('inf'),
            aggregate={
                "count": ["count"],
                "total": ["sum", "@value"],
                "avg": ["mean", "@value"]
            }
        ).evaluate())
        
        # Should have exactly 2 groups
        assert len(result) == 2
        
        # Find group A
        group_a = next(g for g in result if g["key"] == "A")
        assert group_a["count"] == 3
        assert group_a["total"] == 55  # 10 + 15 + 30
        assert group_a["avg"] == pytest.approx(55/3)
        
        # Find group B
        group_b = next(g for g in result if g["key"] == "B")
        assert group_b["count"] == 2
        assert group_b["total"] == 45  # 20 + 25
        assert group_b["avg"] == 22.5


class TestWindowedJoin:
    """Test windowed join operation"""
    
    def test_join_with_sliding_window(self):
        """Test join with sliding window buffer"""
        left_data = [
            {"id": i, "name": f"User{i}"} 
            for i in range(1, 101)  # 100 users
        ]
        right_data = [
            {"user_id": i % 100 + 1, "action": f"Action{i}"} 
            for i in range(1, 201)  # 200 actions
        ]
        
        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})
        
        # Small window - may miss some joins
        result = list(left_stream.join(
            right_stream,
            on=["@", [["key", "id"]]],  # Join on left.id = right.user_id
            on_right=["@", [["key", "user_id"]]],
            how="inner",
            window_size=20
        ).evaluate())
        
        # With small window, won't get all 200 matches
        assert len(result) < 200
    
    def test_join_with_infinite_window(self):
        """Test join with infinite window gives exact results"""
        left_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        right_data = [
            {"user_id": 2, "city": "NYC"},
            {"user_id": 3, "city": "LA"},
            {"user_id": 4, "city": "Chicago"},
        ]
        
        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})
        
        # Infinite window - exact results
        result = list(left_stream.join(
            right_stream,
            on=["@", [["key", "id"]]],
            on_right=["@", [["key", "user_id"]]],
            how="outer",
            window_size=float('inf')
        ).evaluate())
        
        # Should have all 4 unique ids (1,2,3,4)
        assert len(result) == 4
        
        # Check specific matches
        alice = next(r for r in result if r["left"] and r["left"]["name"] == "Alice")
        assert alice["right"] is None  # No match for Alice
        
        bob = next(r for r in result if r["left"] and r["left"]["name"] == "Bob")
        assert bob["right"]["city"] == "NYC"
        
        chicago = next(r for r in result if r["right"] and r["right"]["city"] == "Chicago")
        assert chicago["left"] is None  # No match for user_id=4


class TestWindowedSetOperations:
    """Test windowed intersect and except operations"""
    
    def test_intersect_with_window(self):
        """Test intersect with windowing"""
        # For windowed intersect to work, data must have overlapping values
        # at similar stream positions. Use interleaved data for testing.
        left_data = list(range(1, 21))  # 1-20
        right_data = list(range(1, 21))  # 1-20 (same values at same positions)

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        # Small window - with aligned data, should find some intersections
        result = list(left_stream.intersect(
            right_stream,
            window_size=5
        ).evaluate())

        # With aligned identical data and small window, we should get some matches
        # (the first 5 items at least, as window is pre-filled)
        assert len(result) > 0
        assert all(1 <= x <= 20 for x in result)

        # Infinite window - exact intersection (all items match)
        left_data = list(range(1, 101))  # 1-100
        right_data = list(range(50, 151))  # 50-150

        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})

        result = list(left_stream.intersect(
            right_stream,
            window_size=float('inf')
        ).evaluate())

        # Should get exactly 50-100
        assert result == list(range(50, 101))
    
    def test_except_with_window(self):
        """Test except/difference with windowing"""
        left_data = list(range(1, 51))  # 1-50
        right_data = [10, 20, 30, 40, 50]  # Remove these
        
        left_stream = stream({"type": "memory", "data": left_data})
        right_stream = stream({"type": "memory", "data": right_data})
        
        # Infinite window - exact difference
        result = list(left_stream.except_from(
            right_stream,
            window_size=float('inf')
        ).evaluate())
        
        # Should have 1-50 except 10,20,30,40,50
        assert 10 not in result
        assert 20 not in result
        assert 30 not in result
        assert 40 not in result
        assert 50 not in result
        assert len(result) == 45  # 50 - 5


class TestWindowingEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_window_size_validation(self):
        """Test that window_size is validated properly"""
        s = stream({"type": "memory", "data": [1, 2, 3]})
        
        # Negative window size should raise error
        with pytest.raises(ValueError, match="window_size must be positive"):
            list(s.distinct(window_size=-1).evaluate())
        
        # Zero window size should raise error
        with pytest.raises(ValueError, match="window_size must be positive"):
            list(s.distinct(window_size=0).evaluate())
    
    def test_window_size_type_coercion(self):
        """Test window_size handles different numeric types"""
        s = stream({"type": "memory", "data": [1, 1, 2, 2, 3, 3]})
        
        # String "inf" should work
        result = list(s.distinct(window_size="inf").evaluate())
        assert result == [1, 2, 3]
        
        # Very large int should work like infinity
        result = list(s.distinct(window_size=10**9).evaluate())
        assert result == [1, 2, 3]
    
    def test_memory_warning_with_infinite_window(self):
        """Test that infinite window produces a warning for large datasets"""
        # This would need a logging capture mechanism
        # For now, just ensure it doesn't crash
        large_data = list(range(10000))
        s = stream({"type": "memory", "data": large_data})
        
        # Should complete without error but potentially with warning
        result = list(s.distinct(window_size=float('inf')).evaluate())
        assert len(result) == 10000


class TestStreamingBehavior:
    """Test that windowed operations are truly streaming"""
    
    def test_distinct_memory_usage(self):
        """Test that windowed distinct uses bounded memory"""
        # Create a generator that would be too large to fit in memory
        def large_generator():
            for i in range(1000000):  # 1 million items
                yield i % 100  # Only 100 unique values
        
        # This should work with small window
        s = stream({"type": "generator", "generator": large_generator})
        
        # Process with small window - should use bounded memory
        result_count = 0
        for item in s.distinct(window_size=1000).evaluate():
            result_count += 1
            if result_count > 10000:  # Stop early for test speed
                break
        
        assert result_count > 100  # Should have duplicates across windows
    
    def test_groupby_streaming_aggregation(self):
        """Test that groupby aggregates in streaming fashion"""
        def data_generator():
            for i in range(10000):
                yield {"category": f"Cat{i % 10}", "value": i}
        
        s = stream({"type": "generator", "generator": data_generator})
        
        # Small window for streaming aggregation
        result = list(s.groupby(
            key=["@", [["key", "category"]]],
            window_size=100,
            aggregate={"count": ["count"]}
        ).evaluate())
        
        # Should have multiple groups for same category
        cat0_groups = [r for r in result if r["key"] == "Cat0"]
        assert len(cat0_groups) > 1  # Multiple windows created multiple groups


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
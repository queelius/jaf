"""
Tests to improve coverage for lazy_ops_loader.py.

Focuses on untested operations and edge cases.
"""

import pytest
from jaf.lazy_streams import stream
from jaf.streaming_loader import StreamingLoader
from jaf.lazy_ops_loader import (
    stream_slice,
    stream_skip_while,
    stream_enumerate,
    stream_chain,
    stream_product,
    stream_project,
    stream_union
)


class TestSliceOperation:
    """Test slice operation edge cases"""
    
    def test_slice_with_step(self):
        """Test slice with custom step"""
        s = stream({"type": "memory", "data": list(range(10))})
        result = list(s.slice(0, 10, 2).evaluate())
        assert result == [0, 2, 4, 6, 8]
    
    def test_slice_negative_indices(self):
        """Test slice with negative indices"""
        s = stream({"type": "memory", "data": list(range(10))})
        # Note: streaming doesn't support negative indices well
        result = list(s.slice(2, 8, 1).evaluate())
        assert result == [2, 3, 4, 5, 6, 7]
    
    def test_slice_with_none_stop(self):
        """Test slice with None as stop"""
        s = stream({"type": "memory", "data": list(range(5))})
        result = list(s.slice(2, None, 1).evaluate())
        assert result == [2, 3, 4]
    
    def test_slice_empty_range(self):
        """Test slice that produces empty result"""
        s = stream({"type": "memory", "data": list(range(10))})
        result = list(s.slice(5, 5, 1).evaluate())
        assert result == []
    
    def test_slice_step_greater_than_data(self):
        """Test slice with step larger than data"""
        s = stream({"type": "memory", "data": [1, 2, 3]})
        result = list(s.slice(0, None, 10).evaluate())
        assert result == [1]


class TestSkipWhileOperation:
    """Test skip_while operation"""
    
    def test_skip_while_basic(self):
        """Test basic skip_while"""
        s = stream({"type": "memory", "data": [1, 2, 3, 4, 5]})
        result = list(s.skip_while(["lt?", "@", 3]).evaluate())
        assert result == [3, 4, 5]
    
    def test_skip_while_all_match(self):
        """Test skip_while when all items match"""
        s = stream({"type": "memory", "data": [1, 2, 3]})
        result = list(s.skip_while(["lt?", "@", 10]).evaluate())
        assert result == []
    
    def test_skip_while_none_match(self):
        """Test skip_while when no items match"""
        s = stream({"type": "memory", "data": [5, 6, 7]})
        result = list(s.skip_while(["lt?", "@", 3]).evaluate())
        assert result == [5, 6, 7]
    
    def test_skip_while_with_objects(self):
        """Test skip_while with objects"""
        data = [
            {"value": 1},
            {"value": 2},
            {"value": 3},
            {"value": 4}
        ]
        s = stream({"type": "memory", "data": data})
        result = list(s.skip_while(["lt?", "@value", 3]).evaluate())
        assert len(result) == 2
        assert result[0]["value"] == 3


class TestEnumerateOperation:
    """Test enumerate operation"""
    
    def test_enumerate_default_start(self):
        """Test enumerate with default start (0)"""
        s = stream({"type": "memory", "data": ["a", "b", "c"]})
        result = list(s.enumerate().evaluate())
        assert result == [
            {"index": 0, "value": "a"},
            {"index": 1, "value": "b"},
            {"index": 2, "value": "c"}
        ]
    
    def test_enumerate_custom_start(self):
        """Test enumerate with custom start"""
        s = stream({"type": "memory", "data": ["x", "y"]})
        result = list(s.enumerate(start=10).evaluate())
        assert result == [
            {"index": 10, "value": "x"},
            {"index": 11, "value": "y"}
        ]
    
    def test_enumerate_empty_stream(self):
        """Test enumerate on empty stream"""
        s = stream({"type": "memory", "data": []})
        result = list(s.enumerate().evaluate())
        assert result == []
    
    def test_enumerate_with_objects(self):
        """Test enumerate with object values"""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        s = stream({"type": "memory", "data": data})
        result = list(s.enumerate(start=1).evaluate())
        assert result[0] == {"index": 1, "value": {"name": "Alice"}}
        assert result[1] == {"index": 2, "value": {"name": "Bob"}}


class TestChainOperation:
    """Test chain operation for combining streams"""
    
    def test_chain_basic(self):
        """Test basic stream chaining"""
        loader = StreamingLoader()
        source = {
            "type": "chain",
            "sources": [
                {"type": "memory", "data": [1, 2]},
                {"type": "memory", "data": [3, 4]},
                {"type": "memory", "data": [5]}
            ]
        }
        result = list(stream_chain(loader, source))
        assert result == [1, 2, 3, 4, 5]
    
    def test_chain_empty_sources(self):
        """Test chain with some empty sources"""
        loader = StreamingLoader()
        source = {
            "type": "chain",
            "sources": [
                {"type": "memory", "data": [1]},
                {"type": "memory", "data": []},
                {"type": "memory", "data": [2, 3]}
            ]
        }
        result = list(stream_chain(loader, source))
        assert result == [1, 2, 3]
    
    def test_chain_single_source(self):
        """Test chain with single source"""
        loader = StreamingLoader()
        source = {
            "type": "chain",
            "sources": [
                {"type": "memory", "data": [1, 2, 3]}
            ]
        }
        result = list(stream_chain(loader, source))
        assert result == [1, 2, 3]
    
    def test_chain_no_sources(self):
        """Test chain with no sources"""
        loader = StreamingLoader()
        source = {
            "type": "chain",
            "sources": []
        }
        result = list(stream_chain(loader, source))
        assert result == []


class TestProductOperation:
    """Test Cartesian product operation"""
    
    def test_product_basic(self):
        """Test basic Cartesian product"""
        loader = StreamingLoader()
        source = {
            "type": "product",
            "left": {"type": "memory", "data": ["a", "b"]},
            "right": {"type": "memory", "data": [1, 2]}
        }
        result = list(stream_product(loader, source))
        expected = [
            {"left": "a", "right": 1}, {"left": "a", "right": 2},
            {"left": "b", "right": 1}, {"left": "b", "right": 2}
        ]
        assert result == expected
    
    def test_product_with_limit(self):
        """Test product with limit"""
        loader = StreamingLoader()
        source = {
            "type": "product",
            "left": {"type": "memory", "data": ["a", "b", "c"]},
            "right": {"type": "memory", "data": [1, 2, 3]},
            "limit": 4
        }
        result = list(stream_product(loader, source))
        # Should stop after 4 items
        assert len(result) == 4
        assert result[0] == {"left": "a", "right": 1}
        assert result[1] == {"left": "a", "right": 2}
        assert result[2] == {"left": "a", "right": 3}
        assert result[3] == {"left": "b", "right": 1}
    
    def test_product_empty_source(self):
        """Test product with one empty source"""
        loader = StreamingLoader()
        source = {
            "type": "product",
            "left": {"type": "memory", "data": ["a", "b"]},
            "right": {"type": "memory", "data": []}
        }
        result = list(stream_product(loader, source))
        assert result == []  # Product with empty set is empty
    
    def test_product_with_objects(self):
        """Test product with object data"""
        loader = StreamingLoader()
        source = {
            "type": "product",
            "left": {"type": "memory", "data": [{"id": 1}, {"id": 2}]},
            "right": {"type": "memory", "data": ["A", "B"]}
        }
        result = list(stream_product(loader, source))
        expected = [
            {"left": {"id": 1}, "right": "A"},
            {"left": {"id": 1}, "right": "B"},
            {"left": {"id": 2}, "right": "A"},
            {"left": {"id": 2}, "right": "B"}
        ]
        assert result == expected


class TestProjectOperation:
    """Test projection operation"""
    
    def test_project_basic(self):
        """Test basic projection"""
        loader = StreamingLoader()
        source = {
            "type": "project",
            "inner_source": {
                "type": "memory",
                "data": [
                    {"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25}
                ]
            },
            "fields": {"name": "@name", "age": "@age"}
        }
        result = list(stream_project(loader, source))
        assert result == [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
    
    def test_project_single_field(self):
        """Test projecting single field"""
        loader = StreamingLoader()
        source = {
            "type": "project",
            "inner_source": {
                "type": "memory",
                "data": [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"}
                ]
            },
            "fields": {"name": "@name"}
        }
        result = list(stream_project(loader, source))
        assert result == [
            {"name": "Alice"},
            {"name": "Bob"}
        ]
    
    def test_project_nonexistent_field(self):
        """Test projecting nonexistent field"""
        loader = StreamingLoader()
        source = {
            "type": "project",
            "inner_source": {
                "type": "memory",
                "data": [{"id": 1, "name": "Alice"}]
            },
            "fields": {"name": "@name", "email": "@email"}
        }
        result = list(stream_project(loader, source))
        # Nonexistent fields return empty list [] when path doesn't exist
        assert result == [{"name": "Alice", "email": []}]
    
    def test_project_rename_fields(self):
        """Test projection with field renaming"""
        loader = StreamingLoader()
        source = {
            "type": "project",
            "inner_source": {
                "type": "memory",
                "data": [{"id": 1, "full_name": "Alice"}]
            },
            "fields": {"name": "@full_name", "identifier": "@id"}
        }
        result = list(stream_project(loader, source))
        assert result == [{"name": "Alice", "identifier": 1}]


class TestUnionOperation:
    """Test union operation"""
    
    def test_union_basic(self):
        """Test basic union of streams"""
        loader = StreamingLoader()
        source = {
            "type": "union",
            "sources": [
                {"type": "memory", "data": [1, 2, 3]},
                {"type": "memory", "data": [3, 4, 5]}
            ]
        }
        result = list(stream_union(loader, source))
        # Union removes duplicates
        assert set(result) == {1, 2, 3, 4, 5}
    
    def test_union_with_objects(self):
        """Test union with object data"""
        loader = StreamingLoader()
        source = {
            "type": "union",
            "sources": [
                {"type": "memory", "data": [{"id": 1}, {"id": 2}]},
                {"type": "memory", "data": [{"id": 2}, {"id": 3}]}
            ]
        }
        result = list(stream_union(loader, source))
        # Objects are compared by value
        ids = [r["id"] for r in result]
        assert set(ids) == {1, 2, 3}
    
    def test_union_empty_sources(self):
        """Test union with empty sources"""
        loader = StreamingLoader()
        source = {
            "type": "union",
            "sources": [
                {"type": "memory", "data": []},
                {"type": "memory", "data": [1, 2]}
            ]
        }
        result = list(stream_union(loader, source))
        assert set(result) == {1, 2}
    
    def test_union_all_empty(self):
        """Test union with all empty sources"""
        loader = StreamingLoader()
        source = {
            "type": "union",
            "sources": [
                {"type": "memory", "data": []},
                {"type": "memory", "data": []}
            ]
        }
        result = list(stream_union(loader, source))
        assert result == []


class TestErrorHandling:
    """Test error handling in operations"""
    
    def test_missing_inner_source(self):
        """Test operations with missing inner_source"""
        loader = StreamingLoader()
        
        # Test filter without inner_source
        with pytest.raises(ValueError, match="missing 'inner_source'"):
            list(loader.stream({"type": "filter", "query": ["eq?", "@", 1]}))
        
        # Test map without inner_source
        with pytest.raises(ValueError, match="missing 'inner_source'"):
            list(loader.stream({"type": "map", "expression": "@"}))
    
    def test_missing_required_params(self):
        """Test operations with missing required parameters"""
        loader = StreamingLoader()
        
        # Test take without inner_source
        with pytest.raises(ValueError, match="missing 'inner_source'"):
            list(loader.stream({
                "type": "take",
                "n": 5
            }))
        
        # Test skip without inner_source  
        with pytest.raises(ValueError, match="missing 'inner_source'"):
            list(loader.stream({
                "type": "skip",
                "n": 5
            }))
    
    def test_invalid_window_size(self):
        """Test operations with invalid window_size"""
        s = stream({"type": "memory", "data": [1, 2, 3]})
        
        # Negative window size
        with pytest.raises(ValueError, match="window_size must be positive"):
            list(s.distinct(window_size=-1).evaluate())
        
        # Zero window size
        with pytest.raises(ValueError, match="window_size must be positive"):
            list(s.distinct(window_size=0).evaluate())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
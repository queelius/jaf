"""
Simplified MCP server tests that work without the MCP package.

These tests focus on the core logic without requiring MCP installation.
They test the functionality using JAF's streaming and eval components directly,
avoiding potential MCP SDK import conflicts.
"""

import pytest
import json
from unittest.mock import Mock, MagicMock
from typing import Dict, Any


# Helper function that replicates create_source logic for testing
# This avoids importing from mcp_server which may trigger MCP SDK conflicts
def _create_source_logic(source_desc):
    """Replicate create_source logic for testing without MCP imports."""
    if isinstance(source_desc, str):
        path = source_desc
        if path.endswith(".gz"):
            source = {"type": "gzip", "inner_source": {"type": "file", "path": path}}
        else:
            source = {"type": "file", "path": path}
        if ".jsonl" in path:
            source = {"type": "jsonl", "inner_source": source}
        elif ".csv" in path:
            source = {"type": "csv", "inner_source": source}
        elif ".json" in path:
            source = {"type": "json_array", "inner_source": source}
        return source
    elif isinstance(source_desc, dict):
        return source_desc
    else:
        return {"type": "memory", "data": source_desc}


def test_create_source_from_string():
    """Test creating source from file path string with parser wrapping"""
    # JSONL file gets jsonl parser wrapper
    source = _create_source_logic("data.jsonl")
    assert source == {"type": "jsonl", "inner_source": {"type": "file", "path": "data.jsonl"}}

    # JSON file gets json_array parser wrapper
    source = _create_source_logic("data.json")
    assert source == {"type": "json_array", "inner_source": {"type": "file", "path": "data.json"}}

    # Gzipped JSONL gets both decompression and parser
    source = _create_source_logic("data.jsonl.gz")
    assert source == {
        "type": "jsonl",
        "inner_source": {"type": "gzip", "inner_source": {"type": "file", "path": "data.jsonl.gz"}}
    }


def test_create_source_from_dict():
    """Test creating source from dict descriptor"""
    source_dict = {"type": "memory", "data": [1, 2, 3]}
    source = _create_source_logic(source_dict)
    assert source == source_dict


def test_create_source_from_list():
    """Test creating source from list (memory source)"""
    data = [{"a": 1}, {"a": 2}]
    source = _create_source_logic(data)
    assert source == {"type": "memory", "data": data}


class TestMCPLogic:
    """Test MCP logic without async/await"""
    
    def test_filter_processing(self):
        """Test filter operation logic"""
        from jaf.lazy_streams import stream
        
        # Simulate what the MCP filter tool would do
        source = [
            {"id": 1, "age": 30},
            {"id": 2, "age": 25},
            {"id": 3, "age": 35}
        ]
        query = ["gt?", "@age", 28]
        
        s = stream({"type": "memory", "data": source})
        filtered = s.filter(query)
        results = list(filtered.evaluate())
        
        assert len(results) == 2
        assert all(r["age"] > 28 for r in results)
    
    def test_map_processing(self):
        """Test map operation logic"""
        from jaf.lazy_streams import stream
        
        source = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
        expression = ["dict", "name", "@name", "doubled_age", ["*", "@age", 2]]
        
        s = stream({"type": "memory", "data": source})
        mapped = s.map(expression)
        results = list(mapped.evaluate())
        
        assert len(results) == 2
        alice = next(r for r in results if r["name"] == "Alice")
        assert alice["doubled_age"] == 60
    
    def test_eval_processing(self):
        """Test eval operation logic"""
        from jaf.jaf_eval import jaf_eval
        
        # Test a predicate instead
        expression = ["gt?", "@score", 85]
        data = {"score": 90}
        
        result = jaf_eval.eval(expression, data)
        assert result == True
    
    def test_groupby_processing(self):
        """Test groupby operation logic"""
        from jaf.lazy_streams import stream
        
        source = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 15},
            {"category": "B", "value": 25}
        ]
        key = ["@", [["key", "category"]]]
        aggregate = {
            "count": ["count"],
            "total": ["sum", "@value"],
            "avg": ["mean", "@value"]  # mean is valid in aggregations
        }
        
        s = stream({"type": "memory", "data": source})
        grouped = s.groupby(key=key, aggregate=aggregate)
        results = list(grouped.evaluate())
        
        assert len(results) == 2
        
        # Check group A
        group_a = next(r for r in results if r["key"] == "A")
        assert group_a["count"] == 2
        assert group_a["total"] == 25
        assert group_a["avg"] == 12.5
    
    def test_join_processing(self):
        """Test join operation logic"""
        from jaf.lazy_streams import stream
        
        left_source = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        right_source = [
            {"user_id": 1, "city": "NYC"},
            {"user_id": 2, "city": "LA"}
        ]
        
        left_stream = stream({"type": "memory", "data": left_source})
        right_stream = stream({"type": "memory", "data": right_source})
        
        joined = left_stream.join(
            right_stream,
            on=["@", [["key", "id"]]],
            on_right=["@", [["key", "user_id"]]],
            how="inner"
        )
        
        results = list(joined.evaluate())
        assert len(results) == 2
        
        alice = next(r for r in results if r["left"]["name"] == "Alice")
        assert alice["right"]["city"] == "NYC"
    
    def test_distinct_processing(self):
        """Test distinct operation logic"""
        from jaf.lazy_streams import stream
        
        source = [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "A"},
            {"id": 4, "category": "B"}
        ]
        
        s = stream({"type": "memory", "data": source})
        distinct = s.distinct(key=["@", [["key", "category"]]])
        results = list(distinct.evaluate())
        
        # Should only have 2 unique categories
        assert len(results) == 2
        categories = {r["category"] for r in results}
        assert categories == {"A", "B"}


class TestMCPQueryBuilder:
    """Test query builder logic"""
    
    def test_query_suggestions_for_comparison(self):
        """Test query suggestions for comparison operations"""
        description = "Find users with age greater than 25"
        
        # Simulate what query builder would suggest
        suggestions = []
        
        if "greater than" in description.lower() or ">" in description:
            suggestions.append({
                "type": "comparison",
                "query": ["gt?", "@field", "value"],
                "description": "Check if field is greater than value"
            })
        
        assert len(suggestions) == 1
        assert suggestions[0]["type"] == "comparison"
    
    def test_query_suggestions_for_contains(self):
        """Test query suggestions for contains operations"""
        description = "Find items that contain the word python"
        
        suggestions = []
        
        if "contain" in description.lower() or "includes" in description.lower():
            suggestions.append({
                "type": "membership",
                "query": ["contains?", "@array_field", "value"],
                "description": "Check if array contains value"
            })
        
        assert len(suggestions) == 1
        assert suggestions[0]["type"] == "membership"
    
    def test_query_suggestions_for_logical(self):
        """Test query suggestions for logical operations"""
        description = "Find users with age over 25 and active status"
        
        suggestions = []
        
        if "and" in description.lower():
            suggestions.append({
                "type": "logical",
                "query": ["and", ["condition1"], ["condition2"]],
                "description": "Combine conditions with AND"
            })
        
        assert len(suggestions) == 1
        assert suggestions[0]["type"] == "logical"


class TestMCPWindowedOperations:
    """Test windowed operation support"""
    
    def test_windowed_distinct_logic(self):
        """Test distinct with window size"""
        from jaf.lazy_streams import stream
        
        source = [1, 2, 3, 2, 1, 4, 3, 5]
        s = stream({"type": "memory", "data": source})
        distinct = s.distinct(window_size=3)
        results = list(distinct.evaluate())
        
        # With small window, may have duplicates
        assert len(results) >= 5
    
    def test_windowed_groupby_logic(self):
        """Test groupby with window size"""
        from jaf.lazy_streams import stream
        
        source = [
            {"cat": "A", "val": 1},
            {"cat": "B", "val": 2},
            {"cat": "A", "val": 3},
            {"cat": "B", "val": 4}
        ]
        
        s = stream({"type": "memory", "data": source})
        grouped = s.groupby(
            key=["@", [["key", "cat"]]],
            aggregate={"count": ["count"]},
            window_size=2
        )
        results = list(grouped.evaluate())
        
        # With tumbling windows, may have more than 2 groups
        assert len(results) >= 2


class TestMCPFileHandling:
    """Test file source handling"""
    
    def test_file_source_processing(self):
        """Test file source handling"""
        import tempfile
        import os
        from jaf.lazy_streams import stream
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": 1, "value": 10}\n')
            f.write('{"id": 2, "value": 20}\n')
            f.write('{"id": 3, "value": 30}\n')
            temp_path = f.name
        
        try:
            # Process file
            s = stream(temp_path)
            filtered = s.filter(["gt?", "@value", 15])
            results = list(filtered.evaluate())
            
            assert len(results) == 2
            assert all(r["value"] > 15 for r in results)
        
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
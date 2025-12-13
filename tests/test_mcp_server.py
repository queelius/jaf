"""
Test suite for MCP (Model Context Protocol) server.

Tests MCP tool registration, invocation, and error handling.
These tests require the MCP SDK and should be run in isolation to avoid
import conflicts with other test modules.

Run with: pytest tests/test_mcp_server.py -v
"""

import pytest
import json
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typing import List, Dict, Any

# Check if MCP is available
try:
    import mcp
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False

# Skip entire module if MCP not available to avoid import issues
if not MCP_AVAILABLE:
    pytest.skip("MCP SDK not installed - run: pip install mcp", allow_module_level=True)

# Import from the module only if MCP is available
from jaf.mcp_server import (
    handle_list_tools,
    handle_call_tool,
    create_source,
    Tool,
    TextContent
)


class TestMCPToolRegistration:
    """Test MCP tool registration and listing"""
    
    @pytest.mark.asyncio
    async def test_list_tools(self):
        """Test that all tools are properly listed"""
        tools = await handle_list_tools()
        
        # Check we have the expected tools
        tool_names = [tool.name for tool in tools]
        expected_tools = [
            "jaf_filter",
            "jaf_map",
            "jaf_eval",
            "jaf_groupby",
            "jaf_join",
            "jaf_distinct",
            "jaf_query_builder"
        ]
        
        for expected in expected_tools:
            assert expected in tool_names
        
        # Check tool has required fields
        for tool in tools:
            assert hasattr(tool, 'name')
            assert hasattr(tool, 'description')
            assert hasattr(tool, 'inputSchema')
    
    @pytest.mark.asyncio
    async def test_filter_tool_schema(self):
        """Test filter tool has correct schema"""
        tools = await handle_list_tools()
        filter_tool = next(t for t in tools if t.name == "jaf_filter")
        
        schema = filter_tool.inputSchema
        assert schema["type"] == "object"
        assert "source" in schema["properties"]
        assert "query" in schema["properties"]
        assert "limit" in schema["properties"]
        assert "source" in schema["required"]
        assert "query" in schema["required"]
    
    @pytest.mark.asyncio
    async def test_join_tool_schema(self):
        """Test join tool has complex schema"""
        tools = await handle_list_tools()
        join_tool = next(t for t in tools if t.name == "jaf_join")
        
        schema = join_tool.inputSchema
        assert "left_source" in schema["properties"]
        assert "right_source" in schema["properties"]
        assert "on" in schema["properties"]
        assert "how" in schema["properties"]
        
        # Check enum for join type
        assert schema["properties"]["how"]["enum"] == ["inner", "left", "right", "outer"]


class TestMCPToolInvocation:
    """Test MCP tool invocation"""
    
    @pytest.mark.asyncio
    async def test_filter_tool_invocation(self):
        """Test filter tool execution"""
        arguments = {
            "source": [
                {"id": 1, "age": 30},
                {"id": 2, "age": 25},
                {"id": 3, "age": 35}
            ],
            "query": ["gt?", "@age", 28],
            "limit": 10
        }
        
        results = await handle_call_tool("jaf_filter", arguments)
        
        assert len(results) == 1
        assert results[0].type == "text"
        
        # Parse the JSON response
        data = json.loads(results[0].text)
        assert "results" in data
        assert "count" in data
        assert "query" in data
        
        # Check filtered results
        assert data["count"] == 2  # age > 28: 30 and 35
        assert len(data["results"]) == 2
    
    @pytest.mark.asyncio
    async def test_map_tool_invocation(self):
        """Test map tool execution"""
        arguments = {
            "source": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ],
            "expression": ["dict", "name", "@name", "doubled_age", ["*", "@age", 2]]
        }
        
        results = await handle_call_tool("jaf_map", arguments)
        
        data = json.loads(results[0].text)
        assert data["count"] == 2
        
        # Check transformed results
        alice = next(r for r in data["results"] if r["name"] == "Alice")
        assert alice["doubled_age"] == 60
    
    @pytest.mark.asyncio
    async def test_eval_tool_invocation(self):
        """Test eval tool execution"""
        arguments = {
            "expression": ["mean", "@scores"],
            "data": {"scores": [80, 90, 100]}
        }
        
        results = await handle_call_tool("jaf_eval", arguments)
        
        data = json.loads(results[0].text)
        assert data["result"] == 90
        assert data["expression"] == ["mean", "@scores"]
    
    @pytest.mark.asyncio
    async def test_groupby_tool_invocation(self):
        """Test groupby tool execution"""
        arguments = {
            "source": [
                {"category": "A", "value": 10},
                {"category": "B", "value": 20},
                {"category": "A", "value": 15},
                {"category": "B", "value": 25}
            ],
            "key": ["@", [["key", "category"]]],
            "aggregate": {
                "count": ["count"],
                "total": ["sum", "@value"],
                "avg": ["mean", "@value"]
            }
        }
        
        results = await handle_call_tool("jaf_groupby", arguments)
        
        data = json.loads(results[0].text)
        assert data["count"] == 2  # 2 groups
        
        # Check group A
        group_a = next(r for r in data["results"] if r["key"] == "A")
        assert group_a["count"] == 2
        assert group_a["total"] == 25  # 10 + 15
        assert group_a["avg"] == 12.5
    
    @pytest.mark.asyncio
    async def test_join_tool_invocation(self):
        """Test join tool execution"""
        arguments = {
            "left_source": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ],
            "right_source": [
                {"user_id": 1, "city": "NYC"},
                {"user_id": 2, "city": "LA"}
            ],
            "on": ["@", [["key", "id"]]],
            "on_right": ["@", [["key", "user_id"]]],
            "how": "inner"
        }
        
        results = await handle_call_tool("jaf_join", arguments)
        
        data = json.loads(results[0].text)
        assert data["count"] == 2
        assert data["join_type"] == "inner"
        
        # Check joined results
        alice = next(r for r in data["results"] if r["left"]["name"] == "Alice")
        assert alice["right"]["city"] == "NYC"
    
    @pytest.mark.asyncio
    async def test_distinct_tool_invocation(self):
        """Test distinct tool execution"""
        arguments = {
            "source": [
                {"id": 1, "category": "A"},
                {"id": 2, "category": "B"},
                {"id": 3, "category": "A"},
                {"id": 4, "category": "B"}
            ],
            "key": ["@", [["key", "category"]]]
        }
        
        results = await handle_call_tool("jaf_distinct", arguments)
        
        data = json.loads(results[0].text)
        assert data["unique_count"] == 2  # Only A and B
    
    @pytest.mark.asyncio
    async def test_query_builder_tool(self):
        """Test query builder tool"""
        arguments = {
            "description": "Find users with age greater than 25 and active status",
            "examples": [
                {"age": 30, "active": True},
                {"age": 20, "active": True}
            ]
        }
        
        results = await handle_call_tool("jaf_query_builder", arguments)
        
        data = json.loads(results[0].text)
        assert "query_suggestions" in data
        assert "operators" in data
        assert "path_syntax" in data
        
        # Should suggest comparison operator
        suggestions = data["query_suggestions"]
        assert any("comparison" in s.get("type", "") for s in suggestions)


class TestMCPSourceCreation:
    """Test source creation helpers"""
    
    def test_create_source_from_string(self):
        """Test creating source from file path string with parser wrapping"""
        source = create_source("data.jsonl")
        assert source == {"type": "jsonl", "inner_source": {"type": "file", "path": "data.jsonl"}}
    
    def test_create_source_from_dict(self):
        """Test creating source from dict descriptor"""
        source_dict = {"type": "memory", "data": [1, 2, 3]}
        source = create_source(source_dict)
        assert source == source_dict
    
    def test_create_source_from_list(self):
        """Test creating source from list (memory source)"""
        data = [{"a": 1}, {"a": 2}]
        source = create_source(data)
        assert source == {"type": "memory", "data": data}


class TestMCPErrorHandling:
    """Test error handling in MCP server"""
    
    @pytest.mark.asyncio
    async def test_unknown_tool_name(self):
        """Test handling of unknown tool name"""
        results = await handle_call_tool("unknown_tool", {})
        
        assert len(results) == 1
        assert "Unknown tool: unknown_tool" in results[0].text
    
    @pytest.mark.asyncio
    async def test_invalid_arguments(self):
        """Test handling of invalid arguments"""
        # Filter with invalid query (should handle gracefully)
        arguments = {
            "source": [{"a": 1}],
            "query": "not_an_array"  # Should be array
        }
        
        # This should handle the error internally
        results = await handle_call_tool("jaf_filter", arguments)
        
        # Should return some result (error or empty)
        assert len(results) == 1
    
    @pytest.mark.asyncio
    async def test_file_source_handling(self):
        """Test file source handling"""
        import tempfile
        import os
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": 1, "value": 10}\n')
            f.write('{"id": 2, "value": 20}\n')
            temp_path = f.name
        
        try:
            arguments = {
                "source": temp_path,
                "query": ["gt?", "@value", 15]
            }
            
            results = await handle_call_tool("jaf_filter", arguments)
            
            data = json.loads(results[0].text)
            assert data["count"] == 1  # Only value=20
            assert data["results"][0]["value"] == 20
        
        finally:
            os.unlink(temp_path)


class TestMCPWindowedOperations:
    """Test windowed operation support in MCP"""
    
    @pytest.mark.asyncio
    async def test_windowed_distinct(self):
        """Test distinct with window size"""
        arguments = {
            "source": [1, 2, 3, 2, 1, 4, 3, 5],
            "window_size": 3
        }
        
        results = await handle_call_tool("jaf_distinct", arguments)
        
        data = json.loads(results[0].text)
        # With small window, may have duplicates
        assert data["unique_count"] >= 5
    
    @pytest.mark.asyncio
    async def test_windowed_groupby(self):
        """Test groupby with window size"""
        arguments = {
            "source": [
                {"cat": "A", "val": 1},
                {"cat": "B", "val": 2},
                {"cat": "A", "val": 3},
                {"cat": "B", "val": 4}
            ],
            "key": ["@", [["key", "cat"]]],
            "aggregate": {"count": ["count"]},
            "window_size": 2
        }
        
        results = await handle_call_tool("jaf_groupby", arguments)
        
        data = json.loads(results[0].text)
        # With tumbling windows, may have more than 2 groups
        assert data["count"] >= 2
    
    @pytest.mark.asyncio
    async def test_windowed_join(self):
        """Test join with window size"""
        arguments = {
            "left_source": [{"id": i} for i in range(10)],
            "right_source": [{"id": i, "val": i*10} for i in range(10)],
            "on": ["@", [["key", "id"]]],
            "on_right": ["@", [["key", "id"]]],
            "how": "inner",
            "window_size": 5
        }

        results = await handle_call_tool("jaf_join", arguments)

        data = json.loads(results[0].text)
        # Windowed join returns some matches (may be all if window covers overlap)
        assert data["count"] >= 1  # At least some matches
        assert data["join_type"] == "inner"


class TestMCPIntegration:
    """Test full MCP integration scenarios"""
    
    @pytest.mark.asyncio
    async def test_complex_pipeline(self):
        """Test complex multi-step pipeline"""
        # Step 1: Filter
        filter_args = {
            "source": [
                {"id": 1, "age": 30, "dept": "A"},
                {"id": 2, "age": 25, "dept": "B"},
                {"id": 3, "age": 35, "dept": "A"},
                {"id": 4, "age": 28, "dept": "B"}
            ],
            "query": ["gte?", "@age", 28]
        }
        
        filter_results = await handle_call_tool("jaf_filter", filter_args)
        filter_data = json.loads(filter_results[0].text)
        
        # Step 2: Group filtered results
        group_args = {
            "source": filter_data["results"],
            "key": ["@", [["key", "dept"]]],
            "aggregate": {
                "count": ["count"],
                "avg_age": ["mean", "@age"]
            }
        }
        
        group_results = await handle_call_tool("jaf_groupby", group_args)
        group_data = json.loads(group_results[0].text)
        
        assert group_data["count"] == 2  # Departments A and B
        
        # Check department A (ages 30, 35)
        dept_a = next(r for r in group_data["results"] if r["key"] == "A")
        assert dept_a["count"] == 2
        assert dept_a["avg_age"] == 32.5
    
    @pytest.mark.asyncio
    async def test_query_builder_with_validation(self):
        """Test query builder with example validation"""
        arguments = {
            "description": "Find items with price between 10 and 50 that are in stock",
            "examples": [
                {"price": 25, "in_stock": True},
                {"price": 60, "in_stock": True},
                {"price": 15, "in_stock": False}
            ]
        }
        
        results = await handle_call_tool("jaf_query_builder", arguments)
        
        data = json.loads(results[0].text)
        
        # Should suggest logical AND operation
        assert any("logical" in s.get("type", "") for s in data["query_suggestions"])
        
        # Should have comparison operators in suggestions
        assert "comparison" in data["operators"]
        assert "gte?" in data["operators"]["comparison"]
        assert "lte?" in data["operators"]["comparison"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
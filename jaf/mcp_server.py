#!/usr/bin/env python3
"""
MCP (Model Context Protocol) server for JAF.

This server exposes JAF operations as MCP tools that can be used by
LLMs like Claude to process and analyze JSON data.
"""

import json
import asyncio
from typing import Any, Dict, List, Optional
import sys
from pathlib import Path

# MCP SDK imports
try:
    from mcp.server import Server, NotificationOptions
    from mcp.server.models import InitializationOptions
    from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
    from mcp.server.stdio import stdio_server
except ImportError as e:
    # Only exit if we're running as main module
    if __name__ == "__main__":
        print("Error: MCP SDK not installed. Install with: pip install mcp", file=sys.stderr)
        sys.exit(1)
    else:
        # For testing, create mock classes
        class Server:
            def __init__(self, name): pass
            def list_tools(self): return lambda f: f
            def call_tool(self): return lambda f: f
            def run(self, *args): pass
        
        class Tool:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class TextContent:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
        
        class InitializationOptions:
            def __init__(self, **kwargs): pass
        
        def stdio_server(): return None, None

from .lazy_streams import stream
from .jaf_eval import jaf_eval
from .streaming_loader import StreamingLoader


# Create the MCP server
server = Server("jaf-mcp")


@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """List available JAF tools."""
    return [
        Tool(
            name="jaf_filter",
            description="Filter JSON data using JAF query expressions",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": ["string", "object"],
                        "description": "Data source (file path or source descriptor)"
                    },
                    "query": {
                        "type": "array",
                        "description": "JAF filter query expression"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results"
                    }
                },
                "required": ["source", "query"]
            }
        ),
        Tool(
            name="jaf_map",
            description="Transform JSON data using JAF expressions",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": ["string", "object"],
                        "description": "Data source (file path or source descriptor)"
                    },
                    "expression": {
                        "type": "array",
                        "description": "JAF transformation expression"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results"
                    }
                },
                "required": ["source", "expression"]
            }
        ),
        Tool(
            name="jaf_eval",
            description="Evaluate a JAF expression against data",
            inputSchema={
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "array",
                        "description": "JAF expression to evaluate"
                    },
                    "data": {
                        "description": "Data to evaluate against"
                    }
                },
                "required": ["expression", "data"]
            }
        ),
        Tool(
            name="jaf_groupby",
            description="Group JSON data by key with optional aggregations",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": ["string", "object"],
                        "description": "Data source (file path or source descriptor)"
                    },
                    "key": {
                        "type": "array",
                        "description": "JAF expression for grouping key"
                    },
                    "aggregate": {
                        "type": "object",
                        "description": "Aggregation operations"
                    },
                    "window_size": {
                        "type": "number",
                        "description": "Window size for streaming groupby"
                    }
                },
                "required": ["source", "key"]
            }
        ),
        Tool(
            name="jaf_join",
            description="Join two JSON data streams",
            inputSchema={
                "type": "object",
                "properties": {
                    "left_source": {
                        "type": ["string", "object"],
                        "description": "Left stream source"
                    },
                    "right_source": {
                        "type": ["string", "object"],
                        "description": "Right stream source"
                    },
                    "on": {
                        "type": "array",
                        "description": "Join key expression for left stream"
                    },
                    "on_right": {
                        "type": "array",
                        "description": "Join key expression for right stream"
                    },
                    "how": {
                        "type": "string",
                        "enum": ["inner", "left", "right", "outer"],
                        "description": "Join type"
                    },
                    "window_size": {
                        "type": "number",
                        "description": "Window size for streaming join"
                    }
                },
                "required": ["left_source", "right_source", "on"]
            }
        ),
        Tool(
            name="jaf_distinct",
            description="Get distinct/unique values from JSON data",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": ["string", "object"],
                        "description": "Data source (file path or source descriptor)"
                    },
                    "key": {
                        "type": "array",
                        "description": "JAF expression for uniqueness key"
                    },
                    "window_size": {
                        "type": "number",
                        "description": "Window size for streaming distinct"
                    }
                },
                "required": ["source"]
            }
        ),
        Tool(
            name="jaf_query_builder",
            description="Build complex JAF queries interactively",
            inputSchema={
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": "Natural language description of the query"
                    },
                    "examples": {
                        "type": "array",
                        "description": "Example data to test against"
                    }
                },
                "required": ["description"]
            }
        )
    ]


def create_source(source_desc: Any) -> Dict[str, Any]:
    """Convert source descriptor to proper format with appropriate parsers."""
    if isinstance(source_desc, str):
        # Simple file path - need to wrap with appropriate parser
        path = source_desc

        # Build base source with decompression if needed
        if path.endswith(".gz"):
            source = {"type": "gzip", "inner_source": {"type": "file", "path": path}}
        else:
            source = {"type": "file", "path": path}

        # Add parser based on format
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
        # If it's a list, assume it's in-memory data
        return {"type": "memory", "data": source_desc}


@server.call_tool()
async def handle_call_tool(name: str, arguments: Any) -> List[TextContent]:
    """Handle tool calls."""
    
    if name == "jaf_filter":
        source = create_source(arguments.get("source"))
        query = arguments.get("query")
        limit = arguments.get("limit")
        
        s = stream(source).filter(query)
        if limit:
            s = s.take(limit)
        
        results = list(s.evaluate())
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "results": results,
                "count": len(results),
                "query": query
            }, indent=2)
        )]
    
    elif name == "jaf_map":
        source = create_source(arguments.get("source"))
        expression = arguments.get("expression")
        limit = arguments.get("limit")
        
        s = stream(source).map(expression)
        if limit:
            s = s.take(limit)
        
        results = list(s.evaluate())
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "results": results,
                "count": len(results),
                "expression": expression
            }, indent=2)
        )]
    
    elif name == "jaf_eval":
        expression = arguments.get("expression")
        data = arguments.get("data")
        
        result = jaf_eval.eval(expression, data)
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "result": result,
                "expression": expression,
                "data": data
            }, indent=2)
        )]
    
    elif name == "jaf_groupby":
        source = create_source(arguments.get("source"))
        key = arguments.get("key")
        aggregate = arguments.get("aggregate", {})
        window_size = arguments.get("window_size", float('inf'))
        
        s = stream(source).groupby(
            key=key,
            aggregate=aggregate,
            window_size=window_size
        )
        
        results = list(s.evaluate())
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "results": results,
                "count": len(results),
                "key": key,
                "aggregations": aggregate
            }, indent=2)
        )]
    
    elif name == "jaf_join":
        left_source = create_source(arguments.get("left_source"))
        right_source = create_source(arguments.get("right_source"))
        on = arguments.get("on")
        on_right = arguments.get("on_right")
        how = arguments.get("how", "inner")
        window_size = arguments.get("window_size", float('inf'))
        
        left_stream = stream(left_source)
        right_stream = stream(right_source)
        
        joined = left_stream.join(
            right_stream,
            on=on,
            on_right=on_right,
            how=how,
            window_size=window_size
        )
        
        results = list(joined.evaluate())
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "results": results,
                "count": len(results),
                "join_type": how
            }, indent=2)
        )]
    
    elif name == "jaf_distinct":
        source = create_source(arguments.get("source"))
        key = arguments.get("key")
        window_size = arguments.get("window_size", float('inf'))
        
        s = stream(source).distinct(
            key=key,
            window_size=window_size
        )
        
        results = list(s.evaluate())
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "results": results,
                "unique_count": len(results)
            }, indent=2)
        )]
    
    elif name == "jaf_query_builder":
        description = arguments.get("description")
        examples = arguments.get("examples", [])
        
        # Generate example queries based on description
        query_suggestions = []
        
        if "greater than" in description.lower() or ">" in description:
            query_suggestions.append({
                "type": "comparison",
                "query": ["gt?", "@field", "value"],
                "description": "Check if field is greater than value"
            })
        
        if "contains" in description.lower() or "includes" in description.lower():
            query_suggestions.append({
                "type": "membership",
                "query": ["contains?", "@array_field", "value"],
                "description": "Check if array contains value"
            })
        
        if "and" in description.lower():
            query_suggestions.append({
                "type": "logical",
                "query": ["and", ["condition1"], ["condition2"]],
                "description": "Combine conditions with AND"
            })
        
        if "nested" in description.lower() or "." in description:
            query_suggestions.append({
                "type": "path",
                "query": ["eq?", "@parent.child.field", "value"],
                "description": "Access nested fields with dot notation"
            })
        
        # Test queries against examples if provided
        tested_queries = []
        for query_info in query_suggestions:
            if examples:
                # Try to test the query
                test_results = []
                for example in examples[:3]:  # Test on first 3 examples
                    try:
                        # This is a template, would need actual field names
                        result = {"example": example, "would_match": "unknown"}
                        test_results.append(result)
                    except:
                        pass
                query_info["test_results"] = test_results
            tested_queries.append(query_info)
        
        return [TextContent(
            type="text",
            text=json.dumps({
                "description": description,
                "query_suggestions": tested_queries,
                "operators": {
                    "comparison": ["eq?", "neq?", "gt?", "gte?", "lt?", "lte?"],
                    "membership": ["in?", "contains?"],
                    "string": ["starts-with?", "ends-with?", "regex-match?"],
                    "type": ["is-string?", "is-number?", "is-array?", "is-object?"],
                    "logical": ["and", "or", "not"],
                    "existence": ["exists?", "is-null?", "is-empty?"]
                },
                "path_syntax": {
                    "simple": "@field",
                    "nested": "@parent.child",
                    "array_index": "@items.0",
                    "wildcard": "@items.*",
                    "recursive": "@**.field"
                }
            }, indent=2)
        )]
    
    else:
        return [TextContent(
            type="text",
            text=f"Unknown tool: {name}"
        )]


async def main():
    """Run the MCP server."""
    # Run the server using stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="jaf-mcp",
                server_version="1.0.0"
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
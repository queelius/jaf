"""
FastAPI integration for JAF streaming operations.

This module provides a REST API for JAF operations including:
- Streaming data from various sources
- Applying filters and transformations
- Performing set operations
- Real-time data processing with WebSockets
"""

from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import json
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .lazy_streams import stream
from .jaf_eval import jaf_eval
from .streaming_loader import StreamingLoader


# Pydantic models for request/response
class SourceDescriptor(BaseModel):
    """Source descriptor for creating streams."""
    type: str = Field(..., description="Source type (file, directory, memory, etc.)")
    path: Optional[str] = Field(None, description="Path for file/directory sources")
    pattern: Optional[str] = Field(None, description="Pattern for directory sources")
    recursive: Optional[bool] = Field(False, description="Recursive search for directories")
    data: Optional[List[Any]] = Field(None, description="Data for memory sources")
    inner_source: Optional[Dict[str, Any]] = Field(None, description="Inner source for nested sources")
    

class FilterRequest(BaseModel):
    """Request for filtering a stream."""
    source: Union[str, Dict[str, Any]] = Field(..., description="Source descriptor or file path")
    query: List = Field(..., description="JAF filter query expression")
    limit: Optional[int] = Field(None, description="Limit number of results")
    

class MapRequest(BaseModel):
    """Request for mapping/transforming a stream."""
    source: Union[str, Dict[str, Any]] = Field(..., description="Source descriptor or file path")
    expression: List = Field(..., description="JAF transformation expression")
    limit: Optional[int] = Field(None, description="Limit number of results")


class JoinRequest(BaseModel):
    """Request for joining two streams."""
    left_source: Union[str, Dict[str, Any]] = Field(..., description="Left stream source")
    right_source: Union[str, Dict[str, Any]] = Field(..., description="Right stream source")
    on: List = Field(..., description="Join key expression for left stream")
    on_right: Optional[List] = Field(None, description="Join key expression for right stream")
    how: str = Field("inner", description="Join type (inner, left, right, outer)")
    window_size: float = Field(float('inf'), description="Window size for streaming join")
    limit: Optional[int] = Field(None, description="Limit number of results")


class GroupByRequest(BaseModel):
    """Request for grouping a stream."""
    source: Union[str, Dict[str, Any]] = Field(..., description="Source descriptor or file path")
    key: List = Field(..., description="Grouping key expression")
    aggregate: Optional[Dict[str, List]] = Field(None, description="Aggregation operations")
    window_size: float = Field(float('inf'), description="Window size for streaming groupby")
    limit: Optional[int] = Field(None, description="Limit number of results")


class EvalRequest(BaseModel):
    """Request for evaluating a JAF expression."""
    expression: List = Field(..., description="JAF expression to evaluate")
    data: Any = Field(..., description="Data to evaluate against")


# Create FastAPI app
app = FastAPI(
    title="JAF Streaming API",
    description="REST API for JAF streaming data operations",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "JAF Streaming API",
        "version": "1.0.0",
        "endpoints": {
            "filter": "POST /filter - Filter a stream with a JAF query",
            "map": "POST /map - Transform stream values",
            "join": "POST /join - Join two streams",
            "groupby": "POST /groupby - Group stream by key",
            "eval": "POST /eval - Evaluate JAF expression",
            "stream": "GET /stream - Stream data from a source",
            "ws": "WS /ws - WebSocket for real-time streaming"
        }
    }


def create_source(source_desc: Union[str, Dict[str, Any], List]) -> Dict[str, Any]:
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
    if isinstance(source_desc, list):
        # Raw list becomes memory source
        return {"type": "memory", "data": source_desc}
    return source_desc


async def stream_generator(data_stream):
    """Async generator for streaming responses."""
    for item in data_stream.evaluate():
        yield json.dumps(item) + "\n"
        await asyncio.sleep(0)  # Allow other tasks to run


@app.post("/filter")
async def filter_stream(request: FilterRequest):
    """
    Filter a stream with a JAF query.
    
    Returns results as newline-delimited JSON stream.
    """
    try:
        source = create_source(request.source)
        s = stream(source)
        filtered = s.filter(request.query)
        
        if request.limit:
            filtered = filtered.take(request.limit)
        
        return StreamingResponse(
            stream_generator(filtered),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": "inline; filename=filtered.jsonl"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/map")
async def map_stream(request: MapRequest):
    """
    Transform values in a stream.
    
    Returns results as newline-delimited JSON stream.
    """
    try:
        source = create_source(request.source)
        s = stream(source)
        mapped = s.map(request.expression)
        
        if request.limit:
            mapped = mapped.take(request.limit)
        
        return StreamingResponse(
            stream_generator(mapped),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": "inline; filename=mapped.jsonl"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/join")
async def join_streams(request: JoinRequest):
    """
    Join two streams.
    
    Returns results as newline-delimited JSON stream.
    """
    try:
        left_source = create_source(request.left_source)
        right_source = create_source(request.right_source)
        
        left_stream = stream(left_source)
        right_stream = stream(right_source)
        
        joined = left_stream.join(
            right_stream,
            on=request.on,
            on_right=request.on_right,
            how=request.how,
            window_size=request.window_size
        )
        
        if request.limit:
            joined = joined.take(request.limit)
        
        return StreamingResponse(
            stream_generator(joined),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": "inline; filename=joined.jsonl"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/groupby")
async def groupby_stream(request: GroupByRequest):
    """
    Group a stream by key with optional aggregations.
    
    Returns results as newline-delimited JSON stream.
    """
    try:
        source = create_source(request.source)
        s = stream(source)
        
        grouped = s.groupby(
            key=request.key,
            aggregate=request.aggregate,
            window_size=request.window_size
        )
        
        if request.limit:
            grouped = grouped.take(request.limit)
        
        return StreamingResponse(
            stream_generator(grouped),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": "inline; filename=grouped.jsonl"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/eval")
async def eval_expression(request: EvalRequest):
    """
    Evaluate a JAF expression against data.
    
    Returns the result as JSON.
    """
    try:
        result = jaf_eval.eval(request.expression, request.data)
        return JSONResponse(content={"result": result})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/stream/{source_type}")
async def stream_data(
    source_type: str,
    path: Optional[str] = None,
    pattern: Optional[str] = "*.json*",
    recursive: bool = False,
    limit: Optional[int] = None
):
    """
    Stream data from a source.

    Returns results as newline-delimited JSON stream.
    """
    try:
        if source_type == "file":
            if not path:
                raise HTTPException(status_code=400, detail="Path required for file source")
            # Use create_source to get proper parser wrapping
            source = create_source(path)
        elif source_type == "directory":
            if not path:
                raise HTTPException(status_code=400, detail="Path required for directory source")
            source = {"type": "directory", "path": path, "pattern": pattern, "recursive": recursive}
        else:
            source = {"type": source_type}

        s = stream(source)

        if limit:
            s = s.take(limit)

        return StreamingResponse(
            stream_generator(s),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f"inline; filename={source_type}.jsonl"}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time streaming operations.
    
    Accepts JSON messages with operation requests and streams results back.
    """
    await websocket.accept()
    
    try:
        while True:
            # Receive message from client
            data = await websocket.receive_json()
            
            operation = data.get("operation")
            if not operation:
                await websocket.send_json({"error": "No operation specified"})
                continue
            
            try:
                if operation == "filter":
                    source = create_source(data.get("source"))
                    query = data.get("query")
                    limit = data.get("limit")
                    
                    s = stream(source).filter(query)
                    if limit:
                        s = s.take(limit)
                    
                    # Stream results
                    for item in s.evaluate():
                        await websocket.send_json({"data": item})
                    await websocket.send_json({"done": True})
                    
                elif operation == "map":
                    source = create_source(data.get("source"))
                    expression = data.get("expression")
                    limit = data.get("limit")
                    
                    s = stream(source).map(expression)
                    if limit:
                        s = s.take(limit)
                    
                    # Stream results
                    for item in s.evaluate():
                        await websocket.send_json({"data": item})
                    await websocket.send_json({"done": True})
                    
                elif operation == "eval":
                    expression = data.get("expression")
                    eval_data = data.get("data")
                    result = jaf_eval.eval(expression, eval_data)
                    await websocket.send_json({"result": result})
                    
                else:
                    await websocket.send_json({"error": f"Unknown operation: {operation}"})
                    
            except Exception as e:
                await websocket.send_json({"error": str(e)})
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        await websocket.send_json({"error": str(e)})
        await websocket.close()


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
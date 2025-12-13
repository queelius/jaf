"""
Test suite for FastAPI integration.

Tests REST endpoints, WebSocket connections, and streaming responses.
"""

import pytest
import json
import asyncio
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import WebSocket
import tempfile
import os

# Import the FastAPI app
from jaf.api import app, create_source


class TestFastAPIEndpoints:
    """Test REST API endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample test data"""
        return [
            {"id": 1, "name": "Alice", "age": 30, "department": "Engineering"},
            {"id": 2, "name": "Bob", "age": 25, "department": "Marketing"},
            {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering"},
            {"id": 4, "name": "Diana", "age": 28, "department": "Design"}
        ]
    
    @pytest.fixture
    def temp_jsonl_file(self, sample_data):
        """Create temporary JSONL file with sample data"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for item in sample_data:
                f.write(json.dumps(item) + '\n')
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        os.unlink(temp_path)
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns API info"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert data["name"] == "JAF Streaming API"
        assert "endpoints" in data
    
    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}
    
    def test_filter_endpoint_with_memory_source(self, client, sample_data):
        """Test filter endpoint with in-memory data"""
        response = client.post("/filter", json={
            "source": {"type": "memory", "data": sample_data},
            "query": ["gt?", "@age", 28],
            "limit": 10
        })
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"
        
        # Parse NDJSON response
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert len(results) == 2  # Alice (30) and Charlie (35)
        names = {r["name"] for r in results}
        assert names == {"Alice", "Charlie"}
    
    def test_filter_endpoint_with_file_source(self, client, temp_jsonl_file):
        """Test filter endpoint with file source"""
        response = client.post("/filter", json={
            "source": temp_jsonl_file,
            "query": ["eq?", "@department", "Engineering"]
        })
        
        assert response.status_code == 200
        
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert len(results) == 2  # Alice and Charlie
        names = {r["name"] for r in results}
        assert names == {"Alice", "Charlie"}
    
    def test_map_endpoint(self, client, sample_data):
        """Test map/transform endpoint"""
        response = client.post("/map", json={
            "source": {"type": "memory", "data": sample_data},
            "expression": ["dict", "name", "@name", "age", "@age"],
            "limit": 2
        })
        
        assert response.status_code == 200
        
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert len(results) == 2  # Limited to 2
        for result in results:
            assert "name" in result
            assert "age" in result
            assert "department" not in result  # Excluded by map
    
    def test_groupby_endpoint(self, client, sample_data):
        """Test groupby endpoint with aggregations"""
        response = client.post("/groupby", json={
            "source": {"type": "memory", "data": sample_data},
            "key": ["@", [["key", "department"]]],
            "aggregate": {
                "count": ["count"],
                "avg_age": ["mean", "@age"]
            }
        })
        
        assert response.status_code == 200
        
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        # Should have 3 groups: Engineering, Marketing, Design
        assert len(results) == 3
        
        # Check Engineering group
        eng_group = next(r for r in results if r["key"] == "Engineering")
        assert eng_group["count"] == 2
        assert eng_group["avg_age"] == 32.5  # (30 + 35) / 2
    
    def test_join_endpoint(self, client):
        """Test join endpoint"""
        users = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        
        orders = [
            {"user_id": 1, "product": "Laptop"},
            {"user_id": 2, "product": "Phone"},
            {"user_id": 1, "product": "Mouse"}
        ]
        
        response = client.post("/join", json={
            "left_source": {"type": "memory", "data": users},
            "right_source": {"type": "memory", "data": orders},
            "on": ["@", [["key", "id"]]],
            "on_right": ["@", [["key", "user_id"]]],
            "how": "inner"
        })
        
        assert response.status_code == 200
        
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert len(results) == 3  # 3 orders matched
        
        # Check first match
        alice_laptop = next(r for r in results 
                           if r["left"]["name"] == "Alice" 
                           and r["right"]["product"] == "Laptop")
        assert alice_laptop["left"]["id"] == 1
        assert alice_laptop["right"]["user_id"] == 1
    
    def test_eval_endpoint(self, client):
        """Test eval endpoint"""
        response = client.post("/eval", json={
            "expression": ["mean", "@scores"],
            "data": {"scores": [80, 90, 85, 95]}
        })
        
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert data["result"] == 87.5
    
    def test_stream_endpoint(self, client, temp_jsonl_file):
        """Test generic stream endpoint"""
        # Extract just the filename from the path
        response = client.get(f"/stream/file?path={temp_jsonl_file}&limit=2")
        
        assert response.status_code == 200
        
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert len(results) == 2  # Limited to 2
    
    def test_error_handling(self, client):
        """Test error handling for invalid requests"""
        # Invalid query syntax - Pydantic validates type, returns 422
        response = client.post("/filter", json={
            "source": {"type": "memory", "data": []},
            "query": "invalid_query"  # Should be array
        })

        assert response.status_code == 422  # Pydantic validation error

        # Missing required field
        response = client.post("/filter", json={
            "source": {"type": "memory", "data": []}
            # Missing query
        })

        assert response.status_code == 422  # Validation error
    
    def test_create_source_function(self):
        """Test source creation helper"""
        # String to file source - now wraps with appropriate parser
        source = create_source("data.jsonl")
        assert source == {"type": "jsonl", "inner_source": {"type": "file", "path": "data.jsonl"}}

        # JSON file gets json_array parser
        source = create_source("data.json")
        assert source == {"type": "json_array", "inner_source": {"type": "file", "path": "data.json"}}

        # Gzipped JSONL gets both decompression and parser
        source = create_source("data.jsonl.gz")
        assert source == {
            "type": "jsonl",
            "inner_source": {"type": "gzip", "inner_source": {"type": "file", "path": "data.jsonl.gz"}}
        }

        # Dict passes through
        source = create_source({"type": "memory", "data": []})
        assert source == {"type": "memory", "data": []}

        # List to memory source
        source = create_source([1, 2, 3])
        assert source == {"type": "memory", "data": [1, 2, 3]}


class TestWebSocketEndpoint:
    """Test WebSocket functionality"""
    
    @pytest.fixture
    def client(self):
        """Create test client with WebSocket support"""
        return TestClient(app)
    
    def test_websocket_filter_operation(self, client):
        """Test filter operation over WebSocket"""
        with client.websocket_connect("/ws") as websocket:
            # Send filter request
            websocket.send_json({
                "operation": "filter",
                "source": {"type": "memory", "data": [
                    {"a": 1}, {"a": 2}, {"a": 3}
                ]},
                "query": ["gt?", "@a", 1],
                "limit": 10
            })
            
            # Receive results
            results = []
            while True:
                data = websocket.receive_json()
                if data.get("done"):
                    break
                if "data" in data:
                    results.append(data["data"])
            
            assert len(results) == 2  # a=2 and a=3
            assert all(r["a"] > 1 for r in results)
    
    def test_websocket_map_operation(self, client):
        """Test map operation over WebSocket"""
        with client.websocket_connect("/ws") as websocket:
            websocket.send_json({
                "operation": "map",
                "source": {"type": "memory", "data": [
                    {"name": "Alice", "age": 30},
                    {"name": "Bob", "age": 25}
                ]},
                "expression": "@name"
            })
            
            results = []
            while True:
                data = websocket.receive_json()
                if data.get("done"):
                    break
                if "data" in data:
                    results.append(data["data"])
            
            assert results == ["Alice", "Bob"]
    
    def test_websocket_eval_operation(self, client):
        """Test eval operation over WebSocket"""
        with client.websocket_connect("/ws") as websocket:
            websocket.send_json({
                "operation": "eval",
                "expression": ["sum", "@values"],
                "data": {"values": [10, 20, 30]}
            })
            
            data = websocket.receive_json()
            assert "result" in data
            assert data["result"] == 60
    
    def test_websocket_error_handling(self, client):
        """Test WebSocket error handling"""
        with client.websocket_connect("/ws") as websocket:
            # Send invalid operation
            websocket.send_json({
                "operation": "invalid_op"
            })
            
            data = websocket.receive_json()
            assert "error" in data
            assert "Unknown operation" in data["error"]
            
            # Send request with missing fields
            websocket.send_json({
                "operation": "filter"
                # Missing source and query
            })
            
            data = websocket.receive_json()
            assert "error" in data


class TestStreamingResponse:
    """Test streaming response functionality"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_large_dataset_streaming(self, client):
        """Test streaming with larger dataset"""
        # Create large dataset
        large_data = [{"id": i, "value": i * 10} for i in range(100)]
        
        response = client.post("/filter", json={
            "source": {"type": "memory", "data": large_data},
            "query": ["gt?", "@value", 500],
            "limit": 20
        })
        
        assert response.status_code == 200
        
        # Parse streaming response
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert len(results) == 20  # Limited to 20
        assert all(r["value"] > 500 for r in results)
    
    def test_streaming_with_complex_pipeline(self, client):
        """Test streaming with complex operations"""
        data = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": "A", "value": 15},
            {"category": "B", "value": 25}
        ]
        
        # First filter, then map
        response = client.post("/filter", json={
            "source": {"type": "memory", "data": data},
            "query": ["eq?", "@category", "A"]
        })
        
        lines = response.text.strip().split('\n')
        filtered = [json.loads(line) for line in lines if line]
        
        # Now map the filtered results
        response = client.post("/map", json={
            "source": {"type": "memory", "data": filtered},
            "expression": ["*", "@value", 2]
        })
        
        lines = response.text.strip().split('\n')
        results = [json.loads(line) for line in lines if line]
        
        assert results == [20, 30]  # 10*2, 15*2


class TestCORSAndMiddleware:
    """Test CORS and middleware configuration"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_cors_headers(self, client):
        """Test CORS headers are present"""
        response = client.options("/", headers={
            "Origin": "http://example.com",
            "Access-Control-Request-Method": "POST"
        })

        assert "access-control-allow-origin" in response.headers
        # When allow_credentials=True, CORS echoes the specific origin rather than "*"
        assert response.headers["access-control-allow-origin"] in ["*", "http://example.com"]
    
    def test_content_type_headers(self, client):
        """Test correct content types"""
        # JSON response
        response = client.post("/eval", json={
            "expression": ["eq?", 1, 1],
            "data": {}
        })
        assert "application/json" in response.headers["content-type"]
        
        # NDJSON streaming response
        response = client.post("/filter", json={
            "source": {"type": "memory", "data": [{"a": 1}]},
            "query": ["exists?", "@a"]
        })
        assert response.headers["content-type"] == "application/x-ndjson"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
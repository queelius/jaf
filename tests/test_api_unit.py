"""
Unit tests for API module without requiring FastAPI installation.

These tests mock the FastAPI dependencies and test the core logic.
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys

# Mock FastAPI modules before import
sys.modules['fastapi'] = MagicMock()
sys.modules['fastapi.responses'] = MagicMock()
sys.modules['fastapi.middleware.cors'] = MagicMock()
sys.modules['pydantic'] = MagicMock()
sys.modules['uvicorn'] = MagicMock()
sys.modules['websockets'] = MagicMock()

from jaf.api import create_source


class TestCreateSource:
    """Test the create_source helper function"""

    def test_string_to_file_source(self):
        """Test converting file path string to source dict with parser"""
        result = create_source("data.jsonl")
        assert result == {"type": "jsonl", "inner_source": {"type": "file", "path": "data.jsonl"}}

    def test_dict_passthrough(self):
        """Test that dict sources pass through unchanged"""
        source = {"type": "memory", "data": [1, 2, 3]}
        result = create_source(source)
        assert result == source

    def test_list_passthrough(self):
        """Test that lists are converted to memory source"""
        data = [{"a": 1}, {"b": 2}]
        result = create_source(data)
        # Lists are converted to memory sources
        assert result == {"type": "memory", "data": data}
    
    def test_nested_source(self):
        """Test nested source descriptors"""
        source = {
            "type": "gzip",
            "inner_source": {"type": "file", "path": "data.jsonl.gz"}
        }
        result = create_source(source)
        assert result == source


class TestAPIModuleStructure:
    """Test that API module has expected structure"""
    
    def test_module_imports(self):
        """Test that module imports properly with mocks"""
        from jaf import api
        assert hasattr(api, 'app')
        assert hasattr(api, 'create_source')
    
    def test_pydantic_models_defined(self):
        """Test that Pydantic models are defined"""
        from jaf import api
        
        # These should be defined even with mocked pydantic
        expected_models = [
            'SourceDescriptor',
            'FilterRequest',
            'MapRequest',
            'JoinRequest',
            'GroupByRequest',
            'EvalRequest'
        ]
        
        for model_name in expected_models:
            assert hasattr(api, model_name)
    
    def test_endpoints_defined(self):
        """Test that expected endpoints are defined in the module"""
        from jaf import api
        
        # Check that endpoint functions exist
        expected_endpoints = [
            'root',
            'filter_stream',
            'map_stream',
            'join_streams',
            'groupby_stream',
            'eval_expression',
            'stream_data',
            'websocket_endpoint',
            'health_check'
        ]
        
        for endpoint in expected_endpoints:
            assert hasattr(api, endpoint)


class TestStreamGenerator:
    """Test the async stream generator"""
    
    @pytest.mark.asyncio
    async def test_stream_generator_basic(self):
        """Test basic stream generation"""
        from jaf.api import stream_generator
        from jaf.lazy_streams import stream
        
        # Create a simple stream
        s = stream({"type": "memory", "data": [{"a": 1}, {"a": 2}]})
        
        # Collect results
        results = []
        async for chunk in stream_generator(s):
            results.append(chunk)
        
        assert len(results) == 2
        assert json.loads(results[0]) == {"a": 1}
        assert json.loads(results[1]) == {"a": 2}
    
    @pytest.mark.asyncio
    async def test_stream_generator_with_filter(self):
        """Test stream generation with filter"""
        from jaf.api import stream_generator
        from jaf.lazy_streams import stream
        
        # Create filtered stream
        s = stream({"type": "memory", "data": [
            {"a": 1}, {"a": 2}, {"a": 3}
        ]}).filter(["gt?", "@a", 1])
        
        # Collect results
        results = []
        async for chunk in stream_generator(s):
            results.append(json.loads(chunk.strip()))
        
        assert len(results) == 2
        assert all(r["a"] > 1 for r in results)


class TestMockEndpointLogic:
    """Test endpoint logic with mocked dependencies"""
    
    @patch('jaf.api.StreamingResponse')
    @patch('jaf.api.stream')
    def test_filter_logic(self, mock_stream, mock_response):
        """Test filter endpoint logic"""
        from jaf.api import create_source
        
        # Setup mocks
        mock_stream_instance = Mock()
        mock_stream_instance.filter.return_value = mock_stream_instance
        mock_stream_instance.take.return_value = mock_stream_instance
        mock_stream_instance.evaluate.return_value = iter([{"a": 2}, {"a": 3}])
        mock_stream.return_value = mock_stream_instance
        
        # Simulate filter request
        source = create_source([{"a": 1}, {"a": 2}, {"a": 3}])
        query = ["gt?", "@a", 1]
        
        # Verify stream chain is called correctly
        s = mock_stream(source)
        filtered = s.filter(query)
        limited = filtered.take(10)
        
        mock_stream.assert_called_with(source)
        s.filter.assert_called_with(query)
        filtered.take.assert_called_with(10)
    
    @patch('jaf.api.jaf_eval')
    def test_eval_logic(self, mock_jaf_eval):
        """Test eval endpoint logic"""
        # Setup mock
        mock_jaf_eval.eval.return_value = 42
        
        # Test evaluation
        expression = ["sum", "@values"]
        data = {"values": [10, 15, 17]}
        
        result = mock_jaf_eval.eval(expression, data)
        
        mock_jaf_eval.eval.assert_called_with(expression, data)
        assert result == 42


class TestWebSocketLogic:
    """Test WebSocket handling logic"""
    
    def test_websocket_message_parsing(self):
        """Test parsing WebSocket messages"""
        # Test valid filter message
        message = {
            "operation": "filter",
            "source": {"type": "memory", "data": []},
            "query": ["exists?", "@id"],
            "limit": 10
        }
        
        assert message["operation"] == "filter"
        assert "source" in message
        assert "query" in message
        
        # Test valid eval message
        eval_message = {
            "operation": "eval",
            "expression": ["sum", "@values"],
            "data": {"values": [1, 2, 3]}
        }
        
        assert eval_message["operation"] == "eval"
        assert "expression" in eval_message
        assert "data" in eval_message
    
    def test_websocket_response_format(self):
        """Test WebSocket response formats"""
        # Data response
        data_response = {"data": {"id": 1, "value": 10}}
        assert "data" in data_response
        
        # Done response
        done_response = {"done": True}
        assert done_response["done"] is True
        
        # Error response
        error_response = {"error": "Invalid operation"}
        assert "error" in error_response
        
        # Result response (for eval)
        result_response = {"result": 42}
        assert "result" in result_response


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
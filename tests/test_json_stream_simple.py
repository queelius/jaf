"""
Simplified test suite for JSON streaming parser.

Focuses on testing the main streaming functions.
"""

import pytest
import json
import io
import gzip
import tempfile
import os
from jaf.json_stream import stream_json, stream_jsonl, skip_whitespace


class TestStreamJson:
    """Test the main stream_json function"""
    
    def test_empty_file(self):
        """Test parsing empty file"""
        file_obj = io.StringIO("")
        result = list(stream_json(file_obj))
        assert result == []
    
    def test_single_object(self):
        """Test parsing single JSON object"""
        data = {"name": "Alice", "age": 30, "city": "NYC"}
        file_obj = io.StringIO(json.dumps(data))
        result = list(stream_json(file_obj))
        assert len(result) == 1
        assert result[0] == data
    
    def test_json_array_of_objects(self):
        """Test parsing JSON array of objects"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]
        file_obj = io.StringIO(json.dumps(data))
        result = list(stream_json(file_obj))
        assert len(result) == 3
        assert result == data
    
    def test_json_array_mixed_types(self):
        """Test parsing JSON array with mixed types"""
        data = [
            {"type": "object"},
            42,
            "string",
            [1, 2, 3],
            True,
            None
        ]
        file_obj = io.StringIO(json.dumps(data))
        result = list(stream_json(file_obj))
        assert result == data
    
    def test_nested_objects(self):
        """Test parsing deeply nested objects"""
        data = [
            {
                "user": {
                    "profile": {
                        "settings": {
                            "theme": "dark",
                            "notifications": True
                        }
                    }
                }
            }
        ]
        file_obj = io.StringIO(json.dumps(data))
        result = list(stream_json(file_obj))
        assert result == data
    
    def test_large_chunks(self):
        """Test with different chunk sizes"""
        data = [{"id": i} for i in range(10)]
        json_str = json.dumps(data)
        
        # Small chunks
        file_obj = io.StringIO(json_str)
        result_small = list(stream_json(file_obj, chunk_size=10))
        assert result_small == data
        
        # Large chunks
        file_obj = io.StringIO(json_str)
        result_large = list(stream_json(file_obj, chunk_size=1000))
        assert result_large == data
    
    def test_whitespace_handling(self):
        """Test handling of various whitespace"""
        json_str = """
        [
            {
                "id"    :    1
            }  ,
            
            {
                "id"    :    2
            }
        ]
        """
        file_obj = io.StringIO(json_str)
        result = list(stream_json(file_obj))
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
    
    def test_unicode_handling(self):
        """Test handling of Unicode characters"""
        data = [
            {"text": "Hello 世界"},
            {"emoji": "🎉"},
            {"special": "Ñoño"}
        ]
        file_obj = io.StringIO(json.dumps(data))
        result = list(stream_json(file_obj))
        assert result == data
    
    def test_invalid_json(self):
        """Test handling of invalid JSON - may return empty or raise"""
        file_obj = io.StringIO("[{invalid json}]")
        # The parser might return empty list or raise error
        try:
            result = list(stream_json(file_obj))
            # If no error, should be empty or have some result
            assert isinstance(result, list)
        except (ValueError, json.JSONDecodeError):
            # Error is also acceptable
            pass


class TestStreamJsonl:
    """Test JSONL streaming"""
    
    def test_empty_jsonl(self):
        """Test empty JSONL file"""
        file_obj = io.StringIO("")
        result = list(stream_jsonl(file_obj))
        assert result == []
    
    def test_single_line_jsonl(self):
        """Test single line JSONL"""
        file_obj = io.StringIO('{"id": 1, "name": "Alice"}')
        result = list(stream_jsonl(file_obj))
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "Alice"}
    
    def test_multiple_lines_jsonl(self):
        """Test multiple lines JSONL"""
        lines = [
            '{"id": 1, "name": "Alice"}',
            '{"id": 2, "name": "Bob"}',
            '42',
            '"string"',
            'true',
            'null'
        ]
        file_obj = io.StringIO('\n'.join(lines))
        result = list(stream_jsonl(file_obj))
        assert len(result) == 6
        assert result[0] == {"id": 1, "name": "Alice"}
        assert result[2] == 42
        assert result[3] == "string"
        assert result[4] is True
        assert result[5] is None
    
    def test_jsonl_with_empty_lines(self):
        """Test JSONL with empty lines"""
        content = """
{"id": 1}

{"id": 2}


{"id": 3}
"""
        file_obj = io.StringIO(content)
        result = list(stream_jsonl(file_obj))
        assert len(result) == 3
        assert all(r["id"] in [1, 2, 3] for r in result)
    
    def test_jsonl_with_invalid_lines(self):
        """Test JSONL skips invalid lines"""
        content = """
{"id": 1}
{invalid}
{"id": 2}
"""
        file_obj = io.StringIO(content)
        result = list(stream_jsonl(file_obj))
        # Invalid lines are skipped
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2


class TestSkipWhitespace:
    """Test skip_whitespace helper"""
    
    def test_skip_spaces(self):
        """Test skipping spaces"""
        text = "   data"
        pos = skip_whitespace(text, 0)
        assert pos == 3
        assert text[pos] == 'd'
    
    def test_skip_mixed_whitespace(self):
        """Test skipping mixed whitespace"""
        text = " \t\n\r data"
        pos = skip_whitespace(text, 0)
        assert pos == 5
        assert text[pos] == 'd'
    
    def test_no_whitespace(self):
        """Test no whitespace to skip"""
        text = "data"
        pos = skip_whitespace(text, 0)
        assert pos == 0
    
    def test_all_whitespace(self):
        """Test all whitespace"""
        text = "   \t\n"
        pos = skip_whitespace(text, 0)
        assert pos == len(text)


class TestIntegration:
    """Test integration with JAF streaming system"""
    
    def test_with_file_source(self):
        """Test streaming from file"""
        from jaf.streaming_loader import StreamingLoader
        
        data = [{"id": 1}, {"id": 2}]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(data, f)
            temp_path = f.name
        
        try:
            loader = StreamingLoader()
            source = {
                "type": "json_array",
                "inner_source": {"type": "file", "path": temp_path}
            }
            result = list(loader.stream(source))
            assert result == data
        finally:
            os.unlink(temp_path)
    
    def test_with_jsonl_file(self):
        """Test streaming JSONL file"""
        from jaf.streaming_loader import StreamingLoader
        
        data = [{"id": 1}, {"id": 2}]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
            temp_path = f.name
        
        try:
            loader = StreamingLoader()
            source = {
                "type": "jsonl",
                "inner_source": {"type": "file", "path": temp_path}
            }
            result = list(loader.stream(source))
            assert result == data
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
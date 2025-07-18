"""Test kwargs support for stream() function."""

import pytest
from jaf.lazy_streams import stream


def test_stream_with_file_path():
    """Test stream with simple file path."""
    s = stream("data.jsonl")
    assert s.collection_source == {
        "type": "jsonl",
        "inner_source": {"type": "file", "path": "data.jsonl"}
    }


def test_stream_with_dict():
    """Test stream with dict descriptor."""
    source_dict = {"type": "file", "path": "data.json"}
    s = stream(source_dict)
    assert s.collection_source == source_dict


def test_stream_with_kwargs():
    """Test stream with kwargs."""
    s = stream(type="file", path="data.json")
    assert s.collection_source == {"type": "file", "path": "data.json"}


def test_stream_with_complex_kwargs():
    """Test stream with multiple kwargs."""
    s = stream(type="directory", path="/data", recursive=True, pattern="*.json*")
    assert s.collection_source == {
        "type": "directory",
        "path": "/data",
        "recursive": True,
        "pattern": "*.json*"
    }


def test_stream_with_fibonacci_kwargs():
    """Test stream with fibonacci codata source using kwargs."""
    s = stream(type="fibonacci", limit=100)
    assert s.collection_source == {"type": "fibonacci", "limit": 100}


def test_stream_with_unpacked_dict():
    """Test stream with unpacked dict using **."""
    # Note: 'source' is a reserved parameter name, so we need to test with
    # a dict that doesn't have 'source' as a key
    source_dict = {"type": "directory", "path": "/data", "recursive": True}
    s = stream(**source_dict)
    assert s.collection_source == source_dict


def test_stream_with_nested_source():
    """Test stream with nested source kwargs."""
    s = stream(
        type="gzip",
        inner_source={"type": "file", "path": "data.jsonl.gz"}
    )
    assert s.collection_source == {
        "type": "gzip",
        "inner_source": {"type": "file", "path": "data.jsonl.gz"}
    }


def test_stream_error_no_args():
    """Test error when no arguments provided."""
    with pytest.raises(ValueError, match="Must provide either"):
        stream()
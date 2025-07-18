import pytest
import json
from pathlib import Path
from jaf.streaming_loader import StreamingLoader


@pytest.fixture
def setup_test_files(tmp_path):
    # Setup for json_array
    json_array_file = tmp_path / "test_array.json"
    json_array_file.write_text(
        json.dumps([{"id": 1, "type": "array"}, {"id": 2, "type": "array"}])
    )

    # Setup for single json object
    json_object_file = tmp_path / "test_object.json"
    json_object_file.write_text(json.dumps({"id": 3, "type": "object"}))

    # Setup for jsonl
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text(
        '{"id": 4, "type": "jsonl"}\n{"id": 5, "type": "jsonl"}\n42\n"hello"'
    )

    # Setup for directory
    dir_path = tmp_path / "test_dir"
    dir_path.mkdir()
    (dir_path / "a.json").write_text(json.dumps([{"id": 6, "type": "dir_json"}]))
    (dir_path / "b.jsonl").write_text('{"id": 7, "type": "dir_jsonl"}')
    (dir_path / "c.json").write_text(json.dumps({"id": 8, "single": "object"}))

    # Setup for gzipped file
    import gzip

    jsonl_gz_file = tmp_path / "test.jsonl.gz"
    with gzip.open(jsonl_gz_file, "wt") as f:
        f.write('{"id": 9, "type": "gzip"}\n{"id": 10, "type": "gzip"}')

    return {
        "json_array_file": json_array_file,
        "json_object_file": json_object_file,
        "jsonl_file": jsonl_file,
        "dir_path": dir_path,
        "jsonl_gz_file": jsonl_gz_file,
    }


def test_stream_json_array(setup_test_files):
    """Test streaming elements from a JSON array."""
    loader = StreamingLoader()
    source = {
        "type": "json_array",
        "inner_source": {
            "type": "file",
            "path": str(setup_test_files["json_array_file"]),
        },
    }

    results = list(loader.stream(source))
    assert len(results) == 2
    assert results[0] == {"id": 1, "type": "array"}
    assert results[1] == {"id": 2, "type": "array"}


def test_stream_json_object(setup_test_files):
    """Test loading a single JSON object file."""
    loader = StreamingLoader()
    source = {
        "type": "json_array",  # Will detect it's not an array and yield the single object
        "inner_source": {
            "type": "file",
            "path": str(setup_test_files["json_object_file"]),
        },
    }

    results = list(loader.stream(source))
    assert len(results) == 1
    assert results[0] == {"id": 3, "type": "object"}


def test_stream_json_value(setup_test_files):
    """Test treating entire JSON file as single value."""
    loader = StreamingLoader()
    source = {
        "type": "json_value",
        "inner_source": {
            "type": "file",
            "path": str(setup_test_files["json_array_file"]),
        },
    }

    results = list(loader.stream(source))
    assert len(results) == 1
    assert results[0] == [{"id": 1, "type": "array"}, {"id": 2, "type": "array"}]


def test_stream_jsonl(setup_test_files):
    """Test streaming from JSONL file with mixed value types."""
    loader = StreamingLoader()
    source = {
        "type": "jsonl",
        "inner_source": {"type": "file", "path": str(setup_test_files["jsonl_file"])},
    }

    results = list(loader.stream(source))
    assert len(results) == 4
    assert results[0] == {"id": 4, "type": "jsonl"}
    assert results[1] == {"id": 5, "type": "jsonl"}
    assert results[2] == 42
    assert results[3] == "hello"


def test_stream_directory(setup_test_files):
    """Test streaming from directory of JSON files."""
    loader = StreamingLoader()
    source = {
        "type": "directory",
        "path": str(setup_test_files["dir_path"]),
        "pattern": "*.json*",
    }

    results = list(loader.stream(source))
    # Should get objects from all files
    assert len(results) == 3
    ids = [r.get("id") for r in results if isinstance(r, dict)]
    assert 6 in ids  # From a.json array
    assert 7 in ids  # From b.jsonl
    assert 8 in ids  # From c.json object


def test_stream_directory_file_as_object(setup_test_files):
    """Test streaming directory with file_as_object option."""
    loader = StreamingLoader()
    source = {
        "type": "directory",
        "path": str(setup_test_files["dir_path"]),
        "pattern": "a.json",
        "file_as_object": True,
    }

    results = list(loader.stream(source))
    # Should get the entire array as one item
    assert len(results) == 1
    assert results[0] == [{"id": 6, "type": "dir_json"}]


def test_stream_gzipped_jsonl(setup_test_files):
    """Test streaming from gzipped JSONL file."""
    loader = StreamingLoader()
    source = {
        "type": "jsonl",
        "inner_source": {
            "type": "gzip",
            "inner_source": {
                "type": "file",
                "path": str(setup_test_files["jsonl_gz_file"]),
            },
        },
    }

    results = list(loader.stream(source))
    assert len(results) == 2
    assert results[0] == {"id": 9, "type": "gzip"}
    assert results[1] == {"id": 10, "type": "gzip"}


def test_stream_memory_source():
    """Test streaming from in-memory data."""
    loader = StreamingLoader()
    source = {"type": "memory", "data": [{"id": 1}, 42, "hello", [1, 2, 3]]}

    results = list(loader.stream(source))
    assert len(results) == 4
    assert results[0] == {"id": 1}
    assert results[1] == 42
    assert results[2] == "hello"
    assert results[3] == [1, 2, 3]


def test_unknown_source_type():
    """Test error handling for unknown source type."""
    loader = StreamingLoader()
    source = {"type": "unknown"}

    with pytest.raises(ValueError, match="Unknown source type: unknown"):
        list(loader.stream(source))


def test_missing_source_type():
    """Test error handling for missing source type."""
    loader = StreamingLoader()
    source = {"path": "/some/path"}

    with pytest.raises(ValueError, match="Source descriptor missing 'type' field"):
        list(loader.stream(source))


def test_custom_loader_registration():
    """Test registering custom loader."""
    loader = StreamingLoader()

    def custom_loader(loader_instance, source):
        yield {"custom": True}
        yield {"custom": False}

    loader.register("custom", custom_loader)

    source = {"type": "custom"}
    results = list(loader.stream(source))
    assert len(results) == 2
    assert results[0] == {"custom": True}
    assert results[1] == {"custom": False}

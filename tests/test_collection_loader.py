import pytest
import json
from pathlib import Path
from jaf.collection_loader import CollectionLoader, _load_from_path, _load_from_directory

@pytest.fixture
def setup_test_files(tmp_path):
    # Setup for json_array
    json_array_file = tmp_path / "test_array.json"
    json_array_file.write_text(json.dumps([{"id": 1, "type": "array"}]))

    # Setup for jsonl
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text('{\"id\": 2, \"type\": \"jsonl\"}\n{\"id\": 3, \"type\": \"jsonl\"}')

    # Setup for directory
    dir_path = tmp_path / "test_dir"
    dir_path.mkdir()
    (dir_path / "a.json").write_text(json.dumps([{"id": 4, "type": "dir_json"}]))
    (dir_path / "b.jsonl").write_text('{\"id\": 5, \"type\": \"dir_jsonl\"}')

    return {
        "json_array_file": json_array_file,
        "jsonl_file": jsonl_file,
        "dir_path": dir_path
    }

def test_load_from_directory(setup_test_files):
    dir_path = setup_test_files["dir_path"]
    source = {
        "type": "directory",
        "files": [str(dir_path / "a.json"), str(dir_path / "b.jsonl")]
    }
    data = _load_from_directory(source)
    assert data == [{"id": 4, "type": "dir_json"}, {"id": 5, "type": "dir_jsonl"}]

def test_load_from_path_json_array(setup_test_files):
    source = {"path": str(setup_test_files["json_array_file"])}
    data = _load_from_path(source)
    assert data == [{"id": 1, "type": "array"}]

def test_load_from_path_jsonl(setup_test_files):
    source = {"path": str(setup_test_files["jsonl_file"])}
    data = _load_from_path(source)
    assert data == [{"id": 2, "type": "jsonl"}, {"id": 3, "type": "jsonl"}]

def test_collection_loader_dispatch(setup_test_files):
    loader = CollectionLoader()
    
    # Test json_array
    source_array = {"type": "json_array", "path": str(setup_test_files["json_array_file"])}
    data_array = loader.load(source_array)
    assert data_array == [{"id": 1, "type": "array"}]

    # Test jsonl
    source_jsonl = {"type": "jsonl", "path": str(setup_test_files["jsonl_file"])}
    data_jsonl = loader.load(source_jsonl)
    assert data_jsonl == [{"id": 2, "type": "jsonl"}, {"id": 3, "type": "jsonl"}]

    # Test directory
    dir_path = setup_test_files["dir_path"]
    source_dir = {
        "type": "directory",
        "files": [str(dir_path / "a.json"), str(dir_path / "b.jsonl")]
    }
    data_dir = loader.load(source_dir)
    assert data_dir == [{"id": 4, "type": "dir_json"}, {"id": 5, "type": "dir_jsonl"}]

def test_loader_unknown_type():
    loader = CollectionLoader()
    with pytest.raises(ValueError, match="No loader registered for source type: 'unknown'"):
        loader.load({"type": "unknown"})

def test_loader_missing_type():
    loader = CollectionLoader()
    with pytest.raises(ValueError, match="Collection source is missing 'type' key."):
        loader.load({})

def test_load_from_path_empty_file(tmp_path):
    empty_file = tmp_path / "empty.json"
    empty_file.write_text("")
    source = {"path": str(empty_file)}
    assert _load_from_path(source) == []

def test_load_from_path_file_not_found():
    source = {"path": "non_existent_file.json"}
    with pytest.raises(FileNotFoundError):
        _load_from_path(source)

def test_load_from_path_invalid_json(tmp_path):
    invalid_file = tmp_path / "invalid.json"
    invalid_file.write_text("{\"key\": \"value\"") # Incomplete JSON
    source = {"path": str(invalid_file)}
    with pytest.raises(ValueError, match="File is not a valid JSONL or a file containing a single JSON array"):
        _load_from_path(source)

def test_load_from_directory_missing_files_key():
    with pytest.raises(ValueError, match="Directory source is missing 'files' key."):
        _load_from_directory({"type": "directory"})

def test_load_from_directory_file_not_found(setup_test_files):
    dir_path = setup_test_files["dir_path"]
    source = {
        "type": "directory",
        "files": [str(dir_path / "a.json"), "non_existent_file.jsonl"]
    }
    with pytest.raises(IOError, match="Failed to load or parse file 'non_existent_file.jsonl'"):
        _load_from_directory(source)

def test_collection_loader_register_custom_loader():
    loader = CollectionLoader()
    
    def custom_loader(source):
        return [{"data": source["custom_data"]}]
        
    loader.register_loader("custom", custom_loader)
    
    source = {"type": "custom", "custom_data": "my_test_data"}
    data = loader.load(source)
    assert data == [{"data": "my_test_data"}]


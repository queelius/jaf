"""
Comprehensive test suite for io_utils.py

Tests file I/O operations, directory walking, and JSON parsing.
Follows TDD principles: focuses on behavior contracts, not implementation.
Uses pytest fixtures and temporary files for isolation.
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from jaf.io_utils import (
    walk_data_files,
    load_objects_from_file,
    load_collection,
    load_objects_from_string
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_json_array_file(temp_dir):
    """Create a temporary JSON array file"""
    file_path = os.path.join(temp_dir, "data.json")
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]
    with open(file_path, "w") as f:
        json.dump(data, f)
    return file_path


@pytest.fixture
def sample_jsonl_file(temp_dir):
    """Create a temporary JSONL file"""
    file_path = os.path.join(temp_dir, "data.jsonl")
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]
    with open(file_path, "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    return file_path


@pytest.fixture
def sample_json_object_file(temp_dir):
    """Create a temporary JSON file with single object"""
    file_path = os.path.join(temp_dir, "single.json")
    data = {"id": 1, "name": "Alice", "age": 30}
    with open(file_path, "w") as f:
        json.dump(data, f)
    return file_path


class TestWalkDataFiles:
    """Test walk_data_files directory traversal"""

    def test_walk_non_recursive_finds_json_files(self, temp_dir):
        """Should find JSON and JSONL files in directory (non-recursive)"""
        # Create test files
        json_file = os.path.join(temp_dir, "data.json")
        jsonl_file = os.path.join(temp_dir, "data.jsonl")
        txt_file = os.path.join(temp_dir, "readme.txt")

        for file_path in [json_file, jsonl_file, txt_file]:
            Path(file_path).touch()

        # Walk directory
        found = list(walk_data_files(temp_dir, recursive=False))

        # Should find JSON and JSONL, not TXT
        assert len(found) == 2
        assert json_file in found
        assert jsonl_file in found
        assert txt_file not in found

    def test_walk_recursive_finds_nested_files(self, temp_dir):
        """Should find files in subdirectories when recursive=True"""
        # Create nested directory structure
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir)

        root_file = os.path.join(temp_dir, "root.json")
        nested_file = os.path.join(subdir, "nested.jsonl")

        Path(root_file).touch()
        Path(nested_file).touch()

        # Walk recursively
        found = list(walk_data_files(temp_dir, recursive=True))

        assert len(found) == 2
        assert root_file in found
        assert nested_file in found

    def test_walk_non_recursive_skips_subdirectories(self, temp_dir):
        """Should skip subdirectories when recursive=False"""
        # Create nested structure
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir)

        root_file = os.path.join(temp_dir, "root.json")
        nested_file = os.path.join(subdir, "nested.json")

        Path(root_file).touch()
        Path(nested_file).touch()

        # Walk non-recursively
        found = list(walk_data_files(temp_dir, recursive=False))

        assert len(found) == 1
        assert root_file in found
        assert nested_file not in found

    def test_walk_empty_directory(self, temp_dir):
        """Should return empty list for empty directory"""
        found = list(walk_data_files(temp_dir, recursive=False))
        assert len(found) == 0

    def test_walk_non_existent_directory(self):
        """Should handle non-existent directory gracefully"""
        non_existent = "/tmp/this_directory_does_not_exist_12345"
        found = list(walk_data_files(non_existent, recursive=False))
        assert len(found) == 0

    def test_walk_filters_by_extension(self, temp_dir):
        """Should only find .json and .jsonl files, not others"""
        # Create various file types
        extensions = [".json", ".jsonl", ".txt", ".csv", ".xml", ".yaml"]
        for ext in extensions:
            Path(os.path.join(temp_dir, f"file{ext}")).touch()

        found = list(walk_data_files(temp_dir, recursive=False))

        # Should only find .json and .jsonl
        assert len(found) == 2
        assert all(f.endswith((".json", ".jsonl")) for f in found)

    def test_walk_ignores_directories_that_look_like_files(self, temp_dir):
        """Should not include directories even if they have .json extension"""
        # Create a directory with .json name (edge case)
        dir_with_json_name = os.path.join(temp_dir, "looks_like.json")
        os.makedirs(dir_with_json_name)

        real_file = os.path.join(temp_dir, "real.json")
        Path(real_file).touch()

        found = list(walk_data_files(temp_dir, recursive=False))

        # Should only find the real file
        assert len(found) == 1
        assert found[0] == real_file


class TestLoadObjectsFromFile:
    """Test load_objects_from_file JSON/JSONL parsing"""

    def test_load_json_array_file(self, sample_json_array_file):
        """Should load JSON array file correctly"""
        result = load_objects_from_file(sample_json_array_file)

        assert result is not None
        assert len(result) == 3
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"
        assert result[2]["name"] == "Charlie"

    def test_load_jsonl_file(self, sample_jsonl_file):
        """Should load JSONL file correctly"""
        result = load_objects_from_file(sample_jsonl_file)

        assert result is not None
        assert len(result) == 3
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"
        assert result[2]["name"] == "Charlie"

    def test_load_json_single_object(self, sample_json_object_file):
        """Should load single JSON object as list of one"""
        result = load_objects_from_file(sample_json_object_file)

        assert result is not None
        assert len(result) == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30

    def test_load_empty_json_file(self, temp_dir):
        """Should return None for empty JSON file"""
        empty_file = os.path.join(temp_dir, "empty.json")
        with open(empty_file, "w") as f:
            f.write("")

        result = load_objects_from_file(empty_file)
        assert result is None

    def test_load_whitespace_only_json_file(self, temp_dir):
        """Should return None for whitespace-only JSON file"""
        whitespace_file = os.path.join(temp_dir, "whitespace.json")
        with open(whitespace_file, "w") as f:
            f.write("   \n  \t  \n  ")

        result = load_objects_from_file(whitespace_file)
        assert result is None

    def test_load_empty_jsonl_file(self, temp_dir):
        """Should return None for empty JSONL file"""
        empty_file = os.path.join(temp_dir, "empty.jsonl")
        Path(empty_file).touch()

        result = load_objects_from_file(empty_file)
        assert result is None

    def test_load_jsonl_with_blank_lines(self, temp_dir):
        """Should skip blank lines in JSONL files"""
        file_path = os.path.join(temp_dir, "with_blanks.jsonl")
        with open(file_path, "w") as f:
            f.write('{"id": 1}\n')
            f.write('\n')  # Blank line
            f.write('{"id": 2}\n')
            f.write('  \n')  # Whitespace line
            f.write('{"id": 3}\n')

        result = load_objects_from_file(file_path)

        assert result is not None
        assert len(result) == 3
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        assert result[2]["id"] == 3

    def test_load_invalid_json_file(self, temp_dir):
        """Should return None for malformed JSON"""
        invalid_file = os.path.join(temp_dir, "invalid.json")
        with open(invalid_file, "w") as f:
            f.write('{"invalid": json}')  # Missing quotes

        result = load_objects_from_file(invalid_file)
        assert result is None

    def test_load_invalid_jsonl_file(self, temp_dir):
        """Should return None if JSONL has invalid line"""
        invalid_file = os.path.join(temp_dir, "invalid.jsonl")
        with open(invalid_file, "w") as f:
            f.write('{"valid": true}\n')
            f.write('invalid json line\n')

        result = load_objects_from_file(invalid_file)
        assert result is None

    def test_load_non_existent_file(self):
        """Should return None for non-existent file"""
        result = load_objects_from_file("/tmp/does_not_exist_12345.json")
        assert result is None

    def test_load_unsupported_file_extension(self, temp_dir):
        """Should return None for unsupported file types"""
        txt_file = os.path.join(temp_dir, "data.txt")
        with open(txt_file, "w") as f:
            f.write("not json")

        result = load_objects_from_file(txt_file)
        assert result is None

    def test_load_preserves_data_types(self, temp_dir):
        """Should preserve various JSON data types"""
        file_path = os.path.join(temp_dir, "types.json")
        data = [
            {
                "string": "hello",
                "number": 42,
                "float": 3.14,
                "bool_true": True,
                "bool_false": False,
                "null": None,
                "array": [1, 2, 3],
                "object": {"nested": "value"}
            }
        ]
        with open(file_path, "w") as f:
            json.dump(data, f)

        result = load_objects_from_file(file_path)

        assert result is not None
        obj = result[0]
        assert obj["string"] == "hello"
        assert obj["number"] == 42
        assert obj["float"] == 3.14
        assert obj["bool_true"] is True
        assert obj["bool_false"] is False
        assert obj["null"] is None
        assert obj["array"] == [1, 2, 3]
        assert obj["object"] == {"nested": "value"}


class TestLoadCollection:
    """Test load_collection source descriptor handling"""

    def test_load_buffered_stdin_source(self):
        """Should return content from buffered_stdin source"""
        source = {
            "type": "buffered_stdin",
            "content": [{"id": 1}, {"id": 2}, {"id": 3}]
        }

        result = load_collection(source)

        assert len(result) == 3
        assert result[0]["id"] == 1

    def test_load_directory_source(self, temp_dir):
        """Should load objects from multiple files in directory source"""
        # Create multiple files
        file1 = os.path.join(temp_dir, "file1.json")
        file2 = os.path.join(temp_dir, "file2.json")

        with open(file1, "w") as f:
            json.dump([{"id": 1}, {"id": 2}], f)

        with open(file2, "w") as f:
            json.dump([{"id": 3}, {"id": 4}], f)

        source = {
            "type": "directory",
            "files": [file1, file2]
        }

        result = load_collection(source)

        assert len(result) == 4
        assert result[0]["id"] == 1
        assert result[3]["id"] == 4

    def test_load_jsonl_source(self, sample_jsonl_file):
        """Should load JSONL file from source descriptor"""
        source = {
            "type": "jsonl",
            "path": sample_jsonl_file
        }

        result = load_collection(source)

        assert len(result) == 3
        assert result[0]["name"] == "Alice"

    def test_load_json_array_source(self, sample_json_array_file):
        """Should load JSON array file from source descriptor"""
        source = {
            "type": "json_array",
            "path": sample_json_array_file
        }

        result = load_collection(source)

        assert len(result) == 3
        assert result[0]["name"] == "Alice"

    def test_load_source_with_missing_path(self):
        """Should return empty list if path doesn't exist"""
        source = {
            "type": "json_array",
            "path": "/tmp/nonexistent_12345.json"
        }

        result = load_collection(source)
        assert len(result) == 0

    def test_load_unsupported_source_type(self):
        """Should raise NotImplementedError for unknown source types"""
        source = {
            "type": "unknown_type",
            "path": "/some/path"
        }

        with pytest.raises(NotImplementedError):
            load_collection(source)

    def test_load_directory_skips_invalid_files(self, temp_dir):
        """Should skip files that can't be loaded in directory source"""
        valid_file = os.path.join(temp_dir, "valid.json")
        invalid_file = os.path.join(temp_dir, "invalid.json")

        with open(valid_file, "w") as f:
            json.dump([{"id": 1}], f)

        with open(invalid_file, "w") as f:
            f.write("invalid json")

        source = {
            "type": "directory",
            "files": [valid_file, invalid_file]
        }

        result = load_collection(source)

        # Should only get objects from valid file
        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_load_buffered_stdin_with_empty_content(self):
        """Should handle empty content in buffered_stdin"""
        source = {
            "type": "buffered_stdin",
            "content": []
        }

        result = load_collection(source)
        assert len(result) == 0

    def test_load_buffered_stdin_without_content_key(self):
        """Should handle missing content key in buffered_stdin"""
        source = {
            "type": "buffered_stdin"
        }

        result = load_collection(source)
        assert len(result) == 0


class TestLoadObjectsFromString:
    """Test load_objects_from_string format detection and parsing"""

    def test_load_json_array_string(self):
        """Should parse JSON array string"""
        content = '[{"id": 1}, {"id": 2}, {"id": 3}]'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert len(objects) == 3
        assert format_type == "json_array"
        assert objects[0]["id"] == 1

    def test_load_json_object_string(self):
        """Should parse single JSON object as list of one"""
        content = '{"id": 1, "name": "Alice"}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert len(objects) == 1
        assert format_type == "json_object"
        assert objects[0]["name"] == "Alice"

    def test_load_jsonl_string(self):
        """Should parse JSONL string (multiple lines)"""
        content = '{"id": 1}\n{"id": 2}\n{"id": 3}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert len(objects) == 3
        assert format_type == "jsonl"
        assert objects[0]["id"] == 1

    def test_load_jsonl_string_with_blank_lines(self):
        """Should skip blank lines in JSONL string"""
        content = '{"id": 1}\n\n{"id": 2}\n  \n{"id": 3}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert len(objects) == 3
        assert format_type == "jsonl"

    def test_load_empty_string(self):
        """Should return None for empty string"""
        objects, format_type = load_objects_from_string("")

        assert objects is None
        assert format_type is None

    def test_load_whitespace_only_string(self):
        """Should return None for whitespace-only string"""
        objects, format_type = load_objects_from_string("   \n  \t  ")

        assert objects is None
        assert format_type is None

    def test_load_invalid_json_string(self):
        """Should return None for invalid JSON"""
        content = '{"invalid": json}'

        objects, format_type = load_objects_from_string(content)

        assert objects is None
        assert format_type is None

    def test_load_mixed_valid_invalid_jsonl(self):
        """Should return None if any JSONL line is invalid"""
        content = '{"id": 1}\ninvalid line\n{"id": 3}'

        objects, format_type = load_objects_from_string(content)

        assert objects is None
        assert format_type is None

    def test_load_preserves_data_types_from_string(self):
        """Should preserve JSON data types when parsing string"""
        content = '''
        {
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "bool": true,
            "null": null,
            "array": [1, 2, 3],
            "object": {"key": "value"}
        }
        '''

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert format_type == "json_object"
        obj = objects[0]
        assert obj["string"] == "hello"
        assert obj["number"] == 42
        assert obj["float"] == 3.14
        assert obj["bool"] is True
        assert obj["null"] is None
        assert obj["array"] == [1, 2, 3]
        assert obj["object"]["key"] == "value"

    def test_load_single_line_jsonl(self):
        """Should handle single-line JSONL as fallback"""
        # If it's valid as JSON object, it should be detected as json_object first
        content = '{"id": 1}'

        objects, format_type = load_objects_from_string(content)

        # Should be detected as JSON object, not JSONL
        assert format_type == "json_object"
        assert len(objects) == 1

    def test_load_multiline_json_object(self):
        """Should parse prettified JSON object correctly"""
        content = '''{
            "id": 1,
            "name": "Alice",
            "age": 30
        }'''

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert format_type == "json_object"
        assert objects[0]["name"] == "Alice"


class TestLoadObjectsFromStringEdgeCases:
    """Test edge cases for string parsing"""

    def test_load_nested_arrays(self):
        """Should handle nested array structures"""
        content = '[[1, 2], [3, 4], [5, 6]]'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert format_type == "json_array"
        assert len(objects) == 3
        assert objects[0] == [1, 2]

    def test_load_deeply_nested_objects(self):
        """Should handle deeply nested object structures"""
        content = '''{
            "level1": {
                "level2": {
                    "level3": {
                        "value": 42
                    }
                }
            }
        }'''

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert objects[0]["level1"]["level2"]["level3"]["value"] == 42

    def test_load_unicode_characters(self):
        """Should handle unicode characters in JSON"""
        content = '{"name": "アリス", "emoji": "😀"}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert objects[0]["name"] == "アリス"
        assert objects[0]["emoji"] == "😀"

    def test_load_escaped_characters(self):
        """Should handle escaped characters in JSON"""
        content = r'{"message": "Line 1\nLine 2\tTabbed"}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        # JSON parser converts escape sequences to actual characters
        assert "\n" in objects[0]["message"]  # Newline character
        assert "\t" in objects[0]["message"]  # Tab character

    def test_load_large_numbers(self):
        """Should handle large numbers correctly"""
        content = '{"big": 999999999999999999, "small": -999999999999999999}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert objects[0]["big"] == 999999999999999999
        assert objects[0]["small"] == -999999999999999999

    def test_load_special_float_values(self):
        """Should handle float precision"""
        content = '{"pi": 3.141592653589793, "e": 2.718281828459045}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert objects[0]["pi"] == 3.141592653589793

    def test_load_empty_array(self):
        """Should handle empty JSON array"""
        content = '[]'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert len(objects) == 0
        assert format_type == "json_array"

    def test_load_empty_object(self):
        """Should handle empty JSON object"""
        content = '{}'

        objects, format_type = load_objects_from_string(content)

        assert objects is not None
        assert len(objects) == 1
        assert objects[0] == {}
        assert format_type == "json_object"

"""
Basic tests for console script utilities that don't depend on the old JafResultSet system.
"""

import pytest
import os
import json
import sys
from unittest import mock

from jaf.console_script import main
from jaf.io_utils import walk_data_files, load_objects_from_file


# Helper to create dummy files
def create_dummy_file(path, content, is_jsonl=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        if is_jsonl:
            for item in content:
                f.write(json.dumps(item) + "\n")
        elif isinstance(content, str):
            f.write(content)
        else:
            json.dump(content, f)


class TestWalkDataFiles:
    def test_empty_directory(self, tmp_path):
        assert list(walk_data_files(str(tmp_path), recursive=False)) == []

    def test_no_json_jsonl_files(self, tmp_path):
        create_dummy_file(tmp_path / "file.txt", "text content")
        create_dummy_file(tmp_path / "other.dat", "binary data")
        assert list(walk_data_files(str(tmp_path), recursive=False)) == []

    def test_finds_json_and_jsonl_files(self, tmp_path):
        json_file = tmp_path / "data.json"
        jsonl_file = tmp_path / "data.jsonl"
        create_dummy_file(json_file, {})
        create_dummy_file(jsonl_file, [{}], is_jsonl=True)

        expected_files = sorted([str(json_file), str(jsonl_file)])
        assert (
            sorted(list(walk_data_files(str(tmp_path), recursive=False)))
            == expected_files
        )

    def test_recursive_walk(self, tmp_path):
        sub_dir = tmp_path / "sub"
        json_file_root = tmp_path / "root.json"
        json_file_sub = sub_dir / "sub.json"
        create_dummy_file(json_file_root, {})
        create_dummy_file(json_file_sub, {})

        expected_files = sorted([str(json_file_root), str(json_file_sub)])
        assert (
            sorted(list(walk_data_files(str(tmp_path), recursive=True)))
            == expected_files
        )

    def test_non_recursive_walk(self, tmp_path):
        sub_dir = tmp_path / "sub"
        json_file_root = tmp_path / "root.json"
        json_file_sub = sub_dir / "sub.json"  # Should not be found
        create_dummy_file(json_file_root, {})
        create_dummy_file(json_file_sub, {})

        expected_files = [str(json_file_root)]
        assert (
            sorted(list(walk_data_files(str(tmp_path), recursive=False)))
            == expected_files
        )

    def test_non_existent_directory(self, tmp_path):
        non_existent_dir = tmp_path / "does_not_exist"
        assert list(walk_data_files(str(non_existent_dir), recursive=False)) == []


class TestLoadObjectsFromFile:
    def test_load_valid_json_list(self, tmp_path):
        file_path = tmp_path / "data.json"
        content = [{"id": 1}, {"id": 2}]
        create_dummy_file(file_path, content)
        assert load_objects_from_file(str(file_path)) == content

    def test_load_valid_json_single_object(self, tmp_path):
        file_path = tmp_path / "data.json"
        content = {"id": 1}
        create_dummy_file(file_path, content)
        assert load_objects_from_file(str(file_path)) == [content]

    def test_load_valid_jsonl(self, tmp_path):
        file_path = tmp_path / "data.jsonl"
        content = [{"id": 1}, {"id": 2}]
        create_dummy_file(file_path, content, is_jsonl=True)
        assert load_objects_from_file(str(file_path)) == content

    def test_load_valid_jsonl_two_strings(self, tmp_path):
        file_path = tmp_path / "data.jsonl"
        content = ["string1", "string2"]
        create_dummy_file(file_path, content, is_jsonl=True)
        assert load_objects_from_file(str(file_path)) == content

    def test_load_empty_json_array(self, tmp_path):
        file_path = tmp_path / "empty.json"
        create_dummy_file(file_path, [])
        assert load_objects_from_file(str(file_path)) is None

    def test_load_empty_jsonl(self, tmp_path):
        file_path = tmp_path / "empty.jsonl"
        create_dummy_file(file_path, "", is_jsonl=False)  # Empty file
        assert load_objects_from_file(str(file_path)) is None

    def test_load_json_with_mixed_types_in_list(self, tmp_path, caplog):
        file_path = tmp_path / "mixed.json"
        content = [{"id": 1}, "string", {"id": 2}, None]
        create_dummy_file(file_path, content)
        expected = [{"id": 1}, "string", {"id": 2}, None]
        assert load_objects_from_file(str(file_path)) == expected

    def test_load_jsonl_with_mixed_types(self, tmp_path, caplog):
        file_path = tmp_path / "mixed.jsonl"
        raw_content = '[{"id": 1}]\n"string"\n\n{"id": 2}\nnull\n'
        create_dummy_file(file_path, raw_content, is_jsonl=False)  # Write raw
        expected = [[{"id": 1}], "string", {"id": 2}, None]
        assert load_objects_from_file(str(file_path)) == expected

    def test_load_malformed_json(self, tmp_path, caplog):
        file_path = tmp_path / "malformed.json"
        create_dummy_file(file_path, "{'id': 1")  # Malformed
        assert load_objects_from_file(str(file_path)) is None
        assert "JSON decode error" in caplog.text

    def test_load_malformed_jsonl_line(self, tmp_path, caplog):
        file_path = tmp_path / "malformed.jsonl"
        raw_content = '{"id": 2, "val":}\n{"id": 3}\n'
        create_dummy_file(file_path, raw_content, is_jsonl=False)  # Write raw
        # The new behavior is to fail fast and return None if any line is bad.
        assert load_objects_from_file(str(file_path)) is None
        assert "JSON decode error" in caplog.text

    def test_load_non_existent_file(self, tmp_path, caplog):
        file_path = tmp_path / "non_existent.json"
        assert load_objects_from_file(str(file_path)) is None

    def test_load_json_value_string(self, tmp_path, caplog):
        file_path = tmp_path / "scalar.json"
        create_dummy_file(file_path, '"just a JSON string value"\n')
        assert load_objects_from_file(str(file_path)) == ["just a JSON string value"]

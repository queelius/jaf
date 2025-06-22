import pytest
import os
import json
import sys
from unittest import mock
from typing import List, Dict, Any

# Assuming your project structure allows this import:
# If jaf is a package, and console_script is a module within it.
from jaf.console_script import (
    walk_data_files,
    load_objects_from_file,
    main,
    load_jaf_result_set_from_input
)
from jaf.io_utils import walk_data_files, load_objects_from_file
from jaf.result_set import JafResultSet # For type hints and potentially mocking
from jaf.jaf import jafError # For exception checking

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
        assert sorted(list(walk_data_files(str(tmp_path), recursive=False))) == expected_files

    def test_recursive_walk(self, tmp_path):
        sub_dir = tmp_path / "sub"
        json_file_root = tmp_path / "root.json"
        json_file_sub = sub_dir / "sub.json"
        create_dummy_file(json_file_root, {})
        create_dummy_file(json_file_sub, {})

        expected_files = sorted([str(json_file_root), str(json_file_sub)])
        assert sorted(list(walk_data_files(str(tmp_path), recursive=True))) == expected_files

    def test_non_recursive_walk(self, tmp_path):
        sub_dir = tmp_path / "sub"
        json_file_root = tmp_path / "root.json"
        json_file_sub = sub_dir / "sub.json" # Should not be found
        create_dummy_file(json_file_root, {})
        create_dummy_file(json_file_sub, {})

        expected_files = [str(json_file_root)]
        assert sorted(list(walk_data_files(str(tmp_path), recursive=False))) == expected_files

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
        create_dummy_file(file_path, "", is_jsonl=False) # Empty file
        assert load_objects_from_file(str(file_path)) is None

    def test_load_json_with_mixed_types_in_list(self, tmp_path, caplog):
        file_path = tmp_path / "mixed.json"
        content = [{"id": 1}, "string", {"id": 2}, None]
        create_dummy_file(file_path, content)
        expected = [{"id": 1}, "string", {"id": 2}, None]
        assert load_objects_from_file(str(file_path)) == expected

    def test_load_jsonl_with_mixed_types(self, tmp_path, caplog):
        file_path = tmp_path / "mixed.jsonl"
        raw_content = '[{"id": 1}]\n\"string\"\n\n{"id": 2}\nnull\n'
        create_dummy_file(file_path, raw_content, is_jsonl=False) # Write raw
        expected = [[{"id": 1}], "string", {"id": 2}, None]
        assert load_objects_from_file(str(file_path)) == expected

    def test_load_malformed_json(self, tmp_path, caplog):
        file_path = tmp_path / "malformed.json"
        create_dummy_file(file_path, "{'id': 1") # Malformed
        assert load_objects_from_file(str(file_path)) is None
        assert "JSON decode error" in caplog.text

    def test_load_malformed_jsonl_line(self, tmp_path, caplog):
        file_path = tmp_path / "malformed.jsonl"
        raw_content = '{"id": 2, "val":}\n{"id": 3}\n'
        create_dummy_file(file_path, raw_content, is_jsonl=False) # Write raw
        # The new behavior is to fail fast and return None if any line is bad.
        assert load_objects_from_file(str(file_path)) is None
        assert "JSON decode error" in caplog.text

    def test_load_non_existent_file(self, tmp_path, caplog):
        file_path = tmp_path / "non_existent.json"
        assert load_objects_from_file(str(file_path)) is None
        # The function itself logs, so we check for that if desired,
        # but the primary check is the None return.
        # assert f"Error reading or parsing {file_path}" in caplog.text # This depends on exact log message

    def test_load_json_value_string(self, tmp_path, caplog):
        file_path = tmp_path / "scalar.json"
        create_dummy_file(file_path, "\"just a JSON string value\"\n")
        assert load_objects_from_file(str(file_path)) == ["just a JSON string value"]

class TestLoadJafResultSetFromInput:
    def test_load_from_valid_file(self, tmp_path):
        file_path = tmp_path / "rs.json"
        rs_data = {"indices": [0, 2], "collection_size": 3, "collection_id": "test_id"}
        create_dummy_file(file_path, rs_data)

        with mock.patch("jaf.result_set.JafResultSet.from_dict", 
                        return_value=JafResultSet(set(rs_data["indices"]), rs_data["collection_size"], rs_data["collection_id"]),
                        create=True) as mock_from_dict: # Added create=True
            result = load_jaf_result_set_from_input(str(file_path), "input_rs1")
            mock_from_dict.assert_called_once_with(rs_data)
            assert isinstance(result, JafResultSet)
            assert result.indices == {0, 2}

    def test_load_from_stdin(self, tmp_path):
        rs_data = {"indices": [1], "collection_size": 2, "collection_id": "stdin_id"}
        json_rs_data = json.dumps(rs_data)

        with mock.patch("sys.stdin.read", return_value=json_rs_data):
            with mock.patch("jaf.result_set.JafResultSet.from_dict", return_value=JafResultSet(set(rs_data["indices"]), rs_data["collection_size"], rs_data["collection_id"])) as mock_from_dict:
                result = load_jaf_result_set_from_input("-", "input_rs1")
                mock_from_dict.assert_called_once_with(rs_data)
                assert isinstance(result, JafResultSet)
                assert result.indices == {1}

    def test_load_non_existent_file_exits(self, tmp_path, capsys):
        with pytest.raises(SystemExit) as e:
            load_jaf_result_set_from_input(str(tmp_path / "ghost.json"), "input_rs1")
        assert e.value.code != 0
        captured = capsys.readouterr()
        assert "JafResultSet file" in captured.err and "not found" in captured.err
        
    def test_load_malformed_json_exits(self, tmp_path, capsys):
        file_path = tmp_path / "bad_rs.json"
        create_dummy_file(file_path, "{'bad': json")
        with pytest.raises(SystemExit) as e:
            load_jaf_result_set_from_input(str(file_path), "input_rs1")
        assert e.value.code != 0
        captured = capsys.readouterr()
        assert "not valid JSON" in captured.err

    def test_load_invalid_rs_structure_exits(self, tmp_path, capsys):
        file_path = tmp_path / "invalid_rs.json"
        # Valid JSON, but not a valid JafResultSet structure
        rs_data = {"foo": "bar"}
        create_dummy_file(file_path, rs_data)

        # Mock from_dict to simulate a structure error
        with mock.patch("jaf.result_set.JafResultSet.from_dict", side_effect=ValueError("Invalid structure")) as mock_from_dict:
            with pytest.raises(SystemExit) as e:
                load_jaf_result_set_from_input(str(file_path), "input_rs1")
            assert e.value.code != 0
            captured = capsys.readouterr()
            assert "invalid structure" in captured.err
            mock_from_dict.assert_called_once_with(rs_data)

    def test_load_empty_input_exits(self, tmp_path, capsys):
        file_path = tmp_path / "empty_rs.json"
        create_dummy_file(file_path, "") # Empty file
        with pytest.raises(SystemExit) as e:
            load_jaf_result_set_from_input(str(file_path), "input_rs1")
        assert e.value.code != 0
        captured = capsys.readouterr()
        assert "No data provided" in captured.err

# --- Tests for main() and command handlers (more complex, often integration-style) ---
# These would typically involve mocking sys.argv, capturing stdout/stderr,
# and mocking the core jaf() function and JafResultSet methods.

@pytest.fixture
def mock_jaf_result_set_instance():
    """Fixture to create a mock JafResultSet instance."""
    mock_rs = mock.Mock(spec=JafResultSet)
    mock_rs.indices = {0, 1}
    mock_rs.collection_size = 2
    mock_rs.collection_id = "mock_collection"
    mock_rs.collection_source = None
    mock_rs.to_dict.return_value = {
        "indices": sorted(list(mock_rs.indices)),
        "collection_size": mock_rs.collection_size,
        "collection_id": mock_rs.collection_id,
        "collection_source": mock_rs.collection_source
    }
    return mock_rs


class TestMainConsoleScriptFilter:
    @mock.patch("jaf.console_script.load_objects_from_file")
    @mock.patch("jaf.console_script.jaf") # Mock the core jaf function
    def test_filter_single_file_outputs_jafresultset_json(
        self, mock_jaf_func, mock_load_objects, tmp_path, capsys, mock_jaf_result_set_instance
    ):
        input_file = tmp_path / "data.json"
        create_dummy_file(input_file, [{"id":1},{"id":2}])
        mock_load_objects.return_value = [{"id":1},{"id":2}] # Mock loaded data
        
        # Configure the mock jaf function to return our mock JafResultSet
        mock_jaf_func.return_value = mock_jaf_result_set_instance
        
        test_args = ["jaf", "filter", str(input_file), "--query", "[[\"key\", \"id\"]]"]
        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err
        
        expected_output_dict = mock_jaf_result_set_instance.to_dict()
        # Output should be a single JafResultSet JSON object
        assert json.loads(captured.out) == expected_output_dict
        
        mock_load_objects.assert_called_once_with(str(input_file))
        abs_file_path = os.path.abspath(str(input_file))
        mock_jaf_func.assert_called_once_with(
            mock_load_objects.return_value, 
            [["key", "id"]], 
            collection_id=abs_file_path,
            collection_source={"type": "json_array", "path": abs_file_path}
        )


class TestMainConsoleScriptBooleanOps:
    @mock.patch("jaf.console_script.load_jaf_result_set_from_input")
    def test_boolean_and_operation(self, mock_load_rs, capsys, mock_jaf_result_set_instance):
        rs1_path = "rs1.json"
        rs2_path = "rs2.json"

        mock_rs1 = mock.Mock(spec=JafResultSet)
        mock_rs1.indices = {0, 1, 2}
        mock_rs1.collection_size = 3
        mock_rs1.collection_id = "test"
        
        mock_rs2 = mock.Mock(spec=JafResultSet)
        mock_rs2.indices = {1, 2, 3}
        mock_rs2.collection_size = 3 # Compatible
        mock_rs2.collection_id = "test" # Compatible

        # Mock the AND method
        mock_and_result = mock.Mock(spec=JafResultSet)
        # Ensure to_dict returns a dict that matches the expected JSON output
        mock_and_result.to_dict.return_value = {"indices": [1,2], "collection_size": 3, "collection_id": "test", "collection_source": None}
        mock_rs1.AND.return_value = mock_and_result

        # load_jaf_result_set_from_input will be called
"""
Tests for the streaming console script functionality.
"""

import pytest
import json
import sys
import os
from unittest import mock
from io import StringIO

from jaf.console_script import main
from jaf.lazy_streams import LazyDataStream, FilteredStream


class TestStreamingConsoleScript:
    """Test the new streaming console script commands"""

    def test_stream_command_with_filter(self, tmp_path, capsys):
        """Test stream command with filter operation"""
        # Create test data file
        test_file = tmp_path / "data.json"
        test_data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        test_file.write_text(json.dumps(test_data))

        # Use stream command with filter (stream defaults to eager evaluation)
        test_args = ["jaf", "stream", str(test_file), "--filter", '["gt?", "@age", 30]']

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # Stream command should output actual data (eager by default)
        lines = captured.out.strip().split("\n")
        assert len(lines) == 1
        assert json.loads(lines[0])["name"] == "Charlie"

    def test_filter_command_with_file(self, tmp_path, capsys):
        """Test filter command with a JSON file"""
        # Create test data file
        test_file = tmp_path / "data.json"
        test_data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        test_file.write_text(json.dumps(test_data))

        # Run filter command
        test_args = ["jaf", "filter", str(test_file), '["gt?", "@age", 30]']

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # Should output stream descriptor (lazy by default)
        output = json.loads(captured.out)
        assert output["stream_type"] == "FilteredStream"
        assert output["query"] == ["gt?", "@age", 30]

    def test_map_command_with_stdin(self, capsys):
        """Test map command reading from stdin"""
        test_data = [{"name": "Alice", "score": 95}, {"name": "Bob", "score": 87}]
        stdin_data = "\n".join(json.dumps(item) for item in test_data)

        test_args = ["jaf", "map", "-", '["upper-case", "@name"]']

        with mock.patch("sys.stdin", StringIO(stdin_data)):
            with mock.patch.object(sys, "argv", test_args):
                main()

        captured = capsys.readouterr()
        assert not captured.err

        # Map command outputs stream descriptor by default
        output = json.loads(captured.out)
        assert output["stream_type"] == "MappedStream"
        assert output["expression"] == ["upper-case", "@name"]

    def test_take_command(self, tmp_path, capsys):
        """Test take command with fibonacci source"""
        # Create a stream descriptor file
        stream_file = tmp_path / "fib.json"
        stream_desc = {"type": "fibonacci"}
        stream_file.write_text(json.dumps(stream_desc))

        test_args = ["jaf", "take", str(stream_file), "5"]

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # Take command outputs stream descriptor by default
        output = json.loads(captured.out)
        assert output["collection_source"]["type"] == "take"
        assert output["collection_source"]["n"] == 5

    def test_and_command_with_filtered_streams(self, tmp_path, capsys):
        """Test AND operation on filtered streams"""
        # Create test data
        test_file = tmp_path / "data.json"
        test_data = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True},
            {"name": "Diana", "age": 28, "active": True},
        ]
        test_file.write_text(json.dumps(test_data))

        # Create first filtered stream JSON
        stream1_file = tmp_path / "stream1.json"
        stream1_desc = {
            "stream_type": "FilteredStream",
            "query": ["eq?", "@active", True],
            "collection_source": {
                "type": "filter",
                "query": ["eq?", "@active", True],
                "inner_source": {"type": "file", "path": str(test_file)},
            },
        }
        stream1_file.write_text(json.dumps(stream1_desc))

        # Create second filtered stream JSON
        stream2_file = tmp_path / "stream2.json"
        stream2_desc = {
            "stream_type": "FilteredStream",
            "query": ["gt?", "@age", 30],
            "collection_source": {
                "type": "filter",
                "query": ["gt?", "@age", 30],
                "inner_source": {"type": "file", "path": str(test_file)},
            },
        }
        stream2_file.write_text(json.dumps(stream2_desc))

        # Run AND command
        test_args = ["jaf", "and", str(stream1_file), str(stream2_file)]

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # AND command outputs stream descriptor by default
        output = json.loads(captured.out)
        assert output["stream_type"] == "FilteredStream"
        # The query should be a complex AND of both conditions
        assert output["query"][0] == "and"

    def test_info_command(self, tmp_path, capsys):
        """Test info command showing stream metadata"""
        # Create a stream descriptor with collection_source
        stream_desc = {"collection_source": {"type": "fibonacci"}}
        stream_file = tmp_path / "stream.json"
        stream_file.write_text(json.dumps(stream_desc))

        test_args = ["jaf", "info", str(stream_file)]

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # Should output stream info
        output = json.loads(captured.out)
        assert output["type"] == "LazyDataStream"
        assert output["source_type"] == "fibonacci"
        assert "pipeline" in output

    def test_eval_flag_outputs_data(self, tmp_path, capsys):
        """Test that --eval flag outputs data instead of stream descriptor"""
        test_file = tmp_path / "data.json"
        test_data = [{"x": 1}, {"x": 2}, {"x": 3}]
        test_file.write_text(json.dumps(test_data))

        test_args = ["jaf", "filter", str(test_file), '["gt?", "@x", 1]', "--eval"]

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # Should output actual data with --eval flag
        lines = captured.out.strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["x"] == 2
        assert json.loads(lines[1])["x"] == 3

    def test_eval_command(self, tmp_path, capsys):
        """Test eval command evaluates stream descriptors"""
        # Create a stream descriptor file
        stream_file = tmp_path / "stream.json"
        stream_desc = {
            "stream_type": "FilteredStream",
            "query": ["gt?", "@score", 90],
            "collection_source": {
                "type": "memory",
                "data": [
                    {"name": "Alice", "score": 95},
                    {"name": "Bob", "score": 87},
                    {"name": "Charlie", "score": 92},
                ],
            },
        }
        stream_file.write_text(json.dumps(stream_desc))

        test_args = ["jaf", "eval", str(stream_file)]

        with mock.patch.object(sys, "argv", test_args):
            main()

        captured = capsys.readouterr()
        assert not captured.err

        # Should output the evaluated results
        lines = captured.out.strip().split("\n")
        results = [json.loads(line) for line in lines]

        # Filter results should only include scores > 90
        high_scorers = [r for r in results if r["score"] > 90]
        assert len(high_scorers) == 2
        names = {r["name"] for r in high_scorers}
        assert names == {"Alice", "Charlie"}

    def test_piping_operations(self, tmp_path, capsys):
        """Test piping output from one command to another"""
        test_file = tmp_path / "data.json"
        test_data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
            {"name": "Charlie", "score": 92},
        ]
        test_file.write_text(json.dumps(test_data))

        # First command: filter (lazy by default)
        test_args1 = ["jaf", "filter", str(test_file), '["gt?", "@score", 90]']

        with mock.patch.object(sys, "argv", test_args1):
            main()

        captured1 = capsys.readouterr()
        stream_desc = captured1.out

        # Second command: map reading from stdin
        test_args2 = ["jaf", "map", "-", "@name"]

        with mock.patch("sys.stdin", StringIO(stream_desc)):
            with mock.patch.object(sys, "argv", test_args2):
                main()

        captured2 = capsys.readouterr()
        assert not captured2.err

        # Should get stream descriptor for the mapped stream
        output = json.loads(captured2.out)
        assert output["stream_type"] == "MappedStream"
        # Expression is stored as AST
        assert output["expression"] == ["@", [["key", "name"]]]

    def test_error_handling_invalid_query(self, tmp_path, capsys):
        """Test error handling for invalid query with eager evaluation"""
        # Create a test file
        test_file = tmp_path / "data.json"
        test_file.write_text(json.dumps([{"x": 1}, {"x": 2}, {"x": 3}]))

        # Use --eval to force eager evaluation and trigger the error
        test_args = ["jaf", "filter", str(test_file), '["invalid-op", "@x"]', "--eval"]

        with pytest.raises(SystemExit) as exc_info:
            with mock.patch.object(sys, "argv", test_args):
                main()

        assert exc_info.value.code != 0
        # The error is logged, not printed to stderr

    def test_help_command(self, capsys):
        """Test help output"""
        test_args = ["jaf", "--help"]

        with pytest.raises(SystemExit) as exc_info:
            with mock.patch.object(sys, "argv", test_args):
                main()

        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "usage: jaf" in captured.out
        assert "stream" in captured.out
        assert "filter" in captured.out
        assert "map" in captured.out

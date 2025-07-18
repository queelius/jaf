"""Test CLI source specification flags."""

import os
import json
import tempfile
import pytest
from jaf.console_script import main
import sys
from io import StringIO


def run_jaf_command(monkeypatch, argv, capture_output=True):
    """Helper to run a JAF command and capture output."""
    monkeypatch.setattr(sys, "argv", argv)
    
    if capture_output:
        output = StringIO()
        error = StringIO()
        monkeypatch.setattr(sys, "stdout", output)
        monkeypatch.setattr(sys, "stderr", error)
        
        try:
            main()
        except SystemExit as e:
            if e.code != 0:
                print(f"Error output: {error.getvalue()}")
                raise
        
        return output.getvalue()
    else:
        main()


def test_filter_with_recursive_pattern(tmp_path, monkeypatch):
    """Test filtering with --recursive and --pattern flags."""
    # Create a directory structure with JSON files
    (tmp_path / "subdir").mkdir()
    
    # Create some JSON files
    with open(tmp_path / "data1.json", "w") as f:
        json.dump([{"type": "A", "value": 1}, {"type": "B", "value": 2}], f)
    
    with open(tmp_path / "data2.jsonl", "w") as f:
        f.write('{"type": "A", "value": 3}\n')
        f.write('{"type": "C", "value": 4}\n')
    
    with open(tmp_path / "subdir" / "data3.json", "w") as f:
        json.dump([{"type": "A", "value": 5}], f)
    
    # Test without recursive - should only find files in top dir
    output = run_jaf_command(
        monkeypatch,
        ["jaf", "filter", str(tmp_path), '["eq?", "@type", "A"]', "--pattern", "*.json*", "--eval"]
    )
    
    results = [json.loads(line) for line in output.strip().split("\n") if line]
    assert len(results) == 2  # Only from data1.json and data2.jsonl
    
    # Test with recursive - should find all files
    output = run_jaf_command(
        monkeypatch,
        ["jaf", "filter", str(tmp_path), '["eq?", "@type", "A"]', "--recursive", "--pattern", "*.json*", "--eval"]
    )
    
    results = [json.loads(line) for line in output.strip().split("\n") if line]
    assert len(results) == 3  # From all three files


def test_csv_with_delimiter_headers(tmp_path, monkeypatch):
    """Test CSV processing with custom delimiter and headers flags."""
    # Create a CSV file with semicolon delimiter
    csv_file = tmp_path / "data.csv"
    with open(csv_file, "w") as f:
        f.write("name;age;city\n")
        f.write("Alice;30;NYC\n")
        f.write("Bob;25;LA\n")
    
    # Test with custom delimiter and headers
    output = run_jaf_command(
        monkeypatch,
        ["jaf", "filter", str(csv_file), '["gt?", "@age", 28]', "--delimiter", ";", "--headers", "--eval"]
    )
    
    results = [json.loads(line) for line in output.strip().split("\n") if line]
    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    
    # Test with no headers
    tsv_file = tmp_path / "data.tsv"
    with open(tsv_file, "w") as f:
        f.write("Alice\t30\tNYC\n")
        f.write("Bob\t25\tLA\n")
    
    output = run_jaf_command(
        monkeypatch,
        ["jaf", "map", str(tsv_file), '["@", [["key", "1"]]]', "--delimiter", "\t", "--no-headers", "--eval"]
    )
    
    results = [json.loads(line) for line in output.strip().split("\n") if line]
    assert results == ["30", "25"]  # CSV parser returns strings


def test_stream_command_with_source_flags(tmp_path, monkeypatch):
    """Test stream command with source specification flags."""
    # Create test files
    (tmp_path / "logs").mkdir()
    
    with open(tmp_path / "logs" / "app.jsonl", "w") as f:
        f.write('{"level": "INFO", "msg": "Started"}\n')
        f.write('{"level": "ERROR", "msg": "Failed"}\n')
    
    with open(tmp_path / "logs" / "debug.log", "w") as f:
        f.write('{"level": "DEBUG", "msg": "Trace"}\n')
    
    # Use stream command with directory source and pattern
    output = run_jaf_command(
        monkeypatch,
        ["jaf", "stream", str(tmp_path / "logs"), "--pattern", "*.jsonl", 
         "--filter", '["eq?", "@level", "ERROR"]', "--map", '"@msg"']
    )
    
    results = [json.loads(line) for line in output.strip().split("\n") if line]
    assert results == ["Failed"]
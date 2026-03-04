"""
Pytest configuration for JAF tests.

Handles conditional test collection to avoid import conflicts with optional dependencies.

Optional dependency tests:
- test_api.py: Requires FastAPI (pip install jaf[api])
- test_mcp_server.py: Requires MCP SDK (pip install jaf[mcp])

Run specific test files directly when dependencies are available:
    pytest tests/test_api.py -v
    pytest tests/test_mcp_server.py -v
"""

import pytest


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "mcp: marks tests as requiring the MCP SDK"
    )
    config.addinivalue_line(
        "markers", "api: marks tests as requiring FastAPI"
    )


def pytest_ignore_collect(collection_path, config):
    """
    Ignore optional dependency test files during full test suite runs.

    This prevents import errors when optional dependencies aren't installed.
    Tests can still be run directly when their dependencies are available.
    """
    args = config.invocation_params.args
    filename = collection_path.name

    # Files that require optional dependencies
    optional_dep_tests = {
        "test_mcp_server.py": "mcp",
        # test_api.py uses pytest.importorskip so it handles itself
    }

    if filename in optional_dep_tests:
        # Allow if specifically requested
        for arg in args:
            if filename in str(arg):
                return False
        # Skip if running all tests
        return True

    return False

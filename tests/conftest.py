"""
Pytest configuration for JAF tests.

Handles conditional test collection to avoid import conflicts.
"""

import pytest


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "mcp: marks tests as requiring the MCP SDK"
    )


def pytest_ignore_collect(collection_path, config):
    """
    Ignore test_mcp_server.py during collection when running full test suite.

    The MCP SDK has metaclass conflicts that cause collection errors when
    running alongside other tests. Run MCP tests separately:
        pytest tests/test_mcp_server.py -v
    """
    # Check if we're running a specific file or the whole suite
    args = config.invocation_params.args

    # If running full suite (no specific test files given), skip MCP tests
    if collection_path.name == "test_mcp_server.py":
        # Allow if specifically requested
        for arg in args:
            if "test_mcp_server.py" in str(arg):
                return False
        # Skip if running all tests
        return True

    return False

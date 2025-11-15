# TDD Guide for JAF Development

## Overview

This guide provides Test-Driven Development (TDD) principles and best practices specifically for JAF (Just Another Flow) development. JAF is a streaming JSON data processing system with lazy evaluation, making test design particularly important for ensuring correctness without sacrificing performance.

## Core TDD Principles for JAF

### 1. Write Tests That Define Behavior, Not Implementation

**Bad Test (Implementation-Focused):**
```python
def test_filter_uses_generator():
    """Tests internal implementation detail"""
    stream = LazyDataStream(source)
    filtered = stream.filter(query)
    # Testing that it uses a generator internally
    assert isinstance(filtered._generator, types.GeneratorType)
```

**Good Test (Behavior-Focused):**
```python
def test_filter_returns_matching_items():
    """Tests observable behavior"""
    data = [{"age": 30}, {"age": 25}, {"age": 35}]
    stream = LazyDataStream({"type": "memory", "data": data})

    result = list(stream.filter(["gt?", "@age", 28]).evaluate())

    assert len(result) == 2
    assert result[0]["age"] == 30
    assert result[1]["age"] == 35
```

### 2. Test Contracts, Not Construction

JAF uses various internal data structures (PathValues, MissingPath, LazyDataStream). Tests should verify the **contracts** these types fulfill, not their internal implementation.

**Contract Examples:**
- `PathValues`: Behaves like a list but signals multi-match path results
- `MissingPath`: Sentinel that is falsy and distinct from None
- `LazyDataStream`: Chainable operations that evaluate lazily

**Example Contract Test:**
```python
def test_pathvalues_distinguishes_from_regular_list():
    """PathValues type indicates multi-match path result"""
    regular_value = [1, 2, 3]  # A field that IS a list
    path_result = PathValues([1, 2, 3])  # Result from @items.* path

    # Same content, different semantics
    assert regular_value == path_result
    assert not isinstance(regular_value, PathValues)
    assert isinstance(path_result, PathValues)
```

### 3. Design Tests for Refactoring Resilience

A good test suite enables fearless refactoring. Tests should pass even after major internal changes, as long as behavior is preserved.

**Resilience Checklist:**
- [ ] Tests use only public APIs
- [ ] Tests don't depend on internal variable names
- [ ] Tests don't assert on implementation details (e.g., internal state)
- [ ] Tests use meaningful test data, not magic numbers
- [ ] Tests are independent and can run in any order

### 4. Follow the Red-Green-Refactor Cycle

**Red:**
```python
def test_stream_supports_windowed_distinct():
    """Write failing test first"""
    data = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}]
    stream = LazyDataStream({"type": "memory", "data": data})

    # This will fail until implemented
    result = list(stream.distinct(
        key=["@", [["key", "id"]]],
        window_size=2
    ).evaluate())

    # Expected behavior defined upfront
    assert len(result) <= 4  # May have duplicates with window
```

**Green:**
Implement minimal code to pass the test.

**Refactor:**
Improve code quality while keeping tests green.

## JAF-Specific Test Patterns

### Testing Streaming Operations

JAF operations are lazy - they don't execute until `.evaluate()` is called. Tests must account for this.

**Pattern:**
```python
def test_operation_name():
    """Given-When-Then structure"""
    # Given: Source data and initial state
    data = [{"x": 1}, {"x": 2}, {"x": 3}]
    stream = LazyDataStream({"type": "memory", "data": data})

    # When: Operation is applied
    result_stream = stream.filter(["gt?", "@x", 1])

    # Then: Evaluate and verify behavior
    results = list(result_stream.evaluate())
    assert len(results) == 2
    assert results[0]["x"] == 2
```

### Testing Multiple Source Types

JAF supports multiple data sources (memory, file, generator). Use parametrization to test across all source types.

**Pattern:**
```python
@pytest.mark.parametrize("source_type,source_data", [
    ("memory", [{"id": 1}, {"id": 2}]),
    ("file", "/tmp/test.jsonl"),
    ("generator", ({"id": i} for i in range(1, 3)))
])
def test_filter_works_with_all_sources(source_type, source_data):
    """Filter should work regardless of source type"""
    if source_type == "memory":
        source = {"type": "memory", "data": source_data}
    elif source_type == "file":
        # Setup file fixture
        setup_test_file(source_data)
        source = {"type": "file", "path": source_data}
    else:
        source = {"type": "generator", "generator": source_data}

    stream = LazyDataStream(source)
    result = list(stream.filter(["eq?", "@id", 1]).evaluate())

    assert len(result) == 1
    assert result[0]["id"] == 1
```

### Testing Path Operations

Path operations are central to JAF. Test edge cases thoroughly.

**Key Edge Cases:**
```python
def test_path_evaluation_edge_cases():
    """Path evaluation should handle all edge cases"""
    test_cases = [
        # (data, path, expected_result)
        ({}, [["key", "missing"]], MissingPath()),
        ({"x": []}, [["key", "x"]], []),  # Empty array exists
        ({"x": None}, [["key", "x"]], None),  # Null value exists
        ({"x": {"y": 0}}, [["key", "x"], ["key", "y"]], 0),  # Zero is valid
        ([], [["index", 0]], MissingPath()),  # Index on empty array
    ]

    for data, path, expected in test_cases:
        result = eval_path(path, data)
        if isinstance(expected, MissingPath):
            assert isinstance(result, MissingPath)
        else:
            assert result == expected
```

### Testing Error Conditions

Don't just test happy paths. Errors are part of the contract.

**Pattern:**
```python
def test_operation_raises_error_for_invalid_input():
    """Should raise appropriate error with clear message"""
    stream = LazyDataStream({"type": "memory", "data": []})

    with pytest.raises(ValueError, match="window_size must be positive"):
        stream.distinct(window_size=-1)
```

### Testing Windowed Operations

Windowed operations trade accuracy for bounded memory. Test both modes.

**Pattern:**
```python
def test_distinct_with_infinite_window():
    """Infinite window provides exact results"""
    data = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}]
    stream = LazyDataStream({"type": "memory", "data": data})

    result = list(stream.distinct(
        key=["@", [["key", "id"]]],
        window_size=float('inf')
    ).evaluate())

    assert len(result) == 3  # Exact deduplication

def test_distinct_with_finite_window():
    """Finite window may allow some duplicates"""
    data = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}, {"id": 1}]
    stream = LazyDataStream({"type": "memory", "data": data})

    result = list(stream.distinct(
        key=["@", [["key", "id"]]],
        window_size=2
    ).evaluate())

    # With window_size=2, may see id:1 multiple times
    assert len(result) >= 3
    assert len(result) <= 5
```

## Test Organization

### File Structure

```
tests/
├── test_path_types.py              # Unit tests for path types
├── test_path_operations.py          # Unit tests for path ops
├── test_jaf_eval.py                 # Unit tests for evaluation
├── test_lazy_streams.py             # Unit tests for stream classes
├── test_lazy_ops_loader.py          # Integration tests for operations
├── test_streaming_loader.py         # Integration tests for loaders
├── test_integration.py              # End-to-end tests
├── test_api.py                      # API endpoint tests
├── test_mcp_server.py               # MCP integration tests
└── test_dsl_compiler.py             # DSL compilation tests
```

### Test Class Organization

```python
class TestFeatureName:
    """Group related tests together"""

    def setup_method(self):
        """Setup before each test (use sparingly)"""
        pass

    def test_basic_behavior(self):
        """Test simplest case first"""
        pass

    def test_with_various_inputs(self):
        """Test with different valid inputs"""
        pass

    def test_edge_cases(self):
        """Test boundary conditions"""
        pass

    def test_error_handling(self):
        """Test invalid inputs and errors"""
        pass
```

## Test Naming Conventions

Good test names serve as documentation:

```python
# ❌ Bad: Vague, doesn't describe behavior
def test_filter()
def test_error()

# ✅ Good: Clear, describes behavior and conditions
def test_filter_returns_items_matching_query()
def test_filter_returns_empty_list_when_no_matches()
def test_filter_raises_error_when_query_is_invalid()
def test_distinct_with_window_size_uses_bounded_memory()
```

**Pattern:** `test_<operation>_<behavior>_when_<condition>`

## Test Data Best Practices

### Use Meaningful Test Data

```python
# ❌ Bad: Magic numbers, unclear intent
data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

# ✅ Good: Clear domain model
data = [
    {"name": "Alice", "age": 30, "department": "Engineering"},
    {"name": "Bob", "age": 25, "department": "Sales"}
]
```

### Use Builders for Complex Data

```python
class DataBuilder:
    """Builder for test data"""
    def __init__(self):
        self._data = []

    def with_user(self, name, age=30):
        self._data.append({"name": name, "age": age})
        return self

    def build(self):
        return self._data

# Usage
data = (DataBuilder()
    .with_user("Alice", 30)
    .with_user("Bob", 25)
    .build())
```

### Use Fixtures for Shared Setup

```python
@pytest.fixture
def sample_users():
    """Reusable test data"""
    return [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35}
    ]

def test_with_fixture(sample_users):
    stream = LazyDataStream({"type": "memory", "data": sample_users})
    result = list(stream.filter(["gt?", "@age", 28]).evaluate())
    assert len(result) == 2
```

## Assertion Best Practices

### Provide Clear Failure Messages

```python
# ❌ Bad: Unclear what went wrong
assert result == expected

# ✅ Good: Clear failure message
assert result == expected, \
    f"Expected {expected} but got {result} for input {input_data}"

# ✅ Better: Use pytest's detailed assertions
assert result == expected  # pytest shows both values automatically
```

### Assert One Logical Concept Per Test

```python
# ❌ Bad: Testing multiple unrelated things
def test_everything():
    result = operation()
    assert result is not None
    assert len(result) > 0
    assert result[0].has_property("x")
    assert other_unrelated_thing()

# ✅ Good: Focused tests
def test_operation_returns_non_empty_list():
    result = operation()
    assert len(result) > 0

def test_operation_returns_objects_with_property_x():
    result = operation()
    assert all(item.has_property("x") for item in result)
```

## Coverage Philosophy

### Focus Coverage on Critical Paths

**High Priority:**
- Complex business logic
- Error handling paths
- Edge cases and boundaries
- Public API contracts
- Path evaluation logic
- Operator implementations

**Low Priority:**
- Simple getters/setters
- Logging statements
- Framework boilerplate
- Obvious delegation

### Aim for Meaningful Coverage, Not 100%

```python
# Don't test framework code
def test_json_dumps_works():  # ❌ Don't test stdlib
    assert json.dumps({"a": 1}) == '{"a": 1}'

# Test your code's use of frameworks
def test_stream_serializes_to_json():  # ✅ Test your usage
    stream = LazyDataStream(...)
    result = stream.to_json()
    assert json.loads(result) == expected_structure
```

## Regression Tests

When bugs are found, write tests:

```python
def test_regression_issue_123_empty_arrays_vs_missing_paths():
    """
    Regression test for Issue #123.

    Previously, empty arrays were incorrectly treated as missing paths.
    This test ensures the fix remains in place.
    """
    data = {"items": []}

    # Empty array should be truthy with exists?
    assert jaf_eval.eval(["exists?", "@items"], data) is True

    # But should be empty with is-empty?
    assert jaf_eval.eval(["is-empty?", "@items"], data) is True

    # Missing path should fail exists?
    assert jaf_eval.eval(["exists?", "@missing"], data) is False
```

## Common Anti-Patterns to Avoid

### 1. Testing Private Methods

```python
# ❌ Bad: Testing internal implementation
def test_internal_cache_structure():
    stream = LazyDataStream(...)
    assert len(stream._internal_cache) == 0

# ✅ Good: Test observable behavior
def test_stream_evaluates_lazily():
    side_effects = []
    def generator():
        side_effects.append("evaluated")
        yield {"x": 1}

    stream = LazyDataStream({"type": "generator", "generator": generator()})
    # Stream created but not evaluated
    assert len(side_effects) == 0

    # Evaluation triggers generation
    list(stream.evaluate())
    assert len(side_effects) > 0
```

### 2. Excessive Mocking

```python
# ❌ Bad: Mocking everything
def test_with_too_many_mocks():
    mock_loader = Mock()
    mock_stream = Mock()
    mock_evaluator = Mock()
    # ... too many mocks, not testing real behavior

# ✅ Good: Mock at architectural boundaries only
def test_with_minimal_mocking():
    # Real objects for unit under test
    stream = LazyDataStream({"type": "memory", "data": [{"x": 1}]})

    # Only mock external dependencies
    with patch('jaf.external_api.fetch') as mock_fetch:
        mock_fetch.return_value = {"y": 2}
        result = stream.enrich_with_api()
```

### 3. Test Interdependence

```python
# ❌ Bad: Tests depend on each other
class TestBadSuite:
    data = None

    def test_step1(self):
        self.data = setup_data()

    def test_step2(self):
        # Depends on test_step1 running first
        result = process(self.data)

# ✅ Good: Independent tests
class TestGoodSuite:
    def test_step1(self):
        data = setup_data()
        assert data is not None

    def test_step2(self):
        data = setup_data()  # Own setup
        result = process(data)
```

### 4. Brittle Assertions

```python
# ❌ Bad: Exact string matching
def test_error_message():
    with pytest.raises(ValueError, match="Error: Invalid input in function foo at line 42"):
        # Breaks if error message format changes
        operation()

# ✅ Good: Match key parts only
def test_error_message():
    with pytest.raises(ValueError, match="Invalid input"):
        operation()
```

## Example: Complete TDD Workflow

Let's add a new feature: `stream.sample(n)` that returns `n` random items.

### Step 1: Write Failing Test (Red)

```python
def test_sample_returns_n_random_items():
    """Sample should return exactly n items from stream"""
    data = [{"id": i} for i in range(100)]
    stream = LazyDataStream({"type": "memory", "data": data})

    result = list(stream.sample(10).evaluate())

    assert len(result) == 10
    assert all("id" in item for item in result)
    # Items should be from original data
    ids = {item["id"] for item in result}
    assert ids.issubset(set(range(100)))
```

Run: Test fails (method doesn't exist)

### Step 2: Implement Minimal Code (Green)

```python
class LazyDataStream:
    def sample(self, n):
        """Return n random items from stream"""
        import random
        all_items = list(self.evaluate())
        sampled = random.sample(all_items, min(n, len(all_items)))
        return LazyDataStream({"type": "memory", "data": sampled})
```

Run: Test passes

### Step 3: Refactor

Add edge case tests:

```python
def test_sample_handles_n_greater_than_stream_size():
    """Sample should return all items if n > size"""
    data = [{"id": i} for i in range(5)]
    stream = LazyDataStream({"type": "memory", "data": data})

    result = list(stream.sample(10).evaluate())

    assert len(result) == 5  # Not 10

def test_sample_with_empty_stream():
    """Sample should return empty list for empty stream"""
    stream = LazyDataStream({"type": "memory", "data": []})

    result = list(stream.sample(5).evaluate())

    assert len(result) == 0
```

Refactor implementation for better performance or clarity while keeping tests green.

## Quick Reference

### TDD Checklist

- [ ] Write test first (Red)
- [ ] Test defines desired behavior
- [ ] Test has clear Given-When-Then structure
- [ ] Test uses meaningful data
- [ ] Test asserts on behavior, not implementation
- [ ] Write minimal code to pass (Green)
- [ ] Refactor while keeping tests green
- [ ] Test names clearly describe behavior
- [ ] Tests are independent
- [ ] Tests cover edge cases
- [ ] Tests cover error conditions

### When to Write Tests

- ✅ Before implementing new features (TDD)
- ✅ After finding bugs (regression tests)
- ✅ When refactoring (safety net)
- ✅ For critical business logic
- ✅ For public APIs
- ❌ For trivial getters/setters
- ❌ For framework/library code
- ❌ For obviously correct delegation

## Further Reading

- JAF Architecture: `/docs/architecture.md`
- Path System: `/docs/paths.md`
- Operators: `/docs/operators.md`
- Streaming: `/docs/streaming.md`

## Contributing

When submitting PRs, ensure:
1. All new features have tests
2. All bug fixes have regression tests
3. Test coverage doesn't decrease
4. Tests follow patterns in this guide
5. CI passes all tests

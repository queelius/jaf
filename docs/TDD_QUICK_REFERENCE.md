# TDD Quick Reference Card

## The Golden Rules

1. **Test behavior, not implementation**
2. **Red → Green → Refactor**
3. **One logical assertion per test**
4. **Tests should enable refactoring**
5. **Clear test names describe behavior**

## Test Structure Template

```python
def test_<operation>_<expected_behavior>_when_<condition>():
    """<Clear description of what this tests>"""
    # Given: Setup and initial state
    data = [{"id": 1}, {"id": 2}]
    stream = LazyDataStream({"type": "memory", "data": data})

    # When: Perform the operation
    result = stream.filter(["eq?", "@id", 1])

    # Then: Verify expected behavior
    evaluated = list(result.evaluate())
    assert len(evaluated) == 1
    assert evaluated[0]["id"] == 1
```

## Common Patterns

### Testing Stream Operations

```python
def test_stream_operation():
    # Create test data
    data = [...]
    stream = LazyDataStream({"type": "memory", "data": data})

    # Apply operation (returns new stream)
    result_stream = stream.operation(...)

    # Evaluate to get actual results
    results = list(result_stream.evaluate())

    # Assert on results
    assert results == expected
```

### Testing Path Operations

```python
@pytest.mark.parametrize("data,path,expected", [
    ({}, [["key", "missing"]], MissingPath()),
    ({"x": []}, [["key", "x"]], []),
    ({"x": None}, [["key", "x"]], None),
])
def test_path_evaluation(data, path, expected):
    result = eval_path(path, data)
    if isinstance(expected, MissingPath):
        assert isinstance(result, MissingPath)
    else:
        assert result == expected
```

### Testing Error Conditions

```python
def test_raises_error_for_invalid_input():
    stream = LazyDataStream({"type": "memory", "data": []})

    with pytest.raises(ValueError, match="window_size must be positive"):
        stream.distinct(window_size=-1)
```

### Using Fixtures

```python
@pytest.fixture
def sample_data():
    return [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]

def test_with_fixture(sample_data):
    stream = LazyDataStream({"type": "memory", "data": sample_data})
    result = list(stream.filter(["gt?", "@age", 28]).evaluate())
    assert len(result) == 1
```

## What to Test

### ✓ Do Test

- Public API behavior
- Error conditions and messages
- Edge cases (empty, null, boundary)
- Integration between components
- Contract fulfillment
- Return values and side effects

### ✗ Don't Test

- Private methods
- Internal implementation details
- Framework/library code
- Obvious delegation
- Simple getters/setters

## Test Checklist

Before committing tests, verify:

- [ ] Test name clearly describes behavior
- [ ] Test follows Given-When-Then structure
- [ ] Test uses meaningful test data
- [ ] Test asserts on behavior, not implementation
- [ ] Test is independent (can run alone)
- [ ] Test covers an edge case
- [ ] Test covers error handling
- [ ] No access to private methods/attributes
- [ ] No excessive mocking
- [ ] Clear failure messages

## Anti-Patterns to Avoid

### ❌ Testing Implementation

```python
# Bad
assert stream._internal_cache == {}

# Good
assert stream.is_empty()
```

### ❌ Testing Private Methods

```python
# Bad
assert obj._internal_method() == 42

# Good
assert obj.public_method() == 42
```

### ❌ Excessive Mocking

```python
# Bad
mock_a = Mock()
mock_b = Mock()
mock_c = Mock()
# ... testing only mock interactions

# Good
real_stream = LazyDataStream(...)
with patch('external_api') as mock_api:
    # Only mock external dependency
```

### ❌ Unclear Test Names

```python
# Bad
def test_filter()
def test_error()

# Good
def test_filter_returns_matching_items()
def test_filter_raises_error_when_query_is_invalid()
```

### ❌ Testing Multiple Concepts

```python
# Bad
def test_everything():
    assert a()
    assert b()
    assert c()  # Unrelated

# Good
def test_a_returns_expected_value()
def test_b_returns_expected_value()
def test_c_returns_expected_value()
```

## Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test file
pytest tests/test_path_types.py

# Run specific test
pytest tests/test_path_types.py::TestMissingPath::test_missing_path_is_falsy

# Run tests matching pattern
pytest -k "test_path"

# Run with verbose output
pytest -v

# Stop on first failure
pytest -x
```

## Coverage Commands

```bash
# Generate coverage report
pytest --cov=jaf --cov-report=term tests/

# Generate HTML coverage report
pytest --cov=jaf --cov-report=html tests/
open htmlcov/index.html

# Check coverage for specific module
pytest --cov=jaf.path_types --cov-report=term tests/
```

## Useful Assertions

```python
# Basic assertions
assert value == expected
assert value != unexpected
assert value is expected_object
assert value is not None

# Collection assertions
assert len(collection) == 3
assert item in collection
assert all(predicate(x) for x in collection)
assert any(predicate(x) for x in collection)

# Type assertions
assert isinstance(obj, ExpectedType)
assert not isinstance(obj, UnexpectedType)

# Exception assertions
with pytest.raises(ValueError):
    operation()

with pytest.raises(ValueError, match="specific message"):
    operation()

# Approximate float comparison
assert value == pytest.approx(3.14159, rel=1e-5)
```

## Parametrization

```python
@pytest.mark.parametrize("input,expected", [
    (1, 2),
    (2, 4),
    (3, 6),
])
def test_doubles_input(input, expected):
    assert double(input) == expected

# Multiple parameters
@pytest.mark.parametrize("x,y,expected", [
    (1, 1, 2),
    (2, 3, 5),
    (5, 7, 12),
])
def test_addition(x, y, expected):
    assert x + y == expected
```

## Quick TDD Workflow

1. **Write failing test** (Red)
   ```python
   def test_new_feature():
       result = new_feature()  # Doesn't exist yet
       assert result == expected
   ```

2. **Run test** → It fails ✗
   ```bash
   pytest tests/test_new_feature.py
   ```

3. **Write minimal code** (Green)
   ```python
   def new_feature():
       return expected  # Simplest implementation
   ```

4. **Run test** → It passes ✓

5. **Refactor**
   - Improve code quality
   - Keep tests green
   - Add more tests for edge cases

6. **Repeat**

## Common Fixtures

```python
# Temporary directory
@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

# Sample data
@pytest.fixture
def sample_users():
    return [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25}
    ]

# Mock object
@pytest.fixture
def mock_api():
    with patch('module.api_call') as mock:
        mock.return_value = {"status": "ok"}
        yield mock
```

## Resources

- Full Guide: `/docs/TDD_GUIDE.md`
- Test Improvements: `/TEST_IMPROVEMENTS_SUMMARY.md`
- pytest Docs: https://docs.pytest.org/
- Coverage.py: https://coverage.readthedocs.io/

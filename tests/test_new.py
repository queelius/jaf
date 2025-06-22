import pytest
from jaf.utils import adapt_jaf_operator

class TestAdaptJafOperator:  # Renamed class to reflect the function being tested
    """Test the adapt_jaf_operator function utility"""
    
    def test_basic_function_wrapping(self):
        """Test basic function wrapping with correct arguments"""
        # Simple function that adds two numbers
        def add(x, y, obj):
            return x + y
        
        wrapped_add, n = adapt_jaf_operator(3, add)
        result = wrapped_add(5, 3, obj={})
        assert result == 8
    
    def test_boolean_aggregation_any(self):
        """Test boolean result aggregation using any()"""
        # Function that returns boolean
        def is_positive(x, obj):
            return x > 0
        
        wrapped_func, n = adapt_jaf_operator(2, is_positive)
        
        # True case
        result = wrapped_func(5, obj={})
        assert result is True
        
        # False case  
        result = wrapped_func(-3, obj={})
        assert result is False
    
    def test_single_result_unwrapping(self):
        """Test that single non-boolean results are unwrapped"""
        def get_length(x, obj):
            return len(x)
        
        wrapped_func, n = adapt_jaf_operator(2, get_length)
        result = wrapped_func([1, 2, 3], obj={})
        assert result == 3
        assert isinstance(result, int)
    
    def test_list_flattening_logic(self):
        """Test the list flattening logic for nested lists"""
        def return_nested_list(obj):
            return [[1, 2, 3]]
        
        wrapped_func, n = adapt_jaf_operator(1, return_nested_list)
        result = wrapped_func(obj={})
        assert result == [1, 2, 3]
    
    def test_type_error_handling(self):
        """Test graceful handling of type errors"""
        def divide(x, y, obj):
            return x / y
        
        wrapped_func, n = adapt_jaf_operator(3, divide)
        
        # This should catch TypeError and return False
        result = wrapped_func("string", 5, obj={})
        assert result is False
    
    def test_attribute_error_handling(self):
        """Test graceful handling of attribute errors"""
        def call_method(x, obj):
            return x.nonexistent_method()
        
        wrapped_func, n = adapt_jaf_operator(2, call_method)
        
        # This should catch AttributeError and return False
        result = wrapped_func("string", obj={})
        assert result is False
    
    def test_argument_count_validation(self):
        """Test argument count validation"""
        def simple_func(x, obj):
            return x
        
        wrapped_func, n = adapt_jaf_operator(2, simple_func)
        
        # Wrong number of arguments should raise ValueError
        with pytest.raises(ValueError, match="'simple_func' expects 1 arguments, got 3"):
            wrapped_func(1, 2, 3, obj={})
    
    def test_empty_list_handling(self):
        """Test handling of empty lists"""
        def return_empty_list(obj):
            return []
        
        wrapped_func, n = adapt_jaf_operator(1, return_empty_list)
        result = wrapped_func(obj={})
        assert result == []
    
    def test_none_value_handling(self):
        """Test handling of None values"""
        def return_none(obj):
            return None
        
        wrapped_func, n = adapt_jaf_operator(1, return_none)
        result = wrapped_func(obj={})
        assert result is None
    
    def test_multiple_boolean_results(self):
        """Test aggregation of multiple boolean results"""
        # This simulates what happens with wildcard matches
        def check_equality(x, y, obj):
            if isinstance(x, list) and isinstance(y, str):
                # Simulate checking if any item in list equals y
                return any(item == y for item in x)
            return x == y
        
        wrapped_func, n = adapt_jaf_operator(3, check_equality)
        
        # Test with matching case
        result = wrapped_func(["a", "b", "c"], "b", obj={})
        assert result is True
        
        # Test with non-matching case
        result = wrapped_func(["a", "b", "c"], "d", obj={})
        assert result is False
    
    def test_mixed_return_types(self):
        """Test handling when results are not all booleans"""
        def mixed_return(x, obj):
            if x > 5:
                return x * 2
            else:
                return True
        
        wrapped_func, n = adapt_jaf_operator(2, mixed_return)
        
        # Single non-boolean result
        result = wrapped_func(10, obj={})
        assert result == 20
        
        # Single boolean result  
        result = wrapped_func(3, obj={})
        assert result is True
    
    def test_predicate_function_pattern(self):
        """Test the typical predicate function pattern used in JAF"""
        def eq_predicate(x1, x2, obj):
            return x1 == x2 and type(x1) == type(x2)
        
        wrapped_func, n = adapt_jaf_operator(3, eq_predicate)
        
        # Matching values and types
        result = wrapped_func("hello", "hello", obj={})
        assert result is True
        
        # Different values
        result = wrapped_func("hello", "world", obj={})
        assert result is False
        
        # Same value, different types
        result = wrapped_func(42, "42", obj={})
        assert result is False
    
    def test_exception_propagation(self):
        """Test that non-type/attribute errors are propagated"""
        def raise_value_error(obj):
            raise ValueError("Custom error")
        
        wrapped_func, n = adapt_jaf_operator(1, raise_value_error)
        
        # ValueError should be propagated, not caught
        with pytest.raises(ValueError, match="Custom error"):
            wrapped_func(obj={})
    
    def test_function_with_obj_parameter(self):
        """Test that the obj parameter is passed correctly"""
        def access_obj(key, obj):
            return obj.get(key, "not found")
        
        wrapped_func, n = adapt_jaf_operator(2, access_obj)
        test_obj = {"name": "Alice", "age": 30}
        
        result = wrapped_func("name", obj=test_obj)
        assert result == "Alice"
        
        result = wrapped_func("missing", obj=test_obj)
        assert result == "not found"
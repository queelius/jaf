# jaf/tests/test_jaf.py

import unittest
import datetime
import re
from jaf.jaf import jaf, jafError
from jaf.jaf_eval import jaf_eval

class TestJAF(unittest.TestCase):
    def setUp(self):
        # Sample data for testing
        self.data = [
            {
                'id': 1,
                'name': 'DataScienceRepo',
                'language': 'Python',
                'stars': 150,
                'forks': 30,
                'description': 'A repository for data science projects.',
                'owner': {'name': 'alice', 'active': True}
            },
            {
                'id': 2,
                'name': 'WebAppRepo',
                'language': 'JavaScript',
                'stars': 90,
                'forks': 15,
                'description': 'A repository for web applications.',
                'owner': {'name': 'bob', 'active': False}
            },
            {
                'id': 3,
                'name': 'MultiLanguageRepo',
                'languages': {
                    'frontend': 'JavaScript',
                    'backend': 'Python',
                    'data': 'Python'
                },
                'stars': 200,
                'forks': 50,
                'description': 'A repository supporting multiple languages.',
                'owner': {'name': 'carol', 'active': True}
            },
            {
                'id': 4,
                'name': 'EmptyRepo',
                'language': None,
                'stars': 0,
                'forks': 0,
                'description': '',
                'owner': {'name': 'dave', 'active': False}
            }
        ]

    # -----------------------
    # Tests for get_path_value
    # -----------------------
    def test_get_path_value_simple_path(self):
        # Test simple path without wildcards
        obj = self.data[0]
        path = 'owner.name'
        expected = ['alice']
        result = jaf_eval.eval(['path', path], obj)
        self.assertEqual(result, expected)

    def test_get_path_value_single_wildcard(self):
        # Test path with single '*' wildcard
        obj = self.data[2]
        path = 'languages.*'
        expected = ['JavaScript', 'Python', 'Python']
        result = jaf_eval.eval(['path', path], obj)
        self.assertCountEqual(result, expected)

    def test_get_path_value_double_wildcard(self):
        # Test path with double '**' wildcard
        obj = self.data[0]
        path = '**.name'
        expected = ['alice']
        result = jaf_eval.eval(['path', path], obj)
        self.assertCountEqual(result, expected)

        obj = self.data[2]
        path = '**.name'
        expected = ['carol']
        result = jaf_eval.eval(['path', path], obj)
        self.assertCountEqual(result, expected)

    def test_get_path_value_nonexistent_path(self):
        # Test path that does not exist
        obj = self.data[0]
        path = 'nonexistent.path'
        expected = []
        result = jaf_eval.eval(['path', path], obj)
        self.assertEqual(result, expected)

    # -----------------------
    # Tests for path_exists
    # -----------------------
    def test_path_exists_true(self):
        # Test path exists
        obj = self.data[0]
        path = 'owner.name'
        result = jaf_eval.eval(['path-exists?', path], obj)
        self.assertTrue(result)

    def test_path_exists_false(self):
        # Test path does not exist
        obj = self.data[0]
        path = 'owner.age'
        result = jaf_eval.eval(['path-exists?', path], obj)
        self.assertFalse(result)

    def test_path_exists_with_wildcards(self):
        # Test path exists with wildcards
        obj = self.data[2]
        path = 'languages.*'
        result = jaf_eval.eval(['path-exists?', path], obj)
        self.assertTrue(result)

        path = 'languages.**.nonexistent'
        result = jaf_eval.eval(['path-exists?', path], obj)
        self.assertFalse(result)

    # -----------------------
    # Tests for predicates
    # -----------------------
    def test_predicate_eq_single_value_true(self):
        # Test 'eq?' predicate with single value
        query = ['eq?', ['path', 'language'], 'Python']
        obj = self.data[0]
        result = jaf_eval.eval(query, obj)
        self.assertTrue(result)

    def test_predicate_eq_single_value_false(self):
        # Test 'eq?' predicate with single value
        query = ['eq?', ['path', 'language'], 'Java']
        obj = self.data[0]
        result = jaf_eval.eval(query, obj)
        self.assertFalse(result)

    def test_predicate_eq_multiple_values_true(self):
        # Test 'eq?' predicate with multiple values, at least one matches
        query = ['eq?', ['path', 'languages.*'], 'Python']
        obj = self.data[2]
        result = jaf_eval.eval(query, obj)
        self.assertTrue(result)

    def test_predicate_eq_multiple_values_false(self):
        # Test 'eq?' predicate with multiple values, none match
        query = ['eq?', ['path', 'languages.*'], 'Ruby']
        obj = self.data[2]
        result = jaf_eval.eval(query, obj)
        self.assertFalse(result)

    def test_predicate_gt_single_value_true(self):
        # Test 'gt?' predicate with single value
        query = ['gt?', ['path', 'stars'], 100]
        obj = self.data[0]
        result = jaf_eval.eval(query, obj)
        self.assertTrue(result)

    def test_predicate_gt_single_value_false(self):
        # Test 'gt?' predicate with single value
        query = ['gt?', ['path', 'stars'], 200]
        obj = self.data[1]
        result = jaf_eval.eval(query, obj)
        self.assertFalse(result)

    def test_predicate_gt_multiple_values_true(self):
        # Test 'gt?' predicate with multiple values
        query = ['gt?', ['path', 'languages.*'], 'Python']  # Incorrect usage, but testing behavior
        obj = self.data[2]
        with self.assertRaises(TypeError):
            jaf_eval.eval(query, obj)

    # -----------------------
    # Tests for logical operators
    # -----------------------
    def test_logical_and_true(self):
        # Test 'and' operator where all conditions are true
        query = [
            'and',
            ['eq?', ['lower-case', ['path', 'language']], 'python'],
            ['gt?', ['path', 'stars'], 100]
        ]
        expected_ids = [1, 3]
        result = jaf(self.data, query)
        result_ids = [obj['id'] for obj in result]
        self.assertCountEqual(result_ids, expected_ids)

    def test_logical_and_false(self):
        # Test 'and' operator where one condition is false
        query = [
            'and',
            ['eq?', ['lower-case', ['path', 'language']], 'python'],
            ['gt?', ['path', 'stars'], 200]
        ]
        expected_ids = [1]  # Only repo 1 has language 'Python' and stars > 200 (none)
        result = jaf(self.data, query)
        self.assertEqual(len(result), 0)

    def test_logical_or_true(self):
        # Test 'or' operator where at least one condition is true
        query = [
            'or',
            ['eq?', ['path', 'language'], 'JavaScript'],
            ['gt?', ['path', 'stars'], 150]
        ]
        expected_ids = [1, 2, 3]
        result = jaf(self.data, query)
        result_ids = [obj['id'] for obj in result]
        self.assertCountEqual(result_ids, expected_ids)

    def test_logical_not_true(self):
        # Test 'not' operator
        query = [
            'not',
            ['eq?', ['path', 'language'], 'Python']
        ]
        expected_ids = [2, 4]
        result = jaf(self.data, query)
        result_ids = [obj['id'] for obj in result]
        self.assertCountEqual(result_ids, expected_ids)

    def test_logical_not_false(self):
        # Test 'not' operator
        query = [
            'not',
            ['eq?', ['path', 'language'], 'JavaScript']
        ]
        expected_ids = [1, 3, 4]
        result = jaf(self.data, query)
        result_ids = [obj['id'] for obj in result]
        self.assertCountEqual(result_ids, expected_ids)

    # -----------------------
    # Tests for transformation functions
    # -----------------------
    def test_lower_case_single_value(self):
        # Test 'lower-case' function with single value
        query = ['lower-case', ['path', 'language']]
        obj = self.data[0]
        expected = ['python']
        result = jaf_eval.eval(query, obj)
        self.assertEqual(result, expected)

    def test_upper_case_multiple_values(self):
        # Test 'upper-case' function with multiple values
        query = ['upper-case', ['path', 'languages.*']]
        obj = self.data[2]
        expected = ['JAVASCRIPT', 'PYTHON', 'PYTHON']
        result = jaf_eval.eval(query, obj)
        self.assertCountEqual(result, expected)

    def test_concat_strings(self):
        # Test 'concat' function
        query = ['concat', ['path', 'owner.name'], ['path', 'language']]
        obj = self.data[0]
        expected = 'alicePython'
        result = jaf_eval.eval(query, obj)
        self.assertEqual(result, expected)

        obj = self.data[2]
        expected = 'carol'
        result = jaf_eval.eval(['concat', ['path', 'owner.name']], obj)
        self.assertEqual(result, expected)

    # -----------------------
    # Tests for error handling
    # -----------------------
    def test_error_unknown_operator(self):
        # Test unknown operator
        query = ['unknown-op', ['path', 'language'], 'Python']
        with self.assertRaises(ValueError):
            jaf(self.data, query)

    def test_error_invalid_query_format(self):
        # Test invalid query format
        query = 'invalid_query_format'
        with self.assertRaises(jafError):
            jaf(self.data, query)

    def test_error_incorrect_number_of_args(self):
        # Test incorrect number of arguments for an operator
        query = ['eq?']  # Missing arguments
        with self.assertRaises(ValueError):
            jaf(self.data, query)

    def test_error_type_mismatch(self):
        # Test type mismatch, e.g., calling 'lower-case' on a list
        query = ['lower-case', ['path', 'languages.*']]
        obj = self.data[2]
        expected = ['javascript', 'python', 'python']
        result = jaf_eval.eval(query, obj)
        self.assertCountEqual(result, expected)

    # -----------------------
    # Additional Tests
    # -----------------------
    def test_date_functions(self):
        # Test datetime functions
        obj = {
            'event': {
                'start': '2023-01-01',
                'end': '2023-12-31 23:59:59'
            }
        }
        query_date = ['date', ['path', 'event.start']]
        expected_date = [datetime.datetime(2023, 1, 1)]
        result_date = jaf_eval.eval(query_date, obj)
        self.assertEqual(result_date, expected_date)

        query_datetime = ['datetime', ['path', 'event.end']]
        expected_datetime = [datetime.datetime(2023, 12, 31, 23, 59, 59)]
        result_datetime = jaf_eval.eval(query_datetime, obj)
        self.assertEqual(result_datetime, expected_datetime)

    def test_if_function_true(self):
        # Test 'if' function when condition is true
        query = ['if', True, 'yes', 'no', None]
        expected = 'yes'
        result = jaf_eval.eval(query, None)
        self.assertEqual(result, expected)

    def test_if_function_false(self):
        # Test 'if' function when condition is false
        query = ['if', False, 'yes', 'no', None]
        expected = 'no'
        result = jaf_eval.eval(query, None)

    def test_cond_function(self):
        # Test 'cond' function
        query = [
            'cond',
            ['eq?', ['path', 'language'], 'Python', 'is_python'],
            ['eq?', ['path', 'language'], 'JavaScript', 'is_js'],
            'unknown'
        ]
        obj = self.data[0]  # language is 'Python'
        expected = 'is_python'
        result = jaf_eval.eval(query, obj)
        self.assertEqual(result, expected)

        obj = self.data[1]  # language is 'JavaScript'
        expected = 'is_js'
        result = jaf_eval.eval(query, obj)
        self.assertEqual(result, expected)

        obj = self.data[3]  # language is None
        expected = 'unknown'
        result = jaf_eval.eval(query, obj)
        self.assertEqual(result, expected)

    def test_sum_function(self):
        # Test 'sum' function
        query = ['sum', 1, 2, 3]
        expected = 6
        result = jaf_eval.eval(query, None)
        self.assertEqual(result, expected)

        query = ['sum', ['path', 'stars'], 10]
        # For each object, sum stars + 10
        for obj in self.data:
            result = jaf_eval.eval(query, obj)
            expected = (obj['stars'] if isinstance(obj['stars'], (int, float)) else 0) + 10
            self.assertEqual(result, expected)

    def test_max_function(self):
        # Test 'max' function
        query = ['max', 1, 5, 3]
        expected = 5
        result = jaf_eval.eval(query, None)
        self.assertEqual(result, expected)

        query = ['max', ['path', 'stars']]
        # For each object, find max stars (single value)
        for obj in self.data:
            result = jaf_eval.eval(query, obj)
            expected = obj['stars']
            self.assertEqual(result, expected)

    def test_map_function(self):
        # Test 'map' function
        query = ['map', ['path', 'stars'], ['gt?', ['path', 'stars'], 100], None]
        obj = self.data[0]
        result = jaf_eval.eval(query, obj)
        expected = [True]  # 150 > 100
        self.assertEqual(result, expected)

        obj = self.data[1]
        result = jaf_eval.eval(query, obj)
        expected = [False]  # 90 > 100
        self.assertEqual(result, expected)

    def test_filter_function(self):
        # Test 'filter' function
        query = ['filter', ['path', 'languages.*'], ['eq?', ['path', 'languages.*'], 'Python'], None]
        obj = self.data[2]
        result = jaf_eval.eval(query, obj)
        expected = ['Python', 'Python']
        self.assertCountEqual(result, expected)

    # -----------------------
    # Test Handling of Empty Values
    # -----------------------
    def test_empty_values(self):
        # Test handling empty strings and None
        obj = self.data[3]  # EmptyRepo
        query = ['empty?', ['path', 'description']]
        result = jaf_eval.eval(query, obj)
        self.assertTrue(result)

        query = ['empty?', ['path', 'language']]
        result = jaf_eval.eval(query, obj)
        self.assertTrue(result)

        query = ['empty?', ['path', 'name']]
        result = jaf_eval.eval(query, obj)
        self.assertFalse(result)

    def test_multiple_wildcard_matches(self):
        # Test multiple wildcard matches
        query = [
            'and',
            ['eq?', ['lower-case', ['path', 'languages.*']], 'python'],
            ['gt?', ['path', 'stars'], 100]
        ]
        expected_ids = [3]  # Only MultiLanguageRepo satisfies both
        result = jaf(self.data, query)
        result_ids = [obj['id'] for obj in result]
        self.assertCountEqual(result_ids, expected_ids)

    def test_merge_function(self):
        # Test 'merge' function
        query = ['merge', ['path', 'languages.*'], None]
        obj = self.data[2]
        result = jaf_eval.eval(query, obj)
        expected = {'frontend': 'JavaScript', 'backend': 'Python', 'data': 'Python'}
        self.assertEqual(result, expected)

    # Add more tests as needed...

if __name__ == '__main__':
    unittest.main()

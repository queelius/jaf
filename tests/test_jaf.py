import pytest
from jaf.core import jaf, jafError

sample_repos = [
    {
        'id': 1,
        'name': 'DataScienceRepo',
        'language': 'Python',
        'stars': 150,
        'forks': 30,
        'description': 'A repository for data science projects.',
        'owner': {
            'name': 'alice',
            'active': True
        }
    },
    {
        'id': 2,
        'name': 'WebDevRepo',
        'language': 'JavaScript',
        'stars': 80,
        'forks': 45,
        'description': 'Repository for web development.',
        'owner': {
            'name': 'bob',
            'active': False
        }
    },
    {
        'id': 3,
        'name': 'MachineLearning',
        'language': 'Python',
        'stars': 200,
        'forks': 60,
        'description': 'Machine learning algorithms and models.',
        'owner': {
            'name': 'carol',
            'active': True
        }
    },
    {
        'id': 4,
        'name': 'DataVisualization',
        'language': 'R',
        'stars': 120,
        'forks': 20,
        'description': 'Tools for data visualization.',
        'owner': {
            'name': 'dave',
            'active': True
        }
    },
    {
        'id': 5,
        'name': 'EmptyRepo',
        'language': None,
        'stars': 0,
        'forks': 0,
        'description': '',
        'owner': {
            'name': 'eve',
            'active': False
        }
    },
    {
        'id': 6,
        'name': 'FullFeatureRepo',
        'language': ['Python', 'JavaScript'],
        'stars': 250,
        'forks': 80,
        'description': 'A repository with full feature set.',
        'owner': {
            'name': 'frank',
            'active': True
        }
    }
]

def test_ast_query():
    query_ast = ['language', 'eq', 'Python']
    filtered = jaf(sample_repos, query_ast)
    assert [repo['id'] for repo in filtered] == [1, 3]

def test_dsl_query():
    query_dsl = 'language eq "Python" AND stars gt 100'
    filtered = jaf(sample_repos, query_dsl, is_dsl=True)
    assert [repo['id'] for repo in filtered] == [1, 3]

def test_complex_dsl_query():
    query_dsl_complex = 'NOT language eq "R" AND (stars gt 100 OR forks gt 50)'
    filtered = jaf(sample_repos, query_dsl_complex, is_dsl=True)
    assert [repo['id'] for repo in filtered] == [1, 3, 6]

def test_in_operator():
    query_dsl_in = 'language in "Python"'
    filtered = jaf(sample_repos, query_dsl_in, is_dsl=True)
    assert [repo['id'] for repo in filtered] == [6]

def test_not_operator():
    query_dsl_not = 'NOT language eq "JavaScript" AND stars gt 100'
    filtered = jaf(sample_repos, query_dsl_not, is_dsl=True)
    assert [repo['id'] for repo in filtered] == [1, 3, 4, 6]

def test_contains_operator():
    query_dsl_contains = 'description contains "data"'
    filtered = jaf(sample_repos, query_dsl_contains, is_dsl=True)
    assert [repo['id'] for repo in filtered] == [1, 4]

def test_invalid_operator():
    query_invalid = 'language unknown "Python"'
    with pytest.raises(jafError):
        jaf(sample_repos, query_invalid, is_dsl=True)

def test_empty_query():
    with pytest.raises(jafError):
        jaf(sample_repos, query=None)

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
    }
]

def main():
    try:
        query_dsl = 'language eq "Python" AND stars gt 100'
        filtered = jaf(sample_repos, query_dsl, is_dsl=True)
        print("Filtered Repositories:", [repo['id'] for repo in filtered])
    except jafError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

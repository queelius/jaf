from jaf import jaf, jafError

repos = [
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
        'language': 'Python',
        'stars': 10,
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
        #query_dsl = 'language eq "Python" AND stars gt 100'
        query = [
            'and',
                ['eq?', 'language', 'Python'],
                ['gt?', 'stars', 100]
        ]
        filter_repos = jaf(repos, query)
        print("Filtered Repositories:", filter_repos)
    except jafError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

# examples/basic_usage.py

from siftarray.core import sift_array, FilterError

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
    # ... other repositories ...
]

def main():
    try:
        query_dsl = 'language eq "Python" AND stars gt 100'
        filtered = sift_array(sample_repos, query_dsl, is_dsl=True)
        print("Filtered Repositories:", [repo['id'] for repo in filtered])
    except FilterError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

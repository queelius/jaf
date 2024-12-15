from jaf import jaf, jafError
import json

repos = [
    {
        'owner': {
            'name': 'alice',
            'active': True
        }
    },
    {
        'owner': {
            'name': 'bob',
            'active': False
        }
    },
    {
        'language': 'Python',
        'owner': {
            'name': 'carol',
            'active': True
        }
    },
    {
        'owner': {
            'name': 'dave',
            'active': True
        }
    },
    {
        'language': 'Python',
        'owner': {
            'name': 'eve',
            'active': False
        }
    }
]

def main():
    try:
        #query_dsl = 'language eq "Python" AND stars gt 100'
        #query = [
        #    'and',
        #        ['eq?', 'language', 'Python'],
        #        ['gt?', 'stars', 100]
        #]
        query = ['path-exists?', 'language']
        filter_repos = jaf(repos, query)
        print("Filtered Repositories:")
        for repo in filter_repos:
            print(json.dumps(repo, indent=2))
    except jafError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

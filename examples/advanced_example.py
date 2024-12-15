# Example Usage
if __name__ == "__main__":
    query = [
        'and',
            [
                'or',
                    ['gt', ['date-diff', ['now'], 'owner.dob'], 18]
            ],
            ['eq', ['lower-case', 'owner.name'], 'alex'],
            ['lt', 'owner.age', 80],
        [
            'and',
                ['contains', 'asset.description', 'bitcoin'],
                ['gt', 'asset.amount', 1]
        ]
    ]

    obj = {
        'owner': {
            'name': 'Alex',
            'dob': 1985,  # Assuming 'dob' is the year of birth
            'age': 49,
            'city': 'no where'
        },
        'asset': {
            'description': 'bitcoin',
            'amount': 34
        }
    }

    j = jaf()

    # Mock 'now' function to return 2024 for consistent date-diff calculation
    def mock_now(args, obj):
        return 2024
    j.funcs['now'] = (mock_now, 0)

    try:
        result = eval(query, obj)
        print("Does the object satisfy the query?", result)
    except Exception as e:
        print("Error during evaluation:", e)

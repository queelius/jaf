"""
Integration tests for JAF filtering with complex real-world scenarios.
"""

import pytest
from jaf import jaf, JafQuerySet  # Added JafQuerySet


class TestRealWorldScenarios:
    """Test JAF with realistic data scenarios"""

    def setup_method(self):
        """Set up realistic test data"""
        self.users = [
            {
                "id": 1,
                "name": "Alice Johnson",
                "email": "alice.johnson@company.com",
                "age": 30,
                "department": "Engineering",
                "role": "Senior Developer",
                "active": True,
                "skills": ["Python", "JavaScript", "React"],
                "projects": [
                    {"name": "WebApp", "status": "completed", "priority": "high"},
                    {"name": "API", "status": "in-progress", "priority": "medium"},
                ],
                "profile": {
                    "settings": {"theme": "dark", "notifications": True},
                    "preferences": {"language": "en", "timezone": "UTC"},
                },
                "hire_date": "2020-03-15",
                "salary": 95000,
            },
            {
                "id": 2,
                "name": "Bob Smith",
                "email": "bob.smith@company.com",
                "age": 25,
                "department": "Marketing",
                "role": "Marketing Specialist",
                "active": True,
                "skills": ["SEO", "Content Writing", "Analytics"],
                "projects": [
                    {"name": "Campaign", "status": "completed", "priority": "low"},
                    {"name": "Analysis", "status": "pending", "priority": "high"},
                ],
                "profile": {
                    "settings": {"theme": "light", "notifications": False},
                    "preferences": {"language": "en", "timezone": "PST"},
                },
                "hire_date": "2022-06-01",
                "salary": 65000,
            },
            {
                "id": 3,
                "name": "Charlie Davis",
                "email": "charlie.davis@company.com",
                "age": 35,
                "department": "Engineering",
                "role": "Team Lead",
                "active": False,
                "skills": ["Python", "Go", "DevOps", "Leadership"],
                "projects": [
                    {
                        "name": "Infrastructure",
                        "status": "in-progress",
                        "priority": "high",
                    },
                    {"name": "Migration", "status": "completed", "priority": "medium"},
                ],
                "profile": {
                    "settings": {"theme": "dark", "notifications": True},
                    "preferences": {"language": "en", "timezone": "EST"},
                },
                "hire_date": "2018-01-10",
                "salary": 110000,
            },
            {
                "id": 4,
                "name": "Diana Wilson",
                "email": "diana.wilson@company.com",
                "age": 28,
                "department": "Design",
                "role": "UX Designer",
                "active": True,
                "skills": ["Figma", "Sketch", "User Research"],
                "projects": [
                    {"name": "Redesign", "status": "pending", "priority": "high"},
                    {"name": "Research", "status": "completed", "priority": "low"},
                ],
                "profile": {
                    "settings": {"theme": "light", "notifications": True},
                    "preferences": {"language": "es", "timezone": "PST"},
                },
                "hire_date": "2021-09-20",
                "salary": 80000,
            },
        ]
        self.collection_id = "users_v1"

    def test_simple_filtering(self):
        """Test simple field-based filtering"""
        # Find all active users
        result = jaf(
            self.users,
            ["eq?", ["@", [["key", "active"]]], True],
            collection_id=self.collection_id,
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (active: Alice, Bob, Diana)
        assert len(matching_objects) == 3
        assert result.collection_id == self.collection_id
        # Verify we got the expected users by checking their names
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Bob Smith", "Diana Wilson"}

        # Find Engineering department
        result = jaf(self.users, ["eq?", ["@", [["key", "department"]]], "Engineering"])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (Engineering: Alice, Charlie)
        assert len(matching_objects) == 2
        # Verify we got the expected users by checking their names
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis"}

        # Find users over 30
        result = jaf(self.users, ["gt?", ["@", [["key", "age"]]], 30])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (age > 30: Charlie Davis)
        assert len(matching_objects) == 1
        # Verify we got the expected user by checking their name
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Charlie Davis"}

    def test_string_operations(self):
        """Test string-based filtering operations"""
        # Find users with company email
        result = jaf(
            self.users, ["ends-with?", ["@", [["key", "email"]]], "company.com"]
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got all users (all have company emails)
        assert len(matching_objects) == 4
        # Verify we got all expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {
            "Alice Johnson",
            "Bob Smith",
            "Charlie Davis",
            "Diana Wilson",
        }

        # Find users with names starting with 'A'
        result = jaf(self.users, ["starts-with?", ["@", [["key", "name"]]], "A"])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (names starting with 'A': Alice Johnson)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson"}

        # Case-insensitive role search
        result = jaf(
            self.users, ["eq?", ["lower-case", ["@", [["key", "role"]]]], "team lead"]
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (role "Team Lead": Charlie Davis)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Charlie Davis"}

    def test_array_operations(self):
        """Test array-based filtering"""
        # Find users with Python skills
        result = jaf(self.users, ["in?", "Python", ["@", [["key", "skills"]]]])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (Python skills: Alice Johnson, Charlie Davis)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis"}

        # Find users with specific number of skills
        result = jaf(self.users, ["eq?", ["length", ["@", [["key", "skills"]]]], 3])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (3 skills: Alice Johnson, Bob Smith, Diana Wilson)
        assert len(matching_objects) == 3
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Bob Smith", "Diana Wilson"}

        # Find users with more than 3 skills
        result = jaf(self.users, ["gt?", ["length", ["@", [["key", "skills"]]]], 3])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (>3 skills: Charlie Davis has 4 skills)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Charlie Davis"}

    def test_nested_object_filtering(self):
        """Test filtering on nested object properties"""
        # Find users with dark theme
        result = jaf(
            self.users,
            [
                "eq?",
                ["@", [["key", "profile"], ["key", "settings"], ["key", "theme"]]],
                "dark",
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (dark theme: Alice Johnson, Charlie Davis)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis"}

        # Find users with notifications enabled
        result = jaf(
            self.users,
            [
                "eq?",
                [
                    "@",
                    [["key", "profile"], ["key", "settings"], ["key", "notifications"]],
                ],
                True,
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (notifications enabled: Alice Johnson, Charlie Davis, Diana Wilson)
        assert len(matching_objects) == 3
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis", "Diana Wilson"}

        # Find Spanish language users
        result = jaf(
            self.users,
            [
                "eq?",
                [
                    "@",
                    [["key", "profile"], ["key", "preferences"], ["key", "language"]],
                ],
                "es",
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (Spanish language: Diana Wilson)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Diana Wilson"}

    def test_wildcard_filtering(self):
        """Test wildcard-based filtering"""
        # Find users with any completed project
        result = jaf(
            self.users,
            [
                "eq?",
                ["@", [["key", "projects"], ["wc_level"], ["key", "status"]]],
                "completed",
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got all users (all have at least one completed project)
        assert len(matching_objects) == 4
        # Verify we got all expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {
            "Alice Johnson",
            "Bob Smith",
            "Charlie Davis",
            "Diana Wilson",
        }

        # Find users with any high priority project
        result = jaf(
            self.users,
            [
                "eq?",
                ["@", [["key", "projects"], ["wc_level"], ["key", "priority"]]],
                "high",
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got all users (all have at least one high priority project)
        assert len(matching_objects) == 4
        # Verify we got all expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {
            "Alice Johnson",
            "Bob Smith",
            "Charlie Davis",
            "Diana Wilson",
        }

        # Find users with any project named "API"
        result = jaf(
            self.users,
            ["eq?", ["@", [["key", "projects"], ["wc_level"], ["key", "name"]]], "API"],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (project named "API": Alice Johnson)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson"}

    def test_complex_logical_conditions(self):
        """Test complex logical combinations"""
        # Active Engineering users
        result = jaf(
            self.users,
            [
                "and",
                ["eq?", ["@", [["key", "active"]]], True],
                ["eq?", ["@", [["key", "department"]]], "Engineering"],
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (active Engineering: Alice Johnson)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson"}

        # Users in Engineering OR Design
        result = jaf(
            self.users,
            [
                "or",
                ["eq?", ["@", [["key", "department"]]], "Engineering"],
                ["eq?", ["@", [["key", "department"]]], "Design"],
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (Engineering OR Design: Alice Johnson, Charlie Davis, Diana Wilson)
        assert len(matching_objects) == 3
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis", "Diana Wilson"}

        # High earners (salary > 80k) with Python skills
        result = jaf(
            self.users,
            [
                "and",
                ["gt?", ["@", [["key", "salary"]]], 80000],
                ["in?", "Python", ["@", [["key", "skills"]]]],
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (high earners with Python: Alice Johnson, Charlie Davis)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis"}

    def test_conditional_logic(self):
        """Test conditional (if) logic"""
        # Check if user is senior (age > 30) or has leadership skills
        result = jaf(
            self.users,
            [
                "or",
                ["gt?", ["@", [["key", "age"]]], 30],
                ["in?", "Leadership", ["@", [["key", "skills"]]]],
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (age > 30 OR Leadership: Charlie Davis)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Charlie Davis"}

        # Complex conditional: if active, check department, else check salary
        result = jaf(
            self.users,
            [
                "if",
                ["eq?", ["@", [["key", "active"]]], True],
                ["eq?", ["@", [["key", "department"]]], "Engineering"],  # Alice
                ["gt?", ["@", [["key", "salary"]]], 100000],
            ],
        )  # Charlie
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (if active then Engineering, else salary > 100k: Alice Johnson, Charlie Davis)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis"}

    def test_existence_checks(self):
        """Test existence-based filtering"""
        # Users with profile settings
        result = jaf(
            self.users, ["exists?", ["@", [["key", "profile"], ["key", "settings"]]]]
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got all users (all have profile settings)
        assert len(matching_objects) == 4
        # Verify we got all expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {
            "Alice Johnson",
            "Bob Smith",
            "Charlie Davis",
            "Diana Wilson",
        }

        # Users with projects
        result = jaf(self.users, ["exists?", ["@", [["key", "projects"]]]])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got all users (all have projects)
        assert len(matching_objects) == 4
        # Verify we got all expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {
            "Alice Johnson",
            "Bob Smith",
            "Charlie Davis",
            "Diana Wilson",
        }

        # Check for non-existent field
        result = jaf(self.users, ["exists?", ["@", [["key", "bonus"]]]])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got no users (none have bonus field)
        assert len(matching_objects) == 0

    def test_negation_patterns(self):
        """Test negation patterns"""
        # Not active users
        result = jaf(self.users, ["not", ["eq?", ["@", [["key", "active"]]], True]])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (not active: Charlie Davis)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Charlie Davis"}

        # Users not in Engineering
        result = jaf(
            self.users, ["not", ["eq?", ["@", [["key", "department"]]], "Engineering"]]
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (not Engineering: Bob Smith, Diana Wilson)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Bob Smith", "Diana Wilson"}

        # Users without Python skills
        result = jaf(self.users, ["not", ["in?", "Python", ["@", [["key", "skills"]]]]])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (no Python skills: Bob Smith, Diana Wilson)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Bob Smith", "Diana Wilson"}

    def test_salary_and_compensation_queries(self):
        """Test salary-based filtering scenarios"""
        # High earners
        result = jaf(self.users, ["gte?", ["@", [["key", "salary"]]], 90000])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (salary >= 90k: Alice Johnson, Charlie Davis)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Charlie Davis"}

        # Mid-range earners
        result = jaf(
            self.users,
            [
                "and",
                ["gte?", ["@", [["key", "salary"]]], 70000],
                ["lt?", ["@", [["key", "salary"]]], 100000],
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (70k <= salary < 100k: Alice Johnson, Diana Wilson)
        assert len(matching_objects) == 2
        # Verify we got the expected users
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice Johnson", "Diana Wilson"}

        # Entry level salaries
        result = jaf(self.users, ["lt?", ["@", [["key", "salary"]]], 70000])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right users (salary < 70k: Bob Smith)
        assert len(matching_objects) == 1
        # Verify we got the expected user
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Bob Smith"}


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_empty_and_null_values(self):
        """Test handling of empty and null values"""
        data = [
            {"name": "Alice", "items": []},
            {"name": "Bob", "items": None},
            {"name": "Charlie", "items": [1, 2, 3]},
            {"name": "", "items": []},
        ]

        # Find objects with empty items
        result = jaf(data, ["eq?", ["length", ["@", [["key", "items"]]]], 0])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (empty items: Alice and empty name)
        assert len(matching_objects) == 2
        # Verify we got the expected objects
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", ""}

        # Find objects with non-empty names
        result = jaf(data, ["neq?", ["@", [["key", "name"]]], ""])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (non-empty names: Alice, Bob, Charlie)
        assert len(matching_objects) == 3
        # Verify we got the expected objects
        matching_names = {obj["name"] for obj in matching_objects}
        assert matching_names == {"Alice", "Bob", "Charlie"}

    def test_mixed_data_types(self):
        """Test filtering with mixed data types"""
        data = [
            {"value": 42},
            {"value": "42"},
            {"value": 42.0},
            {"value": True},
            {"value": [42]},
        ]

        # Find numeric values (int)
        result = jaf(data, ["eq?", ["type", ["@", [["key", "value"]]]], "int"])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (int type: value 42)
        assert len(matching_objects) == 1
        assert matching_objects[0]["value"] == 42

        # Find string values
        result = jaf(data, ["eq?", ["type", ["@", [["key", "value"]]]], "str"])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (str type: value "42")
        assert len(matching_objects) == 1
        assert matching_objects[0]["value"] == "42"

        # Find list values
        result = jaf(data, ["eq?", ["type", ["@", [["key", "value"]]]], "list"])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (list type: value [42])
        assert len(matching_objects) == 1
        assert matching_objects[0]["value"] == [42]

    def test_deeply_nested_structures(self):
        """Test very deeply nested data structures"""
        data = [
            {
                "level1": {
                    "level2": {"level3": {"level4": {"level5": {"target": "found"}}}}
                }
            },
            {"level1": {"level2": {"level3": {"different": "value"}}}},
        ]

        # Deep path access
        result = jaf(
            data,
            [
                "eq?",
                [
                    "@",
                    [
                        ["key", "level1"],
                        ["key", "level2"],
                        ["key", "level3"],
                        ["key", "level4"],
                        ["key", "level5"],
                        ["key", "target"],
                    ],
                ],
                "found",
            ],
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (deep nested target: found)
        assert len(matching_objects) == 1
        assert (
            matching_objects[0]["level1"]["level2"]["level3"]["level4"]["level5"][
                "target"
            ]
            == "found"
        )

        # Recursive wildcard search
        result = jaf(
            data, ["eq?", ["@", [["wc_recursive"], ["key", "target"]]], "found"]
        )
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right objects (recursive search for target: found)
        assert len(matching_objects) == 1
        assert (
            matching_objects[0]["level1"]["level2"]["level3"]["level4"]["level5"][
                "target"
            ]
            == "found"
        )

    def test_large_arrays(self):
        """Test performance with larger data sets"""
        # Create a larger dataset
        large_data = []
        for i in range(100):  # Reduced from 1000 for faster test execution
            large_data.append(
                {"id": i, "name": f"User{i}", "active": i % 2 == 0, "score": i * 10}
            )

        # Find active users (should be 50 users: 0, 2, 4, ..., 98)
        result = jaf(large_data, ["eq?", ["@", [["key", "active"]]], True])
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right number of active users (50 total)
        assert len(matching_objects) == 50
        # Verify all matching objects are active
        assert all(obj["active"] for obj in matching_objects)
        # Verify all have even IDs (0, 2, 4, ..., 98)
        matching_ids = {obj["id"] for obj in matching_objects}
        expected_ids = set(range(0, 100, 2))
        assert matching_ids == expected_ids

        # Find high scorers
        result = jaf(
            large_data, ["gt?", ["@", [["key", "score"]]], 900]
        )  # 91*10 to 99*10 -> 910 to 990
        assert isinstance(result, JafQuerySet)
        # Evaluate to get actual matching objects
        matching_objects = list(result.evaluate())
        # Check we got the right number of high scorers (9 total: 91-99)
        assert len(matching_objects) == 9
        # Verify all have scores > 900
        assert all(obj["score"] > 900 for obj in matching_objects)
        # Verify we got the expected IDs (91-99)
        matching_ids = {obj["id"] for obj in matching_objects}
        expected_ids = set(range(91, 100))
        assert matching_ids == expected_ids

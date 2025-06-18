"""
Integration tests for JAF filtering with complex real-world scenarios.
"""
import pytest
from jaf import jaf, JafResultSet # Added JafResultSet


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
                    {"name": "API", "status": "in-progress", "priority": "medium"}
                ],
                "profile": {
                    "settings": {"theme": "dark", "notifications": True},
                    "preferences": {"language": "en", "timezone": "UTC"}
                },
                "hire_date": "2020-03-15",
                "salary": 95000
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
                    {"name": "Analysis", "status": "pending", "priority": "high"}
                ],
                "profile": {
                    "settings": {"theme": "light", "notifications": False},
                    "preferences": {"language": "en", "timezone": "PST"}
                },
                "hire_date": "2022-06-01",
                "salary": 65000
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
                    {"name": "Infrastructure", "status": "in-progress", "priority": "high"},
                    {"name": "Migration", "status": "completed", "priority": "medium"}
                ],
                "profile": {
                    "settings": {"theme": "dark", "notifications": True},
                    "preferences": {"language": "en", "timezone": "EST"}
                },
                "hire_date": "2018-01-10",
                "salary": 110000
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
                    {"name": "Research", "status": "completed", "priority": "low"}
                ],
                "profile": {
                    "settings": {"theme": "light", "notifications": True},
                    "preferences": {"language": "es", "timezone": "PST"}
                },
                "hire_date": "2021-09-20",
                "salary": 80000
            }
        ]
        self.collection_id = "users_v1"
    
    def test_simple_filtering(self):
        """Test simple field-based filtering"""
        # Find all active users
        result = jaf(self.users, ["eq?", ["path", [["key", "active"]]], True], collection_id=self.collection_id)
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 3]
        assert result.collection_size == len(self.users)
        assert result.collection_id == self.collection_id
        
        # Find Engineering department
        result = jaf(self.users, ["eq?", ["path", [["key", "department"]]], "Engineering"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2]
        assert result.collection_size == len(self.users)
        
        # Find users over 30
        result = jaf(self.users, ["gt?", ["path", [["key", "age"]]], 30])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [2]
        assert result.collection_size == len(self.users)
    
    def test_string_operations(self):
        """Test string-based filtering operations"""
        # Find users with company email
        result = jaf(self.users, ["ends-with?", "company.com", ["path", [["key", "email"]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 2, 3]
        assert result.collection_size == len(self.users)
        
        # Find users with names starting with 'A'
        result = jaf(self.users, ["starts-with?", "A", ["path", [["key", "name"]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(self.users)
        
        # Case-insensitive role search
        result = jaf(self.users, ["eq?", ["lower-case", ["path", [["key", "role"]]]], "team lead"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [2]
        assert result.collection_size == len(self.users)
    
    def test_array_operations(self):
        """Test array-based filtering"""
        # Find users with Python skills
        result = jaf(self.users, ["in?", "Python", ["path", [["key", "skills"]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2]
        assert result.collection_size == len(self.users)
    
        # Find users with specific number of skills
        result = jaf(self.users, ["eq?", ["length", ["path", [["key", "skills"]]]], 3])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 3]
        assert result.collection_size == len(self.users)
        
        # Find users with more than 3 skills
        result = jaf(self.users, ["gt?", ["length", ["path", [["key", "skills"]]]], 3])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [2]
        assert result.collection_size == len(self.users)
    
    def test_nested_object_filtering(self):
        """Test filtering on nested object properties"""
        # Find users with dark theme
        result = jaf(self.users, ["eq?", ["path", [["key", "profile"], ["key", "settings"], ["key", "theme"]]], "dark"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2]
        assert result.collection_size == len(self.users)
        
        # Find users with notifications enabled
        result = jaf(self.users, ["eq?", ["path", [["key", "profile"], ["key", "settings"], ["key", "notifications"]]], True])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2, 3]
        assert result.collection_size == len(self.users)
        
        # Find Spanish language users
        result = jaf(self.users, ["eq?", ["path", [["key", "profile"], ["key", "preferences"], ["key", "language"]]], "es"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [3]
        assert result.collection_size == len(self.users)
    
    def test_wildcard_filtering(self):
        """Test wildcard-based filtering"""
        # Find users with any completed project
        result = jaf(self.users, ["eq?", ["path", [["key", "projects"], ["wc_level"], ["key", "status"]]], "completed"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 2, 3] # Alice, Bob, Charlie, Diana all have at least one completed
        assert result.collection_size == len(self.users)
        
        # Find users with any high priority project
        result = jaf(self.users, ["eq?", ["path", [["key", "projects"], ["wc_level"], ["key", "priority"]]], "high"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 2, 3] # Alice, Bob, Charlie, Diana all have at least one high priority
        assert result.collection_size == len(self.users)
        
        # Find users with any project named "API"
        result = jaf(self.users, ["eq?", ["path", [["key", "projects"], ["wc_level"], ["key", "name"]]], "API"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(self.users)
    
    def test_complex_logical_conditions(self):
        """Test complex logical combinations"""
        # Active Engineering users
        result = jaf(self.users, ["and",
                                 ["eq?", ["path", [["key", "active"]]], True],
                                 ["eq?", ["path", [["key", "department"]]], "Engineering"]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(self.users)
        
        # Users in Engineering OR Design
        result = jaf(self.users, ["or",
                                 ["eq?", ["path", [["key", "department"]]], "Engineering"],
                                 ["eq?", ["path", [["key", "department"]]], "Design"]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2, 3]
        assert result.collection_size == len(self.users)
        
        # High earners (salary > 80k) with Python skills
        result = jaf(self.users, ["and",
                                 ["gt?", ["path", [["key", "salary"]]], 80000],
                                 ["in?", "Python", ["path", [["key", "skills"]]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2]
        assert result.collection_size == len(self.users)
    
    def test_conditional_logic(self):
        """Test conditional (if) logic"""
        # Check if user is senior (age > 30) or has leadership skills
        result = jaf(self.users, ["or",
                                 ["gt?", ["path", [["key", "age"]]], 30],
                                 ["in?", "Leadership", ["path", [["key", "skills"]]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [2] # Charlie (age 35 and Leadership)
        assert result.collection_size == len(self.users)
        
        # Complex conditional: if active, check department, else check salary
        result = jaf(self.users, ["if",
                                 ["eq?", ["path", [["key", "active"]]], True],
                                 ["eq?", ["path", [["key", "department"]]], "Engineering"], # Alice
                                 ["gt?", ["path", [["key", "salary"]]], 100000]]) # Charlie
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2]
        assert result.collection_size == len(self.users)
    
    def test_existence_checks(self):
        """Test existence-based filtering"""
        # Users with profile settings
        result = jaf(self.users, ["exists?", ["path", [["key", "profile"], ["key", "settings"]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 2, 3]
        assert result.collection_size == len(self.users)
        
        # Users with projects
        result = jaf(self.users, ["exists?", ["path", [["key", "projects"]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 2, 3]
        assert result.collection_size == len(self.users)
        
        # Check for non-existent field
        result = jaf(self.users, ["exists?", ["path", [["key", "bonus"]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == []
        assert result.collection_size == len(self.users)
    
    def test_negation_patterns(self):
        """Test negation patterns"""
        # Not active users
        result = jaf(self.users, ["not", ["eq?", ["path", [["key", "active"]]], True]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [2]
        assert result.collection_size == len(self.users)
        
        # Users not in Engineering
        result = jaf(self.users, ["not", ["eq?", ["path", [["key", "department"]]], "Engineering"]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [1, 3]
        assert result.collection_size == len(self.users)
        
        # Users without Python skills
        result = jaf(self.users, ["not", ["in?", "Python", ["path", [["key", "skills"]]]]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [1, 3]
        assert result.collection_size == len(self.users)
    
    def test_salary_and_compensation_queries(self):
        """Test salary-based filtering scenarios"""
        # High earners
        result = jaf(self.users, ["gte?", ["path", [["key", "salary"]]], 90000])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 2]
        assert result.collection_size == len(self.users)
        
        # Mid-range earners
        result = jaf(self.users, ["and",
                                 ["gte?", ["path", [["key", "salary"]]], 70000],
                                 ["lt?", ["path", [["key", "salary"]]], 100000]])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 3]
        assert result.collection_size == len(self.users)
        
        # Entry level salaries
        result = jaf(self.users, ["lt?", ["path", [["key", "salary"]]], 70000])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [1]
        assert result.collection_size == len(self.users)


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_and_null_values(self):
        """Test handling of empty and null values"""
        data = [
            {"name": "Alice", "items": []},
            {"name": "Bob", "items": None},
            {"name": "Charlie", "items": [1, 2, 3]},
            {"name": "", "items": []}
        ]
        
        # Find objects with empty items
        result = jaf(data, ["eq?", ["length", ["path", [["key", "items"]]]], 0])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 3]
        assert result.collection_size == len(data)
        
        # Find objects with non-empty names
        result = jaf(data, ["neq?", ["path", [["key", "name"]]], ""])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0, 1, 2]
        assert result.collection_size == len(data)
    
    def test_mixed_data_types(self):
        """Test filtering with mixed data types"""
        data = [
            {"value": 42},
            {"value": "42"},
            {"value": 42.0},
            {"value": True},
            {"value": [42]}
        ]
        
        # Find numeric values (int)
        result = jaf(data, ["eq?", ["type", ["path", [["key", "value"]]]], "int"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(data)
        
        # Find string values
        result = jaf(data, ["eq?", ["type", ["path", [["key", "value"]]]], "str"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [1]
        assert result.collection_size == len(data)
        
        # Find list values
        result = jaf(data, ["eq?", ["type", ["path", [["key", "value"]]]], "list"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [4]
        assert result.collection_size == len(data)
    
    def test_deeply_nested_structures(self):
        """Test very deeply nested data structures"""
        data = [
            {
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "level5": {
                                    "target": "found"
                                }
                            }
                        }
                    }
                }
            },
            {
                "level1": {
                    "level2": {
                        "level3": {
                            "different": "value"
                        }
                    }
                }
            }
        ]
        
        # Deep path access
        result = jaf(data, ["eq?", ["path", [["key", "level1"], ["key", "level2"], ["key", "level3"], ["key", "level4"], ["key", "level5"], ["key", "target"]]], "found"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(data)
        
        # Recursive wildcard search
        result = jaf(data, ["eq?", ["path", [["wc_recursive"], ["key", "target"]]], "found"])
        assert isinstance(result, JafResultSet)
        assert sorted(list(result.indices)) == [0]
        assert result.collection_size == len(data)
    
    def test_large_arrays(self):
        """Test performance with larger data sets"""
        # Create a larger dataset
        large_data = []
        for i in range(100): # Reduced from 1000 for faster test execution
            large_data.append({
                "id": i,
                "name": f"User{i}",
                "active": i % 2 == 0,
                "score": i * 10
            })
        
        # Find active users (should be 50 users: 0, 2, 4, ..., 98)
        result = jaf(large_data, ["eq?", ["path", [["key", "active"]]], True])
        assert isinstance(result, JafResultSet)
        assert len(result.indices) == 50
        assert all(idx % 2 == 0 for idx in result.indices)
        assert result.collection_size == len(large_data)
        
        # Find high scorers
        result = jaf(large_data, ["gt?", ["path", [["key", "score"]]], 900]) # 91*10 to 99*10 -> 910 to 990
        assert isinstance(result, JafResultSet)
        assert len(result.indices) == 9 # Indices 91, 92, ..., 99
        assert result.collection_size == len(large_data)

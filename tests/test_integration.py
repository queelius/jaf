"""
Integration tests for JAF filtering with complex real-world scenarios.
"""
import pytest
from jaf import jaf


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
    
    def test_simple_filtering(self):
        """Test simple field-based filtering"""
        # Find all active users
        result = jaf(self.users, ["eq?", ["path", ["active"]], True])
        assert result == [0, 1, 3]
        
        # Find Engineering department
        result = jaf(self.users, ["eq?", ["path", ["department"]], "Engineering"])
        assert result == [0, 2]
        
        # Find users over 30
        result = jaf(self.users, ["gt?", ["path", ["age"]], 30])
        assert result == [2]
    
    def test_string_operations(self):
        """Test string-based filtering operations"""
        # Find users with company email
        result = jaf(self.users, ["ends-with?", "@company.com", ["path", ["email"]]])
        assert result == [0, 1, 2, 3]
        
        # Find users with names starting with 'A'
        result = jaf(self.users, ["starts-with?", "A", ["path", ["name"]]])
        assert result == [0]
        
        # Case-insensitive role search
        result = jaf(self.users, ["eq?", ["lower-case", ["path", ["role"]]], "team lead"])
        assert result == [2]
    
    def test_array_operations(self):
        """Test array-based filtering"""
        # Find users with Python skills
        result = jaf(self.users, ["in?", "Python", ["path", ["skills"]]])
        assert result == [0, 2]
    
        # Find users with specific number of skills
        result = jaf(self.users, ["eq?", ["length", ["path", ["skills"]]], 3])
        assert result == [0, 1, 3]
        
        # Find users with more than 3 skills
        result = jaf(self.users, ["gt?", ["length", ["path", ["skills"]]], 3])
        assert result == [2]
    
    def test_nested_object_filtering(self):
        """Test filtering on nested object properties"""
        # Find users with dark theme
        result = jaf(self.users, ["eq?", ["path", ["profile", "settings", "theme"]], "dark"])
        assert result == [0, 2]
        
        # Find users with notifications enabled
        result = jaf(self.users, ["eq?", ["path", ["profile", "settings", "notifications"]], True])
        assert result == [0, 2, 3]
        
        # Find Spanish language users
        result = jaf(self.users, ["eq?", ["path", ["profile", "preferences", "language"]], "es"])
        assert result == [3]
    
    def test_wildcard_filtering(self):
        """Test wildcard-based filtering"""
        # Find users with any completed project
        result = jaf(self.users, ["eq?", ["path", ["projects", "*", "status"]], "completed"])
        assert result == [0, 1, 2, 3]
        
        # Find users with any high priority project
        result = jaf(self.users, ["eq?", ["path", ["projects", "*", "priority"]], "high"])
        assert result == [0, 1, 2, 3]
        
        # Find users with any project named "API"
        result = jaf(self.users, ["eq?", ["path", ["projects", "*", "name"]], "API"])
        assert result == [0]
    
    def test_complex_logical_conditions(self):
        """Test complex logical combinations"""
        # Active Engineering users
        result = jaf(self.users, ["and",
                                 ["eq?", ["path", ["active"]], True],
                                 ["eq?", ["path", ["department"]], "Engineering"]])
        assert result == [0]
        
        # Users in Engineering OR Design
        result = jaf(self.users, ["or",
                                 ["eq?", ["path", ["department"]], "Engineering"],
                                 ["eq?", ["path", ["department"]], "Design"]])
        assert result == [0, 2, 3]
        
        # High earners (salary > 80k) with Python skills
        result = jaf(self.users, ["and",
                                 ["gt?", ["path", ["salary"]], 80000],
                                 ["in?", "Python", ["path", ["skills"]]]])
        assert result == [0, 2]
    
    def test_conditional_logic(self):
        """Test conditional (if) logic"""
        # Check if user is senior (age > 30) or has leadership skills
        result = jaf(self.users, ["or",
                                 ["gt?", ["path", ["age"]], 30],
                                 ["in?", "Leadership", ["path", ["skills"]]]])
        assert result == [2]
        
        # Complex conditional: if active, check department, else check salary
        result = jaf(self.users, ["if",
                                 ["eq?", ["path", ["active"]], True],
                                 ["eq?", ["path", ["department"]], "Engineering"],
                                 ["gt?", ["path", ["salary"]], 100000]])
        assert result == [0, 2]  # Alice (active+Engineering), Charlie (inactive+high salary)
    
    def test_existence_checks(self):
        """Test existence-based filtering"""
        # Users with profile settings
        result = jaf(self.users, ["exists?", ["path", ["profile", "settings"]]])
        assert result == [0, 1, 2, 3]
        
        # Users with projects
        result = jaf(self.users, ["exists?", ["path", ["projects"]]])
        assert result == [0, 1, 2, 3]
        
        # Check for non-existent field
        result = jaf(self.users, ["exists?", ["path", ["bonus"]]])
        assert result == []
    
    def test_negation_patterns(self):
        """Test negation patterns"""
        # Not active users
        result = jaf(self.users, ["not", ["eq?", ["path", ["active"]], True]])
        assert result == [2]
        
        # Users not in Engineering
        result = jaf(self.users, ["not", ["eq?", ["path", ["department"]], "Engineering"]])
        assert result == [1, 3]
        
        # Users without Python skills
        result = jaf(self.users, ["not", ["in?", "Python", ["path", ["skills"]]]])
        assert result == [1, 3]
    
    def test_salary_and_compensation_queries(self):
        """Test salary-based filtering scenarios"""
        # High earners
        result = jaf(self.users, ["gte?", ["path", ["salary"]], 90000])
        assert result == [0, 2]
        
        # Mid-range earners
        result = jaf(self.users, ["and",
                                 ["gte?", ["path", ["salary"]], 70000],
                                 ["lt?", ["path", ["salary"]], 100000]])
        assert result == [0, 3]
        
        # Entry level salaries
        result = jaf(self.users, ["lt?", ["path", ["salary"]], 70000])
        assert result == [1]


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
        result = jaf(data, ["eq?", ["length", ["path", ["items"]]], 0])
        assert result == [0, 3]  # Alice and empty name user
        
        # Find objects with non-empty names
        result = jaf(data, ["neq?", ["path", ["name"]], ""])
        assert result == [0, 1, 2]
    
    def test_mixed_data_types(self):
        """Test filtering with mixed data types"""
        data = [
            {"value": 42},
            {"value": "42"},
            {"value": 42.0},
            {"value": True},
            {"value": [42]}
        ]
        
        # Find numeric values
        result = jaf(data, ["eq?", ["type", ["path", ["value"]]], "int"])
        assert result == [0]
        
        # Find string values
        result = jaf(data, ["eq?", ["type", ["path", ["value"]]], "str"])
        assert result == [1]
        
        # Find list values
        result = jaf(data, ["eq?", ["type", ["path", ["value"]]], "list"])
        assert result == [4]
    
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
        result = jaf(data, ["eq?", ["path", ["level1", "level2", "level3", "level4", "level5", "target"]], "found"])
        assert result == [0]
        
        # Recursive wildcard search
        result = jaf(data, ["eq?", ["path", ["**", "target"]], "found"])
        assert result == [0]
    
    def test_large_arrays(self):
        """Test performance with larger data sets"""
        # Create a larger dataset
        large_data = []
        for i in range(100):
            large_data.append({
                "id": i,
                "name": f"User{i}",
                "active": i % 2 == 0,
                "score": i * 10
            })
        
        # Find active users (should be 50 users: 0, 2, 4, ..., 98)
        result = jaf(large_data, ["eq?", ["path", ["active"]], True])
        assert len(result) == 50
        assert all(i % 2 == 0 for i in result)
        
        # Find high scorers
        result = jaf(large_data, ["gt?", ["path", ["score"]], 900])
        assert len(result) == 9  # Users with scores 910, 920, ..., 990

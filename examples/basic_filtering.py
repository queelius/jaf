#!/usr/bin/env python3
"""
Basic JAF Filtering Examples

Demonstrates fundamental JAF operations: filtering, mapping, and chaining.
"""

from jaf import stream

# Sample data
users = [
    {"name": "Alice", "age": 30, "department": "Engineering", "salary": 95000},
    {"name": "Bob", "age": 25, "department": "Marketing", "salary": 55000},
    {"name": "Charlie", "age": 35, "department": "Engineering", "salary": 120000},
    {"name": "Diana", "age": 28, "department": "Design", "salary": 75000},
    {"name": "Eve", "age": 42, "department": "Engineering", "salary": 150000},
]


def basic_filter():
    """Filter users by age."""
    print("=== Users over 30 ===")
    results = stream({"type": "memory", "data": users}) \
        .filter(["gt?", "@age", 30]) \
        .evaluate()

    for user in results:
        print(f"  {user['name']}: {user['age']} years old")


def filter_with_and():
    """Filter with multiple conditions."""
    print("\n=== Engineers earning over $100k ===")
    results = stream({"type": "memory", "data": users}) \
        .filter(["and",
            ["eq?", "@department", "Engineering"],
            ["gt?", "@salary", 100000]
        ]) \
        .evaluate()

    for user in results:
        print(f"  {user['name']}: ${user['salary']:,}")


def map_transform():
    """Transform data with map."""
    print("\n=== User summaries ===")
    results = stream({"type": "memory", "data": users}) \
        .map(["dict",
            "name", "@name",
            "department", "@department",
            "salary_k", ["/", "@salary", 1000]
        ]) \
        .evaluate()

    for item in results:
        print(f"  {item['name']} ({item['department']}): ${item['salary_k']}k")


def chain_operations():
    """Chain multiple operations."""
    print("\n=== Top 3 engineering salaries ===")
    results = stream({"type": "memory", "data": users}) \
        .filter(["eq?", "@department", "Engineering"]) \
        .map(["dict", "name", "@name", "salary", "@salary"]) \
        .take(3) \
        .evaluate()

    for user in results:
        print(f"  {user['name']}: ${user['salary']:,}")


def using_infix_syntax():
    """Using the infix DSL syntax via dsl_compiler."""
    from jaf.dsl_compiler import smart_compile

    print("\n=== Using infix syntax: salary > 60000 ===")
    # Compile the infix DSL to JSON AST
    query = smart_compile("@salary > 60000")
    results = stream({"type": "memory", "data": users}) \
        .filter(query) \
        .evaluate()

    for user in results:
        print(f"  {user['name']}: ${user['salary']:,}")


if __name__ == "__main__":
    basic_filter()
    filter_with_and()
    map_transform()
    chain_operations()
    using_infix_syntax()

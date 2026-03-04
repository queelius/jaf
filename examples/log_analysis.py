#!/usr/bin/env python3
"""
Log Analysis Example

Demonstrates using JAF for analyzing structured log data.
"""

import json
import random
import tempfile
from datetime import datetime, timedelta
from jaf import stream


def generate_sample_logs(filepath, n=1000):
    """Generate sample log entries."""
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    services = ["api", "auth", "database", "cache", "worker"]
    endpoints = ["/users", "/orders", "/products", "/health", "/admin"]
    status_codes = [200, 201, 400, 401, 403, 404, 500, 502, 503]

    base_time = datetime.now() - timedelta(hours=24)

    with open(filepath, "w") as f:
        for i in range(n):
            # Weighted random level (most are INFO)
            level = random.choices(
                levels,
                weights=[5, 60, 20, 10, 5]
            )[0]

            entry = {
                "timestamp": (base_time + timedelta(seconds=i * 60)).isoformat(),
                "level": level,
                "service": random.choice(services),
                "endpoint": random.choice(endpoints),
                "status_code": random.choice(status_codes),
                "response_time_ms": random.randint(10, 2000),
                "user_id": f"user_{random.randint(1, 100)}",
                "request_id": f"req_{i:06d}"
            }
            f.write(json.dumps(entry) + "\n")

    return filepath


def analyze_errors(logfile):
    """Find all error and critical log entries."""
    print("=== Error Analysis ===\n")

    errors = list(
        stream(logfile)
        .filter(["or", ["eq?", "@level", "ERROR"], ["eq?", "@level", "CRITICAL"]])
        .take(10)
        .evaluate()
    )

    print(f"Found {len(errors)} errors (showing first 10):")
    for entry in errors:
        print(f"  [{entry['level']}] {entry['service']}: {entry['endpoint']} "
              f"(status {entry['status_code']})")


def slow_requests(logfile, threshold_ms=1000):
    """Find slow API requests."""
    print(f"\n=== Slow Requests (>{threshold_ms}ms) ===\n")

    slow = list(
        stream(logfile)
        .filter(["gt?", "@response_time_ms", threshold_ms])
        .map(["dict",
            "endpoint", "@endpoint",
            "time_ms", "@response_time_ms",
            "service", "@service"
        ])
        .take(5)
        .evaluate()
    )

    print(f"Found {len(slow)} slow requests (showing first 5):")
    for req in slow:
        print(f"  {req['endpoint']}: {req['time_ms']}ms ({req['service']})")


def service_error_summary(logfile):
    """Group errors by service."""
    print("\n=== Errors by Service ===\n")

    # Filter errors first, then group
    error_entries = list(
        stream(logfile)
        .filter(["or", ["eq?", "@level", "ERROR"], ["eq?", "@level", "CRITICAL"]])
        .evaluate()
    )

    if not error_entries:
        print("No errors found!")
        return

    # Group by service
    summary = list(
        stream({"type": "memory", "data": error_entries})
        .groupby(
            key=["@", [["key", "service"]]],
            aggregate={
                "error_count": ["count"],
                "latest_error": ["max", "@timestamp"]
            }
        )
        .evaluate()
    )

    for group in summary:
        print(f"  {group['key']}: {group['error_count']} errors "
              f"(latest: {group['latest_error']})")


def status_code_distribution(logfile):
    """Analyze HTTP status code distribution."""
    print("\n=== Status Code Distribution ===\n")

    summary = list(
        stream(logfile)
        .groupby(
            key=["@", [["key", "status_code"]]],
            aggregate={"count": ["count"]}
        )
        .evaluate()
    )

    # Sort by status code
    summary.sort(key=lambda x: x["key"])

    total = sum(g["count"] for g in summary)
    for group in summary:
        pct = (group["count"] / total) * 100
        bar = "#" * int(pct / 2)
        print(f"  {group['key']}: {group['count']:4d} ({pct:5.1f}%) {bar}")


def unique_users(logfile):
    """Count unique users efficiently."""
    print("\n=== Unique Users ===\n")

    users = list(
        stream(logfile)
        .distinct(key=["@", [["key", "user_id"]]])
        .map("@user_id")
        .evaluate()
    )

    print(f"Unique users: {len(users)}")
    print(f"Sample: {users[:5]}")


if __name__ == "__main__":
    # Create temporary log file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        logfile = f.name

    print(f"Generating sample logs to {logfile}...\n")
    generate_sample_logs(logfile, n=1000)

    # Run analyses
    analyze_errors(logfile)
    slow_requests(logfile)
    service_error_summary(logfile)
    status_code_distribution(logfile)
    unique_users(logfile)

    print(f"\nLog file: {logfile}")

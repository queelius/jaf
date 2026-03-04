#!/bin/bash
# JAF CLI Examples
#
# This script demonstrates various JAF command-line patterns.
# Run these commands directly or use as reference.

# Create sample data
cat > /tmp/users.jsonl << 'EOF'
{"id": 1, "name": "Alice", "age": 30, "dept": "Engineering"}
{"id": 2, "name": "Bob", "age": 25, "dept": "Marketing"}
{"id": 3, "name": "Charlie", "age": 35, "dept": "Engineering"}
{"id": 4, "name": "Diana", "age": 28, "dept": "Design"}
{"id": 5, "name": "Eve", "age": 42, "dept": "Engineering"}
EOF

echo "=== Sample Data ==="
cat /tmp/users.jsonl
echo

# Basic filtering
echo "=== Filter: age > 30 ==="
jaf filter /tmp/users.jsonl '["gt?", "@age", 30]' --eval
echo

# Using S-expression syntax
echo "=== Filter: S-expression syntax ==="
jaf filter /tmp/users.jsonl '(gt? @age 30)' --eval
echo

# Using infix DSL syntax
echo "=== Filter: Infix DSL syntax ==="
jaf filter /tmp/users.jsonl '@age > 30' --eval
echo

# Multiple conditions with AND
echo "=== Filter: Engineers over 30 ==="
jaf filter /tmp/users.jsonl '(and (eq? @dept "Engineering") (gt? @age 30))' --eval
echo

# Map/transform data
echo "=== Map: Extract names ==="
jaf map /tmp/users.jsonl '@name' --eval
echo

# Map to new structure
echo "=== Map: Create summaries ==="
jaf map /tmp/users.jsonl '["dict", "person", "@name", "age", "@age"]' --eval
echo

# Stream command with chaining
echo "=== Stream: Filter + Map + Take ==="
jaf stream /tmp/users.jsonl \
    --filter '["eq?", "@dept", "Engineering"]' \
    --map '["dict", "name", "@name", "age", "@age"]' \
    --take 2
echo

# Distinct with exact strategy
echo "=== Distinct: By department (exact) ==="
jaf distinct /tmp/users.jsonl --key '@dept' --eval
echo

# Distinct with probabilistic strategy
echo "=== Distinct: By department (probabilistic) ==="
jaf distinct /tmp/users.jsonl \
    --key '@dept' \
    --strategy probabilistic \
    --bloom-expected-items 100 \
    --eval
echo

# Piping commands together
echo "=== Piping: Filter then take ==="
jaf filter /tmp/users.jsonl '["gt?", "@age", 25]' | jaf take - 2 --eval
echo

# Working with stdin
echo "=== Stdin: Process inline JSON ==="
echo '[{"x": 1}, {"x": 2}, {"x": 3}]' | jaf filter - '["gt?", "@x", 1]' --eval
echo

# Get info about a stream
echo "=== Info: Stream descriptor ==="
jaf filter /tmp/users.jsonl '["gt?", "@age", 30]' | jaf info -
echo

# Lazy evaluation (output stream descriptor)
echo "=== Lazy: Output stream descriptor ==="
jaf filter /tmp/users.jsonl '["gt?", "@age", 30]'
echo

# Evaluate a lazy stream
echo "=== Eval: Evaluate stream descriptor ==="
jaf filter /tmp/users.jsonl '["gt?", "@age", 30]' | jaf eval -
echo

# Cleanup
rm /tmp/users.jsonl

echo "=== Examples complete! ==="

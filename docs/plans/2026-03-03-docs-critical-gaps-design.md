# Design: Fix Critical Documentation Gaps

**Date:** 2026-03-03
**Scope:** Fix the 3 worst documentation files — api-guide.md, cli-reference.md, cookbook.md
**Audience:** End users learning JAF, pedagogical tone

## Problem

The documentation was written before v0.7/v0.8 added major features (set operations, windowed operations, probabilistic strategies). Three files have critical gaps:

1. **api-guide.md** — Missing 6 stream methods: `distinct()`, `groupby()`, `join()`, `intersect()`, `except_from()`, and no mention of windowed operations or probabilistic strategies
2. **cli-reference.md** — Missing the `distinct` command entirely, and the `stream` command docs omit `--distinct`, `--distinct-key`, `--strategy`, `--window-size`, `--bloom-*` flags
3. **cookbook.md** — Uses phantom operators (`map`, `any`, `hash`, `concat`) that don't exist in `jaf_eval.py`

## Design

### 1. api-guide.md — Add Set & Windowed Operations

Insert a new section **"Set and Aggregation Operations"** after the current "Batching and Enumeration" section (~line 204). Contains:

#### a. `distinct()` (3 strategies)
- Exact (default), windowed, probabilistic
- `key` parameter for field-based dedup
- Example progression: simple → keyed → windowed → probabilistic

#### b. `groupby()` with aggregate
- Key expression + optional aggregate dict
- Aggregate operators: `sum`, `mean`, `count`
- `window_size` for tumbling windows
- Example: group logs by level, aggregate counts

#### c. `join()` (4 types)
- `inner`, `left`, `right`, `outer`
- `on` / `on_right` for asymmetric keys
- `window_size` for bounded joins
- Example: join users with orders

#### d. `intersect()` / `except_from()`
- Two-stream operations with optional key
- Probabilistic strategy for large streams
- Example: find common users between two files

#### e. Windowed Operations concept section
- Brief explanation of the memory/accuracy trade-off
- Table: `float('inf')` (exact) vs finite window vs probabilistic
- When to use each strategy

#### f. `take_while()` / `skip_while()` (currently undocumented)
- Already have code examples in the file but worth ensuring complete

### 2. cli-reference.md — Add `distinct` command + `stream` flags

#### a. New `distinct` section (after `batch`)
Document from actual argparse code:
- `jaf distinct <input> [--key EXPR] [--strategy {exact,windowed,probabilistic}] [--window-size N] [--bloom-expected-items N] [--bloom-fp-rate RATE] [--eval]`
- Examples: basic, keyed, probabilistic

#### b. Update `stream` command section
Add the undocumented flags:
- `--distinct`, `--distinct-key`
- `--strategy {exact,windowed,probabilistic}`
- `--window-size N`
- `--bloom-expected-items N`, `--bloom-fp-rate RATE`
- Examples showing stream with distinct and probabilistic strategy

### 3. cookbook.md — Fix phantom operators

The following cookbook examples use operators that don't exist in `jaf_eval.py`:

| Line(s) | Phantom Operator | Fix |
|---------|-----------------|-----|
| 215 | `["join", ["map", "@items", "@name"], ", "]` | Replace with `["join", "@items.*.name", ", "]` if wildcard works, or rewrite as a note about limitation |
| 225-226 | `["any", ["map", "@items", ...]]` | Rewrite using `["contains?", ...]` or note this requires pre-processing |
| 234 | `["sum", ["map", "@items", "@price"]]` | Use groupby/aggregate or manual Python loop |
| 239 | `["unique", ["map", "@items", "@category"]]` | Use `["unique", "@items.*.category"]` with wildcard |
| 293 | `["hash", "@id"]` | Remove — no hash operator exists |
| 208-217 | `["concat", ...]` | Rewrite using `["join", [...], " "]` if it exists, or restructure |

**Strategy:** For each phantom operator:
1. Check if a wildcard path (`@items.*.name`) achieves the same result
2. If yes, replace with the wildcard approach
3. If no, rewrite the example to use Python for that step, keeping JAF for what it's good at
4. Add a note where nested array operations are a current limitation

### 4. Minor fixes across all three files

- Fix typo "Alterntively" → "Alternatively" in api-guide.md line 54
- Fix `["* ", ...]` (extra space) in cli-reference.md line 71

## Non-Goals

- Not restructuring the doc site navigation
- Not rewriting getting-started.md, query-language.md, or advanced.md
- Not adding new doc pages (windowed ops gets a section in api-guide, not its own page)
- Not touching TDD docs or specification.md

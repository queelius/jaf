#!/usr/bin/env python3
"""
gen_test_jsonl.py – generate newline-delimited JSON test data
-------------------------------------------------------------

A re-spin of gen_test_json.py that streams objects into
one (or several) .jsonl files.

JSONL is handy for:
• streaming pipelines (Unix tools, Spark, jq, etc.)
• incremental ingestion (e.g. BigQuery, OpenSearch)
• easy diffing / cat / head / tail

No external dependencies – just stdlib.
"""

import argparse
import json
import random
import string
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Union

# ─── Toy data pools ────────────────────────────────────────────────────────────
NAMES = [
    "Alice", "Bob", "Charlie", "Dana", "Eve", "Frank",
    "Grace", "Heidi", "Ivan", "Jack", "Kim", "Liam",
]
TYPES = ["user", "admin", "moderator", "guest", "bot", "superadmin"]
TAGS = ["alpha", "beta", "legacy", "critical", "staff", "new", "founder"]

def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

# ─── Base object factory ───────────────────────────────────────────────────────
def build_base() -> Dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "name": random.choice(NAMES),
        "type": random.choice(TYPES),
        "attributes": {
            "age": random.randint(0, 90),
            "active": random.choice([True, False]),
            "tags": random.sample(TAGS, k=random.randint(0, 3)),
            "metadata": {
                "created": iso_now(),
                "updated": iso_now(),
            },
        },
    }

# ─── Mutators / edge-case mixers ───────────────────────────────────────────────
def drop_random_field(obj: Dict[str, Any]) -> None:
    candidates = ["name", "type", ("attributes", "age"), ("attributes", "metadata")]
    field = random.choice(candidates)
    if isinstance(field, tuple):
        obj.setdefault(field[0], {}).pop(field[1], None)
    else:
        obj.pop(field, None)

def add_extra_field(obj: Dict[str, Any]) -> None:
    obj["extra_" + random.choice(string.ascii_lowercase)] = "surprise!"

def nest_history(obj: Dict[str, Any]) -> None:
    obj["attributes"]["metadata"]["history"] = {
        "logins": [
            {"ts": iso_now(), "ip": f"192.168.1.{random.randint(1,254)}"}
            for _ in range(random.randint(1, 3))
        ]
    }

def break_types(obj: Dict[str, Any]) -> None:
    obj["id"] = random.randint(1, 9_999)
    obj["name"] = [obj.get("name", "Anon")]
    obj["type"] = True
    a = obj["attributes"]
    a["age"], a["active"], a["tags"] = "unknown", "yes", "oops"
    meta = a["metadata"]
    meta["created"], meta["updated"] = 123456, {}

def to_array(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [obj, build_base()]

VARIANTS = [
    ("missing_field", drop_random_field, 0.15),
    ("extra_field", add_extra_field, 0.15),
    ("deeply_nested", nest_history, 0.10),
    ("bad_types", break_types, 0.10),
    ("array_of_entities", to_array, 0.10),
    # ~40 % untouched
]

def generate_record() -> Union[Dict[str, Any], List[Any]]:
    obj = build_base()
    r, cum = random.random(), 0.0
    for _name, mut, prob in VARIANTS:
        cum += prob
        if r < cum:
            result = mut(obj)
            return result if result is not None else obj
    return obj

# ─── CLI / writer ──────────────────────────────────────────────────────────────
def write_shard(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for _ in range(count):
            rec = generate_record()
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate JSONL test corpus.")
    ap.add_argument("-n", "--num", type=int, default=100, help="total records")
    ap.add_argument("-o", "--outfile", type=Path, default=Path("data.jsonl"),
                    help="output file (ignored if sharding)")
    ap.add_argument("--shard-size", type=int, default=0,
                    help="max lines per shard (0 = no sharding)")
    ap.add_argument("-p", "--prefix", default="part-",
                    help="prefix for shard filenames")
    ap.add_argument("--seed", type=int, help="PRNG seed")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.shard_size and args.shard_size > 0:
        outdir = args.outfile.parent if args.outfile.suffix else args.outfile
        outdir.mkdir(parents=True, exist_ok=True)
        remaining = args.num
        shard_idx = 0
        while remaining > 0:
            take = min(remaining, args.shard_size)
            shard_path = outdir / f"{args.prefix}{shard_idx:04d}.jsonl"
            write_shard(shard_path, take)
            remaining -= take
            shard_idx += 1
        print(f"✅ Wrote {args.num} records across {shard_idx} shards in {outdir.resolve()}")
    else:
        write_shard(args.outfile, args.num)
        print(f"✅ Wrote {args.num} records to {args.outfile.resolve()}")

if __name__ == "__main__":
    main()

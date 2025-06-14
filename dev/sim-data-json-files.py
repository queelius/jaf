#!/usr/bin/env python3
"""
gen_test_json.py  –  quick-and-dirty JSON corpus generator
----------------------------------------------------------

Creates a mix of “mostly-schema-compliant” records plus
edge-case variants (missing/extra fields, bad types, deep nesting, arrays).

Files are written into the *current* directory unless you specify --outdir.

Example:
    ./gen_test_json.py -n 1000 -p record- --seed 42
"""

import argparse
import json
import random
import string
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Union

NAMES = [
    "Alice", "Bob", "Charlie", "Dana", "Eve", "Frank",
    "Grace", "Heidi", "Ivan", "Jack", "Kim", "Liam",
]
TYPES = ["user", "admin", "moderator", "guest", "bot", "superadmin"]
TAGS = ["alpha", "beta", "legacy", "critical", "staff", "new", "founder"]


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --- base object ------------------------------------------------------------
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


# --- variant mutators -------------------------------------------------------
def drop_random_field(obj: Dict[str, Any]) -> None:
    """Delete a random top-level or second-level field."""
    candidates = ["name", "type", ("attributes", "age"), ("attributes", "metadata")]
    field = random.choice(candidates)
    if isinstance(field, tuple):
        obj.setdefault(field[0], {}).pop(field[1], None)
    else:
        obj.pop(field, None)


def add_extra_field(obj: Dict[str, Any]) -> None:
    obj["extra_" + random.choice(string.ascii_lowercase)] = "surprise!"


def nest_history(obj: Dict[str, Any]) -> None:
    meta = obj["attributes"]["metadata"]
    meta["history"] = {
        "logins": [
            {"ts": iso_now(), "ip": f"192.168.1.{random.randint(1, 254)}"}
            for _ in range(random.randint(1, 3))
        ]
    }


def break_types(obj: Dict[str, Any]) -> None:
    obj["id"] = random.randint(1, 9999)
    obj["name"] = [obj.get("name", "Anon")]
    obj["type"] = True
    attrs = obj["attributes"]
    attrs["age"] = "unknown"
    attrs["active"] = "yes"
    attrs["tags"] = "oops"
    meta = attrs["metadata"]
    meta["created"] = 1234567890
    meta["updated"] = {}


def to_array(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a small array of sibling entities."""
    return [obj, build_base()]


VARIANTS = [
    ("missing_field", drop_random_field, 0.15),
    ("extra_field", add_extra_field, 0.15),
    ("deeply_nested", nest_history, 0.10),
    ("bad_types", break_types, 0.10),
    ("array_of_entities", to_array, 0.10),  # returns list instead of dict
    # The rest (~40 %) stay normal
]


# ---------------------------------------------------------------------------
def generate_record() -> Union[Dict[str, Any], List[Any]]:
    obj = build_base()
    r = random.random()
    cumulative = 0.0
    for _name, mutator, prob in VARIANTS:
        cumulative += prob
        if r < cumulative:
            result = mutator(obj)  # may mutate in-place or return list
            return result if result is not None else obj
    return obj  # normal case


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic JSON files.")
    parser.add_argument("-n", "--num", type=int, default=100, help="number of files")
    parser.add_argument(
        "-p", "--prefix", default="item-", help="filename prefix (default item-)"
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("."),
        help="output directory (defaults to current dir)",
    )
    parser.add_argument("--seed", type=int, help="PRNG seed (for reproducibility)")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    args.outdir.mkdir(parents=True, exist_ok=True)

    for i in range(1, args.num + 1):
        payload = generate_record()
        fname = args.outdir / f"{args.prefix}{i:04d}.json"
        with fname.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)

    print(f"✅ Wrote {args.num} JSON files to {args.outdir.resolve()}")


if __name__ == "__main__":
    main()

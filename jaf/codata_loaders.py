"""
Codata/infinite collection loaders for JAF.

These loaders generate potentially infinite streams of data, perfect for
testing lazy evaluation and demonstrating the power of streaming processing.
"""

import random
import itertools
import time
import math
from typing import Generator, Dict, Any, Optional, List
from datetime import datetime, timedelta


def stream_prng(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream pseudo-random generated data based on a seed.

    Args:
        source: Dict with:
            - seed: Random seed (optional, defaults to None)
            - template: Template for generated objects
            - limit: Optional limit on number of items (for testing)

    Example:
        {
            "type": "prng",
            "seed": 42,
            "template": {
                "type": "user",
                "id": {"$random": "int", "min": 1, "max": 1000000},
                "name": {"$random": "choice", "choices": ["Alice", "Bob", "Charlie", "Diana"]},
                "age": {"$random": "int", "min": 18, "max": 80},
                "score": {"$random": "float", "min": 0, "max": 100},
                "active": {"$random": "bool"},
                "tags": {"$random": "sample", "population": ["admin", "user", "premium", "beta"], "k": 2}
            }
        }
    """
    seed = source.get("seed")
    template = source.get("template", {})
    limit = source.get("limit")

    rng = random.Random(seed)
    count = 0

    while limit is None or count < limit:
        obj = _generate_from_template(template, rng)
        yield obj
        count += 1


def _generate_from_template(template: Any, rng: random.Random) -> Any:
    """Recursively generate data from a template."""
    if isinstance(template, dict):
        if "$random" in template:
            # Handle random generation directive
            rand_type = template["$random"]

            if rand_type == "int":
                return rng.randint(template.get("min", 0), template.get("max", 100))
            elif rand_type == "float":
                return rng.uniform(template.get("min", 0), template.get("max", 1))
            elif rand_type == "bool":
                return rng.choice([True, False])
            elif rand_type == "choice":
                return rng.choice(template["choices"])
            elif rand_type == "sample":
                population = template["population"]
                k = min(template.get("k", 1), len(population))
                return rng.sample(population, k)
            elif rand_type == "string":
                length = template.get("length", 10)
                chars = template.get("chars", "abcdefghijklmnopqrstuvwxyz")
                return "".join(rng.choice(chars) for _ in range(length))
            elif rand_type == "uuid":
                # Simple UUID-like string
                return f"{rng.randint(0, 0xffffffff):08x}-{rng.randint(0, 0xffff):04x}-{rng.randint(0, 0xffff):04x}-{rng.randint(0, 0xffff):04x}-{rng.randint(0, 0xffffffffffff):012x}"
        else:
            # Regular dict - recursively process values
            return {k: _generate_from_template(v, rng) for k, v in template.items()}
    elif isinstance(template, list):
        # Process each item in the list
        return [_generate_from_template(item, rng) for item in template]
    else:
        # Literal value
        return template


def stream_fibonacci(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream Fibonacci sequence as objects.

    Args:
        source: Dict with:
            - start_index: Starting index in sequence (default: 0)
            - include_metadata: Whether to include additional metadata (default: True)
    """
    start_index = source.get("start_index", 0)
    include_metadata = source.get("include_metadata", True)

    a, b = 0, 1
    index = 0

    # Skip to start_index if needed
    for _ in range(start_index):
        a, b = b, a + b
        index += 1

    while True:
        if include_metadata:
            yield {
                "index": index,
                "value": a,
                "ratio_to_previous": b / a if a != 0 else None,
                "is_even": a % 2 == 0,
                "digits": len(str(a)),
                "sum_of_digits": sum(int(d) for d in str(a)),
            }
        else:
            yield {"index": index, "value": a}

        a, b = b, a + b
        index += 1


def stream_time_series(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream time series data with various patterns.

    Args:
        source: Dict with:
            - start_time: ISO timestamp or "now" (default: "now")
            - interval_seconds: Seconds between data points (default: 60)
            - pattern: "sine", "random_walk", "sawtooth", "square" (default: "sine")
            - noise: Amount of random noise to add (default: 0.1)
            - trend: Linear trend to add per step (default: 0)
    """
    pattern = source.get("pattern", "sine")
    interval = source.get("interval_seconds", 60)
    noise_level = source.get("noise", 0.1)
    trend = source.get("trend", 0)

    start_time = source.get("start_time", "now")
    if start_time == "now":
        current_time = datetime.now()
    else:
        current_time = datetime.fromisoformat(start_time)

    rng = random.Random(source.get("seed"))
    step = 0
    base_value = 50
    current_value = base_value

    while True:
        # Calculate pattern value
        if pattern == "sine":
            pattern_value = base_value + 20 * math.sin(step * 0.1)
        elif pattern == "sawtooth":
            pattern_value = base_value + 20 * ((step % 20) / 10 - 1)
        elif pattern == "square":
            pattern_value = base_value + 20 * (1 if (step // 10) % 2 == 0 else -1)
        elif pattern == "random_walk":
            current_value += rng.uniform(-2, 2)
            pattern_value = current_value
        else:
            pattern_value = base_value

        # Add trend and noise
        value = (
            pattern_value
            + trend * step
            + rng.uniform(-noise_level * 10, noise_level * 10)
        )

        yield {
            "timestamp": current_time.isoformat(),
            "value": value,
            "step": step,
            "day_of_week": current_time.strftime("%A"),
            "hour": current_time.hour,
            "is_weekend": current_time.weekday() >= 5,
        }

        current_time += timedelta(seconds=interval)
        step += 1


def stream_fractal(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream points from fractal patterns (Mandelbrot set, Julia set, etc.).

    Args:
        source: Dict with:
            - fractal_type: "mandelbrot", "julia" (default: "mandelbrot")
            - resolution: Points per unit (default: 100)
            - bounds: {"xmin": -2, "xmax": 2, "ymin": -2, "ymax": 2}
            - max_iterations: Maximum iterations for escape (default: 100)
            - julia_c: Complex constant for Julia set (default: -0.7+0.27i)
    """
    fractal_type = source.get("fractal_type", "mandelbrot")
    resolution = source.get("resolution", 100)
    bounds = source.get("bounds", {"xmin": -2, "xmax": 2, "ymin": -2, "ymax": 2})
    max_iter = source.get("max_iterations", 100)

    # For Julia sets
    julia_c = source.get("julia_c", {"real": -0.7, "imag": 0.27015})
    c_julia = complex(julia_c["real"], julia_c["imag"])

    xmin, xmax = bounds["xmin"], bounds["xmax"]
    ymin, ymax = bounds["ymin"], bounds["ymax"]

    # Generate points in a spiral pattern for better visualization
    for i in itertools.count():
        # Convert linear index to 2D coordinates in a expanding pattern
        ring = int(math.sqrt(i))
        pos_in_ring = i - ring * ring

        if ring == 0:
            x_idx, y_idx = 0, 0
        else:
            side_length = 2 * ring
            if pos_in_ring < side_length:
                x_idx = ring
                y_idx = pos_in_ring - ring
            elif pos_in_ring < 2 * side_length:
                x_idx = ring - (pos_in_ring - side_length)
                y_idx = ring
            elif pos_in_ring < 3 * side_length:
                x_idx = -ring
                y_idx = ring - (pos_in_ring - 2 * side_length)
            else:
                x_idx = -ring + (pos_in_ring - 3 * side_length)
                y_idx = -ring

        # Convert to fractal coordinates
        x = xmin + (x_idx + resolution) * (xmax - xmin) / (2 * resolution)
        y = ymin + (y_idx + resolution) * (ymax - ymin) / (2 * resolution)

        # Skip if outside bounds
        if x < xmin or x > xmax or y < ymin or y > ymax:
            continue

        # Calculate fractal value
        if fractal_type == "mandelbrot":
            c = complex(x, y)
            z = 0
        else:  # julia
            z = complex(x, y)
            c = c_julia

        iterations = 0
        while abs(z) < 2 and iterations < max_iter:
            z = z * z + c
            iterations += 1

        yield {
            "x": x,
            "y": y,
            "iterations": iterations,
            "escaped": iterations < max_iter,
            "convergence_rate": iterations / max_iter,
            "magnitude": abs(z),
        }


def stream_composite(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream data by composing/transforming data from other sources.

    Args:
        source: Dict with:
            - sources: List of source descriptors to combine
            - mode: "zip", "round_robin", "merge" (default: "zip")
            - transform: Optional template to transform combined data
    """
    sources = source.get("sources", [])
    mode = source.get("mode", "zip")
    transform = source.get("transform")

    if not sources:
        return

    # Create streams for each source
    streams = [loader.stream(src) for src in sources]

    if mode == "zip":
        # Combine items from each stream together
        for items in itertools.zip_longest(*streams, fillvalue=None):
            if transform:
                yield _apply_transform(items, transform)
            else:
                yield {"sources": [item for item in items if item is not None]}

    elif mode == "round_robin":
        # Alternate between sources
        for item in itertools.chain.from_iterable(
            itertools.zip_longest(*streams, fillvalue=None)
        ):
            if item is not None:
                yield item

    elif mode == "merge":
        # Merge all items into single stream (may exhaust memory if sources are infinite!)
        for stream in streams:
            for item in stream:
                yield item


def _apply_transform(items: List[Any], transform: Dict[str, Any]) -> Dict[str, Any]:
    """Apply a transformation template to combined items."""
    result = {}
    for key, spec in transform.items():
        if isinstance(spec, dict) and "$source" in spec:
            source_idx = spec["$source"]
            field = spec.get("field")
            if 0 <= source_idx < len(items) and items[source_idx] is not None:
                if field:
                    result[key] = items[source_idx].get(field)
                else:
                    result[key] = items[source_idx]
        else:
            result[key] = spec
    return result


def stream_prime(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream prime numbers with various properties.

    Args:
        source: Dict with:
            - start: Starting number to check (default: 2)
            - include_gaps: Include gap to previous prime (default: True)
            - include_factors: Include prime factorization (default: False)
    """
    start = source.get("start", 2)
    include_gaps = source.get("include_gaps", True)
    include_factors = source.get("include_factors", False)

    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                return False
        return True

    current = max(2, start)
    prev_prime = None
    prime_count = 0

    while True:
        if is_prime(current):
            prime_count += 1

            result = {
                "value": current,
                "index": prime_count,
                "is_twin_prime": is_prime(current + 2)
                or (current > 2 and is_prime(current - 2)),
                "digit_sum": sum(int(d) for d in str(current)),
                "last_digit": current % 10,
            }

            if include_gaps and prev_prime is not None:
                result["gap"] = current - prev_prime
                result["gap_is_even"] = (current - prev_prime) % 2 == 0

            if include_factors:
                # For primes, the only factors are 1 and itself
                result["factors"] = [1, current]
                result["factor_count"] = 2

            yield result
            prev_prime = current

        current += 1


def stream_distribution(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream numbers from various probability distributions.

    Args:
        source: Dict with:
            - distribution: Name of distribution (uniform, normal, exponential, poisson, etc.)
            - seed: Random seed for reproducibility (optional)
            - limit: Optional limit on number of values
            - as_object: If True, yield {"value": x, "index": i}, else just x (default: True)
            - parameters: Distribution-specific parameters

    Supported distributions:
        - uniform: min, max (default: 0, 1)
        - normal/gaussian: mean, std (default: 0, 1)
        - exponential: lambda/rate (default: 1.0)
        - poisson: lambda/mean (default: 1.0)
        - binomial: n, p (default: 10, 0.5)
        - gamma: alpha/shape, beta/scale (default: 1.0, 1.0)
        - beta: alpha, beta (default: 1.0, 1.0)
        - lognormal: mean, std (of underlying normal) (default: 0, 1)
        - chi2: df (degrees of freedom) (default: 1)
        - pareto: alpha/shape (default: 1.0)
        - weibull: shape, scale (default: 1.0, 1.0)

    Example:
        {
            "type": "distribution",
            "distribution": "normal",
            "seed": 42,
            "parameters": {"mean": 100, "std": 15},
            "as_object": true
        }
    """
    dist_name = source.get("distribution", "uniform").lower()
    seed = source.get("seed")
    limit = source.get("limit")
    as_object = source.get("as_object", True)
    params = source.get("parameters", {})

    rng = random.Random(seed)
    count = 0

    # Distribution generators
    if dist_name in ["uniform", "unif"]:
        min_val = params.get("min", 0)
        max_val = params.get("max", 1)
        generator = lambda: rng.uniform(min_val, max_val)

    elif dist_name in ["normal", "gaussian", "norm"]:
        mean = params.get("mean", params.get("mu", 0))
        std = params.get("std", params.get("sigma", 1))
        generator = lambda: rng.gauss(mean, std)

    elif dist_name in ["exponential", "exp"]:
        rate = params.get("lambda", params.get("rate", 1.0))
        generator = lambda: rng.expovariate(rate)

    elif dist_name == "poisson":
        mean = params.get("lambda", params.get("mean", 1.0))

        # Simple Poisson using Knuth's algorithm for small lambda
        def poisson():
            L = math.exp(-mean)
            k = 0
            p = 1.0
            while p > L:
                k += 1
                p *= rng.random()
            return k - 1

        generator = poisson

    elif dist_name == "binomial":
        n = params.get("n", 10)
        p = params.get("p", 0.5)
        generator = lambda: sum(1 for _ in range(n) if rng.random() < p)

    elif dist_name == "gamma":
        alpha = params.get("alpha", params.get("shape", 1.0))
        beta = params.get("beta", params.get("scale", 1.0))
        generator = lambda: rng.gammavariate(alpha, beta)

    elif dist_name == "beta":
        alpha = params.get("alpha", 1.0)
        beta = params.get("beta", 1.0)
        generator = lambda: rng.betavariate(alpha, beta)

    elif dist_name == "lognormal":
        mean = params.get("mean", params.get("mu", 0))
        std = params.get("std", params.get("sigma", 1))
        generator = lambda: rng.lognormvariate(mean, std)

    elif dist_name in ["chi2", "chisquare"]:
        df = params.get("df", 1)
        # Chi-square is gamma with alpha=df/2, beta=2
        generator = lambda: rng.gammavariate(df / 2, 2)

    elif dist_name == "pareto":
        alpha = params.get("alpha", params.get("shape", 1.0))
        generator = lambda: rng.paretovariate(alpha)

    elif dist_name == "weibull":
        shape = params.get("shape", params.get("k", 1.0))
        scale = params.get("scale", params.get("lambda", 1.0))
        generator = lambda: scale * rng.weibullvariate(scale, shape)

    else:
        raise ValueError(f"Unknown distribution: {dist_name}")

    # Generate values
    while limit is None or count < limit:
        value = generator()

        if as_object:
            yield {
                "index": count,
                "value": value,
                "distribution": dist_name,
            }
        else:
            yield value

        count += 1


def stream_counter(
    loader: "StreamingLoader", source: Dict[str, Any]
) -> Generator[Any, None, None]:
    """
    Stream a simple counter/sequence.

    Args:
        source: Dict with:
            - start: Starting value (default: 0)
            - step: Increment step (default: 1)
            - limit: Optional limit
            - as_object: If True, yield {"index": i, "value": v}, else just v
    """
    start = source.get("start", 0)
    step = source.get("step", 1)
    limit = source.get("limit")
    as_object = source.get("as_object", False)

    current = start
    index = 0

    while limit is None or index < limit:
        if as_object:
            yield {"index": index, "value": current}
        else:
            yield current

        current += step
        index += 1


# Register the codata loaders
def register_codata_loaders(loader: "StreamingLoader"):
    """Register all codata loaders with the streaming loader."""
    loader.register("prng", stream_prng)
    loader.register("distribution", stream_distribution)
    loader.register("counter", stream_counter)
    loader.register("fibonacci", stream_fibonacci)
    loader.register("time_series", stream_time_series)
    loader.register("fractal", stream_fractal)
    loader.register("composite", stream_composite)
    loader.register("prime", stream_prime)

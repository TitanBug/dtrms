import math


def dominant_share(allocated: dict, capacity: dict) -> float:
    """Max over resources of allocated/capacity -- the bottleneck resource's
    utilisation fraction. Used by the least_loaded / best_fit policies."""
    shares = []
    for r, cap in capacity.items():
        if cap <= 0:
            continue
        shares.append(allocated.get(r, 0) / cap)
    return max(shares) if shares else 0.0


def dominant_free_share(allocated: dict, capacity: dict) -> float:
    """Min over resources of (capacity-allocated)/capacity -- how much
    headroom is left on the scarcest resource. best_fit picks the site
    that minimises this (tightest fit, preserves big holes elsewhere)."""
    frees = []
    for r, cap in capacity.items():
        if cap <= 0:
            continue
        frees.append((cap - allocated.get(r, 0)) / cap)
    return min(frees) if frees else 0.0


def fits(allocated: dict, demand: dict, capacity: dict) -> bool:
    for r, cap in capacity.items():
        if allocated.get(r, 0) + demand.get(r, 0) > cap + 1e-9:
            return False
    return True


def jains_fairness_index(xs) -> float:
    """J = (sum x_i)^2 / (n * sum x_i^2). J=1 means perfectly proportional
    to tickets (design doc 4.4)."""
    xs = [x for x in xs if x is not None]
    n = len(xs)
    if n == 0:
        return None
    s1 = sum(xs)
    s2 = sum(x * x for x in xs)
    if s2 == 0:
        return 1.0
    return (s1 * s1) / (n * s2)


def format_sid(n: int) -> str:
    return f"S{n:05d}"


def ceil_div(a, b):
    return math.ceil(a / b)

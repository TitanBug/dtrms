"""
Network topology G = (V, E). Each link has a capacity (Mb/s) and a
latency (ms). Placement uses the shortest-*latency* path (design doc
5.1), and every link on that path must have enough free bandwidth.
"""
import heapq


class Topology:
    def __init__(self, links):
        # links: [{"a":..., "b":..., "capacity":..., "latency_ms":...}, ...]
        self.adj = {}                 # node -> [(neighbor, latency_ms), ...]
        self.capacity = {}            # frozenset({a,b}) -> capacity (Mb/s)
        self.reserved = {}            # frozenset({a,b}) -> reserved bandwidth (Mb/s)
        self.canonical = {}           # frozenset({a,b}) -> (a, b) for display
        for link in links:
            a, b, cap, lat = link["a"], link["b"], link["capacity"], link["latency_ms"]
            self.adj.setdefault(a, []).append((b, lat))
            self.adj.setdefault(b, []).append((a, lat))
            key = frozenset((a, b))
            self.capacity[key] = cap
            self.reserved[key] = 0.0
            self.canonical[key] = (a, b)

    def shortest_path(self, src, dst):
        """Dijkstra by latency. Returns (path_nodes, total_latency_ms),
        or (None, None) if unreachable."""
        if src == dst:
            return [src], 0
        dist = {src: 0}
        prev = {}
        visited = set()
        pq = [(0, src)]
        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            if u == dst:
                break
            for v, w in self.adj.get(u, []):
                nd = d + w
                if nd < dist.get(v, float("inf")):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))
        if dst not in dist:
            return None, None
        path = [dst]
        while path[-1] != src:
            path.append(prev[path[-1]])
        path.reverse()
        return path, dist[dst]

    def path_edges(self, path):
        return [frozenset((path[i], path[i + 1])) for i in range(len(path) - 1)]

    def free_bw(self, key) -> float:
        return self.capacity[key] - self.reserved[key]

    def path_has_bandwidth(self, path, bw: float):
        """Returns (ok, blocking_key_or_None)."""
        for key in self.path_edges(path):
            if self.free_bw(key) + 1e-9 < bw:
                return False, key
        return True, None

    def reserve_path(self, path, bw: float):
        for key in self.path_edges(path):
            self.reserved[key] += bw

    def release_path(self, path, bw: float):
        for key in self.path_edges(path):
            self.reserved[key] = max(0.0, self.reserved[key] - bw)

    def link_utilisation(self) -> dict:
        out = {}
        for key, cap in self.capacity.items():
            a, b = self.canonical[key]
            out[f"{a}<->{b}"] = {
                "capacity": cap,
                "reserved": round(self.reserved[key], 3),
                "utilisation": round(self.reserved[key] / cap, 4) if cap else 0.0,
            }
        return out

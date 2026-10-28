import bisect
import hashlib
from collections.abc import Iterable


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str] = (), virtual_nodes: int = 128) -> None:
        if virtual_nodes <= 0:
            raise ValueError("virtual_nodes must be positive")
        self.virtual_nodes = virtual_nodes
        self._nodes: set[str] = set()
        self._tokens: list[int] = []
        self._owners: dict[int, str] = {}
        for node in sorted(set(nodes)):
            self.add_node(node)

    @staticmethod
    def _hash(value: str) -> int:
        return int.from_bytes(hashlib.sha256(value.encode("utf-8")).digest(), "big")

    @property
    def nodes(self) -> tuple[str, ...]:
        return tuple(sorted(self._nodes))

    def add_node(self, node: str) -> None:
        if not node or node in self._nodes:
            return
        self._nodes.add(node)
        for replica in range(self.virtual_nodes):
            token = self._hash(f"{node}:{replica}")
            self._owners[token] = node
            bisect.insort(self._tokens, token)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.remove(node)
        removed = {self._hash(f"{node}:{replica}") for replica in range(self.virtual_nodes)}
        self._tokens = [token for token in self._tokens if token not in removed]
        for token in removed:
            self._owners.pop(token, None)

    def get_node(self, key: str) -> str:
        return self.get_nodes(key, 1)[0]

    def get_nodes(self, key: str, count: int) -> list[str]:
        if count <= 0:
            raise ValueError("count must be positive")
        if not self._tokens:
            raise LookupError("hash ring has no nodes")
        wanted = min(count, len(self._nodes))
        index = bisect.bisect_left(self._tokens, self._hash(key))
        selected: list[str] = []
        scanned = 0
        while len(selected) < wanted and scanned < len(self._tokens):
            owner = self._owners[self._tokens[(index + scanned) % len(self._tokens)]]
            if owner not in selected:
                selected.append(owner)
            scanned += 1
        return selected


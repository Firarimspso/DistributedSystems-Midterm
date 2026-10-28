from typing import Protocol

import httpx


class StorageOperations(Protocol):
    def exists(self, node_id: str, chunk_hash: str) -> bool: ...
    def put(self, node_id: str, chunk_hash: str, data: bytes) -> None: ...
    def get(self, node_id: str, chunk_hash: str) -> bytes: ...
    def delete(self, node_id: str, chunk_hash: str) -> None: ...


class HTTPStorageClient:
    def __init__(self, node_urls: dict[str, str], timeout: float = 5.0) -> None:
        self.node_urls = node_urls
        self.client = httpx.Client(timeout=timeout)

    def _url(self, node_id: str, chunk_hash: str) -> str:
        return f"{self.node_urls[node_id].rstrip('/')}/chunks/{chunk_hash}"

    def exists(self, node_id: str, chunk_hash: str) -> bool:
        response = self.client.head(self._url(node_id, chunk_hash))
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def put(self, node_id: str, chunk_hash: str, data: bytes) -> None:
        response = self.client.put(self._url(node_id, chunk_hash), content=data)
        response.raise_for_status()

    def get(self, node_id: str, chunk_hash: str) -> bytes:
        response = self.client.get(self._url(node_id, chunk_hash))
        response.raise_for_status()
        return response.content

    def delete(self, node_id: str, chunk_hash: str) -> None:
        response = self.client.delete(self._url(node_id, chunk_hash))
        if response.status_code != 404:
            response.raise_for_status()

    def close(self) -> None:
        self.client.close()


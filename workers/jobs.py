import argparse
import asyncio
import json
import os
import uuid
from dataclasses import asdict, dataclass

import httpx


@dataclass(frozen=True)
class MaintenanceJob:
    job_id: str
    job_type: str
    attempt: int = 0
    max_attempts: int = 3

    @classmethod
    def create(cls, job_type: str) -> "MaintenanceJob":
        if job_type not in {"repair", "rebalance", "garbage_collection"}:
            raise ValueError("unsupported maintenance job type")
        return cls(str(uuid.uuid4()), job_type)


class RabbitManagementQueue:
    """Small RabbitMQ Management API adapter; no additional AMQP dependency."""

    def __init__(self, url: str, user: str, password: str, queue: str = "maintenance") -> None:
        self.client = httpx.AsyncClient(base_url=url, auth=(user, password), timeout=5)
        self.queue = queue

    async def publish(self, job: MaintenanceJob) -> None:
        response = await self.client.post("/api/exchanges/%2F/amq.default/publish", json={
            "properties": {"delivery_mode": 2}, "routing_key": self.queue,
            "payload": json.dumps(asdict(job)), "payload_encoding": "string"})
        response.raise_for_status()
        if not response.json().get("routed"):
            raise RuntimeError("RabbitMQ did not route maintenance job")

    async def get(self) -> MaintenanceJob | None:
        response = await self.client.post(f"/api/queues/%2F/{self.queue}/get", json={
            "count": 1, "ackmode": "ack_requeue_false", "encoding": "auto", "truncate": 50000})
        response.raise_for_status()
        messages = response.json()
        return MaintenanceJob(**json.loads(messages[0]["payload"])) if messages else None

    async def close(self) -> None:
        await self.client.aclose()


class JobExecutor:
    PATHS = {"repair": "/repairs/run", "rebalance": "/maintenance/rebalance",
             "garbage_collection": "/maintenance/garbage-collection"}

    def __init__(self, metadata_url: str, client: httpx.AsyncClient | None = None) -> None:
        self.client = client or httpx.AsyncClient(base_url=metadata_url, timeout=30)
        self.completed: set[str] = set()

    async def execute(self, job: MaintenanceJob) -> bool:
        if job.job_id in self.completed:
            return False
        response = await self.client.post(self.PATHS[job.job_type], headers={"Idempotency-Key": job.job_id})
        response.raise_for_status()
        self.completed.add(job.job_id)
        return True

    async def close(self) -> None:
        await self.client.aclose()


async def worker_loop(queue: RabbitManagementQueue, executor: JobExecutor, once: bool = False) -> None:
    while True:
        job = await queue.get()
        if job is None:
            if once:
                return
            await asyncio.sleep(1)
            continue
        try:
            await executor.execute(job)
        except Exception:
            if job.attempt + 1 < job.max_attempts:
                await queue.publish(MaintenanceJob(job.job_id, job.job_type, job.attempt + 1, job.max_attempts))
            if once:
                return


async def main_async(command: str, job_type: str | None) -> None:
    queue = RabbitManagementQueue(os.getenv("RABBITMQ_MANAGEMENT_URL", "http://rabbitmq:15672"),
                                  os.getenv("RABBITMQ_USER", "storage"),
                                  os.getenv("RABBITMQ_PASSWORD", "storage"))
    executor = JobExecutor(os.getenv("METADATA_URL", "http://metadata:9000"))
    try:
        if command == "enqueue":
            await queue.publish(MaintenanceJob.create(job_type))
        else:
            await worker_loop(queue, executor)
    finally:
        await queue.close()
        await executor.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["work", "enqueue"])
    parser.add_argument("job_type", nargs="?", choices=["repair", "rebalance", "garbage_collection"])
    args = parser.parse_args()
    if args.command == "enqueue" and not args.job_type:
        parser.error("enqueue requires job_type")
    asyncio.run(main_async(args.command, args.job_type))


if __name__ == "__main__":
    main()


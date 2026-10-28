import asyncio

import httpx

from workers.jobs import JobExecutor, MaintenanceJob, worker_loop


class FakeQueue:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.published = []

    async def get(self):
        return self.jobs.pop(0) if self.jobs else None

    async def publish(self, job):
        self.published.append(job)


def test_job_executor_is_idempotent() -> None:
    calls = []
    async def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"ok": True})
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://metadata")
    executor = JobExecutor("http://metadata", client)
    job = MaintenanceJob("stable-id", "repair")
    assert asyncio.run(executor.execute(job)) is True
    assert asyncio.run(executor.execute(job)) is False
    assert calls == ["/repairs/run"]
    asyncio.run(client.aclose())


def test_failed_job_is_requeued_with_bounded_retry() -> None:
    async def handler(request):
        return httpx.Response(503)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://metadata")
    executor = JobExecutor("http://metadata", client)
    queue = FakeQueue([MaintenanceJob("retry-id", "rebalance", attempt=0, max_attempts=3)])
    asyncio.run(worker_loop(queue, executor, once=True))
    assert len(queue.published) == 1
    assert queue.published[0].job_id == "retry-id"
    assert queue.published[0].attempt == 1
    asyncio.run(client.aclose())

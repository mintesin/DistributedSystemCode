import asyncio
from webClient import WebClient

class Aggregator:
    def __init__(self):
        self.web_client = WebClient()

    async def init(self):
        await self.web_client.init_session()

    async def send_tasks_to_workers(self, worker_addresses, tasks):
        tasks_coroutines = [
            self.web_client.send_task(worker, task.encode())
            for worker, task in zip(worker_addresses, tasks)
        ]
        results = await asyncio.gather(*tasks_coroutines)
        return results

    async def close(self):
        await self.web_client.close()

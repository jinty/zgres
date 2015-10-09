import asyncio
from asyncio import sleep

class FakeSleeper:

    def __init__(self, loops=1):
        self.log = []
        self.loops = loops
        self.finished = asyncio.Event()
        self.wait = self.finished.wait
        asyncio.get_event_loop().call_later(1, self._watchdog)

    def _watchdog(self):
        self.finished.set()
        raise Exception('Timed Out')

    async def __call__(self, delay):
        self.log.append(delay)
        if len(self.log) >= self.loops:
            self.finished.set()
            await sleep(10000)
            raise AssertionError('boom')
